use bytes::Bytes;

use ratatosk_resp::frame::RespFrame;

use crate::keyspace::ServerState;
use crate::security::next_audit_stamp;

use super::{ClientState, CommandOutcome, err, wrong_arity};

const AUTH_FAILURE: &str = "ERR invalid username-password pair or user is disabled.";
const AUTH_FAILURE_THRESHOLD: u32 = 5;

pub(super) enum AuthenticationResult {
    Authenticated,
    Rejected(CommandOutcome),
}

pub(super) fn authenticate_client(
    server: &ServerState,
    username: &Bytes,
    password: &Bytes,
    client: &mut ClientState,
) -> AuthenticationResult {
    if client.auth_failure_count() >= AUTH_FAILURE_THRESHOLD {
        record_auth_attempt(username, client, false);
        return AuthenticationResult::Rejected(auth_failure_outcome(
            client.auth_failure_count(),
            true,
        ));
    }

    let (authenticated, shared_ip_blocked) = if let Some(peer_ip) = client.peer_ip() {
        // Admission, password verification, and failure recording are one
        // critical section so concurrent connections from the same IP cannot
        // all pass the threshold check at once.
        let mut limiter = server.auth_rate_limiter.lock();
        if limiter.is_blocked(peer_ip) {
            (false, true)
        } else if server.acl.authenticate_user(username, password) {
            (true, false)
        } else {
            (false, limiter.record_failure(peer_ip))
        }
    } else {
        (server.acl.authenticate_user(username, password), false)
    };

    if authenticated {
        client.authenticated = true;
        client.acl_user = username.clone();
        client.reset_auth_failures();
        record_auth_attempt(username, client, true);
        return AuthenticationResult::Authenticated;
    }

    let failures = client.increment_auth_failures();
    let should_close = shared_ip_blocked || failures >= AUTH_FAILURE_THRESHOLD;
    if should_close {
        tracing::warn!(
            target = "ratatosk::security",
            client_id = client.id(),
            "authentication failure threshold reached; disconnecting client"
        );
    }
    record_auth_attempt(username, client, false);
    AuthenticationResult::Rejected(auth_failure_outcome(failures, should_close))
}

fn auth_failure_delay_ms(failures: u32) -> u64 {
    use rand::Rng;
    let exponent = failures.saturating_sub(1).min(5);
    let base = 100u64.saturating_mul(1u64 << exponent).min(2000);
    let jitter: f64 = rand::thread_rng().gen_range(0.8..1.2);
    (base as f64 * jitter) as u64
}

fn auth_failure_outcome(failures: u32, should_close: bool) -> CommandOutcome {
    let delay = auth_failure_delay_ms(failures);
    if should_close {
        CommandOutcome::close_with_delay(err(AUTH_FAILURE), delay)
    } else {
        CommandOutcome::reply_with_delay(err(AUTH_FAILURE), delay)
    }
}

fn record_auth_attempt(username: &Bytes, client: &ClientState, success: bool) {
    let result_label = if success { "success" } else { "failure" };
    metrics::counter!("ratatosk_auth_attempts_total", "result" => result_label).increment(1);

    let username_text = String::from_utf8_lossy(username).into_owned();
    let payload = format!(
        "event=AUTH client_id={} username={} success={}",
        client.id(),
        username_text,
        success
    );
    let stamp = next_audit_stamp("AUTH", &payload);
    tracing::info!(
        target = "ratatosk::audit",
        event = "AUTH",
        audit_seq = stamp.seq,
        audit_prev_hash = %stamp.prev_hash,
        audit_hash = %stamp.hash,
        client_id = client.id(),
        username = %username_text,
        success,
        "ACL authentication attempt"
    );
}

pub(super) fn cmd_auth(
    args: &[Bytes],
    server: &ServerState,
    client: &mut ClientState,
) -> CommandOutcome {
    let (username, password) = match args {
        [password] => (Bytes::from_static(b"default"), password),
        [username, password] => (username.clone(), password),
        _ => return wrong_arity("auth"),
    };

    match authenticate_client(server, &username, password, client) {
        AuthenticationResult::Authenticated => CommandOutcome::reply(RespFrame::ok()),
        AuthenticationResult::Rejected(outcome) => outcome,
    }
}

pub(super) fn cmd_reset(
    args: &[Bytes],
    server: &mut ServerState,
    client: &mut ClientState,
) -> CommandOutcome {
    if !args.is_empty() {
        return wrong_arity("reset");
    }

    server.tracking_remove_client(client.id());
    server.unregister_monitor(client.id());
    server.pubsub.unsubscribe_all(client.id());
    client.release_watches(&server.data);
    client.reset_for_connection();
    CommandOutcome::reply(RespFrame::simple_str("RESET"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::acl::AclState;

    fn password_server() -> ServerState {
        let mut server = ServerState::with_default_dbs();
        let default = server
            .acl
            .get_or_create_user_mut(&Bytes::from_static(b"default"));
        default.nopass = false;
        default
            .passwords
            .insert(AclState::hash_password(b"secret").expect("test password should hash"));
        server
    }

    #[test]
    fn malformed_auth_is_not_a_guess_and_success_resets_only_connection_count() {
        let server = password_server();
        let mut client = ClientState::new(1);

        for _ in 0..5 {
            assert!(matches!(
                cmd_auth(&[], &server, &mut client).response,
                RespFrame::Error(_)
            ));
        }
        assert_eq!(client.auth_failure_count(), 0);

        for _ in 0..4 {
            let outcome = cmd_auth(&[Bytes::from_static(b"wrong")], &server, &mut client);
            assert!(!outcome.close);
        }
        assert_eq!(client.auth_failure_count(), 4);

        let success = cmd_auth(&[Bytes::from_static(b"secret")], &server, &mut client);
        assert_eq!(success.response, RespFrame::ok());
        assert_eq!(client.auth_failure_count(), 0);

        for _ in 0..4 {
            assert!(!cmd_auth(&[Bytes::from_static(b"wrong")], &server, &mut client).close);
        }
        assert!(cmd_auth(&[Bytes::from_static(b"wrong")], &server, &mut client).close);
    }
}
