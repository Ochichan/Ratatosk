use bytes::Bytes;

use ratatosk_resp::frame::RespFrame;

use crate::keyspace::ServerState;
use crate::security::next_audit_stamp;

use super::{ClientState, CommandOutcome, err, wrong_arity};

const AUTH_FAILURE: &str = "ERR invalid username-password pair or user is disabled.";
const AUTH_FAILURE_THRESHOLD: u32 = 5;

pub(super) fn authenticate_client(
    server: &ServerState,
    username: &Bytes,
    password: &Bytes,
    client: &mut ClientState,
) -> bool {
    if server.acl.authenticate_user(username, password) {
        client.authenticated = true;
        client.acl_user = username.clone();
        return true;
    }
    false
}

fn auth_failure_delay_ms(failures: u32) -> u64 {
    use rand::Rng;
    let base = std::cmp::min(
        100u64.saturating_mul(1u64 << (failures.saturating_sub(1))),
        2000,
    );
    let jitter: f64 = rand::thread_rng().gen_range(0.8..1.2);
    (base as f64 * jitter) as u64
}

fn auth_failure_outcome(failures: u32) -> CommandOutcome {
    let delay = auth_failure_delay_ms(failures);
    if failures >= AUTH_FAILURE_THRESHOLD {
        CommandOutcome::close_with_delay(err(AUTH_FAILURE), delay)
    } else {
        CommandOutcome::reply_with_delay(err(AUTH_FAILURE), delay)
    }
}

pub(super) fn cmd_auth(
    args: &[Bytes],
    server: &ServerState,
    client: &mut ClientState,
) -> CommandOutcome {
    let username = args.first().map(|_| {
        if args.len() == 1 {
            Bytes::from_static(b"default")
        } else {
            args[0].clone()
        }
    });

    let result = match args {
        [password] => {
            if authenticate_client(server, &Bytes::from_static(b"default"), password, client) {
                client.reset_auth_failures();
                CommandOutcome::reply(RespFrame::ok())
            } else {
                let failures = client.increment_auth_failures();
                if failures >= AUTH_FAILURE_THRESHOLD {
                    tracing::warn!(
                        target = "ratatosk::security",
                        client_id = client.id(),
                        failures,
                        "AUTH failure threshold reached, disconnecting client"
                    );
                }
                auth_failure_outcome(failures)
            }
        }
        [username, password] => {
            if authenticate_client(server, username, password, client) {
                client.reset_auth_failures();
                CommandOutcome::reply(RespFrame::ok())
            } else {
                let failures = client.increment_auth_failures();
                if failures >= AUTH_FAILURE_THRESHOLD {
                    tracing::warn!(
                        target = "ratatosk::security",
                        client_id = client.id(),
                        failures,
                        "AUTH failure threshold reached, disconnecting client"
                    );
                }
                auth_failure_outcome(failures)
            }
        }
        _ => wrong_arity("auth"),
    };

    let success = !matches!(result.response, RespFrame::Error(_));
    let result_label = if success { "success" } else { "failure" };
    metrics::counter!("ratatosk_auth_attempts_total", "result" => result_label.to_string())
        .increment(1);

    let username_text =
        String::from_utf8_lossy(username.as_ref().unwrap_or(&Bytes::from_static(b"unknown")))
            .into_owned();
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

    result
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
    client.reset_for_connection();
    CommandOutcome::reply(RespFrame::simple_str("RESET"))
}
