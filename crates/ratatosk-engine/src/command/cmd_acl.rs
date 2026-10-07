use bytes::Bytes;
use rand::RngCore;
use rand::rngs::OsRng;

use ratatosk_resp::frame::RespFrame;

use crate::keyspace::{AclState, AclUser, ServerState};
use crate::security::next_audit_stamp;

use super::{
    ClientState, CommandOutcome, acl_required_categories, err, parse_i64, to_uppercase_bytes,
    wrong_arity,
};

pub(super) fn cmd_acl(
    args: &[Bytes],
    server: &mut ServerState,
    client: &ClientState,
) -> CommandOutcome {
    if args.is_empty() {
        return wrong_arity("acl");
    }

    let subcommand = to_uppercase_bytes(&args[0]);
    match subcommand.as_slice() {
        b"HELP" => {
            if args.len() != 1 {
                return wrong_arity("acl");
            }
            CommandOutcome::reply(RespFrame::Array(vec![
                RespFrame::bulk_str(
                    "CAT [category] -- List ACL categories or commands in a category.",
                ),
                RespFrame::bulk_str("DELUSER <username> [username ...] -- Remove ACL users."),
                RespFrame::bulk_str(
                    "DRYRUN <username> <command> [arg ...] -- Simulate ACL rule check.",
                ),
                RespFrame::bulk_str(
                    "GENPASS [bits] -- Generate a cryptographically secure password.",
                ),
                RespFrame::bulk_str("GETUSER <username> -- Return ACL rules for the user."),
                RespFrame::bulk_str("LIST -- Show ACL rules for all users."),
                RespFrame::bulk_str("LOAD -- Reload ACL rules from disk."),
                RespFrame::bulk_str("LOG [count|RESET] -- Show or reset ACL log."),
                RespFrame::bulk_str("SAVE -- Persist ACL rules to disk."),
                RespFrame::bulk_str(
                    "SETUSER <username> [rule ...] -- Create/modify ACL user (passwords and category grants only).",
                ),
                RespFrame::bulk_str("USERS -- List ACL users."),
                RespFrame::bulk_str("WHOAMI -- Return the current ACL username."),
                RespFrame::bulk_str("HELP -- Show this help."),
            ]))
        }
        b"CAT" => {
            if args.len() > 2 {
                return wrong_arity("acl");
            }
            if args.len() == 1 {
                return CommandOutcome::reply(RespFrame::Array(vec![
                    RespFrame::bulk_str("admin"),
                    RespFrame::bulk_str("read"),
                    RespFrame::bulk_str("write"),
                    RespFrame::bulk_str("pubsub"),
                    RespFrame::bulk_str("connection"),
                    RespFrame::bulk_str("fast"),
                ]));
            }
            let category = to_uppercase_bytes(&args[1]);
            let items: Vec<RespFrame> = match category.as_slice() {
                b"ADMIN" => vec![RespFrame::bulk_str("acl"), RespFrame::bulk_str("config")],
                b"READ" => vec![RespFrame::bulk_str("get"), RespFrame::bulk_str("mget")],
                b"WRITE" => vec![RespFrame::bulk_str("set"), RespFrame::bulk_str("del")],
                b"CONNECTION" => vec![RespFrame::bulk_str("client"), RespFrame::bulk_str("hello")],
                _ => vec![],
            };
            CommandOutcome::reply(RespFrame::Array(items))
        }
        b"WHOAMI" => {
            if args.len() != 1 {
                return wrong_arity("acl");
            }
            CommandOutcome::reply(RespFrame::BulkString(Some(client.acl_user.clone())))
        }
        b"USERS" => {
            if args.len() != 1 {
                return wrong_arity("acl");
            }
            CommandOutcome::reply(RespFrame::Array(
                server
                    .acl
                    .user_names()
                    .into_iter()
                    .map(|name| RespFrame::BulkString(Some(name)))
                    .collect(),
            ))
        }
        b"LIST" => {
            if args.len() != 1 {
                return wrong_arity("acl");
            }
            let mut rows = Vec::new();
            for name in server.acl.user_names() {
                let Some(user) = server.acl.get_user(&name) else {
                    continue;
                };
                let state = if user.enabled { "on" } else { "off" };
                let pass_state = if user.nopass { "nopass" } else { "#pass" };
                let command_rule = if user.allow_all_commands {
                    "+@all".to_string()
                } else {
                    let mut cats = user
                        .allowed_categories
                        .iter()
                        .map(|v| String::from_utf8_lossy(v).to_ascii_lowercase())
                        .collect::<Vec<_>>();
                    cats.sort();
                    if cats.is_empty() {
                        "-@all".to_string()
                    } else {
                        cats.into_iter()
                            .map(|cat| format!("+@{cat}"))
                            .collect::<Vec<_>>()
                            .join(" ")
                    }
                };
                let line = format!(
                    "user {} {} {} ~* &* {}",
                    String::from_utf8_lossy(&name),
                    state,
                    pass_state,
                    command_rule
                );
                rows.push(RespFrame::bulk_str(&line));
            }
            CommandOutcome::reply(RespFrame::Array(rows))
        }
        b"GETUSER" => {
            let [_, username] = args else {
                return wrong_arity("acl");
            };
            let Some(user) = server.acl.get_user(username) else {
                return CommandOutcome::reply(RespFrame::Null);
            };

            let mut flags = Vec::new();
            flags.push(RespFrame::bulk_str(if user.enabled { "on" } else { "off" }));
            flags.push(RespFrame::bulk_str(if user.nopass {
                "nopass"
            } else {
                "resetpass"
            }));

            let commands = if user.allow_all_commands {
                "+@all".to_string()
            } else {
                let mut categories = user
                    .allowed_categories
                    .iter()
                    .map(|category| String::from_utf8_lossy(category).to_ascii_lowercase())
                    .collect::<Vec<_>>();
                categories.sort();
                categories
                    .into_iter()
                    .map(|category| format!("+@{category}"))
                    .collect::<Vec<_>>()
                    .join(" ")
            };

            CommandOutcome::reply(RespFrame::Map(vec![
                (RespFrame::bulk_str("flags"), RespFrame::Array(flags)),
                // Never expose password hashes or secrets over command output.
                (RespFrame::bulk_str("passwords"), RespFrame::Array(vec![])),
                (
                    RespFrame::bulk_str("commands"),
                    RespFrame::bulk_str(&commands),
                ),
                (RespFrame::bulk_str("keys"), RespFrame::bulk_str("~*")),
                (RespFrame::bulk_str("channels"), RespFrame::bulk_str("&*")),
                (RespFrame::bulk_str("selectors"), RespFrame::Array(vec![])),
            ]))
        }
        b"SETUSER" => {
            if args.len() < 2 {
                return wrong_arity("acl");
            }
            let username = args[1].clone();
            // Rules apply to a copy that replaces the stored user only when
            // every rule is valid, so a rejected SETUSER changes nothing and
            // creates no user (Redis applies SETUSER all or nothing).
            let mut user: AclUser = server
                .acl
                .get_user(&username)
                .cloned()
                .unwrap_or_else(AclUser::new_disabled);
            {
                let user = &mut user;
                for rule in &args[2..] {
                    if rule.eq_ignore_ascii_case(b"on") {
                        user.enabled = true;
                    } else if rule.eq_ignore_ascii_case(b"off") {
                        user.enabled = false;
                    } else if rule.eq_ignore_ascii_case(b"nopass") {
                        // Like Redis, nopass also forgets every password.
                        user.passwords.clear();
                        user.nopass = true;
                    } else if rule.eq_ignore_ascii_case(b"resetpass") {
                        user.passwords.clear();
                        user.nopass = false;
                    } else if rule.starts_with(b">") {
                        let Some(hash) = AclState::hash_password(&rule[1..]) else {
                            return CommandOutcome::reply(err("ERR invalid ACL password"));
                        };
                        user.passwords.insert(hash);
                        user.nopass = false;
                    } else if rule.starts_with(b"<") {
                        user.remove_password(&rule[1..]);
                    } else if rule.eq_ignore_ascii_case(b"reset") {
                        user.enabled = false;
                        user.nopass = false;
                        user.passwords.clear();
                        user.allow_all_commands = false;
                        user.allowed_categories.clear();
                    } else if rule.eq_ignore_ascii_case(b"allcommands")
                        || rule.eq_ignore_ascii_case(b"+@all")
                    {
                        user.allow_all_commands = true;
                    } else if rule.eq_ignore_ascii_case(b"nocommands")
                        || rule.eq_ignore_ascii_case(b"-@all")
                    {
                        user.allow_all_commands = false;
                        user.allowed_categories.clear();
                    } else if let Some(category) = parse_acl_category_rule(rule, b'+') {
                        user.allowed_categories.insert(category);
                    } else if let Some(category) = parse_acl_category_rule(rule, b'-') {
                        user.allowed_categories.remove(&category);
                    } else if rule.starts_with(b"+@") || rule.starts_with(b"-@") {
                        return CommandOutcome::reply(err("ERR syntax error"));
                    } else if is_accepted_non_category_rule(rule) {
                        return CommandOutcome::reply(unsupported_acl_rule(rule));
                    } else {
                        return CommandOutcome::reply(err("ERR syntax error"));
                    }
                }
            }
            server.acl.put_user(username.clone(), user);
            let rule_count = args.len().saturating_sub(2);
            server.acl.push_log(Bytes::from(format!(
                "SETUSER {} rules={} OK",
                String::from_utf8_lossy(&username),
                rule_count
            )));
            let username_text = String::from_utf8_lossy(&username).into_owned();
            let payload = format!(
                "event=ACL_SETUSER client_id={} username={} rules={}",
                client.id(),
                username_text,
                rule_count
            );
            let stamp = next_audit_stamp("ACL_SETUSER", &payload);
            tracing::info!(
                target = "ratatosk::audit",
                event = "ACL_SETUSER",
                audit_seq = stamp.seq,
                audit_prev_hash = %stamp.prev_hash,
                audit_hash = %stamp.hash,
                client_id = client.id(),
                username = %username_text,
                rules = rule_count,
                "ACL user updated"
            );
            CommandOutcome::reply(RespFrame::ok()).with_acl_dirty()
        }
        b"DELUSER" => {
            if args.len() < 2 {
                return wrong_arity("acl");
            }
            let removed = server.acl.del_users(&args[1..]);
            server
                .acl
                .push_log(Bytes::from(format!("DELUSER count={removed}")));
            let requested_users = args.len().saturating_sub(1);
            let payload = format!(
                "event=ACL_DELUSER client_id={} requested_users={} removed_users={}",
                client.id(),
                requested_users,
                removed
            );
            let stamp = next_audit_stamp("ACL_DELUSER", &payload);
            tracing::info!(
                target = "ratatosk::audit",
                event = "ACL_DELUSER",
                audit_seq = stamp.seq,
                audit_prev_hash = %stamp.prev_hash,
                audit_hash = %stamp.hash,
                client_id = client.id(),
                requested_users = requested_users,
                removed_users = removed,
                "ACL users removed"
            );
            CommandOutcome::reply(RespFrame::Integer(removed)).with_acl_dirty()
        }
        b"GENPASS" => {
            if args.len() > 2 {
                return wrong_arity("acl");
            }
            let bits = match args.get(1).map(parse_i64) {
                None => 256usize,
                Some(Some(v @ 1..=4096)) => v as usize,
                Some(_) => {
                    return CommandOutcome::reply(err(
                        "ERR ACL GENPASS argument must be the number of bits for the output password, a positive number up to 4096",
                    ));
                }
            };
            // One hex character per four bits, rounded up, as in Redis.
            let chars = bits.div_ceil(4);
            let bytes_len = chars.div_ceil(2);
            let mut entropy = vec![0u8; bytes_len];
            OsRng.fill_bytes(&mut entropy);
            let mut out = to_hex(&entropy);
            out.truncate(chars);
            CommandOutcome::reply(RespFrame::bulk_str(&out))
        }
        b"LOG" => {
            if args.len() > 2 {
                return wrong_arity("acl");
            }
            if let Some(arg) = args.get(1) {
                if arg.eq_ignore_ascii_case(b"reset") {
                    server.acl.log_reset();
                    return CommandOutcome::reply(RespFrame::ok());
                }
            }
            let count = if let Some(arg) = args.get(1) {
                let Some(v) = parse_i64(arg) else {
                    return CommandOutcome::reply(err(
                        "ERR value is not an integer or out of range",
                    ));
                };
                if v < 0 {
                    return CommandOutcome::reply(err("ERR value is out of range"));
                }
                usize::try_from(v).unwrap_or(0)
            } else {
                10usize
            };
            let rows = server
                .acl
                .log(count)
                .into_iter()
                .map(|line| RespFrame::BulkString(Some(line)))
                .collect::<Vec<_>>();
            CommandOutcome::reply(RespFrame::Array(rows))
        }
        b"LOAD" | b"SAVE" => {
            if args.len() != 1 {
                return wrong_arity("acl");
            }
            let path = AclState::file_path(server.config.dir());
            match subcommand.as_slice() {
                b"LOAD" => match AclState::load_from_file(&path) {
                    Ok(Some(loaded)) => {
                        server.acl = loaded;
                        CommandOutcome::reply(RespFrame::ok()).with_acl_dirty()
                    }
                    Ok(None) => CommandOutcome::reply(err(&format!(
                        "ERR ACL file does not exist: {}",
                        path.display()
                    ))),
                    Err(error) => {
                        CommandOutcome::reply(err(&format!("ERR ACL LOAD failed: {}", error)))
                    }
                },
                b"SAVE" => match server.acl.save_to_file(&path) {
                    Ok(()) => CommandOutcome::reply(RespFrame::ok()),
                    Err(error) => {
                        CommandOutcome::reply(err(&format!("ERR ACL SAVE failed: {}", error)))
                    }
                },
                _ => CommandOutcome::reply(err(
                    "ERR unknown ACL subcommand or wrong number of arguments",
                )),
            }
        }
        b"DRYRUN" => {
            if args.len() < 3 {
                return wrong_arity("acl");
            }
            let username = &args[1];
            let Some(spec) = super::find_command_spec(&args[2]) else {
                return CommandOutcome::reply(err("ERR unknown command"));
            };
            let categories = acl_required_categories(spec);
            if server.acl.command_allowed(username, &categories) {
                CommandOutcome::reply(RespFrame::ok())
            } else {
                CommandOutcome::reply(err("NOPERM ACL DRYRUN denied command"))
            }
        }
        _ => CommandOutcome::reply(err(
            "ERR unknown ACL subcommand or wrong number of arguments",
        )),
    }
}

fn parse_acl_category_rule(rule: &Bytes, prefix: u8) -> Option<Bytes> {
    if rule.len() < 3 {
        return None;
    }
    if rule[0] != prefix || rule[1] != b'@' {
        return None;
    }
    let category = Bytes::copy_from_slice(&rule[2..]);
    if is_valid_acl_category(&category) {
        Some(Bytes::from(category.as_ref().to_ascii_lowercase()))
    } else {
        None
    }
}

fn is_valid_acl_category(category: &Bytes) -> bool {
    category.eq_ignore_ascii_case(b"admin")
        || category.eq_ignore_ascii_case(b"read")
        || category.eq_ignore_ascii_case(b"write")
        || category.eq_ignore_ascii_case(b"pubsub")
        || category.eq_ignore_ascii_case(b"connection")
        || category.eq_ignore_ascii_case(b"fast")
}

fn is_accepted_non_category_rule(rule: &Bytes) -> bool {
    rule.starts_with(b"+")
        || rule.starts_with(b"-")
        || rule.starts_with(b"~")
        || rule.starts_with(b"&")
        || rule.eq_ignore_ascii_case(b"allkeys")
        || rule.eq_ignore_ascii_case(b"allchannels")
}

fn unsupported_acl_rule(rule: &Bytes) -> RespFrame {
    let rule_text = String::from_utf8_lossy(rule);
    err(&format!(
        "ERR ACL rule '{rule_text}' is not supported in this build"
    ))
}

fn to_hex(raw: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(raw.len().saturating_mul(2));
    for byte in raw {
        out.push(HEX[(byte >> 4) as usize] as char);
        out.push(HEX[(byte & 0x0F) as usize] as char);
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::command::cmd_auth_session::cmd_auth;
    use crate::keyspace::ServerState;

    #[test]
    fn test_auth_with_default_user_nopass() {
        let server = ServerState::new(16);
        let mut client = ClientState::new(1);

        let password = Bytes::from_static(b"anypassword");
        let outcome = cmd_auth(&[password], &server, &mut client);

        // Default user has nopass enabled, so any password succeeds
        match &outcome.response {
            RespFrame::SimpleString(s) if s == "OK" => {
                // Expected: AUTH succeeds with default user
            }
            other => panic!("Expected OK response, got: {:?}", other),
        }

        // Verify client is authenticated
        assert!(client.authenticated);
        assert_eq!(client.acl_user, Bytes::from_static(b"default"));
    }

    #[test]
    fn test_auth_with_nonexistent_user() {
        let server = ServerState::new(16);
        let mut client = ClientState::new(2);

        let username = Bytes::from_static(b"nonexistent");
        let password = Bytes::from_static(b"anypass");
        let outcome = cmd_auth(&[username, password], &server, &mut client);

        // Nonexistent user should fail
        match &outcome.response {
            RespFrame::Error(_) => {
                // Expected: AUTH failure for nonexistent user
            }
            other => panic!("Expected error response, got: {:?}", other),
        }

        // Verify client is not authenticated
        assert!(!client.authenticated);
    }

    fn setuser(server: &mut ServerState, args: &[&str]) -> RespFrame {
        let client = ClientState::new(9);
        let mut argv = vec![Bytes::from_static(b"SETUSER")];
        argv.extend(
            args.iter()
                .map(|arg| Bytes::copy_from_slice(arg.as_bytes())),
        );
        cmd_acl(&argv, server, &client).response
    }

    fn auth(server: &ServerState, user: &str, password: &str) -> bool {
        let mut client = ClientState::new(10);
        let outcome = cmd_auth(
            &[
                Bytes::copy_from_slice(user.as_bytes()),
                Bytes::copy_from_slice(password.as_bytes()),
            ],
            server,
            &mut client,
        );
        matches!(outcome.response, RespFrame::SimpleString(_)) && client.authenticated
    }

    #[test]
    fn rejected_setuser_creates_and_changes_nothing() {
        let mut server = ServerState::new(16);

        // A bad rule after `on nopass` must not leave a usable user behind.
        assert!(matches!(
            setuser(&mut server, &["ghost", "on", "nopass", "~*", "+@all"]),
            RespFrame::Error(_)
        ));
        assert!(server.acl.get_user(&Bytes::from_static(b"ghost")).is_none());
        assert!(!auth(&server, "ghost", "anything"));

        assert!(matches!(
            setuser(&mut server, &["u3", "on", "nopass", "+@all", "-exec"]),
            RespFrame::Error(_)
        ));
        assert!(server.acl.get_user(&Bytes::from_static(b"u3")).is_none());

        // An existing user keeps every field when a later rule fails.
        assert_eq!(
            setuser(&mut server, &["alice", "on", ">secret", "+@read"]),
            RespFrame::ok()
        );
        assert!(matches!(
            setuser(&mut server, &["alice", "nopass", "+@all", "+@bogus"]),
            RespFrame::Error(_)
        ));
        let alice = server
            .acl
            .get_user(&Bytes::from_static(b"alice"))
            .expect("alice exists");
        assert!(alice.enabled && !alice.nopass && !alice.allow_all_commands);
        assert!(alice.category_allowed(b"read") && !alice.category_allowed(b"write"));
        assert!(auth(&server, "alice", "secret"));
        assert!(!auth(&server, "alice", "other"));
    }

    #[test]
    fn setuser_password_rules_apply_in_order() {
        let mut server = ServerState::new(16);
        assert_eq!(
            setuser(&mut server, &["bob", "on", ">one", "<one", ">two"]),
            RespFrame::ok()
        );
        assert!(!auth(&server, "bob", "one"));
        assert!(auth(&server, "bob", "two"));

        // Removing before adding keeps the added password.
        assert_eq!(
            setuser(&mut server, &["bob", "<three", ">three"]),
            RespFrame::ok()
        );
        assert!(auth(&server, "bob", "three"));

        // nopass forgets every password, as in Redis.
        assert_eq!(setuser(&mut server, &["bob", "nopass"]), RespFrame::ok());
        let bob = server
            .acl
            .get_user(&Bytes::from_static(b"bob"))
            .expect("bob exists");
        assert!(bob.nopass && bob.passwords.is_empty());
        assert_eq!(setuser(&mut server, &["bob", ">four"]), RespFrame::ok());
        assert!(!auth(&server, "bob", "two"));
        assert!(auth(&server, "bob", "four"));
    }

    #[test]
    fn acl_save_and_load_roundtrip_acl_state() {
        let dir = tempfile::tempdir().expect("tmpdir");
        let mut server = ServerState::new(16);
        server.config.set_dir(dir.path().to_path_buf());
        let client = ClientState::new(3);

        assert_eq!(
            cmd_acl(
                &[
                    Bytes::from_static(b"SETUSER"),
                    Bytes::from_static(b"alice"),
                    Bytes::from_static(b"on"),
                    Bytes::from_static(b">secret"),
                    Bytes::from_static(b"+@read"),
                ],
                &mut server,
                &client,
            )
            .response,
            RespFrame::ok()
        );

        assert_eq!(
            cmd_acl(&[Bytes::from_static(b"SAVE")], &mut server, &client).response,
            RespFrame::ok()
        );

        let mut loaded_server = ServerState::new(16);
        loaded_server.config.set_dir(dir.path().to_path_buf());

        assert_eq!(
            cmd_acl(&[Bytes::from_static(b"LOAD")], &mut loaded_server, &client,).response,
            RespFrame::ok()
        );

        let alice = loaded_server
            .acl
            .get_user(&Bytes::from_static(b"alice"))
            .expect("alice should be loaded");
        assert!(alice.enabled);
        assert!(!alice.nopass);
        assert!(alice.allowed_categories.contains(b"read" as &[u8]));
    }

    #[test]
    fn acl_setuser_rejects_rules_that_are_not_enforced() {
        let mut server = ServerState::new(16);
        let client = ClientState::new(4);

        assert_eq!(
            cmd_acl(
                &[
                    Bytes::from_static(b"SETUSER"),
                    Bytes::from_static(b"scoped"),
                    Bytes::from_static(b"~cache:*"),
                ],
                &mut server,
                &client,
            )
            .response,
            RespFrame::error_str("ERR ACL rule '~cache:*' is not supported in this build")
        );
        assert_eq!(
            cmd_acl(
                &[
                    Bytes::from_static(b"SETUSER"),
                    Bytes::from_static(b"scoped"),
                    Bytes::from_static(b"+get"),
                ],
                &mut server,
                &client,
            )
            .response,
            RespFrame::error_str("ERR ACL rule '+get' is not supported in this build")
        );
    }
}
