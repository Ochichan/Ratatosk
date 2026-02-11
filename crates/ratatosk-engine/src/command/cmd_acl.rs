use bytes::Bytes;
use rand::RngCore;
use rand::rngs::OsRng;

use ratatosk_resp::frame::RespFrame;

use crate::keyspace::{AclState, AclUser, ServerState};

use super::{
    ClientState, CommandOutcome, acl_required_categories, err, parse_i64, to_uppercase_bytes,
    wrong_arity,
};

const AUTH_FAILURE: &str = "ERR invalid username-password pair or user is disabled.";

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
                RespFrame::bulk_str("LOAD -- Reload ACL rules (baseline no-op)."),
                RespFrame::bulk_str("LOG [count|RESET] -- Show or reset ACL log."),
                RespFrame::bulk_str("SAVE -- Persist ACL rules (baseline no-op)."),
                RespFrame::bulk_str("SETUSER <username> [rule ...] -- Create/modify ACL user."),
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
            let mut remove_passwords = Vec::new();
            {
                let user: &mut AclUser = server.acl.get_or_create_user_mut(&username);
                for rule in &args[2..] {
                    if rule.eq_ignore_ascii_case(b"on") {
                        user.enabled = true;
                    } else if rule.eq_ignore_ascii_case(b"off") {
                        user.enabled = false;
                    } else if rule.eq_ignore_ascii_case(b"nopass") {
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
                        remove_passwords.push(Bytes::copy_from_slice(&rule[1..]));
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
                        // Accepted baseline tokens not enforced yet: key/channel scopes and command-level grants.
                    } else {
                        return CommandOutcome::reply(err("ERR syntax error"));
                    }
                }
            }
            for password in remove_passwords {
                let _ = server.acl.remove_password(&username, &password);
            }
            server.acl.push_log(Bytes::from(format!(
                "SETUSER {} rules={} OK",
                String::from_utf8_lossy(&username),
                args.len().saturating_sub(2)
            )));
            CommandOutcome::reply(RespFrame::ok())
        }
        b"DELUSER" => {
            if args.len() < 2 {
                return wrong_arity("acl");
            }
            let removed = server.acl.del_users(&args[1..]);
            server
                .acl
                .push_log(Bytes::from(format!("DELUSER count={removed}")));
            CommandOutcome::reply(RespFrame::Integer(removed))
        }
        b"GENPASS" => {
            if args.len() > 2 {
                return wrong_arity("acl");
            }
            let bits = if let Some(raw) = args.get(1) {
                let Some(v) = parse_i64(raw) else {
                    return CommandOutcome::reply(err(
                        "ERR value is not an integer or out of range",
                    ));
                };
                if v <= 0 {
                    return CommandOutcome::reply(err("ERR value is out of range"));
                }
                usize::try_from(v).unwrap_or(256)
            } else {
                256usize
            };
            let chars = (bits / 4).clamp(8, 256);
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
            CommandOutcome::reply(RespFrame::ok())
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

pub(super) fn cmd_auth(
    args: &[Bytes],
    server: &ServerState,
    client: &mut ClientState,
) -> CommandOutcome {
    match args {
        [password] => {
            if authenticate_client(server, &Bytes::from_static(b"default"), password, client) {
                CommandOutcome::reply(RespFrame::ok())
            } else {
                CommandOutcome::reply(err(AUTH_FAILURE))
            }
        }
        [username, password] => {
            if authenticate_client(server, username, password, client) {
                CommandOutcome::reply(RespFrame::ok())
            } else {
                CommandOutcome::reply(err(AUTH_FAILURE))
            }
        }
        _ => wrong_arity("auth"),
    }
}

pub(super) fn cmd_reset(args: &[Bytes], client: &mut ClientState) -> CommandOutcome {
    if !args.is_empty() {
        return wrong_arity("reset");
    }

    client.reset_for_connection();
    CommandOutcome::reply(RespFrame::simple_str("RESET"))
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

fn to_hex(raw: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(raw.len().saturating_mul(2));
    for byte in raw {
        out.push(HEX[(byte >> 4) as usize] as char);
        out.push(HEX[(byte & 0x0F) as usize] as char);
    }
    out
}
