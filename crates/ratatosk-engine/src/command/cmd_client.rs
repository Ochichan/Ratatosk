use bytes::Bytes;

use ratatosk_resp::frame::RespFrame;

use crate::keyspace::ServerState;
use crate::security::should_reject_shell_metacharacters;

use super::{ClientState, CommandOutcome, err, now_ms, parse_i64, to_uppercase_bytes, wrong_arity};

pub(super) fn cmd_client(
    args: &[Bytes],
    _server: &mut ServerState,
    client: &mut ClientState,
) -> CommandOutcome {
    if args.is_empty() {
        return wrong_arity("client");
    }

    let subcommand = to_uppercase_bytes(&args[0]);
    match subcommand.as_slice() {
        b"ID" => {
            if args.len() != 1 {
                return wrong_arity("client");
            }
            CommandOutcome::reply(RespFrame::Integer(client.id))
        }
        b"GETNAME" => {
            if args.len() != 1 {
                return wrong_arity("client");
            }
            CommandOutcome::reply(RespFrame::BulkString(client.name.clone()))
        }
        b"SETNAME" => {
            let [_, name] = args else {
                return wrong_arity("client");
            };
            if name.is_empty() {
                client.name = None;
            } else if let Err(response) = validate_client_name(name) {
                return CommandOutcome::reply(response);
            } else {
                client.name = Some(name.clone());
            }
            CommandOutcome::reply(RespFrame::ok())
        }
        b"INFO" => {
            if args.len() != 1 {
                return wrong_arity("client");
            }
            let line = format_client_info_line(client);
            CommandOutcome::reply(RespFrame::bulk_str(&line))
        }
        b"LIST" => {
            let include_current = match parse_client_list_filter(&args[1..], client.id) {
                Ok(include_current) => include_current,
                Err(response) => return CommandOutcome::reply(response),
            };

            let mut payload = String::new();
            if include_current {
                payload.push_str(&format_client_info_line(client));
                payload.push('\n');
            }
            CommandOutcome::reply(RespFrame::bulk_str(&payload))
        }
        b"KILL" => cmd_client_kill(&args[1..], client),
        b"PAUSE" => cmd_client_pause(&args[1..]),
        b"UNPAUSE" => cmd_client_unpause(&args[1..]),
        b"UNBLOCK" => cmd_client_unblock(&args[1..], client),
        b"TRACKING" => cmd_client_tracking(&args[1..], client),
        b"TRACKINGINFO" => cmd_client_trackinginfo(&args[1..], client),
        b"CACHING" => cmd_client_caching(&args[1..], client),
        b"GETREDIR" => cmd_client_getredir(&args[1..], client),
        b"SETINFO" => cmd_client_setinfo(&args[1..]),
        b"NO-EVICT" => cmd_client_no_evict(&args[1..], client),
        b"NO-TOUCH" => cmd_client_no_touch(&args[1..], client),
        b"REPLY" => cmd_client_reply(&args[1..], client),
        b"HELP" => {
            if args.len() != 1 {
                return wrong_arity("client");
            }
            CommandOutcome::reply(RespFrame::Array(vec![
                RespFrame::bulk_str("ID -- Return the connection id."),
                RespFrame::bulk_str("GETNAME -- Return the connection name."),
                RespFrame::bulk_str("SETNAME <name> -- Set the connection name."),
                RespFrame::bulk_str(
                    "INFO -- Return information and statistics about the current client connection.",
                ),
                RespFrame::bulk_str(
                    "LIST [TYPE <normal|master|replica|pubsub>] [ID <id>] -- Return information about connected clients.",
                ),
                RespFrame::bulk_str(
                    "KILL <addr>|ID <id> [...] -- Kill client(s) (baseline supports ID filter).",
                ),
                RespFrame::bulk_str(
                    "PAUSE <timeout> [WRITE|ALL] -- Pause clients (baseline no-op).",
                ),
                RespFrame::bulk_str("UNPAUSE -- Resume clients (baseline no-op)."),
                RespFrame::bulk_str(
                    "UNBLOCK <id> [TIMEOUT|ERROR] -- Unblock client (baseline no-op).",
                ),
                RespFrame::bulk_str(
                    "TRACKING ON|OFF [REDIRECT <id> ...] -- Client-side caching tracking controls.",
                ),
                RespFrame::bulk_str(
                    "TRACKINGINFO -- Return tracking state for current connection.",
                ),
                RespFrame::bulk_str("CACHING YES|NO -- Toggle assisted client-side caching mode."),
                RespFrame::bulk_str("GETREDIR -- Return current tracking redirect client id."),
                RespFrame::bulk_str(
                    "SETINFO LIB-NAME|LIB-VER <value> -- Set client library metadata (baseline no-op).",
                ),
                RespFrame::bulk_str(
                    "NO-EVICT ON|OFF -- Toggle no-evict hint (baseline local state).",
                ),
                RespFrame::bulk_str(
                    "NO-TOUCH ON|OFF -- Toggle no-touch hint (baseline local state).",
                ),
                RespFrame::bulk_str(
                    "REPLY ON|OFF|SKIP -- Reply mode control (baseline local state).",
                ),
                RespFrame::bulk_str("HELP -- Show this help."),
            ]))
        }
        _ => CommandOutcome::reply(err("ERR syntax error")),
    }
}

pub(super) fn validate_client_name(name: &Bytes) -> Result<(), RespFrame> {
    if name.len() > 128 {
        return Err(err("ERR Client name is invalid"));
    }

    if name
        .iter()
        .any(|byte| byte.is_ascii_control() || byte.is_ascii_whitespace())
        || should_reject_shell_metacharacters(name)
    {
        return Err(err("ERR Client name is invalid"));
    }

    Ok(())
}

fn is_client_kill_option(raw: &Bytes) -> bool {
    matches!(
        to_uppercase_bytes(raw).as_slice(),
        b"ID" | b"TYPE" | b"USER" | b"ADDR" | b"LADDR" | b"SKIPME" | b"MAXAGE"
    )
}

fn cmd_client_kill(args: &[Bytes], client: &ClientState) -> CommandOutcome {
    if args.is_empty() {
        return wrong_arity("client");
    }

    // Legacy form: CLIENT KILL <ip:port>
    if args.len() == 1 && !is_client_kill_option(&args[0]) {
        return CommandOutcome::reply(RespFrame::ok());
    }

    let mut id_filter: Option<i64> = None;
    let mut idx = 0usize;
    while idx < args.len() {
        let option = to_uppercase_bytes(&args[idx]);
        match option.as_slice() {
            b"ID" => {
                if idx + 1 >= args.len() {
                    return CommandOutcome::reply(err("ERR syntax error"));
                }
                let Some(id) = parse_i64(&args[idx + 1]) else {
                    return CommandOutcome::reply(err(
                        "ERR value is not an integer or out of range",
                    ));
                };
                id_filter = Some(id);
                idx += 2;
            }
            b"TYPE" | b"USER" | b"ADDR" | b"LADDR" | b"SKIPME" => {
                if idx + 1 >= args.len() {
                    return CommandOutcome::reply(err("ERR syntax error"));
                }
                idx += 2;
            }
            b"MAXAGE" => {
                if idx + 1 >= args.len() {
                    return CommandOutcome::reply(err("ERR syntax error"));
                }
                let Some(maxage) = parse_i64(&args[idx + 1]) else {
                    return CommandOutcome::reply(err(
                        "ERR value is not an integer or out of range",
                    ));
                };
                if maxage < 0 {
                    return CommandOutcome::reply(err("ERR value is out of range"));
                }
                idx += 2;
            }
            _ => return CommandOutcome::reply(err("ERR syntax error")),
        }
    }

    let killed = i64::from(id_filter.is_some_and(|id| id == client.id));
    CommandOutcome::reply(RespFrame::Integer(killed))
}

fn cmd_client_pause(args: &[Bytes]) -> CommandOutcome {
    let [timeout_raw, rest @ ..] = args else {
        return wrong_arity("client");
    };

    let Some(timeout) = parse_i64(timeout_raw) else {
        return CommandOutcome::reply(err("ERR timeout is not an integer or out of range"));
    };
    if timeout < 0 {
        return CommandOutcome::reply(err("ERR timeout is negative"));
    }

    if !rest.is_empty() {
        if rest.len() != 1 {
            return CommandOutcome::reply(err("ERR syntax error"));
        }
        let mode = to_uppercase_bytes(&rest[0]);
        if !matches!(mode.as_slice(), b"WRITE" | b"ALL") {
            return CommandOutcome::reply(err("ERR syntax error"));
        }
    }

    CommandOutcome::reply(RespFrame::ok())
}

fn cmd_client_unpause(args: &[Bytes]) -> CommandOutcome {
    if !args.is_empty() {
        return wrong_arity("client");
    }

    CommandOutcome::reply(RespFrame::ok())
}

fn cmd_client_unblock(args: &[Bytes], _client: &ClientState) -> CommandOutcome {
    let [client_id_raw, rest @ ..] = args else {
        return wrong_arity("client");
    };

    let Some(_client_id) = parse_i64(client_id_raw) else {
        return CommandOutcome::reply(err("ERR value is not an integer or out of range"));
    };

    if !rest.is_empty() {
        if rest.len() != 1 {
            return CommandOutcome::reply(err("ERR syntax error"));
        }

        let option = to_uppercase_bytes(&rest[0]);
        if !matches!(option.as_slice(), b"TIMEOUT" | b"ERROR") {
            return CommandOutcome::reply(err("ERR syntax error"));
        }
    }

    CommandOutcome::reply(RespFrame::Integer(0))
}

fn parse_on_off(arg: &Bytes) -> Option<bool> {
    let upper = to_uppercase_bytes(arg);
    match upper.as_slice() {
        b"ON" | b"YES" => Some(true),
        b"OFF" | b"NO" => Some(false),
        _ => None,
    }
}

fn cmd_client_tracking(args: &[Bytes], client: &mut ClientState) -> CommandOutcome {
    if args.is_empty() {
        return wrong_arity("client");
    }

    let first = to_uppercase_bytes(&args[0]);
    match first.as_slice() {
        b"ON" => client.tracking_enabled = true,
        b"OFF" => {
            if args.len() != 1 {
                return CommandOutcome::reply(err("ERR syntax error"));
            }
            client.tracking_enabled = false;
            client.tracking_redirect = -1;
            return CommandOutcome::reply(RespFrame::ok());
        }
        _ => return CommandOutcome::reply(err("ERR syntax error")),
    }

    let mut idx = 1usize;
    while idx < args.len() {
        let option = to_uppercase_bytes(&args[idx]);
        match option.as_slice() {
            b"REDIRECT" => {
                if idx + 1 >= args.len() {
                    return CommandOutcome::reply(err("ERR syntax error"));
                }
                let Some(id) = parse_i64(&args[idx + 1]) else {
                    return CommandOutcome::reply(err(
                        "ERR value is not an integer or out of range",
                    ));
                };
                client.tracking_redirect = id;
                idx += 2;
            }
            b"BCAST" | b"OPTIN" | b"OPTOUT" | b"NOLOOP" => idx += 1,
            b"PREFIX" => {
                if idx + 1 >= args.len() {
                    return CommandOutcome::reply(err("ERR syntax error"));
                }
                idx += 2;
            }
            _ => return CommandOutcome::reply(err("ERR syntax error")),
        }
    }

    CommandOutcome::reply(RespFrame::ok())
}

fn cmd_client_trackinginfo(args: &[Bytes], client: &ClientState) -> CommandOutcome {
    if !args.is_empty() {
        return wrong_arity("client");
    }

    let flags = if client.tracking_enabled {
        vec![RespFrame::bulk_str("on")]
    } else {
        vec![RespFrame::bulk_str("off")]
    };

    CommandOutcome::reply(RespFrame::Map(vec![
        (RespFrame::bulk_str("flags"), RespFrame::Array(flags)),
        (
            RespFrame::bulk_str("redirect"),
            RespFrame::Integer(client.tracking_redirect),
        ),
        (RespFrame::bulk_str("prefixes"), RespFrame::Array(vec![])),
    ]))
}

fn cmd_client_caching(args: &[Bytes], client: &mut ClientState) -> CommandOutcome {
    let [mode] = args else {
        return wrong_arity("client");
    };

    let Some(enabled) = parse_on_off(mode) else {
        return CommandOutcome::reply(err("ERR syntax error"));
    };
    client.caching_enabled = enabled;
    CommandOutcome::reply(RespFrame::ok())
}

fn cmd_client_getredir(args: &[Bytes], client: &ClientState) -> CommandOutcome {
    if !args.is_empty() {
        return wrong_arity("client");
    }

    CommandOutcome::reply(RespFrame::Integer(client.tracking_redirect))
}

fn cmd_client_setinfo(args: &[Bytes]) -> CommandOutcome {
    let [field, _value] = args else {
        return wrong_arity("client");
    };

    let upper = to_uppercase_bytes(field);
    if !matches!(upper.as_slice(), b"LIB-NAME" | b"LIB-VER") {
        return CommandOutcome::reply(err("ERR syntax error"));
    }

    CommandOutcome::reply(RespFrame::ok())
}

fn cmd_client_no_evict(args: &[Bytes], client: &mut ClientState) -> CommandOutcome {
    let [mode] = args else {
        return wrong_arity("client");
    };
    let Some(enabled) = parse_on_off(mode) else {
        return CommandOutcome::reply(err("ERR syntax error"));
    };
    client.no_evict = enabled;
    CommandOutcome::reply(RespFrame::ok())
}

fn cmd_client_no_touch(args: &[Bytes], client: &mut ClientState) -> CommandOutcome {
    let [mode] = args else {
        return wrong_arity("client");
    };
    let Some(enabled) = parse_on_off(mode) else {
        return CommandOutcome::reply(err("ERR syntax error"));
    };
    client.no_touch = enabled;
    CommandOutcome::reply(RespFrame::ok())
}

fn cmd_client_reply(args: &[Bytes], client: &mut ClientState) -> CommandOutcome {
    let [mode] = args else {
        return wrong_arity("client");
    };

    let upper = to_uppercase_bytes(mode);
    if !matches!(upper.as_slice(), b"ON" | b"OFF" | b"SKIP") {
        return CommandOutcome::reply(err("ERR syntax error"));
    }

    client.reply_mode = Bytes::from(upper.to_ascii_lowercase());
    CommandOutcome::reply(RespFrame::ok())
}

fn parse_client_list_filter(args: &[Bytes], client_id: i64) -> Result<bool, RespFrame> {
    let mut include_current = true;
    let mut idx = 0usize;

    while idx < args.len() {
        let option = to_uppercase_bytes(&args[idx]);
        match option.as_slice() {
            b"TYPE" => {
                if idx + 1 >= args.len() {
                    return Err(err("ERR syntax error"));
                }

                let type_filter = to_uppercase_bytes(&args[idx + 1]);
                include_current &= matches!(type_filter.as_slice(), b"NORMAL");
                idx += 2;
            }
            b"ID" => {
                if idx + 1 >= args.len() {
                    return Err(err("ERR syntax error"));
                }

                let Some(id_filter) = parse_i64(&args[idx + 1]) else {
                    return Err(err("ERR value is not an integer or out of range"));
                };

                include_current &= id_filter == client_id;
                idx += 2;
            }
            _ => return Err(err("ERR syntax error")),
        }
    }

    Ok(include_current)
}

pub(super) fn format_client_info_line(client: &ClientState) -> String {
    let now = now_ms();
    let age = (now.saturating_sub(client.created_at_ms) / 1000).max(0);
    let idle = (now.saturating_sub(client.last_interaction_ms) / 1000).max(0);
    let name = client
        .name
        .as_ref()
        .map(|value| String::from_utf8_lossy(value).into_owned())
        .unwrap_or_default();
    let cmd = if client.last_command.is_empty() {
        "NULL".to_string()
    } else {
        String::from_utf8_lossy(&client.last_command).to_ascii_lowercase()
    };
    let user = String::from_utf8_lossy(&client.acl_user).into_owned();
    let multi = if client.in_multi {
        client.tx_queue.len() as i64
    } else {
        -1
    };

    format!(
        "id={} addr=127.0.0.1:0 laddr=127.0.0.1:0 fd=-1 name={} age={} idle={} flags=N db={} sub=0 psub=0 ssub=0 multi={} qbuf=0 qbuf-free=0 argv-mem=0 multi-mem=0 rbs=0 rbp=0 obl=0 oll=0 omem=0 tot-mem=0 events=r cmd={} user={user} redir=-1 resp=2",
        client.id, name, age, idle, client.selected_db, multi, cmd
    )
}
