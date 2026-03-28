use bytes::Bytes;

use ratatosk_resp::frame::RespFrame;

use crate::keyspace::ServerState;
use crate::security::should_reject_shell_metacharacters;

use super::{
    ClientState, CommandOutcome, cmd_client_introspection, cmd_client_tracking, err, parse_i64,
    to_uppercase_stack, wrong_arity,
};

pub(super) fn cmd_client(
    args: &[Bytes],
    server: &mut ServerState,
    client: &mut ClientState,
) -> CommandOutcome {
    if args.is_empty() {
        return wrong_arity("client");
    }

    let subcommand = to_uppercase_stack(&args[0]);
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
            cmd_client_introspection::cmd_client_info(server, client)
        }
        b"LIST" => cmd_client_introspection::cmd_client_list(&args[1..], server, client),
        b"KILL" => cmd_client_kill(&args[1..], client),
        b"PAUSE" => cmd_client_pause(&args[1..]),
        b"UNPAUSE" => cmd_client_unpause(&args[1..]),
        b"UNBLOCK" => cmd_client_unblock(&args[1..], client),
        b"TRACKING" => cmd_client_tracking::cmd_client_tracking(&args[1..], server, client),
        b"TRACKINGINFO" => cmd_client_tracking::cmd_client_trackinginfo(&args[1..], server, client),
        b"CACHING" => cmd_client_tracking::cmd_client_caching(&args[1..], client),
        b"GETREDIR" => cmd_client_tracking::cmd_client_getredir(&args[1..], client),
        b"SETINFO" => cmd_client_tracking::cmd_client_setinfo(&args[1..], client),
        b"NO-EVICT" => cmd_client_tracking::cmd_client_no_evict(&args[1..], client),
        b"NO-TOUCH" => cmd_client_tracking::cmd_client_no_touch(&args[1..], client),
        b"REPLY" => cmd_client_tracking::cmd_client_reply(&args[1..], client),
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
                    "PAUSE <timeout> [WRITE|ALL] -- Not supported in this Ratatosk build.",
                ),
                RespFrame::bulk_str("UNPAUSE -- Not supported in this Ratatosk build."),
                RespFrame::bulk_str(
                    "UNBLOCK <id> [TIMEOUT|ERROR] -- Validate an unblock request and report how many clients were released.",
                ),
                RespFrame::bulk_str(
                    "TRACKING ON|OFF [REDIRECT <id>] [BCAST [PREFIX <prefix> ...]] [NOLOOP|OPTIN|OPTOUT] -- Client-side caching tracking controls.",
                ),
                RespFrame::bulk_str(
                    "TRACKINGINFO -- Return tracking state for current connection.",
                ),
                RespFrame::bulk_str("CACHING YES|NO -- Toggle assisted client-side caching mode."),
                RespFrame::bulk_str("GETREDIR -- Return current tracking redirect client id."),
                RespFrame::bulk_str(
                    "SETINFO LIB-NAME|LIB-VER <value> -- Set local client library metadata for this connection.",
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
        to_uppercase_stack(raw).as_slice(),
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
        let option = to_uppercase_stack(&args[idx]);
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
    if args.is_empty() {
        return wrong_arity("client");
    }
    CommandOutcome::reply(err(
        "ERR CLIENT PAUSE is not supported in this Ratatosk build",
    ))
}

fn cmd_client_unpause(args: &[Bytes]) -> CommandOutcome {
    if !args.is_empty() {
        return wrong_arity("client");
    }
    CommandOutcome::reply(err(
        "ERR CLIENT UNPAUSE is not supported in this Ratatosk build",
    ))
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

        let option = to_uppercase_stack(&rest[0]);
        if !matches!(option.as_slice(), b"TIMEOUT" | b"ERROR") {
            return CommandOutcome::reply(err("ERR syntax error"));
        }
    }

    CommandOutcome::reply(RespFrame::Integer(0))
}
