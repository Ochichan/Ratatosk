use bytes::Bytes;

use ratatosk_resp::frame::RespFrame;

use crate::keyspace::ServerState;

use super::{ClientState, CommandOutcome, err, parse_i64, to_uppercase_stack, wrong_arity};

fn parse_on_off(arg: &Bytes) -> Option<bool> {
    let upper = to_uppercase_stack(arg);
    match upper.as_slice() {
        b"ON" | b"YES" => Some(true),
        b"OFF" | b"NO" => Some(false),
        _ => None,
    }
}

pub(super) fn cmd_client_tracking(
    args: &[Bytes],
    server: &mut ServerState,
    client: &mut ClientState,
) -> CommandOutcome {
    if args.is_empty() {
        return wrong_arity("client");
    }

    let first = to_uppercase_stack(&args[0]);
    match first.as_slice() {
        b"ON" => {}
        b"OFF" => {
            if args.len() != 1 {
                return CommandOutcome::reply(err("ERR syntax error"));
            }
            server.tracking_clear_tracker(client.id());
            client.tracking_enabled = false;
            client.tracking_redirect = -1;
            client.tracking_broadcast = false;
            client.tracking_no_loop = false;
            client.tracking_optin = false;
            client.tracking_optout = false;
            client.tracking_prefixes.clear();
            client.set_tracking_caching(true);
            return CommandOutcome::reply(RespFrame::ok());
        }
        _ => return CommandOutcome::reply(err("ERR syntax error")),
    }

    let mut tracking_redirect = -1;
    let mut tracking_broadcast = false;
    let mut tracking_no_loop = false;
    let mut tracking_optin = false;
    let mut tracking_optout = false;
    let mut tracking_prefixes = Vec::new();
    let mut idx = 1usize;
    while idx < args.len() {
        let option = to_uppercase_stack(&args[idx]);
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
                if id < 0 {
                    return CommandOutcome::reply(err("ERR redirect id is out of range"));
                }
                tracking_redirect = id;
                idx += 2;
            }
            b"BCAST" => {
                tracking_broadcast = true;
                idx += 1;
            }
            b"NOLOOP" => {
                tracking_no_loop = true;
                idx += 1;
            }
            b"OPTIN" => {
                tracking_optin = true;
                idx += 1;
            }
            b"OPTOUT" => {
                tracking_optout = true;
                idx += 1;
            }
            b"PREFIX" => {
                if idx + 1 >= args.len() {
                    return CommandOutcome::reply(err("ERR syntax error"));
                }
                tracking_prefixes.push(args[idx + 1].clone());
                idx += 2;
            }
            _ => return CommandOutcome::reply(err("ERR syntax error")),
        }
    }

    if !tracking_broadcast && !tracking_prefixes.is_empty() {
        return CommandOutcome::reply(err(
            "ERR PREFIX option requires BCAST in this Ratatosk baseline",
        ));
    }
    if tracking_optin && tracking_optout {
        return CommandOutcome::reply(err("ERR OPTIN and OPTOUT are mutually exclusive"));
    }
    if tracking_broadcast && (tracking_optin || tracking_optout) {
        return CommandOutcome::reply(err("ERR OPTIN and OPTOUT are not compatible with BCAST"));
    }
    if tracking_redirect >= 0
        && tracking_redirect != client.id()
        && server.client_snapshot(tracking_redirect).is_none()
    {
        return CommandOutcome::reply(err(
            "ERR CLIENT TRACKING REDIRECT target client is not connected",
        ));
    }

    server.tracking_clear_tracker(client.id());
    client.tracking_enabled = true;
    client.tracking_redirect = tracking_redirect;
    client.tracking_broadcast = tracking_broadcast;
    client.tracking_no_loop = tracking_no_loop;
    client.tracking_optin = tracking_optin;
    client.tracking_optout = tracking_optout;
    client.tracking_prefixes = tracking_prefixes;
    client.reset_tracking_caching_mode();

    if client.tracking_broadcast {
        server.tracking_configure_broadcast(
            client.id(),
            server.tracking_target_client_id(client.id(), client.tracking_redirect),
            client.tracking_no_loop,
            client.tracking_prefixes.clone(),
        );
    }

    CommandOutcome::reply(RespFrame::ok())
}

pub(super) fn cmd_client_trackinginfo(
    args: &[Bytes],
    server: &ServerState,
    client: &ClientState,
) -> CommandOutcome {
    if !args.is_empty() {
        return wrong_arity("client");
    }

    let mut flags = if client.tracking_enabled {
        vec![RespFrame::bulk_str("on")]
    } else {
        vec![RespFrame::bulk_str("off")]
    };
    if client.tracking_broadcast {
        flags.push(RespFrame::bulk_str("bcast"));
    }
    if client.tracking_no_loop {
        flags.push(RespFrame::bulk_str("noloop"));
    }
    if client.tracking_optin {
        flags.push(RespFrame::bulk_str("optin"));
    }
    if client.tracking_optout {
        flags.push(RespFrame::bulk_str("optout"));
    }
    if server.tracking_redirect_broken(client.id()) {
        flags.push(RespFrame::bulk_str("broken_redirect"));
    }

    let redirect = if !client.tracking_enabled {
        -1
    } else if client.tracking_redirect < 0 {
        0
    } else {
        client.tracking_redirect
    };

    CommandOutcome::reply(RespFrame::Map(vec![
        (RespFrame::bulk_str("flags"), RespFrame::Array(flags)),
        (
            RespFrame::bulk_str("redirect"),
            RespFrame::Integer(redirect),
        ),
        (
            RespFrame::bulk_str("prefixes"),
            RespFrame::Array(
                client
                    .tracking_prefixes
                    .iter()
                    .cloned()
                    .map(|prefix| RespFrame::BulkString(Some(prefix)))
                    .collect(),
            ),
        ),
    ]))
}

pub(super) fn cmd_client_caching(args: &[Bytes], client: &mut ClientState) -> CommandOutcome {
    let [mode] = args else {
        return wrong_arity("client");
    };

    if !client.tracking_accepts_caching_toggle() {
        return CommandOutcome::reply(err(
            "ERR CLIENT CACHING is only valid with CLIENT TRACKING in OPTIN or OPTOUT mode",
        ));
    }

    let Some(enabled) = parse_on_off(mode) else {
        return CommandOutcome::reply(err("ERR syntax error"));
    };
    client.set_tracking_caching(enabled);
    CommandOutcome::reply(RespFrame::ok())
}

pub(super) fn cmd_client_getredir(args: &[Bytes], client: &ClientState) -> CommandOutcome {
    if !args.is_empty() {
        return wrong_arity("client");
    }

    let redirect = if !client.tracking_enabled {
        -1
    } else if client.tracking_redirect < 0 {
        0
    } else {
        client.tracking_redirect
    };

    CommandOutcome::reply(RespFrame::Integer(redirect))
}

pub(super) fn cmd_client_setinfo(args: &[Bytes], client: &mut ClientState) -> CommandOutcome {
    let [field, value] = args else {
        return wrong_arity("client");
    };

    let upper = to_uppercase_stack(field);
    match upper.as_slice() {
        b"LIB-NAME" => client.set_lib_name(value.clone()),
        b"LIB-VER" => client.set_lib_ver(value.clone()),
        _ => return CommandOutcome::reply(err("ERR syntax error")),
    }

    CommandOutcome::reply(RespFrame::ok())
}

pub(super) fn cmd_client_no_evict(args: &[Bytes], client: &mut ClientState) -> CommandOutcome {
    let [mode] = args else {
        return wrong_arity("client");
    };
    let Some(enabled) = parse_on_off(mode) else {
        return CommandOutcome::reply(err("ERR syntax error"));
    };
    client.no_evict = enabled;
    CommandOutcome::reply(RespFrame::ok())
}

pub(super) fn cmd_client_no_touch(args: &[Bytes], client: &mut ClientState) -> CommandOutcome {
    let [mode] = args else {
        return wrong_arity("client");
    };
    let Some(enabled) = parse_on_off(mode) else {
        return CommandOutcome::reply(err("ERR syntax error"));
    };
    client.no_touch = enabled;
    CommandOutcome::reply(RespFrame::ok())
}

pub(super) fn cmd_client_reply(args: &[Bytes], client: &mut ClientState) -> CommandOutcome {
    let [mode] = args else {
        return wrong_arity("client");
    };

    let upper = to_uppercase_stack(mode);
    let mode_str = match upper.as_slice() {
        b"ON" => b"on" as &[u8],
        b"OFF" => b"off",
        b"SKIP" => b"skip",
        _ => return CommandOutcome::reply(err("ERR syntax error")),
    };

    client.reply_mode = Bytes::from_static(mode_str);
    CommandOutcome::reply(RespFrame::ok())
}
