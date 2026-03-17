use std::fmt::Write;

use bytes::Bytes;

use ratatosk_resp::frame::RespFrame;

use crate::keyspace::{ClientSnapshot, ServerState};
use crate::security::should_reject_shell_metacharacters;

use super::{ClientState, CommandOutcome, err, parse_i64, to_uppercase_stack, wrong_arity};

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
            let line = server
                .client_snapshot(client.id())
                .map(|snapshot| {
                    format_client_snapshot_line(snapshot, server.client_is_blocked(snapshot.id))
                })
                .unwrap_or_else(|| format_client_info_line(client));
            CommandOutcome::reply(RespFrame::bulk_str(&line))
        }
        b"LIST" => {
            let filter = match parse_client_list_filter(&args[1..]) {
                Ok(filter) => filter,
                Err(response) => return CommandOutcome::reply(response),
            };

            let mut payload = String::new();
            let snapshots = server.client_snapshots();
            if snapshots.is_empty() {
                if client_snapshot_matches_filter(
                    &filter,
                    &client.snapshot(
                        Bytes::from_static(b"127.0.0.1:0"),
                        Bytes::from_static(b"127.0.0.1:0"),
                    ),
                    false,
                ) {
                    payload.push_str(&format_client_info_line(client));
                    payload.push('\n');
                }
            } else {
                for snapshot in snapshots {
                    let blocked = server.client_is_blocked(snapshot.id);
                    if client_snapshot_matches_filter(&filter, &snapshot, blocked) {
                        payload.push_str(&format_client_snapshot_line(&snapshot, blocked));
                        payload.push('\n');
                    }
                }
            }
            CommandOutcome::reply(RespFrame::bulk_str(&payload))
        }
        b"KILL" => cmd_client_kill(&args[1..], client),
        b"PAUSE" => cmd_client_pause(&args[1..]),
        b"UNPAUSE" => cmd_client_unpause(&args[1..]),
        b"UNBLOCK" => cmd_client_unblock(&args[1..], client),
        b"TRACKING" => cmd_client_tracking(&args[1..], server, client),
        b"TRACKINGINFO" => cmd_client_trackinginfo(&args[1..], server, client),
        b"CACHING" => cmd_client_caching(&args[1..], client),
        b"GETREDIR" => cmd_client_getredir(&args[1..], server, client),
        b"SETINFO" => cmd_client_setinfo(&args[1..], client),
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
                    "TRACKING ON|OFF [REDIRECT <id>] [BCAST [PREFIX <prefix> ...]] [NOLOOP|OPTIN|OPTOUT] -- Client-side caching tracking controls.",
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

fn parse_on_off(arg: &Bytes) -> Option<bool> {
    let upper = to_uppercase_stack(arg);
    match upper.as_slice() {
        b"ON" | b"YES" => Some(true),
        b"OFF" | b"NO" => Some(false),
        _ => None,
    }
}

fn cmd_client_tracking(
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

fn cmd_client_trackinginfo(
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

fn cmd_client_caching(args: &[Bytes], client: &mut ClientState) -> CommandOutcome {
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

fn cmd_client_getredir(
    args: &[Bytes],
    server: &ServerState,
    client: &ClientState,
) -> CommandOutcome {
    if !args.is_empty() {
        return wrong_arity("client");
    }

    let _ = server;
    let redirect = if !client.tracking_enabled {
        -1
    } else if client.tracking_redirect < 0 {
        0
    } else {
        client.tracking_redirect
    };

    CommandOutcome::reply(RespFrame::Integer(redirect))
}

fn cmd_client_setinfo(args: &[Bytes], client: &mut ClientState) -> CommandOutcome {
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

    // Use case-insensitive comparison for validation
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

#[derive(Default)]
struct ClientListFilter {
    type_filter: Option<Bytes>,
    id_filter: Option<i64>,
}

fn parse_client_list_filter(args: &[Bytes]) -> Result<ClientListFilter, RespFrame> {
    let mut filter = ClientListFilter::default();
    let mut idx = 0usize;

    while idx < args.len() {
        let option = to_uppercase_stack(&args[idx]);
        match option.as_slice() {
            b"TYPE" => {
                if idx + 1 >= args.len() {
                    return Err(err("ERR syntax error"));
                }

                let type_filter = to_uppercase_stack(&args[idx + 1]);
                filter.type_filter = Some(Bytes::copy_from_slice(type_filter.as_slice()));
                idx += 2;
            }
            b"ID" => {
                if idx + 1 >= args.len() {
                    return Err(err("ERR syntax error"));
                }

                let Some(id_filter) = parse_i64(&args[idx + 1]) else {
                    return Err(err("ERR value is not an integer or out of range"));
                };

                filter.id_filter = Some(id_filter);
                idx += 2;
            }
            _ => return Err(err("ERR syntax error")),
        }
    }

    Ok(filter)
}

fn client_snapshot_matches_filter(
    filter: &ClientListFilter,
    snapshot: &ClientSnapshot,
    _blocked: bool,
) -> bool {
    if let Some(id_filter) = filter.id_filter {
        if snapshot.id != id_filter {
            return false;
        }
    }

    if let Some(type_filter) = &filter.type_filter {
        match type_filter.as_ref() {
            b"NORMAL" => {
                if snapshot.sub > 0 {
                    return false;
                }
            }
            b"PUBSUB" => {
                if snapshot.sub == 0 {
                    return false;
                }
            }
            b"MASTER" | b"REPLICA" => return false,
            _ => return false,
        }
    }

    true
}

pub(super) fn format_client_info_line(client: &ClientState) -> String {
    let snapshot = client.snapshot(
        Bytes::from_static(b"127.0.0.1:0"),
        Bytes::from_static(b"127.0.0.1:0"),
    );
    format_client_snapshot_line(&snapshot, false)
}

pub(super) fn format_client_snapshot_line(snapshot: &ClientSnapshot, blocked: bool) -> String {
    const ESTIMATED_CAPACITY: usize = 256;
    let mut out = String::with_capacity(ESTIMATED_CAPACITY);

    let name = snapshot
        .name
        .as_ref()
        .map(|v| String::from_utf8_lossy(v).into_owned())
        .unwrap_or_default();
    let mut flags = snapshot.flags.to_vec();
    if blocked && !flags.contains(&b'b') {
        flags.push(b'b');
    }
    let flags = String::from_utf8_lossy(&flags).into_owned();
    let cmd = String::from_utf8_lossy(&snapshot.cmd).to_ascii_lowercase();
    let user = String::from_utf8_lossy(&snapshot.user).into_owned();

    let _ = write!(
        out,
        "id={} addr={} laddr={} fd=-1 name={} age={} idle={} flags={} db={} sub={} psub={} ssub={} multi={} qbuf=0 qbuf-free=0 argv-mem=0 multi-mem=0 rbs=0 rbp=0 obl=0 oll=0 omem=0 tot-mem=0 events=r cmd={} user={} lib-name={} lib-ver={} redir={} resp={}",
        snapshot.id,
        String::from_utf8_lossy(&snapshot.addr),
        String::from_utf8_lossy(&snapshot.laddr),
        name,
        snapshot.age_seconds,
        snapshot.idle_seconds,
        flags,
        snapshot.db,
        snapshot.sub,
        snapshot.psub,
        snapshot.ssub,
        snapshot.multi,
        cmd,
        user,
        snapshot
            .lib_name
            .as_ref()
            .map(|v| String::from_utf8_lossy(v).into_owned())
            .unwrap_or_default(),
        snapshot
            .lib_ver
            .as_ref()
            .map(|v| String::from_utf8_lossy(v).into_owned())
            .unwrap_or_default(),
        snapshot.redir,
        snapshot.resp
    );
    out
}
