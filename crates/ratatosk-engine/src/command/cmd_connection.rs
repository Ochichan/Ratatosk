use bytes::Bytes;

use ratatosk_resp::frame::RespFrame;

use crate::keyspace::ServerState;

use super::{
    ClientState, CommandOutcome, CommandSpec, all_command_specs, cmd_client, command_spec_count,
    err, find_command_spec, parse_i64, parse_usize, to_uppercase_bytes, wrong_arity,
};

pub(super) fn cmd_ping(args: &[Bytes], server: &ServerState) -> CommandOutcome {
    match args {
        [] => CommandOutcome::reply(RespFrame::pong()),
        [message] => {
            // Deep health check: "PING HEALTH" returns detailed server status
            if message.eq_ignore_ascii_case(b"HEALTH") {
                return CommandOutcome::reply(RespFrame::bulk_str(&generate_health_report(server)));
            }
            CommandOutcome::reply(RespFrame::BulkString(Some(message.clone())))
        }
        _ => wrong_arity("ping"),
    }
}

fn generate_health_report(server: &ServerState) -> String {
    let connected_clients = server.stats.connected_clients();

    let rdb_status = match server.last_rdb_save_status() {
        Some(Ok(())) => "ok",
        Some(Err(_)) => "error",
        None => "none",
    };

    let aof_enabled = server.aof_enabled();
    let memory_used = server.stats.cached_memory_estimate();
    let maxmemory = server.config.maxmemory() as u64;

    let memory_status = if maxmemory == 0 {
        "unbounded"
    } else if memory_used > maxmemory {
        "critical"
    } else if memory_used >= (maxmemory * 9 / 10) {
        "warning"
    } else {
        "ok"
    };

    let status = if memory_status == "critical" || rdb_status == "error" {
        "degraded"
    } else {
        "ok"
    };

    let total_keys: usize = (0..server.db_count()).map(|idx| server.db(idx).len()).sum();

    format!(
        "status:{status}|connected_clients:{connected_clients}|db_count:{}|keys:{}|rdb_save_in_progress:{}|rdb_last_bgsave_status:{rdb_status}|aof_enabled:{}|memory_status:{memory_status}|memory_used_bytes:{}|maxmemory_bytes:{}|uptime_seconds:{}",
        server.db_count(),
        total_keys,
        server.rdb_save_in_progress(),
        aof_enabled,
        memory_used,
        maxmemory,
        server.uptime_seconds(),
    )
}

pub(super) fn cmd_echo(args: &[Bytes]) -> CommandOutcome {
    match args {
        [message] => CommandOutcome::reply(RespFrame::BulkString(Some(message.clone()))),
        _ => wrong_arity("echo"),
    }
}

pub(super) fn cmd_quit(args: &[Bytes]) -> CommandOutcome {
    if !args.is_empty() {
        return wrong_arity("quit");
    }

    CommandOutcome::close(RespFrame::ok())
}

pub(super) fn cmd_hello(
    args: &[Bytes],
    server: &ServerState,
    client: &mut ClientState,
) -> CommandOutcome {
    let mut idx = 0usize;
    let mut proto = 3i64;

    if let Some(first) = args.first() {
        if let Some(version) = parse_i64(first) {
            if version != 2 && version != 3 {
                return CommandOutcome::reply(err("NOPROTO unsupported protocol version"));
            }
            proto = version;
            idx = 1;
        } else {
            let token = to_uppercase_bytes(first);
            if !matches!(token.as_slice(), b"AUTH" | b"SETNAME") {
                return CommandOutcome::reply(err("NOPROTO unsupported protocol version"));
            }
        }
    }

    while idx < args.len() {
        let option = to_uppercase_bytes(&args[idx]);
        match option.as_slice() {
            b"AUTH" => {
                if idx + 2 >= args.len() {
                    return CommandOutcome::reply(err("ERR syntax error"));
                }

                if !super::cmd_acl::authenticate_client(
                    server,
                    &args[idx + 1],
                    &args[idx + 2],
                    client,
                ) {
                    return CommandOutcome::reply(err(
                        "ERR invalid username-password pair or user is disabled.",
                    ));
                }
                idx += 3;
            }
            b"SETNAME" => {
                if idx + 1 >= args.len() {
                    return CommandOutcome::reply(err("ERR syntax error"));
                }
                if let Err(response) = cmd_client::validate_client_name(&args[idx + 1]) {
                    return CommandOutcome::reply(response);
                }
                client.name = Some(args[idx + 1].clone());
                idx += 2;
            }
            _ => return CommandOutcome::reply(err("ERR syntax error")),
        }
    }

    if !client.authenticated {
        return CommandOutcome::reply(err(
            "NOAUTH HELLO must be called with the client already authenticated",
        ));
    }

    let response = RespFrame::Map(vec![
        (
            RespFrame::bulk_str("server"),
            RespFrame::bulk_str("ratatosk"),
        ),
        (
            RespFrame::bulk_str("version"),
            RespFrame::bulk_str(env!("CARGO_PKG_VERSION")),
        ),
        (RespFrame::bulk_str("proto"), RespFrame::Integer(proto)),
        (RespFrame::bulk_str("id"), RespFrame::Integer(client.id)),
        (
            RespFrame::bulk_str("mode"),
            RespFrame::bulk_str("standalone"),
        ),
        (RespFrame::bulk_str("role"), RespFrame::bulk_str("master")),
        (RespFrame::bulk_str("modules"), RespFrame::Array(vec![])),
    ]);

    CommandOutcome::reply(response)
}

pub(super) fn cmd_command(args: &[Bytes]) -> CommandOutcome {
    if args.is_empty() {
        return CommandOutcome::reply(command_full_reply());
    }

    let subcommand = to_uppercase_bytes(&args[0]);
    match subcommand.as_slice() {
        b"COUNT" => {
            if args.len() != 1 {
                return wrong_arity("command");
            }
            CommandOutcome::reply(RespFrame::Integer(command_spec_count() as i64))
        }
        b"LIST" => {
            if args.len() != 1 {
                return wrong_arity("command");
            }
            CommandOutcome::reply(command_list_reply())
        }
        b"INFO" => cmd_command_info(&args[1..]),
        b"DOCS" => cmd_command_docs(&args[1..]),
        b"GETKEYS" => cmd_command_getkeys(&args[1..], false),
        b"GETKEYSANDFLAGS" => cmd_command_getkeys(&args[1..], true),
        b"HELP" => {
            if args.len() != 1 {
                return wrong_arity("command");
            }
            CommandOutcome::reply(RespFrame::Array(vec![
                RespFrame::bulk_str("COUNT -- Return the total number of commands."),
                RespFrame::bulk_str("LIST -- Return the command names."),
                RespFrame::bulk_str(
                    "INFO command-name [command-name ...] -- Return command details.",
                ),
                RespFrame::bulk_str(
                    "DOCS [command-name ...] -- Return command docs map for command names.",
                ),
                RespFrame::bulk_str("HELP -- Show this help."),
            ]))
        }
        _ => CommandOutcome::reply(err("ERR unsupported COMMAND subcommand")),
    }
}

fn cmd_command_info(args: &[Bytes]) -> CommandOutcome {
    if args.is_empty() {
        return wrong_arity("command");
    }

    let mut frames = Vec::with_capacity(args.len());
    for name in args {
        if let Some(spec) = find_command_spec(name) {
            frames.push(command_spec_frame(spec));
        } else {
            frames.push(RespFrame::Null);
        }
    }

    CommandOutcome::reply(RespFrame::Array(frames))
}

fn cmd_command_docs(args: &[Bytes]) -> CommandOutcome {
    let specs = if args.is_empty() {
        all_command_specs().collect::<Vec<_>>()
    } else {
        let mut out = Vec::with_capacity(args.len());
        for name in args {
            if let Some(spec) = find_command_spec(name) {
                out.push(spec);
            }
        }
        out
    };

    let mut rows = Vec::with_capacity(specs.len());
    for spec in specs {
        let flags = spec
            .flags
            .iter()
            .map(|flag| RespFrame::bulk_str(flag))
            .collect::<Vec<_>>();
        rows.push((
            RespFrame::BulkString(Some(Bytes::from(spec.name.to_ascii_lowercase()))),
            RespFrame::Map(vec![
                (
                    RespFrame::bulk_str("summary"),
                    RespFrame::bulk_str("Baseline command metadata in Ratatosk."),
                ),
                (
                    RespFrame::bulk_str("arity"),
                    RespFrame::Integer(i64::from(spec.arity)),
                ),
                (RespFrame::bulk_str("flags"), RespFrame::Array(flags)),
            ]),
        ));
    }

    CommandOutcome::reply(RespFrame::Map(rows))
}

pub(super) fn cmd_debug(args: &[Bytes]) -> CommandOutcome {
    if args.is_empty() {
        return wrong_arity("debug");
    }

    let subcommand = to_uppercase_bytes(&args[0]);
    match subcommand.as_slice() {
        b"HELP" => {
            if args.len() != 1 {
                return wrong_arity("debug");
            }
            CommandOutcome::reply(RespFrame::Array(vec![
                RespFrame::bulk_str("HELP -- Show this help."),
                RespFrame::bulk_str(
                    "OBJECT <key> -- Return debug metadata for a key (not supported in baseline).",
                ),
            ]))
        }
        _ => CommandOutcome::reply(err("ERR DEBUG subcommand is not supported")),
    }
}

pub(super) fn cmd_lolwut(args: &[Bytes]) -> CommandOutcome {
    if args.is_empty() {
        return CommandOutcome::reply(RespFrame::BulkString(Some(Bytes::from_static(
            b"Ratatosk says hi from standalone mode.",
        ))));
    }

    if args.len() == 2 && args[0].eq_ignore_ascii_case(b"VERSION") {
        let Some(version) = parse_i64(&args[1]) else {
            return CommandOutcome::reply(err("ERR value is not an integer or out of range"));
        };
        return CommandOutcome::reply(RespFrame::BulkString(Some(Bytes::from(format!(
            "Ratatosk says hi from standalone mode (version {version})."
        )))));
    }

    CommandOutcome::reply(err("ERR syntax error"))
}

pub(super) fn cmd_module(args: &[Bytes]) -> CommandOutcome {
    if args.is_empty() {
        return wrong_arity("module");
    }

    let subcommand = to_uppercase_bytes(&args[0]);
    match subcommand.as_slice() {
        b"HELP" => {
            if args.len() != 1 {
                return wrong_arity("module");
            }
            CommandOutcome::reply(RespFrame::Array(vec![
                RespFrame::bulk_str("LIST -- Return loaded modules."),
                RespFrame::bulk_str("LOAD path [arg ...] -- Load a module (unsupported)."),
                RespFrame::bulk_str(
                    "LOADEX path [CONFIG name value ...] [ARGS ...] -- Load module with options (unsupported).",
                ),
                RespFrame::bulk_str("UNLOAD name -- Unload module (unsupported)."),
                RespFrame::bulk_str("HELP -- Show this help."),
            ]))
        }
        b"LIST" => {
            if args.len() != 1 {
                return wrong_arity("module");
            }
            CommandOutcome::reply(RespFrame::Array(vec![]))
        }
        b"LOAD" | b"LOADEX" | b"UNLOAD" => {
            CommandOutcome::reply(err("ERR MODULE command is not supported in this build"))
        }
        _ => CommandOutcome::reply(err("ERR unknown subcommand for MODULE")),
    }
}

pub(super) fn cmd_hotkeys(args: &[Bytes]) -> CommandOutcome {
    if args.is_empty() {
        return wrong_arity("hotkeys");
    }

    let subcommand = to_uppercase_bytes(&args[0]);
    match subcommand.as_slice() {
        b"GET" => {
            if args.len() != 1 {
                return wrong_arity("hotkeys");
            }
            CommandOutcome::reply(RespFrame::Array(vec![]))
        }
        b"RESET" | b"START" | b"STOP" => {
            if args.len() != 1 {
                return wrong_arity("hotkeys");
            }
            CommandOutcome::reply(RespFrame::ok())
        }
        b"HELP" => {
            if args.len() != 1 {
                return wrong_arity("hotkeys");
            }
            CommandOutcome::reply(RespFrame::Array(vec![
                RespFrame::bulk_str("GET -- Return hot key samples."),
                RespFrame::bulk_str("RESET -- Reset collected hot key samples."),
                RespFrame::bulk_str("START -- Start hot key sampling."),
                RespFrame::bulk_str("STOP -- Stop hot key sampling."),
                RespFrame::bulk_str("HELP -- Show this help."),
            ]))
        }
        _ => CommandOutcome::reply(err("ERR unknown subcommand for HOTKEYS")),
    }
}

pub(super) fn cmd_failover(_args: &[Bytes]) -> CommandOutcome {
    CommandOutcome::reply(err("ERR FAILOVER is not supported in standalone mode"))
}

pub(super) fn cmd_shutdown(_args: &[Bytes]) -> CommandOutcome {
    CommandOutcome::reply(err("ERR SHUTDOWN is not supported in this build"))
}

pub(super) fn cmd_trimslots(args: &[Bytes]) -> CommandOutcome {
    let [slot] = args else {
        return wrong_arity("trimslots");
    };

    let Some(_slot_id) = parse_usize(slot) else {
        return CommandOutcome::reply(err("ERR value is not an integer or out of range"));
    };

    CommandOutcome::reply(RespFrame::Integer(0))
}

pub(super) fn cmd_command_getkeys(args: &[Bytes], with_flags: bool) -> CommandOutcome {
    if args.is_empty() {
        return wrong_arity("command");
    }

    let Some(spec) = find_command_spec(&args[0]) else {
        let name = String::from_utf8_lossy(&args[0]).to_string();
        return CommandOutcome::reply(err(&format!(
            "ERR Invalid command specified, or key spec not found for '{name}'"
        )));
    };

    let argv_len = args.len();
    let Some(positions) = extract_command_key_positions(spec, argv_len) else {
        return CommandOutcome::reply(RespFrame::Array(vec![]));
    };

    let keys: Vec<RespFrame> = positions
        .into_iter()
        .filter_map(|pos| {
            args.get(pos).map(|key| {
                if with_flags {
                    RespFrame::Array(vec![
                        RespFrame::BulkString(Some(key.clone())),
                        RespFrame::Array(vec![
                            RespFrame::bulk_str("RW"),
                            RespFrame::bulk_str("access"),
                            RespFrame::bulk_str("update"),
                        ]),
                    ])
                } else {
                    RespFrame::BulkString(Some(key.clone()))
                }
            })
        })
        .collect();

    CommandOutcome::reply(RespFrame::Array(keys))
}

pub(super) fn command_arity_matches(arity: i16, argc: usize) -> bool {
    if arity >= 0 {
        argc == arity as usize
    } else {
        argc >= (-arity) as usize
    }
}

pub(super) fn extract_command_key_positions(
    spec: CommandSpec,
    argv_len: usize,
) -> Option<Vec<usize>> {
    if spec.first_key == 0 {
        return None;
    }

    let first = spec.first_key as usize;
    let last = if spec.last_key < 0 {
        argv_len.saturating_sub(1)
    } else {
        spec.last_key as usize
    };
    let step = spec.key_step.max(1) as usize;

    let mut positions = Vec::new();
    let mut pos = first;
    while pos <= last && pos < argv_len {
        positions.push(pos);
        pos += step;
    }

    Some(positions)
}

fn command_full_reply() -> RespFrame {
    let specs = all_command_specs().collect::<Vec<_>>();
    let frames = specs.iter().map(|spec| command_spec_frame(*spec)).collect();
    RespFrame::Array(frames)
}

fn command_list_reply() -> RespFrame {
    let names = all_command_specs()
        .map(|spec| RespFrame::BulkString(Some(Bytes::from(spec.name.to_ascii_lowercase()))))
        .collect();
    RespFrame::Array(names)
}

fn command_spec_frame(spec: CommandSpec) -> RespFrame {
    let flags = spec
        .flags
        .iter()
        .map(|flag| RespFrame::bulk_str(flag))
        .collect::<Vec<_>>();

    RespFrame::Array(vec![
        RespFrame::BulkString(Some(Bytes::from(spec.name.to_ascii_lowercase()))),
        RespFrame::Integer(i64::from(spec.arity)),
        RespFrame::Array(flags),
        RespFrame::Integer(spec.first_key),
        RespFrame::Integer(spec.last_key),
        RespFrame::Integer(spec.key_step),
    ])
}
