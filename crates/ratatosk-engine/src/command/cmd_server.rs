use std::collections::HashSet;

use bytes::Bytes;

use glob_match::glob_match;
use ratatosk_resp::frame::RespFrame;

use crate::keyspace::{ServerState, StoredValue, purge_expired_key, purge_expired_keys};
use crate::object::now_us;

use super::{
    ClientState, CommandOutcome, err, now_ms, parse_i64, parse_usize, to_uppercase_bytes,
    wrong_arity,
};

pub(super) fn cmd_dbsize(
    args: &[Bytes],
    server: &mut ServerState,
    client: &ClientState,
) -> CommandOutcome {
    if !args.is_empty() {
        return wrong_arity("dbsize");
    }

    let now = now_ms();
    let db = server.db_mut(client.selected_db);
    purge_expired_keys(db, now);
    CommandOutcome::reply(RespFrame::Integer(db.len() as i64))
}

pub(super) fn cmd_time(args: &[Bytes]) -> CommandOutcome {
    if !args.is_empty() {
        return wrong_arity("time");
    }

    let total_us = now_us();
    let sec = total_us / 1_000_000;
    let micro = total_us - sec.saturating_mul(1_000_000);
    CommandOutcome::reply(RespFrame::Array(vec![
        RespFrame::BulkString(Some(Bytes::from(sec.to_string()))),
        RespFrame::BulkString(Some(Bytes::from(micro.to_string()))),
    ]))
}

pub(super) fn cmd_info(
    args: &[Bytes],
    server: &mut ServerState,
    _client: &ClientState,
) -> CommandOutcome {
    let mut include_server = false;
    let mut include_clients = false;
    let mut include_stats = false;
    let mut include_keyspace = false;
    let mut include_persistence = false;

    if args.is_empty() {
        include_server = true;
        include_clients = true;
        include_stats = true;
        include_keyspace = true;
        include_persistence = true;
    } else {
        for section in args {
            let upper = to_uppercase_bytes(section);
            match upper.as_slice() {
                b"ALL" | b"DEFAULT" => {
                    include_server = true;
                    include_clients = true;
                    include_stats = true;
                    include_keyspace = true;
                    include_persistence = true;
                }
                b"SERVER" => include_server = true,
                b"CLIENTS" => include_clients = true,
                b"STATS" => include_stats = true,
                b"KEYSPACE" => include_keyspace = true,
                b"PERSISTENCE" => include_persistence = true,
                _ => {}
            }
        }
    }

    let now = now_ms();
    let mut out = String::new();

    if include_server {
        append_info_server_section(&mut out, server, now);
    }
    if include_clients {
        append_info_clients_section(&mut out, server);
    }
    if include_stats {
        append_info_stats_section(&mut out, server);
    }
    if include_persistence {
        append_info_persistence_section(&mut out, server);
    }
    if include_keyspace {
        append_info_keyspace_section(&mut out, server, now);
    }

    CommandOutcome::reply(RespFrame::bulk_str(&out))
}

pub(super) fn cmd_monitor(args: &[Bytes]) -> CommandOutcome {
    if !args.is_empty() {
        return wrong_arity("monitor");
    }

    CommandOutcome::reply(RespFrame::ok())
}

pub(super) fn cmd_role(args: &[Bytes]) -> CommandOutcome {
    if !args.is_empty() {
        return wrong_arity("role");
    }

    CommandOutcome::reply(RespFrame::Array(vec![
        RespFrame::bulk_str("master"),
        RespFrame::Integer(0),
        RespFrame::Array(vec![]),
    ]))
}

pub(super) fn cmd_replconf(args: &[Bytes]) -> CommandOutcome {
    if args.len() < 2 {
        return wrong_arity("replconf");
    }

    let option = to_uppercase_bytes(&args[0]);
    match option.as_slice() {
        b"LISTENING-PORT" => {
            if args.len() != 2 {
                return wrong_arity("replconf");
            }
            let Some(port) = parse_i64(&args[1]) else {
                return CommandOutcome::reply(err("ERR value is not an integer or out of range"));
            };
            if !(0..=65535).contains(&port) {
                return CommandOutcome::reply(err("ERR value is out of range"));
            }
            CommandOutcome::reply(RespFrame::ok())
        }
        b"CAPA" => {
            if args.len() < 2 {
                return wrong_arity("replconf");
            }
            CommandOutcome::reply(RespFrame::ok())
        }
        b"ACK" => {
            if args.len() != 2 {
                return wrong_arity("replconf");
            }
            let Some(_offset) = parse_i64(&args[1]) else {
                return CommandOutcome::reply(err("ERR value is not an integer or out of range"));
            };
            CommandOutcome::reply(RespFrame::ok())
        }
        b"GETACK" => {
            if args.len() != 2 {
                return wrong_arity("replconf");
            }
            if !args[1].eq_ignore_ascii_case(b"*") {
                return CommandOutcome::reply(err("ERR syntax error"));
            }
            CommandOutcome::reply(RespFrame::Array(vec![
                RespFrame::bulk_str("REPLCONF"),
                RespFrame::bulk_str("ACK"),
                RespFrame::bulk_str("0"),
            ]))
        }
        b"IP-ADDRESS" => {
            if args.len() != 2 {
                return wrong_arity("replconf");
            }
            CommandOutcome::reply(RespFrame::ok())
        }
        _ => CommandOutcome::reply(err("ERR Unknown REPLCONF option")),
    }
}

pub(super) fn cmd_sync(args: &[Bytes]) -> CommandOutcome {
    if !args.is_empty() {
        return wrong_arity("sync");
    }

    CommandOutcome::reply(err("ERR SYNC is not supported in standalone mode"))
}

pub(super) fn cmd_psync(args: &[Bytes]) -> CommandOutcome {
    let [replid, offset_raw] = args else {
        return wrong_arity("psync");
    };

    if replid.as_ref() != b"?" {
        let _ = String::from_utf8_lossy(replid);
    }

    let Some(_offset) = parse_i64(offset_raw) else {
        return CommandOutcome::reply(err("ERR value is not an integer or out of range"));
    };

    CommandOutcome::reply(RespFrame::simple_str(
        "FULLRESYNC 0000000000000000000000000000000000000000 0",
    ))
}

pub(super) fn cmd_replicaof(args: &[Bytes]) -> CommandOutcome {
    let [host, port_raw] = args else {
        return wrong_arity("replicaof");
    };

    if host.eq_ignore_ascii_case(b"NO") && port_raw.eq_ignore_ascii_case(b"ONE") {
        return CommandOutcome::reply(RespFrame::ok());
    }

    let Some(port) = parse_i64(port_raw) else {
        return CommandOutcome::reply(err("ERR value is not an integer or out of range"));
    };
    if !(0..=65535).contains(&port) {
        return CommandOutcome::reply(err("ERR value is out of range"));
    }

    CommandOutcome::reply(RespFrame::ok())
}

pub(super) fn cmd_latency(args: &[Bytes], server: &mut ServerState) -> CommandOutcome {
    if args.is_empty() {
        return wrong_arity("latency");
    }

    let subcommand = to_uppercase_bytes(&args[0]);
    match subcommand.as_slice() {
        b"HELP" => {
            if args.len() != 1 {
                return wrong_arity("latency");
            }
            CommandOutcome::reply(RespFrame::Array(vec![
                RespFrame::bulk_str("DOCTOR -- Return a human readable latency report."),
                RespFrame::bulk_str("GRAPH <event> -- Return an ASCII latency graph."),
                RespFrame::bulk_str("HISTORY <event> -- Return timestamp-latency samples."),
                RespFrame::bulk_str("HISTOGRAM [event ...] -- Return latency histogram buckets."),
                RespFrame::bulk_str("LATEST -- Return the latest latency samples."),
                RespFrame::bulk_str("RESET [event ...] -- Reset latency events."),
                RespFrame::bulk_str("HELP -- Show this help."),
            ]))
        }
        b"LATEST" => {
            if args.len() != 1 {
                return wrong_arity("latency");
            }
            let rows = server
                .stats
                .latency_latest()
                .into_iter()
                .map(|(event, ts, latest_ms, max_ms)| {
                    RespFrame::Array(vec![
                        RespFrame::BulkString(Some(event)),
                        RespFrame::Integer(ts),
                        RespFrame::Integer(latest_ms),
                        RespFrame::Integer(max_ms),
                    ])
                })
                .collect::<Vec<_>>();
            CommandOutcome::reply(RespFrame::Array(rows))
        }
        b"HISTORY" => {
            let [_, event] = args else {
                return wrong_arity("latency");
            };
            let rows = server
                .stats
                .latency_history(event)
                .into_iter()
                .map(|(ts, ms)| {
                    RespFrame::Array(vec![RespFrame::Integer(ts), RespFrame::Integer(ms)])
                })
                .collect::<Vec<_>>();
            CommandOutcome::reply(RespFrame::Array(rows))
        }
        b"RESET" => {
            let removed = server.stats.latency_reset(&args[1..]);
            CommandOutcome::reply(RespFrame::Integer(removed))
        }
        b"DOCTOR" => {
            if args.len() != 1 {
                return wrong_arity("latency");
            }
            let events = server.stats.latency_event_names();
            if events.is_empty() {
                return CommandOutcome::reply(RespFrame::bulk_str(
                    "No latency spikes were observed in the current baseline window.",
                ));
            }
            CommandOutcome::reply(RespFrame::bulk_str(&format!(
                "Observed latency samples for {} event classes.",
                events.len()
            )))
        }
        b"GRAPH" => {
            let [_, event] = args else {
                return wrong_arity("latency");
            };
            let history = server.stats.latency_history(event);
            let latest_ms = history.last().map(|(_, ms)| *ms).unwrap_or(0);
            let max_ms = history.iter().map(|(_, ms)| *ms).max().unwrap_or(0);
            let name = String::from_utf8_lossy(event);
            let graph = format!(
                "{name} - samples: {} latest: {latest_ms} ms max: {max_ms} ms",
                history.len()
            );
            CommandOutcome::reply(RespFrame::bulk_str(&graph))
        }
        b"HISTOGRAM" => {
            let events = if args.len() == 1 {
                server.stats.latency_event_names()
            } else {
                args[1..].to_vec()
            };

            let mut out = Vec::new();
            for event in events {
                let history = server.stats.latency_history(&event);
                if history.is_empty() {
                    continue;
                }

                let mut b0 = 0i64;
                let mut b1 = 0i64;
                let mut b2 = 0i64;
                let mut b3 = 0i64;
                for (_, ms) in history {
                    if ms <= 1 {
                        b0 += 1;
                    } else if ms <= 5 {
                        b1 += 1;
                    } else if ms <= 20 {
                        b2 += 1;
                    } else {
                        b3 += 1;
                    }
                }

                out.push(RespFrame::Array(vec![
                    RespFrame::BulkString(Some(event)),
                    RespFrame::Array(vec![
                        RespFrame::Array(vec![RespFrame::bulk_str("le=1"), RespFrame::Integer(b0)]),
                        RespFrame::Array(vec![RespFrame::bulk_str("le=5"), RespFrame::Integer(b1)]),
                        RespFrame::Array(vec![
                            RespFrame::bulk_str("le=20"),
                            RespFrame::Integer(b2),
                        ]),
                        RespFrame::Array(vec![
                            RespFrame::bulk_str("gt=20"),
                            RespFrame::Integer(b3),
                        ]),
                    ]),
                ]));
            }

            CommandOutcome::reply(RespFrame::Array(out))
        }
        _ => CommandOutcome::reply(err(
            "ERR unknown LATENCY subcommand or wrong number of arguments",
        )),
    }
}

pub(super) fn cmd_config(
    args: &[Bytes],
    server: &mut ServerState,
    client: &ClientState,
) -> CommandOutcome {
    if args.is_empty() {
        return wrong_arity("config");
    }

    let subcommand = to_uppercase_bytes(&args[0]);
    match subcommand.as_slice() {
        b"GET" => cmd_config_get(&args[1..], server),
        b"SET" => cmd_config_set(&args[1..], server, client),
        b"REWRITE" => {
            if args.len() != 1 {
                return wrong_arity("config");
            }
            match rewrite_config_file(server) {
                Ok(()) => CommandOutcome::reply(RespFrame::ok()),
                Err(e) => CommandOutcome::reply(err(&format!("ERR CONFIG REWRITE failed: {e}"))),
            }
        }
        b"RESETSTAT" => {
            if args.len() != 1 {
                return wrong_arity("config");
            }
            server.stats.reset();
            CommandOutcome::reply(RespFrame::ok())
        }
        b"HELP" => {
            if args.len() != 1 {
                return wrong_arity("config");
            }
            CommandOutcome::reply(RespFrame::Array(vec![
                RespFrame::bulk_str(
                    "GET <pattern> [<pattern> ...] -- Return parameters matching one or more glob-style patterns.",
                ),
                RespFrame::bulk_str(
                    "SET <parameter> <value> [<parameter> <value> ...] -- Set one or more server parameters.",
                ),
                RespFrame::bulk_str("REWRITE -- Persist the configuration file (baseline no-op)."),
                RespFrame::bulk_str("RESETSTAT -- Reset statistics reported by INFO."),
                RespFrame::bulk_str("HELP -- Show this help."),
            ]))
        }
        _ => CommandOutcome::reply(err("ERR syntax error")),
    }
}

enum ConfigSetOp {
    Timeout(i64),
    AppendOnly(bool),
    AppendFsync(Bytes),
    DbFilename(String),
    Dir(std::path::PathBuf),
    Save(Bytes),
    SlowlogLogSlowerThan(i64),
    SlowlogMaxLen(usize),
}

pub(super) fn cmd_config_get(args: &[Bytes], server: &ServerState) -> CommandOutcome {
    if args.is_empty() {
        return wrong_arity("config");
    }

    let mut rows = Vec::new();
    let mut seen = HashSet::new();
    let known = known_config_values(server);

    for pattern in args {
        let pattern_text = String::from_utf8_lossy(pattern).to_string();
        for (name, value) in &known {
            let name_text = String::from_utf8_lossy(name);
            if glob_match(&pattern_text, &name_text) && seen.insert(name.clone()) {
                rows.push(RespFrame::BulkString(Some(name.clone())));
                rows.push(RespFrame::BulkString(Some(value.clone())));
            }
        }
    }

    CommandOutcome::reply(RespFrame::Array(rows))
}

pub(super) fn cmd_config_set(
    args: &[Bytes],
    server: &mut ServerState,
    client: &ClientState,
) -> CommandOutcome {
    if args.is_empty() || args.len() % 2 != 0 {
        return wrong_arity("config");
    }

    let mut ops = Vec::new();
    let mut idx = 0usize;
    while idx < args.len() {
        let name = to_uppercase_bytes(&args[idx]);
        let value = &args[idx + 1];
        let (op, param) = match name.as_slice() {
            b"TIMEOUT" => {
                let Some(parsed) = parse_i64(value) else {
                    return CommandOutcome::reply(err(
                        "ERR value is not an integer or out of range",
                    ));
                };
                if parsed < 0 {
                    return CommandOutcome::reply(err("ERR value is out of range"));
                }
                (ConfigSetOp::Timeout(parsed), "timeout")
            }
            b"APPENDONLY" => {
                if value.eq_ignore_ascii_case(b"yes") {
                    (ConfigSetOp::AppendOnly(true), "appendonly")
                } else if value.eq_ignore_ascii_case(b"no") {
                    (ConfigSetOp::AppendOnly(false), "appendonly")
                } else {
                    return CommandOutcome::reply(err("ERR argument must be 'yes' or 'no'"));
                }
            }
            b"APPENDFSYNC" => {
                let text = String::from_utf8_lossy(value).to_ascii_lowercase();
                match text.as_str() {
                    "always" | "everysec" | "no" => {
                        (ConfigSetOp::AppendFsync(Bytes::from(text)), "appendfsync")
                    }
                    _ => {
                        return CommandOutcome::reply(err(
                            "ERR argument must be 'always', 'everysec', or 'no'",
                        ));
                    }
                }
            }
            b"DBFILENAME" => {
                let text = String::from_utf8_lossy(value).into_owned();
                (ConfigSetOp::DbFilename(text), "dbfilename")
            }
            b"DIR" => {
                let text = String::from_utf8_lossy(value).into_owned();
                (ConfigSetOp::Dir(std::path::PathBuf::from(text)), "dir")
            }
            b"SAVE" => (ConfigSetOp::Save(value.clone()), "save"),
            b"SLOWLOG-LOG-SLOWER-THAN" => {
                let Some(parsed) = parse_i64(value) else {
                    return CommandOutcome::reply(err(
                        "ERR value is not an integer or out of range",
                    ));
                };
                (ConfigSetOp::SlowlogLogSlowerThan(parsed), "slowlog-log-slower-than")
            }
            b"SLOWLOG-MAX-LEN" => {
                let Some(parsed) = parse_i64(value) else {
                    return CommandOutcome::reply(err(
                        "ERR value is not an integer or out of range",
                    ));
                };
                if parsed < 0 {
                    return CommandOutcome::reply(err("ERR value is out of range"));
                }
                (ConfigSetOp::SlowlogMaxLen(parsed as usize), "slowlog-max-len")
            }
            b"DATABASES" => {
                return CommandOutcome::reply(err("ERR Unsupported CONFIG parameter: databases"));
            }
            _ => {
                return CommandOutcome::reply(err(
                    "ERR Unknown option or number of arguments for CONFIG SET",
                ));
            }
        };

        ops.push((op, param));
        idx += 2;
    }

    for (op, param) in ops {
        tracing::info!(
            client_id = client.id(),
            param = param,
            "CONFIG SET executed"
        );
        match op {
            ConfigSetOp::Timeout(value) => server.config.set_timeout(value),
            ConfigSetOp::AppendOnly(value) => server.config.set_appendonly(value),
            ConfigSetOp::AppendFsync(value) => server.config.set_appendfsync(value),
            ConfigSetOp::DbFilename(value) => server.config.set_dbfilename(value),
            ConfigSetOp::Dir(value) => server.config.set_dir(value),
            ConfigSetOp::Save(value) => server.config.set_save(value),
            ConfigSetOp::SlowlogLogSlowerThan(value) => {
                server.stats.set_slowlog_log_slower_than_us(value)
            }
            ConfigSetOp::SlowlogMaxLen(value) => server.stats.set_slowlog_max_len(value),
        }
    }

    CommandOutcome::reply(RespFrame::ok())
}

pub(super) fn known_config_values(server: &ServerState) -> Vec<(Bytes, Bytes)> {
    vec![
        (
            Bytes::from_static(b"appendonly"),
            Bytes::from(if server.config.appendonly() {
                "yes"
            } else {
                "no"
            }),
        ),
        (
            Bytes::from_static(b"appendfsync"),
            server.config.appendfsync().clone(),
        ),
        (
            Bytes::from_static(b"databases"),
            Bytes::from(server.db_count().to_string()),
        ),
        (
            Bytes::from_static(b"dbfilename"),
            Bytes::from(server.config.dbfilename().to_string()),
        ),
        (
            Bytes::from_static(b"dir"),
            Bytes::from(server.config.dir().to_string_lossy().into_owned()),
        ),
        (Bytes::from_static(b"save"), server.config.save().clone()),
        (
            Bytes::from_static(b"slowlog-log-slower-than"),
            Bytes::from(server.stats.slowlog_log_slower_than_us().to_string()),
        ),
        (
            Bytes::from_static(b"slowlog-max-len"),
            Bytes::from(server.stats.slowlog_max_len().to_string()),
        ),
        (
            Bytes::from_static(b"timeout"),
            Bytes::from(server.config.timeout().to_string()),
        ),
    ]
}

pub(super) fn cmd_slowlog(args: &[Bytes], server: &mut ServerState) -> CommandOutcome {
    if args.is_empty() {
        return wrong_arity("slowlog");
    }

    let subcommand = to_uppercase_bytes(&args[0]);
    match subcommand.as_slice() {
        b"GET" => {
            if args.len() > 2 {
                return wrong_arity("slowlog");
            }
            let count = if let Some(raw) = args.get(1) {
                let Some(value) = parse_i64(raw) else {
                    return CommandOutcome::reply(err(
                        "ERR value is not an integer or out of range",
                    ));
                };
                if value < 0 {
                    return CommandOutcome::reply(err("ERR value is out of range"));
                }
                value as usize
            } else {
                10usize
            };

            let rows = server
                .stats
                .slowlog_entries()
                .iter()
                .take(count)
                .map(|entry| {
                    RespFrame::Array(vec![
                        RespFrame::Integer(entry.id),
                        RespFrame::Integer(entry.unix_time),
                        RespFrame::Integer(entry.duration_us),
                        RespFrame::Array(
                            entry
                                .argv
                                .iter()
                                .cloned()
                                .map(|arg| RespFrame::BulkString(Some(arg)))
                                .collect::<Vec<_>>(),
                        ),
                    ])
                })
                .collect::<Vec<_>>();
            CommandOutcome::reply(RespFrame::Array(rows))
        }
        b"LEN" => {
            if args.len() != 1 {
                return wrong_arity("slowlog");
            }
            CommandOutcome::reply(RespFrame::Integer(server.stats.slowlog_len() as i64))
        }
        b"RESET" => {
            if args.len() != 1 {
                return wrong_arity("slowlog");
            }
            server.stats.slowlog_reset();
            CommandOutcome::reply(RespFrame::ok())
        }
        b"HELP" => {
            if args.len() != 1 {
                return wrong_arity("slowlog");
            }
            CommandOutcome::reply(RespFrame::Array(vec![
                RespFrame::bulk_str("GET [count] -- Return the slow log entries."),
                RespFrame::bulk_str("LEN -- Return the current number of entries in the slow log."),
                RespFrame::bulk_str("RESET -- Reset the slow log."),
                RespFrame::bulk_str("HELP -- Show this help."),
            ]))
        }
        _ => CommandOutcome::reply(err("ERR syntax error")),
    }
}

pub(super) fn cmd_memory(
    args: &[Bytes],
    server: &mut ServerState,
    client: &ClientState,
) -> CommandOutcome {
    if args.is_empty() {
        return wrong_arity("memory");
    }

    let subcommand = to_uppercase_bytes(&args[0]);
    match subcommand.as_slice() {
        b"USAGE" => cmd_memory_usage(&args[1..], server, client),
        b"STATS" => cmd_memory_stats(&args[1..], server),
        b"DOCTOR" => cmd_memory_doctor(&args[1..]),
        b"MALLOC-STATS" => cmd_memory_malloc_stats(&args[1..]),
        b"PURGE" => cmd_memory_purge(&args[1..]),
        b"HELP" => {
            if args.len() != 1 {
                return wrong_arity("memory");
            }
            CommandOutcome::reply(RespFrame::Array(vec![
                RespFrame::bulk_str(
                    "USAGE <key> [SAMPLES <count>] -- Estimate memory usage of a key.",
                ),
                RespFrame::bulk_str("STATS -- Return allocator and dataset memory statistics."),
                RespFrame::bulk_str("DOCTOR -- Return memory health diagnosis text."),
                RespFrame::bulk_str("MALLOC-STATS -- Return allocator stats text."),
                RespFrame::bulk_str("PURGE -- Ask allocator to release free pages."),
                RespFrame::bulk_str("HELP -- Show this help."),
            ]))
        }
        _ => CommandOutcome::reply(err("ERR syntax error")),
    }
}

pub(super) fn cmd_memory_usage(
    args: &[Bytes],
    server: &mut ServerState,
    client: &ClientState,
) -> CommandOutcome {
    if args.is_empty() || args.len() > 3 {
        return wrong_arity("memory");
    }

    let key = &args[0];
    if args.len() == 3 {
        if !args[1].eq_ignore_ascii_case(b"SAMPLES") {
            return CommandOutcome::reply(err("ERR syntax error"));
        }

        let Some(samples) = parse_i64(&args[2]) else {
            return CommandOutcome::reply(err("ERR value is not an integer or out of range"));
        };
        if samples < 0 {
            return CommandOutcome::reply(err("ERR value is out of range"));
        }
    } else if args.len() == 2 {
        return CommandOutcome::reply(err("ERR syntax error"));
    }

    let now = now_ms();
    let db = server.db_mut(client.selected_db);
    purge_expired_key(db, key, now);

    let Some(value) = db.get(key) else {
        return CommandOutcome::reply(RespFrame::Null);
    };

    let estimate = estimate_value_memory_usage(key, value);
    CommandOutcome::reply(RespFrame::Integer(estimate))
}

pub(super) fn estimate_value_memory_usage(key: &Bytes, value: &StoredValue) -> i64 {
    use crate::keyspace::ValueData;

    let mut total = key.len().saturating_add(64);

    match &value.data {
        ValueData::String(v) => {
            total = total.saturating_add(v.len());
        }
        ValueData::Hash(hash) => {
            total = total.saturating_add(48);
            for (field, item) in hash {
                total = total
                    .saturating_add(field.len())
                    .saturating_add(item.value.len())
                    .saturating_add(24);
            }
        }
        ValueData::List(list) => {
            total = total.saturating_add(48);
            for item in list {
                total = total.saturating_add(item.len()).saturating_add(8);
            }
        }
        ValueData::Set(set) => {
            total = total.saturating_add(48);
            for item in set {
                total = total.saturating_add(item.len()).saturating_add(16);
            }
        }
        ValueData::SortedSet(zset) => {
            total = total.saturating_add(64);
            for entry in zset.by_score.keys() {
                total = total
                    .saturating_add(entry.member.len())
                    .saturating_add(8)
                    .saturating_add(24);
            }
        }
        ValueData::Stream { entries, groups } => {
            total = total.saturating_add(64);
            for entry in entries {
                total = total.saturating_add(32);
                for (k, v) in &entry.fields {
                    total = total
                        .saturating_add(k.len())
                        .saturating_add(v.len())
                        .saturating_add(16);
                }
            }
            total = total.saturating_add(groups.len().saturating_mul(64));
        }
    }

    i64::try_from(total).unwrap_or(i64::MAX)
}
pub(super) fn cmd_memory_stats(args: &[Bytes], server: &ServerState) -> CommandOutcome {
    if !args.is_empty() {
        return wrong_arity("memory");
    }

    let total_keys = (0..server.db_count())
        .map(|db| server.db(db).len() as i64)
        .sum::<i64>();
    let total_dataset_bytes = (0..server.db_count())
        .flat_map(|db| server.db(db).iter())
        .map(|(key, value)| estimate_value_memory_usage(key, value))
        .sum::<i64>();

    CommandOutcome::reply(RespFrame::Map(vec![
        (
            RespFrame::bulk_str("peak.allocated"),
            RespFrame::Integer(total_dataset_bytes),
        ),
        (
            RespFrame::bulk_str("total.allocated"),
            RespFrame::Integer(total_dataset_bytes),
        ),
        (
            RespFrame::bulk_str("dataset.bytes"),
            RespFrame::Integer(total_dataset_bytes),
        ),
        (
            RespFrame::bulk_str("dataset.keys"),
            RespFrame::Integer(total_keys),
        ),
        (
            RespFrame::bulk_str("allocator.active"),
            RespFrame::Integer(total_dataset_bytes),
        ),
    ]))
}

pub(super) fn cmd_memory_doctor(args: &[Bytes]) -> CommandOutcome {
    if !args.is_empty() {
        return wrong_arity("memory");
    }

    CommandOutcome::reply(RespFrame::bulk_str(
        "Hi Sam, this instance uses baseline memory diagnostics. No critical issues detected.",
    ))
}

pub(super) fn cmd_memory_malloc_stats(args: &[Bytes]) -> CommandOutcome {
    if !args.is_empty() {
        return wrong_arity("memory");
    }

    CommandOutcome::reply(RespFrame::bulk_str(
        "allocator:system
active:baseline
",
    ))
}

pub(super) fn cmd_memory_purge(args: &[Bytes]) -> CommandOutcome {
    if !args.is_empty() {
        return wrong_arity("memory");
    }

    CommandOutcome::reply(RespFrame::ok())
}

pub(super) fn append_info_server_section(out: &mut String, server: &ServerState, now_ms: i64) {
    let uptime_seconds = (now_ms.saturating_sub(server.started_at_ms()) / 1000).max(0);
    out.push_str("# Server\r\n");
    out.push_str("redis_version:7.2.0-ratatosk\r\n");
    out.push_str("redis_mode:standalone\r\n");
    out.push_str(&format!("uptime_in_seconds:{uptime_seconds}\r\n"));
    out.push_str(&format!("uptime_in_days:{}\r\n", uptime_seconds / 86_400));
    out.push_str("\r\n");
}

pub(super) fn append_info_clients_section(out: &mut String, server: &ServerState) {
    out.push_str("# Clients\r\n");
    out.push_str(&format!(
        "connected_clients:{}\r\n",
        server.stats.connected_clients()
    ));
    out.push_str("blocked_clients:0\r\n");
    out.push_str("tracking_clients:0\r\n");
    out.push_str("\r\n");
}

pub(super) fn append_info_stats_section(out: &mut String, server: &ServerState) {
    out.push_str("# Stats\r\n");
    out.push_str(&format!(
        "total_connections_received:{}\r\n",
        server.total_connections_received()
    ));
    out.push_str(&format!(
        "total_commands_processed:{}\r\n",
        server.stats.total_commands_processed()
    ));
    out.push_str(&format!(
        "instantaneous_ops_per_sec:{}\r\n",
        server.stats.instantaneous_ops_per_sec()
    ));
    out.push_str(&format!(
        "total_net_input_bytes:{}\r\n",
        server.stats.total_net_input_bytes()
    ));
    out.push_str(&format!(
        "total_net_output_bytes:{}\r\n",
        server.stats.total_net_output_bytes()
    ));
    out.push_str(&format!(
        "evicted_keys:{}\r\n",
        server.stats.evicted_keys()
    ));
    out.push_str(&format!(
        "expired_keys:{}\r\n",
        server.stats.expired_keys()
    ));
    out.push_str(&format!(
        "keyspace_hits:{}\r\n",
        server.stats.keyspace_hits()
    ));
    out.push_str(&format!(
        "keyspace_misses:{}\r\n",
        server.stats.keyspace_misses()
    ));
    out.push_str("\r\n");
}

pub(super) fn append_info_persistence_section(out: &mut String, server: &ServerState) {
    use ratatosk_core::time::now_sec;
    
    out.push_str("# Persistence\r\n");
    
    // RDB section
    out.push_str(&format!(
        "rdb_last_save_time:{}\r\n",
        server.stats.last_save_unix_sec()
    ));
    out.push_str(&format!(
        "rdb_last_save_elapsed:{}\r\n",
        now_sec().saturating_sub(server.stats.last_save_unix_sec())
    ));
    out.push_str(&format!(
        "rdb_bgsave_in_progress:{}\r\n",
        i32::from(server.rdb_save_in_progress())
    ));
    
    let (status, error_msg) = match server.last_rdb_save_status() {
        Some(Ok(())) | None => ("ok", None),
        Some(Err(e)) => ("err", Some(e.as_str())),
    };
    out.push_str(&format!("rdb_last_bgsave_status:{status}\r\n"));
    if let Some(err) = error_msg {
        out.push_str(&format!("rdb_last_bgsave_error:{err}\r\n"));
    }
    if let Some(time_ms) = server.last_rdb_save_time_ms() {
        out.push_str(&format!("rdb_last_save_timestamp_ms:{time_ms}\r\n"));
    }
    
    // AOF section
    out.push_str(&format!("aof_enabled:{}\r\n", i32::from(server.aof_enabled())));
    out.push_str(&format!(
        "aof_rewrite_in_progress:0\r\n"
    ));
    out.push_str(&format!(
        "aof_current_size:0\r\n"
    ));
    out.push_str(&format!(
        "aof_base_size:0\r\n"
    ));
    
    out.push_str("\r\n");
}

pub(super) fn append_info_keyspace_section(
    out: &mut String,
    server: &mut ServerState,
    now_ms: i64,
) {
    out.push_str("# Keyspace\r\n");

    let db_count = server.db_count();
    for db_idx in 0..db_count {
        let db = server.db_mut(db_idx);
        purge_expired_keys(db, now_ms);

        if db.is_empty() {
            continue;
        }

        let mut expires = 0u64;
        let mut ttl_sum = 0i64;
        for value in db.values() {
            if let Some(expire_at_ms) = value.expire_at_ms {
                if expire_at_ms > now_ms {
                    expires = expires.saturating_add(1);
                    ttl_sum = ttl_sum.saturating_add(expire_at_ms.saturating_sub(now_ms));
                }
            }
        }

        let avg_ttl = if expires == 0 {
            0
        } else {
            ttl_sum / i64::try_from(expires).unwrap_or(1)
        };

        out.push_str(&format!(
            "db{db_idx}:keys={},expires={expires},avg_ttl={avg_ttl}\r\n",
            db.len()
        ));
    }

    out.push_str("\r\n");
}

pub(super) fn cmd_lastsave(args: &[Bytes], server: &ServerState) -> CommandOutcome {
    if !args.is_empty() {
        return wrong_arity("lastsave");
    }

    CommandOutcome::reply(RespFrame::Integer(server.stats.last_save_unix_sec()))
}

pub(super) fn cmd_save(args: &[Bytes], server: &mut ServerState) -> CommandOutcome {
    if !args.is_empty() {
        return wrong_arity("save");
    }

    if server.rdb_save_in_progress() {
        return CommandOutcome::reply(err("ERR Background save already in progress"));
    }

    CommandOutcome::reply(RespFrame::ok())
}

pub(super) fn cmd_bgsave(args: &[Bytes], server: &mut ServerState) -> CommandOutcome {
    if args.len() > 1 {
        return wrong_arity("bgsave");
    }

    if let Some(mode) = args.first() {
        if !mode.eq_ignore_ascii_case(b"SCHEDULE") {
            return CommandOutcome::reply(err("ERR syntax error"));
        }
    }

    if server.rdb_save_in_progress() {
        return CommandOutcome::reply(err("ERR Background save already in progress"));
    }

    CommandOutcome::reply(RespFrame::simple_str("Background saving started"))
}

pub(super) fn cmd_bgrewriteaof(args: &[Bytes]) -> CommandOutcome {
    if !args.is_empty() {
        return wrong_arity("bgrewriteaof");
    }

    CommandOutcome::reply(RespFrame::simple_str(
        "Background append only file rewriting started",
    ))
}

pub(super) fn cmd_sflush(args: &[Bytes]) -> CommandOutcome {
    if let Err(outcome) = parse_flush_mode(args, "sflush") {
        return outcome;
    }

    CommandOutcome::reply(RespFrame::ok())
}

pub(super) fn cmd_swapdb(args: &[Bytes], server: &mut ServerState) -> CommandOutcome {
    let [left, right] = args else {
        return wrong_arity("swapdb");
    };

    let Some(left_idx) = parse_usize(left) else {
        return CommandOutcome::reply(err("ERR value is not an integer or out of range"));
    };
    let Some(right_idx) = parse_usize(right) else {
        return CommandOutcome::reply(err("ERR value is not an integer or out of range"));
    };

    if left_idx >= server.db_count() || right_idx >= server.db_count() {
        return CommandOutcome::reply(err("ERR DB index is out of range"));
    }

    if left_idx != right_idx {
        server.swap_dbs(left_idx, right_idx);
    }

    CommandOutcome::reply(RespFrame::ok())
}

pub(super) fn parse_flush_mode(args: &[Bytes], command: &str) -> Result<(), CommandOutcome> {
    if args.len() > 1 {
        return Err(wrong_arity(command));
    }

    if let Some(mode) = args.first() {
        if !mode.eq_ignore_ascii_case(b"SYNC") && !mode.eq_ignore_ascii_case(b"ASYNC") {
            return Err(CommandOutcome::reply(err("ERR syntax error")));
        }
    }

    Ok(())
}

pub(super) fn cmd_flushdb(
    args: &[Bytes],
    server: &mut ServerState,
    client: &ClientState,
) -> CommandOutcome {
    if let Err(outcome) = parse_flush_mode(args, "flushdb") {
        return outcome;
    }

    server.clear_db(client.selected_db);
    CommandOutcome::reply(RespFrame::ok())
}

pub(super) fn cmd_flushall(args: &[Bytes], server: &mut ServerState) -> CommandOutcome {
    if let Err(outcome) = parse_flush_mode(args, "flushall") {
        return outcome;
    }

    server.clear_all_dbs();
    CommandOutcome::reply(RespFrame::ok())
}

/// Rewrite the configuration file with current runtime settings.
fn rewrite_config_file(server: &ServerState) -> Result<(), String> {
    use std::io::Write;
    
    let config_dir = server.config.dir();
    let config_path = config_dir.join("ratatosk.conf");
    let temp_path = config_path.with_extension("conf.tmp");
    
    // Create config directory if it doesn't exist
    if !config_dir.exists() {
        std::fs::create_dir_all(config_dir)
            .map_err(|e| format!("creating config directory: {e}"))?;
    }
    
    let mut file = std::io::BufWriter::new(
        std::fs::File::create(&temp_path)
            .map_err(|e| format!("creating temp config file: {e}"))?
    );
    
    // Write header
    writeln!(file, "# Ratatosk configuration file")
        .map_err(|e| format!("writing config header: {e}"))?;
    writeln!(file, "# Auto-generated by CONFIG REWRITE")
        .map_err(|e| format!("writing config header: {e}"))?;
    writeln!(file, "# Generated at: {}", 
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0)
    ).map_err(|e| format!("writing config header: {e}"))?;
    writeln!(file).map_err(|e| format!("writing config: {e}"))?;
    
    // Write current configuration values
    writeln!(file, "# Network")
        .map_err(|e| format!("writing config: {e}"))?;
    writeln!(file, "bind 127.0.0.1")
        .map_err(|e| format!("writing config: {e}"))?;
    writeln!(file, "port 6379")
        .map_err(|e| format!("writing config: {e}"))?;
    writeln!(file).map_err(|e| format!("writing config: {e}"))?;
    
    writeln!(file, "# Persistence")
        .map_err(|e| format!("writing config: {e}"))?;
    writeln!(file, "dir {}", server.config.dir().display())
        .map_err(|e| format!("writing config: {e}"))?;
    writeln!(file, "dbfilename {}", server.config.dbfilename())
        .map_err(|e| format!("writing config: {e}"))?;
    writeln!(file, "appendonly {}", if server.config.appendonly() { "yes" } else { "no" })
        .map_err(|e| format!("writing config: {e}"))?;
    writeln!(file, "appendfsync {}", String::from_utf8_lossy(server.config.appendfsync()))
        .map_err(|e| format!("writing config: {e}"))?;
    writeln!(file).map_err(|e| format!("writing config: {e}"))?;
    
    writeln!(file, "# Memory management")
        .map_err(|e| format!("writing config: {e}"))?;
    writeln!(file, "maxmemory {}", server.config.maxmemory())
        .map_err(|e| format!("writing config: {e}"))?;
    writeln!(file, "maxmemory-policy {}", String::from_utf8_lossy(server.config.maxmemory_policy()))
        .map_err(|e| format!("writing config: {e}"))?;
    writeln!(file).map_err(|e| format!("writing config: {e}"))?;
    
    writeln!(file, "# Logging")
        .map_err(|e| format!("writing config: {e}"))?;
    writeln!(file, "slowlog-log-slower-than {}", server.stats.slowlog_log_slower_than_us())
        .map_err(|e| format!("writing config: {e}"))?;
    writeln!(file, "slowlog-max-len {}", server.stats.slowlog_max_len())
        .map_err(|e| format!("writing config: {e}"))?;
    
    // Flush and close file
    drop(file);
    
    // Atomically rename temp file to actual config file
    std::fs::rename(&temp_path, &config_path)
        .map_err(|e| format!("renaming config file: {e}"))?;
    
    tracing::info!(
        target = "ratatosk::config",
        path = %config_path.display(),
        "Configuration file rewritten"
    );
    
    Ok(())
}
