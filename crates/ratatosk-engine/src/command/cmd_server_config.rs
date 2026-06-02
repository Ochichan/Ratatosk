use std::collections::HashSet;

use bytes::Bytes;
use glob_match::glob_match;
use ratatosk_resp::frame::RespFrame;

use crate::keyspace::{AtomicStatsState, ServerState};
use crate::security::next_audit_stamp;

use super::{ClientState, CommandOutcome, err, parse_i64, to_uppercase_bytes, wrong_arity};

pub(super) fn cmd_config(
    args: &[Bytes],
    server: &mut ServerState,
    atomic_stats: Option<&AtomicStatsState>,
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
            if let Some(stats) = atomic_stats {
                stats.reset();
            }
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
                RespFrame::bulk_str(
                    "REWRITE -- Persist the current runtime configuration to ratatosk.conf.",
                ),
                RespFrame::bulk_str("RESETSTAT -- Reset statistics reported by INFO."),
                RespFrame::bulk_str("HELP -- Show this help."),
            ]))
        }
        _ => CommandOutcome::reply(err("ERR syntax error")),
    }
}

enum ConfigSetOp {
    Timeout(i64),
    Hz(u32),
    AppendOnly(bool),
    AppendFsync(Bytes),
    DbFilename(String),
    Dir(std::path::PathBuf),
    Save(Bytes),
    SlowlogLogSlowerThan(i64),
    SlowlogMaxLen(usize),
    LatencyTracking(bool),
    PubsubQueueHardLimit(usize),
    PubsubQueueSoftLimit(usize),
    PubsubQueueSoftSeconds(u64),
    ActiveExpireCycleLookups(usize),
    ActiveExpireCycleThresholdPct(u32),
    QueryBufferLimit(usize),
    OutputBufferFlushThreshold(usize),
    ClientWriteTimeoutSec(u64),
    CompatibilityMode(Bytes),
    ProtectedMode(Bytes),
}

fn cmd_config_get(args: &[Bytes], server: &ServerState) -> CommandOutcome {
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

fn cmd_config_set(
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
            b"HZ" => {
                let parsed = match parse_i64(value) {
                    Some(v) if (1..=500).contains(&v) => v,
                    _ => {
                        return CommandOutcome::reply(err("ERR Invalid value for hz (1..500)"));
                    }
                };
                (ConfigSetOp::Hz(parsed as u32), "hz")
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
            b"COMPATIBILITY-MODE" => {
                let text = String::from_utf8_lossy(value).to_ascii_lowercase();
                match text.as_str() {
                    "compat" | "strict" => (
                        ConfigSetOp::CompatibilityMode(Bytes::from(text)),
                        "compatibility-mode",
                    ),
                    _ => {
                        return CommandOutcome::reply(err(
                            "ERR argument must be 'compat' or 'strict'",
                        ));
                    }
                }
            }
            b"PROTECTED-MODE" => {
                let text = String::from_utf8_lossy(value).to_ascii_lowercase();
                match text.as_str() {
                    "yes" | "no" => (
                        ConfigSetOp::ProtectedMode(Bytes::from(text)),
                        "protected-mode",
                    ),
                    _ => {
                        return CommandOutcome::reply(err("ERR argument must be 'yes' or 'no'"));
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
                (
                    ConfigSetOp::SlowlogLogSlowerThan(parsed),
                    "slowlog-log-slower-than",
                )
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
                (
                    ConfigSetOp::SlowlogMaxLen(parsed as usize),
                    "slowlog-max-len",
                )
            }
            b"LATENCY-TRACKING" => {
                if value.eq_ignore_ascii_case(b"yes") {
                    (ConfigSetOp::LatencyTracking(true), "latency-tracking")
                } else if value.eq_ignore_ascii_case(b"no") {
                    (ConfigSetOp::LatencyTracking(false), "latency-tracking")
                } else {
                    return CommandOutcome::reply(err("ERR argument must be 'yes' or 'no'"));
                }
            }
            b"PUBSUB-QUEUE-HARD-LIMIT" => {
                let Some(parsed) = parse_i64(value) else {
                    return CommandOutcome::reply(err(
                        "ERR value is not an integer or out of range",
                    ));
                };
                if parsed < 0 {
                    return CommandOutcome::reply(err("ERR value is out of range"));
                }
                (
                    ConfigSetOp::PubsubQueueHardLimit(parsed as usize),
                    "pubsub-queue-hard-limit",
                )
            }
            b"PUBSUB-QUEUE-SOFT-LIMIT" => {
                let Some(parsed) = parse_i64(value) else {
                    return CommandOutcome::reply(err(
                        "ERR value is not an integer or out of range",
                    ));
                };
                if parsed < 0 {
                    return CommandOutcome::reply(err("ERR value is out of range"));
                }
                (
                    ConfigSetOp::PubsubQueueSoftLimit(parsed as usize),
                    "pubsub-queue-soft-limit",
                )
            }
            b"PUBSUB-QUEUE-SOFT-SECONDS" => {
                let Some(parsed) = parse_i64(value) else {
                    return CommandOutcome::reply(err(
                        "ERR value is not an integer or out of range",
                    ));
                };
                if parsed < 0 {
                    return CommandOutcome::reply(err("ERR value is out of range"));
                }
                (
                    ConfigSetOp::PubsubQueueSoftSeconds(parsed as u64),
                    "pubsub-queue-soft-seconds",
                )
            }
            b"ACTIVE-EXPIRE-CYCLE-LOOKUPS" => {
                let parsed = match parse_i64(value) {
                    Some(v) if (1..=1000).contains(&v) => v,
                    _ => {
                        return CommandOutcome::reply(err(
                            "ERR Invalid value for active-expire-cycle-lookups (1..1000)",
                        ));
                    }
                };
                (
                    ConfigSetOp::ActiveExpireCycleLookups(parsed as usize),
                    "active-expire-cycle-lookups",
                )
            }
            b"ACTIVE-EXPIRE-CYCLE-THRESHOLD-PCT" => {
                let parsed = match parse_i64(value) {
                    Some(v) if (1..=100).contains(&v) => v,
                    _ => {
                        return CommandOutcome::reply(err(
                            "ERR Invalid value for active-expire-cycle-threshold-pct (1..100)",
                        ));
                    }
                };
                (
                    ConfigSetOp::ActiveExpireCycleThresholdPct(parsed as u32),
                    "active-expire-cycle-threshold-pct",
                )
            }
            b"QUERY-BUFFER-LIMIT" => {
                let parsed = match parse_i64(value) {
                    Some(v) if v >= 1024 => v,
                    _ => {
                        return CommandOutcome::reply(err(
                            "ERR Invalid value for query-buffer-limit (min 1024)",
                        ));
                    }
                };
                (
                    ConfigSetOp::QueryBufferLimit(parsed as usize),
                    "query-buffer-limit",
                )
            }
            b"OUTPUT-BUFFER-FLUSH-THRESHOLD" => {
                let parsed = match parse_i64(value) {
                    Some(v) if v >= 1024 => v,
                    _ => {
                        return CommandOutcome::reply(err(
                            "ERR Invalid value for output-buffer-flush-threshold (min 1024)",
                        ));
                    }
                };
                (
                    ConfigSetOp::OutputBufferFlushThreshold(parsed as usize),
                    "output-buffer-flush-threshold",
                )
            }
            b"CLIENT-WRITE-TIMEOUT-SEC" => {
                let parsed = match parse_i64(value) {
                    Some(v) if (1..=3600).contains(&v) => v,
                    _ => {
                        return CommandOutcome::reply(err(
                            "ERR Invalid value for client-write-timeout-sec (1..3600)",
                        ));
                    }
                };
                (
                    ConfigSetOp::ClientWriteTimeoutSec(parsed as u64),
                    "client-write-timeout-sec",
                )
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
        let payload = format!("event=CONFIG_SET client_id={} param={}", client.id(), param);
        let stamp = next_audit_stamp("CONFIG_SET", &payload);
        tracing::info!(
            target = "ratatosk::audit",
            event = "CONFIG_SET",
            audit_seq = stamp.seq,
            audit_prev_hash = %stamp.prev_hash,
            audit_hash = %stamp.hash,
            client_id = client.id(),
            param = param,
            "configuration parameter changed"
        );
        match op {
            ConfigSetOp::Timeout(value) => server.config.set_timeout(value),
            ConfigSetOp::Hz(value) => server.config.set_hz(value),
            ConfigSetOp::AppendOnly(value) => server.config.set_appendonly(value),
            ConfigSetOp::AppendFsync(value) => server.config.set_appendfsync(value),
            ConfigSetOp::DbFilename(value) => server.config.set_dbfilename(value),
            ConfigSetOp::Dir(value) => server.config.set_dir(value),
            ConfigSetOp::Save(value) => server.config.set_save(value),
            ConfigSetOp::SlowlogLogSlowerThan(value) => {
                server.stats.set_slowlog_log_slower_than_us(value)
            }
            ConfigSetOp::SlowlogMaxLen(value) => server.stats.set_slowlog_max_len(value),
            ConfigSetOp::LatencyTracking(value) => server.stats.set_latency_tracking_enabled(value),
            ConfigSetOp::PubsubQueueHardLimit(value) => {
                server.config.set_pubsub_queue_hard_limit(value)
            }
            ConfigSetOp::PubsubQueueSoftLimit(value) => {
                server.config.set_pubsub_queue_soft_limit(value)
            }
            ConfigSetOp::PubsubQueueSoftSeconds(value) => {
                server.config.set_pubsub_queue_soft_seconds(value)
            }
            ConfigSetOp::ActiveExpireCycleLookups(value) => {
                server.config.set_active_expire_cycle_lookups(value)
            }
            ConfigSetOp::ActiveExpireCycleThresholdPct(value) => {
                server.config.set_active_expire_cycle_threshold_pct(value)
            }
            ConfigSetOp::QueryBufferLimit(value) => server.config.set_query_buffer_limit(value),
            ConfigSetOp::OutputBufferFlushThreshold(value) => {
                server.config.set_output_buffer_flush_threshold(value)
            }
            ConfigSetOp::ClientWriteTimeoutSec(value) => {
                server.config.set_client_write_timeout_sec(value)
            }
            ConfigSetOp::CompatibilityMode(value) => server.config.set_compatibility_mode(value),
            ConfigSetOp::ProtectedMode(value) => server.config.set_protected_mode(value),
        }
    }

    server.pubsub.set_queue_limits(
        server.config.pubsub_queue_hard_limit(),
        server.config.pubsub_queue_soft_limit(),
        server.config.pubsub_queue_soft_seconds(),
    );

    CommandOutcome::reply(RespFrame::ok()).with_config_dirty()
}

fn known_config_values(server: &ServerState) -> Vec<(Bytes, Bytes)> {
    vec![
        (
            Bytes::from_static(b"bind"),
            Bytes::from(server.config.bind().to_string()),
        ),
        (
            Bytes::from_static(b"port"),
            Bytes::from(server.config.port().to_string()),
        ),
        (
            Bytes::from_static(b"maxclients"),
            Bytes::from(server.config.max_clients().to_string()),
        ),
        (
            Bytes::from_static(b"output-buffer-limit-bytes"),
            Bytes::from(server.config.output_buffer_limit_bytes().to_string()),
        ),
        (
            Bytes::from_static(b"shutdown-grace-ms"),
            Bytes::from(server.config.shutdown_grace_period_ms().to_string()),
        ),
        (
            Bytes::from_static(b"client-timeout-sec"),
            Bytes::from(server.config.client_timeout_sec().to_string()),
        ),
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
            Bytes::from_static(b"compatibility-mode"),
            server.config.compatibility_mode().clone(),
        ),
        (
            Bytes::from_static(b"protected-mode"),
            server.config.protected_mode().clone(),
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
            Bytes::from_static(b"latency-tracking"),
            Bytes::from(if server.stats.latency_tracking_enabled() {
                "yes"
            } else {
                "no"
            }),
        ),
        (
            Bytes::from_static(b"timeout"),
            Bytes::from(server.config.timeout().to_string()),
        ),
        (
            Bytes::from_static(b"maxmemory"),
            Bytes::from(server.config.maxmemory().to_string()),
        ),
        (
            Bytes::from_static(b"maxmemory-policy"),
            server.config.maxmemory_policy().clone(),
        ),
        (
            Bytes::from_static(b"maxmemory-samples"),
            Bytes::from(server.config.maxmemory_samples().to_string()),
        ),
        (
            Bytes::from_static(b"hz"),
            Bytes::from(server.config.hz().to_string()),
        ),
        (
            Bytes::from_static(b"notify-keyspace-events"),
            server.config.notify_keyspace_events().clone(),
        ),
        (
            Bytes::from_static(b"lazyfree-lazy-expire"),
            Bytes::from(if server.config.lazyfree_lazy_expire() {
                "yes"
            } else {
                "no"
            }),
        ),
        (
            Bytes::from_static(b"lazyfree-lazy-server-del"),
            Bytes::from(if server.config.lazyfree_lazy_server_del() {
                "yes"
            } else {
                "no"
            }),
        ),
        (
            Bytes::from_static(b"lazyfree-lazy-user-del"),
            Bytes::from(if server.config.lazyfree_lazy_user_del() {
                "yes"
            } else {
                "no"
            }),
        ),
        (
            Bytes::from_static(b"tcp-keepalive"),
            Bytes::from(server.config.tcp_keepalive().to_string()),
        ),
        (
            Bytes::from_static(b"pubsub-queue-hard-limit"),
            Bytes::from(server.config.pubsub_queue_hard_limit().to_string()),
        ),
        (
            Bytes::from_static(b"pubsub-queue-soft-limit"),
            Bytes::from(server.config.pubsub_queue_soft_limit().to_string()),
        ),
        (
            Bytes::from_static(b"pubsub-queue-soft-seconds"),
            Bytes::from(server.config.pubsub_queue_soft_seconds().to_string()),
        ),
        (
            Bytes::from_static(b"active-expire-cycle-lookups"),
            Bytes::from(server.config.active_expire_cycle_lookups().to_string()),
        ),
        (
            Bytes::from_static(b"active-expire-cycle-threshold-pct"),
            Bytes::from(
                server
                    .config
                    .active_expire_cycle_threshold_pct()
                    .to_string(),
            ),
        ),
        (
            Bytes::from_static(b"query-buffer-limit"),
            Bytes::from(server.config.query_buffer_limit().to_string()),
        ),
        (
            Bytes::from_static(b"output-buffer-flush-threshold"),
            Bytes::from(server.config.output_buffer_flush_threshold().to_string()),
        ),
        (
            Bytes::from_static(b"client-write-timeout-sec"),
            Bytes::from(server.config.client_write_timeout_sec().to_string()),
        ),
    ]
}

fn format_config_scalar_value(value: &str) -> String {
    if value.is_empty()
        || value
            .chars()
            .any(|ch| ch.is_whitespace() || matches!(ch, '#' | '"' | '\\'))
    {
        let mut escaped = String::with_capacity(value.len() + 2);
        escaped.push('"');
        for ch in value.chars() {
            match ch {
                '\\' => escaped.push_str("\\\\"),
                '"' => escaped.push_str("\\\""),
                '\n' => escaped.push_str("\\n"),
                '\r' => escaped.push_str("\\r"),
                '\t' => escaped.push_str("\\t"),
                other => escaped.push(other),
            }
        }
        escaped.push('"');
        return escaped;
    }

    value.to_string()
}

fn rewrite_config_file(server: &ServerState) -> Result<(), String> {
    use std::io::Write;

    let config_dir = server.config.dir();
    let config_path = config_dir.join("ratatosk.conf");
    let temp_path = config_path.with_extension("conf.tmp");

    if !config_dir.exists() {
        std::fs::create_dir_all(config_dir)
            .map_err(|e| format!("creating config directory: {e}"))?;
    }

    let mut file = std::io::BufWriter::new(
        std::fs::File::create(&temp_path).map_err(|e| format!("creating temp config file: {e}"))?,
    );

    writeln!(file, "# Ratatosk configuration file")
        .map_err(|e| format!("writing config header: {e}"))?;
    writeln!(file, "# Auto-generated by CONFIG REWRITE")
        .map_err(|e| format!("writing config header: {e}"))?;
    writeln!(
        file,
        "# Generated at: {}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0)
    )
    .map_err(|e| format!("writing config header: {e}"))?;
    writeln!(file).map_err(|e| format!("writing config: {e}"))?;

    writeln!(file, "# Network").map_err(|e| format!("writing config: {e}"))?;
    writeln!(
        file,
        "bind {}",
        format_config_scalar_value(server.config.bind())
    )
    .map_err(|e| format!("writing config: {e}"))?;
    writeln!(file, "port {}", server.config.port()).map_err(|e| format!("writing config: {e}"))?;
    writeln!(file, "maxclients {}", server.config.max_clients())
        .map_err(|e| format!("writing config: {e}"))?;
    writeln!(
        file,
        "output-buffer-limit-bytes {}",
        server.config.output_buffer_limit_bytes()
    )
    .map_err(|e| format!("writing config: {e}"))?;
    writeln!(
        file,
        "shutdown-grace-ms {}",
        server.config.shutdown_grace_period_ms()
    )
    .map_err(|e| format!("writing config: {e}"))?;
    writeln!(
        file,
        "client-timeout-sec {}",
        server.config.client_timeout_sec()
    )
    .map_err(|e| format!("writing config: {e}"))?;
    writeln!(file, "timeout {}", server.config.timeout())
        .map_err(|e| format!("writing config: {e}"))?;
    writeln!(
        file,
        "compatibility-mode {}",
        String::from_utf8_lossy(server.config.compatibility_mode())
    )
    .map_err(|e| format!("writing config: {e}"))?;
    writeln!(
        file,
        "protected-mode {}",
        String::from_utf8_lossy(server.config.protected_mode())
    )
    .map_err(|e| format!("writing config: {e}"))?;
    writeln!(file).map_err(|e| format!("writing config: {e}"))?;

    writeln!(file, "# Persistence").map_err(|e| format!("writing config: {e}"))?;
    writeln!(
        file,
        "dir {}",
        format_config_scalar_value(&server.config.dir().display().to_string())
    )
    .map_err(|e| format!("writing config: {e}"))?;
    writeln!(
        file,
        "dbfilename {}",
        format_config_scalar_value(server.config.dbfilename())
    )
    .map_err(|e| format!("writing config: {e}"))?;
    writeln!(
        file,
        "appendonly {}",
        if server.config.appendonly() {
            "yes"
        } else {
            "no"
        }
    )
    .map_err(|e| format!("writing config: {e}"))?;
    writeln!(
        file,
        "appendfsync {}",
        String::from_utf8_lossy(server.config.appendfsync())
    )
    .map_err(|e| format!("writing config: {e}"))?;
    let save = String::from_utf8_lossy(server.config.save());
    if save.is_empty() {
        writeln!(file, "save \"\"").map_err(|e| format!("writing config: {e}"))?;
    } else {
        writeln!(file, "save {save}").map_err(|e| format!("writing config: {e}"))?;
    }
    writeln!(file).map_err(|e| format!("writing config: {e}"))?;

    writeln!(file, "# Memory management").map_err(|e| format!("writing config: {e}"))?;
    writeln!(file, "maxmemory {}", server.config.maxmemory())
        .map_err(|e| format!("writing config: {e}"))?;
    writeln!(
        file,
        "maxmemory-policy {}",
        String::from_utf8_lossy(server.config.maxmemory_policy())
    )
    .map_err(|e| format!("writing config: {e}"))?;
    writeln!(
        file,
        "maxmemory-samples {}",
        server.config.maxmemory_samples()
    )
    .map_err(|e| format!("writing config: {e}"))?;
    writeln!(
        file,
        "lazyfree-lazy-expire {}",
        if server.config.lazyfree_lazy_expire() {
            "yes"
        } else {
            "no"
        }
    )
    .map_err(|e| format!("writing config: {e}"))?;
    writeln!(
        file,
        "lazyfree-lazy-server-del {}",
        if server.config.lazyfree_lazy_server_del() {
            "yes"
        } else {
            "no"
        }
    )
    .map_err(|e| format!("writing config: {e}"))?;
    writeln!(
        file,
        "lazyfree-lazy-user-del {}",
        if server.config.lazyfree_lazy_user_del() {
            "yes"
        } else {
            "no"
        }
    )
    .map_err(|e| format!("writing config: {e}"))?;
    writeln!(file).map_err(|e| format!("writing config: {e}"))?;

    writeln!(file, "# Runtime").map_err(|e| format!("writing config: {e}"))?;
    writeln!(file, "hz {}", server.config.hz()).map_err(|e| format!("writing config: {e}"))?;
    writeln!(
        file,
        "active-expire-cycle-lookups {}",
        server.config.active_expire_cycle_lookups()
    )
    .map_err(|e| format!("writing config: {e}"))?;
    writeln!(
        file,
        "active-expire-cycle-threshold-pct {}",
        server.config.active_expire_cycle_threshold_pct()
    )
    .map_err(|e| format!("writing config: {e}"))?;
    writeln!(
        file,
        "query-buffer-limit {}",
        server.config.query_buffer_limit()
    )
    .map_err(|e| format!("writing config: {e}"))?;
    writeln!(
        file,
        "output-buffer-flush-threshold {}",
        server.config.output_buffer_flush_threshold()
    )
    .map_err(|e| format!("writing config: {e}"))?;
    writeln!(
        file,
        "client-write-timeout-sec {}",
        server.config.client_write_timeout_sec()
    )
    .map_err(|e| format!("writing config: {e}"))?;
    writeln!(file, "tcp-keepalive {}", server.config.tcp_keepalive())
        .map_err(|e| format!("writing config: {e}"))?;
    writeln!(file).map_err(|e| format!("writing config: {e}"))?;

    writeln!(file, "# Pub/Sub and notifications").map_err(|e| format!("writing config: {e}"))?;
    writeln!(
        file,
        "notify-keyspace-events {}",
        format_config_scalar_value(&String::from_utf8_lossy(
            server.config.notify_keyspace_events()
        ))
    )
    .map_err(|e| format!("writing config: {e}"))?;
    writeln!(
        file,
        "pubsub-queue-hard-limit {}",
        server.config.pubsub_queue_hard_limit()
    )
    .map_err(|e| format!("writing config: {e}"))?;
    writeln!(
        file,
        "pubsub-queue-soft-limit {}",
        server.config.pubsub_queue_soft_limit()
    )
    .map_err(|e| format!("writing config: {e}"))?;
    writeln!(
        file,
        "pubsub-queue-soft-seconds {}",
        server.config.pubsub_queue_soft_seconds()
    )
    .map_err(|e| format!("writing config: {e}"))?;
    writeln!(file).map_err(|e| format!("writing config: {e}"))?;

    writeln!(file, "# Logging").map_err(|e| format!("writing config: {e}"))?;
    writeln!(
        file,
        "slowlog-log-slower-than {}",
        server.stats.slowlog_log_slower_than_us()
    )
    .map_err(|e| format!("writing config: {e}"))?;
    writeln!(file, "slowlog-max-len {}", server.stats.slowlog_max_len())
        .map_err(|e| format!("writing config: {e}"))?;
    writeln!(
        file,
        "latency-tracking {}",
        if server.stats.latency_tracking_enabled() {
            "yes"
        } else {
            "no"
        }
    )
    .map_err(|e| format!("writing config: {e}"))?;

    drop(file);

    std::fs::rename(&temp_path, &config_path).map_err(|e| format!("renaming config file: {e}"))?;

    tracing::info!(
        target = "ratatosk::config",
        path = %config_path.display(),
        "Configuration file rewritten"
    );

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::rewrite_config_file;
    use crate::keyspace::ServerState;

    #[test]
    fn rewrite_config_file_persists_current_runtime_settings() {
        let dir = tempfile::tempdir().expect("tempdir");
        let config_dir = dir.path().join("data with spaces");
        std::fs::create_dir_all(&config_dir).expect("create config dir");
        let mut server = ServerState::with_default_dbs();
        server.config.set_bind("0.0.0.0".to_string());
        server.config.set_port(6381);
        server.config.set_dir(config_dir.clone());
        server
            .config
            .set_dbfilename("snapshot data.rdb".to_string());
        server.config.set_appendonly(true);
        server
            .config
            .set_appendfsync(bytes::Bytes::from_static(b"always"));

        rewrite_config_file(&server).expect("rewrite config file");

        let config_path = config_dir.join("ratatosk.conf");
        let contents = std::fs::read_to_string(&config_path).expect("read rewritten config");
        assert!(contents.contains("# Auto-generated by CONFIG REWRITE"));
        assert!(contents.contains("bind 0.0.0.0"));
        assert!(contents.contains("port 6381"));
        assert!(contents.contains(&format!("dir \"{}\"", config_dir.display())));
        assert!(contents.contains("dbfilename \"snapshot data.rdb\""));
        assert!(contents.contains("appendonly yes"));
        assert!(contents.contains("appendfsync always"));
    }
}
