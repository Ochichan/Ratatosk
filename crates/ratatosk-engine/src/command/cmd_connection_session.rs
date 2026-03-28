use std::{
    fs::OpenOptions,
    io::Write,
    path::{Path, PathBuf},
};

use bytes::Bytes;
use fs2::available_space;

use ratatosk_resp::frame::RespFrame;

use crate::keyspace::{AtomicStatsState, HotStatsSnapshot, ServerState};
use crate::security::audit_health_snapshot;

use super::{
    ClientState, CommandOutcome, cmd_client, err, parse_i64, to_uppercase_bytes, wrong_arity,
};

pub(super) fn cmd_ping(
    args: &[Bytes],
    server: &mut ServerState,
    atomic_stats: Option<&AtomicStatsState>,
) -> CommandOutcome {
    match args {
        [] => CommandOutcome::reply(RespFrame::pong()),
        [message] => {
            // Deep health check: "PING HEALTH" returns detailed server status
            if message.eq_ignore_ascii_case(b"HEALTH") {
                let stats = HotStatsSnapshot::merged(&server.stats, atomic_stats);
                return CommandOutcome::reply(RespFrame::bulk_str(&generate_health_report(
                    server, stats,
                )));
            }
            CommandOutcome::reply(RespFrame::BulkString(Some(message.clone())))
        }
        _ => wrong_arity("ping"),
    }
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

                if !super::cmd_auth_session::authenticate_client(
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

    client.set_protocol_version(proto);

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

fn generate_health_report(server: &ServerState, stats: HotStatsSnapshot) -> String {
    const MIN_HEALTH_DISK_BYTES: u64 = 64 * 1024 * 1024;

    let connected_clients = server
        .connected_client_snapshots()
        .max(stats.connected_clients as usize);
    let audit_status = audit_health_snapshot();

    let rdb_status = match server.last_rdb_save_status() {
        Some(Ok(())) => "ok",
        Some(Err(_)) => "error",
        None => "none",
    };

    let aof_enabled = server.aof_enabled();
    let memory_used = stats.cached_memory_estimate;
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

    let (disk_writable, disk_available_bytes, disk_error) =
        check_storage_health(server.config.dir());
    let disk_status = if !disk_writable {
        "error"
    } else if disk_available_bytes < MIN_HEALTH_DISK_BYTES {
        "low_space"
    } else {
        "ok"
    };

    let (aof_writable, aof_error) = check_aof_health(server.config.dir(), aof_enabled);
    let aof_latched_error = server.aof_last_error().map(str::to_owned);
    let aof_write_latched = aof_latched_error.is_some();
    let aof_rewrite_in_progress = server.aof_rewrite_in_progress();
    let aof_rewrite_status = match server.last_aof_rewrite_status() {
        Some(Ok(())) => "ok",
        Some(Err(_)) => "error",
        None => "none",
    };

    let status = if memory_status == "critical"
        || disk_status == "error"
        || (aof_enabled && (!aof_writable || aof_write_latched))
    {
        "unhealthy"
    } else if rdb_status == "error"
        || disk_status == "low_space"
        || aof_rewrite_status == "error"
        || audit_status.dirty
    {
        "degraded"
    } else {
        "healthy"
    };

    let total_keys: usize = (0..server.db_count()).map(|idx| server.db(idx).len()).sum();

    let mut report = format!(
        "status:{status}|version:{}|git_hash:{}|build_unix_ts:{}|connected_clients:{connected_clients}|db_count:{}|keys:{}|rdb_save_in_progress:{}|rdb_last_bgsave_status:{rdb_status}|aof_enabled:{}|aof_writable:{}|aof_write_latched:{}|aof_rewrite_in_progress:{}|aof_rewrite_status:{}|audit_chain_dirty:{}|audit_recovery_status:{}|memory_status:{memory_status}|memory_used_bytes:{}|maxmemory_bytes:{}|disk_status:{disk_status}|disk_writable:{}|disk_available_bytes:{}|uptime_seconds:{}",
        env!("CARGO_PKG_VERSION"),
        env!("GIT_HASH"),
        env!("BUILD_UNIX_TS"),
        server.db_count(),
        total_keys,
        server.rdb_save_in_progress(),
        aof_enabled,
        aof_writable,
        aof_write_latched,
        aof_rewrite_in_progress,
        aof_rewrite_status,
        audit_status.dirty,
        audit_status.recovery_status,
        memory_used,
        maxmemory,
        disk_writable,
        disk_available_bytes,
        server.uptime_seconds(),
    );

    if let Some(error) = disk_error {
        report.push_str("|disk_error:");
        report.push_str(&error.replace('|', "_"));
    }

    if let Some(error) = aof_error {
        report.push_str("|aof_error:");
        report.push_str(&error.replace('|', "_"));
    }

    if let Some(error) = aof_latched_error {
        report.push_str("|aof_latched_error:");
        report.push_str(&error.replace('|', "_"));
    }

    report
}

fn check_storage_health(dir: &Path) -> (bool, u64, Option<String>) {
    let available = available_space(dir).unwrap_or(0);
    let probe_file = PathBuf::from(dir).join(".ratatosk_health_probe");

    match OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true)
        .open(&probe_file)
    {
        Ok(mut file) => {
            let write_result = file.write_all(b"ok");
            let _ = std::fs::remove_file(&probe_file);
            if write_result.is_ok() {
                (true, available, None)
            } else {
                (
                    false,
                    available,
                    Some("failed to write probe file".to_string()),
                )
            }
        }
        Err(error) => (false, available, Some(error.to_string())),
    }
}

fn check_aof_health(dir: &Path, aof_enabled: bool) -> (bool, Option<String>) {
    if !aof_enabled {
        return (true, None);
    }

    let aof_path = PathBuf::from(dir).join("appendonly.aof");
    match OpenOptions::new().create(true).append(true).open(&aof_path) {
        Ok(_) => (true, None),
        Err(error) => (false, Some(error.to_string())),
    }
}
