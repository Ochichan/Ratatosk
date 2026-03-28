use std::fs;

use bytes::Bytes;

use ratatosk_resp::frame::RespFrame;

use crate::keyspace::{AtomicStatsState, HotStatsSnapshot, ServerState, purge_expired_keys};
use crate::security::audit_health_snapshot;

use super::{CommandOutcome, now_ms, to_uppercase_bytes};

pub(super) fn cmd_info(
    args: &[Bytes],
    server: &mut ServerState,
    atomic_stats: Option<&AtomicStatsState>,
) -> CommandOutcome {
    let mut include_server = false;
    let mut include_clients = false;
    let mut include_stats = false;
    let mut include_keyspace = false;
    let mut include_persistence = false;
    let mut include_replication = false;

    if args.is_empty() {
        include_server = true;
        include_clients = true;
        include_stats = true;
        include_keyspace = true;
        include_persistence = true;
        include_replication = true;
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
                    include_replication = true;
                }
                b"SERVER" => include_server = true,
                b"CLIENTS" => include_clients = true,
                b"STATS" => include_stats = true,
                b"KEYSPACE" => include_keyspace = true,
                b"PERSISTENCE" => include_persistence = true,
                b"REPLICATION" => include_replication = true,
                _ => {}
            }
        }
    }

    let now = now_ms();
    let stats = HotStatsSnapshot::merged(&server.stats, atomic_stats);
    let mut out = String::new();

    if include_server {
        append_info_server_section(&mut out, server, now, stats);
    }
    if include_clients {
        append_info_clients_section(&mut out, server, stats);
    }
    if include_stats {
        append_info_stats_section(&mut out, stats);
    }
    if include_replication {
        super::cmd_server_replication::append_info_replication_section(&mut out, server);
    }
    if include_persistence {
        append_info_persistence_section(&mut out, server);
    }
    if include_keyspace {
        append_info_keyspace_section(&mut out, server, now);
    }

    CommandOutcome::reply(RespFrame::bulk_str(&out))
}

fn append_info_server_section(
    out: &mut String,
    server: &ServerState,
    now_ms: i64,
    stats: HotStatsSnapshot,
) {
    let uptime_seconds = (now_ms.saturating_sub(server.started_at_ms()) / 1000).max(0);
    out.push_str("# Server\r\n");
    out.push_str(&format!(
        "redis_version:{}-ratatosk\r\n",
        env!("CARGO_PKG_VERSION")
    ));
    out.push_str("redis_mode:standalone\r\n");
    out.push_str(&format!("ratatosk_git_hash:{}\r\n", env!("GIT_HASH")));
    out.push_str(&format!(
        "ratatosk_build_unix_ts:{}\r\n",
        env!("BUILD_UNIX_TS")
    ));
    out.push_str(&format!("uptime_in_seconds:{uptime_seconds}\r\n"));
    out.push_str(&format!("uptime_in_days:{}\r\n", uptime_seconds / 86_400));
    out.push_str(&format!(
        "health_status:{}\r\n",
        server_health_status(server, stats)
    ));
    out.push_str(&format!(
        "bridge_contract_version:{}\r\n",
        crate::keyspace::BRIDGE_CONTRACT_VERSION
    ));
    out.push_str("\r\n");
}

fn server_health_status(server: &ServerState, stats: HotStatsSnapshot) -> &'static str {
    let audit_status = audit_health_snapshot();
    let memory_critical = server.config.maxmemory() > 0
        && stats.cached_memory_estimate >= server.config.maxmemory() as u64;
    let persistence_unhealthy = server.aof_write_latched();
    let persistence_degraded = server.last_rdb_save_status().is_some_and(Result::is_err)
        || server.last_aof_rewrite_status().is_some_and(Result::is_err)
        || audit_status.dirty;

    if !memory_critical && !persistence_unhealthy && !persistence_degraded {
        "healthy"
    } else if memory_critical || persistence_unhealthy {
        "unhealthy"
    } else {
        "degraded"
    }
}

fn file_size_or_zero(path: Option<&std::path::PathBuf>) -> u64 {
    path.and_then(|path| fs::metadata(path).ok().map(|metadata| metadata.len()))
        .unwrap_or(0)
}

fn append_info_clients_section(out: &mut String, server: &ServerState, stats: HotStatsSnapshot) {
    out.push_str("# Clients\r\n");
    out.push_str(&format!(
        "connected_clients:{}\r\n",
        server
            .connected_client_snapshots()
            .max(stats.connected_clients as usize)
    ));
    out.push_str(&format!("blocked_clients:{}\r\n", server.blocked_clients()));
    out.push_str(&format!(
        "tracking_clients:{}\r\n",
        server.tracking_clients()
    ));
    out.push_str("\r\n");
}

fn append_info_stats_section(out: &mut String, stats: HotStatsSnapshot) {
    out.push_str("# Stats\r\n");
    out.push_str(&format!(
        "total_connections_received:{}\r\n",
        stats.total_connections_received
    ));
    out.push_str(&format!(
        "total_commands_processed:{}\r\n",
        stats.total_commands_processed
    ));
    out.push_str(&format!(
        "instantaneous_ops_per_sec:{}\r\n",
        stats.instantaneous_ops_per_sec
    ));
    out.push_str(&format!(
        "total_net_input_bytes:{}\r\n",
        stats.total_net_input_bytes
    ));
    out.push_str(&format!(
        "total_net_output_bytes:{}\r\n",
        stats.total_net_output_bytes
    ));
    out.push_str(&format!("evicted_keys:{}\r\n", stats.evicted_keys));
    out.push_str(&format!("expired_keys:{}\r\n", stats.expired_keys));
    out.push_str(&format!("keyspace_hits:{}\r\n", stats.keyspace_hits));
    out.push_str(&format!("keyspace_misses:{}\r\n", stats.keyspace_misses));
    out.push_str("\r\n");
}

fn append_info_persistence_section(out: &mut String, server: &ServerState) {
    use ratatosk_core::time::now_sec;

    out.push_str("# Persistence\r\n");
    let audit_status = audit_health_snapshot();

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

    out.push_str(&format!(
        "aof_enabled:{}\r\n",
        i32::from(server.aof_enabled())
    ));
    let aof_write_latched = server.aof_write_latched();
    out.push_str(&format!(
        "aof_write_latched:{}\r\n",
        i32::from(aof_write_latched)
    ));
    if let Some(error) = server.aof_last_error() {
        let sanitized = error.replace(['\r', '\n'], " ");
        out.push_str(&format!("aof_last_error:{}\r\n", sanitized));
    }
    out.push_str("aof_rewrite_supported:1\r\n");
    out.push_str(&format!(
        "aof_rewrite_in_progress:{}\r\n",
        i32::from(server.aof_rewrite_in_progress())
    ));
    let (rewrite_status, rewrite_error) = match server.last_aof_rewrite_status() {
        Some(Ok(())) => ("ok", None),
        Some(Err(error)) => ("err", Some(error.as_str())),
        None => ("none", None),
    };
    out.push_str(&format!("aof_last_rewrite_status:{rewrite_status}\r\n"));
    if let Some(error) = rewrite_error {
        let sanitized = error.replace(['\r', '\n'], " ");
        out.push_str(&format!("aof_last_rewrite_error:{}\r\n", sanitized));
    }
    if let Some(time_ms) = server.last_aof_rewrite_time_ms() {
        out.push_str(&format!("aof_last_rewrite_timestamp_ms:{time_ms}\r\n"));
    }
    out.push_str(&format!(
        "aof_current_size:{}\r\n",
        file_size_or_zero(server.aof_current_path())
    ));
    out.push_str(&format!(
        "aof_base_size:{}\r\n",
        file_size_or_zero(server.aof_base_path())
    ));
    out.push_str(&format!(
        "audit_chain_dirty:{}\r\n",
        i32::from(audit_status.dirty)
    ));
    out.push_str(&format!(
        "audit_recovery_status:{}\r\n",
        audit_status.recovery_status
    ));

    out.push_str("\r\n");
}

fn append_info_keyspace_section(out: &mut String, server: &mut ServerState, now_ms: i64) {
    out.push_str("# Keyspace\r\n");

    let db_count = server.db_count();
    for db_idx in 0..db_count {
        let mut db = server.db_mut(db_idx);
        purge_expired_keys(&mut db, now_ms);

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
