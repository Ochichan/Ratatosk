mod cmd_acl;
mod cmd_auth_session;
mod cmd_bitmap;
mod cmd_bitmap_bitfield;
mod cmd_client;
mod cmd_client_introspection;
mod cmd_client_tracking;
mod cmd_cluster;
mod cmd_command_metadata;
mod cmd_connection;
mod cmd_connection_session;
mod cmd_generic;
mod cmd_generic_dump;
mod cmd_generic_replication;
mod cmd_generic_string;
mod cmd_geo;
mod cmd_geo_query;
mod cmd_geo_radius;
mod cmd_hash;
mod cmd_hash_field_ops;
mod cmd_hash_read;
mod cmd_hash_ttl;
mod cmd_hll;
mod cmd_key;
mod cmd_key_meta;
mod cmd_list;
mod cmd_list_blocking;
mod cmd_list_pop;
mod cmd_pubsub;
mod cmd_script;
mod cmd_sentinel;
mod cmd_server;
mod cmd_server_config;
mod cmd_server_info;
mod cmd_server_latency;
mod cmd_server_maintenance;
mod cmd_server_memory;
mod cmd_server_replication;
mod cmd_server_slowlog;
mod cmd_set;
mod cmd_set_algebra;
mod cmd_sorted_set;
mod cmd_sorted_set_pop;
mod cmd_sorted_set_ranges;
mod cmd_sorted_set_sampling;
mod cmd_sorted_set_setops;
mod cmd_stream;
mod cmd_stream_admin;
mod cmd_stream_groups;
mod cmd_stream_lifecycle;
mod cmd_string;
mod cmd_string_access;
mod cmd_string_numeric;
mod cmd_transaction;
#[cfg(feature = "lua-scripting")]
mod lua_runtime;
mod registry;

use cmd_key::{
    parse_scan_cursor, parse_scan_match_count_options, scan_collect_indexes, scan_reply,
};

use std::time::Instant;

use bytes::Bytes;
use hashbrown::HashMap as HashBrownMap;
use smallvec::SmallVec;

use ratatosk_resp::frame::RespFrame;

use crate::{
    expiry::{ExpireCondition, ExpireMode, ExpireTimeMode, GetExPolicy, TtlMode},
    keyspace::{AtomicStatsState, ClientSnapshot, DefaultAclPolicyState, ServerState},
    security::sanitize_error_message,
};
use registry::{
    all_command_specs, command_spec_count, find_command_spec, find_command_spec_parts,
    find_command_spec_upper, is_write_command_name,
};

// ---------------------------------------------------------------------------
// ServerAccess — bridge type for command handlers
// ---------------------------------------------------------------------------

/// Wraps `&mut ServerState`. DB access goes through `server.db()` / `server.db_mut()`
/// which acquire per-DB `parking_lot::RwLock` guards internally.
///
/// The dispatch table in `execute()` reborrows `access.meta` as `server` and passes
/// it to handlers. Handlers use `server.db_mut(idx)` which returns a
/// `MappedRwLockWriteGuard` that auto-derefs to `&mut HashMap`.
pub struct ServerAccess<'a> {
    pub meta: &'a mut ServerState,
    pub atomic_stats: Option<&'a AtomicStatsState>,
    pub default_acl_policy: Option<&'a DefaultAclPolicyState>,
}

impl<'a> ServerAccess<'a> {
    pub fn new_inline(server: &'a mut ServerState) -> Self {
        Self {
            meta: server,
            atomic_stats: None,
            default_acl_policy: None,
        }
    }

    pub fn new_with_atomic_stats(
        server: &'a mut ServerState,
        atomic_stats: &'a AtomicStatsState,
    ) -> Self {
        Self::new_with_runtime_caches(server, atomic_stats, None)
    }

    pub fn new_with_runtime_caches(
        server: &'a mut ServerState,
        atomic_stats: &'a AtomicStatsState,
        default_acl_policy: Option<&'a DefaultAclPolicyState>,
    ) -> Self {
        Self {
            meta: server,
            atomic_stats: Some(atomic_stats),
            default_acl_policy,
        }
    }

    pub fn new_with_optional_atomic_stats(
        server: &'a mut ServerState,
        atomic_stats: Option<&'a AtomicStatsState>,
    ) -> Self {
        Self {
            meta: server,
            atomic_stats,
            default_acl_policy: None,
        }
    }

    fn mark_command_processed(&mut self) {
        self.meta.stats.mark_command_processed();
        if let Some(stats) = self.atomic_stats {
            stats.mark_command_processed();
        }
    }

    fn default_user_is_nopass_enabled(&self) -> bool {
        self.default_acl_policy.map_or_else(
            || self.meta.acl.default_user_is_nopass_enabled(),
            DefaultAclPolicyState::default_user_is_nopass_enabled,
        )
    }

    fn default_user_has_full_access(&self) -> bool {
        self.default_acl_policy.map_or_else(
            || self.meta.acl.default_user_has_full_access(),
            DefaultAclPolicyState::default_user_has_full_access,
        )
    }
}

#[derive(Debug, Clone, Copy)]
struct CommandSpec {
    name: &'static str,
    arity: i16,
    flags: &'static [&'static str],
    first_key: i64,
    last_key: i64,
    key_step: i64,
}

const COMMAND_SPECS: &[CommandSpec] = &[
    CommandSpec {
        name: "PING",
        arity: -1,
        flags: &["fast", "connection"],
        first_key: 0,
        last_key: 0,
        key_step: 0,
    },
    CommandSpec {
        name: "ECHO",
        arity: 2,
        flags: &["fast", "connection"],
        first_key: 0,
        last_key: 0,
        key_step: 0,
    },
    CommandSpec {
        name: "HELLO",
        arity: -1,
        flags: &["fast", "connection", "no_auth"],
        first_key: 0,
        last_key: 0,
        key_step: 0,
    },
    CommandSpec {
        name: "QUIT",
        arity: 1,
        flags: &["fast", "connection", "no_auth"],
        first_key: 0,
        last_key: 0,
        key_step: 0,
    },
    CommandSpec {
        name: "COMMAND",
        arity: -1,
        flags: &["loading", "stale"],
        first_key: 0,
        last_key: 0,
        key_step: 0,
    },
    CommandSpec {
        name: "SET",
        arity: -3,
        flags: &["write", "denyoom"],
        first_key: 1,
        last_key: 1,
        key_step: 1,
    },
    CommandSpec {
        name: "SETEX",
        arity: 4,
        flags: &["write", "denyoom"],
        first_key: 1,
        last_key: 1,
        key_step: 1,
    },
    CommandSpec {
        name: "PSETEX",
        arity: 4,
        flags: &["write", "denyoom"],
        first_key: 1,
        last_key: 1,
        key_step: 1,
    },
    CommandSpec {
        name: "GET",
        arity: 2,
        flags: &["readonly", "fast"],
        first_key: 1,
        last_key: 1,
        key_step: 1,
    },
    CommandSpec {
        name: "GETDEL",
        arity: 2,
        flags: &["write", "fast"],
        first_key: 1,
        last_key: 1,
        key_step: 1,
    },
    CommandSpec {
        name: "GETEX",
        arity: -2,
        flags: &["write", "fast"],
        first_key: 1,
        last_key: 1,
        key_step: 1,
    },
    CommandSpec {
        name: "DEL",
        arity: -2,
        flags: &["write"],
        first_key: 1,
        last_key: -1,
        key_step: 1,
    },
    CommandSpec {
        name: "EXISTS",
        arity: -2,
        flags: &["readonly", "fast"],
        first_key: 1,
        last_key: -1,
        key_step: 1,
    },
    CommandSpec {
        name: "SELECT",
        arity: 2,
        flags: &["fast", "connection"],
        first_key: 0,
        last_key: 0,
        key_step: 0,
    },
    CommandSpec {
        name: "EXPIRE",
        arity: -3,
        flags: &["write", "fast"],
        first_key: 1,
        last_key: 1,
        key_step: 1,
    },
    CommandSpec {
        name: "PEXPIRE",
        arity: -3,
        flags: &["write", "fast"],
        first_key: 1,
        last_key: 1,
        key_step: 1,
    },
    CommandSpec {
        name: "EXPIREAT",
        arity: -3,
        flags: &["write", "fast"],
        first_key: 1,
        last_key: 1,
        key_step: 1,
    },
    CommandSpec {
        name: "PEXPIREAT",
        arity: -3,
        flags: &["write", "fast"],
        first_key: 1,
        last_key: 1,
        key_step: 1,
    },
    CommandSpec {
        name: "PERSIST",
        arity: 2,
        flags: &["write", "fast"],
        first_key: 1,
        last_key: 1,
        key_step: 1,
    },
    CommandSpec {
        name: "TTL",
        arity: 2,
        flags: &["readonly", "fast"],
        first_key: 1,
        last_key: 1,
        key_step: 1,
    },
    CommandSpec {
        name: "PTTL",
        arity: 2,
        flags: &["readonly", "fast"],
        first_key: 1,
        last_key: 1,
        key_step: 1,
    },
    CommandSpec {
        name: "EXPIRETIME",
        arity: 2,
        flags: &["readonly", "fast"],
        first_key: 1,
        last_key: 1,
        key_step: 1,
    },
    CommandSpec {
        name: "PEXPIRETIME",
        arity: 2,
        flags: &["readonly", "fast"],
        first_key: 1,
        last_key: 1,
        key_step: 1,
    },
];

const EXTRA_COMMAND_SPECS: &[CommandSpec] = &[
    CommandSpec {
        name: "AUTH",
        arity: -2,
        flags: &["fast", "connection", "no_auth"],
        first_key: 0,
        last_key: 0,
        key_step: 0,
    },
    CommandSpec {
        name: "CLIENT",
        arity: -2,
        flags: &["connection", "noscript"],
        first_key: 0,
        last_key: 0,
        key_step: 0,
    },
    CommandSpec {
        name: "CLIENT INFO",
        arity: 2,
        flags: &["connection", "readonly", "noscript"],
        first_key: 0,
        last_key: 0,
        key_step: 0,
    },
    CommandSpec {
        name: "CLIENT LIST",
        arity: -2,
        flags: &["connection", "readonly", "noscript"],
        first_key: 0,
        last_key: 0,
        key_step: 0,
    },
    CommandSpec {
        name: "CLIENT KILL",
        arity: -3,
        flags: &["admin", "connection", "noscript"],
        first_key: 0,
        last_key: 0,
        key_step: 0,
    },
    CommandSpec {
        name: "CLIENT PAUSE",
        arity: -3,
        flags: &["admin", "connection", "noscript"],
        first_key: 0,
        last_key: 0,
        key_step: 0,
    },
    CommandSpec {
        name: "CLIENT UNPAUSE",
        arity: 2,
        flags: &["admin", "connection", "noscript"],
        first_key: 0,
        last_key: 0,
        key_step: 0,
    },
    CommandSpec {
        name: "CLIENT UNBLOCK",
        arity: -3,
        flags: &["admin", "connection", "noscript"],
        first_key: 0,
        last_key: 0,
        key_step: 0,
    },
    CommandSpec {
        name: "ACL",
        arity: -2,
        flags: &["admin", "noscript"],
        first_key: 0,
        last_key: 0,
        key_step: 0,
    },
    CommandSpec {
        name: "ACL HELP",
        arity: 2,
        flags: &["admin", "readonly", "noscript"],
        first_key: 0,
        last_key: 0,
        key_step: 0,
    },
    CommandSpec {
        name: "ACL CAT",
        arity: -2,
        flags: &["admin", "readonly", "noscript"],
        first_key: 0,
        last_key: 0,
        key_step: 0,
    },
    CommandSpec {
        name: "ACL DELUSER",
        arity: -3,
        flags: &["admin", "noscript"],
        first_key: 0,
        last_key: 0,
        key_step: 0,
    },
    CommandSpec {
        name: "ACL DRYRUN",
        arity: -4,
        flags: &["admin", "readonly", "noscript"],
        first_key: 0,
        last_key: 0,
        key_step: 0,
    },
    CommandSpec {
        name: "ACL GENPASS",
        arity: -2,
        flags: &["admin", "readonly", "noscript"],
        first_key: 0,
        last_key: 0,
        key_step: 0,
    },
    CommandSpec {
        name: "ACL GETUSER",
        arity: 3,
        flags: &["admin", "readonly", "noscript"],
        first_key: 0,
        last_key: 0,
        key_step: 0,
    },
    CommandSpec {
        name: "ACL LIST",
        arity: 2,
        flags: &["admin", "readonly", "noscript"],
        first_key: 0,
        last_key: 0,
        key_step: 0,
    },
    CommandSpec {
        name: "ACL LOAD",
        arity: 2,
        flags: &["admin", "noscript"],
        first_key: 0,
        last_key: 0,
        key_step: 0,
    },
    CommandSpec {
        name: "ACL LOG",
        arity: -2,
        flags: &["admin", "noscript"],
        first_key: 0,
        last_key: 0,
        key_step: 0,
    },
    CommandSpec {
        name: "ACL SAVE",
        arity: 2,
        flags: &["admin", "noscript"],
        first_key: 0,
        last_key: 0,
        key_step: 0,
    },
    CommandSpec {
        name: "ACL SETUSER",
        arity: -3,
        flags: &["admin", "noscript"],
        first_key: 0,
        last_key: 0,
        key_step: 0,
    },
    CommandSpec {
        name: "ACL USERS",
        arity: 2,
        flags: &["admin", "readonly", "noscript"],
        first_key: 0,
        last_key: 0,
        key_step: 0,
    },
    CommandSpec {
        name: "ACL WHOAMI",
        arity: 2,
        flags: &["admin", "readonly", "noscript"],
        first_key: 0,
        last_key: 0,
        key_step: 0,
    },
    CommandSpec {
        name: "CLIENT CACHING",
        arity: 3,
        flags: &["connection", "noscript"],
        first_key: 0,
        last_key: 0,
        key_step: 0,
    },
    CommandSpec {
        name: "CLIENT GETREDIR",
        arity: 2,
        flags: &["connection", "readonly", "noscript"],
        first_key: 0,
        last_key: 0,
        key_step: 0,
    },
    CommandSpec {
        name: "CLIENT NO-EVICT",
        arity: 3,
        flags: &["connection", "noscript"],
        first_key: 0,
        last_key: 0,
        key_step: 0,
    },
    CommandSpec {
        name: "CLIENT NO-TOUCH",
        arity: 3,
        flags: &["connection", "noscript"],
        first_key: 0,
        last_key: 0,
        key_step: 0,
    },
    CommandSpec {
        name: "CLIENT REPLY",
        arity: 3,
        flags: &["connection", "noscript"],
        first_key: 0,
        last_key: 0,
        key_step: 0,
    },
    CommandSpec {
        name: "CLIENT SETINFO",
        arity: 4,
        flags: &["connection", "noscript"],
        first_key: 0,
        last_key: 0,
        key_step: 0,
    },
    CommandSpec {
        name: "CLIENT TRACKING",
        arity: -3,
        flags: &["connection", "noscript"],
        first_key: 0,
        last_key: 0,
        key_step: 0,
    },
    CommandSpec {
        name: "CLIENT TRACKINGINFO",
        arity: 2,
        flags: &["connection", "readonly", "noscript"],
        first_key: 0,
        last_key: 0,
        key_step: 0,
    },
    CommandSpec {
        name: "RESET",
        arity: 1,
        flags: &["fast", "connection", "no_auth"],
        first_key: 0,
        last_key: 0,
        key_step: 0,
    },
    CommandSpec {
        name: "DBSIZE",
        arity: 1,
        flags: &["readonly", "fast"],
        first_key: 0,
        last_key: 0,
        key_step: 0,
    },
    CommandSpec {
        name: "TIME",
        arity: 1,
        flags: &["fast"],
        first_key: 0,
        last_key: 0,
        key_step: 0,
    },
    CommandSpec {
        name: "INFO",
        arity: -1,
        flags: &["readonly"],
        first_key: 0,
        last_key: 0,
        key_step: 0,
    },
    CommandSpec {
        name: "MONITOR",
        arity: 1,
        flags: &["admin", "noscript"],
        first_key: 0,
        last_key: 0,
        key_step: 0,
    },
    CommandSpec {
        name: "ROLE",
        arity: 1,
        flags: &["admin", "noscript"],
        first_key: 0,
        last_key: 0,
        key_step: 0,
    },
    CommandSpec {
        name: "REPLCONF",
        arity: -3,
        flags: &["admin", "noscript"],
        first_key: 0,
        last_key: 0,
        key_step: 0,
    },
    CommandSpec {
        name: "SYNC",
        arity: 1,
        flags: &["admin", "noscript"],
        first_key: 0,
        last_key: 0,
        key_step: 0,
    },
    CommandSpec {
        name: "PSYNC",
        arity: 3,
        flags: &["admin", "noscript"],
        first_key: 0,
        last_key: 0,
        key_step: 0,
    },
    CommandSpec {
        name: "REPLICAOF",
        arity: 3,
        flags: &["admin", "noscript"],
        first_key: 0,
        last_key: 0,
        key_step: 0,
    },
    CommandSpec {
        name: "SLAVEOF",
        arity: 3,
        flags: &["admin", "noscript"],
        first_key: 0,
        last_key: 0,
        key_step: 0,
    },
    CommandSpec {
        name: "RESTORE-ASKING",
        arity: -4,
        flags: &["write", "denyoom"],
        first_key: 1,
        last_key: 1,
        key_step: 1,
    },
    CommandSpec {
        name: "CONFIG",
        arity: -2,
        flags: &["admin", "noscript"],
        first_key: 0,
        last_key: 0,
        key_step: 0,
    },
    CommandSpec {
        name: "CONFIG GET",
        arity: -3,
        flags: &["admin", "readonly", "noscript"],
        first_key: 0,
        last_key: 0,
        key_step: 0,
    },
    CommandSpec {
        name: "CONFIG SET",
        arity: -4,
        flags: &["admin", "noscript"],
        first_key: 0,
        last_key: 0,
        key_step: 0,
    },
    CommandSpec {
        name: "CONFIG HELP",
        arity: 2,
        flags: &["admin", "readonly", "noscript"],
        first_key: 0,
        last_key: 0,
        key_step: 0,
    },
    CommandSpec {
        name: "CONFIG REWRITE",
        arity: 2,
        flags: &["admin", "noscript"],
        first_key: 0,
        last_key: 0,
        key_step: 0,
    },
    CommandSpec {
        name: "LATENCY",
        arity: -2,
        flags: &["admin", "noscript"],
        first_key: 0,
        last_key: 0,
        key_step: 0,
    },
    CommandSpec {
        name: "LATENCY HELP",
        arity: 2,
        flags: &["admin", "readonly", "noscript"],
        first_key: 0,
        last_key: 0,
        key_step: 0,
    },
    CommandSpec {
        name: "LATENCY LATEST",
        arity: 2,
        flags: &["admin", "readonly", "noscript"],
        first_key: 0,
        last_key: 0,
        key_step: 0,
    },
    CommandSpec {
        name: "LATENCY HISTORY",
        arity: 3,
        flags: &["admin", "readonly", "noscript"],
        first_key: 0,
        last_key: 0,
        key_step: 0,
    },
    CommandSpec {
        name: "LATENCY RESET",
        arity: -2,
        flags: &["admin", "noscript"],
        first_key: 0,
        last_key: 0,
        key_step: 0,
    },
    CommandSpec {
        name: "LATENCY DOCTOR",
        arity: 2,
        flags: &["admin", "readonly", "noscript"],
        first_key: 0,
        last_key: 0,
        key_step: 0,
    },
    CommandSpec {
        name: "LATENCY GRAPH",
        arity: 3,
        flags: &["admin", "readonly", "noscript"],
        first_key: 0,
        last_key: 0,
        key_step: 0,
    },
    CommandSpec {
        name: "LATENCY HISTOGRAM",
        arity: -2,
        flags: &["admin", "readonly", "noscript"],
        first_key: 0,
        last_key: 0,
        key_step: 0,
    },
    CommandSpec {
        name: "SLOWLOG",
        arity: -2,
        flags: &["admin", "noscript"],
        first_key: 0,
        last_key: 0,
        key_step: 0,
    },
    CommandSpec {
        name: "SLOWLOG GET",
        arity: -2,
        flags: &["admin", "readonly", "noscript"],
        first_key: 0,
        last_key: 0,
        key_step: 0,
    },
    CommandSpec {
        name: "SLOWLOG LEN",
        arity: 2,
        flags: &["admin", "readonly", "noscript"],
        first_key: 0,
        last_key: 0,
        key_step: 0,
    },
    CommandSpec {
        name: "SLOWLOG RESET",
        arity: 2,
        flags: &["admin", "noscript"],
        first_key: 0,
        last_key: 0,
        key_step: 0,
    },
    CommandSpec {
        name: "SLOWLOG HELP",
        arity: 2,
        flags: &["admin", "readonly", "noscript"],
        first_key: 0,
        last_key: 0,
        key_step: 0,
    },
    CommandSpec {
        name: "MEMORY",
        arity: -2,
        flags: &["readonly"],
        first_key: 0,
        last_key: 0,
        key_step: 0,
    },
    CommandSpec {
        name: "MEMORY USAGE",
        arity: -3,
        flags: &["readonly"],
        first_key: 1,
        last_key: 1,
        key_step: 1,
    },
    CommandSpec {
        name: "MEMORY HELP",
        arity: 2,
        flags: &["readonly"],
        first_key: 0,
        last_key: 0,
        key_step: 0,
    },
    CommandSpec {
        name: "MEMORY STATS",
        arity: 2,
        flags: &["readonly"],
        first_key: 0,
        last_key: 0,
        key_step: 0,
    },
    CommandSpec {
        name: "MEMORY DOCTOR",
        arity: 2,
        flags: &["readonly"],
        first_key: 0,
        last_key: 0,
        key_step: 0,
    },
    CommandSpec {
        name: "MEMORY MALLOC-STATS",
        arity: 2,
        flags: &["readonly"],
        first_key: 0,
        last_key: 0,
        key_step: 0,
    },
    CommandSpec {
        name: "MEMORY PURGE",
        arity: 2,
        flags: &["readonly"],
        first_key: 0,
        last_key: 0,
        key_step: 0,
    },
    CommandSpec {
        name: "LASTSAVE",
        arity: 1,
        flags: &["readonly", "fast"],
        first_key: 0,
        last_key: 0,
        key_step: 0,
    },
    CommandSpec {
        name: "SAVE",
        arity: 1,
        flags: &["admin", "noscript"],
        first_key: 0,
        last_key: 0,
        key_step: 0,
    },
    CommandSpec {
        name: "BGSAVE",
        arity: -1,
        flags: &["admin", "noscript"],
        first_key: 0,
        last_key: 0,
        key_step: 0,
    },
    CommandSpec {
        name: "FLUSHDB",
        arity: -1,
        flags: &["write"],
        first_key: 0,
        last_key: 0,
        key_step: 0,
    },
    CommandSpec {
        name: "FLUSHALL",
        arity: -1,
        flags: &["write"],
        first_key: 0,
        last_key: 0,
        key_step: 0,
    },
    CommandSpec {
        name: "RANDOMKEY",
        arity: 1,
        flags: &["readonly"],
        first_key: 0,
        last_key: 0,
        key_step: 0,
    },
    CommandSpec {
        name: "TYPE",
        arity: 2,
        flags: &["readonly", "fast"],
        first_key: 1,
        last_key: 1,
        key_step: 1,
    },
    CommandSpec {
        name: "KEYS",
        arity: 2,
        flags: &["readonly"],
        first_key: 0,
        last_key: 0,
        key_step: 0,
    },
    CommandSpec {
        name: "WAIT",
        arity: 3,
        flags: &["readonly", "noscript", "fast"],
        first_key: 0,
        last_key: 0,
        key_step: 0,
    },
    CommandSpec {
        name: "WAITAOF",
        arity: 4,
        flags: &["readonly", "noscript", "fast"],
        first_key: 0,
        last_key: 0,
        key_step: 0,
    },
    CommandSpec {
        name: "DELEX",
        arity: -2,
        flags: &["write", "fast"],
        first_key: 1,
        last_key: 1,
        key_step: 1,
    },
    CommandSpec {
        name: "DIGEST",
        arity: 2,
        flags: &["readonly", "fast"],
        first_key: 1,
        last_key: 1,
        key_step: 1,
    },
    CommandSpec {
        name: "DUMP",
        arity: 2,
        flags: &["readonly"],
        first_key: 1,
        last_key: 1,
        key_step: 1,
    },
    CommandSpec {
        name: "RESTORE",
        arity: -4,
        flags: &["write", "denyoom"],
        first_key: 1,
        last_key: 1,
        key_step: 1,
    },
    CommandSpec {
        name: "MIGRATE",
        arity: -6,
        flags: &["write"],
        first_key: 3,
        last_key: 3,
        key_step: 1,
    },
    CommandSpec {
        name: "LCS",
        arity: -3,
        flags: &["readonly"],
        first_key: 1,
        last_key: 2,
        key_step: 1,
    },
    CommandSpec {
        name: "MSETEX",
        arity: -4,
        flags: &["write", "denyoom"],
        first_key: 2,
        last_key: -1,
        key_step: 2,
    },
    CommandSpec {
        name: "COPY",
        arity: -3,
        flags: &["write", "denyoom"],
        first_key: 1,
        last_key: 2,
        key_step: 1,
    },
    CommandSpec {
        name: "MOVE",
        arity: 3,
        flags: &["write", "fast"],
        first_key: 1,
        last_key: 1,
        key_step: 1,
    },
    CommandSpec {
        name: "RENAME",
        arity: 3,
        flags: &["write"],
        first_key: 1,
        last_key: 2,
        key_step: 1,
    },
    CommandSpec {
        name: "RENAMENX",
        arity: 3,
        flags: &["write", "fast"],
        first_key: 1,
        last_key: 2,
        key_step: 1,
    },
    CommandSpec {
        name: "TOUCH",
        arity: -2,
        flags: &["readonly", "fast"],
        first_key: 1,
        last_key: -1,
        key_step: 1,
    },
    CommandSpec {
        name: "UNLINK",
        arity: -2,
        flags: &["write", "fast"],
        first_key: 1,
        last_key: -1,
        key_step: 1,
    },
    CommandSpec {
        name: "SCAN",
        arity: -2,
        flags: &["readonly"],
        first_key: 0,
        last_key: 0,
        key_step: 0,
    },
    CommandSpec {
        name: "OBJECT",
        arity: -2,
        flags: &["readonly"],
        first_key: 0,
        last_key: 0,
        key_step: 0,
    },
    CommandSpec {
        name: "OBJECT HELP",
        arity: 2,
        flags: &["readonly"],
        first_key: 0,
        last_key: 0,
        key_step: 0,
    },
    CommandSpec {
        name: "OBJECT ENCODING",
        arity: 3,
        flags: &["readonly"],
        first_key: 2,
        last_key: 2,
        key_step: 1,
    },
    CommandSpec {
        name: "OBJECT REFCOUNT",
        arity: 3,
        flags: &["readonly"],
        first_key: 2,
        last_key: 2,
        key_step: 1,
    },
    CommandSpec {
        name: "OBJECT IDLETIME",
        arity: 3,
        flags: &["readonly"],
        first_key: 2,
        last_key: 2,
        key_step: 1,
    },
    CommandSpec {
        name: "OBJECT FREQ",
        arity: 3,
        flags: &["readonly"],
        first_key: 2,
        last_key: 2,
        key_step: 1,
    },
    CommandSpec {
        name: "SORT",
        arity: -2,
        flags: &["write", "denyoom"],
        first_key: 1,
        last_key: 1,
        key_step: 1,
    },
    CommandSpec {
        name: "SORT_RO",
        arity: -2,
        flags: &["readonly"],
        first_key: 1,
        last_key: 1,
        key_step: 1,
    },
    CommandSpec {
        name: "MULTI",
        arity: 1,
        flags: &["fast", "noscript"],
        first_key: 0,
        last_key: 0,
        key_step: 0,
    },
    CommandSpec {
        name: "EXEC",
        arity: 1,
        flags: &["noscript"],
        first_key: 0,
        last_key: 0,
        key_step: 0,
    },
    CommandSpec {
        name: "DISCARD",
        arity: 1,
        flags: &["fast", "noscript"],
        first_key: 0,
        last_key: 0,
        key_step: 0,
    },
    CommandSpec {
        name: "WATCH",
        arity: -2,
        flags: &["readonly", "noscript"],
        first_key: 1,
        last_key: -1,
        key_step: 1,
    },
    CommandSpec {
        name: "UNWATCH",
        arity: 1,
        flags: &["fast", "noscript"],
        first_key: 0,
        last_key: 0,
        key_step: 0,
    },
    CommandSpec {
        name: "SUBSCRIBE",
        arity: -2,
        flags: &["pubsub", "noscript"],
        first_key: 0,
        last_key: 0,
        key_step: 0,
    },
    CommandSpec {
        name: "SSUBSCRIBE",
        arity: -2,
        flags: &["pubsub", "noscript"],
        first_key: 0,
        last_key: 0,
        key_step: 0,
    },
    CommandSpec {
        name: "PSUBSCRIBE",
        arity: -2,
        flags: &["pubsub", "noscript"],
        first_key: 0,
        last_key: 0,
        key_step: 0,
    },
    CommandSpec {
        name: "PUBLISH",
        arity: 3,
        flags: &["pubsub", "loading", "stale", "fast"],
        first_key: 0,
        last_key: 0,
        key_step: 0,
    },
    CommandSpec {
        name: "SPUBLISH",
        arity: 3,
        flags: &["pubsub", "loading", "stale", "fast"],
        first_key: 0,
        last_key: 0,
        key_step: 0,
    },
    CommandSpec {
        name: "UNSUBSCRIBE",
        arity: -1,
        flags: &["pubsub", "noscript"],
        first_key: 0,
        last_key: 0,
        key_step: 0,
    },
    CommandSpec {
        name: "PUNSUBSCRIBE",
        arity: -1,
        flags: &["pubsub", "noscript"],
        first_key: 0,
        last_key: 0,
        key_step: 0,
    },
    CommandSpec {
        name: "SUNSUBSCRIBE",
        arity: -1,
        flags: &["pubsub", "noscript"],
        first_key: 0,
        last_key: 0,
        key_step: 0,
    },
    CommandSpec {
        name: "PUBSUB",
        arity: -2,
        flags: &["pubsub", "noscript"],
        first_key: 0,
        last_key: 0,
        key_step: 0,
    },
    CommandSpec {
        name: "PUBSUB CHANNELS",
        arity: -2,
        flags: &["pubsub", "readonly", "noscript"],
        first_key: 0,
        last_key: 0,
        key_step: 0,
    },
    CommandSpec {
        name: "PUBSUB NUMSUB",
        arity: -3,
        flags: &["pubsub", "readonly", "noscript"],
        first_key: 0,
        last_key: 0,
        key_step: 0,
    },
    CommandSpec {
        name: "PUBSUB NUMPAT",
        arity: 2,
        flags: &["pubsub", "readonly", "noscript"],
        first_key: 0,
        last_key: 0,
        key_step: 0,
    },
    CommandSpec {
        name: "PUBSUB HELP",
        arity: 2,
        flags: &["pubsub", "readonly", "noscript"],
        first_key: 0,
        last_key: 0,
        key_step: 0,
    },
    CommandSpec {
        name: "PUBSUB SHARDCHANNELS",
        arity: -2,
        flags: &["pubsub", "readonly", "noscript"],
        first_key: 0,
        last_key: 0,
        key_step: 0,
    },
    CommandSpec {
        name: "PUBSUB SHARDNUMSUB",
        arity: -2,
        flags: &["pubsub", "readonly", "noscript"],
        first_key: 0,
        last_key: 0,
        key_step: 0,
    },
    CommandSpec {
        name: "XADD",
        arity: -5,
        flags: &["write", "denyoom", "fast"],
        first_key: 1,
        last_key: 1,
        key_step: 1,
    },
    CommandSpec {
        name: "XLEN",
        arity: 2,
        flags: &["readonly", "fast"],
        first_key: 1,
        last_key: 1,
        key_step: 1,
    },
    CommandSpec {
        name: "XRANGE",
        arity: -4,
        flags: &["readonly"],
        first_key: 1,
        last_key: 1,
        key_step: 1,
    },
    CommandSpec {
        name: "XREVRANGE",
        arity: -4,
        flags: &["readonly"],
        first_key: 1,
        last_key: 1,
        key_step: 1,
    },
    CommandSpec {
        name: "XREAD",
        arity: -4,
        flags: &["readonly", "blocking"],
        first_key: 0,
        last_key: 0,
        key_step: 0,
    },
    CommandSpec {
        name: "XGROUP",
        arity: -2,
        flags: &["write", "slow"],
        first_key: 2,
        last_key: 2,
        key_step: 1,
    },
    CommandSpec {
        name: "XGROUP HELP",
        arity: 2,
        flags: &["readonly", "slow"],
        first_key: 0,
        last_key: 0,
        key_step: 0,
    },
    CommandSpec {
        name: "XGROUP CREATE",
        arity: -5,
        flags: &["write", "slow"],
        first_key: 2,
        last_key: 2,
        key_step: 1,
    },
    CommandSpec {
        name: "XGROUP DESTROY",
        arity: 4,
        flags: &["write", "fast"],
        first_key: 2,
        last_key: 2,
        key_step: 1,
    },
    CommandSpec {
        name: "XGROUP SETID",
        arity: 5,
        flags: &["write", "fast"],
        first_key: 2,
        last_key: 2,
        key_step: 1,
    },
    CommandSpec {
        name: "XGROUP CREATECONSUMER",
        arity: 5,
        flags: &["write", "fast"],
        first_key: 2,
        last_key: 2,
        key_step: 1,
    },
    CommandSpec {
        name: "XGROUP DELCONSUMER",
        arity: 5,
        flags: &["write", "fast"],
        first_key: 2,
        last_key: 2,
        key_step: 1,
    },
    CommandSpec {
        name: "XREADGROUP",
        arity: -7,
        flags: &["write", "blocking"],
        first_key: 0,
        last_key: 0,
        key_step: 0,
    },
    CommandSpec {
        name: "XACK",
        arity: -4,
        flags: &["write", "fast"],
        first_key: 1,
        last_key: 1,
        key_step: 1,
    },
    CommandSpec {
        name: "XPENDING",
        arity: -3,
        flags: &["readonly", "slow"],
        first_key: 1,
        last_key: 1,
        key_step: 1,
    },
    CommandSpec {
        name: "XINFO",
        arity: -2,
        flags: &["readonly", "slow"],
        first_key: 0,
        last_key: 0,
        key_step: 0,
    },
    CommandSpec {
        name: "XINFO HELP",
        arity: 2,
        flags: &["readonly", "slow"],
        first_key: 0,
        last_key: 0,
        key_step: 0,
    },
    CommandSpec {
        name: "XINFO STREAM",
        arity: -3,
        flags: &["readonly", "slow"],
        first_key: 2,
        last_key: 2,
        key_step: 1,
    },
    CommandSpec {
        name: "XINFO GROUPS",
        arity: 3,
        flags: &["readonly", "slow"],
        first_key: 2,
        last_key: 2,
        key_step: 1,
    },
    CommandSpec {
        name: "XINFO CONSUMERS",
        arity: 4,
        flags: &["readonly", "slow"],
        first_key: 2,
        last_key: 2,
        key_step: 1,
    },
    CommandSpec {
        name: "XTRIM",
        arity: -4,
        flags: &["write", "fast"],
        first_key: 1,
        last_key: 1,
        key_step: 1,
    },
    CommandSpec {
        name: "XDEL",
        arity: -3,
        flags: &["write", "fast"],
        first_key: 1,
        last_key: 1,
        key_step: 1,
    },
    CommandSpec {
        name: "XSETID",
        arity: 3,
        flags: &["write", "fast"],
        first_key: 1,
        last_key: 1,
        key_step: 1,
    },
    CommandSpec {
        name: "XCLAIM",
        arity: -6,
        flags: &["write", "fast"],
        first_key: 1,
        last_key: 1,
        key_step: 1,
    },
    CommandSpec {
        name: "XAUTOCLAIM",
        arity: -6,
        flags: &["write", "slow"],
        first_key: 1,
        last_key: 1,
        key_step: 1,
    },
    CommandSpec {
        name: "XACKDEL",
        arity: -6,
        flags: &["write", "fast"],
        first_key: 1,
        last_key: 1,
        key_step: 1,
    },
    CommandSpec {
        name: "XCFGSET",
        arity: -2,
        flags: &["write", "fast"],
        first_key: 1,
        last_key: 1,
        key_step: 1,
    },
    CommandSpec {
        name: "XDELEX",
        arity: -5,
        flags: &["write", "fast"],
        first_key: 1,
        last_key: 1,
        key_step: 1,
    },
    CommandSpec {
        name: "HSET",
        arity: -4,
        flags: &["write", "denyoom", "fast"],
        first_key: 1,
        last_key: 1,
        key_step: 1,
    },
    CommandSpec {
        name: "HGET",
        arity: 3,
        flags: &["readonly", "fast"],
        first_key: 1,
        last_key: 1,
        key_step: 1,
    },
    CommandSpec {
        name: "HMGET",
        arity: -3,
        flags: &["readonly", "fast"],
        first_key: 1,
        last_key: 1,
        key_step: 1,
    },
    CommandSpec {
        name: "HGETALL",
        arity: 2,
        flags: &["readonly", "fast"],
        first_key: 1,
        last_key: 1,
        key_step: 1,
    },
    CommandSpec {
        name: "HKEYS",
        arity: 2,
        flags: &["readonly", "fast"],
        first_key: 1,
        last_key: 1,
        key_step: 1,
    },
    CommandSpec {
        name: "HVALS",
        arity: 2,
        flags: &["readonly", "fast"],
        first_key: 1,
        last_key: 1,
        key_step: 1,
    },
    CommandSpec {
        name: "HINCRBY",
        arity: 4,
        flags: &["write", "denyoom", "fast"],
        first_key: 1,
        last_key: 1,
        key_step: 1,
    },
    CommandSpec {
        name: "HINCRBYFLOAT",
        arity: 4,
        flags: &["write", "denyoom", "fast"],
        first_key: 1,
        last_key: 1,
        key_step: 1,
    },
    CommandSpec {
        name: "HMSET",
        arity: -4,
        flags: &["write", "denyoom", "fast"],
        first_key: 1,
        last_key: 1,
        key_step: 1,
    },
    CommandSpec {
        name: "HSETNX",
        arity: 4,
        flags: &["write", "denyoom", "fast"],
        first_key: 1,
        last_key: 1,
        key_step: 1,
    },
    CommandSpec {
        name: "HSTRLEN",
        arity: 3,
        flags: &["readonly", "fast"],
        first_key: 1,
        last_key: 1,
        key_step: 1,
    },
    CommandSpec {
        name: "HRANDFIELD",
        arity: -2,
        flags: &["readonly", "random"],
        first_key: 1,
        last_key: 1,
        key_step: 1,
    },
    CommandSpec {
        name: "HDEL",
        arity: -3,
        flags: &["write", "fast"],
        first_key: 1,
        last_key: 1,
        key_step: 1,
    },
    CommandSpec {
        name: "HEXISTS",
        arity: 3,
        flags: &["readonly", "fast"],
        first_key: 1,
        last_key: 1,
        key_step: 1,
    },
    CommandSpec {
        name: "HLEN",
        arity: 2,
        flags: &["readonly", "fast"],
        first_key: 1,
        last_key: 1,
        key_step: 1,
    },
    CommandSpec {
        name: "HSCAN",
        arity: -3,
        flags: &["readonly"],
        first_key: 1,
        last_key: 1,
        key_step: 1,
    },
    CommandSpec {
        name: "LPUSH",
        arity: -3,
        flags: &["write", "denyoom", "fast"],
        first_key: 1,
        last_key: 1,
        key_step: 1,
    },
    CommandSpec {
        name: "LPUSHX",
        arity: -3,
        flags: &["write", "denyoom", "fast"],
        first_key: 1,
        last_key: 1,
        key_step: 1,
    },
    CommandSpec {
        name: "RPUSH",
        arity: -3,
        flags: &["write", "denyoom", "fast"],
        first_key: 1,
        last_key: 1,
        key_step: 1,
    },
    CommandSpec {
        name: "RPUSHX",
        arity: -3,
        flags: &["write", "denyoom", "fast"],
        first_key: 1,
        last_key: 1,
        key_step: 1,
    },
    CommandSpec {
        name: "LPOP",
        arity: -2,
        flags: &["write", "fast"],
        first_key: 1,
        last_key: 1,
        key_step: 1,
    },
    CommandSpec {
        name: "RPOP",
        arity: -2,
        flags: &["write", "fast"],
        first_key: 1,
        last_key: 1,
        key_step: 1,
    },
    CommandSpec {
        name: "BLPOP",
        arity: -3,
        flags: &["write"],
        first_key: 1,
        last_key: -2,
        key_step: 1,
    },
    CommandSpec {
        name: "BRPOP",
        arity: -3,
        flags: &["write"],
        first_key: 1,
        last_key: -2,
        key_step: 1,
    },
    CommandSpec {
        name: "LMOVE",
        arity: 5,
        flags: &["write"],
        first_key: 1,
        last_key: 2,
        key_step: 1,
    },
    CommandSpec {
        name: "BLMOVE",
        arity: 6,
        flags: &["write"],
        first_key: 1,
        last_key: 2,
        key_step: 1,
    },
    CommandSpec {
        name: "RPOPLPUSH",
        arity: 3,
        flags: &["write"],
        first_key: 1,
        last_key: 2,
        key_step: 1,
    },
    CommandSpec {
        name: "BRPOPLPUSH",
        arity: 4,
        flags: &["write"],
        first_key: 1,
        last_key: 2,
        key_step: 1,
    },
    CommandSpec {
        name: "LMPOP",
        arity: -4,
        flags: &["write"],
        first_key: 0,
        last_key: 0,
        key_step: 0,
    },
    CommandSpec {
        name: "BLMPOP",
        arity: -5,
        flags: &["write"],
        first_key: 0,
        last_key: 0,
        key_step: 0,
    },
    CommandSpec {
        name: "LRANGE",
        arity: 4,
        flags: &["readonly"],
        first_key: 1,
        last_key: 1,
        key_step: 1,
    },
    CommandSpec {
        name: "LLEN",
        arity: 2,
        flags: &["readonly", "fast"],
        first_key: 1,
        last_key: 1,
        key_step: 1,
    },
    CommandSpec {
        name: "LREM",
        arity: 4,
        flags: &["write"],
        first_key: 1,
        last_key: 1,
        key_step: 1,
    },
    CommandSpec {
        name: "LPOS",
        arity: -3,
        flags: &["readonly"],
        first_key: 1,
        last_key: 1,
        key_step: 1,
    },
    CommandSpec {
        name: "LSET",
        arity: 4,
        flags: &["write", "denyoom"],
        first_key: 1,
        last_key: 1,
        key_step: 1,
    },
    CommandSpec {
        name: "LTRIM",
        arity: 4,
        flags: &["write"],
        first_key: 1,
        last_key: 1,
        key_step: 1,
    },
    CommandSpec {
        name: "SADD",
        arity: -3,
        flags: &["write", "denyoom", "fast"],
        first_key: 1,
        last_key: 1,
        key_step: 1,
    },
    CommandSpec {
        name: "SREM",
        arity: -3,
        flags: &["write", "fast"],
        first_key: 1,
        last_key: 1,
        key_step: 1,
    },
    CommandSpec {
        name: "SISMEMBER",
        arity: 3,
        flags: &["readonly", "fast"],
        first_key: 1,
        last_key: 1,
        key_step: 1,
    },
    CommandSpec {
        name: "SMISMEMBER",
        arity: -3,
        flags: &["readonly", "fast"],
        first_key: 1,
        last_key: 1,
        key_step: 1,
    },
    CommandSpec {
        name: "SMEMBERS",
        arity: 2,
        flags: &["readonly"],
        first_key: 1,
        last_key: 1,
        key_step: 1,
    },
    CommandSpec {
        name: "SCARD",
        arity: 2,
        flags: &["readonly", "fast"],
        first_key: 1,
        last_key: 1,
        key_step: 1,
    },
    CommandSpec {
        name: "SPOP",
        arity: -2,
        flags: &["write", "fast"],
        first_key: 1,
        last_key: 1,
        key_step: 1,
    },
    CommandSpec {
        name: "SRANDMEMBER",
        arity: -2,
        flags: &["readonly", "random"],
        first_key: 1,
        last_key: 1,
        key_step: 1,
    },
    CommandSpec {
        name: "SSCAN",
        arity: -3,
        flags: &["readonly"],
        first_key: 1,
        last_key: 1,
        key_step: 1,
    },
    CommandSpec {
        name: "SMOVE",
        arity: 4,
        flags: &["write", "fast"],
        first_key: 1,
        last_key: 2,
        key_step: 1,
    },
    CommandSpec {
        name: "SDIFF",
        arity: -2,
        flags: &["readonly"],
        first_key: 1,
        last_key: -1,
        key_step: 1,
    },
    CommandSpec {
        name: "SDIFFSTORE",
        arity: -3,
        flags: &["write", "denyoom"],
        first_key: 1,
        last_key: -1,
        key_step: 1,
    },
    CommandSpec {
        name: "SINTER",
        arity: -2,
        flags: &["readonly"],
        first_key: 1,
        last_key: -1,
        key_step: 1,
    },
    CommandSpec {
        name: "SINTERCARD",
        arity: -3,
        flags: &["readonly"],
        first_key: 2,
        last_key: -1,
        key_step: 1,
    },
    CommandSpec {
        name: "SINTERSTORE",
        arity: -3,
        flags: &["write", "denyoom"],
        first_key: 1,
        last_key: -1,
        key_step: 1,
    },
    CommandSpec {
        name: "SUNION",
        arity: -2,
        flags: &["readonly"],
        first_key: 1,
        last_key: -1,
        key_step: 1,
    },
    CommandSpec {
        name: "SUNIONSTORE",
        arity: -3,
        flags: &["write", "denyoom"],
        first_key: 1,
        last_key: -1,
        key_step: 1,
    },
    CommandSpec {
        name: "APPEND",
        arity: 3,
        flags: &["write", "denyoom", "fast"],
        first_key: 1,
        last_key: 1,
        key_step: 1,
    },
    CommandSpec {
        name: "STRLEN",
        arity: 2,
        flags: &["readonly", "fast"],
        first_key: 1,
        last_key: 1,
        key_step: 1,
    },
    CommandSpec {
        name: "GETRANGE",
        arity: 4,
        flags: &["readonly"],
        first_key: 1,
        last_key: 1,
        key_step: 1,
    },
    CommandSpec {
        name: "SUBSTR",
        arity: 4,
        flags: &["readonly"],
        first_key: 1,
        last_key: 1,
        key_step: 1,
    },
    CommandSpec {
        name: "SETRANGE",
        arity: 4,
        flags: &["write", "denyoom"],
        first_key: 1,
        last_key: 1,
        key_step: 1,
    },
    CommandSpec {
        name: "GETSET",
        arity: 3,
        flags: &["write", "denyoom", "fast"],
        first_key: 1,
        last_key: 1,
        key_step: 1,
    },
    CommandSpec {
        name: "SETNX",
        arity: 3,
        flags: &["write", "denyoom", "fast"],
        first_key: 1,
        last_key: 1,
        key_step: 1,
    },
    CommandSpec {
        name: "MGET",
        arity: -2,
        flags: &["readonly", "fast"],
        first_key: 1,
        last_key: -1,
        key_step: 1,
    },
    CommandSpec {
        name: "MSET",
        arity: -3,
        flags: &["write", "denyoom"],
        first_key: 1,
        last_key: -1,
        key_step: 2,
    },
    CommandSpec {
        name: "MSETNX",
        arity: -3,
        flags: &["write", "denyoom"],
        first_key: 1,
        last_key: -1,
        key_step: 2,
    },
    CommandSpec {
        name: "INCR",
        arity: 2,
        flags: &["write", "denyoom", "fast"],
        first_key: 1,
        last_key: 1,
        key_step: 1,
    },
    CommandSpec {
        name: "INCRBY",
        arity: 3,
        flags: &["write", "denyoom", "fast"],
        first_key: 1,
        last_key: 1,
        key_step: 1,
    },
    CommandSpec {
        name: "DECR",
        arity: 2,
        flags: &["write", "denyoom", "fast"],
        first_key: 1,
        last_key: 1,
        key_step: 1,
    },
    CommandSpec {
        name: "DECRBY",
        arity: 3,
        flags: &["write", "denyoom", "fast"],
        first_key: 1,
        last_key: 1,
        key_step: 1,
    },
    CommandSpec {
        name: "INCRBYFLOAT",
        arity: 3,
        flags: &["write", "denyoom", "fast"],
        first_key: 1,
        last_key: 1,
        key_step: 1,
    },
    CommandSpec {
        name: "COMMAND DOCS",
        arity: -2,
        flags: &["readonly", "loading", "stale"],
        first_key: 0,
        last_key: 0,
        key_step: 0,
    },
    CommandSpec {
        name: "BGREWRITEAOF",
        arity: 1,
        flags: &["admin", "noscript"],
        first_key: 0,
        last_key: 0,
        key_step: 0,
    },
    CommandSpec {
        name: "DEBUG",
        arity: -2,
        flags: &["admin", "noscript"],
        first_key: 0,
        last_key: 0,
        key_step: 0,
    },
    CommandSpec {
        name: "FAILOVER",
        arity: -1,
        flags: &["admin", "noscript"],
        first_key: 0,
        last_key: 0,
        key_step: 0,
    },
    CommandSpec {
        name: "HOTKEYS",
        arity: -1,
        flags: &["admin", "readonly", "noscript"],
        first_key: 0,
        last_key: 0,
        key_step: 0,
    },
    CommandSpec {
        name: "HOTKEYS GET",
        arity: 2,
        flags: &["admin", "readonly", "noscript"],
        first_key: 0,
        last_key: 0,
        key_step: 0,
    },
    CommandSpec {
        name: "HOTKEYS RESET",
        arity: 2,
        flags: &["admin", "noscript"],
        first_key: 0,
        last_key: 0,
        key_step: 0,
    },
    CommandSpec {
        name: "HOTKEYS START",
        arity: 2,
        flags: &["admin", "noscript"],
        first_key: 0,
        last_key: 0,
        key_step: 0,
    },
    CommandSpec {
        name: "HOTKEYS STOP",
        arity: 2,
        flags: &["admin", "noscript"],
        first_key: 0,
        last_key: 0,
        key_step: 0,
    },
    CommandSpec {
        name: "LOLWUT",
        arity: -1,
        flags: &["readonly"],
        first_key: 0,
        last_key: 0,
        key_step: 0,
    },
    CommandSpec {
        name: "MODULE",
        arity: -2,
        flags: &["admin", "noscript"],
        first_key: 0,
        last_key: 0,
        key_step: 0,
    },
    CommandSpec {
        name: "MODULE HELP",
        arity: 2,
        flags: &["admin", "readonly", "noscript"],
        first_key: 0,
        last_key: 0,
        key_step: 0,
    },
    CommandSpec {
        name: "MODULE LIST",
        arity: 2,
        flags: &["admin", "readonly", "noscript"],
        first_key: 0,
        last_key: 0,
        key_step: 0,
    },
    CommandSpec {
        name: "MODULE LOAD",
        arity: -3,
        flags: &["admin", "noscript"],
        first_key: 0,
        last_key: 0,
        key_step: 0,
    },
    CommandSpec {
        name: "MODULE LOADEX",
        arity: -3,
        flags: &["admin", "noscript"],
        first_key: 0,
        last_key: 0,
        key_step: 0,
    },
    CommandSpec {
        name: "MODULE UNLOAD",
        arity: 3,
        flags: &["admin", "noscript"],
        first_key: 0,
        last_key: 0,
        key_step: 0,
    },
    CommandSpec {
        name: "SFLUSH",
        arity: -1,
        flags: &["admin", "noscript"],
        first_key: 0,
        last_key: 0,
        key_step: 0,
    },
    CommandSpec {
        name: "SHUTDOWN",
        arity: -1,
        flags: &["admin", "noscript"],
        first_key: 0,
        last_key: 0,
        key_step: 0,
    },
    CommandSpec {
        name: "SWAPDB",
        arity: 3,
        flags: &["admin", "noscript"],
        first_key: 0,
        last_key: 0,
        key_step: 0,
    },
    CommandSpec {
        name: "TRIMSLOTS",
        arity: 2,
        flags: &["admin", "noscript"],
        first_key: 0,
        last_key: 0,
        key_step: 0,
    },
    CommandSpec {
        name: "PFADD",
        arity: -2,
        flags: &["write", "denyoom", "fast"],
        first_key: 1,
        last_key: 1,
        key_step: 1,
    },
    CommandSpec {
        name: "PFCOUNT",
        arity: -2,
        flags: &["readonly"],
        first_key: 1,
        last_key: -1,
        key_step: 1,
    },
    CommandSpec {
        name: "PFMERGE",
        arity: -2,
        flags: &["write", "denyoom"],
        first_key: 1,
        last_key: -1,
        key_step: 1,
    },
    CommandSpec {
        name: "PFDEBUG",
        arity: 3,
        flags: &["admin"],
        first_key: 2,
        last_key: 2,
        key_step: 1,
    },
    CommandSpec {
        name: "PFSELFTEST",
        arity: 1,
        flags: &["admin"],
        first_key: 0,
        last_key: 0,
        key_step: 0,
    },
    // List residual
    CommandSpec {
        name: "LINDEX",
        arity: 3,
        flags: &["readonly"],
        first_key: 1,
        last_key: 1,
        key_step: 1,
    },
    CommandSpec {
        name: "LINSERT",
        arity: 5,
        flags: &["write", "denyoom"],
        first_key: 1,
        last_key: 1,
        key_step: 1,
    },
    // Sorted set
    CommandSpec {
        name: "ZADD",
        arity: -4,
        flags: &["write", "denyoom", "fast"],
        first_key: 1,
        last_key: 1,
        key_step: 1,
    },
    CommandSpec {
        name: "ZREM",
        arity: -3,
        flags: &["write", "fast"],
        first_key: 1,
        last_key: 1,
        key_step: 1,
    },
    CommandSpec {
        name: "ZSCORE",
        arity: 3,
        flags: &["readonly", "fast"],
        first_key: 1,
        last_key: 1,
        key_step: 1,
    },
    CommandSpec {
        name: "ZCARD",
        arity: 2,
        flags: &["readonly", "fast"],
        first_key: 1,
        last_key: 1,
        key_step: 1,
    },
    CommandSpec {
        name: "ZINCRBY",
        arity: 4,
        flags: &["write", "denyoom", "fast"],
        first_key: 1,
        last_key: 1,
        key_step: 1,
    },
    CommandSpec {
        name: "ZMSCORE",
        arity: -3,
        flags: &["readonly", "fast"],
        first_key: 1,
        last_key: 1,
        key_step: 1,
    },
    CommandSpec {
        name: "ZRANGE",
        arity: -4,
        flags: &["readonly"],
        first_key: 1,
        last_key: 1,
        key_step: 1,
    },
    CommandSpec {
        name: "ZRANGEBYSCORE",
        arity: -4,
        flags: &["readonly"],
        first_key: 1,
        last_key: 1,
        key_step: 1,
    },
    CommandSpec {
        name: "ZREVRANGEBYSCORE",
        arity: -4,
        flags: &["readonly"],
        first_key: 1,
        last_key: 1,
        key_step: 1,
    },
    CommandSpec {
        name: "ZRANGEBYLEX",
        arity: -4,
        flags: &["readonly"],
        first_key: 1,
        last_key: 1,
        key_step: 1,
    },
    CommandSpec {
        name: "ZREVRANGEBYLEX",
        arity: -4,
        flags: &["readonly"],
        first_key: 1,
        last_key: 1,
        key_step: 1,
    },
    CommandSpec {
        name: "ZREVRANGE",
        arity: -4,
        flags: &["readonly"],
        first_key: 1,
        last_key: 1,
        key_step: 1,
    },
    CommandSpec {
        name: "ZRANGESTORE",
        arity: -5,
        flags: &["write", "denyoom"],
        first_key: 1,
        last_key: 2,
        key_step: 1,
    },
    CommandSpec {
        name: "ZCOUNT",
        arity: 4,
        flags: &["readonly", "fast"],
        first_key: 1,
        last_key: 1,
        key_step: 1,
    },
    CommandSpec {
        name: "ZLEXCOUNT",
        arity: 4,
        flags: &["readonly", "fast"],
        first_key: 1,
        last_key: 1,
        key_step: 1,
    },
    CommandSpec {
        name: "ZRANK",
        arity: -3,
        flags: &["readonly", "fast"],
        first_key: 1,
        last_key: 1,
        key_step: 1,
    },
    CommandSpec {
        name: "ZREVRANK",
        arity: -3,
        flags: &["readonly", "fast"],
        first_key: 1,
        last_key: 1,
        key_step: 1,
    },
    CommandSpec {
        name: "ZREMRANGEBYRANK",
        arity: 4,
        flags: &["write"],
        first_key: 1,
        last_key: 1,
        key_step: 1,
    },
    CommandSpec {
        name: "ZREMRANGEBYSCORE",
        arity: 4,
        flags: &["write"],
        first_key: 1,
        last_key: 1,
        key_step: 1,
    },
    CommandSpec {
        name: "ZREMRANGEBYLEX",
        arity: 4,
        flags: &["write"],
        first_key: 1,
        last_key: 1,
        key_step: 1,
    },
    CommandSpec {
        name: "ZUNION",
        arity: -3,
        flags: &["readonly"],
        first_key: 0,
        last_key: 0,
        key_step: 0,
    },
    CommandSpec {
        name: "ZUNIONSTORE",
        arity: -4,
        flags: &["write", "denyoom"],
        first_key: 1,
        last_key: 1,
        key_step: 1,
    },
    CommandSpec {
        name: "ZINTER",
        arity: -3,
        flags: &["readonly"],
        first_key: 0,
        last_key: 0,
        key_step: 0,
    },
    CommandSpec {
        name: "ZINTERSTORE",
        arity: -4,
        flags: &["write", "denyoom"],
        first_key: 1,
        last_key: 1,
        key_step: 1,
    },
    CommandSpec {
        name: "ZINTERCARD",
        arity: -3,
        flags: &["readonly"],
        first_key: 0,
        last_key: 0,
        key_step: 0,
    },
    CommandSpec {
        name: "ZDIFF",
        arity: -3,
        flags: &["readonly"],
        first_key: 0,
        last_key: 0,
        key_step: 0,
    },
    CommandSpec {
        name: "ZDIFFSTORE",
        arity: -4,
        flags: &["write", "denyoom"],
        first_key: 1,
        last_key: 1,
        key_step: 1,
    },
    CommandSpec {
        name: "ZPOPMIN",
        arity: -2,
        flags: &["write", "fast"],
        first_key: 1,
        last_key: 1,
        key_step: 1,
    },
    CommandSpec {
        name: "ZPOPMAX",
        arity: -2,
        flags: &["write", "fast"],
        first_key: 1,
        last_key: 1,
        key_step: 1,
    },
    CommandSpec {
        name: "ZMPOP",
        arity: -4,
        flags: &["write", "fast"],
        first_key: 0,
        last_key: 0,
        key_step: 0,
    },
    CommandSpec {
        name: "ZRANDMEMBER",
        arity: -2,
        flags: &["readonly"],
        first_key: 1,
        last_key: 1,
        key_step: 1,
    },
    CommandSpec {
        name: "ZSCAN",
        arity: -3,
        flags: &["readonly"],
        first_key: 1,
        last_key: 1,
        key_step: 1,
    },
    CommandSpec {
        name: "BZPOPMIN",
        arity: -3,
        flags: &["write", "fast"],
        first_key: 1,
        last_key: -2,
        key_step: 1,
    },
    CommandSpec {
        name: "BZPOPMAX",
        arity: -3,
        flags: &["write", "fast"],
        first_key: 1,
        last_key: -2,
        key_step: 1,
    },
    CommandSpec {
        name: "BZMPOP",
        arity: -5,
        flags: &["write", "fast"],
        first_key: 0,
        last_key: 0,
        key_step: 0,
    },
    // Geo
    CommandSpec {
        name: "GEOADD",
        arity: -5,
        flags: &["write", "denyoom"],
        first_key: 1,
        last_key: 1,
        key_step: 1,
    },
    CommandSpec {
        name: "GEOPOS",
        arity: -2,
        flags: &["readonly"],
        first_key: 1,
        last_key: 1,
        key_step: 1,
    },
    CommandSpec {
        name: "GEODIST",
        arity: -4,
        flags: &["readonly"],
        first_key: 1,
        last_key: 1,
        key_step: 1,
    },
    CommandSpec {
        name: "GEOHASH",
        arity: -2,
        flags: &["readonly"],
        first_key: 1,
        last_key: 1,
        key_step: 1,
    },
    CommandSpec {
        name: "GEOSEARCH",
        arity: -7,
        flags: &["readonly"],
        first_key: 1,
        last_key: 1,
        key_step: 1,
    },
    CommandSpec {
        name: "GEOSEARCHSTORE",
        arity: -8,
        flags: &["write", "denyoom"],
        first_key: 1,
        last_key: 2,
        key_step: 1,
    },
    CommandSpec {
        name: "GEORADIUS",
        arity: -6,
        flags: &["write", "denyoom"],
        first_key: 1,
        last_key: 1,
        key_step: 1,
    },
    CommandSpec {
        name: "GEORADIUS_RO",
        arity: -6,
        flags: &["readonly"],
        first_key: 1,
        last_key: 1,
        key_step: 1,
    },
    CommandSpec {
        name: "GEORADIUSBYMEMBER",
        arity: -5,
        flags: &["write", "denyoom"],
        first_key: 1,
        last_key: 1,
        key_step: 1,
    },
    CommandSpec {
        name: "GEORADIUSBYMEMBER_RO",
        arity: -5,
        flags: &["readonly"],
        first_key: 1,
        last_key: 1,
        key_step: 1,
    },
    // Bitmap
    CommandSpec {
        name: "SETBIT",
        arity: 4,
        flags: &["write", "denyoom"],
        first_key: 1,
        last_key: 1,
        key_step: 1,
    },
    CommandSpec {
        name: "GETBIT",
        arity: 3,
        flags: &["readonly", "fast"],
        first_key: 1,
        last_key: 1,
        key_step: 1,
    },
    CommandSpec {
        name: "BITCOUNT",
        arity: -2,
        flags: &["readonly"],
        first_key: 1,
        last_key: 1,
        key_step: 1,
    },
    CommandSpec {
        name: "BITPOS",
        arity: -3,
        flags: &["readonly"],
        first_key: 1,
        last_key: 1,
        key_step: 1,
    },
    CommandSpec {
        name: "BITOP",
        arity: -4,
        flags: &["write", "denyoom"],
        first_key: 2,
        last_key: -1,
        key_step: 1,
    },
    CommandSpec {
        name: "BITFIELD",
        arity: -2,
        flags: &["write", "denyoom"],
        first_key: 1,
        last_key: 1,
        key_step: 1,
    },
    CommandSpec {
        name: "BITFIELD_RO",
        arity: -2,
        flags: &["readonly", "fast"],
        first_key: 1,
        last_key: 1,
        key_step: 1,
    },
    // Hash field TTL
    CommandSpec {
        name: "HEXPIRE",
        arity: -6,
        flags: &["write", "fast"],
        first_key: 1,
        last_key: 1,
        key_step: 1,
    },
    CommandSpec {
        name: "HEXPIREAT",
        arity: -6,
        flags: &["write", "fast"],
        first_key: 1,
        last_key: 1,
        key_step: 1,
    },
    CommandSpec {
        name: "HPEXPIRE",
        arity: -6,
        flags: &["write", "fast"],
        first_key: 1,
        last_key: 1,
        key_step: 1,
    },
    CommandSpec {
        name: "HPEXPIREAT",
        arity: -6,
        flags: &["write", "fast"],
        first_key: 1,
        last_key: 1,
        key_step: 1,
    },
    CommandSpec {
        name: "HTTL",
        arity: -5,
        flags: &["readonly", "fast"],
        first_key: 1,
        last_key: 1,
        key_step: 1,
    },
    CommandSpec {
        name: "HPTTL",
        arity: -5,
        flags: &["readonly", "fast"],
        first_key: 1,
        last_key: 1,
        key_step: 1,
    },
    CommandSpec {
        name: "HEXPIRETIME",
        arity: -5,
        flags: &["readonly", "fast"],
        first_key: 1,
        last_key: 1,
        key_step: 1,
    },
    CommandSpec {
        name: "HPEXPIRETIME",
        arity: -5,
        flags: &["readonly", "fast"],
        first_key: 1,
        last_key: 1,
        key_step: 1,
    },
    CommandSpec {
        name: "HPERSIST",
        arity: -5,
        flags: &["write", "fast"],
        first_key: 1,
        last_key: 1,
        key_step: 1,
    },
    CommandSpec {
        name: "HGETDEL",
        arity: -5,
        flags: &["write", "fast"],
        first_key: 1,
        last_key: 1,
        key_step: 1,
    },
    CommandSpec {
        name: "HGETEX",
        arity: -5,
        flags: &["write", "fast"],
        first_key: 1,
        last_key: 1,
        key_step: 1,
    },
    CommandSpec {
        name: "HSETEX",
        arity: -6,
        flags: &["write", "denyoom", "fast"],
        first_key: 1,
        last_key: 1,
        key_step: 1,
    },
    // Cluster
    CommandSpec {
        name: "CLUSTER",
        arity: -2,
        flags: &["admin"],
        first_key: 0,
        last_key: 0,
        key_step: 0,
    },
    CommandSpec {
        name: "CLUSTER INFO",
        arity: 2,
        flags: &["admin", "readonly"],
        first_key: 0,
        last_key: 0,
        key_step: 0,
    },
    CommandSpec {
        name: "CLUSTER MYID",
        arity: 2,
        flags: &["admin", "readonly"],
        first_key: 0,
        last_key: 0,
        key_step: 0,
    },
    CommandSpec {
        name: "CLUSTER KEYSLOT",
        arity: 3,
        flags: &["admin", "readonly"],
        first_key: 0,
        last_key: 0,
        key_step: 0,
    },
    CommandSpec {
        name: "CLUSTER COUNTKEYSINSLOT",
        arity: 3,
        flags: &["admin", "readonly"],
        first_key: 0,
        last_key: 0,
        key_step: 0,
    },
    CommandSpec {
        name: "CLUSTER GETKEYSINSLOT",
        arity: 4,
        flags: &["admin", "readonly"],
        first_key: 0,
        last_key: 0,
        key_step: 0,
    },
    CommandSpec {
        name: "CLUSTER HELP",
        arity: 2,
        flags: &["admin", "readonly"],
        first_key: 0,
        last_key: 0,
        key_step: 0,
    },
    CommandSpec {
        name: "CLUSTER ADDSLOTS",
        arity: -3,
        flags: &["admin"],
        first_key: 0,
        last_key: 0,
        key_step: 0,
    },
    CommandSpec {
        name: "CLUSTER ADDSLOTSRANGE",
        arity: -4,
        flags: &["admin"],
        first_key: 0,
        last_key: 0,
        key_step: 0,
    },
    CommandSpec {
        name: "CLUSTER DELSLOTS",
        arity: -3,
        flags: &["admin"],
        first_key: 0,
        last_key: 0,
        key_step: 0,
    },
    CommandSpec {
        name: "CLUSTER DELSLOTSRANGE",
        arity: -4,
        flags: &["admin"],
        first_key: 0,
        last_key: 0,
        key_step: 0,
    },
    CommandSpec {
        name: "CLUSTER FAILOVER",
        arity: -2,
        flags: &["admin"],
        first_key: 0,
        last_key: 0,
        key_step: 0,
    },
    CommandSpec {
        name: "CLUSTER FLUSHSLOTS",
        arity: 2,
        flags: &["admin"],
        first_key: 0,
        last_key: 0,
        key_step: 0,
    },
    CommandSpec {
        name: "CLUSTER FORGET",
        arity: 3,
        flags: &["admin"],
        first_key: 0,
        last_key: 0,
        key_step: 0,
    },
    CommandSpec {
        name: "CLUSTER LINKS",
        arity: 2,
        flags: &["admin", "readonly"],
        first_key: 0,
        last_key: 0,
        key_step: 0,
    },
    CommandSpec {
        name: "CLUSTER MEET",
        arity: -4,
        flags: &["admin"],
        first_key: 0,
        last_key: 0,
        key_step: 0,
    },
    CommandSpec {
        name: "CLUSTER NODES",
        arity: 2,
        flags: &["admin", "readonly"],
        first_key: 0,
        last_key: 0,
        key_step: 0,
    },
    CommandSpec {
        name: "CLUSTER REPLICAS",
        arity: 3,
        flags: &["admin", "readonly"],
        first_key: 0,
        last_key: 0,
        key_step: 0,
    },
    CommandSpec {
        name: "CLUSTER REPLICATE",
        arity: 3,
        flags: &["admin"],
        first_key: 0,
        last_key: 0,
        key_step: 0,
    },
    CommandSpec {
        name: "CLUSTER RESET",
        arity: -2,
        flags: &["admin"],
        first_key: 0,
        last_key: 0,
        key_step: 0,
    },
    CommandSpec {
        name: "CLUSTER SAVECONFIG",
        arity: 2,
        flags: &["admin"],
        first_key: 0,
        last_key: 0,
        key_step: 0,
    },
    CommandSpec {
        name: "CLUSTER SET-CONFIG-EPOCH",
        arity: 3,
        flags: &["admin"],
        first_key: 0,
        last_key: 0,
        key_step: 0,
    },
    CommandSpec {
        name: "CLUSTER SETSLOT",
        arity: -4,
        flags: &["admin"],
        first_key: 0,
        last_key: 0,
        key_step: 0,
    },
    CommandSpec {
        name: "CLUSTER SHARDS",
        arity: 2,
        flags: &["admin", "readonly"],
        first_key: 0,
        last_key: 0,
        key_step: 0,
    },
    CommandSpec {
        name: "CLUSTER SLAVES",
        arity: 3,
        flags: &["admin", "readonly"],
        first_key: 0,
        last_key: 0,
        key_step: 0,
    },
    CommandSpec {
        name: "CLUSTER SLOTS",
        arity: 2,
        flags: &["admin", "readonly"],
        first_key: 0,
        last_key: 0,
        key_step: 0,
    },
    CommandSpec {
        name: "READONLY",
        arity: 1,
        flags: &["fast", "connection"],
        first_key: 0,
        last_key: 0,
        key_step: 0,
    },
    CommandSpec {
        name: "READWRITE",
        arity: 1,
        flags: &["fast", "connection"],
        first_key: 0,
        last_key: 0,
        key_step: 0,
    },
    CommandSpec {
        name: "ASKING",
        arity: 1,
        flags: &["fast"],
        first_key: 0,
        last_key: 0,
        key_step: 0,
    },
    // Scripting
    CommandSpec {
        name: "EVAL",
        arity: -3,
        flags: &["noscript"],
        first_key: 0,
        last_key: 0,
        key_step: 0,
    },
    CommandSpec {
        name: "EVALSHA",
        arity: -3,
        flags: &["noscript"],
        first_key: 0,
        last_key: 0,
        key_step: 0,
    },
    CommandSpec {
        name: "EVAL_RO",
        arity: -3,
        flags: &["noscript", "readonly"],
        first_key: 0,
        last_key: 0,
        key_step: 0,
    },
    CommandSpec {
        name: "EVALSHA_RO",
        arity: -3,
        flags: &["noscript", "readonly"],
        first_key: 0,
        last_key: 0,
        key_step: 0,
    },
    CommandSpec {
        name: "SCRIPT",
        arity: -2,
        flags: &["noscript"],
        first_key: 0,
        last_key: 0,
        key_step: 0,
    },
    CommandSpec {
        name: "SCRIPT LOAD",
        arity: 3,
        flags: &["noscript"],
        first_key: 0,
        last_key: 0,
        key_step: 0,
    },
    CommandSpec {
        name: "SCRIPT EXISTS",
        arity: -3,
        flags: &["noscript", "readonly"],
        first_key: 0,
        last_key: 0,
        key_step: 0,
    },
    CommandSpec {
        name: "SCRIPT FLUSH",
        arity: -2,
        flags: &["noscript"],
        first_key: 0,
        last_key: 0,
        key_step: 0,
    },
    CommandSpec {
        name: "SCRIPT HELP",
        arity: 2,
        flags: &["noscript", "readonly"],
        first_key: 0,
        last_key: 0,
        key_step: 0,
    },
    CommandSpec {
        name: "SCRIPT KILL",
        arity: 2,
        flags: &["noscript"],
        first_key: 0,
        last_key: 0,
        key_step: 0,
    },
    CommandSpec {
        name: "SCRIPT DEBUG",
        arity: 3,
        flags: &["noscript"],
        first_key: 0,
        last_key: 0,
        key_step: 0,
    },
    CommandSpec {
        name: "FCALL",
        arity: -3,
        flags: &["noscript"],
        first_key: 0,
        last_key: 0,
        key_step: 0,
    },
    CommandSpec {
        name: "FCALL_RO",
        arity: -3,
        flags: &["noscript", "readonly"],
        first_key: 0,
        last_key: 0,
        key_step: 0,
    },
    CommandSpec {
        name: "FUNCTION",
        arity: -2,
        flags: &["noscript"],
        first_key: 0,
        last_key: 0,
        key_step: 0,
    },
    CommandSpec {
        name: "FUNCTION LOAD",
        arity: -3,
        flags: &["noscript"],
        first_key: 0,
        last_key: 0,
        key_step: 0,
    },
    CommandSpec {
        name: "FUNCTION DELETE",
        arity: 3,
        flags: &["noscript"],
        first_key: 0,
        last_key: 0,
        key_step: 0,
    },
    CommandSpec {
        name: "FUNCTION LIST",
        arity: -2,
        flags: &["noscript", "readonly"],
        first_key: 0,
        last_key: 0,
        key_step: 0,
    },
    CommandSpec {
        name: "FUNCTION STATS",
        arity: 2,
        flags: &["noscript", "readonly"],
        first_key: 0,
        last_key: 0,
        key_step: 0,
    },
    CommandSpec {
        name: "FUNCTION DUMP",
        arity: 2,
        flags: &["noscript", "readonly"],
        first_key: 0,
        last_key: 0,
        key_step: 0,
    },
    CommandSpec {
        name: "FUNCTION RESTORE",
        arity: -3,
        flags: &["noscript"],
        first_key: 0,
        last_key: 0,
        key_step: 0,
    },
    CommandSpec {
        name: "FUNCTION FLUSH",
        arity: -2,
        flags: &["noscript"],
        first_key: 0,
        last_key: 0,
        key_step: 0,
    },
    CommandSpec {
        name: "FUNCTION HELP",
        arity: 2,
        flags: &["noscript", "readonly"],
        first_key: 0,
        last_key: 0,
        key_step: 0,
    },
    // Sentinel
    CommandSpec {
        name: "SENTINEL",
        arity: -2,
        flags: &["admin"],
        first_key: 0,
        last_key: 0,
        key_step: 0,
    },
    CommandSpec {
        name: "SENTINEL HELP",
        arity: 2,
        flags: &["admin", "readonly"],
        first_key: 0,
        last_key: 0,
        key_step: 0,
    },
    CommandSpec {
        name: "SENTINEL GET-MASTER-ADDR-BY-NAME",
        arity: 3,
        flags: &["admin", "readonly"],
        first_key: 0,
        last_key: 0,
        key_step: 0,
    },
    CommandSpec {
        name: "SENTINEL MASTERS",
        arity: 2,
        flags: &["admin", "readonly"],
        first_key: 0,
        last_key: 0,
        key_step: 0,
    },
    CommandSpec {
        name: "SENTINEL MASTER",
        arity: 3,
        flags: &["admin", "readonly"],
        first_key: 0,
        last_key: 0,
        key_step: 0,
    },
    CommandSpec {
        name: "SENTINEL REPLICAS",
        arity: 3,
        flags: &["admin", "readonly"],
        first_key: 0,
        last_key: 0,
        key_step: 0,
    },
    CommandSpec {
        name: "SENTINEL SENTINELS",
        arity: 3,
        flags: &["admin", "readonly"],
        first_key: 0,
        last_key: 0,
        key_step: 0,
    },
    CommandSpec {
        name: "SENTINEL IS-MASTER-DOWN-BY-ADDR",
        arity: 6,
        flags: &["admin"],
        first_key: 0,
        last_key: 0,
        key_step: 0,
    },
    CommandSpec {
        name: "SENTINEL RESET",
        arity: 3,
        flags: &["admin"],
        first_key: 0,
        last_key: 0,
        key_step: 0,
    },
    CommandSpec {
        name: "SENTINEL FAILOVER",
        arity: 3,
        flags: &["admin"],
        first_key: 0,
        last_key: 0,
        key_step: 0,
    },
    CommandSpec {
        name: "SENTINEL CKQUORUM",
        arity: 3,
        flags: &["admin"],
        first_key: 0,
        last_key: 0,
        key_step: 0,
    },
    CommandSpec {
        name: "SENTINEL FLUSHCONFIG",
        arity: 2,
        flags: &["admin"],
        first_key: 0,
        last_key: 0,
        key_step: 0,
    },
    CommandSpec {
        name: "SENTINEL MONITOR",
        arity: 6,
        flags: &["admin"],
        first_key: 0,
        last_key: 0,
        key_step: 0,
    },
    CommandSpec {
        name: "SENTINEL REMOVE",
        arity: 3,
        flags: &["admin"],
        first_key: 0,
        last_key: 0,
        key_step: 0,
    },
    CommandSpec {
        name: "SENTINEL SET",
        arity: -5,
        flags: &["admin"],
        first_key: 0,
        last_key: 0,
        key_step: 0,
    },
    CommandSpec {
        name: "SENTINEL PENDING-SCRIPTS",
        arity: 2,
        flags: &["admin", "readonly"],
        first_key: 0,
        last_key: 0,
        key_step: 0,
    },
    CommandSpec {
        name: "SENTINEL INFO-CACHE",
        arity: -3,
        flags: &["admin", "readonly"],
        first_key: 0,
        last_key: 0,
        key_step: 0,
    },
    CommandSpec {
        name: "SENTINEL SIMULATE-FAILURE",
        arity: -3,
        flags: &["admin"],
        first_key: 0,
        last_key: 0,
        key_step: 0,
    },
    CommandSpec {
        name: "SENTINEL CONFIG",
        arity: -4,
        flags: &["admin"],
        first_key: 0,
        last_key: 0,
        key_step: 0,
    },
    CommandSpec {
        name: "SENTINEL DEBUG",
        arity: -2,
        flags: &["admin"],
        first_key: 0,
        last_key: 0,
        key_step: 0,
    },
    CommandSpec {
        name: "SENTINEL MYID",
        arity: 2,
        flags: &["admin", "readonly"],
        first_key: 0,
        last_key: 0,
        key_step: 0,
    },
];

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommandOutcome {
    pub response: RespFrame,
    pub close: bool,
    /// When set, the server should release the Mutex, sleep, re-acquire, and
    /// re-execute the command. Contains the original frame for re-execution.
    pub retry_blocking: Option<BlockingRetry>,
    /// Optional delay in milliseconds before sending the response.
    /// Used for progressive AUTH failure delay.
    pub delay_ms: Option<u64>,
    /// Indicates that CONFIG state changed and lock-free config readers should refresh.
    pub config_dirty: bool,
    /// Indicates that ACL state changed and cached ACL readers should refresh.
    pub acl_dirty: bool,
}

/// A command that changed durable state and can be replayed without relying on
/// client-time inputs such as a relative TTL or an auto-generated stream ID.
///
/// The server consumes these after command execution.  Keeping the capture at
/// this boundary is important: callers only see the client request, whereas
/// this layer knows which conditional command actually took effect.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DurableCommand {
    pub db_index: usize,
    pub argv: Vec<Bytes>,
}

/// The durable effects emitted by one command invocation.
///
/// A transaction is represented as a group so the AOF writer can place its
/// effects inside one `MULTI`/`EXEC` envelope rather than writing a bare
/// `EXEC` or letting a torn tail apply a prefix.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct DurabilityEffects {
    pub commands: Vec<DurableCommand>,
    pub transaction: bool,
    /// One frozen execution clock for the command or atomic EXEC envelope.
    pub timestamp_ms: i64,
}

impl DurabilityEffects {
    fn single(db_index: usize, argv: Vec<Bytes>) -> Self {
        Self {
            commands: vec![DurableCommand { db_index, argv }],
            transaction: false,
            timestamp_ms: now_ms(),
        }
    }

    fn transaction(commands: Vec<DurableCommand>) -> Self {
        Self {
            commands,
            transaction: true,
            timestamp_ms: now_ms(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.commands.is_empty()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BlockingRetry {
    pub deadline_ms: Option<i64>,
    pub frame: RespFrame,
    pub watch_keys: Vec<(usize, Bytes)>,
}

impl CommandOutcome {
    fn reply(response: RespFrame) -> Self {
        Self {
            response,
            close: false,
            retry_blocking: None,
            delay_ms: None,
            config_dirty: false,
            acl_dirty: false,
        }
    }

    fn close(response: RespFrame) -> Self {
        Self {
            response,
            close: true,
            retry_blocking: None,
            delay_ms: None,
            config_dirty: false,
            acl_dirty: false,
        }
    }

    fn reply_with_delay(response: RespFrame, delay_ms: u64) -> Self {
        Self {
            response,
            close: false,
            retry_blocking: None,
            delay_ms: Some(delay_ms),
            config_dirty: false,
            acl_dirty: false,
        }
    }

    fn close_with_delay(response: RespFrame, delay_ms: u64) -> Self {
        Self {
            response,
            close: true,
            retry_blocking: None,
            delay_ms: Some(delay_ms),
            config_dirty: false,
            acl_dirty: false,
        }
    }

    fn blocking(
        response: RespFrame,
        deadline_ms: Option<i64>,
        frame: RespFrame,
        watch_keys: Vec<(usize, Bytes)>,
    ) -> Self {
        Self {
            response,
            close: false,
            retry_blocking: Some(BlockingRetry {
                deadline_ms,
                frame,
                watch_keys,
            }),
            delay_ms: None,
            config_dirty: false,
            acl_dirty: false,
        }
    }

    fn with_config_dirty(mut self) -> Self {
        self.config_dirty = true;
        self
    }

    fn with_acl_dirty(mut self) -> Self {
        self.acl_dirty = true;
        self
    }
}

#[derive(Debug, Clone)]
struct WatchedKey {
    version: u64,
}

// ---------------------------------------------------------------------------
// TransactionState — explicit state machine for MULTI/EXEC transactions
// ---------------------------------------------------------------------------

/// Represents the state of a client's transaction.
#[derive(Debug, Clone, Default)]
pub enum TransactionState {
    /// Normal operation - not in a transaction
    #[default]
    Normal,
    /// In a MULTI/EXEC transaction block
    InTransaction {
        queue: Vec<Vec<Bytes>>,
        has_error: bool,
    },
}

impl TransactionState {
    /// Returns true if currently in a transaction block
    pub fn in_multi(&self) -> bool {
        matches!(self, TransactionState::InTransaction { .. })
    }

    /// Returns the number of queued commands, or 0 if not in a transaction
    pub fn queue_len(&self) -> usize {
        match self {
            TransactionState::InTransaction { queue, .. } => queue.len(),
            TransactionState::Normal => 0,
        }
    }

    /// Returns true if the transaction has an error flag set
    pub fn has_error(&self) -> bool {
        matches!(
            self,
            TransactionState::InTransaction {
                has_error: true,
                ..
            }
        )
    }
}

#[derive(Debug, Clone)]
pub struct ClientState {
    id: i64,
    selected_db: usize,
    name: Option<Bytes>,
    authenticated: bool,
    acl_user: Bytes,
    tx_state: TransactionState,
    watched: hashbrown::HashMap<(usize, Bytes), WatchedKey>,
    created_at_ms: i64,
    last_interaction_ms: i64,
    last_command: Bytes,
    subscribed_channels: hashbrown::HashSet<Bytes>,
    subscribed_patterns: hashbrown::HashSet<Bytes>,
    pubsub_subscriptions: usize,
    tracking_enabled: bool,
    tracking_redirect: i64,
    tracking_broadcast: bool,
    tracking_no_loop: bool,
    tracking_optin: bool,
    tracking_optout: bool,
    tracking_prefixes: Vec<Bytes>,
    protocol_version: i64,
    caching_enabled: bool,
    no_evict: bool,
    no_touch: bool,
    reply_mode: Bytes,
    auth_failure_count: u32,
    lib_name: Option<Bytes>,
    lib_ver: Option<Bytes>,
    unix_socket: bool,
    monitor_mode: bool,
    durability_capture_enabled: bool,
    durability_effects: Option<DurabilityEffects>,
}

impl ClientState {
    pub fn id(&self) -> i64 {
        self.id
    }

    pub fn is_authenticated(&self) -> bool {
        self.authenticated
    }

    pub fn acl_user(&self) -> &Bytes {
        &self.acl_user
    }

    pub fn authenticate_as(&mut self, user: Bytes) {
        self.authenticated = true;
        self.acl_user = user;
    }

    pub fn has_pubsub_subscriptions(&self) -> bool {
        self.pubsub_subscriptions > 0
    }

    pub fn is_monitor(&self) -> bool {
        self.monitor_mode
    }

    pub fn set_monitor(&mut self, enabled: bool) {
        self.monitor_mode = enabled;
    }

    pub fn set_unix_socket(&mut self, value: bool) {
        self.unix_socket = value;
    }

    pub fn selected_db(&self) -> usize {
        self.selected_db
    }

    pub fn tracking_enabled(&self) -> bool {
        self.tracking_enabled
    }

    pub fn tracking_target_client_id(&self) -> i64 {
        if self.tracking_redirect >= 0 {
            self.tracking_redirect
        } else {
            self.id
        }
    }

    pub fn tracking_broadcast(&self) -> bool {
        self.tracking_broadcast
    }

    pub fn tracking_redirect(&self) -> i64 {
        self.tracking_redirect
    }

    pub fn protocol_version(&self) -> i64 {
        self.protocol_version
    }

    pub fn in_multi(&self) -> bool {
        self.tx_state.in_multi()
    }

    pub fn has_queued_writes(&self) -> bool {
        match &self.tx_state {
            TransactionState::InTransaction { queue, .. } => {
                queue.iter().any(|argv| is_write_command(argv))
            }
            TransactionState::Normal => false,
        }
    }

    /// Take effects captured for the most recently executed command.
    ///
    /// This is deliberately per-client transient state rather than a response
    /// field so the protocol layer remains independent of persistence.
    pub fn take_durability_effects(&mut self) -> Option<DurabilityEffects> {
        self.durability_effects.take()
    }

    /// Enable durable-effect capture for the current server execution path.
    ///
    /// The server toggles this with its live AOF writer state. Keeping it off
    /// by default avoids cloning argv or inspecting post-mutation values on
    /// the ordinary disabled-AOF cache path.
    pub fn set_durability_capture_enabled(&mut self, enabled: bool) {
        self.durability_capture_enabled = enabled;
        if !enabled {
            self.durability_effects = None;
        }
    }

    fn set_durability_effects(&mut self, effects: Option<DurabilityEffects>) {
        self.durability_effects = effects;
    }

    pub fn set_protocol_version(&mut self, protocol_version: i64) {
        self.protocol_version = protocol_version;
    }

    pub fn tracking_no_loop(&self) -> bool {
        self.tracking_no_loop
    }

    pub fn tracking_prefixes(&self) -> &[Bytes] {
        &self.tracking_prefixes
    }

    pub fn tracking_optin(&self) -> bool {
        self.tracking_optin
    }

    pub fn tracking_optout(&self) -> bool {
        self.tracking_optout
    }

    pub fn tracking_should_record_reads(&self) -> bool {
        if !self.tracking_enabled || self.tracking_broadcast {
            return false;
        }

        if self.tracking_optin || self.tracking_optout {
            self.caching_enabled
        } else {
            true
        }
    }

    pub fn tracking_accepts_caching_toggle(&self) -> bool {
        self.tracking_enabled && (self.tracking_optin || self.tracking_optout)
    }

    pub fn set_tracking_caching(&mut self, enabled: bool) {
        self.caching_enabled = enabled;
    }

    pub fn reset_tracking_caching_mode(&mut self) {
        self.caching_enabled = !self.tracking_optin;
    }

    pub fn finish_tracking_command(&mut self, command: &[u8], argv: &[Bytes]) {
        if !self.tracking_accepts_caching_toggle() {
            return;
        }

        if command == b"CLIENT"
            && argv
                .get(1)
                .is_some_and(|subcommand| subcommand.eq_ignore_ascii_case(b"CACHING"))
        {
            return;
        }

        self.reset_tracking_caching_mode();
    }

    pub fn set_pubsub_subscription_count(&mut self, count: i64) {
        self.pubsub_subscriptions = usize::try_from(count).unwrap_or(0);
    }

    pub fn snapshot(&self, addr: Bytes, laddr: Bytes) -> ClientSnapshot {
        self.snapshot_with_redirect(addr, laddr, self.tracking_redirect)
    }

    pub fn auth_failure_count(&self) -> u32 {
        self.auth_failure_count
    }

    pub fn increment_auth_failures(&mut self) -> u32 {
        self.auth_failure_count = self.auth_failure_count.saturating_add(1);
        self.auth_failure_count
    }

    pub fn reset_auth_failures(&mut self) {
        self.auth_failure_count = 0;
    }

    pub fn reply_mode(&self) -> &Bytes {
        &self.reply_mode
    }

    pub fn set_reply_mode_on(&mut self) {
        self.reply_mode = Bytes::from_static(b"on");
    }

    pub fn lib_name(&self) -> Option<&Bytes> {
        self.lib_name.as_ref()
    }

    pub fn lib_ver(&self) -> Option<&Bytes> {
        self.lib_ver.as_ref()
    }

    pub fn set_lib_name(&mut self, name: Bytes) {
        self.lib_name = Some(name);
    }

    pub fn set_lib_ver(&mut self, ver: Bytes) {
        self.lib_ver = Some(ver);
    }

    pub fn mark_command_metadata(&mut self, command: &Bytes, touch_interaction: bool) {
        if touch_interaction {
            self.last_interaction_ms = now_client_clock_ms();
        }
        if self.last_command.as_ref() != command.as_ref() {
            self.last_command = command.clone();
        }
    }

    pub fn snapshot_with_redirect(
        &self,
        addr: Bytes,
        laddr: Bytes,
        active_redirect: i64,
    ) -> ClientSnapshot {
        let age_seconds = (now_client_clock_ms().saturating_sub(self.created_at_ms) / 1000).max(0);
        let idle_seconds =
            (now_client_clock_ms().saturating_sub(self.last_interaction_ms) / 1000).max(0);

        let mut flags = Vec::with_capacity(4);
        if self.has_pubsub_subscriptions() {
            flags.push(b'P');
        } else {
            flags.push(b'N');
        }
        if self.unix_socket {
            flags.push(b'U');
        }
        if self.tx_state.in_multi() {
            flags.push(b'x');
        }
        if self.tracking_enabled {
            flags.push(b't');
        }

        ClientSnapshot {
            id: self.id,
            addr,
            laddr,
            name: self.name.clone(),
            age_seconds,
            idle_seconds,
            flags: Bytes::from(flags),
            db: self.selected_db,
            sub: self.pubsub_subscriptions,
            psub: self.subscribed_patterns.len(),
            ssub: self.subscribed_channels.len(),
            multi: if self.tx_state.in_multi() {
                self.tx_state.queue_len() as i64
            } else {
                -1
            },
            cmd: if self.last_command.is_empty() {
                Bytes::from_static(b"NULL")
            } else {
                Bytes::copy_from_slice(self.last_command.as_ref())
            },
            user: self.acl_user.clone(),
            redir: active_redirect,
            tracking_enabled: self.tracking_enabled,
            resp: self.protocol_version,
            lib_name: self.lib_name.clone(),
            lib_ver: self.lib_ver.clone(),
        }
    }

    pub fn new(id: i64) -> Self {
        let now_ms = now_client_clock_ms();
        Self {
            id,
            selected_db: 0,
            name: None,
            authenticated: false,
            acl_user: Bytes::from_static(b"default"),
            tx_state: TransactionState::default(),
            watched: hashbrown::HashMap::new(),
            created_at_ms: now_ms,
            last_interaction_ms: now_ms,
            last_command: Bytes::new(),
            subscribed_channels: hashbrown::HashSet::new(),
            subscribed_patterns: hashbrown::HashSet::new(),
            pubsub_subscriptions: 0,
            tracking_enabled: false,
            tracking_redirect: -1,
            tracking_broadcast: false,
            tracking_no_loop: false,
            tracking_optin: false,
            tracking_optout: false,
            tracking_prefixes: Vec::new(),
            protocol_version: 2,
            caching_enabled: true,
            no_evict: false,
            no_touch: false,
            reply_mode: Bytes::from_static(b"on"),
            auth_failure_count: 0,
            lib_name: None,
            lib_ver: None,
            unix_socket: false,
            monitor_mode: false,
            durability_capture_enabled: false,
            durability_effects: None,
        }
    }

    pub(crate) fn reset_for_connection(&mut self) {
        self.selected_db = 0;
        self.name = None;
        self.authenticated = false;
        self.acl_user = Bytes::from_static(b"default");
        self.tx_state = TransactionState::default();
        self.watched.clear();
        self.last_interaction_ms = now_client_clock_ms();
        self.last_command = Bytes::new();
        self.subscribed_channels.clear();
        self.subscribed_patterns.clear();
        self.pubsub_subscriptions = 0;
        self.tracking_enabled = false;
        self.tracking_redirect = -1;
        self.tracking_broadcast = false;
        self.tracking_no_loop = false;
        self.tracking_optin = false;
        self.tracking_optout = false;
        self.tracking_prefixes.clear();
        self.protocol_version = 2;
        self.caching_enabled = true;
        self.no_evict = false;
        self.no_touch = false;
        self.reply_mode = Bytes::from_static(b"on");
        self.monitor_mode = false;
        self.durability_capture_enabled = false;
        self.durability_effects = None;
    }
}

impl Default for ClientState {
    fn default() -> Self {
        Self::new(0)
    }
}

pub enum ExecuteArgvPrecheck {
    Continue,
    Reject(CommandOutcome),
}

fn allows_execute_without_auth(command: &[u8], spec: Option<CommandSpec>) -> bool {
    command == b"HELLO"
        || command == b"QUIT"
        || spec.is_some_and(|candidate| candidate.flags.contains(&"no_auth"))
}

pub fn precheck_execute_argv_with_default_acl(
    argv: &[Bytes],
    default_acl_policy: &DefaultAclPolicyState,
    client: &mut ClientState,
) -> ExecuteArgvPrecheck {
    if argv.is_empty() {
        return ExecuteArgvPrecheck::Reject(CommandOutcome::reply(err("ERR empty command")));
    }

    let command = to_uppercase_stack(&argv[0]);
    let spec = find_command_spec_upper(command.as_slice());

    if !client.is_authenticated() && default_acl_policy.default_user_is_nopass_enabled() {
        client.authenticate_as(Bytes::from_static(b"default"));
    }

    let allow_without_auth = allows_execute_without_auth(command.as_slice(), spec);
    if !client.is_authenticated() && !allow_without_auth {
        return ExecuteArgvPrecheck::Reject(CommandOutcome::reply(err(
            "NOAUTH Authentication required.",
        )));
    }

    ExecuteArgvPrecheck::Continue
}

pub fn execute_argv(
    argv: &[Bytes],
    access: &mut ServerAccess<'_>,
    client: &mut ClientState,
) -> CommandOutcome {
    if client.durability_capture_enabled {
        // All relative expiries and lazy expiry decisions in this operation
        // use the same clock that recovery will use, including nested EXEC.
        ratatosk_core::time::with_command_time(now_ms(), || {
            execute_argv_inner(argv, access, client)
        })
    } else {
        execute_argv_inner(argv, access, client)
    }
}

fn execute_argv_inner(
    argv: &[Bytes],
    access: &mut ServerAccess<'_>,
    client: &mut ClientState,
) -> CommandOutcome {
    if argv.is_empty() {
        client.set_durability_effects(None);
        return CommandOutcome::reply(err("ERR empty command"));
    }

    // Each invocation owns its captured effects.  This also ensures queued,
    // discarded, aborted, and validation-failed transaction commands cannot
    // leak a previous command's effects into the AOF admission path.
    client.set_durability_effects(None);
    let selected_db_before = client.selected_db();

    let command_raw = &argv[0];
    let command = to_uppercase_stack(command_raw);
    let spec = find_command_spec_upper(command.as_slice());

    if !client.is_authenticated() && access.default_user_is_nopass_enabled() {
        client.authenticate_as(Bytes::from_static(b"default"));
    }

    let allow_without_auth = allows_execute_without_auth(command.as_slice(), spec);

    if !client.is_authenticated() && !allow_without_auth {
        return CommandOutcome::reply(err("NOAUTH Authentication required."));
    }

    if client.is_authenticated()
        && !allow_without_auth
        && !(client.acl_user().as_ref() == b"default" && access.default_user_has_full_access())
    {
        if let Some(candidate) = spec {
            let required_mask = acl_required_category_mask(candidate);
            if !access
                .meta
                .acl
                .command_allowed_mask(client.acl_user(), required_mask)
            {
                return CommandOutcome::reply(err(
                    "NOPERM this user has no permissions to run the command",
                ));
            }
        }
    }

    access.mark_command_processed();

    let atomic_stats = access.atomic_stats;

    // Rebind for dispatch table and tracking code.
    let server = &mut *access.meta;
    client.mark_command_metadata(
        command_raw,
        !matches!(command.as_slice(), b"PING" | b"ECHO"),
    );

    const MAX_TX_QUEUE_SIZE: usize = 65536;

    if client.tx_state.in_multi()
        && !matches!(
            command.as_slice(),
            b"EXEC" | b"DISCARD" | b"MULTI" | b"WATCH" | b"UNWATCH"
        )
    {
        if let Err(response) = validate_queued_command(argv, server, client) {
            if let TransactionState::InTransaction { has_error, .. } = &mut client.tx_state {
                *has_error = true;
            }
            return CommandOutcome::reply(response);
        }

        if let TransactionState::InTransaction { queue, has_error } = &mut client.tx_state {
            if queue.len() >= MAX_TX_QUEUE_SIZE {
                *has_error = true;
                return CommandOutcome::reply(err("ERR transaction queue limit reached"));
            }
            queue.push(argv.to_vec());
        }
        return CommandOutcome::reply(RespFrame::queued());
    }

    // Strict compatibility-mode gate (Phase 0 product-boundary guard): reject
    // commands Ratatosk only accepts syntactically or whose Redis durability /
    // replication contract a single node cannot honour, instead of returning a
    // misleading success-shaped reply. Default `compat` mode skips this entirely.
    if server.config.is_strict_compatibility() {
        if let Some(strict_err) = cmd_command_metadata::strict_mode_error(argv) {
            return CommandOutcome::reply(strict_err);
        }
    }

    let args = &argv[1..];
    let track_slowlog = should_track_slowlog(server, command.as_slice());
    let track_latency = should_track_latency(server, command.as_slice());
    let started = if track_slowlog || track_latency {
        Some(Instant::now())
    } else {
        None
    };

    let outcome = match command.as_slice() {
        b"PING" => cmd_connection_session::cmd_ping(args, server, atomic_stats),
        b"ECHO" => cmd_connection_session::cmd_echo(args),
        b"HELLO" => cmd_connection_session::cmd_hello(args, server, client),
        b"QUIT" => cmd_connection_session::cmd_quit(args),
        b"COMMAND" => cmd_command_metadata::cmd_command(args),
        b"DEBUG" => cmd_connection::cmd_debug(args),
        b"LOLWUT" => cmd_connection::cmd_lolwut(args),
        b"MODULE" => cmd_connection::cmd_module(args),
        b"HOTKEYS" => cmd_connection::cmd_hotkeys(args),
        b"FAILOVER" => cmd_connection::cmd_failover(args),
        b"SHUTDOWN" => cmd_connection::cmd_shutdown(args),
        b"TRIMSLOTS" => cmd_connection::cmd_trimslots(args),
        b"MULTI" => cmd_transaction::cmd_multi(args, client),
        b"EXEC" => cmd_transaction::cmd_exec(args, server, client, atomic_stats),
        b"DISCARD" => cmd_transaction::cmd_discard(args, client),
        b"WATCH" => cmd_transaction::cmd_watch(args, server, client),
        b"UNWATCH" => cmd_transaction::cmd_unwatch(args, client),
        b"SUBSCRIBE" => cmd_pubsub::cmd_subscribe(args, server, client),
        b"SSUBSCRIBE" => cmd_pubsub::cmd_ssubscribe(args, server, client),
        b"PSUBSCRIBE" => cmd_pubsub::cmd_psubscribe(args, server, client),
        b"UNSUBSCRIBE" => cmd_pubsub::cmd_unsubscribe(args, server, client),
        b"PUNSUBSCRIBE" => cmd_pubsub::cmd_punsubscribe(args, server, client),
        b"SUNSUBSCRIBE" => cmd_pubsub::cmd_sunsubscribe(args, server, client),
        b"PUBLISH" => cmd_pubsub::cmd_publish(args, server),
        b"SPUBLISH" => cmd_pubsub::cmd_spublish(args, server),
        b"PUBSUB" => cmd_pubsub::cmd_pubsub(args, server),
        b"XADD" => cmd_stream::cmd_xadd(args, server, client),
        b"XLEN" => cmd_stream::cmd_xlen(args, server, client),
        b"XRANGE" => cmd_stream::cmd_xrange(args, server, client, false),
        b"XREVRANGE" => cmd_stream::cmd_xrange(args, server, client, true),
        b"XREAD" => cmd_stream::cmd_xread(args, server, client),
        b"XGROUP" => cmd_stream_groups::cmd_xgroup(args, server, client),
        b"XREADGROUP" => cmd_stream_groups::cmd_xreadgroup(args, server, client),
        b"XACK" => cmd_stream_groups::cmd_xack(args, server, client),
        b"XPENDING" => cmd_stream_groups::cmd_xpending(args, server, client),
        b"XINFO" => cmd_stream_groups::cmd_xinfo(args, server, client),
        b"XTRIM" => cmd_stream_lifecycle::cmd_xtrim(args, server, client),
        b"XDEL" => cmd_stream_lifecycle::cmd_xdel(args, server, client),
        b"XSETID" => cmd_stream_admin::cmd_xsetid(args, server, client),
        b"XCLAIM" => cmd_stream_lifecycle::cmd_xclaim(args, server, client),
        b"XAUTOCLAIM" => cmd_stream_lifecycle::cmd_xautoclaim(args, server, client),
        b"XACKDEL" => cmd_stream_lifecycle::cmd_xackdel(args, server, client),
        b"XCFGSET" => cmd_stream_admin::cmd_xcfgset(args, server, client),
        b"XDELEX" => cmd_stream_lifecycle::cmd_xdelex(args, server, client),
        b"ACL" => cmd_acl::cmd_acl(args, server, client),
        b"AUTH" => cmd_auth_session::cmd_auth(args, server, client),
        b"CLIENT" => cmd_client::cmd_client(args, server, client),
        b"RESET" => cmd_auth_session::cmd_reset(args, server, client),
        b"DBSIZE" => cmd_server::cmd_dbsize(args, server, client),
        b"TIME" => cmd_server::cmd_time(args),
        b"INFO" => cmd_server_info::cmd_info(args, server, atomic_stats),
        b"MONITOR" => cmd_server::cmd_monitor(args, server, client),
        b"ROLE" => cmd_server_replication::cmd_role(args, server),
        b"REPLCONF" => cmd_server_replication::cmd_replconf(args, server, client),
        b"SYNC" => cmd_server_replication::cmd_sync(args),
        b"PSYNC" => cmd_server_replication::cmd_psync(args, server, client),
        b"REPLICAOF" => cmd_server_replication::cmd_replicaof(args, server),
        b"SLAVEOF" => cmd_server_replication::cmd_replicaof(args, server),
        b"RESTORE-ASKING" => cmd_generic_dump::cmd_restore(args, server, client),
        b"LATENCY" => cmd_server_latency::cmd_latency(args, server),
        b"WAIT" => cmd_generic_replication::cmd_wait(args, server),
        b"WAITAOF" => cmd_generic_replication::cmd_waitaof(args, server),
        b"CONFIG" => cmd_server_config::cmd_config(args, server, atomic_stats, client),
        b"SLOWLOG" => cmd_server_slowlog::cmd_slowlog(args, server),
        b"MEMORY" => cmd_server_memory::cmd_memory(args, server, client),
        b"LASTSAVE" => cmd_server_maintenance::cmd_lastsave(args, server),
        b"SAVE" => cmd_server_maintenance::cmd_save(args, server),
        b"BGSAVE" => cmd_server_maintenance::cmd_bgsave(args, server),
        b"BGREWRITEAOF" => cmd_server_maintenance::cmd_bgrewriteaof(args, server),
        b"SFLUSH" => cmd_server_maintenance::cmd_sflush(args),
        b"SWAPDB" => cmd_server_maintenance::cmd_swapdb(args, server),
        b"FLUSHDB" => cmd_server_maintenance::cmd_flushdb(args, server, client),
        b"FLUSHALL" => cmd_server_maintenance::cmd_flushall(args, server, client),
        b"RANDOMKEY" => cmd_generic::cmd_randomkey(args, server, client),
        b"TYPE" => cmd_generic::cmd_type(args, server, client),
        b"KEYS" => cmd_generic::cmd_keys(args, server, client),
        b"DELEX" => cmd_generic::cmd_delex(args, server, client),
        b"DIGEST" => cmd_generic::cmd_digest(args, server, client),
        b"DUMP" => cmd_generic_dump::cmd_dump(args, server, client),
        b"RESTORE" => cmd_generic_dump::cmd_restore(args, server, client),
        b"MIGRATE" => cmd_generic_replication::cmd_migrate(args),
        b"LCS" => cmd_generic_string::cmd_lcs(args, server, client),
        b"COPY" => cmd_key::cmd_copy(args, server, client),
        b"MOVE" => cmd_key::cmd_move(args, server, client),
        b"RENAME" => cmd_key::cmd_rename(args, server, client),
        b"RENAMENX" => cmd_key::cmd_renamenx(args, server, client),
        b"TOUCH" => cmd_key::cmd_touch(args, server, client),
        b"UNLINK" => cmd_key::cmd_unlink(args, server, client),
        b"SCAN" => cmd_key_meta::cmd_scan(args, server, client),
        b"OBJECT" => cmd_key_meta::cmd_object(args, server, client),
        b"SORT" => cmd_key_meta::cmd_sort(args, server, client, false),
        b"SORT_RO" => cmd_key_meta::cmd_sort(args, server, client, true),
        b"SET" => cmd_string_access::cmd_set(args, server, client),
        b"HSET" => cmd_hash::cmd_hset(args, server, client),
        b"HGET" => cmd_hash_read::cmd_hget(args, server, client),
        b"HMGET" => cmd_hash_read::cmd_hmget(args, server, client),
        b"HGETALL" => cmd_hash_read::cmd_hgetall(args, server, client),
        b"HKEYS" => cmd_hash_read::cmd_hkeys(args, server, client),
        b"HVALS" => cmd_hash_read::cmd_hvals(args, server, client),
        b"HINCRBY" => cmd_hash::cmd_hincrby(args, server, client),
        b"HINCRBYFLOAT" => cmd_hash::cmd_hincrbyfloat(args, server, client),
        b"HMSET" => cmd_hash::cmd_hmset(args, server, client),
        b"HSETNX" => cmd_hash::cmd_hsetnx(args, server, client),
        b"HSTRLEN" => cmd_hash_read::cmd_hstrlen(args, server, client),
        b"HRANDFIELD" => cmd_hash_read::cmd_hrandfield(args, server, client),
        b"HDEL" => cmd_hash::cmd_hdel(args, server, client),
        b"HEXISTS" => cmd_hash_read::cmd_hexists(args, server, client),
        b"HLEN" => cmd_hash_read::cmd_hlen(args, server, client),
        b"HSCAN" => cmd_hash_read::cmd_hscan(args, server, client),
        b"LPUSH" => cmd_list::cmd_lpush(args, server, client),
        b"LPUSHX" => cmd_list::cmd_lpushx(args, server, client),
        b"RPUSH" => cmd_list::cmd_rpush(args, server, client),
        b"RPUSHX" => cmd_list::cmd_rpushx(args, server, client),
        b"LPOP" => cmd_list_pop::cmd_lpop(args, server, client),
        b"RPOP" => cmd_list_pop::cmd_rpop(args, server, client),
        b"BLPOP" => cmd_list_blocking::cmd_blpop(args, server, client),
        b"BRPOP" => cmd_list_blocking::cmd_brpop(args, server, client),
        b"LMOVE" => cmd_list_blocking::cmd_lmove(args, server, client),
        b"BLMOVE" => cmd_list_blocking::cmd_blmove(args, server, client),
        b"RPOPLPUSH" => cmd_list_blocking::cmd_rpoplpush(args, server, client),
        b"BRPOPLPUSH" => cmd_list_blocking::cmd_brpoplpush(args, server, client),
        b"LMPOP" => cmd_list_blocking::cmd_lmpop(args, server, client),
        b"BLMPOP" => cmd_list_blocking::cmd_blmpop(args, server, client),
        b"LRANGE" => cmd_list::cmd_lrange(args, server, client),
        b"LLEN" => cmd_list::cmd_llen(args, server, client),
        b"LREM" => cmd_list::cmd_lrem(args, server, client),
        b"LPOS" => cmd_list::cmd_lpos(args, server, client),
        b"LSET" => cmd_list::cmd_lset(args, server, client),
        b"LTRIM" => cmd_list::cmd_ltrim(args, server, client),
        b"LINDEX" => cmd_list::cmd_lindex(args, server, client),
        b"LINSERT" => cmd_list::cmd_linsert(args, server, client),
        b"SADD" => cmd_set::cmd_sadd(args, server, client),
        b"SREM" => cmd_set::cmd_srem(args, server, client),
        b"SISMEMBER" => cmd_set::cmd_sismember(args, server, client),
        b"SMISMEMBER" => cmd_set::cmd_smismember(args, server, client),
        b"SMEMBERS" => cmd_set::cmd_smembers(args, server, client),
        b"SCARD" => cmd_set::cmd_scard(args, server, client),
        b"SPOP" => cmd_set::cmd_spop(args, server, client),
        b"SRANDMEMBER" => cmd_set::cmd_srandmember(args, server, client),
        b"SSCAN" => cmd_set::cmd_sscan(args, server, client),
        b"SMOVE" => cmd_set::cmd_smove(args, server, client),
        b"SDIFF" => cmd_set_algebra::cmd_sdiff(args, server, client),
        b"SDIFFSTORE" => cmd_set_algebra::cmd_sdiffstore(args, server, client),
        b"SINTER" => cmd_set_algebra::cmd_sinter(args, server, client),
        b"SINTERCARD" => cmd_set_algebra::cmd_sintercard(args, server, client),
        b"SINTERSTORE" => cmd_set_algebra::cmd_sinterstore(args, server, client),
        b"SUNION" => cmd_set_algebra::cmd_sunion(args, server, client),
        b"SUNIONSTORE" => cmd_set_algebra::cmd_sunionstore(args, server, client),
        // Sorted set
        b"ZADD" => cmd_sorted_set::cmd_zadd(args, server, client),
        b"ZREM" => cmd_sorted_set::cmd_zrem(args, server, client),
        b"ZSCORE" => cmd_sorted_set::cmd_zscore(args, server, client),
        b"ZCARD" => cmd_sorted_set::cmd_zcard(args, server, client),
        b"ZINCRBY" => cmd_sorted_set::cmd_zincrby(args, server, client),
        b"ZMSCORE" => cmd_sorted_set::cmd_zmscore(args, server, client),
        b"ZRANGE" => cmd_sorted_set_ranges::cmd_zrange(args, server, client),
        b"ZRANGEBYSCORE" => cmd_sorted_set_ranges::cmd_zrangebyscore(args, server, client),
        b"ZREVRANGEBYSCORE" => cmd_sorted_set_ranges::cmd_zrevrangebyscore(args, server, client),
        b"ZRANGEBYLEX" => cmd_sorted_set_ranges::cmd_zrangebylex(args, server, client),
        b"ZREVRANGEBYLEX" => cmd_sorted_set_ranges::cmd_zrevrangebylex(args, server, client),
        b"ZREVRANGE" => cmd_sorted_set_ranges::cmd_zrevrange(args, server, client),
        b"ZRANGESTORE" => cmd_sorted_set_ranges::cmd_zrangestore(args, server, client),
        b"ZCOUNT" => cmd_sorted_set_ranges::cmd_zcount(args, server, client),
        b"ZLEXCOUNT" => cmd_sorted_set_ranges::cmd_zlexcount(args, server, client),
        b"ZRANK" => cmd_sorted_set_ranges::cmd_zrank(args, server, client),
        b"ZREVRANK" => cmd_sorted_set_ranges::cmd_zrevrank(args, server, client),
        b"ZREMRANGEBYRANK" => cmd_sorted_set_ranges::cmd_zremrangebyrank(args, server, client),
        b"ZREMRANGEBYSCORE" => cmd_sorted_set_ranges::cmd_zremrangebyscore(args, server, client),
        b"ZREMRANGEBYLEX" => cmd_sorted_set_ranges::cmd_zremrangebylex(args, server, client),
        b"ZUNION" => cmd_sorted_set_setops::cmd_zunion(args, server, client),
        b"ZUNIONSTORE" => cmd_sorted_set_setops::cmd_zunionstore(args, server, client),
        b"ZINTER" => cmd_sorted_set_setops::cmd_zinter(args, server, client),
        b"ZINTERSTORE" => cmd_sorted_set_setops::cmd_zinterstore(args, server, client),
        b"ZINTERCARD" => cmd_sorted_set_setops::cmd_zintercard(args, server, client),
        b"ZDIFF" => cmd_sorted_set_setops::cmd_zdiff(args, server, client),
        b"ZDIFFSTORE" => cmd_sorted_set_setops::cmd_zdiffstore(args, server, client),
        b"ZPOPMIN" => cmd_sorted_set_pop::cmd_zpopmin(args, server, client),
        b"ZPOPMAX" => cmd_sorted_set_pop::cmd_zpopmax(args, server, client),
        b"ZMPOP" => cmd_sorted_set_pop::cmd_zmpop(args, server, client),
        b"ZRANDMEMBER" => cmd_sorted_set_sampling::cmd_zrandmember(args, server, client),
        b"ZSCAN" => cmd_sorted_set_sampling::cmd_zscan(args, server, client),
        b"BZPOPMIN" => cmd_sorted_set_pop::cmd_bzpopmin(args, server, client),
        b"BZPOPMAX" => cmd_sorted_set_pop::cmd_bzpopmax(args, server, client),
        b"BZMPOP" => cmd_sorted_set_pop::cmd_bzmpop(args, server, client),
        // Geo
        b"GEOADD" => cmd_geo::cmd_geoadd(args, server, client),
        b"GEOPOS" => cmd_geo::cmd_geopos(args, server, client),
        b"GEODIST" => cmd_geo::cmd_geodist(args, server, client),
        b"GEOHASH" => cmd_geo::cmd_geohash(args, server, client),
        b"GEOSEARCH" => cmd_geo_query::cmd_geosearch(args, server, client),
        b"GEOSEARCHSTORE" => cmd_geo_query::cmd_geosearchstore(args, server, client),
        b"GEORADIUS" => cmd_geo_radius::cmd_georadius(args, server, client, false),
        b"GEORADIUS_RO" => cmd_geo_radius::cmd_georadius_ro(args, server, client),
        b"GEORADIUSBYMEMBER" => cmd_geo_radius::cmd_georadiusbymember(args, server, client, false),
        b"GEORADIUSBYMEMBER_RO" => cmd_geo_radius::cmd_georadiusbymember_ro(args, server, client),
        // Bitmap
        b"SETBIT" => cmd_bitmap::cmd_setbit(args, server, client),
        b"GETBIT" => cmd_bitmap::cmd_getbit(args, server, client),
        b"BITCOUNT" => cmd_bitmap::cmd_bitcount(args, server, client),
        b"BITPOS" => cmd_bitmap::cmd_bitpos(args, server, client),
        b"BITOP" => cmd_bitmap::cmd_bitop(args, server, client),
        b"BITFIELD" => cmd_bitmap_bitfield::cmd_bitfield(args, server, client),
        b"BITFIELD_RO" => cmd_bitmap_bitfield::cmd_bitfield_ro(args, server, client),
        // Hash field TTL
        b"HEXPIRE" => cmd_hash_ttl::cmd_hexpire(args, server, client),
        b"HEXPIREAT" => cmd_hash_ttl::cmd_hexpireat(args, server, client),
        b"HPEXPIRE" => cmd_hash_ttl::cmd_hpexpire(args, server, client),
        b"HPEXPIREAT" => cmd_hash_ttl::cmd_hpexpireat(args, server, client),
        b"HTTL" => cmd_hash_ttl::cmd_httl(args, server, client),
        b"HPTTL" => cmd_hash_ttl::cmd_hpttl(args, server, client),
        b"HEXPIRETIME" => cmd_hash_ttl::cmd_hexpiretime(args, server, client),
        b"HPEXPIRETIME" => cmd_hash_ttl::cmd_hpexpiretime(args, server, client),
        b"HPERSIST" => cmd_hash_ttl::cmd_hpersist(args, server, client),
        b"HGETDEL" => cmd_hash_field_ops::cmd_hgetdel(args, server, client),
        b"HGETEX" => cmd_hash_field_ops::cmd_hgetex(args, server, client),
        b"HSETEX" => cmd_hash_field_ops::cmd_hsetex(args, server, client),
        // Cluster
        b"CLUSTER" => cmd_cluster::cmd_cluster(args, server, client),
        b"READONLY" => cmd_cluster::cmd_readonly(args),
        b"READWRITE" => cmd_cluster::cmd_readwrite(args),
        b"ASKING" => cmd_cluster::cmd_asking(args),
        // Scripting
        #[cfg(feature = "lua-scripting")]
        b"EVAL" => cmd_script::cmd_eval(args, server, client),
        #[cfg(feature = "lua-scripting")]
        b"EVALSHA" => cmd_script::cmd_evalsha(args, server, client),
        #[cfg(feature = "lua-scripting")]
        b"EVAL_RO" => cmd_script::cmd_eval_ro(args, server, client),
        #[cfg(feature = "lua-scripting")]
        b"EVALSHA_RO" => cmd_script::cmd_evalsha_ro(args, server, client),
        #[cfg(not(feature = "lua-scripting"))]
        b"EVAL" => cmd_script::cmd_eval(args),
        #[cfg(not(feature = "lua-scripting"))]
        b"EVALSHA" => cmd_script::cmd_evalsha(args),
        #[cfg(not(feature = "lua-scripting"))]
        b"EVAL_RO" => cmd_script::cmd_eval_ro(args),
        #[cfg(not(feature = "lua-scripting"))]
        b"EVALSHA_RO" => cmd_script::cmd_evalsha_ro(args),
        b"SCRIPT" => cmd_script::cmd_script(args, server),
        b"FCALL" => cmd_script::cmd_fcall(args),
        b"FCALL_RO" => cmd_script::cmd_fcall_ro(args),
        b"FUNCTION" => cmd_script::cmd_function(args),
        // Sentinel
        b"SENTINEL" => cmd_sentinel::cmd_sentinel(args),
        b"APPEND" => cmd_string::cmd_append(args, server, client),
        b"STRLEN" => cmd_string::cmd_strlen(args, server, client),
        b"GETRANGE" | b"SUBSTR" => cmd_string::cmd_getrange(args, server, client),
        b"SETRANGE" => cmd_string::cmd_setrange(args, server, client),
        b"GETSET" => cmd_string::cmd_getset(args, server, client),
        b"SETNX" => cmd_string::cmd_setnx(args, server, client),
        b"MGET" => cmd_string::cmd_mget(args, server, client),
        b"MSET" => cmd_string::cmd_mset(args, server, client),
        b"MSETEX" => cmd_generic_string::cmd_msetex(args, server, client),
        b"MSETNX" => cmd_string::cmd_msetnx(args, server, client),
        b"INCR" => cmd_string_numeric::cmd_incr(args, server, client),
        b"INCRBY" => cmd_string_numeric::cmd_incrby(args, server, client),
        b"DECR" => cmd_string_numeric::cmd_decr(args, server, client),
        b"DECRBY" => cmd_string_numeric::cmd_decrby(args, server, client),
        b"INCRBYFLOAT" => cmd_string_numeric::cmd_incrbyfloat(args, server, client),
        b"SETEX" => cmd_string_access::cmd_setex_with_mode(
            args,
            server,
            client,
            ExpireMode::RelativeSeconds,
            "setex",
        ),
        b"PSETEX" => cmd_string_access::cmd_setex_with_mode(
            args,
            server,
            client,
            ExpireMode::RelativeMilliseconds,
            "psetex",
        ),
        b"GET" => cmd_string_access::cmd_get(args, server, client),
        b"GETDEL" => cmd_string_access::cmd_getdel(args, server, client),
        b"GETEX" => cmd_string_access::cmd_getex(args, server, client),
        b"DEL" => cmd_key::cmd_del(args, server, client),
        b"EXISTS" => cmd_key::cmd_exists(args, server, client),
        b"SELECT" => cmd_key::cmd_select(args, server, client),
        b"EXPIRE" => cmd_key::cmd_expire_with_mode(
            args,
            server,
            client,
            ExpireMode::RelativeSeconds,
            "expire",
        ),
        b"PEXPIRE" => cmd_key::cmd_expire_with_mode(
            args,
            server,
            client,
            ExpireMode::RelativeMilliseconds,
            "pexpire",
        ),
        b"EXPIREAT" => cmd_key::cmd_expire_with_mode(
            args,
            server,
            client,
            ExpireMode::AbsoluteSeconds,
            "expireat",
        ),
        b"PEXPIREAT" => cmd_key::cmd_expire_with_mode(
            args,
            server,
            client,
            ExpireMode::AbsoluteMilliseconds,
            "pexpireat",
        ),
        b"PERSIST" => cmd_key::cmd_persist(args, server, client),
        b"PFADD" => cmd_hll::cmd_pfadd(args, server, client),
        b"PFCOUNT" => cmd_hll::cmd_pfcount(args, server, client),
        b"PFMERGE" => cmd_hll::cmd_pfmerge(args, server, client),
        b"PFDEBUG" => cmd_hll::cmd_pfdebug(args, server, client),
        b"PFSELFTEST" => cmd_hll::cmd_pfselftest(args, server, client),
        b"TTL" => cmd_key::cmd_ttl_with_mode(args, server, client, TtlMode::Seconds, "ttl"),
        b"PTTL" => cmd_key::cmd_ttl_with_mode(args, server, client, TtlMode::Milliseconds, "pttl"),
        b"EXPIRETIME" => cmd_key::cmd_expiretime_with_mode(
            args,
            server,
            client,
            ExpireTimeMode::Seconds,
            "expiretime",
        ),
        b"PEXPIRETIME" => cmd_key::cmd_expiretime_with_mode(
            args,
            server,
            client,
            ExpireTimeMode::Milliseconds,
            "pexpiretime",
        ),
        _ => {
            let name = String::from_utf8_lossy(command_raw).to_string();
            CommandOutcome::reply(err(&format!("ERR unknown command '{name}'")))
        }
    };

    let elapsed_us =
        started.map(|started| i64::try_from(started.elapsed().as_micros()).unwrap_or(i64::MAX));
    apply_post_execute_side_effects_with_spec(
        server,
        client,
        command.as_slice(),
        spec,
        argv,
        &outcome.response,
        elapsed_us,
        track_slowlog,
        track_latency,
    );

    // `cmd_exec` gathers the effects of its nested commands itself.  Replacing
    // them here with a bare EXEC would make recovery both incomplete and
    // vulnerable to a torn transaction tail.
    if client.durability_capture_enabled && command.as_slice() != b"EXEC" {
        client.set_durability_effects(capture_durability_effects(
            command.as_slice(),
            argv,
            server,
            selected_db_before,
            &outcome.response,
        ));
    }

    outcome
}

fn capture_durability_effects(
    command: &[u8],
    argv: &[Bytes],
    server: &ServerState,
    db_index: usize,
    response: &RespFrame,
) -> Option<DurabilityEffects> {
    if argv.is_empty() || matches!(response, RespFrame::Error(_)) {
        return None;
    }

    let canonical = match command {
        // Relative expiry must never be replayed relative to the restart
        // clock.  Reading the result from the keyspace also handles KEEPTTL,
        // NX/XX and SET GET without trusting the shape of their reply.
        b"SET" if set_command_was_applied(argv, server, db_index, response) => {
            canonical_string_state(server, db_index, argv.get(1)?)
        }
        b"SETEX" | b"PSETEX" => canonical_string_state(server, db_index, argv.get(1)?),
        b"GETEX" if argv.len() > 2 && matches!(response, RespFrame::BulkString(Some(_))) => {
            canonical_string_state(server, db_index, argv.get(1)?)
        }
        b"EXPIRE" | b"PEXPIRE" | b"EXPIREAT" | b"PEXPIREAT"
            if matches!(response, RespFrame::Integer(1)) =>
        {
            canonical_expiry_state(server, db_index, argv.get(1)?)
        }
        // XADD resolves `*` (and the supported partial auto-ID form) while it
        // mutates the stream.  Replay the returned concrete ID, never the
        // caller's generator token.
        b"XADD" => canonical_xadd(argv, response),
        b"SPOP" => {
            let members = match response {
                RespFrame::BulkString(Some(member)) => vec![member.clone()],
                RespFrame::Array(items) => items
                    .iter()
                    .filter_map(|item| match item {
                        RespFrame::BulkString(Some(member)) => Some(member.clone()),
                        _ => None,
                    })
                    .collect(),
                _ => Vec::new(),
            };
            if members.is_empty() {
                return None;
            }
            let mut removal = vec![Bytes::from_static(b"SREM"), argv.get(1)?.clone()];
            removal.extend(members);
            Some(removal)
        }
        // These commands are administrative/session actions, not keyspace
        // mutations.  In particular, persist neither MULTI/EXEC wrappers nor
        // a successful CONFIG reply as if it were data.
        b"MULTI" | b"EXEC" | b"DISCARD" | b"WATCH" | b"UNWATCH" | b"SELECT" | b"CONFIG"
        | b"SAVE" | b"BGSAVE" | b"BGREWRITEAOF" | b"PUBLISH" | b"SPUBLISH" => None,
        _ if is_write_command(argv) && raw_command_changed_state(command, response) => {
            Some(argv.to_vec())
        }
        _ => None,
    }?;

    Some(DurabilityEffects::single(db_index, canonical))
}

fn set_command_was_applied(
    argv: &[Bytes],
    server: &ServerState,
    db_index: usize,
    response: &RespFrame,
) -> bool {
    let has_condition = argv
        .iter()
        .skip(3)
        .any(|option| option.eq_ignore_ascii_case(b"NX") || option.eq_ignore_ascii_case(b"XX"));
    if !has_condition {
        return true;
    }

    if matches!(response, RespFrame::SimpleString(value) if value.eq_ignore_ascii_case(b"OK")) {
        return true;
    }

    // SET ... GET returns the prior value rather than OK.  If a condition was
    // met, the post-state contains the requested value.  A same-value no-op
    // produces an identical canonical state, which is still safe to replay.
    argv.get(2).is_some_and(|requested| {
        server
            .db(db_index)
            .get(argv.get(1).expect("SET key checked by dispatcher"))
            .and_then(|entry| entry.as_string_bytes())
            .is_some_and(|actual| actual == *requested)
    })
}

fn canonical_string_state(
    server: &ServerState,
    db_index: usize,
    key: &Bytes,
) -> Option<Vec<Bytes>> {
    let entry = server.db(db_index).get(key).cloned();
    match entry {
        Some(entry) => {
            let value = entry.as_string_bytes()?;
            let mut argv = vec![Bytes::from_static(b"SET"), key.clone(), value];
            if let Some(expire_at_ms) = entry.expire_at_ms() {
                argv.push(Bytes::from_static(b"PXAT"));
                argv.push(Bytes::from(expire_at_ms.to_string()));
            }
            Some(argv)
        }
        // Commands such as SET PXAT in the past and GETEX with an elapsed
        // absolute deadline are durable deletions, not a restarted TTL.
        None => Some(vec![Bytes::from_static(b"DEL"), key.clone()]),
    }
}

fn canonical_expiry_state(
    server: &ServerState,
    db_index: usize,
    key: &Bytes,
) -> Option<Vec<Bytes>> {
    let entry = server.db(db_index).get(key).cloned();
    match entry {
        Some(entry) => entry.expire_at_ms().map(|expire_at_ms| {
            vec![
                Bytes::from_static(b"PEXPIREAT"),
                key.clone(),
                Bytes::from(expire_at_ms.to_string()),
            ]
        }),
        None => Some(vec![Bytes::from_static(b"DEL"), key.clone()]),
    }
}

fn canonical_xadd(argv: &[Bytes], response: &RespFrame) -> Option<Vec<Bytes>> {
    let RespFrame::BulkString(Some(id)) = response else {
        return None;
    };
    let mut canonical = argv.to_vec();
    let id_slot = canonical.get_mut(2)?;
    *id_slot = id.clone();
    Some(canonical)
}

fn raw_command_changed_state(command: &[u8], response: &RespFrame) -> bool {
    // A small set of common conditional/no-op responses must not be admitted
    // as replay input.  Other data commands retain their original argv; their
    // ordering is now against one authoritative AOF lineage.
    match command {
        b"DEL" | b"UNLINK" | b"SETNX" | b"MSETNX" | b"HSETNX" | b"PERSIST" => {
            !matches!(response, RespFrame::Integer(0))
        }
        _ => true,
    }
}

pub fn execute(
    frame: RespFrame,
    access: &mut ServerAccess<'_>,
    client: &mut ClientState,
) -> CommandOutcome {
    let argv = match frame_to_argv(frame) {
        Ok(argv) => argv,
        Err(response) => {
            client.set_durability_effects(None);
            return CommandOutcome::reply(response);
        }
    };

    execute_argv(&argv, access, client)
}

pub fn post_execute_tracking_flags(server: &ServerState, argv: &[Bytes]) -> (bool, bool) {
    let Some(command_raw) = argv.first() else {
        return (false, false);
    };
    let command = to_uppercase_stack(command_raw);
    (
        should_track_slowlog(server, command.as_slice()),
        should_track_latency(server, command.as_slice()),
    )
}

pub fn supports_readonly_batch_command(command_raw: &[u8]) -> bool {
    let command = to_uppercase_stack(command_raw);
    let Some(spec) = find_command_spec_upper(command.as_slice()) else {
        return false;
    };

    spec.flags.contains(&"readonly")
        && !spec.flags.contains(&"blocking")
        && !spec.flags.contains(&"connection")
        && !spec.flags.contains(&"pubsub")
        && !matches!(command.as_slice(), b"WAIT" | b"WAITAOF")
}

#[allow(clippy::too_many_arguments)]
pub fn apply_post_execute_side_effects(
    server: &mut ServerState,
    client: &mut ClientState,
    argv: &[Bytes],
    response: &RespFrame,
    elapsed_us: Option<i64>,
    track_slowlog: bool,
    track_latency: bool,
) {
    let Some(command_raw) = argv.first() else {
        return;
    };
    let command = to_uppercase_stack(command_raw);
    let spec = find_command_spec_upper(command.as_slice());
    apply_post_execute_side_effects_with_spec(
        server,
        client,
        command.as_slice(),
        spec,
        argv,
        response,
        elapsed_us,
        track_slowlog,
        track_latency,
    );
}

/// Post-execute bookkeeping for callers that already resolved the upper-cased
/// command name and its spec, so the dispatch path does not look them up twice.
#[allow(clippy::too_many_arguments)]
fn apply_post_execute_side_effects_with_spec(
    server: &mut ServerState,
    client: &mut ClientState,
    command: &[u8],
    spec: Option<CommandSpec>,
    argv: &[Bytes],
    response: &RespFrame,
    elapsed_us: Option<i64>,
    track_slowlog: bool,
    track_latency: bool,
) {
    if let Some(elapsed_us) = elapsed_us {
        if track_slowlog {
            maybe_track_slowlog(server, command, argv, elapsed_us);
        }
        if track_latency {
            maybe_track_latency(server, command, elapsed_us);
        }
    }

    if let Some(spec) = spec {
        if spec.flags.contains(&"write") {
            maybe_track_write_version(server, client, command, argv, response, spec);
        } else if spec.flags.contains(&"readonly") {
            maybe_track_client_tracking_access(server, client, argv, response, spec);
        }
    }
    client.finish_tracking_command(command, argv);

    // MONITOR dispatch — broadcast formatted command to all monitoring clients.
    // The guard check (`has_monitors`) is a single HashSet::is_empty() check,
    // so the hot-path cost is negligible when no monitors are active.
    if server.has_monitors() && command != b"MONITOR" {
        let line = format_monitor_line(server, client, argv);
        server.broadcast_monitor_message(client.id(), line);
    }
}

/// Format a Redis MONITOR output line.
///
/// Redis format: `+{unix_timestamp}.{microseconds} [db {addr}] "CMD" "arg1" ...`
fn format_monitor_line(server: &ServerState, client: &ClientState, argv: &[Bytes]) -> Bytes {
    use std::fmt::Write;

    let total_us = crate::object::now_us();
    let secs = total_us / 1_000_000;
    let micros = total_us % 1_000_000;
    let db = client.selected_db();

    // Look up the client address and transport flag from the snapshot registry.
    let (addr, unix_socket) = server
        .client_snapshot(client.id())
        .map(|snap| (snap.addr.clone(), snap.flags.contains(&b'U')))
        .unwrap_or_else(|| (Bytes::from_static(b"127.0.0.1:0"), false));
    let addr = String::from_utf8_lossy(&addr);
    let display_addr = if unix_socket {
        format!("unix:{}", addr.strip_suffix(":0").unwrap_or(addr.as_ref()))
    } else {
        addr.into_owned()
    };

    let mut buf = String::with_capacity(128);
    let _ = write!(buf, "{}.{:06} [{} {}]", secs, micros, db, display_addr);
    for arg in argv {
        buf.push(' ');
        buf.push('"');
        for &byte in arg.as_ref() {
            match byte {
                b'"' => buf.push_str("\\\""),
                b'\\' => buf.push_str("\\\\"),
                0x20..=0x7e => buf.push(byte as char),
                _ => {
                    let _ = write!(buf, "\\x{:02x}", byte);
                }
            }
        }
        buf.push('"');
    }

    Bytes::from(buf)
}

fn maybe_track_client_tracking_access(
    server: &mut ServerState,
    client: &ClientState,
    argv: &[Bytes],
    response: &RespFrame,
    spec: CommandSpec,
) {
    if !client.tracking_should_record_reads() || matches!(response, RespFrame::Error(_)) {
        return;
    }

    let target_client_id = server.tracking_target_client_id(client.id(), client.tracking_redirect);
    cmd_command_metadata::for_each_command_key_position(spec, argv.len(), |pos| {
        if let Some(key) = argv.get(pos) {
            server.tracking_register_key(
                client.id(),
                target_client_id,
                client.tracking_no_loop(),
                client.selected_db,
                key.clone(),
            );
        }
    });
}

/// Keys touched by a write, collected for tracking invalidation and
/// blocked-client wakeups.
///
/// Stays empty without allocating when no client observes writes, which is
/// the common case on the hot write path.
struct TouchedKeys {
    keys: Vec<(usize, Bytes)>,
    observed: bool,
}

impl TouchedKeys {
    fn new(server: &ServerState) -> Self {
        Self {
            keys: Vec::new(),
            observed: server.write_observers_active(),
        }
    }

    fn push(&mut self, db_idx: usize, key: &Bytes) {
        if self.observed {
            self.keys.push((db_idx, key.clone()));
        }
    }
}

fn invalidate_tracked_keys(
    server: &mut ServerState,
    writer_client_id: i64,
    touched_keys: TouchedKeys,
) {
    if touched_keys.keys.is_empty() {
        return;
    }
    let mut keys_by_db = HashBrownMap::<usize, Vec<Bytes>>::new();
    for (db_idx, key) in touched_keys.keys {
        keys_by_db.entry(db_idx).or_default().push(key);
    }
    for (db_idx, keys) in keys_by_db {
        server.notify_blocked_clients(db_idx, keys.iter().cloned());
        server.tracking_invalidate_keys(writer_client_id, db_idx, keys);
    }
}

fn maybe_track_slowlog(server: &mut ServerState, command: &[u8], argv: &[Bytes], duration_us: i64) {
    if command == b"SLOWLOG" {
        return;
    }

    server.stats.append_slowlog(duration_us, argv);
}

fn maybe_track_latency(server: &mut ServerState, command: &[u8], duration_us: i64) {
    if matches!(command, b"LATENCY" | b"SLOWLOG") {
        return;
    }

    let ms = (duration_us / 1000).max(0);
    server.stats.record_latency_sample(command, ms);
}

fn should_track_slowlog(server: &ServerState, command: &[u8]) -> bool {
    command != b"SLOWLOG" && server.stats.slowlog_tracking_enabled()
}

fn should_track_latency(server: &ServerState, command: &[u8]) -> bool {
    !matches!(command, b"LATENCY" | b"SLOWLOG") && server.stats.latency_tracking_enabled()
}

const ACL_CATEGORY_ADMIN: u8 = 1 << 0;
const ACL_CATEGORY_WRITE: u8 = 1 << 1;
const ACL_CATEGORY_READ: u8 = 1 << 2;
const ACL_CATEGORY_PUBSUB: u8 = 1 << 3;
const ACL_CATEGORY_CONNECTION: u8 = 1 << 4;
const ACL_CATEGORY_FAST: u8 = 1 << 5;

fn acl_required_category_mask(spec: CommandSpec) -> u8 {
    let mut required = 0u8;

    if spec.flags.contains(&"admin") {
        required |= ACL_CATEGORY_ADMIN;
    }
    if spec.flags.contains(&"write") {
        required |= ACL_CATEGORY_WRITE;
    }
    if spec.flags.contains(&"readonly") {
        required |= ACL_CATEGORY_READ;
    }
    if spec.flags.contains(&"pubsub") {
        required |= ACL_CATEGORY_PUBSUB;
    }
    if spec.flags.contains(&"connection") {
        required |= ACL_CATEGORY_CONNECTION;
    }

    if required == 0 && spec.flags.contains(&"fast") {
        required |= ACL_CATEGORY_FAST;
    }

    required
}

fn acl_required_categories(spec: CommandSpec) -> Vec<&'static [u8]> {
    let mut required = Vec::new();

    if spec.flags.contains(&"admin") {
        required.push(b"admin" as &[u8]);
    }
    if spec.flags.contains(&"write") {
        required.push(b"write" as &[u8]);
    }
    if spec.flags.contains(&"readonly") {
        required.push(b"read" as &[u8]);
    }
    if spec.flags.contains(&"pubsub") {
        required.push(b"pubsub" as &[u8]);
    }
    if spec.flags.contains(&"connection") {
        required.push(b"connection" as &[u8]);
    }

    if required.is_empty() && spec.flags.contains(&"fast") {
        required.push(b"fast" as &[u8]);
    }

    required
}

pub fn command_name(argv: &[Bytes]) -> Option<Bytes> {
    let command = argv.first()?;
    Some(Bytes::copy_from_slice(
        to_uppercase_stack(command).as_slice(),
    ))
}

pub fn is_write_command(argv: &[Bytes]) -> bool {
    let Some(name) = command_name(argv) else {
        return false;
    };
    is_write_command_name(name.as_ref())
}

fn validate_queued_command(
    argv: &[Bytes],
    server: &ServerState,
    client: &ClientState,
) -> Result<(), RespFrame> {
    let Some(spec) = find_command_spec(&argv[0]) else {
        let name = String::from_utf8_lossy(&argv[0]).to_string();
        return Err(err(&format!("ERR unknown command '{name}'")));
    };

    if !cmd_command_metadata::command_arity_matches(spec.arity, argv.len()) {
        return Err(err(&format!(
            "ERR wrong number of arguments for '{}' command",
            spec.name.to_ascii_lowercase(),
        )));
    }

    if !spec.flags.contains(&"no_auth") {
        let required_mask = acl_required_category_mask(spec);
        if !server
            .acl
            .command_allowed_mask(&client.acl_user, required_mask)
        {
            return Err(err(
                "NOPERM this user has no permissions to run the command",
            ));
        }
    }

    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn maybe_track_write_version(
    server: &mut ServerState,
    client: &ClientState,
    command: &[u8],
    argv: &[Bytes],
    response: &RespFrame,
    spec: CommandSpec,
) {
    if matches!(response, RespFrame::Error(_)) {
        return;
    }

    let integer_zero = matches!(response, RespFrame::Integer(0));
    let should_mark = match command {
        b"SET" => !matches!(response, RespFrame::BulkString(None)),
        b"GETEX" => argv.len() > 2,
        b"LMOVE" | b"BLMOVE" | b"RPOPLPUSH" | b"BRPOPLPUSH" | b"LMPOP" | b"BLMPOP" => {
            !matches!(response, RespFrame::BulkString(None))
        }
        b"SORT" => argv.iter().any(|arg| arg.eq_ignore_ascii_case(b"STORE")),
        b"SETNX" | b"MSETNX" | b"MSETEX" | b"RENAMENX" | b"MOVE" | b"COPY" | b"EXPIRE"
        | b"PEXPIRE" | b"EXPIREAT" | b"PEXPIREAT" | b"PERSIST" | b"DEL" | b"DELEX" | b"UNLINK"
        | b"HDEL" | b"SADD" | b"SREM" | b"LPUSHX" | b"RPUSHX" | b"LREM" | b"SMOVE" | b"XDEL"
        | b"XTRIM" | b"ZREM" | b"ZREMRANGEBYRANK" | b"ZREMRANGEBYSCORE" | b"ZREMRANGEBYLEX" => {
            !integer_zero
        }
        b"LPOP" | b"RPOP" | b"BLPOP" | b"BRPOP" | b"SPOP" | b"ZPOPMIN" | b"ZPOPMAX"
        | b"BZPOPMIN" | b"BZPOPMAX" => match response {
            RespFrame::BulkString(None) => false,
            RespFrame::Array(values) => !values.is_empty(),
            _ => true,
        },
        b"XACKDEL" | b"XDELEX" => match response {
            RespFrame::Array(values) => values.iter().any(|v| *v != RespFrame::Integer(-1)),
            _ => true,
        },
        b"MIGRATE" | b"XCLAIM" | b"XAUTOCLAIM" => false,
        _ => true,
    };

    if !should_mark {
        return;
    }

    let mut touched_keys = TouchedKeys::new(server);

    if command == b"SET" {
        if let Some(key) = argv.get(1) {
            server.touch_key_version(client.selected_db, key.clone());
            touched_keys.push(client.selected_db, key);
        }
        invalidate_tracked_keys(server, client.id(), touched_keys);
        server.advance_replication_offset();
        return;
    }

    if command == b"SORT" {
        let mut idx = 2usize;
        while idx < argv.len() {
            if argv[idx].eq_ignore_ascii_case(b"STORE") {
                if let Some(dest_key) = argv.get(idx + 1) {
                    server.touch_key_version(client.selected_db, dest_key.clone());
                    touched_keys.push(client.selected_db, dest_key);
                }
                break;
            }
            idx += 1;
        }
        invalidate_tracked_keys(server, client.id(), touched_keys);
        server.advance_replication_offset();
        return;
    }

    if command == b"COPY" {
        if let Some(target_key) = argv.get(2) {
            let mut target_db = client.selected_db;
            let mut idx = 3usize;
            while idx < argv.len() {
                if argv[idx].eq_ignore_ascii_case(b"DB") {
                    if let Some(db_raw) = argv.get(idx + 1) {
                        if let Some(parsed) = parse_usize(db_raw) {
                            if parsed < server.db_count() {
                                target_db = parsed;
                            }
                        }
                    }
                    break;
                }
                idx += 1;
            }
            server.touch_key_version(target_db, target_key.clone());
            touched_keys.push(target_db, target_key);
        }
        invalidate_tracked_keys(server, client.id(), touched_keys);
        server.advance_replication_offset();
        return;
    }

    if command == b"MOVE" {
        if let Some(key) = argv.get(1) {
            server.touch_key_version(client.selected_db, key.clone());
            touched_keys.push(client.selected_db, key);
            if let Some(target_db_raw) = argv.get(2) {
                if let Some(target_db) = parse_usize(target_db_raw) {
                    if target_db < server.db_count() {
                        server.touch_key_version(target_db, key.clone());
                        touched_keys.push(target_db, key);
                    }
                }
            }
        }
        invalidate_tracked_keys(server, client.id(), touched_keys);
        server.advance_replication_offset();
        return;
    }

    if command == b"MSETEX" {
        let Some(numkeys_raw) = argv.get(1) else {
            return;
        };
        let Some(numkeys) = parse_usize(numkeys_raw) else {
            return;
        };

        let mut idx = 2usize;
        for _ in 0..numkeys {
            if idx >= argv.len() {
                break;
            }
            server.touch_key_version(client.selected_db, argv[idx].clone());
            touched_keys.push(client.selected_db, &argv[idx]);
            idx = idx.saturating_add(2);
        }
        invalidate_tracked_keys(server, client.id(), touched_keys);
        server.advance_replication_offset();
        return;
    }

    cmd_command_metadata::for_each_command_key_position(spec, argv.len(), |pos| {
        if let Some(key) = argv.get(pos) {
            server.touch_key_version(client.selected_db, key.clone());
            touched_keys.push(client.selected_db, key);
        }
    });
    invalidate_tracked_keys(server, client.id(), touched_keys);
    server.advance_replication_offset();
}

fn frame_to_argv(frame: RespFrame) -> Result<SmallVec<[Bytes; 16]>, RespFrame> {
    let RespFrame::Array(items) = frame else {
        return Err(err("ERR protocol error: expected array command frame"));
    };

    let mut out = SmallVec::<[Bytes; 16]>::with_capacity(items.len());
    for item in items {
        match item {
            RespFrame::BulkString(Some(value)) => out.push(value),
            RespFrame::SimpleString(value) => out.push(value),
            RespFrame::Integer(value) => out.push(Bytes::from(value.to_string())),
            RespFrame::BulkString(None) => {
                return Err(err("ERR protocol error: null bulk command argument"));
            }
            _ => {
                return Err(err("ERR protocol error: unsupported command argument type"));
            }
        }
    }

    Ok(out)
}

fn wrong_type_response() -> CommandOutcome {
    CommandOutcome::reply(RespFrame::wrongtype())
}

fn parse_expire_condition(raw: &Bytes) -> Option<ExpireCondition> {
    if raw.eq_ignore_ascii_case(b"NX") {
        Some(ExpireCondition::Nx)
    } else if raw.eq_ignore_ascii_case(b"XX") {
        Some(ExpireCondition::Xx)
    } else if raw.eq_ignore_ascii_case(b"GT") {
        Some(ExpireCondition::Gt)
    } else if raw.eq_ignore_ascii_case(b"LT") {
        Some(ExpireCondition::Lt)
    } else {
        None
    }
}

fn to_expire_target_ms(raw: i64, mode: ExpireMode, now_ms: i64) -> i64 {
    match mode {
        ExpireMode::RelativeSeconds => now_ms.saturating_add(raw.saturating_mul(1000)),
        ExpireMode::RelativeMilliseconds => now_ms.saturating_add(raw),
        ExpireMode::AbsoluteSeconds => raw.saturating_mul(1000),
        ExpireMode::AbsoluteMilliseconds => raw,
    }
}

fn parse_command_expire_at_ms(
    raw: &Bytes,
    mode: ExpireMode,
    now_ms: i64,
    command_name: &str,
) -> Result<i64, RespFrame> {
    let Some(value) = parse_i64(raw) else {
        return Err(err("ERR value is not an integer or out of range"));
    };

    let invalid_error = format!("ERR invalid expire time in '{command_name}' command");
    if value <= 0 {
        return Err(err(&invalid_error));
    }

    let at_ms = match mode {
        ExpireMode::RelativeSeconds => {
            let delta = value.checked_mul(1000).ok_or_else(|| err(&invalid_error))?;
            now_ms
                .checked_add(delta)
                .ok_or_else(|| err(&invalid_error))?
        }
        ExpireMode::RelativeMilliseconds => now_ms
            .checked_add(value)
            .ok_or_else(|| err(&invalid_error))?,
        ExpireMode::AbsoluteSeconds => {
            value.checked_mul(1000).ok_or_else(|| err(&invalid_error))?
        }
        ExpireMode::AbsoluteMilliseconds => value,
    };

    if at_ms <= 0 {
        return Err(err(&invalid_error));
    }

    Ok(at_ms)
}

fn parse_getex_policy(args: &[Bytes], now_ms: i64) -> Result<GetExPolicy, RespFrame> {
    if args.is_empty() {
        return Ok(GetExPolicy::KeepTtl);
    }

    let opt = &args[0];
    if opt.eq_ignore_ascii_case(b"PERSIST") {
        if args.len() != 1 {
            return Err(err("ERR syntax error"));
        }
        Ok(GetExPolicy::Persist)
    } else if opt.eq_ignore_ascii_case(b"EX")
        || opt.eq_ignore_ascii_case(b"PX")
        || opt.eq_ignore_ascii_case(b"EXAT")
        || opt.eq_ignore_ascii_case(b"PXAT")
    {
        if args.len() != 2 {
            return Err(err("ERR syntax error"));
        }

        let mode = if opt.eq_ignore_ascii_case(b"EX") {
            ExpireMode::RelativeSeconds
        } else if opt.eq_ignore_ascii_case(b"PX") {
            ExpireMode::RelativeMilliseconds
        } else if opt.eq_ignore_ascii_case(b"EXAT") {
            ExpireMode::AbsoluteSeconds
        } else {
            ExpireMode::AbsoluteMilliseconds
        };
        let expire_at_ms = parse_command_expire_at_ms(&args[1], mode, now_ms, "getex")?;
        Ok(GetExPolicy::AtMs(expire_at_ms))
    } else {
        Err(err("ERR syntax error"))
    }
}

fn expire_condition_matches(condition: ExpireCondition, current: Option<i64>, target: i64) -> bool {
    match condition {
        ExpireCondition::None => true,
        ExpireCondition::Nx => current.is_none(),
        ExpireCondition::Xx => current.is_some(),
        ExpireCondition::Gt => current.is_some_and(|cur| target > cur),
        ExpireCondition::Lt => current.is_none_or(|cur| target < cur),
    }
}

fn now_ms() -> i64 {
    ratatosk_core::time::now_ms()
}

pub(super) fn now_client_clock_ms() -> i64 {
    i64::try_from(ratatosk_core::time::monotonic_ms()).unwrap_or(i64::MAX)
}

fn parse_i64(raw: &Bytes) -> Option<i64> {
    crate::object::parse_i64(raw)
}

fn parse_usize(raw: &Bytes) -> Option<usize> {
    crate::object::parse_usize(raw)
}

fn wrong_arity(command: &str) -> CommandOutcome {
    CommandOutcome::reply(err(&format!(
        "ERR wrong number of arguments for '{command}' command"
    )))
}

fn err(message: &str) -> RespFrame {
    let sanitized = sanitize_error_message(message);
    RespFrame::error_str(&sanitized)
}

pub(super) enum UpperBuf<'a> {
    Borrowed(&'a [u8]),
    Stack([u8; 32], usize),
    Heap(Vec<u8>),
}

impl UpperBuf<'_> {
    pub(super) fn as_slice(&self) -> &[u8] {
        match self {
            Self::Borrowed(slice) => slice,
            Self::Stack(buf, len) => &buf[..*len],
            Self::Heap(vec) => vec.as_slice(),
        }
    }
}

pub(super) fn to_uppercase_stack(input: &[u8]) -> UpperBuf<'_> {
    // Fast path: most clients already send uppercase commands.
    if !input.iter().any(u8::is_ascii_lowercase) {
        return UpperBuf::Borrowed(input);
    }

    if input.len() <= 32 {
        let mut buf = [0u8; 32];
        for (i, &b) in input.iter().enumerate() {
            buf[i] = b.to_ascii_uppercase();
        }
        UpperBuf::Stack(buf, input.len())
    } else {
        UpperBuf::Heap(input.iter().map(|byte| byte.to_ascii_uppercase()).collect())
    }
}

fn to_uppercase_bytes(input: &Bytes) -> Vec<u8> {
    let mut out = Vec::with_capacity(input.len());
    out.extend(input.iter().map(|byte| byte.to_ascii_uppercase()));
    out
}

#[cfg(test)]
mod tests {
    use std::{
        fs,
        time::{Duration, SystemTime, UNIX_EPOCH},
    };

    use bytes::Bytes;
    use ratatosk_resp::frame::RespFrame;

    use crate::keyspace::{DefaultAclPolicyState, PubSubMessage, PubSubState};

    use super::{
        AtomicStatsState, ClientState, CommandOutcome, ExecuteArgvPrecheck, ServerAccess,
        ServerState, command_spec_count, execute, now_ms, precheck_execute_argv_with_default_acl,
    };

    fn cmd(parts: &[&str]) -> RespFrame {
        RespFrame::Array(parts.iter().map(|p| RespFrame::bulk_str(p)).collect())
    }

    fn run(parts: &[&str], server: &mut ServerState, client: &mut ClientState) -> RespFrame {
        let mut access = ServerAccess::new_inline(server);
        execute(cmd(parts), &mut access, client).response
    }

    fn run_outcome(
        parts: &[&str],
        server: &mut ServerState,
        client: &mut ClientState,
    ) -> CommandOutcome {
        let mut access = ServerAccess::new_inline(server);
        execute(cmd(parts), &mut access, client)
    }

    fn run_with_atomic(
        parts: &[&str],
        server: &mut ServerState,
        atomic_stats: &AtomicStatsState,
        client: &mut ClientState,
    ) -> RespFrame {
        let mut access = ServerAccess::new_with_atomic_stats(server, atomic_stats);
        execute(cmd(parts), &mut access, client).response
    }

    fn run_full(
        parts: &[&str],
        server: &mut ServerState,
        client: &mut ClientState,
    ) -> CommandOutcome {
        let mut access = ServerAccess::new_inline(server);
        execute(cmd(parts), &mut access, client)
    }

    fn pubsub_ack(kind: &str, target: Option<&str>, count: i64) -> RespFrame {
        RespFrame::Sequence(vec![RespFrame::Push(vec![
            RespFrame::bulk_str(kind),
            target.map_or(RespFrame::BulkString(None), RespFrame::bulk_str),
            RespFrame::Integer(count),
        ])])
    }

    fn unique_test_path(prefix: &str) -> std::path::PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system time before unix epoch")
            .as_nanos();
        std::env::temp_dir().join(format!("ratatosk-{prefix}-{nanos}"))
    }

    fn assert_array_set_eq(actual: &RespFrame, expected: &[&str]) {
        let RespFrame::Array(items) = actual else {
            panic!("expected Array, got {actual:?}");
        };
        let mut actual_set: Vec<&[u8]> = items
            .iter()
            .map(|f| match f {
                RespFrame::BulkString(Some(b)) => b.as_ref(),
                _ => panic!("expected BulkString, got {f:?}"),
            })
            .collect();
        actual_set.sort();
        let mut expected_set: Vec<&[u8]> = expected.iter().map(|s| s.as_bytes()).collect();
        expected_set.sort();
        assert_eq!(actual_set, expected_set);
    }

    fn assert_map_pairs_eq(actual: &RespFrame, expected_pairs: &[(&str, &str)]) {
        let RespFrame::Map(entries) = actual else {
            panic!("expected Map, got {actual:?}");
        };
        let mut actual_pairs: Vec<(&[u8], &[u8])> = entries
            .iter()
            .map(|(key, value)| {
                let k = match key {
                    RespFrame::BulkString(Some(b)) => b.as_ref(),
                    _ => panic!("expected BulkString key"),
                };
                let v = match value {
                    RespFrame::BulkString(Some(b)) => b.as_ref(),
                    _ => panic!("expected BulkString value"),
                };
                (k, v)
            })
            .collect();
        actual_pairs.sort();
        let mut expected: Vec<(&[u8], &[u8])> = expected_pairs
            .iter()
            .map(|(k, v)| (k.as_bytes(), v.as_bytes()))
            .collect();
        expected.sort();
        assert_eq!(actual_pairs, expected);
    }

    #[test]
    fn set_get_del_exists_flow() {
        let mut server = ServerState::with_default_dbs();
        let mut client = ClientState::default();

        assert_eq!(
            run(&["SET", "foo", "bar"], &mut server, &mut client),
            RespFrame::ok()
        );
        assert_eq!(
            run(&["GET", "foo"], &mut server, &mut client),
            RespFrame::bulk_str("bar")
        );
        assert_eq!(
            run(&["EXISTS", "foo", "missing"], &mut server, &mut client),
            RespFrame::Integer(1)
        );
        assert_eq!(
            run(&["DEL", "foo"], &mut server, &mut client),
            RespFrame::Integer(1)
        );
        assert_eq!(
            run(&["GET", "foo"], &mut server, &mut client),
            RespFrame::BulkString(None)
        );
    }

    fn assert_strict_blocked(frame: &RespFrame, command: &str) {
        match frame {
            RespFrame::Error(msg) => {
                let text = String::from_utf8_lossy(msg);
                assert!(
                    text.contains("strict compatibility mode"),
                    "expected strict-mode error for `{command}`, got: {text}"
                );
            }
            other => panic!("expected strict-mode error for `{command}`, got {other:?}"),
        }
    }

    fn assert_not_strict_blocked(frame: &RespFrame, command: &str) {
        if let RespFrame::Error(msg) = frame {
            let text = String::from_utf8_lossy(msg);
            assert!(
                !text.contains("strict compatibility mode"),
                "command `{command}` should not be strict-blocked, got: {text}"
            );
        }
    }

    #[test]
    fn command_docs_exposes_capability_tier_for_every_command() {
        let mut server = ServerState::with_default_dbs();
        let mut client = ClientState::default();

        let known_tiers = [
            "behavioral_subset",
            "baseline_local",
            "syntax_only",
            "unsupported",
        ];

        let docs = run(&["COMMAND", "DOCS"], &mut server, &mut client);
        let RespFrame::Map(rows) = docs else {
            panic!("COMMAND DOCS should return a map");
        };

        // Every registered command must appear with a capability tier; a missing
        // tier is exactly the silent-success risk Phase 0 guards against.
        assert_eq!(
            rows.len(),
            command_spec_count(),
            "COMMAND DOCS must cover every registered command spec"
        );

        for (name, entry) in &rows {
            let RespFrame::BulkString(Some(name)) = name else {
                panic!("COMMAND DOCS entry name should be a bulk string");
            };
            let name = String::from_utf8_lossy(name);
            let RespFrame::Map(fields) = entry else {
                panic!("COMMAND DOCS entry for `{name}` should be a map");
            };
            let tier = fields.iter().find_map(|(key, value)| {
                if *key == RespFrame::bulk_str("ratatosk_capability_tier") {
                    if let RespFrame::BulkString(Some(tier)) = value {
                        return Some(String::from_utf8_lossy(tier).into_owned());
                    }
                }
                None
            });
            let tier = tier
                .unwrap_or_else(|| panic!("`{name}` is missing ratatosk_capability_tier in DOCS"));
            assert!(
                known_tiers.contains(&tier.as_str()),
                "`{name}` has unknown capability tier `{tier}`"
            );
        }
    }

    #[test]
    fn strict_mode_blocks_unsupported_and_metadata_commands() {
        let mut server = ServerState::with_default_dbs();
        server
            .config
            .set_compatibility_mode(Bytes::from_static(b"strict"));
        let mut client = ClientState::default();

        // WAIT/WAITAOF imply replica-backed acknowledgement; CLUSTER/SENTINEL/
        // FUNCTION/FCALL are unsupported; READONLY/CLIENT UNBLOCK are syntax_only;
        // CLIENT PAUSE is unsupported. All must surface a structured strict error.
        for parts in [
            vec!["WAIT", "0", "100"],
            vec!["WAITAOF", "0", "0", "100"],
            vec!["CLUSTER", "SETSLOT", "1", "STABLE"],
            vec!["SENTINEL", "MASTERS"],
            vec!["FUNCTION", "LOAD", "code"],
            vec!["FCALL", "f", "0"],
            vec!["READONLY"],
            vec!["CLIENT", "UNBLOCK", "1"],
            vec!["CLIENT", "PAUSE", "100"],
        ] {
            let frame = run(&parts, &mut server, &mut client);
            assert_strict_blocked(&frame, &parts.join(" "));
        }
    }

    #[test]
    fn strict_mode_allows_supported_commands() {
        let mut server = ServerState::with_default_dbs();
        server
            .config
            .set_compatibility_mode(Bytes::from_static(b"strict"));
        let mut client = ClientState::default();

        assert_eq!(
            run(&["SET", "k", "v"], &mut server, &mut client),
            RespFrame::ok()
        );
        assert_eq!(
            run(&["GET", "k"], &mut server, &mut client),
            RespFrame::bulk_str("v")
        );
        // Introspection that clients rely on to discover capability tiers must
        // stay available even in strict mode.
        for parts in [
            vec!["PING"],
            vec!["CLIENT", "LIST"],
            vec!["CONFIG", "GET", "compatibility-mode"],
            vec!["CLUSTER", "INFO"],
            vec!["COMMAND", "DOCS", "GET"],
        ] {
            let frame = run(&parts, &mut server, &mut client);
            assert_not_strict_blocked(&frame, &parts.join(" "));
        }
    }

    #[test]
    fn compat_mode_is_default_and_does_not_block() {
        let mut server = ServerState::with_default_dbs();
        let mut client = ClientState::default();
        assert!(!server.config.is_strict_compatibility());

        // The same commands strict mode rejects pass through untouched by default.
        for parts in [vec!["WAIT", "0", "0"], vec!["READONLY"]] {
            let frame = run(&parts, &mut server, &mut client);
            assert_not_strict_blocked(&frame, &parts.join(" "));
        }
    }

    #[test]
    fn capability_tier_matches_gap_ledger_for_every_spec() {
        // 3-way consistency guard (ship gate #4): the tier the engine reports via
        // COMMAND DOCS (`command_capability_tier`) must equal the gap-ledger
        // source of truth for every command that exists both as a runtime spec
        // and a ledger entry. Tiers were audited per-command on 2026-06-02.
        let ledger_path = concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../docs/redis-gap-ledger.json"
        );
        let raw = std::fs::read_to_string(ledger_path).expect("read gap ledger");
        let ledger: serde_json::Value = serde_json::from_str(&raw).expect("parse gap ledger");
        let commands = ledger["commands"].as_array().expect("commands array");
        let ledger_tier: std::collections::HashMap<&str, &str> = commands
            .iter()
            .map(|c| {
                (
                    c["name"].as_str().expect("name"),
                    c["capability_tier"].as_str().expect("tier"),
                )
            })
            .collect();

        let mut mismatches = Vec::new();
        let mut checked = 0usize;
        for spec in super::registry::all_command_specs() {
            if let Some(expected) = ledger_tier.get(spec.name) {
                checked += 1;
                let actual = super::cmd_command_metadata::command_capability_tier(spec);
                if actual != *expected {
                    mismatches.push(format!("{}: code={actual} ledger={expected}", spec.name));
                }
            }
        }
        assert!(checked > 300, "expected to check most specs, got {checked}");
        assert!(
            mismatches.is_empty(),
            "code vs gap-ledger capability-tier mismatches ({}):\n{}",
            mismatches.len(),
            mismatches.join("\n")
        );
    }

    #[test]
    fn strict_mode_blocks_every_unsupported_and_syntax_only_command() {
        // Exhaustive guard (Phase 5): over the *entire* registered command set,
        // strict mode must reject exactly the commands whose capability tier is
        // `unsupported`/`syntax_only`, plus `WAIT`/`WAITAOF`, and nothing else.
        // Any drift in the tier table or the strict policy flips a command here.
        let mut blocked_total = 0usize;
        for spec in super::registry::all_command_specs() {
            let argv: Vec<Bytes> = spec
                .name
                .split(' ')
                .map(|part| Bytes::copy_from_slice(part.as_bytes()))
                .collect();
            let blocked = super::cmd_command_metadata::strict_mode_error(&argv).is_some();
            let tier = super::cmd_command_metadata::command_capability_tier(spec);
            let policy_blocked = matches!(tier, "unsupported" | "syntax_only")
                || matches!(spec.name, "WAIT" | "WAITAOF");
            assert_eq!(
                blocked, policy_blocked,
                "strict-mode decision for `{}` (tier={tier}) disagrees with the documented policy",
                spec.name
            );
            if blocked {
                blocked_total += 1;
            }
        }
        // Sanity: the blocked set is non-trivial (guards against a no-op policy).
        assert!(
            blocked_total >= 50,
            "expected strict mode to block a substantial set, got {blocked_total}"
        );
    }

    #[test]
    fn config_set_compatibility_mode_toggles_strict_at_runtime() {
        let mut server = ServerState::with_default_dbs();
        let mut client = ClientState::default();

        // Default reports compat through CONFIG GET.
        assert_eq!(
            run(
                &["CONFIG", "GET", "compatibility-mode"],
                &mut server,
                &mut client
            ),
            RespFrame::Array(vec![
                RespFrame::bulk_str("compatibility-mode"),
                RespFrame::bulk_str("compat"),
            ])
        );

        // WAIT is allowed in compat mode.
        assert_not_strict_blocked(&run(&["WAIT", "0", "0"], &mut server, &mut client), "WAIT");

        // Flip to strict at runtime and confirm the gate now fires.
        assert_eq!(
            run(
                &["CONFIG", "SET", "compatibility-mode", "strict"],
                &mut server,
                &mut client
            ),
            RespFrame::ok()
        );
        assert!(server.config.is_strict_compatibility());
        assert_strict_blocked(&run(&["WAIT", "0", "0"], &mut server, &mut client), "WAIT");

        // Invalid values are rejected.
        match run(
            &["CONFIG", "SET", "compatibility-mode", "bogus"],
            &mut server,
            &mut client,
        ) {
            RespFrame::Error(_) => {}
            other => panic!("expected error for invalid compatibility-mode, got {other:?}"),
        }
    }

    #[test]
    fn config_get_set_protected_mode_round_trips() {
        let mut server = ServerState::with_default_dbs();
        let mut client = ClientState::default();

        // Default reports yes through CONFIG GET.
        assert_eq!(
            run(
                &["CONFIG", "GET", "protected-mode"],
                &mut server,
                &mut client
            ),
            RespFrame::Array(vec![
                RespFrame::bulk_str("protected-mode"),
                RespFrame::bulk_str("yes"),
            ])
        );
        assert!(server.config.is_protected_mode());

        // Toggle to no at runtime and confirm CONFIG GET reflects it.
        assert_eq!(
            run(
                &["CONFIG", "SET", "protected-mode", "no"],
                &mut server,
                &mut client
            ),
            RespFrame::ok()
        );
        assert!(!server.config.is_protected_mode());
        assert_eq!(
            run(
                &["CONFIG", "GET", "protected-mode"],
                &mut server,
                &mut client
            ),
            RespFrame::Array(vec![
                RespFrame::bulk_str("protected-mode"),
                RespFrame::bulk_str("no"),
            ])
        );

        // Invalid values are rejected.
        match run(
            &["CONFIG", "SET", "protected-mode", "bogus"],
            &mut server,
            &mut client,
        ) {
            RespFrame::Error(_) => {}
            other => panic!("expected error for invalid protected-mode, got {other:?}"),
        }
    }

    #[test]
    fn config_get_reports_startup_only_unix_socket_values() {
        let mut server = ServerState::with_default_dbs();
        let mut client = ClientState::default();
        server
            .config
            .set_unixsocket(Some(std::path::PathBuf::from("/tmp/ratatosk.sock")));
        server.config.set_unixsocketperm(0o750);

        assert_eq!(
            run(&["CONFIG", "GET", "unixsocket"], &mut server, &mut client),
            RespFrame::Array(vec![
                RespFrame::bulk_str("unixsocket"),
                RespFrame::bulk_str("/tmp/ratatosk.sock"),
            ])
        );
        assert_eq!(
            run(
                &["CONFIG", "GET", "unixsocketperm"],
                &mut server,
                &mut client
            ),
            RespFrame::Array(vec![
                RespFrame::bulk_str("unixsocketperm"),
                RespFrame::bulk_str("750"),
            ])
        );

        let bind_error = run(
            &["CONFIG", "SET", "bind", "127.0.0.1"],
            &mut server,
            &mut client,
        );
        let unixsocket_error = run(
            &["CONFIG", "SET", "unixsocket", "/tmp/other.sock"],
            &mut server,
            &mut client,
        );
        let unixsocketperm_error = run(
            &["CONFIG", "SET", "unixsocketperm", "700"],
            &mut server,
            &mut client,
        );
        assert_eq!(unixsocket_error, bind_error);
        assert_eq!(unixsocketperm_error, bind_error);
    }

    #[test]
    fn unix_socket_client_flag_and_monitor_line_match_redis() {
        let mut client = ClientState::new(42);
        client.set_unix_socket(true);
        let snapshot = client.snapshot(
            Bytes::from_static(b"/tmp/ratatosk.sock:0"),
            Bytes::from_static(b"/tmp/ratatosk.sock:0"),
        );
        assert_eq!(snapshot.flags, Bytes::from_static(b"NU"));

        let mut server = ServerState::with_default_dbs();
        server.upsert_client_snapshot(snapshot);
        let line = super::format_monitor_line(&server, &client, &[Bytes::from_static(b"PING")]);
        let line = String::from_utf8_lossy(&line);
        assert!(line.contains("[0 unix:/tmp/ratatosk.sock] \"PING\""));

        let tcp_snapshot = ClientState::new(43).snapshot(
            Bytes::from_static(b"127.0.0.1:43"),
            Bytes::from_static(b"127.0.0.1:6379"),
        );
        assert_eq!(tcp_snapshot.flags, Bytes::from_static(b"N"));
    }

    #[test]
    fn select_switches_database() {
        let mut server = ServerState::with_default_dbs();
        let mut client = ClientState::default();

        assert_eq!(
            run(&["SET", "k", "v0"], &mut server, &mut client),
            RespFrame::ok()
        );
        assert_eq!(
            run(&["SELECT", "1"], &mut server, &mut client),
            RespFrame::ok()
        );
        assert_eq!(
            run(&["GET", "k"], &mut server, &mut client),
            RespFrame::BulkString(None)
        );
    }

    #[test]
    fn set_get_option_returns_previous_value() {
        let mut server = ServerState::with_default_dbs();
        let mut client = ClientState::default();

        assert_eq!(
            run(&["SET", "foo", "v1"], &mut server, &mut client),
            RespFrame::ok()
        );
        assert_eq!(
            run(&["SET", "foo", "v2", "GET"], &mut server, &mut client),
            RespFrame::bulk_str("v1")
        );
        assert_eq!(
            run(&["GET", "foo"], &mut server, &mut client),
            RespFrame::bulk_str("v2")
        );
    }

    #[test]
    fn set_nx_xx_conditions() {
        let mut server = ServerState::with_default_dbs();
        let mut client = ClientState::default();

        assert_eq!(
            run(&["SET", "foo", "one", "NX"], &mut server, &mut client),
            RespFrame::ok()
        );
        assert_eq!(
            run(&["SET", "foo", "two", "NX"], &mut server, &mut client),
            RespFrame::BulkString(None)
        );
        assert_eq!(
            run(&["SET", "foo", "two", "XX"], &mut server, &mut client),
            RespFrame::ok()
        );
        assert_eq!(
            run(&["SET", "bar", "two", "XX"], &mut server, &mut client),
            RespFrame::BulkString(None)
        );
    }

    #[test]
    fn set_with_expire_and_ttl_expires_key() {
        let mut server = ServerState::with_default_dbs();
        let mut client = ClientState::default();

        assert_eq!(
            run(&["SET", "tmp", "v", "EX", "1"], &mut server, &mut client),
            RespFrame::ok()
        );

        let ttl = run(&["TTL", "tmp"], &mut server, &mut client);
        let RespFrame::Integer(ttl_secs) = ttl else {
            panic!("TTL should return integer");
        };
        assert!((0..=1).contains(&ttl_secs));

        std::thread::sleep(Duration::from_millis(1200));

        assert_eq!(
            run(&["TTL", "tmp"], &mut server, &mut client),
            RespFrame::Integer(-2)
        );
    }

    #[test]
    fn expire_and_ttl_flow() {
        let mut server = ServerState::with_default_dbs();
        let mut client = ClientState::default();

        assert_eq!(
            run(&["SET", "foo", "bar"], &mut server, &mut client),
            RespFrame::ok()
        );
        assert_eq!(
            run(&["TTL", "foo"], &mut server, &mut client),
            RespFrame::Integer(-1)
        );

        assert_eq!(
            run(&["EXPIRE", "foo", "1"], &mut server, &mut client),
            RespFrame::Integer(1)
        );

        let ttl = run(&["TTL", "foo"], &mut server, &mut client);
        let RespFrame::Integer(ttl_secs) = ttl else {
            panic!("TTL should return integer");
        };
        assert!((0..=1).contains(&ttl_secs));
    }

    #[test]
    fn pexpire_pttl_and_expiretime_family() {
        let mut server = ServerState::with_default_dbs();
        let mut client = ClientState::default();

        assert_eq!(
            run(&["SET", "foo", "bar"], &mut server, &mut client),
            RespFrame::ok()
        );

        assert_eq!(
            run(&["PEXPIRE", "foo", "1500"], &mut server, &mut client),
            RespFrame::Integer(1)
        );

        let pttl = run(&["PTTL", "foo"], &mut server, &mut client);
        let RespFrame::Integer(ms) = pttl else {
            panic!("PTTL should return integer");
        };
        assert!(ms > 0 && ms <= 1500);

        let now = now_ms();
        let deadline = now + 5000;
        assert_eq!(
            run(
                &["PEXPIREAT", "foo", &deadline.to_string()],
                &mut server,
                &mut client,
            ),
            RespFrame::Integer(1)
        );

        let pexpiretime = run(&["PEXPIRETIME", "foo"], &mut server, &mut client);
        assert_eq!(pexpiretime, RespFrame::Integer(deadline));

        let expiretime = run(&["EXPIRETIME", "foo"], &mut server, &mut client);
        assert_eq!(expiretime, RespFrame::Integer(deadline / 1000));
    }

    #[test]
    fn expire_lt_and_gt_conditions() {
        let mut server = ServerState::with_default_dbs();
        let mut client = ClientState::default();

        assert_eq!(
            run(&["SET", "foo", "bar"], &mut server, &mut client),
            RespFrame::ok()
        );

        // Persistent key is treated as infinite TTL: GT fails, LT succeeds.
        assert_eq!(
            run(&["EXPIRE", "foo", "10", "GT"], &mut server, &mut client),
            RespFrame::Integer(0)
        );
        assert_eq!(
            run(&["EXPIRE", "foo", "10", "LT"], &mut server, &mut client),
            RespFrame::Integer(1)
        );
    }

    #[test]
    fn persist_setex_psetex_flow() {
        let mut server = ServerState::with_default_dbs();
        let mut client = ClientState::default();

        assert_eq!(
            run(&["SETEX", "foo", "2", "bar"], &mut server, &mut client),
            RespFrame::ok()
        );
        let ttl = run(&["TTL", "foo"], &mut server, &mut client);
        let RespFrame::Integer(ttl_secs) = ttl else {
            panic!("TTL should return integer");
        };
        assert!((0..=2).contains(&ttl_secs));

        assert_eq!(
            run(&["PERSIST", "foo"], &mut server, &mut client),
            RespFrame::Integer(1)
        );
        assert_eq!(
            run(&["TTL", "foo"], &mut server, &mut client),
            RespFrame::Integer(-1)
        );
        assert_eq!(
            run(&["PERSIST", "foo"], &mut server, &mut client),
            RespFrame::Integer(0)
        );
        assert_eq!(
            run(&["PERSIST", "missing"], &mut server, &mut client),
            RespFrame::Integer(0)
        );

        assert_eq!(
            run(&["PSETEX", "px", "1500", "v"], &mut server, &mut client),
            RespFrame::ok()
        );
        let pttl = run(&["PTTL", "px"], &mut server, &mut client);
        let RespFrame::Integer(ms) = pttl else {
            panic!("PTTL should return integer");
        };
        assert!(ms > 0 && ms <= 1500);

        assert_eq!(
            run(&["SETEX", "bad", "0", "x"], &mut server, &mut client),
            RespFrame::error_str("ERR invalid expire time in 'setex' command")
        );
        assert_eq!(
            run(&["PSETEX", "bad", "nope", "x"], &mut server, &mut client),
            RespFrame::error_str("ERR value is not an integer or out of range")
        );
    }

    #[test]
    fn getdel_and_getex_flow() {
        let mut server = ServerState::with_default_dbs();
        let mut client = ClientState::default();

        assert_eq!(
            run(&["SET", "k", "v"], &mut server, &mut client),
            RespFrame::ok()
        );

        assert_eq!(
            run(&["GETEX", "k"], &mut server, &mut client),
            RespFrame::bulk_str("v")
        );
        assert_eq!(
            run(&["TTL", "k"], &mut server, &mut client),
            RespFrame::Integer(-1)
        );

        assert_eq!(
            run(&["GETEX", "k", "EX", "1"], &mut server, &mut client),
            RespFrame::bulk_str("v")
        );
        let ttl = run(&["TTL", "k"], &mut server, &mut client);
        let RespFrame::Integer(ttl_secs) = ttl else {
            panic!("TTL should return integer");
        };
        assert!((0..=1).contains(&ttl_secs));

        assert_eq!(
            run(&["GETEX", "k", "PERSIST"], &mut server, &mut client),
            RespFrame::bulk_str("v")
        );
        assert_eq!(
            run(&["TTL", "k"], &mut server, &mut client),
            RespFrame::Integer(-1)
        );

        let expired_at = now_ms() - 1;
        assert_eq!(
            run(
                &["GETEX", "k", "PXAT", &expired_at.to_string()],
                &mut server,
                &mut client,
            ),
            RespFrame::bulk_str("v")
        );
        assert_eq!(
            run(&["GET", "k"], &mut server, &mut client),
            RespFrame::BulkString(None)
        );

        assert_eq!(
            run(&["GETDEL", "k"], &mut server, &mut client),
            RespFrame::BulkString(None)
        );
        assert_eq!(
            run(&["SET", "k", "next"], &mut server, &mut client),
            RespFrame::ok()
        );
        assert_eq!(
            run(&["GETDEL", "k"], &mut server, &mut client),
            RespFrame::bulk_str("next")
        );
        assert_eq!(
            run(&["GET", "k"], &mut server, &mut client),
            RespFrame::BulkString(None)
        );

        assert_eq!(
            run(&["GETEX", "missing", "EX", "1"], &mut server, &mut client),
            RespFrame::BulkString(None)
        );
    }

    #[test]
    fn getex_error_cases() {
        let mut server = ServerState::with_default_dbs();
        let mut client = ClientState::default();

        assert_eq!(
            run(&["SET", "k", "v"], &mut server, &mut client),
            RespFrame::ok()
        );

        assert_eq!(
            run(&["GETEX", "k", "EX"], &mut server, &mut client),
            RespFrame::error_str("ERR syntax error")
        );
        assert_eq!(
            run(&["GETEX", "k", "EX", "nope"], &mut server, &mut client),
            RespFrame::error_str("ERR value is not an integer or out of range")
        );
        assert_eq!(
            run(&["GETEX", "k", "EX", "0"], &mut server, &mut client),
            RespFrame::error_str("ERR invalid expire time in 'getex' command")
        );
        assert_eq!(
            run(
                &["GETEX", "k", "PERSIST", "EX", "1"],
                &mut server,
                &mut client,
            ),
            RespFrame::error_str("ERR syntax error")
        );
        assert_eq!(
            run(&["GETEX", "k", "NOPE"], &mut server, &mut client),
            RespFrame::error_str("ERR syntax error")
        );
    }

    #[test]
    fn hello_setname_and_command_info() {
        let mut server = ServerState::with_default_dbs();
        let mut client = ClientState::default();

        assert_eq!(
            run(
                &["ACL", "SETUSER", "u", "on", ">p", "+@all"],
                &mut server,
                &mut client,
            ),
            RespFrame::ok()
        );

        let hello = run(
            &["HELLO", "3", "SETNAME", "c1", "AUTH", "u", "p"],
            &mut server,
            &mut client,
        );
        let RespFrame::Map(entries) = hello else {
            panic!("HELLO should return map");
        };
        assert!(!entries.is_empty());

        assert_eq!(
            run(&["HELLO", "4"], &mut server, &mut client),
            RespFrame::error_str("NOPROTO unsupported protocol version")
        );
        assert_eq!(
            run(&["HELLO", "proto"], &mut server, &mut client),
            RespFrame::error_str("NOPROTO unsupported protocol version")
        );
        assert_eq!(
            run(&["HELLO", "SETNAME"], &mut server, &mut client),
            RespFrame::error_str("ERR syntax error")
        );
        assert_eq!(
            run(&["HELLO", "AUTH", "u"], &mut server, &mut client),
            RespFrame::error_str("ERR syntax error")
        );

        let hello2 = run(&["HELLO", "2"], &mut server, &mut client);
        let RespFrame::Map(entries2) = hello2 else {
            panic!("HELLO 2 should return map");
        };
        let proto_entry = entries2
            .iter()
            .find(|(k, _)| *k == RespFrame::bulk_str("proto"))
            .map(|(_, v)| v.clone());
        assert_eq!(proto_entry, Some(RespFrame::Integer(2)));

        assert_eq!(
            run(&["COMMAND", "COUNT"], &mut server, &mut client),
            RespFrame::Integer(command_spec_count() as i64)
        );

        let list = run(&["COMMAND", "LIST"], &mut server, &mut client);
        let RespFrame::Array(list_entries) = list else {
            panic!("COMMAND LIST should return array");
        };
        assert_eq!(list_entries.len(), command_spec_count());

        let info = run(
            &["COMMAND", "INFO", "GET", "UNKNOWN"],
            &mut server,
            &mut client,
        );
        let RespFrame::Array(info_entries) = info else {
            panic!("COMMAND INFO should return array");
        };
        assert_eq!(info_entries.len(), 2);
        assert_eq!(info_entries[1], RespFrame::Null);
    }

    #[test]
    fn m0_compat_new_commands() {
        let mut server = ServerState::with_default_dbs();
        let mut client = ClientState::default();

        assert_eq!(
            run(
                &["ACL", "SETUSER", "u", "on", ">p", "+@all"],
                &mut server,
                &mut client,
            ),
            RespFrame::ok()
        );
        assert_eq!(
            run(&["AUTH", "u", "p"], &mut server, &mut client),
            RespFrame::ok()
        );

        assert_eq!(
            run(&["CLIENT", "SETNAME", "n1"], &mut server, &mut client),
            RespFrame::ok()
        );
        assert_eq!(
            run(&["CLIENT", "GETNAME"], &mut server, &mut client),
            RespFrame::bulk_str("n1")
        );
        assert_eq!(
            run(&["CLIENT", "ID"], &mut server, &mut client),
            RespFrame::Integer(0)
        );

        assert_eq!(
            run(&["DBSIZE"], &mut server, &mut client),
            RespFrame::Integer(0)
        );
        assert_eq!(
            run(&["SET", "foo", "bar"], &mut server, &mut client),
            RespFrame::ok()
        );
        assert_eq!(
            run(&["DBSIZE"], &mut server, &mut client),
            RespFrame::Integer(1)
        );

        let time = run(&["TIME"], &mut server, &mut client);
        let RespFrame::Array(parts) = time else {
            panic!("TIME should return array");
        };
        assert_eq!(parts.len(), 2);
        for part in parts {
            let RespFrame::BulkString(Some(raw)) = part else {
                panic!("TIME parts should be bulk strings");
            };
            assert!(String::from_utf8_lossy(&raw).parse::<i64>().is_ok());
        }

        let lastsave_before = run(&["LASTSAVE"], &mut server, &mut client);
        let RespFrame::Integer(lastsave_before) = lastsave_before else {
            panic!("LASTSAVE should return integer");
        };
        assert!(lastsave_before > 0);

        assert_eq!(run(&["SAVE"], &mut server, &mut client), RespFrame::ok());
        let lastsave_after = run(&["LASTSAVE"], &mut server, &mut client);
        let RespFrame::Integer(lastsave_after) = lastsave_after else {
            panic!("LASTSAVE should return integer");
        };
        assert!(lastsave_after >= lastsave_before);

        let client_info = run(&["CLIENT", "INFO"], &mut server, &mut client);
        let RespFrame::BulkString(Some(client_info)) = client_info else {
            panic!("CLIENT INFO should return bulk string");
        };
        let client_info_text = String::from_utf8_lossy(&client_info).to_string();
        assert!(client_info_text.contains("id=0"));
        assert!(client_info_text.contains("name=n1"));

        let client_list = run(&["CLIENT", "LIST"], &mut server, &mut client);
        let RespFrame::BulkString(Some(client_list)) = client_list else {
            panic!("CLIENT LIST should return bulk string");
        };
        let client_list_text = String::from_utf8_lossy(&client_list).to_string();
        assert!(client_list_text.contains("id=0"));

        assert_eq!(
            run(
                &["CLIENT", "LIST", "TYPE", "master"],
                &mut server,
                &mut client
            ),
            RespFrame::bulk_str("")
        );
        assert_eq!(
            run(
                &["CLIENT", "LIST", "TYPE", "normal"],
                &mut server,
                &mut client
            ),
            RespFrame::bulk_str(&client_list_text)
        );

        assert_eq!(
            run(&["CLIENT", "LIST", "ID", "999"], &mut server, &mut client),
            RespFrame::bulk_str("")
        );

        assert!(matches!(
            run(&["CLIENT", "PAUSE", "10"], &mut server, &mut client),
            RespFrame::Error(_)
        ));
        assert!(matches!(
            run(
                &["CLIENT", "PAUSE", "10", "WRITE"],
                &mut server,
                &mut client
            ),
            RespFrame::Error(_)
        ));
        assert!(matches!(
            run(&["CLIENT", "UNPAUSE"], &mut server, &mut client),
            RespFrame::Error(_)
        ));

        assert_eq!(
            run(&["CLIENT", "UNBLOCK", "0"], &mut server, &mut client),
            RespFrame::Integer(0)
        );
        assert_eq!(
            run(
                &["CLIENT", "UNBLOCK", "0", "ERROR"],
                &mut server,
                &mut client
            ),
            RespFrame::Integer(0)
        );

        assert_eq!(
            run(&["CLIENT", "KILL", "127.0.0.1:0"], &mut server, &mut client),
            RespFrame::ok()
        );
        assert_eq!(
            run(&["CLIENT", "KILL", "ID", "0"], &mut server, &mut client),
            RespFrame::Integer(1)
        );

        let monitor_docs = run(&["COMMAND", "DOCS", "MONITOR"], &mut server, &mut client);
        let RespFrame::Map(monitor_entries) = monitor_docs else {
            panic!("COMMAND DOCS MONITOR should return map");
        };
        assert_eq!(monitor_entries.len(), 1);
        let RespFrame::Map(monitor_docs) = &monitor_entries[0].1 else {
            panic!("COMMAND DOCS MONITOR entry should return map");
        };
        assert!(monitor_docs.iter().any(|(key, value)| {
            *key == RespFrame::bulk_str("ratatosk_capability_tier")
                && *value == RespFrame::bulk_str("baseline_local")
        }));
        assert_eq!(run(&["MONITOR"], &mut server, &mut client), RespFrame::ok());
        assert!(client.is_monitor());
        assert_eq!(server.monitor_client_count(), 1);

        server.upsert_client_snapshot(ClientState::new(42).snapshot(
            Bytes::from_static(b"127.0.0.1:42"),
            Bytes::from_static(b"127.0.0.1:6379"),
        ));
        let mut rx42 = server.pubsub.register_client(42);

        assert_eq!(
            run(
                &["CLIENT", "TRACKING", "ON", "REDIRECT", "42"],
                &mut server,
                &mut client
            ),
            RespFrame::ok()
        );
        assert_eq!(
            run(&["CLIENT", "GETREDIR"], &mut server, &mut client),
            RespFrame::Integer(42)
        );

        let tracking_info = run(&["CLIENT", "TRACKINGINFO"], &mut server, &mut client);
        let RespFrame::Map(entries) = tracking_info else {
            panic!("CLIENT TRACKINGINFO should return map");
        };
        assert!(!entries.is_empty());

        let mut tracking_target = ClientState::new(42);
        assert_eq!(
            run(
                &["CLIENT", "TRACKING", "ON", "REDIRECT", "42"],
                &mut server,
                &mut client
            ),
            RespFrame::ok()
        );
        assert_eq!(
            run(
                &["SET", "tracked-key", "v1"],
                &mut server,
                &mut tracking_target
            ),
            RespFrame::ok()
        );
        assert_eq!(
            run(&["GET", "tracked-key"], &mut server, &mut client),
            RespFrame::bulk_str("v1")
        );
        assert_eq!(
            run(
                &["SET", "tracked-key", "v2"],
                &mut server,
                &mut tracking_target
            ),
            RespFrame::ok()
        );
        let pending_tracking = PubSubState::drain_rx(&mut rx42);
        assert_eq!(pending_tracking.len(), 1);
        match &pending_tracking[0] {
            PubSubMessage::Invalidate { keys } => {
                assert_eq!(keys, &vec![Bytes::from_static(b"tracked-key")]);
            }
            other => panic!("expected invalidate push, got {other:?}"),
        }

        // CLIENT CACHING requires OPTIN or OPTOUT mode; current tracking is
        // in normal mode, so this should return an error.
        assert!(matches!(
            run(&["CLIENT", "CACHING", "NO"], &mut server, &mut client),
            RespFrame::Error(_)
        ));
        assert_eq!(
            run(
                &["CLIENT", "SETINFO", "LIB-NAME", "ratatosk"],
                &mut server,
                &mut client
            ),
            RespFrame::ok()
        );
        assert_eq!(
            run(
                &["CLIENT", "SETINFO", "LIB-VER", "1.0.0"],
                &mut server,
                &mut client
            ),
            RespFrame::ok()
        );
        let client_info = run(&["CLIENT", "INFO"], &mut server, &mut client);
        let RespFrame::BulkString(Some(client_info)) = client_info else {
            panic!("CLIENT INFO should return bulk string");
        };
        let client_info_text = String::from_utf8_lossy(&client_info);
        assert!(
            client_info_text.contains("lib-name=ratatosk"),
            "{client_info_text}"
        );
        assert!(
            client_info_text.contains("lib-ver=1.0.0"),
            "{client_info_text}"
        );
        assert_eq!(
            run(&["CLIENT", "NO-EVICT", "ON"], &mut server, &mut client),
            RespFrame::ok()
        );
        assert_eq!(
            run(&["CLIENT", "NO-TOUCH", "ON"], &mut server, &mut client),
            RespFrame::ok()
        );
        assert_eq!(
            run(&["CLIENT", "REPLY", "SKIP"], &mut server, &mut client),
            RespFrame::ok()
        );

        assert_eq!(
            run(&["CLIENT", "TRACKING", "OFF"], &mut server, &mut client),
            RespFrame::ok()
        );
        assert_eq!(
            run(&["CLIENT", "GETREDIR"], &mut server, &mut client),
            RespFrame::Integer(-1)
        );

        let acl_help = run(&["ACL", "HELP"], &mut server, &mut client);
        let RespFrame::Array(acl_help_rows) = acl_help else {
            panic!("ACL HELP should return array");
        };
        assert!(!acl_help_rows.is_empty());

        assert_eq!(
            run(&["ACL", "WHOAMI"], &mut server, &mut client),
            RespFrame::bulk_str("u")
        );

        assert_eq!(
            run(
                &["ACL", "SETUSER", "alice", "on", ">secret", "+@all"],
                &mut server,
                &mut client
            ),
            RespFrame::ok()
        );

        let users = run(&["ACL", "USERS"], &mut server, &mut client);
        let RespFrame::Array(user_rows) = users else {
            panic!("ACL USERS should return array");
        };
        assert!(user_rows.contains(&RespFrame::bulk_str("default")));
        assert!(user_rows.contains(&RespFrame::bulk_str("alice")));

        let getuser = run(&["ACL", "GETUSER", "alice"], &mut server, &mut client);
        let RespFrame::Map(getuser_map) = getuser else {
            panic!("ACL GETUSER should return map");
        };
        assert!(!getuser_map.is_empty());

        let acl_log = run(&["ACL", "LOG", "1"], &mut server, &mut client);
        let RespFrame::Array(_acl_log_rows) = acl_log else {
            panic!("ACL LOG should return array");
        };

        assert_eq!(
            run(&["ACL", "DELUSER", "alice"], &mut server, &mut client),
            RespFrame::Integer(1)
        );
        assert_eq!(
            run(
                &["ACL", "DRYRUN", "default", "GET", "k"],
                &mut server,
                &mut client
            ),
            RespFrame::ok()
        );

        let mut replica_client = ClientState::new(42);
        assert_eq!(
            run(
                &["REPLCONF", "listening-port", "6379"],
                &mut server,
                &mut replica_client
            ),
            RespFrame::ok()
        );
        assert_eq!(
            run(
                &["REPLCONF", "ip-address", "10.0.0.2"],
                &mut server,
                &mut replica_client
            ),
            RespFrame::ok()
        );
        assert_eq!(
            run(
                &["REPLCONF", "capa", "psync2", "eof"],
                &mut server,
                &mut replica_client
            ),
            RespFrame::ok()
        );
        {
            let role_reply = run(&["ROLE"], &mut server, &mut client);
            let RespFrame::Array(entries) = role_reply else {
                panic!("ROLE should return array");
            };
            assert_eq!(entries[0], RespFrame::bulk_str("master"));
            // Offset reflects accumulated writes
            assert!(matches!(entries[1], RespFrame::Integer(_)));
        }
        {
            let replconf_reply = run(
                &["REPLCONF", "getack", "*"],
                &mut server,
                &mut replica_client,
            );
            let RespFrame::Array(entries) = replconf_reply else {
                panic!("REPLCONF getack should return array");
            };
            assert_eq!(entries[0], RespFrame::bulk_str("REPLCONF"));
            assert_eq!(entries[1], RespFrame::bulk_str("ACK"));
            // Offset reflects current server state
            assert!(matches!(entries[2], RespFrame::BulkString(_)));
        }
        assert!(matches!(
            run(&["PSYNC", "?", "-1"], &mut server, &mut replica_client),
            RespFrame::Error(_)
        ));
        assert_eq!(
            run(&["SYNC"], &mut server, &mut client),
            RespFrame::error_str("ERR SYNC is not supported in standalone mode")
        );
        assert_eq!(
            run(
                &["SET", "replication-key", "value"],
                &mut server,
                &mut client
            ),
            RespFrame::ok()
        );
        assert_eq!(
            run(&["WAIT", "1", "100"], &mut server, &mut client),
            RespFrame::Integer(0)
        );
        assert_eq!(
            run(&["REPLCONF", "ack", "1"], &mut server, &mut replica_client),
            RespFrame::ok()
        );
        // WAIT may return 0 or 1 depending on whether PSYNC established the replica
        let wait_result = run(&["WAIT", "1", "100"], &mut server, &mut client);
        assert!(matches!(wait_result, RespFrame::Integer(_)));
        let waitaof_result = run(&["WAITAOF", "1", "1", "100"], &mut server, &mut client);
        assert!(matches!(waitaof_result, RespFrame::Array(_)));

        let replication_info = run(&["INFO", "replication"], &mut server, &mut client);
        let RespFrame::BulkString(Some(replication_info_body)) = replication_info else {
            panic!("INFO replication should return bulk string");
        };
        let replication_info_text =
            std::str::from_utf8(&replication_info_body).expect("valid INFO replication utf8");
        assert!(replication_info_text.contains("role:master"));
        // Replica is registered via REPLCONF even though PSYNC is unsupported
        assert!(replication_info_text.contains("connected_slaves:"));
        assert!(replication_info_text.contains("master_repl_offset:"));

        assert!(matches!(
            run(
                &["REPLICAOF", "127.0.0.1", "6380"],
                &mut server,
                &mut client
            ),
            RespFrame::Error(_)
        ));
        // REPLICAOF NO ONE still works
        assert_eq!(
            run(&["REPLICAOF", "NO", "ONE"], &mut server, &mut client),
            RespFrame::ok()
        );
        // After REPLICAOF NO ONE, role should be master
        {
            let role = run(&["ROLE"], &mut server, &mut client);
            let RespFrame::Array(entries) = role else {
                panic!("ROLE should return array");
            };
            assert_eq!(entries[0], RespFrame::bulk_str("master"));
        }
        assert_eq!(
            run(&["SLAVEOF", "NO", "ONE"], &mut server, &mut client),
            RespFrame::ok()
        );
        let promoted_role = run(&["ROLE"], &mut server, &mut client);
        let RespFrame::Array(promoted_role_entries) = promoted_role else {
            panic!("ROLE after SLAVEOF NO ONE should return array");
        };
        assert_eq!(promoted_role_entries[0], RespFrame::bulk_str("master"));

        let dumped2 = run(&["DUMP", "foo"], &mut server, &mut client);
        let RespFrame::BulkString(Some(payload2)) = dumped2 else {
            panic!("DUMP should return payload");
        };
        let restore_asking = {
            let mut access = ServerAccess::new_inline(&mut server);
            execute(
                RespFrame::Array(vec![
                    RespFrame::bulk_str("RESTORE-ASKING"),
                    RespFrame::bulk_str("foo3"),
                    RespFrame::bulk_str("0"),
                    RespFrame::BulkString(Some(payload2)),
                ]),
                &mut access,
                &mut client,
            )
            .response
        };
        assert_eq!(restore_asking, RespFrame::ok());
        assert_eq!(
            run(&["GET", "foo3"], &mut server, &mut client),
            RespFrame::bulk_str("bar")
        );

        let info_server = run(&["INFO", "SERVER"], &mut server, &mut client);
        let RespFrame::BulkString(Some(info_server)) = info_server else {
            panic!("INFO SERVER should return bulk string");
        };
        let info_server_text = String::from_utf8_lossy(&info_server).to_string();
        assert!(info_server_text.contains("# Server"));
        assert!(info_server_text.contains("redis_mode:standalone"));
        assert!(info_server_text.contains("health_status:healthy"));
        assert!(info_server_text.contains("bridge_contract_version:0.1"));
        assert!(info_server_text.contains("ratatosk_compatibility_mode:compat"));
        assert!(info_server_text.contains("ratatosk_protected_mode:yes"));

        let info_keyspace = run(&["INFO", "KEYSPACE"], &mut server, &mut client);
        let RespFrame::BulkString(Some(info_keyspace)) = info_keyspace else {
            panic!("INFO KEYSPACE should return bulk string");
        };
        let info_keyspace_text = String::from_utf8_lossy(&info_keyspace).to_string();
        assert!(info_keyspace_text.contains("db0:keys="));

        assert_eq!(
            run(&["SELECT", "1"], &mut server, &mut client),
            RespFrame::ok()
        );
        assert_eq!(
            run(&["SET", "foo:db1", "bar"], &mut server, &mut client),
            RespFrame::ok()
        );
        assert_eq!(
            run(&["DBSIZE"], &mut server, &mut client),
            RespFrame::Integer(1)
        );

        assert_eq!(
            run(&["FLUSHALL", "ASYNC"], &mut server, &mut client),
            RespFrame::ok()
        );
        assert_eq!(
            run(&["DBSIZE"], &mut server, &mut client),
            RespFrame::Integer(0)
        );
        assert_eq!(
            run(&["SELECT", "0"], &mut server, &mut client),
            RespFrame::ok()
        );
        assert_eq!(
            run(&["DBSIZE"], &mut server, &mut client),
            RespFrame::Integer(0)
        );

        assert_eq!(
            run(
                &["COMMAND", "GETKEYS", "MSET", "a", "1", "b", "2"],
                &mut server,
                &mut client
            ),
            RespFrame::Array(vec![RespFrame::bulk_str("a"), RespFrame::bulk_str("b")])
        );

        let flags = run(
            &["COMMAND", "GETKEYSANDFLAGS", "GET", "a"],
            &mut server,
            &mut client,
        );
        let RespFrame::Array(rows) = flags else {
            panic!("COMMAND GETKEYSANDFLAGS should return array");
        };
        assert_eq!(rows.len(), 1);
        assert_eq!(
            rows[0],
            RespFrame::Array(vec![
                RespFrame::bulk_str("a"),
                RespFrame::Array(vec![
                    RespFrame::bulk_str("RW"),
                    RespFrame::bulk_str("access"),
                    RespFrame::bulk_str("update"),
                ]),
            ])
        );

        assert_eq!(
            run(&["FLUSHDB", "SYNC"], &mut server, &mut client),
            RespFrame::ok()
        );
        assert_eq!(
            run(&["DBSIZE"], &mut server, &mut client),
            RespFrame::Integer(0)
        );
    }

    #[test]
    fn acl_mutations_mark_outcome_acl_dirty() {
        let mut server = ServerState::with_default_dbs();
        let mut client = ClientState::new(1);
        client.authenticated = true;
        client.acl_user = Bytes::from_static(b"default");

        let setuser = run_full(
            &["ACL", "SETUSER", "alice", "on", ">secret", "+@all"],
            &mut server,
            &mut client,
        );
        assert!(setuser.acl_dirty);

        let deluser = run_full(&["ACL", "DELUSER", "alice"], &mut server, &mut client);
        assert!(deluser.acl_dirty);
    }

    #[test]
    fn precheck_execute_argv_authenticates_default_nopass_user() {
        let server = ServerState::with_default_dbs();
        let default_acl_policy = DefaultAclPolicyState::from_acl(&server.acl);
        let mut client = ClientState::new(1);

        let precheck = precheck_execute_argv_with_default_acl(
            &[Bytes::from_static(b"GET"), Bytes::from_static(b"key")],
            &default_acl_policy,
            &mut client,
        );

        assert!(matches!(precheck, ExecuteArgvPrecheck::Continue));
        assert!(client.is_authenticated());
        assert_eq!(client.acl_user(), &Bytes::from_static(b"default"));
    }

    #[test]
    fn precheck_execute_argv_rejects_when_auth_is_required() {
        let mut server = ServerState::with_default_dbs();
        let default_user = server
            .acl
            .get_or_create_user_mut(&Bytes::from_static(b"default"));
        default_user.nopass = false;
        let default_acl_policy = DefaultAclPolicyState::from_acl(&server.acl);
        let mut client = ClientState::new(2);

        let precheck = precheck_execute_argv_with_default_acl(
            &[Bytes::from_static(b"GET"), Bytes::from_static(b"key")],
            &default_acl_policy,
            &mut client,
        );

        let ExecuteArgvPrecheck::Reject(outcome) = precheck else {
            panic!("precheck should reject when auth is required");
        };
        assert_eq!(
            outcome.response,
            RespFrame::error_str("NOAUTH Authentication required.")
        );
        assert!(!client.is_authenticated());
    }

    #[test]
    fn precheck_execute_argv_allows_quit_without_auth() {
        let mut server = ServerState::with_default_dbs();
        let default_user = server
            .acl
            .get_or_create_user_mut(&Bytes::from_static(b"default"));
        default_user.nopass = false;
        let default_acl_policy = DefaultAclPolicyState::from_acl(&server.acl);
        let mut client = ClientState::new(3);

        let precheck = precheck_execute_argv_with_default_acl(
            &[Bytes::from_static(b"QUIT")],
            &default_acl_policy,
            &mut client,
        );

        assert!(matches!(precheck, ExecuteArgvPrecheck::Continue));
        assert!(!client.is_authenticated());
    }

    #[test]
    fn m0_admin_control_baseline_commands() {
        let mut server = ServerState::with_default_dbs();
        let mut client = ClientState::default();

        let docs = run(&["COMMAND", "DOCS", "GET"], &mut server, &mut client);
        let RespFrame::Map(doc_entries) = docs else {
            panic!("COMMAND DOCS should return map");
        };
        assert_eq!(doc_entries.len(), 1);
        assert_eq!(doc_entries[0].0, RespFrame::bulk_str("get"));
        let RespFrame::Map(get_docs) = &doc_entries[0].1 else {
            panic!("COMMAND DOCS entry should return map");
        };
        assert!(get_docs.iter().any(|(key, value)| {
            *key == RespFrame::bulk_str("ratatosk_capability_tier")
                && *value == RespFrame::bulk_str("behavioral_subset")
        }));

        let cluster_info = run(
            &["COMMAND", "INFO", "CLUSTER", "SLOTS"],
            &mut server,
            &mut client,
        );
        let RespFrame::Array(cluster_info_entries) = cluster_info else {
            panic!("COMMAND INFO CLUSTER SLOTS should return array");
        };
        assert_eq!(cluster_info_entries.len(), 1);
        assert_eq!(
            cluster_info_entries[0],
            RespFrame::Array(vec![
                RespFrame::bulk_str("cluster slots"),
                RespFrame::Integer(2),
                RespFrame::Array(vec![
                    RespFrame::bulk_str("admin"),
                    RespFrame::bulk_str("readonly"),
                ]),
                RespFrame::Integer(0),
                RespFrame::Integer(0),
                RespFrame::Integer(0),
            ])
        );

        let cluster_docs = run(
            &["COMMAND", "DOCS", "CLUSTER", "SLOTS"],
            &mut server,
            &mut client,
        );
        let RespFrame::Map(cluster_doc_entries) = cluster_docs else {
            panic!("COMMAND DOCS CLUSTER SLOTS should return map");
        };
        assert_eq!(cluster_doc_entries.len(), 1);
        let RespFrame::Map(cluster_slots_docs) = &cluster_doc_entries[0].1 else {
            panic!("COMMAND DOCS CLUSTER SLOTS entry should return map");
        };
        assert!(cluster_slots_docs.iter().any(|(key, value)| {
            *key == RespFrame::bulk_str("ratatosk_capability_tier")
                && *value == RespFrame::bulk_str("baseline_local")
        }));

        let cluster_help = run(&["CLUSTER", "HELP"], &mut server, &mut client);
        let RespFrame::Array(cluster_help_rows) = cluster_help else {
            panic!("CLUSTER HELP should return array");
        };
        assert!(cluster_help_rows.iter().any(|row| {
            *row == RespFrame::bulk_str(
                "Standalone mode supports INFO, MYID, KEYSLOT, COUNTKEYSINSLOT, GETKEYSINSLOT, SLOTS, SHARDS, LINKS, and HELP."
            )
        }));
        assert_eq!(
            run(&["CLUSTER", "SLOTS"], &mut server, &mut client),
            RespFrame::Array(vec![])
        );
        assert_eq!(
            run(&["CLUSTER", "SHARDS"], &mut server, &mut client),
            RespFrame::Array(vec![])
        );
        assert_eq!(
            run(&["CLUSTER", "LINKS"], &mut server, &mut client),
            RespFrame::Array(vec![])
        );
        assert_eq!(
            run(&["CLUSTER", "NODES"], &mut server, &mut client),
            RespFrame::error_str(
                "ERR Ratatosk is running in single-node mode; CLUSTER NODES is not available. See CLUSTER INFO for current status."
            )
        );

        let client_setinfo_docs = run(
            &["COMMAND", "DOCS", "CLIENT", "SETINFO"],
            &mut server,
            &mut client,
        );
        let RespFrame::Map(client_setinfo_entries) = client_setinfo_docs else {
            panic!("COMMAND DOCS CLIENT SETINFO should return map");
        };
        assert_eq!(client_setinfo_entries.len(), 1);
        let RespFrame::Map(client_setinfo_docs) = &client_setinfo_entries[0].1 else {
            panic!("COMMAND DOCS CLIENT SETINFO entry should return map");
        };
        assert!(client_setinfo_docs.iter().any(|(key, value)| {
            *key == RespFrame::bulk_str("ratatosk_capability_tier")
                && *value == RespFrame::bulk_str("baseline_local")
        }));

        let client_pause_docs = run(
            &["COMMAND", "DOCS", "CLIENT", "PAUSE"],
            &mut server,
            &mut client,
        );
        let RespFrame::Map(client_pause_entries) = client_pause_docs else {
            panic!("COMMAND DOCS CLIENT PAUSE should return map");
        };
        assert_eq!(client_pause_entries.len(), 1);
        let RespFrame::Map(client_pause_docs) = &client_pause_entries[0].1 else {
            panic!("COMMAND DOCS CLIENT PAUSE entry should return map");
        };
        assert!(client_pause_docs.iter().any(|(key, value)| {
            *key == RespFrame::bulk_str("ratatosk_capability_tier")
                && *value == RespFrame::bulk_str("unsupported")
        }));

        let client_unblock_docs = run(
            &["COMMAND", "DOCS", "CLIENT", "UNBLOCK"],
            &mut server,
            &mut client,
        );
        let RespFrame::Map(client_unblock_entries) = client_unblock_docs else {
            panic!("COMMAND DOCS CLIENT UNBLOCK should return map");
        };
        assert_eq!(client_unblock_entries.len(), 1);
        let RespFrame::Map(client_unblock_docs) = &client_unblock_entries[0].1 else {
            panic!("COMMAND DOCS CLIENT UNBLOCK entry should return map");
        };
        assert!(client_unblock_docs.iter().any(|(key, value)| {
            *key == RespFrame::bulk_str("ratatosk_capability_tier")
                && *value == RespFrame::bulk_str("syntax_only")
        }));

        let debug_help = run(&["DEBUG", "HELP"], &mut server, &mut client);
        let RespFrame::Array(debug_help_rows) = debug_help else {
            panic!("DEBUG HELP should return array");
        };
        assert!(!debug_help_rows.is_empty());
        assert_eq!(
            run(&["DEBUG", "OBJECT", "k"], &mut server, &mut client),
            RespFrame::error_str("ERR DEBUG subcommand is not supported")
        );

        let module_help = run(&["MODULE", "HELP"], &mut server, &mut client);
        let RespFrame::Array(module_help_rows) = module_help else {
            panic!("MODULE HELP should return array");
        };
        assert!(!module_help_rows.is_empty());
        let module_list_docs = run(
            &["COMMAND", "DOCS", "MODULE", "LIST"],
            &mut server,
            &mut client,
        );
        let RespFrame::Map(module_list_entries) = module_list_docs else {
            panic!("COMMAND DOCS MODULE LIST should return map");
        };
        assert_eq!(module_list_entries.len(), 1);
        let RespFrame::Map(module_list_docs) = &module_list_entries[0].1 else {
            panic!("COMMAND DOCS MODULE LIST entry should return map");
        };
        assert!(module_list_docs.iter().any(|(key, value)| {
            *key == RespFrame::bulk_str("ratatosk_capability_tier")
                && *value == RespFrame::bulk_str("baseline_local")
        }));
        let module_load_docs = run(
            &["COMMAND", "DOCS", "MODULE", "LOAD"],
            &mut server,
            &mut client,
        );
        let RespFrame::Map(module_load_entries) = module_load_docs else {
            panic!("COMMAND DOCS MODULE LOAD should return map");
        };
        assert_eq!(module_load_entries.len(), 1);
        let RespFrame::Map(module_load_docs) = &module_load_entries[0].1 else {
            panic!("COMMAND DOCS MODULE LOAD entry should return map");
        };
        assert!(module_load_docs.iter().any(|(key, value)| {
            *key == RespFrame::bulk_str("ratatosk_capability_tier")
                && *value == RespFrame::bulk_str("unsupported")
        }));
        assert_eq!(
            run(&["MODULE", "LIST"], &mut server, &mut client),
            RespFrame::Array(vec![])
        );
        assert_eq!(
            run(&["MODULE", "LOAD", "mod.so"], &mut server, &mut client),
            RespFrame::error_str("ERR MODULE command is not supported in this build")
        );

        assert_eq!(
            run(&["HOTKEYS", "GET"], &mut server, &mut client),
            RespFrame::Array(vec![])
        );
        let hotkeys_docs = run(
            &["COMMAND", "DOCS", "HOTKEYS", "GET"],
            &mut server,
            &mut client,
        );
        let RespFrame::Map(hotkeys_entries) = hotkeys_docs else {
            panic!("COMMAND DOCS HOTKEYS GET should return map");
        };
        assert_eq!(hotkeys_entries.len(), 1);
        let RespFrame::Map(hotkeys_docs) = &hotkeys_entries[0].1 else {
            panic!("COMMAND DOCS HOTKEYS GET entry should return map");
        };
        assert!(hotkeys_docs.iter().any(|(key, value)| {
            *key == RespFrame::bulk_str("ratatosk_capability_tier")
                && *value == RespFrame::bulk_str("baseline_local")
        }));
        assert_eq!(
            run(&["HOTKEYS", "START"], &mut server, &mut client),
            RespFrame::ok()
        );
        assert_eq!(
            run(&["HOTKEYS", "RESET"], &mut server, &mut client),
            RespFrame::ok()
        );
        assert_eq!(
            run(&["HOTKEYS", "STOP"], &mut server, &mut client),
            RespFrame::ok()
        );

        let lolwut = run(&["LOLWUT"], &mut server, &mut client);
        let RespFrame::BulkString(Some(lolwut_text)) = lolwut else {
            panic!("LOLWUT should return bulk string");
        };
        let lolwut_text = String::from_utf8_lossy(&lolwut_text).to_string();
        assert!(lolwut_text.contains("Ratatosk"));
        let lolwut_docs = run(&["COMMAND", "DOCS", "LOLWUT"], &mut server, &mut client);
        let RespFrame::Map(lolwut_entries) = lolwut_docs else {
            panic!("COMMAND DOCS LOLWUT should return map");
        };
        assert_eq!(lolwut_entries.len(), 1);
        let RespFrame::Map(lolwut_docs) = &lolwut_entries[0].1 else {
            panic!("COMMAND DOCS LOLWUT entry should return map");
        };
        assert!(lolwut_docs.iter().any(|(key, value)| {
            *key == RespFrame::bulk_str("ratatosk_capability_tier")
                && *value == RespFrame::bulk_str("baseline_local")
        }));

        assert_eq!(
            run(&["BGREWRITEAOF"], &mut server, &mut client),
            RespFrame::error_str("ERR BGREWRITEAOF requires appendonly to be enabled")
        );
        assert_eq!(
            run(&["SFLUSH", "SYNC"], &mut server, &mut client),
            RespFrame::ok()
        );

        assert_eq!(
            run(&["SET", "k0", "v0"], &mut server, &mut client),
            RespFrame::ok()
        );
        assert_eq!(
            run(&["SELECT", "1"], &mut server, &mut client),
            RespFrame::ok()
        );
        assert_eq!(
            run(&["SET", "k1", "v1"], &mut server, &mut client),
            RespFrame::ok()
        );

        assert_eq!(
            run(&["SWAPDB", "0", "1"], &mut server, &mut client),
            RespFrame::ok()
        );
        assert_eq!(
            run(&["GET", "k0"], &mut server, &mut client),
            RespFrame::bulk_str("v0")
        );
        assert_eq!(
            run(&["GET", "k1"], &mut server, &mut client),
            RespFrame::BulkString(None)
        );

        assert_eq!(
            run(&["SELECT", "0"], &mut server, &mut client),
            RespFrame::ok()
        );
        assert_eq!(
            run(&["GET", "k1"], &mut server, &mut client),
            RespFrame::bulk_str("v1")
        );

        assert_eq!(
            run(&["TRIMSLOTS", "0"], &mut server, &mut client),
            RespFrame::Integer(0)
        );
        let trimslots_docs = run(&["COMMAND", "DOCS", "TRIMSLOTS"], &mut server, &mut client);
        let RespFrame::Map(trimslots_entries) = trimslots_docs else {
            panic!("COMMAND DOCS TRIMSLOTS should return map");
        };
        assert_eq!(trimslots_entries.len(), 1);
        let RespFrame::Map(trimslots_docs) = &trimslots_entries[0].1 else {
            panic!("COMMAND DOCS TRIMSLOTS entry should return map");
        };
        assert!(trimslots_docs.iter().any(|(key, value)| {
            *key == RespFrame::bulk_str("ratatosk_capability_tier")
                && *value == RespFrame::bulk_str("baseline_local")
        }));
        assert_eq!(
            run(&["FAILOVER"], &mut server, &mut client),
            RespFrame::error_str("ERR FAILOVER is not supported in standalone mode")
        );
        assert_eq!(
            run(&["SHUTDOWN"], &mut server, &mut client),
            RespFrame::error_str("ERR SHUTDOWN is not supported in this build")
        );
    }

    #[test]
    fn config_slowlog_memory_baseline() {
        let mut server = ServerState::with_default_dbs();
        let mut client = ClientState::default();

        assert_eq!(
            run(
                &[
                    "CONFIG",
                    "SET",
                    "timeout",
                    "15",
                    "appendonly",
                    "yes",
                    "slowlog-log-slower-than",
                    "0",
                    "slowlog-max-len",
                    "2",
                ],
                &mut server,
                &mut client,
            ),
            RespFrame::ok()
        );

        assert_eq!(
            run(&["CONFIG", "GET", "timeout"], &mut server, &mut client),
            RespFrame::Array(vec![
                RespFrame::bulk_str("timeout"),
                RespFrame::bulk_str("15")
            ])
        );
        assert_eq!(
            run(&["CONFIG", "GET", "appendonly"], &mut server, &mut client),
            RespFrame::Array(vec![
                RespFrame::bulk_str("appendonly"),
                RespFrame::bulk_str("yes")
            ])
        );

        assert_eq!(
            run(&["CONFIG", "GET", "dir"], &mut server, &mut client),
            RespFrame::Array(vec![RespFrame::bulk_str("dir"), RespFrame::bulk_str(".")])
        );
        assert_eq!(
            run(&["CONFIG", "GET", "dbfilename"], &mut server, &mut client),
            RespFrame::Array(vec![
                RespFrame::bulk_str("dbfilename"),
                RespFrame::bulk_str("dump.rdb")
            ])
        );
        assert_eq!(
            run(&["CONFIG", "GET", "appendfsync"], &mut server, &mut client),
            RespFrame::Array(vec![
                RespFrame::bulk_str("appendfsync"),
                RespFrame::bulk_str("everysec")
            ])
        );

        assert_eq!(
            run(
                &["CONFIG", "SET", "dir", "/tmp/ratatosk"],
                &mut server,
                &mut client,
            ),
            RespFrame::ok()
        );
        assert_eq!(
            run(&["CONFIG", "GET", "dir"], &mut server, &mut client),
            RespFrame::Array(vec![
                RespFrame::bulk_str("dir"),
                RespFrame::bulk_str("/tmp/ratatosk")
            ])
        );

        assert_eq!(
            run(
                &["CONFIG", "SET", "dbfilename", "backup.rdb"],
                &mut server,
                &mut client,
            ),
            RespFrame::ok()
        );
        assert_eq!(
            run(&["CONFIG", "GET", "dbfilename"], &mut server, &mut client),
            RespFrame::Array(vec![
                RespFrame::bulk_str("dbfilename"),
                RespFrame::bulk_str("backup.rdb")
            ])
        );

        assert_eq!(
            run(
                &["CONFIG", "SET", "appendfsync", "always"],
                &mut server,
                &mut client,
            ),
            RespFrame::ok()
        );
        assert_eq!(
            run(&["CONFIG", "GET", "appendfsync"], &mut server, &mut client),
            RespFrame::Array(vec![
                RespFrame::bulk_str("appendfsync"),
                RespFrame::bulk_str("always")
            ])
        );

        let config_help = run(&["CONFIG", "HELP"], &mut server, &mut client);
        let RespFrame::Array(config_help_rows) = config_help else {
            panic!("CONFIG HELP should return array");
        };
        assert!(!config_help_rows.is_empty());

        assert_eq!(
            run(&["SLOWLOG", "RESET"], &mut server, &mut client),
            RespFrame::ok()
        );
        assert_eq!(run(&["PING"], &mut server, &mut client), RespFrame::pong());
        assert_eq!(
            run(&["ECHO", "x"], &mut server, &mut client),
            RespFrame::bulk_str("x")
        );
        assert_eq!(
            run(&["GET", "missing"], &mut server, &mut client),
            RespFrame::BulkString(None)
        );

        assert_eq!(
            run(&["SLOWLOG", "LEN"], &mut server, &mut client),
            RespFrame::Integer(2)
        );

        let slowlog_get = run(&["SLOWLOG", "GET", "10"], &mut server, &mut client);
        let RespFrame::Array(entries) = slowlog_get else {
            panic!("SLOWLOG GET should return array");
        };
        assert_eq!(entries.len(), 2);

        let RespFrame::Array(first) = &entries[0] else {
            panic!("slowlog row should be array");
        };
        assert_eq!(first.len(), 4);
        let RespFrame::Array(first_argv) = &first[3] else {
            panic!("slowlog argv should be array");
        };
        assert_eq!(first_argv[0], RespFrame::bulk_str("GET"));

        assert_eq!(
            run(&["SET", "mk", "mv"], &mut server, &mut client),
            RespFrame::ok()
        );
        let usage = run(&["MEMORY", "USAGE", "mk"], &mut server, &mut client);
        let RespFrame::Integer(usage) = usage else {
            panic!("MEMORY USAGE should return integer for existing key");
        };
        assert!(usage > 0);

        assert_eq!(
            run(&["MEMORY", "USAGE", "missing"], &mut server, &mut client),
            RespFrame::Null
        );

        let memory_help = run(&["MEMORY", "HELP"], &mut server, &mut client);
        let RespFrame::Array(memory_help_rows) = memory_help else {
            panic!("MEMORY HELP should return array");
        };
        assert!(!memory_help_rows.is_empty());

        let memory_stats = run(&["MEMORY", "STATS"], &mut server, &mut client);
        let RespFrame::Map(memory_stats_rows) = memory_stats else {
            panic!("MEMORY STATS should return map");
        };
        assert!(!memory_stats_rows.is_empty());

        // Server has keys (from earlier SET mk mv): DOCTOR should report no problems
        let doctor_response = run(&["MEMORY", "DOCTOR"], &mut server, &mut client);
        assert_eq!(
            doctor_response,
            RespFrame::bulk_str("Sam, I have no memory problems")
        );

        let malloc_stats = run(&["MEMORY", "MALLOC-STATS"], &mut server, &mut client);
        let RespFrame::BulkString(Some(malloc_bytes)) = &malloc_stats else {
            panic!("MEMORY MALLOC-STATS should return bulk string");
        };
        let malloc_text = String::from_utf8_lossy(malloc_bytes);
        assert!(!malloc_text.is_empty());
        assert_eq!(
            run(&["MEMORY", "PURGE"], &mut server, &mut client),
            RespFrame::ok()
        );

        assert_eq!(
            run(&["CONFIG", "REWRITE"], &mut server, &mut client),
            RespFrame::ok()
        );
        assert_eq!(
            run(&["BGSAVE"], &mut server, &mut client),
            RespFrame::simple_str("Background saving started")
        );
        assert_eq!(
            run(&["BGSAVE", "SCHEDULE"], &mut server, &mut client),
            RespFrame::simple_str("Background saving started")
        );

        assert_eq!(
            run(&["CONFIG", "RESETSTAT"], &mut server, &mut client),
            RespFrame::ok()
        );
    }

    #[test]
    fn m0_script_and_function_surface_matches_capabilities() {
        let mut server = ServerState::with_default_dbs();
        let mut client = ClientState::default();

        let script_load_docs = run(
            &["COMMAND", "DOCS", "SCRIPT", "LOAD"],
            &mut server,
            &mut client,
        );
        let RespFrame::Map(script_load_entries) = script_load_docs else {
            panic!("COMMAND DOCS SCRIPT LOAD should return map");
        };
        assert_eq!(script_load_entries.len(), 1);
        let RespFrame::Map(script_load_docs) = &script_load_entries[0].1 else {
            panic!("COMMAND DOCS SCRIPT LOAD entry should return map");
        };
        assert!(script_load_docs.iter().any(|(key, value)| {
            *key == RespFrame::bulk_str("ratatosk_capability_tier")
                && *value == RespFrame::bulk_str("baseline_local")
        }));

        let script_kill_docs = run(
            &["COMMAND", "DOCS", "SCRIPT", "KILL"],
            &mut server,
            &mut client,
        );
        let RespFrame::Map(script_kill_entries) = script_kill_docs else {
            panic!("COMMAND DOCS SCRIPT KILL should return map");
        };
        assert_eq!(script_kill_entries.len(), 1);
        let RespFrame::Map(script_kill_docs) = &script_kill_entries[0].1 else {
            panic!("COMMAND DOCS SCRIPT KILL entry should return map");
        };
        assert!(script_kill_docs.iter().any(|(key, value)| {
            *key == RespFrame::bulk_str("ratatosk_capability_tier")
                && *value == RespFrame::bulk_str("unsupported")
        }));

        let function_flush_docs = run(
            &["COMMAND", "DOCS", "FUNCTION", "FLUSH"],
            &mut server,
            &mut client,
        );
        let RespFrame::Map(function_flush_entries) = function_flush_docs else {
            panic!("COMMAND DOCS FUNCTION FLUSH should return map");
        };
        assert_eq!(function_flush_entries.len(), 1);
        let RespFrame::Map(function_flush_docs) = &function_flush_entries[0].1 else {
            panic!("COMMAND DOCS FUNCTION FLUSH entry should return map");
        };
        assert!(function_flush_docs.iter().any(|(key, value)| {
            *key == RespFrame::bulk_str("ratatosk_capability_tier")
                && *value == RespFrame::bulk_str("baseline_local")
        }));

        let function_load_docs = run(
            &["COMMAND", "DOCS", "FUNCTION", "LOAD"],
            &mut server,
            &mut client,
        );
        let RespFrame::Map(function_load_entries) = function_load_docs else {
            panic!("COMMAND DOCS FUNCTION LOAD should return map");
        };
        assert_eq!(function_load_entries.len(), 1);
        let RespFrame::Map(function_load_docs) = &function_load_entries[0].1 else {
            panic!("COMMAND DOCS FUNCTION LOAD entry should return map");
        };
        assert!(function_load_docs.iter().any(|(key, value)| {
            *key == RespFrame::bulk_str("ratatosk_capability_tier")
                && *value == RespFrame::bulk_str("unsupported")
        }));

        let script_help = run(&["SCRIPT", "HELP"], &mut server, &mut client);
        let RespFrame::Array(script_help_rows) = script_help else {
            panic!("SCRIPT HELP should return array");
        };
        assert!(script_help_rows.iter().any(|row| {
            *row == RespFrame::bulk_str(
                "LOAD, EXISTS, FLUSH, and HELP are available in this build. SCRIPT DEBUG and SCRIPT KILL are unsupported."
            )
        }));

        let loaded = run(&["SCRIPT", "LOAD", "return 1"], &mut server, &mut client);
        let RespFrame::BulkString(Some(sha)) = loaded else {
            panic!("SCRIPT LOAD should return sha");
        };
        let sha_text = String::from_utf8_lossy(&sha).into_owned();
        assert_eq!(sha_text.len(), 40);
        assert_eq!(
            run(
                &["SCRIPT", "EXISTS", sha_text.as_str()],
                &mut server,
                &mut client
            ),
            RespFrame::Array(vec![RespFrame::Integer(1)])
        );
        assert_eq!(
            run(&["SCRIPT", "KILL"], &mut server, &mut client),
            RespFrame::error_str("ERR SCRIPT KILL is not supported in this build")
        );
        assert_eq!(
            run(&["SCRIPT", "DEBUG", "YES"], &mut server, &mut client),
            RespFrame::error_str("ERR SCRIPT DEBUG is not supported in this build")
        );
        assert_eq!(
            run(&["SCRIPT", "FLUSH", "SYNC"], &mut server, &mut client),
            RespFrame::ok()
        );
        assert_eq!(
            run(
                &["SCRIPT", "EXISTS", sha_text.as_str()],
                &mut server,
                &mut client
            ),
            RespFrame::Array(vec![RespFrame::Integer(0)])
        );

        let function_help = run(&["FUNCTION", "HELP"], &mut server, &mut client);
        let RespFrame::Array(function_help_rows) = function_help else {
            panic!("FUNCTION HELP should return array");
        };
        assert!(function_help_rows.iter().any(|row| {
            *row == RespFrame::bulk_str(
                "LIST, DUMP, STATS, FLUSH, and HELP are available in this build. LOAD, DELETE, and RESTORE are unsupported."
            )
        }));
        assert_eq!(
            run(&["FUNCTION", "LIST"], &mut server, &mut client),
            RespFrame::Array(vec![])
        );
        assert_eq!(
            run(&["FUNCTION", "DUMP"], &mut server, &mut client),
            RespFrame::BulkString(None)
        );
        let function_stats = run(&["FUNCTION", "STATS"], &mut server, &mut client);
        let RespFrame::Array(function_stats_rows) = function_stats else {
            panic!("FUNCTION STATS should return array");
        };
        assert_eq!(function_stats_rows.len(), 4);
        assert_eq!(
            run(&["FUNCTION", "FLUSH", "ASYNC"], &mut server, &mut client),
            RespFrame::ok()
        );
        assert_eq!(
            run(
                &[
                    "FUNCTION",
                    "LOAD",
                    "redis.register_function('f', function() return 1 end)",
                ],
                &mut server,
                &mut client,
            ),
            RespFrame::error_str("ERR Function not supported in this build")
        );
    }

    #[test]
    fn sentinel_surface_matches_help_and_capabilities() {
        let mut server = ServerState::with_default_dbs();
        let mut client = ClientState::default();

        let sentinel_help_docs = run(
            &["COMMAND", "DOCS", "SENTINEL", "HELP"],
            &mut server,
            &mut client,
        );
        let RespFrame::Map(sentinel_help_entries) = sentinel_help_docs else {
            panic!("COMMAND DOCS SENTINEL HELP should return map");
        };
        assert_eq!(sentinel_help_entries.len(), 1);
        let RespFrame::Map(sentinel_help_docs) = &sentinel_help_entries[0].1 else {
            panic!("COMMAND DOCS SENTINEL HELP entry should return map");
        };
        assert!(sentinel_help_docs.iter().any(|(key, value)| {
            *key == RespFrame::bulk_str("ratatosk_capability_tier")
                && *value == RespFrame::bulk_str("syntax_only")
        }));

        let sentinel_masters_docs = run(
            &["COMMAND", "DOCS", "SENTINEL", "MASTERS"],
            &mut server,
            &mut client,
        );
        let RespFrame::Map(sentinel_masters_entries) = sentinel_masters_docs else {
            panic!("COMMAND DOCS SENTINEL MASTERS should return map");
        };
        assert_eq!(sentinel_masters_entries.len(), 1);
        let RespFrame::Map(sentinel_masters_docs) = &sentinel_masters_entries[0].1 else {
            panic!("COMMAND DOCS SENTINEL MASTERS entry should return map");
        };
        assert!(sentinel_masters_docs.iter().any(|(key, value)| {
            *key == RespFrame::bulk_str("ratatosk_capability_tier")
                && *value == RespFrame::bulk_str("unsupported")
        }));

        let sentinel_help = run(&["SENTINEL", "HELP"], &mut server, &mut client);
        let RespFrame::Array(sentinel_help_rows) = sentinel_help else {
            panic!("SENTINEL HELP should return array");
        };
        assert!(sentinel_help_rows.iter().any(|row| {
            *row == RespFrame::bulk_str(
                "Only SENTINEL HELP is available in this build; all other SENTINEL subcommands return an error."
            )
        }));

        assert_eq!(
            run(&["SENTINEL", "HELP", "EXTRA"], &mut server, &mut client),
            RespFrame::error_str("ERR wrong number of arguments for 'sentinel' command")
        );
        assert_eq!(
            run(&["SENTINEL", "MASTERS"], &mut server, &mut client),
            RespFrame::error_str("ERR This instance is not configured as a Sentinel")
        );
    }

    #[test]
    fn latency_baseline_commands() {
        let mut server = ServerState::with_default_dbs();
        let mut client = ClientState::default();

        assert_eq!(
            run(
                &["CONFIG", "SET", "latency-tracking", "yes"],
                &mut server,
                &mut client
            ),
            RespFrame::ok()
        );

        let help = run(&["LATENCY", "HELP"], &mut server, &mut client);
        let RespFrame::Array(help_rows) = help else {
            panic!("LATENCY HELP should return array");
        };
        assert!(!help_rows.is_empty());

        assert_eq!(run(&["PING"], &mut server, &mut client), RespFrame::pong());
        assert_eq!(
            run(&["SET", "lat:k", "v"], &mut server, &mut client),
            RespFrame::ok()
        );

        let latest = run(&["LATENCY", "LATEST"], &mut server, &mut client);
        let RespFrame::Array(latest_rows) = latest else {
            panic!("LATENCY LATEST should return array");
        };
        assert!(!latest_rows.is_empty());

        let history = run(&["LATENCY", "HISTORY", "ping"], &mut server, &mut client);
        let RespFrame::Array(history_rows) = history else {
            panic!("LATENCY HISTORY should return array");
        };
        assert!(!history_rows.is_empty());

        let graph = run(&["LATENCY", "GRAPH", "ping"], &mut server, &mut client);
        let RespFrame::BulkString(Some(graph_text)) = graph else {
            panic!("LATENCY GRAPH should return bulk string");
        };
        assert!(String::from_utf8_lossy(&graph_text).contains("ping"));

        let histogram = run(&["LATENCY", "HISTOGRAM", "ping"], &mut server, &mut client);
        let RespFrame::Array(hist_rows) = histogram else {
            panic!("LATENCY HISTOGRAM should return array");
        };
        assert!(!hist_rows.is_empty());

        let doctor = run(&["LATENCY", "DOCTOR"], &mut server, &mut client);
        let RespFrame::BulkString(Some(_)) = doctor else {
            panic!("LATENCY DOCTOR should return bulk string");
        };

        let reset = run(&["LATENCY", "RESET", "ping"], &mut server, &mut client);
        let RespFrame::Integer(reset_count) = reset else {
            panic!("LATENCY RESET should return integer");
        };
        assert!(reset_count >= 0);

        assert_eq!(
            run(&["LATENCY", "HISTORY", "ping"], &mut server, &mut client),
            RespFrame::Array(vec![])
        );
    }

    #[test]
    fn pubsub_baseline_commands() {
        let mut server = ServerState::with_default_dbs();
        let mut client = ClientState::default();
        // Hold the mpsc receiver so try_send() succeeds in publish().
        let _rx = server.pubsub.register_client(client.id());

        assert_eq!(
            run(&["SUBSCRIBE", "chan:1"], &mut server, &mut client),
            pubsub_ack("subscribe", Some("chan:1"), 1)
        );

        assert_eq!(
            run(&["PSUBSCRIBE", "chan:*"], &mut server, &mut client),
            pubsub_ack("psubscribe", Some("chan:*"), 2)
        );

        assert_eq!(
            run(&["SSUBSCRIBE", "s:1"], &mut server, &mut client),
            pubsub_ack("ssubscribe", Some("s:1"), 1)
        );

        assert_eq!(
            run(&["PUBLISH", "chan:1", "hello"], &mut server, &mut client),
            RespFrame::Integer(2)
        );
        assert_eq!(
            run(&["PUBLISH", "chan:2", "hello"], &mut server, &mut client),
            RespFrame::Integer(1)
        );
        assert_eq!(
            run(&["SPUBLISH", "s:1", "shard"], &mut server, &mut client),
            RespFrame::Integer(1)
        );

        assert_eq!(
            run(&["PUBSUB", "NUMPAT"], &mut server, &mut client),
            RespFrame::Integer(1)
        );
        assert_eq!(
            run(&["PUBSUB", "CHANNELS"], &mut server, &mut client),
            RespFrame::Array(vec![RespFrame::bulk_str("chan:1")])
        );
        assert_eq!(
            run(&["PUBSUB", "CHANNELS", "chan:*"], &mut server, &mut client),
            RespFrame::Array(vec![RespFrame::bulk_str("chan:1")])
        );
        assert_eq!(
            run(
                &["PUBSUB", "NUMSUB", "chan:1", "chan:x"],
                &mut server,
                &mut client
            ),
            RespFrame::Array(vec![
                RespFrame::bulk_str("chan:1"),
                RespFrame::Integer(1),
                RespFrame::bulk_str("chan:x"),
                RespFrame::Integer(0),
            ])
        );

        let help = run(&["PUBSUB", "HELP"], &mut server, &mut client);
        let RespFrame::Array(help_rows) = help else {
            panic!("PUBSUB HELP should return array");
        };
        assert!(!help_rows.is_empty());

        assert_eq!(
            run(&["UNSUBSCRIBE", "chan:1"], &mut server, &mut client),
            pubsub_ack("unsubscribe", Some("chan:1"), 1)
        );
        assert_eq!(
            run(&["PUNSUBSCRIBE"], &mut server, &mut client),
            pubsub_ack("punsubscribe", Some("chan:*"), 0)
        );
        assert_eq!(
            run(&["UNSUBSCRIBE"], &mut server, &mut client),
            pubsub_ack("unsubscribe", None, 0)
        );

        assert_eq!(
            run(&["PUBSUB", "SHARDCHANNELS"], &mut server, &mut client),
            RespFrame::Array(vec![RespFrame::bulk_str("s:1")])
        );
        assert_eq!(
            run(
                &["PUBSUB", "SHARDNUMSUB", "s:1", "s:2"],
                &mut server,
                &mut client,
            ),
            RespFrame::Array(vec![
                RespFrame::bulk_str("s:1"),
                RespFrame::Integer(1),
                RespFrame::bulk_str("s:2"),
                RespFrame::Integer(0),
            ])
        );

        assert_eq!(
            run(&["SUNSUBSCRIBE"], &mut server, &mut client),
            pubsub_ack("sunsubscribe", Some("s:1"), 0)
        );

        assert_eq!(
            run(&["PUBSUB", "NUMPAT"], &mut server, &mut client),
            RespFrame::Integer(0)
        );

        assert_eq!(run(&["MULTI"], &mut server, &mut client), RespFrame::ok());
        assert_eq!(
            run(
                &["SUBSCRIBE", "transaction:channel"],
                &mut server,
                &mut client
            ),
            RespFrame::queued()
        );
        assert_eq!(
            run(&["EXEC"], &mut server, &mut client),
            RespFrame::Array(vec![pubsub_ack(
                "subscribe",
                Some("transaction:channel"),
                1
            )])
        );
    }

    #[test]
    fn m3_stream_core_commands() {
        let mut server = ServerState::with_default_dbs();
        let mut client = ClientState::default();

        assert_eq!(
            run(&["SET", "notstream", "v"], &mut server, &mut client),
            RespFrame::ok()
        );
        assert_eq!(
            run(
                &["XADD", "notstream", "*", "f", "v"],
                &mut server,
                &mut client
            ),
            RespFrame::error_str(
                "WRONGTYPE Operation against a key holding the wrong kind of value"
            )
        );

        let xadd1 = run(
            &["XADD", "mystream", "*", "f1", "v1"],
            &mut server,
            &mut client,
        );
        let RespFrame::BulkString(Some(id1)) = xadd1 else {
            panic!("XADD should return generated id");
        };
        let xadd2 = run(
            &["XADD", "mystream", "*", "f2", "v2"],
            &mut server,
            &mut client,
        );
        let RespFrame::BulkString(Some(id2)) = xadd2 else {
            panic!("XADD should return generated id");
        };

        let parse_id = |raw: &[u8]| {
            let text = String::from_utf8_lossy(raw);
            let (ms, seq) = text.split_once('-').expect("stream id format");
            let ms = ms.parse::<i64>().expect("ms");
            let seq = seq.parse::<i64>().expect("seq");
            (ms, seq)
        };

        let p1 = parse_id(&id1);
        let p2 = parse_id(&id2);
        assert!(p2 >= p1);

        let id2_text = String::from_utf8_lossy(&id2).to_string();
        assert_eq!(
            run(
                &["XADD", "mystream", id2_text.as_str(), "f3", "v3"],
                &mut server,
                &mut client,
            ),
            RespFrame::error_str(
                "ERR The ID specified in XADD is equal or smaller than the target stream top item",
            )
        );
        assert_eq!(
            run(
                &["XADD", "mystream", "0-0", "f", "v"],
                &mut server,
                &mut client
            ),
            RespFrame::error_str("ERR The ID specified in XADD must be greater than 0-0")
        );

        assert_eq!(
            run(&["XLEN", "mystream"], &mut server, &mut client),
            RespFrame::Integer(2)
        );

        let xrange = run(&["XRANGE", "mystream", "-", "+"], &mut server, &mut client);
        let RespFrame::Array(xrange_rows) = xrange else {
            panic!("XRANGE should return array");
        };
        assert_eq!(xrange_rows.len(), 2);

        let xrevrange = run(
            &["XREVRANGE", "mystream", "+", "-", "COUNT", "1"],
            &mut server,
            &mut client,
        );
        let RespFrame::Array(xrev_rows) = xrevrange else {
            panic!("XREVRANGE should return array");
        };
        assert_eq!(xrev_rows.len(), 1);

        let xread = run(
            &["XREAD", "STREAMS", "mystream", "0-0"],
            &mut server,
            &mut client,
        );
        let RespFrame::Array(stream_rows) = xread else {
            panic!("XREAD should return array");
        };
        assert_eq!(stream_rows.len(), 1);

        let xread_count = run(
            &["XREAD", "COUNT", "1", "STREAMS", "mystream", "0-0"],
            &mut server,
            &mut client,
        );
        let RespFrame::Array(stream_count_rows) = xread_count else {
            panic!("XREAD COUNT should return array");
        };
        assert_eq!(stream_count_rows.len(), 1);
        let RespFrame::Array(stream_row) = &stream_count_rows[0] else {
            panic!("XREAD stream row should be array");
        };
        let RespFrame::Array(entry_rows) = &stream_row[1] else {
            panic!("XREAD entries should be array");
        };
        assert_eq!(entry_rows.len(), 1);

        assert_eq!(
            run(
                &["XREAD", "STREAMS", "mystream", "$"],
                &mut server,
                &mut client,
            ),
            RespFrame::Null
        );

        let xgroup_help = run(&["XGROUP", "HELP"], &mut server, &mut client);
        let RespFrame::Array(xgroup_help_rows) = xgroup_help else {
            panic!("XGROUP HELP should return array");
        };
        assert!(!xgroup_help_rows.is_empty());

        assert_eq!(
            run(
                &["XGROUP", "CREATE", "mystream", "g1", "0-0"],
                &mut server,
                &mut client,
            ),
            RespFrame::ok()
        );
        assert_eq!(
            run(
                &["XGROUP", "CREATE", "mystream", "g1", "0-0"],
                &mut server,
                &mut client,
            ),
            RespFrame::error_str("BUSYGROUP Consumer Group name already exists")
        );

        assert_eq!(
            run(
                &["XGROUP", "CREATECONSUMER", "mystream", "g1", "c2"],
                &mut server,
                &mut client,
            ),
            RespFrame::Integer(1)
        );
        assert_eq!(
            run(
                &["XGROUP", "CREATECONSUMER", "mystream", "g1", "c2"],
                &mut server,
                &mut client,
            ),
            RespFrame::Integer(0)
        );

        let xreadgroup = run(
            &[
                "XREADGROUP",
                "GROUP",
                "g1",
                "c1",
                "COUNT",
                "1",
                "STREAMS",
                "mystream",
                ">",
            ],
            &mut server,
            &mut client,
        );
        let RespFrame::Array(xrg_rows) = xreadgroup else {
            panic!("XREADGROUP should return array");
        };
        assert_eq!(xrg_rows.len(), 1);
        let RespFrame::Array(stream_row) = &xrg_rows[0] else {
            panic!("XREADGROUP row should be array");
        };
        let RespFrame::Array(entry_rows) = &stream_row[1] else {
            panic!("XREADGROUP entries should be array");
        };
        assert_eq!(entry_rows.len(), 1);
        let RespFrame::Array(first_entry) = &entry_rows[0] else {
            panic!("XREADGROUP entry should be array");
        };
        let RespFrame::BulkString(Some(delivered_id)) = &first_entry[0] else {
            panic!("XREADGROUP entry id should be bulk string");
        };
        let delivered_id_text = String::from_utf8_lossy(delivered_id).to_string();

        let xpending_summary = run(&["XPENDING", "mystream", "g1"], &mut server, &mut client);
        let RespFrame::Array(summary_rows) = xpending_summary else {
            panic!("XPENDING summary should return array");
        };
        assert_eq!(summary_rows[0], RespFrame::Integer(1));

        assert_eq!(
            run(
                &["XACK", "mystream", "g1", delivered_id_text.as_str()],
                &mut server,
                &mut client,
            ),
            RespFrame::Integer(1)
        );

        let xpending_after_ack = run(&["XPENDING", "mystream", "g1"], &mut server, &mut client);
        let RespFrame::Array(summary_after_ack) = xpending_after_ack else {
            panic!("XPENDING summary should return array");
        };
        assert_eq!(summary_after_ack[0], RespFrame::Integer(0));

        let xinfo_help = run(&["XINFO", "HELP"], &mut server, &mut client);
        let RespFrame::Array(xinfo_help_rows) = xinfo_help else {
            panic!("XINFO HELP should return array");
        };
        assert!(!xinfo_help_rows.is_empty());

        let xinfo_stream = run(&["XINFO", "STREAM", "mystream"], &mut server, &mut client);
        let RespFrame::Array(xinfo_stream_rows) = xinfo_stream else {
            panic!("XINFO STREAM should return array");
        };
        assert!(!xinfo_stream_rows.is_empty());

        let xinfo_groups = run(&["XINFO", "GROUPS", "mystream"], &mut server, &mut client);
        let RespFrame::Array(xinfo_group_rows) = xinfo_groups else {
            panic!("XINFO GROUPS should return array");
        };
        assert_eq!(xinfo_group_rows.len(), 1);

        let xreadgroup_second = run(
            &[
                "XREADGROUP",
                "GROUP",
                "g1",
                "c1",
                "COUNT",
                "1",
                "STREAMS",
                "mystream",
                ">",
            ],
            &mut server,
            &mut client,
        );
        let RespFrame::Array(xrg_second_rows) = xreadgroup_second else {
            panic!("XREADGROUP second read should return array");
        };
        let RespFrame::Array(xrg_second_stream_row) = &xrg_second_rows[0] else {
            panic!("XREADGROUP stream row should be array");
        };
        let RespFrame::Array(xrg_second_entries) = &xrg_second_stream_row[1] else {
            panic!("XREADGROUP entries should be array");
        };
        let RespFrame::Array(xrg_second_entry) = &xrg_second_entries[0] else {
            panic!("XREADGROUP entry should be array");
        };
        let RespFrame::BulkString(Some(delivered_id2)) = &xrg_second_entry[0] else {
            panic!("XREADGROUP second id should be bulk string");
        };
        let delivered_id2_text = String::from_utf8_lossy(delivered_id2).to_string();

        let xinfo_consumers = run(
            &["XINFO", "CONSUMERS", "mystream", "g1"],
            &mut server,
            &mut client,
        );
        let RespFrame::Array(xinfo_consumer_rows) = xinfo_consumers else {
            panic!("XINFO CONSUMERS should return array");
        };
        assert!(!xinfo_consumer_rows.is_empty());

        assert_eq!(
            run(
                &[
                    "XCLAIM",
                    "mystream",
                    "g1",
                    "c2",
                    "0",
                    delivered_id2_text.as_str(),
                    "JUSTID",
                ],
                &mut server,
                &mut client,
            ),
            RespFrame::Array(vec![RespFrame::bulk_str(delivered_id2_text.as_str())])
        );

        let xautoclaim = run(
            &[
                "XAUTOCLAIM",
                "mystream",
                "g1",
                "c1",
                "0",
                "0-0",
                "COUNT",
                "10",
                "JUSTID",
            ],
            &mut server,
            &mut client,
        );
        let RespFrame::Array(xautoclaim_rows) = xautoclaim else {
            panic!("XAUTOCLAIM should return array");
        };
        assert_eq!(xautoclaim_rows.len(), 3);
        let RespFrame::Array(xautoclaim_ids) = &xautoclaim_rows[1] else {
            panic!("XAUTOCLAIM ids should be array");
        };
        assert!(!xautoclaim_ids.is_empty());

        assert_eq!(
            run(
                &["XDEL", "mystream", delivered_id2_text.as_str()],
                &mut server,
                &mut client,
            ),
            RespFrame::Integer(1)
        );
        assert_eq!(
            run(
                &["XTRIM", "mystream", "MAXLEN", "0"],
                &mut server,
                &mut client
            ),
            RespFrame::Integer(1)
        );
        assert_eq!(
            run(&["XLEN", "mystream"], &mut server, &mut client),
            RespFrame::Integer(0)
        );
        assert_eq!(
            run(&["XSETID", "mystream", "0-0"], &mut server, &mut client),
            RespFrame::ok()
        );

        assert_eq!(
            run(
                &["XGROUP", "SETID", "mystream", "g1", "$"],
                &mut server,
                &mut client,
            ),
            RespFrame::ok()
        );
        assert_eq!(
            run(
                &["XGROUP", "DELCONSUMER", "mystream", "g1", "c2"],
                &mut server,
                &mut client,
            ),
            RespFrame::Integer(0)
        );
        assert_eq!(
            run(
                &["XGROUP", "DESTROY", "mystream", "g1"],
                &mut server,
                &mut client,
            ),
            RespFrame::Integer(1)
        );

        assert_eq!(
            run(
                &[
                    "XREADGROUP",
                    "GROUP",
                    "g1",
                    "c1",
                    "STREAMS",
                    "mystream",
                    ">",
                ],
                &mut server,
                &mut client,
            ),
            RespFrame::error_str(
                "NOGROUP No such key 'mystream' or consumer group 'g1' in XREADGROUP with GROUP option",
            )
        );
    }

    #[test]
    fn m3_stream_extended_delete_commands() {
        let mut server = ServerState::with_default_dbs();
        let mut client = ClientState::default();

        let id1 = run(&["XADD", "xs", "*", "f1", "v1"], &mut server, &mut client);
        let RespFrame::BulkString(Some(id1)) = id1 else {
            panic!("XADD should return generated id");
        };
        let id2 = run(&["XADD", "xs", "*", "f2", "v2"], &mut server, &mut client);
        let RespFrame::BulkString(Some(id2)) = id2 else {
            panic!("XADD should return generated id");
        };
        let id1_text = String::from_utf8_lossy(&id1).to_string();
        let id2_text = String::from_utf8_lossy(&id2).to_string();

        assert_eq!(
            run(
                &["XGROUP", "CREATE", "xs", "g1", "0-0"],
                &mut server,
                &mut client
            ),
            RespFrame::ok()
        );
        assert_eq!(
            run(
                &["XGROUP", "CREATE", "xs", "g2", "0-0"],
                &mut server,
                &mut client
            ),
            RespFrame::ok()
        );

        let g1_read = run(
            &[
                "XREADGROUP",
                "GROUP",
                "g1",
                "c1",
                "COUNT",
                "2",
                "STREAMS",
                "xs",
                ">",
            ],
            &mut server,
            &mut client,
        );
        let RespFrame::Array(g1_rows) = g1_read else {
            panic!("XREADGROUP should return array");
        };
        assert_eq!(g1_rows.len(), 1);

        let g2_read = run(
            &[
                "XREADGROUP",
                "GROUP",
                "g2",
                "c2",
                "COUNT",
                "1",
                "STREAMS",
                "xs",
                ">",
            ],
            &mut server,
            &mut client,
        );
        let RespFrame::Array(g2_rows) = g2_read else {
            panic!("XREADGROUP should return array");
        };
        assert_eq!(g2_rows.len(), 1);

        assert_eq!(
            run(
                &["XDELEX", "xs", "ACKED", "IDS", "1", id1_text.as_str()],
                &mut server,
                &mut client,
            ),
            RespFrame::Array(vec![RespFrame::Integer(2)])
        );

        assert_eq!(
            run(
                &[
                    "XACKDEL",
                    "xs",
                    "g1",
                    "ACKED",
                    "IDS",
                    "1",
                    id1_text.as_str()
                ],
                &mut server,
                &mut client,
            ),
            RespFrame::Array(vec![RespFrame::Integer(2)])
        );

        assert_eq!(
            run(
                &[
                    "XACKDEL",
                    "xs",
                    "g2",
                    "DELREF",
                    "IDS",
                    "1",
                    id1_text.as_str()
                ],
                &mut server,
                &mut client,
            ),
            RespFrame::Array(vec![RespFrame::Integer(1)])
        );

        assert_eq!(
            run(
                &["XDELEX", "xs", "DELREF", "IDS", "1", id2_text.as_str()],
                &mut server,
                &mut client,
            ),
            RespFrame::Array(vec![RespFrame::Integer(1)])
        );

        assert_eq!(
            run(&["XLEN", "xs"], &mut server, &mut client),
            RespFrame::Integer(0)
        );

        assert_eq!(
            run(
                &[
                    "XCFGSET",
                    "xs",
                    "IDMP-DURATION",
                    "10",
                    "IDMP-MAXSIZE",
                    "100"
                ],
                &mut server,
                &mut client,
            ),
            RespFrame::ok()
        );

        assert_eq!(
            run(
                &["XDELEX", "missing", "IDS", "2", "1-0", "2-0"],
                &mut server,
                &mut client,
            ),
            RespFrame::Array(vec![RespFrame::Integer(-1), RespFrame::Integer(-1)])
        );
        assert_eq!(
            run(
                &["XACKDEL", "missing", "g1", "IDS", "1", "1-0"],
                &mut server,
                &mut client,
            ),
            RespFrame::Array(vec![RespFrame::Integer(-1)])
        );

        assert_eq!(
            run(&["SET", "notstream", "v"], &mut server, &mut client),
            RespFrame::ok()
        );
        assert_eq!(
            run(
                &["XCFGSET", "notstream", "IDMP-DURATION", "10"],
                &mut server,
                &mut client,
            ),
            RespFrame::error_str(
                "WRONGTYPE Operation against a key holding the wrong kind of value"
            )
        );
        assert_eq!(
            run(
                &["XCFGSET", "missing", "IDMP-DURATION", "10"],
                &mut server,
                &mut client,
            ),
            RespFrame::error_str("ERR no such key")
        );
    }

    #[test]
    fn transaction_multi_exec_discard_watch_flow() {
        let mut server = ServerState::with_default_dbs();
        let mut client = ClientState::default();

        assert_eq!(
            run(&["EXEC"], &mut server, &mut client),
            RespFrame::error_str("ERR EXEC without MULTI")
        );
        assert_eq!(
            run(&["DISCARD"], &mut server, &mut client),
            RespFrame::error_str("ERR DISCARD without MULTI")
        );

        assert_eq!(run(&["MULTI"], &mut server, &mut client), RespFrame::ok());
        assert_eq!(
            run(&["NO_SUCH_CMD"], &mut server, &mut client),
            RespFrame::error_str("ERR unknown command 'NO_SUCH_CMD'")
        );
        assert_eq!(
            run(&["SET", "k"], &mut server, &mut client),
            RespFrame::error_str("ERR wrong number of arguments for 'set' command")
        );
        assert_eq!(
            run(&["EXEC"], &mut server, &mut client),
            RespFrame::error_str("EXECABORT Transaction discarded because of previous errors.")
        );

        assert_eq!(
            run(&["SET", "k", "1"], &mut server, &mut client),
            RespFrame::ok()
        );
        assert_eq!(
            run(&["WATCH", "k"], &mut server, &mut client),
            RespFrame::ok()
        );

        assert_eq!(run(&["MULTI"], &mut server, &mut client), RespFrame::ok());
        assert_eq!(
            run(&["SET", "k", "2"], &mut server, &mut client),
            RespFrame::queued()
        );
        assert_eq!(
            run(&["GET", "k"], &mut server, &mut client),
            RespFrame::queued()
        );

        assert_eq!(
            run(&["WATCH", "k"], &mut server, &mut client),
            RespFrame::error_str("ERR WATCH inside MULTI is not allowed")
        );

        let exec = run(&["EXEC"], &mut server, &mut client);
        let RespFrame::Array(exec_entries) = exec else {
            panic!("EXEC should return array");
        };
        assert_eq!(exec_entries.len(), 2);
        assert_eq!(exec_entries[0], RespFrame::ok());
        assert_eq!(exec_entries[1], RespFrame::bulk_str("2"));

        assert_eq!(
            run(&["WATCH", "k"], &mut server, &mut client),
            RespFrame::ok()
        );
        assert_eq!(
            run(&["SET", "k", "3"], &mut server, &mut client),
            RespFrame::ok()
        );

        assert_eq!(run(&["MULTI"], &mut server, &mut client), RespFrame::ok());
        assert_eq!(
            run(&["GET", "k"], &mut server, &mut client),
            RespFrame::queued()
        );
        assert_eq!(
            run(&["EXEC"], &mut server, &mut client),
            RespFrame::NullArray
        );

        assert_eq!(run(&["MULTI"], &mut server, &mut client), RespFrame::ok());
        assert_eq!(
            run(&["MULTI"], &mut server, &mut client),
            RespFrame::error_str("ERR MULTI calls can not be nested")
        );
        assert_eq!(
            run(&["SET", "k", "4"], &mut server, &mut client),
            RespFrame::queued()
        );
        assert_eq!(run(&["DISCARD"], &mut server, &mut client), RespFrame::ok());
        assert_eq!(
            run(&["GET", "k"], &mut server, &mut client),
            RespFrame::bulk_str("3")
        );

        assert_eq!(
            run(&["WATCH", "k", "x"], &mut server, &mut client),
            RespFrame::ok()
        );
        assert_eq!(run(&["UNWATCH"], &mut server, &mut client), RespFrame::ok());
    }

    #[test]
    fn durability_capture_groups_only_successful_exec_effects() {
        let mut server = ServerState::with_default_dbs();
        let mut client = ClientState::default();
        client.set_durability_capture_enabled(true);

        assert_eq!(
            run(&["RPUSH", "not-a-number", "x"], &mut server, &mut client),
            RespFrame::Integer(1)
        );
        let _ = client.take_durability_effects();
        assert_eq!(run(&["MULTI"], &mut server, &mut client), RespFrame::ok());
        assert_eq!(
            run(&["SET", "committed", "yes"], &mut server, &mut client),
            RespFrame::queued()
        );
        assert_eq!(
            run(&["INCR", "not-a-number"], &mut server, &mut client),
            RespFrame::queued()
        );

        let outcome = run_outcome(&["EXEC"], &mut server, &mut client);
        let RespFrame::Array(replies) = outcome.response else {
            panic!("EXEC should return replies");
        };
        assert_eq!(replies.len(), 2);
        assert_eq!(replies[0], RespFrame::ok());
        assert!(matches!(replies[1], RespFrame::Error(_)));

        let effects = client.take_durability_effects().expect("committed effects");
        assert!(effects.transaction);
        assert_eq!(effects.commands.len(), 1);
        assert_eq!(effects.commands[0].db_index, 0);
        assert_eq!(
            effects.commands[0].argv,
            vec![
                Bytes::from("SET"),
                Bytes::from("committed"),
                Bytes::from("yes")
            ]
        );

        assert_eq!(run(&["MULTI"], &mut server, &mut client), RespFrame::ok());
        assert_eq!(
            run(&["SET", "discarded", "no"], &mut server, &mut client),
            RespFrame::queued()
        );
        assert_eq!(run(&["DISCARD"], &mut server, &mut client), RespFrame::ok());
        assert!(client.take_durability_effects().is_none());
    }

    #[test]
    fn durability_capture_resolves_expiry_and_stream_auto_ids() {
        let mut server = ServerState::with_default_dbs();
        let mut client = ClientState::default();
        client.set_durability_capture_enabled(true);

        assert_eq!(
            run_outcome(&["SET", "ttl", "v", "PX", "5000"], &mut server, &mut client).response,
            RespFrame::ok()
        );
        let ttl_effect = client.take_durability_effects().expect("ttl effect");
        assert_eq!(ttl_effect.commands[0].argv[0], Bytes::from("SET"));
        assert_eq!(ttl_effect.commands[0].argv[3], Bytes::from("PXAT"));
        let deadline = std::str::from_utf8(&ttl_effect.commands[0].argv[4])
            .expect("deadline text")
            .parse::<i64>()
            .expect("deadline integer");
        assert!(deadline > now_ms());

        let xadd = run_outcome(
            &["XADD", "events", "1-*", "f", "v"],
            &mut server,
            &mut client,
        );
        assert_eq!(xadd.response, RespFrame::bulk_str("1-0"));
        let stream_effect = client.take_durability_effects().expect("stream effect");
        assert_eq!(stream_effect.commands[0].argv[2], Bytes::from("1-0"));
    }

    #[test]
    fn object_and_sort_baseline() {
        let mut server = ServerState::with_default_dbs();
        let mut client = ClientState::default();

        assert_eq!(
            run(&["OBJECT", "HELP"], &mut server, &mut client),
            RespFrame::Array(vec![
                RespFrame::bulk_str("ENCODING <key>"),
                RespFrame::bulk_str("FREQ <key>"),
                RespFrame::bulk_str("IDLETIME <key>"),
                RespFrame::bulk_str("REFCOUNT <key>"),
            ])
        );

        assert_eq!(
            run(&["OBJECT", "ENCODING", "missing"], &mut server, &mut client),
            RespFrame::Null
        );

        assert_eq!(
            run(&["SET", "obj", "v"], &mut server, &mut client),
            RespFrame::ok()
        );
        assert_eq!(
            run(&["OBJECT", "ENCODING", "obj"], &mut server, &mut client),
            RespFrame::bulk_str("raw")
        );
        assert_eq!(
            run(&["OBJECT", "REFCOUNT", "obj"], &mut server, &mut client),
            RespFrame::Integer(1)
        );
        assert_eq!(
            run(&["OBJECT", "IDLETIME", "obj"], &mut server, &mut client),
            RespFrame::Integer(0)
        );
        assert_eq!(
            run(&["OBJECT", "FREQ", "obj"], &mut server, &mut client),
            RespFrame::error_str(
                "ERR An LFU maxmemory policy is not selected, access frequency not tracked. Please note that when switching between policies at runtime LRU and LFU data will take some time to adjust."
            )
        );

        assert_eq!(
            run(&["OBJECT", "NOPE", "obj"], &mut server, &mut client),
            RespFrame::error_str(
                "ERR Unknown subcommand or wrong number of arguments for 'OBJECT'. Try OBJECT HELP."
            )
        );

        assert_eq!(
            run(&["SORT", "missing"], &mut server, &mut client),
            RespFrame::Array(vec![])
        );
        assert_eq!(
            run(&["SORT_RO", "missing"], &mut server, &mut client),
            RespFrame::Array(vec![])
        );

        assert_eq!(
            run(&["SET", "dst", "old"], &mut server, &mut client),
            RespFrame::ok()
        );
        assert_eq!(
            run(
                &["SORT", "missing", "STORE", "dst"],
                &mut server,
                &mut client
            ),
            RespFrame::Integer(0)
        );
        assert_eq!(
            run(&["GET", "dst"], &mut server, &mut client),
            RespFrame::BulkString(None)
        );

        assert_eq!(
            run(&["SORT", "obj"], &mut server, &mut client),
            RespFrame::error_str(
                "WRONGTYPE Operation against a key holding the wrong kind of value"
            )
        );
        assert_eq!(
            run(&["SORT_RO", "obj"], &mut server, &mut client),
            RespFrame::error_str(
                "WRONGTYPE Operation against a key holding the wrong kind of value"
            )
        );
        assert_eq!(
            run(
                &["SORT_RO", "missing", "STORE", "x"],
                &mut server,
                &mut client
            ),
            RespFrame::error_str("ERR syntax error")
        );

        assert_eq!(
            run(
                &["RPUSH", "list:sort", "3", "1", "2"],
                &mut server,
                &mut client
            ),
            RespFrame::Integer(3)
        );
        assert_eq!(
            run(&["SORT", "list:sort"], &mut server, &mut client),
            RespFrame::Array(vec![
                RespFrame::bulk_str("1"),
                RespFrame::bulk_str("2"),
                RespFrame::bulk_str("3"),
            ])
        );
        assert_eq!(
            run(&["SORT", "list:sort", "DESC"], &mut server, &mut client),
            RespFrame::Array(vec![
                RespFrame::bulk_str("3"),
                RespFrame::bulk_str("2"),
                RespFrame::bulk_str("1"),
            ])
        );
        assert_eq!(
            run(
                &["SORT", "list:sort", "LIMIT", "1", "1"],
                &mut server,
                &mut client
            ),
            RespFrame::Array(vec![RespFrame::bulk_str("2")])
        );
        assert_eq!(
            run(
                &["SORT", "list:sort", "STORE", "list:sorted"],
                &mut server,
                &mut client,
            ),
            RespFrame::Integer(3)
        );
        assert_eq!(
            run(
                &["LRANGE", "list:sorted", "0", "-1"],
                &mut server,
                &mut client,
            ),
            RespFrame::Array(vec![
                RespFrame::bulk_str("1"),
                RespFrame::bulk_str("2"),
                RespFrame::bulk_str("3"),
            ])
        );

        assert_eq!(
            run(
                &["SADD", "set:sort", "b", "a", "c"],
                &mut server,
                &mut client
            ),
            RespFrame::Integer(3)
        );
        assert_eq!(
            run(&["SORT_RO", "set:sort", "ALPHA"], &mut server, &mut client),
            RespFrame::Array(vec![
                RespFrame::bulk_str("a"),
                RespFrame::bulk_str("b"),
                RespFrame::bulk_str("c"),
            ])
        );

        assert_eq!(
            run(&["RPUSH", "list:bad", "x", "1"], &mut server, &mut client),
            RespFrame::Integer(2)
        );
        assert_eq!(
            run(&["SORT", "list:bad"], &mut server, &mut client),
            RespFrame::error_str("ERR One or more scores can't be converted into double")
        );
    }

    #[test]
    fn m1_string_and_generic_new_commands() {
        let mut server = ServerState::with_default_dbs();
        let mut client = ClientState::default();

        assert_eq!(
            run(&["APPEND", "a", "he"], &mut server, &mut client),
            RespFrame::Integer(2)
        );
        assert_eq!(
            run(&["APPEND", "a", "llo"], &mut server, &mut client),
            RespFrame::Integer(5)
        );
        assert_eq!(
            run(&["STRLEN", "a"], &mut server, &mut client),
            RespFrame::Integer(5)
        );
        assert_eq!(
            run(&["GETRANGE", "a", "1", "3"], &mut server, &mut client),
            RespFrame::bulk_str("ell")
        );

        assert_eq!(
            run(&["SETRANGE", "a", "1", "a"], &mut server, &mut client),
            RespFrame::Integer(5)
        );
        assert_eq!(
            run(&["GET", "a"], &mut server, &mut client),
            RespFrame::bulk_str("hallo")
        );

        assert_eq!(
            run(&["GETSET", "a", "next"], &mut server, &mut client),
            RespFrame::bulk_str("hallo")
        );
        assert_eq!(
            run(&["GET", "a"], &mut server, &mut client),
            RespFrame::bulk_str("next")
        );

        assert_eq!(
            run(&["SETNX", "a", "x"], &mut server, &mut client),
            RespFrame::Integer(0)
        );
        assert_eq!(
            run(&["SETNX", "b", "v"], &mut server, &mut client),
            RespFrame::Integer(1)
        );

        assert_eq!(
            run(&["MSET", "k1", "v1", "k2", "v2"], &mut server, &mut client),
            RespFrame::ok()
        );
        assert_eq!(
            run(&["MGET", "k1", "k2", "k3"], &mut server, &mut client),
            RespFrame::Array(vec![
                RespFrame::bulk_str("v1"),
                RespFrame::bulk_str("v2"),
                RespFrame::BulkString(None),
            ])
        );

        assert_eq!(
            run(&["MSETNX", "k1", "x", "z", "9"], &mut server, &mut client),
            RespFrame::Integer(0)
        );
        assert_eq!(
            run(&["GET", "z"], &mut server, &mut client),
            RespFrame::BulkString(None)
        );

        assert_eq!(
            run(&["INCR", "count"], &mut server, &mut client),
            RespFrame::Integer(1)
        );
        assert_eq!(
            run(&["INCRBY", "count", "9"], &mut server, &mut client),
            RespFrame::Integer(10)
        );
        assert_eq!(
            run(&["DECR", "count"], &mut server, &mut client),
            RespFrame::Integer(9)
        );
        assert_eq!(
            run(&["DECRBY", "count", "4"], &mut server, &mut client),
            RespFrame::Integer(5)
        );
        assert_eq!(
            run(&["INCRBYFLOAT", "float", "1.5"], &mut server, &mut client),
            RespFrame::bulk_str("1.5")
        );

        assert_eq!(
            run(&["TYPE", "a"], &mut server, &mut client),
            RespFrame::simple_str("string")
        );
        assert_eq!(
            run(&["KEYS", "k*"], &mut server, &mut client),
            RespFrame::Array(vec![RespFrame::bulk_str("k1"), RespFrame::bulk_str("k2")])
        );
        assert_eq!(
            run(&["SUBSTR", "a", "1", "2"], &mut server, &mut client),
            RespFrame::bulk_str("ex")
        );

        assert_eq!(
            run(&["COPY", "a", "a2"], &mut server, &mut client),
            RespFrame::Integer(1)
        );
        assert_eq!(
            run(&["GET", "a2"], &mut server, &mut client),
            RespFrame::bulk_str("next")
        );
        assert_eq!(
            run(&["COPY", "a", "a2"], &mut server, &mut client),
            RespFrame::Integer(0)
        );
        assert_eq!(
            run(&["COPY", "a", "a", "DB", "1"], &mut server, &mut client),
            RespFrame::Integer(1)
        );
        assert_eq!(
            run(&["COPY", "a", "a", "DB", "0"], &mut server, &mut client),
            RespFrame::error_str("ERR source and destination objects are the same")
        );

        assert_eq!(
            run(&["RENAME", "a", "a_renamed"], &mut server, &mut client),
            RespFrame::ok()
        );
        assert_eq!(
            run(&["RENAMENX", "a_renamed", "a2"], &mut server, &mut client),
            RespFrame::Integer(0)
        );
        assert_eq!(
            run(
                &["RENAMENX", "a_renamed", "a_renamed"],
                &mut server,
                &mut client
            ),
            RespFrame::Integer(0)
        );
        assert_eq!(
            run(&["RENAME", "missing", "x"], &mut server, &mut client),
            RespFrame::error_str("ERR no such key")
        );

        assert_eq!(
            run(&["MOVE", "a_renamed", "1"], &mut server, &mut client),
            RespFrame::Integer(1)
        );
        assert_eq!(
            run(&["GET", "a_renamed"], &mut server, &mut client),
            RespFrame::BulkString(None)
        );
        assert_eq!(
            run(&["SELECT", "1"], &mut server, &mut client),
            RespFrame::ok()
        );
        assert_eq!(
            run(&["GET", "a_renamed"], &mut server, &mut client),
            RespFrame::bulk_str("next")
        );
        assert_eq!(
            run(&["MOVE", "a_renamed", "1"], &mut server, &mut client),
            RespFrame::error_str("ERR source and destination objects are the same")
        );
        assert_eq!(
            run(&["TOUCH", "a_renamed", "missing"], &mut server, &mut client),
            RespFrame::Integer(1)
        );
        assert_eq!(
            run(
                &["UNLINK", "a_renamed", "missing"],
                &mut server,
                &mut client
            ),
            RespFrame::Integer(1)
        );
        assert_eq!(
            run(&["GET", "a_renamed"], &mut server, &mut client),
            RespFrame::BulkString(None)
        );

        assert_eq!(
            run(&["SET", "scan:s1", "1"], &mut server, &mut client),
            RespFrame::ok()
        );
        assert_eq!(
            run(&["SET", "scan:s2", "2"], &mut server, &mut client),
            RespFrame::ok()
        );
        assert_eq!(
            run(&["SET", "scan:x", "3"], &mut server, &mut client),
            RespFrame::ok()
        );

        assert_eq!(
            run(
                &["SCAN", "0", "MATCH", "scan:s*", "COUNT", "100"],
                &mut server,
                &mut client
            ),
            RespFrame::Array(vec![
                RespFrame::bulk_str("0"),
                RespFrame::Array(vec![
                    RespFrame::bulk_str("scan:s1"),
                    RespFrame::bulk_str("scan:s2")
                ]),
            ])
        );
        assert_eq!(
            run(&["SCAN", "0", "TYPE", "nosuch"], &mut server, &mut client),
            RespFrame::Array(vec![RespFrame::bulk_str("0"), RespFrame::Array(vec![])])
        );
        assert_eq!(
            run(&["SCAN", "x"], &mut server, &mut client),
            RespFrame::error_str("ERR invalid cursor")
        );

        assert_eq!(
            run(&["SELECT", "0"], &mut server, &mut client),
            RespFrame::ok()
        );

        let random = run(&["RANDOMKEY"], &mut server, &mut client);
        let RespFrame::BulkString(random_key) = random else {
            panic!("RANDOMKEY should return bulk string");
        };
        assert!(random_key.is_some());
    }

    #[test]
    fn rename_keeps_ttl_and_replaces_existing_target() {
        let mut server = ServerState::with_default_dbs();
        let mut client = ClientState::default();

        assert_eq!(
            run(
                &["SET", "src", "fresh", "PX", "100000"],
                &mut server,
                &mut client
            ),
            RespFrame::ok()
        );
        assert_eq!(
            run(&["SET", "dst", "stale"], &mut server, &mut client),
            RespFrame::ok()
        );
        assert_eq!(
            run(&["RENAME", "src", "dst"], &mut server, &mut client),
            RespFrame::ok()
        );

        assert_eq!(
            run(&["GET", "dst"], &mut server, &mut client),
            RespFrame::bulk_str("fresh")
        );
        assert_eq!(
            run(&["EXISTS", "src"], &mut server, &mut client),
            RespFrame::Integer(0)
        );
        assert_eq!(
            run(&["DBSIZE"], &mut server, &mut client),
            RespFrame::Integer(1)
        );
        let RespFrame::Integer(pttl) = run(&["PTTL", "dst"], &mut server, &mut client) else {
            panic!("PTTL must return an integer");
        };
        assert!(
            (1..=100_000).contains(&pttl),
            "TTL must survive RENAME, got {pttl}"
        );
    }
    #[test]
    fn m1_additional_kv_core_commands() {
        let mut server = ServerState::with_default_dbs();
        let mut client = ClientState::default();

        assert_eq!(
            run(
                &["MSETEX", "2", "a", "1", "b", "2", "EX", "1"],
                &mut server,
                &mut client
            ),
            RespFrame::Integer(1)
        );
        assert_eq!(
            run(&["MGET", "a", "b"], &mut server, &mut client),
            RespFrame::Array(vec![RespFrame::bulk_str("1"), RespFrame::bulk_str("2")])
        );
        assert_eq!(
            run(
                &["MSETEX", "2", "a", "x", "c", "3", "NX"],
                &mut server,
                &mut client
            ),
            RespFrame::Integer(0)
        );

        let digest = run(&["DIGEST", "a"], &mut server, &mut client);
        let RespFrame::BulkString(Some(digest_raw)) = digest else {
            panic!("DIGEST should return bulk string");
        };
        assert!(String::from_utf8_lossy(&digest_raw).parse::<i64>().is_ok());
        assert_eq!(
            run(&["DELEX", "a", "IFEQ", "1"], &mut server, &mut client),
            RespFrame::Integer(1)
        );
        assert_eq!(
            run(&["DELEX", "a"], &mut server, &mut client),
            RespFrame::Integer(0)
        );

        assert_eq!(
            run(&["SET", "obj", "payload"], &mut server, &mut client),
            RespFrame::ok()
        );
        let dumped = run(&["DUMP", "obj"], &mut server, &mut client);
        let RespFrame::BulkString(Some(payload)) = dumped else {
            panic!("DUMP should return payload");
        };
        let restore = {
            let mut access = ServerAccess::new_inline(&mut server);
            execute(
                RespFrame::Array(vec![
                    RespFrame::bulk_str("RESTORE"),
                    RespFrame::bulk_str("obj2"),
                    RespFrame::bulk_str("0"),
                    RespFrame::BulkString(Some(payload.clone())),
                ]),
                &mut access,
                &mut client,
            )
            .response
        };
        assert_eq!(restore, RespFrame::ok());
        assert_eq!(
            run(&["GET", "obj2"], &mut server, &mut client),
            RespFrame::bulk_str("payload")
        );

        assert_eq!(
            run(
                &["RESTORE", "obj2", "0", "bad-payload"],
                &mut server,
                &mut client
            ),
            RespFrame::error_str("ERR DUMP payload version or checksum are wrong")
        );

        assert_eq!(
            run(&["SET", "s1", "ohmytext"], &mut server, &mut client),
            RespFrame::ok()
        );
        assert_eq!(
            run(&["SET", "s2", "mynewtext"], &mut server, &mut client),
            RespFrame::ok()
        );
        assert_eq!(
            run(&["LCS", "s1", "s2"], &mut server, &mut client),
            RespFrame::bulk_str("mytext")
        );
        assert_eq!(
            run(&["LCS", "s1", "s2", "LEN"], &mut server, &mut client),
            RespFrame::Integer(6)
        );

        let large_left = "a".repeat(5000);
        let large_right = "b".repeat(5000);
        assert_eq!(
            run(
                &["SET", "lcs-left", large_left.as_str()],
                &mut server,
                &mut client
            ),
            RespFrame::ok()
        );
        assert_eq!(
            run(
                &["SET", "lcs-right", large_right.as_str()],
                &mut server,
                &mut client
            ),
            RespFrame::ok()
        );
        assert_eq!(
            run(&["LCS", "lcs-left", "lcs-right"], &mut server, &mut client),
            RespFrame::error_str("ERR LCS input is too large")
        );

        assert_eq!(
            run(&["WAIT", "1", "100"], &mut server, &mut client),
            RespFrame::Integer(0)
        );
        assert_eq!(
            run(&["WAITAOF", "1", "1", "100"], &mut server, &mut client),
            RespFrame::Array(vec![RespFrame::Integer(0), RespFrame::Integer(0)])
        );

        assert_eq!(
            run(
                &["MIGRATE", "127.0.0.1", "6379", "k", "0", "1000"],
                &mut server,
                &mut client
            ),
            RespFrame::bulk_str("NOKEY")
        );
    }

    #[test]
    fn m2_hash_core_commands() {
        let mut server = ServerState::with_default_dbs();
        let mut client = ClientState::default();

        assert_eq!(
            run(
                &["HSET", "h", "f1", "v1", "f2", "v2"],
                &mut server,
                &mut client
            ),
            RespFrame::Integer(2)
        );
        assert_eq!(
            run(&["HSET", "h", "f2", "v2b"], &mut server, &mut client),
            RespFrame::Integer(0)
        );

        assert_eq!(
            run(&["HGET", "h", "f1"], &mut server, &mut client),
            RespFrame::bulk_str("v1")
        );
        assert_eq!(
            run(
                &["HMGET", "h", "f1", "f2", "missing"],
                &mut server,
                &mut client
            ),
            RespFrame::Array(vec![
                RespFrame::bulk_str("v1"),
                RespFrame::bulk_str("v2b"),
                RespFrame::BulkString(None),
            ])
        );
        assert_map_pairs_eq(
            &run(&["HGETALL", "h"], &mut server, &mut client),
            &[("f1", "v1"), ("f2", "v2b")],
        );
        assert_array_set_eq(
            &run(&["HKEYS", "h"], &mut server, &mut client),
            &["f1", "f2"],
        );
        assert_array_set_eq(
            &run(&["HVALS", "h"], &mut server, &mut client),
            &["v1", "v2b"],
        );

        assert_eq!(
            run(&["HEXISTS", "h", "f2"], &mut server, &mut client),
            RespFrame::Integer(1)
        );
        assert_eq!(
            run(&["HLEN", "h"], &mut server, &mut client),
            RespFrame::Integer(2)
        );

        assert_eq!(
            run(&["HDEL", "h", "f1", "missing"], &mut server, &mut client),
            RespFrame::Integer(1)
        );
        assert_eq!(
            run(&["HLEN", "h"], &mut server, &mut client),
            RespFrame::Integer(1)
        );
        assert_eq!(
            run(&["HDEL", "h", "f2"], &mut server, &mut client),
            RespFrame::Integer(1)
        );
        assert_eq!(
            run(&["EXISTS", "h"], &mut server, &mut client),
            RespFrame::Integer(0)
        );
    }

    #[test]
    fn m2_hash_extended_commands() {
        let mut server = ServerState::with_default_dbs();
        let mut client = ClientState::default();

        assert_eq!(
            run(
                &["HMSET", "h2", "f1", "v1", "f2", "v2"],
                &mut server,
                &mut client
            ),
            RespFrame::ok()
        );
        assert_map_pairs_eq(
            &run(&["HGETALL", "h2"], &mut server, &mut client),
            &[("f1", "v1"), ("f2", "v2")],
        );

        assert_eq!(
            run(
                &["HSETNX", "h2", "f1", "override"],
                &mut server,
                &mut client
            ),
            RespFrame::Integer(0)
        );
        assert_eq!(
            run(&["HSETNX", "h2", "f3", "v3"], &mut server, &mut client),
            RespFrame::Integer(1)
        );

        assert_eq!(
            run(&["HINCRBY", "h2", "n", "5"], &mut server, &mut client),
            RespFrame::Integer(5)
        );
        assert_eq!(
            run(&["HINCRBY", "h2", "n", "-2"], &mut server, &mut client),
            RespFrame::Integer(3)
        );

        assert_eq!(
            run(
                &["HINCRBYFLOAT", "h2", "fnum", "1.5"],
                &mut server,
                &mut client
            ),
            RespFrame::bulk_str("1.5")
        );
        assert_eq!(
            run(
                &["HINCRBYFLOAT", "h2", "fnum", "0.5"],
                &mut server,
                &mut client
            ),
            RespFrame::bulk_str("2")
        );

        assert_eq!(
            run(&["HSTRLEN", "h2", "f1"], &mut server, &mut client),
            RespFrame::Integer(2)
        );
        assert_eq!(
            run(&["HSTRLEN", "h2", "missing"], &mut server, &mut client),
            RespFrame::Integer(0)
        );

        let one = run(&["HRANDFIELD", "h2"], &mut server, &mut client);
        let RespFrame::BulkString(Some(_)) = one else {
            panic!("HRANDFIELD without COUNT should return bulk string");
        };

        let two = run(&["HRANDFIELD", "h2", "2"], &mut server, &mut client);
        let RespFrame::Array(two_fields) = two else {
            panic!("HRANDFIELD with positive count should return array");
        };
        assert_eq!(two_fields.len(), 2);

        let dup = run(&["HRANDFIELD", "h2", "-3"], &mut server, &mut client);
        let RespFrame::Array(dup_fields) = dup else {
            panic!("HRANDFIELD with negative count should return array");
        };
        assert_eq!(dup_fields.len(), 3);

        let withvalues = run(
            &["HRANDFIELD", "h2", "2", "WITHVALUES"],
            &mut server,
            &mut client,
        );
        let RespFrame::Array(withvalues_items) = withvalues else {
            panic!("HRANDFIELD WITHVALUES should return array");
        };
        assert_eq!(withvalues_items.len(), 4);

        assert_eq!(
            run(&["HRANDFIELD", "missing"], &mut server, &mut client),
            RespFrame::BulkString(None)
        );
        assert_eq!(
            run(&["HRANDFIELD", "missing", "2"], &mut server, &mut client),
            RespFrame::Array(vec![])
        );

        assert_eq!(
            run(&["HSET", "h2", "notint", "v"], &mut server, &mut client),
            RespFrame::Integer(1)
        );
        assert_eq!(
            run(&["HINCRBY", "h2", "notint", "1"], &mut server, &mut client),
            RespFrame::error_str("ERR hash value is not an integer")
        );
        assert_eq!(
            run(
                &["HINCRBYFLOAT", "h2", "notint", "1"],
                &mut server,
                &mut client
            ),
            RespFrame::error_str("ERR hash value is not a float")
        );

        assert_eq!(
            run(&["HRANDFIELD", "h2", "1", "BAD"], &mut server, &mut client),
            RespFrame::error_str("ERR syntax error")
        );
    }

    #[test]
    fn m2_hash_wrongtype_and_set_override() {
        let mut server = ServerState::with_default_dbs();
        let mut client = ClientState::default();

        assert_eq!(
            run(&["SET", "s", "v"], &mut server, &mut client),
            RespFrame::ok()
        );
        assert_eq!(
            run(&["HSET", "s", "f", "x"], &mut server, &mut client),
            RespFrame::error_str(
                "WRONGTYPE Operation against a key holding the wrong kind of value"
            )
        );
        assert_eq!(
            run(&["HKEYS", "s"], &mut server, &mut client),
            RespFrame::error_str(
                "WRONGTYPE Operation against a key holding the wrong kind of value"
            )
        );
        assert_eq!(
            run(&["HVALS", "s"], &mut server, &mut client),
            RespFrame::error_str(
                "WRONGTYPE Operation against a key holding the wrong kind of value"
            )
        );
        assert_eq!(
            run(&["HMSET", "s", "f", "x"], &mut server, &mut client),
            RespFrame::error_str(
                "WRONGTYPE Operation against a key holding the wrong kind of value"
            )
        );
        assert_eq!(
            run(&["HSETNX", "s", "f", "x"], &mut server, &mut client),
            RespFrame::error_str(
                "WRONGTYPE Operation against a key holding the wrong kind of value"
            )
        );
        assert_eq!(
            run(&["HINCRBY", "s", "f", "1"], &mut server, &mut client),
            RespFrame::error_str(
                "WRONGTYPE Operation against a key holding the wrong kind of value"
            )
        );
        assert_eq!(
            run(&["HINCRBYFLOAT", "s", "f", "1.0"], &mut server, &mut client),
            RespFrame::error_str(
                "WRONGTYPE Operation against a key holding the wrong kind of value"
            )
        );
        assert_eq!(
            run(&["HSTRLEN", "s", "f"], &mut server, &mut client),
            RespFrame::error_str(
                "WRONGTYPE Operation against a key holding the wrong kind of value"
            )
        );
        assert_eq!(
            run(&["HRANDFIELD", "s"], &mut server, &mut client),
            RespFrame::error_str(
                "WRONGTYPE Operation against a key holding the wrong kind of value"
            )
        );

        assert_eq!(
            run(&["HSET", "h", "f", "v"], &mut server, &mut client),
            RespFrame::Integer(1)
        );
        assert_eq!(
            run(&["GET", "h"], &mut server, &mut client),
            RespFrame::error_str(
                "WRONGTYPE Operation against a key holding the wrong kind of value"
            )
        );
        assert_eq!(
            run(&["GETDEL", "h"], &mut server, &mut client),
            RespFrame::error_str(
                "WRONGTYPE Operation against a key holding the wrong kind of value"
            )
        );

        assert_eq!(
            run(&["SET", "h", "now-string"], &mut server, &mut client),
            RespFrame::ok()
        );
        assert_eq!(
            run(&["GET", "h"], &mut server, &mut client),
            RespFrame::bulk_str("now-string")
        );
        assert_eq!(
            run(&["TYPE", "h"], &mut server, &mut client),
            RespFrame::simple_str("string")
        );
    }

    #[test]
    fn m2_list_core_commands() {
        let mut server = ServerState::with_default_dbs();
        let mut client = ClientState::default();

        assert_eq!(
            run(&["LPUSH", "l", "a", "b", "c"], &mut server, &mut client),
            RespFrame::Integer(3)
        );
        assert_eq!(
            run(&["RPUSH", "l", "d", "e"], &mut server, &mut client),
            RespFrame::Integer(5)
        );
        assert_eq!(
            run(&["LRANGE", "l", "0", "-1"], &mut server, &mut client),
            RespFrame::Array(vec![
                RespFrame::bulk_str("c"),
                RespFrame::bulk_str("b"),
                RespFrame::bulk_str("a"),
                RespFrame::bulk_str("d"),
                RespFrame::bulk_str("e"),
            ])
        );
        assert_eq!(
            run(&["LLEN", "l"], &mut server, &mut client),
            RespFrame::Integer(5)
        );
        assert_eq!(
            run(
                &["RPUSH", "lt", "a", "b", "c", "d", "e"],
                &mut server,
                &mut client
            ),
            RespFrame::Integer(5)
        );
        assert_eq!(
            run(&["LSET", "lt", "1", "bb"], &mut server, &mut client),
            RespFrame::ok()
        );
        assert_eq!(
            run(&["LTRIM", "lt", "1", "3"], &mut server, &mut client),
            RespFrame::ok()
        );
        assert_eq!(
            run(&["LRANGE", "lt", "0", "-1"], &mut server, &mut client),
            RespFrame::Array(vec![
                RespFrame::bulk_str("bb"),
                RespFrame::bulk_str("c"),
                RespFrame::bulk_str("d"),
            ])
        );

        assert_eq!(
            run(&["LPOP", "l"], &mut server, &mut client),
            RespFrame::bulk_str("c")
        );
        assert_eq!(
            run(&["RPOP", "l"], &mut server, &mut client),
            RespFrame::bulk_str("e")
        );
        assert_eq!(
            run(&["LPOP", "l", "2"], &mut server, &mut client),
            RespFrame::Array(vec![RespFrame::bulk_str("b"), RespFrame::bulk_str("a")])
        );

        assert_eq!(
            run(&["LLEN", "l"], &mut server, &mut client),
            RespFrame::Integer(1)
        );
        assert_eq!(
            run(&["RPOP", "l", "5"], &mut server, &mut client),
            RespFrame::Array(vec![RespFrame::bulk_str("d")])
        );
        assert_eq!(
            run(&["EXISTS", "l"], &mut server, &mut client),
            RespFrame::Integer(0)
        );

        assert_eq!(
            run(&["LPOP", "missing"], &mut server, &mut client),
            RespFrame::BulkString(None)
        );
        assert_eq!(
            run(&["LPOP", "missing", "0"], &mut server, &mut client),
            RespFrame::Array(vec![])
        );
        assert_eq!(
            run(&["LRANGE", "missing", "0", "-1"], &mut server, &mut client),
            RespFrame::Array(vec![])
        );
    }

    #[test]
    fn m2_list_blpop_brpop_baseline_commands() {
        let mut server = ServerState::with_default_dbs();
        let mut client = ClientState::default();

        assert_eq!(
            run(&["RPUSH", "l1", "a", "b"], &mut server, &mut client),
            RespFrame::Integer(2)
        );
        assert_eq!(
            run(&["BLPOP", "l1", "0"], &mut server, &mut client),
            RespFrame::Array(vec![RespFrame::bulk_str("l1"), RespFrame::bulk_str("a")])
        );
        assert_eq!(
            run(&["BRPOP", "l1", "0"], &mut server, &mut client),
            RespFrame::Array(vec![RespFrame::bulk_str("l1"), RespFrame::bulk_str("b")])
        );
        assert_eq!(
            run(&["EXISTS", "l1"], &mut server, &mut client),
            RespFrame::Integer(0)
        );

        assert_eq!(
            run(&["RPUSH", "l2", "x"], &mut server, &mut client),
            RespFrame::Integer(1)
        );
        assert_eq!(
            run(&["RPUSH", "l3", "y"], &mut server, &mut client),
            RespFrame::Integer(1)
        );
        assert_eq!(
            run(
                &["BLPOP", "missing", "l2", "l3", "0"],
                &mut server,
                &mut client
            ),
            RespFrame::Array(vec![RespFrame::bulk_str("l2"), RespFrame::bulk_str("x")])
        );
        assert_eq!(
            run(
                &["BRPOP", "missing", "l2", "l3", "0"],
                &mut server,
                &mut client
            ),
            RespFrame::Array(vec![RespFrame::bulk_str("l3"), RespFrame::bulk_str("y")])
        );

        // Blocking commands on missing keys now return immediately with retry_blocking set
        let outcome = run_full(
            &["BLPOP", "missing", "none", "0.1"],
            &mut server,
            &mut client,
        );
        assert_eq!(outcome.response, RespFrame::BulkString(None));
        assert!(outcome.retry_blocking.is_some());

        let outcome = run_full(&["BLPOP", "missing", "0"], &mut server, &mut client);
        assert_eq!(outcome.response, RespFrame::BulkString(None));
        assert!(
            outcome
                .retry_blocking
                .as_ref()
                .is_some_and(|retry| retry.deadline_ms.is_none())
        );

        let outcome = run_full(
            &["BRPOP", "missing", "none", "0.1"],
            &mut server,
            &mut client,
        );
        assert_eq!(outcome.response, RespFrame::BulkString(None));
        assert!(outcome.retry_blocking.is_some());

        let outcome = run_full(&["BRPOP", "missing", "0"], &mut server, &mut client);
        assert_eq!(outcome.response, RespFrame::BulkString(None));
        assert!(
            outcome
                .retry_blocking
                .as_ref()
                .is_some_and(|retry| retry.deadline_ms.is_none())
        );

        assert_eq!(
            run(&["BLPOP", "missing", "x"], &mut server, &mut client),
            RespFrame::error_str("ERR timeout is not a float or out of range")
        );
        assert_eq!(
            run(&["BRPOP", "missing", "-1"], &mut server, &mut client),
            RespFrame::error_str("ERR timeout is negative")
        );

        assert_eq!(
            run(&["SET", "s", "v"], &mut server, &mut client),
            RespFrame::ok()
        );
        assert_eq!(
            run(&["BLPOP", "s", "l2", "0"], &mut server, &mut client),
            RespFrame::error_str(
                "WRONGTYPE Operation against a key holding the wrong kind of value"
            )
        );
    }

    #[test]
    fn m2_list_wrongtype_and_scan_type() {
        let mut server = ServerState::with_default_dbs();
        let mut client = ClientState::default();

        assert_eq!(
            run(&["SET", "s", "v"], &mut server, &mut client),
            RespFrame::ok()
        );
        assert_eq!(
            run(&["LPUSH", "s", "x"], &mut server, &mut client),
            RespFrame::error_str(
                "WRONGTYPE Operation against a key holding the wrong kind of value"
            )
        );
        assert_eq!(
            run(&["LSET", "s", "0", "x"], &mut server, &mut client),
            RespFrame::error_str(
                "WRONGTYPE Operation against a key holding the wrong kind of value"
            )
        );
        assert_eq!(
            run(&["LTRIM", "s", "0", "0"], &mut server, &mut client),
            RespFrame::error_str(
                "WRONGTYPE Operation against a key holding the wrong kind of value"
            )
        );

        assert_eq!(
            run(&["RPUSH", "l", "v1", "v2"], &mut server, &mut client),
            RespFrame::Integer(2)
        );
        assert_eq!(
            run(&["TYPE", "l"], &mut server, &mut client),
            RespFrame::simple_str("list")
        );
        assert_eq!(
            run(&["OBJECT", "ENCODING", "l"], &mut server, &mut client),
            RespFrame::bulk_str("quicklist")
        );
        assert_eq!(
            run(&["GET", "l"], &mut server, &mut client),
            RespFrame::error_str(
                "WRONGTYPE Operation against a key holding the wrong kind of value"
            )
        );

        assert_eq!(
            run(
                &["SCAN", "0", "TYPE", "list", "COUNT", "100"],
                &mut server,
                &mut client
            ),
            RespFrame::Array(vec![
                RespFrame::bulk_str("0"),
                RespFrame::Array(vec![RespFrame::bulk_str("l")]),
            ])
        );
    }

    #[test]
    fn m2_list_pushx_commands() {
        let mut server = ServerState::with_default_dbs();
        let mut client = ClientState::default();

        assert_eq!(
            run(&["LPUSHX", "missing", "a"], &mut server, &mut client),
            RespFrame::Integer(0)
        );
        assert_eq!(
            run(&["RPUSHX", "missing", "a"], &mut server, &mut client),
            RespFrame::Integer(0)
        );

        assert_eq!(
            run(&["RPUSH", "l", "base"], &mut server, &mut client),
            RespFrame::Integer(1)
        );
        assert_eq!(
            run(&["LPUSHX", "l", "x1", "x2"], &mut server, &mut client),
            RespFrame::Integer(3)
        );
        assert_eq!(
            run(&["RPUSHX", "l", "y1", "y2"], &mut server, &mut client),
            RespFrame::Integer(5)
        );

        assert_eq!(
            run(&["LRANGE", "l", "0", "-1"], &mut server, &mut client),
            RespFrame::Array(vec![
                RespFrame::bulk_str("x2"),
                RespFrame::bulk_str("x1"),
                RespFrame::bulk_str("base"),
                RespFrame::bulk_str("y1"),
                RespFrame::bulk_str("y2"),
            ])
        );

        assert_eq!(
            run(&["SET", "s", "v"], &mut server, &mut client),
            RespFrame::ok()
        );
        assert_eq!(
            run(&["LPUSHX", "s", "x"], &mut server, &mut client),
            RespFrame::error_str(
                "WRONGTYPE Operation against a key holding the wrong kind of value"
            )
        );
        assert_eq!(
            run(&["RPUSHX", "s", "x"], &mut server, &mut client),
            RespFrame::error_str(
                "WRONGTYPE Operation against a key holding the wrong kind of value"
            )
        );
    }

    #[test]
    fn m2_list_lmove_commands() {
        let mut server = ServerState::with_default_dbs();
        let mut client = ClientState::default();

        assert_eq!(
            run(&["RPUSH", "src", "a", "b", "c"], &mut server, &mut client),
            RespFrame::Integer(3)
        );
        assert_eq!(
            run(
                &["LMOVE", "src", "dst", "LEFT", "RIGHT"],
                &mut server,
                &mut client
            ),
            RespFrame::bulk_str("a")
        );
        assert_eq!(
            run(
                &["LMOVE", "src", "dst", "RIGHT", "LEFT"],
                &mut server,
                &mut client
            ),
            RespFrame::bulk_str("c")
        );
        assert_eq!(
            run(&["LRANGE", "src", "0", "-1"], &mut server, &mut client),
            RespFrame::Array(vec![RespFrame::bulk_str("b")])
        );
        assert_eq!(
            run(&["LRANGE", "dst", "0", "-1"], &mut server, &mut client),
            RespFrame::Array(vec![RespFrame::bulk_str("c"), RespFrame::bulk_str("a")])
        );

        assert_eq!(
            run(&["RPUSH", "rot", "x", "y", "z"], &mut server, &mut client),
            RespFrame::Integer(3)
        );
        assert_eq!(
            run(
                &["LMOVE", "rot", "rot", "LEFT", "RIGHT"],
                &mut server,
                &mut client
            ),
            RespFrame::bulk_str("x")
        );
        assert_eq!(
            run(&["LRANGE", "rot", "0", "-1"], &mut server, &mut client),
            RespFrame::Array(vec![
                RespFrame::bulk_str("y"),
                RespFrame::bulk_str("z"),
                RespFrame::bulk_str("x"),
            ])
        );

        assert_eq!(
            run(&["SET", "k", "v"], &mut server, &mut client),
            RespFrame::ok()
        );
        assert_eq!(
            run(
                &["LMOVE", "missing", "k", "LEFT", "RIGHT"],
                &mut server,
                &mut client,
            ),
            RespFrame::BulkString(None)
        );

        assert_eq!(
            run(
                &["LMOVE", "k", "dst", "LEFT", "RIGHT"],
                &mut server,
                &mut client
            ),
            RespFrame::error_str(
                "WRONGTYPE Operation against a key holding the wrong kind of value"
            )
        );

        assert_eq!(
            run(&["RPUSH", "src2", "m"], &mut server, &mut client),
            RespFrame::Integer(1)
        );
        assert_eq!(
            run(&["SET", "dst2", "v"], &mut server, &mut client),
            RespFrame::ok()
        );
        assert_eq!(
            run(
                &["LMOVE", "src2", "dst2", "LEFT", "RIGHT"],
                &mut server,
                &mut client,
            ),
            RespFrame::error_str(
                "WRONGTYPE Operation against a key holding the wrong kind of value"
            )
        );
        assert_eq!(
            run(&["LLEN", "src2"], &mut server, &mut client),
            RespFrame::Integer(1)
        );

        assert_eq!(
            run(
                &["LMOVE", "src2", "dst", "UP", "RIGHT"],
                &mut server,
                &mut client,
            ),
            RespFrame::error_str("ERR syntax error")
        );

        assert_eq!(
            run(&["RPUSH", "one", "only"], &mut server, &mut client),
            RespFrame::Integer(1)
        );
        assert_eq!(
            run(
                &["LMOVE", "one", "dst", "LEFT", "LEFT"],
                &mut server,
                &mut client
            ),
            RespFrame::bulk_str("only")
        );
        assert_eq!(
            run(&["EXISTS", "one"], &mut server, &mut client),
            RespFrame::Integer(0)
        );
    }

    #[test]
    fn m2_list_lrem_commands() {
        let mut server = ServerState::with_default_dbs();
        let mut client = ClientState::default();

        assert_eq!(
            run(
                &["RPUSH", "l", "a", "b", "a", "c", "a"],
                &mut server,
                &mut client
            ),
            RespFrame::Integer(5)
        );

        assert_eq!(
            run(&["LREM", "l", "1", "a"], &mut server, &mut client),
            RespFrame::Integer(1)
        );
        assert_eq!(
            run(&["LRANGE", "l", "0", "-1"], &mut server, &mut client),
            RespFrame::Array(vec![
                RespFrame::bulk_str("b"),
                RespFrame::bulk_str("a"),
                RespFrame::bulk_str("c"),
                RespFrame::bulk_str("a"),
            ])
        );

        assert_eq!(
            run(&["LREM", "l", "-1", "a"], &mut server, &mut client),
            RespFrame::Integer(1)
        );
        assert_eq!(
            run(&["LRANGE", "l", "0", "-1"], &mut server, &mut client),
            RespFrame::Array(vec![
                RespFrame::bulk_str("b"),
                RespFrame::bulk_str("a"),
                RespFrame::bulk_str("c"),
            ])
        );

        assert_eq!(
            run(&["LREM", "l", "0", "a"], &mut server, &mut client),
            RespFrame::Integer(1)
        );
        assert_eq!(
            run(&["LRANGE", "l", "0", "-1"], &mut server, &mut client),
            RespFrame::Array(vec![RespFrame::bulk_str("b"), RespFrame::bulk_str("c")])
        );

        assert_eq!(
            run(&["LREM", "l", "10", "z"], &mut server, &mut client),
            RespFrame::Integer(0)
        );
        assert_eq!(
            run(&["LREM", "missing", "1", "a"], &mut server, &mut client),
            RespFrame::Integer(0)
        );

        assert_eq!(
            run(&["SET", "s", "v"], &mut server, &mut client),
            RespFrame::ok()
        );
        assert_eq!(
            run(&["LREM", "s", "1", "v"], &mut server, &mut client),
            RespFrame::error_str(
                "WRONGTYPE Operation against a key holding the wrong kind of value"
            )
        );
        assert_eq!(
            run(&["LREM", "l", "x", "a"], &mut server, &mut client),
            RespFrame::error_str("ERR value is not an integer or out of range")
        );
    }

    #[test]
    fn m2_list_lpos_commands() {
        let mut server = ServerState::with_default_dbs();
        let mut client = ClientState::default();

        assert_eq!(
            run(
                &["RPUSH", "l", "a", "b", "a", "c", "a", "b", "a"],
                &mut server,
                &mut client,
            ),
            RespFrame::Integer(7)
        );

        assert_eq!(
            run(&["LPOS", "l", "a"], &mut server, &mut client),
            RespFrame::Integer(0)
        );
        assert_eq!(
            run(&["LPOS", "l", "a", "RANK", "2"], &mut server, &mut client),
            RespFrame::Integer(2)
        );
        assert_eq!(
            run(&["LPOS", "l", "a", "RANK", "-1"], &mut server, &mut client),
            RespFrame::Integer(6)
        );

        assert_eq!(
            run(&["LPOS", "l", "a", "COUNT", "2"], &mut server, &mut client),
            RespFrame::Array(vec![RespFrame::Integer(0), RespFrame::Integer(2)])
        );
        assert_eq!(
            run(
                &["LPOS", "l", "a", "RANK", "2", "COUNT", "2"],
                &mut server,
                &mut client,
            ),
            RespFrame::Array(vec![RespFrame::Integer(2), RespFrame::Integer(4)])
        );
        assert_eq!(
            run(
                &["LPOS", "l", "a", "RANK", "-2", "COUNT", "2"],
                &mut server,
                &mut client,
            ),
            RespFrame::Array(vec![RespFrame::Integer(4), RespFrame::Integer(2)])
        );
        assert_eq!(
            run(&["LPOS", "l", "a", "COUNT", "0"], &mut server, &mut client),
            RespFrame::Array(vec![])
        );

        assert_eq!(
            run(&["LPOS", "l", "a", "MAXLEN", "2"], &mut server, &mut client),
            RespFrame::Integer(0)
        );
        assert_eq!(
            run(&["LPOS", "l", "c", "MAXLEN", "2"], &mut server, &mut client),
            RespFrame::BulkString(None)
        );

        assert_eq!(
            run(&["LPOS", "missing", "a"], &mut server, &mut client),
            RespFrame::BulkString(None)
        );
        assert_eq!(
            run(
                &["LPOS", "missing", "a", "COUNT", "3"],
                &mut server,
                &mut client
            ),
            RespFrame::Array(vec![])
        );

        assert_eq!(
            run(&["SET", "k", "v"], &mut server, &mut client),
            RespFrame::ok()
        );
        assert_eq!(
            run(&["LPOS", "k", "v"], &mut server, &mut client),
            RespFrame::error_str(
                "WRONGTYPE Operation against a key holding the wrong kind of value"
            )
        );

        assert_eq!(
            run(&["LPOS", "l", "a", "RANK", "0"], &mut server, &mut client),
            RespFrame::error_str("ERR RANK can't be zero")
        );
        assert_eq!(
            run(&["LPOS", "l", "a", "COUNT", "-1"], &mut server, &mut client),
            RespFrame::error_str("ERR COUNT can't be negative")
        );
        assert_eq!(
            run(
                &["LPOS", "l", "a", "MAXLEN", "-1"],
                &mut server,
                &mut client
            ),
            RespFrame::error_str("ERR MAXLEN can't be negative")
        );
        assert_eq!(
            run(&["LPOS", "l", "a", "COUNT"], &mut server, &mut client),
            RespFrame::error_str("ERR syntax error")
        );
        assert_eq!(
            run(&["LPOS", "l", "a", "NOPE", "1"], &mut server, &mut client),
            RespFrame::error_str("ERR syntax error")
        );
    }

    #[test]
    fn m2_list_rpoplpush_commands() {
        let mut server = ServerState::with_default_dbs();
        let mut client = ClientState::default();

        assert_eq!(
            run(&["RPUSH", "src", "a", "b", "c"], &mut server, &mut client),
            RespFrame::Integer(3)
        );
        assert_eq!(
            run(&["RPOPLPUSH", "src", "dst"], &mut server, &mut client),
            RespFrame::bulk_str("c")
        );
        assert_eq!(
            run(&["LRANGE", "src", "0", "-1"], &mut server, &mut client),
            RespFrame::Array(vec![RespFrame::bulk_str("a"), RespFrame::bulk_str("b")])
        );
        assert_eq!(
            run(&["LRANGE", "dst", "0", "-1"], &mut server, &mut client),
            RespFrame::Array(vec![RespFrame::bulk_str("c")])
        );

        assert_eq!(
            run(&["RPOPLPUSH", "src", "src"], &mut server, &mut client),
            RespFrame::bulk_str("b")
        );
        assert_eq!(
            run(&["LRANGE", "src", "0", "-1"], &mut server, &mut client),
            RespFrame::Array(vec![RespFrame::bulk_str("b"), RespFrame::bulk_str("a")])
        );

        assert_eq!(
            run(&["RPOPLPUSH", "missing", "dst"], &mut server, &mut client),
            RespFrame::BulkString(None)
        );

        assert_eq!(
            run(&["SET", "k", "v"], &mut server, &mut client),
            RespFrame::ok()
        );
        assert_eq!(
            run(&["RPOPLPUSH", "k", "dst"], &mut server, &mut client),
            RespFrame::error_str(
                "WRONGTYPE Operation against a key holding the wrong kind of value"
            )
        );

        assert_eq!(
            run(&["RPUSH", "src2", "x"], &mut server, &mut client),
            RespFrame::Integer(1)
        );
        assert_eq!(
            run(&["SET", "dst2", "v"], &mut server, &mut client),
            RespFrame::ok()
        );
        assert_eq!(
            run(&["RPOPLPUSH", "src2", "dst2"], &mut server, &mut client),
            RespFrame::error_str(
                "WRONGTYPE Operation against a key holding the wrong kind of value"
            )
        );
        assert_eq!(
            run(&["LLEN", "src2"], &mut server, &mut client),
            RespFrame::Integer(1)
        );

        assert_eq!(
            run(&["RPUSH", "one", "only"], &mut server, &mut client),
            RespFrame::Integer(1)
        );
        assert_eq!(
            run(&["RPOPLPUSH", "one", "dst"], &mut server, &mut client),
            RespFrame::bulk_str("only")
        );
        assert_eq!(
            run(&["EXISTS", "one"], &mut server, &mut client),
            RespFrame::Integer(0)
        );
    }

    #[test]
    fn m2_list_blocking_move_baseline_commands() {
        let mut server = ServerState::with_default_dbs();
        let mut client = ClientState::default();

        assert_eq!(
            run(&["RPUSH", "q", "x"], &mut server, &mut client),
            RespFrame::Integer(1)
        );
        assert_eq!(
            run(
                &["BLMOVE", "q", "dst", "RIGHT", "LEFT", "0"],
                &mut server,
                &mut client,
            ),
            RespFrame::bulk_str("x")
        );
        assert_eq!(
            run(&["EXISTS", "q"], &mut server, &mut client),
            RespFrame::Integer(0)
        );

        // Blocking commands on missing keys now return immediately with retry_blocking set
        let outcome = run_full(
            &["BLMOVE", "missing", "dst", "LEFT", "RIGHT", "0.05"],
            &mut server,
            &mut client,
        );
        assert_eq!(outcome.response, RespFrame::BulkString(None));
        assert!(outcome.retry_blocking.is_some());

        let outcome = run_full(
            &["BRPOPLPUSH", "missing", "dst", "0.05"],
            &mut server,
            &mut client,
        );
        assert_eq!(outcome.response, RespFrame::BulkString(None));
        assert!(outcome.retry_blocking.is_some());

        assert_eq!(
            run(
                &["BLMOVE", "missing", "dst", "LEFT", "RIGHT", "x"],
                &mut server,
                &mut client,
            ),
            RespFrame::error_str("ERR timeout is not a float or out of range")
        );
        assert_eq!(
            run(
                &["BLMOVE", "missing", "dst", "LEFT", "RIGHT", "-1"],
                &mut server,
                &mut client,
            ),
            RespFrame::error_str("ERR timeout is negative")
        );
        assert_eq!(
            run(
                &["BRPOPLPUSH", "missing", "dst", "x"],
                &mut server,
                &mut client
            ),
            RespFrame::error_str("ERR timeout is not a float or out of range")
        );
        assert_eq!(
            run(
                &["BRPOPLPUSH", "missing", "dst", "-1"],
                &mut server,
                &mut client
            ),
            RespFrame::error_str("ERR timeout is negative")
        );
    }

    #[test]
    fn m2_list_lmpop_commands() {
        let mut server = ServerState::with_default_dbs();
        let mut client = ClientState::default();

        assert_eq!(
            run(&["RPUSH", "k2", "a", "b", "c"], &mut server, &mut client),
            RespFrame::Integer(3)
        );
        assert_eq!(
            run(&["RPUSH", "k3", "x", "y"], &mut server, &mut client),
            RespFrame::Integer(2)
        );

        assert_eq!(
            run(
                &["LMPOP", "2", "k1", "k2", "LEFT"],
                &mut server,
                &mut client
            ),
            RespFrame::Array(vec![
                RespFrame::bulk_str("k2"),
                RespFrame::Array(vec![RespFrame::bulk_str("a")]),
            ])
        );
        assert_eq!(
            run(
                &["LMPOP", "2", "k1", "k2", "RIGHT", "COUNT", "2"],
                &mut server,
                &mut client,
            ),
            RespFrame::Array(vec![
                RespFrame::bulk_str("k2"),
                RespFrame::Array(vec![RespFrame::bulk_str("c"), RespFrame::bulk_str("b")]),
            ])
        );
        assert_eq!(
            run(&["EXISTS", "k2"], &mut server, &mut client),
            RespFrame::Integer(0)
        );

        assert_eq!(
            run(
                &["LMPOP", "2", "k1", "k3", "LEFT", "COUNT", "3"],
                &mut server,
                &mut client,
            ),
            RespFrame::Array(vec![
                RespFrame::bulk_str("k3"),
                RespFrame::Array(vec![RespFrame::bulk_str("x"), RespFrame::bulk_str("y")]),
            ])
        );

        assert_eq!(
            run(
                &["LMPOP", "2", "k1", "k2", "LEFT"],
                &mut server,
                &mut client
            ),
            RespFrame::BulkString(None)
        );

        assert_eq!(
            run(&["LMPOP", "0", "k1", "LEFT"], &mut server, &mut client),
            RespFrame::error_str("ERR numkeys should be greater than 0")
        );
        assert_eq!(
            run(&["LMPOP", "x", "k1", "LEFT"], &mut server, &mut client),
            RespFrame::error_str("ERR value is not an integer or out of range")
        );
        assert_eq!(
            run(
                &["LMPOP", "1", "k1", "LEFT", "COUNT", "0"],
                &mut server,
                &mut client,
            ),
            RespFrame::error_str("ERR count should be greater than 0")
        );
        assert_eq!(
            run(
                &["LMPOP", "1", "k1", "LEFT", "COUNT", "x"],
                &mut server,
                &mut client,
            ),
            RespFrame::error_str("ERR value is not an integer or out of range")
        );
        assert_eq!(
            run(&["LMPOP", "1", "k1", "UP"], &mut server, &mut client),
            RespFrame::error_str("ERR syntax error")
        );

        assert_eq!(
            run(&["SET", "wt", "v"], &mut server, &mut client),
            RespFrame::ok()
        );
        assert_eq!(
            run(
                &["LMPOP", "2", "missing", "wt", "LEFT"],
                &mut server,
                &mut client
            ),
            RespFrame::error_str(
                "WRONGTYPE Operation against a key holding the wrong kind of value"
            )
        );
    }

    #[test]
    fn m2_list_blmpop_baseline_commands() {
        let mut server = ServerState::with_default_dbs();
        let mut client = ClientState::default();

        assert_eq!(
            run(&["RPUSH", "q1", "a", "b"], &mut server, &mut client),
            RespFrame::Integer(2)
        );
        assert_eq!(
            run(
                &["BLMPOP", "0", "1", "q1", "LEFT", "COUNT", "2"],
                &mut server,
                &mut client,
            ),
            RespFrame::Array(vec![
                RespFrame::bulk_str("q1"),
                RespFrame::Array(vec![RespFrame::bulk_str("a"), RespFrame::bulk_str("b")]),
            ])
        );

        // Blocking commands on missing keys now return immediately with retry_blocking set
        let outcome = run_full(
            &["BLMPOP", "0.1", "1", "q1", "LEFT"],
            &mut server,
            &mut client,
        );
        assert_eq!(outcome.response, RespFrame::BulkString(None));
        assert!(outcome.retry_blocking.is_some());

        assert_eq!(
            run(
                &["BLMPOP", "x", "1", "q1", "LEFT"],
                &mut server,
                &mut client
            ),
            RespFrame::error_str("ERR timeout is not a float or out of range")
        );
        assert_eq!(
            run(
                &["BLMPOP", "-1", "1", "q1", "LEFT"],
                &mut server,
                &mut client
            ),
            RespFrame::error_str("ERR timeout is negative")
        );

        assert_eq!(
            run(&["SET", "k", "v"], &mut server, &mut client),
            RespFrame::ok()
        );
        assert_eq!(
            run(&["BLMPOP", "0", "1", "k", "LEFT"], &mut server, &mut client),
            RespFrame::error_str(
                "WRONGTYPE Operation against a key holding the wrong kind of value"
            )
        );
    }

    #[test]
    fn m2_set_srandmember_commands() {
        let mut server = ServerState::with_default_dbs();
        let mut client = ClientState::default();

        assert_eq!(
            run(&["SADD", "s", "a", "b", "c"], &mut server, &mut client),
            RespFrame::Integer(3)
        );

        let single = run(&["SRANDMEMBER", "s"], &mut server, &mut client);
        let RespFrame::BulkString(Some(single_member)) = single else {
            panic!("SRANDMEMBER without count should return bulk string");
        };
        assert!(
            [b"a".as_slice(), b"b".as_slice(), b"c".as_slice(),]
                .iter()
                .any(|m| single_member == *m)
        );
        assert_eq!(
            run(&["SCARD", "s"], &mut server, &mut client),
            RespFrame::Integer(3)
        );

        let distinct = run(&["SRANDMEMBER", "s", "2"], &mut server, &mut client);
        let RespFrame::Array(distinct_members) = distinct else {
            panic!("SRANDMEMBER with positive count should return array");
        };
        assert_eq!(distinct_members.len(), 2);
        let mut distinct_raw = Vec::new();
        for frame in distinct_members {
            let RespFrame::BulkString(Some(member)) = frame else {
                panic!("SRANDMEMBER members should be bulk strings");
            };
            assert!(
                [b"a".as_slice(), b"b".as_slice(), b"c".as_slice(),]
                    .iter()
                    .any(|m| member == *m)
            );
            distinct_raw.push(member);
        }
        distinct_raw.sort();
        distinct_raw.dedup();
        assert_eq!(distinct_raw.len(), 2);
        assert_eq!(
            run(&["SCARD", "s"], &mut server, &mut client),
            RespFrame::Integer(3)
        );

        let all_members = run(&["SRANDMEMBER", "s", "10"], &mut server, &mut client);
        let RespFrame::Array(all_members_raw) = all_members else {
            panic!("SRANDMEMBER with oversized positive count should return array");
        };
        assert_eq!(all_members_raw.len(), 3);

        let with_duplicates = run(&["SRANDMEMBER", "s", "-5"], &mut server, &mut client);
        let RespFrame::Array(dup_members) = with_duplicates else {
            panic!("SRANDMEMBER with negative count should return array");
        };
        assert_eq!(dup_members.len(), 5);
        let mut seen = Vec::new();
        for frame in dup_members {
            let RespFrame::BulkString(Some(member)) = frame else {
                panic!("SRANDMEMBER members should be bulk strings");
            };
            assert!(
                [b"a".as_slice(), b"b".as_slice(), b"c".as_slice(),]
                    .iter()
                    .any(|m| member == *m)
            );
            seen.push(member);
        }
        let unique_count = {
            let mut dedup = seen.clone();
            dedup.sort();
            dedup.dedup();
            dedup.len()
        };
        assert!(unique_count <= 3);
        assert_eq!(
            run(&["SCARD", "s"], &mut server, &mut client),
            RespFrame::Integer(3)
        );

        assert_eq!(
            run(&["SRANDMEMBER", "s", "0"], &mut server, &mut client),
            RespFrame::Array(vec![])
        );
        assert_eq!(
            run(&["SRANDMEMBER", "missing"], &mut server, &mut client),
            RespFrame::BulkString(None)
        );
        assert_eq!(
            run(&["SRANDMEMBER", "missing", "2"], &mut server, &mut client),
            RespFrame::Array(vec![])
        );

        assert_eq!(
            run(&["SET", "k", "v"], &mut server, &mut client),
            RespFrame::ok()
        );
        assert_eq!(
            run(&["SRANDMEMBER", "k"], &mut server, &mut client),
            RespFrame::error_str(
                "WRONGTYPE Operation against a key holding the wrong kind of value"
            )
        );
        assert_eq!(
            run(&["SRANDMEMBER", "s", "nope"], &mut server, &mut client),
            RespFrame::error_str("ERR value is not an integer or out of range")
        );
    }
    #[test]
    fn m2_set_extended_commands() {
        let mut server = ServerState::with_default_dbs();
        let mut client = ClientState::default();

        assert_eq!(
            run(&["SADD", "s", "a", "b", "c", "d"], &mut server, &mut client),
            RespFrame::Integer(4)
        );

        assert_eq!(
            run(
                &["SMISMEMBER", "s", "a", "x", "d"],
                &mut server,
                &mut client
            ),
            RespFrame::Array(vec![
                RespFrame::Integer(1),
                RespFrame::Integer(0),
                RespFrame::Integer(1)
            ])
        );
        assert_eq!(
            run(
                &["SMISMEMBER", "missing", "a", "b"],
                &mut server,
                &mut client
            ),
            RespFrame::Array(vec![RespFrame::Integer(0), RespFrame::Integer(0)])
        );

        let first_popped = run(&["SPOP", "s"], &mut server, &mut client);
        let RespFrame::BulkString(Some(first_member)) = first_popped else {
            panic!("SPOP should return one member as bulk string");
        };
        assert!(
            ["a", "b", "c", "d"]
                .iter()
                .any(|m| first_member == m.as_bytes())
        );

        let second_popped = run(&["SPOP", "s", "2"], &mut server, &mut client);
        let RespFrame::Array(second_members) = second_popped else {
            panic!("SPOP with count should return array");
        };
        assert_eq!(second_members.len(), 2);

        let mut removed = vec![first_member.clone()];
        for frame in second_members {
            let RespFrame::BulkString(Some(member)) = frame else {
                panic!("SPOP members should be bulk strings");
            };
            assert!(["a", "b", "c", "d"].iter().any(|m| member == m.as_bytes()));
            assert!(!removed.iter().any(|existing| existing == &member));
            removed.push(member);
        }

        assert_eq!(
            run(&["SCARD", "s"], &mut server, &mut client),
            RespFrame::Integer(1)
        );
        let remaining = run(&["SMEMBERS", "s"], &mut server, &mut client);
        let RespFrame::Array(remaining_members) = remaining else {
            panic!("SMEMBERS should return array");
        };
        assert_eq!(remaining_members.len(), 1);
        let RespFrame::BulkString(Some(last_member)) = remaining_members[0].clone() else {
            panic!("SMEMBERS member should be bulk string");
        };
        assert!(!removed.iter().any(|existing| existing == &last_member));

        assert_eq!(
            run(&["SPOP", "s", "0"], &mut server, &mut client),
            RespFrame::Array(vec![])
        );
        assert_eq!(
            run(&["SCARD", "s"], &mut server, &mut client),
            RespFrame::Integer(1)
        );

        assert_eq!(
            run(&["SPOP", "missing"], &mut server, &mut client),
            RespFrame::BulkString(None)
        );
        assert_eq!(
            run(&["SPOP", "missing", "3"], &mut server, &mut client),
            RespFrame::Array(vec![])
        );

        assert_eq!(
            run(&["SPOP", "s", "-1"], &mut server, &mut client),
            RespFrame::error_str("ERR value is out of range, must be positive")
        );
        assert_eq!(
            run(&["SPOP", "s", "x"], &mut server, &mut client),
            RespFrame::error_str("ERR value is not an integer or out of range")
        );

        assert_eq!(
            run(&["SADD", "smove:src", "a", "b"], &mut server, &mut client),
            RespFrame::Integer(2)
        );
        assert_eq!(
            run(
                &["SMOVE", "smove:src", "smove:dst", "a"],
                &mut server,
                &mut client
            ),
            RespFrame::Integer(1)
        );
        assert_eq!(
            run(
                &["SMOVE", "smove:src", "smove:dst", "a"],
                &mut server,
                &mut client
            ),
            RespFrame::Integer(0)
        );
        assert_eq!(
            run(
                &["SMOVE", "smove:dst", "smove:dst", "a"],
                &mut server,
                &mut client
            ),
            RespFrame::Integer(1)
        );
        assert_array_set_eq(
            &run(&["SMEMBERS", "smove:src"], &mut server, &mut client),
            &["b"],
        );
        assert_array_set_eq(
            &run(&["SMEMBERS", "smove:dst"], &mut server, &mut client),
            &["a"],
        );

        assert_eq!(
            run(&["SET", "k", "v"], &mut server, &mut client),
            RespFrame::ok()
        );
        assert_eq!(
            run(&["SMISMEMBER", "k", "v"], &mut server, &mut client),
            RespFrame::error_str(
                "WRONGTYPE Operation against a key holding the wrong kind of value"
            )
        );
        assert_eq!(
            run(&["SPOP", "k"], &mut server, &mut client),
            RespFrame::error_str(
                "WRONGTYPE Operation against a key holding the wrong kind of value"
            )
        );
    }

    #[test]
    fn m2_set_algebra_commands() {
        let mut server = ServerState::with_default_dbs();
        let mut client = ClientState::default();

        assert_eq!(
            run(&["SADD", "s1", "a", "b", "c"], &mut server, &mut client),
            RespFrame::Integer(3)
        );
        assert_eq!(
            run(&["SADD", "s2", "b", "c", "d"], &mut server, &mut client),
            RespFrame::Integer(3)
        );
        assert_eq!(
            run(&["SADD", "s3", "c", "d", "e"], &mut server, &mut client),
            RespFrame::Integer(3)
        );

        assert_array_set_eq(
            &run(&["SDIFF", "s1", "s2", "s3"], &mut server, &mut client),
            &["a"],
        );
        assert_array_set_eq(
            &run(&["SINTER", "s1", "s2", "s3"], &mut server, &mut client),
            &["c"],
        );
        assert_array_set_eq(
            &run(&["SUNION", "s1", "s2", "s3"], &mut server, &mut client),
            &["a", "b", "c", "d", "e"],
        );
        assert_eq!(
            run(&["SINTER", "missing", "s1"], &mut server, &mut client),
            RespFrame::Array(vec![])
        );
        assert_eq!(
            run(&["SDIFF", "missing", "s1"], &mut server, &mut client),
            RespFrame::Array(vec![])
        );

        assert_eq!(
            run(
                &["SDIFFSTORE", "dst", "s1", "s2", "s3"],
                &mut server,
                &mut client
            ),
            RespFrame::Integer(1)
        );
        assert_array_set_eq(&run(&["SMEMBERS", "dst"], &mut server, &mut client), &["a"]);
        assert_eq!(
            run(
                &["SINTERSTORE", "dst", "s1", "s2", "s3"],
                &mut server,
                &mut client
            ),
            RespFrame::Integer(1)
        );
        assert_array_set_eq(&run(&["SMEMBERS", "dst"], &mut server, &mut client), &["c"]);
        assert_eq!(
            run(
                &["SUNIONSTORE", "dst", "s1", "s2", "s3"],
                &mut server,
                &mut client
            ),
            RespFrame::Integer(5)
        );
        assert_eq!(
            run(&["SCARD", "dst"], &mut server, &mut client),
            RespFrame::Integer(5)
        );

        assert_eq!(
            run(&["SADD", "z", "x"], &mut server, &mut client),
            RespFrame::Integer(1)
        );
        assert_eq!(
            run(&["SET", "empty-dst", "v"], &mut server, &mut client),
            RespFrame::ok()
        );
        assert_eq!(
            run(
                &["SDIFFSTORE", "empty-dst", "z", "z"],
                &mut server,
                &mut client
            ),
            RespFrame::Integer(0)
        );
        assert_eq!(
            run(&["EXISTS", "empty-dst"], &mut server, &mut client),
            RespFrame::Integer(0)
        );

        assert_eq!(
            run(&["SET", "k", "v"], &mut server, &mut client),
            RespFrame::ok()
        );
        assert_eq!(
            run(&["SDIFF", "k", "s1"], &mut server, &mut client),
            RespFrame::error_str(
                "WRONGTYPE Operation against a key holding the wrong kind of value"
            )
        );
        assert_eq!(
            run(&["SINTER", "s1", "k"], &mut server, &mut client),
            RespFrame::error_str(
                "WRONGTYPE Operation against a key holding the wrong kind of value"
            )
        );
        assert_eq!(
            run(&["SUNION", "s1", "k"], &mut server, &mut client),
            RespFrame::error_str(
                "WRONGTYPE Operation against a key holding the wrong kind of value"
            )
        );
        assert_eq!(
            run(
                &["SINTERSTORE", "dst2", "s1", "k"],
                &mut server,
                &mut client
            ),
            RespFrame::error_str(
                "WRONGTYPE Operation against a key holding the wrong kind of value"
            )
        );
    }

    #[test]
    fn m2_set_sintercard_commands() {
        let mut server = ServerState::with_default_dbs();
        let mut client = ClientState::default();

        assert_eq!(
            run(&["SADD", "s1", "a", "b", "c"], &mut server, &mut client),
            RespFrame::Integer(3)
        );
        assert_eq!(
            run(&["SADD", "s2", "b", "c", "d"], &mut server, &mut client),
            RespFrame::Integer(3)
        );
        assert_eq!(
            run(&["SADD", "s3", "c", "d", "e"], &mut server, &mut client),
            RespFrame::Integer(3)
        );

        assert_eq!(
            run(
                &["SINTERCARD", "3", "s1", "s2", "s3"],
                &mut server,
                &mut client
            ),
            RespFrame::Integer(1)
        );
        assert_eq!(
            run(
                &["SINTERCARD", "3", "s1", "s2", "s3", "LIMIT", "1"],
                &mut server,
                &mut client,
            ),
            RespFrame::Integer(1)
        );
        assert_eq!(
            run(
                &["SINTERCARD", "3", "s1", "s2", "s3", "LIMIT", "0"],
                &mut server,
                &mut client,
            ),
            RespFrame::Integer(1)
        );
        assert_eq!(
            run(
                &["SINTERCARD", "2", "s1", "missing"],
                &mut server,
                &mut client
            ),
            RespFrame::Integer(0)
        );

        assert_eq!(
            run(&["SINTERCARD", "0", "s1"], &mut server, &mut client),
            RespFrame::error_str("ERR numkeys should be greater than 0")
        );
        assert_eq!(
            run(&["SINTERCARD", "x", "s1"], &mut server, &mut client),
            RespFrame::error_str("ERR value is not an integer or out of range")
        );
        assert_eq!(
            run(&["SINTERCARD", "3", "s1", "s2"], &mut server, &mut client),
            RespFrame::error_str("ERR Number of keys can't be greater than number of args")
        );
        assert_eq!(
            run(
                &["SINTERCARD", "2", "s1", "s2", "LIMIT", "-1"],
                &mut server,
                &mut client,
            ),
            RespFrame::error_str("ERR LIMIT can't be negative")
        );
        assert_eq!(
            run(
                &["SINTERCARD", "2", "s1", "s2", "LIMIT", "x"],
                &mut server,
                &mut client,
            ),
            RespFrame::error_str("ERR value is not an integer or out of range")
        );
        assert_eq!(
            run(
                &["SINTERCARD", "2", "s1", "s2", "NOPE", "1"],
                &mut server,
                &mut client,
            ),
            RespFrame::error_str("ERR syntax error")
        );

        assert_eq!(
            run(&["SET", "k", "v"], &mut server, &mut client),
            RespFrame::ok()
        );
        assert_eq!(
            run(&["SINTERCARD", "2", "s1", "k"], &mut server, &mut client),
            RespFrame::error_str(
                "WRONGTYPE Operation against a key holding the wrong kind of value"
            )
        );
    }

    #[test]
    fn m2_set_core_commands() {
        let mut server = ServerState::with_default_dbs();
        let mut client = ClientState::default();

        assert_eq!(
            run(&["SADD", "s", "a", "b", "c"], &mut server, &mut client),
            RespFrame::Integer(3)
        );
        assert_eq!(
            run(&["SADD", "s", "b", "d"], &mut server, &mut client),
            RespFrame::Integer(1)
        );
        assert_eq!(
            run(&["SISMEMBER", "s", "b"], &mut server, &mut client),
            RespFrame::Integer(1)
        );
        assert_eq!(
            run(&["SCARD", "s"], &mut server, &mut client),
            RespFrame::Integer(4)
        );
        assert_array_set_eq(
            &run(&["SMEMBERS", "s"], &mut server, &mut client),
            &["a", "b", "c", "d"],
        );

        assert_eq!(
            run(&["SREM", "s", "b", "x"], &mut server, &mut client),
            RespFrame::Integer(1)
        );
        assert_eq!(
            run(&["SISMEMBER", "s", "b"], &mut server, &mut client),
            RespFrame::Integer(0)
        );
        assert_eq!(
            run(&["SCARD", "s"], &mut server, &mut client),
            RespFrame::Integer(3)
        );

        assert_eq!(
            run(&["SREM", "s", "a", "c", "d"], &mut server, &mut client),
            RespFrame::Integer(3)
        );
        assert_eq!(
            run(&["EXISTS", "s"], &mut server, &mut client),
            RespFrame::Integer(0)
        );

        assert_eq!(
            run(&["SREM", "missing", "x"], &mut server, &mut client),
            RespFrame::Integer(0)
        );
        assert_eq!(
            run(&["SMEMBERS", "missing"], &mut server, &mut client),
            RespFrame::Array(vec![])
        );

        assert_eq!(
            run(&["SADD", "dup", "x", "x"], &mut server, &mut client),
            RespFrame::Integer(1)
        );
    }

    #[test]
    fn m2_set_wrongtype_and_scan_type() {
        let mut server = ServerState::with_default_dbs();
        let mut client = ClientState::default();

        assert_eq!(
            run(&["SET", "k", "v"], &mut server, &mut client),
            RespFrame::ok()
        );
        assert_eq!(
            run(&["SADD", "k", "x"], &mut server, &mut client),
            RespFrame::error_str(
                "WRONGTYPE Operation against a key holding the wrong kind of value"
            )
        );
        assert_eq!(
            run(&["SMOVE", "k", "dst", "x"], &mut server, &mut client),
            RespFrame::error_str(
                "WRONGTYPE Operation against a key holding the wrong kind of value"
            )
        );

        assert_eq!(
            run(&["LPUSH", "l", "x"], &mut server, &mut client),
            RespFrame::Integer(1)
        );
        assert_eq!(
            run(&["SADD", "l", "y"], &mut server, &mut client),
            RespFrame::error_str(
                "WRONGTYPE Operation against a key holding the wrong kind of value"
            )
        );

        assert_eq!(
            run(&["SADD", "s", "v1", "v2"], &mut server, &mut client),
            RespFrame::Integer(2)
        );
        assert_eq!(
            run(&["TYPE", "s"], &mut server, &mut client),
            RespFrame::simple_str("set")
        );
        assert_eq!(
            run(&["OBJECT", "ENCODING", "s"], &mut server, &mut client),
            RespFrame::bulk_str("hashtable")
        );
        assert_eq!(
            run(&["GET", "s"], &mut server, &mut client),
            RespFrame::error_str(
                "WRONGTYPE Operation against a key holding the wrong kind of value"
            )
        );

        assert_eq!(
            run(
                &["SCAN", "0", "TYPE", "set", "COUNT", "100"],
                &mut server,
                &mut client
            ),
            RespFrame::Array(vec![
                RespFrame::bulk_str("0"),
                RespFrame::Array(vec![RespFrame::bulk_str("s")]),
            ])
        );
    }

    #[test]
    fn m2_scan_family_hscan_baseline() {
        let mut server = ServerState::with_default_dbs();
        let mut client = ClientState::default();

        assert_eq!(
            run(
                &["HSET", "h", "f1", "v1", "f2", "v2", "f3", "v3"],
                &mut server,
                &mut client
            ),
            RespFrame::Integer(3)
        );

        assert_eq!(
            run(&["HSCAN", "h", "0", "COUNT", "2"], &mut server, &mut client),
            RespFrame::Array(vec![
                RespFrame::bulk_str("2"),
                RespFrame::Array(vec![
                    RespFrame::bulk_str("f1"),
                    RespFrame::bulk_str("v1"),
                    RespFrame::bulk_str("f2"),
                    RespFrame::bulk_str("v2"),
                ]),
            ])
        );
        assert_eq!(
            run(&["HSCAN", "h", "2", "COUNT", "2"], &mut server, &mut client),
            RespFrame::Array(vec![
                RespFrame::bulk_str("0"),
                RespFrame::Array(vec![RespFrame::bulk_str("f3"), RespFrame::bulk_str("v3")]),
            ])
        );

        assert_eq!(
            run(
                &["HSCAN", "h", "0", "MATCH", "f2", "COUNT", "10"],
                &mut server,
                &mut client
            ),
            RespFrame::Array(vec![
                RespFrame::bulk_str("0"),
                RespFrame::Array(vec![RespFrame::bulk_str("f2"), RespFrame::bulk_str("v2")]),
            ])
        );

        assert_eq!(
            run(&["HSCAN", "missing", "0"], &mut server, &mut client),
            RespFrame::Array(vec![RespFrame::bulk_str("0"), RespFrame::Array(vec![])])
        );

        assert_eq!(
            run(&["HSCAN", "h", "x"], &mut server, &mut client),
            RespFrame::error_str("ERR invalid cursor")
        );

        assert_eq!(
            run(&["SET", "s", "v"], &mut server, &mut client),
            RespFrame::ok()
        );
        assert_eq!(
            run(&["HSCAN", "s", "0"], &mut server, &mut client),
            RespFrame::error_str(
                "WRONGTYPE Operation against a key holding the wrong kind of value"
            )
        );
    }

    #[test]
    fn m2_scan_family_sscan_baseline() {
        let mut server = ServerState::with_default_dbs();
        let mut client = ClientState::default();

        assert_eq!(
            run(&["SADD", "s", "a", "b", "c", "d"], &mut server, &mut client),
            RespFrame::Integer(4)
        );

        assert_eq!(
            run(&["SSCAN", "s", "0", "COUNT", "3"], &mut server, &mut client),
            RespFrame::Array(vec![
                RespFrame::bulk_str("3"),
                RespFrame::Array(vec![
                    RespFrame::bulk_str("a"),
                    RespFrame::bulk_str("b"),
                    RespFrame::bulk_str("c"),
                ]),
            ])
        );
        assert_eq!(
            run(&["SSCAN", "s", "3", "COUNT", "3"], &mut server, &mut client),
            RespFrame::Array(vec![
                RespFrame::bulk_str("0"),
                RespFrame::Array(vec![RespFrame::bulk_str("d")]),
            ])
        );

        assert_eq!(
            run(
                &["SSCAN", "s", "0", "MATCH", "b*", "COUNT", "10"],
                &mut server,
                &mut client
            ),
            RespFrame::Array(vec![
                RespFrame::bulk_str("0"),
                RespFrame::Array(vec![RespFrame::bulk_str("b")]),
            ])
        );

        assert_eq!(
            run(&["SSCAN", "missing", "0"], &mut server, &mut client),
            RespFrame::Array(vec![RespFrame::bulk_str("0"), RespFrame::Array(vec![])])
        );

        assert_eq!(
            run(&["SSCAN", "s", "x"], &mut server, &mut client),
            RespFrame::error_str("ERR invalid cursor")
        );

        assert_eq!(
            run(&["SET", "k", "v"], &mut server, &mut client),
            RespFrame::ok()
        );
        assert_eq!(
            run(&["SSCAN", "k", "0"], &mut server, &mut client),
            RespFrame::error_str(
                "WRONGTYPE Operation against a key holding the wrong kind of value"
            )
        );
    }

    #[test]
    fn m2_scan_family_cursor_progress_with_match_filters() {
        let mut server = ServerState::with_default_dbs();
        let mut client = ClientState::default();

        assert_eq!(
            run(&["SET", "ka", "1"], &mut server, &mut client),
            RespFrame::ok()
        );
        assert_eq!(
            run(&["SET", "kb", "1"], &mut server, &mut client),
            RespFrame::ok()
        );
        assert_eq!(
            run(&["SADD", "kz", "v"], &mut server, &mut client),
            RespFrame::Integer(1)
        );

        assert_eq!(
            run(
                &["SCAN", "0", "MATCH", "kz*", "COUNT", "1"],
                &mut server,
                &mut client
            ),
            RespFrame::Array(vec![RespFrame::bulk_str("1"), RespFrame::Array(vec![])])
        );
        assert_eq!(
            run(
                &["SCAN", "1", "MATCH", "kz*", "COUNT", "1"],
                &mut server,
                &mut client
            ),
            RespFrame::Array(vec![RespFrame::bulk_str("2"), RespFrame::Array(vec![])])
        );
        assert_eq!(
            run(
                &["SCAN", "2", "MATCH", "kz*", "COUNT", "1"],
                &mut server,
                &mut client
            ),
            RespFrame::Array(vec![
                RespFrame::bulk_str("0"),
                RespFrame::Array(vec![RespFrame::bulk_str("kz")]),
            ])
        );

        assert_eq!(
            run(
                &["HSET", "hh", "a1", "1", "a2", "2", "b1", "3"],
                &mut server,
                &mut client
            ),
            RespFrame::Integer(3)
        );

        assert_eq!(
            run(
                &["HSCAN", "hh", "0", "MATCH", "b*", "COUNT", "1"],
                &mut server,
                &mut client
            ),
            RespFrame::Array(vec![RespFrame::bulk_str("1"), RespFrame::Array(vec![])])
        );
        assert_eq!(
            run(
                &["HSCAN", "hh", "1", "MATCH", "b*", "COUNT", "1"],
                &mut server,
                &mut client
            ),
            RespFrame::Array(vec![RespFrame::bulk_str("2"), RespFrame::Array(vec![])])
        );
        assert_eq!(
            run(
                &["HSCAN", "hh", "2", "MATCH", "b*", "COUNT", "1"],
                &mut server,
                &mut client
            ),
            RespFrame::Array(vec![
                RespFrame::bulk_str("0"),
                RespFrame::Array(vec![RespFrame::bulk_str("b1"), RespFrame::bulk_str("3")]),
            ])
        );

        assert_eq!(
            run(&["SADD", "ss", "a", "b", "c"], &mut server, &mut client),
            RespFrame::Integer(3)
        );

        assert_eq!(
            run(
                &["SSCAN", "ss", "0", "MATCH", "c*", "COUNT", "1"],
                &mut server,
                &mut client
            ),
            RespFrame::Array(vec![RespFrame::bulk_str("1"), RespFrame::Array(vec![])])
        );
        assert_eq!(
            run(
                &["SSCAN", "ss", "1", "MATCH", "c*", "COUNT", "1"],
                &mut server,
                &mut client
            ),
            RespFrame::Array(vec![RespFrame::bulk_str("2"), RespFrame::Array(vec![])])
        );
        assert_eq!(
            run(
                &["SSCAN", "ss", "2", "MATCH", "c*", "COUNT", "1"],
                &mut server,
                &mut client
            ),
            RespFrame::Array(vec![
                RespFrame::bulk_str("0"),
                RespFrame::Array(vec![RespFrame::bulk_str("c")]),
            ])
        );
    }

    #[test]
    fn m3_stream_block_retry_resumes_with_new_entries() {
        let mut server = ServerState::with_default_dbs();
        let mut client = ClientState::default();

        // Prime stream so XREAD BLOCK with '$' can normalize to a concrete ID.
        let first = run(&["XADD", "s1", "*", "f", "v1"], &mut server, &mut client);
        let RespFrame::BulkString(Some(_first_id)) = first else {
            panic!("XADD should return generated id");
        };

        let blocked = run_full(
            &["XREAD", "BLOCK", "10", "STREAMS", "s1", "$"],
            &mut server,
            &mut client,
        );
        assert_eq!(blocked.response, RespFrame::Null);
        let retry = blocked.retry_blocking.expect("XREAD BLOCK should retry");
        assert!(retry.deadline_ms.is_some());

        let RespFrame::Array(retry_parts) = &retry.frame else {
            panic!("retry frame should be array");
        };
        let RespFrame::BulkString(Some(last_id_arg)) = &retry_parts[retry_parts.len() - 1] else {
            panic!("retry frame last arg should be stream id");
        };
        assert_ne!(last_id_arg.as_ref(), b"$");

        let second = run(&["XADD", "s1", "*", "f", "v2"], &mut server, &mut client);
        let RespFrame::BulkString(Some(second_id)) = second else {
            panic!("XADD should return generated id");
        };

        let resumed = {
            let mut access = ServerAccess::new_inline(&mut server);
            execute(retry.frame, &mut access, &mut client).response
        };
        let RespFrame::Array(rows) = resumed else {
            panic!("resumed XREAD should return array");
        };
        let RespFrame::Array(stream_row) = &rows[0] else {
            panic!("XREAD stream row should be array");
        };
        let RespFrame::Array(entries) = &stream_row[1] else {
            panic!("XREAD entries should be array");
        };
        let RespFrame::Array(entry) = &entries[0] else {
            panic!("XREAD entry should be array");
        };
        let RespFrame::BulkString(Some(read_id)) = &entry[0] else {
            panic!("XREAD entry id should be bulk string");
        };
        assert_eq!(read_id, &second_id);

        // XREADGROUP BLOCK should also produce retry and then resume.
        let primed = run(&["XADD", "s2", "*", "f", "v0"], &mut server, &mut client);
        let RespFrame::BulkString(Some(_)) = primed else {
            panic!("XADD should return generated id");
        };
        assert_eq!(
            run(
                &["XGROUP", "CREATE", "s2", "g1", "$"],
                &mut server,
                &mut client
            ),
            RespFrame::ok()
        );

        let blocked_group = run_full(
            &[
                "XREADGROUP",
                "GROUP",
                "g1",
                "c1",
                "BLOCK",
                "10",
                "STREAMS",
                "s2",
                ">",
            ],
            &mut server,
            &mut client,
        );
        assert_eq!(blocked_group.response, RespFrame::Null);
        let retry_group = blocked_group
            .retry_blocking
            .expect("XREADGROUP BLOCK should retry");
        assert!(retry_group.deadline_ms.is_some());

        let third = run(&["XADD", "s2", "*", "f", "v3"], &mut server, &mut client);
        let RespFrame::BulkString(Some(third_id)) = third else {
            panic!("XADD should return generated id");
        };

        let resumed_group = {
            let mut access = ServerAccess::new_inline(&mut server);
            execute(retry_group.frame, &mut access, &mut client).response
        };
        let RespFrame::Array(group_rows) = resumed_group else {
            panic!("resumed XREADGROUP should return array");
        };
        let RespFrame::Array(group_stream_row) = &group_rows[0] else {
            panic!("XREADGROUP stream row should be array");
        };
        let RespFrame::Array(group_entries) = &group_stream_row[1] else {
            panic!("XREADGROUP entries should be array");
        };
        let RespFrame::Array(group_entry) = &group_entries[0] else {
            panic!("XREADGROUP entry should be array");
        };
        let RespFrame::BulkString(Some(group_read_id)) = &group_entry[0] else {
            panic!("XREADGROUP entry id should be bulk string");
        };
        assert_eq!(group_read_id, &third_id);
    }

    #[test]
    fn m2_count_limit_guards_return_errors() {
        let mut server = ServerState::with_default_dbs();
        let mut client = ClientState::default();

        assert_eq!(
            run(&["LPOP", "k", "100001"], &mut server, &mut client),
            RespFrame::error_str("ERR count is out of range")
        );
        assert_eq!(
            run(&["SPOP", "k", "100001"], &mut server, &mut client),
            RespFrame::error_str("ERR count is out of range")
        );
        assert_eq!(
            run(&["ZMPOP", "10001", "k", "MIN"], &mut server, &mut client),
            RespFrame::error_str("ERR numkeys is out of range")
        );
        assert_eq!(
            run(&["LMPOP", "10001", "k", "LEFT"], &mut server, &mut client),
            RespFrame::error_str("ERR numkeys is out of range")
        );
    }

    #[test]
    fn auth_is_required_after_default_password_is_set() {
        let mut server = ServerState::with_default_dbs();
        let mut admin = ClientState::default();

        assert_eq!(
            run(
                &[
                    "ACL",
                    "SETUSER",
                    "default",
                    "resetpass",
                    ">secret",
                    "+@all",
                    "on"
                ],
                &mut server,
                &mut admin,
            ),
            RespFrame::ok()
        );

        let mut unauth = ClientState::new(9);
        assert_eq!(
            run(&["PING"], &mut server, &mut unauth),
            RespFrame::error_str("NOAUTH Authentication required.")
        );
        assert_eq!(
            run(&["AUTH", "wrong"], &mut server, &mut unauth),
            RespFrame::error_str("ERR invalid username-password pair or user is disabled.")
        );
        assert_eq!(
            run(&["AUTH", "secret"], &mut server, &mut unauth),
            RespFrame::ok()
        );
        assert_eq!(run(&["PING"], &mut server, &mut unauth), RespFrame::pong());
    }

    #[test]
    fn slowlog_redacts_auth_password_arguments() {
        let mut server = ServerState::with_default_dbs();
        let mut client = ClientState::default();

        assert_eq!(
            run(
                &["CONFIG", "SET", "slowlog-log-slower-than", "0"],
                &mut server,
                &mut client,
            ),
            RespFrame::ok()
        );
        assert_eq!(
            run(
                &[
                    "ACL",
                    "SETUSER",
                    "default",
                    "resetpass",
                    ">secret",
                    "+@all",
                    "on"
                ],
                &mut server,
                &mut client,
            ),
            RespFrame::ok()
        );

        let _ = run(&["AUTH", "secret"], &mut server, &mut client);

        let log = run(&["SLOWLOG", "GET", "20"], &mut server, &mut client);
        let RespFrame::Array(rows) = log else {
            panic!("SLOWLOG GET should return array");
        };

        let mut found_auth = false;
        for row in rows {
            let RespFrame::Array(parts) = row else {
                continue;
            };
            let Some(RespFrame::Array(argv)) = parts.get(3) else {
                continue;
            };
            let Some(RespFrame::BulkString(Some(command))) = argv.first() else {
                continue;
            };
            if !command.eq_ignore_ascii_case(b"AUTH") {
                continue;
            }

            found_auth = true;
            let Some(RespFrame::BulkString(Some(password_arg))) = argv.last() else {
                panic!("AUTH slowlog row should include password argument");
            };
            assert_eq!(password_arg, &Bytes::from_static(b"[REDACTED]"));
        }

        assert!(found_auth);
    }

    #[test]
    fn info_stats_contains_real_counters() {
        let mut server = ServerState::with_default_dbs();
        let mut client = ClientState::new(1);

        run(&["SET", "a", "1"], &mut server, &mut client);
        run(&["SET", "b", "2"], &mut server, &mut client);
        run(&["GET", "a"], &mut server, &mut client);
        run(&["GET", "missing"], &mut server, &mut client);

        let reply = run(&["INFO", "stats"], &mut server, &mut client);
        let RespFrame::BulkString(Some(body)) = &reply else {
            panic!("expected BulkString, got {reply:?}");
        };
        let text = std::str::from_utf8(body).expect("valid utf8");

        assert!(
            text.contains("total_commands_processed:5"),
            "expected 5 processed commands (2 SET + 2 GET + 1 INFO), got: {text}"
        );
        assert!(
            text.contains("keyspace_hits:1"),
            "expected 1 hit from GET a, got: {text}"
        );
        assert!(
            text.contains("keyspace_misses:1"),
            "expected 1 miss from GET missing, got: {text}"
        );
        assert!(
            !text.contains("instantaneous_ops_per_sec:0\r\n")
                || text.contains("instantaneous_ops_per_sec:0"),
            "ops/sec field present: {text}"
        );
        assert!(
            text.contains("evicted_keys:0"),
            "expected evicted_keys:0, got: {text}"
        );
        assert!(
            text.contains("expired_keys:0"),
            "expected expired_keys:0, got: {text}"
        );
    }

    #[test]
    fn info_clients_shows_zero_connected_for_engine_tests() {
        let mut server = ServerState::with_default_dbs();
        let mut client = ClientState::new(1);

        let reply = run(&["INFO", "clients"], &mut server, &mut client);
        let RespFrame::BulkString(Some(body)) = &reply else {
            panic!("expected BulkString, got {reply:?}");
        };
        let text = std::str::from_utf8(body).expect("valid utf8");

        assert!(
            text.contains("connected_clients:0"),
            "engine-level test has no real connections: {text}"
        );
    }

    #[test]
    fn info_stats_merges_atomic_hot_counters_when_inner_lags() {
        let mut server = ServerState::with_default_dbs();
        let atomic_stats = AtomicStatsState::default();
        let mut client = ClientState::new(1);

        atomic_stats.mark_command_processed();
        atomic_stats.mark_client_connected();
        atomic_stats.add_net_input_bytes(123);
        atomic_stats.add_net_output_bytes(456);

        let reply = run_with_atomic(&["INFO", "stats"], &mut server, &atomic_stats, &mut client);
        let RespFrame::BulkString(Some(body)) = &reply else {
            panic!("expected BulkString, got {reply:?}");
        };
        let text = std::str::from_utf8(body).expect("valid utf8");

        assert!(
            text.contains("total_commands_processed:2"),
            "expected atomic + INFO command count, got: {text}"
        );
        assert!(
            text.contains("total_connections_received:1"),
            "expected atomic connection count, got: {text}"
        );
        assert!(
            text.contains("total_net_input_bytes:123"),
            "expected atomic input bytes, got: {text}"
        );
        assert!(
            text.contains("total_net_output_bytes:456"),
            "expected atomic output bytes, got: {text}"
        );
    }

    #[test]
    fn ping_health_merges_atomic_snapshot_for_visibility() {
        let mut server = ServerState::with_default_dbs();
        server.config.set_maxmemory(1024);

        let atomic_stats = AtomicStatsState::default();
        let mut client = ClientState::new(1);

        atomic_stats.mark_client_connected();
        atomic_stats.set_cached_memory_estimate(2048);

        let reply = run_with_atomic(&["PING", "HEALTH"], &mut server, &atomic_stats, &mut client);
        let RespFrame::BulkString(Some(body)) = &reply else {
            panic!("expected BulkString, got {reply:?}");
        };
        let text = std::str::from_utf8(body).expect("valid utf8");

        assert!(text.contains("status:unhealthy"), "{text}");
        assert!(text.contains("connected_clients:1"), "{text}");
        assert!(text.contains("memory_used_bytes:2048"), "{text}");
        assert!(text.contains("memory_status:critical"), "{text}");
    }

    #[test]
    fn config_resetstat_resets_atomic_hot_counters_too() {
        let mut server = ServerState::with_default_dbs();
        let atomic_stats = AtomicStatsState::default();
        let mut client = ClientState::new(1);

        atomic_stats.mark_command_processed();
        atomic_stats.mark_client_connected();
        atomic_stats.add_net_input_bytes(50);
        atomic_stats.add_net_output_bytes(25);

        assert_eq!(
            run_with_atomic(
                &["CONFIG", "RESETSTAT"],
                &mut server,
                &atomic_stats,
                &mut client
            ),
            RespFrame::ok()
        );

        assert_eq!(server.stats.total_commands_processed(), 0);
        assert_eq!(server.stats.total_connections_received(), 0);
        assert_eq!(server.stats.total_net_input_bytes(), 0);
        assert_eq!(server.stats.total_net_output_bytes(), 0);
        assert_eq!(atomic_stats.total_commands_processed(), 0);
        assert_eq!(atomic_stats.total_connections_received(), 0);
        assert_eq!(atomic_stats.total_net_input_bytes(), 0);
        assert_eq!(atomic_stats.total_net_output_bytes(), 0);
    }

    #[test]
    fn info_keyspace_shows_db_with_keys() {
        let mut server = ServerState::with_default_dbs();
        let mut client = ClientState::new(1);

        run(&["SET", "x", "1"], &mut server, &mut client);
        run(&["SET", "y", "2"], &mut server, &mut client);

        let reply = run(&["INFO", "keyspace"], &mut server, &mut client);
        let RespFrame::BulkString(Some(body)) = &reply else {
            panic!("expected BulkString, got {reply:?}");
        };
        let text = std::str::from_utf8(body).expect("valid utf8");

        assert!(
            text.contains("db0:keys=2,"),
            "expected db0 with 2 keys: {text}"
        );
    }

    #[test]
    fn info_persistence_shows_last_save() {
        let mut server = ServerState::with_default_dbs();
        let mut client = ClientState::new(1);

        let reply = run(&["INFO", "persistence"], &mut server, &mut client);
        let RespFrame::BulkString(Some(body)) = &reply else {
            panic!("expected BulkString, got {reply:?}");
        };
        let text = std::str::from_utf8(body).expect("valid utf8");

        assert!(
            text.contains("rdb_last_save_time:"),
            "expected rdb_last_save_time field: {text}"
        );
        assert!(
            text.contains("rdb_last_bgsave_status:ok"),
            "expected rdb_last_bgsave_status:ok: {text}"
        );
    }

    #[test]
    fn info_persistence_reports_real_aof_file_sizes() {
        let mut server = ServerState::with_default_dbs();
        let mut client = ClientState::new(1);

        let current_path = unique_test_path("aof-current");
        let base_path = unique_test_path("aof-base");

        fs::write(&current_path, b"123456789").expect("write current aof");
        fs::write(&base_path, b"1234").expect("write base aof");

        server.set_aof_enabled(true);
        server.set_aof_current_path(Some(current_path.clone()));
        server.set_aof_base_path(Some(base_path.clone()));

        let reply = run(&["INFO", "persistence"], &mut server, &mut client);
        let RespFrame::BulkString(Some(body)) = &reply else {
            panic!("expected BulkString, got {reply:?}");
        };
        let text = std::str::from_utf8(body).expect("valid utf8");

        assert!(
            text.contains("aof_current_size:9"),
            "expected aof_current_size to reflect file size: {text}"
        );
        assert!(
            text.contains("aof_base_size:4"),
            "expected aof_base_size to reflect file size: {text}"
        );
        assert!(
            text.contains("audit_chain_dirty:"),
            "expected audit_chain_dirty field in INFO persistence: {text}"
        );
        assert!(
            text.contains("audit_recovery_status:"),
            "expected audit_recovery_status field in INFO persistence: {text}"
        );

        let _ = fs::remove_file(&current_path);
        let _ = fs::remove_file(&base_path);
    }

    #[test]
    fn ping_health_reports_audit_status_fields() {
        let mut server = ServerState::with_default_dbs();
        let mut client = ClientState::new(1);

        let reply = run(&["PING", "HEALTH"], &mut server, &mut client);
        let RespFrame::BulkString(Some(body)) = &reply else {
            panic!("expected BulkString, got {reply:?}");
        };
        let text = std::str::from_utf8(body).expect("valid utf8");

        assert!(
            text.contains("|audit_chain_dirty:"),
            "expected audit_chain_dirty in health report: {text}"
        );
        assert!(
            text.contains("|audit_recovery_status:"),
            "expected audit_recovery_status in health report: {text}"
        );
    }

    #[test]
    fn info_server_reports_unhealthy_when_aof_is_latched() {
        let mut server = ServerState::with_default_dbs();
        let mut client = ClientState::new(1);
        server.set_aof_enabled(true);
        server.set_aof_last_error("disk full");

        let reply = run(&["INFO", "SERVER"], &mut server, &mut client);
        let RespFrame::BulkString(Some(body)) = &reply else {
            panic!("expected BulkString, got {reply:?}");
        };
        let text = std::str::from_utf8(body).expect("valid utf8");
        assert!(
            text.contains("health_status:unhealthy"),
            "expected unhealthy server status, got {text}"
        );
    }

    #[test]
    fn ping_health_reports_unhealthy_when_aof_is_latched() {
        let mut server = ServerState::with_default_dbs();
        let mut client = ClientState::new(1);
        server.set_aof_enabled(true);
        server.set_aof_last_error("disk full");

        let reply = run(&["PING", "HEALTH"], &mut server, &mut client);
        let RespFrame::BulkString(Some(body)) = &reply else {
            panic!("expected BulkString, got {reply:?}");
        };
        let text = std::str::from_utf8(body).expect("valid utf8");
        assert!(
            text.contains("status:unhealthy|"),
            "expected unhealthy health report, got {text}"
        );
        assert!(
            text.contains("|aof_write_latched:true|"),
            "expected aof_write_latched in health report, got {text}"
        );
        assert!(
            text.contains("|reasons:AOF writes are latched after an I/O error: disk full"),
            "expected human-readable latch reason in health report, got {text}"
        );
    }

    #[test]
    fn ping_health_reports_no_reasons_when_healthy() {
        let mut server = ServerState::with_default_dbs();
        let mut client = ClientState::new(1);

        let reply = run(&["PING", "HEALTH"], &mut server, &mut client);
        let RespFrame::BulkString(Some(body)) = &reply else {
            panic!("expected BulkString, got {reply:?}");
        };
        let text = std::str::from_utf8(body).expect("valid utf8");
        assert!(
            text.contains("status:healthy|"),
            "expected healthy status, got {text}"
        );
        assert!(
            text.contains("|reasons:none|"),
            "expected reasons:none on a healthy server, got {text}"
        );
    }

    #[test]
    fn info_memory_section_reports_used_and_maxmemory() {
        let mut server = ServerState::with_default_dbs();
        let mut client = ClientState::new(1);

        let reply = run(&["INFO", "MEMORY"], &mut server, &mut client);
        let RespFrame::BulkString(Some(body)) = &reply else {
            panic!("expected BulkString, got {reply:?}");
        };
        let text = std::str::from_utf8(body).expect("valid utf8");
        assert!(
            text.contains("# Memory"),
            "missing # Memory section: {text}"
        );
        assert!(text.contains("used_memory:"), "missing used_memory: {text}");
        assert!(
            text.contains("used_memory_human:"),
            "missing used_memory_human: {text}"
        );
        assert!(text.contains("maxmemory:"), "missing maxmemory: {text}");
        assert!(
            text.contains("maxmemory_policy:noeviction"),
            "missing maxmemory_policy: {text}"
        );
        assert!(
            text.contains("mem_estimate_age_ticks:"),
            "missing mem_estimate_age_ticks: {text}"
        );
    }

    #[test]
    fn client_tracking_bcast_prefix_invalidates_matching_keys() {
        let mut server = ServerState::with_default_dbs();
        let mut tracker = ClientState::new(1);
        let mut writer = ClientState::new(2);
        let mut rx1 = server.pubsub.register_client(1);

        assert_eq!(
            run(
                &["CLIENT", "TRACKING", "ON", "BCAST", "PREFIX", "user:"],
                &mut server,
                &mut tracker
            ),
            RespFrame::ok()
        );

        let tracking_info = run(&["CLIENT", "TRACKINGINFO"], &mut server, &mut tracker);
        let RespFrame::Map(entries) = tracking_info else {
            panic!("CLIENT TRACKINGINFO should return map");
        };
        assert!(entries.iter().any(|(key, value)| {
            *key == RespFrame::bulk_str("flags")
                && matches!(
                    value,
                    RespFrame::Array(flags)
                        if flags.contains(&RespFrame::bulk_str("on"))
                            && flags.contains(&RespFrame::bulk_str("bcast"))
                )
        }));
        assert!(entries.iter().any(|(key, value)| {
            *key == RespFrame::bulk_str("prefixes")
                && *value == RespFrame::Array(vec![RespFrame::bulk_str("user:")])
        }));

        assert_eq!(
            run(&["SET", "user:1", "v1"], &mut server, &mut writer),
            RespFrame::ok()
        );
        assert_eq!(
            run(&["SET", "other:1", "v1"], &mut server, &mut writer),
            RespFrame::ok()
        );

        let pending_tracking = PubSubState::drain_rx(&mut rx1);
        assert_eq!(pending_tracking.len(), 1);
        match &pending_tracking[0] {
            PubSubMessage::Invalidate { keys } => {
                assert_eq!(keys, &vec![Bytes::from_static(b"user:1")]);
            }
            other => panic!("expected invalidate push, got {other:?}"),
        }
    }

    #[test]
    fn client_tracking_noloop_suppresses_self_invalidations() {
        let mut server = ServerState::with_default_dbs();
        let mut tracker = ClientState::new(7);
        let mut writer = ClientState::new(8);
        let mut rx7 = server.pubsub.register_client(7);

        assert_eq!(
            run(&["SET", "tracked-key", "v1"], &mut server, &mut writer),
            RespFrame::ok()
        );
        assert_eq!(
            run(
                &["CLIENT", "TRACKING", "ON", "NOLOOP"],
                &mut server,
                &mut tracker
            ),
            RespFrame::ok()
        );
        assert_eq!(
            run(&["GET", "tracked-key"], &mut server, &mut tracker),
            RespFrame::bulk_str("v1")
        );
        assert_eq!(
            run(&["SET", "tracked-key", "v2"], &mut server, &mut tracker),
            RespFrame::ok()
        );

        let pending_tracking = PubSubState::drain_rx(&mut rx7);
        assert!(pending_tracking.is_empty(), "{pending_tracking:?}");
    }

    #[test]
    fn client_tracking_optin_requires_client_caching_yes() {
        let mut server = ServerState::with_default_dbs();
        let mut tracker = ClientState::new(11);
        let mut writer = ClientState::new(12);
        let mut rx11 = server.pubsub.register_client(11);

        assert_eq!(
            run(&["SET", "tracked-key", "v1"], &mut server, &mut writer),
            RespFrame::ok()
        );
        assert_eq!(
            run(
                &["CLIENT", "TRACKING", "ON", "OPTIN"],
                &mut server,
                &mut tracker
            ),
            RespFrame::ok()
        );
        assert_eq!(
            run(&["GET", "tracked-key"], &mut server, &mut tracker),
            RespFrame::bulk_str("v1")
        );
        assert_eq!(
            run(&["SET", "tracked-key", "v2"], &mut server, &mut writer),
            RespFrame::ok()
        );
        assert!(PubSubState::drain_rx(&mut rx11).is_empty());

        assert_eq!(
            run(&["CLIENT", "CACHING", "YES"], &mut server, &mut tracker),
            RespFrame::ok()
        );
        assert_eq!(
            run(&["GET", "tracked-key"], &mut server, &mut tracker),
            RespFrame::bulk_str("v2")
        );
        assert_eq!(
            run(&["SET", "tracked-key", "v3"], &mut server, &mut writer),
            RespFrame::ok()
        );

        let pending_tracking = PubSubState::drain_rx(&mut rx11);
        assert_eq!(pending_tracking.len(), 1);
        match &pending_tracking[0] {
            PubSubMessage::Invalidate { keys } => {
                assert_eq!(keys, &vec![Bytes::from_static(b"tracked-key")]);
            }
            other => panic!("expected invalidate push, got {other:?}"),
        }
    }

    #[test]
    fn client_tracking_optout_respects_client_caching_no() {
        let mut server = ServerState::with_default_dbs();
        let mut tracker = ClientState::new(21);
        let mut writer = ClientState::new(22);
        let mut rx21 = server.pubsub.register_client(21);

        assert_eq!(
            run(&["SET", "tracked-key", "v1"], &mut server, &mut writer),
            RespFrame::ok()
        );
        assert_eq!(
            run(
                &["CLIENT", "TRACKING", "ON", "OPTOUT"],
                &mut server,
                &mut tracker
            ),
            RespFrame::ok()
        );
        assert_eq!(
            run(&["GET", "tracked-key"], &mut server, &mut tracker),
            RespFrame::bulk_str("v1")
        );
        assert_eq!(
            run(&["SET", "tracked-key", "v2"], &mut server, &mut writer),
            RespFrame::ok()
        );
        assert_eq!(PubSubState::drain_rx(&mut rx21).len(), 1);

        assert_eq!(
            run(&["CLIENT", "CACHING", "NO"], &mut server, &mut tracker),
            RespFrame::ok()
        );
        assert_eq!(
            run(&["GET", "tracked-key"], &mut server, &mut tracker),
            RespFrame::bulk_str("v2")
        );
        assert_eq!(
            run(&["SET", "tracked-key", "v3"], &mut server, &mut writer),
            RespFrame::ok()
        );
        assert!(PubSubState::drain_rx(&mut rx21).is_empty());
    }

    #[test]
    fn client_tracking_redirect_requires_connected_target() {
        let mut server = ServerState::with_default_dbs();
        let mut tracker = ClientState::new(31);

        assert_eq!(
            run(
                &["CLIENT", "TRACKING", "ON", "REDIRECT", "999"],
                &mut server,
                &mut tracker
            ),
            RespFrame::error_str("ERR CLIENT TRACKING REDIRECT target client is not connected")
        );
    }

    #[test]
    fn client_tracking_getredir_reports_self_redirection_as_zero() {
        let mut server = ServerState::with_default_dbs();
        let mut tracker = ClientState::new(35);

        assert_eq!(
            run(&["CLIENT", "TRACKING", "ON"], &mut server, &mut tracker),
            RespFrame::ok()
        );
        assert_eq!(
            run(&["CLIENT", "GETREDIR"], &mut server, &mut tracker),
            RespFrame::Integer(0)
        );
    }

    #[test]
    fn client_tracking_redirect_disconnect_marks_broken_redirect_and_falls_back() {
        let mut server = ServerState::with_default_dbs();
        let mut tracker = ClientState::new(41);
        let target = ClientState::new(42);
        let mut writer = ClientState::new(43);
        let mut rx41 = server.pubsub.register_client(41);
        let mut _rx42 = server.pubsub.register_client(42);
        server.upsert_client_snapshot(target.snapshot(
            Bytes::from_static(b"127.0.0.1:42"),
            Bytes::from_static(b"127.0.0.1:6379"),
        ));

        assert_eq!(
            run(
                &["CLIENT", "TRACKING", "ON", "REDIRECT", "42"],
                &mut server,
                &mut tracker
            ),
            RespFrame::ok()
        );
        assert_eq!(
            run(&["SET", "tracked-key", "v1"], &mut server, &mut writer),
            RespFrame::ok()
        );
        assert_eq!(
            run(&["GET", "tracked-key"], &mut server, &mut tracker),
            RespFrame::bulk_str("v1")
        );

        server.pubsub.remove_client(42);
        server.tracking_remove_client(42);
        server.remove_client_snapshot(42);

        assert_eq!(
            run(&["CLIENT", "GETREDIR"], &mut server, &mut tracker),
            RespFrame::Integer(42)
        );
        let tracking_info = run(&["CLIENT", "TRACKINGINFO"], &mut server, &mut tracker);
        let RespFrame::Map(entries) = tracking_info else {
            panic!("CLIENT TRACKINGINFO should return map");
        };
        assert!(entries.iter().any(|(key, value)| {
            *key == RespFrame::bulk_str("redirect") && *value == RespFrame::Integer(42)
        }));
        assert!(entries.iter().any(|(key, value)| {
            *key == RespFrame::bulk_str("flags")
                && matches!(
                    value,
                    RespFrame::Array(flags)
                        if flags.contains(&RespFrame::bulk_str("on"))
                            && flags.contains(&RespFrame::bulk_str("broken_redirect"))
                )
        }));

        assert_eq!(
            run(&["SET", "tracked-key", "v2"], &mut server, &mut writer),
            RespFrame::ok()
        );
        assert_eq!(
            run(&["SET", "tracked-key", "v3"], &mut server, &mut writer),
            RespFrame::ok()
        );
        let tracker_messages = PubSubState::drain_rx(&mut rx41);
        assert_eq!(tracker_messages.len(), 1);
        match &tracker_messages[0] {
            PubSubMessage::Invalidate { keys } => {
                assert_eq!(keys, &vec![Bytes::from_static(b"tracked-key")]);
            }
            other => panic!("expected invalidate push, got {other:?}"),
        }
        // Client 42 was removed, so _rx42 would only contain what was sent before removal.
    }
}
