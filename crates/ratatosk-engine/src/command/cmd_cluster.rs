use bytes::Bytes;

use ratatosk_resp::frame::RespFrame;

use crate::keyspace::ServerState;
use crate::slot;

use super::{ClientState, CommandOutcome, err, now_ms, parse_i64, to_uppercase_bytes, wrong_arity};

// ---------------------------------------------------------------------------
// CLUSTER <subcommand>
// ---------------------------------------------------------------------------

pub(super) fn cmd_cluster(
    args: &[Bytes],
    server: &mut ServerState,
    client: &ClientState,
) -> CommandOutcome {
    if args.is_empty() {
        return wrong_arity("cluster");
    }

    let sub = to_uppercase_bytes(&args[0]);
    match sub.as_slice() {
        b"INFO" => cluster_info(),
        b"MYID" => cluster_myid(server),
        b"KEYSLOT" => cluster_keyslot(&args[1..]),
        b"COUNTKEYSINSLOT" => cluster_countkeysinslot(&args[1..], server, client),
        b"GETKEYSINSLOT" => cluster_getkeysinslot(&args[1..], server, client),
        b"HELP" => cluster_help(),
        _ => cluster_stub(),
    }
}

// ---------------------------------------------------------------------------
// Subcommands
// ---------------------------------------------------------------------------

fn cluster_info() -> CommandOutcome {
    let info = "\
cluster_enabled:0\r\n\
cluster_state:ok\r\n\
cluster_slots_assigned:0\r\n\
cluster_slots_ok:0\r\n\
cluster_slots_pfail:0\r\n\
cluster_slots_fail:0\r\n\
cluster_known_nodes:1\r\n\
cluster_size:0\r\n\
cluster_current_epoch:0\r\n\
cluster_my_epoch:0\r\n\
cluster_stats_messages_sent:0\r\n\
cluster_stats_messages_received:0\r\n\
total_cluster_links_buffer_limit_exceeded:0\r\n";

    CommandOutcome::reply(RespFrame::BulkString(Some(Bytes::copy_from_slice(
        info.as_bytes(),
    ))))
}

fn cluster_myid(server: &ServerState) -> CommandOutcome {
    CommandOutcome::reply(RespFrame::BulkString(Some(server.cluster_node_id.clone())))
}

fn cluster_keyslot(args: &[Bytes]) -> CommandOutcome {
    if args.len() != 1 {
        return wrong_arity("cluster|keyslot");
    }

    let slot = slot::key_hash_slot(&args[0]);
    CommandOutcome::reply(RespFrame::Integer(i64::from(slot)))
}

fn cluster_countkeysinslot(
    args: &[Bytes],
    server: &mut ServerState,
    client: &ClientState,
) -> CommandOutcome {
    if args.len() != 1 {
        return wrong_arity("cluster|countkeysinslot");
    }

    let Some(slot_num) = parse_i64(&args[0]) else {
        return CommandOutcome::reply(err("ERR Invalid or out of range slot"));
    };

    if slot_num < 0 || slot_num >= i64::from(slot::SLOT_COUNT) {
        return CommandOutcome::reply(err("ERR Invalid or out of range slot"));
    }

    let target_slot = slot_num as u16;
    let now = now_ms();
    let db = server.db_mut(client.selected_db);
    crate::keyspace::purge_expired_keys(db, now);

    let count = db
        .iter()
        .filter(|(key, _)| slot::key_hash_slot(key) == target_slot)
        .count();

    CommandOutcome::reply(RespFrame::Integer(count as i64))
}

fn cluster_getkeysinslot(
    args: &[Bytes],
    server: &mut ServerState,
    client: &ClientState,
) -> CommandOutcome {
    if args.len() != 2 {
        return wrong_arity("cluster|getkeysinslot");
    }

    let Some(slot_num) = parse_i64(&args[0]) else {
        return CommandOutcome::reply(err("ERR Invalid or out of range slot"));
    };

    if slot_num < 0 || slot_num >= i64::from(slot::SLOT_COUNT) {
        return CommandOutcome::reply(err("ERR Invalid or out of range slot"));
    }

    let Some(max_count) = parse_i64(&args[1]) else {
        return CommandOutcome::reply(err("ERR value is not an integer or out of range"));
    };

    if max_count < 0 {
        return CommandOutcome::reply(err("ERR value is not an integer or out of range"));
    }

    let target_slot = slot_num as u16;
    let limit = max_count as usize;
    let now = now_ms();
    let db = server.db_mut(client.selected_db);
    crate::keyspace::purge_expired_keys(db, now);

    let keys: Vec<RespFrame> = db
        .iter()
        .filter(|(key, _)| slot::key_hash_slot(key) == target_slot)
        .take(limit)
        .map(|(key, _)| RespFrame::BulkString(Some(key.clone())))
        .collect();

    CommandOutcome::reply(RespFrame::Array(keys))
}

fn cluster_help() -> CommandOutcome {
    let lines: Vec<RespFrame> = vec![
        RespFrame::BulkString(Some(Bytes::from_static(
            b"CLUSTER <subcommand> [<arg> [value] [opt] ...]. Subcommands are:",
        ))),
        RespFrame::BulkString(Some(Bytes::from_static(
            b"Only INFO, MYID, KEYSLOT, COUNTKEYSINSLOT, GETKEYSINSLOT, and HELP are available in standalone mode.",
        ))),
        RespFrame::BulkString(Some(Bytes::from_static(b"COUNTKEYSINSLOT <slot>"))),
        RespFrame::BulkString(Some(Bytes::from_static(
            b"    Return the number of keys in <slot>.",
        ))),
        RespFrame::BulkString(Some(Bytes::from_static(b"GETKEYSINSLOT <slot> <count>"))),
        RespFrame::BulkString(Some(Bytes::from_static(
            b"    Return key names stored by current node in a slot.",
        ))),
        RespFrame::BulkString(Some(Bytes::from_static(b"INFO"))),
        RespFrame::BulkString(Some(Bytes::from_static(
            b"    Return information about the cluster.",
        ))),
        RespFrame::BulkString(Some(Bytes::from_static(b"KEYSLOT <key>"))),
        RespFrame::BulkString(Some(Bytes::from_static(
            b"    Return the hash slot for <key>.",
        ))),
        RespFrame::BulkString(Some(Bytes::from_static(b"MYID"))),
        RespFrame::BulkString(Some(Bytes::from_static(b"    Return the node ID."))),
        RespFrame::BulkString(Some(Bytes::from_static(b"NODES"))),
        RespFrame::BulkString(Some(Bytes::from_static(
            b"    Return cluster configuration of nodes.",
        ))),
        RespFrame::BulkString(Some(Bytes::from_static(b"REPLICAS <node-id>"))),
        RespFrame::BulkString(Some(Bytes::from_static(b"    Return <node-id> replicas."))),
        RespFrame::BulkString(Some(Bytes::from_static(b"RESET [HARD|SOFT]"))),
        RespFrame::BulkString(Some(Bytes::from_static(b"    Reset a node."))),
        RespFrame::BulkString(Some(Bytes::from_static(b"SLOTS"))),
        RespFrame::BulkString(Some(Bytes::from_static(
            b"    Return information about slots range mappings.",
        ))),
        RespFrame::BulkString(Some(Bytes::from_static(b"SHARDS"))),
        RespFrame::BulkString(Some(Bytes::from_static(
            b"    Return information about slot range mappings (shard-oriented).",
        ))),
        RespFrame::BulkString(Some(Bytes::from_static(b"LINKS"))),
        RespFrame::BulkString(Some(Bytes::from_static(
            b"    Return a list of cluster bus links.",
        ))),
        RespFrame::BulkString(Some(Bytes::from_static(b"ADDSLOTS <slot> [<slot> ...]"))),
        RespFrame::BulkString(Some(Bytes::from_static(
            b"    Assign new hash slots to receiving node.",
        ))),
        RespFrame::BulkString(Some(Bytes::from_static(
            b"ADDSLOTSRANGE <start slot> <end slot> [<start slot> <end slot> ...]",
        ))),
        RespFrame::BulkString(Some(Bytes::from_static(
            b"    Assign new hash slots to receiving node.",
        ))),
        RespFrame::BulkString(Some(Bytes::from_static(b"DELSLOTS <slot> [<slot> ...]"))),
        RespFrame::BulkString(Some(Bytes::from_static(
            b"    Set hash slots as unbound in receiving node.",
        ))),
        RespFrame::BulkString(Some(Bytes::from_static(
            b"DELSLOTSRANGE <start slot> <end slot> [<start slot> <end slot> ...]",
        ))),
        RespFrame::BulkString(Some(Bytes::from_static(
            b"    Set hash slots as unbound in receiving node.",
        ))),
        RespFrame::BulkString(Some(Bytes::from_static(b"FAILOVER [FORCE|TAKEOVER]"))),
        RespFrame::BulkString(Some(Bytes::from_static(
            b"    Forces a replica to perform a manual failover of its master.",
        ))),
        RespFrame::BulkString(Some(Bytes::from_static(b"FLUSHSLOTS"))),
        RespFrame::BulkString(Some(Bytes::from_static(
            b"    Delete own slots information.",
        ))),
        RespFrame::BulkString(Some(Bytes::from_static(b"FORGET <node-id>"))),
        RespFrame::BulkString(Some(Bytes::from_static(
            b"    Remove a node from the nodes table.",
        ))),
        RespFrame::BulkString(Some(Bytes::from_static(b"MEET <ip> <port> [<bus-port>]"))),
        RespFrame::BulkString(Some(Bytes::from_static(
            b"    Force a node cluster to handshake with another at <ip>:<port>.",
        ))),
        RespFrame::BulkString(Some(Bytes::from_static(b"REPLICATE <node-id>"))),
        RespFrame::BulkString(Some(Bytes::from_static(
            b"    Reconfigure a node as a replica of the specified master node.",
        ))),
        RespFrame::BulkString(Some(Bytes::from_static(b"SAVECONFIG"))),
        RespFrame::BulkString(Some(Bytes::from_static(
            b"    Force saving cluster state on disk.",
        ))),
        RespFrame::BulkString(Some(Bytes::from_static(b"SET-CONFIG-EPOCH <epoch>"))),
        RespFrame::BulkString(Some(Bytes::from_static(
            b"    Set config epoch of current node.",
        ))),
        RespFrame::BulkString(Some(Bytes::from_static(
            b"SETSLOT <slot> (IMPORTING|MIGRATING|STABLE|NODE <node-id>)",
        ))),
        RespFrame::BulkString(Some(Bytes::from_static(
            b"    Bind a hash slot to a specific node.",
        ))),
        RespFrame::BulkString(Some(Bytes::from_static(b"HELP"))),
        RespFrame::BulkString(Some(Bytes::from_static(b"    Print this help."))),
    ];

    CommandOutcome::reply(RespFrame::Array(lines))
}

fn cluster_stub() -> CommandOutcome {
    CommandOutcome::reply(err(
        "ERR This instance has cluster support disabled. Check the 'cluster-enabled' configuration directive.",
    ))
}

// ---------------------------------------------------------------------------
// Standalone cluster-related commands
// ---------------------------------------------------------------------------

pub(super) fn cmd_readonly(args: &[Bytes]) -> CommandOutcome {
    if !args.is_empty() {
        return wrong_arity("readonly");
    }
    CommandOutcome::reply(RespFrame::ok())
}

pub(super) fn cmd_readwrite(args: &[Bytes]) -> CommandOutcome {
    if !args.is_empty() {
        return wrong_arity("readwrite");
    }
    CommandOutcome::reply(RespFrame::ok())
}

pub(super) fn cmd_asking(args: &[Bytes]) -> CommandOutcome {
    if !args.is_empty() {
        return wrong_arity("asking");
    }
    CommandOutcome::reply(RespFrame::ok())
}
