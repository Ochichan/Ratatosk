use bytes::Bytes;
use ratatosk_resp::frame::RespFrame;

use crate::keyspace::ServerState;

use super::{CommandOutcome, err, parse_i64, wrong_arity};

pub(super) fn cmd_wait(args: &[Bytes], server: &ServerState) -> CommandOutcome {
    let [num_replicas_raw, timeout_raw] = args else {
        return wrong_arity("wait");
    };

    let Some(num_replicas) = parse_i64(num_replicas_raw) else {
        return CommandOutcome::reply(err("ERR value is not an integer or out of range"));
    };
    let Some(_timeout_ms) = parse_i64(timeout_raw) else {
        return CommandOutcome::reply(err("ERR value is not an integer or out of range"));
    };
    if num_replicas < 0 {
        return CommandOutcome::reply(err("ERR value is out of range"));
    }

    let acked = server.replication_acked_replicas(server.replication_offset()) as i64;
    if num_replicas > 0 && acked == 0 {
        tracing::warn!(
            target = "ratatosk::replication",
            requested_replicas = num_replicas,
            "WAIT returning 0: Ratatosk is running in single-node mode with no replicas"
        );
    }
    CommandOutcome::reply(RespFrame::Integer(acked.min(num_replicas)))
}

pub(super) fn cmd_waitaof(args: &[Bytes], server: &ServerState) -> CommandOutcome {
    let [num_local_raw, num_replicas_raw, timeout_raw] = args else {
        return wrong_arity("waitaof");
    };

    let Some(num_local) = parse_i64(num_local_raw) else {
        return CommandOutcome::reply(err("ERR value is not an integer or out of range"));
    };
    let Some(num_replicas) = parse_i64(num_replicas_raw) else {
        return CommandOutcome::reply(err("ERR value is not an integer or out of range"));
    };
    let Some(_timeout_ms) = parse_i64(timeout_raw) else {
        return CommandOutcome::reply(err("ERR value is not an integer or out of range"));
    };

    if num_local < 0 || num_replicas < 0 {
        return CommandOutcome::reply(err("ERR value is out of range"));
    }

    let local_ack = if server.aof_enabled() && !server.aof_write_latched() {
        1
    } else {
        0
    };
    let replica_ack = server.replication_acked_replicas(server.replication_offset()) as i64;
    if num_replicas > 0 && replica_ack == 0 {
        tracing::warn!(
            target = "ratatosk::replication",
            requested_replicas = num_replicas,
            "WAITAOF returning 0 replica acks: Ratatosk is running in single-node mode with no replicas"
        );
    }
    CommandOutcome::reply(RespFrame::Array(vec![
        RespFrame::Integer(local_ack.min(num_local)),
        RespFrame::Integer(replica_ack.min(num_replicas)),
    ]))
}

pub(super) fn cmd_migrate(args: &[Bytes]) -> CommandOutcome {
    if args.len() < 5 {
        return wrong_arity("migrate");
    }

    CommandOutcome::reply(RespFrame::bulk_str("NOKEY"))
}
