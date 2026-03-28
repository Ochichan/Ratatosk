use bytes::Bytes;

use ratatosk_resp::frame::RespFrame;

use crate::keyspace::ServerState;
use crate::security::next_audit_stamp;

use super::{ClientState, CommandOutcome, err, parse_usize, wrong_arity};

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

pub(super) fn cmd_bgrewriteaof(args: &[Bytes], server: &mut ServerState) -> CommandOutcome {
    if !args.is_empty() {
        return wrong_arity("bgrewriteaof");
    }

    if !server.aof_enabled() {
        metrics::counter!("ratatosk_bgrewriteaof_requests_total", "result" => "aof_disabled")
            .increment(1);
        return CommandOutcome::reply(err("ERR BGREWRITEAOF requires appendonly to be enabled"));
    }

    if server.aof_rewrite_in_progress() {
        metrics::counter!("ratatosk_bgrewriteaof_requests_total", "result" => "in_progress")
            .increment(1);
        return CommandOutcome::reply(err("ERR Background AOF rewrite already in progress"));
    }

    server.set_aof_rewrite_in_progress(true);
    metrics::counter!("ratatosk_bgrewriteaof_requests_total", "result" => "started").increment(1);
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

#[allow(clippy::result_large_err)]
fn parse_flush_mode(args: &[Bytes], command: &str) -> Result<(), CommandOutcome> {
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

    let mode = args
        .first()
        .map(|raw| String::from_utf8_lossy(raw).to_ascii_uppercase())
        .unwrap_or_else(|| "SYNC".to_string());

    server.clear_db(client.selected_db);

    let payload = format!(
        "event=FLUSHDB client_id={} db={} mode={}",
        client.id(),
        client.selected_db,
        mode
    );
    let stamp = next_audit_stamp("FLUSHDB", &payload);
    tracing::info!(
        target = "ratatosk::audit",
        event = "FLUSHDB",
        audit_seq = stamp.seq,
        audit_prev_hash = %stamp.prev_hash,
        audit_hash = %stamp.hash,
        client_id = client.id(),
        db = client.selected_db,
        mode = mode,
        "database cleared"
    );

    CommandOutcome::reply(RespFrame::ok())
}

pub(super) fn cmd_flushall(
    args: &[Bytes],
    server: &mut ServerState,
    client: &ClientState,
) -> CommandOutcome {
    if let Err(outcome) = parse_flush_mode(args, "flushall") {
        return outcome;
    }

    let mode = args
        .first()
        .map(|raw| String::from_utf8_lossy(raw).to_ascii_uppercase())
        .unwrap_or_else(|| "SYNC".to_string());

    let db_count = server.db_count();
    server.clear_all_dbs();

    let payload = format!(
        "event=FLUSHALL client_id={} db_count={} mode={}",
        client.id(),
        db_count,
        mode
    );
    let stamp = next_audit_stamp("FLUSHALL", &payload);
    tracing::info!(
        target = "ratatosk::audit",
        event = "FLUSHALL",
        audit_seq = stamp.seq,
        audit_prev_hash = %stamp.prev_hash,
        audit_hash = %stamp.hash,
        client_id = client.id(),
        db_count = db_count,
        mode = mode,
        "all databases cleared"
    );

    CommandOutcome::reply(RespFrame::ok())
}
