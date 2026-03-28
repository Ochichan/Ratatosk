use bytes::Bytes;

use ratatosk_resp::frame::RespFrame;

use crate::keyspace::{ServerState, purge_expired_keys};
use crate::object::now_us;

use super::{ClientState, CommandOutcome, now_ms, wrong_arity};

pub(super) fn cmd_dbsize(
    args: &[Bytes],
    server: &mut ServerState,
    client: &ClientState,
) -> CommandOutcome {
    if !args.is_empty() {
        return wrong_arity("dbsize");
    }

    let now = now_ms();
    let mut db = server.db_mut(client.selected_db);
    purge_expired_keys(&mut db, now);
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

pub(super) fn cmd_monitor(
    args: &[Bytes],
    server: &mut ServerState,
    client: &mut ClientState,
) -> CommandOutcome {
    if !args.is_empty() {
        return wrong_arity("monitor");
    }

    server.register_monitor(client.id());
    client.set_monitor(true);
    CommandOutcome::reply(RespFrame::ok())
}
