use bytes::Bytes;

use ratatosk_resp::frame::RespFrame;

use crate::keyspace::ServerState;
use crate::object::now_us;

use super::{ClientState, CommandOutcome, wrong_arity};

pub(super) fn cmd_dbsize(
    args: &[Bytes],
    server: &mut ServerState,
    client: &ClientState,
) -> CommandOutcome {
    if !args.is_empty() {
        return wrong_arity("dbsize");
    }

    // Like Redis, DBSIZE counts expired keys that are not yet reclaimed.
    let db = server.db(client.selected_db);
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
