use bytes::Bytes;

use ratatosk_resp::frame::RespFrame;

use crate::keyspace::{ServerState, purge_expired_key};

use super::cmd_stream::parse_stream_id;
use super::{
    ClientState, CommandOutcome, err, now_ms, parse_i64, to_uppercase_bytes, wrong_arity,
    wrong_type_response,
};

pub(super) fn cmd_xsetid(
    args: &[Bytes],
    server: &mut ServerState,
    client: &ClientState,
) -> CommandOutcome {
    let [key, id_raw] = args else {
        return wrong_arity("xsetid");
    };

    let Some(new_id) = parse_stream_id(id_raw) else {
        return CommandOutcome::reply(err(
            "ERR Invalid stream ID specified as stream command argument",
        ));
    };

    let now = now_ms();
    let mut db = server.db_mut(client.selected_db);
    purge_expired_key(&mut db, key, now);

    let Some(entry) = db.get(key) else {
        return CommandOutcome::reply(err("ERR no such key"));
    };
    let Some(stream) = entry.as_stream_entries() else {
        return wrong_type_response();
    };

    if stream.last().is_some_and(|last| new_id < last.id) {
        return CommandOutcome::reply(err(
            "ERR The ID specified in XSETID is smaller than the target stream top item",
        ));
    }

    CommandOutcome::reply(RespFrame::ok())
}

pub(super) fn cmd_xcfgset(
    args: &[Bytes],
    server: &mut ServerState,
    client: &ClientState,
) -> CommandOutcome {
    if args.is_empty() {
        return wrong_arity("xcfgset");
    }

    let key = &args[0];
    let mut idx = 1usize;

    while idx < args.len() {
        let option = to_uppercase_bytes(&args[idx]);
        match option.as_slice() {
            b"IDMP-DURATION" | b"IDMP-MAXSIZE" => {
                let Some(raw_value) = args.get(idx + 1) else {
                    return CommandOutcome::reply(err("ERR syntax error"));
                };
                let Some(value) = parse_i64(raw_value) else {
                    return CommandOutcome::reply(err(
                        "ERR value is not an integer or out of range",
                    ));
                };
                if value < 0 {
                    return CommandOutcome::reply(err(
                        "ERR value is not an integer or out of range",
                    ));
                }
                idx += 2;
            }
            _ => return CommandOutcome::reply(err("ERR syntax error")),
        }
    }

    let now = now_ms();
    let mut db = server.db_mut(client.selected_db);
    purge_expired_key(&mut db, key, now);

    let Some(entry) = db.get(key) else {
        return CommandOutcome::reply(err("ERR no such key"));
    };
    if !entry.is_stream() {
        return wrong_type_response();
    }

    CommandOutcome::reply(RespFrame::ok())
}
