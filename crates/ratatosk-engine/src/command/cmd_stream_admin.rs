use bytes::Bytes;

use ratatosk_resp::frame::RespFrame;

use crate::keyspace::{ServerState, StreamId, purge_expired_key};

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
    // Checks run in the order of Redis's xsetidCommand: the ID, the options,
    // the key, then the ID against the stream's own state.
    let [key, id_raw, options @ ..] = args else {
        return wrong_arity("xsetid");
    };
    let Some(new_id) = parse_stream_id(id_raw) else {
        return CommandOutcome::reply(err(
            "ERR Invalid stream ID specified as stream command argument",
        ));
    };

    let mut entries_added = None;
    let mut max_deleted_id = StreamId { ms: 0, seq: 0 };
    let mut idx = 0usize;
    while idx < options.len() {
        let option = to_uppercase_bytes(&options[idx]);
        let Some(value) = options.get(idx + 1) else {
            return CommandOutcome::reply(err("ERR syntax error"));
        };
        match option.as_slice() {
            b"ENTRIESADDED" => {
                let Some(parsed) = parse_i64(value) else {
                    return CommandOutcome::reply(err(
                        "ERR value is not an integer or out of range",
                    ));
                };
                let Ok(parsed) = u64::try_from(parsed) else {
                    return CommandOutcome::reply(err("ERR entries_added must be positive"));
                };
                entries_added = Some(parsed);
            }
            b"MAXDELETEDID" => {
                let Some(parsed) = parse_stream_id(value) else {
                    return CommandOutcome::reply(err(
                        "ERR Invalid stream ID specified as stream command argument",
                    ));
                };
                if new_id < parsed {
                    return CommandOutcome::reply(err(
                        "ERR The ID specified in XSETID is smaller than the provided max_deleted_entry_id",
                    ));
                }
                max_deleted_id = parsed;
            }
            _ => return CommandOutcome::reply(err("ERR syntax error")),
        }
        idx += 2;
    }

    let now = now_ms();
    let mut db = server.db_mut(client.selected_db);
    purge_expired_key(&mut db, key, now);
    let Some(entry) = db.get_mut(key) else {
        return CommandOutcome::reply(err("ERR no such key"));
    };
    let Some((stream, meta)) = entry.as_stream_entries_and_meta_mut() else {
        return wrong_type_response();
    };

    if new_id < meta.max_deleted_id {
        return CommandOutcome::reply(err(
            "ERR The ID specified in XSETID is smaller than current max_deleted_entry_id",
        ));
    }
    if let Some(top) = stream.last() {
        if new_id < top.id {
            return CommandOutcome::reply(err(
                "ERR The ID specified in XSETID is smaller than the target stream top item",
            ));
        }
        if entries_added.is_some_and(|added| (stream.len() as u64) > added) {
            return CommandOutcome::reply(err(
                "ERR The entries_added specified in XSETID is smaller than the target stream length",
            ));
        }
    }

    meta.last_id = new_id;
    if let Some(added) = entries_added {
        meta.entries_added = added;
    }
    if max_deleted_id != (StreamId { ms: 0, seq: 0 }) {
        meta.max_deleted_id = max_deleted_id;
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
