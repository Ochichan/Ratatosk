use std::hash::{Hash, Hasher};

use bytes::Bytes;

use glob_match::glob_match;
use ratatosk_resp::frame::RespFrame;

use crate::keyspace::{ServerState, purge_expired_key, purge_expired_keys};
use crate::security::next_audit_stamp;

use super::{
    ClientState, CommandOutcome, err, now_ms, to_uppercase_bytes, wrong_arity, wrong_type_response,
};

pub(super) fn cmd_randomkey(
    args: &[Bytes],
    server: &mut ServerState,
    client: &ClientState,
) -> CommandOutcome {
    if !args.is_empty() {
        return wrong_arity("randomkey");
    }

    let now = now_ms();
    let mut db = server.db_mut(client.selected_db);
    purge_expired_keys(&mut db, now);

    let key = db.keys().next().cloned();
    CommandOutcome::reply(RespFrame::BulkString(key))
}

pub(super) fn cmd_type(
    args: &[Bytes],
    server: &mut ServerState,
    client: &ClientState,
) -> CommandOutcome {
    let [key] = args else {
        return wrong_arity("type");
    };

    let now = now_ms();
    let mut db = server.db_mut(client.selected_db);
    purge_expired_key(&mut db, key, now);

    if let Some(entry) = db.get(key) {
        CommandOutcome::reply(RespFrame::simple_str(entry.type_name()))
    } else {
        CommandOutcome::reply(RespFrame::simple_str("none"))
    }
}

pub(super) fn cmd_keys(
    args: &[Bytes],
    server: &mut ServerState,
    client: &ClientState,
) -> CommandOutcome {
    let [pattern] = args else {
        return wrong_arity("keys");
    };

    let now = now_ms();
    let mut db = server.db_mut(client.selected_db);
    purge_expired_keys(&mut db, now);

    let pattern = String::from_utf8_lossy(pattern).to_string();
    let mut out = db
        .keys()
        .filter(|key| glob_match(&pattern, &String::from_utf8_lossy(key)))
        .cloned()
        .collect::<Vec<_>>();
    out.sort();

    let frames = out
        .into_iter()
        .map(|key| RespFrame::BulkString(Some(key)))
        .collect::<Vec<_>>();

    CommandOutcome::reply(RespFrame::Array(frames))
}

pub(super) fn cmd_delex(
    args: &[Bytes],
    server: &mut ServerState,
    client: &ClientState,
) -> CommandOutcome {
    if args.is_empty() {
        return wrong_arity("delex");
    }

    let key = &args[0];
    let now = now_ms();
    let mut db = server.db_mut(client.selected_db);
    purge_expired_key(&mut db, key, now);

    let Some(entry) = db.get(key).cloned() else {
        return CommandOutcome::reply(RespFrame::Integer(0));
    };
    let Some(value) = entry.as_string() else {
        return wrong_type_response();
    };
    let should_delete = match args {
        [_] => true,
        [_, condition_raw, expected] => {
            let condition = to_uppercase_bytes(condition_raw);
            let digest = digest_i64_string(value.as_ref());
            match condition.as_slice() {
                b"IFEQ" => *value == *expected,
                b"IFNE" => *value != *expected,
                b"IFDEQ" => digest.as_bytes() == expected.as_ref(),
                b"IFDNE" => digest.as_bytes() != expected.as_ref(),
                _ => return CommandOutcome::reply(err("ERR syntax error")),
            }
        }
        _ => return CommandOutcome::reply(err("ERR syntax error")),
    };

    let removed = if should_delete {
        db.remove(key);
        1
    } else {
        0
    };

    let key_text = String::from_utf8_lossy(key).into_owned();
    let payload = format!(
        "event=DELEX client_id={} db={} key={} removed={}",
        client.id(),
        client.selected_db,
        key_text,
        removed
    );
    let stamp = next_audit_stamp("DELEX", &payload);
    tracing::info!(
        target = "ratatosk::audit",
        event = "DELEX",
        audit_seq = stamp.seq,
        audit_prev_hash = %stamp.prev_hash,
        audit_hash = %stamp.hash,
        client_id = client.id(),
        db = client.selected_db,
        key = %key_text,
        removed,
        "conditional delete executed"
    );

    CommandOutcome::reply(RespFrame::Integer(removed))
}

pub(super) fn cmd_digest(
    args: &[Bytes],
    server: &mut ServerState,
    client: &ClientState,
) -> CommandOutcome {
    let [key] = args else {
        return wrong_arity("digest");
    };

    let now = now_ms();
    let mut db = server.db_mut(client.selected_db);
    purge_expired_key(&mut db, key, now);

    let Some(entry) = db.get(key) else {
        return CommandOutcome::reply(RespFrame::BulkString(None));
    };
    let Some(value) = entry.as_string() else {
        return wrong_type_response();
    };

    let digest = digest_i64_string(value.as_ref());
    CommandOutcome::reply(RespFrame::bulk_str(&digest))
}

pub(super) fn digest_i64_string(data: &[u8]) -> String {
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    data.hash(&mut hasher);
    let signed = hasher.finish() as i64;
    signed.to_string()
}
