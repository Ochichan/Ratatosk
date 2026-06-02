use bytes::Bytes;

use ratatosk_resp::frame::RespFrame;

use crate::keyspace::{ServerState, StoredValue, purge_expired_key};
use crate::object::normalize_range;

use super::{
    ClientState, CommandOutcome, err, now_ms, parse_i64, wrong_arity, wrong_type_response,
};

pub(super) fn cmd_append(
    args: &[Bytes],
    server: &mut ServerState,
    client: &ClientState,
) -> CommandOutcome {
    let [key, append] = args else {
        return wrong_arity("append");
    };

    let now = now_ms();
    let mut db = server.db_mut(client.selected_db);
    purge_expired_key(&mut db, key, now);

    let (value, expire_at_ms) = if let Some(existing) = db.get(key) {
        let Some(s) = existing.as_string() else {
            return wrong_type_response();
        };
        (s.clone(), existing.expire_at_ms())
    } else {
        (Bytes::new(), None)
    };

    // Pre-allocate exact capacity for concatenated result
    let new_len = value.len() + append.len();
    let mut combined = Vec::with_capacity(new_len);
    combined.extend_from_slice(&value);
    combined.extend_from_slice(append);

    db.insert(
        key.clone(),
        StoredValue::string(Bytes::from(combined), expire_at_ms),
    );

    CommandOutcome::reply(RespFrame::Integer(new_len as i64))
}

pub(super) fn cmd_strlen(
    args: &[Bytes],
    server: &mut ServerState,
    client: &ClientState,
) -> CommandOutcome {
    let [key] = args else {
        return wrong_arity("strlen");
    };

    let now = now_ms();
    let mut db = server.db_mut(client.selected_db);
    purge_expired_key(&mut db, key, now);

    let Some(entry) = db.get(key) else {
        return CommandOutcome::reply(RespFrame::Integer(0));
    };
    let Some(s) = entry.as_string() else {
        return wrong_type_response();
    };

    CommandOutcome::reply(RespFrame::Integer(s.len() as i64))
}

pub(super) fn cmd_getrange(
    args: &[Bytes],
    server: &mut ServerState,
    client: &ClientState,
) -> CommandOutcome {
    let [key, start_raw, end_raw] = args else {
        return wrong_arity("getrange");
    };

    let Some(start) = parse_i64(start_raw) else {
        return CommandOutcome::reply(err("ERR value is not an integer or out of range"));
    };
    let Some(end) = parse_i64(end_raw) else {
        return CommandOutcome::reply(err("ERR value is not an integer or out of range"));
    };

    let now = now_ms();
    let mut db = server.db_mut(client.selected_db);
    purge_expired_key(&mut db, key, now);

    let Some(entry) = db.get(key) else {
        return CommandOutcome::reply(RespFrame::bulk_str(""));
    };
    let Some(s) = entry.as_string() else {
        return wrong_type_response();
    };

    let bytes = s.as_ref();
    let Some((range_start, range_end)) = normalize_range(bytes.len(), start, end) else {
        return CommandOutcome::reply(RespFrame::bulk_str(""));
    };

    CommandOutcome::reply(RespFrame::BulkString(Some(Bytes::copy_from_slice(
        &bytes[range_start..=range_end],
    ))))
}

pub(super) fn cmd_setrange(
    args: &[Bytes],
    server: &mut ServerState,
    client: &ClientState,
) -> CommandOutcome {
    let [key, offset_raw, value] = args else {
        return wrong_arity("setrange");
    };

    let Some(offset_i64) = parse_i64(offset_raw) else {
        return CommandOutcome::reply(err("ERR value is not an integer or out of range"));
    };
    if offset_i64 < 0 {
        return CommandOutcome::reply(err("ERR offset is out of range"));
    }
    let Ok(offset) = usize::try_from(offset_i64) else {
        return CommandOutcome::reply(err("ERR offset is out of range"));
    };

    let now = now_ms();
    let mut db = server.db_mut(client.selected_db);
    purge_expired_key(&mut db, key, now);

    if value.is_empty() {
        let Some(entry) = db.get(key) else {
            return CommandOutcome::reply(RespFrame::Integer(0));
        };
        let Some(s) = entry.as_string() else {
            return wrong_type_response();
        };
        return CommandOutcome::reply(RespFrame::Integer(s.len() as i64));
    }

    let (mut base, expire_at_ms) = if let Some(existing) = db.get(key) {
        let Some(s) = existing.as_string() else {
            return wrong_type_response();
        };
        (s.to_vec(), existing.expire_at_ms())
    } else {
        (Vec::new(), None)
    };

    let Some(required_len) = offset.checked_add(value.len()) else {
        return CommandOutcome::reply(err("ERR offset is out of range"));
    };
    if base.len() < required_len {
        base.resize(required_len, 0);
    }
    base[offset..offset + value.len()].copy_from_slice(value);

    let new_len = base.len();
    db.insert(
        key.clone(),
        StoredValue::string(Bytes::from(base), expire_at_ms),
    );

    CommandOutcome::reply(RespFrame::Integer(new_len as i64))
}

pub(super) fn cmd_getset(
    args: &[Bytes],
    server: &mut ServerState,
    client: &ClientState,
) -> CommandOutcome {
    let [key, value] = args else {
        return wrong_arity("getset");
    };

    let now = now_ms();
    let mut db = server.db_mut(client.selected_db);
    purge_expired_key(&mut db, key, now);

    if db.get(key).is_some_and(|entry| !entry.is_string()) {
        return wrong_type_response();
    }

    let previous = db.insert(key.clone(), StoredValue::string(value.clone(), None));

    CommandOutcome::reply(RespFrame::BulkString(
        previous.and_then(|entry| entry.as_string_bytes()),
    ))
}

pub(super) fn cmd_setnx(
    args: &[Bytes],
    server: &mut ServerState,
    client: &ClientState,
) -> CommandOutcome {
    let [key, value] = args else {
        return wrong_arity("setnx");
    };

    let now = now_ms();
    let mut db = server.db_mut(client.selected_db);
    purge_expired_key(&mut db, key, now);

    if db.contains_key(key) {
        return CommandOutcome::reply(RespFrame::Integer(0));
    }

    db.insert(key.clone(), StoredValue::string(value.clone(), None));
    CommandOutcome::reply(RespFrame::Integer(1))
}

pub(super) fn cmd_mget(
    args: &[Bytes],
    server: &mut ServerState,
    client: &ClientState,
) -> CommandOutcome {
    if args.is_empty() {
        return wrong_arity("mget");
    }

    let now = now_ms();
    let mut db = server.db_mut(client.selected_db);
    let mut out = Vec::with_capacity(args.len());

    for key in args {
        purge_expired_key(&mut db, key, now);
        let value = db.get(key).and_then(|entry| entry.as_string_bytes());
        out.push(RespFrame::BulkString(value));
    }

    CommandOutcome::reply(RespFrame::Array(out))
}

pub(super) fn cmd_mset(
    args: &[Bytes],
    server: &mut ServerState,
    client: &ClientState,
) -> CommandOutcome {
    if args.len() < 2 || args.len() % 2 != 0 {
        return wrong_arity("mset");
    }

    let mut db = server.db_mut(client.selected_db);
    let mut idx = 0usize;
    while idx < args.len() {
        db.insert(
            args[idx].clone(),
            StoredValue::string(args[idx + 1].clone(), None),
        );
        idx += 2;
    }

    CommandOutcome::reply(RespFrame::ok())
}

pub(super) fn cmd_msetnx(
    args: &[Bytes],
    server: &mut ServerState,
    client: &ClientState,
) -> CommandOutcome {
    if args.len() < 2 || args.len() % 2 != 0 {
        return wrong_arity("msetnx");
    }

    let now = now_ms();
    let mut db = server.db_mut(client.selected_db);

    let mut idx = 0usize;
    while idx < args.len() {
        purge_expired_key(&mut db, &args[idx], now);
        if db.contains_key(&args[idx]) {
            return CommandOutcome::reply(RespFrame::Integer(0));
        }
        idx += 2;
    }

    let mut idx = 0usize;
    while idx < args.len() {
        db.insert(
            args[idx].clone(),
            StoredValue::string(args[idx + 1].clone(), None),
        );
        idx += 2;
    }

    CommandOutcome::reply(RespFrame::Integer(1))
}
