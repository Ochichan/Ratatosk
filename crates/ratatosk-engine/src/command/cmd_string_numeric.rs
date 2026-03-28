use bytes::Bytes;
use itoa::Buffer;

use ratatosk_resp::frame::RespFrame;

use crate::keyspace::{ServerState, StoredValue, purge_expired_key};
use crate::object::{format_f64_for_redis, parse_f64};

use super::{
    ClientState, CommandOutcome, err, now_ms, parse_i64, wrong_arity, wrong_type_response,
};

pub(super) fn cmd_incr(
    args: &[Bytes],
    server: &mut ServerState,
    client: &ClientState,
) -> CommandOutcome {
    let [key] = args else {
        return wrong_arity("incr");
    };
    cmd_incr_decr_with_delta(server, client, key, 1)
}

pub(super) fn cmd_incrby(
    args: &[Bytes],
    server: &mut ServerState,
    client: &ClientState,
) -> CommandOutcome {
    let [key, by_raw] = args else {
        return wrong_arity("incrby");
    };

    let Some(by) = parse_i64(by_raw) else {
        return CommandOutcome::reply(err("ERR value is not an integer or out of range"));
    };
    cmd_incr_decr_with_delta(server, client, key, by)
}

pub(super) fn cmd_decr(
    args: &[Bytes],
    server: &mut ServerState,
    client: &ClientState,
) -> CommandOutcome {
    let [key] = args else {
        return wrong_arity("decr");
    };
    cmd_incr_decr_with_delta(server, client, key, -1)
}

pub(super) fn cmd_decrby(
    args: &[Bytes],
    server: &mut ServerState,
    client: &ClientState,
) -> CommandOutcome {
    let [key, by_raw] = args else {
        return wrong_arity("decrby");
    };

    let Some(by) = parse_i64(by_raw) else {
        return CommandOutcome::reply(err("ERR value is not an integer or out of range"));
    };
    if by == i64::MIN {
        return CommandOutcome::reply(err("ERR decrement would overflow"));
    }
    cmd_incr_decr_with_delta(server, client, key, -by)
}

pub(super) fn cmd_incrbyfloat(
    args: &[Bytes],
    server: &mut ServerState,
    client: &ClientState,
) -> CommandOutcome {
    let [key, by_raw] = args else {
        return wrong_arity("incrbyfloat");
    };

    let Some(by) = parse_f64(by_raw) else {
        return CommandOutcome::reply(err("ERR value is not a valid float"));
    };

    let now = now_ms();
    let mut db = server.db_mut(client.selected_db);
    purge_expired_key(&mut db, key, now);

    let (base, expire_at_ms) = if let Some(existing) = db.get(key) {
        let Some(s) = existing.as_string() else {
            return wrong_type_response();
        };
        let Some(parsed) = parse_f64(s) else {
            return CommandOutcome::reply(err("ERR value is not a valid float"));
        };
        (parsed, existing.expire_at_ms)
    } else {
        (0.0, None)
    };

    let next = base + by;
    if !next.is_finite() {
        return CommandOutcome::reply(err("ERR increment would produce NaN or Infinity"));
    }

    let encoded = format_f64_for_redis(next);
    let encoded_for_reply = encoded.clone();
    db.insert(key.clone(), StoredValue::string(encoded, expire_at_ms));

    CommandOutcome::reply(RespFrame::BulkString(Some(encoded_for_reply)))
}

pub(super) fn cmd_incr_decr_with_delta(
    server: &mut ServerState,
    client: &ClientState,
    key: &Bytes,
    delta: i64,
) -> CommandOutcome {
    let now = now_ms();
    let mut db = server.db_mut(client.selected_db);
    purge_expired_key(&mut db, key, now);

    let (current, expire_at_ms) = if let Some(existing) = db.get(key) {
        let Some(s) = existing.as_string() else {
            return wrong_type_response();
        };
        let Some(parsed) = parse_i64(s) else {
            return CommandOutcome::reply(err("ERR value is not an integer or out of range"));
        };
        (parsed, existing.expire_at_ms)
    } else {
        (0, None)
    };

    let Some(next) = current.checked_add(delta) else {
        return CommandOutcome::reply(err("ERR increment or decrement would overflow"));
    };

    let mut buf = Buffer::new();
    db.insert(
        key.clone(),
        StoredValue::string(
            Bytes::copy_from_slice(buf.format(next).as_bytes()),
            expire_at_ms,
        ),
    );
    CommandOutcome::reply(RespFrame::Integer(next))
}
