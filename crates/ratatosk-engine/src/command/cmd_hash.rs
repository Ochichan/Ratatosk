use bytes::Bytes;

use hashbrown::HashMap;
use ratatosk_resp::frame::RespFrame;

use crate::keyspace::{HashFieldEntry, ServerState, StoredValue, purge_expired_key};
use crate::object::{format_f64_for_redis, parse_f64};

use super::{
    ClientState, CommandOutcome, err, now_ms, parse_i64, wrong_arity, wrong_type_response,
};

/// Check if a hash field is alive (not expired).
pub(super) fn field_alive(entry: &HashFieldEntry, now: i64) -> bool {
    !entry.is_expired(now)
}

pub(super) fn cmd_hset(
    args: &[Bytes],
    server: &mut ServerState,
    client: &ClientState,
) -> CommandOutcome {
    if args.len() < 3 || args.len() % 2 == 0 {
        return wrong_arity("hset");
    }

    let key = &args[0];
    let now = now_ms();
    let mut db = server.db_mut(client.selected_db);
    purge_expired_key(&mut db, key, now);

    if !db.contains_key(key) {
        let mut fields = HashMap::new();
        let mut idx = 1usize;
        while idx < args.len() {
            fields.insert(
                args[idx].clone(),
                HashFieldEntry::new(args[idx + 1].clone()),
            );
            idx += 2;
        }
        let added = fields.len() as i64;
        db.insert(key.clone(), StoredValue::hash(fields, None));
        return CommandOutcome::reply(RespFrame::Integer(added));
    }

    let Some(entry) = db.get_mut(key) else {
        return CommandOutcome::reply(RespFrame::Integer(0));
    };
    let Some(hash) = entry.as_hash_mut() else {
        return wrong_type_response();
    };

    let mut added = 0i64;
    let mut idx = 1usize;
    while idx < args.len() {
        // HSET overwrites and clears field TTL
        let was_new = hash
            .insert(
                args[idx].clone(),
                HashFieldEntry::new(args[idx + 1].clone()),
            )
            .is_none_or(|old| old.is_expired(now));
        if was_new {
            added += 1;
        }
        idx += 2;
    }

    CommandOutcome::reply(RespFrame::Integer(added))
}

pub(super) fn cmd_hincrby(
    args: &[Bytes],
    server: &mut ServerState,
    client: &ClientState,
) -> CommandOutcome {
    let [key, field, increment_raw] = args else {
        return wrong_arity("hincrby");
    };

    let Some(increment) = parse_i64(increment_raw) else {
        return CommandOutcome::reply(err("ERR value is not an integer or out of range"));
    };

    let now = now_ms();
    let mut db = server.db_mut(client.selected_db);
    purge_expired_key(&mut db, key, now);

    if !db.contains_key(key) {
        let mut hash = HashMap::new();
        hash.insert(
            field.clone(),
            HashFieldEntry::new(Bytes::from(increment.to_string())),
        );
        db.insert(key.clone(), StoredValue::hash(hash, None));
        return CommandOutcome::reply(RespFrame::Integer(increment));
    }

    let Some(entry) = db.get_mut(key) else {
        return CommandOutcome::reply(RespFrame::Integer(increment));
    };
    let Some(hash) = entry.as_hash_mut() else {
        return wrong_type_response();
    };

    let current = if let Some(fe) = hash.get(field).filter(|e| field_alive(e, now)) {
        let Some(v) = parse_i64(&fe.value) else {
            return CommandOutcome::reply(err("ERR hash value is not an integer"));
        };
        v
    } else {
        0
    };

    let Some(next) = current.checked_add(increment) else {
        return CommandOutcome::reply(err("ERR increment or decrement would overflow"));
    };

    hash.insert(
        field.clone(),
        HashFieldEntry::new(Bytes::from(next.to_string())),
    );
    CommandOutcome::reply(RespFrame::Integer(next))
}

pub(super) fn cmd_hincrbyfloat(
    args: &[Bytes],
    server: &mut ServerState,
    client: &ClientState,
) -> CommandOutcome {
    let [key, field, increment_raw] = args else {
        return wrong_arity("hincrbyfloat");
    };

    let Some(increment) = parse_f64(increment_raw) else {
        return CommandOutcome::reply(err("ERR value is not a valid float"));
    };

    let now = now_ms();
    let mut db = server.db_mut(client.selected_db);
    purge_expired_key(&mut db, key, now);

    if !db.contains_key(key) {
        let formatted = format_f64_for_redis(increment);
        let mut hash = HashMap::new();
        hash.insert(field.clone(), HashFieldEntry::new(formatted.clone()));
        db.insert(key.clone(), StoredValue::hash(hash, None));
        return CommandOutcome::reply(RespFrame::BulkString(Some(formatted)));
    }

    let Some(entry) = db.get_mut(key) else {
        let formatted = format_f64_for_redis(increment);
        return CommandOutcome::reply(RespFrame::BulkString(Some(formatted)));
    };
    let Some(hash) = entry.as_hash_mut() else {
        return wrong_type_response();
    };

    let current = if let Some(fe) = hash.get(field).filter(|e| field_alive(e, now)) {
        let Some(v) = parse_f64(&fe.value) else {
            return CommandOutcome::reply(err("ERR hash value is not a float"));
        };
        v
    } else {
        0.0
    };

    let next = current + increment;
    if !next.is_finite() {
        return CommandOutcome::reply(err("ERR increment would produce NaN or Infinity"));
    }

    let formatted = format_f64_for_redis(next);
    hash.insert(field.clone(), HashFieldEntry::new(formatted.clone()));
    CommandOutcome::reply(RespFrame::BulkString(Some(formatted)))
}

pub(super) fn cmd_hmset(
    args: &[Bytes],
    server: &mut ServerState,
    client: &ClientState,
) -> CommandOutcome {
    if args.len() < 3 || args.len() % 2 == 0 {
        return wrong_arity("hmset");
    }

    let key = &args[0];
    let now = now_ms();
    let mut db = server.db_mut(client.selected_db);
    purge_expired_key(&mut db, key, now);

    if !db.contains_key(key) {
        let mut fields = HashMap::new();
        let mut idx = 1usize;
        while idx < args.len() {
            fields.insert(
                args[idx].clone(),
                HashFieldEntry::new(args[idx + 1].clone()),
            );
            idx += 2;
        }
        db.insert(key.clone(), StoredValue::hash(fields, None));
        return CommandOutcome::reply(RespFrame::ok());
    }

    let Some(entry) = db.get_mut(key) else {
        return CommandOutcome::reply(RespFrame::ok());
    };
    let Some(hash) = entry.as_hash_mut() else {
        return wrong_type_response();
    };

    let mut idx = 1usize;
    while idx < args.len() {
        hash.insert(
            args[idx].clone(),
            HashFieldEntry::new(args[idx + 1].clone()),
        );
        idx += 2;
    }

    CommandOutcome::reply(RespFrame::ok())
}

pub(super) fn cmd_hsetnx(
    args: &[Bytes],
    server: &mut ServerState,
    client: &ClientState,
) -> CommandOutcome {
    let [key, field, value] = args else {
        return wrong_arity("hsetnx");
    };

    let now = now_ms();
    let mut db = server.db_mut(client.selected_db);
    purge_expired_key(&mut db, key, now);

    if !db.contains_key(key) {
        let mut hash = HashMap::new();
        hash.insert(field.clone(), HashFieldEntry::new(value.clone()));
        db.insert(key.clone(), StoredValue::hash(hash, None));
        return CommandOutcome::reply(RespFrame::Integer(1));
    }

    let Some(entry) = db.get_mut(key) else {
        return CommandOutcome::reply(RespFrame::Integer(0));
    };
    let Some(hash) = entry.as_hash_mut() else {
        return wrong_type_response();
    };

    // Treat expired fields as non-existent
    if hash.get(field).is_some_and(|e| field_alive(e, now)) {
        return CommandOutcome::reply(RespFrame::Integer(0));
    }

    hash.insert(field.clone(), HashFieldEntry::new(value.clone()));
    CommandOutcome::reply(RespFrame::Integer(1))
}

pub(super) fn cmd_hdel(
    args: &[Bytes],
    server: &mut ServerState,
    client: &ClientState,
) -> CommandOutcome {
    if args.len() < 2 {
        return wrong_arity("hdel");
    }

    let key = &args[0];

    let now = now_ms();
    let mut db = server.db_mut(client.selected_db);
    purge_expired_key(&mut db, key, now);

    let Some(entry) = db.get_mut(key) else {
        return CommandOutcome::reply(RespFrame::Integer(0));
    };
    let Some(hash) = entry.as_hash_mut() else {
        return wrong_type_response();
    };

    let mut removed = 0i64;
    for field in &args[1..] {
        if let Some(fe) = hash.remove(field) {
            if !fe.is_expired(now) {
                removed += 1;
            }
        }
    }

    if hash.is_empty() || hash.values().all(|e| e.is_expired(now)) {
        db.remove(key);
    }

    CommandOutcome::reply(RespFrame::Integer(removed))
}
