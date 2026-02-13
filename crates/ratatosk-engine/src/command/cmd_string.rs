use bytes::Bytes;

use ratatosk_resp::frame::RespFrame;

use crate::expiry::{ExpireMode, GetExPolicy, SetExpirePolicy};
use crate::keyspace::{ServerState, StoredValue, purge_expired_key};
use crate::object::{format_f64_for_redis, normalize_range, parse_f64};

use super::{
    ClientState, CommandOutcome, err, now_ms, parse_command_expire_at_ms, parse_getex_policy,
    parse_i64, to_uppercase_bytes, wrong_arity, wrong_type_response,
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
    let db = server.db_mut(client.selected_db);
    purge_expired_key(db, key, now);

    let (mut value, expire_at_ms) = if let Some(existing) = db.get(key) {
        let Some(s) = existing.as_string() else {
            return wrong_type_response();
        };
        (s.to_vec(), existing.expire_at_ms)
    } else {
        (Vec::new(), None)
    };
    value.extend_from_slice(append);

    let new_len = value.len();
    db.insert(
        key.clone(),
        StoredValue::string(Bytes::from(value), expire_at_ms),
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
    let db = server.db_mut(client.selected_db);
    purge_expired_key(db, key, now);

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
    let db = server.db_mut(client.selected_db);
    purge_expired_key(db, key, now);

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
    let db = server.db_mut(client.selected_db);
    purge_expired_key(db, key, now);

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
        (s.to_vec(), existing.expire_at_ms)
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
    let db = server.db_mut(client.selected_db);
    purge_expired_key(db, key, now);

    if db.get(key).is_some_and(|entry| !entry.is_string()) {
        return wrong_type_response();
    }

    let previous = db.insert(key.clone(), StoredValue::string(value.clone(), None));

    CommandOutcome::reply(RespFrame::BulkString(
        previous.and_then(|entry| entry.as_string().cloned()),
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
    let db = server.db_mut(client.selected_db);
    purge_expired_key(db, key, now);

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
    let db = server.db_mut(client.selected_db);
    let mut out = Vec::with_capacity(args.len());

    for key in args {
        purge_expired_key(db, key, now);
        let value = db.get(key).and_then(|entry| entry.as_string().cloned());
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

    let db = server.db_mut(client.selected_db);
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
    let db = server.db_mut(client.selected_db);

    let mut idx = 0usize;
    while idx < args.len() {
        purge_expired_key(db, &args[idx], now);
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
    let db = server.db_mut(client.selected_db);
    purge_expired_key(db, key, now);

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
    db.insert(
        key.clone(),
        StoredValue::string(encoded.clone(), expire_at_ms),
    );

    CommandOutcome::reply(RespFrame::BulkString(Some(encoded)))
}

pub(super) fn cmd_incr_decr_with_delta(
    server: &mut ServerState,
    client: &ClientState,
    key: &Bytes,
    delta: i64,
) -> CommandOutcome {
    let now = now_ms();
    let db = server.db_mut(client.selected_db);
    purge_expired_key(db, key, now);

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

    db.insert(
        key.clone(),
        StoredValue::string(Bytes::from(next.to_string()), expire_at_ms),
    );
    CommandOutcome::reply(RespFrame::Integer(next))
}

pub(super) fn cmd_set(
    args: &[Bytes],
    server: &mut ServerState,
    client: &ClientState,
) -> CommandOutcome {
    if args.len() < 2 {
        return wrong_arity("set");
    }

    let key = args[0].clone();
    let value = args[1].clone();

    let mut nx = false;
    let mut xx = false;
    let mut get_old = false;
    let mut expire_policy = SetExpirePolicy::None;
    let now = now_ms();

    let mut idx = 2usize;
    while idx < args.len() {
        let option = to_uppercase_bytes(&args[idx]);
        match option.as_slice() {
            b"NX" => {
                nx = true;
                idx += 1;
            }
            b"XX" => {
                xx = true;
                idx += 1;
            }
            b"GET" => {
                get_old = true;
                idx += 1;
            }
            b"KEEPTTL" => {
                if !matches!(expire_policy, SetExpirePolicy::None) {
                    return CommandOutcome::reply(err("ERR syntax error"));
                }
                expire_policy = SetExpirePolicy::KeepTtl;
                idx += 1;
            }
            b"EX" | b"PX" | b"EXAT" | b"PXAT" => {
                if idx + 1 >= args.len() || !matches!(expire_policy, SetExpirePolicy::None) {
                    return CommandOutcome::reply(err("ERR syntax error"));
                }

                let Some(raw) = parse_i64(&args[idx + 1]) else {
                    return CommandOutcome::reply(err(
                        "ERR value is not an integer or out of range",
                    ));
                };
                if raw <= 0 {
                    return CommandOutcome::reply(err("ERR invalid expire time in 'set' command"));
                }

                let at_ms = match option.as_slice() {
                    b"EX" => now.saturating_add(raw.saturating_mul(1000)),
                    b"PX" => now.saturating_add(raw),
                    b"EXAT" => raw.saturating_mul(1000),
                    b"PXAT" => raw,
                    _ => {
                        debug_assert!(false, "SET expire option validated by outer match");
                        return CommandOutcome::reply(err("ERR syntax error"));
                    }
                };
                expire_policy = SetExpirePolicy::AtMs(at_ms);
                idx += 2;
            }
            _ => return CommandOutcome::reply(err("ERR syntax error")),
        }
    }

    if nx && xx {
        return CommandOutcome::reply(err("ERR syntax error"));
    }

    let db = server.db_mut(client.selected_db);
    purge_expired_key(db, &key, now);

    let existing_entry = db.get(&key);
    let should_set = (!nx || existing_entry.is_none()) && (!xx || existing_entry.is_some());

    if !should_set {
        if get_old {
            return match existing_entry {
                Some(entry) if entry.is_string() => {
                    CommandOutcome::reply(RespFrame::BulkString(entry.as_string().cloned()))
                }
                Some(_) => wrong_type_response(),
                None => CommandOutcome::reply(RespFrame::BulkString(None)),
            };
        }
        return CommandOutcome::reply(RespFrame::BulkString(None));
    }

    if get_old && existing_entry.is_some_and(|entry| !entry.is_string()) {
        return wrong_type_response();
    }

    let previous_value = if get_old {
        existing_entry.and_then(|entry| entry.as_string().cloned())
    } else {
        None
    };

    let expire_at_ms = match expire_policy {
        SetExpirePolicy::None => None,
        SetExpirePolicy::KeepTtl => existing_entry.and_then(|entry| entry.expire_at_ms),
        SetExpirePolicy::AtMs(ts) => Some(ts),
    };

    if let Some(expire_ts) = expire_at_ms {
        if expire_ts <= now {
            db.remove(&key);
        } else {
            db.insert(key, StoredValue::string(value, expire_at_ms));
        }
    } else {
        db.insert(key, StoredValue::string(value, expire_at_ms));
    }

    if get_old {
        return CommandOutcome::reply(RespFrame::BulkString(previous_value));
    }

    CommandOutcome::reply(RespFrame::ok())
}

pub(super) fn cmd_get(
    args: &[Bytes],
    server: &mut ServerState,
    client: &ClientState,
) -> CommandOutcome {
    let [key] = args else {
        return wrong_arity("get");
    };

    let now = now_ms();
    let db = server.db_mut(client.selected_db);
    purge_expired_key(db, key, now);

    let found = db.contains_key(key);
    if !found {
        server.stats.mark_keyspace_miss();
        return CommandOutcome::reply(RespFrame::BulkString(None));
    }

    server.stats.mark_keyspace_hit();
    let entry = &server.db(client.selected_db)[key];
    if !entry.is_string() {
        return wrong_type_response();
    }

    CommandOutcome::reply(RespFrame::BulkString(entry.as_string().cloned()))
}

pub(super) fn cmd_setex_with_mode(
    args: &[Bytes],
    server: &mut ServerState,
    client: &ClientState,
    mode: ExpireMode,
    command_name: &str,
) -> CommandOutcome {
    let [key, timeout_raw, value] = args else {
        return wrong_arity(command_name);
    };

    let now = now_ms();
    let expire_at_ms = match parse_command_expire_at_ms(timeout_raw, mode, now, command_name) {
        Ok(v) => v,
        Err(response) => return CommandOutcome::reply(response),
    };

    let db = server.db_mut(client.selected_db);
    purge_expired_key(db, key, now);

    if expire_at_ms <= now {
        db.remove(key);
    } else {
        db.insert(
            key.clone(),
            StoredValue::string(value.clone(), Some(expire_at_ms)),
        );
    }

    CommandOutcome::reply(RespFrame::ok())
}

pub(super) fn cmd_getdel(
    args: &[Bytes],
    server: &mut ServerState,
    client: &ClientState,
) -> CommandOutcome {
    let [key] = args else {
        return wrong_arity("getdel");
    };

    let now = now_ms();
    let db = server.db_mut(client.selected_db);
    purge_expired_key(db, key, now);

    let Some(entry) = db.get(key) else {
        return CommandOutcome::reply(RespFrame::BulkString(None));
    };
    if !entry.is_string() {
        return wrong_type_response();
    }

    let value = db
        .remove(key)
        .and_then(|removed| removed.as_string().cloned());
    CommandOutcome::reply(RespFrame::BulkString(value))
}

pub(super) fn cmd_getex(
    args: &[Bytes],
    server: &mut ServerState,
    client: &ClientState,
) -> CommandOutcome {
    if args.is_empty() {
        return wrong_arity("getex");
    }

    let key = &args[0];
    let now = now_ms();
    let policy = match parse_getex_policy(&args[1..], now) {
        Ok(policy) => policy,
        Err(response) => return CommandOutcome::reply(response),
    };

    let db = server.db_mut(client.selected_db);
    purge_expired_key(db, key, now);

    let Some(entry) = db.get(key) else {
        return CommandOutcome::reply(RespFrame::BulkString(None));
    };
    let Some(current_value) = entry.as_string().cloned() else {
        return wrong_type_response();
    };

    match policy {
        GetExPolicy::KeepTtl => {}
        GetExPolicy::Persist => {
            if let Some(entry) = db.get_mut(key) {
                entry.expire_at_ms = None;
            }
        }
        GetExPolicy::AtMs(expire_at_ms) => {
            if expire_at_ms <= now {
                db.remove(key);
            } else if let Some(entry) = db.get_mut(key) {
                entry.expire_at_ms = Some(expire_at_ms);
            }
        }
    }

    CommandOutcome::reply(RespFrame::BulkString(Some(current_value)))
}
