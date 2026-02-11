use bytes::Bytes;

use glob_match::glob_match;
use hashbrown::HashMap;
use ratatosk_resp::frame::RespFrame;

use crate::keyspace::{HashFieldEntry, ServerState, StoredValue, purge_expired_key};
use crate::object::{format_f64_for_redis, now_us, parse_f64};

use super::{
    ClientState, CommandOutcome, err, now_ms, parse_i64, parse_scan_cursor,
    parse_scan_match_count_options, scan_collect_indexes, scan_reply, wrong_arity,
    wrong_type_response,
};

const MAX_HASH_RANDOM_COUNT: usize = 100_000;

/// Check if a hash field is alive (not expired).
fn field_alive(entry: &HashFieldEntry, now: i64) -> bool {
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
    let db = server.db_mut(client.selected_db);
    purge_expired_key(db, key, now);

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

pub(super) fn cmd_hget(
    args: &[Bytes],
    server: &mut ServerState,
    client: &ClientState,
) -> CommandOutcome {
    let [key, field] = args else {
        return wrong_arity("hget");
    };

    let now = now_ms();
    let db = server.db_mut(client.selected_db);
    purge_expired_key(db, key, now);

    let Some(entry) = db.get(key) else {
        return CommandOutcome::reply(RespFrame::BulkString(None));
    };
    let Some(hash) = entry.as_hash() else {
        return wrong_type_response();
    };

    let value = hash
        .get(field)
        .filter(|e| field_alive(e, now))
        .map(|e| e.value.clone());
    CommandOutcome::reply(RespFrame::BulkString(value))
}

pub(super) fn cmd_hmget(
    args: &[Bytes],
    server: &mut ServerState,
    client: &ClientState,
) -> CommandOutcome {
    if args.len() < 2 {
        return wrong_arity("hmget");
    }

    let key = &args[0];
    let fields = &args[1..];

    let now = now_ms();
    let db = server.db_mut(client.selected_db);
    purge_expired_key(db, key, now);

    let Some(entry) = db.get(key) else {
        return CommandOutcome::reply(RespFrame::Array(
            fields
                .iter()
                .map(|_| RespFrame::BulkString(None))
                .collect::<Vec<_>>(),
        ));
    };
    let Some(hash) = entry.as_hash() else {
        return wrong_type_response();
    };

    CommandOutcome::reply(RespFrame::Array(
        fields
            .iter()
            .map(|field| {
                RespFrame::BulkString(
                    hash.get(field)
                        .filter(|e| field_alive(e, now))
                        .map(|e| e.value.clone()),
                )
            })
            .collect::<Vec<_>>(),
    ))
}

pub(super) fn cmd_hgetall(
    args: &[Bytes],
    server: &mut ServerState,
    client: &ClientState,
) -> CommandOutcome {
    let [key] = args else {
        return wrong_arity("hgetall");
    };

    let now = now_ms();
    let db = server.db_mut(client.selected_db);
    purge_expired_key(db, key, now);

    let Some(entry) = db.get(key) else {
        return CommandOutcome::reply(RespFrame::Array(vec![]));
    };
    let Some(hash) = entry.as_hash() else {
        return wrong_type_response();
    };

    let mut out = Vec::with_capacity(hash.len() * 2);
    for (field, fe) in hash.iter() {
        if !field_alive(fe, now) {
            continue;
        }
        out.push(RespFrame::BulkString(Some(field.clone())));
        out.push(RespFrame::BulkString(Some(fe.value.clone())));
    }

    CommandOutcome::reply(RespFrame::Array(out))
}

pub(super) fn cmd_hkeys(
    args: &[Bytes],
    server: &mut ServerState,
    client: &ClientState,
) -> CommandOutcome {
    let [key] = args else {
        return wrong_arity("hkeys");
    };

    let now = now_ms();
    let db = server.db_mut(client.selected_db);
    purge_expired_key(db, key, now);

    let Some(entry) = db.get(key) else {
        return CommandOutcome::reply(RespFrame::Array(vec![]));
    };
    let Some(hash) = entry.as_hash() else {
        return wrong_type_response();
    };

    CommandOutcome::reply(RespFrame::Array(
        hash.iter()
            .filter(|(_, fe)| field_alive(fe, now))
            .map(|(field, _)| RespFrame::BulkString(Some(field.clone())))
            .collect::<Vec<_>>(),
    ))
}

pub(super) fn cmd_hvals(
    args: &[Bytes],
    server: &mut ServerState,
    client: &ClientState,
) -> CommandOutcome {
    let [key] = args else {
        return wrong_arity("hvals");
    };

    let now = now_ms();
    let db = server.db_mut(client.selected_db);
    purge_expired_key(db, key, now);

    let Some(entry) = db.get(key) else {
        return CommandOutcome::reply(RespFrame::Array(vec![]));
    };
    let Some(hash) = entry.as_hash() else {
        return wrong_type_response();
    };

    CommandOutcome::reply(RespFrame::Array(
        hash.iter()
            .filter(|(_, fe)| field_alive(fe, now))
            .map(|(_, fe)| RespFrame::BulkString(Some(fe.value.clone())))
            .collect::<Vec<_>>(),
    ))
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
    let db = server.db_mut(client.selected_db);
    purge_expired_key(db, key, now);

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
    let db = server.db_mut(client.selected_db);
    purge_expired_key(db, key, now);

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
    let db = server.db_mut(client.selected_db);
    purge_expired_key(db, key, now);

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
    let db = server.db_mut(client.selected_db);
    purge_expired_key(db, key, now);

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

pub(super) fn cmd_hstrlen(
    args: &[Bytes],
    server: &mut ServerState,
    client: &ClientState,
) -> CommandOutcome {
    let [key, field] = args else {
        return wrong_arity("hstrlen");
    };

    let now = now_ms();
    let db = server.db_mut(client.selected_db);
    purge_expired_key(db, key, now);

    let Some(entry) = db.get(key) else {
        return CommandOutcome::reply(RespFrame::Integer(0));
    };
    let Some(hash) = entry.as_hash() else {
        return wrong_type_response();
    };

    let len = hash
        .get(field)
        .filter(|e| field_alive(e, now))
        .map_or(0usize, |e| e.value.len());
    CommandOutcome::reply(RespFrame::Integer(len as i64))
}

pub(super) fn cmd_hrandfield(
    args: &[Bytes],
    server: &mut ServerState,
    client: &ClientState,
) -> CommandOutcome {
    if args.is_empty() || args.len() > 3 {
        return wrong_arity("hrandfield");
    }

    let key = &args[0];
    let mut count: Option<i64> = None;
    let mut withvalues = false;

    if let Some(raw_count) = args.get(1) {
        let Some(parsed) = parse_i64(raw_count) else {
            return CommandOutcome::reply(err("ERR value is not an integer or out of range"));
        };
        count = Some(parsed);
    }

    if args.len() == 3 {
        if !args[2].eq_ignore_ascii_case(b"WITHVALUES") {
            return CommandOutcome::reply(err("ERR syntax error"));
        }
        if count.is_none() {
            return CommandOutcome::reply(err("ERR syntax error"));
        }
        withvalues = true;
    }

    let now = now_ms();
    let db = server.db_mut(client.selected_db);
    purge_expired_key(db, key, now);

    let Some(entry) = db.get(key) else {
        return if count.is_some() {
            CommandOutcome::reply(RespFrame::Array(vec![]))
        } else {
            CommandOutcome::reply(RespFrame::BulkString(None))
        };
    };
    let Some(hash) = entry.as_hash() else {
        return wrong_type_response();
    };

    let fields: Vec<Bytes> = hash
        .iter()
        .filter(|(_, fe)| field_alive(fe, now))
        .map(|(k, _)| k.clone())
        .collect();

    if fields.is_empty() {
        return if count.is_some() {
            CommandOutcome::reply(RespFrame::Array(vec![]))
        } else {
            CommandOutcome::reply(RespFrame::BulkString(None))
        };
    }

    let start = usize::try_from(now_us()).unwrap_or(0) % fields.len();

    match count {
        None => CommandOutcome::reply(RespFrame::BulkString(Some(fields[start].clone()))),
        Some(raw_count) if raw_count >= 0 => {
            let Ok(requested) = usize::try_from(raw_count) else {
                return CommandOutcome::reply(err("ERR value is not an integer or out of range"));
            };
            if requested > MAX_HASH_RANDOM_COUNT {
                return CommandOutcome::reply(err("ERR count is out of range"));
            }
            if requested == 0 {
                return CommandOutcome::reply(RespFrame::Array(vec![]));
            }

            let mut rotated = fields;
            rotated.rotate_left(start);
            let selected = rotated.into_iter().take(requested).collect::<Vec<_>>();

            if withvalues {
                let mut out = Vec::with_capacity(selected.len().saturating_mul(2));
                for field in selected {
                    out.push(RespFrame::BulkString(Some(field.clone())));
                    let value = hash
                        .get(&field)
                        .filter(|e| field_alive(e, now))
                        .map(|e| e.value.clone())
                        .unwrap_or_default();
                    out.push(RespFrame::BulkString(Some(value)));
                }
                CommandOutcome::reply(RespFrame::Array(out))
            } else {
                CommandOutcome::reply(RespFrame::Array(
                    selected
                        .into_iter()
                        .map(|field| RespFrame::BulkString(Some(field)))
                        .collect::<Vec<_>>(),
                ))
            }
        }
        Some(raw_count) => {
            let Ok(requested) = usize::try_from(raw_count.saturating_neg()) else {
                return CommandOutcome::reply(err("ERR value is not an integer or out of range"));
            };
            if requested > MAX_HASH_RANDOM_COUNT {
                return CommandOutcome::reply(err("ERR count is out of range"));
            }
            let selected = (0..requested)
                .map(|offset| {
                    let idx = (start + offset) % fields.len();
                    fields[idx].clone()
                })
                .collect::<Vec<_>>();

            if withvalues {
                let mut out = Vec::with_capacity(selected.len().saturating_mul(2));
                for field in selected {
                    out.push(RespFrame::BulkString(Some(field.clone())));
                    let value = hash
                        .get(&field)
                        .filter(|e| field_alive(e, now))
                        .map(|e| e.value.clone())
                        .unwrap_or_default();
                    out.push(RespFrame::BulkString(Some(value)));
                }
                CommandOutcome::reply(RespFrame::Array(out))
            } else {
                CommandOutcome::reply(RespFrame::Array(
                    selected
                        .into_iter()
                        .map(|field| RespFrame::BulkString(Some(field)))
                        .collect::<Vec<_>>(),
                ))
            }
        }
    }
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
    let db = server.db_mut(client.selected_db);
    purge_expired_key(db, key, now);

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

pub(super) fn cmd_hexists(
    args: &[Bytes],
    server: &mut ServerState,
    client: &ClientState,
) -> CommandOutcome {
    let [key, field] = args else {
        return wrong_arity("hexists");
    };

    let now = now_ms();
    let db = server.db_mut(client.selected_db);
    purge_expired_key(db, key, now);

    let Some(entry) = db.get(key) else {
        return CommandOutcome::reply(RespFrame::Integer(0));
    };
    let Some(hash) = entry.as_hash() else {
        return wrong_type_response();
    };

    let exists = hash.get(field).is_some_and(|e| field_alive(e, now));
    CommandOutcome::reply(RespFrame::Integer(if exists { 1 } else { 0 }))
}

pub(super) fn cmd_hlen(
    args: &[Bytes],
    server: &mut ServerState,
    client: &ClientState,
) -> CommandOutcome {
    let [key] = args else {
        return wrong_arity("hlen");
    };

    let now = now_ms();
    let db = server.db_mut(client.selected_db);
    purge_expired_key(db, key, now);

    let Some(entry) = db.get(key) else {
        return CommandOutcome::reply(RespFrame::Integer(0));
    };
    let Some(hash) = entry.as_hash() else {
        return wrong_type_response();
    };

    let count = hash.values().filter(|e| field_alive(e, now)).count();
    CommandOutcome::reply(RespFrame::Integer(count as i64))
}

pub(super) fn cmd_hscan(
    args: &[Bytes],
    server: &mut ServerState,
    client: &ClientState,
) -> CommandOutcome {
    let [key, cursor_raw, options @ ..] = args else {
        return wrong_arity("hscan");
    };

    let cursor = match parse_scan_cursor(cursor_raw) {
        Ok(cursor) => cursor,
        Err(response) => return CommandOutcome::reply(response),
    };

    let (pattern, count) = match parse_scan_match_count_options(options) {
        Ok(parsed) => parsed,
        Err(response) => return CommandOutcome::reply(response),
    };

    let now = now_ms();
    let db = server.db_mut(client.selected_db);
    purge_expired_key(db, key, now);

    let Some(entry) = db.get(key) else {
        return scan_reply(0, vec![]);
    };
    let Some(hash) = entry.as_hash() else {
        return wrong_type_response();
    };

    let mut entries: Vec<(Bytes, Bytes)> = hash
        .iter()
        .filter(|(_, fe)| field_alive(fe, now))
        .map(|(field, fe)| (field.clone(), fe.value.clone()))
        .collect();
    entries.sort_by(|(af, _), (bf, _)| af.cmp(bf));

    let (next_cursor, matched_indexes) =
        scan_collect_indexes(&entries, cursor, count, |(field, _)| {
            if let Some(pattern) = &pattern {
                glob_match(pattern, &String::from_utf8_lossy(field))
            } else {
                true
            }
        });

    let mut out = Vec::with_capacity(matched_indexes.len().saturating_mul(2));
    for idx in matched_indexes {
        let (field, value) = &entries[idx];
        out.push(RespFrame::BulkString(Some(field.clone())));
        out.push(RespFrame::BulkString(Some(value.clone())));
    }

    scan_reply(next_cursor, out)
}
