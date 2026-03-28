use bytes::Bytes;
use hashbrown::HashMap;

use ratatosk_resp::frame::RespFrame;

use crate::keyspace::{HashFieldEntry, ServerState, StoredValue, purge_expired_key};

use super::{
    ClientState, CommandOutcome,
    cmd_hash_ttl::{FieldExpireMode, parse_fields_suffix, to_absolute_ms},
    err, now_ms, parse_i64, wrong_arity, wrong_type_response,
};

pub(super) fn cmd_hgetdel(
    args: &[Bytes],
    server: &mut ServerState,
    client: &ClientState,
) -> CommandOutcome {
    if args.len() < 3 {
        return wrong_arity("hgetdel");
    }

    let key = &args[0];

    let fields = match parse_fields_suffix(args, 1) {
        Ok(f) => f,
        Err(resp) => return CommandOutcome::reply(resp),
    };

    let now = now_ms();
    let mut db = server.db_mut(client.selected_db);
    purge_expired_key(&mut db, key, now);

    let Some(stored) = db.get_mut(key) else {
        let results: Vec<RespFrame> = fields.iter().map(|_| RespFrame::BulkString(None)).collect();
        return CommandOutcome::reply(RespFrame::Array(results));
    };
    let Some(hash) = stored.as_hash_mut() else {
        return wrong_type_response();
    };

    let results: Vec<RespFrame> = fields
        .iter()
        .map(|field| {
            if let Some(entry) = hash.remove(field) {
                if entry.is_expired(now) {
                    RespFrame::BulkString(None)
                } else {
                    RespFrame::BulkString(Some(entry.value))
                }
            } else {
                RespFrame::BulkString(None)
            }
        })
        .collect();

    if hash.is_empty() {
        db.remove(key);
    }

    CommandOutcome::reply(RespFrame::Array(results))
}

pub(super) fn cmd_hgetex(
    args: &[Bytes],
    server: &mut ServerState,
    client: &ClientState,
) -> CommandOutcome {
    if args.len() < 3 {
        return wrong_arity("hgetex");
    }

    let key = &args[0];
    let mut cursor = 1usize;
    let policy = if let Some(opt) = args.get(cursor) {
        if opt.eq_ignore_ascii_case(b"FIELDS") {
            HgetexPolicy::None
        } else if opt.eq_ignore_ascii_case(b"PERSIST") {
            cursor += 1;
            HgetexPolicy::Persist
        } else if opt.eq_ignore_ascii_case(b"EX") {
            let Some(val_raw) = args.get(cursor + 1) else {
                return CommandOutcome::reply(err("ERR syntax error"));
            };
            let Some(val) = parse_i64(val_raw) else {
                return CommandOutcome::reply(err("ERR value is not an integer or out of range"));
            };
            if val <= 0 {
                return CommandOutcome::reply(err("ERR invalid expire time in 'hgetex' command"));
            }
            cursor += 2;
            HgetexPolicy::Expire(val, FieldExpireMode::RelativeSec)
        } else if opt.eq_ignore_ascii_case(b"PX") {
            let Some(val_raw) = args.get(cursor + 1) else {
                return CommandOutcome::reply(err("ERR syntax error"));
            };
            let Some(val) = parse_i64(val_raw) else {
                return CommandOutcome::reply(err("ERR value is not an integer or out of range"));
            };
            if val <= 0 {
                return CommandOutcome::reply(err("ERR invalid expire time in 'hgetex' command"));
            }
            cursor += 2;
            HgetexPolicy::Expire(val, FieldExpireMode::RelativeMs)
        } else if opt.eq_ignore_ascii_case(b"EXAT") {
            let Some(val_raw) = args.get(cursor + 1) else {
                return CommandOutcome::reply(err("ERR syntax error"));
            };
            let Some(val) = parse_i64(val_raw) else {
                return CommandOutcome::reply(err("ERR value is not an integer or out of range"));
            };
            if val <= 0 {
                return CommandOutcome::reply(err("ERR invalid expire time in 'hgetex' command"));
            }
            cursor += 2;
            HgetexPolicy::Expire(val, FieldExpireMode::AbsoluteSec)
        } else if opt.eq_ignore_ascii_case(b"PXAT") {
            let Some(val_raw) = args.get(cursor + 1) else {
                return CommandOutcome::reply(err("ERR syntax error"));
            };
            let Some(val) = parse_i64(val_raw) else {
                return CommandOutcome::reply(err("ERR value is not an integer or out of range"));
            };
            if val <= 0 {
                return CommandOutcome::reply(err("ERR invalid expire time in 'hgetex' command"));
            }
            cursor += 2;
            HgetexPolicy::Expire(val, FieldExpireMode::AbsoluteMs)
        } else {
            return CommandOutcome::reply(err("ERR syntax error"));
        }
    } else {
        return wrong_arity("hgetex");
    };

    let fields = match parse_fields_suffix(args, cursor) {
        Ok(f) => f,
        Err(resp) => return CommandOutcome::reply(resp),
    };

    let now = now_ms();
    let mut db = server.db_mut(client.selected_db);
    purge_expired_key(&mut db, key, now);

    let Some(stored) = db.get_mut(key) else {
        let results: Vec<RespFrame> = fields.iter().map(|_| RespFrame::BulkString(None)).collect();
        return CommandOutcome::reply(RespFrame::Array(results));
    };
    let Some(hash) = stored.as_hash_mut() else {
        return wrong_type_response();
    };

    let expire_at_ms = match &policy {
        HgetexPolicy::None | HgetexPolicy::Persist => None,
        HgetexPolicy::Expire(raw, mode) => {
            let Some(at) = to_absolute_ms(*raw, *mode, now) else {
                return CommandOutcome::reply(err("ERR invalid expire time in 'hgetex' command"));
            };
            Some(at)
        }
    };

    let results: Vec<RespFrame> = fields
        .iter()
        .map(|field| {
            let Some(entry) = hash.get_mut(field) else {
                return RespFrame::BulkString(None);
            };
            if entry.is_expired(now) {
                return RespFrame::BulkString(None);
            }
            let value = entry.value.clone();
            match &policy {
                HgetexPolicy::None => {}
                HgetexPolicy::Persist => entry.expire_at_ms = None,
                HgetexPolicy::Expire(_, _) => entry.expire_at_ms = expire_at_ms,
            }
            RespFrame::BulkString(Some(value))
        })
        .collect();

    CommandOutcome::reply(RespFrame::Array(results))
}

pub(super) fn cmd_hsetex(
    args: &[Bytes],
    server: &mut ServerState,
    client: &ClientState,
) -> CommandOutcome {
    if args.len() < 5 {
        return wrong_arity("hsetex");
    }

    let key = &args[0];

    let Some(seconds) = parse_i64(&args[1]) else {
        return CommandOutcome::reply(err("ERR value is not an integer or out of range"));
    };
    if seconds <= 0 {
        return CommandOutcome::reply(err("ERR invalid expire time in 'hsetex' command"));
    }

    let Some(numfields) = parse_i64(&args[2]) else {
        return CommandOutcome::reply(err("ERR value is not an integer or out of range"));
    };
    if numfields <= 0 {
        return CommandOutcome::reply(err("ERR numfields must be positive"));
    }
    let n = numfields as usize;

    let pairs_start = 3usize;
    let expected_pair_count = n * 2;
    if pairs_start + expected_pair_count != args.len() {
        return wrong_arity("hsetex");
    }

    let now = now_ms();
    let Some(expire_at_ms) = now.checked_add(seconds.saturating_mul(1000)) else {
        return CommandOutcome::reply(err("ERR invalid expire time in 'hsetex' command"));
    };

    let mut db = server.db_mut(client.selected_db);
    purge_expired_key(&mut db, key, now);

    if !db.contains_key(key) {
        let mut hash = HashMap::with_capacity(n);
        let mut idx = pairs_start;
        while idx < args.len() {
            hash.insert(
                args[idx].clone(),
                HashFieldEntry::with_ttl(args[idx + 1].clone(), expire_at_ms),
            );
            idx += 2;
        }
        let added = hash.len() as i64;
        db.insert(key.clone(), StoredValue::hash(hash, None));
        return CommandOutcome::reply(RespFrame::Integer(added));
    }

    let Some(stored) = db.get_mut(key) else {
        return CommandOutcome::reply(RespFrame::Integer(0));
    };
    let Some(hash) = stored.as_hash_mut() else {
        return wrong_type_response();
    };

    let mut added = 0i64;
    let mut idx = pairs_start;
    while idx < args.len() {
        let field = &args[idx];
        let value = &args[idx + 1];
        let is_new = !hash.contains_key(field);
        hash.insert(
            field.clone(),
            HashFieldEntry::with_ttl(value.clone(), expire_at_ms),
        );
        if is_new {
            added += 1;
        }
        idx += 2;
    }

    CommandOutcome::reply(RespFrame::Integer(added))
}

#[derive(Debug)]
enum HgetexPolicy {
    None,
    Persist,
    Expire(i64, FieldExpireMode),
}
