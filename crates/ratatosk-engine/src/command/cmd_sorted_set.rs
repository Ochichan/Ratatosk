use bytes::Bytes;

use ratatosk_resp::frame::RespFrame;

use crate::keyspace::{ServerState, SortedSet, StoredValue, purge_expired_key};
use crate::object::{format_f64_for_redis, parse_f64};

use super::{
    ClientState, CommandOutcome, err, now_ms, to_uppercase_bytes, wrong_arity, wrong_type_response,
};

pub(super) const MAX_ZSET_NUMKEYS: usize = 10_000;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Collect all entries from a sorted set in ascending order by score.
pub(super) fn sorted_entries(zset: &SortedSet) -> Vec<(Bytes, f64)> {
    zset.by_score
        .keys()
        .map(|e| (e.member.clone(), e.score.value()))
        .collect()
}

/// Build a flat WITHSCORES response array: [member, score, member, score, ...]
pub(super) fn entries_to_resp(entries: &[(Bytes, f64)], with_scores: bool) -> Vec<RespFrame> {
    let cap = if with_scores {
        entries.len().saturating_mul(2)
    } else {
        entries.len()
    };
    let mut out = Vec::with_capacity(cap);
    for (member, score) in entries {
        out.push(RespFrame::BulkString(Some(member.clone())));
        if with_scores {
            out.push(RespFrame::BulkString(Some(format_f64_for_redis(*score))));
        }
    }
    out
}

// ---------------------------------------------------------------------------
// 2a: Basic CRUD
// ---------------------------------------------------------------------------

pub(super) fn cmd_zadd(
    args: &[Bytes],
    server: &mut ServerState,
    client: &ClientState,
) -> CommandOutcome {
    if args.len() < 3 {
        return wrong_arity("zadd");
    }

    let key = &args[0];

    // Parse flags
    let mut nx = false;
    let mut xx = false;
    let mut gt = false;
    let mut lt = false;
    let mut ch = false;
    let mut idx = 1usize;

    loop {
        if idx >= args.len() {
            return wrong_arity("zadd");
        }
        let upper = to_uppercase_bytes(&args[idx]);
        match upper.as_slice() {
            b"NX" => {
                nx = true;
                idx += 1;
            }
            b"XX" => {
                xx = true;
                idx += 1;
            }
            b"GT" => {
                gt = true;
                idx += 1;
            }
            b"LT" => {
                lt = true;
                idx += 1;
            }
            b"CH" => {
                ch = true;
                idx += 1;
            }
            _ => break,
        }
    }

    if nx && xx {
        return CommandOutcome::reply(err(
            "ERR XX and NX options at the same time are not compatible",
        ));
    }
    if nx && (gt || lt) {
        return CommandOutcome::reply(err(
            "ERR GT, LT, and NX options at the same time are not compatible",
        ));
    }

    // Remaining args must be score-member pairs
    let pairs_slice = &args[idx..];
    if pairs_slice.is_empty() || pairs_slice.len() % 2 != 0 {
        return wrong_arity("zadd");
    }

    // Pre-parse all scores
    let mut pairs = Vec::with_capacity(pairs_slice.len() / 2);
    let mut i = 0;
    while i < pairs_slice.len() {
        let Some(score) = parse_f64(&pairs_slice[i]) else {
            return CommandOutcome::reply(err("ERR value is not a valid float"));
        };
        if score.is_nan() {
            return CommandOutcome::reply(err("ERR value is not a valid float"));
        }
        let member = pairs_slice[i + 1].clone();
        pairs.push((score, member));
        i += 2;
    }

    let now = now_ms();
    let mut db = server.db_mut(client.selected_db);
    purge_expired_key(&mut db, key, now);

    if !db.contains_key(key) {
        if xx {
            // XX: only update existing -- nothing to do on empty set
            return CommandOutcome::reply(RespFrame::Integer(0));
        }
        let mut zset = SortedSet::default();
        let mut added = 0i64;
        for (score, member) in &pairs {
            if zset.insert(member.clone(), *score) {
                added += 1;
            }
        }
        db.insert(key.clone(), StoredValue::sorted_set(zset, None));
        return CommandOutcome::reply(RespFrame::Integer(added));
    }

    let Some(entry) = db.get_mut(key) else {
        return CommandOutcome::reply(RespFrame::Integer(0));
    };
    let Some(zset) = entry.as_sorted_set_mut() else {
        return wrong_type_response();
    };

    let mut added = 0i64;
    let mut changed = 0i64;

    for (score, member) in &pairs {
        let existing_score = zset.score(member);

        match existing_score {
            Some(old_score) => {
                // Member exists
                if nx {
                    // NX: skip existing
                    continue;
                }
                let new_score = *score;

                // GT: only update if new > old
                if gt && new_score <= old_score {
                    continue;
                }
                // LT: only update if new < old
                if lt && new_score >= old_score {
                    continue;
                }

                let _ = new_score; // suppress warning
                if (new_score - old_score).abs() > f64::EPSILON
                    || new_score.to_bits() != old_score.to_bits()
                {
                    zset.insert(member.clone(), new_score);
                    changed += 1;
                }
            }
            None => {
                // Member does not exist
                if xx {
                    // XX: skip new members
                    continue;
                }
                zset.insert(member.clone(), *score);
                added += 1;
                changed += 1;
            }
        }
    }

    let count = if ch { changed } else { added };
    CommandOutcome::reply(RespFrame::Integer(count))
}

pub(super) fn cmd_zrem(
    args: &[Bytes],
    server: &mut ServerState,
    client: &ClientState,
) -> CommandOutcome {
    if args.len() < 2 {
        return wrong_arity("zrem");
    }

    let key = &args[0];
    let now = now_ms();
    let mut db = server.db_mut(client.selected_db);
    purge_expired_key(&mut db, key, now);

    let Some(entry) = db.get_mut(key) else {
        return CommandOutcome::reply(RespFrame::Integer(0));
    };
    let Some(zset) = entry.as_sorted_set_mut() else {
        return wrong_type_response();
    };

    let mut removed = 0i64;
    for member in &args[1..] {
        if zset.remove(member) {
            removed += 1;
        }
    }

    if zset.is_empty() {
        db.remove(key);
    }

    CommandOutcome::reply(RespFrame::Integer(removed))
}

pub(super) fn cmd_zscore(
    args: &[Bytes],
    server: &mut ServerState,
    client: &ClientState,
) -> CommandOutcome {
    let [key, member] = args else {
        return wrong_arity("zscore");
    };

    let now = now_ms();
    let mut db = server.db_mut(client.selected_db);
    purge_expired_key(&mut db, key, now);

    let Some(entry) = db.get(key) else {
        return CommandOutcome::reply(RespFrame::BulkString(None));
    };
    let Some(zset) = entry.as_sorted_set() else {
        return wrong_type_response();
    };

    match zset.score(member) {
        Some(score) => {
            CommandOutcome::reply(RespFrame::BulkString(Some(format_f64_for_redis(score))))
        }
        None => CommandOutcome::reply(RespFrame::BulkString(None)),
    }
}

pub(super) fn cmd_zcard(
    args: &[Bytes],
    server: &mut ServerState,
    client: &ClientState,
) -> CommandOutcome {
    let [key] = args else {
        return wrong_arity("zcard");
    };

    let now = now_ms();
    let mut db = server.db_mut(client.selected_db);
    purge_expired_key(&mut db, key, now);

    let Some(entry) = db.get(key) else {
        return CommandOutcome::reply(RespFrame::Integer(0));
    };
    let Some(zset) = entry.as_sorted_set() else {
        return wrong_type_response();
    };

    CommandOutcome::reply(RespFrame::Integer(zset.len() as i64))
}

pub(super) fn cmd_zincrby(
    args: &[Bytes],
    server: &mut ServerState,
    client: &ClientState,
) -> CommandOutcome {
    let [key, increment_raw, member] = args else {
        return wrong_arity("zincrby");
    };

    let Some(increment) = parse_f64(increment_raw) else {
        return CommandOutcome::reply(err("ERR value is not a valid float"));
    };
    if increment.is_nan() {
        return CommandOutcome::reply(err("ERR value is not a valid float"));
    }

    let now = now_ms();
    let mut db = server.db_mut(client.selected_db);
    purge_expired_key(&mut db, key, now);

    if !db.contains_key(key) {
        let mut zset = SortedSet::default();
        zset.insert(member.clone(), increment);
        db.insert(key.clone(), StoredValue::sorted_set(zset, None));
        return CommandOutcome::reply(RespFrame::BulkString(Some(format_f64_for_redis(increment))));
    }

    let Some(entry) = db.get_mut(key) else {
        return CommandOutcome::reply(RespFrame::BulkString(Some(format_f64_for_redis(increment))));
    };
    let Some(zset) = entry.as_sorted_set_mut() else {
        return wrong_type_response();
    };

    let old = zset.score(member).unwrap_or(0.0);
    let new_score = old + increment;
    if new_score.is_nan() {
        return CommandOutcome::reply(err("ERR resulting score is not a number (NaN)"));
    }
    zset.insert(member.clone(), new_score);
    CommandOutcome::reply(RespFrame::BulkString(Some(format_f64_for_redis(new_score))))
}

pub(super) fn cmd_zmscore(
    args: &[Bytes],
    server: &mut ServerState,
    client: &ClientState,
) -> CommandOutcome {
    if args.len() < 2 {
        return wrong_arity("zmscore");
    }

    let key = &args[0];
    let members = &args[1..];

    let now = now_ms();
    let mut db = server.db_mut(client.selected_db);
    purge_expired_key(&mut db, key, now);

    let Some(entry) = db.get(key) else {
        let out: Vec<RespFrame> = members
            .iter()
            .map(|_| RespFrame::BulkString(None))
            .collect();
        return CommandOutcome::reply(RespFrame::Array(out));
    };
    let Some(zset) = entry.as_sorted_set() else {
        return wrong_type_response();
    };

    let out: Vec<RespFrame> = members
        .iter()
        .map(|m| match zset.score(m) {
            Some(s) => RespFrame::BulkString(Some(format_f64_for_redis(s))),
            None => RespFrame::BulkString(None),
        })
        .collect();

    CommandOutcome::reply(RespFrame::Array(out))
}
