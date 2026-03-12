use bytes::Bytes;

use glob_match::glob_match;
use ratatosk_resp::frame::RespFrame;

use crate::keyspace::{
    LexBound, ScoreBound, ServerState, SortedSet, SortedSetScore, StoredValue, purge_expired_key,
};
use crate::object::{format_f64_for_redis, now_us, parse_f64};

use super::cmd_list::{blocking_deadline_ms, parse_blocking_timeout_seconds};
use super::{
    ClientState, CommandOutcome, err, now_ms, parse_i64, to_uppercase_bytes, wrong_arity,
    wrong_type_response,
};
use super::{parse_scan_cursor, parse_scan_match_count_options, scan_collect_indexes, scan_reply};

const MAX_ZSET_POP_COUNT: usize = 100_000;
const MAX_ZSET_NUMKEYS: usize = 10_000;
const MAX_ZSET_RANDOM_COUNT: usize = 100_000;

fn blocking_watch_keys(client: &ClientState, keys: &[Bytes]) -> Vec<(usize, Bytes)> {
    keys.iter()
        .cloned()
        .map(|key| (client.selected_db(), key))
        .collect()
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn parse_score_bound(raw: &Bytes) -> Option<ScoreBound> {
    let s = std::str::from_utf8(raw).ok()?;
    match s {
        "-inf" => Some(ScoreBound::NegInf),
        "+inf" | "inf" => Some(ScoreBound::PosInf),
        _ if s.starts_with('(') => {
            let val = s[1..].parse::<f64>().ok()?;
            Some(ScoreBound::Exclusive(val))
        }
        _ => {
            let val = s.parse::<f64>().ok()?;
            Some(ScoreBound::Inclusive(val))
        }
    }
}

fn parse_lex_bound(raw: &Bytes) -> Option<LexBound> {
    if raw == b"-" as &[u8] {
        return Some(LexBound::NegInf);
    }
    if raw == b"+" as &[u8] {
        return Some(LexBound::PosInf);
    }
    if raw.starts_with(b"[") {
        return Some(LexBound::Inclusive(Bytes::copy_from_slice(&raw[1..])));
    }
    if raw.starts_with(b"(") {
        return Some(LexBound::Exclusive(Bytes::copy_from_slice(&raw[1..])));
    }
    None
}

fn score_in_range(score: f64, min: &ScoreBound, max: &ScoreBound) -> bool {
    let above_min = match min {
        ScoreBound::NegInf => true,
        ScoreBound::Inclusive(v) => score >= *v,
        ScoreBound::Exclusive(v) => score > *v,
        ScoreBound::PosInf => false,
    };
    let below_max = match max {
        ScoreBound::PosInf => true,
        ScoreBound::Inclusive(v) => score <= *v,
        ScoreBound::Exclusive(v) => score < *v,
        ScoreBound::NegInf => false,
    };
    above_min && below_max
}

fn member_in_lex_range(member: &Bytes, min: &LexBound, max: &LexBound) -> bool {
    let above_min = match min {
        LexBound::NegInf => true,
        LexBound::Inclusive(v) => member >= v,
        LexBound::Exclusive(v) => member > v,
        LexBound::PosInf => false,
    };
    let below_max = match max {
        LexBound::PosInf => true,
        LexBound::Inclusive(v) => member <= v,
        LexBound::Exclusive(v) => member < v,
        LexBound::NegInf => false,
    };
    above_min && below_max
}

#[derive(Debug, Clone, Copy)]
enum Aggregate {
    Sum,
    Min,
    Max,
}

/// Parse WEIGHTS and AGGREGATE options that follow the keys in set operations.
/// `option_start` is the index in `args` where option parsing begins (after keys).
fn parse_weights_aggregate(
    args: &[Bytes],
    option_start: usize,
    numkeys: usize,
) -> Result<(Vec<f64>, Aggregate), RespFrame> {
    let mut weights = vec![1.0f64; numkeys];
    let mut aggregate = Aggregate::Sum;

    let mut idx = option_start;
    while idx < args.len() {
        let upper = to_uppercase_bytes(&args[idx]);
        match upper.as_slice() {
            b"WEIGHTS" => {
                if idx + numkeys >= args.len() {
                    return Err(err("ERR syntax error"));
                }
                for (i, w_raw) in args[idx + 1..idx + 1 + numkeys].iter().enumerate() {
                    let Some(w) = parse_f64(w_raw) else {
                        return Err(err("ERR weight value is not a float"));
                    };
                    weights[i] = w;
                }
                idx += 1 + numkeys;
            }
            b"AGGREGATE" => {
                if idx + 1 >= args.len() {
                    return Err(err("ERR syntax error"));
                }
                let agg_upper = to_uppercase_bytes(&args[idx + 1]);
                aggregate = match agg_upper.as_slice() {
                    b"SUM" => Aggregate::Sum,
                    b"MIN" => Aggregate::Min,
                    b"MAX" => Aggregate::Max,
                    _ => return Err(err("ERR syntax error")),
                };
                idx += 2;
            }
            _ => return Err(err("ERR syntax error")),
        }
    }

    Ok((weights, aggregate))
}

fn aggregate_score(agg: Aggregate, a: f64, b: f64) -> f64 {
    match agg {
        Aggregate::Sum => a + b,
        Aggregate::Min => a.min(b),
        Aggregate::Max => a.max(b),
    }
}

/// Collect all entries from a sorted set in ascending order by score.
fn sorted_entries(zset: &SortedSet) -> Vec<(Bytes, f64)> {
    zset.by_score
        .keys()
        .map(|e| (e.member.clone(), e.score.value()))
        .collect()
}

/// Build a flat WITHSCORES response array: [member, score, member, score, ...]
fn entries_to_resp(entries: &[(Bytes, f64)], with_scores: bool) -> Vec<RespFrame> {
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

fn build_blocking_frame(command_name: &str, args: &[Bytes]) -> RespFrame {
    let mut parts = Vec::with_capacity(1 + args.len());
    parts.push(RespFrame::BulkString(Some(Bytes::copy_from_slice(
        command_name.as_bytes(),
    ))));
    for arg in args {
        parts.push(RespFrame::BulkString(Some(arg.clone())));
    }
    RespFrame::Array(parts)
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
    let db = server.db_mut(client.selected_db);
    purge_expired_key(db, key, now);

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
    let db = server.db_mut(client.selected_db);
    purge_expired_key(db, key, now);

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
    let db = server.db_mut(client.selected_db);
    purge_expired_key(db, key, now);

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
    let db = server.db_mut(client.selected_db);
    purge_expired_key(db, key, now);

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
    let db = server.db_mut(client.selected_db);
    purge_expired_key(db, key, now);

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
    let db = server.db_mut(client.selected_db);
    purge_expired_key(db, key, now);

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

// ---------------------------------------------------------------------------
// 2b: Range Queries — shared helpers
// ---------------------------------------------------------------------------

type ZrangeOptions = (RangeMode, bool, Option<(i64, i64)>, bool);

#[derive(Debug, Clone, Copy)]
#[allow(clippy::enum_variant_names)]
enum RangeMode {
    ByRank,
    ByScore,
    ByLex,
}

/// Core range collection from a sorted set. Returns elements in the requested order.
#[allow(clippy::too_many_arguments)]
fn zrange_collect(
    zset: &SortedSet,
    args: &[Bytes],
    min_raw: &Bytes,
    max_raw: &Bytes,
    mode: RangeMode,
    rev: bool,
    limit_offset: Option<(i64, i64)>,
    with_scores: bool,
) -> Result<Vec<(Bytes, f64)>, RespFrame> {
    let mut selected: Vec<(Bytes, f64)> = match mode {
        RangeMode::ByRank => {
            let Some(start_i) = parse_i64(min_raw) else {
                return Err(err("ERR value is not an integer or out of range"));
            };
            let Some(stop_i) = parse_i64(max_raw) else {
                return Err(err("ERR value is not an integer or out of range"));
            };

            let len = zset.len();
            if len == 0 {
                Vec::new()
            } else {
                let len_i64 = len as i64;
                let start = if start_i < 0 {
                    (start_i + len_i64).max(0) as usize
                } else {
                    usize::try_from(start_i).unwrap_or(usize::MAX)
                };
                let mut stop = if stop_i < 0 {
                    (stop_i + len_i64).max(0) as usize
                } else {
                    usize::try_from(stop_i).unwrap_or(usize::MAX)
                };

                stop = stop.min(len.saturating_sub(1));
                if start > stop || start >= len {
                    Vec::new()
                } else {
                    let take_len = stop.saturating_sub(start).saturating_add(1);
                    if rev {
                        let skip = len.saturating_sub(1).saturating_sub(stop);
                        zset.by_score
                            .keys()
                            .rev()
                            .skip(skip)
                            .take(take_len)
                            .map(|entry| (entry.member.clone(), entry.score.value()))
                            .collect()
                    } else {
                        zset.by_score
                            .keys()
                            .skip(start)
                            .take(take_len)
                            .map(|entry| (entry.member.clone(), entry.score.value()))
                            .collect()
                    }
                }
            }
        }
        RangeMode::ByScore => {
            let (smin, smax) = if rev {
                // In REV mode for ZRANGE, min and max are swapped semantically:
                // ZRANGE key max min BYSCORE REV
                let Some(high) = parse_score_bound(min_raw) else {
                    return Err(err("ERR min or max is not a float"));
                };
                let Some(low) = parse_score_bound(max_raw) else {
                    return Err(err("ERR min or max is not a float"));
                };
                (low, high)
            } else {
                let Some(low) = parse_score_bound(min_raw) else {
                    return Err(err("ERR min or max is not a float"));
                };
                let Some(high) = parse_score_bound(max_raw) else {
                    return Err(err("ERR min or max is not a float"));
                };
                (low, high)
            };

            if rev {
                zset.by_score
                    .keys()
                    .rev()
                    .filter(|entry| score_in_range(entry.score.value(), &smin, &smax))
                    .map(|entry| (entry.member.clone(), entry.score.value()))
                    .collect()
            } else {
                zset.by_score
                    .keys()
                    .filter(|entry| score_in_range(entry.score.value(), &smin, &smax))
                    .map(|entry| (entry.member.clone(), entry.score.value()))
                    .collect()
            }
        }
        RangeMode::ByLex => {
            let (lmin, lmax) = if rev {
                let Some(high) = parse_lex_bound(min_raw) else {
                    return Err(err("ERR min or max is not a valid string range item"));
                };
                let Some(low) = parse_lex_bound(max_raw) else {
                    return Err(err("ERR min or max is not a valid string range item"));
                };
                (low, high)
            } else {
                let Some(low) = parse_lex_bound(min_raw) else {
                    return Err(err("ERR min or max is not a valid string range item"));
                };
                let Some(high) = parse_lex_bound(max_raw) else {
                    return Err(err("ERR min or max is not a valid string range item"));
                };
                (low, high)
            };

            if rev {
                zset.by_score
                    .keys()
                    .rev()
                    .filter(|entry| member_in_lex_range(&entry.member, &lmin, &lmax))
                    .map(|entry| (entry.member.clone(), entry.score.value()))
                    .collect()
            } else {
                zset.by_score
                    .keys()
                    .filter(|entry| member_in_lex_range(&entry.member, &lmin, &lmax))
                    .map(|entry| (entry.member.clone(), entry.score.value()))
                    .collect()
            }
        }
    };

    // Apply LIMIT if present
    if let Some((offset, count)) = limit_offset {
        if offset < 0 {
            return Err(err("ERR value is not an integer or out of range"));
        }
        let off = offset as usize;
        if off >= selected.len() {
            selected.clear();
        } else {
            if off > 0 {
                selected.drain(0..off);
            }
            if count >= 0 {
                selected.truncate(count as usize);
            }
            // count < 0 means no limit (Redis behavior)
        }
    }

    let _ = (args, with_scores); // mark as used
    Ok(selected)
}

/// Parse the unified ZRANGE options tail: [BYSCORE|BYLEX] [REV] [LIMIT offset count] [WITHSCORES]
fn parse_zrange_options(options: &[Bytes]) -> Result<ZrangeOptions, RespFrame> {
    let mut mode = RangeMode::ByRank;
    let mut rev = false;
    let mut limit: Option<(i64, i64)> = None;
    let mut with_scores = false;

    let mut idx = 0usize;
    while idx < options.len() {
        let option = &options[idx];
        if option.eq_ignore_ascii_case(b"BYSCORE") {
            mode = RangeMode::ByScore;
            idx += 1;
            continue;
        }
        if option.eq_ignore_ascii_case(b"BYLEX") {
            mode = RangeMode::ByLex;
            idx += 1;
            continue;
        }
        if option.eq_ignore_ascii_case(b"REV") {
            rev = true;
            idx += 1;
            continue;
        }
        if option.eq_ignore_ascii_case(b"LIMIT") {
            if idx + 2 >= options.len() {
                return Err(err("ERR syntax error"));
            }
            let Some(offset) = parse_i64(&options[idx + 1]) else {
                return Err(err("ERR value is not an integer or out of range"));
            };
            let Some(count) = parse_i64(&options[idx + 2]) else {
                return Err(err("ERR value is not an integer or out of range"));
            };
            limit = Some((offset, count));
            idx += 3;
            continue;
        }
        if option.eq_ignore_ascii_case(b"WITHSCORES") {
            with_scores = true;
            idx += 1;
            continue;
        }

        return Err(err("ERR syntax error"));
    }

    Ok((mode, rev, limit, with_scores))
}

pub(super) fn cmd_zrange(
    args: &[Bytes],
    server: &mut ServerState,
    client: &ClientState,
) -> CommandOutcome {
    if args.len() < 3 {
        return wrong_arity("zrange");
    }

    let key = &args[0];
    let min_raw = &args[1];
    let max_raw = &args[2];
    let options = &args[3..];

    let (mode, rev, limit, with_scores) = match parse_zrange_options(options) {
        Ok(v) => v,
        Err(e) => return CommandOutcome::reply(e),
    };

    let now = now_ms();
    let db = server.db_mut(client.selected_db);
    purge_expired_key(db, key, now);

    let Some(entry) = db.get(key) else {
        return CommandOutcome::reply(RespFrame::Array(vec![]));
    };
    let Some(zset) = entry.as_sorted_set() else {
        return wrong_type_response();
    };

    let selected = match zrange_collect(zset, args, min_raw, max_raw, mode, rev, limit, with_scores)
    {
        Ok(v) => v,
        Err(e) => return CommandOutcome::reply(e),
    };

    CommandOutcome::reply(RespFrame::Array(entries_to_resp(&selected, with_scores)))
}

pub(super) fn cmd_zrangebyscore(
    args: &[Bytes],
    server: &mut ServerState,
    client: &ClientState,
) -> CommandOutcome {
    if args.len() < 3 {
        return wrong_arity("zrangebyscore");
    }

    let key = &args[0];
    let min_raw = &args[1];
    let max_raw = &args[2];

    let mut with_scores = false;
    let mut limit: Option<(i64, i64)> = None;
    let mut idx = 3usize;

    while idx < args.len() {
        let upper = to_uppercase_bytes(&args[idx]);
        match upper.as_slice() {
            b"WITHSCORES" => {
                with_scores = true;
                idx += 1;
            }
            b"LIMIT" => {
                if idx + 2 >= args.len() {
                    return CommandOutcome::reply(err("ERR syntax error"));
                }
                let Some(offset) = parse_i64(&args[idx + 1]) else {
                    return CommandOutcome::reply(err(
                        "ERR value is not an integer or out of range",
                    ));
                };
                let Some(count) = parse_i64(&args[idx + 2]) else {
                    return CommandOutcome::reply(err(
                        "ERR value is not an integer or out of range",
                    ));
                };
                limit = Some((offset, count));
                idx += 3;
            }
            _ => return CommandOutcome::reply(err("ERR syntax error")),
        }
    }

    let now = now_ms();
    let db = server.db_mut(client.selected_db);
    purge_expired_key(db, key, now);

    let Some(entry) = db.get(key) else {
        return CommandOutcome::reply(RespFrame::Array(vec![]));
    };
    let Some(zset) = entry.as_sorted_set() else {
        return wrong_type_response();
    };

    let selected = match zrange_collect(
        zset,
        args,
        min_raw,
        max_raw,
        RangeMode::ByScore,
        false,
        limit,
        with_scores,
    ) {
        Ok(v) => v,
        Err(e) => return CommandOutcome::reply(e),
    };

    CommandOutcome::reply(RespFrame::Array(entries_to_resp(&selected, with_scores)))
}

pub(super) fn cmd_zrevrangebyscore(
    args: &[Bytes],
    server: &mut ServerState,
    client: &ClientState,
) -> CommandOutcome {
    // ZREVRANGEBYSCORE key max min [WITHSCORES] [LIMIT offset count]
    if args.len() < 3 {
        return wrong_arity("zrevrangebyscore");
    }

    let key = &args[0];
    let max_raw = &args[1]; // note: max comes first
    let min_raw = &args[2];

    let mut with_scores = false;
    let mut limit: Option<(i64, i64)> = None;
    let mut idx = 3usize;

    while idx < args.len() {
        let upper = to_uppercase_bytes(&args[idx]);
        match upper.as_slice() {
            b"WITHSCORES" => {
                with_scores = true;
                idx += 1;
            }
            b"LIMIT" => {
                if idx + 2 >= args.len() {
                    return CommandOutcome::reply(err("ERR syntax error"));
                }
                let Some(offset) = parse_i64(&args[idx + 1]) else {
                    return CommandOutcome::reply(err(
                        "ERR value is not an integer or out of range",
                    ));
                };
                let Some(count) = parse_i64(&args[idx + 2]) else {
                    return CommandOutcome::reply(err(
                        "ERR value is not an integer or out of range",
                    ));
                };
                limit = Some((offset, count));
                idx += 3;
            }
            _ => return CommandOutcome::reply(err("ERR syntax error")),
        }
    }

    let now = now_ms();
    let db = server.db_mut(client.selected_db);
    purge_expired_key(db, key, now);

    let Some(entry) = db.get(key) else {
        return CommandOutcome::reply(RespFrame::Array(vec![]));
    };
    let Some(zset) = entry.as_sorted_set() else {
        return wrong_type_response();
    };

    // For ZREVRANGEBYSCORE, min_raw and max_raw are already correct: min is min, max is max.
    // We use rev=true to reverse the result.
    let selected = match zrange_collect(
        zset,
        args,
        min_raw,
        max_raw,
        RangeMode::ByScore,
        true,
        limit,
        with_scores,
    ) {
        Ok(v) => v,
        Err(e) => return CommandOutcome::reply(e),
    };

    CommandOutcome::reply(RespFrame::Array(entries_to_resp(&selected, with_scores)))
}

pub(super) fn cmd_zrangebylex(
    args: &[Bytes],
    server: &mut ServerState,
    client: &ClientState,
) -> CommandOutcome {
    if args.len() < 3 {
        return wrong_arity("zrangebylex");
    }

    let key = &args[0];
    let min_raw = &args[1];
    let max_raw = &args[2];

    let mut limit: Option<(i64, i64)> = None;
    let mut idx = 3usize;

    while idx < args.len() {
        let upper = to_uppercase_bytes(&args[idx]);
        match upper.as_slice() {
            b"LIMIT" => {
                if idx + 2 >= args.len() {
                    return CommandOutcome::reply(err("ERR syntax error"));
                }
                let Some(offset) = parse_i64(&args[idx + 1]) else {
                    return CommandOutcome::reply(err(
                        "ERR value is not an integer or out of range",
                    ));
                };
                let Some(count) = parse_i64(&args[idx + 2]) else {
                    return CommandOutcome::reply(err(
                        "ERR value is not an integer or out of range",
                    ));
                };
                limit = Some((offset, count));
                idx += 3;
            }
            _ => return CommandOutcome::reply(err("ERR syntax error")),
        }
    }

    let now = now_ms();
    let db = server.db_mut(client.selected_db);
    purge_expired_key(db, key, now);

    let Some(entry) = db.get(key) else {
        return CommandOutcome::reply(RespFrame::Array(vec![]));
    };
    let Some(zset) = entry.as_sorted_set() else {
        return wrong_type_response();
    };

    let selected = match zrange_collect(
        zset,
        args,
        min_raw,
        max_raw,
        RangeMode::ByLex,
        false,
        limit,
        false,
    ) {
        Ok(v) => v,
        Err(e) => return CommandOutcome::reply(e),
    };

    CommandOutcome::reply(RespFrame::Array(entries_to_resp(&selected, false)))
}

pub(super) fn cmd_zrevrangebylex(
    args: &[Bytes],
    server: &mut ServerState,
    client: &ClientState,
) -> CommandOutcome {
    // ZREVRANGEBYLEX key max min [LIMIT offset count]
    if args.len() < 3 {
        return wrong_arity("zrevrangebylex");
    }

    let key = &args[0];
    let max_raw = &args[1]; // note: max first
    let min_raw = &args[2];

    let mut limit: Option<(i64, i64)> = None;
    let mut idx = 3usize;

    while idx < args.len() {
        let upper = to_uppercase_bytes(&args[idx]);
        match upper.as_slice() {
            b"LIMIT" => {
                if idx + 2 >= args.len() {
                    return CommandOutcome::reply(err("ERR syntax error"));
                }
                let Some(offset) = parse_i64(&args[idx + 1]) else {
                    return CommandOutcome::reply(err(
                        "ERR value is not an integer or out of range",
                    ));
                };
                let Some(count) = parse_i64(&args[idx + 2]) else {
                    return CommandOutcome::reply(err(
                        "ERR value is not an integer or out of range",
                    ));
                };
                limit = Some((offset, count));
                idx += 3;
            }
            _ => return CommandOutcome::reply(err("ERR syntax error")),
        }
    }

    let now = now_ms();
    let db = server.db_mut(client.selected_db);
    purge_expired_key(db, key, now);

    let Some(entry) = db.get(key) else {
        return CommandOutcome::reply(RespFrame::Array(vec![]));
    };
    let Some(zset) = entry.as_sorted_set() else {
        return wrong_type_response();
    };

    // min_raw and max_raw are already the min and max bounds.
    // rev=true will reverse the output.
    let selected = match zrange_collect(
        zset,
        args,
        min_raw,
        max_raw,
        RangeMode::ByLex,
        true,
        limit,
        false,
    ) {
        Ok(v) => v,
        Err(e) => return CommandOutcome::reply(e),
    };

    CommandOutcome::reply(RespFrame::Array(entries_to_resp(&selected, false)))
}

pub(super) fn cmd_zrevrange(
    args: &[Bytes],
    server: &mut ServerState,
    client: &ClientState,
) -> CommandOutcome {
    if args.len() < 3 {
        return wrong_arity("zrevrange");
    }

    let key = &args[0];
    let start_raw = &args[1];
    let stop_raw = &args[2];

    let with_scores = args.len() > 3 && args[3].eq_ignore_ascii_case(b"WITHSCORES");

    let now = now_ms();
    let db = server.db_mut(client.selected_db);
    purge_expired_key(db, key, now);

    let Some(entry) = db.get(key) else {
        return CommandOutcome::reply(RespFrame::Array(vec![]));
    };
    let Some(zset) = entry.as_sorted_set() else {
        return wrong_type_response();
    };

    let all = sorted_entries(zset);
    let len = all.len() as i64;

    let Some(start_i) = parse_i64(start_raw) else {
        return CommandOutcome::reply(err("ERR value is not an integer or out of range"));
    };
    let Some(stop_i) = parse_i64(stop_raw) else {
        return CommandOutcome::reply(err("ERR value is not an integer or out of range"));
    };

    let start = if start_i < 0 {
        (start_i + len).max(0) as usize
    } else {
        start_i as usize
    };
    let stop = if stop_i < 0 {
        (stop_i + len).max(0) as usize
    } else {
        (stop_i as usize).min(if all.is_empty() { 0 } else { all.len() - 1 })
    };

    if start > stop || start >= all.len() {
        return CommandOutcome::reply(RespFrame::Array(vec![]));
    }

    let mut slice = all[start..=stop].to_vec();
    slice.reverse();

    CommandOutcome::reply(RespFrame::Array(entries_to_resp(&slice, with_scores)))
}

pub(super) fn cmd_zrangestore(
    args: &[Bytes],
    server: &mut ServerState,
    client: &ClientState,
) -> CommandOutcome {
    // ZRANGESTORE dst src min max [BYSCORE|BYLEX] [REV] [LIMIT offset count]
    if args.len() < 4 {
        return wrong_arity("zrangestore");
    }

    let dst = &args[0];
    let src = &args[1];
    let min_raw = &args[2];
    let max_raw = &args[3];
    let options = &args[4..];

    // Parse options (same as ZRANGE but no WITHSCORES)
    let mut mode = RangeMode::ByRank;
    let mut rev = false;
    let mut limit: Option<(i64, i64)> = None;

    let mut idx = 0usize;
    while idx < options.len() {
        let upper = to_uppercase_bytes(&options[idx]);
        match upper.as_slice() {
            b"BYSCORE" => {
                mode = RangeMode::ByScore;
                idx += 1;
            }
            b"BYLEX" => {
                mode = RangeMode::ByLex;
                idx += 1;
            }
            b"REV" => {
                rev = true;
                idx += 1;
            }
            b"LIMIT" => {
                if idx + 2 >= options.len() {
                    return CommandOutcome::reply(err("ERR syntax error"));
                }
                let Some(offset) = parse_i64(&options[idx + 1]) else {
                    return CommandOutcome::reply(err(
                        "ERR value is not an integer or out of range",
                    ));
                };
                let Some(count) = parse_i64(&options[idx + 2]) else {
                    return CommandOutcome::reply(err(
                        "ERR value is not an integer or out of range",
                    ));
                };
                limit = Some((offset, count));
                idx += 3;
            }
            _ => return CommandOutcome::reply(err("ERR syntax error")),
        }
    }

    let now = now_ms();
    let db = server.db_mut(client.selected_db);
    purge_expired_key(db, src, now);

    let selected = {
        let Some(entry) = db.get(src) else {
            // Source does not exist -- delete destination and return 0
            db.remove(dst);
            return CommandOutcome::reply(RespFrame::Integer(0));
        };
        let Some(zset) = entry.as_sorted_set() else {
            return wrong_type_response();
        };

        match zrange_collect(zset, args, min_raw, max_raw, mode, rev, limit, false) {
            Ok(v) => v,
            Err(e) => return CommandOutcome::reply(e),
        }
    };

    let count = selected.len() as i64;

    if selected.is_empty() {
        db.remove(dst);
    } else {
        let mut new_zset = SortedSet::default();
        for (member, score) in &selected {
            new_zset.insert(member.clone(), *score);
        }
        db.insert(dst.clone(), StoredValue::sorted_set(new_zset, None));
    }

    CommandOutcome::reply(RespFrame::Integer(count))
}

pub(super) fn cmd_zcount(
    args: &[Bytes],
    server: &mut ServerState,
    client: &ClientState,
) -> CommandOutcome {
    let [key, min_raw, max_raw] = args else {
        return wrong_arity("zcount");
    };

    let Some(smin) = parse_score_bound(min_raw) else {
        return CommandOutcome::reply(err("ERR min or max is not a float"));
    };
    let Some(smax) = parse_score_bound(max_raw) else {
        return CommandOutcome::reply(err("ERR min or max is not a float"));
    };

    let now = now_ms();
    let db = server.db_mut(client.selected_db);
    purge_expired_key(db, key, now);

    let Some(entry) = db.get(key) else {
        return CommandOutcome::reply(RespFrame::Integer(0));
    };
    let Some(zset) = entry.as_sorted_set() else {
        return wrong_type_response();
    };

    let count = zset
        .by_score
        .keys()
        .filter(|e| score_in_range(e.score.value(), &smin, &smax))
        .count();

    CommandOutcome::reply(RespFrame::Integer(count as i64))
}

pub(super) fn cmd_zlexcount(
    args: &[Bytes],
    server: &mut ServerState,
    client: &ClientState,
) -> CommandOutcome {
    let [key, min_raw, max_raw] = args else {
        return wrong_arity("zlexcount");
    };

    let Some(lmin) = parse_lex_bound(min_raw) else {
        return CommandOutcome::reply(err("ERR min or max is not a valid string range item"));
    };
    let Some(lmax) = parse_lex_bound(max_raw) else {
        return CommandOutcome::reply(err("ERR min or max is not a valid string range item"));
    };

    let now = now_ms();
    let db = server.db_mut(client.selected_db);
    purge_expired_key(db, key, now);

    let Some(entry) = db.get(key) else {
        return CommandOutcome::reply(RespFrame::Integer(0));
    };
    let Some(zset) = entry.as_sorted_set() else {
        return wrong_type_response();
    };

    let count = zset
        .by_score
        .keys()
        .filter(|e| member_in_lex_range(&e.member, &lmin, &lmax))
        .count();

    CommandOutcome::reply(RespFrame::Integer(count as i64))
}

// ---------------------------------------------------------------------------
// 2c: Rank / Remove by Range
// ---------------------------------------------------------------------------

pub(super) fn cmd_zrank(
    args: &[Bytes],
    server: &mut ServerState,
    client: &ClientState,
) -> CommandOutcome {
    let [key, member] = args else {
        return wrong_arity("zrank");
    };

    let now = now_ms();
    let db = server.db_mut(client.selected_db);
    purge_expired_key(db, key, now);

    let Some(entry) = db.get(key) else {
        return CommandOutcome::reply(RespFrame::BulkString(None));
    };
    let Some(zset) = entry.as_sorted_set() else {
        return wrong_type_response();
    };

    match zset.rank(member) {
        Some(r) => CommandOutcome::reply(RespFrame::Integer(r as i64)),
        None => CommandOutcome::reply(RespFrame::BulkString(None)),
    }
}

pub(super) fn cmd_zrevrank(
    args: &[Bytes],
    server: &mut ServerState,
    client: &ClientState,
) -> CommandOutcome {
    let [key, member] = args else {
        return wrong_arity("zrevrank");
    };

    let now = now_ms();
    let db = server.db_mut(client.selected_db);
    purge_expired_key(db, key, now);

    let Some(entry) = db.get(key) else {
        return CommandOutcome::reply(RespFrame::BulkString(None));
    };
    let Some(zset) = entry.as_sorted_set() else {
        return wrong_type_response();
    };

    match zset.rev_rank(member) {
        Some(r) => CommandOutcome::reply(RespFrame::Integer(r as i64)),
        None => CommandOutcome::reply(RespFrame::BulkString(None)),
    }
}

pub(super) fn cmd_zremrangebyrank(
    args: &[Bytes],
    server: &mut ServerState,
    client: &ClientState,
) -> CommandOutcome {
    let [key, start_raw, stop_raw] = args else {
        return wrong_arity("zremrangebyrank");
    };

    let Some(start_i) = parse_i64(start_raw) else {
        return CommandOutcome::reply(err("ERR value is not an integer or out of range"));
    };
    let Some(stop_i) = parse_i64(stop_raw) else {
        return CommandOutcome::reply(err("ERR value is not an integer or out of range"));
    };

    let now = now_ms();
    let db = server.db_mut(client.selected_db);
    purge_expired_key(db, key, now);

    let Some(entry) = db.get_mut(key) else {
        return CommandOutcome::reply(RespFrame::Integer(0));
    };
    let Some(zset) = entry.as_sorted_set_mut() else {
        return wrong_type_response();
    };

    let len = zset.len() as i64;
    let start = if start_i < 0 {
        (start_i + len).max(0) as usize
    } else {
        start_i as usize
    };
    let stop = if stop_i < 0 {
        (stop_i + len).max(0) as usize
    } else {
        (stop_i as usize).min(if zset.is_empty() { 0 } else { zset.len() - 1 })
    };

    if start > stop || start >= zset.len() {
        return CommandOutcome::reply(RespFrame::Integer(0));
    }

    // Collect members to remove by rank
    let to_remove: Vec<Bytes> = zset
        .by_score
        .keys()
        .skip(start)
        .take(stop - start + 1)
        .map(|e| e.member.clone())
        .collect();

    let removed = to_remove.len() as i64;
    for member in &to_remove {
        zset.remove(member);
    }

    if zset.is_empty() {
        db.remove(key);
    }

    CommandOutcome::reply(RespFrame::Integer(removed))
}

pub(super) fn cmd_zremrangebyscore(
    args: &[Bytes],
    server: &mut ServerState,
    client: &ClientState,
) -> CommandOutcome {
    let [key, min_raw, max_raw] = args else {
        return wrong_arity("zremrangebyscore");
    };

    let Some(smin) = parse_score_bound(min_raw) else {
        return CommandOutcome::reply(err("ERR min or max is not a float"));
    };
    let Some(smax) = parse_score_bound(max_raw) else {
        return CommandOutcome::reply(err("ERR min or max is not a float"));
    };

    let now = now_ms();
    let db = server.db_mut(client.selected_db);
    purge_expired_key(db, key, now);

    let Some(entry) = db.get_mut(key) else {
        return CommandOutcome::reply(RespFrame::Integer(0));
    };
    let Some(zset) = entry.as_sorted_set_mut() else {
        return wrong_type_response();
    };

    let to_remove: Vec<Bytes> = zset
        .by_score
        .keys()
        .filter(|e| score_in_range(e.score.value(), &smin, &smax))
        .map(|e| e.member.clone())
        .collect();

    let removed = to_remove.len() as i64;
    for member in &to_remove {
        zset.remove(member);
    }

    if zset.is_empty() {
        db.remove(key);
    }

    CommandOutcome::reply(RespFrame::Integer(removed))
}

pub(super) fn cmd_zremrangebylex(
    args: &[Bytes],
    server: &mut ServerState,
    client: &ClientState,
) -> CommandOutcome {
    let [key, min_raw, max_raw] = args else {
        return wrong_arity("zremrangebylex");
    };

    let Some(lmin) = parse_lex_bound(min_raw) else {
        return CommandOutcome::reply(err("ERR min or max is not a valid string range item"));
    };
    let Some(lmax) = parse_lex_bound(max_raw) else {
        return CommandOutcome::reply(err("ERR min or max is not a valid string range item"));
    };

    let now = now_ms();
    let db = server.db_mut(client.selected_db);
    purge_expired_key(db, key, now);

    let Some(entry) = db.get_mut(key) else {
        return CommandOutcome::reply(RespFrame::Integer(0));
    };
    let Some(zset) = entry.as_sorted_set_mut() else {
        return wrong_type_response();
    };

    let to_remove: Vec<Bytes> = zset
        .by_score
        .keys()
        .filter(|e| member_in_lex_range(&e.member, &lmin, &lmax))
        .map(|e| e.member.clone())
        .collect();

    let removed = to_remove.len() as i64;
    for member in &to_remove {
        zset.remove(member);
    }

    if zset.is_empty() {
        db.remove(key);
    }

    CommandOutcome::reply(RespFrame::Integer(removed))
}

// ---------------------------------------------------------------------------
// 2d: Set Operations
// ---------------------------------------------------------------------------

/// Read a sorted set from the db; returns an empty set if the key does not exist.
/// Returns Err if the key exists but is the wrong type.
#[allow(clippy::result_large_err)]
fn read_zset_or_empty(
    db: &hashbrown::HashMap<Bytes, StoredValue>,
    key: &Bytes,
) -> Result<Vec<(Bytes, f64)>, CommandOutcome> {
    let Some(entry) = db.get(key) else {
        return Ok(Vec::new());
    };
    let Some(zset) = entry.as_sorted_set() else {
        return Err(wrong_type_response());
    };
    Ok(sorted_entries(zset))
}

#[allow(clippy::result_large_err)]
fn compute_union(
    db: &hashbrown::HashMap<Bytes, StoredValue>,
    keys: &[Bytes],
    weights: &[f64],
    aggregate: Aggregate,
) -> Result<Vec<(Bytes, f64)>, CommandOutcome> {
    let mut result: hashbrown::HashMap<Bytes, f64> = hashbrown::HashMap::new();

    for (i, key) in keys.iter().enumerate() {
        let entries = read_zset_or_empty(db, key)?;
        let w = weights.get(i).copied().unwrap_or(1.0);
        for (member, score) in entries {
            let weighted = score * w;
            result
                .entry(member)
                .and_modify(|existing| *existing = aggregate_score(aggregate, *existing, weighted))
                .or_insert(weighted);
        }
    }

    let mut out: Vec<(Bytes, f64)> = result.into_iter().collect();
    out.sort_by(|(am, as_), (bm, bs)| {
        SortedSetScore(*as_)
            .cmp(&SortedSetScore(*bs))
            .then_with(|| am.cmp(bm))
    });
    Ok(out)
}

#[allow(clippy::result_large_err)]
fn compute_inter(
    db: &hashbrown::HashMap<Bytes, StoredValue>,
    keys: &[Bytes],
    weights: &[f64],
    aggregate: Aggregate,
) -> Result<Vec<(Bytes, f64)>, CommandOutcome> {
    if keys.is_empty() {
        return Ok(Vec::new());
    }

    // Start with the first set
    let first_entries = read_zset_or_empty(db, &keys[0])?;
    let w0 = weights.first().copied().unwrap_or(1.0);
    let mut result: hashbrown::HashMap<Bytes, f64> = first_entries
        .into_iter()
        .map(|(m, s)| (m, s * w0))
        .collect();

    for (i, key) in keys.iter().enumerate().skip(1) {
        let entries = read_zset_or_empty(db, key)?;
        let w = weights.get(i).copied().unwrap_or(1.0);
        let other: hashbrown::HashMap<Bytes, f64> =
            entries.into_iter().map(|(m, s)| (m, s * w)).collect();

        result.retain(|member, existing| {
            if let Some(other_score) = other.get(member) {
                *existing = aggregate_score(aggregate, *existing, *other_score);
                true
            } else {
                false
            }
        });

        if result.is_empty() {
            break;
        }
    }

    let mut out: Vec<(Bytes, f64)> = result.into_iter().collect();
    out.sort_by(|(am, as_), (bm, bs)| {
        SortedSetScore(*as_)
            .cmp(&SortedSetScore(*bs))
            .then_with(|| am.cmp(bm))
    });
    Ok(out)
}

#[allow(clippy::result_large_err)]
fn compute_diff(
    db: &hashbrown::HashMap<Bytes, StoredValue>,
    keys: &[Bytes],
) -> Result<Vec<(Bytes, f64)>, CommandOutcome> {
    if keys.is_empty() {
        return Ok(Vec::new());
    }

    let first_entries = read_zset_or_empty(db, &keys[0])?;
    let mut result: hashbrown::HashMap<Bytes, f64> = first_entries.into_iter().collect();

    for key in keys.iter().skip(1) {
        let entries = read_zset_or_empty(db, key)?;
        for (member, _) in entries {
            result.remove(&member);
        }
        if result.is_empty() {
            break;
        }
    }

    let mut out: Vec<(Bytes, f64)> = result.into_iter().collect();
    out.sort_by(|(am, as_), (bm, bs)| {
        SortedSetScore(*as_)
            .cmp(&SortedSetScore(*bs))
            .then_with(|| am.cmp(bm))
    });
    Ok(out)
}

/// Parse numkeys + key list from args starting at a given index.
/// Returns (numkeys, keys_slice_end_index, numkeys_value).
#[allow(clippy::result_large_err)]
fn parse_numkeys_and_keys(
    args: &[Bytes],
    start: usize,
    command_name: &str,
) -> Result<(usize, usize), CommandOutcome> {
    let Some(nk_raw) = args.get(start) else {
        return Err(wrong_arity(command_name));
    };
    let Some(nk_i64) = parse_i64(nk_raw) else {
        return Err(CommandOutcome::reply(err(
            "ERR value is not an integer or out of range",
        )));
    };
    if nk_i64 <= 0 {
        return Err(CommandOutcome::reply(err(
            "ERR numkeys should be greater than 0",
        )));
    }
    let Ok(numkeys) = usize::try_from(nk_i64) else {
        return Err(CommandOutcome::reply(err(
            "ERR value is not an integer or out of range",
        )));
    };
    if numkeys > MAX_ZSET_NUMKEYS {
        return Err(CommandOutcome::reply(err("ERR numkeys is out of range")));
    }
    let keys_end = start + 1 + numkeys;
    if keys_end > args.len() {
        return Err(CommandOutcome::reply(err(
            "ERR Number of keys can't be greater than number of args",
        )));
    }
    Ok((numkeys, keys_end))
}

pub(super) fn cmd_zunion(
    args: &[Bytes],
    server: &mut ServerState,
    client: &ClientState,
) -> CommandOutcome {
    if args.len() < 2 {
        return wrong_arity("zunion");
    }

    let (numkeys, keys_end) = match parse_numkeys_and_keys(args, 0, "zunion") {
        Ok(v) => v,
        Err(outcome) => return outcome,
    };
    let keys = &args[1..keys_end];

    // Parse WITHSCORES from remaining options
    let mut with_scores = false;
    let option_args = &args[keys_end..];

    // We need to separate WITHSCORES from WEIGHTS/AGGREGATE
    // Find WITHSCORES and strip it before passing to parse_weights_aggregate
    let mut filtered_options: Vec<Bytes> = Vec::with_capacity(option_args.len());
    for opt in option_args {
        if opt.eq_ignore_ascii_case(b"WITHSCORES") {
            with_scores = true;
        } else {
            filtered_options.push(opt.clone());
        }
    }

    let (weights, aggregate) = match parse_weights_aggregate(&filtered_options, 0, numkeys) {
        Ok(v) => v,
        Err(e) => return CommandOutcome::reply(e),
    };

    let now = now_ms();
    let db = server.db_mut(client.selected_db);
    for key in keys {
        purge_expired_key(db, key, now);
    }

    let result = match compute_union(db, keys, &weights, aggregate) {
        Ok(v) => v,
        Err(outcome) => return outcome,
    };

    CommandOutcome::reply(RespFrame::Array(entries_to_resp(&result, with_scores)))
}

pub(super) fn cmd_zunionstore(
    args: &[Bytes],
    server: &mut ServerState,
    client: &ClientState,
) -> CommandOutcome {
    if args.len() < 3 {
        return wrong_arity("zunionstore");
    }

    let dest = &args[0];
    let (numkeys, keys_end) = match parse_numkeys_and_keys(args, 1, "zunionstore") {
        Ok(v) => v,
        Err(outcome) => return outcome,
    };
    let keys = &args[2..keys_end];

    let (weights, aggregate) = match parse_weights_aggregate(args, keys_end, numkeys) {
        Ok(v) => v,
        Err(e) => return CommandOutcome::reply(e),
    };

    let now = now_ms();
    let db = server.db_mut(client.selected_db);
    for key in keys {
        purge_expired_key(db, key, now);
    }

    let result = match compute_union(db, keys, &weights, aggregate) {
        Ok(v) => v,
        Err(outcome) => return outcome,
    };

    let count = result.len() as i64;
    if result.is_empty() {
        db.remove(dest);
    } else {
        let mut zset = SortedSet::default();
        for (member, score) in &result {
            zset.insert(member.clone(), *score);
        }
        db.insert(dest.clone(), StoredValue::sorted_set(zset, None));
    }

    CommandOutcome::reply(RespFrame::Integer(count))
}

pub(super) fn cmd_zinter(
    args: &[Bytes],
    server: &mut ServerState,
    client: &ClientState,
) -> CommandOutcome {
    if args.len() < 2 {
        return wrong_arity("zinter");
    }

    let (numkeys, keys_end) = match parse_numkeys_and_keys(args, 0, "zinter") {
        Ok(v) => v,
        Err(outcome) => return outcome,
    };
    let keys = &args[1..keys_end];

    let mut with_scores = false;
    let option_args = &args[keys_end..];

    let mut filtered_options: Vec<Bytes> = Vec::with_capacity(option_args.len());
    for opt in option_args {
        if opt.eq_ignore_ascii_case(b"WITHSCORES") {
            with_scores = true;
        } else {
            filtered_options.push(opt.clone());
        }
    }

    let (weights, aggregate) = match parse_weights_aggregate(&filtered_options, 0, numkeys) {
        Ok(v) => v,
        Err(e) => return CommandOutcome::reply(e),
    };

    let now = now_ms();
    let db = server.db_mut(client.selected_db);
    for key in keys {
        purge_expired_key(db, key, now);
    }

    let result = match compute_inter(db, keys, &weights, aggregate) {
        Ok(v) => v,
        Err(outcome) => return outcome,
    };

    CommandOutcome::reply(RespFrame::Array(entries_to_resp(&result, with_scores)))
}

pub(super) fn cmd_zinterstore(
    args: &[Bytes],
    server: &mut ServerState,
    client: &ClientState,
) -> CommandOutcome {
    if args.len() < 3 {
        return wrong_arity("zinterstore");
    }

    let dest = &args[0];
    let (numkeys, keys_end) = match parse_numkeys_and_keys(args, 1, "zinterstore") {
        Ok(v) => v,
        Err(outcome) => return outcome,
    };
    let keys = &args[2..keys_end];

    let (weights, aggregate) = match parse_weights_aggregate(args, keys_end, numkeys) {
        Ok(v) => v,
        Err(e) => return CommandOutcome::reply(e),
    };

    let now = now_ms();
    let db = server.db_mut(client.selected_db);
    for key in keys {
        purge_expired_key(db, key, now);
    }

    let result = match compute_inter(db, keys, &weights, aggregate) {
        Ok(v) => v,
        Err(outcome) => return outcome,
    };

    let count = result.len() as i64;
    if result.is_empty() {
        db.remove(dest);
    } else {
        let mut zset = SortedSet::default();
        for (member, score) in &result {
            zset.insert(member.clone(), *score);
        }
        db.insert(dest.clone(), StoredValue::sorted_set(zset, None));
    }

    CommandOutcome::reply(RespFrame::Integer(count))
}

pub(super) fn cmd_zintercard(
    args: &[Bytes],
    server: &mut ServerState,
    client: &ClientState,
) -> CommandOutcome {
    if args.len() < 2 {
        return wrong_arity("zintercard");
    }

    let (_numkeys, keys_end) = match parse_numkeys_and_keys(args, 0, "zintercard") {
        Ok(v) => v,
        Err(outcome) => return outcome,
    };
    let keys = &args[1..keys_end];

    let mut limit = 0usize;
    let mut idx = keys_end;
    while idx < args.len() {
        if args[idx].eq_ignore_ascii_case(b"LIMIT") {
            let Some(limit_raw) = args.get(idx + 1) else {
                return CommandOutcome::reply(err("ERR syntax error"));
            };
            let Some(parsed) = parse_i64(limit_raw) else {
                return CommandOutcome::reply(err("ERR value is not an integer or out of range"));
            };
            if parsed < 0 {
                return CommandOutcome::reply(err("ERR LIMIT can't be negative"));
            }
            let Ok(parsed_limit) = usize::try_from(parsed) else {
                return CommandOutcome::reply(err("ERR value is not an integer or out of range"));
            };
            limit = parsed_limit;
            idx += 2;
        } else {
            return CommandOutcome::reply(err("ERR syntax error"));
        }
    }

    let now = now_ms();
    let db = server.db_mut(client.selected_db);
    for key in keys {
        purge_expired_key(db, key, now);
    }

    // Compute intersection cardinality efficiently by finding the smallest set first
    if keys.is_empty() {
        return CommandOutcome::reply(RespFrame::Integer(0));
    }

    // Gather all sets
    let mut sets: Vec<Vec<(Bytes, f64)>> = Vec::with_capacity(keys.len());
    for key in keys {
        match read_zset_or_empty(db, key) {
            Ok(entries) => {
                if entries.is_empty() {
                    return CommandOutcome::reply(RespFrame::Integer(0));
                }
                sets.push(entries);
            }
            Err(outcome) => return outcome,
        }
    }

    // Start with the smallest set for efficiency
    let mut smallest_idx = 0;
    let mut smallest_len = sets[0].len();
    for (i, s) in sets.iter().enumerate().skip(1) {
        if s.len() < smallest_len {
            smallest_idx = i;
            smallest_len = s.len();
        }
    }

    let mut count = 0usize;
    let candidate_members: Vec<Bytes> = sets[smallest_idx].iter().map(|(m, _)| m.clone()).collect();

    // Build HashSets for the other sets
    let mut member_sets: Vec<hashbrown::HashSet<&Bytes>> = Vec::with_capacity(sets.len());
    for s in &sets {
        let hs: hashbrown::HashSet<&Bytes> = s.iter().map(|(m, _)| m).collect();
        member_sets.push(hs);
    }

    for member in &candidate_members {
        let in_all = member_sets.iter().all(|ms| ms.contains(member));
        if in_all {
            count += 1;
            if limit > 0 && count >= limit {
                break;
            }
        }
    }

    CommandOutcome::reply(RespFrame::Integer(count as i64))
}

pub(super) fn cmd_zdiff(
    args: &[Bytes],
    server: &mut ServerState,
    client: &ClientState,
) -> CommandOutcome {
    if args.len() < 2 {
        return wrong_arity("zdiff");
    }

    let (_numkeys, keys_end) = match parse_numkeys_and_keys(args, 0, "zdiff") {
        Ok(v) => v,
        Err(outcome) => return outcome,
    };
    let keys = &args[1..keys_end];

    let with_scores = args
        .get(keys_end)
        .is_some_and(|opt| opt.eq_ignore_ascii_case(b"WITHSCORES"));

    let now = now_ms();
    let db = server.db_mut(client.selected_db);
    for key in keys {
        purge_expired_key(db, key, now);
    }

    let result = match compute_diff(db, keys) {
        Ok(v) => v,
        Err(outcome) => return outcome,
    };

    CommandOutcome::reply(RespFrame::Array(entries_to_resp(&result, with_scores)))
}

pub(super) fn cmd_zdiffstore(
    args: &[Bytes],
    server: &mut ServerState,
    client: &ClientState,
) -> CommandOutcome {
    if args.len() < 3 {
        return wrong_arity("zdiffstore");
    }

    let dest = &args[0];
    let (_numkeys, keys_end) = match parse_numkeys_and_keys(args, 1, "zdiffstore") {
        Ok(v) => v,
        Err(outcome) => return outcome,
    };
    let keys = &args[2..keys_end];

    let now = now_ms();
    let db = server.db_mut(client.selected_db);
    for key in keys {
        purge_expired_key(db, key, now);
    }

    let result = match compute_diff(db, keys) {
        Ok(v) => v,
        Err(outcome) => return outcome,
    };

    let count = result.len() as i64;
    if result.is_empty() {
        db.remove(dest);
    } else {
        let mut zset = SortedSet::default();
        for (member, score) in &result {
            zset.insert(member.clone(), *score);
        }
        db.insert(dest.clone(), StoredValue::sorted_set(zset, None));
    }

    CommandOutcome::reply(RespFrame::Integer(count))
}

// ---------------------------------------------------------------------------
// 2e: Pop / Random / Scan
// ---------------------------------------------------------------------------

pub(super) fn cmd_zpopmin(
    args: &[Bytes],
    server: &mut ServerState,
    client: &ClientState,
) -> CommandOutcome {
    zpop_impl(args, server, client, true, "zpopmin")
}

pub(super) fn cmd_zpopmax(
    args: &[Bytes],
    server: &mut ServerState,
    client: &ClientState,
) -> CommandOutcome {
    zpop_impl(args, server, client, false, "zpopmax")
}

fn zpop_impl(
    args: &[Bytes],
    server: &mut ServerState,
    client: &ClientState,
    pop_min: bool,
    command_name: &str,
) -> CommandOutcome {
    if args.is_empty() || args.len() > 2 {
        return wrong_arity(command_name);
    }

    let key = &args[0];
    let count = if args.len() == 2 {
        let Some(raw_count) = parse_i64(&args[1]) else {
            return CommandOutcome::reply(err("ERR value is not an integer or out of range"));
        };
        if raw_count < 0 {
            return CommandOutcome::reply(err("ERR value is out of range, must be positive"));
        }
        let Ok(parsed_count) = usize::try_from(raw_count) else {
            return CommandOutcome::reply(err("ERR value is not an integer or out of range"));
        };
        if parsed_count > MAX_ZSET_POP_COUNT {
            return CommandOutcome::reply(err("ERR count is out of range"));
        }
        parsed_count
    } else {
        1
    };

    if count == 0 {
        return CommandOutcome::reply(RespFrame::Array(vec![]));
    }

    let now = now_ms();
    let db = server.db_mut(client.selected_db);
    purge_expired_key(db, key, now);

    let Some(entry) = db.get_mut(key) else {
        return CommandOutcome::reply(RespFrame::Array(vec![]));
    };
    let Some(zset) = entry.as_sorted_set_mut() else {
        return wrong_type_response();
    };

    let mut popped = Vec::with_capacity(count.min(zset.len()));
    for _ in 0..count {
        let entry_opt = if pop_min {
            zset.by_score.keys().next().cloned()
        } else {
            zset.by_score.keys().next_back().cloned()
        };
        let Some(e) = entry_opt else {
            break;
        };
        popped.push((e.member.clone(), e.score.value()));
        zset.remove(&e.member);
    }

    if zset.is_empty() {
        db.remove(key);
    }

    let mut out = Vec::with_capacity(popped.len().saturating_mul(2));
    for (member, score) in &popped {
        out.push(RespFrame::BulkString(Some(member.clone())));
        out.push(RespFrame::BulkString(Some(format_f64_for_redis(*score))));
    }

    CommandOutcome::reply(RespFrame::Array(out))
}

pub(super) fn cmd_zmpop(
    args: &[Bytes],
    server: &mut ServerState,
    client: &ClientState,
) -> CommandOutcome {
    zmpop_inner(args, server, client, false)
}

fn zmpop_inner(
    args: &[Bytes],
    server: &mut ServerState,
    client: &ClientState,
    blocking: bool,
) -> CommandOutcome {
    let command_name = if blocking { "bzmpop" } else { "zmpop" };
    let mut idx = 0usize;
    let mut timeout_sec = 0.0f64;

    if blocking {
        if args.len() < 4 {
            return wrong_arity(command_name);
        }
        timeout_sec = match parse_blocking_timeout_seconds(&args[0]) {
            Ok(timeout) => timeout,
            Err(response) => return CommandOutcome::reply(response),
        };
        idx = 1;
    } else if args.len() < 3 {
        return wrong_arity(command_name);
    }

    let Some(numkeys_raw) = args.get(idx) else {
        return wrong_arity(command_name);
    };
    let Some(numkeys_i64) = parse_i64(numkeys_raw) else {
        return CommandOutcome::reply(err("ERR value is not an integer or out of range"));
    };
    if numkeys_i64 <= 0 {
        return CommandOutcome::reply(err("ERR numkeys should be greater than 0"));
    }

    let Ok(numkeys) = usize::try_from(numkeys_i64) else {
        return CommandOutcome::reply(err("ERR value is not an integer or out of range"));
    };
    if numkeys > MAX_ZSET_NUMKEYS {
        return CommandOutcome::reply(err("ERR numkeys is out of range"));
    }
    let first_key_idx = idx + 1;
    let side_idx = first_key_idx.saturating_add(numkeys);
    if side_idx >= args.len() {
        return wrong_arity(command_name);
    }

    let keys = args[first_key_idx..side_idx].to_vec();
    let Some(side_raw) = args.get(side_idx) else {
        return wrong_arity(command_name);
    };
    let upper_side = to_uppercase_bytes(side_raw);
    let pop_min = match upper_side.as_slice() {
        b"MIN" => true,
        b"MAX" => false,
        _ => return CommandOutcome::reply(err("ERR syntax error")),
    };

    let mut count = 1usize;
    let option_args = &args[(side_idx + 1)..];
    if !option_args.is_empty() {
        if option_args.len() != 2 || !option_args[0].eq_ignore_ascii_case(b"COUNT") {
            return CommandOutcome::reply(err("ERR syntax error"));
        }
        let Some(parsed_count) = parse_i64(&option_args[1]) else {
            return CommandOutcome::reply(err("ERR value is not an integer or out of range"));
        };
        if parsed_count <= 0 {
            return CommandOutcome::reply(err("ERR count should be greater than 0"));
        }
        let Ok(parsed_count) = usize::try_from(parsed_count) else {
            return CommandOutcome::reply(err("ERR value is not an integer or out of range"));
        };
        if parsed_count > MAX_ZSET_POP_COUNT {
            return CommandOutcome::reply(err("ERR count is out of range"));
        }
        count = parsed_count;
    }

    if !blocking {
        return try_zmpop_once(&keys, pop_min, count, server, client);
    }

    let deadline_ms = blocking_deadline_ms(timeout_sec);

    let outcome = try_zmpop_once(&keys, pop_min, count, server, client);
    if !matches!(outcome.response, RespFrame::BulkString(None)) {
        return outcome;
    }

    if let Some(deadline) = deadline_ms {
        if now_ms() >= deadline {
            return outcome;
        }
    }

    let full_frame = build_blocking_frame("BZMPOP", args);
    CommandOutcome::blocking(
        outcome.response,
        deadline_ms,
        full_frame,
        blocking_watch_keys(client, &keys),
    )
}

fn try_zmpop_once(
    keys: &[Bytes],
    pop_min: bool,
    count: usize,
    server: &mut ServerState,
    client: &ClientState,
) -> CommandOutcome {
    let now = now_ms();
    let db = server.db_mut(client.selected_db);

    for key in keys {
        purge_expired_key(db, key, now);

        let mut remove_key = false;
        let popped = {
            let Some(entry) = db.get_mut(key) else {
                continue;
            };
            let Some(zset) = entry.as_sorted_set_mut() else {
                return wrong_type_response();
            };
            if zset.is_empty() {
                continue;
            }

            let mut items = Vec::with_capacity(count.min(zset.len()));
            for _ in 0..count {
                let entry_opt = if pop_min {
                    zset.by_score.keys().next().cloned()
                } else {
                    zset.by_score.keys().next_back().cloned()
                };
                let Some(e) = entry_opt else {
                    break;
                };
                items.push((e.member.clone(), e.score.value()));
                zset.remove(&e.member);
            }

            if zset.is_empty() {
                remove_key = true;
            }

            items
        };

        if remove_key {
            db.remove(key);
        }

        if popped.is_empty() {
            continue;
        }

        let values: Vec<RespFrame> = popped
            .into_iter()
            .map(|(member, score)| {
                RespFrame::Array(vec![
                    RespFrame::BulkString(Some(member)),
                    RespFrame::BulkString(Some(format_f64_for_redis(score))),
                ])
            })
            .collect();

        return CommandOutcome::reply(RespFrame::Array(vec![
            RespFrame::BulkString(Some(key.clone())),
            RespFrame::Array(values),
        ]));
    }

    CommandOutcome::reply(RespFrame::BulkString(None))
}

pub(super) fn cmd_zrandmember(
    args: &[Bytes],
    server: &mut ServerState,
    client: &ClientState,
) -> CommandOutcome {
    if args.is_empty() || args.len() > 3 {
        return wrong_arity("zrandmember");
    }

    let key = &args[0];
    let count = if args.len() >= 2 {
        let Some(parsed) = parse_i64(&args[1]) else {
            return CommandOutcome::reply(err("ERR value is not an integer or out of range"));
        };
        Some(parsed)
    } else {
        None
    };

    let with_scores = args.len() == 3 && args[2].eq_ignore_ascii_case(b"WITHSCORES");

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
    let Some(zset) = entry.as_sorted_set() else {
        return wrong_type_response();
    };

    let members: Vec<(Bytes, f64)> = sorted_entries(zset);
    if members.is_empty() {
        return if count.is_some() {
            CommandOutcome::reply(RespFrame::Array(vec![]))
        } else {
            CommandOutcome::reply(RespFrame::BulkString(None))
        };
    }

    let start = usize::try_from(now_us()).unwrap_or(0) % members.len();

    match count {
        None => {
            let (m, _) = &members[start];
            CommandOutcome::reply(RespFrame::BulkString(Some(m.clone())))
        }
        Some(raw_count) if raw_count >= 0 => {
            let Ok(requested) = usize::try_from(raw_count) else {
                return CommandOutcome::reply(err("ERR value is not an integer or out of range"));
            };
            if requested > MAX_ZSET_RANDOM_COUNT {
                return CommandOutcome::reply(err("ERR count is out of range"));
            }
            if requested == 0 {
                return CommandOutcome::reply(RespFrame::Array(vec![]));
            }

            // Unique members, no duplicates
            let take = requested.min(members.len());
            let mut rotated = members;
            rotated.rotate_left(start);

            let selected = &rotated[..take];
            CommandOutcome::reply(RespFrame::Array(entries_to_resp(selected, with_scores)))
        }
        Some(raw_count) => {
            // Negative count: allow duplicates
            let Ok(requested) = usize::try_from(raw_count.saturating_neg()) else {
                return CommandOutcome::reply(err("ERR value is not an integer or out of range"));
            };
            if requested > MAX_ZSET_RANDOM_COUNT {
                return CommandOutcome::reply(err("ERR count is out of range"));
            }
            let mut out = Vec::with_capacity(if with_scores {
                requested.saturating_mul(2)
            } else {
                requested
            });
            for offset in 0..requested {
                let idx = (start + offset) % members.len();
                let (m, s) = &members[idx];
                out.push(RespFrame::BulkString(Some(m.clone())));
                if with_scores {
                    out.push(RespFrame::BulkString(Some(format_f64_for_redis(*s))));
                }
            }
            CommandOutcome::reply(RespFrame::Array(out))
        }
    }
}

pub(super) fn cmd_zscan(
    args: &[Bytes],
    server: &mut ServerState,
    client: &ClientState,
) -> CommandOutcome {
    let [key, cursor_raw, options @ ..] = args else {
        return wrong_arity("zscan");
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
    let Some(zset) = entry.as_sorted_set() else {
        return wrong_type_response();
    };

    // Collect (member, score) pairs, sorted by member for determinism
    let mut entries: Vec<(Bytes, f64)> = zset
        .by_member
        .iter()
        .map(|(member, score)| (member.clone(), score.value()))
        .collect();
    entries.sort_by(|(am, _), (bm, _)| am.cmp(bm));

    let (next_cursor, matched_indexes) =
        scan_collect_indexes(&entries, cursor, count, |(member, _)| {
            if let Some(pattern) = &pattern {
                glob_match(pattern, &String::from_utf8_lossy(member))
            } else {
                true
            }
        });

    let mut out = Vec::with_capacity(matched_indexes.len().saturating_mul(2));
    for idx in matched_indexes {
        let (member, score) = &entries[idx];
        out.push(RespFrame::BulkString(Some(member.clone())));
        out.push(RespFrame::BulkString(Some(format_f64_for_redis(*score))));
    }

    scan_reply(next_cursor, out)
}

// ---------------------------------------------------------------------------
// 2f: Blocking
// ---------------------------------------------------------------------------

pub(super) fn cmd_bzpopmin(
    args: &[Bytes],
    server: &mut ServerState,
    client: &ClientState,
) -> CommandOutcome {
    cmd_bzpop(args, server, client, true, "bzpopmin")
}

pub(super) fn cmd_bzpopmax(
    args: &[Bytes],
    server: &mut ServerState,
    client: &ClientState,
) -> CommandOutcome {
    cmd_bzpop(args, server, client, false, "bzpopmax")
}

fn cmd_bzpop(
    args: &[Bytes],
    server: &mut ServerState,
    client: &ClientState,
    pop_min: bool,
    command_name: &str,
) -> CommandOutcome {
    if args.len() < 2 {
        return wrong_arity(command_name);
    }

    let timeout_raw = &args[args.len() - 1];
    let timeout_sec = match parse_blocking_timeout_seconds(timeout_raw) {
        Ok(timeout) => timeout,
        Err(response) => return CommandOutcome::reply(response),
    };

    let keys = &args[..args.len() - 1];
    let deadline_ms = blocking_deadline_ms(timeout_sec);

    let outcome = try_bzpop_once(keys, server, client, pop_min);
    if !matches!(outcome.response, RespFrame::BulkString(None)) {
        return outcome;
    }

    if let Some(deadline) = deadline_ms {
        if now_ms() >= deadline {
            return outcome;
        }
    }

    let full_frame = build_blocking_frame(command_name, args);
    CommandOutcome::blocking(
        outcome.response,
        deadline_ms,
        full_frame,
        blocking_watch_keys(client, keys),
    )
}

fn try_bzpop_once(
    keys: &[Bytes],
    server: &mut ServerState,
    client: &ClientState,
    pop_min: bool,
) -> CommandOutcome {
    let now = now_ms();
    let db = server.db_mut(client.selected_db);

    for key in keys {
        purge_expired_key(db, key, now);

        let mut remove_key = false;
        let popped = {
            let Some(entry) = db.get_mut(key) else {
                continue;
            };
            let Some(zset) = entry.as_sorted_set_mut() else {
                return wrong_type_response();
            };

            let entry_opt = if pop_min {
                zset.by_score.keys().next().cloned()
            } else {
                zset.by_score.keys().next_back().cloned()
            };

            let Some(e) = entry_opt else {
                continue;
            };

            let member = e.member.clone();
            let score = e.score.value();
            zset.remove(&member);

            if zset.is_empty() {
                remove_key = true;
            }

            Some((member, score))
        };

        if remove_key {
            db.remove(key);
        }

        let Some((member, score)) = popped else {
            continue;
        };

        return CommandOutcome::reply(RespFrame::Array(vec![
            RespFrame::BulkString(Some(key.clone())),
            RespFrame::BulkString(Some(member)),
            RespFrame::BulkString(Some(format_f64_for_redis(score))),
        ]));
    }

    CommandOutcome::reply(RespFrame::BulkString(None))
}

pub(super) fn cmd_bzmpop(
    args: &[Bytes],
    server: &mut ServerState,
    client: &ClientState,
) -> CommandOutcome {
    zmpop_inner(args, server, client, true)
}
