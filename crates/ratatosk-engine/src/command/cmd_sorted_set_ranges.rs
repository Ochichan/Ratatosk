use bytes::Bytes;

use ratatosk_resp::frame::RespFrame;

use crate::keyspace::{
    LexBound, ScoreBound, ServerState, SortedSet, StoredValue, purge_expired_key,
};

use super::cmd_sorted_set::entries_to_resp;
use super::{
    ClientState, CommandOutcome, err, now_ms, parse_i64, to_uppercase_bytes, wrong_arity,
    wrong_type_response,
};

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
                        zset.by_score
                            .keys()
                            .rev()
                            .skip(start)
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
        }
    }

    let _ = (args, with_scores);
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
    let mut db = server.db_mut(client.selected_db);
    purge_expired_key(&mut db, key, now);

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
    let mut db = server.db_mut(client.selected_db);
    purge_expired_key(&mut db, key, now);

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
    if args.len() < 3 {
        return wrong_arity("zrevrangebyscore");
    }

    let key = &args[0];
    let max_raw = &args[1];
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
    let mut db = server.db_mut(client.selected_db);
    purge_expired_key(&mut db, key, now);

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
    let mut db = server.db_mut(client.selected_db);
    purge_expired_key(&mut db, key, now);

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
    if args.len() < 3 {
        return wrong_arity("zrevrangebylex");
    }

    let key = &args[0];
    let max_raw = &args[1];
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
    let mut db = server.db_mut(client.selected_db);
    purge_expired_key(&mut db, key, now);

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
    let with_scores = match &args[3..] {
        [] => false,
        [option] if option.eq_ignore_ascii_case(b"WITHSCORES") => true,
        _ => return CommandOutcome::reply(err("ERR syntax error")),
    };

    let now = now_ms();
    let mut db = server.db_mut(client.selected_db);
    purge_expired_key(&mut db, key, now);

    let Some(entry) = db.get(key) else {
        return CommandOutcome::reply(RespFrame::Array(vec![]));
    };
    let Some(zset) = entry.as_sorted_set() else {
        return wrong_type_response();
    };

    let selected = match zrange_collect(
        zset,
        args,
        start_raw,
        stop_raw,
        RangeMode::ByRank,
        true,
        None,
        with_scores,
    ) {
        Ok(v) => v,
        Err(e) => return CommandOutcome::reply(e),
    };

    CommandOutcome::reply(RespFrame::Array(entries_to_resp(&selected, with_scores)))
}

pub(super) fn cmd_zrangestore(
    args: &[Bytes],
    server: &mut ServerState,
    client: &ClientState,
) -> CommandOutcome {
    if args.len() < 4 {
        return wrong_arity("zrangestore");
    }

    let dst = &args[0];
    let src = &args[1];
    let min_raw = &args[2];
    let max_raw = &args[3];
    let options = &args[4..];

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
    let mut db = server.db_mut(client.selected_db);
    purge_expired_key(&mut db, src, now);

    let selected = {
        let Some(entry) = db.get(src) else {
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
    let mut db = server.db_mut(client.selected_db);
    purge_expired_key(&mut db, key, now);

    let Some(entry) = db.get(key) else {
        return CommandOutcome::reply(RespFrame::Integer(0));
    };
    let Some(zset) = entry.as_sorted_set() else {
        return wrong_type_response();
    };

    let count = zset
        .by_score
        .keys()
        .filter(|entry| score_in_range(entry.score.value(), &smin, &smax))
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
    let mut db = server.db_mut(client.selected_db);
    purge_expired_key(&mut db, key, now);

    let Some(entry) = db.get(key) else {
        return CommandOutcome::reply(RespFrame::Integer(0));
    };
    let Some(zset) = entry.as_sorted_set() else {
        return wrong_type_response();
    };

    let count = zset
        .by_score
        .keys()
        .filter(|entry| member_in_lex_range(&entry.member, &lmin, &lmax))
        .count();

    CommandOutcome::reply(RespFrame::Integer(count as i64))
}

pub(super) fn cmd_zrank(
    args: &[Bytes],
    server: &mut ServerState,
    client: &ClientState,
) -> CommandOutcome {
    let [key, member] = args else {
        return wrong_arity("zrank");
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

    match zset.rank(member) {
        Some(rank) => CommandOutcome::reply(RespFrame::Integer(rank as i64)),
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
    let mut db = server.db_mut(client.selected_db);
    purge_expired_key(&mut db, key, now);

    let Some(entry) = db.get(key) else {
        return CommandOutcome::reply(RespFrame::BulkString(None));
    };
    let Some(zset) = entry.as_sorted_set() else {
        return wrong_type_response();
    };

    match zset.rev_rank(member) {
        Some(rank) => CommandOutcome::reply(RespFrame::Integer(rank as i64)),
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
    let mut db = server.db_mut(client.selected_db);
    purge_expired_key(&mut db, key, now);

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

    let to_remove: Vec<Bytes> = zset
        .by_score
        .keys()
        .skip(start)
        .take(stop - start + 1)
        .map(|entry| entry.member.clone())
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
    let mut db = server.db_mut(client.selected_db);
    purge_expired_key(&mut db, key, now);

    let Some(entry) = db.get_mut(key) else {
        return CommandOutcome::reply(RespFrame::Integer(0));
    };
    let Some(zset) = entry.as_sorted_set_mut() else {
        return wrong_type_response();
    };

    let to_remove: Vec<Bytes> = zset
        .by_score
        .keys()
        .filter(|entry| score_in_range(entry.score.value(), &smin, &smax))
        .map(|entry| entry.member.clone())
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
    let mut db = server.db_mut(client.selected_db);
    purge_expired_key(&mut db, key, now);

    let Some(entry) = db.get_mut(key) else {
        return CommandOutcome::reply(RespFrame::Integer(0));
    };
    let Some(zset) = entry.as_sorted_set_mut() else {
        return wrong_type_response();
    };

    let to_remove: Vec<Bytes> = zset
        .by_score
        .keys()
        .filter(|entry| member_in_lex_range(&entry.member, &lmin, &lmax))
        .map(|entry| entry.member.clone())
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
