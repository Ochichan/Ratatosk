use super::shared_support::frame_to_argv_for_persistence;
use std::{
    sync::OnceLock,
    time::{SystemTime, UNIX_EPOCH},
};

use ratatosk_engine::{
    command::supports_readonly_batch_command,
    eviction::estimate_object_memory,
    keyspace::{DbShard, LexBound, ScoreBound, SortedSet},
    object::{format_f64_for_redis, parse_i64},
};

use super::*;

const READONLY_BATCH_ENV: &str = "RATATOSK_PIPELINE_READONLY_BATCH_LOCK";
static READONLY_BATCH_ENABLED: OnceLock<bool> = OnceLock::new();

fn readonly_batch_enabled() -> bool {
    *READONLY_BATCH_ENABLED.get_or_init(|| match std::env::var(READONLY_BATCH_ENV) {
        Ok(value) => {
            !(value == "0"
                || value.eq_ignore_ascii_case("false")
                || value.eq_ignore_ascii_case("no")
                || value.eq_ignore_ascii_case("off"))
        }
        Err(_) => true,
    })
}

fn is_lock_free_fast_command_name(command: &[u8]) -> bool {
    command.eq_ignore_ascii_case(b"PING")
        || command.eq_ignore_ascii_case(b"ECHO")
        || command.eq_ignore_ascii_case(b"TIME")
        || command.eq_ignore_ascii_case(b"DBSIZE")
        || command.eq_ignore_ascii_case(b"TYPE")
        || command.eq_ignore_ascii_case(b"EXISTS")
        || command.eq_ignore_ascii_case(b"GET")
        || command.eq_ignore_ascii_case(b"MGET")
        || command.eq_ignore_ascii_case(b"STRLEN")
        || command.eq_ignore_ascii_case(b"BITCOUNT")
        || command.eq_ignore_ascii_case(b"GETRANGE")
        || command.eq_ignore_ascii_case(b"SUBSTR")
        || command.eq_ignore_ascii_case(b"HGET")
        || command.eq_ignore_ascii_case(b"HMGET")
        || command.eq_ignore_ascii_case(b"HGETALL")
        || command.eq_ignore_ascii_case(b"HKEYS")
        || command.eq_ignore_ascii_case(b"HVALS")
        || command.eq_ignore_ascii_case(b"HEXISTS")
        || command.eq_ignore_ascii_case(b"HLEN")
        || command.eq_ignore_ascii_case(b"HSTRLEN")
        || command.eq_ignore_ascii_case(b"SISMEMBER")
        || command.eq_ignore_ascii_case(b"SMISMEMBER")
        || command.eq_ignore_ascii_case(b"SCARD")
        || command.eq_ignore_ascii_case(b"ZSCORE")
        || command.eq_ignore_ascii_case(b"ZCARD")
        || command.eq_ignore_ascii_case(b"ZMSCORE")
        || command.eq_ignore_ascii_case(b"ZCOUNT")
        || command.eq_ignore_ascii_case(b"ZLEXCOUNT")
        || command.eq_ignore_ascii_case(b"ZRANGE")
        || command.eq_ignore_ascii_case(b"ZRANGEBYSCORE")
        || command.eq_ignore_ascii_case(b"ZREVRANGEBYSCORE")
        || command.eq_ignore_ascii_case(b"ZRANGEBYLEX")
        || command.eq_ignore_ascii_case(b"ZREVRANGEBYLEX")
        || command.eq_ignore_ascii_case(b"ZREVRANGE")
        || command.eq_ignore_ascii_case(b"ZRANK")
        || command.eq_ignore_ascii_case(b"ZREVRANK")
        || command.eq_ignore_ascii_case(b"LLEN")
        || command.eq_ignore_ascii_case(b"LINDEX")
        || command.eq_ignore_ascii_case(b"LRANGE")
        || command.eq_ignore_ascii_case(b"TTL")
        || command.eq_ignore_ascii_case(b"PTTL")
        || command.eq_ignore_ascii_case(b"EXPIRETIME")
        || command.eq_ignore_ascii_case(b"PEXPIRETIME")
        || command.eq_ignore_ascii_case(b"GETBIT")
}

fn is_readonly_batch_command(command: &[u8]) -> bool {
    is_lock_free_fast_command_name(command) || supports_readonly_batch_command(command)
}

fn wrong_arity_response(command: &str) -> RespFrame {
    RespFrame::error_str(&format!(
        "ERR wrong number of arguments for '{command}' command"
    ))
}

fn get_bit(data: &[u8], offset: usize) -> u8 {
    let byte_idx = offset / 8;
    let bit_idx = 7 - (offset % 8);
    if byte_idx >= data.len() {
        0
    } else {
        (data[byte_idx] >> bit_idx) & 1
    }
}

fn popcount_byte(byte: u8) -> i64 {
    i64::from(byte.count_ones())
}

fn resolve_range(mut start: i64, mut end: i64, len: usize) -> (usize, usize) {
    let len_i64 = len as i64;
    if start < 0 {
        start += len_i64;
    }
    if end < 0 {
        end += len_i64;
    }
    if start < 0 {
        start = 0;
    }
    if end < 0 {
        return (1, 0);
    }
    (start as usize, end as usize)
}

fn tracked_remove_from_shard<Q>(
    shard: &mut DbShard,
    server_state: &SharedServerState,
    selected_db: usize,
    key: &Q,
) where
    Bytes: std::borrow::Borrow<Q>,
    Q: std::hash::Hash + Eq + ?Sized,
{
    let Some((owned_key, value)) = shard.data.remove_entry(key) else {
        return;
    };

    shard.expires.remove(owned_key.as_ref());
    let freed = estimate_object_memory(&owned_key, &value);
    server_state.data.sub_memory(selected_db, freed);
}

fn purge_expired_key_in_shard(
    shard: &mut DbShard,
    server_state: &SharedServerState,
    selected_db: usize,
    key: &Bytes,
    now_ms: i64,
) {
    if shard
        .data
        .get(key.as_ref())
        .is_some_and(|value| value.expire_at_ms().is_some_and(|ts| ts <= now_ms))
    {
        tracked_remove_from_shard(shard, server_state, selected_db, key.as_ref());
    }
}

fn purge_expired_keys_in_shard(
    shard: &mut DbShard,
    server_state: &SharedServerState,
    selected_db: usize,
    now_ms: i64,
) {
    let expired_keys = shard
        .data
        .iter()
        .filter(|(_, value)| value.expire_at_ms().is_some_and(|ts| ts <= now_ms))
        .map(|(key, _)| key.clone())
        .collect::<Vec<_>>();

    for key in expired_keys {
        tracked_remove_from_shard(shard, server_state, selected_db, &key);
    }
}

fn parse_score_bound(raw: &Bytes) -> Option<ScoreBound> {
    let raw = std::str::from_utf8(raw).ok()?;
    match raw {
        "-inf" => Some(ScoreBound::NegInf),
        "+inf" | "inf" => Some(ScoreBound::PosInf),
        _ if raw.starts_with('(') => Some(ScoreBound::Exclusive(raw[1..].parse::<f64>().ok()?)),
        _ => Some(ScoreBound::Inclusive(raw.parse::<f64>().ok()?)),
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
        ScoreBound::Inclusive(value) => score >= *value,
        ScoreBound::Exclusive(value) => score > *value,
        ScoreBound::PosInf => false,
    };
    let below_max = match max {
        ScoreBound::PosInf => true,
        ScoreBound::Inclusive(value) => score <= *value,
        ScoreBound::Exclusive(value) => score < *value,
        ScoreBound::NegInf => false,
    };
    above_min && below_max
}

fn member_in_lex_range(member: &Bytes, min: &LexBound, max: &LexBound) -> bool {
    let above_min = match min {
        LexBound::NegInf => true,
        LexBound::Inclusive(value) => member >= value,
        LexBound::Exclusive(value) => member > value,
        LexBound::PosInf => false,
    };
    let below_max = match max {
        LexBound::PosInf => true,
        LexBound::Inclusive(value) => member <= value,
        LexBound::Exclusive(value) => member < value,
        LexBound::NegInf => false,
    };
    above_min && below_max
}

#[derive(Clone, Copy)]
enum LockFreeZrangeMode {
    Rank,
    Score,
    Lex,
}

type LockFreeZrangeOptions = (LockFreeZrangeMode, bool, Option<(i64, i64)>, bool);

fn parse_lock_free_limit(offset_raw: &Bytes, count_raw: &Bytes) -> Result<(i64, i64), RespFrame> {
    let Some(offset) = parse_i64(offset_raw) else {
        return Err(RespFrame::error_str(
            "ERR value is not an integer or out of range",
        ));
    };
    let Some(count) = parse_i64(count_raw) else {
        return Err(RespFrame::error_str(
            "ERR value is not an integer or out of range",
        ));
    };
    Ok((offset, count))
}

fn parse_lock_free_limit_only_options(options: &[Bytes]) -> Result<Option<(i64, i64)>, RespFrame> {
    let mut limit = None;
    let mut idx = 0usize;
    while idx < options.len() {
        if !options[idx].eq_ignore_ascii_case(b"LIMIT") || idx + 2 >= options.len() {
            return Err(RespFrame::error_str("ERR syntax error"));
        }
        limit = Some(parse_lock_free_limit(&options[idx + 1], &options[idx + 2])?);
        idx += 3;
    }
    Ok(limit)
}

fn parse_lock_free_limit_with_scores_options(
    options: &[Bytes],
) -> Result<(Option<(i64, i64)>, bool), RespFrame> {
    let mut limit = None;
    let mut with_scores = false;
    let mut idx = 0usize;
    while idx < options.len() {
        if options[idx].eq_ignore_ascii_case(b"WITHSCORES") {
            with_scores = true;
            idx += 1;
            continue;
        }
        if options[idx].eq_ignore_ascii_case(b"LIMIT") && idx + 2 < options.len() {
            limit = Some(parse_lock_free_limit(&options[idx + 1], &options[idx + 2])?);
            idx += 3;
            continue;
        }
        return Err(RespFrame::error_str("ERR syntax error"));
    }
    Ok((limit, with_scores))
}

fn parse_lock_free_zrange_options(options: &[Bytes]) -> Result<LockFreeZrangeOptions, RespFrame> {
    let mut mode = LockFreeZrangeMode::Rank;
    let mut rev = false;
    let mut limit = None;
    let mut with_scores = false;

    let mut idx = 0usize;
    while idx < options.len() {
        let option = &options[idx];
        if option.eq_ignore_ascii_case(b"BYSCORE") {
            mode = LockFreeZrangeMode::Score;
            idx += 1;
            continue;
        }
        if option.eq_ignore_ascii_case(b"BYLEX") {
            mode = LockFreeZrangeMode::Lex;
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
                return Err(RespFrame::error_str("ERR syntax error"));
            }
            let Some(offset) = parse_i64(&options[idx + 1]) else {
                return Err(RespFrame::error_str(
                    "ERR value is not an integer or out of range",
                ));
            };
            let Some(count) = parse_i64(&options[idx + 2]) else {
                return Err(RespFrame::error_str(
                    "ERR value is not an integer or out of range",
                ));
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

        return Err(RespFrame::error_str("ERR syntax error"));
    }

    Ok((mode, rev, limit, with_scores))
}

fn zrange_entries_to_resp(entries: &[(Bytes, f64)], with_scores: bool) -> RespFrame {
    let capacity = if with_scores {
        entries.len().saturating_mul(2)
    } else {
        entries.len()
    };
    let mut out = Vec::with_capacity(capacity);
    for (member, score) in entries {
        out.push(RespFrame::BulkString(Some(member.clone())));
        if with_scores {
            out.push(RespFrame::BulkString(Some(format_f64_for_redis(*score))));
        }
    }
    RespFrame::Array(out)
}

fn collect_lock_free_zrange_entries(
    zset: &SortedSet,
    min_raw: &Bytes,
    max_raw: &Bytes,
    mode: LockFreeZrangeMode,
    rev: bool,
) -> Result<Vec<(Bytes, f64)>, RespFrame> {
    Ok(match mode {
        LockFreeZrangeMode::Rank => {
            let Some(start_i) = parse_i64(min_raw) else {
                return Err(RespFrame::error_str(
                    "ERR value is not an integer or out of range",
                ));
            };
            let Some(stop_i) = parse_i64(max_raw) else {
                return Err(RespFrame::error_str(
                    "ERR value is not an integer or out of range",
                ));
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
        LockFreeZrangeMode::Score => {
            let (min, max) = if rev {
                let Some(high) = parse_score_bound(min_raw) else {
                    return Err(RespFrame::error_str("ERR min or max is not a float"));
                };
                let Some(low) = parse_score_bound(max_raw) else {
                    return Err(RespFrame::error_str("ERR min or max is not a float"));
                };
                (low, high)
            } else {
                let Some(low) = parse_score_bound(min_raw) else {
                    return Err(RespFrame::error_str("ERR min or max is not a float"));
                };
                let Some(high) = parse_score_bound(max_raw) else {
                    return Err(RespFrame::error_str("ERR min or max is not a float"));
                };
                (low, high)
            };

            if rev {
                zset.by_score
                    .keys()
                    .rev()
                    .filter(|entry| score_in_range(entry.score.value(), &min, &max))
                    .map(|entry| (entry.member.clone(), entry.score.value()))
                    .collect()
            } else {
                zset.by_score
                    .keys()
                    .filter(|entry| score_in_range(entry.score.value(), &min, &max))
                    .map(|entry| (entry.member.clone(), entry.score.value()))
                    .collect()
            }
        }
        LockFreeZrangeMode::Lex => {
            let (min, max) = if rev {
                let Some(high) = parse_lex_bound(min_raw) else {
                    return Err(RespFrame::error_str(
                        "ERR min or max is not a valid string range item",
                    ));
                };
                let Some(low) = parse_lex_bound(max_raw) else {
                    return Err(RespFrame::error_str(
                        "ERR min or max is not a valid string range item",
                    ));
                };
                (low, high)
            } else {
                let Some(low) = parse_lex_bound(min_raw) else {
                    return Err(RespFrame::error_str(
                        "ERR min or max is not a valid string range item",
                    ));
                };
                let Some(high) = parse_lex_bound(max_raw) else {
                    return Err(RespFrame::error_str(
                        "ERR min or max is not a valid string range item",
                    ));
                };
                (low, high)
            };

            if rev {
                zset.by_score
                    .keys()
                    .rev()
                    .filter(|entry| member_in_lex_range(&entry.member, &min, &max))
                    .map(|entry| (entry.member.clone(), entry.score.value()))
                    .collect()
            } else {
                zset.by_score
                    .keys()
                    .filter(|entry| member_in_lex_range(&entry.member, &min, &max))
                    .map(|entry| (entry.member.clone(), entry.score.value()))
                    .collect()
            }
        }
    })
}

fn apply_lock_free_zrange_limit(
    selected: &mut Vec<(Bytes, f64)>,
    limit: Option<(i64, i64)>,
) -> Result<(), RespFrame> {
    if let Some((offset, count)) = limit {
        if offset < 0 {
            return Err(RespFrame::error_str(
                "ERR value is not an integer or out of range",
            ));
        }
        let offset = offset as usize;
        if offset >= selected.len() {
            selected.clear();
        } else {
            if offset > 0 {
                selected.drain(0..offset);
            }
            if count >= 0 {
                selected.truncate(count as usize);
            }
        }
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn lock_free_zrange_response(
    server_state: &SharedServerState,
    selected_db: usize,
    key: &Bytes,
    min_raw: &Bytes,
    max_raw: &Bytes,
    mode: LockFreeZrangeMode,
    rev: bool,
    limit: Option<(i64, i64)>,
    with_scores: bool,
) -> Result<RespFrame, RespFrame> {
    let now_ms = ratatosk_core::time::now_ms();
    let mut db = server_state.data.write_db(selected_db);
    purge_expired_key_in_shard(&mut db, server_state, selected_db, key, now_ms);

    let response = match db.data.get(key.as_ref()) {
        None => RespFrame::Array(vec![]),
        Some(entry) => match entry.as_sorted_set() {
            None => RespFrame::wrongtype(),
            Some(zset) => {
                let mut selected =
                    collect_lock_free_zrange_entries(zset, min_raw, max_raw, mode, rev)?;
                apply_lock_free_zrange_limit(&mut selected, limit)?;
                zrange_entries_to_resp(&selected, with_scores)
            }
        },
    };

    Ok(response)
}

pub(super) fn try_execute_lock_free_fast_command(
    argv: &[Bytes],
    server_state: &SharedServerState,
    client_state: &mut ClientState,
) -> Option<CommandOutcome> {
    if client_state.in_multi() {
        return None;
    }

    let [command, args @ ..] = argv else {
        return None;
    };

    if !is_lock_free_fast_command_name(command) {
        return None;
    }

    let default_acl_policy = server_state.default_acl_policy();
    if !client_state.is_authenticated() {
        if !(default_acl_policy.default_user_is_nopass_enabled()
            && default_acl_policy.default_user_has_full_access())
        {
            return None;
        }
        client_state.authenticate_as(Bytes::from_static(b"default"));
    } else if client_state.acl_user().as_ref() != b"default"
        || !default_acl_policy.default_user_has_full_access()
    {
        return None;
    }

    let response = if command.eq_ignore_ascii_case(b"PING") {
        match args {
            [] => RespFrame::pong(),
            [message] if !message.eq_ignore_ascii_case(b"HEALTH") => {
                RespFrame::BulkString(Some(message.clone()))
            }
            [message] if message.eq_ignore_ascii_case(b"HEALTH") => {
                return None;
            }
            _ => wrong_arity_response("ping"),
        }
    } else if command.eq_ignore_ascii_case(b"ECHO") {
        match args {
            [message] => RespFrame::BulkString(Some(message.clone())),
            _ => wrong_arity_response("echo"),
        }
    } else if command.eq_ignore_ascii_case(b"TIME") {
        match args {
            [] => {
                let total_us = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .map(|duration| duration.as_micros())
                    .unwrap_or_default();
                let sec = total_us / 1_000_000;
                let micro = total_us.saturating_sub(sec.saturating_mul(1_000_000));
                RespFrame::Array(vec![
                    RespFrame::BulkString(Some(Bytes::from(sec.to_string()))),
                    RespFrame::BulkString(Some(Bytes::from(micro.to_string()))),
                ])
            }
            _ => wrong_arity_response("time"),
        }
    } else if command.eq_ignore_ascii_case(b"DBSIZE") {
        match args {
            [] => {
                let now_ms = ratatosk_core::time::now_ms();
                let selected_db = client_state.selected_db();
                let mut db = server_state.data.write_db(selected_db);
                purge_expired_keys_in_shard(&mut db, server_state, selected_db, now_ms);
                RespFrame::Integer(db.data.len() as i64)
            }
            _ => wrong_arity_response("dbsize"),
        }
    } else if command.eq_ignore_ascii_case(b"TYPE") {
        match args {
            [key] => {
                let now_ms = ratatosk_core::time::now_ms();
                let mut db = server_state.data.write_db(client_state.selected_db());
                purge_expired_key_in_shard(
                    &mut db,
                    server_state,
                    client_state.selected_db(),
                    key,
                    now_ms,
                );
                let value_type = db
                    .data
                    .get(key.as_ref())
                    .map_or("none", ratatosk_engine::keyspace::StoredValue::type_name);
                RespFrame::simple_str(value_type)
            }
            _ => wrong_arity_response("type"),
        }
    } else if command.eq_ignore_ascii_case(b"GET") {
        match args {
            [key] => {
                let now_ms = ratatosk_core::time::now_ms();
                let mut db = server_state.data.write_db(client_state.selected_db());
                purge_expired_key_in_shard(
                    &mut db,
                    server_state,
                    client_state.selected_db(),
                    key,
                    now_ms,
                );

                match db.data.get(key.as_ref()) {
                    None => {
                        server_state.stats.mark_keyspace_miss();
                        RespFrame::BulkString(None)
                    }
                    Some(entry) if !entry.is_string() => RespFrame::wrongtype(),
                    Some(entry) => {
                        server_state.stats.mark_keyspace_hit();
                        RespFrame::BulkString(entry.as_string_bytes())
                    }
                }
            }
            _ => wrong_arity_response("get"),
        }
    } else if command.eq_ignore_ascii_case(b"MGET") {
        match args {
            [] => wrong_arity_response("mget"),
            _ => {
                let now_ms = ratatosk_core::time::now_ms();
                let mut db = server_state.data.write_db(client_state.selected_db());
                let mut out = Vec::with_capacity(args.len());
                for key in args {
                    purge_expired_key_in_shard(
                        &mut db,
                        server_state,
                        client_state.selected_db(),
                        key,
                        now_ms,
                    );
                    let value = db
                        .data
                        .get(key.as_ref())
                        .and_then(|entry| entry.as_string_bytes());
                    out.push(RespFrame::BulkString(value));
                }
                RespFrame::Array(out)
            }
        }
    } else if command.eq_ignore_ascii_case(b"STRLEN") {
        match args {
            [key] => {
                let now_ms = ratatosk_core::time::now_ms();
                let mut db = server_state.data.write_db(client_state.selected_db());
                purge_expired_key_in_shard(
                    &mut db,
                    server_state,
                    client_state.selected_db(),
                    key,
                    now_ms,
                );
                match db.data.get(key.as_ref()) {
                    None => RespFrame::Integer(0),
                    Some(entry) if !entry.is_string() => RespFrame::wrongtype(),
                    Some(entry) => {
                        let len = entry.as_string_bytes().map_or(0, |b| b.len());
                        RespFrame::Integer(i64::try_from(len).unwrap_or(i64::MAX))
                    }
                }
            }
            _ => wrong_arity_response("strlen"),
        }
    } else if command.eq_ignore_ascii_case(b"BITCOUNT") {
        if args.is_empty() || args.len() == 2 || args.len() > 4 {
            RespFrame::error_str("ERR wrong number of arguments for 'bitcount' command")
        } else {
            let key = &args[0];
            let now_ms = ratatosk_core::time::now_ms();
            let mut db = server_state.data.write_db(client_state.selected_db());
            purge_expired_key_in_shard(
                &mut db,
                server_state,
                client_state.selected_db(),
                key,
                now_ms,
            );

            match db.data.get(key.as_ref()) {
                None => RespFrame::Integer(0),
                Some(entry) if !entry.is_string() => RespFrame::wrongtype(),
                Some(entry) => {
                    let data = entry.as_string().map_or(&[][..], Bytes::as_ref);
                    if data.is_empty() {
                        RespFrame::Integer(0)
                    } else if args.len() == 1 {
                        RespFrame::Integer(data.iter().map(|byte| popcount_byte(*byte)).sum())
                    } else {
                        let Some(start_raw) = parse_i64(&args[1]) else {
                            return Some(CommandOutcome {
                                response: RespFrame::error_str(
                                    "ERR value is not an integer or out of range",
                                ),
                                close: false,
                                retry_blocking: None,
                                delay_ms: None,
                                config_dirty: false,
                                acl_dirty: false,
                            });
                        };
                        let Some(end_raw) = parse_i64(&args[2]) else {
                            return Some(CommandOutcome {
                                response: RespFrame::error_str(
                                    "ERR value is not an integer or out of range",
                                ),
                                close: false,
                                retry_blocking: None,
                                delay_ms: None,
                                config_dirty: false,
                                acl_dirty: false,
                            });
                        };

                        let bit_mode = if args.len() == 4 {
                            if args[3].eq_ignore_ascii_case(b"BYTE") {
                                false
                            } else if args[3].eq_ignore_ascii_case(b"BIT") {
                                true
                            } else {
                                return Some(CommandOutcome {
                                    response: RespFrame::error_str("ERR syntax error"),
                                    close: false,
                                    retry_blocking: None,
                                    delay_ms: None,
                                    config_dirty: false,
                                    acl_dirty: false,
                                });
                            }
                        } else {
                            false
                        };

                        if bit_mode {
                            let total_bits = data.len().saturating_mul(8);
                            let (start, end) = resolve_range(start_raw, end_raw, total_bits);
                            if start > end || start >= total_bits {
                                RespFrame::Integer(0)
                            } else {
                                let end = end.min(total_bits.saturating_sub(1));
                                let mut count = 0i64;
                                for bit_pos in start..=end {
                                    count += i64::from(get_bit(data, bit_pos));
                                }
                                RespFrame::Integer(count)
                            }
                        } else {
                            let byte_len = data.len();
                            let (start, end) = resolve_range(start_raw, end_raw, byte_len);
                            if start > end || start >= byte_len {
                                RespFrame::Integer(0)
                            } else {
                                let end = end.min(byte_len.saturating_sub(1));
                                RespFrame::Integer(
                                    data[start..=end]
                                        .iter()
                                        .map(|byte| popcount_byte(*byte))
                                        .sum(),
                                )
                            }
                        }
                    }
                }
            }
        }
    } else if command.eq_ignore_ascii_case(b"GETBIT") {
        match args {
            [key, offset_raw] => {
                let Some(offset) = parse_i64(offset_raw) else {
                    return Some(CommandOutcome {
                        response: RespFrame::error_str(
                            "ERR bit offset is not an integer or out of range",
                        ),
                        close: false,
                        retry_blocking: None,
                        delay_ms: None,
                        config_dirty: false,
                        acl_dirty: false,
                    });
                };
                if offset < 0 {
                    return Some(CommandOutcome {
                        response: RespFrame::error_str(
                            "ERR bit offset is not an integer or out of range",
                        ),
                        close: false,
                        retry_blocking: None,
                        delay_ms: None,
                        config_dirty: false,
                        acl_dirty: false,
                    });
                }

                let now_ms = ratatosk_core::time::now_ms();
                let mut db = server_state.data.write_db(client_state.selected_db());
                purge_expired_key_in_shard(
                    &mut db,
                    server_state,
                    client_state.selected_db(),
                    key,
                    now_ms,
                );

                match db.data.get(key.as_ref()) {
                    None => RespFrame::Integer(0),
                    Some(entry) if !entry.is_string() => RespFrame::wrongtype(),
                    Some(entry) => RespFrame::Integer(i64::from(get_bit(
                        entry.as_string().map_or(&[][..], Bytes::as_ref),
                        offset as usize,
                    ))),
                }
            }
            _ => wrong_arity_response("getbit"),
        }
    } else if command.eq_ignore_ascii_case(b"GETRANGE") || command.eq_ignore_ascii_case(b"SUBSTR") {
        match args {
            [key, start_raw, end_raw] => {
                let Some(start) = parse_i64(start_raw) else {
                    return Some(CommandOutcome {
                        response: RespFrame::error_str(
                            "ERR value is not an integer or out of range",
                        ),
                        close: false,
                        retry_blocking: None,
                        delay_ms: None,
                        config_dirty: false,
                        acl_dirty: false,
                    });
                };
                let Some(end) = parse_i64(end_raw) else {
                    return Some(CommandOutcome {
                        response: RespFrame::error_str(
                            "ERR value is not an integer or out of range",
                        ),
                        close: false,
                        retry_blocking: None,
                        delay_ms: None,
                        config_dirty: false,
                        acl_dirty: false,
                    });
                };

                let now_ms = ratatosk_core::time::now_ms();
                let mut db = server_state.data.write_db(client_state.selected_db());
                purge_expired_key_in_shard(
                    &mut db,
                    server_state,
                    client_state.selected_db(),
                    key,
                    now_ms,
                );

                match db.data.get(key.as_ref()) {
                    None => RespFrame::BulkString(Some(Bytes::new())),
                    Some(entry) if !entry.is_string() => RespFrame::wrongtype(),
                    Some(entry) => {
                        let bytes = entry.as_string().map_or(&[][..], Bytes::as_ref);
                        if let Some((range_start, range_end)) =
                            normalize_range(bytes.len(), start, end)
                        {
                            RespFrame::BulkString(Some(Bytes::copy_from_slice(
                                &bytes[range_start..=range_end],
                            )))
                        } else {
                            RespFrame::BulkString(Some(Bytes::new()))
                        }
                    }
                }
            }
            _ => wrong_arity_response("getrange"),
        }
    } else if command.eq_ignore_ascii_case(b"HGET") {
        match args {
            [key, field] => {
                let now_ms = ratatosk_core::time::now_ms();
                let mut db = server_state.data.write_db(client_state.selected_db());
                purge_expired_key_in_shard(
                    &mut db,
                    server_state,
                    client_state.selected_db(),
                    key,
                    now_ms,
                );

                match db.data.get(key.as_ref()) {
                    None => RespFrame::BulkString(None),
                    Some(entry) => match entry.as_hash() {
                        None => RespFrame::wrongtype(),
                        Some(hash) => RespFrame::BulkString(
                            hash.get(field)
                                .filter(|field_entry| !field_entry.is_expired(now_ms))
                                .map(|field_entry| field_entry.value.clone()),
                        ),
                    },
                }
            }
            _ => wrong_arity_response("hget"),
        }
    } else if command.eq_ignore_ascii_case(b"HMGET") {
        match args {
            [] | [_] => wrong_arity_response("hmget"),
            [key, fields @ ..] => {
                let now_ms = ratatosk_core::time::now_ms();
                let mut db = server_state.data.write_db(client_state.selected_db());
                purge_expired_key_in_shard(
                    &mut db,
                    server_state,
                    client_state.selected_db(),
                    key,
                    now_ms,
                );

                match db.data.get(key.as_ref()) {
                    None => RespFrame::Array(
                        fields
                            .iter()
                            .map(|_| RespFrame::BulkString(None))
                            .collect::<Vec<_>>(),
                    ),
                    Some(entry) => match entry.as_hash() {
                        None => RespFrame::wrongtype(),
                        Some(hash) => RespFrame::Array(
                            fields
                                .iter()
                                .map(|field| {
                                    RespFrame::BulkString(
                                        hash.get(field)
                                            .filter(|field_entry| !field_entry.is_expired(now_ms))
                                            .map(|field_entry| field_entry.value.clone()),
                                    )
                                })
                                .collect::<Vec<_>>(),
                        ),
                    },
                }
            }
        }
    } else if command.eq_ignore_ascii_case(b"HGETALL") {
        match args {
            [key] => {
                let now_ms = ratatosk_core::time::now_ms();
                let mut db = server_state.data.write_db(client_state.selected_db());
                purge_expired_key_in_shard(
                    &mut db,
                    server_state,
                    client_state.selected_db(),
                    key,
                    now_ms,
                );

                match db.data.get(key.as_ref()) {
                    None => RespFrame::Array(vec![]),
                    Some(entry) => match entry.as_hash() {
                        None => RespFrame::wrongtype(),
                        Some(hash) => {
                            let mut out = Vec::with_capacity(hash.len().saturating_mul(2));
                            for (field, field_entry) in hash.iter() {
                                if field_entry.is_expired(now_ms) {
                                    continue;
                                }
                                out.push(RespFrame::BulkString(Some(field.clone())));
                                out.push(RespFrame::BulkString(Some(field_entry.value.clone())));
                            }
                            RespFrame::Array(out)
                        }
                    },
                }
            }
            _ => wrong_arity_response("hgetall"),
        }
    } else if command.eq_ignore_ascii_case(b"HKEYS") {
        match args {
            [key] => {
                let now_ms = ratatosk_core::time::now_ms();
                let mut db = server_state.data.write_db(client_state.selected_db());
                purge_expired_key_in_shard(
                    &mut db,
                    server_state,
                    client_state.selected_db(),
                    key,
                    now_ms,
                );

                match db.data.get(key.as_ref()) {
                    None => RespFrame::Array(vec![]),
                    Some(entry) => match entry.as_hash() {
                        None => RespFrame::wrongtype(),
                        Some(hash) => RespFrame::Array(
                            hash.iter()
                                .filter(|(_, field_entry)| !field_entry.is_expired(now_ms))
                                .map(|(field, _)| RespFrame::BulkString(Some(field.clone())))
                                .collect::<Vec<_>>(),
                        ),
                    },
                }
            }
            _ => wrong_arity_response("hkeys"),
        }
    } else if command.eq_ignore_ascii_case(b"HVALS") {
        match args {
            [key] => {
                let now_ms = ratatosk_core::time::now_ms();
                let mut db = server_state.data.write_db(client_state.selected_db());
                purge_expired_key_in_shard(
                    &mut db,
                    server_state,
                    client_state.selected_db(),
                    key,
                    now_ms,
                );

                match db.data.get(key.as_ref()) {
                    None => RespFrame::Array(vec![]),
                    Some(entry) => match entry.as_hash() {
                        None => RespFrame::wrongtype(),
                        Some(hash) => RespFrame::Array(
                            hash.iter()
                                .filter(|(_, field_entry)| !field_entry.is_expired(now_ms))
                                .map(|(_, field_entry)| {
                                    RespFrame::BulkString(Some(field_entry.value.clone()))
                                })
                                .collect::<Vec<_>>(),
                        ),
                    },
                }
            }
            _ => wrong_arity_response("hvals"),
        }
    } else if command.eq_ignore_ascii_case(b"HEXISTS") {
        match args {
            [key, field] => {
                let now_ms = ratatosk_core::time::now_ms();
                let mut db = server_state.data.write_db(client_state.selected_db());
                purge_expired_key_in_shard(
                    &mut db,
                    server_state,
                    client_state.selected_db(),
                    key,
                    now_ms,
                );

                match db.data.get(key.as_ref()) {
                    None => RespFrame::Integer(0),
                    Some(entry) => match entry.as_hash() {
                        None => RespFrame::wrongtype(),
                        Some(hash) => RespFrame::Integer(
                            if hash
                                .get(field)
                                .is_some_and(|field_entry| !field_entry.is_expired(now_ms))
                            {
                                1
                            } else {
                                0
                            },
                        ),
                    },
                }
            }
            _ => wrong_arity_response("hexists"),
        }
    } else if command.eq_ignore_ascii_case(b"HLEN") {
        match args {
            [key] => {
                let now_ms = ratatosk_core::time::now_ms();
                let mut db = server_state.data.write_db(client_state.selected_db());
                purge_expired_key_in_shard(
                    &mut db,
                    server_state,
                    client_state.selected_db(),
                    key,
                    now_ms,
                );

                match db.data.get(key.as_ref()) {
                    None => RespFrame::Integer(0),
                    Some(entry) => match entry.as_hash() {
                        None => RespFrame::wrongtype(),
                        Some(hash) => RespFrame::Integer(
                            i64::try_from(
                                hash.values()
                                    .filter(|field_entry| !field_entry.is_expired(now_ms))
                                    .count(),
                            )
                            .unwrap_or(i64::MAX),
                        ),
                    },
                }
            }
            _ => wrong_arity_response("hlen"),
        }
    } else if command.eq_ignore_ascii_case(b"HSTRLEN") {
        match args {
            [key, field] => {
                let now_ms = ratatosk_core::time::now_ms();
                let mut db = server_state.data.write_db(client_state.selected_db());
                purge_expired_key_in_shard(
                    &mut db,
                    server_state,
                    client_state.selected_db(),
                    key,
                    now_ms,
                );

                match db.data.get(key.as_ref()) {
                    None => RespFrame::Integer(0),
                    Some(entry) => match entry.as_hash() {
                        None => RespFrame::wrongtype(),
                        Some(hash) => RespFrame::Integer(
                            i64::try_from(
                                hash.get(field)
                                    .filter(|field_entry| !field_entry.is_expired(now_ms))
                                    .map_or(0usize, |field_entry| field_entry.value.len()),
                            )
                            .unwrap_or(i64::MAX),
                        ),
                    },
                }
            }
            _ => wrong_arity_response("hstrlen"),
        }
    } else if command.eq_ignore_ascii_case(b"SISMEMBER") {
        match args {
            [key, member] => {
                let now_ms = ratatosk_core::time::now_ms();
                let mut db = server_state.data.write_db(client_state.selected_db());
                purge_expired_key_in_shard(
                    &mut db,
                    server_state,
                    client_state.selected_db(),
                    key,
                    now_ms,
                );

                match db.data.get(key.as_ref()) {
                    None => RespFrame::Integer(0),
                    Some(entry) => match entry.set_contains(member) {
                        None => RespFrame::wrongtype(),
                        Some(contains) => RespFrame::Integer(if contains { 1 } else { 0 }),
                    },
                }
            }
            _ => wrong_arity_response("sismember"),
        }
    } else if command.eq_ignore_ascii_case(b"SMISMEMBER") {
        match args {
            [] | [_] => wrong_arity_response("smismember"),
            [key, members @ ..] => {
                let now_ms = ratatosk_core::time::now_ms();
                let mut db = server_state.data.write_db(client_state.selected_db());
                purge_expired_key_in_shard(
                    &mut db,
                    server_state,
                    client_state.selected_db(),
                    key,
                    now_ms,
                );

                match db.data.get(key.as_ref()) {
                    None => RespFrame::Array(
                        members
                            .iter()
                            .map(|_| RespFrame::Integer(0))
                            .collect::<Vec<_>>(),
                    ),
                    Some(entry) => {
                        if !entry.is_set() {
                            RespFrame::wrongtype()
                        } else {
                            RespFrame::Array(
                                members
                                    .iter()
                                    .map(|member| {
                                        RespFrame::Integer(
                                            if entry.set_contains(member).unwrap_or(false) {
                                                1
                                            } else {
                                                0
                                            },
                                        )
                                    })
                                    .collect::<Vec<_>>(),
                            )
                        }
                    }
                }
            }
        }
    } else if command.eq_ignore_ascii_case(b"SCARD") {
        match args {
            [key] => {
                let now_ms = ratatosk_core::time::now_ms();
                let mut db = server_state.data.write_db(client_state.selected_db());
                purge_expired_key_in_shard(
                    &mut db,
                    server_state,
                    client_state.selected_db(),
                    key,
                    now_ms,
                );

                match db.data.get(key.as_ref()) {
                    None => RespFrame::Integer(0),
                    Some(entry) => match entry.set_len() {
                        None => RespFrame::wrongtype(),
                        Some(len) => RespFrame::Integer(i64::try_from(len).unwrap_or(i64::MAX)),
                    },
                }
            }
            _ => wrong_arity_response("scard"),
        }
    } else if command.eq_ignore_ascii_case(b"ZSCORE") {
        match args {
            [key, member] => {
                let now_ms = ratatosk_core::time::now_ms();
                let mut db = server_state.data.write_db(client_state.selected_db());
                purge_expired_key_in_shard(
                    &mut db,
                    server_state,
                    client_state.selected_db(),
                    key,
                    now_ms,
                );

                match db.data.get(key.as_ref()) {
                    None => RespFrame::BulkString(None),
                    Some(entry) => match entry.as_sorted_set() {
                        None => RespFrame::wrongtype(),
                        Some(zset) => {
                            RespFrame::BulkString(zset.score(member).map(format_f64_for_redis))
                        }
                    },
                }
            }
            _ => wrong_arity_response("zscore"),
        }
    } else if command.eq_ignore_ascii_case(b"ZCARD") {
        match args {
            [key] => {
                let now_ms = ratatosk_core::time::now_ms();
                let mut db = server_state.data.write_db(client_state.selected_db());
                purge_expired_key_in_shard(
                    &mut db,
                    server_state,
                    client_state.selected_db(),
                    key,
                    now_ms,
                );

                match db.data.get(key.as_ref()) {
                    None => RespFrame::Integer(0),
                    Some(entry) => match entry.as_sorted_set() {
                        None => RespFrame::wrongtype(),
                        Some(zset) => {
                            RespFrame::Integer(i64::try_from(zset.len()).unwrap_or(i64::MAX))
                        }
                    },
                }
            }
            _ => wrong_arity_response("zcard"),
        }
    } else if command.eq_ignore_ascii_case(b"ZMSCORE") {
        match args {
            [] | [_] => wrong_arity_response("zmscore"),
            [key, members @ ..] => {
                let now_ms = ratatosk_core::time::now_ms();
                let mut db = server_state.data.write_db(client_state.selected_db());
                purge_expired_key_in_shard(
                    &mut db,
                    server_state,
                    client_state.selected_db(),
                    key,
                    now_ms,
                );

                match db.data.get(key.as_ref()) {
                    None => RespFrame::Array(
                        members
                            .iter()
                            .map(|_| RespFrame::BulkString(None))
                            .collect::<Vec<_>>(),
                    ),
                    Some(entry) => match entry.as_sorted_set() {
                        None => RespFrame::wrongtype(),
                        Some(zset) => RespFrame::Array(
                            members
                                .iter()
                                .map(|member| {
                                    RespFrame::BulkString(
                                        zset.score(member).map(format_f64_for_redis),
                                    )
                                })
                                .collect::<Vec<_>>(),
                        ),
                    },
                }
            }
        }
    } else if command.eq_ignore_ascii_case(b"ZCOUNT") {
        match args {
            [key, min_raw, max_raw] => {
                let Some(min) = parse_score_bound(min_raw) else {
                    return Some(CommandOutcome {
                        response: RespFrame::error_str("ERR min or max is not a float"),
                        close: false,
                        retry_blocking: None,
                        delay_ms: None,
                        config_dirty: false,
                        acl_dirty: false,
                    });
                };
                let Some(max) = parse_score_bound(max_raw) else {
                    return Some(CommandOutcome {
                        response: RespFrame::error_str("ERR min or max is not a float"),
                        close: false,
                        retry_blocking: None,
                        delay_ms: None,
                        config_dirty: false,
                        acl_dirty: false,
                    });
                };

                let now_ms = ratatosk_core::time::now_ms();
                let mut db = server_state.data.write_db(client_state.selected_db());
                purge_expired_key_in_shard(
                    &mut db,
                    server_state,
                    client_state.selected_db(),
                    key,
                    now_ms,
                );

                match db.data.get(key.as_ref()) {
                    None => RespFrame::Integer(0),
                    Some(entry) => match entry.as_sorted_set() {
                        None => RespFrame::wrongtype(),
                        Some(zset) => RespFrame::Integer(
                            i64::try_from(
                                zset.by_score
                                    .keys()
                                    .filter(|member| {
                                        score_in_range(member.score.value(), &min, &max)
                                    })
                                    .count(),
                            )
                            .unwrap_or(i64::MAX),
                        ),
                    },
                }
            }
            _ => wrong_arity_response("zcount"),
        }
    } else if command.eq_ignore_ascii_case(b"ZLEXCOUNT") {
        match args {
            [key, min_raw, max_raw] => {
                let Some(min) = parse_lex_bound(min_raw) else {
                    return Some(CommandOutcome {
                        response: RespFrame::error_str(
                            "ERR min or max is not a valid string range item",
                        ),
                        close: false,
                        retry_blocking: None,
                        delay_ms: None,
                        config_dirty: false,
                        acl_dirty: false,
                    });
                };
                let Some(max) = parse_lex_bound(max_raw) else {
                    return Some(CommandOutcome {
                        response: RespFrame::error_str(
                            "ERR min or max is not a valid string range item",
                        ),
                        close: false,
                        retry_blocking: None,
                        delay_ms: None,
                        config_dirty: false,
                        acl_dirty: false,
                    });
                };

                let now_ms = ratatosk_core::time::now_ms();
                let mut db = server_state.data.write_db(client_state.selected_db());
                purge_expired_key_in_shard(
                    &mut db,
                    server_state,
                    client_state.selected_db(),
                    key,
                    now_ms,
                );

                match db.data.get(key.as_ref()) {
                    None => RespFrame::Integer(0),
                    Some(entry) => match entry.as_sorted_set() {
                        None => RespFrame::wrongtype(),
                        Some(zset) => RespFrame::Integer(
                            i64::try_from(
                                zset.by_score
                                    .keys()
                                    .filter(|member| {
                                        member_in_lex_range(&member.member, &min, &max)
                                    })
                                    .count(),
                            )
                            .unwrap_or(i64::MAX),
                        ),
                    },
                }
            }
            _ => wrong_arity_response("zlexcount"),
        }
    } else if command.eq_ignore_ascii_case(b"ZRANGE") {
        match args {
            [key, min_raw, max_raw, options @ ..] => {
                let (mode, rev, limit, with_scores) = match parse_lock_free_zrange_options(options)
                {
                    Ok(parsed) => parsed,
                    Err(response) => {
                        return Some(CommandOutcome {
                            response,
                            close: false,
                            retry_blocking: None,
                            delay_ms: None,
                            config_dirty: false,
                            acl_dirty: false,
                        });
                    }
                };
                match lock_free_zrange_response(
                    server_state,
                    client_state.selected_db(),
                    key,
                    min_raw,
                    max_raw,
                    mode,
                    rev,
                    limit,
                    with_scores,
                ) {
                    Ok(response) => response,
                    Err(response) => {
                        return Some(CommandOutcome {
                            response,
                            close: false,
                            retry_blocking: None,
                            delay_ms: None,
                            config_dirty: false,
                            acl_dirty: false,
                        });
                    }
                }
            }
            _ => wrong_arity_response("zrange"),
        }
    } else if command.eq_ignore_ascii_case(b"ZRANGEBYSCORE") {
        match args {
            [key, min_raw, max_raw, options @ ..] => {
                let (limit, with_scores) = match parse_lock_free_limit_with_scores_options(options)
                {
                    Ok(parsed) => parsed,
                    Err(response) => {
                        return Some(CommandOutcome {
                            response,
                            close: false,
                            retry_blocking: None,
                            delay_ms: None,
                            config_dirty: false,
                            acl_dirty: false,
                        });
                    }
                };

                match lock_free_zrange_response(
                    server_state,
                    client_state.selected_db(),
                    key,
                    min_raw,
                    max_raw,
                    LockFreeZrangeMode::Score,
                    false,
                    limit,
                    with_scores,
                ) {
                    Ok(response) => response,
                    Err(response) => {
                        return Some(CommandOutcome {
                            response,
                            close: false,
                            retry_blocking: None,
                            delay_ms: None,
                            config_dirty: false,
                            acl_dirty: false,
                        });
                    }
                }
            }
            _ => wrong_arity_response("zrangebyscore"),
        }
    } else if command.eq_ignore_ascii_case(b"ZREVRANGEBYSCORE") {
        match args {
            [key, max_raw, min_raw, options @ ..] => {
                let (limit, with_scores) = match parse_lock_free_limit_with_scores_options(options)
                {
                    Ok(parsed) => parsed,
                    Err(response) => {
                        return Some(CommandOutcome {
                            response,
                            close: false,
                            retry_blocking: None,
                            delay_ms: None,
                            config_dirty: false,
                            acl_dirty: false,
                        });
                    }
                };

                match lock_free_zrange_response(
                    server_state,
                    client_state.selected_db(),
                    key,
                    max_raw,
                    min_raw,
                    LockFreeZrangeMode::Score,
                    true,
                    limit,
                    with_scores,
                ) {
                    Ok(response) => response,
                    Err(response) => {
                        return Some(CommandOutcome {
                            response,
                            close: false,
                            retry_blocking: None,
                            delay_ms: None,
                            config_dirty: false,
                            acl_dirty: false,
                        });
                    }
                }
            }
            _ => wrong_arity_response("zrevrangebyscore"),
        }
    } else if command.eq_ignore_ascii_case(b"ZRANGEBYLEX") {
        match args {
            [key, min_raw, max_raw, options @ ..] => {
                let limit = match parse_lock_free_limit_only_options(options) {
                    Ok(parsed) => parsed,
                    Err(response) => {
                        return Some(CommandOutcome {
                            response,
                            close: false,
                            retry_blocking: None,
                            delay_ms: None,
                            config_dirty: false,
                            acl_dirty: false,
                        });
                    }
                };

                match lock_free_zrange_response(
                    server_state,
                    client_state.selected_db(),
                    key,
                    min_raw,
                    max_raw,
                    LockFreeZrangeMode::Lex,
                    false,
                    limit,
                    false,
                ) {
                    Ok(response) => response,
                    Err(response) => {
                        return Some(CommandOutcome {
                            response,
                            close: false,
                            retry_blocking: None,
                            delay_ms: None,
                            config_dirty: false,
                            acl_dirty: false,
                        });
                    }
                }
            }
            _ => wrong_arity_response("zrangebylex"),
        }
    } else if command.eq_ignore_ascii_case(b"ZREVRANGEBYLEX") {
        match args {
            [key, max_raw, min_raw, options @ ..] => {
                let limit = match parse_lock_free_limit_only_options(options) {
                    Ok(parsed) => parsed,
                    Err(response) => {
                        return Some(CommandOutcome {
                            response,
                            close: false,
                            retry_blocking: None,
                            delay_ms: None,
                            config_dirty: false,
                            acl_dirty: false,
                        });
                    }
                };

                match lock_free_zrange_response(
                    server_state,
                    client_state.selected_db(),
                    key,
                    max_raw,
                    min_raw,
                    LockFreeZrangeMode::Lex,
                    true,
                    limit,
                    false,
                ) {
                    Ok(response) => response,
                    Err(response) => {
                        return Some(CommandOutcome {
                            response,
                            close: false,
                            retry_blocking: None,
                            delay_ms: None,
                            config_dirty: false,
                            acl_dirty: false,
                        });
                    }
                }
            }
            _ => wrong_arity_response("zrevrangebylex"),
        }
    } else if command.eq_ignore_ascii_case(b"ZREVRANGE") {
        match args {
            [key, start_raw, stop_raw] => match lock_free_zrange_response(
                server_state,
                client_state.selected_db(),
                key,
                start_raw,
                stop_raw,
                LockFreeZrangeMode::Rank,
                true,
                None,
                false,
            ) {
                Ok(response) => response,
                Err(response) => {
                    return Some(CommandOutcome {
                        response,
                        close: false,
                        retry_blocking: None,
                        delay_ms: None,
                        config_dirty: false,
                        acl_dirty: false,
                    });
                }
            },
            [key, start_raw, stop_raw, with_scores_raw]
                if with_scores_raw.eq_ignore_ascii_case(b"WITHSCORES") =>
            {
                match lock_free_zrange_response(
                    server_state,
                    client_state.selected_db(),
                    key,
                    start_raw,
                    stop_raw,
                    LockFreeZrangeMode::Rank,
                    true,
                    None,
                    true,
                ) {
                    Ok(response) => response,
                    Err(response) => {
                        return Some(CommandOutcome {
                            response,
                            close: false,
                            retry_blocking: None,
                            delay_ms: None,
                            config_dirty: false,
                            acl_dirty: false,
                        });
                    }
                }
            }
            [_, _, _, ..] => RespFrame::error_str("ERR syntax error"),
            _ => wrong_arity_response("zrevrange"),
        }
    } else if command.eq_ignore_ascii_case(b"ZRANK") {
        match args {
            [key, member] => {
                let now_ms = ratatosk_core::time::now_ms();
                let mut db = server_state.data.write_db(client_state.selected_db());
                purge_expired_key_in_shard(
                    &mut db,
                    server_state,
                    client_state.selected_db(),
                    key,
                    now_ms,
                );

                match db.data.get(key.as_ref()) {
                    None => RespFrame::BulkString(None),
                    Some(entry) => match entry.as_sorted_set() {
                        None => RespFrame::wrongtype(),
                        Some(zset) => match zset.rank(member) {
                            Some(rank) => {
                                RespFrame::Integer(i64::try_from(rank).unwrap_or(i64::MAX))
                            }
                            None => RespFrame::BulkString(None),
                        },
                    },
                }
            }
            _ => wrong_arity_response("zrank"),
        }
    } else if command.eq_ignore_ascii_case(b"ZREVRANK") {
        match args {
            [key, member] => {
                let now_ms = ratatosk_core::time::now_ms();
                let mut db = server_state.data.write_db(client_state.selected_db());
                purge_expired_key_in_shard(
                    &mut db,
                    server_state,
                    client_state.selected_db(),
                    key,
                    now_ms,
                );

                match db.data.get(key.as_ref()) {
                    None => RespFrame::BulkString(None),
                    Some(entry) => match entry.as_sorted_set() {
                        None => RespFrame::wrongtype(),
                        Some(zset) => match zset.rev_rank(member) {
                            Some(rank) => {
                                RespFrame::Integer(i64::try_from(rank).unwrap_or(i64::MAX))
                            }
                            None => RespFrame::BulkString(None),
                        },
                    },
                }
            }
            _ => wrong_arity_response("zrevrank"),
        }
    } else if command.eq_ignore_ascii_case(b"LLEN") {
        match args {
            [key] => {
                let now_ms = ratatosk_core::time::now_ms();
                let mut db = server_state.data.write_db(client_state.selected_db());
                purge_expired_key_in_shard(
                    &mut db,
                    server_state,
                    client_state.selected_db(),
                    key,
                    now_ms,
                );

                match db.data.get(key.as_ref()) {
                    None => RespFrame::Integer(0),
                    Some(entry) => match entry.as_list() {
                        None => RespFrame::wrongtype(),
                        Some(list) => {
                            RespFrame::Integer(i64::try_from(list.len()).unwrap_or(i64::MAX))
                        }
                    },
                }
            }
            _ => wrong_arity_response("llen"),
        }
    } else if command.eq_ignore_ascii_case(b"LINDEX") {
        match args {
            [key, raw_index] => {
                let Some(raw_index) = parse_i64(raw_index) else {
                    return Some(CommandOutcome {
                        response: RespFrame::error_str(
                            "ERR value is not an integer or out of range",
                        ),
                        close: false,
                        retry_blocking: None,
                        delay_ms: None,
                        config_dirty: false,
                        acl_dirty: false,
                    });
                };

                let now_ms = ratatosk_core::time::now_ms();
                let mut db = server_state.data.write_db(client_state.selected_db());
                purge_expired_key_in_shard(
                    &mut db,
                    server_state,
                    client_state.selected_db(),
                    key,
                    now_ms,
                );

                match db.data.get(key.as_ref()) {
                    None => RespFrame::BulkString(None),
                    Some(entry) => match entry.as_list() {
                        None => RespFrame::wrongtype(),
                        Some(list) => {
                            let len = i64::try_from(list.len()).unwrap_or(i64::MAX);
                            let index = if raw_index < 0 {
                                len.saturating_add(raw_index)
                            } else {
                                raw_index
                            };

                            if index < 0 || index >= len {
                                RespFrame::BulkString(None)
                            } else {
                                RespFrame::BulkString(Some(list[index as usize].clone()))
                            }
                        }
                    },
                }
            }
            _ => wrong_arity_response("lindex"),
        }
    } else if command.eq_ignore_ascii_case(b"LRANGE") {
        match args {
            [key, start_raw, end_raw] => {
                let Some(start) = parse_i64(start_raw) else {
                    return Some(CommandOutcome {
                        response: RespFrame::error_str(
                            "ERR value is not an integer or out of range",
                        ),
                        close: false,
                        retry_blocking: None,
                        delay_ms: None,
                        config_dirty: false,
                        acl_dirty: false,
                    });
                };
                let Some(end) = parse_i64(end_raw) else {
                    return Some(CommandOutcome {
                        response: RespFrame::error_str(
                            "ERR value is not an integer or out of range",
                        ),
                        close: false,
                        retry_blocking: None,
                        delay_ms: None,
                        config_dirty: false,
                        acl_dirty: false,
                    });
                };

                let now_ms = ratatosk_core::time::now_ms();
                let mut db = server_state.data.write_db(client_state.selected_db());
                purge_expired_key_in_shard(
                    &mut db,
                    server_state,
                    client_state.selected_db(),
                    key,
                    now_ms,
                );

                match db.data.get(key.as_ref()) {
                    None => RespFrame::Array(vec![]),
                    Some(entry) => match entry.as_list() {
                        None => RespFrame::wrongtype(),
                        Some(list) => {
                            let Some((range_start, range_end)) =
                                normalize_range(list.len(), start, end)
                            else {
                                return Some(CommandOutcome {
                                    response: RespFrame::Array(vec![]),
                                    close: false,
                                    retry_blocking: None,
                                    delay_ms: None,
                                    config_dirty: false,
                                    acl_dirty: false,
                                });
                            };

                            let count = range_end.saturating_sub(range_start).saturating_add(1);
                            RespFrame::Array(
                                list.iter()
                                    .skip(range_start)
                                    .take(count)
                                    .cloned()
                                    .map(|value| RespFrame::BulkString(Some(value)))
                                    .collect::<Vec<_>>(),
                            )
                        }
                    },
                }
            }
            _ => wrong_arity_response("lrange"),
        }
    } else if command.eq_ignore_ascii_case(b"TTL")
        || command.eq_ignore_ascii_case(b"PTTL")
        || command.eq_ignore_ascii_case(b"EXPIRETIME")
        || command.eq_ignore_ascii_case(b"PEXPIRETIME")
    {
        let command_name = if command.eq_ignore_ascii_case(b"TTL") {
            "ttl"
        } else if command.eq_ignore_ascii_case(b"PTTL") {
            "pttl"
        } else if command.eq_ignore_ascii_case(b"EXPIRETIME") {
            "expiretime"
        } else {
            "pexpiretime"
        };
        match args {
            [key] => {
                let now_ms = ratatosk_core::time::now_ms();
                let mut db = server_state.data.write_db(client_state.selected_db());
                purge_expired_key_in_shard(
                    &mut db,
                    server_state,
                    client_state.selected_db(),
                    key,
                    now_ms,
                );

                if let Some(entry) = db.data.get(key.as_ref()) {
                    if let Some(expire_at_ms) = entry.expire_at_ms() {
                        if command.eq_ignore_ascii_case(b"TTL")
                            || command.eq_ignore_ascii_case(b"PTTL")
                        {
                            let remaining_ms = expire_at_ms.saturating_sub(now_ms);
                            if remaining_ms <= 0 {
                                tracked_remove_from_shard(
                                    &mut db,
                                    server_state,
                                    client_state.selected_db(),
                                    key.as_ref(),
                                );
                                RespFrame::Integer(-2)
                            } else if command.eq_ignore_ascii_case(b"TTL") {
                                RespFrame::Integer(remaining_ms / 1000)
                            } else {
                                RespFrame::Integer(remaining_ms)
                            }
                        } else if command.eq_ignore_ascii_case(b"EXPIRETIME") {
                            RespFrame::Integer(expire_at_ms / 1000)
                        } else {
                            RespFrame::Integer(expire_at_ms)
                        }
                    } else {
                        RespFrame::Integer(-1)
                    }
                } else {
                    RespFrame::Integer(-2)
                }
            }
            _ => wrong_arity_response(command_name),
        }
    } else {
        match args {
            [] => wrong_arity_response("exists"),
            _ => {
                let now_ms = ratatosk_core::time::now_ms();
                let mut db = server_state.data.write_db(client_state.selected_db());
                let mut count = 0i64;
                let total_keys = args.len() as u64;
                for key in args {
                    purge_expired_key_in_shard(
                        &mut db,
                        server_state,
                        client_state.selected_db(),
                        key,
                        now_ms,
                    );
                    if db.data.contains_key(key.as_ref()) {
                        count += 1;
                    }
                }

                let hits = count as u64;
                let misses = total_keys.saturating_sub(hits);
                server_state.stats.add_keyspace_hits(hits);
                server_state.stats.add_keyspace_misses(misses);
                RespFrame::Integer(count)
            }
        }
    };

    client_state.mark_command_metadata(
        command,
        !command.eq_ignore_ascii_case(b"PING") && !command.eq_ignore_ascii_case(b"ECHO"),
    );

    server_state.stats.mark_command_processed();

    Some(CommandOutcome {
        response,
        close: false,
        retry_blocking: None,
        delay_ms: None,
        config_dirty: false,
        acl_dirty: false,
    })
}

fn can_execute_lock_free_fast_command(
    argv: &[Bytes],
    server_state: &SharedServerState,
    client_state: &ClientState,
) -> bool {
    if client_state.in_multi() {
        return false;
    }

    let [command, args @ ..] = argv else {
        return false;
    };

    if !is_lock_free_fast_command_name(command) {
        return false;
    }

    if command.eq_ignore_ascii_case(b"PING")
        && matches!(args, [message] if message.eq_ignore_ascii_case(b"HEALTH"))
    {
        return false;
    }

    let default_acl_policy = server_state.default_acl_policy();
    let has_default_access = default_acl_policy.default_user_has_full_access();
    if !client_state.is_authenticated() {
        return default_acl_policy.default_user_is_nopass_enabled() && has_default_access;
    }

    client_state.acl_user().as_ref() == b"default" && has_default_access
}

pub(super) async fn try_run_readonly_batch(
    frames: Vec<RespFrame>,
    server_state: &SharedServerState,
    client_state: &mut ClientState,
) -> Result<Vec<CommandOutcome>, Vec<RespFrame>> {
    if !readonly_batch_enabled() || frames.len() < 2 {
        return Err(frames);
    }

    let mut argvs = Vec::with_capacity(frames.len());
    let mut command_names = Vec::with_capacity(frames.len());
    for frame in &frames {
        let Some(argv) = frame_to_argv_for_persistence(frame) else {
            return Err(frames);
        };
        let Some(command) = argv.first() else {
            return Err(frames);
        };
        if is_write_command(&argv) || !is_readonly_batch_command(command) {
            return Err(frames);
        }
        command_names.push(String::from_utf8_lossy(command).to_ascii_uppercase());
        argvs.push(argv);
    }

    if argvs
        .iter()
        .all(|argv| can_execute_lock_free_fast_command(argv, server_state, client_state))
    {
        let mut outcomes = Vec::with_capacity(command_names.len());
        let mut elapsed_us_by_command = Vec::with_capacity(command_names.len());
        for (argv, command_name) in argvs.iter().zip(command_names.iter()) {
            breadcrumbs::record_command(
                client_state.id(),
                command_name,
                client_state.selected_db(),
                0,
                "batch_execute",
            );

            let start = std::time::Instant::now();
            let outcome = try_execute_lock_free_fast_command(argv, server_state, client_state)
                .expect("lock-free batch eligibility checked ahead of execution");
            let duration = start.elapsed();
            let success = !matches!(outcome.response, ratatosk_resp::RespFrame::Error(_));
            metrics::record_command(command_name, success, duration.as_secs_f64());

            if duration.as_millis() > 1 {
                tracing::debug!(
                    target = "ratatosk::slow_command",
                    command = %command_name,
                    duration_ms = duration.as_micros() as f64 / 1000.0,
                    "slow command detected in readonly lock-free batch"
                );
            }
            elapsed_us_by_command.push(i64::try_from(duration.as_micros()).unwrap_or(i64::MAX));
            outcomes.push(outcome);
        }

        let lock_wait_start = std::time::Instant::now();
        let mut server = server_state.meta.lock().await;
        metrics::record_server_state_lock_wait_ms(
            "batch_execute_lock_free_post",
            lock_wait_start.elapsed().as_secs_f64() * 1000.0,
        );

        let lock_hold_start = std::time::Instant::now();
        server.stats.catch_up_from_atomic(&server_state.stats);
        for ((argv, outcome), elapsed_us) in argvs
            .iter()
            .zip(outcomes.iter())
            .zip(elapsed_us_by_command.into_iter())
        {
            let (track_slowlog, track_latency) = post_execute_tracking_flags(&server, argv);
            apply_post_execute_side_effects(
                &mut server,
                client_state,
                argv,
                &outcome.response,
                Some(elapsed_us),
                track_slowlog,
                track_latency,
            );
        }
        metrics::record_server_state_lock_hold_ms(
            "batch_execute_lock_free_post",
            lock_hold_start.elapsed().as_secs_f64() * 1000.0,
        );

        return Ok(outcomes);
    }

    let mut outcomes = Vec::with_capacity(command_names.len());
    let mut total_lock_wait = Duration::ZERO;
    let mut total_lock_hold = Duration::ZERO;
    for (argv, command_name) in argvs.into_iter().zip(command_names.iter()) {
        breadcrumbs::record_command(
            client_state.id(),
            command_name,
            client_state.selected_db(),
            0,
            "batch_execute",
        );

        let start = std::time::Instant::now();
        let precheck = precheck_execute_argv_with_default_acl(
            &argv,
            server_state.default_acl_policy(),
            client_state,
        );
        let outcome = match precheck {
            ExecuteArgvPrecheck::Reject(outcome) => outcome,
            ExecuteArgvPrecheck::Continue => {
                let lock_wait_start = std::time::Instant::now();
                {
                    let mut server = server_state.meta.lock().await;
                    total_lock_wait = total_lock_wait.saturating_add(lock_wait_start.elapsed());
                    let lock_hold_start = std::time::Instant::now();
                    let mut access = ServerAccess::new_with_runtime_caches(
                        &mut server,
                        &server_state.stats,
                        Some(server_state.default_acl_policy()),
                    );
                    let outcome = execute_argv(&argv, &mut access, client_state);
                    total_lock_hold = total_lock_hold.saturating_add(lock_hold_start.elapsed());
                    outcome
                }
            }
        };
        let duration = start.elapsed();
        let success = !matches!(outcome.response, ratatosk_resp::RespFrame::Error(_));
        metrics::record_command(command_name, success, duration.as_secs_f64());

        if duration.as_millis() > 1 {
            tracing::debug!(
                target = "ratatosk::slow_command",
                command = %command_name,
                duration_ms = duration.as_micros() as f64 / 1000.0,
                "slow command detected in readonly batch"
            );
        }
        outcomes.push(outcome);
    }
    metrics::record_server_state_lock_wait_ms(
        "batch_execute_readonly",
        total_lock_wait.as_secs_f64() * 1000.0,
    );
    metrics::record_server_state_lock_hold_ms(
        "batch_execute_readonly",
        total_lock_hold.as_secs_f64() * 1000.0,
    );

    Ok(outcomes)
}
