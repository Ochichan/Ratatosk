use std::{
    io,
    sync::{Arc, OnceLock},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use bytes::{Bytes, BytesMut};
use ratatosk_engine::{
    command::{
        ClientState, CommandOutcome, ExecuteArgvPrecheck, ServerAccess,
        apply_post_execute_side_effects, execute, execute_argv, is_write_command,
        post_execute_tracking_flags, precheck_execute_argv_with_default_acl,
        supports_readonly_batch_command,
    },
    keyspace::{LexBound, PubSubMessage, ScoreBound, SharedState, SortedSet},
    object::{format_f64_for_redis, normalize_range, parse_i64},
};
use ratatosk_resp::{RespFrame, encode, encode_to_vec, encoded_len, parse};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
    sync::Notify,
    time::timeout,
};

use crate::breadcrumbs;
use crate::config::DEFAULT_OUTPUT_BUFFER_LIMIT_BYTES;
use crate::metrics;
use crate::persistence::{
    PersistenceRuntime, append_aof_command, run_save, start_bgrewriteaof, start_bgsave,
};

// Fallback defaults — runtime values are read from ConfigState at connection start.
#[allow(dead_code)]
const QUERY_BUFFER_LIMIT: usize = 1024 * 1024;
#[allow(dead_code)]
const OUTPUT_BUFFER_FLUSH_THRESHOLD: usize = 16 * 1024;
#[allow(dead_code)]
const WRITE_TIMEOUT: Duration = Duration::from_secs(5);
// Maximum wait between blocking retries.  With FIFO wake-one semantics only
// the front-of-queue waiter is notified, so exponential backoff is no longer
// needed — this constant caps the wait that guards against lost notifications.
const BLOCKING_RETRY_POLL_CAP: Duration = Duration::from_millis(500);
const AOF_APPEND_SLOW_THRESHOLD: Duration = Duration::from_secs(3);
const OUTPUT_BUFFER_LIMIT_ERR: &str = "ERR output buffer limit exceeded";
const AOF_WRITE_LATCH_ERR_PREFIX: &str =
    "MISCONF writes are blocked because AOF persistence is in an error state";
const READONLY_BATCH_ENV: &str = "RATATOSK_PIPELINE_READONLY_BATCH_LOCK";
static READONLY_BATCH_ENABLED: OnceLock<bool> = OnceLock::new();

pub type SharedServerState = Arc<SharedState>;

/// Check if an error represents an expected client disconnect (not a server error).
fn is_benign_disconnect(error: &io::Error) -> bool {
    matches!(
        error.kind(),
        io::ErrorKind::BrokenPipe
            | io::ErrorKind::ConnectionReset
            | io::ErrorKind::ConnectionAborted
            | io::ErrorKind::TimedOut
            | io::ErrorKind::UnexpectedEof
    )
}

fn socket_addr_bytes(stream: &TcpStream) -> (Bytes, Bytes) {
    let addr = stream
        .peer_addr()
        .map(|addr| Bytes::from(addr.to_string()))
        .unwrap_or_else(|_| Bytes::from_static(b"127.0.0.1:0"));
    let laddr = stream
        .local_addr()
        .map(|addr| Bytes::from(addr.to_string()))
        .unwrap_or_else(|_| Bytes::from_static(b"127.0.0.1:0"));
    (addr, laddr)
}

async fn refresh_client_snapshot(
    server_state: &SharedServerState,
    client_state: &ClientState,
    addr: &Bytes,
    laddr: &Bytes,
    blocked: bool,
) {
    let mut server = server_state.meta.lock().await;
    server.stats.catch_up_from_atomic(&server_state.stats);
    server.upsert_client_snapshot(client_state.snapshot_with_redirect(
        addr.clone(),
        laddr.clone(),
        client_state.tracking_redirect(),
    ));
    server.set_client_blocked(client_state.id(), blocked);
}

async fn flush_pending_input_bytes(
    server_state: &SharedServerState,
    pending_input_bytes: &mut u64,
) {
    if *pending_input_bytes == 0 {
        return;
    }

    let bytes = std::mem::take(pending_input_bytes);
    let mut server = server_state.meta.lock().await;
    server.stats.add_net_input_bytes(bytes);
}

#[derive(Debug, Clone, Copy)]
pub struct ClientIoLimits {
    pub output_buffer_limit_bytes: usize,
    pub client_read_timeout_sec: u64,
}

impl Default for ClientIoLimits {
    fn default() -> Self {
        Self {
            output_buffer_limit_bytes: DEFAULT_OUTPUT_BUFFER_LIMIT_BYTES,
            client_read_timeout_sec: 0,
        }
    }
}

fn append_encoded_frame(
    output: &mut Vec<u8>,
    frame: &RespFrame,
    output_limit_bytes: usize,
) -> bool {
    let frame_len = encoded_len(frame);
    if output.len().saturating_add(frame_len) > output_limit_bytes {
        return false;
    }

    output.reserve(frame_len);
    encode_to_vec(frame, output);
    true
}

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
    if db
        .data
        .get(key.as_ref())
        .is_some_and(|value| value.expire_at_ms.is_some_and(|ts| ts <= now_ms))
    {
        db.data.remove(key.as_ref());
    }

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

fn try_execute_lock_free_fast_command(
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
                let mut db = server_state.data.write_db(client_state.selected_db());
                db.data
                    .retain(|_, value| value.expire_at_ms.is_none_or(|ts| ts > now_ms));
                RespFrame::Integer(db.data.len() as i64)
            }
            _ => wrong_arity_response("dbsize"),
        }
    } else if command.eq_ignore_ascii_case(b"TYPE") {
        match args {
            [key] => {
                let now_ms = ratatosk_core::time::now_ms();
                let mut db = server_state.data.write_db(client_state.selected_db());
                if db
                    .data
                    .get(key.as_ref())
                    .is_some_and(|value| value.expire_at_ms.is_some_and(|ts| ts <= now_ms))
                {
                    db.data.remove(key.as_ref());
                }
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
                if db
                    .data
                    .get(key.as_ref())
                    .is_some_and(|value| value.expire_at_ms.is_some_and(|ts| ts <= now_ms))
                {
                    db.data.remove(key.as_ref());
                }

                match db.data.get(key.as_ref()) {
                    None => {
                        server_state.stats.mark_keyspace_miss();
                        RespFrame::BulkString(None)
                    }
                    Some(entry) if !entry.is_string() => RespFrame::wrongtype(),
                    Some(entry) => {
                        server_state.stats.mark_keyspace_hit();
                        RespFrame::BulkString(entry.as_string().cloned())
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
                    if db
                        .data
                        .get(key.as_ref())
                        .is_some_and(|value| value.expire_at_ms.is_some_and(|ts| ts <= now_ms))
                    {
                        db.data.remove(key.as_ref());
                    }
                    let value = db
                        .data
                        .get(key.as_ref())
                        .and_then(|entry| entry.as_string().cloned());
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
                if db
                    .data
                    .get(key.as_ref())
                    .is_some_and(|value| value.expire_at_ms.is_some_and(|ts| ts <= now_ms))
                {
                    db.data.remove(key.as_ref());
                }
                match db.data.get(key.as_ref()) {
                    None => RespFrame::Integer(0),
                    Some(entry) if !entry.is_string() => RespFrame::wrongtype(),
                    Some(entry) => RespFrame::Integer(
                        i64::try_from(entry.as_string().map_or(0, Bytes::len)).unwrap_or(i64::MAX),
                    ),
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
            if db
                .data
                .get(key.as_ref())
                .is_some_and(|value| value.expire_at_ms.is_some_and(|ts| ts <= now_ms))
            {
                db.data.remove(key.as_ref());
            }

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
                if db
                    .data
                    .get(key.as_ref())
                    .is_some_and(|value| value.expire_at_ms.is_some_and(|ts| ts <= now_ms))
                {
                    db.data.remove(key.as_ref());
                }

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
                if db
                    .data
                    .get(key.as_ref())
                    .is_some_and(|value| value.expire_at_ms.is_some_and(|ts| ts <= now_ms))
                {
                    db.data.remove(key.as_ref());
                }

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
                if db
                    .data
                    .get(key.as_ref())
                    .is_some_and(|value| value.expire_at_ms.is_some_and(|ts| ts <= now_ms))
                {
                    db.data.remove(key.as_ref());
                }

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
                if db
                    .data
                    .get(key.as_ref())
                    .is_some_and(|value| value.expire_at_ms.is_some_and(|ts| ts <= now_ms))
                {
                    db.data.remove(key.as_ref());
                }

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
                if db
                    .data
                    .get(key.as_ref())
                    .is_some_and(|value| value.expire_at_ms.is_some_and(|ts| ts <= now_ms))
                {
                    db.data.remove(key.as_ref());
                }

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
                if db
                    .data
                    .get(key.as_ref())
                    .is_some_and(|value| value.expire_at_ms.is_some_and(|ts| ts <= now_ms))
                {
                    db.data.remove(key.as_ref());
                }

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
                if db
                    .data
                    .get(key.as_ref())
                    .is_some_and(|value| value.expire_at_ms.is_some_and(|ts| ts <= now_ms))
                {
                    db.data.remove(key.as_ref());
                }

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
                if db
                    .data
                    .get(key.as_ref())
                    .is_some_and(|value| value.expire_at_ms.is_some_and(|ts| ts <= now_ms))
                {
                    db.data.remove(key.as_ref());
                }

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
                if db
                    .data
                    .get(key.as_ref())
                    .is_some_and(|value| value.expire_at_ms.is_some_and(|ts| ts <= now_ms))
                {
                    db.data.remove(key.as_ref());
                }

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
                if db
                    .data
                    .get(key.as_ref())
                    .is_some_and(|value| value.expire_at_ms.is_some_and(|ts| ts <= now_ms))
                {
                    db.data.remove(key.as_ref());
                }

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
                if db
                    .data
                    .get(key.as_ref())
                    .is_some_and(|value| value.expire_at_ms.is_some_and(|ts| ts <= now_ms))
                {
                    db.data.remove(key.as_ref());
                }

                match db.data.get(key.as_ref()) {
                    None => RespFrame::Integer(0),
                    Some(entry) => match entry.as_set() {
                        None => RespFrame::wrongtype(),
                        Some(set) => RespFrame::Integer(if set.contains(member) { 1 } else { 0 }),
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
                if db
                    .data
                    .get(key.as_ref())
                    .is_some_and(|value| value.expire_at_ms.is_some_and(|ts| ts <= now_ms))
                {
                    db.data.remove(key.as_ref());
                }

                match db.data.get(key.as_ref()) {
                    None => RespFrame::Array(
                        members
                            .iter()
                            .map(|_| RespFrame::Integer(0))
                            .collect::<Vec<_>>(),
                    ),
                    Some(entry) => match entry.as_set() {
                        None => RespFrame::wrongtype(),
                        Some(set) => RespFrame::Array(
                            members
                                .iter()
                                .map(|member| {
                                    RespFrame::Integer(if set.contains(member) { 1 } else { 0 })
                                })
                                .collect::<Vec<_>>(),
                        ),
                    },
                }
            }
        }
    } else if command.eq_ignore_ascii_case(b"SCARD") {
        match args {
            [key] => {
                let now_ms = ratatosk_core::time::now_ms();
                let mut db = server_state.data.write_db(client_state.selected_db());
                if db
                    .data
                    .get(key.as_ref())
                    .is_some_and(|value| value.expire_at_ms.is_some_and(|ts| ts <= now_ms))
                {
                    db.data.remove(key.as_ref());
                }

                match db.data.get(key.as_ref()) {
                    None => RespFrame::Integer(0),
                    Some(entry) => match entry.as_set() {
                        None => RespFrame::wrongtype(),
                        Some(set) => {
                            RespFrame::Integer(i64::try_from(set.len()).unwrap_or(i64::MAX))
                        }
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
                if db
                    .data
                    .get(key.as_ref())
                    .is_some_and(|value| value.expire_at_ms.is_some_and(|ts| ts <= now_ms))
                {
                    db.data.remove(key.as_ref());
                }

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
                if db
                    .data
                    .get(key.as_ref())
                    .is_some_and(|value| value.expire_at_ms.is_some_and(|ts| ts <= now_ms))
                {
                    db.data.remove(key.as_ref());
                }

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
                if db
                    .data
                    .get(key.as_ref())
                    .is_some_and(|value| value.expire_at_ms.is_some_and(|ts| ts <= now_ms))
                {
                    db.data.remove(key.as_ref());
                }

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
                if db
                    .data
                    .get(key.as_ref())
                    .is_some_and(|value| value.expire_at_ms.is_some_and(|ts| ts <= now_ms))
                {
                    db.data.remove(key.as_ref());
                }

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
                if db
                    .data
                    .get(key.as_ref())
                    .is_some_and(|value| value.expire_at_ms.is_some_and(|ts| ts <= now_ms))
                {
                    db.data.remove(key.as_ref());
                }

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
                if db
                    .data
                    .get(key.as_ref())
                    .is_some_and(|value| value.expire_at_ms.is_some_and(|ts| ts <= now_ms))
                {
                    db.data.remove(key.as_ref());
                }

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
                if db
                    .data
                    .get(key.as_ref())
                    .is_some_and(|value| value.expire_at_ms.is_some_and(|ts| ts <= now_ms))
                {
                    db.data.remove(key.as_ref());
                }

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
                if db
                    .data
                    .get(key.as_ref())
                    .is_some_and(|value| value.expire_at_ms.is_some_and(|ts| ts <= now_ms))
                {
                    db.data.remove(key.as_ref());
                }

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
                if db
                    .data
                    .get(key.as_ref())
                    .is_some_and(|value| value.expire_at_ms.is_some_and(|ts| ts <= now_ms))
                {
                    db.data.remove(key.as_ref());
                }

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
                if db
                    .data
                    .get(key.as_ref())
                    .is_some_and(|value| value.expire_at_ms.is_some_and(|ts| ts <= now_ms))
                {
                    db.data.remove(key.as_ref());
                }

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
                if db
                    .data
                    .get(key.as_ref())
                    .is_some_and(|value| value.expire_at_ms.is_some_and(|ts| ts <= now_ms))
                {
                    db.data.remove(key.as_ref());
                }

                if let Some(entry) = db.data.get(key.as_ref()) {
                    if let Some(expire_at_ms) = entry.expire_at_ms {
                        if command.eq_ignore_ascii_case(b"TTL")
                            || command.eq_ignore_ascii_case(b"PTTL")
                        {
                            let remaining_ms = expire_at_ms.saturating_sub(now_ms);
                            if remaining_ms <= 0 {
                                db.data.remove(key.as_ref());
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
                    if db
                        .data
                        .get(key.as_ref())
                        .is_some_and(|value| value.expire_at_ms.is_some_and(|ts| ts <= now_ms))
                    {
                        db.data.remove(key.as_ref());
                    }
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

async fn try_run_readonly_batch(
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

fn aof_write_latch_error(detail: &str) -> RespFrame {
    RespFrame::error_str(&format!(
        "{AOF_WRITE_LATCH_ERR_PREFIX}; last_error={detail}"
    ))
}

async fn set_aof_write_latch(server_state: &SharedServerState, error: String) {
    let mut server = server_state.meta.lock().await;
    server.set_aof_last_error(error.clone());
    drop(server);

    metrics::set_aof_write_latched(true);
    tracing::error!(
        target = "ratatosk::aof",
        error = %error,
        "AOF write latch engaged; write commands will be rejected"
    );
}

async fn clear_aof_write_latch_if_set(server_state: &SharedServerState) {
    let mut server = server_state.meta.lock().await;
    let was_latched = server.aof_write_latched();
    if was_latched {
        server.clear_aof_last_error();
    }
    drop(server);

    if was_latched {
        metrics::set_aof_write_latched(false);
        tracing::warn!(
            target = "ratatosk::aof",
            "AOF write latch cleared after successful append"
        );
    }
}

async fn wait_for_blocking_ready(
    stream: &TcpStream,
    wait_for: Duration,
    notifier: &Notify,
) -> io::Result<bool> {
    if wait_for.is_zero() {
        return Ok(false);
    }

    let sleep = tokio::time::sleep(wait_for);
    tokio::pin!(sleep);
    tokio::select! {
        _ = notifier.notified() => Ok(false),
        _ = &mut sleep => Ok(false),
        result = stream.readable() => match result {
            Ok(()) => {
                let mut probe = [0u8; 1];
                match stream.peek(&mut probe).await {
                    Ok(0) => Ok(true),
                    Ok(_) => Ok(false),
                    Err(error)
                        if matches!(
                            error.kind(),
                            io::ErrorKind::WouldBlock | io::ErrorKind::Interrupted
                        ) =>
                    {
                        Ok(false)
                    }
                    Err(error) => Err(error),
                }
            }
            Err(error)
                if matches!(
                    error.kind(),
                    io::ErrorKind::WouldBlock | io::ErrorKind::Interrupted
                ) =>
            {
                Ok(false)
            }
            Err(error) => Err(error),
        }
    }
}

/// Result of the select-based I/O wait: either a pubsub message arrived,
/// a monitor notification arrived, or network data was read.
enum WaitResult {
    PubSubMsg(PubSubMessage),
    PubSubClosed,
    MonitorWake,
    NetworkRead(usize),
}

async fn wait_for_async_push_or_input(
    stream: &mut TcpStream,
    input: &mut BytesMut,
    pubsub_rx: &mut tokio::sync::mpsc::Receiver<PubSubMessage>,
    monitor_notifier: &Notify,
) -> io::Result<WaitResult> {
    tokio::select! {
        msg = pubsub_rx.recv() => match msg {
            Some(m) => Ok(WaitResult::PubSubMsg(m)),
            None => Ok(WaitResult::PubSubClosed),
        },
        _ = monitor_notifier.notified() => Ok(WaitResult::MonitorWake),
        result = stream.read_buf(input) => result.map(WaitResult::NetworkRead),
    }
}

async fn write_all_with_timeout(
    stream: &mut TcpStream,
    payload: &[u8],
    write_timeout: Duration,
) -> io::Result<()> {
    if payload.is_empty() {
        return Ok(());
    }

    match timeout(write_timeout, stream.write_all(payload)).await {
        Ok(result) => result,
        Err(_) => Err(io::Error::new(
            io::ErrorKind::TimedOut,
            "socket write timeout",
        )),
    }
}

fn load_connection_runtime_config(server_state: &SharedServerState) -> (usize, usize, Duration) {
    let config = server_state.config_cache.load();
    (
        config.query_buffer_limit(),
        config.output_buffer_flush_threshold(),
        Duration::from_secs(config.client_write_timeout_sec()),
    )
}

fn reload_connection_runtime_config(
    server_state: &SharedServerState,
    query_buffer_limit: &mut usize,
    output_buffer_flush_threshold: &mut usize,
    write_timeout: &mut Duration,
) {
    let (query_limit, output_threshold, timeout) = load_connection_runtime_config(server_state);
    *query_buffer_limit = query_limit;
    *output_buffer_flush_threshold = output_threshold;
    *write_timeout = timeout;
}

#[allow(clippy::too_many_arguments)]
async fn run_with_blocking_retry(
    frame: RespFrame,
    server_state: &SharedServerState,
    persistence: &Arc<PersistenceRuntime>,
    client_state: &mut ClientState,
    stream: &TcpStream,
    addr: &Bytes,
    laddr: &Bytes,
) -> io::Result<CommandOutcome> {
    let start = std::time::Instant::now();
    let first_argv = frame_to_argv_for_persistence(&frame);
    let first_db = client_state.selected_db();

    // Extract command name for metrics
    let command_name = first_argv
        .as_ref()
        .and_then(|argv| {
            argv.first()
                .map(|cmd| String::from_utf8_lossy(cmd).to_ascii_uppercase())
        })
        .unwrap_or_else(|| "UNKNOWN".to_string());

    let is_write_operation = first_argv.as_deref().is_some_and(is_write_command);

    breadcrumbs::record_command(client_state.id(), &command_name, first_db, 0, "execute");

    let mut outcome = if let Some(argv) = first_argv.as_deref() {
        let fast_started = std::time::Instant::now();
        if let Some(outcome) = try_execute_lock_free_fast_command(argv, server_state, client_state)
        {
            let lock_wait_start = std::time::Instant::now();
            let mut server = server_state.meta.lock().await;
            metrics::record_server_state_lock_wait_ms(
                "execute_lock_free_post",
                lock_wait_start.elapsed().as_secs_f64() * 1000.0,
            );

            let lock_hold_start = std::time::Instant::now();
            server.stats.catch_up_from_atomic(&server_state.stats);
            let (track_slowlog, track_latency) = post_execute_tracking_flags(&server, argv);
            let elapsed_us = i64::try_from(fast_started.elapsed().as_micros()).unwrap_or(i64::MAX);
            apply_post_execute_side_effects(
                &mut server,
                client_state,
                argv,
                &outcome.response,
                Some(elapsed_us),
                track_slowlog,
                track_latency,
            );
            metrics::record_server_state_lock_hold_ms(
                "execute_lock_free_post",
                lock_hold_start.elapsed().as_secs_f64() * 1000.0,
            );
            outcome
        } else {
            let precheck = precheck_execute_argv_with_default_acl(
                argv,
                server_state.default_acl_policy(),
                client_state,
            );
            if let ExecuteArgvPrecheck::Reject(outcome) = precheck {
                outcome
            } else {
                let lock_wait_start = std::time::Instant::now();
                let mut server = server_state.meta.lock().await;
                metrics::record_server_state_lock_wait_ms(
                    "execute",
                    lock_wait_start.elapsed().as_secs_f64() * 1000.0,
                );

                let lock_hold_start = std::time::Instant::now();
                let aof_latched_error = if is_write_operation && server.aof_enabled() {
                    server.aof_last_error().map(str::to_owned)
                } else {
                    None
                };

                let outcome = if let Some(aof_error) = aof_latched_error {
                    metrics::record_aof_write_rejected("latched");
                    CommandOutcome {
                        response: aof_write_latch_error(&aof_error),
                        close: false,
                        retry_blocking: None,
                        delay_ms: None,
                        config_dirty: false,
                        acl_dirty: false,
                    }
                } else {
                    let mut access = ServerAccess::new_with_runtime_caches(
                        &mut server,
                        &server_state.stats,
                        Some(server_state.default_acl_policy()),
                    );
                    execute_argv(argv, &mut access, client_state)
                };
                if outcome.config_dirty {
                    server_state.update_config_cache(&server.config);
                }
                if outcome.acl_dirty {
                    server_state.update_acl_policy_cache(&server.acl);
                }
                metrics::record_server_state_lock_hold_ms(
                    "execute",
                    lock_hold_start.elapsed().as_secs_f64() * 1000.0,
                );
                outcome
            }
        }
    } else {
        let lock_wait_start = std::time::Instant::now();
        let mut server = server_state.meta.lock().await;
        metrics::record_server_state_lock_wait_ms(
            "execute",
            lock_wait_start.elapsed().as_secs_f64() * 1000.0,
        );

        let lock_hold_start = std::time::Instant::now();
        let aof_latched_error = if is_write_operation && server.aof_enabled() {
            server.aof_last_error().map(str::to_owned)
        } else {
            None
        };

        let outcome = if let Some(aof_error) = aof_latched_error {
            metrics::record_aof_write_rejected("latched");
            CommandOutcome {
                response: aof_write_latch_error(&aof_error),
                close: false,
                retry_blocking: None,
                delay_ms: None,
                config_dirty: false,
                acl_dirty: false,
            }
        } else {
            let mut access = ServerAccess::new_with_runtime_caches(
                &mut server,
                &server_state.stats,
                Some(server_state.default_acl_policy()),
            );
            execute(frame, &mut access, client_state)
        };
        if outcome.config_dirty {
            server_state.update_config_cache(&server.config);
        }
        if outcome.acl_dirty {
            server_state.update_acl_policy_cache(&server.acl);
        }
        metrics::record_server_state_lock_hold_ms(
            "execute",
            lock_hold_start.elapsed().as_secs_f64() * 1000.0,
        );
        outcome
    };

    // Record command metrics
    let duration = start.elapsed();
    let success = !matches!(outcome.response, ratatosk_resp::RespFrame::Error(_));
    metrics::record_command(&command_name, success, duration.as_secs_f64());

    // Log slow commands (> 1ms)
    if duration.as_millis() > 1 {
        tracing::debug!(
            target = "ratatosk::slow_command",
            command = %command_name,
            duration_ms = duration.as_micros() as f64 / 1000.0,
            "slow command detected"
        );
    }

    let Some(retry) = outcome.retry_blocking.clone() else {
        apply_post_execute_persistence(
            server_state,
            persistence,
            first_db,
            first_argv,
            &mut outcome,
        )
        .await;
        refresh_client_snapshot(server_state, client_state, addr, laddr, false).await;
        return Ok(outcome);
    };

    refresh_client_snapshot(server_state, client_state, addr, laddr, true).await;
    let mut notifier = {
        let mut server = server_state.meta.lock().await;
        server.register_blocked_client(client_state.id(), retry.watch_keys.clone())
    };

    let deadline_ms = retry.deadline_ms;
    let mut frame = retry.frame;
    let mut last_response = outcome.response;
    let mut retry_attempts = 0u64;

    loop {
        let now_ms = ratatosk_core::time::monotonic_ms();
        if let Some(deadline) = deadline_ms {
            let deadline_u64 = u64::try_from(deadline).unwrap_or(u64::MAX);
            if now_ms >= deadline_u64 {
                metrics::record_blocking_retry_deadline_exhausted(&command_name);
                metrics::record_blocking_retry_completed(&command_name, retry_attempts);
                {
                    let mut server = server_state.meta.lock().await;
                    server.clear_blocked_client(client_state.id());
                }
                refresh_client_snapshot(server_state, client_state, addr, laddr, false).await;
                return Ok(CommandOutcome {
                    response: last_response,
                    close: false,
                    retry_blocking: None,
                    delay_ms: None,
                    config_dirty: false,
                    acl_dirty: false,
                });
            }
        }

        let wait_for = if let Some(deadline) = deadline_ms {
            let deadline_u64 = u64::try_from(deadline).unwrap_or(u64::MAX);
            let remaining_ms = deadline_u64.saturating_sub(now_ms);
            Duration::from_millis(remaining_ms).min(BLOCKING_RETRY_POLL_CAP)
        } else {
            BLOCKING_RETRY_POLL_CAP
        };

        retry_attempts = retry_attempts.saturating_add(1);
        breadcrumbs::record_command(
            client_state.id(),
            &command_name,
            client_state.selected_db(),
            retry_attempts,
            "retry",
        );
        metrics::record_blocking_retry_iteration(&command_name);
        metrics::record_blocking_retry_wait_ms(&command_name, wait_for.as_millis() as f64);

        if wait_for_blocking_ready(stream, wait_for, notifier.as_ref())
            .await
            .map_err(|error| {
                io::Error::new(
                    error.kind(),
                    format!(
                        "waiting for blocking command retry readiness (command={}, attempt={}): {}",
                        command_name, retry_attempts, error
                    ),
                )
            })?
        {
            return Err(io::Error::new(
                io::ErrorKind::ConnectionReset,
                "client disconnected while waiting for blocking command",
            ));
        }

        let outcome = {
            let argv = frame_to_argv_for_persistence(&frame);
            let db = client_state.selected_db();
            let is_retry_write = argv.as_deref().is_some_and(is_write_command);

            let lock_wait_start = std::time::Instant::now();
            let mut server = server_state.meta.lock().await;
            metrics::record_server_state_lock_wait_ms(
                "retry_execute",
                lock_wait_start.elapsed().as_secs_f64() * 1000.0,
            );

            let lock_hold_start = std::time::Instant::now();
            let aof_latched_error = if is_retry_write && server.aof_enabled() {
                server.aof_last_error().map(str::to_owned)
            } else {
                None
            };
            let mut outcome = if let Some(aof_error) = aof_latched_error {
                metrics::record_aof_write_rejected("latched_retry");
                CommandOutcome {
                    response: aof_write_latch_error(&aof_error),
                    close: false,
                    retry_blocking: None,
                    delay_ms: None,
                    config_dirty: false,
                    acl_dirty: false,
                }
            } else {
                let mut access = ServerAccess::new_with_runtime_caches(
                    &mut server,
                    &server_state.stats,
                    Some(server_state.default_acl_policy()),
                );
                if let Some(argv) = argv.as_deref() {
                    execute_argv(argv, &mut access, client_state)
                } else {
                    execute(frame, &mut access, client_state)
                }
            };
            if outcome.config_dirty {
                server_state.update_config_cache(&server.config);
            }
            if outcome.acl_dirty {
                server_state.update_acl_policy_cache(&server.acl);
            }
            metrics::record_server_state_lock_hold_ms(
                "retry_execute",
                lock_hold_start.elapsed().as_secs_f64() * 1000.0,
            );

            if outcome.retry_blocking.is_none() {
                drop(server);
                apply_post_execute_persistence(server_state, persistence, db, argv, &mut outcome)
                    .await;
            }
            outcome
        };

        let Some(retry) = outcome.retry_blocking else {
            metrics::record_blocking_retry_completed(&command_name, retry_attempts);
            {
                let mut server = server_state.meta.lock().await;
                server.clear_blocked_client(client_state.id());
            }
            refresh_client_snapshot(server_state, client_state, addr, laddr, false).await;
            return Ok(outcome);
        };
        last_response = outcome.response;
        frame = retry.frame;
        notifier = {
            let mut server = server_state.meta.lock().await;
            server.register_blocked_client(client_state.id(), retry.watch_keys.clone())
        };
    }
}

fn frame_to_argv_for_persistence(frame: &RespFrame) -> Option<Vec<Bytes>> {
    let RespFrame::Array(items) = frame else {
        return None;
    };

    let mut out = Vec::with_capacity(items.len());
    for item in items {
        match item {
            RespFrame::BulkString(Some(value)) => out.push(value.clone()),
            RespFrame::SimpleString(value) => out.push(value.clone()),
            RespFrame::Integer(value) => out.push(Bytes::from(value.to_string())),
            RespFrame::BulkString(None) => return None,
            _ => return None,
        }
    }

    Some(out)
}

fn is_queued_response(frame: &RespFrame) -> bool {
    match frame {
        RespFrame::SimpleString(text) => text.eq_ignore_ascii_case(b"QUEUED"),
        _ => false,
    }
}

async fn apply_post_execute_persistence(
    server_state: &SharedServerState,
    persistence: &Arc<PersistenceRuntime>,
    selected_db: usize,
    argv: Option<Vec<Bytes>>,
    outcome: &mut CommandOutcome,
) {
    let Some(argv) = argv else {
        return;
    };
    if argv.is_empty() || matches!(outcome.response, RespFrame::Error(_)) {
        return;
    }

    let command = argv[0].to_ascii_uppercase();

    if command == b"SAVE" && !matches!(outcome.response, RespFrame::Error(_)) {
        if let Err(error) = run_save(server_state, &persistence.rdb_path).await {
            outcome.response = RespFrame::error_str(&format!("ERR SAVE failed: {error}"));
        }
        return;
    }

    if command == b"BGSAVE" && !matches!(outcome.response, RespFrame::Error(_)) {
        if !start_bgsave(Arc::clone(server_state), persistence.rdb_path.clone()).await {
            outcome.response = RespFrame::error_str("ERR Background save already in progress");
        }
        return;
    }

    if command == b"BGREWRITEAOF" && !matches!(outcome.response, RespFrame::Error(_)) {
        if !start_bgrewriteaof(Arc::clone(server_state), Arc::clone(persistence)).await {
            outcome.response =
                RespFrame::error_str("ERR BGREWRITEAOF failed: appendonly is disabled");
        }
        return;
    }

    if is_queued_response(&outcome.response) || !is_write_command(&argv) {
        return;
    }

    // Lock-free config read via ArcSwap.
    let fsync_policy = {
        let config = server_state.config_cache.load();
        String::from_utf8_lossy(config.appendfsync()).to_string()
    };
    let command_name = String::from_utf8_lossy(&argv[0]).to_string();

    let append_start = std::time::Instant::now();
    match append_aof_command(persistence, selected_db, argv).await {
        Ok(()) => {
            let elapsed_ms = append_start.elapsed().as_secs_f64() * 1000.0;
            if append_start.elapsed() > AOF_APPEND_SLOW_THRESHOLD {
                metrics::record_aof_append_timeout("append_slow");
                tracing::warn!(
                    target = "ratatosk::aof",
                    selected_db = selected_db,
                    command = %command_name,
                    elapsed_ms = elapsed_ms,
                    threshold_ms = AOF_APPEND_SLOW_THRESHOLD.as_millis(),
                    "AOF append exceeded slow threshold"
                );
            }
            metrics::record_aof_write(&fsync_policy);
            metrics::record_aof_append_duration_ms(elapsed_ms, "ok");
            clear_aof_write_latch_if_set(server_state).await;
        }
        Err(error) => {
            let elapsed_ms = append_start.elapsed().as_secs_f64() * 1000.0;
            metrics::record_aof_append_duration_ms(elapsed_ms, "error");
            metrics::record_aof_write_error();
            if error.kind() == io::ErrorKind::TimedOut {
                metrics::record_aof_append_timeout("worker");
            }

            let latch_error = format!("AOF append failed for command {}: {}", command_name, error);
            set_aof_write_latch(server_state, latch_error.clone()).await;
            outcome.response = aof_write_latch_error(&latch_error);
            tracing::error!(
                target = "ratatosk::aof",
                selected_db = selected_db,
                command = %command_name,
                elapsed_ms = elapsed_ms,
                error = %error,
                "AOF append failed"
            );
        }
    }
}

/// Encode a single PubSubMessage into the output buffer.
/// Returns `false` if the output buffer would exceed the limit.
fn encode_pubsub_message(
    message: PubSubMessage,
    output: &mut Vec<u8>,
    output_limit_bytes: usize,
    protocol_version: i64,
) -> bool {
    encode_pubsub_messages(vec![message], output, output_limit_bytes, protocol_version)
}

fn encode_pubsub_messages(
    messages: Vec<PubSubMessage>,
    output: &mut Vec<u8>,
    output_limit_bytes: usize,
    protocol_version: i64,
) -> bool {
    let use_push = protocol_version >= 3;

    for message in messages {
        let frame = match message {
            PubSubMessage::Message { channel, payload } => {
                let inner = vec![
                    RespFrame::bulk_str("message"),
                    RespFrame::BulkString(Some(channel)),
                    RespFrame::BulkString(Some(payload)),
                ];
                if use_push {
                    RespFrame::Push(inner)
                } else {
                    RespFrame::Array(inner)
                }
            }
            PubSubMessage::SMessage { channel, payload } => {
                let inner = vec![
                    RespFrame::bulk_str("smessage"),
                    RespFrame::BulkString(Some(channel)),
                    RespFrame::BulkString(Some(payload)),
                ];
                if use_push {
                    RespFrame::Push(inner)
                } else {
                    RespFrame::Array(inner)
                }
            }
            PubSubMessage::PMessage {
                pattern,
                channel,
                payload,
            } => {
                let inner = vec![
                    RespFrame::bulk_str("pmessage"),
                    RespFrame::BulkString(Some(pattern)),
                    RespFrame::BulkString(Some(channel)),
                    RespFrame::BulkString(Some(payload)),
                ];
                if use_push {
                    RespFrame::Push(inner)
                } else {
                    RespFrame::Array(inner)
                }
            }
            PubSubMessage::Invalidate { keys } => {
                let inner = vec![
                    RespFrame::bulk_str("invalidate"),
                    RespFrame::Array(
                        keys.into_iter()
                            .map(|key| RespFrame::BulkString(Some(key)))
                            .collect(),
                    ),
                ];
                if use_push {
                    RespFrame::Push(inner)
                } else {
                    RespFrame::Array(inner)
                }
            }
            PubSubMessage::TrackingRedirectBroken { redirect_client_id } => {
                if use_push {
                    RespFrame::Push(vec![
                        RespFrame::bulk_str("tracking-redir-broken"),
                        RespFrame::Integer(redirect_client_id),
                    ])
                } else {
                    RespFrame::Array(vec![
                        RespFrame::bulk_str("tracking-redir-broken"),
                        RespFrame::Integer(redirect_client_id),
                    ])
                }
            }
        };
        if !append_encoded_frame(output, &frame, output_limit_bytes) {
            return false;
        }
    }

    true
}

pub async fn handle_client(stream: TcpStream, server_state: SharedServerState) -> io::Result<()> {
    let persistence = Arc::new(
        PersistenceRuntime::from_config(&crate::config::ServerConfig::default()).map_err(
            |error| {
                io::Error::new(
                    error.kind(),
                    format!("creating default persistence runtime for client handler: {error}"),
                )
            },
        )?,
    );
    handle_client_with_limits(stream, server_state, persistence, ClientIoLimits::default()).await
}

pub async fn handle_client_with_limits(
    stream: TcpStream,
    server_state: SharedServerState,
    persistence: Arc<PersistenceRuntime>,
    io_limits: ClientIoLimits,
) -> io::Result<()> {
    let (client_id, remote_addr) = {
        // Lock-free: atomic client ID allocation and stats update.
        let id = server_state.alloc_client_id();
        server_state.stats.mark_client_connected();
        let active = server_state.stats.connected_clients();
        metrics::set_active_connections(active as usize);
        metrics::record_connection_event("accepted");
        let addr = stream
            .peer_addr()
            .map(|a| a.to_string())
            .unwrap_or_else(|_| "unknown".to_string());
        (id, addr)
    };

    // Create a tracing span for this client session
    let client_span = tracing::info_span!(
        "client_session",
        client_id = client_id,
        remote_addr = %remote_addr,
    );

    let _span_enter = client_span.enter();

    tracing::debug!(
        target = "ratatosk::client",
        client_id = client_id,
        remote_addr = %remote_addr,
        "client connected"
    );

    let result = handle_client_inner(stream, &server_state, &persistence, client_id, io_limits)
        .await
        .map_err(|error| {
            io::Error::new(
                error.kind(),
                format!(
                    "handling client I/O (client_id={}, remote_addr={}): {}",
                    client_id, remote_addr, error
                ),
            )
        });

    let disconnect_reason = match &result {
        Ok(()) => "closed",
        Err(e) if is_benign_disconnect(e) => "client_disconnect",
        Err(_) => "error",
    };

    {
        let mut server = server_state.meta.lock().await;
        server.stats.mark_client_disconnected();
        server.pubsub.remove_client(client_id);
        server.replication_remove_client(client_id);
        server.tracking_remove_client(client_id);
        server.unregister_monitor(client_id);
        server.remove_client_snapshot(client_id);
    }
    // Lock-free stats update after releasing the inner lock.
    server_state.stats.mark_client_disconnected();
    let active = server_state.stats.connected_clients();
    metrics::set_active_connections(active as usize);
    metrics::record_connection_event(disconnect_reason);

    tracing::debug!(
        target = "ratatosk::client",
        client_id = client_id,
        reason = disconnect_reason,
        "client disconnected"
    );

    result
}

async fn handle_client_inner(
    mut stream: TcpStream,
    server_state: &SharedServerState,
    persistence: &Arc<PersistenceRuntime>,
    client_id: i64,
    io_limits: ClientIoLimits,
) -> io::Result<()> {
    if let Err(error) = stream.set_nodelay(true) {
        tracing::debug!(error = %error, "failed to enable TCP_NODELAY");
    }

    let mut input = BytesMut::with_capacity(4096);
    let mut output = Vec::with_capacity(4096);
    let mut pending_input_bytes = 0u64;
    let mut client_state = ClientState::new(client_id);
    let (addr, laddr) = socket_addr_bytes(&stream);
    let (
        mut pubsub_rx,
        monitor_notifier,
        mut query_buffer_limit,
        mut output_buffer_flush_threshold,
        mut write_timeout,
    ) = {
        let mut server = server_state.meta.lock().await;
        server.stats.mark_client_connected();
        let rx = server.pubsub.register_client(client_id);
        let mn = server.register_monitor_notifier(client_id);
        let (qbl, obft, wt) = load_connection_runtime_config(server_state);
        (rx, mn, qbl, obft, wt)
    };
    refresh_client_snapshot(server_state, &client_state, &addr, &laddr, false).await;
    loop {
        let client_accepts_async_push = client_state.has_pubsub_subscriptions()
            || client_state.tracking_enabled()
            || client_state.is_monitor();

        // Drain any already-buffered pubsub messages (lock-free via mpsc).
        {
            let mut had_pubsub = false;
            while let Ok(msg) = pubsub_rx.try_recv() {
                had_pubsub = true;
                if !encode_pubsub_message(
                    msg,
                    &mut output,
                    io_limits.output_buffer_limit_bytes,
                    client_state.protocol_version(),
                ) {
                    tracing::warn!(
                        client_id = client_state.id(),
                        output_limit_bytes = io_limits.output_buffer_limit_bytes,
                        "disconnecting client: pubsub output frame exceeded buffer limit"
                    );
                    let response = encode(&RespFrame::error_str(OUTPUT_BUFFER_LIMIT_ERR));
                    write_all_with_timeout(&mut stream, &response, write_timeout).await?;
                    return Ok(());
                }
            }
            if had_pubsub {
                write_all_with_timeout(&mut stream, &output, write_timeout).await?;
                output.clear();
            }
        }

        // Drain MONITOR messages — each line is sent as a RESP simple string.
        {
            let monitor_pending = {
                let mut server = server_state.meta.lock().await;
                server.drain_monitor_messages(client_state.id())
            };
            if !monitor_pending.is_empty() {
                for line in monitor_pending {
                    let frame = RespFrame::SimpleString(line);
                    if !append_encoded_frame(
                        &mut output,
                        &frame,
                        io_limits.output_buffer_limit_bytes,
                    ) {
                        tracing::warn!(
                            client_id = client_state.id(),
                            output_limit_bytes = io_limits.output_buffer_limit_bytes,
                            "disconnecting monitor client: output buffer limit exceeded"
                        );
                        let response = encode(&RespFrame::error_str(OUTPUT_BUFFER_LIMIT_ERR));
                        write_all_with_timeout(&mut stream, &response, write_timeout).await?;
                        return Ok(());
                    }
                }
                write_all_with_timeout(&mut stream, &output, write_timeout).await?;
                output.clear();
            }
        }

        // Wait for either: a pubsub push message, a monitor notification,
        // or network input from the client.
        let wait_result = if io_limits.client_read_timeout_sec > 0 && !client_accepts_async_push {
            let idle_duration = Duration::from_secs(io_limits.client_read_timeout_sec);
            tokio::select! {
                msg = pubsub_rx.recv() => match msg {
                    Some(m) => Ok(WaitResult::PubSubMsg(m)),
                    None => Ok(WaitResult::PubSubClosed),
                },
                _ = monitor_notifier.notified() => Ok(WaitResult::MonitorWake),
                result = timeout(idle_duration, stream.read_buf(&mut input)) => match result {
                    Ok(result) => result.map(WaitResult::NetworkRead),
                    Err(_) => {
                        tracing::debug!(
                            client_id = client_id,
                            timeout_sec = io_limits.client_read_timeout_sec,
                            "disconnecting idle client: read timeout"
                        );
                        return Ok(());
                    }
                }
            }?
        } else {
            wait_for_async_push_or_input(
                &mut stream,
                &mut input,
                &mut pubsub_rx,
                monitor_notifier.as_ref(),
            )
            .await?
        };

        let read = match wait_result {
            WaitResult::PubSubMsg(msg) => {
                if !encode_pubsub_message(
                    msg,
                    &mut output,
                    io_limits.output_buffer_limit_bytes,
                    client_state.protocol_version(),
                ) {
                    let response = encode(&RespFrame::error_str(OUTPUT_BUFFER_LIMIT_ERR));
                    write_all_with_timeout(&mut stream, &response, write_timeout).await?;
                    return Ok(());
                }
                write_all_with_timeout(&mut stream, &output, write_timeout).await?;
                output.clear();
                continue;
            }
            WaitResult::PubSubClosed => {
                tracing::warn!(
                    client_id = client_id,
                    "disconnecting pubsub client: push channel closed (overflow)"
                );
                let response = encode(&RespFrame::error_str(
                    "ERR pubsub pending output buffer limit exceeded",
                ));
                write_all_with_timeout(&mut stream, &response, write_timeout).await?;
                return Ok(());
            }
            WaitResult::MonitorWake => {
                // Loop back to drain monitor messages at the top.
                continue;
            }
            WaitResult::NetworkRead(n) => n,
        };

        if read == 0 {
            return Ok(());
        }

        // Lock-free stats update for network I/O bytes.
        server_state.stats.add_net_input_bytes(read as u64);
        pending_input_bytes = pending_input_bytes.saturating_add(read as u64);

        if input.len() > query_buffer_limit {
            flush_pending_input_bytes(server_state, &mut pending_input_bytes).await;
            let frame = RespFrame::error_str("ERR query buffer limit exceeded");
            write_all_with_timeout(&mut stream, &encode(&frame), write_timeout).await?;
            return Ok(());
        }

        let mut parsed_frames = Vec::new();
        loop {
            match parse(&mut input) {
                Ok(Some(frame)) => parsed_frames.push(frame),
                Ok(None) => break,
                Err(error) => {
                    flush_pending_input_bytes(server_state, &mut pending_input_bytes).await;
                    tracing::warn!(
                        target = "ratatosk::protocol",
                        client_id = client_state.id(),
                        input_len = input.len(),
                        error = %error,
                        "protocol parse error; closing client connection"
                    );
                    let response = encode(&RespFrame::error_str("ERR protocol error"));
                    write_all_with_timeout(&mut stream, &response, write_timeout).await?;
                    return Ok(());
                }
            }
        }

        flush_pending_input_bytes(server_state, &mut pending_input_bytes).await;

        let outcomes =
            match try_run_readonly_batch(parsed_frames, server_state, &mut client_state).await {
                Ok(outcomes) => outcomes,
                Err(frames) => {
                    let mut outcomes = Vec::with_capacity(frames.len());
                    for frame in frames {
                        let outcome = run_with_blocking_retry(
                            frame,
                            server_state,
                            persistence,
                            &mut client_state,
                            &stream,
                            &addr,
                            &laddr,
                        )
                        .await
                        .map_err(|error| {
                            io::Error::new(
                                error.kind(),
                                format!(
                                    "executing command pipeline for client_id={}: {}",
                                    client_state.id(),
                                    error
                                ),
                            )
                        })?;
                        outcomes.push(outcome);
                    }
                    outcomes
                }
            };

        let mut should_close = false;
        for outcome in outcomes {
            if outcome.config_dirty {
                reload_connection_runtime_config(
                    server_state,
                    &mut query_buffer_limit,
                    &mut output_buffer_flush_threshold,
                    &mut write_timeout,
                );
            }

            // Apply progressive delay (e.g. AUTH failure backoff) before sending response.
            if let Some(delay_ms) = outcome.delay_ms {
                tokio::time::sleep(std::time::Duration::from_millis(delay_ms)).await;
            }

            // Enforce CLIENT REPLY mode: suppress responses when off/skip.
            // Push notifications (pub/sub, invalidation) are unaffected.
            let reply_mode = client_state.reply_mode().clone();
            let suppress = match reply_mode.as_ref() {
                b"off" => true,
                b"skip" => {
                    // Skip this one response, then reset to "on"
                    client_state.set_reply_mode_on();
                    true
                }
                _ => false,
            };

            if !suppress {
                if !append_encoded_frame(
                    &mut output,
                    &outcome.response,
                    io_limits.output_buffer_limit_bytes,
                ) {
                    tracing::warn!(
                        client_id = client_state.id(),
                        output_limit_bytes = io_limits.output_buffer_limit_bytes,
                        "disconnecting client: command response exceeded output buffer limit"
                    );
                    let response = encode(&RespFrame::error_str(OUTPUT_BUFFER_LIMIT_ERR));
                    write_all_with_timeout(&mut stream, &response, write_timeout).await?;
                    return Ok(());
                }

                if output.len() >= output_buffer_flush_threshold {
                    let out_len = output.len() as u64;
                    write_all_with_timeout(&mut stream, &output, write_timeout).await?;
                    output.clear();
                    server_state.stats.add_net_output_bytes(out_len);
                    let mut server = server_state.meta.lock().await;
                    server.stats.add_net_output_bytes(out_len);
                }
            }

            if outcome.close {
                should_close = true;
                break;
            }
        }

        // After command execution, drain any pubsub messages that arrived
        // during processing (lock-free via mpsc try_recv).
        let should_poll_pubsub =
            client_state.has_pubsub_subscriptions() || client_state.tracking_enabled();
        if should_poll_pubsub {
            while let Ok(msg) = pubsub_rx.try_recv() {
                if !encode_pubsub_message(
                    msg,
                    &mut output,
                    io_limits.output_buffer_limit_bytes,
                    client_state.protocol_version(),
                ) {
                    tracing::warn!(
                        client_id = client_state.id(),
                        output_limit_bytes = io_limits.output_buffer_limit_bytes,
                        "disconnecting client: pubsub output frame exceeded buffer limit"
                    );
                    let response = encode(&RespFrame::error_str(OUTPUT_BUFFER_LIMIT_ERR));
                    write_all_with_timeout(&mut stream, &response, write_timeout).await?;
                    return Ok(());
                }
            }
        }

        // Drain MONITOR messages accumulated during command execution.
        if client_state.is_monitor() {
            let monitor_msgs = {
                let mut server = server_state.meta.lock().await;
                server.drain_monitor_messages(client_state.id())
            };
            for line in monitor_msgs {
                let frame = RespFrame::SimpleString(line);
                if !append_encoded_frame(&mut output, &frame, io_limits.output_buffer_limit_bytes) {
                    tracing::warn!(
                        client_id = client_state.id(),
                        output_limit_bytes = io_limits.output_buffer_limit_bytes,
                        "disconnecting monitor client: output buffer limit exceeded"
                    );
                    let response = encode(&RespFrame::error_str(OUTPUT_BUFFER_LIMIT_ERR));
                    write_all_with_timeout(&mut stream, &response, write_timeout).await?;
                    return Ok(());
                }
            }
        }

        if !output.is_empty() {
            let out_len = output.len() as u64;
            write_all_with_timeout(&mut stream, &output, write_timeout).await?;
            output.clear();
            server_state.stats.add_net_output_bytes(out_len);
            let mut server = server_state.meta.lock().await;
            server.stats.add_net_output_bytes(out_len);
        }

        refresh_client_snapshot(server_state, &client_state, &addr, &laddr, false).await;

        if should_close {
            return Ok(());
        }
    }
}
#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use hashbrown::{HashMap, HashSet};
    use ratatosk_engine::{
        acl::AclUser,
        command::ClientState,
        keyspace::{HashFieldEntry, ServerState, SharedState, SortedSet, StoredValue},
    };
    use ratatosk_resp::RespFrame;
    use std::{collections::VecDeque, sync::Arc, time::Duration};
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::{TcpListener, TcpStream},
        time::timeout,
    };

    use super::{
        ClientIoLimits, handle_client, handle_client_with_limits,
        try_execute_lock_free_fast_command, try_run_readonly_batch,
    };
    use crate::config::DEFAULT_OUTPUT_BUFFER_LIMIT_BYTES;
    use crate::persistence::PersistenceRuntime;

    async fn setup_client_server() -> (TcpStream, tokio::task::JoinHandle<()>) {
        setup_client_server_with_limits(ClientIoLimits::default()).await
    }

    async fn setup_client_server_with_shared(
        io_limits: ClientIoLimits,
    ) -> (TcpStream, Arc<SharedState>, tokio::task::JoinHandle<()>) {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind listener");
        let addr = listener.local_addr().expect("local addr");
        let shared = Arc::new(SharedState::new(ServerState::with_default_dbs()));
        let shared_for_server = Arc::clone(&shared);

        let server_task = tokio::spawn(async move {
            let (socket, _) = listener.accept().await.expect("accept");
            let persistence = Arc::new(
                PersistenceRuntime::from_config(&crate::config::ServerConfig::default())
                    .expect("persistence runtime"),
            );
            handle_client_with_limits(socket, shared_for_server, persistence, io_limits)
                .await
                .expect("handle client");
        });

        let client = TcpStream::connect(addr).await.expect("connect client");
        (client, shared, server_task)
    }

    async fn setup_client_server_with_limits(
        io_limits: ClientIoLimits,
    ) -> (TcpStream, tokio::task::JoinHandle<()>) {
        let (client, _shared, server_task) = setup_client_server_with_shared(io_limits).await;
        (client, server_task)
    }

    async fn setup_client_server_with_persistence(
        io_limits: ClientIoLimits,
        persistence: Arc<PersistenceRuntime>,
    ) -> (TcpStream, tokio::task::JoinHandle<()>) {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind listener");
        let addr = listener.local_addr().expect("local addr");
        let shared = Arc::new(SharedState::new(ServerState::with_default_dbs()));

        let server_task = tokio::spawn(async move {
            let (socket, _) = listener.accept().await.expect("accept");
            handle_client_with_limits(socket, shared, persistence, io_limits)
                .await
                .expect("handle client");
        });

        let client = TcpStream::connect(addr).await.expect("connect client");
        (client, server_task)
    }

    async fn read_reply(stream: &mut TcpStream) -> Vec<u8> {
        let mut buf = vec![0u8; 4096];
        let n = timeout(Duration::from_secs(1), stream.read(&mut buf))
            .await
            .expect("read timeout")
            .expect("read reply");
        buf.truncate(n);
        buf
    }

    /// Search the temp dir for any AOF file and return its concatenated contents.
    fn find_aof_content(dir: &std::path::Path) -> String {
        let mut content = String::new();
        for entry in std::fs::read_dir(dir).expect("read dir") {
            let entry = entry.expect("dir entry");
            let name = entry.file_name();
            let name_str = name.to_string_lossy();
            if name_str.ends_with(".aof") && !name_str.ends_with(".manifest") {
                let text = std::fs::read_to_string(entry.path()).unwrap_or_default();
                content.push_str(&text);
            }
        }
        assert!(
            !content.is_empty(),
            "no AOF file found in {}",
            dir.display()
        );
        content
    }

    fn parse_integer_reply(reply: &[u8]) -> i64 {
        let text = std::str::from_utf8(reply).expect("valid integer reply utf8");
        text.trim_start_matches(':')
            .trim()
            .parse::<i64>()
            .expect("integer reply")
    }

    async fn read_exact_reply(stream: &mut TcpStream, expected_len: usize) -> Vec<u8> {
        let mut out = Vec::with_capacity(expected_len);
        while out.len() < expected_len {
            let chunk = read_reply(stream).await;
            out.extend_from_slice(&chunk);
        }
        out
    }

    #[test]
    fn lock_free_fast_path_auto_auths_default_user_and_updates_atomic_stats() {
        let shared = Arc::new(SharedState::new(ServerState::with_default_dbs()));
        let mut client = ClientState::new(7);
        let argv = vec![Bytes::from_static(b"PING")];

        let outcome = try_execute_lock_free_fast_command(&argv, &shared, &mut client)
            .expect("lock-free PING should be handled");

        assert_eq!(outcome.response, RespFrame::pong());
        assert!(client.is_authenticated());
        assert_eq!(client.acl_user(), &Bytes::from_static(b"default"));
        assert_eq!(shared.stats.total_commands_processed(), 1);
    }

    #[test]
    fn lock_free_fast_path_skips_non_default_authenticated_users() {
        let shared = Arc::new(SharedState::new(ServerState::with_default_dbs()));
        let mut client = ClientState::new(8);
        client.authenticate_as(Bytes::from_static(b"alice"));
        let argv = vec![Bytes::from_static(b"PING")];

        assert!(try_execute_lock_free_fast_command(&argv, &shared, &mut client).is_none());
        assert_eq!(shared.stats.total_commands_processed(), 0);
    }

    #[tokio::test]
    async fn lock_free_fast_path_obeys_acl_cache_and_declines_ping_health() {
        let shared = Arc::new(SharedState::new(ServerState::with_default_dbs()));

        {
            let mut server = shared.meta.lock().await;
            let default_user = server
                .acl
                .get_or_create_user_mut(&Bytes::from_static(b"default"));
            default_user.nopass = false;
            shared.update_acl_policy_cache(&server.acl);
        }

        let mut unauthenticated_client = ClientState::new(9);
        let ping_argv = vec![Bytes::from_static(b"PING")];
        assert!(
            try_execute_lock_free_fast_command(&ping_argv, &shared, &mut unauthenticated_client)
                .is_none()
        );

        let mut authenticated_client = ClientState::new(10);
        authenticated_client.authenticate_as(Bytes::from_static(b"default"));
        let health_argv = vec![Bytes::from_static(b"PING"), Bytes::from_static(b"HEALTH")];
        assert!(
            try_execute_lock_free_fast_command(&health_argv, &shared, &mut authenticated_client)
                .is_none()
        );
    }

    #[test]
    fn lock_free_fast_path_handles_dbsize_and_purges_expired_keys() {
        let shared = Arc::new(SharedState::new(ServerState::with_default_dbs()));
        {
            let mut db = shared.data.write_db(0);
            db.data.insert(
                Bytes::from_static(b"expired"),
                StoredValue::string(Bytes::from_static(b"gone"), Some(1)),
            );
            db.data.insert(
                Bytes::from_static(b"live"),
                StoredValue::string(Bytes::from_static(b"ok"), None),
            );
        }

        let mut client = ClientState::new(11);
        let argv = vec![Bytes::from_static(b"DBSIZE")];
        let outcome = try_execute_lock_free_fast_command(&argv, &shared, &mut client)
            .expect("lock-free DBSIZE should be handled");

        assert_eq!(outcome.response, RespFrame::Integer(1));
        let db = shared.data.read_db(0);
        assert!(!db.data.contains_key(b"expired" as &[u8]));
        assert!(db.data.contains_key(b"live" as &[u8]));
    }

    #[test]
    fn lock_free_fast_path_handles_type_and_exists_and_updates_atomic_stats() {
        let shared = Arc::new(SharedState::new(ServerState::with_default_dbs()));
        {
            let mut db = shared.data.write_db(0);
            db.data.insert(
                Bytes::from_static(b"expired"),
                StoredValue::string(Bytes::from_static(b"gone"), Some(1)),
            );
            db.data.insert(
                Bytes::from_static(b"live"),
                StoredValue::string(Bytes::from_static(b"ok"), None),
            );
        }

        let mut client = ClientState::new(12);
        let type_argv = vec![Bytes::from_static(b"TYPE"), Bytes::from_static(b"live")];
        let type_outcome = try_execute_lock_free_fast_command(&type_argv, &shared, &mut client)
            .expect("lock-free TYPE should be handled");
        assert_eq!(type_outcome.response, RespFrame::simple_str("string"));

        let exists_argv = vec![
            Bytes::from_static(b"EXISTS"),
            Bytes::from_static(b"live"),
            Bytes::from_static(b"expired"),
            Bytes::from_static(b"missing"),
        ];
        let exists_outcome = try_execute_lock_free_fast_command(&exists_argv, &shared, &mut client)
            .expect("lock-free EXISTS should be handled");
        assert_eq!(exists_outcome.response, RespFrame::Integer(1));

        let missing_type_argv = vec![Bytes::from_static(b"TYPE"), Bytes::from_static(b"missing")];
        let missing_type =
            try_execute_lock_free_fast_command(&missing_type_argv, &shared, &mut client)
                .expect("lock-free TYPE none should be handled");
        assert_eq!(missing_type.response, RespFrame::simple_str("none"));

        let db = shared.data.read_db(0);
        assert!(!db.data.contains_key(b"expired" as &[u8]));
        assert!(db.data.contains_key(b"live" as &[u8]));
        assert_eq!(shared.stats.keyspace_hits(), 1);
        assert_eq!(shared.stats.keyspace_misses(), 2);
        assert_eq!(shared.stats.total_commands_processed(), 3);
    }

    #[test]
    fn lock_free_fast_path_handles_string_reads_and_get_stats() {
        let shared = Arc::new(SharedState::new(ServerState::with_default_dbs()));
        {
            let mut db = shared.data.write_db(0);
            db.data.insert(
                Bytes::from_static(b"live"),
                StoredValue::string(Bytes::from_static(b"value"), None),
            );
            db.data.insert(
                Bytes::from_static(b"expired"),
                StoredValue::string(Bytes::from_static(b"gone"), Some(1)),
            );
        }

        let mut client = ClientState::new(13);

        let get_argv = vec![Bytes::from_static(b"GET"), Bytes::from_static(b"live")];
        let get_outcome = try_execute_lock_free_fast_command(&get_argv, &shared, &mut client)
            .expect("lock-free GET should be handled");
        assert_eq!(
            get_outcome.response,
            RespFrame::BulkString(Some(Bytes::from_static(b"value")))
        );

        let strlen_argv = vec![Bytes::from_static(b"STRLEN"), Bytes::from_static(b"live")];
        let strlen_outcome = try_execute_lock_free_fast_command(&strlen_argv, &shared, &mut client)
            .expect("lock-free STRLEN should be handled");
        assert_eq!(strlen_outcome.response, RespFrame::Integer(5));

        let mget_argv = vec![
            Bytes::from_static(b"MGET"),
            Bytes::from_static(b"live"),
            Bytes::from_static(b"expired"),
            Bytes::from_static(b"missing"),
        ];
        let mget_outcome = try_execute_lock_free_fast_command(&mget_argv, &shared, &mut client)
            .expect("lock-free MGET should be handled");
        assert_eq!(
            mget_outcome.response,
            RespFrame::Array(vec![
                RespFrame::BulkString(Some(Bytes::from_static(b"value"))),
                RespFrame::BulkString(None),
                RespFrame::BulkString(None),
            ])
        );

        let missing_get_argv = vec![Bytes::from_static(b"GET"), Bytes::from_static(b"missing")];
        let missing_get =
            try_execute_lock_free_fast_command(&missing_get_argv, &shared, &mut client)
                .expect("lock-free GET miss should be handled");
        assert_eq!(missing_get.response, RespFrame::BulkString(None));

        let db = shared.data.read_db(0);
        assert!(!db.data.contains_key(b"expired" as &[u8]));
        assert_eq!(shared.stats.keyspace_hits(), 1);
        assert_eq!(shared.stats.keyspace_misses(), 1);
        assert_eq!(shared.stats.total_commands_processed(), 4);
    }

    #[test]
    fn lock_free_fast_path_handles_getrange_and_substr() {
        let shared = Arc::new(SharedState::new(ServerState::with_default_dbs()));
        {
            let mut db = shared.data.write_db(0);
            db.data.insert(
                Bytes::from_static(b"alpha"),
                StoredValue::string(Bytes::from_static(b"value"), None),
            );
            db.data.insert(
                Bytes::from_static(b"expired"),
                StoredValue::string(Bytes::from_static(b"gone"), Some(1)),
            );
        }

        let mut client = ClientState::new(14);

        let getrange_argv = vec![
            Bytes::from_static(b"GETRANGE"),
            Bytes::from_static(b"alpha"),
            Bytes::from_static(b"1"),
            Bytes::from_static(b"3"),
        ];
        let getrange = try_execute_lock_free_fast_command(&getrange_argv, &shared, &mut client)
            .expect("lock-free GETRANGE should be handled");
        assert_eq!(
            getrange.response,
            RespFrame::BulkString(Some(Bytes::from_static(b"alu")))
        );

        let substr_argv = vec![
            Bytes::from_static(b"SUBSTR"),
            Bytes::from_static(b"alpha"),
            Bytes::from_static(b"-2"),
            Bytes::from_static(b"-1"),
        ];
        let substr = try_execute_lock_free_fast_command(&substr_argv, &shared, &mut client)
            .expect("lock-free SUBSTR should be handled");
        assert_eq!(
            substr.response,
            RespFrame::BulkString(Some(Bytes::from_static(b"ue")))
        );

        let expired_argv = vec![
            Bytes::from_static(b"GETRANGE"),
            Bytes::from_static(b"expired"),
            Bytes::from_static(b"0"),
            Bytes::from_static(b"10"),
        ];
        let expired = try_execute_lock_free_fast_command(&expired_argv, &shared, &mut client)
            .expect("lock-free GETRANGE on expired key should be handled");
        assert_eq!(expired.response, RespFrame::BulkString(Some(Bytes::new())));

        let db = shared.data.read_db(0);
        assert!(!db.data.contains_key(b"expired" as &[u8]));
        assert_eq!(shared.stats.total_commands_processed(), 3);
    }

    #[test]
    fn lock_free_fast_path_handles_hash_set_and_zset_reads() {
        let shared = Arc::new(SharedState::new(ServerState::with_default_dbs()));
        {
            let mut hash = HashMap::new();
            hash.insert(
                Bytes::from_static(b"live"),
                HashFieldEntry::new(Bytes::from_static(b"payload")),
            );
            hash.insert(
                Bytes::from_static(b"expired"),
                HashFieldEntry::with_ttl(Bytes::from_static(b"gone"), 1),
            );

            let mut set = HashSet::new();
            set.insert(Bytes::from_static(b"a"));
            set.insert(Bytes::from_static(b"b"));

            let mut zset = SortedSet::default();
            assert!(zset.insert(Bytes::from_static(b"one"), 1.0));
            assert!(zset.insert(Bytes::from_static(b"two"), 2.5));

            let mut db = shared.data.write_db(0);
            db.data
                .insert(Bytes::from_static(b"hash"), StoredValue::hash(hash, None));
            db.data
                .insert(Bytes::from_static(b"set"), StoredValue::set(set, None));
            db.data.insert(
                Bytes::from_static(b"zset"),
                StoredValue::sorted_set(zset, None),
            );
        }

        let mut client = ClientState::new(15);

        let hget = try_execute_lock_free_fast_command(
            &[
                Bytes::from_static(b"HGET"),
                Bytes::from_static(b"hash"),
                Bytes::from_static(b"live"),
            ],
            &shared,
            &mut client,
        )
        .expect("lock-free HGET should be handled");
        assert_eq!(
            hget.response,
            RespFrame::BulkString(Some(Bytes::from_static(b"payload")))
        );

        let hmget = try_execute_lock_free_fast_command(
            &[
                Bytes::from_static(b"HMGET"),
                Bytes::from_static(b"hash"),
                Bytes::from_static(b"live"),
                Bytes::from_static(b"expired"),
                Bytes::from_static(b"missing"),
            ],
            &shared,
            &mut client,
        )
        .expect("lock-free HMGET should be handled");
        assert_eq!(
            hmget.response,
            RespFrame::Array(vec![
                RespFrame::BulkString(Some(Bytes::from_static(b"payload"))),
                RespFrame::BulkString(None),
                RespFrame::BulkString(None),
            ])
        );

        let hgetall = try_execute_lock_free_fast_command(
            &[Bytes::from_static(b"HGETALL"), Bytes::from_static(b"hash")],
            &shared,
            &mut client,
        )
        .expect("lock-free HGETALL should be handled");
        assert_eq!(
            hgetall.response,
            RespFrame::Array(vec![
                RespFrame::BulkString(Some(Bytes::from_static(b"live"))),
                RespFrame::BulkString(Some(Bytes::from_static(b"payload"))),
            ])
        );

        let hkeys = try_execute_lock_free_fast_command(
            &[Bytes::from_static(b"HKEYS"), Bytes::from_static(b"hash")],
            &shared,
            &mut client,
        )
        .expect("lock-free HKEYS should be handled");
        assert_eq!(
            hkeys.response,
            RespFrame::Array(vec![RespFrame::BulkString(Some(Bytes::from_static(
                b"live"
            )))])
        );

        let hvals = try_execute_lock_free_fast_command(
            &[Bytes::from_static(b"HVALS"), Bytes::from_static(b"hash")],
            &shared,
            &mut client,
        )
        .expect("lock-free HVALS should be handled");
        assert_eq!(
            hvals.response,
            RespFrame::Array(vec![RespFrame::BulkString(Some(Bytes::from_static(
                b"payload"
            )))])
        );

        let hexists = try_execute_lock_free_fast_command(
            &[
                Bytes::from_static(b"HEXISTS"),
                Bytes::from_static(b"hash"),
                Bytes::from_static(b"expired"),
            ],
            &shared,
            &mut client,
        )
        .expect("lock-free HEXISTS should be handled");
        assert_eq!(hexists.response, RespFrame::Integer(0));

        let hlen = try_execute_lock_free_fast_command(
            &[Bytes::from_static(b"HLEN"), Bytes::from_static(b"hash")],
            &shared,
            &mut client,
        )
        .expect("lock-free HLEN should be handled");
        assert_eq!(hlen.response, RespFrame::Integer(1));

        let sismember = try_execute_lock_free_fast_command(
            &[
                Bytes::from_static(b"SISMEMBER"),
                Bytes::from_static(b"set"),
                Bytes::from_static(b"b"),
            ],
            &shared,
            &mut client,
        )
        .expect("lock-free SISMEMBER should be handled");
        assert_eq!(sismember.response, RespFrame::Integer(1));

        let smismember = try_execute_lock_free_fast_command(
            &[
                Bytes::from_static(b"SMISMEMBER"),
                Bytes::from_static(b"set"),
                Bytes::from_static(b"a"),
                Bytes::from_static(b"missing"),
            ],
            &shared,
            &mut client,
        )
        .expect("lock-free SMISMEMBER should be handled");
        assert_eq!(
            smismember.response,
            RespFrame::Array(vec![RespFrame::Integer(1), RespFrame::Integer(0)])
        );

        let scard = try_execute_lock_free_fast_command(
            &[Bytes::from_static(b"SCARD"), Bytes::from_static(b"set")],
            &shared,
            &mut client,
        )
        .expect("lock-free SCARD should be handled");
        assert_eq!(scard.response, RespFrame::Integer(2));

        let zscore = try_execute_lock_free_fast_command(
            &[
                Bytes::from_static(b"ZSCORE"),
                Bytes::from_static(b"zset"),
                Bytes::from_static(b"two"),
            ],
            &shared,
            &mut client,
        )
        .expect("lock-free ZSCORE should be handled");
        assert_eq!(
            zscore.response,
            RespFrame::BulkString(Some(Bytes::from_static(b"2.5")))
        );

        let zcard = try_execute_lock_free_fast_command(
            &[Bytes::from_static(b"ZCARD"), Bytes::from_static(b"zset")],
            &shared,
            &mut client,
        )
        .expect("lock-free ZCARD should be handled");
        assert_eq!(zcard.response, RespFrame::Integer(2));

        assert_eq!(shared.stats.total_commands_processed(), 12);
    }

    #[test]
    fn lock_free_fast_path_handles_bitmap_hash_strlen_and_zset_rank_reads() {
        let shared = Arc::new(SharedState::new(ServerState::with_default_dbs()));
        {
            let mut hash = HashMap::new();
            hash.insert(
                Bytes::from_static(b"live"),
                HashFieldEntry::new(Bytes::from_static(b"payload")),
            );
            hash.insert(
                Bytes::from_static(b"expired"),
                HashFieldEntry::with_ttl(Bytes::from_static(b"gone"), 1),
            );

            let mut zset = SortedSet::default();
            assert!(zset.insert(Bytes::from_static(b"alpha"), 1.0));
            assert!(zset.insert(Bytes::from_static(b"beta"), 2.0));
            assert!(zset.insert(Bytes::from_static(b"gamma"), 3.0));

            let mut db = shared.data.write_db(0);
            db.data
                .insert(Bytes::from_static(b"hash"), StoredValue::hash(hash, None));
            db.data.insert(
                Bytes::from_static(b"bits"),
                StoredValue::string(Bytes::from_static(b"A"), None),
            );
            db.data.insert(
                Bytes::from_static(b"expired-bits"),
                StoredValue::string(Bytes::from_static(b"\xFF"), Some(1)),
            );
            db.data.insert(
                Bytes::from_static(b"zset"),
                StoredValue::sorted_set(zset, None),
            );
        }

        let mut client = ClientState::new(20);

        let hstrlen = try_execute_lock_free_fast_command(
            &[
                Bytes::from_static(b"HSTRLEN"),
                Bytes::from_static(b"hash"),
                Bytes::from_static(b"live"),
            ],
            &shared,
            &mut client,
        )
        .expect("lock-free HSTRLEN should be handled");
        assert_eq!(hstrlen.response, RespFrame::Integer(7));

        let expired_hstrlen = try_execute_lock_free_fast_command(
            &[
                Bytes::from_static(b"HSTRLEN"),
                Bytes::from_static(b"hash"),
                Bytes::from_static(b"expired"),
            ],
            &shared,
            &mut client,
        )
        .expect("lock-free HSTRLEN on expired field should be handled");
        assert_eq!(expired_hstrlen.response, RespFrame::Integer(0));

        let getbit = try_execute_lock_free_fast_command(
            &[
                Bytes::from_static(b"GETBIT"),
                Bytes::from_static(b"bits"),
                Bytes::from_static(b"1"),
            ],
            &shared,
            &mut client,
        )
        .expect("lock-free GETBIT should be handled");
        assert_eq!(getbit.response, RespFrame::Integer(1));

        let expired_getbit = try_execute_lock_free_fast_command(
            &[
                Bytes::from_static(b"GETBIT"),
                Bytes::from_static(b"expired-bits"),
                Bytes::from_static(b"0"),
            ],
            &shared,
            &mut client,
        )
        .expect("lock-free GETBIT on expired key should be handled");
        assert_eq!(expired_getbit.response, RespFrame::Integer(0));

        let zmscore = try_execute_lock_free_fast_command(
            &[
                Bytes::from_static(b"ZMSCORE"),
                Bytes::from_static(b"zset"),
                Bytes::from_static(b"beta"),
                Bytes::from_static(b"missing"),
            ],
            &shared,
            &mut client,
        )
        .expect("lock-free ZMSCORE should be handled");
        assert_eq!(
            zmscore.response,
            RespFrame::Array(vec![
                RespFrame::BulkString(Some(Bytes::from_static(b"2"))),
                RespFrame::BulkString(None),
            ])
        );

        let zcount = try_execute_lock_free_fast_command(
            &[
                Bytes::from_static(b"ZCOUNT"),
                Bytes::from_static(b"zset"),
                Bytes::from_static(b"1"),
                Bytes::from_static(b"2"),
            ],
            &shared,
            &mut client,
        )
        .expect("lock-free ZCOUNT should be handled");
        assert_eq!(zcount.response, RespFrame::Integer(2));

        let zlexcount = try_execute_lock_free_fast_command(
            &[
                Bytes::from_static(b"ZLEXCOUNT"),
                Bytes::from_static(b"zset"),
                Bytes::from_static(b"[beta"),
                Bytes::from_static(b"[gamma"),
            ],
            &shared,
            &mut client,
        )
        .expect("lock-free ZLEXCOUNT should be handled");
        assert_eq!(zlexcount.response, RespFrame::Integer(2));

        let zrank = try_execute_lock_free_fast_command(
            &[
                Bytes::from_static(b"ZRANK"),
                Bytes::from_static(b"zset"),
                Bytes::from_static(b"beta"),
            ],
            &shared,
            &mut client,
        )
        .expect("lock-free ZRANK should be handled");
        assert_eq!(zrank.response, RespFrame::Integer(1));

        let zrevrank = try_execute_lock_free_fast_command(
            &[
                Bytes::from_static(b"ZREVRANK"),
                Bytes::from_static(b"zset"),
                Bytes::from_static(b"beta"),
            ],
            &shared,
            &mut client,
        )
        .expect("lock-free ZREVRANK should be handled");
        assert_eq!(zrevrank.response, RespFrame::Integer(1));

        let db = shared.data.read_db(0);
        assert!(!db.data.contains_key(b"expired-bits" as &[u8]));
        assert_eq!(shared.stats.total_commands_processed(), 9);
    }

    #[test]
    fn lock_free_fast_path_handles_zrange_variants() {
        let shared = Arc::new(SharedState::new(ServerState::with_default_dbs()));
        {
            let mut zset = SortedSet::default();
            assert!(zset.insert(Bytes::from_static(b"alpha"), 1.0));
            assert!(zset.insert(Bytes::from_static(b"beta"), 1.0));
            assert!(zset.insert(Bytes::from_static(b"gamma"), 2.0));

            let mut db = shared.data.write_db(0);
            db.data.insert(
                Bytes::from_static(b"zset"),
                StoredValue::sorted_set(zset, None),
            );
        }

        let mut client = ClientState::new(22);

        let rank = try_execute_lock_free_fast_command(
            &[
                Bytes::from_static(b"ZRANGE"),
                Bytes::from_static(b"zset"),
                Bytes::from_static(b"0"),
                Bytes::from_static(b"-1"),
            ],
            &shared,
            &mut client,
        )
        .expect("lock-free ZRANGE rank should be handled");
        assert_eq!(
            rank.response,
            RespFrame::Array(vec![
                RespFrame::BulkString(Some(Bytes::from_static(b"alpha"))),
                RespFrame::BulkString(Some(Bytes::from_static(b"beta"))),
                RespFrame::BulkString(Some(Bytes::from_static(b"gamma"))),
            ])
        );

        let with_scores = try_execute_lock_free_fast_command(
            &[
                Bytes::from_static(b"ZRANGE"),
                Bytes::from_static(b"zset"),
                Bytes::from_static(b"0"),
                Bytes::from_static(b"1"),
                Bytes::from_static(b"WITHSCORES"),
            ],
            &shared,
            &mut client,
        )
        .expect("lock-free ZRANGE WITHSCORES should be handled");
        assert_eq!(
            with_scores.response,
            RespFrame::Array(vec![
                RespFrame::BulkString(Some(Bytes::from_static(b"alpha"))),
                RespFrame::BulkString(Some(Bytes::from_static(b"1"))),
                RespFrame::BulkString(Some(Bytes::from_static(b"beta"))),
                RespFrame::BulkString(Some(Bytes::from_static(b"1"))),
            ])
        );

        let by_score_rev = try_execute_lock_free_fast_command(
            &[
                Bytes::from_static(b"ZRANGE"),
                Bytes::from_static(b"zset"),
                Bytes::from_static(b"2"),
                Bytes::from_static(b"1"),
                Bytes::from_static(b"BYSCORE"),
                Bytes::from_static(b"REV"),
                Bytes::from_static(b"LIMIT"),
                Bytes::from_static(b"0"),
                Bytes::from_static(b"2"),
            ],
            &shared,
            &mut client,
        )
        .expect("lock-free ZRANGE BYSCORE REV should be handled");
        assert_eq!(
            by_score_rev.response,
            RespFrame::Array(vec![
                RespFrame::BulkString(Some(Bytes::from_static(b"gamma"))),
                RespFrame::BulkString(Some(Bytes::from_static(b"beta"))),
            ])
        );

        let by_lex = try_execute_lock_free_fast_command(
            &[
                Bytes::from_static(b"ZRANGE"),
                Bytes::from_static(b"zset"),
                Bytes::from_static(b"[alpha"),
                Bytes::from_static(b"[beta"),
                Bytes::from_static(b"BYLEX"),
            ],
            &shared,
            &mut client,
        )
        .expect("lock-free ZRANGE BYLEX should be handled");
        assert_eq!(
            by_lex.response,
            RespFrame::Array(vec![
                RespFrame::BulkString(Some(Bytes::from_static(b"alpha"))),
                RespFrame::BulkString(Some(Bytes::from_static(b"beta"))),
            ])
        );

        let by_score = try_execute_lock_free_fast_command(
            &[
                Bytes::from_static(b"ZRANGEBYSCORE"),
                Bytes::from_static(b"zset"),
                Bytes::from_static(b"1"),
                Bytes::from_static(b"2"),
                Bytes::from_static(b"WITHSCORES"),
                Bytes::from_static(b"LIMIT"),
                Bytes::from_static(b"1"),
                Bytes::from_static(b"2"),
            ],
            &shared,
            &mut client,
        )
        .expect("lock-free ZRANGEBYSCORE should be handled");
        assert_eq!(
            by_score.response,
            RespFrame::Array(vec![
                RespFrame::BulkString(Some(Bytes::from_static(b"beta"))),
                RespFrame::BulkString(Some(Bytes::from_static(b"1"))),
                RespFrame::BulkString(Some(Bytes::from_static(b"gamma"))),
                RespFrame::BulkString(Some(Bytes::from_static(b"2"))),
            ])
        );

        let rev_by_score = try_execute_lock_free_fast_command(
            &[
                Bytes::from_static(b"ZREVRANGEBYSCORE"),
                Bytes::from_static(b"zset"),
                Bytes::from_static(b"2"),
                Bytes::from_static(b"1"),
                Bytes::from_static(b"WITHSCORES"),
                Bytes::from_static(b"LIMIT"),
                Bytes::from_static(b"0"),
                Bytes::from_static(b"2"),
            ],
            &shared,
            &mut client,
        )
        .expect("lock-free ZREVRANGEBYSCORE should be handled");
        assert_eq!(
            rev_by_score.response,
            RespFrame::Array(vec![
                RespFrame::BulkString(Some(Bytes::from_static(b"gamma"))),
                RespFrame::BulkString(Some(Bytes::from_static(b"2"))),
                RespFrame::BulkString(Some(Bytes::from_static(b"beta"))),
                RespFrame::BulkString(Some(Bytes::from_static(b"1"))),
            ])
        );

        let rev_by_lex = try_execute_lock_free_fast_command(
            &[
                Bytes::from_static(b"ZREVRANGEBYLEX"),
                Bytes::from_static(b"zset"),
                Bytes::from_static(b"[beta"),
                Bytes::from_static(b"[alpha"),
                Bytes::from_static(b"LIMIT"),
                Bytes::from_static(b"0"),
                Bytes::from_static(b"2"),
            ],
            &shared,
            &mut client,
        )
        .expect("lock-free ZREVRANGEBYLEX should be handled");
        assert_eq!(
            rev_by_lex.response,
            RespFrame::Array(vec![
                RespFrame::BulkString(Some(Bytes::from_static(b"beta"))),
                RespFrame::BulkString(Some(Bytes::from_static(b"alpha"))),
            ])
        );

        let rev_rank = try_execute_lock_free_fast_command(
            &[
                Bytes::from_static(b"ZREVRANGE"),
                Bytes::from_static(b"zset"),
                Bytes::from_static(b"0"),
                Bytes::from_static(b"1"),
                Bytes::from_static(b"WITHSCORES"),
            ],
            &shared,
            &mut client,
        )
        .expect("lock-free ZREVRANGE should be handled");
        assert_eq!(
            rev_rank.response,
            RespFrame::Array(vec![
                RespFrame::BulkString(Some(Bytes::from_static(b"gamma"))),
                RespFrame::BulkString(Some(Bytes::from_static(b"2"))),
                RespFrame::BulkString(Some(Bytes::from_static(b"beta"))),
                RespFrame::BulkString(Some(Bytes::from_static(b"1"))),
            ])
        );

        assert_eq!(shared.stats.total_commands_processed(), 8);
    }

    #[test]
    fn lock_free_fast_path_handles_bitcount() {
        let shared = Arc::new(SharedState::new(ServerState::with_default_dbs()));
        {
            let mut db = shared.data.write_db(0);
            db.data.insert(
                Bytes::from_static(b"bits"),
                StoredValue::string(Bytes::from_static(b"AB"), None),
            );
            db.data.insert(
                Bytes::from_static(b"expired"),
                StoredValue::string(Bytes::from_static(b"\xFF"), Some(1)),
            );
        }

        let mut client = ClientState::new(21);

        let all_bits = try_execute_lock_free_fast_command(
            &[Bytes::from_static(b"BITCOUNT"), Bytes::from_static(b"bits")],
            &shared,
            &mut client,
        )
        .expect("lock-free BITCOUNT should be handled");
        assert_eq!(all_bits.response, RespFrame::Integer(4));

        let last_byte = try_execute_lock_free_fast_command(
            &[
                Bytes::from_static(b"BITCOUNT"),
                Bytes::from_static(b"bits"),
                Bytes::from_static(b"-1"),
                Bytes::from_static(b"-1"),
            ],
            &shared,
            &mut client,
        )
        .expect("lock-free BITCOUNT byte range should be handled");
        assert_eq!(last_byte.response, RespFrame::Integer(2));

        let first_byte_bits = try_execute_lock_free_fast_command(
            &[
                Bytes::from_static(b"BITCOUNT"),
                Bytes::from_static(b"bits"),
                Bytes::from_static(b"0"),
                Bytes::from_static(b"7"),
                Bytes::from_static(b"BIT"),
            ],
            &shared,
            &mut client,
        )
        .expect("lock-free BITCOUNT bit mode should be handled");
        assert_eq!(first_byte_bits.response, RespFrame::Integer(2));

        let prefix_bits = try_execute_lock_free_fast_command(
            &[
                Bytes::from_static(b"BITCOUNT"),
                Bytes::from_static(b"bits"),
                Bytes::from_static(b"0"),
                Bytes::from_static(b"3"),
                Bytes::from_static(b"BIT"),
            ],
            &shared,
            &mut client,
        )
        .expect("lock-free BITCOUNT partial bit range should be handled");
        assert_eq!(prefix_bits.response, RespFrame::Integer(1));

        let missing = try_execute_lock_free_fast_command(
            &[
                Bytes::from_static(b"BITCOUNT"),
                Bytes::from_static(b"missing"),
            ],
            &shared,
            &mut client,
        )
        .expect("lock-free BITCOUNT missing should be handled");
        assert_eq!(missing.response, RespFrame::Integer(0));

        let expired = try_execute_lock_free_fast_command(
            &[
                Bytes::from_static(b"BITCOUNT"),
                Bytes::from_static(b"expired"),
            ],
            &shared,
            &mut client,
        )
        .expect("lock-free BITCOUNT expired should be handled");
        assert_eq!(expired.response, RespFrame::Integer(0));

        let db = shared.data.read_db(0);
        assert!(!db.data.contains_key(b"expired" as &[u8]));
        assert_eq!(shared.stats.total_commands_processed(), 6);
    }

    #[test]
    fn lock_free_fast_path_handles_list_reads() {
        let shared = Arc::new(SharedState::new(ServerState::with_default_dbs()));
        {
            let mut db = shared.data.write_db(0);
            db.data.insert(
                Bytes::from_static(b"list"),
                StoredValue::list(
                    VecDeque::from(vec![
                        Bytes::from_static(b"zero"),
                        Bytes::from_static(b"one"),
                        Bytes::from_static(b"two"),
                    ]),
                    None,
                ),
            );
            db.data.insert(
                Bytes::from_static(b"expired"),
                StoredValue::list(VecDeque::from(vec![Bytes::from_static(b"gone")]), Some(1)),
            );
        }

        let mut client = ClientState::new(16);

        let llen = try_execute_lock_free_fast_command(
            &[Bytes::from_static(b"LLEN"), Bytes::from_static(b"list")],
            &shared,
            &mut client,
        )
        .expect("lock-free LLEN should be handled");
        assert_eq!(llen.response, RespFrame::Integer(3));

        let lindex = try_execute_lock_free_fast_command(
            &[
                Bytes::from_static(b"LINDEX"),
                Bytes::from_static(b"list"),
                Bytes::from_static(b"-1"),
            ],
            &shared,
            &mut client,
        )
        .expect("lock-free LINDEX should be handled");
        assert_eq!(
            lindex.response,
            RespFrame::BulkString(Some(Bytes::from_static(b"two")))
        );

        let lrange = try_execute_lock_free_fast_command(
            &[
                Bytes::from_static(b"LRANGE"),
                Bytes::from_static(b"list"),
                Bytes::from_static(b"0"),
                Bytes::from_static(b"1"),
            ],
            &shared,
            &mut client,
        )
        .expect("lock-free LRANGE should be handled");
        assert_eq!(
            lrange.response,
            RespFrame::Array(vec![
                RespFrame::BulkString(Some(Bytes::from_static(b"zero"))),
                RespFrame::BulkString(Some(Bytes::from_static(b"one"))),
            ])
        );

        let expired = try_execute_lock_free_fast_command(
            &[
                Bytes::from_static(b"LRANGE"),
                Bytes::from_static(b"expired"),
                Bytes::from_static(b"0"),
                Bytes::from_static(b"-1"),
            ],
            &shared,
            &mut client,
        )
        .expect("lock-free LRANGE on expired key should be handled");
        assert_eq!(expired.response, RespFrame::Array(vec![]));

        let db = shared.data.read_db(0);
        assert!(!db.data.contains_key(b"expired" as &[u8]));
        assert_eq!(shared.stats.total_commands_processed(), 4);
    }

    #[test]
    fn lock_free_fast_path_handles_ttl_family() {
        let shared = Arc::new(SharedState::new(ServerState::with_default_dbs()));
        let expire_at_ms = ratatosk_core::time::now_ms().saturating_add(5_000);
        {
            let mut db = shared.data.write_db(0);
            db.data.insert(
                Bytes::from_static(b"expiring"),
                StoredValue::string(Bytes::from_static(b"value"), Some(expire_at_ms)),
            );
            db.data.insert(
                Bytes::from_static(b"persistent"),
                StoredValue::string(Bytes::from_static(b"value"), None),
            );
            db.data.insert(
                Bytes::from_static(b"expired"),
                StoredValue::string(Bytes::from_static(b"gone"), Some(1)),
            );
        }

        let mut client = ClientState::new(19);

        let ttl = try_execute_lock_free_fast_command(
            &[Bytes::from_static(b"TTL"), Bytes::from_static(b"expiring")],
            &shared,
            &mut client,
        )
        .expect("lock-free TTL should be handled");
        let RespFrame::Integer(ttl_value) = ttl.response else {
            panic!("TTL should return integer");
        };
        assert!((4..=5).contains(&ttl_value), "ttl={ttl_value}");

        let pttl = try_execute_lock_free_fast_command(
            &[Bytes::from_static(b"PTTL"), Bytes::from_static(b"expiring")],
            &shared,
            &mut client,
        )
        .expect("lock-free PTTL should be handled");
        let RespFrame::Integer(pttl_value) = pttl.response else {
            panic!("PTTL should return integer");
        };
        assert!((4_000..=5_000).contains(&pttl_value), "pttl={pttl_value}");

        let expiretime = try_execute_lock_free_fast_command(
            &[
                Bytes::from_static(b"EXPIRETIME"),
                Bytes::from_static(b"expiring"),
            ],
            &shared,
            &mut client,
        )
        .expect("lock-free EXPIRETIME should be handled");
        assert_eq!(expiretime.response, RespFrame::Integer(expire_at_ms / 1000));

        let pexpiretime = try_execute_lock_free_fast_command(
            &[
                Bytes::from_static(b"PEXPIRETIME"),
                Bytes::from_static(b"expiring"),
            ],
            &shared,
            &mut client,
        )
        .expect("lock-free PEXPIRETIME should be handled");
        assert_eq!(pexpiretime.response, RespFrame::Integer(expire_at_ms));

        let persistent = try_execute_lock_free_fast_command(
            &[
                Bytes::from_static(b"TTL"),
                Bytes::from_static(b"persistent"),
            ],
            &shared,
            &mut client,
        )
        .expect("lock-free TTL persistent should be handled");
        assert_eq!(persistent.response, RespFrame::Integer(-1));

        let missing = try_execute_lock_free_fast_command(
            &[Bytes::from_static(b"PTTL"), Bytes::from_static(b"missing")],
            &shared,
            &mut client,
        )
        .expect("lock-free PTTL missing should be handled");
        assert_eq!(missing.response, RespFrame::Integer(-2));

        let expired = try_execute_lock_free_fast_command(
            &[Bytes::from_static(b"TTL"), Bytes::from_static(b"expired")],
            &shared,
            &mut client,
        )
        .expect("lock-free TTL expired should be handled");
        assert_eq!(expired.response, RespFrame::Integer(-2));

        let db = shared.data.read_db(0);
        assert!(!db.data.contains_key(b"expired" as &[u8]));
        assert_eq!(shared.stats.total_commands_processed(), 7);
    }

    #[tokio::test]
    async fn inline_ping_and_echo() {
        let (mut client, server_task) = setup_client_server().await;

        client.write_all(b"PING\r\n").await.expect("write ping");
        let ping = read_reply(&mut client).await;
        assert_eq!(ping, b"+PONG\r\n");

        client.write_all(b"ECHO hi\r\n").await.expect("write echo");
        let echo = read_reply(&mut client).await;
        assert_eq!(echo, b"$2\r\nhi\r\n");

        client.write_all(b"QUIT\r\n").await.expect("write quit");
        let quit = read_reply(&mut client).await;
        assert_eq!(quit, b"+OK\r\n");

        let mut eof = [0u8; 1];
        let n = client.read(&mut eof).await.expect("read eof");
        assert_eq!(n, 0);

        server_task.await.expect("server task complete");
    }

    #[tokio::test]
    async fn m1_set_get_exists_del_select() {
        let (mut client, server_task) = setup_client_server().await;

        client
            .write_all(b"*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n")
            .await
            .expect("write set");
        assert_eq!(read_reply(&mut client).await, b"+OK\r\n");

        client
            .write_all(b"*2\r\n$3\r\nGET\r\n$3\r\nfoo\r\n")
            .await
            .expect("write get");
        assert_eq!(read_reply(&mut client).await, b"$3\r\nbar\r\n");

        client
            .write_all(b"*2\r\n$6\r\nSELECT\r\n$1\r\n1\r\n")
            .await
            .expect("write select");
        assert_eq!(read_reply(&mut client).await, b"+OK\r\n");

        client
            .write_all(b"*2\r\n$3\r\nGET\r\n$3\r\nfoo\r\n")
            .await
            .expect("write get db1");
        assert_eq!(read_reply(&mut client).await, b"$-1\r\n");

        client.write_all(b"QUIT\r\n").await.expect("write quit");
        let _ = read_reply(&mut client).await;

        server_task.await.expect("server task complete");
    }

    #[tokio::test]
    async fn pipelined_commands_return_batched_replies() {
        let (mut client, server_task) = setup_client_server().await;

        client
            .write_all(b"PING\r\nECHO hi\r\nQUIT\r\n")
            .await
            .expect("write pipelined commands");

        let expected = b"+PONG\r\n$2\r\nhi\r\n+OK\r\n";
        let reply = read_exact_reply(&mut client, expected.len()).await;
        assert_eq!(reply, expected);

        let mut eof = [0u8; 1];
        let n = client.read(&mut eof).await.expect("read eof");
        assert_eq!(n, 0);

        server_task.await.expect("server task complete");
    }

    #[tokio::test]
    async fn pipelined_lock_free_readonly_batch_keeps_stats_in_sync() {
        let (mut client, shared, server_task) =
            setup_client_server_with_shared(ClientIoLimits::default()).await;

        client
            .write_all(b"PING\r\nECHO hi\r\nDBSIZE\r\n")
            .await
            .expect("write readonly batch");

        let expected = b"+PONG\r\n$2\r\nhi\r\n:0\r\n";
        let reply = read_exact_reply(&mut client, expected.len()).await;
        assert_eq!(reply, expected);

        client.write_all(b"QUIT\r\n").await.expect("write quit");
        let quit = read_reply(&mut client).await;
        assert_eq!(quit, b"+OK\r\n");

        server_task.await.expect("server task complete");

        let server = shared.meta.lock().await;
        assert_eq!(
            shared.stats.total_commands_processed(),
            server.stats.total_commands_processed()
        );
        assert_eq!(shared.stats.total_commands_processed(), 4);
    }

    #[tokio::test]
    async fn fast_path_commands_still_queue_inside_multi() {
        let (mut client, server_task) = setup_client_server().await;

        client
            .write_all(b"MULTI\r\nDBSIZE\r\nEXEC\r\n")
            .await
            .expect("write multi dbsize exec");
        let reply = read_exact_reply(&mut client, b"+OK\r\n+QUEUED\r\n*1\r\n:0\r\n".len()).await;
        assert_eq!(reply, b"+OK\r\n+QUEUED\r\n*1\r\n:0\r\n");

        client.write_all(b"QUIT\r\n").await.expect("write quit");
        let quit = read_reply(&mut client).await;
        assert_eq!(quit, b"+OK\r\n");

        server_task.await.expect("server task complete");
    }

    #[tokio::test]
    async fn monitor_receives_lock_free_dbsize_commands() {
        const CONNECTIONS: usize = 2;

        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind listener");
        let addr = listener.local_addr().expect("local addr");
        let shared = Arc::new(SharedState::new(ServerState::with_default_dbs()));
        let shared_for_accept = Arc::clone(&shared);

        let accept_task = tokio::spawn(async move {
            let persistence = Arc::new(
                PersistenceRuntime::from_config(&crate::config::ServerConfig::default())
                    .expect("persistence runtime"),
            );
            let mut tasks = Vec::with_capacity(CONNECTIONS);

            for _ in 0..CONNECTIONS {
                let (socket, _) = listener.accept().await.expect("accept");
                let shared = Arc::clone(&shared_for_accept);
                let persistence = Arc::clone(&persistence);
                tasks.push(tokio::spawn(async move {
                    handle_client_with_limits(
                        socket,
                        shared,
                        persistence,
                        ClientIoLimits::default(),
                    )
                    .await
                    .expect("handle client");
                }));
            }

            for task in tasks {
                task.await.expect("join client task");
            }
        });

        let mut monitor_client = TcpStream::connect(addr).await.expect("connect monitor");
        let mut command_client = TcpStream::connect(addr)
            .await
            .expect("connect command client");

        monitor_client
            .write_all(b"MONITOR\r\n")
            .await
            .expect("enable monitor");
        assert_eq!(read_reply(&mut monitor_client).await, b"+OK\r\n");

        command_client
            .write_all(b"DBSIZE\r\n")
            .await
            .expect("write dbsize");
        assert_eq!(read_reply(&mut command_client).await, b":0\r\n");

        let monitor_line = read_reply(&mut monitor_client).await;
        let monitor_text = String::from_utf8_lossy(&monitor_line);
        assert!(monitor_text.starts_with('+'));
        assert!(monitor_text.contains("\"DBSIZE\""), "{monitor_text}");

        drop(command_client);

        monitor_client
            .write_all(b"QUIT\r\n")
            .await
            .expect("quit monitor client");
        assert_eq!(read_reply(&mut monitor_client).await, b"+OK\r\n");

        accept_task.await.expect("accept task complete");
    }

    #[tokio::test]
    async fn exists_fast_path_keeps_keyspace_stats_in_sync() {
        let (mut client, shared, server_task) =
            setup_client_server_with_shared(ClientIoLimits::default()).await;
        {
            let mut db = shared.data.write_db(0);
            db.data.insert(
                Bytes::from_static(b"live"),
                StoredValue::string(Bytes::from_static(b"ok"), None),
            );
        }

        client
            .write_all(b"EXISTS live missing\r\n")
            .await
            .expect("write exists");
        assert_eq!(read_reply(&mut client).await, b":1\r\n");

        client.write_all(b"QUIT\r\n").await.expect("write quit");
        assert_eq!(read_reply(&mut client).await, b"+OK\r\n");

        server_task.await.expect("server task complete");

        let server = shared.meta.lock().await;
        assert_eq!(shared.stats.keyspace_hits(), 1);
        assert_eq!(shared.stats.keyspace_misses(), 1);
        assert_eq!(server.stats.keyspace_hits(), 1);
        assert_eq!(server.stats.keyspace_misses(), 1);
        assert_eq!(
            shared.stats.total_commands_processed(),
            server.stats.total_commands_processed()
        );
    }

    #[tokio::test]
    async fn pipelined_lock_free_string_reads_keep_stats_in_sync() {
        let (mut client, shared, server_task) =
            setup_client_server_with_shared(ClientIoLimits::default()).await;
        {
            let mut db = shared.data.write_db(0);
            db.data.insert(
                Bytes::from_static(b"alpha"),
                StoredValue::string(Bytes::from_static(b"value"), None),
            );
        }

        client
            .write_all(b"GET alpha\r\nSTRLEN alpha\r\nMGET alpha missing\r\n")
            .await
            .expect("write readonly string batch");

        let expected = b"$5\r\nvalue\r\n:5\r\n*2\r\n$5\r\nvalue\r\n$-1\r\n";
        let reply = read_exact_reply(&mut client, expected.len()).await;
        assert_eq!(reply, expected);

        client.write_all(b"QUIT\r\n").await.expect("write quit");
        assert_eq!(read_reply(&mut client).await, b"+OK\r\n");

        server_task.await.expect("server task complete");

        let server = shared.meta.lock().await;
        assert_eq!(shared.stats.keyspace_hits(), 1);
        assert_eq!(shared.stats.keyspace_misses(), 0);
        assert_eq!(server.stats.keyspace_hits(), 1);
        assert_eq!(server.stats.keyspace_misses(), 0);
        assert_eq!(
            shared.stats.total_commands_processed(),
            server.stats.total_commands_processed()
        );
        assert_eq!(shared.stats.total_commands_processed(), 4);
    }

    #[tokio::test]
    async fn pipelined_lock_free_string_ranges_work() {
        let (mut client, shared, server_task) =
            setup_client_server_with_shared(ClientIoLimits::default()).await;
        {
            let mut db = shared.data.write_db(0);
            db.data.insert(
                Bytes::from_static(b"alpha"),
                StoredValue::string(Bytes::from_static(b"value"), None),
            );
        }

        client
            .write_all(b"GETRANGE alpha 1 3\r\nSUBSTR alpha -2 -1\r\n")
            .await
            .expect("write readonly range batch");

        let expected = b"$3\r\nalu\r\n$2\r\nue\r\n";
        let reply = read_exact_reply(&mut client, expected.len()).await;
        assert_eq!(reply, expected);

        client.write_all(b"QUIT\r\n").await.expect("write quit");
        assert_eq!(read_reply(&mut client).await, b"+OK\r\n");

        server_task.await.expect("server task complete");

        let server = shared.meta.lock().await;
        assert_eq!(
            shared.stats.total_commands_processed(),
            server.stats.total_commands_processed()
        );
        assert_eq!(shared.stats.total_commands_processed(), 3);
    }

    #[tokio::test]
    async fn pipelined_lock_free_hash_set_and_zset_reads_work() {
        let (mut client, shared, server_task) =
            setup_client_server_with_shared(ClientIoLimits::default()).await;
        {
            let mut hash = HashMap::new();
            hash.insert(
                Bytes::from_static(b"live"),
                HashFieldEntry::new(Bytes::from_static(b"payload")),
            );
            let mut set = HashSet::new();
            set.insert(Bytes::from_static(b"a"));
            set.insert(Bytes::from_static(b"b"));
            let mut zset = SortedSet::default();
            assert!(zset.insert(Bytes::from_static(b"one"), 1.0));
            assert!(zset.insert(Bytes::from_static(b"two"), 2.5));

            let mut db = shared.data.write_db(0);
            db.data
                .insert(Bytes::from_static(b"hash"), StoredValue::hash(hash, None));
            db.data
                .insert(Bytes::from_static(b"set"), StoredValue::set(set, None));
            db.data.insert(
                Bytes::from_static(b"zset"),
                StoredValue::sorted_set(zset, None),
            );
        }

        client
            .write_all(
                b"HGET hash live\r\nHGETALL hash\r\nHKEYS hash\r\nHVALS hash\r\nHLEN hash\r\nSISMEMBER set b\r\nSMISMEMBER set a missing\r\nZSCORE zset two\r\n",
            )
            .await
            .expect("write readonly collection batch");

        let expected = b"$7\r\npayload\r\n*2\r\n$4\r\nlive\r\n$7\r\npayload\r\n*1\r\n$4\r\nlive\r\n*1\r\n$7\r\npayload\r\n:1\r\n:1\r\n*2\r\n:1\r\n:0\r\n$3\r\n2.5\r\n";
        let reply = read_exact_reply(&mut client, expected.len()).await;
        assert_eq!(reply, expected);

        client.write_all(b"QUIT\r\n").await.expect("write quit");
        assert_eq!(read_reply(&mut client).await, b"+OK\r\n");

        server_task.await.expect("server task complete");

        let server = shared.meta.lock().await;
        assert_eq!(
            shared.stats.total_commands_processed(),
            server.stats.total_commands_processed()
        );
        assert_eq!(shared.stats.total_commands_processed(), 9);
    }

    #[tokio::test]
    async fn pipelined_lock_free_list_reads_work() {
        let (mut client, shared, server_task) =
            setup_client_server_with_shared(ClientIoLimits::default()).await;
        {
            let mut db = shared.data.write_db(0);
            db.data.insert(
                Bytes::from_static(b"list"),
                StoredValue::list(
                    VecDeque::from(vec![
                        Bytes::from_static(b"zero"),
                        Bytes::from_static(b"one"),
                        Bytes::from_static(b"two"),
                    ]),
                    None,
                ),
            );
        }

        client
            .write_all(b"LLEN list\r\nLINDEX list -1\r\nLRANGE list 0 1\r\n")
            .await
            .expect("write readonly list batch");

        let expected = b":3\r\n$3\r\ntwo\r\n*2\r\n$4\r\nzero\r\n$3\r\none\r\n";
        let reply = read_exact_reply(&mut client, expected.len()).await;
        assert_eq!(reply, expected);

        client.write_all(b"QUIT\r\n").await.expect("write quit");
        assert_eq!(read_reply(&mut client).await, b"+OK\r\n");

        server_task.await.expect("server task complete");

        let server = shared.meta.lock().await;
        assert_eq!(
            shared.stats.total_commands_processed(),
            server.stats.total_commands_processed()
        );
        assert_eq!(shared.stats.total_commands_processed(), 4);
    }

    #[tokio::test]
    async fn readonly_batch_supports_non_default_authenticated_user() {
        let shared = Arc::new(SharedState::new(ServerState::with_default_dbs()));
        {
            let mut server = shared.meta.lock().await;
            let alice = server
                .acl
                .get_or_create_user_mut(&Bytes::from_static(b"alice"));
            *alice = AclUser {
                enabled: true,
                nopass: false,
                passwords: HashSet::new(),
                allow_all_commands: true,
                allowed_categories: HashSet::new(),
            };
        }
        {
            let mut db = shared.data.write_db(0);
            db.data.insert(
                Bytes::from_static(b"key"),
                StoredValue::string(Bytes::from_static(b"value"), None),
            );
        }

        let mut client = ClientState::new(17);
        client.authenticate_as(Bytes::from_static(b"alice"));

        let outcomes = try_run_readonly_batch(
            vec![
                RespFrame::Array(vec![
                    RespFrame::BulkString(Some(Bytes::from_static(b"GET"))),
                    RespFrame::BulkString(Some(Bytes::from_static(b"key"))),
                ]),
                RespFrame::Array(vec![
                    RespFrame::BulkString(Some(Bytes::from_static(b"STRLEN"))),
                    RespFrame::BulkString(Some(Bytes::from_static(b"key"))),
                ]),
            ],
            &shared,
            &mut client,
        )
        .await
        .expect("readonly batch should execute for authenticated alice");

        assert_eq!(outcomes.len(), 2);
        assert_eq!(
            outcomes[0].response,
            RespFrame::BulkString(Some(Bytes::from_static(b"value")))
        );
        assert_eq!(outcomes[1].response, RespFrame::Integer(5));

        let server = shared.meta.lock().await;
        assert_eq!(shared.stats.total_commands_processed(), 2);
        assert_eq!(server.stats.total_commands_processed(), 2);
    }

    #[tokio::test]
    async fn readonly_batch_accepts_supported_readonly_commands() {
        let shared = Arc::new(SharedState::new(ServerState::with_default_dbs()));
        {
            let mut server = shared.meta.lock().await;
            let alice = server
                .acl
                .get_or_create_user_mut(&Bytes::from_static(b"alice"));
            *alice = AclUser {
                enabled: true,
                nopass: false,
                passwords: HashSet::new(),
                allow_all_commands: true,
                allowed_categories: HashSet::new(),
            };
        }
        {
            let mut hash = HashMap::new();
            hash.insert(
                Bytes::from_static(b"field"),
                HashFieldEntry::new(Bytes::from_static(b"payload")),
            );

            let mut zset = SortedSet::default();
            assert!(zset.insert(Bytes::from_static(b"one"), 1.0));
            assert!(zset.insert(Bytes::from_static(b"two"), 2.0));
            assert!(zset.insert(Bytes::from_static(b"three"), 3.0));

            let mut db = shared.data.write_db(0);
            db.data
                .insert(Bytes::from_static(b"hash"), StoredValue::hash(hash, None));
            db.data.insert(
                Bytes::from_static(b"zset"),
                StoredValue::sorted_set(zset, None),
            );
            db.data.insert(
                Bytes::from_static(b"key"),
                StoredValue::string(Bytes::from_static(b"value"), None),
            );
        }

        let mut client = ClientState::new(18);
        client.authenticate_as(Bytes::from_static(b"alice"));

        let outcomes = try_run_readonly_batch(
            vec![
                RespFrame::Array(vec![
                    RespFrame::BulkString(Some(Bytes::from_static(b"HSTRLEN"))),
                    RespFrame::BulkString(Some(Bytes::from_static(b"hash"))),
                    RespFrame::BulkString(Some(Bytes::from_static(b"field"))),
                ]),
                RespFrame::Array(vec![
                    RespFrame::BulkString(Some(Bytes::from_static(b"ZRANGE"))),
                    RespFrame::BulkString(Some(Bytes::from_static(b"zset"))),
                    RespFrame::BulkString(Some(Bytes::from_static(b"0"))),
                    RespFrame::BulkString(Some(Bytes::from_static(b"-1"))),
                ]),
                RespFrame::Array(vec![
                    RespFrame::BulkString(Some(Bytes::from_static(b"ZREVRANGE"))),
                    RespFrame::BulkString(Some(Bytes::from_static(b"zset"))),
                    RespFrame::BulkString(Some(Bytes::from_static(b"0"))),
                    RespFrame::BulkString(Some(Bytes::from_static(b"1"))),
                    RespFrame::BulkString(Some(Bytes::from_static(b"WITHSCORES"))),
                ]),
                RespFrame::Array(vec![
                    RespFrame::BulkString(Some(Bytes::from_static(b"ZRANGEBYSCORE"))),
                    RespFrame::BulkString(Some(Bytes::from_static(b"zset"))),
                    RespFrame::BulkString(Some(Bytes::from_static(b"2"))),
                    RespFrame::BulkString(Some(Bytes::from_static(b"3"))),
                ]),
                RespFrame::Array(vec![
                    RespFrame::BulkString(Some(Bytes::from_static(b"TTL"))),
                    RespFrame::BulkString(Some(Bytes::from_static(b"key"))),
                ]),
            ],
            &shared,
            &mut client,
        )
        .await
        .expect("readonly batch should accept non-fast readonly commands");

        assert_eq!(outcomes.len(), 5);
        assert_eq!(outcomes[0].response, RespFrame::Integer(7));
        assert_eq!(
            outcomes[1].response,
            RespFrame::Array(vec![
                RespFrame::BulkString(Some(Bytes::from_static(b"one"))),
                RespFrame::BulkString(Some(Bytes::from_static(b"two"))),
                RespFrame::BulkString(Some(Bytes::from_static(b"three"))),
            ])
        );
        assert_eq!(
            outcomes[2].response,
            RespFrame::Array(vec![
                RespFrame::BulkString(Some(Bytes::from_static(b"three"))),
                RespFrame::BulkString(Some(Bytes::from_static(b"3"))),
                RespFrame::BulkString(Some(Bytes::from_static(b"two"))),
                RespFrame::BulkString(Some(Bytes::from_static(b"2"))),
            ])
        );
        assert_eq!(
            outcomes[3].response,
            RespFrame::Array(vec![
                RespFrame::BulkString(Some(Bytes::from_static(b"two"))),
                RespFrame::BulkString(Some(Bytes::from_static(b"three"))),
            ])
        );
        assert_eq!(outcomes[4].response, RespFrame::Integer(-1));

        let server = shared.meta.lock().await;
        assert_eq!(shared.stats.total_commands_processed(), 5);
        assert_eq!(server.stats.total_commands_processed(), 5);
    }

    #[tokio::test]
    async fn pipelined_lock_free_bitmap_hash_and_zset_rank_reads_work() {
        let (mut client, shared, server_task) =
            setup_client_server_with_shared(ClientIoLimits::default()).await;
        {
            let mut hash = HashMap::new();
            hash.insert(
                Bytes::from_static(b"field"),
                HashFieldEntry::new(Bytes::from_static(b"payload")),
            );
            let mut zset = SortedSet::default();
            assert!(zset.insert(Bytes::from_static(b"alpha"), 1.0));
            assert!(zset.insert(Bytes::from_static(b"beta"), 2.0));
            assert!(zset.insert(Bytes::from_static(b"gamma"), 3.0));

            let mut db = shared.data.write_db(0);
            db.data
                .insert(Bytes::from_static(b"hash"), StoredValue::hash(hash, None));
            db.data.insert(
                Bytes::from_static(b"bits"),
                StoredValue::string(Bytes::from_static(b"A"), None),
            );
            db.data.insert(
                Bytes::from_static(b"zset"),
                StoredValue::sorted_set(zset, None),
            );
        }

        client
            .write_all(
                b"HSTRLEN hash field\r\nGETBIT bits 1\r\nZMSCORE zset beta missing\r\nZCOUNT zset 1 2\r\nZRANK zset beta\r\nZREVRANK zset beta\r\n",
            )
            .await
            .expect("write readonly bitmap/hash/zset batch");

        let expected = b":7\r\n:1\r\n*2\r\n$1\r\n2\r\n$-1\r\n:2\r\n:1\r\n:1\r\n";
        let reply = read_exact_reply(&mut client, expected.len()).await;
        assert_eq!(reply, expected);

        client.write_all(b"QUIT\r\n").await.expect("write quit");
        assert_eq!(read_reply(&mut client).await, b"+OK\r\n");

        server_task.await.expect("server task complete");

        let server = shared.meta.lock().await;
        assert_eq!(
            shared.stats.total_commands_processed(),
            server.stats.total_commands_processed()
        );
        assert_eq!(shared.stats.total_commands_processed(), 7);
    }

    #[tokio::test]
    async fn pipelined_lock_free_bitcount_reads_work() {
        let (mut client, shared, server_task) =
            setup_client_server_with_shared(ClientIoLimits::default()).await;
        {
            let mut db = shared.data.write_db(0);
            db.data.insert(
                Bytes::from_static(b"bits"),
                StoredValue::string(Bytes::from_static(b"AB"), None),
            );
        }

        client
            .write_all(b"BITCOUNT bits\r\nBITCOUNT bits -1 -1\r\nBITCOUNT bits 0 3 BIT\r\n")
            .await
            .expect("write readonly bitcount batch");

        let expected = b":4\r\n:2\r\n:1\r\n";
        let reply = read_exact_reply(&mut client, expected.len()).await;
        assert_eq!(reply, expected);

        client.write_all(b"QUIT\r\n").await.expect("write quit");
        assert_eq!(read_reply(&mut client).await, b"+OK\r\n");

        server_task.await.expect("server task complete");

        let server = shared.meta.lock().await;
        assert_eq!(
            shared.stats.total_commands_processed(),
            server.stats.total_commands_processed()
        );
        assert_eq!(shared.stats.total_commands_processed(), 4);
    }

    #[tokio::test]
    async fn pipelined_lock_free_zrange_reads_work() {
        let (mut client, shared, server_task) =
            setup_client_server_with_shared(ClientIoLimits::default()).await;
        {
            let mut zset = SortedSet::default();
            assert!(zset.insert(Bytes::from_static(b"alpha"), 1.0));
            assert!(zset.insert(Bytes::from_static(b"beta"), 1.0));
            assert!(zset.insert(Bytes::from_static(b"gamma"), 2.0));

            let mut db = shared.data.write_db(0);
            db.data.insert(
                Bytes::from_static(b"zset"),
                StoredValue::sorted_set(zset, None),
            );
        }

        client
            .write_all(
                b"ZRANGE zset 0 -1\r\nZRANGE zset 0 1 WITHSCORES\r\nZRANGE zset 2 1 BYSCORE REV LIMIT 0 2\r\nZREVRANGE zset 0 1 WITHSCORES\r\nZRANGEBYSCORE zset 1 2 WITHSCORES LIMIT 1 2\r\n",
            )
            .await
            .expect("write readonly zrange batch");

        let expected = b"*3\r\n$5\r\nalpha\r\n$4\r\nbeta\r\n$5\r\ngamma\r\n*4\r\n$5\r\nalpha\r\n$1\r\n1\r\n$4\r\nbeta\r\n$1\r\n1\r\n*2\r\n$5\r\ngamma\r\n$4\r\nbeta\r\n*4\r\n$5\r\ngamma\r\n$1\r\n2\r\n$4\r\nbeta\r\n$1\r\n1\r\n*4\r\n$4\r\nbeta\r\n$1\r\n1\r\n$5\r\ngamma\r\n$1\r\n2\r\n";
        let reply = read_exact_reply(&mut client, expected.len()).await;
        assert_eq!(reply, expected);

        client.write_all(b"QUIT\r\n").await.expect("write quit");
        assert_eq!(read_reply(&mut client).await, b"+OK\r\n");

        server_task.await.expect("server task complete");

        let server = shared.meta.lock().await;
        assert_eq!(
            shared.stats.total_commands_processed(),
            server.stats.total_commands_processed()
        );
        assert_eq!(shared.stats.total_commands_processed(), 6);
    }

    #[tokio::test]
    async fn pipelined_lock_free_ttl_family_reads_work() {
        let (mut client, shared, server_task) =
            setup_client_server_with_shared(ClientIoLimits::default()).await;
        let expire_at_ms = ratatosk_core::time::now_ms().saturating_add(10_000);
        {
            let mut db = shared.data.write_db(0);
            db.data.insert(
                Bytes::from_static(b"expiring"),
                StoredValue::string(Bytes::from_static(b"value"), Some(expire_at_ms)),
            );
            db.data.insert(
                Bytes::from_static(b"persistent"),
                StoredValue::string(Bytes::from_static(b"value"), None),
            );
        }

        client
            .write_all(
                b"EXPIRETIME expiring\r\nPEXPIRETIME expiring\r\nTTL persistent\r\nPTTL missing\r\n",
            )
            .await
            .expect("write readonly ttl batch");

        let expected = format!(
            ":{}\r\n:{}\r\n:-1\r\n:-2\r\n",
            expire_at_ms / 1000,
            expire_at_ms
        );
        let reply = read_exact_reply(&mut client, expected.len()).await;
        assert_eq!(reply, expected.as_bytes());

        client.write_all(b"QUIT\r\n").await.expect("write quit");
        assert_eq!(read_reply(&mut client).await, b"+OK\r\n");

        server_task.await.expect("server task complete");

        let server = shared.meta.lock().await;
        assert_eq!(
            shared.stats.total_commands_processed(),
            server.stats.total_commands_processed()
        );
        assert_eq!(shared.stats.total_commands_processed(), 5);
    }

    #[tokio::test]
    async fn oversized_response_disconnects_client() {
        let limits = ClientIoLimits {
            output_buffer_limit_bytes: 256,
            client_read_timeout_sec: 0,
        };
        let (mut client, server_task) = setup_client_server_with_limits(limits).await;

        let payload = "x".repeat(1024);
        let command = format!("ECHO {payload}\r\n");
        client
            .write_all(command.as_bytes())
            .await
            .expect("write oversized echo");

        let reply = read_reply(&mut client).await;
        assert_eq!(reply, b"-ERR output buffer limit exceeded\r\n");

        let mut eof = [0u8; 1];
        let n = client.read(&mut eof).await.expect("read eof");
        assert_eq!(n, 0);

        server_task.await.expect("server task complete");
    }

    #[tokio::test]
    async fn peer_close_after_ping_cleans_up_connection_state() {
        let (mut client, shared, server_task) =
            setup_client_server_with_shared(ClientIoLimits::default()).await;

        client.write_all(b"PING\r\n").await.expect("write ping");
        assert_eq!(read_reply(&mut client).await, b"+PONG\r\n");

        drop(client);

        timeout(Duration::from_secs(1), server_task)
            .await
            .expect("server task timeout")
            .expect("server task complete");

        assert_eq!(shared.stats.connected_clients(), 0);
        assert_eq!(shared.stats.total_connections_received(), 1);

        let server = shared.meta.lock().await;
        assert_eq!(server.stats.connected_clients(), 0);
        assert_eq!(server.stats.total_connections_received(), 1);
        assert_eq!(server.connected_client_snapshots(), 0);
        assert_eq!(server.blocked_clients(), 0);
        assert_eq!(server.tracking_clients(), 0);
        assert_eq!(server.monitor_client_count(), 0);
        assert!(server.client_snapshot(1).is_none());
        assert!(server.pubsub.client_channels(1).is_empty());
        assert!(server.pubsub.client_shard_channels(1).is_empty());
        assert!(server.pubsub.client_patterns(1).is_empty());
    }

    #[tokio::test]
    async fn peer_close_after_subscribe_cleans_up_pubsub_state() {
        let (mut client, shared, server_task) =
            setup_client_server_with_shared(ClientIoLimits::default()).await;

        client
            .write_all(b"*2\r\n$9\r\nSUBSCRIBE\r\n$4\r\nnews\r\n")
            .await
            .expect("subscribe");
        let subscribe_reply = read_reply(&mut client).await;
        assert!(
            subscribe_reply
                .windows(b"subscribe".len())
                .any(|window| window == b"subscribe")
        );

        drop(client);

        timeout(Duration::from_secs(1), server_task)
            .await
            .expect("server task timeout")
            .expect("server task complete");

        let server = shared.meta.lock().await;
        assert_eq!(server.connected_client_snapshots(), 0);
        assert!(server.client_snapshot(1).is_none());
        assert!(server.pubsub.client_channels(1).is_empty());
        assert_eq!(server.pubsub.numsub(&[Bytes::from("news")])[0].1, 0);
    }

    #[tokio::test]
    async fn repeated_peer_closes_do_not_leave_connected_clients_behind() {
        const CONNECTIONS: usize = 8;

        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind listener");
        let addr = listener.local_addr().expect("local addr");
        let shared = Arc::new(SharedState::new(ServerState::with_default_dbs()));
        let shared_for_accept = Arc::clone(&shared);

        let accept_task = tokio::spawn(async move {
            let persistence = Arc::new(
                PersistenceRuntime::from_config(&crate::config::ServerConfig::default())
                    .expect("persistence runtime"),
            );
            let mut tasks = Vec::with_capacity(CONNECTIONS);

            for _ in 0..CONNECTIONS {
                let (socket, _) = listener.accept().await.expect("accept");
                let shared = Arc::clone(&shared_for_accept);
                let persistence = Arc::clone(&persistence);
                tasks.push(tokio::spawn(async move {
                    handle_client_with_limits(
                        socket,
                        shared,
                        persistence,
                        ClientIoLimits::default(),
                    )
                    .await
                    .expect("handle client");
                }));
            }

            for task in tasks {
                task.await.expect("join client task");
            }
        });

        for _ in 0..CONNECTIONS {
            let mut client = TcpStream::connect(addr).await.expect("connect client");
            client.write_all(b"PING\r\n").await.expect("write ping");
            assert_eq!(read_reply(&mut client).await, b"+PONG\r\n");
            drop(client);
        }

        timeout(Duration::from_secs(2), accept_task)
            .await
            .expect("accept task timeout")
            .expect("accept task complete");

        assert_eq!(shared.stats.connected_clients(), 0);
        assert_eq!(
            shared.stats.total_connections_received(),
            CONNECTIONS as u64
        );

        let server = shared.meta.lock().await;
        assert_eq!(server.stats.connected_clients(), 0);
        assert_eq!(
            server.stats.total_connections_received(),
            CONNECTIONS as u64
        );
        assert_eq!(server.connected_client_snapshots(), 0);
    }

    #[tokio::test]
    async fn info_stats_reports_total_connections_received_from_runtime_stats() {
        let (mut client, server_task) = setup_client_server().await;

        client
            .write_all(b"INFO stats\r\n")
            .await
            .expect("write info stats");
        let info = read_reply(&mut client).await;
        let info_text = String::from_utf8_lossy(&info);
        assert!(
            info_text.contains("total_connections_received:1"),
            "{info_text}"
        );

        client.write_all(b"QUIT\r\n").await.expect("quit");
        let _ = read_reply(&mut client).await;
        server_task.await.expect("server task complete");
    }

    #[tokio::test]
    async fn runtime_stats_keep_atomic_and_meta_counters_in_sync() {
        let (mut client, shared, server_task) =
            setup_client_server_with_shared(ClientIoLimits::default()).await;

        client
            .write_all(b"PING\r\nECHO hi\r\nQUIT\r\n")
            .await
            .expect("write commands");

        let expected = b"+PONG\r\n$2\r\nhi\r\n+OK\r\n";
        let reply = read_exact_reply(&mut client, expected.len()).await;
        assert_eq!(reply, expected);

        server_task.await.expect("server task complete");

        let server = shared.meta.lock().await;
        assert_eq!(
            shared.stats.total_commands_processed(),
            server.stats.total_commands_processed()
        );
        assert_eq!(
            shared.stats.total_net_input_bytes(),
            server.stats.total_net_input_bytes()
        );
        assert_eq!(
            shared.stats.total_net_output_bytes(),
            server.stats.total_net_output_bytes()
        );
        assert_eq!(shared.stats.total_commands_processed(), 3);
        assert!(shared.stats.total_net_input_bytes() > 0);
        assert!(shared.stats.total_net_output_bytes() > 0);
    }

    #[tokio::test]
    async fn config_set_query_buffer_limit_applies_to_current_connection() {
        let (mut client, shared, server_task) =
            setup_client_server_with_shared(ClientIoLimits::default()).await;

        client
            .write_all(b"CONFIG SET query-buffer-limit 1024\r\n")
            .await
            .expect("set query buffer limit");
        assert_eq!(read_reply(&mut client).await, b"+OK\r\n");
        assert_eq!(shared.config_cache.load().query_buffer_limit(), 1024);

        let payload = "x".repeat(1100);
        let command = format!("ECHO {payload}\r\n");
        client
            .write_all(command.as_bytes())
            .await
            .expect("write oversized query");

        let reply = read_reply(&mut client).await;
        assert_eq!(reply, b"-ERR query buffer limit exceeded\r\n");

        let mut eof = [0u8; 1];
        let n = client.read(&mut eof).await.expect("read eof");
        assert_eq!(n, 0);

        server_task.await.expect("server task complete");
    }

    #[tokio::test]
    async fn config_set_hz_round_trips_and_updates_config_cache() {
        let (mut client, shared, server_task) =
            setup_client_server_with_shared(ClientIoLimits::default()).await;

        client
            .write_all(b"CONFIG SET hz 25\r\n")
            .await
            .expect("set hz");
        assert_eq!(read_reply(&mut client).await, b"+OK\r\n");
        assert_eq!(shared.config_cache.load().hz(), 25);

        client
            .write_all(b"CONFIG GET hz\r\n")
            .await
            .expect("get hz");
        assert_eq!(
            read_reply(&mut client).await,
            b"*2\r\n$2\r\nhz\r\n$2\r\n25\r\n"
        );

        client.write_all(b"QUIT\r\n").await.expect("quit");
        let _ = read_reply(&mut client).await;
        server_task.await.expect("server task complete");
    }

    #[tokio::test]
    async fn pubsub_cross_client_fanout() {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind listener");
        let addr = listener.local_addr().expect("local addr");
        let shared = Arc::new(SharedState::new(ServerState::with_default_dbs()));

        let shared_for_accept = Arc::clone(&shared);
        let accept_task = tokio::spawn(async move {
            let (sock_a, _) = listener.accept().await.expect("accept a");
            let (sock_b, _) = listener.accept().await.expect("accept b");

            let shared_a = Arc::clone(&shared_for_accept);
            let task_a = tokio::spawn(async move {
                handle_client(sock_a, shared_a).await.expect("handle a");
            });
            let shared_b = Arc::clone(&shared_for_accept);
            let task_b = tokio::spawn(async move {
                handle_client(sock_b, shared_b).await.expect("handle b");
            });

            task_a.await.expect("join a");
            task_b.await.expect("join b");
        });

        let mut sub = TcpStream::connect(addr).await.expect("connect sub");
        let mut pubc = TcpStream::connect(addr).await.expect("connect pub");

        sub.write_all(b"*2\r\n$9\r\nSUBSCRIBE\r\n$4\r\nnews\r\n")
            .await
            .expect("subscribe");
        let sub_ack = read_reply(&mut sub).await;
        assert!(
            sub_ack
                .windows(b"subscribe".len())
                .any(|w| w == b"subscribe")
        );

        pubc.write_all(b"*3\r\n$7\r\nPUBLISH\r\n$4\r\nnews\r\n$5\r\nhello\r\n")
            .await
            .expect("publish");
        let pub_reply = read_reply(&mut pubc).await;
        assert_eq!(pub_reply, b":1\r\n");

        let pushed = read_reply(&mut sub).await;
        assert!(pushed.windows(b"message".len()).any(|w| w == b"message"));
        assert!(pushed.windows(b"news".len()).any(|w| w == b"news"));
        assert!(pushed.windows(b"hello".len()).any(|w| w == b"hello"));

        sub.write_all(b"*2\r\n$10\r\nSSUBSCRIBE\r\n$6\r\nshard1\r\n")
            .await
            .expect("ssubscribe");
        let ssub_ack = read_reply(&mut sub).await;
        assert!(
            ssub_ack
                .windows(b"ssubscribe".len())
                .any(|w| w == b"ssubscribe")
        );

        pubc.write_all(b"*3\r\n$8\r\nSPUBLISH\r\n$6\r\nshard1\r\n$5\r\nworld\r\n")
            .await
            .expect("spublish");
        let spub_reply = read_reply(&mut pubc).await;
        assert_eq!(spub_reply, b":1\r\n");

        let shard_pushed = read_reply(&mut sub).await;
        assert!(
            shard_pushed
                .windows(b"smessage".len())
                .any(|w| w == b"smessage")
        );
        assert!(
            shard_pushed
                .windows(b"shard1".len())
                .any(|w| w == b"shard1")
        );
        assert!(shard_pushed.windows(b"world".len()).any(|w| w == b"world"));

        sub.write_all(b"QUIT\r\n").await.expect("quit sub");
        let _ = read_reply(&mut sub).await;
        pubc.write_all(b"QUIT\r\n").await.expect("quit pub");
        let _ = read_reply(&mut pubc).await;

        accept_task.await.expect("accept task join");
    }

    #[tokio::test]
    async fn client_tracking_pushes_invalidation_cross_client() {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind listener");
        let addr = listener.local_addr().expect("local addr");
        let shared = Arc::new(SharedState::new(ServerState::with_default_dbs()));

        let shared_for_accept = Arc::clone(&shared);
        let accept_task = tokio::spawn(async move {
            let (sock_tracker, _) = listener.accept().await.expect("accept tracker");
            let (sock_writer, _) = listener.accept().await.expect("accept writer");

            let shared_tracker = Arc::clone(&shared_for_accept);
            let task_tracker = tokio::spawn(async move {
                handle_client(sock_tracker, shared_tracker)
                    .await
                    .expect("handle tracker");
            });
            let shared_writer = Arc::clone(&shared_for_accept);
            let task_writer = tokio::spawn(async move {
                handle_client(sock_writer, shared_writer)
                    .await
                    .expect("handle writer");
            });

            task_tracker.await.expect("join tracker");
            task_writer.await.expect("join writer");
        });

        let mut tracker = TcpStream::connect(addr).await.expect("connect tracker");
        let mut writer = TcpStream::connect(addr).await.expect("connect writer");

        writer
            .write_all(b"*3\r\n$3\r\nSET\r\n$7\r\ntracked\r\n$2\r\nv1\r\n")
            .await
            .expect("seed tracked key");
        assert_eq!(read_reply(&mut writer).await, b"+OK\r\n");

        tracker
            .write_all(b"*3\r\n$6\r\nCLIENT\r\n$8\r\nTRACKING\r\n$2\r\nON\r\n")
            .await
            .expect("enable tracking");
        assert_eq!(read_reply(&mut tracker).await, b"+OK\r\n");

        tracker
            .write_all(b"*2\r\n$3\r\nGET\r\n$7\r\ntracked\r\n")
            .await
            .expect("read tracked key");
        assert_eq!(read_reply(&mut tracker).await, b"$2\r\nv1\r\n");

        writer
            .write_all(b"*3\r\n$3\r\nSET\r\n$7\r\ntracked\r\n$2\r\nv2\r\n")
            .await
            .expect("update tracked key");
        assert_eq!(read_reply(&mut writer).await, b"+OK\r\n");

        let invalidation = read_reply(&mut tracker).await;
        assert!(
            invalidation
                .windows(b"invalidate".len())
                .any(|w| w == b"invalidate"),
            "expected invalidate push, got {:?}",
            String::from_utf8_lossy(&invalidation)
        );
        assert!(
            invalidation
                .windows(b"tracked".len())
                .any(|w| w == b"tracked"),
            "expected tracked key in invalidate push, got {:?}",
            String::from_utf8_lossy(&invalidation)
        );

        tracker.write_all(b"QUIT\r\n").await.expect("quit tracker");
        let _ = read_reply(&mut tracker).await;
        writer.write_all(b"QUIT\r\n").await.expect("quit writer");
        let _ = read_reply(&mut writer).await;

        accept_task.await.expect("accept task join");
    }

    #[tokio::test]
    async fn client_tracking_bcast_prefix_pushes_matching_invalidation() {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind listener");
        let addr = listener.local_addr().expect("local addr");
        let shared = Arc::new(SharedState::new(ServerState::with_default_dbs()));

        let shared_for_accept = Arc::clone(&shared);
        let accept_task = tokio::spawn(async move {
            let (sock_tracker, _) = listener.accept().await.expect("accept tracker");
            let (sock_writer, _) = listener.accept().await.expect("accept writer");

            let shared_tracker = Arc::clone(&shared_for_accept);
            let task_tracker = tokio::spawn(async move {
                handle_client(sock_tracker, shared_tracker)
                    .await
                    .expect("handle tracker");
            });
            let shared_writer = Arc::clone(&shared_for_accept);
            let task_writer = tokio::spawn(async move {
                handle_client(sock_writer, shared_writer)
                    .await
                    .expect("handle writer");
            });

            task_tracker.await.expect("join tracker");
            task_writer.await.expect("join writer");
        });

        let mut tracker = TcpStream::connect(addr).await.expect("connect tracker");
        let mut writer = TcpStream::connect(addr).await.expect("connect writer");

        tracker
            .write_all(
                b"*6\r\n$6\r\nCLIENT\r\n$8\r\nTRACKING\r\n$2\r\nON\r\n$5\r\nBCAST\r\n$6\r\nPREFIX\r\n$5\r\nuser:\r\n",
            )
            .await
            .expect("enable bcast prefix tracking");
        assert_eq!(read_reply(&mut tracker).await, b"+OK\r\n");

        writer
            .write_all(b"*3\r\n$3\r\nSET\r\n$6\r\nother:\r\n$2\r\nv1\r\n")
            .await
            .expect("write non matching key");
        assert_eq!(read_reply(&mut writer).await, b"+OK\r\n");

        writer
            .write_all(b"*3\r\n$3\r\nSET\r\n$6\r\nuser:1\r\n$2\r\nv2\r\n")
            .await
            .expect("write matching key");
        assert_eq!(read_reply(&mut writer).await, b"+OK\r\n");

        let invalidation = read_reply(&mut tracker).await;
        assert!(
            invalidation
                .windows(b"invalidate".len())
                .any(|w| w == b"invalidate"),
            "expected invalidate push, got {:?}",
            String::from_utf8_lossy(&invalidation)
        );
        assert!(
            invalidation
                .windows(b"user:1".len())
                .any(|w| w == b"user:1"),
            "expected matching key in invalidate push, got {:?}",
            String::from_utf8_lossy(&invalidation)
        );
        assert!(
            !invalidation
                .windows(b"other:".len())
                .any(|w| w == b"other:"),
            "unexpected non-matching key in invalidate push, got {:?}",
            String::from_utf8_lossy(&invalidation)
        );

        tracker.write_all(b"QUIT\r\n").await.expect("quit tracker");
        let _ = read_reply(&mut tracker).await;
        writer.write_all(b"QUIT\r\n").await.expect("quit writer");
        let _ = read_reply(&mut writer).await;

        accept_task.await.expect("accept task join");
    }

    #[tokio::test]
    async fn client_tracking_redirect_pushes_invalidation_to_target() {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind listener");
        let addr = listener.local_addr().expect("local addr");
        let shared = Arc::new(SharedState::new(ServerState::with_default_dbs()));

        let shared_for_accept = Arc::clone(&shared);
        let accept_task = tokio::spawn(async move {
            let (sock_tracker, _) = listener.accept().await.expect("accept tracker");
            let (sock_target, _) = listener.accept().await.expect("accept target");
            let (sock_writer, _) = listener.accept().await.expect("accept writer");

            let shared_tracker = Arc::clone(&shared_for_accept);
            let task_tracker = tokio::spawn(async move {
                handle_client(sock_tracker, shared_tracker)
                    .await
                    .expect("handle tracker");
            });
            let shared_target = Arc::clone(&shared_for_accept);
            let task_target = tokio::spawn(async move {
                handle_client(sock_target, shared_target)
                    .await
                    .expect("handle target");
            });
            let shared_writer = Arc::clone(&shared_for_accept);
            let task_writer = tokio::spawn(async move {
                handle_client(sock_writer, shared_writer)
                    .await
                    .expect("handle writer");
            });

            task_tracker.await.expect("join tracker");
            task_target.await.expect("join target");
            task_writer.await.expect("join writer");
        });

        let mut tracker = TcpStream::connect(addr).await.expect("connect tracker");
        let mut target = TcpStream::connect(addr).await.expect("connect target");
        let mut writer = TcpStream::connect(addr).await.expect("connect writer");

        target
            .write_all(b"*2\r\n$6\r\nCLIENT\r\n$2\r\nID\r\n")
            .await
            .expect("request target id");
        let target_id = parse_integer_reply(&read_reply(&mut target).await);

        let tracking_command = format!(
            "*5\r\n$6\r\nCLIENT\r\n$8\r\nTRACKING\r\n$2\r\nON\r\n$8\r\nREDIRECT\r\n${}\r\n{}\r\n",
            target_id.to_string().len(),
            target_id
        );
        tracker
            .write_all(tracking_command.as_bytes())
            .await
            .expect("enable redirect tracking");
        assert_eq!(read_reply(&mut tracker).await, b"+OK\r\n");

        writer
            .write_all(b"*3\r\n$3\r\nSET\r\n$7\r\ntracked\r\n$2\r\nv1\r\n")
            .await
            .expect("seed tracked key");
        assert_eq!(read_reply(&mut writer).await, b"+OK\r\n");

        tracker
            .write_all(b"*2\r\n$3\r\nGET\r\n$7\r\ntracked\r\n")
            .await
            .expect("read tracked key");
        assert_eq!(read_reply(&mut tracker).await, b"$2\r\nv1\r\n");

        writer
            .write_all(b"*3\r\n$3\r\nSET\r\n$7\r\ntracked\r\n$2\r\nv2\r\n")
            .await
            .expect("update tracked key");
        assert_eq!(read_reply(&mut writer).await, b"+OK\r\n");

        let invalidation = read_reply(&mut target).await;
        assert!(
            invalidation
                .windows(b"invalidate".len())
                .any(|w| w == b"invalidate"),
            "expected invalidate push, got {:?}",
            String::from_utf8_lossy(&invalidation)
        );
        assert!(
            invalidation
                .windows(b"tracked".len())
                .any(|w| w == b"tracked"),
            "expected tracked key in invalidate push, got {:?}",
            String::from_utf8_lossy(&invalidation)
        );

        tracker.write_all(b"QUIT\r\n").await.expect("quit tracker");
        let _ = read_reply(&mut tracker).await;
        target.write_all(b"QUIT\r\n").await.expect("quit target");
        let _ = read_reply(&mut target).await;
        writer.write_all(b"QUIT\r\n").await.expect("quit writer");
        let _ = read_reply(&mut writer).await;

        accept_task.await.expect("accept task join");
    }

    #[tokio::test]
    async fn client_tracking_redirect_disconnect_marks_broken_redirect_and_falls_back() {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind listener");
        let addr = listener.local_addr().expect("local addr");
        let shared = Arc::new(SharedState::new(ServerState::with_default_dbs()));

        let shared_for_accept = Arc::clone(&shared);
        let accept_task = tokio::spawn(async move {
            let (sock_tracker, _) = listener.accept().await.expect("accept tracker");
            let (sock_target, _) = listener.accept().await.expect("accept target");
            let (sock_writer, _) = listener.accept().await.expect("accept writer");

            let shared_tracker = Arc::clone(&shared_for_accept);
            let task_tracker = tokio::spawn(async move {
                handle_client(sock_tracker, shared_tracker)
                    .await
                    .expect("handle tracker");
            });
            let shared_target = Arc::clone(&shared_for_accept);
            let task_target = tokio::spawn(async move {
                handle_client(sock_target, shared_target)
                    .await
                    .expect("handle target");
            });
            let shared_writer = Arc::clone(&shared_for_accept);
            let task_writer = tokio::spawn(async move {
                handle_client(sock_writer, shared_writer)
                    .await
                    .expect("handle writer");
            });

            task_tracker.await.expect("join tracker");
            task_target.await.expect("join target");
            task_writer.await.expect("join writer");
        });

        let mut tracker = TcpStream::connect(addr).await.expect("connect tracker");
        let mut target = TcpStream::connect(addr).await.expect("connect target");
        let mut writer = TcpStream::connect(addr).await.expect("connect writer");

        tracker
            .write_all(b"*2\r\n$5\r\nHELLO\r\n$1\r\n3\r\n")
            .await
            .expect("switch tracker to resp3");
        let hello = read_reply(&mut tracker).await;
        assert!(
            hello.windows(b"proto".len()).any(|w| w == b"proto"),
            "expected HELLO map, got {:?}",
            String::from_utf8_lossy(&hello)
        );

        target
            .write_all(b"*2\r\n$6\r\nCLIENT\r\n$2\r\nID\r\n")
            .await
            .expect("request target id");
        let target_id = parse_integer_reply(&read_reply(&mut target).await);

        let tracking_command = format!(
            "*5\r\n$6\r\nCLIENT\r\n$8\r\nTRACKING\r\n$2\r\nON\r\n$8\r\nREDIRECT\r\n${}\r\n{}\r\n",
            target_id.to_string().len(),
            target_id
        );
        tracker
            .write_all(tracking_command.as_bytes())
            .await
            .expect("enable redirect tracking");
        assert_eq!(read_reply(&mut tracker).await, b"+OK\r\n");

        writer
            .write_all(b"*3\r\n$3\r\nSET\r\n$7\r\ntracked\r\n$2\r\nv1\r\n")
            .await
            .expect("seed tracked key");
        assert_eq!(read_reply(&mut writer).await, b"+OK\r\n");

        tracker
            .write_all(b"*2\r\n$3\r\nGET\r\n$7\r\ntracked\r\n")
            .await
            .expect("read tracked key");
        assert_eq!(read_reply(&mut tracker).await, b"$2\r\nv1\r\n");

        target.write_all(b"QUIT\r\n").await.expect("quit target");
        let _ = read_reply(&mut target).await;
        let target_id_text = target_id.to_string();
        let broken_redirect = read_reply(&mut tracker).await;
        assert_eq!(broken_redirect.first().copied(), Some(b'>'));
        assert!(
            broken_redirect
                .windows(b"tracking-redir-broken".len())
                .any(|w| w == b"tracking-redir-broken"),
            "expected tracking-redir-broken push, got {:?}",
            String::from_utf8_lossy(&broken_redirect)
        );
        assert!(
            broken_redirect
                .windows(target_id_text.len())
                .any(|w| w == target_id_text.as_bytes()),
            "expected broken redirect id in push, got {:?}",
            String::from_utf8_lossy(&broken_redirect)
        );

        tracker
            .write_all(b"*2\r\n$6\r\nCLIENT\r\n$8\r\nGETREDIR\r\n")
            .await
            .expect("request active redirect");
        assert_eq!(
            read_reply(&mut tracker).await,
            format!(":{}\r\n", target_id).as_bytes()
        );

        writer
            .write_all(b"*3\r\n$3\r\nSET\r\n$7\r\ntracked\r\n$2\r\nv2\r\n")
            .await
            .expect("update tracked key");
        assert_eq!(read_reply(&mut writer).await, b"+OK\r\n");

        let invalidation = read_reply(&mut tracker).await;
        assert!(
            invalidation
                .windows(b"invalidate".len())
                .any(|w| w == b"invalidate"),
            "expected invalidate push, got {:?}",
            String::from_utf8_lossy(&invalidation)
        );
        assert!(
            invalidation
                .windows(b"tracked".len())
                .any(|w| w == b"tracked"),
            "expected tracked key in invalidate push, got {:?}",
            String::from_utf8_lossy(&invalidation)
        );

        tracker.write_all(b"QUIT\r\n").await.expect("quit tracker");
        let _ = read_reply(&mut tracker).await;
        writer.write_all(b"QUIT\r\n").await.expect("quit writer");
        let _ = read_reply(&mut writer).await;

        accept_task.await.expect("accept task join");
    }

    #[tokio::test]
    async fn client_registry_reports_blocked_and_tracking_clients() {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind listener");
        let addr = listener.local_addr().expect("local addr");
        let shared = Arc::new(SharedState::new(ServerState::with_default_dbs()));

        let shared_for_accept = Arc::clone(&shared);
        let accept_task = tokio::spawn(async move {
            let (sock_blocked, _) = listener.accept().await.expect("accept blocked");
            let (sock_observer, _) = listener.accept().await.expect("accept observer");

            let shared_blocked = Arc::clone(&shared_for_accept);
            let task_blocked = tokio::spawn(async move {
                handle_client(sock_blocked, shared_blocked)
                    .await
                    .expect("handle blocked");
            });
            let shared_observer = Arc::clone(&shared_for_accept);
            let task_observer = tokio::spawn(async move {
                handle_client(sock_observer, shared_observer)
                    .await
                    .expect("handle observer");
            });

            task_blocked.await.expect("join blocked");
            task_observer.await.expect("join observer");
        });

        let mut blocked = TcpStream::connect(addr).await.expect("connect blocked");
        let mut observer = TcpStream::connect(addr).await.expect("connect observer");

        observer
            .write_all(b"*3\r\n$6\r\nCLIENT\r\n$8\r\nTRACKING\r\n$2\r\nON\r\n")
            .await
            .expect("enable tracking");
        assert_eq!(read_reply(&mut observer).await, b"+OK\r\n");

        blocked
            .write_all(b"*3\r\n$5\r\nBLPOP\r\n$7\r\nmissing\r\n$1\r\n1\r\n")
            .await
            .expect("start blocking pop");
        tokio::time::sleep(Duration::from_millis(50)).await;

        observer
            .write_all(b"*2\r\n$4\r\nINFO\r\n$7\r\nclients\r\n")
            .await
            .expect("info clients");
        let info = read_reply(&mut observer).await;
        let info_text = String::from_utf8_lossy(&info);
        assert!(info_text.contains("connected_clients:2"), "{info_text}");
        assert!(info_text.contains("blocked_clients:1"), "{info_text}");
        assert!(info_text.contains("tracking_clients:1"), "{info_text}");

        observer
            .write_all(b"*2\r\n$6\r\nCLIENT\r\n$4\r\nLIST\r\n")
            .await
            .expect("client list");
        let list = read_reply(&mut observer).await;
        let list_text = String::from_utf8_lossy(&list);
        assert!(list_text.contains("flags=Nt"), "{list_text}");
        assert!(list_text.contains("flags=Nb"), "{list_text}");

        let blocked_reply = read_reply(&mut blocked).await;
        assert!(
            blocked_reply == b"*-1\r\n" || blocked_reply == b"$-1\r\n",
            "unexpected BLPOP timeout reply: {:?}",
            String::from_utf8_lossy(&blocked_reply)
        );

        blocked.write_all(b"QUIT\r\n").await.expect("quit blocked");
        let _ = read_reply(&mut blocked).await;
        observer
            .write_all(b"QUIT\r\n")
            .await
            .expect("quit observer");
        let _ = read_reply(&mut observer).await;

        accept_task.await.expect("accept task join");
    }

    #[tokio::test]
    async fn blocking_list_pop_wakes_on_matching_write() {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind listener");
        let addr = listener.local_addr().expect("local addr");
        let shared = Arc::new(SharedState::new(ServerState::with_default_dbs()));

        let shared_for_accept = Arc::clone(&shared);
        let accept_task = tokio::spawn(async move {
            let (sock_blocked, _) = listener.accept().await.expect("accept blocked");
            let (sock_writer, _) = listener.accept().await.expect("accept writer");

            let shared_blocked = Arc::clone(&shared_for_accept);
            let task_blocked = tokio::spawn(async move {
                handle_client(sock_blocked, shared_blocked)
                    .await
                    .expect("handle blocked");
            });
            let shared_writer = Arc::clone(&shared_for_accept);
            let task_writer = tokio::spawn(async move {
                handle_client(sock_writer, shared_writer)
                    .await
                    .expect("handle writer");
            });

            task_blocked.await.expect("join blocked");
            task_writer.await.expect("join writer");
        });

        let mut blocked = TcpStream::connect(addr).await.expect("connect blocked");
        let mut writer = TcpStream::connect(addr).await.expect("connect writer");

        blocked
            .write_all(b"*3\r\n$5\r\nBLPOP\r\n$7\r\nwake-me\r\n$1\r\n5\r\n")
            .await
            .expect("start blocking pop");
        tokio::time::sleep(Duration::from_millis(50)).await;

        writer
            .write_all(b"*3\r\n$5\r\nLPUSH\r\n$7\r\nwake-me\r\n$7\r\npayload\r\n")
            .await
            .expect("push payload");
        assert_eq!(read_reply(&mut writer).await, b":1\r\n");

        let reply = read_reply(&mut blocked).await;
        let reply_text = String::from_utf8_lossy(&reply);
        assert!(reply_text.contains("wake-me"), "{reply_text}");
        assert!(reply_text.contains("payload"), "{reply_text}");

        blocked.write_all(b"QUIT\r\n").await.expect("quit blocked");
        let _ = read_reply(&mut blocked).await;
        writer.write_all(b"QUIT\r\n").await.expect("quit writer");
        let _ = read_reply(&mut writer).await;

        accept_task.await.expect("accept task join");
    }

    #[tokio::test]
    async fn client_read_timeout_disconnects_idle_client() {
        let limits = ClientIoLimits {
            output_buffer_limit_bytes: DEFAULT_OUTPUT_BUFFER_LIMIT_BYTES,
            client_read_timeout_sec: 1,
        };
        let (mut client, server_task) = setup_client_server_with_limits(limits).await;

        client.write_all(b"PING\r\n").await.expect("write ping");
        let ping = read_reply(&mut client).await;
        assert_eq!(ping, b"+PONG\r\n");

        tokio::time::sleep(Duration::from_secs(2)).await;

        let mut buf = [0u8; 1];
        let n = client.read(&mut buf).await.expect("read after timeout");
        assert_eq!(n, 0, "expected EOF after timeout");

        server_task.await.expect("server task complete");
    }

    #[tokio::test]
    async fn write_commands_append_to_aof() {
        let dir = tempfile::tempdir().expect("tmpdir");
        let config = crate::config::ServerConfig {
            dir: dir.path().to_path_buf(),
            appendonly: true,
            appendfsync: "always".to_string(),
            ..crate::config::ServerConfig::default()
        };
        let persistence = Arc::new(PersistenceRuntime::from_config(&config).expect("runtime"));

        let (mut client, server_task) =
            setup_client_server_with_persistence(ClientIoLimits::default(), persistence).await;

        client
            .write_all(b"*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n")
            .await
            .expect("write set");
        assert_eq!(read_reply(&mut client).await, b"+OK\r\n");

        client.write_all(b"QUIT\r\n").await.expect("quit");
        let _ = read_reply(&mut client).await;
        server_task.await.expect("server task complete");

        // Find the actual AOF file written — may be the legacy filename or a
        // manifest-managed incremental file depending on bootstrap layout.
        let aof_content = find_aof_content(dir.path());
        assert!(
            aof_content.contains("SET"),
            "AOF should contain SET command"
        );
        assert!(aof_content.contains("foo"), "AOF should contain key 'foo'");
        assert!(
            aof_content.contains("bar"),
            "AOF should contain value 'bar'"
        );
    }
}
