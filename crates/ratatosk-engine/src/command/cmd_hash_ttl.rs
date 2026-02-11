use bytes::Bytes;
use hashbrown::HashMap;

use ratatosk_resp::frame::RespFrame;

use crate::keyspace::{HashFieldEntry, ServerState, StoredValue, purge_expired_key};

use super::{
    ClientState, CommandOutcome, err, now_ms, parse_i64, wrong_arity, wrong_type_response,
};

// ---------------------------------------------------------------------------
// Field-level expire condition (NX / XX / GT / LT)
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy)]
enum FieldExpireCondition {
    Nx,
    Xx,
    Gt,
    Lt,
}

fn parse_field_expire_condition(raw: &Bytes) -> Option<FieldExpireCondition> {
    if raw.eq_ignore_ascii_case(b"NX") {
        Some(FieldExpireCondition::Nx)
    } else if raw.eq_ignore_ascii_case(b"XX") {
        Some(FieldExpireCondition::Xx)
    } else if raw.eq_ignore_ascii_case(b"GT") {
        Some(FieldExpireCondition::Gt)
    } else if raw.eq_ignore_ascii_case(b"LT") {
        Some(FieldExpireCondition::Lt)
    } else {
        None
    }
}

// ---------------------------------------------------------------------------
// Time conversion modes
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy)]
enum FieldExpireMode {
    RelativeSec,
    RelativeMs,
    AbsoluteSec,
    AbsoluteMs,
}

#[derive(Debug, Clone, Copy)]
enum FieldTtlMode {
    Seconds,
    Milliseconds,
    AbsoluteSeconds,
    AbsoluteMilliseconds,
}

// ---------------------------------------------------------------------------
// FIELDS numfields field... suffix parser
// ---------------------------------------------------------------------------

fn parse_fields_suffix(args: &[Bytes], start_idx: usize) -> Result<&[Bytes], RespFrame> {
    let Some(fields_kw) = args.get(start_idx) else {
        return Err(err("ERR syntax error"));
    };
    if !fields_kw.eq_ignore_ascii_case(b"FIELDS") {
        return Err(err("ERR syntax error"));
    }
    let Some(numfields_raw) = args.get(start_idx + 1) else {
        return Err(err("ERR syntax error"));
    };
    let Some(numfields) = parse_i64(numfields_raw) else {
        return Err(err("ERR value is not an integer or out of range"));
    };
    if numfields <= 0 {
        return Err(err("ERR numfields must be positive"));
    }
    let n = numfields as usize;
    let fields_start = start_idx + 2;
    let fields_end = fields_start + n;
    if fields_end != args.len() {
        return Err(err("ERR syntax error"));
    }
    Ok(&args[fields_start..fields_end])
}

// ---------------------------------------------------------------------------
// Per-field expiry setting logic
// ---------------------------------------------------------------------------

fn set_field_expiry(
    hash: &mut HashMap<Bytes, HashFieldEntry>,
    field: &Bytes,
    expire_at_ms: i64,
    condition: Option<FieldExpireCondition>,
    now: i64,
) -> i64 {
    let Some(entry) = hash.get_mut(field) else {
        return -2;
    };
    if entry.is_expired(now) {
        return -2;
    }
    match condition {
        None => {
            entry.expire_at_ms = Some(expire_at_ms);
            1
        }
        Some(FieldExpireCondition::Nx) => {
            if entry.expire_at_ms.is_none() {
                entry.expire_at_ms = Some(expire_at_ms);
                1
            } else {
                0
            }
        }
        Some(FieldExpireCondition::Xx) => {
            if entry.expire_at_ms.is_some() {
                entry.expire_at_ms = Some(expire_at_ms);
                1
            } else {
                0
            }
        }
        Some(FieldExpireCondition::Gt) => {
            if entry.expire_at_ms.is_none_or(|cur| expire_at_ms > cur) {
                entry.expire_at_ms = Some(expire_at_ms);
                1
            } else {
                0
            }
        }
        Some(FieldExpireCondition::Lt) => {
            if entry.expire_at_ms.is_none_or(|cur| expire_at_ms < cur) {
                entry.expire_at_ms = Some(expire_at_ms);
                1
            } else {
                0
            }
        }
    }
}

/// Convert a raw time argument to an absolute millisecond timestamp.
fn to_absolute_ms(raw: i64, mode: FieldExpireMode, now: i64) -> Option<i64> {
    match mode {
        FieldExpireMode::RelativeSec => {
            let delta = raw.checked_mul(1000)?;
            now.checked_add(delta)
        }
        FieldExpireMode::RelativeMs => now.checked_add(raw),
        FieldExpireMode::AbsoluteSec => raw.checked_mul(1000),
        FieldExpireMode::AbsoluteMs => Some(raw),
    }
}

// ---------------------------------------------------------------------------
// HEXPIRE / HEXPIREAT / HPEXPIRE / HPEXPIREAT — shared implementation
// ---------------------------------------------------------------------------

/// Common implementation for HEXPIRE, HEXPIREAT, HPEXPIRE, HPEXPIREAT.
///
/// Argument layout: key time [NX|XX|GT|LT] FIELDS numfields field [field ...]
fn cmd_hexpire_common(
    args: &[Bytes],
    server: &mut ServerState,
    client: &ClientState,
    mode: FieldExpireMode,
    command_name: &str,
) -> CommandOutcome {
    // Minimum: key time FIELDS numfields field  =>  5 args (after stripping command name)
    if args.len() < 4 {
        return wrong_arity(command_name);
    }

    let key = &args[0];

    let Some(time_raw) = parse_i64(&args[1]) else {
        return CommandOutcome::reply(err("ERR value is not an integer or out of range"));
    };

    // For relative modes, the value must be positive.
    // For absolute modes, the value must be positive as well (0 or negative is invalid).
    let invalid_msg = format!("ERR invalid expire time in '{command_name}' command");
    match mode {
        FieldExpireMode::RelativeSec | FieldExpireMode::RelativeMs => {
            if time_raw <= 0 {
                return CommandOutcome::reply(err(&invalid_msg));
            }
        }
        FieldExpireMode::AbsoluteSec | FieldExpireMode::AbsoluteMs => {
            if time_raw <= 0 {
                return CommandOutcome::reply(err(&invalid_msg));
            }
        }
    }

    // Parse optional condition (NX / XX / GT / LT)
    let mut cursor = 2usize;
    let condition = if let Some(maybe_cond) = args.get(cursor) {
        if let Some(cond) = parse_field_expire_condition(maybe_cond) {
            cursor += 1;
            Some(cond)
        } else {
            // Not a condition token — must be FIELDS keyword
            None
        }
    } else {
        return wrong_arity(command_name);
    };

    // Parse FIELDS numfields field...
    let fields = match parse_fields_suffix(args, cursor) {
        Ok(f) => f,
        Err(resp) => return CommandOutcome::reply(resp),
    };

    let now = now_ms();
    let db = server.db_mut(client.selected_db);
    purge_expired_key(db, key, now);

    // Key does not exist -> all fields report -2
    let Some(stored) = db.get_mut(key) else {
        let results: Vec<RespFrame> = fields.iter().map(|_| RespFrame::Integer(-2)).collect();
        return CommandOutcome::reply(RespFrame::Array(results));
    };
    let Some(hash) = stored.as_hash_mut() else {
        return wrong_type_response();
    };

    let Some(expire_at_ms) = to_absolute_ms(time_raw, mode, now) else {
        return CommandOutcome::reply(err(&invalid_msg));
    };

    let results: Vec<RespFrame> = fields
        .iter()
        .map(|field| {
            RespFrame::Integer(set_field_expiry(hash, field, expire_at_ms, condition, now))
        })
        .collect();

    CommandOutcome::reply(RespFrame::Array(results))
}

// ---------------------------------------------------------------------------
// HEXPIRE
// ---------------------------------------------------------------------------

pub(super) fn cmd_hexpire(
    args: &[Bytes],
    server: &mut ServerState,
    client: &ClientState,
) -> CommandOutcome {
    cmd_hexpire_common(
        args,
        server,
        client,
        FieldExpireMode::RelativeSec,
        "hexpire",
    )
}

// ---------------------------------------------------------------------------
// HEXPIREAT
// ---------------------------------------------------------------------------

pub(super) fn cmd_hexpireat(
    args: &[Bytes],
    server: &mut ServerState,
    client: &ClientState,
) -> CommandOutcome {
    cmd_hexpire_common(
        args,
        server,
        client,
        FieldExpireMode::AbsoluteSec,
        "hexpireat",
    )
}

// ---------------------------------------------------------------------------
// HPEXPIRE
// ---------------------------------------------------------------------------

pub(super) fn cmd_hpexpire(
    args: &[Bytes],
    server: &mut ServerState,
    client: &ClientState,
) -> CommandOutcome {
    cmd_hexpire_common(
        args,
        server,
        client,
        FieldExpireMode::RelativeMs,
        "hpexpire",
    )
}

// ---------------------------------------------------------------------------
// HPEXPIREAT
// ---------------------------------------------------------------------------

pub(super) fn cmd_hpexpireat(
    args: &[Bytes],
    server: &mut ServerState,
    client: &ClientState,
) -> CommandOutcome {
    cmd_hexpire_common(
        args,
        server,
        client,
        FieldExpireMode::AbsoluteMs,
        "hpexpireat",
    )
}

// ---------------------------------------------------------------------------
// HTTL / HPTTL / HEXPIRETIME / HPEXPIRETIME — shared implementation
// ---------------------------------------------------------------------------

fn field_ttl_value(entry: &HashFieldEntry, mode: FieldTtlMode, now: i64) -> i64 {
    if entry.is_expired(now) {
        return -2;
    }
    let Some(expire_at) = entry.expire_at_ms else {
        return -1;
    };
    match mode {
        FieldTtlMode::Seconds => {
            // Remaining TTL in seconds (ceiling division)
            let remaining_ms = expire_at.saturating_sub(now);
            if remaining_ms <= 0 {
                return -2;
            }
            (remaining_ms + 999) / 1000
        }
        FieldTtlMode::Milliseconds => {
            let remaining_ms = expire_at.saturating_sub(now);
            if remaining_ms <= 0 {
                return -2;
            }
            remaining_ms
        }
        FieldTtlMode::AbsoluteSeconds => expire_at / 1000,
        FieldTtlMode::AbsoluteMilliseconds => expire_at,
    }
}

fn cmd_httl_common(
    args: &[Bytes],
    server: &mut ServerState,
    client: &ClientState,
    mode: FieldTtlMode,
    command_name: &str,
) -> CommandOutcome {
    // Minimum: key FIELDS numfields field  =>  4 args
    if args.len() < 3 {
        return wrong_arity(command_name);
    }

    let key = &args[0];

    // Parse FIELDS numfields field...
    let fields = match parse_fields_suffix(args, 1) {
        Ok(f) => f,
        Err(resp) => return CommandOutcome::reply(resp),
    };

    let now = now_ms();
    let db = server.db_mut(client.selected_db);
    purge_expired_key(db, key, now);

    let Some(stored) = db.get(key) else {
        let results: Vec<RespFrame> = fields.iter().map(|_| RespFrame::Integer(-2)).collect();
        return CommandOutcome::reply(RespFrame::Array(results));
    };
    let Some(hash) = stored.as_hash() else {
        return wrong_type_response();
    };

    let results: Vec<RespFrame> = fields
        .iter()
        .map(|field| {
            let Some(entry) = hash.get(field) else {
                return RespFrame::Integer(-2);
            };
            RespFrame::Integer(field_ttl_value(entry, mode, now))
        })
        .collect();

    CommandOutcome::reply(RespFrame::Array(results))
}

// ---------------------------------------------------------------------------
// HTTL
// ---------------------------------------------------------------------------

pub(super) fn cmd_httl(
    args: &[Bytes],
    server: &mut ServerState,
    client: &ClientState,
) -> CommandOutcome {
    cmd_httl_common(args, server, client, FieldTtlMode::Seconds, "httl")
}

// ---------------------------------------------------------------------------
// HPTTL
// ---------------------------------------------------------------------------

pub(super) fn cmd_hpttl(
    args: &[Bytes],
    server: &mut ServerState,
    client: &ClientState,
) -> CommandOutcome {
    cmd_httl_common(args, server, client, FieldTtlMode::Milliseconds, "hpttl")
}

// ---------------------------------------------------------------------------
// HEXPIRETIME
// ---------------------------------------------------------------------------

pub(super) fn cmd_hexpiretime(
    args: &[Bytes],
    server: &mut ServerState,
    client: &ClientState,
) -> CommandOutcome {
    cmd_httl_common(
        args,
        server,
        client,
        FieldTtlMode::AbsoluteSeconds,
        "hexpiretime",
    )
}

// ---------------------------------------------------------------------------
// HPEXPIRETIME
// ---------------------------------------------------------------------------

pub(super) fn cmd_hpexpiretime(
    args: &[Bytes],
    server: &mut ServerState,
    client: &ClientState,
) -> CommandOutcome {
    cmd_httl_common(
        args,
        server,
        client,
        FieldTtlMode::AbsoluteMilliseconds,
        "hpexpiretime",
    )
}

// ---------------------------------------------------------------------------
// HPERSIST
// ---------------------------------------------------------------------------

pub(super) fn cmd_hpersist(
    args: &[Bytes],
    server: &mut ServerState,
    client: &ClientState,
) -> CommandOutcome {
    // Argument layout: key FIELDS numfields field [field ...]
    if args.len() < 3 {
        return wrong_arity("hpersist");
    }

    let key = &args[0];

    let fields = match parse_fields_suffix(args, 1) {
        Ok(f) => f,
        Err(resp) => return CommandOutcome::reply(resp),
    };

    let now = now_ms();
    let db = server.db_mut(client.selected_db);
    purge_expired_key(db, key, now);

    let Some(stored) = db.get_mut(key) else {
        let results: Vec<RespFrame> = fields.iter().map(|_| RespFrame::Integer(-2)).collect();
        return CommandOutcome::reply(RespFrame::Array(results));
    };
    let Some(hash) = stored.as_hash_mut() else {
        return wrong_type_response();
    };

    let results: Vec<RespFrame> = fields
        .iter()
        .map(|field| {
            let Some(entry) = hash.get_mut(field) else {
                return RespFrame::Integer(-2);
            };
            if entry.is_expired(now) {
                return RespFrame::Integer(-2);
            }
            if entry.expire_at_ms.is_some() {
                entry.expire_at_ms = None;
                RespFrame::Integer(1)
            } else {
                RespFrame::Integer(-1)
            }
        })
        .collect();

    CommandOutcome::reply(RespFrame::Array(results))
}

// ---------------------------------------------------------------------------
// HGETDEL
// ---------------------------------------------------------------------------

pub(super) fn cmd_hgetdel(
    args: &[Bytes],
    server: &mut ServerState,
    client: &ClientState,
) -> CommandOutcome {
    // Argument layout: key FIELDS numfields field [field ...]
    if args.len() < 3 {
        return wrong_arity("hgetdel");
    }

    let key = &args[0];

    let fields = match parse_fields_suffix(args, 1) {
        Ok(f) => f,
        Err(resp) => return CommandOutcome::reply(resp),
    };

    let now = now_ms();
    let db = server.db_mut(client.selected_db);
    purge_expired_key(db, key, now);

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
                // Treat expired fields as non-existent, but still remove them
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

    // Remove key if hash becomes empty
    if hash.is_empty() {
        db.remove(key);
    }

    CommandOutcome::reply(RespFrame::Array(results))
}

// ---------------------------------------------------------------------------
// HGETEX
// ---------------------------------------------------------------------------

/// HGETEX key [EX seconds | PX ms | EXAT unix-sec | PXAT unix-ms | PERSIST] FIELDS numfields field...
pub(super) fn cmd_hgetex(
    args: &[Bytes],
    server: &mut ServerState,
    client: &ClientState,
) -> CommandOutcome {
    // Minimum: key FIELDS numfields field  =>  4 args (no TTL option)
    // With option: key EX seconds FIELDS numfields field => 6 args
    // With PERSIST: key PERSIST FIELDS numfields field => 5 args
    if args.len() < 3 {
        return wrong_arity("hgetex");
    }

    let key = &args[0];

    // Parse optional TTL policy before FIELDS keyword
    let mut cursor = 1usize;
    let policy = if let Some(opt) = args.get(cursor) {
        if opt.eq_ignore_ascii_case(b"FIELDS") {
            // No TTL option — go straight to FIELDS
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

    // Parse FIELDS numfields field...
    let fields = match parse_fields_suffix(args, cursor) {
        Ok(f) => f,
        Err(resp) => return CommandOutcome::reply(resp),
    };

    let now = now_ms();
    let db = server.db_mut(client.selected_db);
    purge_expired_key(db, key, now);

    let Some(stored) = db.get_mut(key) else {
        let results: Vec<RespFrame> = fields.iter().map(|_| RespFrame::BulkString(None)).collect();
        return CommandOutcome::reply(RespFrame::Array(results));
    };
    let Some(hash) = stored.as_hash_mut() else {
        return wrong_type_response();
    };

    // Compute absolute expiry once (if needed)
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
            // Apply TTL policy
            match &policy {
                HgetexPolicy::None => {}
                HgetexPolicy::Persist => {
                    entry.expire_at_ms = None;
                }
                HgetexPolicy::Expire(_, _) => {
                    // expire_at_ms is Some here since we computed it above
                    entry.expire_at_ms = expire_at_ms;
                }
            }
            RespFrame::BulkString(Some(value))
        })
        .collect();

    CommandOutcome::reply(RespFrame::Array(results))
}

#[derive(Debug)]
enum HgetexPolicy {
    None,
    Persist,
    Expire(i64, FieldExpireMode),
}

// ---------------------------------------------------------------------------
// HSETEX
// ---------------------------------------------------------------------------

/// HSETEX key seconds numfields field value [field value ...]
pub(super) fn cmd_hsetex(
    args: &[Bytes],
    server: &mut ServerState,
    client: &ClientState,
) -> CommandOutcome {
    // Minimum: key seconds numfields field value  =>  5 args
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

    // Each field needs a field-value pair
    let pairs_start = 3usize;
    let expected_pair_count = n * 2;
    if pairs_start + expected_pair_count != args.len() {
        return wrong_arity("hsetex");
    }

    let now = now_ms();
    let Some(expire_at_ms) = now.checked_add(seconds.saturating_mul(1000)) else {
        return CommandOutcome::reply(err("ERR invalid expire time in 'hsetex' command"));
    };

    let db = server.db_mut(client.selected_db);
    purge_expired_key(db, key, now);

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
