use bytes::Bytes;

use glob_match::glob_match;
use hashbrown::HashSet;
use ratatosk_resp::frame::RespFrame;

use crate::keyspace::{ServerState, StoredValue, purge_expired_key};
use crate::object::now_us;

use super::{
    ClientState, CommandOutcome, err, now_ms, parse_i64, parse_usize, wrong_arity,
    wrong_type_response,
};
use super::{parse_scan_cursor, parse_scan_match_count_options, scan_collect_indexes, scan_reply};

const MAX_SET_POP_COUNT: usize = 100_000;
const MAX_SET_RANDOM_COUNT: usize = 100_000;

pub(super) fn cmd_sadd(
    args: &[Bytes],
    server: &mut ServerState,
    client: &ClientState,
) -> CommandOutcome {
    if args.len() < 2 {
        return wrong_arity("sadd");
    }

    let key = &args[0];
    let now = now_ms();
    let db = server.db_mut(client.selected_db);
    purge_expired_key(db, key, now);

    if !db.contains_key(key) {
        let mut set = HashSet::with_capacity(args.len().saturating_sub(1));
        for member in &args[1..] {
            set.insert(member.clone());
        }
        let added = set.len() as i64;
        db.insert(key.clone(), StoredValue::set(set, None));
        return CommandOutcome::reply(RespFrame::Integer(added));
    }

    let Some(entry) = db.get_mut(key) else {
        return CommandOutcome::reply(RespFrame::Integer(0));
    };
    let Some(set) = entry.as_set_mut() else {
        return wrong_type_response();
    };

    let mut added = 0i64;
    for member in &args[1..] {
        if set.insert(member.clone()) {
            added += 1;
        }
    }

    CommandOutcome::reply(RespFrame::Integer(added))
}

pub(super) fn cmd_srem(
    args: &[Bytes],
    server: &mut ServerState,
    client: &ClientState,
) -> CommandOutcome {
    if args.len() < 2 {
        return wrong_arity("srem");
    }

    let key = &args[0];
    let now = now_ms();
    let db = server.db_mut(client.selected_db);
    purge_expired_key(db, key, now);

    let Some(entry) = db.get_mut(key) else {
        return CommandOutcome::reply(RespFrame::Integer(0));
    };
    let Some(set) = entry.as_set_mut() else {
        return wrong_type_response();
    };

    let mut removed = 0i64;
    for member in &args[1..] {
        if set.remove(member) {
            removed += 1;
        }
    }

    if set.is_empty() {
        db.remove(key);
    }

    CommandOutcome::reply(RespFrame::Integer(removed))
}

pub(super) fn cmd_sismember(
    args: &[Bytes],
    server: &mut ServerState,
    client: &ClientState,
) -> CommandOutcome {
    let [key, member] = args else {
        return wrong_arity("sismember");
    };

    let now = now_ms();
    let db = server.db_mut(client.selected_db);
    purge_expired_key(db, key, now);

    let Some(entry) = db.get(key) else {
        return CommandOutcome::reply(RespFrame::Integer(0));
    };
    let Some(set) = entry.as_set() else {
        return wrong_type_response();
    };

    CommandOutcome::reply(RespFrame::Integer(if set.contains(member) { 1 } else { 0 }))
}

pub(super) fn cmd_smismember(
    args: &[Bytes],
    server: &mut ServerState,
    client: &ClientState,
) -> CommandOutcome {
    if args.len() < 2 {
        return wrong_arity("smismember");
    }

    let key = &args[0];
    let members = &args[1..];

    let now = now_ms();
    let db = server.db_mut(client.selected_db);
    purge_expired_key(db, key, now);

    let Some(entry) = db.get(key) else {
        return CommandOutcome::reply(RespFrame::Array(
            members
                .iter()
                .map(|_| RespFrame::Integer(0))
                .collect::<Vec<_>>(),
        ));
    };
    let Some(set) = entry.as_set() else {
        return wrong_type_response();
    };

    CommandOutcome::reply(RespFrame::Array(
        members
            .iter()
            .map(|member| RespFrame::Integer(if set.contains(member) { 1 } else { 0 }))
            .collect::<Vec<_>>(),
    ))
}

pub(super) fn cmd_smembers(
    args: &[Bytes],
    server: &mut ServerState,
    client: &ClientState,
) -> CommandOutcome {
    let [key] = args else {
        return wrong_arity("smembers");
    };

    let now = now_ms();
    let db = server.db_mut(client.selected_db);
    purge_expired_key(db, key, now);

    let Some(entry) = db.get(key) else {
        return CommandOutcome::reply(RespFrame::Array(vec![]));
    };
    let Some(set) = entry.as_set() else {
        return wrong_type_response();
    };

    CommandOutcome::reply(RespFrame::Array(
        set.iter()
            .map(|member| RespFrame::BulkString(Some(member.clone())))
            .collect::<Vec<_>>(),
    ))
}

pub(super) fn cmd_scard(
    args: &[Bytes],
    server: &mut ServerState,
    client: &ClientState,
) -> CommandOutcome {
    let [key] = args else {
        return wrong_arity("scard");
    };

    let now = now_ms();
    let db = server.db_mut(client.selected_db);
    purge_expired_key(db, key, now);

    let Some(entry) = db.get(key) else {
        return CommandOutcome::reply(RespFrame::Integer(0));
    };
    let Some(set) = entry.as_set() else {
        return wrong_type_response();
    };

    CommandOutcome::reply(RespFrame::Integer(set.len() as i64))
}

pub(super) fn cmd_spop(
    args: &[Bytes],
    server: &mut ServerState,
    client: &ClientState,
) -> CommandOutcome {
    if args.is_empty() || args.len() > 2 {
        return wrong_arity("spop");
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
        if parsed_count > MAX_SET_POP_COUNT {
            return CommandOutcome::reply(err("ERR count is out of range"));
        }
        Some(parsed_count)
    } else {
        None
    };

    if matches!(count, Some(0)) {
        return CommandOutcome::reply(RespFrame::Array(vec![]));
    }

    let now = now_ms();
    let db = server.db_mut(client.selected_db);
    purge_expired_key(db, key, now);

    if !db.contains_key(key) {
        return if count.is_some() {
            CommandOutcome::reply(RespFrame::Array(vec![]))
        } else {
            CommandOutcome::reply(RespFrame::BulkString(None))
        };
    }

    let mut remove_key = false;
    let response = {
        let Some(entry) = db.get_mut(key) else {
            return if count.is_some() {
                CommandOutcome::reply(RespFrame::Array(vec![]))
            } else {
                CommandOutcome::reply(RespFrame::BulkString(None))
            };
        };
        let Some(set) = entry.as_set_mut() else {
            return wrong_type_response();
        };

        match count {
            None => {
                if set.is_empty() {
                    remove_key = true;
                    CommandOutcome::reply(RespFrame::BulkString(None))
                } else {
                    let idx = usize::try_from(now_us()).unwrap_or(0) % set.len();
                    if let Some(member) = set.iter().nth(idx).cloned() {
                        set.remove(&member);
                        if set.is_empty() {
                            remove_key = true;
                        }
                        CommandOutcome::reply(RespFrame::BulkString(Some(member)))
                    } else {
                        remove_key = true;
                        CommandOutcome::reply(RespFrame::BulkString(None))
                    }
                }
            }
            Some(requested) => {
                if requested >= set.len() {
                    let out: Vec<RespFrame> = set
                        .drain()
                        .map(|member| RespFrame::BulkString(Some(member)))
                        .collect();
                    remove_key = true;
                    CommandOutcome::reply(RespFrame::Array(out))
                } else {
                    let start = usize::try_from(now_us()).unwrap_or(0) % set.len();
                    let mut selected = Vec::with_capacity(requested);
                    selected.extend(set.iter().skip(start).take(requested).cloned());
                    if selected.len() < requested {
                        let remaining = requested - selected.len();
                        selected.extend(set.iter().take(remaining).cloned());
                    }

                    let mut out = Vec::with_capacity(selected.len());
                    for member in selected {
                        set.remove(&member);
                        out.push(RespFrame::BulkString(Some(member)));
                    }
                    if set.is_empty() {
                        remove_key = true;
                    }
                    CommandOutcome::reply(RespFrame::Array(out))
                }
            }
        }
    };

    if remove_key {
        db.remove(key);
    }

    response
}

pub(super) fn srandmember_start_index(len: usize) -> usize {
    if len == 0 {
        0
    } else {
        usize::try_from(now_us()).unwrap_or(0) % len
    }
}

pub(super) fn cmd_srandmember(
    args: &[Bytes],
    server: &mut ServerState,
    client: &ClientState,
) -> CommandOutcome {
    if args.is_empty() || args.len() > 2 {
        return wrong_arity("srandmember");
    }

    let key = &args[0];
    let count = if args.len() == 2 {
        let Some(parsed) = parse_i64(&args[1]) else {
            return CommandOutcome::reply(err("ERR value is not an integer or out of range"));
        };
        Some(parsed)
    } else {
        None
    };

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
    let Some(set) = entry.as_set() else {
        return wrong_type_response();
    };

    if set.is_empty() {
        return if count.is_some() {
            CommandOutcome::reply(RespFrame::Array(vec![]))
        } else {
            CommandOutcome::reply(RespFrame::BulkString(None))
        };
    }

    let start = srandmember_start_index(set.len());

    match count {
        None => {
            let Some(member) = set.iter().nth(start).cloned() else {
                return CommandOutcome::reply(RespFrame::BulkString(None));
            };
            CommandOutcome::reply(RespFrame::BulkString(Some(member)))
        }
        Some(raw_count) if raw_count >= 0 => {
            let Ok(requested) = usize::try_from(raw_count) else {
                return CommandOutcome::reply(err("ERR value is not an integer or out of range"));
            };
            if requested > MAX_SET_RANDOM_COUNT {
                return CommandOutcome::reply(err("ERR count is out of range"));
            }
            if requested == 0 {
                return CommandOutcome::reply(RespFrame::Array(vec![]));
            }

            let take = requested.min(set.len());
            let mut out = Vec::with_capacity(take);
            for member in set.iter().skip(start).take(take) {
                out.push(RespFrame::BulkString(Some(member.clone())));
            }

            if out.len() < take {
                let remaining = take - out.len();
                for member in set.iter().take(remaining) {
                    out.push(RespFrame::BulkString(Some(member.clone())));
                }
            }

            CommandOutcome::reply(RespFrame::Array(out))
        }
        Some(raw_count) => {
            let Ok(requested) = usize::try_from(raw_count.saturating_neg()) else {
                return CommandOutcome::reply(err("ERR value is not an integer or out of range"));
            };
            if requested > MAX_SET_RANDOM_COUNT {
                return CommandOutcome::reply(err("ERR count is out of range"));
            }

            let mut iter = set.iter().cycle().skip(start);
            let mut out = Vec::with_capacity(requested);
            for _ in 0..requested {
                let member = iter
                    .next()
                    .expect("set iterator is infinite when the set is non-empty");
                out.push(RespFrame::BulkString(Some(member.clone())));
            }
            CommandOutcome::reply(RespFrame::Array(out))
        }
    }
}

pub(super) fn cmd_sscan(
    args: &[Bytes],
    server: &mut ServerState,
    client: &ClientState,
) -> CommandOutcome {
    let [key, cursor_raw, options @ ..] = args else {
        return wrong_arity("sscan");
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
    let Some(set) = entry.as_set() else {
        return wrong_type_response();
    };

    let mut members = set.iter().cloned().collect::<Vec<_>>();
    members.sort();

    let (next_cursor, matched_indexes) = scan_collect_indexes(&members, cursor, count, |member| {
        if let Some(pattern) = &pattern {
            glob_match(pattern, &String::from_utf8_lossy(member))
        } else {
            true
        }
    });

    let out = matched_indexes
        .into_iter()
        .map(|idx| RespFrame::BulkString(Some(members[idx].clone())))
        .collect::<Vec<_>>();

    scan_reply(next_cursor, out)
}

pub(super) fn cmd_smove(
    args: &[Bytes],
    server: &mut ServerState,
    client: &ClientState,
) -> CommandOutcome {
    let [source, destination, member] = args else {
        return wrong_arity("smove");
    };

    let now = now_ms();
    let db = server.db_mut(client.selected_db);
    purge_expired_key(db, source, now);
    purge_expired_key(db, destination, now);

    if source == destination {
        let Some(entry) = db.get_mut(source) else {
            return CommandOutcome::reply(RespFrame::Integer(0));
        };
        let Some(set) = entry.as_set_mut() else {
            return wrong_type_response();
        };
        return CommandOutcome::reply(RespFrame::Integer(if set.contains(member) { 1 } else { 0 }));
    }

    if let Some(destination_entry) = db.get(destination) {
        if !destination_entry.is_set() {
            return wrong_type_response();
        }
    }

    let moved = {
        let Some(source_entry) = db.get_mut(source) else {
            return CommandOutcome::reply(RespFrame::Integer(0));
        };
        let Some(source_set) = source_entry.as_set_mut() else {
            return wrong_type_response();
        };
        source_set.remove(member)
    };

    if !moved {
        return CommandOutcome::reply(RespFrame::Integer(0));
    }

    let source_empty = db
        .get(source)
        .and_then(|entry| entry.as_set())
        .is_some_and(|set| set.is_empty());
    if source_empty {
        db.remove(source);
    }

    if let Some(destination_entry) = db.get_mut(destination) {
        let Some(destination_set) = destination_entry.as_set_mut() else {
            return wrong_type_response();
        };
        destination_set.insert(member.clone());
    } else {
        let mut destination_set = HashSet::new();
        destination_set.insert(member.clone());
        db.insert(destination.clone(), StoredValue::set(destination_set, None));
    }

    CommandOutcome::reply(RespFrame::Integer(1))
}

pub(super) fn set_result_array(result: HashSet<Bytes>) -> RespFrame {
    RespFrame::Array(
        result
            .into_iter()
            .map(|member| RespFrame::BulkString(Some(member)))
            .collect::<Vec<_>>(),
    )
}

pub(super) fn cmd_sdiff(
    args: &[Bytes],
    server: &mut ServerState,
    client: &ClientState,
) -> CommandOutcome {
    if args.len() < 2 {
        return wrong_arity("sdiff");
    }

    let first_key = &args[0];
    let others = &args[1..];
    let now = now_ms();
    let db = server.db_mut(client.selected_db);
    purge_expired_key(db, first_key, now);

    let Some(first_entry) = db.get(first_key) else {
        return CommandOutcome::reply(RespFrame::Array(vec![]));
    };
    let Some(first_set) = first_entry.as_set() else {
        return wrong_type_response();
    };

    let mut result = first_set.clone();
    for key in others {
        purge_expired_key(db, key, now);
        let Some(entry) = db.get(key) else {
            continue;
        };
        let Some(other_set) = entry.as_set() else {
            return wrong_type_response();
        };
        for member in other_set {
            result.remove(member);
        }
        if result.is_empty() {
            break;
        }
    }

    CommandOutcome::reply(set_result_array(result))
}

pub(super) fn cmd_sinter(
    args: &[Bytes],
    server: &mut ServerState,
    client: &ClientState,
) -> CommandOutcome {
    if args.len() < 2 {
        return wrong_arity("sinter");
    }

    let now = now_ms();
    let db = server.db_mut(client.selected_db);
    for key in args {
        purge_expired_key(db, key, now);
    }

    let mut sets = Vec::with_capacity(args.len());
    for key in args {
        let Some(entry) = db.get(key) else {
            return CommandOutcome::reply(RespFrame::Array(vec![]));
        };
        let Some(set) = entry.as_set() else {
            return wrong_type_response();
        };
        sets.push(set);
    }

    let mut smallest_idx = 0usize;
    let mut smallest_len = usize::MAX;
    for (idx, set) in sets.iter().enumerate() {
        if set.len() < smallest_len {
            smallest_len = set.len();
            smallest_idx = idx;
        }
    }

    if smallest_len == 0 {
        return CommandOutcome::reply(RespFrame::Array(vec![]));
    }

    let base_set = sets.swap_remove(smallest_idx);
    let mut result = Vec::with_capacity(base_set.len());
    for member in base_set {
        if sets.iter().all(|set| set.contains(member)) {
            result.push(RespFrame::BulkString(Some(member.clone())));
        }
    }

    CommandOutcome::reply(RespFrame::Array(result))
}

pub(super) fn cmd_sintercard(
    args: &[Bytes],
    server: &mut ServerState,
    client: &ClientState,
) -> CommandOutcome {
    if args.len() < 2 {
        return wrong_arity("sintercard");
    }

    let Some(numkeys) = parse_usize(&args[0]) else {
        return CommandOutcome::reply(err("ERR value is not an integer or out of range"));
    };
    if numkeys == 0 {
        return CommandOutcome::reply(err("ERR numkeys should be greater than 0"));
    }
    if numkeys > args.len().saturating_sub(1) {
        return CommandOutcome::reply(err(
            "ERR Number of keys can't be greater than number of args",
        ));
    }

    let keys = &args[1..1 + numkeys];
    let mut limit = 0usize;
    let mut idx = 1 + numkeys;
    while idx < args.len() {
        if !args[idx].eq_ignore_ascii_case(b"LIMIT") {
            return CommandOutcome::reply(err("ERR syntax error"));
        }

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
    }

    let now = now_ms();
    let db = server.db_mut(client.selected_db);
    for key in keys {
        purge_expired_key(db, key, now);
    }

    let mut smallest_idx = 0usize;
    let mut smallest_len = usize::MAX;
    for (i, key) in keys.iter().enumerate() {
        let Some(entry) = db.get(key) else {
            return CommandOutcome::reply(RespFrame::Integer(0));
        };
        let Some(set) = entry.as_set() else {
            return wrong_type_response();
        };
        if set.len() < smallest_len {
            smallest_len = set.len();
            smallest_idx = i;
        }
    }

    if smallest_len == 0 {
        return CommandOutcome::reply(RespFrame::Integer(0));
    }

    let Some(base_entry) = db.get(&keys[smallest_idx]) else {
        return CommandOutcome::reply(RespFrame::Integer(0));
    };
    let Some(base_set) = base_entry.as_set() else {
        return wrong_type_response();
    };

    let mut cardinality = 0i64;
    for member in base_set {
        let mut present_in_all = true;
        for (i, key) in keys.iter().enumerate() {
            if i == smallest_idx {
                continue;
            }
            let Some(entry) = db.get(key) else {
                present_in_all = false;
                break;
            };
            let Some(set) = entry.as_set() else {
                return wrong_type_response();
            };
            if !set.contains(member) {
                present_in_all = false;
                break;
            }
        }

        if present_in_all {
            cardinality += 1;
            if limit > 0 && cardinality >= limit as i64 {
                break;
            }
        }
    }

    CommandOutcome::reply(RespFrame::Integer(cardinality))
}

pub(super) fn cmd_sunion(
    args: &[Bytes],
    server: &mut ServerState,
    client: &ClientState,
) -> CommandOutcome {
    if args.len() < 2 {
        return wrong_arity("sunion");
    }

    let now = now_ms();
    let db = server.db_mut(client.selected_db);
    let mut result = HashSet::new();

    for key in args {
        purge_expired_key(db, key, now);
        let Some(entry) = db.get(key) else {
            continue;
        };
        let Some(set) = entry.as_set() else {
            return wrong_type_response();
        };
        result.extend(set.iter().cloned());
    }

    CommandOutcome::reply(set_result_array(result))
}

pub(super) fn cmd_sdiffstore(
    args: &[Bytes],
    server: &mut ServerState,
    client: &ClientState,
) -> CommandOutcome {
    let [destination, source_keys @ ..] = args else {
        return wrong_arity("sdiffstore");
    };
    if source_keys.is_empty() {
        return wrong_arity("sdiffstore");
    }

    let now = now_ms();
    let db = server.db_mut(client.selected_db);
    purge_expired_key(db, destination, now);
    purge_expired_key(db, &source_keys[0], now);

    let mut result = match db.get(&source_keys[0]) {
        None => HashSet::new(),
        Some(entry) => match entry.as_set() {
            Some(set) => set.clone(),
            None => return wrong_type_response(),
        },
    };

    for key in &source_keys[1..] {
        purge_expired_key(db, key, now);
        let Some(entry) = db.get(key) else {
            continue;
        };
        let Some(set) = entry.as_set() else {
            return wrong_type_response();
        };
        for member in set {
            result.remove(member);
        }
        if result.is_empty() {
            break;
        }
    }

    let stored_len = result.len() as i64;
    if result.is_empty() {
        db.remove(destination);
    } else {
        db.insert(destination.clone(), StoredValue::set(result, None));
    }

    CommandOutcome::reply(RespFrame::Integer(stored_len))
}

pub(super) fn cmd_sinterstore(
    args: &[Bytes],
    server: &mut ServerState,
    client: &ClientState,
) -> CommandOutcome {
    let [destination, source_keys @ ..] = args else {
        return wrong_arity("sinterstore");
    };
    if source_keys.is_empty() {
        return wrong_arity("sinterstore");
    }

    let now = now_ms();
    let db = server.db_mut(client.selected_db);
    purge_expired_key(db, destination, now);
    for key in source_keys {
        purge_expired_key(db, key, now);
    }

    let result = {
        let mut sets = Vec::with_capacity(source_keys.len());
        for key in source_keys {
            let Some(entry) = db.get(key) else {
                sets.clear();
                break;
            };
            let Some(set) = entry.as_set() else {
                return wrong_type_response();
            };
            sets.push(set);
        }

        if sets.is_empty() {
            HashSet::new()
        } else {
            let mut smallest_idx = 0usize;
            let mut smallest_len = usize::MAX;
            for (idx, set) in sets.iter().enumerate() {
                if set.len() < smallest_len {
                    smallest_len = set.len();
                    smallest_idx = idx;
                }
            }

            if smallest_len == 0 {
                HashSet::new()
            } else {
                let base_set = sets.swap_remove(smallest_idx);
                let mut result = HashSet::with_capacity(base_set.len());
                for member in base_set {
                    if sets.iter().all(|set| set.contains(member)) {
                        result.insert(member.clone());
                    }
                }
                result
            }
        }
    };

    let stored_len = result.len() as i64;
    if result.is_empty() {
        db.remove(destination);
    } else {
        db.insert(destination.clone(), StoredValue::set(result, None));
    }

    CommandOutcome::reply(RespFrame::Integer(stored_len))
}

pub(super) fn cmd_sunionstore(
    args: &[Bytes],
    server: &mut ServerState,
    client: &ClientState,
) -> CommandOutcome {
    let [destination, source_keys @ ..] = args else {
        return wrong_arity("sunionstore");
    };
    if source_keys.is_empty() {
        return wrong_arity("sunionstore");
    }

    let now = now_ms();
    let db = server.db_mut(client.selected_db);
    purge_expired_key(db, destination, now);
    let mut result = HashSet::new();

    for key in source_keys {
        purge_expired_key(db, key, now);
        let Some(entry) = db.get(key) else {
            continue;
        };
        let Some(set) = entry.as_set() else {
            return wrong_type_response();
        };
        result.extend(set.iter().cloned());
    }

    let stored_len = result.len() as i64;
    if result.is_empty() {
        db.remove(destination);
    } else {
        db.insert(destination.clone(), StoredValue::set(result, None));
    }

    CommandOutcome::reply(RespFrame::Integer(stored_len))
}
