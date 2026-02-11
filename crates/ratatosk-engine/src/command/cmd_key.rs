use std::collections::VecDeque;

use bytes::Bytes;

use crate::expiry::{ExpireCondition, ExpireMode, ExpireTimeMode, TtlMode};
use crate::keyspace::{ServerState, StoredValue, purge_expired_key, purge_expired_keys};
use crate::object::parse_f64;
use glob_match::glob_match;
use ratatosk_resp::frame::RespFrame;

use super::{
    ClientState, CommandOutcome, err, expire_condition_matches, now_ms, parse_expire_condition,
    parse_i64, parse_usize, to_expire_target_ms, to_uppercase_bytes, wrong_arity,
    wrong_type_response,
};

pub(super) fn cmd_persist(
    args: &[Bytes],
    server: &mut ServerState,
    client: &ClientState,
) -> CommandOutcome {
    let [key] = args else {
        return wrong_arity("persist");
    };

    let now = now_ms();
    let db = server.db_mut(client.selected_db);
    purge_expired_key(db, key, now);

    let Some(entry) = db.get_mut(key) else {
        return CommandOutcome::reply(RespFrame::Integer(0));
    };

    let removed = entry.expire_at_ms.take().is_some();
    CommandOutcome::reply(RespFrame::Integer(if removed { 1 } else { 0 }))
}

pub(super) fn cmd_del(
    args: &[Bytes],
    server: &mut ServerState,
    client: &ClientState,
) -> CommandOutcome {
    if args.is_empty() {
        return wrong_arity("del");
    }

    let now = now_ms();
    let db = server.db_mut(client.selected_db);
    let mut removed = 0i64;
    for key in args {
        purge_expired_key(db, key, now);
        if db.remove(key).is_some() {
            removed += 1;
        }
    }

    CommandOutcome::reply(RespFrame::Integer(removed))
}

pub(super) fn cmd_exists(
    args: &[Bytes],
    server: &mut ServerState,
    client: &ClientState,
) -> CommandOutcome {
    if args.is_empty() {
        return wrong_arity("exists");
    }

    let now = now_ms();
    let db = server.db_mut(client.selected_db);
    let mut count = 0i64;
    for key in args {
        purge_expired_key(db, key, now);
        if db.contains_key(key) {
            count += 1;
        }
    }

    CommandOutcome::reply(RespFrame::Integer(count))
}

pub(super) fn cmd_touch(
    args: &[Bytes],
    server: &mut ServerState,
    client: &ClientState,
) -> CommandOutcome {
    if args.is_empty() {
        return wrong_arity("touch");
    }

    let now = now_ms();
    let db = server.db_mut(client.selected_db);
    let mut count = 0i64;
    for key in args {
        purge_expired_key(db, key, now);
        if db.contains_key(key) {
            count += 1;
        }
    }

    CommandOutcome::reply(RespFrame::Integer(count))
}

pub(super) fn cmd_unlink(
    args: &[Bytes],
    server: &mut ServerState,
    client: &ClientState,
) -> CommandOutcome {
    if args.is_empty() {
        return wrong_arity("unlink");
    }

    cmd_del(args, server, client)
}

pub(super) fn cmd_rename(
    args: &[Bytes],
    server: &mut ServerState,
    client: &ClientState,
) -> CommandOutcome {
    cmd_rename_with_mode(args, server, client, false, "rename")
}

pub(super) fn cmd_renamenx(
    args: &[Bytes],
    server: &mut ServerState,
    client: &ClientState,
) -> CommandOutcome {
    cmd_rename_with_mode(args, server, client, true, "renamenx")
}

pub(super) fn cmd_rename_with_mode(
    args: &[Bytes],
    server: &mut ServerState,
    client: &ClientState,
    only_if_missing: bool,
    command_name: &str,
) -> CommandOutcome {
    let [source, target] = args else {
        return wrong_arity(command_name);
    };

    let now = now_ms();
    let db = server.db_mut(client.selected_db);
    purge_expired_key(db, source, now);

    let Some(value) = db.get(source).cloned() else {
        return CommandOutcome::reply(err("ERR no such key"));
    };

    if source == target {
        return if only_if_missing {
            CommandOutcome::reply(RespFrame::Integer(0))
        } else {
            CommandOutcome::reply(RespFrame::ok())
        };
    }

    purge_expired_key(db, target, now);
    if only_if_missing && db.contains_key(target) {
        return CommandOutcome::reply(RespFrame::Integer(0));
    }

    db.remove(source);
    db.insert(target.clone(), value);

    if only_if_missing {
        CommandOutcome::reply(RespFrame::Integer(1))
    } else {
        CommandOutcome::reply(RespFrame::ok())
    }
}

pub(super) fn cmd_move(
    args: &[Bytes],
    server: &mut ServerState,
    client: &ClientState,
) -> CommandOutcome {
    let [key, db_index_raw] = args else {
        return wrong_arity("move");
    };

    let Some(target_db_index) = parse_usize(db_index_raw) else {
        return CommandOutcome::reply(err("ERR value is not an integer or out of range"));
    };

    if target_db_index >= server.db_count() {
        return CommandOutcome::reply(err("ERR DB index is out of range"));
    }

    if target_db_index == client.selected_db {
        return CommandOutcome::reply(err("ERR source and destination objects are the same"));
    }

    let now = now_ms();

    {
        let source_db = server.db_mut(client.selected_db);
        purge_expired_key(source_db, key, now);
    }

    if !server.db(client.selected_db).contains_key(key) {
        return CommandOutcome::reply(RespFrame::Integer(0));
    }

    {
        let target_db = server.db_mut(target_db_index);
        purge_expired_key(target_db, key, now);
        if target_db.contains_key(key) {
            return CommandOutcome::reply(RespFrame::Integer(0));
        }
    }

    let moved_value = {
        let source_db = server.db_mut(client.selected_db);
        source_db.remove(key)
    };

    let Some(moved_value) = moved_value else {
        return CommandOutcome::reply(RespFrame::Integer(0));
    };

    let target_db = server.db_mut(target_db_index);
    target_db.insert(key.clone(), moved_value);
    CommandOutcome::reply(RespFrame::Integer(1))
}

pub(super) fn cmd_copy(
    args: &[Bytes],
    server: &mut ServerState,
    client: &ClientState,
) -> CommandOutcome {
    if args.len() < 2 {
        return wrong_arity("copy");
    }

    let source_key = &args[0];
    let target_key = &args[1];

    let mut target_db_index = client.selected_db;
    let mut replace = false;

    let mut idx = 2usize;
    while idx < args.len() {
        let option = to_uppercase_bytes(&args[idx]);
        match option.as_slice() {
            b"REPLACE" => {
                replace = true;
                idx += 1;
            }
            b"DB" => {
                if idx + 1 >= args.len() {
                    return CommandOutcome::reply(err("ERR syntax error"));
                }

                let Some(parsed_db_index) = parse_usize(&args[idx + 1]) else {
                    return CommandOutcome::reply(err(
                        "ERR value is not an integer or out of range",
                    ));
                };
                target_db_index = parsed_db_index;
                idx += 2;
            }
            _ => {
                return CommandOutcome::reply(err("ERR syntax error"));
            }
        }
    }

    if target_db_index >= server.db_count() {
        return CommandOutcome::reply(err("ERR DB index is out of range"));
    }

    if target_db_index == client.selected_db && source_key == target_key {
        return CommandOutcome::reply(err("ERR source and destination objects are the same"));
    }

    let now = now_ms();

    {
        let source_db = server.db_mut(client.selected_db);
        purge_expired_key(source_db, source_key, now);
    }

    let Some(source_value) = server.db(client.selected_db).get(source_key).cloned() else {
        return CommandOutcome::reply(RespFrame::Integer(0));
    };

    let target_db = server.db_mut(target_db_index);
    purge_expired_key(target_db, target_key, now);

    if target_db.contains_key(target_key) && !replace {
        return CommandOutcome::reply(RespFrame::Integer(0));
    }

    target_db.insert(target_key.clone(), source_value);
    CommandOutcome::reply(RespFrame::Integer(1))
}

pub(super) fn parse_scan_cursor(raw: &Bytes) -> Result<usize, RespFrame> {
    let Some(cursor_value) = parse_i64(raw) else {
        return Err(err("ERR invalid cursor"));
    };
    if cursor_value < 0 {
        return Err(err("ERR invalid cursor"));
    }

    usize::try_from(cursor_value).map_err(|_| err("ERR invalid cursor"))
}

pub(super) fn parse_scan_match_count_options(
    options: &[Bytes],
) -> Result<(Option<String>, usize), RespFrame> {
    let mut pattern: Option<String> = None;
    let mut count: usize = 10;

    let mut idx = 0usize;
    while idx < options.len() {
        let option = &options[idx];
        if option.eq_ignore_ascii_case(b"MATCH") {
            if idx + 1 >= options.len() {
                return Err(err("ERR syntax error"));
            }
            pattern = Some(String::from_utf8_lossy(&options[idx + 1]).to_string());
            idx += 2;
            continue;
        }

        if option.eq_ignore_ascii_case(b"COUNT") {
            if idx + 1 >= options.len() {
                return Err(err("ERR syntax error"));
            }
            let Some(parsed_count) = parse_i64(&options[idx + 1]) else {
                return Err(err("ERR value is not an integer or out of range"));
            };
            if parsed_count < 1 {
                return Err(err("ERR syntax error"));
            }
            let Ok(parsed_count) = usize::try_from(parsed_count) else {
                return Err(err("ERR syntax error"));
            };
            count = parsed_count;
            idx += 2;
            continue;
        }

        return Err(err("ERR syntax error"));
    }

    Ok((pattern, count))
}

pub(super) fn scan_collect_indexes<T, F>(
    items: &[T],
    cursor: usize,
    count: usize,
    mut is_match: F,
) -> (usize, Vec<usize>)
where
    F: FnMut(&T) -> bool,
{
    if items.is_empty() {
        return (0, vec![]);
    }

    let mut idx = cursor.min(items.len());
    let mut scanned = 0usize;
    let mut matched = Vec::new();

    while idx < items.len() && scanned < count {
        if is_match(&items[idx]) {
            matched.push(idx);
        }
        idx += 1;
        scanned += 1;
    }

    let next_cursor = if idx >= items.len() { 0 } else { idx };
    (next_cursor, matched)
}

pub(super) fn scan_reply(next_cursor: usize, entries: Vec<RespFrame>) -> CommandOutcome {
    CommandOutcome::reply(RespFrame::Array(vec![
        RespFrame::BulkString(Some(Bytes::from(next_cursor.to_string()))),
        RespFrame::Array(entries),
    ]))
}

pub(super) fn cmd_scan(
    args: &[Bytes],
    server: &mut ServerState,
    client: &ClientState,
) -> CommandOutcome {
    let [cursor_raw, options @ ..] = args else {
        return wrong_arity("scan");
    };

    let cursor = match parse_scan_cursor(cursor_raw) {
        Ok(cursor) => cursor,
        Err(response) => return CommandOutcome::reply(response),
    };

    #[derive(Clone, Copy)]
    enum ScanTypeFilter {
        String,
        Hash,
        List,
        Set,
        ZSet,
        Unknown,
    }

    let mut pattern: Option<String> = None;
    let mut count: usize = 10;
    let mut type_filter: Option<ScanTypeFilter> = None;

    let mut idx = 0usize;
    while idx < options.len() {
        let option = &options[idx];
        if option.eq_ignore_ascii_case(b"MATCH") {
            if idx + 1 >= options.len() {
                return CommandOutcome::reply(err("ERR syntax error"));
            }
            pattern = Some(String::from_utf8_lossy(&options[idx + 1]).to_string());
            idx += 2;
            continue;
        }

        if option.eq_ignore_ascii_case(b"COUNT") {
            if idx + 1 >= options.len() {
                return CommandOutcome::reply(err("ERR syntax error"));
            }
            let Some(parsed_count) = parse_i64(&options[idx + 1]) else {
                return CommandOutcome::reply(err("ERR value is not an integer or out of range"));
            };
            if parsed_count < 1 {
                return CommandOutcome::reply(err("ERR syntax error"));
            }
            let Ok(parsed_count) = usize::try_from(parsed_count) else {
                return CommandOutcome::reply(err("ERR syntax error"));
            };
            count = parsed_count;
            idx += 2;
            continue;
        }

        if option.eq_ignore_ascii_case(b"TYPE") {
            let Some(raw_type) = options.get(idx + 1) else {
                return CommandOutcome::reply(err("ERR syntax error"));
            };
            type_filter = Some(if raw_type.eq_ignore_ascii_case(b"STRING") {
                ScanTypeFilter::String
            } else if raw_type.eq_ignore_ascii_case(b"HASH") {
                ScanTypeFilter::Hash
            } else if raw_type.eq_ignore_ascii_case(b"LIST") {
                ScanTypeFilter::List
            } else if raw_type.eq_ignore_ascii_case(b"SET") {
                ScanTypeFilter::Set
            } else if raw_type.eq_ignore_ascii_case(b"ZSET") {
                ScanTypeFilter::ZSet
            } else {
                ScanTypeFilter::Unknown
            });
            idx += 2;
            continue;
        }

        return CommandOutcome::reply(err("ERR syntax error"));
    }

    let now = now_ms();
    let db = server.db_mut(client.selected_db);
    purge_expired_keys(db, now);

    let mut keys = db.keys().cloned().collect::<Vec<_>>();
    keys.sort();

    let (next_cursor, matched_indexes) = scan_collect_indexes(&keys, cursor, count, |key| {
        if let Some(pattern) = &pattern {
            if !glob_match(pattern, &String::from_utf8_lossy(key)) {
                return false;
            }
        }

        if let Some(type_filter) = type_filter {
            let Some(entry) = db.get(key) else {
                return false;
            };

            match type_filter {
                ScanTypeFilter::String => entry.is_string(),
                ScanTypeFilter::Hash => entry.is_hash(),
                ScanTypeFilter::List => entry.is_list(),
                ScanTypeFilter::Set => entry.is_set(),
                ScanTypeFilter::ZSet => entry.is_sorted_set(),
                ScanTypeFilter::Unknown => false,
            }
        } else {
            true
        }
    });

    let entries = matched_indexes
        .into_iter()
        .map(|idx| RespFrame::BulkString(Some(keys[idx].clone())))
        .collect::<Vec<_>>();

    scan_reply(next_cursor, entries)
}

pub(super) fn cmd_object(
    args: &[Bytes],
    server: &mut ServerState,
    client: &ClientState,
) -> CommandOutcome {
    if args.is_empty() {
        return wrong_arity("object");
    }

    let subcommand = to_uppercase_bytes(&args[0]);
    match subcommand.as_slice() {
        b"HELP" if args.len() == 1 => CommandOutcome::reply(RespFrame::Array(vec![
            RespFrame::bulk_str("ENCODING <key>"),
            RespFrame::bulk_str("FREQ <key>"),
            RespFrame::bulk_str("IDLETIME <key>"),
            RespFrame::bulk_str("REFCOUNT <key>"),
        ])),
        b"ENCODING" | b"REFCOUNT" | b"IDLETIME" | b"FREQ" if args.len() == 2 => {
            let key = &args[1];
            let now = now_ms();
            let db = server.db_mut(client.selected_db);
            purge_expired_key(db, key, now);

            let Some(entry) = db.get(key) else {
                return CommandOutcome::reply(RespFrame::Null);
            };

            match subcommand.as_slice() {
                b"ENCODING" => {
                    let encoding = if entry.is_hash() {
                        "hashtable"
                    } else if entry.is_list() {
                        "quicklist"
                    } else if entry.is_set() {
                        "hashtable"
                    } else if entry.is_sorted_set() {
                        "skiplist"
                    } else {
                        "raw"
                    };
                    CommandOutcome::reply(RespFrame::bulk_str(encoding))
                }
                b"REFCOUNT" => CommandOutcome::reply(RespFrame::Integer(1)),
                b"IDLETIME" => CommandOutcome::reply(RespFrame::Integer(0)),
                b"FREQ" => CommandOutcome::reply(err(
                    "ERR An LFU maxmemory policy is not selected, access frequency not tracked. Please note that when switching between policies at runtime LRU and LFU data will take some time to adjust.",
                )),
                _ => unreachable!(),
            }
        }
        _ => CommandOutcome::reply(err(
            "ERR Unknown subcommand or wrong number of arguments for 'OBJECT'. Try OBJECT HELP.",
        )),
    }
}

pub(super) fn cmd_sort(
    args: &[Bytes],
    server: &mut ServerState,
    client: &ClientState,
    readonly: bool,
) -> CommandOutcome {
    let command_name = if readonly { "sort_ro" } else { "sort" };
    let [key, options @ ..] = args else {
        return wrong_arity(command_name);
    };

    let mut desc = false;
    let mut alpha = false;
    let mut limit_start: usize = 0;
    let mut limit_count: Option<usize> = None;
    let mut store_key: Option<Bytes> = None;
    let mut _by_pattern: Option<Bytes> = None;
    let mut _get_patterns: Vec<Bytes> = Vec::new();

    let mut idx = 0usize;
    while idx < options.len() {
        let option = to_uppercase_bytes(&options[idx]);
        match option.as_slice() {
            b"ASC" => {
                desc = false;
                idx += 1;
            }
            b"DESC" => {
                desc = true;
                idx += 1;
            }
            b"ALPHA" => {
                alpha = true;
                idx += 1;
            }
            b"BY" => {
                if idx + 1 >= options.len() {
                    return CommandOutcome::reply(err("ERR syntax error"));
                }
                _by_pattern = Some(options[idx + 1].clone());
                idx += 2;
            }
            b"GET" => {
                if idx + 1 >= options.len() {
                    return CommandOutcome::reply(err("ERR syntax error"));
                }
                _get_patterns.push(options[idx + 1].clone());
                idx += 2;
            }
            b"LIMIT" => {
                if idx + 2 >= options.len() {
                    return CommandOutcome::reply(err("ERR syntax error"));
                }

                let Some(raw_start) = parse_i64(&options[idx + 1]) else {
                    return CommandOutcome::reply(err(
                        "ERR value is not an integer or out of range",
                    ));
                };
                let Some(raw_count) = parse_i64(&options[idx + 2]) else {
                    return CommandOutcome::reply(err(
                        "ERR value is not an integer or out of range",
                    ));
                };

                limit_start = if raw_start < 0 {
                    0
                } else {
                    let Ok(start) = usize::try_from(raw_start) else {
                        return CommandOutcome::reply(err(
                            "ERR value is not an integer or out of range",
                        ));
                    };
                    start
                };
                limit_count = if raw_count < 0 {
                    None
                } else {
                    let Ok(count) = usize::try_from(raw_count) else {
                        return CommandOutcome::reply(err(
                            "ERR value is not an integer or out of range",
                        ));
                    };
                    Some(count)
                };
                idx += 3;
            }
            b"STORE" => {
                if readonly || idx + 1 >= options.len() {
                    return CommandOutcome::reply(err("ERR syntax error"));
                }

                store_key = Some(options[idx + 1].clone());
                idx += 2;
            }
            _ => {
                return CommandOutcome::reply(err("ERR syntax error"));
            }
        }
    }

    let now = now_ms();
    let db = server.db_mut(client.selected_db);
    purge_expired_key(db, key, now);

    let Some(entry) = db.get(key) else {
        if let Some(store_key) = store_key {
            db.remove(&store_key);
            return CommandOutcome::reply(RespFrame::Integer(0));
        }

        return CommandOutcome::reply(RespFrame::Array(vec![]));
    };

    let mut values = if let Some(list) = entry.as_list() {
        list.iter().cloned().collect::<Vec<_>>()
    } else if let Some(set) = entry.as_set() {
        set.iter().cloned().collect::<Vec<_>>()
    } else {
        return wrong_type_response();
    };

    if alpha {
        values.sort();
    } else {
        let mut scored = Vec::with_capacity(values.len());
        for value in values {
            let Some(score) = parse_f64(&value) else {
                return CommandOutcome::reply(err(
                    "ERR One or more scores can't be converted into double",
                ));
            };
            scored.push((value, score));
        }
        scored.sort_by(|a, b| a.1.total_cmp(&b.1));
        values = scored
            .into_iter()
            .map(|(value, _)| value)
            .collect::<Vec<_>>();
    }

    if desc {
        values.reverse();
    }

    let start = limit_start.min(values.len());
    let end = match limit_count {
        Some(count) => start.saturating_add(count).min(values.len()),
        None => values.len(),
    };
    let sorted_values = values[start..end].to_vec();

    if let Some(store_key) = store_key {
        if sorted_values.is_empty() {
            db.remove(&store_key);
            return CommandOutcome::reply(RespFrame::Integer(0));
        }

        db.insert(
            store_key,
            StoredValue::list(VecDeque::from(sorted_values.clone()), None),
        );
        return CommandOutcome::reply(RespFrame::Integer(sorted_values.len() as i64));
    }

    let frames = sorted_values
        .into_iter()
        .map(|value| RespFrame::BulkString(Some(value)))
        .collect::<Vec<_>>();
    CommandOutcome::reply(RespFrame::Array(frames))
}

pub(super) fn cmd_select(
    args: &[Bytes],
    server: &ServerState,
    client: &mut ClientState,
) -> CommandOutcome {
    let [db_index_raw] = args else {
        return wrong_arity("select");
    };

    let Some(db_index) = parse_usize(db_index_raw) else {
        return CommandOutcome::reply(err("ERR value is not an integer or out of range"));
    };

    if db_index >= server.db_count() {
        return CommandOutcome::reply(err("ERR DB index is out of range"));
    }

    client.selected_db = db_index;
    CommandOutcome::reply(RespFrame::ok())
}

pub(super) fn cmd_expire_with_mode(
    args: &[Bytes],
    server: &mut ServerState,
    client: &ClientState,
    mode: ExpireMode,
    command_name: &str,
) -> CommandOutcome {
    if args.len() < 2 || args.len() > 3 {
        return wrong_arity(command_name);
    }

    let key = &args[0];
    let Some(timeout_raw) = parse_i64(&args[1]) else {
        return CommandOutcome::reply(err("ERR value is not an integer or out of range"));
    };

    let condition = if args.len() == 3 {
        parse_expire_condition(&args[2])
    } else {
        Some(ExpireCondition::None)
    };
    let Some(condition) = condition else {
        return CommandOutcome::reply(err("ERR syntax error"));
    };

    let now = now_ms();
    let target = to_expire_target_ms(timeout_raw, mode, now);

    let db = server.db_mut(client.selected_db);
    purge_expired_key(db, key, now);

    let Some(current_expire) = db.get(key).map(|entry| entry.expire_at_ms) else {
        return CommandOutcome::reply(RespFrame::Integer(0));
    };

    if !expire_condition_matches(condition, current_expire, target) {
        return CommandOutcome::reply(RespFrame::Integer(0));
    }

    if target <= now {
        db.remove(key);
        return CommandOutcome::reply(RespFrame::Integer(1));
    }

    if let Some(entry) = db.get_mut(key) {
        entry.expire_at_ms = Some(target);
        return CommandOutcome::reply(RespFrame::Integer(1));
    }

    CommandOutcome::reply(RespFrame::Integer(0))
}

pub(super) fn cmd_ttl_with_mode(
    args: &[Bytes],
    server: &mut ServerState,
    client: &ClientState,
    mode: TtlMode,
    command_name: &str,
) -> CommandOutcome {
    let [key] = args else {
        return wrong_arity(command_name);
    };

    let now = now_ms();
    let db = server.db_mut(client.selected_db);
    purge_expired_key(db, key, now);

    let Some(entry) = db.get(key) else {
        return CommandOutcome::reply(RespFrame::Integer(-2));
    };

    let Some(expire_at_ms) = entry.expire_at_ms else {
        return CommandOutcome::reply(RespFrame::Integer(-1));
    };

    let remaining_ms = expire_at_ms.saturating_sub(now);
    if remaining_ms <= 0 {
        db.remove(key);
        return CommandOutcome::reply(RespFrame::Integer(-2));
    }

    let ttl = match mode {
        TtlMode::Seconds => remaining_ms / 1000,
        TtlMode::Milliseconds => remaining_ms,
    };
    CommandOutcome::reply(RespFrame::Integer(ttl))
}

pub(super) fn cmd_expiretime_with_mode(
    args: &[Bytes],
    server: &mut ServerState,
    client: &ClientState,
    mode: ExpireTimeMode,
    command_name: &str,
) -> CommandOutcome {
    let [key] = args else {
        return wrong_arity(command_name);
    };

    let now = now_ms();
    let db = server.db_mut(client.selected_db);
    purge_expired_key(db, key, now);

    let Some(entry) = db.get(key) else {
        return CommandOutcome::reply(RespFrame::Integer(-2));
    };

    let Some(expire_at_ms) = entry.expire_at_ms else {
        return CommandOutcome::reply(RespFrame::Integer(-1));
    };

    let value = match mode {
        ExpireTimeMode::Seconds => expire_at_ms / 1000,
        ExpireTimeMode::Milliseconds => expire_at_ms,
    };
    CommandOutcome::reply(RespFrame::Integer(value))
}
