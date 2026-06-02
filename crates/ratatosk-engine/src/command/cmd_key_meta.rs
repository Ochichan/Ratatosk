use std::collections::VecDeque;

use bytes::Bytes;
use glob_match::glob_match;
use ratatosk_resp::frame::RespFrame;

use crate::keyspace::{ServerState, StoredValue, purge_expired_key, purge_expired_keys};
use crate::object::parse_f64;

use super::cmd_key::{parse_scan_cursor, scan_collect_indexes, scan_reply};
use super::{
    ClientState, CommandOutcome, err, now_ms, parse_i64, to_uppercase_bytes, wrong_arity,
    wrong_type_response,
};

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
    let mut db = server.db_mut(client.selected_db);
    purge_expired_keys(&mut db, now);

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
            let mut db = server.db_mut(client.selected_db);
            purge_expired_key(&mut db, key, now);

            let Some(entry) = db.get(key) else {
                return CommandOutcome::reply(RespFrame::Null);
            };

            match subcommand.as_slice() {
                b"ENCODING" => {
                    let encoding = if entry.is_hash() {
                        "hashtable"
                    } else if entry.is_list() {
                        "quicklist"
                    } else if matches!(entry.data(), crate::keyspace::ValueData::SetInt(_)) {
                        "intset"
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
                _ => {
                    debug_assert!(false, "OBJECT subcommand validated by outer match");
                    CommandOutcome::reply(err(
                        "ERR Unknown subcommand or wrong number of arguments for 'OBJECT'. Try OBJECT HELP.",
                    ))
                }
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
    let mut db = server.db_mut(client.selected_db);
    purge_expired_key(&mut db, key, now);

    let Some(entry) = db.get(key) else {
        if let Some(store_key) = store_key {
            db.remove(&store_key);
            return CommandOutcome::reply(RespFrame::Integer(0));
        }

        return CommandOutcome::reply(RespFrame::Array(vec![]));
    };

    let mut values = if let Some(list) = entry.as_list() {
        list.iter().cloned().collect::<Vec<_>>()
    } else if let Some(set) = entry.set_members() {
        set
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
