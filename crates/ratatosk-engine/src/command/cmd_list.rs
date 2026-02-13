use std::collections::VecDeque;

use bytes::Bytes;

use ratatosk_resp::frame::RespFrame;

use crate::keyspace::{ServerState, StoredValue, purge_expired_key};
use crate::object::{normalize_range, parse_f64};

use super::{
    ClientState, CommandOutcome, err, now_ms, parse_i64, to_uppercase_bytes, wrong_arity,
    wrong_type_response,
};

const MAX_LIST_POP_COUNT: usize = 100_000;
const MAX_LIST_NUMKEYS: usize = 10_000;

pub(super) fn cmd_lpush(
    args: &[Bytes],
    server: &mut ServerState,
    client: &ClientState,
) -> CommandOutcome {
    cmd_push(args, server, client, true, "lpush")
}

pub(super) fn cmd_lpushx(
    args: &[Bytes],
    server: &mut ServerState,
    client: &ClientState,
) -> CommandOutcome {
    cmd_pushx(args, server, client, true, "lpushx")
}

pub(super) fn cmd_rpush(
    args: &[Bytes],
    server: &mut ServerState,
    client: &ClientState,
) -> CommandOutcome {
    cmd_push(args, server, client, false, "rpush")
}

pub(super) fn cmd_rpushx(
    args: &[Bytes],
    server: &mut ServerState,
    client: &ClientState,
) -> CommandOutcome {
    cmd_pushx(args, server, client, false, "rpushx")
}

pub(super) fn cmd_pushx(
    args: &[Bytes],
    server: &mut ServerState,
    client: &ClientState,
    left: bool,
    command_name: &str,
) -> CommandOutcome {
    if args.len() < 2 {
        return wrong_arity(command_name);
    }

    let key = &args[0];
    let now = now_ms();
    let db = server.db_mut(client.selected_db);
    purge_expired_key(db, key, now);

    let Some(entry) = db.get_mut(key) else {
        return CommandOutcome::reply(RespFrame::Integer(0));
    };
    let Some(list) = entry.as_list_mut() else {
        return wrong_type_response();
    };

    for value in &args[1..] {
        if left {
            list.push_front(value.clone());
        } else {
            list.push_back(value.clone());
        }
    }

    CommandOutcome::reply(RespFrame::Integer(list.len() as i64))
}

pub(super) fn cmd_push(
    args: &[Bytes],
    server: &mut ServerState,
    client: &ClientState,
    left: bool,
    command_name: &str,
) -> CommandOutcome {
    if args.len() < 2 {
        return wrong_arity(command_name);
    }

    let key = &args[0];
    let now = now_ms();
    let db = server.db_mut(client.selected_db);
    purge_expired_key(db, key, now);

    if !db.contains_key(key) {
        let mut list = VecDeque::new();
        for value in &args[1..] {
            if left {
                list.push_front(value.clone());
            } else {
                list.push_back(value.clone());
            }
        }
        let len = list.len();
        db.insert(key.clone(), StoredValue::list(list, None));
        return CommandOutcome::reply(RespFrame::Integer(len as i64));
    }

    let Some(entry) = db.get_mut(key) else {
        return CommandOutcome::reply(RespFrame::Integer(0));
    };
    let Some(list) = entry.as_list_mut() else {
        return wrong_type_response();
    };

    for value in &args[1..] {
        if left {
            list.push_front(value.clone());
        } else {
            list.push_back(value.clone());
        }
    }

    CommandOutcome::reply(RespFrame::Integer(list.len() as i64))
}

pub(super) fn cmd_lpop(
    args: &[Bytes],
    server: &mut ServerState,
    client: &ClientState,
) -> CommandOutcome {
    cmd_pop(args, server, client, true, "lpop")
}

pub(super) fn cmd_rpop(
    args: &[Bytes],
    server: &mut ServerState,
    client: &ClientState,
) -> CommandOutcome {
    cmd_pop(args, server, client, false, "rpop")
}

pub(super) fn cmd_blpop(
    args: &[Bytes],
    server: &mut ServerState,
    client: &ClientState,
) -> CommandOutcome {
    cmd_bpop(args, server, client, true, "blpop")
}

pub(super) fn cmd_brpop(
    args: &[Bytes],
    server: &mut ServerState,
    client: &ClientState,
) -> CommandOutcome {
    cmd_bpop(args, server, client, false, "brpop")
}

pub(super) fn cmd_bpop(
    args: &[Bytes],
    server: &mut ServerState,
    client: &ClientState,
    left: bool,
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

    let outcome = try_bpop_once(keys, server, client, left);
    if !matches!(outcome.response, RespFrame::BulkString(None)) {
        return outcome;
    }

    if let Some(deadline) = deadline_ms {
        if ratatosk_core::time::monotonic_ms() as i64 >= deadline {
            return outcome;
        }
    }

    let full_frame = build_blocking_frame(command_name, args);
    CommandOutcome::blocking(outcome.response, deadline_ms, full_frame)
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

pub(super) fn try_bpop_once(
    keys: &[Bytes],
    server: &mut ServerState,
    client: &ClientState,
    left: bool,
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
            let Some(list) = entry.as_list_mut() else {
                return wrong_type_response();
            };

            let popped = if left {
                list.pop_front()
            } else {
                list.pop_back()
            };

            if list.is_empty() {
                remove_key = true;
            }

            popped
        };

        if remove_key {
            db.remove(key);
        }

        let Some(value) = popped else {
            continue;
        };

        return CommandOutcome::reply(RespFrame::Array(vec![
            RespFrame::BulkString(Some(key.clone())),
            RespFrame::BulkString(Some(value)),
        ]));
    }

    CommandOutcome::reply(RespFrame::BulkString(None))
}

pub(super) fn cmd_pop(
    args: &[Bytes],
    server: &mut ServerState,
    client: &ClientState,
    left: bool,
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
        if parsed_count > MAX_LIST_POP_COUNT {
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
        return CommandOutcome::reply(RespFrame::BulkString(None));
    }

    let mut remove_key = false;
    let response = {
        let Some(entry) = db.get_mut(key) else {
            return CommandOutcome::reply(RespFrame::BulkString(None));
        };
        let Some(list) = entry.as_list_mut() else {
            return wrong_type_response();
        };

        match count {
            None => {
                let popped = if left {
                    list.pop_front()
                } else {
                    list.pop_back()
                };
                if list.is_empty() {
                    remove_key = true;
                }
                CommandOutcome::reply(RespFrame::BulkString(popped))
            }
            Some(count) => {
                let mut items = Vec::with_capacity(count);
                for _ in 0..count {
                    let popped = if left {
                        list.pop_front()
                    } else {
                        list.pop_back()
                    };
                    let Some(popped) = popped else {
                        break;
                    };
                    items.push(RespFrame::BulkString(Some(popped)));
                }
                if list.is_empty() {
                    remove_key = true;
                }
                CommandOutcome::reply(RespFrame::Array(items))
            }
        }
    };

    if remove_key {
        db.remove(key);
    }

    response
}

pub(super) fn parse_list_side(raw: &Bytes) -> Option<bool> {
    match to_uppercase_bytes(raw).as_slice() {
        b"LEFT" => Some(true),
        b"RIGHT" => Some(false),
        _ => None,
    }
}

pub(super) fn cmd_lmove(
    args: &[Bytes],
    server: &mut ServerState,
    client: &ClientState,
) -> CommandOutcome {
    let [source, destination, from_raw, to_raw] = args else {
        return wrong_arity("lmove");
    };

    let Some(from_left) = parse_list_side(from_raw) else {
        return CommandOutcome::reply(err("ERR syntax error"));
    };
    let Some(to_left) = parse_list_side(to_raw) else {
        return CommandOutcome::reply(err("ERR syntax error"));
    };

    let now = now_ms();
    let db = server.db_mut(client.selected_db);
    purge_expired_key(db, source, now);
    if source != destination {
        purge_expired_key(db, destination, now);
    }

    if source == destination {
        let Some(entry) = db.get_mut(source) else {
            return CommandOutcome::reply(RespFrame::BulkString(None));
        };
        let Some(list) = entry.as_list_mut() else {
            return wrong_type_response();
        };

        let popped = if from_left {
            list.pop_front()
        } else {
            list.pop_back()
        };
        let Some(value) = popped else {
            return CommandOutcome::reply(RespFrame::BulkString(None));
        };

        if to_left {
            list.push_front(value.clone());
        } else {
            list.push_back(value.clone());
        }

        return CommandOutcome::reply(RespFrame::BulkString(Some(value)));
    }

    let Some(source_entry) = db.get(source) else {
        return CommandOutcome::reply(RespFrame::BulkString(None));
    };
    let Some(source_list) = source_entry.as_list() else {
        return wrong_type_response();
    };
    if source_list.is_empty() {
        return CommandOutcome::reply(RespFrame::BulkString(None));
    }

    if let Some(destination_entry) = db.get(destination) {
        if !destination_entry.is_list() {
            return wrong_type_response();
        }
    }

    let value = {
        let Some(source_entry) = db.get_mut(source) else {
            return CommandOutcome::reply(RespFrame::BulkString(None));
        };
        let Some(source_list) = source_entry.as_list_mut() else {
            return wrong_type_response();
        };
        if from_left {
            source_list.pop_front()
        } else {
            source_list.pop_back()
        }
    };

    let Some(value) = value else {
        return CommandOutcome::reply(RespFrame::BulkString(None));
    };

    if db
        .get(source)
        .and_then(|entry| entry.as_list())
        .is_some_and(|list| list.is_empty())
    {
        db.remove(source);
    }

    if let Some(destination_entry) = db.get_mut(destination) {
        let Some(destination_list) = destination_entry.as_list_mut() else {
            return wrong_type_response();
        };
        if to_left {
            destination_list.push_front(value.clone());
        } else {
            destination_list.push_back(value.clone());
        }
    } else {
        let mut list = VecDeque::new();
        if to_left {
            list.push_front(value.clone());
        } else {
            list.push_back(value.clone());
        }
        db.insert(destination.clone(), StoredValue::list(list, None));
    }

    CommandOutcome::reply(RespFrame::BulkString(Some(value)))
}

pub(super) fn parse_blocking_timeout_seconds(raw: &Bytes) -> Result<f64, RespFrame> {
    let Some(timeout) = parse_f64(raw) else {
        return Err(err("ERR timeout is not a float or out of range"));
    };

    if !timeout.is_finite() {
        return Err(err("ERR timeout is not a float or out of range"));
    }

    if timeout < 0.0 {
        return Err(err("ERR timeout is negative"));
    }

    Ok(timeout)
}

pub(super) fn cmd_rpoplpush(
    args: &[Bytes],
    server: &mut ServerState,
    client: &ClientState,
) -> CommandOutcome {
    let [source, destination] = args else {
        return wrong_arity("rpoplpush");
    };

    let lmove_args = [
        source.clone(),
        destination.clone(),
        Bytes::from_static(b"RIGHT"),
        Bytes::from_static(b"LEFT"),
    ];
    cmd_lmove(&lmove_args, server, client)
}

pub(super) fn cmd_brpoplpush(
    args: &[Bytes],
    server: &mut ServerState,
    client: &ClientState,
) -> CommandOutcome {
    let [source, destination, timeout_raw] = args else {
        return wrong_arity("brpoplpush");
    };

    let timeout_sec = match parse_blocking_timeout_seconds(timeout_raw) {
        Ok(timeout) => timeout,
        Err(response) => return CommandOutcome::reply(response),
    };

    let deadline_ms = blocking_deadline_ms(timeout_sec);
    let wrapped = [source.clone(), destination.clone()];

    let outcome = cmd_rpoplpush(&wrapped, server, client);
    if !matches!(outcome.response, RespFrame::BulkString(None)) {
        return outcome;
    }

    if let Some(deadline) = deadline_ms {
        if ratatosk_core::time::monotonic_ms() as i64 >= deadline {
            return outcome;
        }
    }

    let full_frame = build_blocking_frame("BRPOPLPUSH", args);
    CommandOutcome::blocking(outcome.response, deadline_ms, full_frame)
}

pub(super) fn cmd_blmove(
    args: &[Bytes],
    server: &mut ServerState,
    client: &ClientState,
) -> CommandOutcome {
    let [source, destination, from_raw, to_raw, timeout_raw] = args else {
        return wrong_arity("blmove");
    };

    let timeout_sec = match parse_blocking_timeout_seconds(timeout_raw) {
        Ok(timeout) => timeout,
        Err(response) => return CommandOutcome::reply(response),
    };

    let deadline_ms = blocking_deadline_ms(timeout_sec);
    let wrapped = [
        source.clone(),
        destination.clone(),
        from_raw.clone(),
        to_raw.clone(),
    ];

    let outcome = cmd_lmove(&wrapped, server, client);
    if !matches!(outcome.response, RespFrame::BulkString(None)) {
        return outcome;
    }

    if let Some(deadline) = deadline_ms {
        if ratatosk_core::time::monotonic_ms() as i64 >= deadline {
            return outcome;
        }
    }

    let full_frame = build_blocking_frame("BLMOVE", args);
    CommandOutcome::blocking(outcome.response, deadline_ms, full_frame)
}

pub(super) fn cmd_lmpop(
    args: &[Bytes],
    server: &mut ServerState,
    client: &ClientState,
) -> CommandOutcome {
    cmd_lmpop_inner(args, server, client, false)
}

pub(super) fn cmd_blmpop(
    args: &[Bytes],
    server: &mut ServerState,
    client: &ClientState,
) -> CommandOutcome {
    cmd_lmpop_inner(args, server, client, true)
}

pub(super) fn cmd_lmpop_inner(
    args: &[Bytes],
    server: &mut ServerState,
    client: &ClientState,
    blocking: bool,
) -> CommandOutcome {
    let command_name = if blocking { "blmpop" } else { "lmpop" };
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
    if numkeys > MAX_LIST_NUMKEYS {
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
    let Some(left) = parse_list_side(side_raw) else {
        return CommandOutcome::reply(err("ERR syntax error"));
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
        if parsed_count > MAX_LIST_POP_COUNT {
            return CommandOutcome::reply(err("ERR count is out of range"));
        }
        count = parsed_count;
    }

    if !blocking {
        return try_lmpop_once(&keys, left, count, server, client);
    }

    let deadline_ms = blocking_deadline_ms(timeout_sec);

    let outcome = try_lmpop_once(&keys, left, count, server, client);
    if !matches!(outcome.response, RespFrame::BulkString(None)) {
        return outcome;
    }

    if let Some(deadline) = deadline_ms {
        if ratatosk_core::time::monotonic_ms() as i64 >= deadline {
            return outcome;
        }
    }

    let full_frame = build_blocking_frame("BLMPOP", args);
    CommandOutcome::blocking(outcome.response, deadline_ms, full_frame)
}

pub(super) fn try_lmpop_once(
    keys: &[Bytes],
    left: bool,
    count: usize,
    server: &mut ServerState,
    client: &ClientState,
) -> CommandOutcome {
    let now = now_ms();
    let db = server.db_mut(client.selected_db);

    for key in keys {
        purge_expired_key(db, key, now);

        let mut remove_key = false;
        let popped_values = {
            let Some(entry) = db.get_mut(key) else {
                continue;
            };
            let Some(list) = entry.as_list_mut() else {
                return wrong_type_response();
            };

            let mut popped = Vec::with_capacity(count);
            for _ in 0..count {
                let value = if left {
                    list.pop_front()
                } else {
                    list.pop_back()
                };
                let Some(value) = value else {
                    break;
                };
                popped.push(value);
            }

            if list.is_empty() {
                remove_key = true;
            }

            popped
        };

        if remove_key {
            db.remove(key);
        }

        if popped_values.is_empty() {
            continue;
        }

        let values = popped_values
            .into_iter()
            .map(|value| RespFrame::BulkString(Some(value)))
            .collect::<Vec<_>>();
        return CommandOutcome::reply(RespFrame::Array(vec![
            RespFrame::BulkString(Some(key.clone())),
            RespFrame::Array(values),
        ]));
    }

    CommandOutcome::reply(RespFrame::BulkString(None))
}

pub(super) fn blocking_deadline_ms(timeout_sec: f64) -> Option<i64> {
    use ratatosk_core::time::monotonic_ms;

    if timeout_sec <= 0.0 {
        return None;
    }

    let timeout_ms = (timeout_sec * 1000.0).ceil();
    let timeout_ms = if timeout_ms.is_finite() {
        timeout_ms as u64
    } else {
        u64::MAX
    };

    let now = monotonic_ms();
    let deadline = now.saturating_add(timeout_ms);
    i64::try_from(deadline).ok()
}

pub(super) fn cmd_lrange(
    args: &[Bytes],
    server: &mut ServerState,
    client: &ClientState,
) -> CommandOutcome {
    let [key, start_raw, end_raw] = args else {
        return wrong_arity("lrange");
    };

    let Some(start) = parse_i64(start_raw) else {
        return CommandOutcome::reply(err("ERR value is not an integer or out of range"));
    };
    let Some(end) = parse_i64(end_raw) else {
        return CommandOutcome::reply(err("ERR value is not an integer or out of range"));
    };

    let now = now_ms();
    let db = server.db_mut(client.selected_db);
    purge_expired_key(db, key, now);

    let Some(entry) = db.get(key) else {
        return CommandOutcome::reply(RespFrame::Array(vec![]));
    };
    let Some(list) = entry.as_list() else {
        return wrong_type_response();
    };

    let Some((range_start, range_end)) = normalize_range(list.len(), start, end) else {
        return CommandOutcome::reply(RespFrame::Array(vec![]));
    };

    let count = range_end.saturating_sub(range_start).saturating_add(1);
    let items = list
        .iter()
        .skip(range_start)
        .take(count)
        .cloned()
        .map(|value| RespFrame::BulkString(Some(value)))
        .collect::<Vec<_>>();

    CommandOutcome::reply(RespFrame::Array(items))
}

pub(super) fn cmd_llen(
    args: &[Bytes],
    server: &mut ServerState,
    client: &ClientState,
) -> CommandOutcome {
    let [key] = args else {
        return wrong_arity("llen");
    };

    let now = now_ms();
    let db = server.db_mut(client.selected_db);
    purge_expired_key(db, key, now);

    let Some(entry) = db.get(key) else {
        return CommandOutcome::reply(RespFrame::Integer(0));
    };
    let Some(list) = entry.as_list() else {
        return wrong_type_response();
    };

    CommandOutcome::reply(RespFrame::Integer(list.len() as i64))
}

pub(super) fn cmd_lrem(
    args: &[Bytes],
    server: &mut ServerState,
    client: &ClientState,
) -> CommandOutcome {
    let [key, raw_count, element] = args else {
        return wrong_arity("lrem");
    };

    let Some(count) = parse_i64(raw_count) else {
        return CommandOutcome::reply(err("ERR value is not an integer or out of range"));
    };

    let now = now_ms();
    let db = server.db_mut(client.selected_db);
    purge_expired_key(db, key, now);

    let Some(entry) = db.get_mut(key) else {
        return CommandOutcome::reply(RespFrame::Integer(0));
    };
    let Some(list) = entry.as_list_mut() else {
        return wrong_type_response();
    };

    let mut removed = 0i64;
    if count > 0 {
        let Ok(limit) = usize::try_from(count) else {
            return CommandOutcome::reply(err("ERR value is not an integer or out of range"));
        };
        let mut kept = 0usize;
        list.retain(|elem| {
            if (removed as usize) < limit && elem == element {
                removed += 1;
                false
            } else {
                kept += 1;
                true
            }
        });
    } else if count < 0 {
        let Ok(target) = usize::try_from(count.saturating_neg()) else {
            return CommandOutcome::reply(err("ERR value is not an integer or out of range"));
        };
        // Count matching elements from the tail: mark which ones to remove
        let total_matching = list.iter().filter(|elem| *elem == element).count();
        let skip_from_head = total_matching.saturating_sub(target);
        let mut seen_matching = 0usize;
        list.retain(|elem| {
            if elem == element {
                seen_matching += 1;
                if seen_matching > skip_from_head {
                    removed += 1;
                    false
                } else {
                    true
                }
            } else {
                true
            }
        });
    } else {
        list.retain(|elem| {
            if elem == element {
                removed += 1;
                false
            } else {
                true
            }
        });
    }

    if list.is_empty() {
        db.remove(key);
    }

    CommandOutcome::reply(RespFrame::Integer(removed))
}

pub(super) fn cmd_lpos(
    args: &[Bytes],
    server: &mut ServerState,
    client: &ClientState,
) -> CommandOutcome {
    if args.len() < 2 {
        return wrong_arity("lpos");
    }

    let key = &args[0];
    let element = &args[1];

    let mut rank: i64 = 1;
    let mut count: Option<usize> = None;
    let mut maxlen: Option<usize> = None;

    let mut idx = 2usize;
    while idx < args.len() {
        let option = to_uppercase_bytes(&args[idx]);
        match option.as_slice() {
            b"RANK" => {
                if idx + 1 >= args.len() {
                    return CommandOutcome::reply(err("ERR syntax error"));
                }
                let Some(parsed_rank) = parse_i64(&args[idx + 1]) else {
                    return CommandOutcome::reply(err(
                        "ERR value is not an integer or out of range",
                    ));
                };
                if parsed_rank == 0 {
                    return CommandOutcome::reply(err("ERR RANK can't be zero"));
                }
                rank = parsed_rank;
                idx += 2;
            }
            b"COUNT" => {
                if idx + 1 >= args.len() {
                    return CommandOutcome::reply(err("ERR syntax error"));
                }
                let Some(parsed_count) = parse_i64(&args[idx + 1]) else {
                    return CommandOutcome::reply(err(
                        "ERR value is not an integer or out of range",
                    ));
                };
                if parsed_count < 0 {
                    return CommandOutcome::reply(err("ERR COUNT can't be negative"));
                }
                let Ok(parsed_count) = usize::try_from(parsed_count) else {
                    return CommandOutcome::reply(err(
                        "ERR value is not an integer or out of range",
                    ));
                };
                count = Some(parsed_count);
                idx += 2;
            }
            b"MAXLEN" => {
                if idx + 1 >= args.len() {
                    return CommandOutcome::reply(err("ERR syntax error"));
                }
                let Some(parsed_maxlen) = parse_i64(&args[idx + 1]) else {
                    return CommandOutcome::reply(err(
                        "ERR value is not an integer or out of range",
                    ));
                };
                if parsed_maxlen < 0 {
                    return CommandOutcome::reply(err("ERR MAXLEN can't be negative"));
                }
                let Ok(parsed_maxlen) = usize::try_from(parsed_maxlen) else {
                    return CommandOutcome::reply(err(
                        "ERR value is not an integer or out of range",
                    ));
                };
                maxlen = Some(parsed_maxlen);
                idx += 2;
            }
            _ => return CommandOutcome::reply(err("ERR syntax error")),
        }
    }

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
    let Some(list) = entry.as_list() else {
        return wrong_type_response();
    };

    let scan_limit = maxlen.unwrap_or(list.len()).min(list.len());
    if count == Some(0) {
        return CommandOutcome::reply(RespFrame::Array(vec![]));
    }
    let mut positions = Vec::new();

    if rank > 0 {
        let Ok(target) = usize::try_from(rank) else {
            return CommandOutcome::reply(err("ERR value is not an integer or out of range"));
        };
        let mut seen = 0usize;
        for (pos, item) in list.iter().enumerate().take(scan_limit) {
            if *item == *element {
                seen += 1;
                if seen >= target {
                    positions.push(pos as i64);
                    if count.is_none() {
                        break;
                    }
                    if count.is_some_and(|v| v > 0 && positions.len() >= v) {
                        break;
                    }
                }
            }
        }
    } else {
        let Ok(target) = usize::try_from(rank.saturating_neg()) else {
            return CommandOutcome::reply(err("ERR value is not an integer or out of range"));
        };
        let mut seen = 0usize;
        for pos in (0..scan_limit).rev() {
            if list[pos] == *element {
                seen += 1;
                if seen >= target {
                    positions.push(pos as i64);
                    if count.is_none() {
                        break;
                    }
                    if count.is_some_and(|v| v > 0 && positions.len() >= v) {
                        break;
                    }
                }
            }
        }
    }

    if count.is_some() {
        return CommandOutcome::reply(RespFrame::Array(
            positions
                .into_iter()
                .map(RespFrame::Integer)
                .collect::<Vec<_>>(),
        ));
    }

    if let Some(first) = positions.first() {
        CommandOutcome::reply(RespFrame::Integer(*first))
    } else {
        CommandOutcome::reply(RespFrame::BulkString(None))
    }
}

pub(super) fn cmd_lset(
    args: &[Bytes],
    server: &mut ServerState,
    client: &ClientState,
) -> CommandOutcome {
    let [key, index_raw, value] = args else {
        return wrong_arity("lset");
    };

    let Some(raw_index) = parse_i64(index_raw) else {
        return CommandOutcome::reply(err("ERR value is not an integer or out of range"));
    };

    let now = now_ms();
    let db = server.db_mut(client.selected_db);
    purge_expired_key(db, key, now);

    let Some(entry) = db.get_mut(key) else {
        return CommandOutcome::reply(err("ERR no such key"));
    };
    let Some(list) = entry.as_list_mut() else {
        return wrong_type_response();
    };

    let len = list.len() as i64;
    let index = if raw_index < 0 {
        len.saturating_add(raw_index)
    } else {
        raw_index
    };

    if index < 0 || index >= len {
        return CommandOutcome::reply(err("ERR index out of range"));
    }

    let idx = index as usize;
    list[idx] = value.clone();
    CommandOutcome::reply(RespFrame::ok())
}

pub(super) fn cmd_lindex(
    args: &[Bytes],
    server: &mut ServerState,
    client: &ClientState,
) -> CommandOutcome {
    let [key, index_raw] = args else {
        return wrong_arity("lindex");
    };

    let Some(raw_index) = parse_i64(index_raw) else {
        return CommandOutcome::reply(err("ERR value is not an integer or out of range"));
    };

    let now = now_ms();
    let db = server.db_mut(client.selected_db);
    purge_expired_key(db, key, now);

    let Some(entry) = db.get(key) else {
        return CommandOutcome::reply(RespFrame::BulkString(None));
    };
    let Some(list) = entry.as_list() else {
        return wrong_type_response();
    };

    let len = list.len() as i64;
    let index = if raw_index < 0 {
        len.saturating_add(raw_index)
    } else {
        raw_index
    };

    if index < 0 || index >= len {
        return CommandOutcome::reply(RespFrame::BulkString(None));
    }

    let idx = index as usize;
    CommandOutcome::reply(RespFrame::BulkString(Some(list[idx].clone())))
}

pub(super) fn cmd_linsert(
    args: &[Bytes],
    server: &mut ServerState,
    client: &ClientState,
) -> CommandOutcome {
    let [key, position_raw, pivot, element] = args else {
        return wrong_arity("linsert");
    };

    let upper = to_uppercase_bytes(position_raw);
    let before = match upper.as_slice() {
        b"BEFORE" => true,
        b"AFTER" => false,
        _ => return CommandOutcome::reply(err("ERR syntax error")),
    };

    let now = now_ms();
    let db = server.db_mut(client.selected_db);
    purge_expired_key(db, key, now);

    let Some(entry) = db.get_mut(key) else {
        return CommandOutcome::reply(RespFrame::Integer(0));
    };
    let Some(list) = entry.as_list_mut() else {
        return wrong_type_response();
    };

    let Some(pos) = list.iter().position(|item| item == pivot) else {
        return CommandOutcome::reply(RespFrame::Integer(-1));
    };

    let insert_at = if before { pos } else { pos + 1 };
    list.insert(insert_at, element.clone());

    CommandOutcome::reply(RespFrame::Integer(list.len() as i64))
}

pub(super) fn cmd_ltrim(
    args: &[Bytes],
    server: &mut ServerState,
    client: &ClientState,
) -> CommandOutcome {
    let [key, start_raw, end_raw] = args else {
        return wrong_arity("ltrim");
    };

    let Some(start) = parse_i64(start_raw) else {
        return CommandOutcome::reply(err("ERR value is not an integer or out of range"));
    };
    let Some(end) = parse_i64(end_raw) else {
        return CommandOutcome::reply(err("ERR value is not an integer or out of range"));
    };

    let now = now_ms();
    let db = server.db_mut(client.selected_db);
    purge_expired_key(db, key, now);

    let Some(entry) = db.get_mut(key) else {
        return CommandOutcome::reply(RespFrame::ok());
    };
    let Some(list) = entry.as_list_mut() else {
        return wrong_type_response();
    };

    let Some((range_start, range_end)) = normalize_range(list.len(), start, end) else {
        db.remove(key);
        return CommandOutcome::reply(RespFrame::ok());
    };

    let count = range_end.saturating_sub(range_start).saturating_add(1);
    let trimmed = list
        .iter()
        .skip(range_start)
        .take(count)
        .cloned()
        .collect::<Vec<_>>();
    *list = VecDeque::from(trimmed);

    CommandOutcome::reply(RespFrame::ok())
}
