use std::collections::VecDeque;

use bytes::Bytes;

use ratatosk_resp::frame::RespFrame;

use crate::keyspace::{ServerState, StoredValue, purge_expired_key};
use crate::object::parse_f64;

use super::cmd_list_pop::{MAX_LIST_NUMKEYS, MAX_LIST_POP_COUNT};
use super::{
    ClientState, CommandOutcome, err, now_ms, parse_i64, to_uppercase_bytes, wrong_arity,
    wrong_type_response,
};

fn blocking_watch_keys(client: &ClientState, keys: &[Bytes]) -> Vec<(usize, Bytes)> {
    keys.iter()
        .cloned()
        .map(|key| (client.selected_db(), key))
        .collect()
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
    CommandOutcome::blocking(
        outcome.response,
        deadline_ms,
        full_frame,
        blocking_watch_keys(client, keys),
    )
}

pub(super) fn try_bpop_once(
    keys: &[Bytes],
    server: &mut ServerState,
    client: &ClientState,
    left: bool,
) -> CommandOutcome {
    let now = now_ms();
    let mut db = server.db_mut(client.selected_db);

    for key in keys {
        purge_expired_key(&mut db, key, now);

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
    let mut db = server.db_mut(client.selected_db);
    purge_expired_key(&mut db, source, now);
    if source != destination {
        purge_expired_key(&mut db, destination, now);
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
    CommandOutcome::blocking(
        outcome.response,
        deadline_ms,
        full_frame,
        blocking_watch_keys(client, std::slice::from_ref(source)),
    )
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
    CommandOutcome::blocking(
        outcome.response,
        deadline_ms,
        full_frame,
        blocking_watch_keys(client, std::slice::from_ref(source)),
    )
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
    CommandOutcome::blocking(
        outcome.response,
        deadline_ms,
        full_frame,
        blocking_watch_keys(client, &keys),
    )
}

pub(super) fn try_lmpop_once(
    keys: &[Bytes],
    left: bool,
    count: usize,
    server: &mut ServerState,
    client: &ClientState,
) -> CommandOutcome {
    let now = now_ms();
    let mut db = server.db_mut(client.selected_db);

    for key in keys {
        purge_expired_key(&mut db, key, now);

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
