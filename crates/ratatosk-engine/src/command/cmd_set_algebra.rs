use bytes::Bytes;

use hashbrown::HashSet;
use ratatosk_resp::frame::RespFrame;

use crate::keyspace::{ServerState, StoredValue, purge_expired_key};

use super::{
    ClientState, CommandOutcome, err, now_ms, parse_i64, parse_usize, wrong_arity,
    wrong_type_response,
};

fn set_result_array(result: HashSet<Bytes>) -> RespFrame {
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
    let mut db = server.db_mut(client.selected_db);
    purge_expired_key(&mut db, first_key, now);

    let Some(first_entry) = db.get(first_key) else {
        return CommandOutcome::reply(RespFrame::Array(vec![]));
    };
    let Some(mut result) = first_entry.set_as_hashset() else {
        return wrong_type_response();
    };

    for key in others {
        purge_expired_key(&mut db, key, now);
        let Some(entry) = db.get(key) else {
            continue;
        };
        let Some(other_set) = entry.set_members() else {
            return wrong_type_response();
        };
        for member in &other_set {
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
    let mut db = server.db_mut(client.selected_db);
    for key in args {
        purge_expired_key(&mut db, key, now);
    }

    let mut sets = Vec::with_capacity(args.len());
    for key in args {
        let Some(entry) = db.get(key) else {
            return CommandOutcome::reply(RespFrame::Array(vec![]));
        };
        if !entry.is_set() {
            return wrong_type_response();
        }
        sets.push(entry);
    }

    let mut smallest_idx = 0usize;
    let mut smallest_len = usize::MAX;
    for (idx, entry) in sets.iter().enumerate() {
        let Some(len) = entry.set_len() else {
            return wrong_type_response();
        };
        if len < smallest_len {
            smallest_len = len;
            smallest_idx = idx;
        }
    }

    if smallest_len == 0 {
        return CommandOutcome::reply(RespFrame::Array(vec![]));
    }

    let Some(base_set) = sets[smallest_idx].set_members() else {
        return wrong_type_response();
    };
    let mut result = Vec::with_capacity(base_set.len());
    for member in &base_set {
        if sets
            .iter()
            .enumerate()
            .filter(|(idx, _)| *idx != smallest_idx)
            .all(|(_, entry)| entry.set_contains(member).unwrap_or(false))
        {
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
    let mut db = server.db_mut(client.selected_db);
    for key in keys {
        purge_expired_key(&mut db, key, now);
    }

    let mut smallest_idx = 0usize;
    let mut smallest_len = usize::MAX;
    for (i, key) in keys.iter().enumerate() {
        let Some(entry) = db.get(key) else {
            return CommandOutcome::reply(RespFrame::Integer(0));
        };
        let Some(len) = entry.set_len() else {
            return wrong_type_response();
        };
        if len < smallest_len {
            smallest_len = len;
            smallest_idx = i;
        }
    }

    if smallest_len == 0 {
        return CommandOutcome::reply(RespFrame::Integer(0));
    }

    let Some(base_entry) = db.get(&keys[smallest_idx]) else {
        return CommandOutcome::reply(RespFrame::Integer(0));
    };
    let Some(base_set) = base_entry.set_members() else {
        return wrong_type_response();
    };

    let mut cardinality = 0i64;
    for member in &base_set {
        let mut present_in_all = true;
        for (i, key) in keys.iter().enumerate() {
            if i == smallest_idx {
                continue;
            }
            let Some(entry) = db.get(key) else {
                present_in_all = false;
                break;
            };
            let Some(contains) = entry.set_contains(member) else {
                return wrong_type_response();
            };
            if !contains {
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
    let mut db = server.db_mut(client.selected_db);
    let mut result = HashSet::new();

    for key in args {
        purge_expired_key(&mut db, key, now);
        let Some(entry) = db.get(key) else {
            continue;
        };
        let Some(set) = entry.set_members() else {
            return wrong_type_response();
        };
        result.extend(set);
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
    let mut db = server.db_mut(client.selected_db);
    purge_expired_key(&mut db, destination, now);
    purge_expired_key(&mut db, &source_keys[0], now);

    let mut result = match db.get(&source_keys[0]) {
        None => HashSet::new(),
        Some(entry) => match entry.set_as_hashset() {
            Some(set) => set,
            None => return wrong_type_response(),
        },
    };

    for key in &source_keys[1..] {
        purge_expired_key(&mut db, key, now);
        let Some(entry) = db.get(key) else {
            continue;
        };
        let Some(set) = entry.set_members() else {
            return wrong_type_response();
        };
        for member in &set {
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
    let mut db = server.db_mut(client.selected_db);
    purge_expired_key(&mut db, destination, now);
    for key in source_keys {
        purge_expired_key(&mut db, key, now);
    }

    let result = {
        let mut sets = Vec::with_capacity(source_keys.len());
        for key in source_keys {
            let Some(entry) = db.get(key) else {
                sets.clear();
                break;
            };
            if !entry.is_set() {
                return wrong_type_response();
            }
            sets.push(entry);
        }

        if sets.is_empty() {
            HashSet::new()
        } else {
            let mut smallest_idx = 0usize;
            let mut smallest_len = usize::MAX;
            for (idx, entry) in sets.iter().enumerate() {
                let Some(len) = entry.set_len() else {
                    return wrong_type_response();
                };
                if len < smallest_len {
                    smallest_len = len;
                    smallest_idx = idx;
                }
            }

            if smallest_len == 0 {
                HashSet::new()
            } else {
                let Some(base_set) = sets[smallest_idx].set_members() else {
                    return wrong_type_response();
                };
                let mut result = HashSet::with_capacity(base_set.len());
                for member in &base_set {
                    if sets
                        .iter()
                        .enumerate()
                        .filter(|(idx, _)| *idx != smallest_idx)
                        .all(|(_, entry)| entry.set_contains(member).unwrap_or(false))
                    {
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
    let mut db = server.db_mut(client.selected_db);
    purge_expired_key(&mut db, destination, now);
    let mut result = HashSet::new();

    for key in source_keys {
        purge_expired_key(&mut db, key, now);
        let Some(entry) = db.get(key) else {
            continue;
        };
        let Some(set) = entry.set_members() else {
            return wrong_type_response();
        };
        result.extend(set);
    }

    let stored_len = result.len() as i64;
    if result.is_empty() {
        db.remove(destination);
    } else {
        db.insert(destination.clone(), StoredValue::set(result, None));
    }

    CommandOutcome::reply(RespFrame::Integer(stored_len))
}
