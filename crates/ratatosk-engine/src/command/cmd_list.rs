use std::collections::VecDeque;

use bytes::Bytes;

use ratatosk_resp::frame::RespFrame;

use crate::keyspace::{ServerState, StoredValue, purge_expired_key};
use crate::object::normalize_range;

use super::{
    ClientState, CommandOutcome, err, now_ms, parse_i64, to_uppercase_bytes, wrong_arity,
    wrong_type_response,
};

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
    let mut db = server.db_mut(client.selected_db);
    purge_expired_key(&mut db, key, now);

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
    let mut db = server.db_mut(client.selected_db);
    purge_expired_key(&mut db, key, now);

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
    let mut db = server.db_mut(client.selected_db);
    purge_expired_key(&mut db, key, now);

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
    let mut db = server.db_mut(client.selected_db);
    purge_expired_key(&mut db, key, now);

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
    let mut db = server.db_mut(client.selected_db);
    purge_expired_key(&mut db, key, now);

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
    let mut db = server.db_mut(client.selected_db);
    purge_expired_key(&mut db, key, now);

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
    let mut db = server.db_mut(client.selected_db);
    purge_expired_key(&mut db, key, now);

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
    let mut db = server.db_mut(client.selected_db);
    purge_expired_key(&mut db, key, now);

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
    let mut db = server.db_mut(client.selected_db);
    purge_expired_key(&mut db, key, now);

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
    let mut db = server.db_mut(client.selected_db);
    purge_expired_key(&mut db, key, now);

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
