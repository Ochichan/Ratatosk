use bytes::Bytes;

use hashbrown::{HashMap, HashSet};
use ratatosk_resp::frame::RespFrame;

use crate::keyspace::{
    ServerState, StoredValue, StreamConsumer, StreamEntry, StreamGroup, StreamId, purge_expired_key,
};

use super::cmd_stream::{
    parse_stream_id, stream_entry_frame, stream_id_to_bytes, stream_nogroup_error,
};
use super::{
    ClientState, CommandOutcome, err, now_ms, parse_i64, parse_usize, wrong_arity,
    wrong_type_response,
};

fn stream_contains_id(stream: &[StreamEntry], id: StreamId) -> bool {
    stream.binary_search_by_key(&id, |entry| entry.id).is_ok()
}

pub(super) fn prune_stream_removed_ids(entry: &mut StoredValue, removed_ids: &[StreamId]) {
    if removed_ids.is_empty() {
        return;
    }

    let removed = removed_ids.iter().copied().collect::<HashSet<_>>();
    let Some(groups) = entry.as_stream_groups_mut() else {
        return;
    };

    for group in groups.values_mut() {
        group.pending.retain(|id, _| !removed.contains(id));
        for consumer in group.consumers.values_mut() {
            consumer.pending.retain(|id| !removed.contains(id));
        }
    }
}

pub(super) fn claim_pending_id(
    group: &mut StreamGroup,
    consumer_name: &Bytes,
    id: StreamId,
    now: i64,
) -> bool {
    let Some(pending) = group.pending.get_mut(&id) else {
        return false;
    };

    let previous_consumer = pending.consumer.clone();
    if previous_consumer != *consumer_name {
        if let Some(previous) = group.consumers.get_mut(&previous_consumer) {
            previous.pending.remove(&id);
        }
        pending.consumer = consumer_name.clone();
    }

    pending.deliveries = pending.deliveries.saturating_add(1);
    pending.last_delivered_ms = now;

    let consumer = group
        .consumers
        .entry(consumer_name.clone())
        .or_insert_with(|| StreamConsumer {
            seen_time_ms: now,
            pending: HashSet::new(),
        });
    consumer.seen_time_ms = now;
    consumer.pending.insert(id);

    true
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum StreamDeleteCondition {
    KeepRef,
    DelRef,
    Acked,
}

pub(super) fn parse_stream_delete_condition(raw: &Bytes) -> Option<StreamDeleteCondition> {
    if raw.eq_ignore_ascii_case(b"KEEPREF") {
        Some(StreamDeleteCondition::KeepRef)
    } else if raw.eq_ignore_ascii_case(b"DELREF") {
        Some(StreamDeleteCondition::DelRef)
    } else if raw.eq_ignore_ascii_case(b"ACKED") {
        Some(StreamDeleteCondition::Acked)
    } else {
        None
    }
}

pub(super) fn parse_stream_ids_block(
    args: &[Bytes],
    idx: usize,
) -> Result<Vec<StreamId>, RespFrame> {
    if idx >= args.len() || !args[idx].eq_ignore_ascii_case(b"IDS") {
        return Err(err("ERR syntax error"));
    }

    let Some(num_ids_raw) = args.get(idx + 1) else {
        return Err(err("ERR syntax error"));
    };
    let Some(num_ids) = parse_usize(num_ids_raw) else {
        return Err(err("ERR value is not an integer or out of range"));
    };

    let start = idx.saturating_add(2);
    let end = start.saturating_add(num_ids);
    if end != args.len() {
        return Err(err("ERR syntax error"));
    }

    let mut ids = Vec::with_capacity(num_ids);
    for raw_id in &args[start..end] {
        let Some(id) = parse_stream_id(raw_id) else {
            return Err(err(
                "ERR Invalid stream ID specified as stream command argument",
            ));
        };
        ids.push(id);
    }

    Ok(ids)
}

pub(super) fn stream_has_pending_references(
    groups: &HashMap<Bytes, StreamGroup>,
    id: StreamId,
) -> bool {
    groups.values().any(|group| group.pending.contains_key(&id))
}

pub(super) fn purge_stream_pending_id(groups: &mut HashMap<Bytes, StreamGroup>, id: StreamId) {
    for group in groups.values_mut() {
        group.pending.remove(&id);
        for consumer in group.consumers.values_mut() {
            consumer.pending.remove(&id);
        }
    }
}

pub(super) fn cmd_xtrim(
    args: &[Bytes],
    server: &mut ServerState,
    client: &ClientState,
) -> CommandOutcome {
    if args.len() < 3 {
        return wrong_arity("xtrim");
    }

    let key = &args[0];
    let strategy = &args[1];

    let mut idx = 2usize;
    if idx < args.len() && (args[idx].as_ref() == b"=" || args[idx].as_ref() == b"~") {
        idx += 1;
    }
    if idx >= args.len() {
        return CommandOutcome::reply(err("ERR syntax error"));
    }

    enum TrimStrategy {
        MaxLen(usize),
        MinId(StreamId),
    }

    let mode = if strategy.eq_ignore_ascii_case(b"MAXLEN") {
        let Some(parsed) = parse_usize(&args[idx]) else {
            return CommandOutcome::reply(err("ERR value is not an integer or out of range"));
        };
        TrimStrategy::MaxLen(parsed)
    } else if strategy.eq_ignore_ascii_case(b"MINID") {
        let Some(parsed) = parse_stream_id(&args[idx]) else {
            return CommandOutcome::reply(err(
                "ERR Invalid stream ID specified as stream command argument",
            ));
        };
        TrimStrategy::MinId(parsed)
    } else {
        return CommandOutcome::reply(err("ERR syntax error"));
    };
    idx += 1;

    let mut limit = None;
    while idx < args.len() {
        if !args[idx].eq_ignore_ascii_case(b"LIMIT") {
            return CommandOutcome::reply(err("ERR syntax error"));
        }
        let Some(raw_limit) = args.get(idx + 1) else {
            return CommandOutcome::reply(err("ERR syntax error"));
        };
        let Some(parsed_limit) = parse_usize(raw_limit) else {
            return CommandOutcome::reply(err("ERR value is not an integer or out of range"));
        };
        limit = Some(parsed_limit);
        idx += 2;
    }

    let now = now_ms();
    let mut db = server.db_mut(client.selected_db);
    purge_expired_key(&mut db, key, now);

    let Some(entry) = db.get_mut(key) else {
        return CommandOutcome::reply(RespFrame::Integer(0));
    };
    let Some(stream) = entry.as_stream_entries_mut() else {
        return wrong_type_response();
    };

    let mut remove_count = match mode {
        TrimStrategy::MaxLen(max_len) => stream.len().saturating_sub(max_len),
        TrimStrategy::MinId(min_id) => stream.iter().take_while(|item| item.id < min_id).count(),
    };

    if let Some(max_remove) = limit {
        remove_count = remove_count.min(max_remove);
    }

    if remove_count == 0 {
        return CommandOutcome::reply(RespFrame::Integer(0));
    }

    let removed_ids = stream
        .drain(0..remove_count)
        .map(|entry| entry.id)
        .collect::<Vec<_>>();
    prune_stream_removed_ids(entry, &removed_ids);

    CommandOutcome::reply(RespFrame::Integer(removed_ids.len() as i64))
}

pub(super) fn cmd_xdel(
    args: &[Bytes],
    server: &mut ServerState,
    client: &ClientState,
) -> CommandOutcome {
    let [key, ids @ ..] = args else {
        return wrong_arity("xdel");
    };
    if ids.is_empty() {
        return wrong_arity("xdel");
    }

    let mut id_set = HashSet::new();
    for raw_id in ids {
        let Some(id) = parse_stream_id(raw_id) else {
            return CommandOutcome::reply(err(
                "ERR Invalid stream ID specified as stream command argument",
            ));
        };
        id_set.insert(id);
    }

    let now = now_ms();
    let mut db = server.db_mut(client.selected_db);
    purge_expired_key(&mut db, key, now);

    let Some(entry) = db.get_mut(key) else {
        return CommandOutcome::reply(RespFrame::Integer(0));
    };
    let Some(stream) = entry.as_stream_entries_mut() else {
        return wrong_type_response();
    };

    let mut removed_ids = Vec::new();
    stream.retain(|item| {
        if id_set.remove(&item.id) {
            removed_ids.push(item.id);
            false
        } else {
            true
        }
    });

    prune_stream_removed_ids(entry, &removed_ids);
    CommandOutcome::reply(RespFrame::Integer(removed_ids.len() as i64))
}

pub(super) fn cmd_xclaim(
    args: &[Bytes],
    server: &mut ServerState,
    client: &ClientState,
) -> CommandOutcome {
    if args.len() < 5 {
        return wrong_arity("xclaim");
    }

    let key = &args[0];
    let group_name = &args[1];
    let consumer_name = args[2].clone();

    let Some(min_idle) = parse_i64(&args[3]) else {
        return CommandOutcome::reply(err("ERR value is not an integer or out of range"));
    };
    if min_idle < 0 {
        return CommandOutcome::reply(err("ERR value is not an integer or out of range"));
    }

    let mut justid = false;
    let mut ids = Vec::new();
    let mut idx = 4usize;
    while idx < args.len() {
        if args[idx].eq_ignore_ascii_case(b"JUSTID") {
            justid = true;
            idx += 1;
            continue;
        }

        let Some(parsed_id) = parse_stream_id(&args[idx]) else {
            return CommandOutcome::reply(err("ERR syntax error"));
        };
        ids.push(parsed_id);
        idx += 1;
    }

    if ids.is_empty() {
        return wrong_arity("xclaim");
    }

    let now = now_ms();
    let mut db = server.db_mut(client.selected_db);
    purge_expired_key(&mut db, key, now);

    let Some(entry) = db.get_mut(key) else {
        return stream_nogroup_error(key, group_name);
    };
    if entry.as_stream_entries().is_none() {
        return wrong_type_response();
    }

    let claimed_ids = {
        let Some(groups) = entry.as_stream_groups_mut() else {
            return stream_nogroup_error(key, group_name);
        };
        let Some(group) = groups.get_mut(group_name) else {
            return stream_nogroup_error(key, group_name);
        };

        let mut claimed = Vec::new();
        for id in ids {
            let Some(pending) = group.pending.get(&id) else {
                continue;
            };
            let idle = now.saturating_sub(pending.last_delivered_ms);
            if idle < min_idle {
                continue;
            }

            if claim_pending_id(group, &consumer_name, id, now) {
                claimed.push(id);
            }
        }
        claimed
    };

    let out = if justid {
        claimed_ids
            .iter()
            .copied()
            .map(|id| RespFrame::BulkString(Some(stream_id_to_bytes(id))))
            .collect::<Vec<_>>()
    } else {
        let Some(stream) = entry.as_stream_entries() else {
            return wrong_type_response();
        };

        let mut order_by_id = HashMap::with_capacity(claimed_ids.len());
        for (idx, id) in claimed_ids.iter().copied().enumerate() {
            order_by_id.insert(id, idx);
        }

        let mut ordered_rows: Vec<Option<RespFrame>> = vec![None; claimed_ids.len()];
        let mut filled = 0usize;
        for item in stream {
            if let Some(position) = order_by_id.get(&item.id).copied() {
                if ordered_rows[position].is_none() {
                    ordered_rows[position] = Some(stream_entry_frame(item));
                    filled = filled.saturating_add(1);
                    if filled >= claimed_ids.len() {
                        break;
                    }
                }
            }
        }

        ordered_rows.into_iter().flatten().collect::<Vec<_>>()
    };

    CommandOutcome::reply(RespFrame::Array(out))
}

pub(super) fn cmd_xautoclaim(
    args: &[Bytes],
    server: &mut ServerState,
    client: &ClientState,
) -> CommandOutcome {
    if args.len() < 5 {
        return wrong_arity("xautoclaim");
    }

    let key = &args[0];
    let group_name = &args[1];
    let consumer_name = args[2].clone();

    let Some(min_idle) = parse_i64(&args[3]) else {
        return CommandOutcome::reply(err("ERR value is not an integer or out of range"));
    };
    if min_idle < 0 {
        return CommandOutcome::reply(err("ERR value is not an integer or out of range"));
    }

    let Some(start) = parse_stream_id(&args[4]) else {
        return CommandOutcome::reply(err(
            "ERR Invalid stream ID specified as stream command argument",
        ));
    };

    let mut count = 100usize;
    let mut justid = false;
    let mut idx = 5usize;
    while idx < args.len() {
        if args[idx].eq_ignore_ascii_case(b"COUNT") {
            let Some(raw_count) = args.get(idx + 1) else {
                return CommandOutcome::reply(err("ERR syntax error"));
            };
            let Some(parsed_count) = parse_usize(raw_count) else {
                return CommandOutcome::reply(err("ERR value is not an integer or out of range"));
            };
            count = parsed_count;
            idx += 2;
            continue;
        }
        if args[idx].eq_ignore_ascii_case(b"JUSTID") {
            justid = true;
            idx += 1;
            continue;
        }

        return CommandOutcome::reply(err("ERR syntax error"));
    }

    let now = now_ms();
    let mut db = server.db_mut(client.selected_db);
    purge_expired_key(&mut db, key, now);

    let Some(entry) = db.get_mut(key) else {
        return stream_nogroup_error(key, group_name);
    };
    if entry.as_stream_entries().is_none() {
        return wrong_type_response();
    }

    let (claimed_ids, next_cursor) = {
        let Some(groups) = entry.as_stream_groups_mut() else {
            return stream_nogroup_error(key, group_name);
        };
        let Some(group) = groups.get_mut(group_name) else {
            return stream_nogroup_error(key, group_name);
        };

        let mut pending_ids = group.pending.keys().copied().collect::<Vec<_>>();
        pending_ids.sort_unstable();

        let mut claimed_ids = Vec::new();
        let mut next_cursor = StreamId { ms: 0, seq: 0 };

        for (pos, id) in pending_ids.iter().copied().enumerate() {
            if id < start {
                continue;
            }

            let Some(pending) = group.pending.get(&id) else {
                continue;
            };
            let idle = now.saturating_sub(pending.last_delivered_ms);
            if idle < min_idle {
                continue;
            }

            if claim_pending_id(group, &consumer_name, id, now) {
                claimed_ids.push(id);
                if claimed_ids.len() >= count {
                    next_cursor = pending_ids
                        .get(pos + 1)
                        .copied()
                        .unwrap_or(StreamId { ms: 0, seq: 0 });
                    break;
                }
            }
        }

        (claimed_ids, next_cursor)
    };

    let entries = if justid {
        claimed_ids
            .iter()
            .copied()
            .map(|id| RespFrame::BulkString(Some(stream_id_to_bytes(id))))
            .collect::<Vec<_>>()
    } else {
        let Some(stream) = entry.as_stream_entries() else {
            return wrong_type_response();
        };

        let mut order_by_id = HashMap::with_capacity(claimed_ids.len());
        for (idx, id) in claimed_ids.iter().copied().enumerate() {
            order_by_id.insert(id, idx);
        }

        let mut ordered_rows: Vec<Option<RespFrame>> = vec![None; claimed_ids.len()];
        let mut filled = 0usize;
        for item in stream {
            if let Some(position) = order_by_id.get(&item.id).copied() {
                if ordered_rows[position].is_none() {
                    ordered_rows[position] = Some(stream_entry_frame(item));
                    filled = filled.saturating_add(1);
                    if filled >= claimed_ids.len() {
                        break;
                    }
                }
            }
        }

        ordered_rows.into_iter().flatten().collect::<Vec<_>>()
    };

    CommandOutcome::reply(RespFrame::Array(vec![
        RespFrame::BulkString(Some(stream_id_to_bytes(next_cursor))),
        RespFrame::Array(entries),
        RespFrame::Array(vec![]),
    ]))
}

pub(super) fn cmd_xackdel(
    args: &[Bytes],
    server: &mut ServerState,
    client: &ClientState,
) -> CommandOutcome {
    if args.len() < 5 {
        return wrong_arity("xackdel");
    }

    let key = &args[0];
    let group_name = &args[1];

    let mut idx = 2usize;
    let mut condition = StreamDeleteCondition::KeepRef;
    if idx < args.len() && !args[idx].eq_ignore_ascii_case(b"IDS") {
        let Some(parsed) = parse_stream_delete_condition(&args[idx]) else {
            return CommandOutcome::reply(err("ERR syntax error"));
        };
        condition = parsed;
        idx += 1;
    }

    let ids = match parse_stream_ids_block(args, idx) {
        Ok(ids) => ids,
        Err(response) => return CommandOutcome::reply(response),
    };

    let now = now_ms();
    let mut db = server.db_mut(client.selected_db);
    purge_expired_key(&mut db, key, now);

    let Some(entry) = db.get_mut(key) else {
        return CommandOutcome::reply(RespFrame::Array(
            ids.into_iter()
                .map(|_| RespFrame::Integer(-1))
                .collect::<Vec<_>>(),
        ));
    };
    if !entry.is_stream() {
        return wrong_type_response();
    }
    let Some(groups) = entry.as_stream_groups() else {
        return stream_nogroup_error(key, group_name);
    };
    if !groups.contains_key(group_name) {
        return stream_nogroup_error(key, group_name);
    }

    let mut out = Vec::with_capacity(ids.len());
    let mut removed_ids = HashSet::new();
    for id in ids {
        let exists = entry
            .as_stream_entries()
            .is_some_and(|stream| stream_contains_id(stream, id));
        if !exists {
            out.push(RespFrame::Integer(-1));
            continue;
        }

        let acked = if let Some(groups_mut) = entry.as_stream_groups_mut() {
            if let Some(group) = groups_mut.get_mut(group_name) {
                if let Some(pending) = group.pending.remove(&id) {
                    if let Some(consumer) = group.consumers.get_mut(&pending.consumer) {
                        consumer.pending.remove(&id);
                    }
                    true
                } else {
                    false
                }
            } else {
                false
            }
        } else {
            false
        };

        if !acked {
            out.push(RespFrame::Integer(-1));
            continue;
        }

        let has_pending_elsewhere = entry
            .as_stream_groups()
            .is_some_and(|groups_ref| stream_has_pending_references(groups_ref, id));

        match condition {
            StreamDeleteCondition::KeepRef => {
                removed_ids.insert(id);
                out.push(RespFrame::Integer(if has_pending_elsewhere {
                    2
                } else {
                    1
                }));
            }
            StreamDeleteCondition::DelRef => {
                removed_ids.insert(id);
                if let Some(groups_mut) = entry.as_stream_groups_mut() {
                    purge_stream_pending_id(groups_mut, id);
                }
                out.push(RespFrame::Integer(1));
            }
            StreamDeleteCondition::Acked => {
                if has_pending_elsewhere {
                    out.push(RespFrame::Integer(2));
                } else {
                    removed_ids.insert(id);
                    out.push(RespFrame::Integer(1));
                }
            }
        }
    }

    if !removed_ids.is_empty() {
        if let Some(stream) = entry.as_stream_entries_mut() {
            stream.retain(|item| !removed_ids.contains(&item.id));
        }
    }

    CommandOutcome::reply(RespFrame::Array(out))
}

pub(super) fn cmd_xdelex(
    args: &[Bytes],
    server: &mut ServerState,
    client: &ClientState,
) -> CommandOutcome {
    if args.len() < 4 {
        return wrong_arity("xdelex");
    }

    let key = &args[0];

    let mut idx = 1usize;
    let mut condition = StreamDeleteCondition::KeepRef;
    if idx < args.len() && !args[idx].eq_ignore_ascii_case(b"IDS") {
        let Some(parsed) = parse_stream_delete_condition(&args[idx]) else {
            return CommandOutcome::reply(err("ERR syntax error"));
        };
        condition = parsed;
        idx += 1;
    }

    let ids = match parse_stream_ids_block(args, idx) {
        Ok(ids) => ids,
        Err(response) => return CommandOutcome::reply(response),
    };

    let now = now_ms();
    let mut db = server.db_mut(client.selected_db);
    purge_expired_key(&mut db, key, now);

    let Some(entry) = db.get_mut(key) else {
        return CommandOutcome::reply(RespFrame::Array(
            ids.into_iter()
                .map(|_| RespFrame::Integer(-1))
                .collect::<Vec<_>>(),
        ));
    };
    if !entry.is_stream() {
        return wrong_type_response();
    }

    let mut out = Vec::with_capacity(ids.len());
    let mut removed_ids = HashSet::new();
    for id in ids {
        let exists = entry
            .as_stream_entries()
            .is_some_and(|stream| stream_contains_id(stream, id));
        if !exists {
            out.push(RespFrame::Integer(-1));
            continue;
        }

        let has_pending = entry
            .as_stream_groups()
            .is_some_and(|groups| stream_has_pending_references(groups, id));

        match condition {
            StreamDeleteCondition::KeepRef => {
                removed_ids.insert(id);
                out.push(RespFrame::Integer(if has_pending { 2 } else { 1 }));
            }
            StreamDeleteCondition::DelRef => {
                removed_ids.insert(id);
                if let Some(groups_mut) = entry.as_stream_groups_mut() {
                    purge_stream_pending_id(groups_mut, id);
                }
                out.push(RespFrame::Integer(1));
            }
            StreamDeleteCondition::Acked => {
                if has_pending {
                    out.push(RespFrame::Integer(2));
                } else {
                    removed_ids.insert(id);
                    out.push(RespFrame::Integer(1));
                }
            }
        }
    }

    if !removed_ids.is_empty() {
        if let Some(stream) = entry.as_stream_entries_mut() {
            stream.retain(|item| !removed_ids.contains(&item.id));
        }
    }

    CommandOutcome::reply(RespFrame::Array(out))
}
