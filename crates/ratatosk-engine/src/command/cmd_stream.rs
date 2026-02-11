use std::fmt::Write as _;

use bytes::Bytes;

use hashbrown::{HashMap, HashSet};
use ratatosk_resp::frame::RespFrame;

use crate::keyspace::{
    ServerState, StoredValue, StreamConsumer, StreamEntry, StreamGroup, StreamId,
    StreamPendingEntry, purge_expired_key,
};

use super::{
    ClientState, CommandOutcome, err, now_ms, parse_i64, parse_usize, to_uppercase_bytes,
    wrong_arity, wrong_type_response,
};

pub(super) fn stream_id_to_bytes(id: StreamId) -> Bytes {
    let mut text = String::with_capacity(48);
    let _ = write!(&mut text, "{}-{}", id.ms, id.seq);
    Bytes::from(text)
}

pub(super) fn parse_stream_id(raw: &Bytes) -> Option<StreamId> {
    let text = std::str::from_utf8(raw).ok()?;
    let (ms_raw, seq_raw) = text.split_once('-')?;
    if ms_raw.is_empty() || seq_raw.is_empty() {
        return None;
    }

    let ms = ms_raw.parse::<i64>().ok()?;
    let seq = seq_raw.parse::<i64>().ok()?;
    if ms < 0 || seq < 0 {
        return None;
    }

    Some(StreamId { ms, seq })
}

pub(super) fn parse_stream_range_bound(raw: &Bytes) -> Option<StreamId> {
    if raw.as_ref() == b"-" {
        return Some(StreamId { ms: 0, seq: 0 });
    }
    if raw.as_ref() == b"+" {
        return Some(StreamId {
            ms: i64::MAX,
            seq: i64::MAX,
        });
    }

    let id = parse_stream_id(raw)?;
    Some(id)
}

pub(super) fn next_stream_id(entries: &[StreamEntry]) -> StreamId {
    let now = now_ms();
    let Some(last) = entries.last() else {
        return StreamId { ms: now, seq: 0 };
    };

    if now > last.id.ms {
        StreamId { ms: now, seq: 0 }
    } else {
        StreamId {
            ms: last.id.ms,
            seq: last.id.seq.saturating_add(1),
        }
    }
}

pub(super) fn stream_entry_frame(entry: &StreamEntry) -> RespFrame {
    let mut fields = Vec::with_capacity(entry.fields.len().saturating_mul(2));
    for (field, value) in &entry.fields {
        fields.push(RespFrame::BulkString(Some(field.clone())));
        fields.push(RespFrame::BulkString(Some(value.clone())));
    }

    RespFrame::Array(vec![
        RespFrame::BulkString(Some(stream_id_to_bytes(entry.id))),
        RespFrame::Array(fields),
    ])
}

fn stream_contains_id(stream: &[StreamEntry], id: StreamId) -> bool {
    stream.binary_search_by_key(&id, |entry| entry.id).is_ok()
}

fn blocking_deadline_ms_from_block(block_ms: i64) -> Option<i64> {
    if block_ms <= 0 {
        None
    } else {
        Some(now_ms().saturating_add(block_ms))
    }
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

pub(super) fn cmd_xadd(
    args: &[Bytes],
    server: &mut ServerState,
    client: &ClientState,
) -> CommandOutcome {
    if args.len() < 4 || args.len() % 2 != 0 {
        return wrong_arity("xadd");
    }

    let key = &args[0];
    let id_raw = &args[1];

    let now = now_ms();
    let db = server.db_mut(client.selected_db);
    purge_expired_key(db, key, now);

    if !db.contains_key(key) {
        db.insert(key.clone(), StoredValue::stream(Vec::new(), None));
    }

    let Some(entry) = db.get_mut(key) else {
        return CommandOutcome::reply(err("ERR internal error"));
    };
    let Some(stream) = entry.as_stream_entries_mut() else {
        return wrong_type_response();
    };

    let id = if id_raw.as_ref() == b"*" {
        next_stream_id(stream)
    } else {
        let Some(parsed) = parse_stream_id(id_raw) else {
            return CommandOutcome::reply(err(
                "ERR Invalid stream ID specified as stream command argument",
            ));
        };
        parsed
    };

    if id.ms == 0 && id.seq == 0 {
        return CommandOutcome::reply(err("ERR The ID specified in XADD must be greater than 0-0"));
    }

    if stream.last().is_some_and(|last| id <= last.id) {
        return CommandOutcome::reply(err(
            "ERR The ID specified in XADD is equal or smaller than the target stream top item",
        ));
    }

    let mut fields = Vec::with_capacity((args.len() - 2) / 2);
    let mut idx = 2usize;
    while idx < args.len() {
        fields.push((args[idx].clone(), args[idx + 1].clone()));
        idx += 2;
    }

    stream.push(StreamEntry { id, fields });
    CommandOutcome::reply(RespFrame::BulkString(Some(stream_id_to_bytes(id))))
}

pub(super) fn cmd_xlen(
    args: &[Bytes],
    server: &mut ServerState,
    client: &ClientState,
) -> CommandOutcome {
    let [key] = args else {
        return wrong_arity("xlen");
    };

    let now = now_ms();
    let db = server.db_mut(client.selected_db);
    purge_expired_key(db, key, now);

    let Some(entry) = db.get(key) else {
        return CommandOutcome::reply(RespFrame::Integer(0));
    };
    let Some(stream) = entry.as_stream_entries() else {
        return wrong_type_response();
    };

    CommandOutcome::reply(RespFrame::Integer(stream.len() as i64))
}

pub(super) fn cmd_xrange(
    args: &[Bytes],
    server: &mut ServerState,
    client: &ClientState,
    reverse: bool,
) -> CommandOutcome {
    if args.len() != 3 && args.len() != 5 {
        return if reverse {
            wrong_arity("xrevrange")
        } else {
            wrong_arity("xrange")
        };
    }

    let key = &args[0];

    let count = if args.len() == 5 {
        if !args[3].eq_ignore_ascii_case(b"COUNT") {
            return CommandOutcome::reply(err("ERR syntax error"));
        }
        let Some(parsed) = parse_usize(&args[4]) else {
            return CommandOutcome::reply(err("ERR value is not an integer or out of range"));
        };
        Some(parsed)
    } else {
        None
    };

    let low_high = if reverse {
        let Some(high) = parse_stream_range_bound(&args[1]) else {
            return CommandOutcome::reply(err(
                "ERR Invalid stream ID specified as stream command argument",
            ));
        };
        let Some(low) = parse_stream_range_bound(&args[2]) else {
            return CommandOutcome::reply(err(
                "ERR Invalid stream ID specified as stream command argument",
            ));
        };
        (low, high)
    } else {
        let Some(low) = parse_stream_range_bound(&args[1]) else {
            return CommandOutcome::reply(err(
                "ERR Invalid stream ID specified as stream command argument",
            ));
        };
        let Some(high) = parse_stream_range_bound(&args[2]) else {
            return CommandOutcome::reply(err(
                "ERR Invalid stream ID specified as stream command argument",
            ));
        };
        (low, high)
    };

    let now = now_ms();
    let db = server.db_mut(client.selected_db);
    purge_expired_key(db, key, now);

    let Some(entry) = db.get(key) else {
        return CommandOutcome::reply(RespFrame::Array(vec![]));
    };
    let Some(stream) = entry.as_stream_entries() else {
        return wrong_type_response();
    };

    let (low, high) = low_high;
    if low > high {
        return CommandOutcome::reply(RespFrame::Array(vec![]));
    }

    let mut rows = stream
        .iter()
        .filter(|item| item.id >= low && item.id <= high)
        .map(stream_entry_frame)
        .collect::<Vec<_>>();

    if reverse {
        rows.reverse();
    }
    if let Some(limit) = count {
        rows.truncate(limit);
    }

    CommandOutcome::reply(RespFrame::Array(rows))
}

pub(super) fn cmd_xread(
    args: &[Bytes],
    server: &mut ServerState,
    client: &ClientState,
) -> CommandOutcome {
    if args.len() < 3 {
        return wrong_arity("xread");
    }

    let mut idx = 0usize;
    let mut count = None;
    let mut block_ms = None;

    while idx < args.len() {
        if args[idx].eq_ignore_ascii_case(b"STREAMS") {
            idx += 1;
            break;
        }

        if args[idx].eq_ignore_ascii_case(b"COUNT") {
            let Some(raw) = args.get(idx + 1) else {
                return CommandOutcome::reply(err("ERR syntax error"));
            };
            let Some(parsed) = parse_usize(raw) else {
                return CommandOutcome::reply(err("ERR value is not an integer or out of range"));
            };
            count = Some(parsed);
            idx += 2;
            continue;
        }

        if args[idx].eq_ignore_ascii_case(b"BLOCK") {
            let Some(raw) = args.get(idx + 1) else {
                return CommandOutcome::reply(err("ERR syntax error"));
            };
            let Some(parsed) = parse_i64(raw) else {
                return CommandOutcome::reply(err("ERR value is not an integer or out of range"));
            };
            if parsed < 0 {
                return CommandOutcome::reply(err("ERR value is not an integer or out of range"));
            }
            block_ms = Some(parsed);
            idx += 2;
            continue;
        }

        return CommandOutcome::reply(err("ERR syntax error"));
    }

    if idx >= args.len() {
        return CommandOutcome::reply(err("ERR syntax error"));
    }

    let tail = &args[idx..];
    if tail.len() < 2 || tail.len() % 2 != 0 {
        return CommandOutcome::reply(err("ERR syntax error"));
    }

    let stream_count = tail.len() / 2;
    let keys = &tail[..stream_count];
    let ids = &tail[stream_count..];

    let mut out = Vec::new();
    let now = now_ms();
    let mut normalized_ids = if block_ms.is_some() {
        Some(Vec::with_capacity(stream_count))
    } else {
        None
    };

    for (key, id_raw) in keys.iter().zip(ids.iter()) {
        let db = server.db_mut(client.selected_db);
        purge_expired_key(db, key, now);

        let stream = match db.get(key) {
            Some(entry) => {
                let Some(stream) = entry.as_stream_entries() else {
                    return wrong_type_response();
                };
                Some(stream)
            }
            None => None,
        };

        let threshold = if id_raw.as_ref() == b"$" {
            stream
                .and_then(|entries| entries.last().map(|last| last.id))
                .unwrap_or(StreamId { ms: 0, seq: 0 })
        } else {
            let Some(parsed) = parse_stream_id(id_raw) else {
                return CommandOutcome::reply(err(
                    "ERR Invalid stream ID specified as stream command argument",
                ));
            };
            parsed
        };

        if let Some(normalized_ids) = normalized_ids.as_mut() {
            normalized_ids.push(stream_id_to_bytes(threshold));
        }

        let Some(stream) = stream else {
            continue;
        };

        let rows = if let Some(limit) = count {
            stream
                .iter()
                .filter(|item| item.id > threshold)
                .take(limit)
                .map(stream_entry_frame)
                .collect::<Vec<_>>()
        } else {
            stream
                .iter()
                .filter(|item| item.id > threshold)
                .map(stream_entry_frame)
                .collect::<Vec<_>>()
        };

        if rows.is_empty() {
            continue;
        }

        out.push(RespFrame::Array(vec![
            RespFrame::BulkString(Some(key.clone())),
            RespFrame::Array(rows),
        ]));
    }

    if out.is_empty() {
        if let Some(block_ms) = block_ms {
            let deadline_ms = blocking_deadline_ms_from_block(block_ms);
            if deadline_ms.is_some_and(|deadline| now_ms() >= deadline) {
                return CommandOutcome::reply(RespFrame::Null);
            }

            let mut blocking_args = args.to_vec();
            if let Some(normalized_ids) = normalized_ids {
                let ids_start = idx + stream_count;
                for (offset, normalized_id) in normalized_ids.into_iter().enumerate() {
                    blocking_args[ids_start + offset] = normalized_id;
                }
            }

            let full_frame = build_blocking_frame("XREAD", &blocking_args);
            CommandOutcome::blocking(RespFrame::Null, deadline_ms, full_frame)
        } else {
            CommandOutcome::reply(RespFrame::Null)
        }
    } else {
        CommandOutcome::reply(RespFrame::Array(out))
    }
}

pub(super) fn xreadgroup_nogroup_error(key: &Bytes, group: &Bytes) -> CommandOutcome {
    CommandOutcome::reply(err(&format!(
        "NOGROUP No such key '{}' or consumer group '{}' in XREADGROUP with GROUP option",
        String::from_utf8_lossy(key),
        String::from_utf8_lossy(group)
    )))
}

pub(super) fn stream_nogroup_error(key: &Bytes, group: &Bytes) -> CommandOutcome {
    CommandOutcome::reply(err(&format!(
        "NOGROUP No such key '{}' or consumer group '{}'",
        String::from_utf8_lossy(key),
        String::from_utf8_lossy(group)
    )))
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

pub(super) fn cmd_xgroup(
    args: &[Bytes],
    server: &mut ServerState,
    client: &ClientState,
) -> CommandOutcome {
    if args.is_empty() {
        return wrong_arity("xgroup");
    }

    let subcommand = to_uppercase_bytes(&args[0]);
    match subcommand.as_slice() {
        b"HELP" => {
            if args.len() != 1 {
                return wrong_arity("xgroup");
            }
            CommandOutcome::reply(RespFrame::Array(vec![
                RespFrame::bulk_str(
                    "CREATE <key> <groupname> <id|$> [MKSTREAM] -- Create a consumer group.",
                ),
                RespFrame::bulk_str("DESTROY <key> <groupname> -- Destroy a consumer group."),
                RespFrame::bulk_str(
                    "CREATECONSUMER <key> <groupname> <consumer> -- Create consumer in group.",
                ),
                RespFrame::bulk_str(
                    "DELCONSUMER <key> <groupname> <consumer> -- Delete consumer from group.",
                ),
                RespFrame::bulk_str(
                    "SETID <key> <groupname> <id|$> -- Set group's last delivered ID.",
                ),
                RespFrame::bulk_str("HELP -- Show this help."),
            ]))
        }
        b"CREATE" => {
            if args.len() != 4 && args.len() != 5 {
                return wrong_arity("xgroup");
            }

            let key = &args[1];
            let group_name = &args[2];
            let id_raw = &args[3];
            let mkstream = if args.len() == 5 {
                if !args[4].eq_ignore_ascii_case(b"MKSTREAM") {
                    return CommandOutcome::reply(err("ERR syntax error"));
                }
                true
            } else {
                false
            };

            let now = now_ms();
            let db = server.db_mut(client.selected_db);
            purge_expired_key(db, key, now);

            if !db.contains_key(key) && mkstream {
                db.insert(key.clone(), StoredValue::stream(Vec::new(), None));
            }

            let Some(entry) = db.get_mut(key) else {
                return CommandOutcome::reply(err(
                    "ERR The XGROUP subcommand requires the key to exist. Note that for CREATE you may want to use the MKSTREAM option to create an empty stream automatically.",
                ));
            };
            let Some((stream, groups)) = entry.as_stream_mut() else {
                return wrong_type_response();
            };

            if groups.contains_key(group_name) {
                return CommandOutcome::reply(err("BUSYGROUP Consumer Group name already exists"));
            }

            let id = if id_raw.as_ref() == b"$" {
                stream
                    .last()
                    .map(|item| item.id)
                    .unwrap_or(StreamId { ms: 0, seq: 0 })
            } else {
                let Some(parsed) = parse_stream_id(id_raw) else {
                    return CommandOutcome::reply(err(
                        "ERR Invalid stream ID specified as stream command argument",
                    ));
                };
                parsed
            };

            groups.insert(
                group_name.clone(),
                StreamGroup {
                    last_delivered_id: id,
                    consumers: HashMap::new(),
                    pending: HashMap::new(),
                },
            );

            CommandOutcome::reply(RespFrame::ok())
        }
        b"DESTROY" => {
            let [_, key, group_name] = args else {
                return wrong_arity("xgroup");
            };

            let now = now_ms();
            let db = server.db_mut(client.selected_db);
            purge_expired_key(db, key, now);

            let Some(entry) = db.get_mut(key) else {
                return CommandOutcome::reply(RespFrame::Integer(0));
            };
            if !entry.is_stream() {
                return wrong_type_response();
            }
            let Some(groups) = entry.as_stream_groups_mut() else {
                return CommandOutcome::reply(RespFrame::Integer(0));
            };

            let removed = if groups.remove(group_name).is_some() {
                1
            } else {
                0
            };
            CommandOutcome::reply(RespFrame::Integer(removed))
        }
        b"SETID" => {
            let [_, key, group_name, id_raw] = args else {
                return wrong_arity("xgroup");
            };

            let now = now_ms();
            let db = server.db_mut(client.selected_db);
            purge_expired_key(db, key, now);

            let Some(entry) = db.get_mut(key) else {
                return xreadgroup_nogroup_error(key, group_name);
            };
            let Some((stream, groups)) = entry.as_stream_mut() else {
                return wrong_type_response();
            };
            let Some(group) = groups.get_mut(group_name) else {
                return xreadgroup_nogroup_error(key, group_name);
            };

            group.last_delivered_id = if id_raw.as_ref() == b"$" {
                stream
                    .last()
                    .map(|item| item.id)
                    .unwrap_or(StreamId { ms: 0, seq: 0 })
            } else {
                let Some(parsed) = parse_stream_id(id_raw) else {
                    return CommandOutcome::reply(err(
                        "ERR Invalid stream ID specified as stream command argument",
                    ));
                };
                parsed
            };

            CommandOutcome::reply(RespFrame::ok())
        }
        b"CREATECONSUMER" => {
            let [_, key, group_name, consumer_name] = args else {
                return wrong_arity("xgroup");
            };

            let now = now_ms();
            let db = server.db_mut(client.selected_db);
            purge_expired_key(db, key, now);

            let Some(entry) = db.get_mut(key) else {
                return xreadgroup_nogroup_error(key, group_name);
            };
            if !entry.is_stream() {
                return wrong_type_response();
            }
            let Some(groups) = entry.as_stream_groups_mut() else {
                return xreadgroup_nogroup_error(key, group_name);
            };
            let Some(group) = groups.get_mut(group_name) else {
                return xreadgroup_nogroup_error(key, group_name);
            };

            let created = if group.consumers.contains_key(consumer_name) {
                0
            } else {
                group.consumers.insert(
                    consumer_name.clone(),
                    StreamConsumer {
                        seen_time_ms: now,
                        pending: HashSet::new(),
                    },
                );
                1
            };

            CommandOutcome::reply(RespFrame::Integer(created))
        }
        b"DELCONSUMER" => {
            let [_, key, group_name, consumer_name] = args else {
                return wrong_arity("xgroup");
            };

            let now = now_ms();
            let db = server.db_mut(client.selected_db);
            purge_expired_key(db, key, now);

            let Some(entry) = db.get_mut(key) else {
                return xreadgroup_nogroup_error(key, group_name);
            };
            if !entry.is_stream() {
                return wrong_type_response();
            }
            let Some(groups) = entry.as_stream_groups_mut() else {
                return xreadgroup_nogroup_error(key, group_name);
            };
            let Some(group) = groups.get_mut(group_name) else {
                return xreadgroup_nogroup_error(key, group_name);
            };

            let removed = group
                .consumers
                .remove(consumer_name)
                .map_or(0i64, |consumer| consumer.pending.len() as i64);
            if removed > 0 {
                group
                    .pending
                    .retain(|_, pending| pending.consumer != *consumer_name);
            }

            CommandOutcome::reply(RespFrame::Integer(removed))
        }
        _ => CommandOutcome::reply(err(
            "ERR Unknown XGROUP subcommand or wrong number of arguments for XGROUP",
        )),
    }
}

pub(super) fn cmd_xreadgroup(
    args: &[Bytes],
    server: &mut ServerState,
    client: &ClientState,
) -> CommandOutcome {
    if args.len() < 6 {
        return wrong_arity("xreadgroup");
    }
    if !args[0].eq_ignore_ascii_case(b"GROUP") {
        return CommandOutcome::reply(err("ERR syntax error"));
    }

    let group_name = args[1].clone();
    let consumer_name = args[2].clone();

    let mut idx = 3usize;
    let mut count = None;
    let mut block_ms = None;
    let mut noack = false;

    while idx < args.len() {
        if args[idx].eq_ignore_ascii_case(b"STREAMS") {
            idx += 1;
            break;
        }

        if args[idx].eq_ignore_ascii_case(b"COUNT") {
            let Some(raw) = args.get(idx + 1) else {
                return CommandOutcome::reply(err("ERR syntax error"));
            };
            let Some(parsed) = parse_usize(raw) else {
                return CommandOutcome::reply(err("ERR value is not an integer or out of range"));
            };
            count = Some(parsed);
            idx += 2;
            continue;
        }

        if args[idx].eq_ignore_ascii_case(b"BLOCK") {
            let Some(raw) = args.get(idx + 1) else {
                return CommandOutcome::reply(err("ERR syntax error"));
            };
            let Some(parsed) = parse_i64(raw) else {
                return CommandOutcome::reply(err("ERR value is not an integer or out of range"));
            };
            if parsed < 0 {
                return CommandOutcome::reply(err("ERR value is not an integer or out of range"));
            }
            block_ms = Some(parsed);
            idx += 2;
            continue;
        }

        if args[idx].eq_ignore_ascii_case(b"NOACK") {
            noack = true;
            idx += 1;
            continue;
        }

        return CommandOutcome::reply(err("ERR syntax error"));
    }

    if idx >= args.len() {
        return CommandOutcome::reply(err("ERR syntax error"));
    }

    let tail = &args[idx..];
    if tail.len() < 2 || tail.len() % 2 != 0 {
        return CommandOutcome::reply(err("ERR syntax error"));
    }

    let stream_count = tail.len() / 2;
    let keys = &tail[..stream_count];
    let ids = &tail[stream_count..];

    let mut out = Vec::new();
    let now = now_ms();

    for (key, id_raw) in keys.iter().zip(ids.iter()) {
        let db = server.db_mut(client.selected_db);
        purge_expired_key(db, key, now);

        let Some(entry) = db.get_mut(key) else {
            return xreadgroup_nogroup_error(key, &group_name);
        };
        if entry.as_stream_entries().is_none() {
            return wrong_type_response();
        }

        {
            let Some(groups) = entry.as_stream_groups_mut() else {
                return xreadgroup_nogroup_error(key, &group_name);
            };
            let Some(group) = groups.get_mut(&group_name) else {
                return xreadgroup_nogroup_error(key, &group_name);
            };

            group
                .consumers
                .entry(consumer_name.clone())
                .or_insert_with(|| StreamConsumer {
                    seen_time_ms: now,
                    pending: HashSet::new(),
                })
                .seen_time_ms = now;
        }

        let selected_ids = {
            let Some(stream) = entry.as_stream_entries() else {
                return wrong_type_response();
            };
            let Some(groups) = entry.as_stream_groups() else {
                return xreadgroup_nogroup_error(key, &group_name);
            };
            let Some(group) = groups.get(&group_name) else {
                return xreadgroup_nogroup_error(key, &group_name);
            };

            if id_raw.as_ref() == b">" {
                if let Some(limit) = count {
                    stream
                        .iter()
                        .filter(|item| item.id > group.last_delivered_id)
                        .take(limit)
                        .map(|item| item.id)
                        .collect::<Vec<_>>()
                } else {
                    stream
                        .iter()
                        .filter(|item| item.id > group.last_delivered_id)
                        .map(|item| item.id)
                        .collect::<Vec<_>>()
                }
            } else {
                let Some(parsed) = parse_stream_id(id_raw) else {
                    return CommandOutcome::reply(err(
                        "ERR Invalid stream ID specified as stream command argument",
                    ));
                };

                if let Some(limit) = count {
                    stream
                        .iter()
                        .filter(|item| item.id > parsed)
                        .filter(|item| {
                            group
                                .pending
                                .get(&item.id)
                                .is_some_and(|pending| pending.consumer == consumer_name)
                        })
                        .take(limit)
                        .map(|item| item.id)
                        .collect::<Vec<_>>()
                } else {
                    stream
                        .iter()
                        .filter(|item| item.id > parsed)
                        .filter(|item| {
                            group
                                .pending
                                .get(&item.id)
                                .is_some_and(|pending| pending.consumer == consumer_name)
                        })
                        .map(|item| item.id)
                        .collect::<Vec<_>>()
                }
            }
        };

        if selected_ids.is_empty() {
            continue;
        }

        {
            let Some(groups) = entry.as_stream_groups_mut() else {
                return xreadgroup_nogroup_error(key, &group_name);
            };
            let Some(group) = groups.get_mut(&group_name) else {
                return xreadgroup_nogroup_error(key, &group_name);
            };

            if id_raw.as_ref() == b">" {
                if !noack {
                    let consumer_state = group
                        .consumers
                        .entry(consumer_name.clone())
                        .or_insert_with(|| StreamConsumer {
                            seen_time_ms: now,
                            pending: HashSet::new(),
                        });

                    for id in &selected_ids {
                        consumer_state.pending.insert(*id);
                        group
                            .pending
                            .entry(*id)
                            .and_modify(|pending| {
                                pending.consumer = consumer_name.clone();
                                pending.deliveries = pending.deliveries.saturating_add(1);
                                pending.last_delivered_ms = now;
                            })
                            .or_insert_with(|| StreamPendingEntry {
                                consumer: consumer_name.clone(),
                                deliveries: 1,
                                last_delivered_ms: now,
                            });
                    }
                }

                if let Some(last) = selected_ids.last() {
                    group.last_delivered_id = *last;
                }
            } else if !noack {
                for id in &selected_ids {
                    if let Some(pending) = group.pending.get_mut(id) {
                        pending.deliveries = pending.deliveries.saturating_add(1);
                        pending.last_delivered_ms = now;
                    }
                }
            }
        }

        let rows = {
            let Some(stream) = entry.as_stream_entries() else {
                return wrong_type_response();
            };

            let mut rows = Vec::with_capacity(selected_ids.len());
            let mut selected_iter = selected_ids.iter().copied();
            if let Some(mut target_id) = selected_iter.next() {
                for item in stream {
                    if item.id == target_id {
                        rows.push(stream_entry_frame(item));
                        if let Some(next_id) = selected_iter.next() {
                            target_id = next_id;
                        } else {
                            break;
                        }
                    }
                }
            }

            rows
        };

        out.push(RespFrame::Array(vec![
            RespFrame::BulkString(Some(key.clone())),
            RespFrame::Array(rows),
        ]));
    }

    if out.is_empty() {
        if let Some(block_ms) = block_ms {
            let deadline_ms = blocking_deadline_ms_from_block(block_ms);
            if deadline_ms.is_some_and(|deadline| now_ms() >= deadline) {
                return CommandOutcome::reply(RespFrame::Null);
            }
            let full_frame = build_blocking_frame("XREADGROUP", args);
            CommandOutcome::blocking(RespFrame::Null, deadline_ms, full_frame)
        } else {
            CommandOutcome::reply(RespFrame::Null)
        }
    } else {
        CommandOutcome::reply(RespFrame::Array(out))
    }
}

pub(super) fn cmd_xack(
    args: &[Bytes],
    server: &mut ServerState,
    client: &ClientState,
) -> CommandOutcome {
    let [key, group_name, ids @ ..] = args else {
        return wrong_arity("xack");
    };
    if ids.is_empty() {
        return wrong_arity("xack");
    }

    let now = now_ms();
    let db = server.db_mut(client.selected_db);
    purge_expired_key(db, key, now);

    let Some(entry) = db.get_mut(key) else {
        return CommandOutcome::reply(RespFrame::Integer(0));
    };
    if !entry.is_stream() {
        return wrong_type_response();
    }
    let Some(groups) = entry.as_stream_groups_mut() else {
        return xreadgroup_nogroup_error(key, group_name);
    };
    let Some(group) = groups.get_mut(group_name) else {
        return xreadgroup_nogroup_error(key, group_name);
    };

    let mut removed = 0i64;
    for id_raw in ids {
        let Some(id) = parse_stream_id(id_raw) else {
            return CommandOutcome::reply(err(
                "ERR Invalid stream ID specified as stream command argument",
            ));
        };

        if let Some(pending) = group.pending.remove(&id) {
            if let Some(consumer) = group.consumers.get_mut(&pending.consumer) {
                consumer.pending.remove(&id);
            }
            removed += 1;
        }
    }

    CommandOutcome::reply(RespFrame::Integer(removed))
}

pub(super) fn cmd_xpending(
    args: &[Bytes],
    server: &mut ServerState,
    client: &ClientState,
) -> CommandOutcome {
    if args.len() < 2 {
        return wrong_arity("xpending");
    }

    let key = &args[0];
    let group_name = &args[1];

    let now = now_ms();
    let db = server.db_mut(client.selected_db);
    purge_expired_key(db, key, now);

    let Some(entry) = db.get_mut(key) else {
        return xreadgroup_nogroup_error(key, group_name);
    };
    if !entry.is_stream() {
        return wrong_type_response();
    }
    let Some(groups) = entry.as_stream_groups_mut() else {
        return xreadgroup_nogroup_error(key, group_name);
    };
    let Some(group) = groups.get_mut(group_name) else {
        return xreadgroup_nogroup_error(key, group_name);
    };

    if args.len() == 2 {
        if group.pending.is_empty() {
            return CommandOutcome::reply(RespFrame::Array(vec![
                RespFrame::Integer(0),
                RespFrame::BulkString(None),
                RespFrame::BulkString(None),
                RespFrame::Array(vec![]),
            ]));
        }

        let mut ids = group.pending.keys().copied().collect::<Vec<_>>();
        ids.sort();

        let mut by_consumer: HashMap<&Bytes, i64> = HashMap::new();
        for pending in group.pending.values() {
            *by_consumer.entry(&pending.consumer).or_insert(0) += 1;
        }
        let mut consumers = by_consumer.into_iter().collect::<Vec<_>>();
        consumers.sort_by(|a, b| a.0.cmp(b.0));

        return CommandOutcome::reply(RespFrame::Array(vec![
            RespFrame::Integer(group.pending.len() as i64),
            RespFrame::BulkString(Some(stream_id_to_bytes(ids[0]))),
            RespFrame::BulkString(Some(stream_id_to_bytes(ids[ids.len() - 1]))),
            RespFrame::Array(
                consumers
                    .into_iter()
                    .map(|(consumer, count)| {
                        RespFrame::Array(vec![
                            RespFrame::BulkString(Some((*consumer).clone())),
                            RespFrame::Integer(count),
                        ])
                    })
                    .collect(),
            ),
        ]));
    }

    if args.len() != 5 && args.len() != 6 {
        return wrong_arity("xpending");
    }

    let Some(start) = parse_stream_range_bound(&args[2]) else {
        return CommandOutcome::reply(err(
            "ERR Invalid stream ID specified as stream command argument",
        ));
    };
    let Some(end) = parse_stream_range_bound(&args[3]) else {
        return CommandOutcome::reply(err(
            "ERR Invalid stream ID specified as stream command argument",
        ));
    };
    let Some(count) = parse_usize(&args[4]) else {
        return CommandOutcome::reply(err("ERR value is not an integer or out of range"));
    };
    let consumer_filter = args.get(5);

    let mut pending_ids = group
        .pending
        .iter()
        .filter(|(id, pending)| {
            **id >= start
                && **id <= end
                && consumer_filter.is_none_or(|consumer| pending.consumer == *consumer)
        })
        .map(|(id, _)| *id)
        .collect::<Vec<_>>();
    pending_ids.sort();
    pending_ids.truncate(count);

    CommandOutcome::reply(RespFrame::Array(
        pending_ids
            .into_iter()
            .filter_map(|id| {
                group.pending.get(&id).map(|pending| {
                    RespFrame::Array(vec![
                        RespFrame::BulkString(Some(stream_id_to_bytes(id))),
                        RespFrame::BulkString(Some(pending.consumer.clone())),
                        RespFrame::Integer(now.saturating_sub(pending.last_delivered_ms)),
                        RespFrame::Integer(pending.deliveries),
                    ])
                })
            })
            .collect(),
    ))
}

pub(super) fn cmd_xinfo(
    args: &[Bytes],
    server: &mut ServerState,
    client: &ClientState,
) -> CommandOutcome {
    if args.is_empty() {
        return wrong_arity("xinfo");
    }

    let subcommand = to_uppercase_bytes(&args[0]);
    match subcommand.as_slice() {
        b"HELP" => {
            if args.len() != 1 {
                return wrong_arity("xinfo");
            }
            CommandOutcome::reply(RespFrame::Array(vec![
                RespFrame::bulk_str("STREAM <key> [FULL [COUNT <count>]] -- Show stream metadata."),
                RespFrame::bulk_str("GROUPS <key> -- Show consumer groups for a stream."),
                RespFrame::bulk_str(
                    "CONSUMERS <key> <group> -- Show consumers and pending counts for a group.",
                ),
                RespFrame::bulk_str("HELP -- Show this help."),
            ]))
        }
        b"STREAM" => {
            if args.len() < 2 {
                return wrong_arity("xinfo");
            }

            let key = &args[1];
            let mut full = false;
            let mut count = None;

            let mut idx = 2usize;
            while idx < args.len() {
                if args[idx].eq_ignore_ascii_case(b"FULL") {
                    full = true;
                    idx += 1;
                    continue;
                }
                if args[idx].eq_ignore_ascii_case(b"COUNT") {
                    let Some(raw) = args.get(idx + 1) else {
                        return CommandOutcome::reply(err("ERR syntax error"));
                    };
                    let Some(parsed) = parse_usize(raw) else {
                        return CommandOutcome::reply(err(
                            "ERR value is not an integer or out of range",
                        ));
                    };
                    count = Some(parsed);
                    idx += 2;
                    continue;
                }

                return CommandOutcome::reply(err("ERR syntax error"));
            }

            let now = now_ms();
            let db = server.db_mut(client.selected_db);
            purge_expired_key(db, key, now);

            let Some(entry) = db.get(key) else {
                return CommandOutcome::reply(err("ERR no such key"));
            };
            let Some((stream, groups_ref)) = entry.as_stream() else {
                return wrong_type_response();
            };

            let first_id = stream
                .first()
                .map(|item| item.id)
                .unwrap_or(StreamId { ms: 0, seq: 0 });
            let last_id = stream
                .last()
                .map(|item| item.id)
                .unwrap_or(StreamId { ms: 0, seq: 0 });
            let group_count = groups_ref.len();

            let mut out = vec![
                RespFrame::bulk_str("length"),
                RespFrame::Integer(stream.len() as i64),
                RespFrame::bulk_str("radix-tree-keys"),
                RespFrame::Integer(stream.len() as i64),
                RespFrame::bulk_str("radix-tree-nodes"),
                RespFrame::Integer(if stream.is_empty() { 0 } else { 1 }),
                RespFrame::bulk_str("last-generated-id"),
                RespFrame::BulkString(Some(stream_id_to_bytes(last_id))),
                RespFrame::bulk_str("max-deleted-entry-id"),
                RespFrame::bulk_str("0-0"),
                RespFrame::bulk_str("entries-added"),
                RespFrame::Integer(stream.len() as i64),
                RespFrame::bulk_str("recorded-first-entry-id"),
                RespFrame::BulkString(Some(stream_id_to_bytes(first_id))),
                RespFrame::bulk_str("groups"),
                RespFrame::Integer(group_count as i64),
                RespFrame::bulk_str("first-entry"),
                stream
                    .first()
                    .map(stream_entry_frame)
                    .unwrap_or(RespFrame::Null),
                RespFrame::bulk_str("last-entry"),
                stream
                    .last()
                    .map(stream_entry_frame)
                    .unwrap_or(RespFrame::Null),
            ];

            if full {
                let limit = count.unwrap_or(10);
                let entries = stream
                    .iter()
                    .take(limit)
                    .map(stream_entry_frame)
                    .collect::<Vec<_>>();
                out.push(RespFrame::bulk_str("entries"));
                out.push(RespFrame::Array(entries));
            }

            CommandOutcome::reply(RespFrame::Array(out))
        }
        b"GROUPS" => {
            let [_, key] = args else {
                return wrong_arity("xinfo");
            };

            let now = now_ms();
            let db = server.db_mut(client.selected_db);
            purge_expired_key(db, key, now);

            let Some(entry) = db.get(key) else {
                return CommandOutcome::reply(err("ERR no such key"));
            };
            let Some(groups_ref) = entry.as_stream_groups() else {
                return wrong_type_response();
            };

            let mut group_rows = groups_ref.iter().collect::<Vec<_>>();
            group_rows.sort_by(|a, b| a.0.cmp(b.0));

            let out = group_rows
                .into_iter()
                .map(|(name, group)| {
                    RespFrame::Array(vec![
                        RespFrame::bulk_str("name"),
                        RespFrame::BulkString(Some(name.clone())),
                        RespFrame::bulk_str("consumers"),
                        RespFrame::Integer(group.consumers.len() as i64),
                        RespFrame::bulk_str("pending"),
                        RespFrame::Integer(group.pending.len() as i64),
                        RespFrame::bulk_str("last-delivered-id"),
                        RespFrame::BulkString(Some(stream_id_to_bytes(group.last_delivered_id))),
                    ])
                })
                .collect::<Vec<_>>();

            CommandOutcome::reply(RespFrame::Array(out))
        }
        b"CONSUMERS" => {
            let [_, key, group_name] = args else {
                return wrong_arity("xinfo");
            };

            let now = now_ms();
            let db = server.db_mut(client.selected_db);
            purge_expired_key(db, key, now);

            let Some(entry) = db.get(key) else {
                return stream_nogroup_error(key, group_name);
            };
            if !entry.is_stream() {
                return wrong_type_response();
            }
            let Some(groups) = entry.as_stream_groups() else {
                return stream_nogroup_error(key, group_name);
            };
            let Some(group) = groups.get(group_name) else {
                return stream_nogroup_error(key, group_name);
            };

            let mut consumers = group.consumers.iter().collect::<Vec<_>>();
            consumers.sort_by(|a, b| a.0.cmp(b.0));

            let out = consumers
                .into_iter()
                .map(|(name, consumer)| {
                    RespFrame::Array(vec![
                        RespFrame::bulk_str("name"),
                        RespFrame::BulkString(Some(name.clone())),
                        RespFrame::bulk_str("pending"),
                        RespFrame::Integer(consumer.pending.len() as i64),
                        RespFrame::bulk_str("idle"),
                        RespFrame::Integer(now.saturating_sub(consumer.seen_time_ms)),
                    ])
                })
                .collect::<Vec<_>>();

            CommandOutcome::reply(RespFrame::Array(out))
        }
        _ => CommandOutcome::reply(err("ERR unknown subcommand for XINFO")),
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
    let db = server.db_mut(client.selected_db);
    purge_expired_key(db, key, now);

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
    let db = server.db_mut(client.selected_db);
    purge_expired_key(db, key, now);

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

pub(super) fn cmd_xsetid(
    args: &[Bytes],
    server: &mut ServerState,
    client: &ClientState,
) -> CommandOutcome {
    let [key, id_raw] = args else {
        return wrong_arity("xsetid");
    };

    let Some(new_id) = parse_stream_id(id_raw) else {
        return CommandOutcome::reply(err(
            "ERR Invalid stream ID specified as stream command argument",
        ));
    };

    let now = now_ms();
    let db = server.db_mut(client.selected_db);
    purge_expired_key(db, key, now);

    let Some(entry) = db.get(key) else {
        return CommandOutcome::reply(err("ERR no such key"));
    };
    let Some(stream) = entry.as_stream_entries() else {
        return wrong_type_response();
    };

    if stream.last().is_some_and(|last| new_id < last.id) {
        return CommandOutcome::reply(err(
            "ERR The ID specified in XSETID is smaller than the target stream top item",
        ));
    }

    CommandOutcome::reply(RespFrame::ok())
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
    let db = server.db_mut(client.selected_db);
    purge_expired_key(db, key, now);

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
    let db = server.db_mut(client.selected_db);
    purge_expired_key(db, key, now);

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
    let db = server.db_mut(client.selected_db);
    purge_expired_key(db, key, now);

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
    let db = server.db_mut(client.selected_db);
    purge_expired_key(db, key, now);

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

pub(super) fn cmd_xcfgset(
    args: &[Bytes],
    server: &mut ServerState,
    client: &ClientState,
) -> CommandOutcome {
    if args.is_empty() {
        return wrong_arity("xcfgset");
    }

    let key = &args[0];
    let mut idx = 1usize;

    while idx < args.len() {
        let option = to_uppercase_bytes(&args[idx]);
        match option.as_slice() {
            b"IDMP-DURATION" | b"IDMP-MAXSIZE" => {
                let Some(raw_value) = args.get(idx + 1) else {
                    return CommandOutcome::reply(err("ERR syntax error"));
                };
                let Some(value) = parse_i64(raw_value) else {
                    return CommandOutcome::reply(err(
                        "ERR value is not an integer or out of range",
                    ));
                };
                if value < 0 {
                    return CommandOutcome::reply(err(
                        "ERR value is not an integer or out of range",
                    ));
                }
                idx += 2;
            }
            _ => return CommandOutcome::reply(err("ERR syntax error")),
        }
    }

    let now = now_ms();
    let db = server.db_mut(client.selected_db);
    purge_expired_key(db, key, now);

    let Some(entry) = db.get(key) else {
        return CommandOutcome::reply(err("ERR no such key"));
    };
    if !entry.is_stream() {
        return wrong_type_response();
    }

    CommandOutcome::reply(RespFrame::ok())
}
