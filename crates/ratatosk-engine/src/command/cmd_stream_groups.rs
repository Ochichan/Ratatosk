use bytes::Bytes;

use hashbrown::{HashMap, HashSet};
use ratatosk_resp::frame::RespFrame;

use crate::keyspace::{
    ServerState, StoredValue, StreamConsumer, StreamId, StreamPendingEntry, purge_expired_key,
};

use super::cmd_stream::{
    IntervalEdge, blocking_deadline_ms_from_block, blocking_watch_keys, build_blocking_frame,
    invalid_stream_id, parse_interval_id, parse_stream_id_generic, parse_strict_stream_id,
    stream_entry_frame, stream_id_to_bytes, stream_nogroup_error, xreadgroup_nogroup_error,
};

use super::{
    ClientState, CommandOutcome, err, now_ms, parse_i64, parse_usize, to_uppercase_bytes,
    wrong_arity, wrong_type_response,
};

/// Parses the options after the ID of XGROUP CREATE (MKSTREAM, ENTRIESREAD)
/// and SETID (ENTRIESREAD), as Redis's xgroupCommand does. Returns whether
/// MKSTREAM was given and the ENTRIESREAD value, `None` for the default or
/// `-1`.
fn parse_xgroup_options(options: &[Bytes], create: bool) -> Result<(bool, Option<u64>), RespFrame> {
    let mut mkstream = false;
    let mut entries_read = None;
    let mut idx = 0usize;
    while idx < options.len() {
        if create && options[idx].eq_ignore_ascii_case(b"MKSTREAM") {
            mkstream = true;
            idx += 1;
        } else if options[idx].eq_ignore_ascii_case(b"ENTRIESREAD") && idx + 1 < options.len() {
            let Some(value) = parse_i64(&options[idx + 1]) else {
                return Err(err("ERR value is not an integer or out of range"));
            };
            entries_read = match u64::try_from(value) {
                Ok(value) => Some(value),
                Err(_) if value == -1 => None,
                Err(_) => {
                    return Err(err("ERR value for ENTRIESREAD must be positive or -1"));
                }
            };
            idx += 2;
        } else {
            return Err(err("ERR syntax error"));
        }
    }
    Ok((mkstream, entries_read))
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
            if args.len() < 4 || args.len() > 7 {
                return wrong_arity("xgroup");
            }

            let key = &args[1];
            let group_name = &args[2];
            let id_raw = &args[3];
            let (mkstream, entries_read) = match parse_xgroup_options(&args[4..], true) {
                Ok(parsed) => parsed,
                Err(reply) => return CommandOutcome::reply(reply),
            };

            let now = now_ms();
            let mut db = server.db_mut(client.selected_db);
            purge_expired_key(&mut db, key, now);

            // Same order as Redis: key and type first, then the ID, and
            // MKSTREAM only once the command can no longer fail, so a
            // rejected command leaves no empty stream behind.
            let existing_last_id = match db.get(key) {
                Some(entry) => {
                    let Some(meta) = entry.as_stream_meta() else {
                        return wrong_type_response();
                    };
                    Some((meta.last_id, meta.entries_added))
                }
                None if mkstream => None,
                None => {
                    return CommandOutcome::reply(err(
                        "ERR The XGROUP subcommand requires the key to exist. Note that for CREATE you may want to use the MKSTREAM option to create an empty stream automatically.",
                    ));
                }
            };
            let id = if id_raw.as_ref() == b"$" {
                // `$` is the stream's last generated ID.
                existing_last_id.map_or(StreamId { ms: 0, seq: 0 }, |(last_id, _)| last_id)
            } else {
                let Some(parsed) = parse_strict_stream_id(id_raw) else {
                    return CommandOutcome::reply(invalid_stream_id());
                };
                parsed
            };

            if existing_last_id.is_none() {
                db.insert(key.clone(), StoredValue::stream(Vec::new(), None));
            }
            let Some(groups) = db
                .get_mut(key)
                .and_then(|entry| entry.as_stream_groups_mut())
            else {
                return CommandOutcome::reply(err("ERR internal error"));
            };

            if groups.contains_key(group_name) {
                return CommandOutcome::reply(err("BUSYGROUP Consumer Group name already exists"));
            }

            groups.insert(
                group_name.clone(),
                crate::keyspace::StreamGroup {
                    last_delivered_id: id,
                    // Never above the entries added so far, as in Redis.
                    entries_read: entries_read
                        .map(|read| read.min(existing_last_id.map_or(0, |(_, added)| added))),
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
            let mut db = server.db_mut(client.selected_db);
            purge_expired_key(&mut db, key, now);

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
            let [_, key, group_name, id_raw, options @ ..] = args else {
                return wrong_arity("xgroup");
            };
            if !matches!(options.len(), 0 | 2) {
                return wrong_arity("xgroup");
            }
            let (_, entries_read) = match parse_xgroup_options(options, false) {
                Ok(parsed) => parsed,
                Err(reply) => return CommandOutcome::reply(reply),
            };

            let now = now_ms();
            let mut db = server.db_mut(client.selected_db);
            purge_expired_key(&mut db, key, now);

            let Some(entry) = db.get_mut(key) else {
                return xreadgroup_nogroup_error(key, group_name);
            };
            // `$` is the stream's last generated ID.
            let last_id = entry.as_stream_meta().map(|meta| meta.last_id);
            let entries_added = entry.as_stream_meta().map_or(0, |meta| meta.entries_added);
            let Some(groups) = entry.as_stream_groups_mut() else {
                return wrong_type_response();
            };
            let Some(group) = groups.get_mut(group_name) else {
                return xreadgroup_nogroup_error(key, group_name);
            };

            group.last_delivered_id = if id_raw.as_ref() == b"$" {
                last_id.unwrap_or(StreamId { ms: 0, seq: 0 })
            } else {
                let Some(parsed) = parse_stream_id_generic(id_raw, 0, false) else {
                    return CommandOutcome::reply(invalid_stream_id());
                };
                parsed
            };
            // SETID without ENTRIESREAD forgets the counter, as in Redis.
            group.entries_read = entries_read.map(|read| read.min(entries_added));

            CommandOutcome::reply(RespFrame::ok())
        }
        b"CREATECONSUMER" => {
            let [_, key, group_name, consumer_name] = args else {
                return wrong_arity("xgroup");
            };

            let now = now_ms();
            let mut db = server.db_mut(client.selected_db);
            purge_expired_key(&mut db, key, now);

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
            let mut db = server.db_mut(client.selected_db);
            purge_expired_key(&mut db, key, now);

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

    // As Redis, every stream's group and ID are checked before any entry is
    // delivered, so a bad ID later in the list changes nothing.
    {
        let now = now_ms();
        for (key, id_raw) in keys.iter().zip(ids.iter()) {
            let mut db = server.db_mut(client.selected_db);
            purge_expired_key(&mut db, key, now);
            let Some(entry) = db.get(key) else {
                return xreadgroup_nogroup_error(key, &group_name);
            };
            if !entry.is_stream() {
                return wrong_type_response();
            }
            if !entry
                .as_stream_groups()
                .is_some_and(|groups| groups.contains_key(&group_name))
            {
                return xreadgroup_nogroup_error(key, &group_name);
            }
            match id_raw.as_ref() {
                b"$" | b"+" => {
                    let id = String::from_utf8_lossy(id_raw);
                    return CommandOutcome::reply(err(&format!(
                        "ERR The {id} ID is meaningless in the context of XREADGROUP: you want to read the history of this consumer by specifying a proper ID, or use the > ID to get new messages. The {id} ID would just return an empty result set."
                    )));
                }
                b">" => {}
                _ => {
                    if parse_strict_stream_id(id_raw).is_none() {
                        return CommandOutcome::reply(invalid_stream_id());
                    }
                }
            }
        }
    }

    let mut out = Vec::new();
    let now = now_ms();

    for (key, id_raw) in keys.iter().zip(ids.iter()) {
        let mut db = server.db_mut(client.selected_db);
        purge_expired_key(&mut db, key, now);

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
                let Some(parsed) = parse_strict_stream_id(id_raw) else {
                    return CommandOutcome::reply(invalid_stream_id());
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
            let Some((entries, groups, meta)) = entry.as_stream_parts_mut() else {
                return xreadgroup_nogroup_error(key, &group_name);
            };
            let Some(group) = groups.get_mut(&group_name) else {
                return xreadgroup_nogroup_error(key, &group_name);
            };

            if id_raw.as_ref() == b">" {
                if !noack {
                    // An ID still pending for another consumer (after XGROUP SETID
                    // rewound the group) moves to this one, so it leaves the old
                    // owner's list, and its delivery count restarts at 1, as in
                    // Redis.
                    for id in &selected_ids {
                        let previous = group
                            .pending
                            .get(id)
                            .filter(|pending| pending.consumer != consumer_name)
                            .map(|pending| pending.consumer.clone());
                        if let Some(previous) = previous {
                            if let Some(owner) = group.consumers.get_mut(&previous) {
                                owner.pending.remove(id);
                            }
                        }
                    }
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
                                pending.deliveries = 1;
                                pending.last_delivered_ms = now;
                            })
                            .or_insert_with(|| StreamPendingEntry {
                                consumer: consumer_name.clone(),
                                deliveries: 1,
                                last_delivered_ms: now,
                            });
                    }
                }

                // As Redis, per entry past the group's last ID: keep the read
                // counter exact while it is valid and nothing was deleted
                // ahead, otherwise estimate it from the stream's shape.
                for id in &selected_ids {
                    if *id <= group.last_delivered_id {
                        continue;
                    }
                    let first_id = entries.first().map(|entry| entry.id);
                    group.entries_read = match group.entries_read {
                        Some(read)
                            if first_id.is_some_and(|first| group.last_delivered_id >= first)
                                && !meta.range_has_tombstones(
                                    &entries[..],
                                    group.last_delivered_id,
                                ) =>
                        {
                            Some(read + 1)
                        }
                        _ if meta.entries_added > 0 => {
                            meta.estimate_distance_from_first_entry(&entries[..], *id)
                        }
                        other => other,
                    };
                    group.last_delivered_id = *id;
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
            if deadline_ms
                .is_some_and(|deadline| ratatosk_core::time::monotonic_ms() as i64 >= deadline)
            {
                return CommandOutcome::reply(RespFrame::NullArray);
            }
            let full_frame = build_blocking_frame("XREADGROUP", args);
            CommandOutcome::blocking(
                RespFrame::NullArray,
                deadline_ms,
                full_frame,
                blocking_watch_keys(client, keys),
            )
        } else {
            CommandOutcome::reply(RespFrame::NullArray)
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
    let mut db = server.db_mut(client.selected_db);
    purge_expired_key(&mut db, key, now);

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
        let Some(id) = parse_strict_stream_id(id_raw) else {
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

    // As Redis's xpendingCommand, the range form's arguments are checked
    // before the key and group are looked up: `XPENDING key group [[IDLE
    // min-idle] start end count [consumer]]`.
    struct PendingRange<'a> {
        start: StreamId,
        end: StreamId,
        count: usize,
        min_idle: i64,
        consumer: Option<&'a Bytes>,
    }
    let range = if args.len() == 2 {
        None
    } else {
        if !(5..=8).contains(&args.len()) {
            return CommandOutcome::reply(err("ERR syntax error"));
        }
        let mut min_idle = 0i64;
        let mut first = 2usize;
        if args[2].eq_ignore_ascii_case(b"IDLE") {
            let Some(parsed) = parse_i64(&args[3]) else {
                return CommandOutcome::reply(err("ERR value is not an integer or out of range"));
            };
            if args.len() < 7 {
                return CommandOutcome::reply(err("ERR syntax error"));
            }
            min_idle = parsed;
            first = 4;
        }
        let Some(count) = parse_i64(&args[first + 2]) else {
            return CommandOutcome::reply(err("ERR value is not an integer or out of range"));
        };
        let start = match parse_interval_id(&args[first], IntervalEdge::Start) {
            Ok(id) => id,
            Err(reply) => return CommandOutcome::reply(reply),
        };
        let end = match parse_interval_id(&args[first + 1], IntervalEdge::End) {
            Ok(id) => id,
            Err(reply) => return CommandOutcome::reply(reply),
        };
        Some(PendingRange {
            start,
            end,
            count: usize::try_from(count).unwrap_or(0),
            min_idle,
            consumer: args.get(first + 3),
        })
    };

    let now = now_ms();
    let mut db = server.db_mut(client.selected_db);
    purge_expired_key(&mut db, key, now);

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

    let Some(PendingRange {
        start,
        end,
        count,
        min_idle,
        consumer: consumer_filter,
    }) = range
    else {
        if group.pending.is_empty() {
            return CommandOutcome::reply(RespFrame::Array(vec![
                RespFrame::Integer(0),
                RespFrame::BulkString(None),
                RespFrame::BulkString(None),
                RespFrame::NullArray,
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
    };

    // IDLE keeps only entries idle for at least that long.
    let mut pending_ids = group
        .pending
        .iter()
        .filter(|(id, pending)| {
            **id >= start
                && **id <= end
                && consumer_filter.is_none_or(|consumer| pending.consumer == *consumer)
                && (min_idle == 0 || now.saturating_sub(pending.last_delivered_ms) >= min_idle)
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

fn optional_count(value: Option<u64>) -> RespFrame {
    value.map_or(RespFrame::Null, |value| {
        RespFrame::Integer(i64::try_from(value).unwrap_or(i64::MAX))
    })
}

/// The group's `lag`, null when fragmentation makes it unknowable.
fn group_lag_frame(
    entries: &[crate::keyspace::StreamEntry],
    meta: &crate::keyspace::StreamMeta,
    group: &crate::keyspace::StreamGroup,
) -> RespFrame {
    optional_count(meta.group_lag(entries, group))
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
            let mut db = server.db_mut(client.selected_db);
            purge_expired_key(&mut db, key, now);

            let Some(entry) = db.get(key) else {
                return CommandOutcome::reply(err("ERR no such key"));
            };
            let Some((stream, groups_ref)) = entry.as_stream() else {
                return wrong_type_response();
            };
            let meta = entry
                .as_stream_meta()
                .copied()
                .unwrap_or_else(|| crate::keyspace::StreamMeta::derived_from(stream));

            let first_id = stream
                .first()
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
                RespFrame::BulkString(Some(stream_id_to_bytes(meta.last_id))),
                RespFrame::bulk_str("max-deleted-entry-id"),
                RespFrame::BulkString(Some(stream_id_to_bytes(meta.max_deleted_id))),
                RespFrame::bulk_str("entries-added"),
                RespFrame::Integer(i64::try_from(meta.entries_added).unwrap_or(i64::MAX)),
                RespFrame::bulk_str("recorded-first-entry-id"),
                RespFrame::BulkString(Some(stream_id_to_bytes(first_id))),
            ];

            if !full {
                out.push(RespFrame::bulk_str("groups"));
                out.push(RespFrame::Integer(group_count as i64));
                out.push(RespFrame::bulk_str("first-entry"));
                out.push(
                    stream
                        .first()
                        .map(stream_entry_frame)
                        .unwrap_or(RespFrame::Null),
                );
                out.push(RespFrame::bulk_str("last-entry"));
                out.push(
                    stream
                        .last()
                        .map(stream_entry_frame)
                        .unwrap_or(RespFrame::Null),
                );
            } else {
                // COUNT 0 means no limit, as in Redis.
                let limit = match count.unwrap_or(10) {
                    0 => usize::MAX,
                    limit => limit,
                };
                let rows = stream
                    .iter()
                    .take(limit)
                    .map(stream_entry_frame)
                    .collect::<Vec<_>>();
                out.push(RespFrame::bulk_str("entries"));
                out.push(RespFrame::Array(rows));

                let mut group_rows = groups_ref.iter().collect::<Vec<_>>();
                group_rows.sort_by(|a, b| a.0.cmp(b.0));
                let pending_row = |id: &StreamId,
                                   pending: &crate::keyspace::StreamPendingEntry,
                                   with_owner: bool| {
                    let mut row = vec![RespFrame::BulkString(Some(stream_id_to_bytes(*id)))];
                    if with_owner {
                        row.push(RespFrame::BulkString(Some(pending.consumer.clone())));
                    }
                    row.push(RespFrame::Integer(pending.last_delivered_ms));
                    row.push(RespFrame::Integer(pending.deliveries));
                    RespFrame::Array(row)
                };
                let groups_out = group_rows
                    .into_iter()
                    .map(|(name, group)| {
                        let mut pending_ids = group.pending.keys().copied().collect::<Vec<_>>();
                        pending_ids.sort_unstable();
                        let group_pel = pending_ids
                            .iter()
                            .take(limit)
                            .filter_map(|id| {
                                group
                                    .pending
                                    .get(id)
                                    .map(|pending| pending_row(id, pending, true))
                            })
                            .collect::<Vec<_>>();
                        let mut consumers = group.consumers.iter().collect::<Vec<_>>();
                        consumers.sort_by(|a, b| a.0.cmp(b.0));
                        let consumers_out = consumers
                            .into_iter()
                            .map(|(consumer_name, consumer)| {
                                let mut ids = consumer.pending.iter().copied().collect::<Vec<_>>();
                                ids.sort_unstable();
                                let consumer_pel = ids
                                    .iter()
                                    .take(limit)
                                    .filter_map(|id| {
                                        group
                                            .pending
                                            .get(id)
                                            .map(|pending| pending_row(id, pending, false))
                                    })
                                    .collect::<Vec<_>>();
                                RespFrame::Array(vec![
                                    RespFrame::bulk_str("name"),
                                    RespFrame::BulkString(Some(consumer_name.clone())),
                                    RespFrame::bulk_str("seen-time"),
                                    RespFrame::Integer(consumer.seen_time_ms),
                                    // Ratatosk keeps one activity time per consumer.
                                    RespFrame::bulk_str("active-time"),
                                    RespFrame::Integer(consumer.seen_time_ms),
                                    RespFrame::bulk_str("pel-count"),
                                    RespFrame::Integer(consumer.pending.len() as i64),
                                    RespFrame::bulk_str("pending"),
                                    RespFrame::Array(consumer_pel),
                                ])
                            })
                            .collect::<Vec<_>>();
                        RespFrame::Array(vec![
                            RespFrame::bulk_str("name"),
                            RespFrame::BulkString(Some(name.clone())),
                            RespFrame::bulk_str("last-delivered-id"),
                            RespFrame::BulkString(Some(stream_id_to_bytes(
                                group.last_delivered_id,
                            ))),
                            RespFrame::bulk_str("entries-read"),
                            optional_count(group.entries_read),
                            RespFrame::bulk_str("lag"),
                            group_lag_frame(stream, &meta, group),
                            RespFrame::bulk_str("pel-count"),
                            RespFrame::Integer(group.pending.len() as i64),
                            RespFrame::bulk_str("pending"),
                            RespFrame::Array(group_pel),
                            RespFrame::bulk_str("consumers"),
                            RespFrame::Array(consumers_out),
                        ])
                    })
                    .collect::<Vec<_>>();
                out.push(RespFrame::bulk_str("groups"));
                out.push(RespFrame::Array(groups_out));
            }

            CommandOutcome::reply(RespFrame::Array(out))
        }
        b"GROUPS" => {
            let [_, key] = args else {
                return wrong_arity("xinfo");
            };

            let now = now_ms();
            let mut db = server.db_mut(client.selected_db);
            purge_expired_key(&mut db, key, now);

            let Some(entry) = db.get(key) else {
                return CommandOutcome::reply(err("ERR no such key"));
            };
            let Some((entries, groups_ref)) = entry.as_stream() else {
                return wrong_type_response();
            };
            let meta = entry
                .as_stream_meta()
                .copied()
                .unwrap_or_else(|| crate::keyspace::StreamMeta::derived_from(entries));

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
                        RespFrame::bulk_str("entries-read"),
                        optional_count(group.entries_read),
                        RespFrame::bulk_str("lag"),
                        group_lag_frame(entries, &meta, group),
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
            let mut db = server.db_mut(client.selected_db);
            purge_expired_key(&mut db, key, now);

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
