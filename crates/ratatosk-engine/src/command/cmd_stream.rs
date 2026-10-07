use std::fmt::Write as _;

use bytes::Bytes;

use ratatosk_resp::frame::RespFrame;

use crate::keyspace::{ServerState, StoredValue, StreamEntry, StreamId, purge_expired_key};

use super::{
    ClientState, CommandOutcome, err, now_ms, parse_i64, parse_usize, wrong_arity,
    wrong_type_response,
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

/// The largest stream ID; a stream whose last ID is this accepts no more entries.
const MAX_STREAM_ID: StreamId = StreamId {
    ms: i64::MAX,
    seq: i64::MAX,
};

/// The ID right after `id`, carrying into the next millisecond when the
/// sequence is exhausted, as Redis's `streamIncrID`. `None` past the last ID.
fn incremented_stream_id(id: StreamId) -> Option<StreamId> {
    if id.seq < i64::MAX {
        Some(StreamId {
            ms: id.ms,
            seq: id.seq + 1,
        })
    } else if id.ms < i64::MAX {
        Some(StreamId {
            ms: id.ms + 1,
            seq: 0,
        })
    } else {
        None
    }
}

/// The ID `XADD *` assigns after `last_id`, the stream's last generated ID,
/// as Redis's `streamNextID`.
fn next_stream_id(last_id: StreamId) -> Option<StreamId> {
    let now = now_ms();
    if now > last_id.ms {
        Some(StreamId { ms: now, seq: 0 })
    } else {
        incremented_stream_id(last_id)
    }
}

/// The ID `XADD <ms>-*` assigns. Within the last ID's millisecond the sequence
/// continues and, unlike `*`, never carries: an exhausted sequence is `None`.
fn next_stream_id_for_ms(last_id: StreamId, ms: i64) -> Option<StreamId> {
    if ms != last_id.ms {
        return Some(StreamId { ms, seq: 0 });
    }
    (last_id.seq < i64::MAX).then(|| StreamId {
        ms,
        seq: last_id.seq + 1,
    })
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

pub(super) fn blocking_deadline_ms_from_block(block_ms: i64) -> Option<i64> {
    use ratatosk_core::time::monotonic_ms;

    if block_ms <= 0 {
        None
    } else {
        let now = monotonic_ms();
        let timeout = u64::try_from(block_ms).ok()?;
        let deadline = now.saturating_add(timeout);
        i64::try_from(deadline).ok()
    }
}

pub(super) fn build_blocking_frame(command_name: &str, args: &[Bytes]) -> RespFrame {
    let mut parts = Vec::with_capacity(1 + args.len());
    parts.push(RespFrame::BulkString(Some(Bytes::copy_from_slice(
        command_name.as_bytes(),
    ))));
    for arg in args {
        parts.push(RespFrame::BulkString(Some(arg.clone())));
    }
    RespFrame::Array(parts)
}

pub(super) fn blocking_watch_keys(client: &ClientState, keys: &[Bytes]) -> Vec<(usize, Bytes)> {
    keys.iter()
        .cloned()
        .map(|key| (client.selected_db(), key))
        .collect()
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

    // How the ID is chosen, validated before the key is touched so that a
    // rejected XADD never leaves an empty stream behind (Redis rejects these
    // forms while parsing its arguments).
    enum IdSpec {
        Auto,
        AutoSeq(i64),
        Explicit(StreamId),
    }
    let invalid_id = || {
        CommandOutcome::reply(err(
            "ERR Invalid stream ID specified as stream command argument",
        ))
    };
    let spec = if id_raw.as_ref() == b"*" {
        IdSpec::Auto
    } else if let Some(ms_raw) = id_raw.as_ref().strip_suffix(b"-*") {
        let Some(ms) = std::str::from_utf8(ms_raw)
            .ok()
            .and_then(|text| text.parse::<i64>().ok())
            .filter(|ms| *ms >= 0)
        else {
            return invalid_id();
        };
        IdSpec::AutoSeq(ms)
    } else {
        let Some(parsed) = parse_stream_id(id_raw) else {
            return invalid_id();
        };
        if parsed == (StreamId { ms: 0, seq: 0 }) {
            return CommandOutcome::reply(err(
                "ERR The ID specified in XADD must be greater than 0-0",
            ));
        }
        IdSpec::Explicit(parsed)
    };

    let now = now_ms();
    let mut db = server.db_mut(client.selected_db);
    purge_expired_key(&mut db, key, now);

    let last_id = match db.get(key) {
        Some(entry) => {
            let Some(meta) = entry.as_stream_meta() else {
                return wrong_type_response();
            };
            meta.last_id
        }
        None => StreamId { ms: 0, seq: 0 },
    };
    if last_id == MAX_STREAM_ID {
        return CommandOutcome::reply(err(
            "ERR The stream has exhausted the last possible ID, unable to add more items",
        ));
    }
    let id = match spec {
        IdSpec::Auto => next_stream_id(last_id),
        IdSpec::AutoSeq(ms) => next_stream_id_for_ms(last_id, ms),
        IdSpec::Explicit(id) => Some(id),
    };
    // Compared with the last generated ID, not the top entry, so deleting
    // entries never lets an older ID back in.
    let Some(id) = id.filter(|id| *id > last_id) else {
        return CommandOutcome::reply(err(
            "ERR The ID specified in XADD is equal or smaller than the target stream top item",
        ));
    };

    let mut fields = Vec::with_capacity((args.len() - 2) / 2);
    let mut idx = 2usize;
    while idx < args.len() {
        fields.push((args[idx].clone(), args[idx + 1].clone()));
        idx += 2;
    }

    if !db.contains_key(key) {
        db.insert(key.clone(), StoredValue::stream(Vec::new(), None));
    }
    let Some((stream, meta)) = db
        .get_mut(key)
        .and_then(|entry| entry.as_stream_entries_and_meta_mut())
    else {
        return CommandOutcome::reply(err("ERR internal error"));
    };
    stream.push(StreamEntry { id, fields });
    meta.last_id = id;
    meta.entries_added = meta.entries_added.saturating_add(1);
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
    let mut db = server.db_mut(client.selected_db);
    purge_expired_key(&mut db, key, now);

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
    let mut db = server.db_mut(client.selected_db);
    purge_expired_key(&mut db, key, now);

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
        let mut db = server.db_mut(client.selected_db);
        purge_expired_key(&mut db, key, now);

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
            // `$` is the last generated ID, which survives deleting entries.
            db.get(key)
                .and_then(|entry| entry.as_stream_meta())
                .map(|meta| meta.last_id)
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
            if deadline_ms
                .is_some_and(|deadline| ratatosk_core::time::monotonic_ms() as i64 >= deadline)
            {
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
            CommandOutcome::blocking(
                RespFrame::Null,
                deadline_ms,
                full_frame,
                blocking_watch_keys(client, keys),
            )
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
