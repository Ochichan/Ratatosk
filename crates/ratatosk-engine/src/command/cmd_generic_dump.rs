use std::collections::VecDeque;

use bytes::Bytes;

use hashbrown::{HashMap, HashSet};
use ratatosk_resp::frame::RespFrame;

use crate::keyspace::{
    ServerState, StoredValue, StreamConsumer, StreamEntry, StreamGroup, StreamId,
    StreamPendingEntry, purge_expired_key,
};

use super::{ClientState, CommandOutcome, err, now_ms, parse_i64, to_uppercase_bytes, wrong_arity};

const RESTORE_MAX_PAYLOAD_BYTES: usize = 64 * 1024 * 1024;
const RESTORE_MAX_COLLECTION_ITEMS: usize = 1_000_000;
const RESTORE_MAX_STREAM_FIELDS_PER_ENTRY: usize = 1_000_000;
/// Cap on capacity reserved from a count in an untrusted payload.
const RESTORE_MAX_PREALLOC: usize = 1024;
/// Version 2 adds hash field deadlines and stream consumer groups; version 3
/// adds stream metadata (last generated ID, entries added, max deleted ID);
/// version 4 adds each group's entries-read counter.
const DUMP_MAGIC_CURRENT: &[u8] = b"RATSK4";
const DUMP_MAGIC_V3: &[u8] = b"RATSK3";
const DUMP_MAGIC_V2: &[u8] = b"RATSK2";
const DUMP_MAGIC_V1: &[u8] = b"RATSK1";
const DUMP_MAGIC_LEGACY: &[u8] = &[0x41, 0x58, 0x4f, 0x4e, 0x44, 0x31];

pub(super) fn cmd_dump(
    args: &[Bytes],
    server: &mut ServerState,
    client: &ClientState,
) -> CommandOutcome {
    let [key] = args else {
        return wrong_arity("dump");
    };

    let now = now_ms();
    let mut db = server.db_mut(client.selected_db);
    purge_expired_key(&mut db, key, now);

    let Some(entry) = db.get(key) else {
        return CommandOutcome::reply(RespFrame::BulkString(None));
    };

    let payload = serialize_stored_value(entry);
    CommandOutcome::reply(RespFrame::BulkString(Some(payload)))
}

pub(super) fn cmd_restore(
    args: &[Bytes],
    server: &mut ServerState,
    client: &ClientState,
) -> CommandOutcome {
    if args.len() < 3 {
        return wrong_arity("restore");
    }

    let key = &args[0];
    let ttl_raw = &args[1];
    let payload = &args[2];

    let Some(ttl_ms) = parse_i64(ttl_raw) else {
        return CommandOutcome::reply(err("ERR value is not an integer or out of range"));
    };
    if ttl_ms < 0 {
        return CommandOutcome::reply(err("ERR value is out of range"));
    }

    let mut replace = false;
    let mut absttl = false;
    let mut idx = 3usize;
    while idx < args.len() {
        let option = to_uppercase_bytes(&args[idx]);
        match option.as_slice() {
            b"REPLACE" => {
                replace = true;
                idx += 1;
            }
            b"ABSTTL" => {
                absttl = true;
                idx += 1;
            }
            _ => return CommandOutcome::reply(err("ERR syntax error")),
        }
    }

    if payload.len() > RESTORE_MAX_PAYLOAD_BYTES {
        return CommandOutcome::reply(err("ERR DUMP payload is too large"));
    }

    let mut value = match deserialize_stored_value(payload) {
        Some(value) => value,
        None => {
            return CommandOutcome::reply(err("ERR DUMP payload version or checksum are wrong"));
        }
    };

    let now = now_ms();
    let mut db = server.db_mut(client.selected_db);
    purge_expired_key(&mut db, key, now);

    if db.contains_key(key) && !replace {
        return CommandOutcome::reply(err("BUSYKEY Target key name already exists."));
    }

    let expire_at_ms = if ttl_ms == 0 {
        None
    } else if absttl {
        Some(ttl_ms)
    } else {
        Some(now.saturating_add(ttl_ms))
    };

    if expire_at_ms.is_some_and(|ts| ts <= now) {
        db.remove(key);
        return CommandOutcome::reply(RespFrame::ok());
    }

    value.set_expire_at_ms(expire_at_ms);
    db.insert(key.clone(), value);

    CommandOutcome::reply(RespFrame::ok())
}

fn put_u32(buf: &mut Vec<u8>, value: usize) {
    let len = u32::try_from(value).unwrap_or(u32::MAX);
    buf.extend_from_slice(&len.to_le_bytes());
}

fn put_i64(buf: &mut Vec<u8>, value: i64) {
    buf.extend_from_slice(&value.to_le_bytes());
}

fn take_u32(raw: &[u8], idx: &mut usize) -> Option<usize> {
    if *idx + 4 > raw.len() {
        return None;
    }
    let mut tmp = [0u8; 4];
    tmp.copy_from_slice(&raw[*idx..*idx + 4]);
    *idx += 4;
    Some(u32::from_le_bytes(tmp) as usize)
}

fn take_i64(raw: &[u8], idx: &mut usize) -> Option<i64> {
    if *idx + 8 > raw.len() {
        return None;
    }
    let mut tmp = [0u8; 8];
    tmp.copy_from_slice(&raw[*idx..*idx + 8]);
    *idx += 8;
    Some(i64::from_le_bytes(tmp))
}

fn put_bytes(buf: &mut Vec<u8>, value: &Bytes) {
    put_u32(buf, value.len());
    buf.extend_from_slice(value);
}

fn take_bytes(raw: &[u8], idx: &mut usize) -> Option<Bytes> {
    let len = take_u32(raw, idx)?;
    if *idx + len > raw.len() {
        return None;
    }
    let out = Bytes::copy_from_slice(&raw[*idx..*idx + len]);
    *idx += len;
    Some(out)
}

fn serialize_stored_value(entry: &StoredValue) -> Bytes {
    use crate::keyspace::ValueData;

    let mut out = Vec::new();
    out.extend_from_slice(DUMP_MAGIC_CURRENT);

    match entry.data() {
        ValueData::String(value) => {
            out.push(b's');
            put_bytes(&mut out, value);
        }
        ValueData::StringInt(n) => {
            out.push(b's');
            let mut buf = itoa::Buffer::new();
            let rendered = Bytes::copy_from_slice(buf.format(*n).as_bytes());
            put_bytes(&mut out, &rendered);
        }
        ValueData::Hash(hash) => {
            out.push(b'h');
            let mut items = hash.iter().collect::<Vec<_>>();
            items.sort_by(|a, b| a.0.cmp(b.0));
            put_u32(&mut out, items.len());
            for (field, entry) in items {
                put_bytes(&mut out, field);
                put_bytes(&mut out, &entry.value);
                put_optional_i64(&mut out, entry.expire_at_ms);
            }
        }
        ValueData::List(list) => {
            out.push(b'l');
            put_u32(&mut out, list.len());
            for item in list {
                put_bytes(&mut out, item);
            }
        }
        ValueData::Set(set) => {
            out.push(b't');
            let mut items = set.iter().cloned().collect::<Vec<_>>();
            items.sort();
            put_u32(&mut out, items.len());
            for item in items {
                put_bytes(&mut out, &item);
            }
        }
        ValueData::SetInt(set) => {
            out.push(b't');
            put_u32(&mut out, set.len());
            for item in set {
                let mut buf = itoa::Buffer::new();
                let rendered = Bytes::copy_from_slice(buf.format(*item).as_bytes());
                put_bytes(&mut out, &rendered);
            }
        }
        ValueData::SortedSet(zset) => {
            out.push(b'z');
            put_u32(&mut out, zset.len());
            for entry in zset.by_score.keys() {
                put_bytes(&mut out, &entry.member);
                put_i64(&mut out, entry.score.0.to_bits() as i64);
            }
        }
        ValueData::Stream {
            entries,
            groups,
            meta,
        } => {
            out.push(b'r');
            put_u32(&mut out, entries.len());
            for item in entries.iter() {
                put_stream_id(&mut out, item.id);
                put_u32(&mut out, item.fields.len());
                for (field, value) in &item.fields {
                    put_bytes(&mut out, field);
                    put_bytes(&mut out, value);
                }
            }
            let mut groups = groups.iter().collect::<Vec<_>>();
            groups.sort_by(|a, b| a.0.cmp(b.0));
            put_u32(&mut out, groups.len());
            for (name, group) in groups {
                put_bytes(&mut out, name);
                put_stream_id(&mut out, group.last_delivered_id);
                // -1 marks an unknown counter.
                put_i64(
                    &mut out,
                    group
                        .entries_read
                        .map_or(-1, |read| i64::try_from(read).unwrap_or(i64::MAX)),
                );
                let mut consumers = group.consumers.iter().collect::<Vec<_>>();
                consumers.sort_by(|a, b| a.0.cmp(b.0));
                put_u32(&mut out, consumers.len());
                for (consumer_name, consumer) in consumers {
                    put_bytes(&mut out, consumer_name);
                    put_i64(&mut out, consumer.seen_time_ms);
                }
                let mut pending = group.pending.iter().collect::<Vec<_>>();
                pending.sort_by_key(|(id, _)| **id);
                put_u32(&mut out, pending.len());
                for (id, entry) in pending {
                    put_stream_id(&mut out, *id);
                    put_bytes(&mut out, &entry.consumer);
                    put_i64(&mut out, entry.deliveries);
                    put_i64(&mut out, entry.last_delivered_ms);
                }
            }
            put_stream_id(&mut out, meta.last_id);
            put_i64(
                &mut out,
                i64::try_from(meta.entries_added).unwrap_or(i64::MAX),
            );
            put_stream_id(&mut out, meta.max_deleted_id);
        }
    }

    Bytes::from(out)
}

fn put_optional_i64(buf: &mut Vec<u8>, value: Option<i64>) {
    match value {
        Some(value) => {
            buf.push(1);
            put_i64(buf, value);
        }
        None => buf.push(0),
    }
}

fn put_stream_id(buf: &mut Vec<u8>, id: StreamId) {
    buf.extend_from_slice(&id.ms.to_le_bytes());
    buf.extend_from_slice(&id.seq.to_le_bytes());
}

fn take_optional_i64(raw: &[u8], idx: &mut usize) -> Option<Option<i64>> {
    let flag = *raw.get(*idx)?;
    *idx += 1;
    match flag {
        0 => Some(None),
        1 => Some(Some(take_i64(raw, idx)?)),
        _ => None,
    }
}

fn take_stream_id(raw: &[u8], idx: &mut usize) -> Option<StreamId> {
    let ms = take_i64(raw, idx)? as u64;
    let seq = take_i64(raw, idx)? as u64;
    Some(StreamId { ms, seq })
}

/// Reads an element count; RESTORE refuses empty lists, sets, hashes and
/// sorted sets like Redis.
fn take_count(raw: &[u8], idx: &mut usize) -> Option<usize> {
    let count = take_u32(raw, idx)?;
    (1..=RESTORE_MAX_COLLECTION_ITEMS)
        .contains(&count)
        .then_some(count)
}

/// Decodes a DUMP payload. Any structural problem yields `None`, which
/// RESTORE reports the way Redis reports a bad payload.
fn deserialize_stored_value(payload: &Bytes) -> Option<StoredValue> {
    let raw = payload.as_ref();
    if raw.len() < DUMP_MAGIC_CURRENT.len() + 1 {
        return None;
    }

    let magic = &raw[..DUMP_MAGIC_CURRENT.len()];
    let version = if magic == DUMP_MAGIC_CURRENT {
        4
    } else if magic == DUMP_MAGIC_V3 {
        3
    } else if magic == DUMP_MAGIC_V2 {
        2
    } else if magic == DUMP_MAGIC_V1 || magic == DUMP_MAGIC_LEGACY {
        1
    } else {
        return None;
    };

    let kind = raw[DUMP_MAGIC_CURRENT.len()];
    let mut idx = DUMP_MAGIC_CURRENT.len() + 1;

    let value = match kind {
        b's' => StoredValue::string(take_bytes(raw, &mut idx)?, None),
        b'h' => {
            let count = take_count(raw, &mut idx)?;
            let mut map = HashMap::with_capacity(count.min(RESTORE_MAX_PREALLOC));
            for _ in 0..count {
                let field = take_bytes(raw, &mut idx)?;
                let value = take_bytes(raw, &mut idx)?;
                let expire_at_ms = if version >= 2 {
                    take_optional_i64(raw, &mut idx)?
                } else {
                    None
                };
                // A deadline that passed in transit is left to the usual
                // lazy/active field expiry, exactly as after an RDB load.
                map.insert(
                    field,
                    crate::keyspace::HashFieldEntry {
                        value,
                        expire_at_ms,
                    },
                );
            }
            StoredValue::hash(map, None)
        }
        b'l' => {
            let count = take_count(raw, &mut idx)?;
            let mut list = VecDeque::with_capacity(count.min(RESTORE_MAX_PREALLOC));
            for _ in 0..count {
                list.push_back(take_bytes(raw, &mut idx)?);
            }
            StoredValue::list(list, None)
        }
        b't' => {
            let count = take_count(raw, &mut idx)?;
            let mut set = HashSet::with_capacity(count.min(RESTORE_MAX_PREALLOC));
            for _ in 0..count {
                set.insert(take_bytes(raw, &mut idx)?);
            }
            StoredValue::set(set, None)
        }
        b'z' => {
            let count = take_count(raw, &mut idx)?;
            let mut zset = crate::keyspace::SortedSet::default();
            for _ in 0..count {
                let member = take_bytes(raw, &mut idx)?;
                let score = f64::from_bits(take_i64(raw, &mut idx)? as u64);
                if score.is_nan() {
                    return None;
                }
                zset.insert(member, score);
            }
            StoredValue::sorted_set(zset, None)
        }
        b'r' => take_stream(raw, &mut idx, version)?,
        _ => return None,
    };

    (idx == raw.len()).then_some(value)
}

fn take_stream(raw: &[u8], idx: &mut usize, version: u8) -> Option<StoredValue> {
    // Unlike other containers a stream may be empty (XGROUP CREATE MKSTREAM).
    let count = take_u32(raw, idx)?;
    if count > RESTORE_MAX_COLLECTION_ITEMS {
        return None;
    }
    let mut entries: Vec<StreamEntry> = Vec::with_capacity(count.min(RESTORE_MAX_PREALLOC));
    for _ in 0..count {
        let id = take_stream_id(raw, idx)?;
        // Entry IDs must strictly increase; the stream commands rely on it.
        if entries.last().is_some_and(|last| last.id >= id) {
            return None;
        }

        let field_count = take_u32(raw, idx)?;
        if field_count > RESTORE_MAX_STREAM_FIELDS_PER_ENTRY {
            return None;
        }
        let mut fields = Vec::with_capacity(field_count.min(RESTORE_MAX_PREALLOC));
        for _ in 0..field_count {
            let field = take_bytes(raw, idx)?;
            let value = take_bytes(raw, idx)?;
            fields.push((field, value));
        }

        entries.push(StreamEntry { id, fields });
    }

    let mut value = StoredValue::stream(entries, None);
    if version < 2 {
        return Some(value);
    }

    let groups = value.as_stream_groups_mut()?;
    let group_count = take_u32(raw, idx)?;
    for _ in 0..group_count {
        let name = take_bytes(raw, idx)?;
        let last_delivered_id = take_stream_id(raw, idx)?;
        let entries_read = if version >= 4 {
            match take_i64(raw, idx)? {
                -1 => None,
                read => Some(u64::try_from(read).ok()?),
            }
        } else {
            None
        };
        let mut consumers = HashMap::new();
        let consumer_count = take_u32(raw, idx)?;
        for _ in 0..consumer_count {
            let consumer_name = take_bytes(raw, idx)?;
            let seen_time_ms = take_i64(raw, idx)?;
            let consumer = StreamConsumer {
                seen_time_ms,
                pending: HashSet::new(),
            };
            if consumers.insert(consumer_name, consumer).is_some() {
                return None;
            }
        }
        let mut pending = HashMap::new();
        let pending_count = take_u32(raw, idx)?;
        for _ in 0..pending_count {
            let id = take_stream_id(raw, idx)?;
            let consumer = take_bytes(raw, idx)?;
            let deliveries = take_i64(raw, idx)?;
            let last_delivered_ms = take_i64(raw, idx)?;
            // Each pending entry is owned by exactly one consumer of the same
            // group; the per-consumer index is rebuilt from the group PEL.
            consumers.get_mut(&consumer)?.pending.insert(id);
            let entry = StreamPendingEntry {
                consumer,
                deliveries,
                last_delivered_ms,
            };
            if pending.insert(id, entry).is_some() {
                return None;
            }
        }
        let group = StreamGroup {
            last_delivered_id,
            entries_read,
            consumers,
            pending,
        };
        if groups.insert(name, group).is_some() {
            return None;
        }
    }
    if version >= 3 {
        let last_id = take_stream_id(raw, idx)?;
        let entries_added = u64::try_from(take_i64(raw, idx)?).ok()?;
        let max_deleted_id = take_stream_id(raw, idx)?;
        let meta = crate::keyspace::StreamMeta {
            last_id,
            entries_added,
            max_deleted_id,
        };
        if !meta.is_valid_for(value.as_stream_entries()?) {
            return None;
        }
        // A read counter past the entries ever added would make lag negative.
        if value.as_stream_groups()?.values().any(|group| {
            group
                .entries_read
                .is_some_and(|read| read > meta.entries_added)
        }) {
            return None;
        }
        *value.as_stream_meta_mut()? = meta;
    }
    Some(value)
}
