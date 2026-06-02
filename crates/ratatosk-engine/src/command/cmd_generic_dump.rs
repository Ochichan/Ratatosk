use std::collections::VecDeque;

use bytes::Bytes;

use hashbrown::{HashMap, HashSet};
use ratatosk_resp::frame::RespFrame;

use crate::keyspace::{ServerState, StoredValue, StreamEntry, StreamId, purge_expired_key};

use super::{ClientState, CommandOutcome, err, now_ms, parse_i64, to_uppercase_bytes, wrong_arity};

const RESTORE_MAX_PAYLOAD_BYTES: usize = 64 * 1024 * 1024;
const RESTORE_MAX_COLLECTION_ITEMS: usize = 1_000_000;
const RESTORE_MAX_STREAM_FIELDS_PER_ENTRY: usize = 1_000_000;
const DUMP_MAGIC_CURRENT: &[u8] = b"RATSK1";
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
            let mut items = hash
                .iter()
                .map(|(k, v)| (k.clone(), v.value.clone()))
                .collect::<Vec<_>>();
            items.sort_by(|a, b| a.0.cmp(&b.0));
            put_u32(&mut out, items.len());
            for (k, v) in items {
                put_bytes(&mut out, &k);
                put_bytes(&mut out, &v);
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
        ValueData::Stream { entries, .. } => {
            out.push(b'r');
            put_u32(&mut out, entries.len());
            for item in entries {
                put_i64(&mut out, item.id.ms);
                put_i64(&mut out, item.id.seq);
                put_u32(&mut out, item.fields.len());
                for (field, value) in &item.fields {
                    put_bytes(&mut out, field);
                    put_bytes(&mut out, value);
                }
            }
        }
    }

    Bytes::from(out)
}

fn deserialize_stored_value(payload: &Bytes) -> Option<StoredValue> {
    let raw = payload.as_ref();
    if raw.len() < DUMP_MAGIC_CURRENT.len() + 1 {
        return None;
    }

    let magic = &raw[..DUMP_MAGIC_CURRENT.len()];
    if magic != DUMP_MAGIC_CURRENT && magic != DUMP_MAGIC_LEGACY {
        return None;
    }

    let kind = raw[DUMP_MAGIC_CURRENT.len()];
    let mut idx = DUMP_MAGIC_CURRENT.len() + 1;

    match kind {
        b's' => {
            let value = take_bytes(raw, &mut idx)?;
            if idx != raw.len() {
                return None;
            }
            Some(StoredValue::string(value, None))
        }
        b'h' => {
            let count = take_u32(raw, &mut idx)?;
            if count > RESTORE_MAX_COLLECTION_ITEMS {
                return None;
            }
            let mut map = HashMap::new();
            for _ in 0..count {
                let k = take_bytes(raw, &mut idx)?;
                let v = take_bytes(raw, &mut idx)?;
                map.insert(k, crate::keyspace::HashFieldEntry::new(v));
            }
            if idx != raw.len() {
                return None;
            }
            Some(StoredValue::hash(map, None))
        }
        b'l' => {
            let count = take_u32(raw, &mut idx)?;
            if count > RESTORE_MAX_COLLECTION_ITEMS {
                return None;
            }
            let mut list = VecDeque::new();
            for _ in 0..count {
                list.push_back(take_bytes(raw, &mut idx)?);
            }
            if idx != raw.len() {
                return None;
            }
            Some(StoredValue::list(list, None))
        }
        b't' => {
            let count = take_u32(raw, &mut idx)?;
            if count > RESTORE_MAX_COLLECTION_ITEMS {
                return None;
            }
            let mut set = HashSet::new();
            for _ in 0..count {
                set.insert(take_bytes(raw, &mut idx)?);
            }
            if idx != raw.len() {
                return None;
            }
            Some(StoredValue::set(set, None))
        }
        b'z' => {
            let count = take_u32(raw, &mut idx)?;
            if count > RESTORE_MAX_COLLECTION_ITEMS {
                return None;
            }
            let mut zset = crate::keyspace::SortedSet::default();
            for _ in 0..count {
                let member = take_bytes(raw, &mut idx)?;
                let score_bits = take_i64(raw, &mut idx)?;
                let score = f64::from_bits(score_bits as u64);
                zset.insert(member, score);
            }
            if idx != raw.len() {
                return None;
            }
            Some(StoredValue::sorted_set(zset, None))
        }
        b'r' => {
            let count = take_u32(raw, &mut idx)?;
            if count > RESTORE_MAX_COLLECTION_ITEMS {
                return None;
            }
            let mut stream = Vec::with_capacity(count);
            for _ in 0..count {
                let ms = take_i64(raw, &mut idx)?;
                let seq = take_i64(raw, &mut idx)?;
                if ms < 0 || seq < 0 {
                    return None;
                }

                let field_count = take_u32(raw, &mut idx)?;
                if field_count > RESTORE_MAX_STREAM_FIELDS_PER_ENTRY {
                    return None;
                }
                let mut fields = Vec::with_capacity(field_count);
                for _ in 0..field_count {
                    let field = take_bytes(raw, &mut idx)?;
                    let value = take_bytes(raw, &mut idx)?;
                    fields.push((field, value));
                }

                stream.push(StreamEntry {
                    id: StreamId { ms, seq },
                    fields,
                });
            }
            if idx != raw.len() {
                return None;
            }
            Some(StoredValue::stream(stream, None))
        }
        _ => None,
    }
}
