use std::{
    collections::VecDeque,
    hash::{Hash, Hasher},
};

use bytes::Bytes;

use glob_match::glob_match;
use hashbrown::{HashMap, HashSet};
use ratatosk_resp::frame::RespFrame;

use crate::keyspace::{
    ServerState, StoredValue, StreamEntry, StreamId, purge_expired_key, purge_expired_keys,
};
use crate::security::next_audit_stamp;

use super::{
    ClientState, CommandOutcome, err, now_ms, parse_i64, to_uppercase_bytes, wrong_arity,
    wrong_type_response,
};

const RESTORE_MAX_PAYLOAD_BYTES: usize = 64 * 1024 * 1024;
const RESTORE_MAX_COLLECTION_ITEMS: usize = 1_000_000;
const RESTORE_MAX_STREAM_FIELDS_PER_ENTRY: usize = 1_000_000;
const LCS_MAX_DP_CELLS: usize = 16_000_000;
const DUMP_MAGIC_CURRENT: &[u8] = b"RATSK1";
const DUMP_MAGIC_LEGACY: &[u8] = &[0x41, 0x58, 0x4f, 0x4e, 0x44, 0x31];

pub(super) fn cmd_randomkey(
    args: &[Bytes],
    server: &mut ServerState,
    client: &ClientState,
) -> CommandOutcome {
    if !args.is_empty() {
        return wrong_arity("randomkey");
    }

    let now = now_ms();
    let mut db = server.db_mut(client.selected_db);
    purge_expired_keys(&mut db, now);

    let key = db.keys().next().cloned();
    CommandOutcome::reply(RespFrame::BulkString(key))
}

pub(super) fn cmd_type(
    args: &[Bytes],
    server: &mut ServerState,
    client: &ClientState,
) -> CommandOutcome {
    let [key] = args else {
        return wrong_arity("type");
    };

    let now = now_ms();
    let mut db = server.db_mut(client.selected_db);
    purge_expired_key(&mut db, key, now);

    if let Some(entry) = db.get(key) {
        CommandOutcome::reply(RespFrame::simple_str(entry.type_name()))
    } else {
        CommandOutcome::reply(RespFrame::simple_str("none"))
    }
}

pub(super) fn cmd_keys(
    args: &[Bytes],
    server: &mut ServerState,
    client: &ClientState,
) -> CommandOutcome {
    let [pattern] = args else {
        return wrong_arity("keys");
    };

    let now = now_ms();
    let mut db = server.db_mut(client.selected_db);
    purge_expired_keys(&mut db, now);

    let pattern = String::from_utf8_lossy(pattern).to_string();
    let mut out = db
        .keys()
        .filter(|key| glob_match(&pattern, &String::from_utf8_lossy(key)))
        .cloned()
        .collect::<Vec<_>>();
    out.sort();

    let frames = out
        .into_iter()
        .map(|key| RespFrame::BulkString(Some(key)))
        .collect::<Vec<_>>();

    CommandOutcome::reply(RespFrame::Array(frames))
}

pub(super) fn cmd_wait(args: &[Bytes], server: &ServerState) -> CommandOutcome {
    let [num_replicas_raw, timeout_raw] = args else {
        return wrong_arity("wait");
    };

    let Some(num_replicas) = parse_i64(num_replicas_raw) else {
        return CommandOutcome::reply(err("ERR value is not an integer or out of range"));
    };
    let Some(_timeout_ms) = parse_i64(timeout_raw) else {
        return CommandOutcome::reply(err("ERR value is not an integer or out of range"));
    };
    if num_replicas < 0 {
        return CommandOutcome::reply(err("ERR value is out of range"));
    }

    let acked = server.replication_acked_replicas(server.replication_offset()) as i64;
    if num_replicas > 0 && acked == 0 {
        tracing::warn!(
            target = "ratatosk::replication",
            requested_replicas = num_replicas,
            "WAIT returning 0: Ratatosk is running in single-node mode with no replicas"
        );
    }
    CommandOutcome::reply(RespFrame::Integer(acked.min(num_replicas)))
}

pub(super) fn cmd_waitaof(args: &[Bytes], server: &ServerState) -> CommandOutcome {
    let [num_local_raw, num_replicas_raw, timeout_raw] = args else {
        return wrong_arity("waitaof");
    };

    let Some(num_local) = parse_i64(num_local_raw) else {
        return CommandOutcome::reply(err("ERR value is not an integer or out of range"));
    };
    let Some(num_replicas) = parse_i64(num_replicas_raw) else {
        return CommandOutcome::reply(err("ERR value is not an integer or out of range"));
    };
    let Some(_timeout_ms) = parse_i64(timeout_raw) else {
        return CommandOutcome::reply(err("ERR value is not an integer or out of range"));
    };

    if num_local < 0 || num_replicas < 0 {
        return CommandOutcome::reply(err("ERR value is out of range"));
    }

    let local_ack = if server.aof_enabled() && !server.aof_write_latched() {
        1
    } else {
        0
    };
    let replica_ack = server.replication_acked_replicas(server.replication_offset()) as i64;
    if num_replicas > 0 && replica_ack == 0 {
        tracing::warn!(
            target = "ratatosk::replication",
            requested_replicas = num_replicas,
            "WAITAOF returning 0 replica acks: Ratatosk is running in single-node mode with no replicas"
        );
    }
    CommandOutcome::reply(RespFrame::Array(vec![
        RespFrame::Integer(local_ack.min(num_local)),
        RespFrame::Integer(replica_ack.min(num_replicas)),
    ]))
}

pub(super) fn cmd_delex(
    args: &[Bytes],
    server: &mut ServerState,
    client: &ClientState,
) -> CommandOutcome {
    if args.is_empty() {
        return wrong_arity("delex");
    }

    let key = &args[0];
    let now = now_ms();
    let mut db = server.db_mut(client.selected_db);
    purge_expired_key(&mut db, key, now);

    let Some(entry) = db.get(key).cloned() else {
        return CommandOutcome::reply(RespFrame::Integer(0));
    };
    let Some(value) = entry.as_string() else {
        return wrong_type_response();
    };
    let should_delete = match args {
        [_] => true,
        [_, condition_raw, expected] => {
            let condition = to_uppercase_bytes(condition_raw);
            let digest = digest_i64_string(value.as_ref());
            match condition.as_slice() {
                b"IFEQ" => *value == *expected,
                b"IFNE" => *value != *expected,
                b"IFDEQ" => digest.as_bytes() == expected.as_ref(),
                b"IFDNE" => digest.as_bytes() != expected.as_ref(),
                _ => return CommandOutcome::reply(err("ERR syntax error")),
            }
        }
        _ => return CommandOutcome::reply(err("ERR syntax error")),
    };

    let removed = if should_delete {
        db.remove(key);
        1
    } else {
        0
    };

    let key_text = String::from_utf8_lossy(key).into_owned();
    let payload = format!(
        "event=DELEX client_id={} db={} key={} removed={}",
        client.id(),
        client.selected_db,
        key_text,
        removed
    );
    let stamp = next_audit_stamp("DELEX", &payload);
    tracing::info!(
        target = "ratatosk::audit",
        event = "DELEX",
        audit_seq = stamp.seq,
        audit_prev_hash = %stamp.prev_hash,
        audit_hash = %stamp.hash,
        client_id = client.id(),
        db = client.selected_db,
        key = %key_text,
        removed,
        "conditional delete executed"
    );

    CommandOutcome::reply(RespFrame::Integer(removed))
}

pub(super) fn cmd_digest(
    args: &[Bytes],
    server: &mut ServerState,
    client: &ClientState,
) -> CommandOutcome {
    let [key] = args else {
        return wrong_arity("digest");
    };

    let now = now_ms();
    let mut db = server.db_mut(client.selected_db);
    purge_expired_key(&mut db, key, now);

    let Some(entry) = db.get(key) else {
        return CommandOutcome::reply(RespFrame::BulkString(None));
    };
    let Some(value) = entry.as_string() else {
        return wrong_type_response();
    };

    let digest = digest_i64_string(value.as_ref());
    CommandOutcome::reply(RespFrame::bulk_str(&digest))
}

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

    value.expire_at_ms = expire_at_ms;
    db.insert(key.clone(), value);

    CommandOutcome::reply(RespFrame::ok())
}

pub(super) fn cmd_migrate(args: &[Bytes]) -> CommandOutcome {
    if args.len() < 5 {
        return wrong_arity("migrate");
    }

    CommandOutcome::reply(RespFrame::bulk_str("NOKEY"))
}

pub(super) fn cmd_lcs(
    args: &[Bytes],
    server: &mut ServerState,
    client: &ClientState,
) -> CommandOutcome {
    if args.len() < 2 {
        return wrong_arity("lcs");
    }

    let key1 = &args[0];
    let key2 = &args[1];

    let mut return_len = false;
    let mut idx = 2usize;
    while idx < args.len() {
        let option = to_uppercase_bytes(&args[idx]);
        match option.as_slice() {
            b"LEN" => {
                return_len = true;
                idx += 1;
            }
            _ => return CommandOutcome::reply(err("ERR syntax error")),
        }
    }

    let now = now_ms();
    let mut db = server.db_mut(client.selected_db);
    purge_expired_key(&mut db, key1, now);
    purge_expired_key(&mut db, key2, now);

    let left = match db.get(key1) {
        None => Bytes::new(),
        Some(entry) => match entry.as_string() {
            Some(v) => v.clone(),
            None => return wrong_type_response(),
        },
    };
    let right = match db.get(key2) {
        None => Bytes::new(),
        Some(entry) => match entry.as_string() {
            Some(v) => v.clone(),
            None => return wrong_type_response(),
        },
    };

    if left.len().saturating_mul(right.len()) > LCS_MAX_DP_CELLS {
        return CommandOutcome::reply(err("ERR LCS input is too large"));
    }

    let lcs = longest_common_subsequence(left.as_ref(), right.as_ref());
    if return_len {
        CommandOutcome::reply(RespFrame::Integer(lcs.len() as i64))
    } else {
        CommandOutcome::reply(RespFrame::BulkString(Some(Bytes::from(lcs))))
    }
}

pub(super) fn cmd_msetex(
    args: &[Bytes],
    server: &mut ServerState,
    client: &ClientState,
) -> CommandOutcome {
    if args.len() < 3 {
        return wrong_arity("msetex");
    }

    let Some(numkeys_i64) = parse_i64(&args[0]) else {
        return CommandOutcome::reply(err("ERR value is not an integer or out of range"));
    };
    if numkeys_i64 <= 0 {
        return CommandOutcome::reply(err("ERR numkeys should be greater than 0"));
    }
    let Ok(numkeys) = usize::try_from(numkeys_i64) else {
        return CommandOutcome::reply(err("ERR value is not an integer or out of range"));
    };

    let pair_tokens = numkeys.saturating_mul(2);
    if args.len() < 1 + pair_tokens {
        return wrong_arity("msetex");
    }

    let mut kvs = Vec::with_capacity(numkeys);
    let mut idx = 1usize;
    for _ in 0..numkeys {
        if idx + 1 >= args.len() {
            return wrong_arity("msetex");
        }
        kvs.push((args[idx].clone(), args[idx + 1].clone()));
        idx += 2;
    }

    let mut nx = false;
    let mut xx = false;
    let mut expire_at_ms: Option<i64> = None;
    let now = now_ms();

    while idx < args.len() {
        let option = to_uppercase_bytes(&args[idx]);
        match option.as_slice() {
            b"NX" => {
                nx = true;
                idx += 1;
            }
            b"XX" => {
                xx = true;
                idx += 1;
            }
            b"EX" | b"PX" => {
                if idx + 1 >= args.len() || expire_at_ms.is_some() {
                    return CommandOutcome::reply(err("ERR syntax error"));
                }
                let Some(raw_ttl) = parse_i64(&args[idx + 1]) else {
                    return CommandOutcome::reply(err(
                        "ERR value is not an integer or out of range",
                    ));
                };
                if raw_ttl <= 0 {
                    return CommandOutcome::reply(err(
                        "ERR invalid expire time in 'msetex' command",
                    ));
                }
                expire_at_ms = Some(if option.as_slice() == b"EX" {
                    now.saturating_add(raw_ttl.saturating_mul(1000))
                } else {
                    now.saturating_add(raw_ttl)
                });
                idx += 2;
            }
            _ => return CommandOutcome::reply(err("ERR syntax error")),
        }
    }

    if nx && xx {
        return CommandOutcome::reply(err("ERR syntax error"));
    }

    let mut db = server.db_mut(client.selected_db);
    for (key, _) in &kvs {
        purge_expired_key(&mut db, key, now);
    }

    if nx && kvs.iter().any(|(key, _)| db.contains_key(key)) {
        return CommandOutcome::reply(RespFrame::Integer(0));
    }
    if xx && kvs.iter().any(|(key, _)| !db.contains_key(key)) {
        return CommandOutcome::reply(RespFrame::Integer(0));
    }

    for (key, value) in kvs {
        db.insert(key, StoredValue::string(value, expire_at_ms));
    }

    CommandOutcome::reply(RespFrame::Integer(1))
}

pub(super) fn digest_i64_string(data: &[u8]) -> String {
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    data.hash(&mut hasher);
    let signed = hasher.finish() as i64;
    signed.to_string()
}

pub(super) fn put_u32(buf: &mut Vec<u8>, value: usize) {
    let len = u32::try_from(value).unwrap_or(u32::MAX);
    buf.extend_from_slice(&len.to_le_bytes());
}

pub(super) fn put_i64(buf: &mut Vec<u8>, value: i64) {
    buf.extend_from_slice(&value.to_le_bytes());
}

pub(super) fn take_u32(raw: &[u8], idx: &mut usize) -> Option<usize> {
    if *idx + 4 > raw.len() {
        return None;
    }
    let mut tmp = [0u8; 4];
    tmp.copy_from_slice(&raw[*idx..*idx + 4]);
    *idx += 4;
    Some(u32::from_le_bytes(tmp) as usize)
}

pub(super) fn take_i64(raw: &[u8], idx: &mut usize) -> Option<i64> {
    if *idx + 8 > raw.len() {
        return None;
    }
    let mut tmp = [0u8; 8];
    tmp.copy_from_slice(&raw[*idx..*idx + 8]);
    *idx += 8;
    Some(i64::from_le_bytes(tmp))
}

pub(super) fn put_bytes(buf: &mut Vec<u8>, value: &Bytes) {
    put_u32(buf, value.len());
    buf.extend_from_slice(value);
}

pub(super) fn take_bytes(raw: &[u8], idx: &mut usize) -> Option<Bytes> {
    let len = take_u32(raw, idx)?;
    if *idx + len > raw.len() {
        return None;
    }
    let out = Bytes::copy_from_slice(&raw[*idx..*idx + len]);
    *idx += len;
    Some(out)
}

pub(super) fn serialize_stored_value(entry: &StoredValue) -> Bytes {
    use crate::keyspace::ValueData;

    let mut out = Vec::new();
    out.extend_from_slice(DUMP_MAGIC_CURRENT);

    match &entry.data {
        ValueData::String(value) => {
            out.push(b's');
            put_bytes(&mut out, value);
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

pub(super) fn deserialize_stored_value(payload: &Bytes) -> Option<StoredValue> {
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

pub(super) fn longest_common_subsequence(a: &[u8], b: &[u8]) -> Vec<u8> {
    if a.is_empty() || b.is_empty() {
        return Vec::new();
    }

    let m = a.len();
    let n = b.len();
    let mut dp = vec![vec![0u16; n + 1]; m + 1];

    for i in 0..m {
        for (j, bj) in b.iter().enumerate().take(n) {
            if a[i] == *bj {
                dp[i + 1][j + 1] = dp[i][j].saturating_add(1);
            } else {
                dp[i + 1][j + 1] = dp[i][j + 1].max(dp[i + 1][j]);
            }
        }
    }

    let mut i = m;
    let mut j = n;
    let mut out = Vec::with_capacity(dp[m][n] as usize);
    while i > 0 && j > 0 {
        if a[i - 1] == b[j - 1] {
            out.push(a[i - 1]);
            i -= 1;
            j -= 1;
        } else if dp[i - 1][j] >= dp[i][j - 1] {
            i -= 1;
        } else {
            j -= 1;
        }
    }

    out.reverse();
    out
}
