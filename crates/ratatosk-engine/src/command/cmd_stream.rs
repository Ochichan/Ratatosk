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

/// Redis's `string2ull`: `string2ll` first (a negative result is refused),
/// then `strtoull` in base 10. `strtoull` skips leading whitespace and takes
/// a `+` or `-` sign, so `+5` and ` 5` parse, `-0` is 0, and a negative number
/// that `string2ll` could not hold wraps around. Overflow and trailing bytes
/// are refused.
fn string2ull(raw: &[u8]) -> Option<u64> {
    if let [b'-', b'1'..=b'9', rest @ ..] = raw {
        if rest.iter().all(u8::is_ascii_digit) {
            let text = std::str::from_utf8(raw).ok()?;
            if text.parse::<i64>().is_ok() {
                return None;
            }
        }
    }

    let mut idx = 0usize;
    while idx < raw.len() && matches!(raw[idx], b' ' | b'\t' | b'\n' | 0x0b | 0x0c | b'\r') {
        idx += 1;
    }
    let negative = match raw.get(idx) {
        Some(b'-') => {
            idx += 1;
            true
        }
        Some(b'+') => {
            idx += 1;
            false
        }
        _ => false,
    };
    let digits = &raw[idx..];
    if digits.is_empty() || !digits.iter().all(u8::is_ascii_digit) {
        return None;
    }
    let mut magnitude = 0u64;
    for digit in digits {
        magnitude = magnitude
            .checked_mul(10)?
            .checked_add(u64::from(digit - b'0'))?;
    }
    Some(if negative {
        magnitude.wrapping_neg()
    } else {
        magnitude
    })
}

/// Redis's `streamGenericParseIDOrReply` without the reply: `<ms>-<seq>`, or
/// a bare `<ms>` whose sequence is `missing_seq`. `-` and `+` are the minimum
/// and maximum IDs unless `strict`. With `auto_seq`, `<ms>-*` parses as
/// `(<ms>, 0)` and reports the sequence as not given. Returns the ID and
/// whether the sequence was given.
fn parse_stream_id_inner(
    raw: &[u8],
    missing_seq: u64,
    strict: bool,
    auto_seq: bool,
) -> Option<(StreamId, bool)> {
    if raw.len() > 127 {
        return None;
    }
    // Redis copies the argument into a C buffer, so an embedded NUL ends it.
    let raw = raw
        .iter()
        .position(|byte| *byte == 0)
        .map_or(raw, |nul| &raw[..nul]);

    if raw == b"-" || raw == b"+" {
        if strict {
            return None;
        }
        let id = if raw == b"-" {
            StreamId { ms: 0, seq: 0 }
        } else {
            MAX_STREAM_ID
        };
        return Some((id, true));
    }

    let (ms_raw, seq_raw) = match raw.iter().position(|byte| *byte == b'-') {
        Some(dash) => (&raw[..dash], Some(&raw[dash + 1..])),
        None => (raw, None),
    };
    let ms = string2ull(ms_raw)?;
    let (seq, seq_given) = match seq_raw {
        Some(b"*") if auto_seq => (0, false),
        Some(seq_raw) => (string2ull(seq_raw)?, true),
        None => (missing_seq, true),
    };
    Some((StreamId { ms, seq }, seq_given))
}

/// Parses a stream ID like Redis's `streamGenericParseIDOrReply`.
pub(super) fn parse_stream_id_generic(
    raw: &[u8],
    missing_seq: u64,
    strict: bool,
) -> Option<StreamId> {
    parse_stream_id_inner(raw, missing_seq, strict, false).map(|(id, _)| id)
}

/// A strict ID (`streamParseStrictIDOrReply`): a bare `<ms>` means `<ms>-0`
/// and `-` and `+` are refused.
pub(super) fn parse_strict_stream_id(raw: &[u8]) -> Option<StreamId> {
    parse_stream_id_generic(raw, 0, true)
}

pub(super) const INVALID_STREAM_ID_ERROR: &str =
    "ERR Invalid stream ID specified as stream command argument";

pub(super) fn invalid_stream_id() -> RespFrame {
    err(INVALID_STREAM_ID_ERROR)
}

/// The ID right before `id`, borrowing from the millisecond when the sequence
/// is 0, as Redis's `streamDecrID`. `None` before the first ID.
pub(super) fn decremented_stream_id(id: StreamId) -> Option<StreamId> {
    if id.seq > 0 {
        Some(StreamId {
            ms: id.ms,
            seq: id.seq - 1,
        })
    } else if id.ms > 0 {
        Some(StreamId {
            ms: id.ms - 1,
            seq: u64::MAX,
        })
    } else {
        None
    }
}

/// Which side of an interval an argument is, as in Redis's
/// `streamParseIntervalIDOrReply`.
#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) enum IntervalEdge {
    Start,
    End,
}

/// Parses an interval bound: an ID where `-` and `+` are allowed and an
/// incomplete one gets sequence 0 (start) or the maximum (end). A `(` prefix
/// (only when the argument is longer than one byte) excludes the ID, which
/// then must be a strict ID, and the bound moves one ID inward. Errors are
/// ready-made replies.
pub(super) fn parse_interval_id(raw: &[u8], edge: IntervalEdge) -> Result<StreamId, RespFrame> {
    let missing_seq = match edge {
        IntervalEdge::Start => 0,
        IntervalEdge::End => u64::MAX,
    };
    if raw.len() > 1 && raw[0] == b'(' {
        let id =
            parse_stream_id_generic(&raw[1..], missing_seq, true).ok_or_else(invalid_stream_id)?;
        return match edge {
            IntervalEdge::Start => incremented_stream_id(id)
                .ok_or_else(|| err("ERR invalid start ID for the interval")),
            IntervalEdge::End => {
                decremented_stream_id(id).ok_or_else(|| err("ERR invalid end ID for the interval"))
            }
        };
    }
    parse_stream_id_generic(raw, missing_seq, false).ok_or_else(invalid_stream_id)
}

/// The largest stream ID; a stream whose last ID is this accepts no more entries.
const MAX_STREAM_ID: StreamId = StreamId {
    ms: u64::MAX,
    seq: u64::MAX,
};

/// The ID right after `id`, carrying into the next millisecond when the
/// sequence is exhausted, as Redis's `streamIncrID`. `None` past the last ID.
pub(super) fn incremented_stream_id(id: StreamId) -> Option<StreamId> {
    if id.seq < u64::MAX {
        Some(StreamId {
            ms: id.ms,
            seq: id.seq + 1,
        })
    } else if id.ms < u64::MAX {
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
    let now = u64::try_from(now_ms()).unwrap_or(0);
    if now > last_id.ms {
        Some(StreamId { ms: now, seq: 0 })
    } else {
        incremented_stream_id(last_id)
    }
}

/// The ID `XADD <ms>-*` assigns. Within the last ID's millisecond the sequence
/// continues and, unlike `*`, never carries: an exhausted sequence is `None`.
fn next_stream_id_for_ms(last_id: StreamId, ms: u64) -> Option<StreamId> {
    if ms != last_id.ms {
        return Some(StreamId { ms, seq: 0 });
    }
    (last_id.seq < u64::MAX).then(|| StreamId {
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
        AutoSeq(u64),
        Explicit(StreamId),
    }
    let invalid_id = || CommandOutcome::reply(invalid_stream_id());
    let spec = if id_raw.as_ref() == b"*" {
        IdSpec::Auto
    } else {
        let Some((parsed, seq_given)) = parse_stream_id_inner(id_raw, 0, true, true) else {
            return invalid_id();
        };
        if !seq_given {
            IdSpec::AutoSeq(parsed.ms)
        } else {
            if parsed == (StreamId { ms: 0, seq: 0 }) {
                return CommandOutcome::reply(err(
                    "ERR The ID specified in XADD must be greater than 0-0",
                ));
            }
            IdSpec::Explicit(parsed)
        }
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

    // As Redis's xrangeGenericCommand: the interval bounds first, the start
    // before the end (XREVRANGE lists the end first), then COUNT.
    let (low_arg, high_arg) = if reverse {
        (&args[2], &args[1])
    } else {
        (&args[1], &args[2])
    };
    let low = match parse_interval_id(low_arg, IntervalEdge::Start) {
        Ok(id) => id,
        Err(reply) => return CommandOutcome::reply(reply),
    };
    let high = match parse_interval_id(high_arg, IntervalEdge::End) {
        Ok(id) => id,
        Err(reply) => return CommandOutcome::reply(reply),
    };
    let low_high = (low, high);

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
        } else if id_raw.as_ref() == b"+" {
            // `+` reads the last entry, so the threshold is the ID right
            // before it (0-0 when the stream is empty or missing).
            stream
                .and_then(|stream| stream.last())
                .map_or(StreamId { ms: 0, seq: 0 }, |last| {
                    decremented_stream_id(last.id).unwrap_or(last.id)
                })
        } else {
            let Some(parsed) = parse_strict_stream_id(id_raw) else {
                return CommandOutcome::reply(invalid_stream_id());
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
                return CommandOutcome::reply(RespFrame::NullArray);
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

#[cfg(test)]
mod tests {
    use super::*;

    fn id(ms: u64, seq: u64) -> StreamId {
        StreamId { ms, seq }
    }

    #[test]
    fn string2ull_matches_redis() {
        assert_eq!(string2ull(b"0"), Some(0));
        assert_eq!(string2ull(b"18446744073709551615"), Some(u64::MAX));
        assert_eq!(string2ull(b"18446744073709551616"), None);
        // strtoull accepts a leading plus sign, whitespace and leading zeros.
        assert_eq!(string2ull(b"+5"), Some(5));
        assert_eq!(string2ull(b" 5"), Some(5));
        assert_eq!(string2ull(b"\t\n5"), Some(5));
        assert_eq!(string2ull(b"007"), Some(7));
        // Trailing bytes, empty input and non-decimal forms are refused.
        assert_eq!(string2ull(b"5 "), None);
        assert_eq!(string2ull(b""), None);
        assert_eq!(string2ull(b"+"), None);
        assert_eq!(string2ull(b"0x10"), None);
        assert_eq!(string2ull(b"1e3"), None);
        // A negative that string2ll parses is refused, but `-0` and a negative
        // beyond i64 go through strtoull and wrap.
        assert_eq!(string2ull(b"-1"), None);
        assert_eq!(string2ull(b"-9223372036854775808"), None);
        assert_eq!(string2ull(b"-0"), Some(0));
        assert_eq!(
            string2ull(b"-9223372036854775809"),
            Some(9_223_372_036_854_775_807)
        );
        assert_eq!(string2ull(b"-18446744073709551615"), Some(1));
        assert_eq!(string2ull(b"-18446744073709551616"), None);
    }

    #[test]
    fn generic_parser_matches_redis() {
        assert_eq!(parse_stream_id_generic(b"5", 0, true), Some(id(5, 0)));
        assert_eq!(parse_stream_id_generic(b"5", 9, true), Some(id(5, 9)));
        assert_eq!(parse_stream_id_generic(b"5-6", 9, true), Some(id(5, 6)));
        assert_eq!(parse_stream_id_generic(b"+5-+6", 0, true), Some(id(5, 6)));
        assert_eq!(parse_stream_id_generic(b"5-", 0, true), None);
        assert_eq!(parse_stream_id_generic(b"-5", 0, true), None);
        assert_eq!(parse_stream_id_generic(b"5-6-7", 0, true), None);
        assert_eq!(parse_stream_id_generic(b"5-*", 0, true), None);
        assert_eq!(parse_stream_id_generic(b"", 0, false), None);
        assert_eq!(parse_stream_id_generic(b"abc", 0, false), None);
        // `-` and `+` only when not strict.
        assert_eq!(parse_stream_id_generic(b"-", 0, true), None);
        assert_eq!(parse_stream_id_generic(b"+", 0, true), None);
        assert_eq!(parse_stream_id_generic(b"-", 7, false), Some(id(0, 0)));
        assert_eq!(parse_stream_id_generic(b"+", 7, false), Some(MAX_STREAM_ID));
        // 127 bytes is the longest accepted argument.
        let mut long = vec![b'0'; 126];
        long.push(b'5');
        assert_eq!(parse_stream_id_generic(&long, 0, true), Some(id(5, 0)));
        long.insert(0, b'0');
        assert_eq!(parse_stream_id_generic(&long, 0, true), None);
        // The auto-sequence form reports that the sequence was not given.
        assert_eq!(
            parse_stream_id_inner(b"5-*", 0, true, true),
            Some((id(5, 0), false))
        );
        assert_eq!(
            parse_stream_id_inner(b"5-1", 0, true, true),
            Some((id(5, 1), true))
        );
    }

    #[test]
    fn interval_parser_matches_redis() {
        let start = |raw: &[u8]| parse_interval_id(raw, IntervalEdge::Start);
        let end = |raw: &[u8]| parse_interval_id(raw, IntervalEdge::End);
        assert_eq!(start(b"5"), Ok(id(5, 0)));
        assert_eq!(end(b"5"), Ok(id(5, u64::MAX)));
        assert_eq!(start(b"-"), Ok(id(0, 0)));
        assert_eq!(end(b"+"), Ok(MAX_STREAM_ID));
        assert_eq!(start(b"(5-1"), Ok(id(5, 2)));
        assert_eq!(start(b"(5-18446744073709551615"), Ok(id(6, 0)));
        assert_eq!(start(b"(5"), Ok(id(5, 1)));
        assert_eq!(end(b"(5-1"), Ok(id(5, 0)));
        assert_eq!(end(b"(5-0"), Ok(id(4, u64::MAX)));
        assert_eq!(end(b"(5"), Ok(id(5, u64::MAX - 1)));
        assert_eq!(
            start(b"(18446744073709551615-18446744073709551615"),
            Err(err("ERR invalid start ID for the interval"))
        );
        assert_eq!(
            end(b"(0-0"),
            Err(err("ERR invalid end ID for the interval"))
        );
        // A lone `(` is not an exclusive prefix, so it is just a bad ID, and
        // `(-` and `(+` are refused because the exclusive form is strict.
        for raw in [&b"("[..], b"(-", b"(+", b"(x", b"((5"] {
            assert_eq!(start(raw), Err(invalid_stream_id()), "{raw:?}");
            assert_eq!(end(raw), Err(invalid_stream_id()), "{raw:?}");
        }
    }
}
