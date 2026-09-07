use bytes::Bytes;
use itoa::Buffer;

use crate::frame::RespFrame;

/// The negotiated RESP wire version used when projecting logical replies.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RespVersion {
    Resp2,
    Resp3,
}

impl RespVersion {
    /// Converts the protocol version stored by a connection into a wire version.
    #[must_use]
    pub const fn from_protocol_version(protocol_version: i64) -> Self {
        if protocol_version >= 3 {
            Self::Resp3
        } else {
            Self::Resp2
        }
    }
}

const SHARED_OK: &[u8] = b"+OK\r\n";
const SHARED_PONG: &[u8] = b"+PONG\r\n";
const SHARED_QUEUED: &[u8] = b"+QUEUED\r\n";
const SHARED_NULL_BULK: &[u8] = b"$-1\r\n";
const SHARED_NULL_ARRAY: &[u8] = b"*-1\r\n";
const SHARED_NULL: &[u8] = b"_\r\n";
const SHARED_INT_ZERO: &[u8] = b":0\r\n";
const SHARED_INT_ONE: &[u8] = b":1\r\n";
const SHARED_INT_NEG_ONE: &[u8] = b":-1\r\n";
const SHARED_INT_NEG_TWO: &[u8] = b":-2\r\n";

/// Encodes a frame exactly as represented.
///
/// This is the protocol-neutral codec entry point used by the parser fuzzing
/// harness. Server replies should use [`encode_for_version`] instead so logical
/// RESP3-only types are projected for RESP2 clients.
pub fn encode(frame: &RespFrame) -> Bytes {
    if let Some(shared) = shared_encoding(frame) {
        return Bytes::from_static(shared);
    }

    let mut out = Vec::with_capacity(128);
    encode_to_vec(frame, &mut out);
    Bytes::from(out)
}

/// Encodes a logical reply after projecting it to the negotiated RESP version.
pub fn encode_for_version(frame: &RespFrame, version: RespVersion) -> Bytes {
    let mut out = Vec::with_capacity(encoded_len_for_version(frame, version));
    encode_to_vec_for_version(frame, &mut out, version);
    Bytes::from(out)
}

pub fn encode_to_vec(frame: &RespFrame, out: &mut Vec<u8>) {
    encode_into(frame, out);
}

/// Appends a logical reply projected to the negotiated RESP version.
pub fn encode_to_vec_for_version(frame: &RespFrame, out: &mut Vec<u8>, version: RespVersion) {
    encode_for_version_into(frame, out, version);
}

pub fn encoded_len(frame: &RespFrame) -> usize {
    encoded_len_inner(frame)
}

/// Returns the wire length after projecting a logical reply to a RESP version.
pub fn encoded_len_for_version(frame: &RespFrame, version: RespVersion) -> usize {
    encoded_len_for_version_inner(frame, version)
}

fn decimal_len_u64(mut value: u64) -> usize {
    let mut digits = 1usize;
    while value >= 10 {
        value /= 10;
        digits = digits.saturating_add(1);
    }
    digits
}

fn decimal_len_i64(value: i64) -> usize {
    if value < 0 {
        1usize.saturating_add(decimal_len_u64(value.unsigned_abs()))
    } else {
        decimal_len_u64(value as u64)
    }
}

fn aggregate_len(items: &[RespFrame], version: Option<RespVersion>) -> usize {
    let mut total = 1usize
        .saturating_add(decimal_len_u64(items.len() as u64))
        .saturating_add(2);
    for item in items {
        let item_len = match version {
            Some(version) => encoded_len_for_version_inner(item, version),
            None => encoded_len_inner(item),
        };
        total = total.saturating_add(item_len);
    }
    total
}

fn map_len(entries: &[(RespFrame, RespFrame)], version: Option<RespVersion>) -> usize {
    let mut total = 1usize
        .saturating_add(decimal_len_u64(entries.len() as u64))
        .saturating_add(2);
    for (key, value) in entries {
        let key_len = match version {
            Some(version) => encoded_len_for_version_inner(key, version),
            None => encoded_len_inner(key),
        };
        let value_len = match version {
            Some(version) => encoded_len_for_version_inner(value, version),
            None => encoded_len_inner(value),
        };
        total = total.saturating_add(key_len).saturating_add(value_len);
    }
    total
}

fn encoded_len_inner(frame: &RespFrame) -> usize {
    if let Some(shared) = shared_encoding(frame) {
        return shared.len();
    }

    match frame {
        RespFrame::Versioned { version, frame } => {
            encoded_len_for_version_inner(frame, RespVersion::from_protocol_version(*version))
        }
        RespFrame::SimpleString(value) | RespFrame::Error(value) => {
            1usize.saturating_add(value.len()).saturating_add(2)
        }
        RespFrame::Integer(value) => 1usize
            .saturating_add(decimal_len_i64(*value))
            .saturating_add(2),
        RespFrame::BulkString(None) => SHARED_NULL_BULK.len(),
        RespFrame::BulkString(Some(value)) => 1usize
            .saturating_add(decimal_len_u64(value.len() as u64))
            .saturating_add(2)
            .saturating_add(value.len())
            .saturating_add(2),
        RespFrame::Array(items) | RespFrame::Push(items) => aggregate_len(items, None),
        RespFrame::Map(entries) => map_len(entries, None),
        RespFrame::NullArray => SHARED_NULL_ARRAY.len(),
        RespFrame::Null => SHARED_NULL.len(),
        RespFrame::Sequence(frames) => frames.iter().fold(0usize, |total, frame| {
            total.saturating_add(encoded_len_inner(frame))
        }),
    }
}

fn encoded_len_for_version_inner(frame: &RespFrame, version: RespVersion) -> usize {
    match frame {
        RespFrame::Versioned { version, frame } => {
            encoded_len_for_version_inner(frame, RespVersion::from_protocol_version(*version))
        }
        RespFrame::SimpleString(_)
        | RespFrame::Error(_)
        | RespFrame::Integer(_)
        | RespFrame::BulkString(Some(_)) => encoded_len_inner(frame),
        RespFrame::BulkString(None) | RespFrame::Null => match version {
            RespVersion::Resp2 => SHARED_NULL_BULK.len(),
            RespVersion::Resp3 => SHARED_NULL.len(),
        },
        RespFrame::NullArray => match version {
            RespVersion::Resp2 => SHARED_NULL_ARRAY.len(),
            RespVersion::Resp3 => SHARED_NULL.len(),
        },
        RespFrame::Array(items) => aggregate_len(items, Some(version)),
        RespFrame::Push(items) => aggregate_len(items, Some(version)),
        RespFrame::Map(entries) => match version {
            RespVersion::Resp3 => map_len(entries, Some(version)),
            RespVersion::Resp2 => {
                let item_count = entries.len().saturating_mul(2);
                let mut total = 1usize
                    .saturating_add(decimal_len_u64(item_count as u64))
                    .saturating_add(2);
                for (key, value) in entries {
                    total = total
                        .saturating_add(encoded_len_for_version_inner(key, version))
                        .saturating_add(encoded_len_for_version_inner(value, version));
                }
                total
            }
        },
        RespFrame::Sequence(frames) => frames.iter().fold(0usize, |total, frame| {
            total.saturating_add(encoded_len_for_version_inner(frame, version))
        }),
    }
}

fn write_aggregate_header(marker: u8, len: usize, out: &mut Vec<u8>) {
    out.push(marker);
    let mut len_buf = Buffer::new();
    out.extend_from_slice(len_buf.format(len).as_bytes());
    out.extend_from_slice(b"\r\n");
}

fn encode_into(frame: &RespFrame, out: &mut Vec<u8>) {
    if let Some(shared) = shared_encoding(frame) {
        out.extend_from_slice(shared);
        return;
    }

    match frame {
        RespFrame::Versioned { version, frame } => {
            encode_for_version_into(frame, out, RespVersion::from_protocol_version(*version))
        }
        RespFrame::SimpleString(value) => {
            out.push(b'+');
            out.extend_from_slice(value);
            out.extend_from_slice(b"\r\n");
        }
        RespFrame::Error(value) => {
            out.push(b'-');
            out.extend_from_slice(value);
            out.extend_from_slice(b"\r\n");
        }
        RespFrame::Integer(value) => {
            out.push(b':');
            let mut buf = Buffer::new();
            out.extend_from_slice(buf.format(*value).as_bytes());
            out.extend_from_slice(b"\r\n");
        }
        RespFrame::BulkString(None) => out.extend_from_slice(SHARED_NULL_BULK),
        RespFrame::BulkString(Some(value)) => {
            out.push(b'$');
            let mut len_buf = Buffer::new();
            out.extend_from_slice(len_buf.format(value.len()).as_bytes());
            out.extend_from_slice(b"\r\n");
            out.extend_from_slice(value);
            out.extend_from_slice(b"\r\n");
        }
        RespFrame::Array(items) => {
            write_aggregate_header(b'*', items.len(), out);
            for item in items {
                encode_into(item, out);
            }
        }
        RespFrame::Push(items) => {
            write_aggregate_header(b'>', items.len(), out);
            for item in items {
                encode_into(item, out);
            }
        }
        RespFrame::Map(entries) => {
            write_aggregate_header(b'%', entries.len(), out);
            for (key, value) in entries {
                encode_into(key, out);
                encode_into(value, out);
            }
        }
        RespFrame::NullArray => out.extend_from_slice(SHARED_NULL_ARRAY),
        RespFrame::Null => out.extend_from_slice(SHARED_NULL),
        RespFrame::Sequence(frames) => {
            for frame in frames {
                encode_into(frame, out);
            }
        }
    }
}

fn encode_for_version_into(frame: &RespFrame, out: &mut Vec<u8>, version: RespVersion) {
    match frame {
        RespFrame::Versioned { version, frame } => {
            encode_for_version_into(frame, out, RespVersion::from_protocol_version(*version))
        }
        RespFrame::SimpleString(_)
        | RespFrame::Error(_)
        | RespFrame::Integer(_)
        | RespFrame::BulkString(Some(_)) => encode_into(frame, out),
        RespFrame::BulkString(None) | RespFrame::Null => match version {
            RespVersion::Resp2 => out.extend_from_slice(SHARED_NULL_BULK),
            RespVersion::Resp3 => out.extend_from_slice(SHARED_NULL),
        },
        RespFrame::NullArray => match version {
            RespVersion::Resp2 => out.extend_from_slice(SHARED_NULL_ARRAY),
            RespVersion::Resp3 => out.extend_from_slice(SHARED_NULL),
        },
        RespFrame::Array(items) => {
            write_aggregate_header(b'*', items.len(), out);
            for item in items {
                encode_for_version_into(item, out, version);
            }
        }
        RespFrame::Push(items) => {
            let marker = match version {
                RespVersion::Resp2 => b'*',
                RespVersion::Resp3 => b'>',
            };
            write_aggregate_header(marker, items.len(), out);
            for item in items {
                encode_for_version_into(item, out, version);
            }
        }
        RespFrame::Map(entries) => match version {
            RespVersion::Resp3 => {
                write_aggregate_header(b'%', entries.len(), out);
                for (key, value) in entries {
                    encode_for_version_into(key, out, version);
                    encode_for_version_into(value, out, version);
                }
            }
            RespVersion::Resp2 => {
                write_aggregate_header(b'*', entries.len().saturating_mul(2), out);
                for (key, value) in entries {
                    encode_for_version_into(key, out, version);
                    encode_for_version_into(value, out, version);
                }
            }
        },
        RespFrame::Sequence(frames) => {
            for frame in frames {
                encode_for_version_into(frame, out, version);
            }
        }
    }
}

fn shared_encoding(frame: &RespFrame) -> Option<&'static [u8]> {
    match frame {
        RespFrame::SimpleString(value) if value.as_ref() == b"OK" => Some(SHARED_OK),
        RespFrame::SimpleString(value) if value.as_ref() == b"PONG" => Some(SHARED_PONG),
        RespFrame::SimpleString(value) if value.as_ref() == b"QUEUED" => Some(SHARED_QUEUED),
        RespFrame::BulkString(None) => Some(SHARED_NULL_BULK),
        RespFrame::Integer(0) => Some(SHARED_INT_ZERO),
        RespFrame::Integer(1) => Some(SHARED_INT_ONE),
        RespFrame::Integer(-1) => Some(SHARED_INT_NEG_ONE),
        RespFrame::Integer(-2) => Some(SHARED_INT_NEG_TWO),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use crate::frame::RespFrame;

    use super::{
        RespVersion, encode, encode_for_version, encode_to_vec, encoded_len,
        encoded_len_for_version,
    };

    #[test]
    fn encode_simple_string() {
        let out = encode(&RespFrame::simple_str("PONG"));
        assert_eq!(out.as_ref(), b"+PONG\r\n");
    }

    #[test]
    fn encode_bulk_string() {
        let out = encode(&RespFrame::bulk_str("hello"));
        assert_eq!(out.as_ref(), b"$5\r\nhello\r\n");
    }

    #[test]
    fn encode_map() {
        let frame = RespFrame::Map(vec![(RespFrame::bulk_str("proto"), RespFrame::Integer(3))]);
        let out = encode(&frame);
        assert_eq!(out.as_ref(), b"%1\r\n$5\r\nproto\r\n:3\r\n");
    }

    #[test]
    fn encode_push() {
        let frame = RespFrame::Push(vec![
            RespFrame::bulk_str("tracking-redir-broken"),
            RespFrame::Integer(42),
        ]);
        let out = encode(&frame);
        assert_eq!(
            out.as_ref(),
            b">2\r\n$21\r\ntracking-redir-broken\r\n:42\r\n"
        );
    }

    #[test]
    fn resp2_projects_maps_pushes_and_nulls() {
        let frame = RespFrame::Map(vec![
            (RespFrame::bulk_str("proto"), RespFrame::Integer(2)),
            (
                RespFrame::bulk_str("events"),
                RespFrame::Push(vec![RespFrame::BulkString(None)]),
            ),
        ]);

        let encoded = encode_for_version(&frame, RespVersion::Resp2);
        assert_eq!(
            encoded.as_ref(),
            b"*4\r\n$5\r\nproto\r\n:2\r\n$6\r\nevents\r\n*1\r\n$-1\r\n"
        );
        assert_eq!(
            encoded_len_for_version(&frame, RespVersion::Resp2),
            encoded.len()
        );
    }

    #[test]
    fn resp3_preserves_maps_and_pushes_and_uses_untyped_nulls() {
        let frame = RespFrame::Map(vec![(
            RespFrame::bulk_str("events"),
            RespFrame::Push(vec![RespFrame::BulkString(None)]),
        )]);

        let encoded = encode_for_version(&frame, RespVersion::Resp3);
        assert_eq!(encoded.as_ref(), b"%1\r\n$6\r\nevents\r\n>1\r\n_\r\n");
        assert_eq!(
            encoded_len_for_version(&frame, RespVersion::Resp3),
            encoded.len()
        );
    }

    #[test]
    fn resp2_retains_null_bulk_and_null_array_distinctions() {
        assert_eq!(
            encode_for_version(&RespFrame::BulkString(None), RespVersion::Resp2).as_ref(),
            b"$-1\r\n"
        );
        assert_eq!(
            encode_for_version(&RespFrame::NullArray, RespVersion::Resp2).as_ref(),
            b"*-1\r\n"
        );
        assert_eq!(
            encode_for_version(&RespFrame::NullArray, RespVersion::Resp3).as_ref(),
            b"_\r\n"
        );
    }

    #[test]
    fn sequence_writes_independent_top_level_frames() {
        let frame = RespFrame::Sequence(vec![
            RespFrame::Push(vec![RespFrame::bulk_str("subscribe")]),
            RespFrame::Push(vec![RespFrame::bulk_str("unsubscribe")]),
        ]);

        assert_eq!(
            encode_for_version(&frame, RespVersion::Resp2).as_ref(),
            b"*1\r\n$9\r\nsubscribe\r\n*1\r\n$11\r\nunsubscribe\r\n"
        );
        assert_eq!(
            encode_for_version(&frame, RespVersion::Resp3).as_ref(),
            b">1\r\n$9\r\nsubscribe\r\n>1\r\n$11\r\nunsubscribe\r\n"
        );
    }

    #[test]
    fn encode_to_vec_appends() {
        let mut out = Vec::from(&b"prefix:"[..]);
        encode_to_vec(&RespFrame::Integer(1), &mut out);
        assert_eq!(out, b"prefix::1\r\n");
    }

    #[test]
    fn encoded_len_matches_encoded_bytes() {
        let frames = vec![
            RespFrame::simple_str("OK"),
            RespFrame::bulk_str("hello"),
            RespFrame::Array(vec![RespFrame::Integer(-42), RespFrame::BulkString(None)]),
            RespFrame::Map(vec![(
                RespFrame::bulk_str("k"),
                RespFrame::Array(vec![RespFrame::bulk_str("v")]),
            )]),
            RespFrame::NullArray,
            RespFrame::Null,
        ];

        for frame in frames {
            let encoded = encode(&frame);
            assert_eq!(encoded_len(&frame), encoded.len());
        }
    }
}
