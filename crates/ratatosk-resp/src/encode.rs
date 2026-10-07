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
            extend_line(out, value);
            out.extend_from_slice(b"\r\n");
        }
        RespFrame::Error(value) => {
            out.push(b'-');
            extend_line(out, value);
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

/// Appends the payload of a line-framed type (simple string or error).
///
/// A CR or LF inside the payload would end the line early and let the rest be
/// read as further replies. Error messages routinely quote client input (an
/// unknown command name, an option), so like Redis each is replaced with a
/// space. The replacement keeps the encoded length unchanged.
#[inline]
fn extend_line(out: &mut Vec<u8>, value: &[u8]) {
    if memchr::memchr2(b'\r', b'\n', value).is_none() {
        out.extend_from_slice(value);
    } else {
        extend_line_replacing_breaks(out, value);
    }
}

#[cold]
#[inline(never)]
fn extend_line_replacing_breaks(out: &mut Vec<u8>, value: &[u8]) {
    out.extend(value.iter().map(|&byte| match byte {
        b'\r' | b'\n' => b' ',
        other => other,
    }));
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

/// Encodes a logical reply as a sequence of bounded byte pieces.
///
/// Concatenating the pieces gives exactly the bytes of
/// [`encode_to_vec_for_version`]. Small values are gathered into pieces of
/// about `chunk_bytes`. A bulk string of at least `zero_copy_min` bytes is
/// yielded as its own `Bytes` handle, without copying. A writer can therefore
/// send a reply of any size while holding at most one piece in memory, even
/// when the reply repeats a large value many times.
pub struct ReplySegments<'a> {
    /// Work left, innermost last. Aggregates keep an iterator over their
    /// children, so this grows with nesting depth rather than reply width.
    pending: Vec<Pending<'a>>,
    ready: std::collections::VecDeque<Bytes>,
    buf: Vec<u8>,
    chunk_bytes: usize,
    zero_copy_min: usize,
}

enum Pending<'a> {
    One(&'a RespFrame, RespVersion),
    Items(std::slice::Iter<'a, RespFrame>, RespVersion),
    Entries(std::slice::Iter<'a, (RespFrame, RespFrame)>, RespVersion),
}

impl<'a> ReplySegments<'a> {
    #[must_use]
    pub fn new(
        frame: &'a RespFrame,
        version: RespVersion,
        chunk_bytes: usize,
        zero_copy_min: usize,
    ) -> Self {
        Self {
            pending: vec![Pending::One(frame, version)],
            ready: std::collections::VecDeque::new(),
            buf: Vec::new(),
            chunk_bytes: chunk_bytes.max(1),
            zero_copy_min: zero_copy_min.max(1),
        }
    }

    fn take_buf(&mut self) -> Bytes {
        Bytes::from(std::mem::take(&mut self.buf))
    }

    /// Writes one frame's own bytes and queues its children in wire order.
    fn step(&mut self, frame: &'a RespFrame, version: RespVersion) {
        match frame {
            RespFrame::Versioned { version, frame } => self.pending.push(Pending::One(
                frame,
                RespVersion::from_protocol_version(*version),
            )),
            RespFrame::Array(items) => {
                write_aggregate_header(b'*', items.len(), &mut self.buf);
                self.pending.push(Pending::Items(items.iter(), version));
            }
            RespFrame::Push(items) => {
                let marker = match version {
                    RespVersion::Resp2 => b'*',
                    RespVersion::Resp3 => b'>',
                };
                write_aggregate_header(marker, items.len(), &mut self.buf);
                self.pending.push(Pending::Items(items.iter(), version));
            }
            RespFrame::Map(entries) => {
                match version {
                    RespVersion::Resp3 => {
                        write_aggregate_header(b'%', entries.len(), &mut self.buf);
                    }
                    RespVersion::Resp2 => {
                        write_aggregate_header(b'*', entries.len().saturating_mul(2), &mut self.buf)
                    }
                }
                self.pending.push(Pending::Entries(entries.iter(), version));
            }
            RespFrame::Sequence(frames) => {
                self.pending.push(Pending::Items(frames.iter(), version))
            }
            RespFrame::BulkString(Some(value)) if value.len() >= self.zero_copy_min => {
                self.buf.push(b'$');
                let mut len_buf = Buffer::new();
                self.buf
                    .extend_from_slice(len_buf.format(value.len()).as_bytes());
                self.buf.extend_from_slice(b"\r\n");
                let header = self.take_buf();
                self.ready.push_back(header);
                self.ready.push_back(value.clone());
                self.buf.extend_from_slice(b"\r\n");
            }
            leaf => encode_for_version_into(leaf, &mut self.buf, version),
        }
    }
}

impl Iterator for ReplySegments<'_> {
    type Item = Bytes;

    fn next(&mut self) -> Option<Bytes> {
        loop {
            if let Some(piece) = self.ready.pop_front() {
                return Some(piece);
            }
            if self.buf.len() >= self.chunk_bytes {
                return Some(self.take_buf());
            }
            let Some(work) = self.pending.pop() else {
                return (!self.buf.is_empty()).then(|| self.take_buf());
            };
            match work {
                Pending::One(frame, version) => self.step(frame, version),
                Pending::Items(mut items, version) => {
                    if let Some(item) = items.next() {
                        self.pending.push(Pending::Items(items, version));
                        self.step(item, version);
                    }
                }
                Pending::Entries(mut entries, version) => {
                    if let Some((key, value)) = entries.next() {
                        self.pending.push(Pending::Entries(entries, version));
                        self.pending.push(Pending::One(value, version));
                        self.step(key, version);
                    }
                }
            }
        }
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
    fn line_frames_cannot_smuggle_extra_replies() {
        let error = RespFrame::error_str("ERR unknown command 'foo\r\n+OK'");
        assert_eq!(
            encode(&error).as_ref(),
            b"-ERR unknown command 'foo  +OK'\r\n"
        );
        assert_eq!(encoded_len(&error), encode(&error).len());

        let status = RespFrame::simple_str("a\nb\rc");
        assert_eq!(
            encode_for_version(&status, RespVersion::Resp3).as_ref(),
            b"+a b c\r\n"
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
    #[test]
    fn reply_segments_concatenate_to_the_full_encoding() {
        use super::{ReplySegments, encode_to_vec_for_version};
        use bytes::Bytes;

        let big = Bytes::from(vec![b'x'; 300]);
        let frames = vec![
            RespFrame::ok(),
            RespFrame::Integer(-7),
            RespFrame::BulkString(None),
            RespFrame::NullArray,
            RespFrame::Null,
            RespFrame::simple_str("line\r\nbreak"),
            RespFrame::BulkString(Some(big.clone())),
            RespFrame::Array(vec![
                RespFrame::bulk_str("a"),
                RespFrame::BulkString(Some(big.clone())),
                RespFrame::Array(vec![RespFrame::Integer(1), RespFrame::Null]),
                RespFrame::Map(vec![(
                    RespFrame::bulk_str("k"),
                    RespFrame::BulkString(Some(big.clone())),
                )]),
            ]),
            RespFrame::Push(vec![RespFrame::bulk_str("message"), RespFrame::NullArray]),
            RespFrame::Sequence(vec![
                RespFrame::ok(),
                RespFrame::Versioned {
                    version: 3,
                    frame: Box::new(RespFrame::Map(vec![(
                        RespFrame::bulk_str("proto"),
                        RespFrame::Integer(3),
                    )])),
                },
                RespFrame::Array(vec![]),
            ]),
            RespFrame::Array((0..1000).map(RespFrame::Integer).collect()),
        ];

        for frame in &frames {
            for version in [RespVersion::Resp2, RespVersion::Resp3] {
                let mut expected = Vec::new();
                encode_to_vec_for_version(frame, &mut expected, version);
                for (chunk_bytes, zero_copy_min) in [(1, 1), (7, 64), (64, 256), (4096, 1 << 20)] {
                    let pieces: Vec<Bytes> =
                        ReplySegments::new(frame, version, chunk_bytes, zero_copy_min).collect();
                    assert!(pieces.iter().all(|piece| !piece.is_empty()));
                    assert_eq!(
                        pieces.concat(),
                        expected,
                        "frame {frame:?} version {version:?} chunk {chunk_bytes} zero-copy {zero_copy_min}"
                    );
                }
            }
        }
    }

    #[test]
    fn reply_segments_share_large_bulk_payloads() {
        use super::ReplySegments;
        use bytes::Bytes;

        let big = Bytes::from(vec![b'y'; 1 << 16]);
        let frame = RespFrame::Array(vec![RespFrame::BulkString(Some(big.clone())); 4]);
        let pieces: Vec<Bytes> =
            ReplySegments::new(&frame, RespVersion::Resp2, 1024, 1024).collect();
        let shared = pieces
            .iter()
            .filter(|piece| piece.as_ptr() == big.as_ptr())
            .count();
        assert_eq!(
            shared, 4,
            "every repeat of the payload should be the same buffer"
        );
        assert!(pieces.iter().all(|piece| piece.len() <= big.len()));
    }
}
