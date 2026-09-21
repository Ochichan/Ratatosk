use bytes::{Bytes, BytesMut};
use thiserror::Error;

use crate::frame::RespFrame;

const MAX_ARRAY_ELEMENTS: usize = 1024 * 1024;
const MAX_MAP_ENTRIES: usize = 512 * 1024;
const MAX_BULK_LENGTH: usize = 512 * 1024 * 1024;
/// Longest inline command line still searched for its terminator (Redis'
/// `PROTO_INLINE_MAX_SIZE`).
const MAX_INLINE_LENGTH: usize = 64 * 1024;
/// Maximum aggregate nesting depth. Both parse phases recurse once per level,
/// so without this bound a few kilobytes of `*1\r\n` exhaust a thread stack
/// and abort the process. Client requests are flat and AOF timestamp envelopes
/// nest one level, so the limit only has to cover generous reply shapes.
const MAX_NESTING_DEPTH: usize = 128;
const ARRAY_PREALLOC_CAP: usize = 4096;
const MAP_PREALLOC_CAP: usize = 4096;

#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum RespParseError {
    #[error("invalid integer: {0}")]
    InvalidInteger(String),

    #[error("invalid bulk string length: {0}")]
    InvalidBulkLength(String),

    #[error("invalid array length: {0}")]
    InvalidArrayLength(String),

    #[error("invalid map length: {0}")]
    InvalidMapLength(String),

    #[error("invalid frame type byte: 0x{0:02x}")]
    InvalidFrameType(u8),

    #[error("aggregate nesting exceeds limit {MAX_NESTING_DEPTH}")]
    NestingTooDeep,

    #[error("CR or LF inside a simple string or error line")]
    InvalidLine,

    #[error("unbalanced quotes in inline command")]
    UnbalancedQuotes,

    #[error("inline command exceeds {MAX_INLINE_LENGTH} bytes")]
    InlineTooLong,

    #[error("missing CRLF terminator")]
    MissingCrlf,
}

// ---------------------------------------------------------------------------
// Frame descriptor — describes the shape of a parsed frame without copying data
// ---------------------------------------------------------------------------

#[derive(Debug)]
enum FrameDesc {
    SimpleString { data_offset: usize, data_len: usize },
    Error { data_offset: usize, data_len: usize },
    Integer(i64),
    BulkString { data_offset: usize, data_len: usize },
    NullBulk,
    Array(Vec<FrameDesc>),
    Map(Vec<(FrameDesc, FrameDesc)>),
    NullArray,
    Null,
    InlineCommand(Vec<InlineToken>),
}

#[derive(Debug)]
enum InlineToken {
    /// Unquoted argument, referenced in place.
    Slice { offset: usize, len: usize },
    /// Argument rewritten by quoting or escapes.
    Owned(Vec<u8>),
}

// ---------------------------------------------------------------------------
// Public API — two-phase zero-copy parse
// ---------------------------------------------------------------------------

pub fn parse(buf: &mut BytesMut) -> Result<Option<RespFrame>, RespParseError> {
    // Phase 1: validate and measure (read-only view, no copies)
    let Some((desc, consumed)) = describe_frame_at(buf.as_ref(), 0, 0)? else {
        return Ok(None);
    };

    // Phase 2: materialize frames from BytesMut (zero-copy via freeze/slice)
    let frozen = buf.split_to(consumed).freeze();
    let frame = materialize(&frozen, desc);
    Ok(Some(frame))
}

// ---------------------------------------------------------------------------
// Phase 1: Describe (validate + measure, no allocations except Vec for containers)
// ---------------------------------------------------------------------------

fn describe_frame_at(
    data: &[u8],
    idx: usize,
    depth: usize,
) -> Result<Option<(FrameDesc, usize)>, RespParseError> {
    if idx >= data.len() {
        return Ok(None);
    }

    let marker = data[idx];
    match marker {
        b'+' => describe_simple_string(data, idx),
        b'-' => describe_error(data, idx),
        b':' => describe_integer(data, idx),
        b'$' => describe_bulk_string(data, idx),
        b'*' => describe_array(data, idx, depth),
        b'%' => describe_map(data, idx, depth),
        b'_' => describe_null(data, idx),
        // Inline commands are a top-level request form. Inside an aggregate
        // an unknown type byte is damage, not the start of a command.
        _ if depth == 0 => describe_inline_command(data, idx),
        other => Err(RespParseError::InvalidFrameType(other)),
    }
}

/// RESP forbids CR and LF inside simple strings and errors; accepting them
/// would yield payloads the encoder cannot represent.
fn ensure_plain_line(line: &[u8]) -> Result<(), RespParseError> {
    match memchr::memchr2(b'\r', b'\n', line) {
        Some(_) => Err(RespParseError::InvalidLine),
        None => Ok(()),
    }
}

fn describe_simple_string(
    data: &[u8],
    idx: usize,
) -> Result<Option<(FrameDesc, usize)>, RespParseError> {
    let Some((line_start, line_len, line_consumed)) = parse_line_offsets(data, idx + 1) else {
        return Ok(None);
    };
    ensure_plain_line(&data[line_start..line_start + line_len])?;

    Ok(Some((
        FrameDesc::SimpleString {
            data_offset: line_start,
            data_len: line_len,
        },
        1 + line_consumed,
    )))
}

fn describe_error(data: &[u8], idx: usize) -> Result<Option<(FrameDesc, usize)>, RespParseError> {
    let Some((line_start, line_len, line_consumed)) = parse_line_offsets(data, idx + 1) else {
        return Ok(None);
    };
    ensure_plain_line(&data[line_start..line_start + line_len])?;

    Ok(Some((
        FrameDesc::Error {
            data_offset: line_start,
            data_len: line_len,
        },
        1 + line_consumed,
    )))
}

fn describe_integer(data: &[u8], idx: usize) -> Result<Option<(FrameDesc, usize)>, RespParseError> {
    let Some((line_start, line_len, line_consumed)) = parse_line_offsets(data, idx + 1) else {
        return Ok(None);
    };

    let line = &data[line_start..line_start + line_len];
    let text =
        std::str::from_utf8(line).map_err(|_| RespParseError::InvalidInteger("utf8".into()))?;
    let value = text
        .parse::<i64>()
        .map_err(|_| RespParseError::InvalidInteger(text.to_string()))?;

    Ok(Some((FrameDesc::Integer(value), 1 + line_consumed)))
}

fn describe_bulk_string(
    data: &[u8],
    idx: usize,
) -> Result<Option<(FrameDesc, usize)>, RespParseError> {
    let Some((line_start, line_len, line_consumed)) = parse_line_offsets(data, idx + 1) else {
        return Ok(None);
    };

    let line = &data[line_start..line_start + line_len];
    let text =
        std::str::from_utf8(line).map_err(|_| RespParseError::InvalidBulkLength("utf8".into()))?;
    let len = text
        .parse::<i64>()
        .map_err(|_| RespParseError::InvalidBulkLength(text.to_string()))?;

    let header_consumed = 1 + line_consumed;
    if len == -1 {
        return Ok(Some((FrameDesc::NullBulk, header_consumed)));
    }
    if len < -1 {
        return Err(RespParseError::InvalidBulkLength(text.to_string()));
    }

    let len = len as usize;
    if len > MAX_BULK_LENGTH {
        return Err(RespParseError::InvalidBulkLength(format!(
            "bulk length {len} exceeds limit {MAX_BULK_LENGTH}"
        )));
    }
    let body_start = idx + header_consumed;
    let body_end = body_start + len;
    let tail_end = body_end + 2;
    if tail_end > data.len() {
        return Ok(None);
    }

    if data[body_end] != b'\r' || data[body_end + 1] != b'\n' {
        return Err(RespParseError::MissingCrlf);
    }

    Ok(Some((
        FrameDesc::BulkString {
            data_offset: body_start,
            data_len: len,
        },
        header_consumed + len + 2,
    )))
}

fn describe_array(
    data: &[u8],
    idx: usize,
    depth: usize,
) -> Result<Option<(FrameDesc, usize)>, RespParseError> {
    if depth >= MAX_NESTING_DEPTH {
        return Err(RespParseError::NestingTooDeep);
    }
    let Some((_line_start, line_len, line_consumed)) = parse_line_offsets(data, idx + 1) else {
        return Ok(None);
    };

    let line_start_actual = idx + 1;
    let line = &data[line_start_actual..line_start_actual + line_len];
    let text =
        std::str::from_utf8(line).map_err(|_| RespParseError::InvalidArrayLength("utf8".into()))?;
    let len = text
        .parse::<i64>()
        .map_err(|_| RespParseError::InvalidArrayLength(text.to_string()))?;

    let mut cur = idx + 1 + line_consumed;
    if len == -1 {
        return Ok(Some((FrameDesc::NullArray, cur - idx)));
    }
    if len < -1 {
        return Err(RespParseError::InvalidArrayLength(text.to_string()));
    }

    let len_usize = len as usize;
    if len_usize > MAX_ARRAY_ELEMENTS {
        return Err(RespParseError::InvalidArrayLength(format!(
            "array length {len_usize} exceeds limit {MAX_ARRAY_ELEMENTS}"
        )));
    }

    let mut descs = Vec::with_capacity(len_usize.min(ARRAY_PREALLOC_CAP));
    for _ in 0..len_usize {
        let Some((desc, consumed)) = describe_frame_at(data, cur, depth + 1)? else {
            return Ok(None);
        };
        cur += consumed;
        descs.push(desc);
    }

    Ok(Some((FrameDesc::Array(descs), cur - idx)))
}

fn describe_map(
    data: &[u8],
    idx: usize,
    depth: usize,
) -> Result<Option<(FrameDesc, usize)>, RespParseError> {
    if depth >= MAX_NESTING_DEPTH {
        return Err(RespParseError::NestingTooDeep);
    }
    let Some((_line_start, line_len, line_consumed)) = parse_line_offsets(data, idx + 1) else {
        return Ok(None);
    };

    let line_start_actual = idx + 1;
    let line = &data[line_start_actual..line_start_actual + line_len];
    let text =
        std::str::from_utf8(line).map_err(|_| RespParseError::InvalidMapLength("utf8".into()))?;
    let len = text
        .parse::<i64>()
        .map_err(|_| RespParseError::InvalidMapLength(text.to_string()))?;

    if len < 0 {
        return Err(RespParseError::InvalidMapLength(text.to_string()));
    }

    let len_usize = len as usize;
    if len_usize > MAX_MAP_ENTRIES {
        return Err(RespParseError::InvalidMapLength(format!(
            "map length {len_usize} exceeds limit {MAX_MAP_ENTRIES}"
        )));
    }

    let mut cur = idx + 1 + line_consumed;
    let mut entries = Vec::with_capacity(len_usize.min(MAP_PREALLOC_CAP));
    for _ in 0..len_usize {
        let Some((key_desc, key_consumed)) = describe_frame_at(data, cur, depth + 1)? else {
            return Ok(None);
        };
        cur += key_consumed;

        let Some((val_desc, val_consumed)) = describe_frame_at(data, cur, depth + 1)? else {
            return Ok(None);
        };
        cur += val_consumed;

        entries.push((key_desc, val_desc));
    }

    Ok(Some((FrameDesc::Map(entries), cur - idx)))
}

fn describe_null(data: &[u8], idx: usize) -> Result<Option<(FrameDesc, usize)>, RespParseError> {
    if idx + 3 > data.len() {
        return Ok(None);
    }

    if data[idx + 1] != b'\r' || data[idx + 2] != b'\n' {
        return Err(RespParseError::MissingCrlf);
    }

    Ok(Some((FrameDesc::Null, 3)))
}

/// Parses an inline (telnet-style) command the way Redis does: the line ends
/// at LF with an optional preceding CR, and arguments follow `sdssplitargs`
/// rules: `"..."` understands the `\n \r \t \b \a \xHH` escapes and `'...'`
/// only `\'`.
fn describe_inline_command(
    data: &[u8],
    idx: usize,
) -> Result<Option<(FrameDesc, usize)>, RespParseError> {
    let Some(newline) = memchr::memchr(b'\n', &data[idx..]) else {
        if data.len() - idx > MAX_INLINE_LENGTH {
            return Err(RespParseError::InlineTooLong);
        }
        return Ok(None);
    };
    let mut line_end = idx + newline;
    if line_end > idx && data[line_end - 1] == b'\r' {
        line_end -= 1;
    }

    let tokens = split_inline_args(data, idx, line_end)?;
    Ok(Some((FrameDesc::InlineCommand(tokens), newline + 1)))
}

fn split_inline_args(
    data: &[u8],
    start: usize,
    end: usize,
) -> Result<Vec<InlineToken>, RespParseError> {
    let mut tokens = Vec::new();
    let mut pos = start;
    loop {
        while pos < end && is_c_space(data[pos]) {
            pos += 1;
        }
        if pos == end {
            return Ok(tokens);
        }

        let token_start = pos;
        // Becomes Some once a quote forces the argument to be rewritten.
        let mut owned: Option<Vec<u8>> = None;
        let mut in_double = false;
        let mut in_single = false;
        loop {
            if in_double {
                let current = owned.get_or_insert_with(Vec::new);
                match data.get(pos..end).unwrap_or_default() {
                    [] => return Err(RespParseError::UnbalancedQuotes),
                    [b'\\', b'x', high, low, ..]
                        if high.is_ascii_hexdigit() && low.is_ascii_hexdigit() =>
                    {
                        current.push(hex_value(*high) << 4 | hex_value(*low));
                        pos += 4;
                    }
                    [b'\\', escaped, ..] => {
                        current.push(match escaped {
                            b'n' => b'\n',
                            b'r' => b'\r',
                            b't' => b'\t',
                            b'b' => 0x08,
                            b'a' => 0x07,
                            other => *other,
                        });
                        pos += 2;
                    }
                    [b'"', rest @ ..] => {
                        // The closing quote must end the argument.
                        if rest.first().is_some_and(|next| !is_c_space(*next)) {
                            return Err(RespParseError::UnbalancedQuotes);
                        }
                        pos += 1;
                        break;
                    }
                    [byte, ..] => {
                        current.push(*byte);
                        pos += 1;
                    }
                }
            } else if in_single {
                let current = owned.get_or_insert_with(Vec::new);
                match data.get(pos..end).unwrap_or_default() {
                    [] => return Err(RespParseError::UnbalancedQuotes),
                    [b'\\', b'\'', ..] => {
                        current.push(b'\'');
                        pos += 2;
                    }
                    [b'\'', rest @ ..] => {
                        if rest.first().is_some_and(|next| !is_c_space(*next)) {
                            return Err(RespParseError::UnbalancedQuotes);
                        }
                        pos += 1;
                        break;
                    }
                    [byte, ..] => {
                        current.push(*byte);
                        pos += 1;
                    }
                }
            } else {
                if pos == end || matches!(data[pos], b' ' | b'\n' | b'\r' | b'\t') {
                    break;
                }
                match data[pos] {
                    b'"' => in_double = true,
                    b'\'' => in_single = true,
                    byte => {
                        if let Some(current) = owned.as_mut() {
                            current.push(byte);
                        }
                        pos += 1;
                        continue;
                    }
                }
                owned.get_or_insert_with(|| data[token_start..pos].to_vec());
                pos += 1;
            }
        }

        tokens.push(match owned {
            Some(bytes) => InlineToken::Owned(bytes),
            None => InlineToken::Slice {
                offset: token_start,
                len: pos - token_start,
            },
        });
    }
}

/// C `isspace`: separates arguments and must follow a closing quote.
fn is_c_space(byte: u8) -> bool {
    byte.is_ascii_whitespace() || byte == 0x0b
}

fn hex_value(digit: u8) -> u8 {
    match digit {
        b'0'..=b'9' => digit - b'0',
        b'a'..=b'f' => digit - b'a' + 10,
        _ => digit - b'A' + 10,
    }
}

// Returns (line_start_offset, line_length, total_consumed_including_crlf)
fn parse_line_offsets(data: &[u8], idx: usize) -> Option<(usize, usize, usize)> {
    let line_end = find_crlf(data, idx)?;
    let line_len = line_end - idx;
    let consumed = line_len + 2;
    Some((idx, line_len, consumed))
}

fn find_crlf(data: &[u8], idx: usize) -> Option<usize> {
    if idx >= data.len() {
        return None;
    }

    let mut search_idx = idx;
    loop {
        let rel = memchr::memchr(b'\r', &data[search_idx..])?;
        let pos = search_idx + rel;
        if pos + 1 >= data.len() {
            return None;
        }
        if data[pos + 1] == b'\n' {
            return Some(pos);
        }
        search_idx = pos + 1;
    }
}

// ---------------------------------------------------------------------------
// Phase 2: Materialize — extract Bytes from frozen buffer (zero-copy)
// ---------------------------------------------------------------------------

fn materialize(frozen: &Bytes, desc: FrameDesc) -> RespFrame {
    match desc {
        FrameDesc::SimpleString {
            data_offset,
            data_len,
        } => RespFrame::SimpleString(frozen.slice(data_offset..data_offset + data_len)),

        FrameDesc::Error {
            data_offset,
            data_len,
        } => RespFrame::Error(frozen.slice(data_offset..data_offset + data_len)),

        FrameDesc::Integer(value) => RespFrame::Integer(value),

        FrameDesc::BulkString {
            data_offset,
            data_len,
        } => RespFrame::BulkString(Some(frozen.slice(data_offset..data_offset + data_len))),

        FrameDesc::NullBulk => RespFrame::BulkString(None),

        FrameDesc::Array(descs) => {
            let frames = descs.into_iter().map(|d| materialize(frozen, d)).collect();
            RespFrame::Array(frames)
        }

        FrameDesc::Map(entries) => {
            let pairs = entries
                .into_iter()
                .map(|(k, v)| (materialize(frozen, k), materialize(frozen, v)))
                .collect();
            RespFrame::Map(pairs)
        }

        FrameDesc::NullArray => RespFrame::NullArray,

        FrameDesc::Null => RespFrame::Null,

        FrameDesc::InlineCommand(tokens) => {
            let args = tokens
                .into_iter()
                .map(|token| {
                    RespFrame::BulkString(Some(match token {
                        InlineToken::Slice { offset, len } => frozen.slice(offset..offset + len),
                        InlineToken::Owned(bytes) => Bytes::from(bytes),
                    }))
                })
                .collect();
            RespFrame::Array(args)
        }
    }
}

#[cfg(test)]
mod tests {
    use bytes::BytesMut;

    use crate::frame::RespFrame;

    use super::parse;

    #[test]
    fn parse_inline_ping() {
        let mut buf = BytesMut::from(&b"PING\r\n"[..]);
        let frame = parse(&mut buf).expect("parse").expect("frame");
        assert_eq!(buf.len(), 0);
        assert_eq!(
            frame,
            RespFrame::Array(vec![RespFrame::BulkString(Some("PING".into()))])
        );
    }

    fn inline(input: &[u8]) -> Result<Option<RespFrame>, super::RespParseError> {
        parse(&mut BytesMut::from(input))
    }

    fn args(parts: &[&[u8]]) -> RespFrame {
        RespFrame::Array(
            parts
                .iter()
                .map(|part| RespFrame::BulkString(Some(bytes::Bytes::copy_from_slice(part))))
                .collect(),
        )
    }

    #[test]
    fn inline_lines_end_at_lf_like_redis() {
        let mut buf = BytesMut::from(&b"PING\nECHO hi\r\n"[..]);
        assert_eq!(parse(&mut buf), Ok(Some(args(&[b"PING"]))));
        assert_eq!(parse(&mut buf), Ok(Some(args(&[b"ECHO", b"hi"]))));
        assert!(buf.is_empty());

        assert_eq!(inline(b"\r\n"), Ok(Some(RespFrame::Array(Vec::new()))));
        assert_eq!(inline(b"  \t \n"), Ok(Some(RespFrame::Array(Vec::new()))));
        assert_eq!(inline(b"PING"), Ok(None));
    }

    #[test]
    fn inline_arguments_follow_sdssplitargs_quoting() {
        assert_eq!(
            inline(b"SET k \"hello world\"\r\n"),
            Ok(Some(args(&[b"SET", b"k", b"hello world"])))
        );
        assert_eq!(
            inline(b"ECHO \"a\\x41\\n\\\"\\q\"\n"),
            Ok(Some(args(&[b"ECHO", b"aA\n\"q"])))
        );
        assert_eq!(
            inline(b"ECHO 'it\\'s' '\\n'\n"),
            Ok(Some(args(&[b"ECHO", b"it's", b"\\n"])))
        );
        assert_eq!(
            inline(b"ECHO pre\"fix sp\" \"\"\n"),
            Ok(Some(args(&[b"ECHO", b"prefix sp", b""])))
        );
        assert_eq!(
            inline(b"ECHO a\x0bb\n"),
            Ok(Some(args(&[b"ECHO", b"a\x0bb"])))
        );
    }

    #[test]
    fn inline_rejects_unbalanced_quotes_and_oversized_lines() {
        for input in [
            &b"ECHO \"open\n"[..],
            b"ECHO 'open\n",
            b"ECHO \"closed\"tail\n",
            b"ECHO 'closed'tail\n",
            b"ECHO \"trailing\\\n",
        ] {
            assert_eq!(inline(input), Err(super::RespParseError::UnbalancedQuotes));
        }

        let long = vec![b'a'; super::MAX_INLINE_LENGTH + 1];
        assert_eq!(inline(&long), Err(super::RespParseError::InlineTooLong));
        assert_eq!(inline(&long[..super::MAX_INLINE_LENGTH]), Ok(None));
    }

    #[test]
    fn inline_commands_are_only_parsed_at_the_top_level() {
        assert_eq!(
            inline(b"*2\r\nSET k\r\n$1\r\nv\r\n"),
            Err(super::RespParseError::InvalidFrameType(b'S'))
        );
    }

    #[test]
    fn line_frames_reject_embedded_cr_or_lf() {
        for input in [&b"+a\rb\r\n"[..], b"+a\nb\r\n", b"-ERR a\rb\r\n"] {
            assert_eq!(inline(input), Err(super::RespParseError::InvalidLine));
        }
        assert_eq!(
            inline(b"-ERR fine\r\n"),
            Ok(Some(RespFrame::Error("ERR fine".into())))
        );
    }

    #[test]
    fn parse_resp_array_bulk() {
        let mut buf = BytesMut::from(&b"*2\r\n$4\r\nECHO\r\n$2\r\nhi\r\n"[..]);
        let frame = parse(&mut buf).expect("parse").expect("frame");
        assert_eq!(buf.len(), 0);

        assert_eq!(
            frame,
            RespFrame::Array(vec![
                RespFrame::BulkString(Some("ECHO".into())),
                RespFrame::BulkString(Some("hi".into())),
            ])
        );
    }

    #[test]
    fn parse_incremental_bulk() {
        let mut buf = BytesMut::from(&b"$5\r\nhel"[..]);
        let first = parse(&mut buf).expect("parse first");
        assert!(first.is_none());

        buf.extend_from_slice(b"lo\r\n");
        let second = parse(&mut buf).expect("parse second").expect("frame");
        assert_eq!(second, RespFrame::BulkString(Some("hello".into())));
        assert_eq!(buf.len(), 0);
    }

    #[test]
    fn rejects_oversized_array_length() {
        let mut buf = BytesMut::from(&b"*99999999\r\n"[..]);
        let result = parse(&mut buf);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(matches!(err, super::RespParseError::InvalidArrayLength(_)));
    }

    #[test]
    fn rejects_oversized_map_length() {
        let mut buf = BytesMut::from(&b"%99999999\r\n"[..]);
        let result = parse(&mut buf);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(matches!(err, super::RespParseError::InvalidMapLength(_)));
    }

    #[test]
    fn rejects_oversized_bulk_length() {
        let mut buf = BytesMut::from(&b"$999999999\r\n"[..]);
        let result = parse(&mut buf);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(matches!(err, super::RespParseError::InvalidBulkLength(_)));
    }

    #[test]
    fn normal_array_still_parses() {
        let mut buf = BytesMut::from(&b"*1\r\n$4\r\nPING\r\n"[..]);
        let frame = parse(&mut buf).expect("parse").expect("frame");
        assert_eq!(
            frame,
            RespFrame::Array(vec![RespFrame::BulkString(Some("PING".into()))])
        );
    }

    #[test]
    fn zero_copy_bulk_string_shares_buffer() {
        let mut buf = BytesMut::from(&b"$5\r\nhello\r\n"[..]);
        let frame = parse(&mut buf).expect("parse").expect("frame");
        let RespFrame::BulkString(Some(data)) = frame else {
            panic!("expected BulkString");
        };
        assert_eq!(data, &b"hello"[..]);
    }

    fn nested_arrays(depth: usize) -> BytesMut {
        let mut buf = BytesMut::with_capacity(depth * 4 + 7);
        for _ in 0..depth {
            buf.extend_from_slice(b"*1\r\n");
        }
        buf.extend_from_slice(b"$1\r\nx\r\n");
        buf
    }

    #[test]
    fn accepts_nesting_up_to_the_limit() {
        let mut buf = nested_arrays(super::MAX_NESTING_DEPTH);
        let mut frame = parse(&mut buf).expect("parse").expect("frame");
        assert!(buf.is_empty());
        for _ in 0..super::MAX_NESTING_DEPTH {
            let RespFrame::Array(mut items) = frame else {
                panic!("expected nested array");
            };
            frame = items.pop().expect("single element");
        }
        assert_eq!(frame, RespFrame::BulkString(Some("x".into())));
    }

    #[test]
    fn rejects_nesting_beyond_the_limit_without_exhausting_the_stack() {
        // A million levels is 4 MB of input; before the depth bound this
        // overflowed the stack and aborted the whole process.
        for depth in [super::MAX_NESTING_DEPTH + 1, 1_000_000] {
            let mut buf = nested_arrays(depth);
            assert_eq!(parse(&mut buf), Err(super::RespParseError::NestingTooDeep));
        }

        let mut map = BytesMut::new();
        for _ in 0..=super::MAX_NESTING_DEPTH {
            map.extend_from_slice(b"%1\r\n+k\r\n");
        }
        assert_eq!(parse(&mut map), Err(super::RespParseError::NestingTooDeep));
    }

    #[test]
    fn zero_copy_simple_string_shares_buffer() {
        let mut buf = BytesMut::from(&b"+OK\r\n"[..]);
        let frame = parse(&mut buf).expect("parse").expect("frame");
        let RespFrame::SimpleString(data) = frame else {
            panic!("expected SimpleString");
        };
        assert_eq!(data, &b"OK"[..]);
    }
}
