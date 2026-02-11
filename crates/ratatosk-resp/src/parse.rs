use bytes::{Bytes, BytesMut};
use thiserror::Error;

use crate::frame::RespFrame;

const MAX_ARRAY_ELEMENTS: usize = 1024 * 1024;
const MAX_MAP_ENTRIES: usize = 512 * 1024;
const MAX_BULK_LENGTH: usize = 512 * 1024 * 1024;
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
    Null,
    InlineCommand(Vec<InlineToken>),
}

#[derive(Debug)]
struct InlineToken {
    offset: usize,
    len: usize,
}

// ---------------------------------------------------------------------------
// Public API — two-phase zero-copy parse
// ---------------------------------------------------------------------------

pub fn parse(buf: &mut BytesMut) -> Result<Option<RespFrame>, RespParseError> {
    // Phase 1: validate and measure (read-only view, no copies)
    let Some((desc, consumed)) = describe_frame_at(buf.as_ref(), 0)? else {
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
        b'*' => describe_array(data, idx),
        b'%' => describe_map(data, idx),
        b'_' => describe_null(data, idx),
        _ => describe_inline_command(data, idx),
    }
}

fn describe_simple_string(
    data: &[u8],
    idx: usize,
) -> Result<Option<(FrameDesc, usize)>, RespParseError> {
    let Some((line_start, line_len, line_consumed)) = parse_line_offsets(data, idx + 1) else {
        return Ok(None);
    };

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

fn describe_array(data: &[u8], idx: usize) -> Result<Option<(FrameDesc, usize)>, RespParseError> {
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
        return Ok(Some((FrameDesc::Null, cur - idx)));
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
        let Some((desc, consumed)) = describe_frame_at(data, cur)? else {
            return Ok(None);
        };
        cur += consumed;
        descs.push(desc);
    }

    Ok(Some((FrameDesc::Array(descs), cur - idx)))
}

fn describe_map(data: &[u8], idx: usize) -> Result<Option<(FrameDesc, usize)>, RespParseError> {
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
        let Some((key_desc, key_consumed)) = describe_frame_at(data, cur)? else {
            return Ok(None);
        };
        cur += key_consumed;

        let Some((val_desc, val_consumed)) = describe_frame_at(data, cur)? else {
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

fn describe_inline_command(
    data: &[u8],
    idx: usize,
) -> Result<Option<(FrameDesc, usize)>, RespParseError> {
    let Some((line_start, line_len, line_consumed)) = parse_line_offsets(data, idx) else {
        return Ok(None);
    };

    let line = &data[line_start..line_start + line_len];
    let mut tokens = Vec::new();
    let mut pos = 0;
    while pos < line.len() {
        if line[pos].is_ascii_whitespace() {
            pos += 1;
            continue;
        }
        let start = pos;
        while pos < line.len() && !line[pos].is_ascii_whitespace() {
            pos += 1;
        }
        tokens.push(InlineToken {
            offset: line_start + start,
            len: pos - start,
        });
    }

    Ok(Some((FrameDesc::InlineCommand(tokens), line_consumed)))
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

        FrameDesc::Null => RespFrame::Null,

        FrameDesc::InlineCommand(tokens) => {
            let args = tokens
                .into_iter()
                .map(|t| RespFrame::BulkString(Some(frozen.slice(t.offset..t.offset + t.len))))
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
