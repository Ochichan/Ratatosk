use bytes::Bytes;
use itoa::Buffer;

use crate::frame::RespFrame;

const SHARED_OK: &[u8] = b"+OK\r\n";
const SHARED_PONG: &[u8] = b"+PONG\r\n";
const SHARED_NULL_BULK: &[u8] = b"$-1\r\n";
const SHARED_INT_ZERO: &[u8] = b":0\r\n";
const SHARED_INT_ONE: &[u8] = b":1\r\n";

pub fn encode(frame: &RespFrame) -> Bytes {
    if let Some(shared) = shared_encoding(frame) {
        return Bytes::from_static(shared);
    }

    let mut out = Vec::with_capacity(128);
    encode_to_vec(frame, &mut out);
    Bytes::from(out)
}

pub fn encode_to_vec(frame: &RespFrame, out: &mut Vec<u8>) {
    encode_into(frame, out);
}

pub fn encoded_len(frame: &RespFrame) -> usize {
    encoded_len_inner(frame)
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

fn encoded_len_inner(frame: &RespFrame) -> usize {
    if let Some(shared) = shared_encoding(frame) {
        return shared.len();
    }

    match frame {
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
        RespFrame::Array(items) => {
            let mut total = 1usize
                .saturating_add(decimal_len_u64(items.len() as u64))
                .saturating_add(2);
            for item in items {
                total = total.saturating_add(encoded_len_inner(item));
            }
            total
        }
        RespFrame::Push(items) => {
            let mut total = 1usize
                .saturating_add(decimal_len_u64(items.len() as u64))
                .saturating_add(2);
            for item in items {
                total = total.saturating_add(encoded_len_inner(item));
            }
            total
        }
        RespFrame::Map(entries) => {
            let mut total = 1usize
                .saturating_add(decimal_len_u64(entries.len() as u64))
                .saturating_add(2);
            for (key, value) in entries {
                total = total.saturating_add(encoded_len_inner(key));
                total = total.saturating_add(encoded_len_inner(value));
            }
            total
        }
        RespFrame::Null => b"_\r\n".len(),
    }
}

fn encode_into(frame: &RespFrame, out: &mut Vec<u8>) {
    if let Some(shared) = shared_encoding(frame) {
        out.extend_from_slice(shared);
        return;
    }

    match frame {
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
        RespFrame::BulkString(None) => {
            out.extend_from_slice(SHARED_NULL_BULK);
        }
        RespFrame::BulkString(Some(value)) => {
            out.push(b'$');
            let mut len_buf = Buffer::new();
            out.extend_from_slice(len_buf.format(value.len()).as_bytes());
            out.extend_from_slice(b"\r\n");
            out.extend_from_slice(value);
            out.extend_from_slice(b"\r\n");
        }
        RespFrame::Array(items) => {
            out.push(b'*');
            let mut len_buf = Buffer::new();
            out.extend_from_slice(len_buf.format(items.len()).as_bytes());
            out.extend_from_slice(b"\r\n");
            for item in items {
                encode_into(item, out);
            }
        }
        RespFrame::Push(items) => {
            out.push(b'>');
            let mut len_buf = Buffer::new();
            out.extend_from_slice(len_buf.format(items.len()).as_bytes());
            out.extend_from_slice(b"\r\n");
            for item in items {
                encode_into(item, out);
            }
        }
        RespFrame::Map(entries) => {
            out.push(b'%');
            let mut len_buf = Buffer::new();
            out.extend_from_slice(len_buf.format(entries.len()).as_bytes());
            out.extend_from_slice(b"\r\n");
            for (key, value) in entries {
                encode_into(key, out);
                encode_into(value, out);
            }
        }
        RespFrame::Null => {
            out.extend_from_slice(b"_\r\n");
        }
    }
}

fn shared_encoding(frame: &RespFrame) -> Option<&'static [u8]> {
    match frame {
        RespFrame::SimpleString(value) if value.as_ref() == b"OK" => Some(SHARED_OK),
        RespFrame::SimpleString(value) if value.as_ref() == b"PONG" => Some(SHARED_PONG),
        RespFrame::BulkString(None) => Some(SHARED_NULL_BULK),
        RespFrame::Integer(0) => Some(SHARED_INT_ZERO),
        RespFrame::Integer(1) => Some(SHARED_INT_ONE),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use crate::frame::RespFrame;

    use super::{encode, encode_to_vec, encoded_len};

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
            RespFrame::Null,
        ];

        for frame in frames {
            let encoded = encode(&frame);
            assert_eq!(encoded_len(&frame), encoded.len());
        }
    }
}
