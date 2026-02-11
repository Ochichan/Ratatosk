use std::sync::LazyLock;

use bytes::Bytes;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RespFrame {
    SimpleString(Bytes),
    Error(Bytes),
    Integer(i64),
    BulkString(Option<Bytes>),
    Array(Vec<RespFrame>),
    Map(Vec<(RespFrame, RespFrame)>),
    Null,
}

const SHARED_INTEGER_COUNT: usize = 10000;

static SHARED_INTEGERS: LazyLock<Vec<Bytes>> = LazyLock::new(|| {
    (0..SHARED_INTEGER_COUNT)
        .map(|i| Bytes::from(i.to_string()))
        .collect()
});

impl RespFrame {
    pub fn simple_str(value: &str) -> Self {
        Self::SimpleString(Bytes::copy_from_slice(value.as_bytes()))
    }

    pub fn error_str(value: &str) -> Self {
        Self::Error(Bytes::copy_from_slice(value.as_bytes()))
    }

    pub fn bulk_str(value: &str) -> Self {
        Self::BulkString(Some(Bytes::copy_from_slice(value.as_bytes())))
    }

    pub fn ok() -> Self {
        Self::SimpleString(Bytes::from_static(b"OK"))
    }

    pub fn pong() -> Self {
        Self::SimpleString(Bytes::from_static(b"PONG"))
    }

    pub fn queued() -> Self {
        Self::SimpleString(Bytes::from_static(b"QUEUED"))
    }

    pub fn wrongtype() -> Self {
        Self::Error(Bytes::from_static(
            b"WRONGTYPE Operation against a key holding the wrong kind of value",
        ))
    }

    pub fn syntax_error() -> Self {
        Self::Error(Bytes::from_static(b"ERR syntax error"))
    }

    pub fn shared_integer(n: i64) -> Self {
        if n >= 0 && (n as usize) < SHARED_INTEGER_COUNT {
            Self::BulkString(Some(SHARED_INTEGERS[n as usize].clone()))
        } else {
            Self::BulkString(Some(Bytes::from(n.to_string())))
        }
    }
}
