//! # ratatosk-resp
//!
//! Zero-copy RESP2/RESP3 protocol parser and encoder.
//!
//! The parser ([`parse`]) is incremental: it returns `Ok(None)` on incomplete input
//! and consumes only complete frames from the buffer. Parsed values reference the
//! original `BytesMut` via `Bytes::slice()` — no data is copied.
//!
//! The encoder ([`encode`]) produces a `Vec<u8>` suitable for writing to a socket,
//! with shared static encodings for common responses (`+OK`, `:0`, `$-1`).

#![forbid(unsafe_code)]

pub mod encode;
pub mod frame;
#[doc(hidden)]
pub mod fuzz_support;
pub mod parse;

pub use encode::{encode, encode_to_vec, encoded_len};
pub use frame::RespFrame;
pub use parse::{RespParseError, parse};
