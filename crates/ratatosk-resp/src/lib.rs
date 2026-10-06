//! # ratatosk-resp
//!
//! Zero-copy RESP2/RESP3 protocol parser and encoder.
//!
//! The parser ([`parse()`]) is incremental: it returns `Ok(None)` on incomplete
//! input and consumes only complete frames from the buffer. Parsed values
//! reference the original `BytesMut` via `Bytes::slice()` — no data is copied.
//! Aggregate nesting and inline command length are bounded, so hostile input
//! ends in a [`RespParseError`] rather than unbounded recursion or buffering.
//!
//! The encoder ([`encode_for_version()`] for replies, [`encode()`] for exact
//! frames) returns `Bytes`; the `encode_to_vec*` variants append to a caller's
//! buffer. Common replies (`+OK`, `+PONG`, `:0`, `$-1`, ...) use shared static
//! encodings.

#![forbid(unsafe_code)]

pub mod encode;
pub mod frame;
#[doc(hidden)]
pub mod fuzz_support;
pub mod parse;

pub use encode::{
    RespVersion, encode, encode_for_version, encode_to_vec, encode_to_vec_for_version, encoded_len,
    encoded_len_for_version,
};
pub use frame::RespFrame;
pub use parse::{RespParseError, parse};
