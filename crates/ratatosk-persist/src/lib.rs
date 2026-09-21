//! # ratatosk-persist
//!
//! Persistence subsystem: RDB snapshots and AOF append-only files.
//!
//! - [`rdb`] — Binary snapshot in the Redis RDB container layout (version 12)
//!   with Ratatosk-private value types. `RdbSaver` serializes a snapshot to
//!   disk; `RdbLoader` restores it with CRC64 verification. It is not a Redis
//!   file-interchange format.
//! - [`aof`] — Append-only file persistence. `AofWriter` appends RESP-encoded
//!   commands; `AofRecovery` replays them; `AofManifest` tracks BASE+INCR files.
//! - [`atomic`] — Atomic file write (tempfile → fsync → rename).
//! - [`error`] — `PersistError` covering I/O, corruption, and checksum failures.

#![forbid(unsafe_code)]

pub mod aof;
pub mod atomic;
pub mod error;
pub mod rdb;
