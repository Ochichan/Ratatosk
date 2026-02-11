//! # ratatosk-persist
//!
//! Persistence subsystem: RDB snapshots and AOF append-only files.
//!
//! - [`rdb`] — Binary RDB format (Redis 7+ compatible). `RdbSaver` serializes
//!   `ServerState` to disk; `RdbLoader` restores it with CRC64 verification.
//! - [`aof`] — Append-only file persistence. `AofWriter` appends RESP-encoded
//!   commands; `AofRecovery` replays them; `AofManifest` tracks BASE+INCR files.
//! - [`atomic`] — Atomic file write (tempfile → fsync → rename).
//! - [`error`] — `PersistError` covering I/O, corruption, and checksum failures.

#![forbid(unsafe_code)]

pub mod aof;
pub mod atomic;
pub mod error;
pub mod rdb;
