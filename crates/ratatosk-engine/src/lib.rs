//! # ratatosk-engine
//!
//! Core Redis engine: keyspace, command dispatch, and data structure management.
//!
//! Key modules:
//! - [`keyspace`] — `ServerState`, `StoredValue`, `PubSubState`, and all DB operations.
//! - [`command`] — 420 command handlers with ACL, transactions, and blocking support.
//! - [`eviction`] — 8 maxmemory policies (LRU/LFU/random/TTL) with sampling.
//! - [`expiry`] — Active expiry cycle for server_cron.
//! - [`notification`] — Keyspace notification system (`__keyspace@<db>__`/`__keyevent@<db>__`).
//! - [`hll`] — HyperLogLog probabilistic counting.
//! - [`slot`] — CRC16 hash slot computation for cluster key routing.

#![forbid(unsafe_code)]

pub mod command;
pub mod direct;
pub mod eviction;
pub mod expiry;
pub mod hll;
pub mod keyspace;
pub mod notification;
pub mod object;
pub mod security;
pub mod slot;
