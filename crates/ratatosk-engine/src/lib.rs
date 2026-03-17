//! # ratatosk-engine
//!
//! Core Redis engine: keyspace, command dispatch, and data structure management.
//!
//! Key modules:
//! - [`keyspace`] — `ServerState`, `StoredValue`, `PubSubState`, and all DB operations.
//! - [`command`] — 420 command handlers with ACL, transactions, and blocking support.
//! - [`config`] — runtime-adjustable engine configuration state.
//! - [`eviction`] — 8 maxmemory policies (LRU/LFU/random/TTL) with sampling.
//! - [`expiry`] — Active expiry cycle for server_cron.
//! - [`notification`] — Keyspace notification system (`__keyspace@<db>__`/`__keyevent@<db>__`).
//! - [`hll`] — HyperLogLog probabilistic counting.
//! - [`slot`] — CRC16 hash slot computation for cluster key routing.
//! - [`stats`] — slowlog, latency, and lock-free hot-path counters.

#![forbid(unsafe_code)]

pub mod acl;
pub mod clients;
pub mod command;
pub mod config;
pub mod direct;
pub mod eviction;
pub mod expiry;
pub mod hll;
pub mod keyspace;
pub mod notification;
pub mod object;
pub mod pubsub;
pub mod replication;
pub mod security;
pub mod slot;
pub mod stats;
pub mod tracking;
