//! # Direct API for Embedded Use
//!
//! Zero-copy direct access to `ServerState` for embedded use cases.
//! This module provides a `DirectDb` struct that bypasses the RESP protocol
//! layer, eliminating allocation overhead for callers that embed Ratatosk
//! directly in their applications.
//!
//! ## Example
//!
//! ```ignore
//! use ratatosk_engine::keyspace::ServerState;
//! use ratatosk_engine::direct::DirectDb;
//!
//! let mut server = ServerState::with_default_dbs();
//! let mut db = server.direct(0);
//!
//! // SortedSet operations
//! db.zadd(b"myzset", 1.0, b"member1");
//! db.zadd(b"myzset", 2.0, b"member2");
//!
//! // Zero-copy range query
//! let entries = db.zrange_with_scores(b"myzset", 0, -1);
//! for (member, score) in &entries {
//!     println!("{}: {}", String::from_utf8_lossy(member), score);
//! }
//! ```

use bytes::{Bytes, BytesMut};
use hashbrown::{HashMap, HashSet};
use smallvec::SmallVec;

use crate::keyspace::{purge_expired_key, HashFieldEntry, ServerState, SortedSet, StoredValue};
use ratatosk_core::time::now_ms;

/// Maximum number of items to return in range operations by default.
const DEFAULT_MAX_RANGE: usize = 10_000;

// ---------------------------------------------------------------------------
// DirectDb - Zero-copy direct database access
// ---------------------------------------------------------------------------

/// Zero-copy direct access to a single database within `ServerState`.
///
/// This struct provides methods to manipulate keys and values directly
/// without going through the RESP protocol layer, eliminating allocation
/// overhead for embedded use cases.
pub struct DirectDb<'a> {
    server: &'a mut ServerState,
    db_idx: usize,
}

impl<'a> DirectDb<'a> {
    /// Create a new `DirectDb` for the specified database index.
    pub(crate) fn new(server: &'a mut ServerState, db_idx: usize) -> Self {
        Self { server, db_idx }
    }

    /// Read path: check expiry via `&[u8]` lookup, allocate `Bytes` only when
    /// the key is actually expired (the rare case).
    fn purge_if_expired_read(&mut self, key: &[u8], now: i64) {
        let is_expired = self
            .server
            .db(self.db_idx)
            .get(key)
            .is_some_and(|v| v.expire_at_ms.is_some_and(|at| at <= now));
        if is_expired {
            let key_bytes = Bytes::copy_from_slice(key);
            purge_expired_key(self.server.db_mut(self.db_idx), &key_bytes, now);
        }
    }

    /// Write path: caller already holds an owned `Bytes` key.
    fn purge_if_expired_write(&mut self, key: &Bytes, now: i64) {
        purge_expired_key(self.server.db_mut(self.db_idx), key, now);
    }

    /// Touch the key version for watch notifications.
    fn touch_version(&mut self, key: &Bytes) {
        self.server.touch_key_version(self.db_idx, key.clone());
    }
}

// ---------------------------------------------------------------------------
// SortedSet Operations — inner implementations
// ---------------------------------------------------------------------------

impl DirectDb<'_> {
    #[allow(clippy::too_many_arguments)]
    fn zadd_opts_inner(
        &mut self,
        key: Bytes,
        score: f64,
        member: Bytes,
        opts: ZaddOptions,
        now: i64,
    ) -> i64 {
        if !score.is_finite() {
            return 0;
        }
        self.purge_if_expired_write(&key, now);

        let result = {
            let db = self.server.db_mut(self.db_idx);
            match db.entry(key.clone()) {
                hashbrown::hash_map::Entry::Vacant(vacant) => {
                    if opts.xx {
                        return 0;
                    }
                    let mut zset = SortedSet::default();
                    zset.insert(member, score);
                    vacant.insert(StoredValue::sorted_set(zset, None));
                    1i64
                }
                hashbrown::hash_map::Entry::Occupied(mut occupied) => {
                    let Some(zset) = occupied.get_mut().as_sorted_set_mut() else {
                        return 0;
                    };
                    let existing_score = zset.score(&member);
                    match existing_score {
                        Some(old_score) => {
                            if opts.nx { return 0; }
                            if opts.gt && score <= old_score { return 0; }
                            if opts.lt && score >= old_score { return 0; }
                            if (score - old_score).abs() > f64::EPSILON
                                || score.to_bits() != old_score.to_bits()
                            {
                                zset.insert(member, score);
                                if opts.ch { 1 } else { 0 }
                            } else {
                                0
                            }
                        }
                        None => {
                            if opts.xx { return 0; }
                            zset.insert(member, score);
                            1
                        }
                    }
                }
            }
        };

        self.touch_version(&key);
        result
    }

    fn zrem_inner(&mut self, key: Bytes, member: Bytes, now: i64) -> bool {
        self.purge_if_expired_write(&key, now);

        let (removed, is_empty) = {
            let db = self.server.db_mut(self.db_idx);
            let Some(entry) = db.get_mut(&key) else { return false; };
            let Some(zset) = entry.as_sorted_set_mut() else { return false; };
            let removed = zset.remove(&member);
            (removed, zset.is_empty())
        };

        if removed {
            self.touch_version(&key);
            if is_empty {
                self.server.db_mut(self.db_idx).remove(&key);
            }
        }
        removed
    }

    fn zremrangebyscore_inner(&mut self, key: Bytes, min: f64, max: f64, now: i64) -> i64 {
        self.purge_if_expired_write(&key, now);

        let (removed, is_empty) = {
            let db = self.server.db_mut(self.db_idx);
            let Some(entry) = db.get_mut(&key) else { return 0 };
            let Some(zset) = entry.as_sorted_set_mut() else { return 0 };
            let removed = zset.remove_range_by_score(min, max) as i64;
            (removed, zset.is_empty())
        };

        if is_empty {
            self.server.db_mut(self.db_idx).remove(&key);
        }
        if removed > 0 {
            self.touch_version(&key);
        }
        removed
    }
}

// ---------------------------------------------------------------------------
// SortedSet Operations — public API
// ---------------------------------------------------------------------------

impl DirectDb<'_> {
    /// Add a member with a score to a sorted set.
    ///
    /// Returns the number of elements added (1 if new, 0 if score updated).
    /// Creates the sorted set if it doesn't exist.
    pub fn zadd(&mut self, key: &[u8], score: f64, member: &[u8]) -> i64 {
        self.zadd_opts(key, score, member, ZaddOptions::default())
    }

    /// Add a member with score and options to a sorted set.
    pub fn zadd_opts(&mut self, key: &[u8], score: f64, member: &[u8], opts: ZaddOptions) -> i64 {
        let key_bytes = Bytes::copy_from_slice(key);
        let member_bytes = Bytes::copy_from_slice(member);
        let now = now_ms();
        self.zadd_opts_inner(key_bytes, score, member_bytes, opts, now)
    }

    /// Remove a member from a sorted set.
    ///
    /// Returns `true` if the member was removed, `false` if it didn't exist.
    pub fn zrem(&mut self, key: &[u8], member: &[u8]) -> bool {
        let key_bytes = Bytes::copy_from_slice(key);
        let member_bytes = Bytes::copy_from_slice(member);
        let now = now_ms();
        self.zrem_inner(key_bytes, member_bytes, now)
    }

    /// Get members within a rank range from a sorted set.
    pub fn zrange_with_scores(&mut self, key: &[u8], start: i64, stop: i64) -> Vec<(Bytes, f64)> {
        self.zrange_with_scores_limit(key, start, stop, DEFAULT_MAX_RANGE)
    }

    /// Get members within a rank range with a limit.
    pub fn zrange_with_scores_limit(
        &mut self,
        key: &[u8],
        start: i64,
        stop: i64,
        limit: usize,
    ) -> Vec<(Bytes, f64)> {
        let now = now_ms();
        self.purge_if_expired_read(key, now);

        let db = self.server.db(self.db_idx);
        let Some(entry) = db.get(key) else {
            return Vec::new();
        };
        let Some(zset) = entry.as_sorted_set() else {
            return Vec::new();
        };

        let len = zset.len() as i64;
        if len == 0 {
            return Vec::new();
        }

        let start_idx = if start < 0 { (start + len).max(0) as usize } else { start as usize };
        let stop_idx = if stop < 0 {
            (stop + len).max(0) as usize
        } else {
            (stop as usize).min(len.saturating_sub(1) as usize)
        };

        if start_idx > stop_idx || start_idx >= zset.len() {
            return Vec::new();
        }

        let take_len = (stop_idx - start_idx + 1).min(limit);
        zset.by_score
            .keys()
            .skip(start_idx)
            .take(take_len)
            .map(|e| (e.member.clone(), e.score.value()))
            .collect()
    }

    /// Get members within a rank range in reverse order.
    pub fn zrevrange_with_scores(&mut self, key: &[u8], start: i64, stop: i64) -> Vec<(Bytes, f64)> {
        self.zrevrange_with_scores_limit(key, start, stop, DEFAULT_MAX_RANGE)
    }

    /// Get members within a rank range in reverse order with a limit.
    pub fn zrevrange_with_scores_limit(
        &mut self,
        key: &[u8],
        start: i64,
        stop: i64,
        limit: usize,
    ) -> Vec<(Bytes, f64)> {
        let now = now_ms();
        self.purge_if_expired_read(key, now);

        let db = self.server.db(self.db_idx);
        let Some(entry) = db.get(key) else {
            return Vec::new();
        };
        let Some(zset) = entry.as_sorted_set() else {
            return Vec::new();
        };

        let len = zset.len();
        if len == 0 {
            return Vec::new();
        }

        let len_i64 = len as i64;
        let start_idx = if start < 0 { (start + len_i64).max(0) as usize } else { start as usize };
        let stop_idx = if stop < 0 {
            (stop + len_i64).max(0) as usize
        } else {
            (stop as usize).min(len.saturating_sub(1))
        };

        if start_idx > stop_idx || start_idx >= len {
            return Vec::new();
        }

        let skip = len.saturating_sub(1).saturating_sub(stop_idx);
        let take_len = (stop_idx - start_idx + 1).min(limit);

        zset.by_score
            .keys()
            .rev()
            .skip(skip)
            .take(take_len)
            .map(|e| (e.member.clone(), e.score.value()))
            .collect()
    }

    /// Remove members within a score range from a sorted set.
    ///
    /// Uses `SortedSet::remove_range_by_score` for O(log n) seek to `min`
    /// instead of a full linear scan.
    pub fn zremrangebyscore(&mut self, key: &[u8], min: f64, max: f64) -> i64 {
        let key_bytes = Bytes::copy_from_slice(key);
        let now = now_ms();
        self.zremrangebyscore_inner(key_bytes, min, max, now)
    }

    /// Get the score of a member in a sorted set.
    pub fn zscore(&mut self, key: &[u8], member: &[u8]) -> Option<f64> {
        let now = now_ms();
        self.purge_if_expired_read(key, now);

        let db = self.server.db(self.db_idx);
        let entry = db.get(key)?;
        let zset = entry.as_sorted_set()?;
        zset.score(member)
    }

    /// Get the cardinality (number of elements) of a sorted set.
    pub fn zcard(&mut self, key: &[u8]) -> usize {
        let now = now_ms();
        self.purge_if_expired_read(key, now);

        let db = self.server.db(self.db_idx);
        let Some(entry) = db.get(key) else {
            return 0;
        };
        let Some(zset) = entry.as_sorted_set() else {
            return 0;
        };
        zset.len()
    }

    /// Increment the score of a member in a sorted set.
    pub fn zincrby(&mut self, key: &[u8], increment: f64, member: &[u8]) -> Option<f64> {
        if increment.is_nan() {
            return None;
        }

        let key_bytes = Bytes::copy_from_slice(key);
        let member_bytes = Bytes::copy_from_slice(member);
        let now = now_ms();
        self.purge_if_expired_write(&key_bytes, now);

        let new_score = {
            let db = self.server.db_mut(self.db_idx);
            match db.entry(key_bytes.clone()) {
                hashbrown::hash_map::Entry::Vacant(vacant) => {
                    let mut zset = SortedSet::default();
                    zset.insert(member_bytes, increment);
                    vacant.insert(StoredValue::sorted_set(zset, None));
                    increment
                }
                hashbrown::hash_map::Entry::Occupied(mut occupied) => {
                    let zset = occupied.get_mut().as_sorted_set_mut()?;
                    let old = zset.score(&member_bytes).unwrap_or(0.0);
                    let new_score = old + increment;
                    if new_score.is_nan() {
                        return None;
                    }
                    zset.insert(member_bytes, new_score);
                    new_score
                }
            }
        };

        self.touch_version(&key_bytes);
        Some(new_score)
    }
}

// ---------------------------------------------------------------------------
// Hash Operations — inner implementations
// ---------------------------------------------------------------------------

impl DirectDb<'_> {
    fn hset_inner(&mut self, key: Bytes, fields: Vec<(Bytes, Bytes)>, now: i64) -> i64 {
        if fields.is_empty() {
            return 0;
        }
        self.purge_if_expired_write(&key, now);

        let added = {
            let db = self.server.db_mut(self.db_idx);
            match db.entry(key.clone()) {
                hashbrown::hash_map::Entry::Vacant(vacant) => {
                    let mut hash = HashMap::with_capacity(fields.len());
                    for (field, value) in fields {
                        hash.insert(field, HashFieldEntry::new(value));
                    }
                    let added = hash.len() as i64;
                    vacant.insert(StoredValue::hash(hash, None));
                    added
                }
                hashbrown::hash_map::Entry::Occupied(mut occupied) => {
                    let Some(hash) = occupied.get_mut().as_hash_mut() else {
                        return 0;
                    };
                    let mut added = 0i64;
                    for (field, value) in fields {
                        match hash.entry(field) {
                            hashbrown::hash_map::Entry::Vacant(v) => {
                                v.insert(HashFieldEntry::new(value));
                                added += 1;
                            }
                            hashbrown::hash_map::Entry::Occupied(mut o) => {
                                o.get_mut().value = value;
                            }
                        }
                    }
                    added
                }
            }
        };

        self.touch_version(&key);
        added
    }

    fn hset_single_inner(&mut self, key: Bytes, field: Bytes, value: Bytes, now: i64) -> i64 {
        self.purge_if_expired_write(&key, now);

        let added = {
            let db = self.server.db_mut(self.db_idx);
            match db.entry(key.clone()) {
                hashbrown::hash_map::Entry::Vacant(vacant) => {
                    let mut hash = HashMap::with_capacity(1);
                    hash.insert(field, HashFieldEntry::new(value));
                    vacant.insert(StoredValue::hash(hash, None));
                    1i64
                }
                hashbrown::hash_map::Entry::Occupied(mut occupied) => {
                    let Some(hash) = occupied.get_mut().as_hash_mut() else {
                        return 0;
                    };
                    match hash.entry(field) {
                        hashbrown::hash_map::Entry::Vacant(v) => {
                            v.insert(HashFieldEntry::new(value));
                            1i64
                        }
                        hashbrown::hash_map::Entry::Occupied(mut o) => {
                            o.get_mut().value = value;
                            0i64
                        }
                    }
                }
            }
        };

        self.touch_version(&key);
        added
    }

    fn hdel_inner(&mut self, key: Bytes, field: Bytes, now: i64) -> bool {
        self.purge_if_expired_write(&key, now);

        let (removed, is_empty) = {
            let db = self.server.db_mut(self.db_idx);
            let Some(entry) = db.get_mut(&key) else { return false; };
            let Some(hash) = entry.as_hash_mut() else { return false; };
            let removed = hash.remove(&field).is_some();
            (removed, hash.is_empty())
        };

        if removed {
            self.touch_version(&key);
            if is_empty {
                self.server.db_mut(self.db_idx).remove(&key);
            }
        }
        removed
    }
}

// ---------------------------------------------------------------------------
// Hash Operations — public API
// ---------------------------------------------------------------------------

impl DirectDb<'_> {
    /// Set a field in a hash.
    pub fn hset(&mut self, key: &[u8], field: &[u8], value: &[u8]) -> i64 {
        let key_bytes = Bytes::copy_from_slice(key);
        let field_bytes = Bytes::copy_from_slice(field);
        let value_bytes = Bytes::copy_from_slice(value);
        let now = now_ms();
        self.hset_single_inner(key_bytes, field_bytes, value_bytes, now)
    }

    /// Set multiple fields in a hash.
    pub fn hset_multi(&mut self, key: &[u8], fields: &[(&[u8], &[u8])]) -> i64 {
        if fields.is_empty() {
            return 0;
        }
        let key_bytes = Bytes::copy_from_slice(key);
        let now = now_ms();
        let owned: Vec<(Bytes, Bytes)> = fields
            .iter()
            .map(|(f, v)| (Bytes::copy_from_slice(f), Bytes::copy_from_slice(v)))
            .collect();
        self.hset_inner(key_bytes, owned, now)
    }

    /// Get a field from a hash.
    pub fn hget(&mut self, key: &[u8], field: &[u8]) -> Option<Bytes> {
        let now = now_ms();
        self.purge_if_expired_read(key, now);

        let db = self.server.db(self.db_idx);
        let entry = db.get(key)?;
        let hash = entry.as_hash()?;
        let field_entry = hash.get(field)?; // &[u8] lookup via Borrow
        Some(field_entry.value.clone())
    }

    /// Get multiple fields from a hash.
    pub fn hmget(&mut self, key: &[u8], fields: &[&[u8]]) -> Vec<Option<Bytes>> {
        let now = now_ms();
        self.purge_if_expired_read(key, now);

        let db = self.server.db(self.db_idx);
        let Some(entry) = db.get(key) else {
            return fields.iter().map(|_| None).collect();
        };
        let Some(hash) = entry.as_hash() else {
            return fields.iter().map(|_| None).collect();
        };

        fields
            .iter()
            .map(|field| {
                hash.get(*field).map(|e| e.value.clone()) // &[u8] lookup via Borrow
            })
            .collect()
    }

    /// Get all fields and values from a hash.
    pub fn hgetall(&mut self, key: &[u8]) -> Vec<(Bytes, Bytes)> {
        let now = now_ms();
        self.purge_if_expired_read(key, now);

        let db = self.server.db(self.db_idx);
        let Some(entry) = db.get(key) else {
            return Vec::new();
        };
        let Some(hash) = entry.as_hash() else {
            return Vec::new();
        };

        hash.iter()
            .map(|(field, entry)| (field.clone(), entry.value.clone()))
            .collect()
    }

    /// Delete a field from a hash.
    pub fn hdel(&mut self, key: &[u8], field: &[u8]) -> bool {
        let key_bytes = Bytes::copy_from_slice(key);
        let field_bytes = Bytes::copy_from_slice(field);
        let now = now_ms();
        self.hdel_inner(key_bytes, field_bytes, now)
    }

    /// Check if a field exists in a hash.
    pub fn hexists(&mut self, key: &[u8], field: &[u8]) -> bool {
        let now = now_ms();
        self.purge_if_expired_read(key, now);

        let db = self.server.db(self.db_idx);
        let Some(entry) = db.get(key) else {
            return false;
        };
        let Some(hash) = entry.as_hash() else {
            return false;
        };
        hash.contains_key(field) // &[u8] lookup via Borrow
    }

    /// Get the number of fields in a hash.
    pub fn hlen(&mut self, key: &[u8]) -> usize {
        let now = now_ms();
        self.purge_if_expired_read(key, now);

        let db = self.server.db(self.db_idx);
        let Some(entry) = db.get(key) else {
            return 0;
        };
        let Some(hash) = entry.as_hash() else {
            return 0;
        };
        hash.len()
    }
}

// ---------------------------------------------------------------------------
// Set Operations — inner implementations
// ---------------------------------------------------------------------------

impl DirectDb<'_> {
    fn sadd_inner(&mut self, key: Bytes, members: Vec<Bytes>, now: i64) -> i64 {
        if members.is_empty() {
            return 0;
        }
        self.purge_if_expired_write(&key, now);

        let added = {
            let db = self.server.db_mut(self.db_idx);
            match db.entry(key.clone()) {
                hashbrown::hash_map::Entry::Vacant(vacant) => {
                    let mut set = HashSet::with_capacity(members.len());
                    for member in members {
                        set.insert(member);
                    }
                    let added = set.len() as i64;
                    vacant.insert(StoredValue::set(set, None));
                    added
                }
                hashbrown::hash_map::Entry::Occupied(mut occupied) => {
                    let Some(set) = occupied.get_mut().as_set_mut() else {
                        return 0;
                    };
                    let mut added = 0i64;
                    for member in members {
                        if set.insert(member) {
                            added += 1;
                        }
                    }
                    added
                }
            }
        };

        if added > 0 {
            self.touch_version(&key);
        }
        added
    }

    fn sadd_single_inner(&mut self, key: Bytes, member: Bytes, now: i64) -> bool {
        self.purge_if_expired_write(&key, now);

        let added = {
            let db = self.server.db_mut(self.db_idx);
            match db.entry(key.clone()) {
                hashbrown::hash_map::Entry::Vacant(vacant) => {
                    let mut set = HashSet::with_capacity(1);
                    set.insert(member);
                    vacant.insert(StoredValue::set(set, None));
                    true
                }
                hashbrown::hash_map::Entry::Occupied(mut occupied) => {
                    let Some(set) = occupied.get_mut().as_set_mut() else {
                        return false;
                    };
                    set.insert(member)
                }
            }
        };

        if added {
            self.touch_version(&key);
        }
        added
    }

    fn srem_inner(&mut self, key: Bytes, member: Bytes, now: i64) -> bool {
        self.purge_if_expired_write(&key, now);

        let (removed, is_empty) = {
            let db = self.server.db_mut(self.db_idx);
            let Some(entry) = db.get_mut(&key) else { return false; };
            let Some(set) = entry.as_set_mut() else { return false; };
            let removed = set.remove(&member);
            (removed, set.is_empty())
        };

        if removed {
            self.touch_version(&key);
            if is_empty {
                self.server.db_mut(self.db_idx).remove(&key);
            }
        }
        removed
    }
}

// ---------------------------------------------------------------------------
// Set Operations — public API
// ---------------------------------------------------------------------------

impl DirectDb<'_> {
    /// Add a member to a set.
    pub fn sadd(&mut self, key: &[u8], member: &[u8]) -> bool {
        let key_bytes = Bytes::copy_from_slice(key);
        let member_bytes = Bytes::copy_from_slice(member);
        let now = now_ms();
        self.sadd_single_inner(key_bytes, member_bytes, now)
    }

    /// Add multiple members to a set.
    pub fn sadd_multi(&mut self, key: &[u8], members: &[&[u8]]) -> i64 {
        if members.is_empty() {
            return 0;
        }
        let key_bytes = Bytes::copy_from_slice(key);
        let now = now_ms();
        let owned: Vec<Bytes> = members.iter().map(|m| Bytes::copy_from_slice(m)).collect();
        self.sadd_inner(key_bytes, owned, now)
    }

    /// Remove a member from a set.
    pub fn srem(&mut self, key: &[u8], member: &[u8]) -> bool {
        let key_bytes = Bytes::copy_from_slice(key);
        let member_bytes = Bytes::copy_from_slice(member);
        let now = now_ms();
        self.srem_inner(key_bytes, member_bytes, now)
    }

    /// Check if a member exists in a set.
    pub fn sismember(&mut self, key: &[u8], member: &[u8]) -> bool {
        let now = now_ms();
        self.purge_if_expired_read(key, now);

        let db = self.server.db(self.db_idx);
        let Some(entry) = db.get(key) else {
            return false;
        };
        let Some(set) = entry.as_set() else {
            return false;
        };
        set.contains(member) // &[u8] lookup via Borrow
    }

    /// Get all members of a set.
    pub fn smembers(&mut self, key: &[u8]) -> Vec<Bytes> {
        let now = now_ms();
        self.purge_if_expired_read(key, now);

        let db = self.server.db(self.db_idx);
        let Some(entry) = db.get(key) else {
            return Vec::new();
        };
        let Some(set) = entry.as_set() else {
            return Vec::new();
        };
        set.iter().cloned().collect()
    }

    /// Get the number of members in a set.
    pub fn scard(&mut self, key: &[u8]) -> usize {
        let now = now_ms();
        self.purge_if_expired_read(key, now);

        let db = self.server.db(self.db_idx);
        let Some(entry) = db.get(key) else {
            return 0;
        };
        let Some(set) = entry.as_set() else {
            return 0;
        };
        set.len()
    }
}

// ---------------------------------------------------------------------------
// String / Key Operations — inner implementations
// ---------------------------------------------------------------------------

impl DirectDb<'_> {
    fn set_inner(&mut self, key: Bytes, value: Bytes, now: i64) {
        self.purge_if_expired_write(&key, now);
        self.server.db_mut(self.db_idx).insert(key.clone(), StoredValue::string(value, None));
        self.touch_version(&key);
    }

    fn del_inner(&mut self, key: Bytes, now: i64) -> bool {
        self.purge_if_expired_write(&key, now);
        let removed = self.server.db_mut(self.db_idx).remove(&key).is_some();
        if removed {
            self.touch_version(&key);
        }
        removed
    }
}

// ---------------------------------------------------------------------------
// String Operations
// ---------------------------------------------------------------------------

impl DirectDb<'_> {
    /// Set a string value.
    pub fn set(&mut self, key: &[u8], value: &[u8]) {
        let key_bytes = Bytes::copy_from_slice(key);
        let value_bytes = Bytes::copy_from_slice(value);
        let now = now_ms();
        self.set_inner(key_bytes, value_bytes, now);
    }

    /// Set a string value with an optional expiration time in milliseconds.
    pub fn set_with_expire(&mut self, key: &[u8], value: &[u8], expire_at_ms: Option<i64>) {
        let key_bytes = Bytes::copy_from_slice(key);
        let value_bytes = Bytes::copy_from_slice(value);
        let now = now_ms();
        self.purge_if_expired_write(&key_bytes, now);
        self.server.db_mut(self.db_idx).insert(key_bytes.clone(), StoredValue::string(value_bytes, expire_at_ms));
        self.touch_version(&key_bytes);
    }

    /// Get a string value.
    pub fn get(&mut self, key: &[u8]) -> Option<Bytes> {
        let now = now_ms();
        self.purge_if_expired_read(key, now);
        self.server.db(self.db_idx).get(key)?.as_string().cloned()
    }

    /// Get a string value and set a new value atomically.
    pub fn getset(&mut self, key: &[u8], value: &[u8]) -> Option<Bytes> {
        let key_bytes = Bytes::copy_from_slice(key);
        let value_bytes = Bytes::copy_from_slice(value);
        let now = now_ms();
        self.purge_if_expired_write(&key_bytes, now);
        let old = self
            .server
            .db_mut(self.db_idx)
            .insert(key_bytes.clone(), StoredValue::string(value_bytes, None));
        self.touch_version(&key_bytes);
        old.and_then(|e| e.as_string().cloned())
    }

    /// Set a string value only if the key doesn't exist.
    pub fn setnx(&mut self, key: &[u8], value: &[u8]) -> bool {
        let key_bytes = Bytes::copy_from_slice(key);
        let now = now_ms();
        self.purge_if_expired_write(&key_bytes, now);

        let db = self.server.db_mut(self.db_idx);
        let hashbrown::hash_map::Entry::Vacant(vacant) = db.entry(key_bytes.clone()) else {
            return false;
        };
        let value_bytes = Bytes::copy_from_slice(value);
        vacant.insert(StoredValue::string(value_bytes, None));
        self.touch_version(&key_bytes);
        true
    }

    /// Append to a string value.
    pub fn append(&mut self, key: &[u8], value: &[u8]) -> usize {
        let key_bytes = Bytes::copy_from_slice(key);
        let now = now_ms();
        self.purge_if_expired_write(&key_bytes, now);

        let db = self.server.db_mut(self.db_idx);
        if let Some(entry) = db.get_mut(&key_bytes) {
            if let Some(existing) = entry.as_string() {
                let mut buf = BytesMut::with_capacity(existing.len() + value.len());
                buf.extend_from_slice(existing);
                buf.extend_from_slice(value);
                let len = buf.len();
                entry.data = crate::keyspace::ValueData::String(buf.freeze());
                self.touch_version(&key_bytes);
                return len;
            }
        }

        // Key doesn't exist, create new
        let value_bytes = Bytes::copy_from_slice(value);
        let len = value_bytes.len();
        db.insert(key_bytes.clone(), StoredValue::string(value_bytes, None));
        self.touch_version(&key_bytes);
        len
    }

    /// Get the length of a string value.
    pub fn strlen(&mut self, key: &[u8]) -> usize {
        let now = now_ms();
        self.purge_if_expired_read(key, now);

        let db = self.server.db(self.db_idx);
        let Some(entry) = db.get(key) else {
            return 0;
        };
        let Some(s) = entry.as_string() else {
            return 0;
        };
        s.len()
    }
}

// ---------------------------------------------------------------------------
// Key Operations
// ---------------------------------------------------------------------------

impl DirectDb<'_> {
    /// Delete a key.
    pub fn del(&mut self, key: &[u8]) -> bool {
        let key_bytes = Bytes::copy_from_slice(key);
        let now = now_ms();
        self.del_inner(key_bytes, now)
    }

    /// Delete multiple keys.
    pub fn del_multi(&mut self, keys: &[&[u8]]) -> i64 {
        let now = now_ms();
        let mut removed = 0i64;
        for key in keys {
            let key_bytes = Bytes::copy_from_slice(key);
            if self.del_inner(key_bytes, now) {
                removed += 1;
            }
        }
        removed
    }

    /// Check if a key exists.
    pub fn exists(&mut self, key: &[u8]) -> bool {
        let now = now_ms();
        self.purge_if_expired_read(key, now);
        self.server.db(self.db_idx).contains_key(key)
    }

    /// Get the type of a key.
    pub fn type_of(&mut self, key: &[u8]) -> Option<&'static str> {
        let now = now_ms();
        self.purge_if_expired_read(key, now);

        let db = self.server.db(self.db_idx);
        let entry = db.get(key)?;
        Some(entry.type_name())
    }

    /// Set an expiration time on a key.
    pub fn expire(&mut self, key: &[u8], expire_at_ms: i64) -> bool {
        let key_bytes = Bytes::copy_from_slice(key);

        let db = self.server.db_mut(self.db_idx);
        let Some(entry) = db.get_mut(&key_bytes) else {
            return false;
        };
        entry.expire_at_ms = Some(expire_at_ms);
        true
    }

    /// Set an expiration time in seconds from now.
    pub fn expire_seconds(&mut self, key: &[u8], seconds: i64) -> bool {
        let expire_at_ms = now_ms() + seconds * 1000;
        self.expire(key, expire_at_ms)
    }

    /// Get the TTL of a key in milliseconds.
    pub fn ttl_ms(&mut self, key: &[u8]) -> Option<i64> {
        let db = self.server.db(self.db_idx);
        let entry = db.get(key)?;
        let expire_at_ms = entry.expire_at_ms?;

        let now = now_ms();
        let remaining = expire_at_ms - now;
        Some(remaining.max(0))
    }

    /// Get the TTL of a key in seconds.
    pub fn ttl(&mut self, key: &[u8]) -> Option<i64> {
        self.ttl_ms(key).map(|ms| ms / 1000)
    }

    /// Remove the expiration from a key.
    pub fn persist(&mut self, key: &[u8]) -> bool {
        let key_bytes = Bytes::copy_from_slice(key);

        let db = self.server.db_mut(self.db_idx);
        let Some(entry) = db.get_mut(&key_bytes) else {
            return false;
        };
        if entry.expire_at_ms.is_none() {
            return false;
        }
        entry.expire_at_ms = None;
        true
    }

    /// Rename a key.
    pub fn rename(&mut self, old_key: &[u8], new_key: &[u8]) -> bool {
        let old_key_bytes = Bytes::copy_from_slice(old_key);
        let new_key_bytes = Bytes::copy_from_slice(new_key);
        let now = now_ms();
        self.purge_if_expired_write(&old_key_bytes, now);

        let db = self.server.db_mut(self.db_idx);
        let Some(value) = db.remove(&old_key_bytes) else {
            return false;
        };

        let new_key_touch = new_key_bytes.clone(); // ref-count incr (cheap)
        db.insert(new_key_bytes, value);
        self.touch_version(&old_key_bytes);
        self.touch_version(&new_key_touch);
        true
    }
}

// ---------------------------------------------------------------------------
// ServerState extension
// ---------------------------------------------------------------------------

impl ServerState {
    /// Get a `DirectDb` for direct access to a specific database.
    pub fn direct(&mut self, db_idx: usize) -> DirectDb<'_> {
        DirectDb::new(self, db_idx)
    }
}

// ---------------------------------------------------------------------------
// Options
// ---------------------------------------------------------------------------

/// Options for ZADD operations.
#[derive(Debug, Clone, Copy, Default)]
pub struct ZaddOptions {
    /// Only add new elements.
    pub nx: bool,
    /// Only update existing elements.
    pub xx: bool,
    /// Only update if new score is greater.
    pub gt: bool,
    /// Only update if new score is less.
    pub lt: bool,
    /// Return changed count instead of added.
    pub ch: bool,
}

impl ZaddOptions {
    /// Create default options.
    pub fn new() -> Self {
        Self::default()
    }

    /// Set NX flag.
    pub fn nx(mut self) -> Self {
        self.nx = true;
        self
    }

    /// Set XX flag.
    pub fn xx(mut self) -> Self {
        self.xx = true;
        self
    }

    /// Set GT flag.
    pub fn gt(mut self) -> Self {
        self.gt = true;
        self
    }

    /// Set LT flag.
    pub fn lt(mut self) -> Self {
        self.lt = true;
        self
    }

    /// Set CH flag.
    pub fn ch(mut self) -> Self {
        self.ch = true;
        self
    }
}

// ---------------------------------------------------------------------------
// Batch Operations
// ---------------------------------------------------------------------------

/// A batch operation to be executed atomically.
#[derive(Debug, Clone)]
pub enum BatchOp {
    /// Add to sorted set
    ZAdd { key: Bytes, score: f64, member: Bytes },
    /// Remove from sorted set
    ZRem { key: Bytes, member: Bytes },
    /// Remove by score range
    ZRemRangeByScore { key: Bytes, min: f64, max: f64 },
    /// Set hash field
    HSet { key: Bytes, field: Bytes, value: Bytes },
    /// Delete hash field
    HDel { key: Bytes, field: Bytes },
    /// Add to set
    SAdd { key: Bytes, member: Bytes },
    /// Remove from set
    SRem { key: Bytes, member: Bytes },
    /// Set string
    Set { key: Bytes, value: Bytes },
    /// Delete key
    Del { key: Bytes },
}

/// Result of a batch operation.
#[derive(Debug, Clone, Default)]
pub struct BatchResult {
    /// Number of elements added/modified
    pub added: i64,
    /// Number of elements removed
    pub removed: i64,
    /// Keys that were touched (cheap `Bytes` ref-count clones)
    pub touched_keys: Vec<Bytes>,
}

/// Builder for batch operations.
pub struct BatchBuilder {
    ops: Vec<BatchOp>,
}

impl BatchBuilder {
    /// Create a new batch builder.
    pub fn new() -> Self {
        Self { ops: Vec::new() }
    }

    /// Create a new batch builder pre-sized for `n` operations.
    pub fn with_capacity(n: usize) -> Self {
        Self { ops: Vec::with_capacity(n) }
    }

    /// Add a ZADD operation.
    pub fn zadd(mut self, key: &[u8], score: f64, member: &[u8]) -> Self {
        self.ops.push(BatchOp::ZAdd {
            key: Bytes::copy_from_slice(key),
            score,
            member: Bytes::copy_from_slice(member),
        });
        self
    }

    /// Add a ZREM operation.
    pub fn zrem(mut self, key: &[u8], member: &[u8]) -> Self {
        self.ops.push(BatchOp::ZRem {
            key: Bytes::copy_from_slice(key),
            member: Bytes::copy_from_slice(member),
        });
        self
    }

    /// Add a ZREMRANGEBYSCORE operation.
    pub fn zremrangebyscore(mut self, key: &[u8], min: f64, max: f64) -> Self {
        self.ops.push(BatchOp::ZRemRangeByScore {
            key: Bytes::copy_from_slice(key),
            min,
            max,
        });
        self
    }

    /// Add an HSET operation.
    pub fn hset(mut self, key: &[u8], field: &[u8], value: &[u8]) -> Self {
        self.ops.push(BatchOp::HSet {
            key: Bytes::copy_from_slice(key),
            field: Bytes::copy_from_slice(field),
            value: Bytes::copy_from_slice(value),
        });
        self
    }

    /// Add an HDEL operation.
    pub fn hdel(mut self, key: &[u8], field: &[u8]) -> Self {
        self.ops.push(BatchOp::HDel {
            key: Bytes::copy_from_slice(key),
            field: Bytes::copy_from_slice(field),
        });
        self
    }

    /// Add an SADD operation.
    pub fn sadd(mut self, key: &[u8], member: &[u8]) -> Self {
        self.ops.push(BatchOp::SAdd {
            key: Bytes::copy_from_slice(key),
            member: Bytes::copy_from_slice(member),
        });
        self
    }

    /// Add an SREM operation.
    pub fn srem(mut self, key: &[u8], member: &[u8]) -> Self {
        self.ops.push(BatchOp::SRem {
            key: Bytes::copy_from_slice(key),
            member: Bytes::copy_from_slice(member),
        });
        self
    }

    /// Add a SET operation.
    pub fn set(mut self, key: &[u8], value: &[u8]) -> Self {
        self.ops.push(BatchOp::Set {
            key: Bytes::copy_from_slice(key),
            value: Bytes::copy_from_slice(value),
        });
        self
    }

    /// Add a DEL operation.
    pub fn del(mut self, key: &[u8]) -> Self {
        self.ops.push(BatchOp::Del {
            key: Bytes::copy_from_slice(key),
        });
        self
    }

    /// Execute all batched operations atomically.
    pub fn commit(self, db: &mut DirectDb<'_>) -> BatchResult {
        let mut result = BatchResult::default();
        let mut touched: SmallVec<[Bytes; 8]> = SmallVec::new();
        let now = now_ms();

        for op in self.ops {
            match op {
                BatchOp::ZAdd { key, score, member } => {
                    let added = db.zadd_opts_inner(key.clone(), score, member, ZaddOptions::default(), now);
                    result.added += added;
                    touched.push(key);
                }
                BatchOp::ZRem { key, member } => {
                    if db.zrem_inner(key.clone(), member, now) {
                        result.removed += 1;
                    }
                    touched.push(key);
                }
                BatchOp::ZRemRangeByScore { key, min, max } => {
                    let removed = db.zremrangebyscore_inner(key.clone(), min, max, now);
                    result.removed += removed;
                    touched.push(key);
                }
                BatchOp::HSet { key, field, value } => {
                    let added = db.hset_single_inner(key.clone(), field, value, now);
                    result.added += added;
                    touched.push(key);
                }
                BatchOp::HDel { key, field } => {
                    if db.hdel_inner(key.clone(), field, now) {
                        result.removed += 1;
                    }
                    touched.push(key);
                }
                BatchOp::SAdd { key, member } => {
                    if db.sadd_single_inner(key.clone(), member, now) {
                        result.added += 1;
                    }
                    touched.push(key);
                }
                BatchOp::SRem { key, member } => {
                    if db.srem_inner(key.clone(), member, now) {
                        result.removed += 1;
                    }
                    touched.push(key);
                }
                BatchOp::Set { key, value } => {
                    db.set_inner(key.clone(), value, now);
                    result.added += 1;
                    touched.push(key);
                }
                BatchOp::Del { key } => {
                    if db.del_inner(key.clone(), now) {
                        result.removed += 1;
                    }
                    touched.push(key);
                }
            }
        }

        touched.sort_unstable();
        touched.dedup();
        result.touched_keys = touched.into_vec();
        result
    }
}

impl Default for BatchBuilder {
    fn default() -> Self {
        Self::new()
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn make_server() -> ServerState {
        ServerState::with_default_dbs()
    }

    #[test]
    fn zadd_basic() {
        let mut server = make_server();
        let mut db = server.direct(0);

        let added = db.zadd(b"myzset", 1.0, b"one");
        assert_eq!(added, 1);

        let added = db.zadd(b"myzset", 2.0, b"two");
        assert_eq!(added, 1);

        let added = db.zadd(b"myzset", 1.5, b"one");
        assert_eq!(added, 0);

        assert_eq!(db.zcard(b"myzset"), 2);
    }

    #[test]
    fn zadd_options() {
        let mut server = make_server();
        let mut db = server.direct(0);

        // NX: don't update existing
        db.zadd(b"myzset", 1.0, b"one");
        let added = db.zadd_opts(b"myzset", 2.0, b"one", ZaddOptions::new().nx());
        assert_eq!(added, 0); // NX blocks update, nothing added
        assert_eq!(db.zscore(b"myzset", b"one"), Some(1.0));

        // XX: don't add new
        let added = db.zadd_opts(b"myzset", 3.0, b"three", ZaddOptions::new().xx());
        assert_eq!(added, 0); // XX blocks new member, nothing added
        assert_eq!(db.zscore(b"myzset", b"three"), None);

        // GT: only update if greater (use ch to see changes)
        let changed = db.zadd_opts(b"myzset", 2.0, b"one", ZaddOptions::new().gt().ch());
        assert_eq!(changed, 1); // Changed because 2.0 > 1.0
        assert_eq!(db.zscore(b"myzset", b"one"), Some(2.0));

        // GT: don't update if not greater
        let changed = db.zadd_opts(b"myzset", 1.0, b"one", ZaddOptions::new().gt().ch());
        assert_eq!(changed, 0); // Not changed because 1.0 <= 2.0
    }

    #[test]
    fn zrange_with_scores() {
        let mut server = make_server();
        let mut db = server.direct(0);

        db.zadd(b"myzset", 1.0, b"one");
        db.zadd(b"myzset", 2.0, b"two");
        db.zadd(b"myzset", 3.0, b"three");

        let range = db.zrange_with_scores(b"myzset", 0, -1);
        assert_eq!(range.len(), 3);
        assert_eq!(range[0], (Bytes::from("one"), 1.0));
        assert_eq!(range[1], (Bytes::from("two"), 2.0));
        assert_eq!(range[2], (Bytes::from("three"), 3.0));

        let range = db.zrange_with_scores(b"myzset", 1, 2);
        assert_eq!(range.len(), 2);
    }

    #[test]
    fn zrevrange_with_scores() {
        let mut server = make_server();
        let mut db = server.direct(0);

        db.zadd(b"myzset", 1.0, b"one");
        db.zadd(b"myzset", 2.0, b"two");
        db.zadd(b"myzset", 3.0, b"three");

        let range = db.zrevrange_with_scores(b"myzset", 0, -1);
        assert_eq!(range.len(), 3);
        assert_eq!(range[0], (Bytes::from("three"), 3.0));
        assert_eq!(range[1], (Bytes::from("two"), 2.0));
        assert_eq!(range[2], (Bytes::from("one"), 1.0));
    }

    #[test]
    fn zremrangebyscore() {
        let mut server = make_server();
        let mut db = server.direct(0);

        db.zadd(b"myzset", 1.0, b"one");
        db.zadd(b"myzset", 2.0, b"two");
        db.zadd(b"myzset", 3.0, b"three");
        db.zadd(b"myzset", 4.0, b"four");

        let removed = db.zremrangebyscore(b"myzset", 2.0, 3.0);
        assert_eq!(removed, 2);
        assert_eq!(db.zcard(b"myzset"), 2);
    }

    #[test]
    fn zincrby() {
        let mut server = make_server();
        let mut db = server.direct(0);

        let score = db.zincrby(b"myzset", 5.0, b"member");
        assert_eq!(score, Some(5.0));

        let score = db.zincrby(b"myzset", 3.0, b"member");
        assert_eq!(score, Some(8.0));
    }

    #[test]
    fn hset_hget() {
        let mut server = make_server();
        let mut db = server.direct(0);

        let added = db.hset(b"myhash", b"field1", b"value1");
        assert_eq!(added, 1);

        let value = db.hget(b"myhash", b"field1");
        assert_eq!(value, Some(Bytes::from("value1")));

        let added = db.hset(b"myhash", b"field1", b"newvalue");
        assert_eq!(added, 0);
    }

    #[test]
    fn hgetall() {
        let mut server = make_server();
        let mut db = server.direct(0);

        db.hset(b"myhash", b"f1", b"v1");
        db.hset(b"myhash", b"f2", b"v2");

        let all = db.hgetall(b"myhash");
        assert_eq!(all.len(), 2);
    }

    #[test]
    fn hdel() {
        let mut server = make_server();
        let mut db = server.direct(0);

        db.hset(b"myhash", b"field", b"value");
        assert!(db.hdel(b"myhash", b"field"));
        assert!(!db.hdel(b"myhash", b"field"));
        assert_eq!(db.hget(b"myhash", b"field"), None);
    }

    #[test]
    fn sadd_srem() {
        let mut server = make_server();
        let mut db = server.direct(0);

        assert!(db.sadd(b"myset", b"member1"));
        assert!(!db.sadd(b"myset", b"member1"));

        assert_eq!(db.scard(b"myset"), 1);
        assert!(db.srem(b"myset", b"member1"));
        assert!(!db.srem(b"myset", b"member1"));
        assert_eq!(db.scard(b"myset"), 0);
    }

    #[test]
    fn set_get() {
        let mut server = make_server();
        let mut db = server.direct(0);

        db.set(b"mykey", b"myvalue");
        let value = db.get(b"mykey");
        assert_eq!(value, Some(Bytes::from("myvalue")));
    }

    #[test]
    fn setnx() {
        let mut server = make_server();
        let mut db = server.direct(0);

        assert!(db.setnx(b"mykey", b"value1"));
        assert!(!db.setnx(b"mykey", b"value2"));
        assert_eq!(db.get(b"mykey"), Some(Bytes::from("value1")));
    }

    #[test]
    fn del_exists() {
        let mut server = make_server();
        let mut db = server.direct(0);

        db.set(b"mykey", b"value");
        assert!(db.exists(b"mykey"));
        assert!(db.del(b"mykey"));
        assert!(!db.exists(b"mykey"));
    }

    #[test]
    fn expire_ttl() {
        let mut server = make_server();
        let mut db = server.direct(0);

        db.set(b"mykey", b"value");
        assert!(db.expire_seconds(b"mykey", 10));

        let ttl = db.ttl(b"mykey");
        assert!(ttl.is_some());
        assert!(ttl.unwrap() <= 10 && ttl.unwrap() > 8);

        assert!(db.persist(b"mykey"));
        assert_eq!(db.ttl(b"mykey"), None);
    }

    #[test]
    fn rename() {
        let mut server = make_server();
        let mut db = server.direct(0);

        db.set(b"oldkey", b"value");
        assert!(db.rename(b"oldkey", b"newkey"));
        assert!(!db.exists(b"oldkey"));
        assert_eq!(db.get(b"newkey"), Some(Bytes::from("value")));
    }

    #[test]
    fn batch_operations() {
        let mut server = make_server();
        let mut db = server.direct(0);

        let result = BatchBuilder::new()
            .zadd(b"z1", 1.0, b"a")
            .zadd(b"z1", 2.0, b"b")
            .hset(b"h1", b"f1", b"v1")
            .set(b"s1", b"value")
            .commit(&mut db);

        assert_eq!(result.added, 4);
        assert_eq!(result.touched_keys.len(), 3);
        assert_eq!(db.zcard(b"z1"), 2);
        assert_eq!(db.hget(b"h1", b"f1"), Some(Bytes::from("v1")));
        assert_eq!(db.get(b"s1"), Some(Bytes::from("value")));
    }

    #[test]
    fn batch_mixed_operations() {
        let mut server = make_server();
        let mut db = server.direct(0);

        db.zadd(b"zset", 1.0, b"one");
        db.zadd(b"zset", 2.0, b"two");
        db.zadd(b"zset", 3.0, b"three");

        let result = BatchBuilder::new()
            .zadd(b"zset", 4.0, b"four")
            .zrem(b"zset", b"one")
            .zremrangebyscore(b"zset", 2.5, 3.5)
            .commit(&mut db);

        assert_eq!(result.added, 1);
        assert_eq!(result.removed, 2);
        assert_eq!(db.zcard(b"zset"), 2);
    }
}
