use argon2::Argon2;
use argon2::password_hash::{
    PasswordHash, PasswordHasher, PasswordVerifier, SaltString, rand_core::OsRng,
};
use bytes::Bytes;
use hashbrown::{HashMap, HashSet};

use crate::security::{sanitize_acl_log_line, sanitize_slowlog_argv};
use ratatosk_core::time::{now_ms as unix_ms_now, now_sec as unix_sec_now};
use std::{
    cmp::Ordering,
    collections::{BTreeMap, VecDeque},
    path::PathBuf,
};

pub const DEFAULT_DB_COUNT: usize = 16;

pub type DbSnapshot = Vec<HashMap<Bytes, StoredValue>>;

// ---------------------------------------------------------------------------
// Stream data types
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct StreamId {
    pub ms: i64,
    pub seq: i64,
}

#[derive(Debug, Clone)]
pub struct StreamEntry {
    pub id: StreamId,
    pub fields: Vec<(Bytes, Bytes)>,
}

#[derive(Debug, Clone)]
pub struct StreamPendingEntry {
    pub consumer: Bytes,
    pub deliveries: i64,
    pub last_delivered_ms: i64,
}

#[derive(Debug, Clone)]
pub struct StreamConsumer {
    pub seen_time_ms: i64,
    pub pending: HashSet<StreamId>,
}

#[derive(Debug, Clone)]
pub struct StreamGroup {
    pub last_delivered_id: StreamId,
    pub consumers: HashMap<Bytes, StreamConsumer>,
    pub pending: HashMap<StreamId, StreamPendingEntry>,
}

// ---------------------------------------------------------------------------
// SortedSet data types
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy)]
pub struct SortedSetScore(pub f64);

impl SortedSetScore {
    pub fn value(self) -> f64 {
        self.0
    }
}

impl PartialEq for SortedSetScore {
    fn eq(&self, other: &Self) -> bool {
        self.0.total_cmp(&other.0) == Ordering::Equal
    }
}

impl Eq for SortedSetScore {}

impl PartialOrd for SortedSetScore {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for SortedSetScore {
    fn cmp(&self, other: &Self) -> Ordering {
        self.0.total_cmp(&other.0)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SortedSetEntry {
    pub score: SortedSetScore,
    pub member: Bytes,
}

impl PartialOrd for SortedSetEntry {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for SortedSetEntry {
    fn cmp(&self, other: &Self) -> Ordering {
        self.score
            .cmp(&other.score)
            .then_with(|| self.member.cmp(&other.member))
    }
}

#[derive(Debug, Clone, Default)]
pub struct SortedSet {
    pub by_score: BTreeMap<SortedSetEntry, ()>,
    pub by_member: HashMap<Bytes, SortedSetScore>,
}

impl SortedSet {
    pub fn len(&self) -> usize {
        self.by_member.len()
    }

    pub fn is_empty(&self) -> bool {
        self.by_member.is_empty()
    }

    pub fn insert(&mut self, member: Bytes, score: f64) -> bool {
        let new_score = SortedSetScore(score);
        if let Some(old_score) = self.by_member.get(&member).copied() {
            if old_score == new_score {
                return false;
            }
            self.by_score.remove(&SortedSetEntry {
                score: old_score,
                member: member.clone(),
            });
        }
        let is_new = self.by_member.insert(member.clone(), new_score).is_none();
        self.by_score.insert(
            SortedSetEntry {
                score: new_score,
                member,
            },
            (),
        );
        is_new
    }

    pub fn remove(&mut self, member: &Bytes) -> bool {
        if let Some(score) = self.by_member.remove(member) {
            self.by_score.remove(&SortedSetEntry {
                score,
                member: member.clone(),
            });
            true
        } else {
            false
        }
    }

    pub fn score(&self, member: &Bytes) -> Option<f64> {
        self.by_member.get(member).map(|s| s.value())
    }

    pub fn rank(&self, member: &Bytes) -> Option<usize> {
        let score = self.by_member.get(member)?;
        let entry = SortedSetEntry {
            score: *score,
            member: member.clone(),
        };
        Some(self.by_score.range(..&entry).count())
    }

    pub fn rev_rank(&self, member: &Bytes) -> Option<usize> {
        self.rank(member)
            .map(|r| self.len().saturating_sub(1).saturating_sub(r))
    }
}

#[derive(Debug, Clone)]
pub enum ScoreBound {
    NegInf,
    PosInf,
    Inclusive(f64),
    Exclusive(f64),
}

#[derive(Debug, Clone)]
pub enum LexBound {
    NegInf,
    PosInf,
    Inclusive(Bytes),
    Exclusive(Bytes),
}

// ---------------------------------------------------------------------------
// HashFieldEntry — hash field with optional per-field TTL
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
pub struct HashFieldEntry {
    pub value: Bytes,
    pub expire_at_ms: Option<i64>,
}

impl HashFieldEntry {
    pub fn new(value: Bytes) -> Self {
        Self {
            value,
            expire_at_ms: None,
        }
    }

    pub fn with_ttl(value: Bytes, expire_at_ms: i64) -> Self {
        Self {
            value,
            expire_at_ms: Some(expire_at_ms),
        }
    }

    pub fn is_expired(&self, now_ms: i64) -> bool {
        self.expire_at_ms.is_some_and(|at| at <= now_ms)
    }
}

// ---------------------------------------------------------------------------
// ScriptCache — Lua script SHA1 cache
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Default)]
pub struct ScriptCache {
    pub scripts: HashMap<Bytes, Bytes>,
}

// ---------------------------------------------------------------------------
// Encoding tag — describes the internal representation of a value
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum Encoding {
    /// Raw bytes (string values, generic).
    Raw = 0,
    /// Integer-encoded string (fits in i64).
    Int = 1,
    /// Quick list (list type, full encoding).
    QuickList = 4,
    /// Hash table (hash / set full encoding).
    HashTable = 5,
    /// Skip list + hash table (sorted set full encoding).
    SkipList = 10,
    /// Radix tree (stream type).
    StreamTree = 12,
}

// ---------------------------------------------------------------------------
// ValueData / StoredValue
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
pub enum ValueData {
    String(Bytes),
    Hash(HashMap<Bytes, HashFieldEntry>),
    List(VecDeque<Bytes>),
    Set(HashSet<Bytes>),
    SortedSet(SortedSet),
    Stream {
        entries: Vec<StreamEntry>,
        groups: HashMap<Bytes, StreamGroup>,
    },
}

#[derive(Debug, Clone)]
pub struct StoredValue {
    pub data: ValueData,
    pub expire_at_ms: Option<i64>,
    /// Internal encoding tag for RDB serialization and future compact encodings.
    pub encoding: Encoding,
    /// 24-bit LRU clock (seconds, wrapping) or LFU counter, used by eviction.
    pub lru_clock: u32,
}

impl StoredValue {
    pub fn string(value: Bytes, expire_at_ms: Option<i64>) -> Self {
        Self {
            data: ValueData::String(value),
            expire_at_ms,
            encoding: Encoding::Raw,
            lru_clock: 0,
        }
    }

    pub fn hash(fields: HashMap<Bytes, HashFieldEntry>, expire_at_ms: Option<i64>) -> Self {
        Self {
            data: ValueData::Hash(fields),
            expire_at_ms,
            encoding: Encoding::HashTable,
            lru_clock: 0,
        }
    }

    pub fn sorted_set(zset: SortedSet, expire_at_ms: Option<i64>) -> Self {
        Self {
            data: ValueData::SortedSet(zset),
            expire_at_ms,
            encoding: Encoding::SkipList,
            lru_clock: 0,
        }
    }

    pub fn list(values: VecDeque<Bytes>, expire_at_ms: Option<i64>) -> Self {
        Self {
            data: ValueData::List(values),
            expire_at_ms,
            encoding: Encoding::QuickList,
            lru_clock: 0,
        }
    }

    pub fn set(values: HashSet<Bytes>, expire_at_ms: Option<i64>) -> Self {
        Self {
            data: ValueData::Set(values),
            expire_at_ms,
            encoding: Encoding::HashTable,
            lru_clock: 0,
        }
    }

    pub fn stream(entries: Vec<StreamEntry>, expire_at_ms: Option<i64>) -> Self {
        Self {
            data: ValueData::Stream {
                entries,
                groups: HashMap::new(),
            },
            expire_at_ms,
            encoding: Encoding::StreamTree,
            lru_clock: 0,
        }
    }

    pub fn is_string(&self) -> bool {
        matches!(self.data, ValueData::String(_))
    }

    pub fn is_hash(&self) -> bool {
        matches!(self.data, ValueData::Hash(_))
    }

    pub fn is_list(&self) -> bool {
        matches!(self.data, ValueData::List(_))
    }

    pub fn is_set(&self) -> bool {
        matches!(self.data, ValueData::Set(_))
    }

    pub fn is_stream(&self) -> bool {
        matches!(self.data, ValueData::Stream { .. })
    }

    pub fn is_sorted_set(&self) -> bool {
        matches!(self.data, ValueData::SortedSet(_))
    }

    pub fn type_name(&self) -> &'static str {
        match &self.data {
            ValueData::String(_) => "string",
            ValueData::Hash(_) => "hash",
            ValueData::List(_) => "list",
            ValueData::Set(_) => "set",
            ValueData::SortedSet(_) => "zset",
            ValueData::Stream { .. } => "stream",
        }
    }

    pub fn as_string(&self) -> Option<&Bytes> {
        match &self.data {
            ValueData::String(v) => Some(v),
            _ => None,
        }
    }

    pub fn as_hash(&self) -> Option<&HashMap<Bytes, HashFieldEntry>> {
        match &self.data {
            ValueData::Hash(h) => Some(h),
            _ => None,
        }
    }

    pub fn as_hash_mut(&mut self) -> Option<&mut HashMap<Bytes, HashFieldEntry>> {
        match &mut self.data {
            ValueData::Hash(h) => Some(h),
            _ => None,
        }
    }

    pub fn as_sorted_set(&self) -> Option<&SortedSet> {
        match &self.data {
            ValueData::SortedSet(z) => Some(z),
            _ => None,
        }
    }

    pub fn as_sorted_set_mut(&mut self) -> Option<&mut SortedSet> {
        match &mut self.data {
            ValueData::SortedSet(z) => Some(z),
            _ => None,
        }
    }

    pub fn as_list(&self) -> Option<&VecDeque<Bytes>> {
        match &self.data {
            ValueData::List(l) => Some(l),
            _ => None,
        }
    }

    pub fn as_list_mut(&mut self) -> Option<&mut VecDeque<Bytes>> {
        match &mut self.data {
            ValueData::List(l) => Some(l),
            _ => None,
        }
    }

    pub fn as_set(&self) -> Option<&HashSet<Bytes>> {
        match &self.data {
            ValueData::Set(s) => Some(s),
            _ => None,
        }
    }

    pub fn as_set_mut(&mut self) -> Option<&mut HashSet<Bytes>> {
        match &mut self.data {
            ValueData::Set(s) => Some(s),
            _ => None,
        }
    }

    pub fn as_stream(&self) -> Option<(&Vec<StreamEntry>, &HashMap<Bytes, StreamGroup>)> {
        match &self.data {
            ValueData::Stream { entries, groups } => Some((entries, groups)),
            _ => None,
        }
    }

    pub fn as_stream_mut(
        &mut self,
    ) -> Option<(&mut Vec<StreamEntry>, &mut HashMap<Bytes, StreamGroup>)> {
        match &mut self.data {
            ValueData::Stream { entries, groups } => Some((entries, groups)),
            _ => None,
        }
    }

    pub fn as_stream_entries(&self) -> Option<&Vec<StreamEntry>> {
        match &self.data {
            ValueData::Stream { entries, .. } => Some(entries),
            _ => None,
        }
    }

    pub fn as_stream_entries_mut(&mut self) -> Option<&mut Vec<StreamEntry>> {
        match &mut self.data {
            ValueData::Stream { entries, .. } => Some(entries),
            _ => None,
        }
    }

    pub fn as_stream_groups(&self) -> Option<&HashMap<Bytes, StreamGroup>> {
        match &self.data {
            ValueData::Stream { groups, .. } => Some(groups),
            _ => None,
        }
    }

    pub fn as_stream_groups_mut(&mut self) -> Option<&mut HashMap<Bytes, StreamGroup>> {
        match &mut self.data {
            ValueData::Stream { groups, .. } => Some(groups),
            _ => None,
        }
    }
}

// ---------------------------------------------------------------------------
// SlowlogEntry
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
pub struct SlowlogEntry {
    pub id: i64,
    pub unix_time: i64,
    pub duration_us: i64,
    pub argv: Vec<Bytes>,
}

// ---------------------------------------------------------------------------
// AclUser
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
pub struct AclUser {
    pub enabled: bool,
    pub nopass: bool,
    pub passwords: HashSet<Bytes>,
    pub allow_all_commands: bool,
    pub allowed_categories: HashSet<Bytes>,
}

impl AclUser {
    pub fn default_user() -> Self {
        Self {
            enabled: true,
            nopass: true,
            passwords: HashSet::new(),
            allow_all_commands: true,
            allowed_categories: HashSet::new(),
        }
    }

    pub fn new_disabled() -> Self {
        Self {
            enabled: false,
            nopass: false,
            passwords: HashSet::new(),
            allow_all_commands: false,
            allowed_categories: HashSet::new(),
        }
    }

    pub fn category_allowed(&self, category: &[u8]) -> bool {
        self.allow_all_commands || self.allowed_categories.contains(category)
    }
}

// ---------------------------------------------------------------------------
// PubSubMessage
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
pub enum PubSubMessage {
    Message {
        channel: Bytes,
        payload: Bytes,
    },
    SMessage {
        channel: Bytes,
        payload: Bytes,
    },
    PMessage {
        pattern: Bytes,
        channel: Bytes,
        payload: Bytes,
    },
}

// ---------------------------------------------------------------------------
// PubSubState — channel/pattern subscriptions + pending delivery
// ---------------------------------------------------------------------------

const PUBSUB_PENDING_QUEUE_LIMIT: usize = 4096;

#[derive(Debug, Default)]
pub struct PubSubState {
    channels: HashMap<Bytes, HashSet<i64>>,
    shard_channels: HashMap<Bytes, HashSet<i64>>,
    patterns: HashMap<Bytes, HashSet<i64>>,
    client_channel_subs: HashMap<i64, HashSet<Bytes>>,
    client_shard_channel_subs: HashMap<i64, HashSet<Bytes>>,
    client_pattern_subs: HashMap<i64, HashSet<Bytes>>,
    pending: HashMap<i64, Vec<PubSubMessage>>,
    overflowed_clients: HashSet<i64>,
}

impl PubSubState {
    pub fn subscribe_channel(&mut self, client_id: i64, channel: Bytes) -> i64 {
        self.channels
            .entry(channel.clone())
            .or_default()
            .insert(client_id);
        self.client_channel_subs
            .entry(client_id)
            .or_default()
            .insert(channel);
        self.client_total_subscriptions(client_id) as i64
    }

    pub fn subscribe_shard_channel(&mut self, client_id: i64, channel: Bytes) -> i64 {
        self.shard_channels
            .entry(channel.clone())
            .or_default()
            .insert(client_id);
        self.client_shard_channel_subs
            .entry(client_id)
            .or_default()
            .insert(channel);
        self.client_total_subscriptions(client_id) as i64
    }

    pub fn subscribe_pattern(&mut self, client_id: i64, pattern: Bytes) -> i64 {
        self.patterns
            .entry(pattern.clone())
            .or_default()
            .insert(client_id);
        self.client_pattern_subs
            .entry(client_id)
            .or_default()
            .insert(pattern);
        self.client_total_subscriptions(client_id) as i64
    }

    pub fn unsubscribe_channel(&mut self, client_id: i64, channel: &Bytes) -> i64 {
        let drop_client_channels =
            if let Some(channels) = self.client_channel_subs.get_mut(&client_id) {
                channels.remove(channel);
                channels.is_empty()
            } else {
                false
            };
        if drop_client_channels {
            self.client_channel_subs.remove(&client_id);
        }

        let drop_channel = if let Some(subscribers) = self.channels.get_mut(channel) {
            subscribers.remove(&client_id);
            subscribers.is_empty()
        } else {
            false
        };
        if drop_channel {
            self.channels.remove(channel);
        }

        self.client_total_subscriptions(client_id) as i64
    }

    pub fn unsubscribe_shard_channel(&mut self, client_id: i64, channel: &Bytes) -> i64 {
        let drop_client_channels =
            if let Some(channels) = self.client_shard_channel_subs.get_mut(&client_id) {
                channels.remove(channel);
                channels.is_empty()
            } else {
                false
            };
        if drop_client_channels {
            self.client_shard_channel_subs.remove(&client_id);
        }

        let drop_channel = if let Some(subscribers) = self.shard_channels.get_mut(channel) {
            subscribers.remove(&client_id);
            subscribers.is_empty()
        } else {
            false
        };
        if drop_channel {
            self.shard_channels.remove(channel);
        }

        self.client_total_subscriptions(client_id) as i64
    }

    pub fn unsubscribe_pattern(&mut self, client_id: i64, pattern: &Bytes) -> i64 {
        let drop_client_patterns =
            if let Some(patterns) = self.client_pattern_subs.get_mut(&client_id) {
                patterns.remove(pattern);
                patterns.is_empty()
            } else {
                false
            };
        if drop_client_patterns {
            self.client_pattern_subs.remove(&client_id);
        }

        let drop_pattern = if let Some(subscribers) = self.patterns.get_mut(pattern) {
            subscribers.remove(&client_id);
            subscribers.is_empty()
        } else {
            false
        };
        if drop_pattern {
            self.patterns.remove(pattern);
        }

        self.client_total_subscriptions(client_id) as i64
    }

    pub fn client_channels(&self, client_id: i64) -> Vec<Bytes> {
        let mut out = self
            .client_channel_subs
            .get(&client_id)
            .map(|set| set.iter().cloned().collect::<Vec<_>>())
            .unwrap_or_default();
        out.sort();
        out
    }

    pub fn client_shard_channels(&self, client_id: i64) -> Vec<Bytes> {
        let mut out = self
            .client_shard_channel_subs
            .get(&client_id)
            .map(|set| set.iter().cloned().collect::<Vec<_>>())
            .unwrap_or_default();
        out.sort();
        out
    }

    pub fn client_patterns(&self, client_id: i64) -> Vec<Bytes> {
        let mut out = self
            .client_pattern_subs
            .get(&client_id)
            .map(|set| set.iter().cloned().collect::<Vec<_>>())
            .unwrap_or_default();
        out.sort();
        out
    }

    pub fn channels_matching(&self, pattern: Option<&str>) -> Vec<Bytes> {
        let mut out = self
            .channels
            .keys()
            .filter(|channel| {
                pattern.is_none_or(|pat| {
                    glob_match::glob_match(pat, &String::from_utf8_lossy(channel))
                })
            })
            .cloned()
            .collect::<Vec<_>>();
        out.sort();
        out
    }

    pub fn shard_channels_matching(&self, pattern: Option<&str>) -> Vec<Bytes> {
        let mut out = self
            .shard_channels
            .keys()
            .filter(|channel| {
                pattern.is_none_or(|pat| {
                    glob_match::glob_match(pat, &String::from_utf8_lossy(channel))
                })
            })
            .cloned()
            .collect::<Vec<_>>();
        out.sort();
        out
    }

    pub fn numsub(&self, channels: &[Bytes]) -> Vec<(Bytes, i64)> {
        channels
            .iter()
            .map(|channel| {
                let count = self
                    .channels
                    .get(channel)
                    .map_or(0i64, |set| set.len() as i64);
                (channel.clone(), count)
            })
            .collect()
    }

    pub fn shard_numsub(&self, channels: &[Bytes]) -> Vec<(Bytes, i64)> {
        channels
            .iter()
            .map(|channel| {
                let count = self
                    .shard_channels
                    .get(channel)
                    .map_or(0i64, |set| set.len() as i64);
                (channel.clone(), count)
            })
            .collect()
    }

    pub fn numpat(&self) -> i64 {
        self.patterns.len() as i64
    }

    fn enqueue_pending(&mut self, client_id: i64, message: PubSubMessage) -> bool {
        if self.overflowed_clients.contains(&client_id) {
            metrics::counter!("ratatosk_pubsub_messages_dropped_total", "reason" => "client_overflowed")
                .increment(1);
            return false;
        }

        let queue = self.pending.entry(client_id).or_default();
        if queue.len() >= PUBSUB_PENDING_QUEUE_LIMIT {
            self.overflowed_clients.insert(client_id);
            metrics::counter!("ratatosk_pubsub_clients_overflowed_total").increment(1);
            metrics::counter!("ratatosk_pubsub_messages_dropped_total", "reason" => "queue_full")
                .increment(1);
            tracing::warn!(
                target = "ratatosk::pubsub",
                client_id,
                queue_size = queue.len(),
                "PubSub client overflowed - queue limit exceeded"
            );
            return false;
        }

        queue.push(message);
        
        // Update queue size gauge for this client
        metrics::gauge!("ratatosk_pubsub_pending_queue_size", "client_id" => client_id.to_string())
            .set(queue.len() as f64);
        
        true
    }

    pub fn take_overflowed_client(&mut self, client_id: i64) -> bool {
        self.overflowed_clients.remove(&client_id)
    }

    pub fn pending_queue_limit(&self) -> usize {
        PUBSUB_PENDING_QUEUE_LIMIT
    }

    pub fn pending_len_for_client(&self, client_id: i64) -> usize {
        self.pending.get(&client_id).map_or(0, Vec::len)
    }

    pub fn client_has_subscriptions(&self, client_id: i64) -> bool {
        self.client_total_subscriptions(client_id) > 0
    }

    pub fn publish(&mut self, channel: &Bytes, payload: &Bytes) -> i64 {
        let mut receivers = 0i64;

        let direct_subscribers = self
            .channels
            .get(channel)
            .map(|set| set.iter().copied().collect::<Vec<_>>())
            .unwrap_or_default();

        for client_id in direct_subscribers {
            if self.enqueue_pending(
                client_id,
                PubSubMessage::Message {
                    channel: channel.clone(),
                    payload: payload.clone(),
                },
            ) {
                receivers += 1;
            }
        }

        let channel_text = String::from_utf8_lossy(channel);
        let patterns = self
            .patterns
            .iter()
            .filter(|(pattern, _)| {
                glob_match::glob_match(&String::from_utf8_lossy(pattern), &channel_text)
            })
            .map(|(pattern, subscribers)| {
                (
                    pattern.clone(),
                    subscribers.iter().copied().collect::<Vec<_>>(),
                )
            })
            .collect::<Vec<_>>();

        for (pattern, subscribers) in patterns {
            for client_id in subscribers {
                if self.enqueue_pending(
                    client_id,
                    PubSubMessage::PMessage {
                        pattern: pattern.clone(),
                        channel: channel.clone(),
                        payload: payload.clone(),
                    },
                ) {
                    receivers += 1;
                }
            }
        }

        receivers
    }

    pub fn publish_shard(&mut self, channel: &Bytes, payload: &Bytes) -> i64 {
        let mut receivers = 0i64;

        let direct_subscribers = self
            .shard_channels
            .get(channel)
            .map(|set| set.iter().copied().collect::<Vec<_>>())
            .unwrap_or_default();

        for client_id in direct_subscribers {
            if self.enqueue_pending(
                client_id,
                PubSubMessage::SMessage {
                    channel: channel.clone(),
                    payload: payload.clone(),
                },
            ) {
                receivers += 1;
            }
        }

        receivers
    }

    pub fn drain_messages(&mut self, client_id: i64) -> Vec<PubSubMessage> {
        self.pending.remove(&client_id).unwrap_or_default()
    }

    pub fn remove_client(&mut self, client_id: i64) {
        if let Some(channels) = self.client_channel_subs.remove(&client_id) {
            for channel in channels {
                if let Some(subscribers) = self.channels.get_mut(&channel) {
                    subscribers.remove(&client_id);
                    if subscribers.is_empty() {
                        self.channels.remove(&channel);
                    }
                }
            }
        }

        if let Some(channels) = self.client_shard_channel_subs.remove(&client_id) {
            for channel in channels {
                if let Some(subscribers) = self.shard_channels.get_mut(&channel) {
                    subscribers.remove(&client_id);
                    if subscribers.is_empty() {
                        self.shard_channels.remove(&channel);
                    }
                }
            }
        }

        if let Some(patterns) = self.client_pattern_subs.remove(&client_id) {
            for pattern in patterns {
                if let Some(subscribers) = self.patterns.get_mut(&pattern) {
                    subscribers.remove(&client_id);
                    if subscribers.is_empty() {
                        self.patterns.remove(&pattern);
                    }
                }
            }
        }

        self.pending.remove(&client_id);
        self.overflowed_clients.remove(&client_id);
    }

    fn client_total_subscriptions(&self, client_id: i64) -> usize {
        self.client_channel_subs
            .get(&client_id)
            .map_or(0usize, HashSet::len)
            .saturating_add(
                self.client_shard_channel_subs
                    .get(&client_id)
                    .map_or(0usize, HashSet::len),
            )
            .saturating_add(
                self.client_pattern_subs
                    .get(&client_id)
                    .map_or(0usize, HashSet::len),
            )
    }

    #[cfg(test)]
    pub fn assert_invariants(&self) {
        // For each client in client_channel_subs, verify they appear in channels
        for (client_id, client_channels) in &self.client_channel_subs {
            for channel in client_channels {
                assert!(
                    self.channels
                        .get(channel)
                        .is_some_and(|subs| subs.contains(client_id)),
                    "client {client_id} subscribed to channel {:?} but not in channels map",
                    channel
                );
            }
        }
        // For each client in channels, verify they appear in client_channel_subs
        for (channel, subscribers) in &self.channels {
            for client_id in subscribers {
                assert!(
                    self.client_channel_subs
                        .get(client_id)
                        .is_some_and(|chs| chs.contains(channel)),
                    "channel {:?} has subscriber {client_id} but not in client_channel_subs",
                    channel
                );
            }
        }

        // Same for shard_channels
        for (client_id, client_channels) in &self.client_shard_channel_subs {
            for channel in client_channels {
                assert!(
                    self.shard_channels
                        .get(channel)
                        .is_some_and(|subs| subs.contains(client_id)),
                    "client {client_id} subscribed to shard channel {:?} but not in shard_channels map",
                    channel
                );
            }
        }
        for (channel, subscribers) in &self.shard_channels {
            for client_id in subscribers {
                assert!(
                    self.client_shard_channel_subs
                        .get(client_id)
                        .is_some_and(|chs| chs.contains(channel)),
                    "shard channel {:?} has subscriber {client_id} but not in client_shard_channel_subs",
                    channel
                );
            }
        }

        // Same for patterns
        for (client_id, client_patterns) in &self.client_pattern_subs {
            for pattern in client_patterns {
                assert!(
                    self.patterns
                        .get(pattern)
                        .is_some_and(|subs| subs.contains(client_id)),
                    "client {client_id} subscribed to pattern {:?} but not in patterns map",
                    pattern
                );
            }
        }
        for (pattern, subscribers) in &self.patterns {
            for client_id in subscribers {
                assert!(
                    self.client_pattern_subs
                        .get(client_id)
                        .is_some_and(|pats| pats.contains(pattern)),
                    "pattern {:?} has subscriber {client_id} but not in client_pattern_subs",
                    pattern
                );
            }
        }
    }
}

// ---------------------------------------------------------------------------
// StatsState — slowlog, latency tracking, command counters
// ---------------------------------------------------------------------------

#[derive(Debug)]
pub struct StatsState {
    total_commands_processed: u64,
    last_save_unix_sec: i64,
    slowlog_log_slower_than_us: i64,
    slowlog_max_len: usize,
    slowlog_entries: VecDeque<SlowlogEntry>,
    next_slowlog_id: i64,
    latency_events: HashMap<Vec<u8>, VecDeque<(i64, i64)>>,
    connected_clients: u64,
    total_net_input_bytes: u64,
    total_net_output_bytes: u64,
    evicted_keys: u64,
    expired_keys: u64,
    keyspace_hits: u64,
    keyspace_misses: u64,
    /// Snapshot of `total_commands_processed` at the previous ops/sec sample.
    prev_commands_snapshot: u64,
    /// Computed instantaneous operations per second.
    instantaneous_ops_per_sec: u64,
    /// Cached memory estimate (bytes).
    cached_memory_estimate: u64,
    /// Cron tick when memory estimate was last computed.
    last_memory_estimate_tick: u64,
}

impl Default for StatsState {
    fn default() -> Self {
        Self {
            total_commands_processed: 0,
            last_save_unix_sec: unix_sec_now(),
            slowlog_log_slower_than_us: 10_000,
            slowlog_max_len: 128,
            slowlog_entries: VecDeque::new(),
            next_slowlog_id: 0,
            latency_events: HashMap::new(),
            connected_clients: 0,
            total_net_input_bytes: 0,
            total_net_output_bytes: 0,
            evicted_keys: 0,
            expired_keys: 0,
            keyspace_hits: 0,
            keyspace_misses: 0,
            prev_commands_snapshot: 0,
            instantaneous_ops_per_sec: 0,
            cached_memory_estimate: 0,
            last_memory_estimate_tick: 0,
        }
    }
}

impl StatsState {
    pub fn mark_command_processed(&mut self) {
        self.total_commands_processed = self.total_commands_processed.saturating_add(1);
    }

    pub fn total_commands_processed(&self) -> u64 {
        self.total_commands_processed
    }

    pub fn adjust_commands_processed_by(&mut self, overcounted: u64) {
        self.total_commands_processed = self.total_commands_processed.saturating_sub(overcounted);
    }

    pub fn reset(&mut self) {
        self.total_commands_processed = 0;
        self.connected_clients = 0;
        self.total_net_input_bytes = 0;
        self.total_net_output_bytes = 0;
        self.evicted_keys = 0;
        self.expired_keys = 0;
        self.keyspace_hits = 0;
        self.keyspace_misses = 0;
        self.prev_commands_snapshot = 0;
        self.instantaneous_ops_per_sec = 0;
    }

    pub fn connected_clients(&self) -> u64 {
        self.connected_clients
    }

    pub fn mark_client_connected(&mut self) {
        self.connected_clients = self.connected_clients.saturating_add(1);
    }

    pub fn mark_client_disconnected(&mut self) {
        self.connected_clients = self.connected_clients.saturating_sub(1);
    }

    pub fn total_net_input_bytes(&self) -> u64 {
        self.total_net_input_bytes
    }

    pub fn add_net_input_bytes(&mut self, bytes: u64) {
        self.total_net_input_bytes = self.total_net_input_bytes.saturating_add(bytes);
    }

    pub fn total_net_output_bytes(&self) -> u64 {
        self.total_net_output_bytes
    }

    pub fn add_net_output_bytes(&mut self, bytes: u64) {
        self.total_net_output_bytes = self.total_net_output_bytes.saturating_add(bytes);
    }

    pub fn evicted_keys(&self) -> u64 {
        self.evicted_keys
    }

    pub fn add_evicted_keys(&mut self, count: u64) {
        self.evicted_keys = self.evicted_keys.saturating_add(count);
    }

    pub fn expired_keys(&self) -> u64 {
        self.expired_keys
    }

    pub fn add_expired_keys(&mut self, count: u64) {
        self.expired_keys = self.expired_keys.saturating_add(count);
    }

    pub fn keyspace_hits(&self) -> u64 {
        self.keyspace_hits
    }

    pub fn mark_keyspace_hit(&mut self) {
        self.keyspace_hits = self.keyspace_hits.saturating_add(1);
    }

    pub fn add_keyspace_hits(&mut self, count: u64) {
        self.keyspace_hits = self.keyspace_hits.saturating_add(count);
    }

    pub fn keyspace_misses(&self) -> u64 {
        self.keyspace_misses
    }

    pub fn mark_keyspace_miss(&mut self) {
        self.keyspace_misses = self.keyspace_misses.saturating_add(1);
    }

    pub fn add_keyspace_misses(&mut self, count: u64) {
        self.keyspace_misses = self.keyspace_misses.saturating_add(count);
    }

    pub fn instantaneous_ops_per_sec(&self) -> u64 {
        self.instantaneous_ops_per_sec
    }

    pub fn sample_ops_per_sec(&mut self, interval_secs: u64) {
        let current = self.total_commands_processed;
        let delta = current.saturating_sub(self.prev_commands_snapshot);
        self.instantaneous_ops_per_sec = if interval_secs > 0 {
            delta / interval_secs
        } else {
            delta
        };
        self.prev_commands_snapshot = current;
    }

    pub fn last_save_unix_sec(&self) -> i64 {
        self.last_save_unix_sec
    }

    pub fn mark_last_save_now(&mut self) {
        self.last_save_unix_sec = unix_sec_now();
    }

    pub fn slowlog_log_slower_than_us(&self) -> i64 {
        self.slowlog_log_slower_than_us
    }

    pub fn set_slowlog_log_slower_than_us(&mut self, value: i64) {
        self.slowlog_log_slower_than_us = value;
    }

    pub fn slowlog_max_len(&self) -> usize {
        self.slowlog_max_len
    }

    pub fn set_slowlog_max_len(&mut self, value: usize) {
        self.slowlog_max_len = value;
        while self.slowlog_entries.len() > self.slowlog_max_len {
            self.slowlog_entries.pop_back();
        }
    }

    pub fn slowlog_len(&self) -> usize {
        self.slowlog_entries.len()
    }

    pub fn slowlog_entries(&self) -> &VecDeque<SlowlogEntry> {
        &self.slowlog_entries
    }

    pub fn slowlog_reset(&mut self) {
        self.slowlog_entries.clear();
    }

    pub fn append_slowlog(&mut self, duration_us: i64, argv: &[Bytes]) {
        let threshold = self.slowlog_log_slower_than_us;
        if threshold < 0 {
            return;
        }
        if duration_us < threshold {
            return;
        }
        if self.slowlog_max_len == 0 {
            return;
        }

        let entry = SlowlogEntry {
            id: self.next_slowlog_id,
            unix_time: unix_sec_now(),
            duration_us,
            argv: sanitize_slowlog_argv(argv),
        };
        self.next_slowlog_id = self.next_slowlog_id.wrapping_add(1);
        self.slowlog_entries.push_front(entry);
        while self.slowlog_entries.len() > self.slowlog_max_len {
            self.slowlog_entries.pop_back();
        }
    }

    pub fn record_latency_sample(&mut self, event: &[u8], latency_ms: i64) {
        let history = self
            .latency_events
            .raw_entry_mut()
            .from_key(event)
            .or_insert_with(|| (event.to_ascii_lowercase(), VecDeque::new()))
            .1;
        history.push_back((unix_sec_now(), latency_ms.max(0)));
        while history.len() > 160 {
            history.pop_front();
        }
    }

    pub fn latency_latest(&self) -> Vec<(Bytes, i64, i64, i64)> {
        let mut out = Vec::new();
        for (event, samples) in &self.latency_events {
            let Some((latest_ts, latest_ms)) = samples.back().copied() else {
                continue;
            };
            let max_ms = samples.iter().map(|(_, ms)| *ms).max().unwrap_or(0);
            out.push((Bytes::from(event.clone()), latest_ts, latest_ms, max_ms));
        }
        out.sort_by(|a, b| a.0.cmp(&b.0));
        out
    }

    pub fn latency_history(&self, event: &Bytes) -> Vec<(i64, i64)> {
        self.latency_events
            .get(event.as_ref())
            .map(|samples| samples.iter().copied().collect())
            .unwrap_or_default()
    }

    pub fn latency_reset(&mut self, events: &[Bytes]) -> i64 {
        if events.is_empty() {
            let removed = self.latency_events.len() as i64;
            self.latency_events.clear();
            return removed;
        }

        let mut removed = 0i64;
        for event in events {
            if self.latency_events.remove(event.as_ref()).is_some() {
                removed += 1;
            }
        }
        removed
    }

    pub fn latency_event_names(&self) -> Vec<Bytes> {
        let mut names = self
            .latency_events
            .keys()
            .map(|k| Bytes::from(k.clone()))
            .collect::<Vec<_>>();
        names.sort();
        names
    }

    pub fn cached_memory_estimate(&self) -> u64 {
        self.cached_memory_estimate
    }

    pub fn set_cached_memory_estimate(&mut self, estimate: u64, tick: u64) {
        self.cached_memory_estimate = estimate;
        self.last_memory_estimate_tick = tick;
    }

    pub fn last_memory_estimate_tick(&self) -> u64 {
        self.last_memory_estimate_tick
    }
}

// ---------------------------------------------------------------------------
// AclState — user accounts + audit log
// ---------------------------------------------------------------------------

#[derive(Debug)]
pub struct AclState {
    users: HashMap<Bytes, AclUser>,
    log: VecDeque<Bytes>,
}

impl Default for AclState {
    fn default() -> Self {
        let mut users = HashMap::new();
        users.insert(Bytes::from_static(b"default"), AclUser::default_user());
        Self {
            users,
            log: VecDeque::new(),
        }
    }
}

impl AclState {
    pub fn user_names(&self) -> Vec<Bytes> {
        let mut names = self.users.keys().cloned().collect::<Vec<_>>();
        names.sort();
        names
    }

    pub fn get_user(&self, username: &Bytes) -> Option<&AclUser> {
        self.users.get(username)
    }

    pub fn get_or_create_user_mut(&mut self, username: &Bytes) -> &mut AclUser {
        self.users
            .entry(username.clone())
            .or_insert_with(AclUser::new_disabled)
    }

    pub fn del_users(&mut self, usernames: &[Bytes]) -> i64 {
        let mut removed = 0i64;
        for username in usernames {
            if username.as_ref() == b"default" {
                continue;
            }
            if self.users.remove(username).is_some() {
                removed += 1;
            }
        }
        removed
    }

    pub fn authenticate_user(&self, username: &Bytes, password: &Bytes) -> bool {
        let Some(user) = self.users.get(username) else {
            return false;
        };
        if !user.enabled {
            return false;
        }
        if user.nopass {
            return true;
        }
        if user.passwords.is_empty() {
            return false;
        }

        for stored in &user.passwords {
            if verify_password_hash(stored, password) {
                return true;
            }
        }

        false
    }

    pub fn default_user_is_nopass_enabled(&self) -> bool {
        self.users
            .get(b"default" as &[u8])
            .is_some_and(|user| user.enabled && user.nopass)
    }

    pub fn command_allowed(&self, username: &Bytes, required_categories: &[&[u8]]) -> bool {
        let Some(user) = self.users.get(username) else {
            return false;
        };
        if !user.enabled {
            return false;
        }
        if user.allow_all_commands {
            return true;
        }

        required_categories
            .iter()
            .all(|category| user.category_allowed(category))
    }

    pub fn command_allowed_mask(&self, username: &Bytes, required_mask: u8) -> bool {
        let Some(user) = self.users.get(username) else {
            return false;
        };
        if !user.enabled {
            return false;
        }
        if user.allow_all_commands || required_mask == 0 {
            return true;
        }

        (required_mask & (1 << 0) == 0 || user.category_allowed(b"admin"))
            && (required_mask & (1 << 1) == 0 || user.category_allowed(b"write"))
            && (required_mask & (1 << 2) == 0 || user.category_allowed(b"read"))
            && (required_mask & (1 << 3) == 0 || user.category_allowed(b"pubsub"))
            && (required_mask & (1 << 4) == 0 || user.category_allowed(b"connection"))
            && (required_mask & (1 << 5) == 0 || user.category_allowed(b"fast"))
    }

    pub fn hash_password(raw_password: &[u8]) -> Option<Bytes> {
        let salt = SaltString::generate(&mut OsRng);
        let hash = Argon2::default()
            .hash_password(raw_password, &salt)
            .ok()?
            .to_string();
        Some(Bytes::from(hash))
    }

    pub fn remove_password(&mut self, username: &Bytes, raw_password: &[u8]) -> bool {
        let Some(user) = self.users.get_mut(username) else {
            return false;
        };

        let mut removed = false;
        let current = user.passwords.iter().cloned().collect::<Vec<_>>();
        for stored in current {
            if verify_password_hash(&stored, raw_password) && user.passwords.remove(&stored) {
                removed = true;
            }
        }

        removed
    }

    pub fn push_log(&mut self, line: Bytes) {
        let sanitized = sanitize_acl_log_line(&String::from_utf8_lossy(&line));
        self.log.push_front(Bytes::from(sanitized));
        while self.log.len() > 128 {
            self.log.pop_back();
        }
    }

    pub fn log(&self, count: usize) -> Vec<Bytes> {
        self.log.iter().take(count).cloned().collect()
    }

    pub fn log_reset(&mut self) {
        self.log.clear();
    }
}

fn verify_password_hash(stored: &Bytes, candidate: &[u8]) -> bool {
    if stored == candidate {
        // Temporary compatibility for legacy in-memory plaintext entries.
        return true;
    }

    let Ok(hash_str) = std::str::from_utf8(stored) else {
        return false;
    };
    let Ok(parsed) = PasswordHash::new(hash_str) else {
        return false;
    };

    Argon2::default()
        .verify_password(candidate, &parsed)
        .is_ok()
}

// ---------------------------------------------------------------------------
// ConfigState — runtime-adjustable configuration
// ---------------------------------------------------------------------------

#[derive(Debug)]
pub struct ConfigState {
    timeout: i64,
    appendonly: bool,
    save: Bytes,
    dir: PathBuf,
    dbfilename: String,
    appendfsync: Bytes,
    maxmemory: usize,
    maxmemory_policy: Bytes,
    maxmemory_samples: usize,
    hz: u32,
    notify_keyspace_events: Bytes,
    lazyfree_lazy_expire: bool,
    lazyfree_lazy_server_del: bool,
    lazyfree_lazy_user_del: bool,
    tcp_keepalive: u32,
}

impl Default for ConfigState {
    fn default() -> Self {
        Self {
            timeout: 0,
            appendonly: false,
            save: Bytes::from_static(b"3600 1 300 100 60 10000"),
            dir: PathBuf::from("."),
            dbfilename: "dump.rdb".to_string(),
            appendfsync: Bytes::from_static(b"everysec"),
            maxmemory: 0,
            maxmemory_policy: Bytes::from_static(b"noeviction"),
            maxmemory_samples: 5,
            hz: 10,
            notify_keyspace_events: Bytes::new(),
            lazyfree_lazy_expire: false,
            lazyfree_lazy_server_del: false,
            lazyfree_lazy_user_del: false,
            tcp_keepalive: 300,
        }
    }
}

impl ConfigState {
    pub fn timeout(&self) -> i64 {
        self.timeout
    }

    pub fn set_timeout(&mut self, value: i64) {
        self.timeout = value;
    }

    pub fn appendonly(&self) -> bool {
        self.appendonly
    }

    pub fn set_appendonly(&mut self, value: bool) {
        self.appendonly = value;
    }

    pub fn save(&self) -> &Bytes {
        &self.save
    }

    pub fn set_save(&mut self, value: Bytes) {
        self.save = value;
    }

    pub fn dir(&self) -> &PathBuf {
        &self.dir
    }

    pub fn set_dir(&mut self, value: PathBuf) {
        self.dir = value;
    }

    pub fn dbfilename(&self) -> &str {
        &self.dbfilename
    }

    pub fn set_dbfilename(&mut self, value: String) {
        self.dbfilename = value;
    }

    pub fn appendfsync(&self) -> &Bytes {
        &self.appendfsync
    }

    pub fn set_appendfsync(&mut self, value: Bytes) {
        self.appendfsync = value;
    }

    pub fn maxmemory(&self) -> usize {
        self.maxmemory
    }

    pub fn set_maxmemory(&mut self, value: usize) {
        self.maxmemory = value;
    }

    pub fn maxmemory_policy(&self) -> &Bytes {
        &self.maxmemory_policy
    }

    pub fn set_maxmemory_policy(&mut self, value: Bytes) {
        self.maxmemory_policy = value;
    }

    pub fn maxmemory_samples(&self) -> usize {
        self.maxmemory_samples
    }

    pub fn set_maxmemory_samples(&mut self, value: usize) {
        self.maxmemory_samples = value;
    }

    pub fn hz(&self) -> u32 {
        self.hz
    }

    pub fn set_hz(&mut self, value: u32) {
        self.hz = value.clamp(1, 500);
    }

    pub fn notify_keyspace_events(&self) -> &Bytes {
        &self.notify_keyspace_events
    }

    pub fn set_notify_keyspace_events(&mut self, value: Bytes) {
        self.notify_keyspace_events = value;
    }

    pub fn lazyfree_lazy_expire(&self) -> bool {
        self.lazyfree_lazy_expire
    }

    pub fn set_lazyfree_lazy_expire(&mut self, value: bool) {
        self.lazyfree_lazy_expire = value;
    }

    pub fn lazyfree_lazy_server_del(&self) -> bool {
        self.lazyfree_lazy_server_del
    }

    pub fn set_lazyfree_lazy_server_del(&mut self, value: bool) {
        self.lazyfree_lazy_server_del = value;
    }

    pub fn lazyfree_lazy_user_del(&self) -> bool {
        self.lazyfree_lazy_user_del
    }

    pub fn set_lazyfree_lazy_user_del(&mut self, value: bool) {
        self.lazyfree_lazy_user_del = value;
    }

    pub fn tcp_keepalive(&self) -> u32 {
        self.tcp_keepalive
    }

    pub fn set_tcp_keepalive(&mut self, value: u32) {
        self.tcp_keepalive = value;
    }
}

// ---------------------------------------------------------------------------
// LazyFreeSender — channel for async background deletion
// ---------------------------------------------------------------------------

/// Sender end of the lazy-free channel.
///
/// When set, large values removed via UNLINK or FLUSHDB ASYNC are
/// sent here instead of being dropped on the main thread.
pub type LazyFreeSender = crossbeam_channel::Sender<StoredValue>;

/// Minimum collection size to qualify for lazy free.
/// Smaller values are dropped inline (cheaper than channel overhead).
pub const LAZY_FREE_THRESHOLD: usize = 64;

/// Check whether a `StoredValue` is large enough to warrant lazy free.
pub fn should_lazy_free(value: &StoredValue) -> bool {
    match &value.data {
        ValueData::String(_) => false,
        ValueData::List(l) => l.len() >= LAZY_FREE_THRESHOLD,
        ValueData::Hash(h) => h.len() >= LAZY_FREE_THRESHOLD,
        ValueData::Set(s) => s.len() >= LAZY_FREE_THRESHOLD,
        ValueData::SortedSet(z) => z.len() >= LAZY_FREE_THRESHOLD,
        ValueData::Stream { entries, .. } => entries.len() >= LAZY_FREE_THRESHOLD,
    }
}

// ---------------------------------------------------------------------------
// ServerState — top-level composition
// ---------------------------------------------------------------------------

#[derive(Debug, Default)]
pub struct ServerState {
    dbs: Vec<HashMap<Bytes, StoredValue>>,
    key_versions: Vec<HashMap<Bytes, u64>>,
    next_client_id: i64,
    next_key_version: u64,
    started_at_ms: i64,
    pub pubsub: PubSubState,
    pub stats: StatsState,
    pub acl: AclState,
    pub config: ConfigState,
    pub script_cache: ScriptCache,
    pub cluster_node_id: Bytes,
    lazy_free_tx: Option<LazyFreeSender>,
    rdb_save_in_progress: bool,
    last_rdb_save_status: Option<Result<(), String>>,
    last_rdb_save_time_ms: Option<i64>,
    aof_enabled: bool,
}

impl ServerState {
    pub fn new(db_count: usize) -> Self {
        let mut dbs = Vec::with_capacity(db_count);
        let mut key_versions = Vec::with_capacity(db_count);
        for _ in 0..db_count {
            dbs.push(HashMap::new());
            key_versions.push(HashMap::new());
        }

        let node_id = generate_cluster_node_id();
        Self {
            dbs,
            key_versions,
            next_client_id: 1,
            next_key_version: 1,
            started_at_ms: unix_ms_now(),
            pubsub: PubSubState::default(),
            stats: StatsState::default(),
            acl: AclState::default(),
            config: ConfigState::default(),
            script_cache: ScriptCache::default(),
            cluster_node_id: node_id,
            lazy_free_tx: None,
            rdb_save_in_progress: false,
            last_rdb_save_status: None,
            last_rdb_save_time_ms: None,
            aof_enabled: false,
        }
    }

    pub fn with_default_dbs() -> Self {
        Self::new(DEFAULT_DB_COUNT)
    }

    pub fn alloc_client_id(&mut self) -> i64 {
        let id = self.next_client_id;
        self.next_client_id = self.next_client_id.wrapping_add(1);
        id
    }

    pub fn total_connections_received(&self) -> u64 {
        self.next_client_id.saturating_sub(1) as u64
    }

    pub fn db_count(&self) -> usize {
        self.dbs.len()
    }

    pub fn db(&self, idx: usize) -> &HashMap<Bytes, StoredValue> {
        &self.dbs[idx]
    }

    pub fn db_mut(&mut self, idx: usize) -> &mut HashMap<Bytes, StoredValue> {
        &mut self.dbs[idx]
    }

    pub fn clear_db(&mut self, idx: usize) {
        self.dbs[idx].clear();
        self.key_versions[idx].clear();
    }

    pub fn swap_dbs(&mut self, left: usize, right: usize) {
        self.dbs.swap(left, right);
        self.key_versions.swap(left, right);
    }

    pub fn clear_all_dbs(&mut self) {
        for db in &mut self.dbs {
            db.clear();
        }
        for versions in &mut self.key_versions {
            versions.clear();
        }
    }

    pub fn snapshot_dbs(&self) -> DbSnapshot {
        self.dbs.clone()
    }

    pub fn load_from_rdb(&mut self, data: DbSnapshot) {
        self.dbs = data;
        self.key_versions = (0..self.dbs.len()).map(|_| HashMap::new()).collect();
    }

    pub fn started_at_ms(&self) -> i64 {
        self.started_at_ms
    }

    /// Returns the server uptime in seconds.
    pub fn uptime_seconds(&self) -> i64 {
        let now = ratatosk_core::time::now_ms();
        (now - self.started_at_ms) / 1000
    }

    pub fn key_version(&self, db_idx: usize, key: &Bytes) -> u64 {
        self.key_versions[db_idx].get(key).copied().unwrap_or(0)
    }

    pub fn touch_key_version(&mut self, db_idx: usize, key: &Bytes) {
        let version = self.next_key_version;
        self.next_key_version = self.next_key_version.wrapping_add(1);
        self.key_versions[db_idx].insert(key.clone(), version);
    }

    /// Set the lazy-free sender channel. Called once during server startup.
    pub fn set_lazy_free_sender(&mut self, sender: LazyFreeSender) {
        self.lazy_free_tx = Some(sender);
    }

    /// Remove a key and send its value to the lazy-free thread if large enough.
    /// Falls back to synchronous drop if no lazy-free channel is configured
    /// or if the value is small.
    pub fn lazy_free_del(&mut self, db_idx: usize, key: &Bytes) -> bool {
        let Some(value) = self.dbs[db_idx].remove(key) else {
            return false;
        };
        self.touch_key_version(db_idx, key);

        if let Some(ref tx) = self.lazy_free_tx {
            if should_lazy_free(&value) {
                // Best-effort send; if channel is full, drop synchronously
                let _ = tx.try_send(value);
                return true;
            }
        }
        // Small value or no channel — drop immediately (implicit)
        drop(value);
        true
    }

    /// Replace a DB with a new empty one, sending the old data to lazy-free.
    pub fn lazy_free_flush_db(&mut self, db_idx: usize) {
        let old_db = std::mem::take(&mut self.dbs[db_idx]);
        self.key_versions[db_idx].clear();

        if let Some(ref tx) = self.lazy_free_tx {
            if old_db.len() >= LAZY_FREE_THRESHOLD {
                // Wrap the entire HashMap in a synthetic StoredValue for transport
                for (_, value) in old_db {
                    let _ = tx.try_send(value);
                }
                return;
            }
        }
        drop(old_db);
    }

    pub fn rdb_save_in_progress(&self) -> bool {
        self.rdb_save_in_progress
    }

    pub fn set_rdb_save_in_progress(&mut self, value: bool) {
        self.rdb_save_in_progress = value;
    }

    pub fn last_rdb_save_status(&self) -> Option<&Result<(), String>> {
        self.last_rdb_save_status.as_ref()
    }

    pub fn set_last_rdb_save_status(&mut self, status: Result<(), String>) {
        self.last_rdb_save_status = Some(status);
    }

    pub fn last_rdb_save_time_ms(&self) -> Option<i64> {
        self.last_rdb_save_time_ms
    }

    pub fn set_last_rdb_save_time_ms(&mut self, value: i64) {
        self.last_rdb_save_time_ms = Some(value);
    }

    pub fn aof_enabled(&self) -> bool {
        self.aof_enabled
    }

    pub fn set_aof_enabled(&mut self, value: bool) {
        self.aof_enabled = value;
    }
}

// ---------------------------------------------------------------------------
// Free functions
// ---------------------------------------------------------------------------

pub fn purge_expired_key(db: &mut HashMap<Bytes, StoredValue>, key: &Bytes, now_ms: i64) {
    if let hashbrown::hash_map::Entry::Occupied(entry) = db.entry(key.clone()) {
        if entry.get().expire_at_ms.is_some_and(|at| at <= now_ms) {
            entry.remove();
        }
    }
}

pub fn purge_expired_keys(db: &mut HashMap<Bytes, StoredValue>, now_ms: i64) {
    db.retain(|_, value| value.expire_at_ms.is_none_or(|ts| ts > now_ms));
}


fn generate_cluster_node_id() -> Bytes {
    use rand::Rng;
    let mut rng = rand::thread_rng();
    let mut buf = [0u8; 40];
    const HEX: &[u8; 16] = b"0123456789abcdef";
    for b in &mut buf {
        *b = HEX[rng.gen_range(0..16)];
    }
    Bytes::copy_from_slice(&buf)
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use bytes::Bytes;

    use super::{PubSubState, ServerState, StatsState};

    #[test]
    fn remove_client_cleans_all_subscriptions() {
        let mut ps = PubSubState::default();

        ps.subscribe_channel(1, Bytes::from("ch1"));
        ps.subscribe_channel(1, Bytes::from("ch2"));
        ps.subscribe_shard_channel(1, Bytes::from("sh1"));
        ps.subscribe_pattern(1, Bytes::from("p*"));
        ps.subscribe_channel(2, Bytes::from("ch1"));

        ps.assert_invariants();

        // Publish so client 1 has pending messages
        ps.publish(&Bytes::from("ch1"), &Bytes::from("msg"));

        ps.remove_client(1);
        ps.assert_invariants();

        // Client 1 should have no entries anywhere
        assert!(ps.client_channels(1).is_empty());
        assert!(ps.client_shard_channels(1).is_empty());
        assert!(ps.client_patterns(1).is_empty());
        assert!(ps.drain_messages(1).is_empty());

        // ch1 should still have client 2
        assert_eq!(ps.numsub(&[Bytes::from("ch1")])[0].1, 1);
        // ch2 should be gone (no subscribers left)
        assert_eq!(ps.numsub(&[Bytes::from("ch2")])[0].1, 0);
    }

    #[test]
    fn publish_after_remove_client_returns_zero() {
        let mut ps = PubSubState::default();

        ps.subscribe_channel(1, Bytes::from("news"));
        assert_eq!(ps.publish(&Bytes::from("news"), &Bytes::from("hi")), 1);

        ps.remove_client(1);
        ps.assert_invariants();

        assert_eq!(ps.publish(&Bytes::from("news"), &Bytes::from("hi")), 0);
    }

    #[test]
    fn remove_nonexistent_client_is_noop() {
        let mut ps = PubSubState::default();
        ps.subscribe_channel(1, Bytes::from("ch1"));

        ps.remove_client(999);
        ps.assert_invariants();

        assert_eq!(ps.numsub(&[Bytes::from("ch1")])[0].1, 1);
    }

    #[test]
    fn client_has_subscriptions_tracks_all_subscription_types() {
        let mut ps = PubSubState::default();
        assert!(!ps.client_has_subscriptions(1));

        ps.subscribe_channel(1, Bytes::from("ch1"));
        assert!(ps.client_has_subscriptions(1));

        ps.unsubscribe_channel(1, &Bytes::from("ch1"));
        assert!(!ps.client_has_subscriptions(1));

        ps.subscribe_shard_channel(1, Bytes::from("sh1"));
        assert!(ps.client_has_subscriptions(1));

        ps.unsubscribe_shard_channel(1, &Bytes::from("sh1"));
        assert!(!ps.client_has_subscriptions(1));

        ps.subscribe_pattern(1, Bytes::from("p*"));
        assert!(ps.client_has_subscriptions(1));

        ps.remove_client(1);
        assert!(!ps.client_has_subscriptions(1));
    }

    #[test]
    fn pending_queue_overflow_marks_client_once_and_drops_new_messages() {
        let mut ps = PubSubState::default();
        let channel = Bytes::from("news");
        let payload = Bytes::from("msg");

        ps.subscribe_channel(1, channel.clone());
        for _ in 0..ps.pending_queue_limit() {
            assert_eq!(ps.publish(&channel, &payload), 1);
        }
        assert_eq!(ps.pending_len_for_client(1), ps.pending_queue_limit());

        // The first publish past the cap marks overflow and drops delivery.
        assert_eq!(ps.publish(&channel, &payload), 0);
        assert!(ps.take_overflowed_client(1));
        // Overflow flag is one-shot per connection loop check.
        assert!(!ps.take_overflowed_client(1));

        // Once overflowed, new deliveries are dropped until client is removed.
        assert_eq!(ps.publish(&channel, &payload), 0);

        let drained = ps.drain_messages(1);
        assert_eq!(drained.len(), ps.pending_queue_limit());

        // Removing and re-subscribing clears overflow bookkeeping.
        ps.remove_client(1);
        ps.subscribe_channel(1, channel.clone());
        assert_eq!(ps.publish(&channel, &payload), 1);
    }

    #[test]
    fn server_state_persistence_fields_default() {
        let state = ServerState::with_default_dbs();
        assert!(!state.rdb_save_in_progress());
        assert!(state.last_rdb_save_status().is_none());
        assert!(state.last_rdb_save_time_ms().is_none());
        assert!(!state.aof_enabled());
    }

    #[test]
    fn server_state_persistence_field_setters() {
        let mut state = ServerState::with_default_dbs();

        state.set_rdb_save_in_progress(true);
        assert!(state.rdb_save_in_progress());

        state.set_last_rdb_save_status(Ok(()));
        assert_eq!(state.last_rdb_save_status(), Some(&Ok(())));

        state.set_last_rdb_save_status(Err("disk full".to_string()));
        assert_eq!(
            state.last_rdb_save_status(),
            Some(&Err("disk full".to_string()))
        );

        state.set_last_rdb_save_time_ms(1_234);
        assert_eq!(state.last_rdb_save_time_ms(), Some(1_234));

        state.set_aof_enabled(true);
        assert!(state.aof_enabled());
    }

    #[test]
    fn config_state_persistence_fields_default() {
        let state = ServerState::with_default_dbs();
        assert_eq!(state.config.dir(), &PathBuf::from("."));
        assert_eq!(state.config.dbfilename(), "dump.rdb");
        assert_eq!(state.config.appendfsync(), &Bytes::from_static(b"everysec"));
        assert!(!state.config.appendonly());
    }

    #[test]
    fn config_state_persistence_field_setters() {
        let mut state = ServerState::with_default_dbs();

        state.config.set_dir(PathBuf::from("/data"));
        assert_eq!(state.config.dir(), &PathBuf::from("/data"));

        state.config.set_dbfilename("backup.rdb".to_string());
        assert_eq!(state.config.dbfilename(), "backup.rdb");

        state.config.set_appendfsync(Bytes::from_static(b"always"));
        assert_eq!(state.config.appendfsync(), &Bytes::from_static(b"always"));
    }

    #[test]
    fn stats_connected_clients_tracks_connect_disconnect() {
        let mut stats = StatsState::default();
        assert_eq!(stats.connected_clients(), 0);

        stats.mark_client_connected();
        stats.mark_client_connected();
        assert_eq!(stats.connected_clients(), 2);

        stats.mark_client_disconnected();
        assert_eq!(stats.connected_clients(), 1);

        stats.mark_client_disconnected();
        assert_eq!(stats.connected_clients(), 0);

        stats.mark_client_disconnected();
        assert_eq!(stats.connected_clients(), 0, "should not underflow");
    }

    #[test]
    fn stats_net_io_bytes_accumulate() {
        let mut stats = StatsState::default();
        assert_eq!(stats.total_net_input_bytes(), 0);
        assert_eq!(stats.total_net_output_bytes(), 0);

        stats.add_net_input_bytes(100);
        stats.add_net_input_bytes(200);
        assert_eq!(stats.total_net_input_bytes(), 300);

        stats.add_net_output_bytes(50);
        stats.add_net_output_bytes(150);
        assert_eq!(stats.total_net_output_bytes(), 200);
    }

    #[test]
    fn stats_evicted_and_expired_keys_accumulate() {
        let mut stats = StatsState::default();
        assert_eq!(stats.evicted_keys(), 0);
        assert_eq!(stats.expired_keys(), 0);

        stats.add_evicted_keys(3);
        stats.add_evicted_keys(7);
        assert_eq!(stats.evicted_keys(), 10);

        stats.add_expired_keys(5);
        assert_eq!(stats.expired_keys(), 5);
    }

    #[test]
    fn stats_keyspace_hits_and_misses() {
        let mut stats = StatsState::default();
        assert_eq!(stats.keyspace_hits(), 0);
        assert_eq!(stats.keyspace_misses(), 0);

        stats.mark_keyspace_hit();
        stats.mark_keyspace_hit();
        stats.mark_keyspace_miss();
        assert_eq!(stats.keyspace_hits(), 2);
        assert_eq!(stats.keyspace_misses(), 1);

        stats.add_keyspace_hits(10);
        stats.add_keyspace_misses(5);
        assert_eq!(stats.keyspace_hits(), 12);
        assert_eq!(stats.keyspace_misses(), 6);
    }

    #[test]
    fn stats_ops_per_sec_sampling() {
        let mut stats = StatsState::default();
        assert_eq!(stats.instantaneous_ops_per_sec(), 0);

        for _ in 0..100 {
            stats.mark_command_processed();
        }
        stats.sample_ops_per_sec(1);
        assert_eq!(stats.instantaneous_ops_per_sec(), 100);

        for _ in 0..50 {
            stats.mark_command_processed();
        }
        stats.sample_ops_per_sec(1);
        assert_eq!(stats.instantaneous_ops_per_sec(), 50);

        stats.sample_ops_per_sec(1);
        assert_eq!(stats.instantaneous_ops_per_sec(), 0, "no new commands");
    }

    #[test]
    fn stats_reset_clears_all_counters() {
        let mut stats = StatsState::default();
        stats.mark_command_processed();
        stats.mark_client_connected();
        stats.add_net_input_bytes(100);
        stats.add_net_output_bytes(200);
        stats.add_evicted_keys(3);
        stats.add_expired_keys(5);
        stats.mark_keyspace_hit();
        stats.mark_keyspace_miss();
        stats.sample_ops_per_sec(1);

        stats.reset();

        assert_eq!(stats.total_commands_processed(), 0);
        assert_eq!(stats.connected_clients(), 0);
        assert_eq!(stats.total_net_input_bytes(), 0);
        assert_eq!(stats.total_net_output_bytes(), 0);
        assert_eq!(stats.evicted_keys(), 0);
        assert_eq!(stats.expired_keys(), 0);
        assert_eq!(stats.keyspace_hits(), 0);
        assert_eq!(stats.keyspace_misses(), 0);
        assert_eq!(stats.instantaneous_ops_per_sec(), 0);
    }

    #[test]
    fn memory_estimate_cache_stores_and_retrieves() {
        let mut stats = StatsState::default();
        
        assert_eq!(stats.cached_memory_estimate(), 0);
        assert_eq!(stats.last_memory_estimate_tick(), 0);
        
        stats.set_cached_memory_estimate(12345, 42);
        
        assert_eq!(stats.cached_memory_estimate(), 12345);
        assert_eq!(stats.last_memory_estimate_tick(), 42);
        
        stats.set_cached_memory_estimate(67890, 100);
        
        assert_eq!(stats.cached_memory_estimate(), 67890);
        assert_eq!(stats.last_memory_estimate_tick(), 100);
    }
}
