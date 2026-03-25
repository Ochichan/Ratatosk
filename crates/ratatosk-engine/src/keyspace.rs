use bytes::Bytes;
use hashbrown::{HashMap, HashSet};
use smallvec::SmallVec;

pub use crate::acl::{AclState, AclUser};
pub use crate::clients::{BlockingState, ClientRegistry, ClientSnapshot};
use crate::config::ConfigState;
pub use crate::pubsub::{PubSubMessage, PubSubState};
pub use crate::replication::{
    ReplicaClientInfo, ReplicaClientState, ReplicationMode, ReplicationState,
};
pub use crate::stats::{AtomicStatsState, SlowlogEntry, StatsState};
pub use crate::tracking::ClientTrackingState;
use ratatosk_core::time::now_ms as unix_ms_now;
use std::{
    cmp::Ordering,
    collections::{BTreeMap, VecDeque},
    path::PathBuf,
    sync::{
        Arc,
        atomic::{AtomicI64, AtomicU64, Ordering as AtomicOrdering},
    },
};
use tokio::sync::{Mutex, Notify};

pub const DEFAULT_DB_COUNT: usize = 16;

/// Bridge contract version exposed in INFO server output.
/// Cross-component bridges use this for compatibility handshakes.
pub const BRIDGE_CONTRACT_VERSION: &str = "0.1";

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

    /// Insert a member with a score into the sorted set.
    ///
    /// Returns `true` if this is a new member, `false` if the score was updated
    /// for an existing member. Rejects NaN and infinite scores.
    pub fn insert(&mut self, member: Bytes, score: f64) -> bool {
        // 1. Score validity check (must be first - reject NaN/Inf)
        if !score.is_finite() {
            return false;
        }

        let new_score = SortedSetScore(score);

        // Single hash probe via entry API (one fewer probe than get + insert).
        match self.by_member.entry(member) {
            hashbrown::hash_map::Entry::Occupied(mut occ) => {
                let old_score = *occ.get();
                if old_score == new_score {
                    return false;
                }
                // Remove old by_score entry before updating by_member.
                self.by_score.remove(&SortedSetEntry {
                    score: old_score,
                    member: occ.key().clone(),
                });
                self.by_score.insert(
                    SortedSetEntry {
                        score: new_score,
                        member: occ.key().clone(),
                    },
                    (),
                );
                *occ.get_mut() = new_score;
                false
            }
            hashbrown::hash_map::Entry::Vacant(vac) => {
                let member_clone = vac.key().clone();
                vac.insert(new_score);
                self.by_score.insert(
                    SortedSetEntry {
                        score: new_score,
                        member: member_clone,
                    },
                    (),
                );
                true
            }
        }
    }

    /// Remove a member from the sorted set.
    ///
    /// Returns `true` if the member was present and removed, `false` otherwise.
    pub fn remove(&mut self, member: &Bytes) -> bool {
        // 1. Remove from by_member first and get the score
        let Some(score) = self.by_member.remove(member) else {
            return false;
        };

        // 2. Remove from by_score using the obtained score
        self.by_score.remove(&SortedSetEntry {
            score,
            member: member.clone(),
        });
        true
    }

    pub fn score(&self, member: &[u8]) -> Option<f64> {
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

    /// Remove all members whose score is in `[min, max]` (inclusive).
    ///
    /// Uses `BTreeMap::range` to seek to `min` in O(log n), avoiding a full
    /// linear scan of all entries.  Returns the number of removed members.
    pub fn remove_range_by_score(&mut self, min: f64, max: f64) -> usize {
        let min_bound = SortedSetEntry {
            score: SortedSetScore(min),
            member: Bytes::new(),
        };
        let members: SmallVec<[Bytes; 16]> = self
            .by_score
            .range(min_bound..)
            .take_while(|(e, _)| e.score.value() <= max)
            .map(|(e, _)| e.member.clone()) // Bytes clone = ref-count incr
            .collect();
        let count = members.len();
        for member in &members {
            self.remove(member);
        }
        count
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
// AofWriteState — explicit state machine for AOF write latch
// ---------------------------------------------------------------------------

/// Represents the state of AOF (Append-Only File) writes.
///
/// When a write error occurs, the AOF enters a "latched" state where
/// further writes are blocked until the error is cleared.
#[derive(Debug, Clone, Default)]
pub enum AofWriteState {
    /// Normal operation - writes are allowed
    #[default]
    Normal,
    /// Latched state - writes blocked due to error
    Latched {
        last_error: String,
        latched_at_ms: i64,
    },
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
// SharedState — independently-locked server state components
// ---------------------------------------------------------------------------

/// Top-level shared state with per-component locking.
///
/// Hot-path operations (stats, config reads) bypass the inner `ServerState`
/// lock entirely via atomic counters and [`arc_swap::ArcSwap`].  The inner
/// lock is still acquired for data mutations and less-frequent operations.
///
/// This is Phase 0c of the concurrency overhaul: independent stats/config
/// access while the bulk of server state remains behind a single lock.
pub struct SharedState {
    /// Per-DB data with `parking_lot::RwLock` — commands on different DBs
    /// run in parallel, same-DB reads share the lock.
    pub data: DataState,

    /// Non-DB server state — ACL, replication, clients, blocking,
    /// tracking, monitors, persistence flags.
    pub meta: Mutex<ServerState>,

    /// Lock-free atomic counters for the hottest stats fields.
    pub stats: AtomicStatsState,

    /// Lock-free read access to the current config.  Updated via
    /// `store()` on CONFIG SET; readers call `load()` without any lock.
    pub config_cache: arc_swap::ArcSwap<ConfigState>,

    /// Atomic client-ID allocator — no lock needed for new connections.
    pub next_client_id: AtomicI64,

    /// Server start timestamp (immutable after init).
    pub started_at_ms: i64,

    /// Cluster node ID (immutable after init).
    pub cluster_node_id: Bytes,
}

impl SharedState {
    /// Create a new `SharedState` from an existing `ServerState`.
    ///
    /// The atomic stats are initialized from the `ServerState`'s current
    /// counters, and the config cache is seeded from its `ConfigState`.
    pub fn new(server: ServerState) -> Self {
        Self::with_data(DataState::default(), server)
    }

    /// Create with a specific number of databases.
    pub fn with_db_count(db_count: usize, server: ServerState) -> Self {
        Self::with_data(DataState::new(db_count), server)
    }

    /// Create from separate DataState and ServerState.
    pub fn with_data(data: DataState, mut server: ServerState) -> Self {
        let config = server.config.clone();
        let started_at_ms = server.started_at_ms;
        let cluster_node_id = server.cluster_node_id.clone();
        let next_client_id = server.next_client_id;
        // Clear next_client_id from inner state — allocation is now atomic.
        server.next_client_id = i64::MAX;

        let stats = AtomicStatsState::from_stats(&server.stats);

        Self {
            data,
            meta: Mutex::new(server),
            stats,
            config_cache: arc_swap::ArcSwap::from_pointee(config),
            next_client_id: AtomicI64::new(next_client_id),
            started_at_ms,
            cluster_node_id,
        }
    }

    /// Allocate a new unique client ID (lock-free).
    pub fn alloc_client_id(&self) -> i64 {
        let id = self.next_client_id.fetch_add(1, AtomicOrdering::Relaxed);
        if id >= ServerState::MAX_CLIENT_ID {
            panic!(
                "client ID pool exhausted (reached {}), restart server to reset",
                ServerState::MAX_CLIENT_ID
            );
        }
        id
    }

    /// Update the config cache after a CONFIG SET.
    ///
    /// Call this while still holding the inner lock so that the ArcSwap
    /// is updated atomically with respect to the canonical `ConfigState`.
    pub fn update_config_cache(&self, config: &ConfigState) {
        self.config_cache.store(Arc::new(config.clone()));
    }

    /// Total connections received (derived from client ID counter).
    pub fn total_connections_received(&self) -> u64 {
        self.stats.total_connections_received()
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
// DbShard + DataState — per-DB data grouping
// ---------------------------------------------------------------------------

/// One logical database — data + WATCH versions.
#[derive(Debug, Clone, Default)]
pub struct DbShard {
    pub data: HashMap<Bytes, StoredValue>,
    pub key_versions: HashMap<Bytes, u64>,
}

/// The DB layer — per-DB `parking_lot::RwLock` for concurrent access.
///
/// Commands on different DBs execute in parallel. Same-DB reads share
/// the lock; writes are exclusive. Lock ordering: always ascending index.
pub struct DataState {
    shards: Vec<parking_lot::RwLock<DbShard>>,
    next_key_version: AtomicU64,
}

impl std::fmt::Debug for DataState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DataState")
            .field("shard_count", &self.shards.len())
            .field(
                "next_key_version",
                &self.next_key_version.load(AtomicOrdering::Relaxed),
            )
            .finish()
    }
}

impl Default for DataState {
    fn default() -> Self {
        Self::new(DEFAULT_DB_COUNT)
    }
}

impl DataState {
    pub fn new(db_count: usize) -> Self {
        let mut shards = Vec::with_capacity(db_count);
        for _ in 0..db_count {
            shards.push(parking_lot::RwLock::new(DbShard::default()));
        }
        Self {
            shards,
            next_key_version: AtomicU64::new(1),
        }
    }

    pub fn db_count(&self) -> usize {
        self.shards.len()
    }

    /// Single-DB read lock.
    pub fn read_db(&self, idx: usize) -> parking_lot::RwLockReadGuard<'_, DbShard> {
        self.shards[idx].read()
    }

    /// Single-DB write lock.
    pub fn write_db(&self, idx: usize) -> parking_lot::RwLockWriteGuard<'_, DbShard> {
        self.shards[idx].write()
    }

    /// Two DBs in ascending order. Asserts a != b.
    pub fn write_two_dbs(
        &self,
        a: usize,
        b: usize,
    ) -> (
        parking_lot::RwLockWriteGuard<'_, DbShard>,
        parking_lot::RwLockWriteGuard<'_, DbShard>,
    ) {
        assert_ne!(a, b, "write_two_dbs called with same index");
        let (lo, hi) = if a < b { (a, b) } else { (b, a) };
        let lo_g = self.shards[lo].write();
        let hi_g = self.shards[hi].write();
        if a < b { (lo_g, hi_g) } else { (hi_g, lo_g) }
    }

    /// All DBs write-locked in ascending order. For FLUSHALL, load_from_rdb.
    pub fn write_all_dbs(&self) -> Vec<parking_lot::RwLockWriteGuard<'_, DbShard>> {
        (0..self.shards.len())
            .map(|i| self.shards[i].write())
            .collect()
    }

    /// Allocate the next key version (atomic, lock-free).
    pub fn alloc_key_version(&self) -> u64 {
        self.next_key_version.fetch_add(1, AtomicOrdering::Relaxed)
    }

    /// Per-DB sequential snapshot. Each DB read-locked briefly, cloned, released.
    pub fn snapshot_all(&self) -> DbSnapshot {
        (0..self.shards.len())
            .map(|i| {
                let guard = self.shards[i].read();
                guard.data.clone()
            })
            .collect()
    }

    /// Load RDB data into all DBs. Acquires all write locks in order.
    pub fn load_from_snapshot(&self, snapshot: DbSnapshot) {
        let mut guards = self.write_all_dbs();
        for (i, db_data) in snapshot.into_iter().enumerate() {
            if i < guards.len() {
                guards[i].data = db_data;
                guards[i].key_versions.clear();
            }
        }
    }
}

// ---------------------------------------------------------------------------
// ServerState — top-level composition
// ---------------------------------------------------------------------------

#[derive(Debug, Default)]
pub struct ServerState {
    pub data: DataState,
    next_client_id: i64,
    started_at_ms: i64,
    pub pubsub: PubSubState,
    pub stats: StatsState,
    pub acl: AclState,
    pub config: ConfigState,
    pub script_cache: ScriptCache,
    pub cluster_node_id: Bytes,
    replication: ReplicationState,
    tracking: ClientTrackingState,
    clients: ClientRegistry,
    blocking: BlockingState,
    lazy_free_tx: Option<LazyFreeSender>,
    rdb_save_in_progress: bool,
    last_rdb_save_status: Option<Result<(), String>>,
    last_rdb_save_time_ms: Option<i64>,
    aof_enabled: bool,
    aof_write_state: AofWriteState,
    aof_rewrite_in_progress: bool,
    last_aof_rewrite_status: Option<Result<(), String>>,
    last_aof_rewrite_time_ms: Option<i64>,
    aof_current_path: Option<PathBuf>,
    aof_base_path: Option<PathBuf>,
    /// Set of client IDs that are in MONITOR mode.
    monitor_clients: HashSet<i64>,
    /// Pending monitor output lines per client. Each entry is a pre-formatted
    /// RESP simple-string line ready for encoding.
    monitor_pending: HashMap<i64, Vec<Bytes>>,
    /// Per-client Notify used to wake MONITOR clients when new monitor output
    /// is available.  Separate from the pub/sub mpsc channel.
    monitor_notifiers: HashMap<i64, Arc<tokio::sync::Notify>>,
}

impl ServerState {
    /// Maximum safe client ID before we risk wraparound.
    /// Using i64::MAX / 2 provides a large safety margin while still
    /// allowing billions of connections.
    const MAX_CLIENT_ID: i64 = i64::MAX / 2;

    pub fn new(db_count: usize) -> Self {
        let node_id = generate_cluster_node_id();
        let config = ConfigState::default();
        let pubsub = PubSubState::new(&config);
        Self {
            data: DataState::new(db_count),
            next_client_id: 1,
            started_at_ms: unix_ms_now(),
            pubsub,
            stats: StatsState::default(),
            acl: AclState::default(),
            config,
            script_cache: ScriptCache::default(),
            cluster_node_id: node_id,
            replication: ReplicationState::default(),
            tracking: ClientTrackingState::default(),
            clients: ClientRegistry::default(),
            blocking: BlockingState::default(),
            lazy_free_tx: None,
            rdb_save_in_progress: false,
            last_rdb_save_status: None,
            last_rdb_save_time_ms: None,
            aof_enabled: false,
            aof_write_state: AofWriteState::default(),
            aof_rewrite_in_progress: false,
            last_aof_rewrite_status: None,
            last_aof_rewrite_time_ms: None,
            aof_current_path: None,
            aof_base_path: None,
            monitor_clients: HashSet::new(),
            monitor_pending: HashMap::new(),
            monitor_notifiers: HashMap::new(),
        }
    }

    pub fn with_default_dbs() -> Self {
        Self::new(DEFAULT_DB_COUNT)
    }

    pub fn alloc_client_id(&mut self) -> i64 {
        if self.next_client_id >= Self::MAX_CLIENT_ID {
            panic!(
                "client ID pool exhausted (reached {}), restart server to reset",
                Self::MAX_CLIENT_ID
            );
        }
        let id = self.next_client_id;
        self.next_client_id += 1;
        id
    }

    pub fn total_connections_received(&self) -> u64 {
        self.stats.total_connections_received()
    }

    pub fn db_count(&self) -> usize {
        self.data.db_count()
    }

    /// Acquire a read lock on a DB, returning a mapped guard to the inner HashMap.
    pub fn db(
        &self,
        idx: usize,
    ) -> parking_lot::MappedRwLockReadGuard<'_, HashMap<Bytes, StoredValue>> {
        parking_lot::RwLockReadGuard::map(self.data.read_db(idx), |s| &s.data)
    }

    /// Acquire a write lock on a DB, returning a mapped guard to the inner HashMap.
    pub fn db_mut(
        &self,
        idx: usize,
    ) -> parking_lot::MappedRwLockWriteGuard<'_, HashMap<Bytes, StoredValue>> {
        parking_lot::RwLockWriteGuard::map(self.data.write_db(idx), |s| &mut s.data)
    }

    pub fn clear_db(&self, idx: usize) {
        let mut shard = self.data.write_db(idx);
        shard.data.clear();
        shard.key_versions.clear();
    }

    pub fn swap_dbs(&self, left: usize, right: usize) {
        let (mut a, mut b) = self.data.write_two_dbs(left, right);
        std::mem::swap(&mut *a, &mut *b);
    }

    pub fn clear_all_dbs(&self) {
        for mut shard in self.data.write_all_dbs() {
            shard.data.clear();
            shard.key_versions.clear();
        }
    }

    pub fn snapshot_dbs(&self) -> DbSnapshot {
        self.data.snapshot_all()
    }

    pub fn load_from_rdb(&self, snapshot: DbSnapshot) {
        self.data.load_from_snapshot(snapshot);
    }

    pub fn key_version(&self, db_idx: usize, key: &Bytes) -> u64 {
        let shard = self.data.read_db(db_idx);
        shard.key_versions.get(key).copied().unwrap_or(0)
    }

    pub fn touch_key_version(&self, db_idx: usize, key: Bytes) {
        let mut shard = self.data.write_db(db_idx);
        let version = self.data.alloc_key_version();
        shard.key_versions.insert(key, version);
    }

    pub fn lazy_free_del(&self, db_idx: usize, key: &Bytes) -> bool {
        let mut shard = self.data.write_db(db_idx);
        let Some(value) = shard.data.remove(key) else {
            return false;
        };
        let version = self.data.alloc_key_version();
        shard.key_versions.insert(key.clone(), version);
        drop(shard);
        self.try_lazy_free(value);
        true
    }

    pub fn lazy_free_flush_db(&self, db_idx: usize) {
        let mut shard = self.data.write_db(db_idx);
        let old_db = std::mem::take(&mut shard.data);
        shard.key_versions.clear();
        drop(shard);

        if let Some(tx) = self.lazy_free_tx() {
            for (_, value) in old_db {
                let _ = tx.try_send(value);
            }
        }
    }

    pub fn started_at_ms(&self) -> i64 {
        self.started_at_ms
    }

    pub fn replication_mode(&self) -> &ReplicationMode {
        &self.replication.mode
    }

    pub fn replication_primary_replid(&self) -> &Bytes {
        &self.replication.primary_replid
    }

    pub fn replication_offset(&self) -> i64 {
        self.replication.master_repl_offset
    }

    pub fn advance_replication_offset(&mut self) {
        self.replication.master_repl_offset = self.replication.master_repl_offset.saturating_add(1);
    }

    pub fn replication_connected_replicas(&self) -> usize {
        self.replication.replicas.len()
    }

    pub fn replication_acked_replicas(&self, target_offset: i64) -> usize {
        self.replication
            .replicas
            .values()
            .filter(|replica| replica.handshake_complete && replica.ack_offset >= target_offset)
            .count()
    }

    pub fn replication_replica_infos(&self, now_ms: i64) -> Vec<ReplicaClientInfo> {
        let mut infos = self
            .replication
            .replicas
            .iter()
            .map(|(client_id, replica)| {
                let lag_seconds = replica
                    .ack_time_ms
                    .map(|ack_time| ((now_ms - ack_time).max(0)) / 1000)
                    .unwrap_or(-1);
                ReplicaClientInfo {
                    client_id: *client_id,
                    listening_port: replica.listening_port.unwrap_or(0),
                    ip_address: replica
                        .ip_address
                        .clone()
                        .unwrap_or_else(|| Bytes::from_static(b"unknown")),
                    ack_offset: replica.ack_offset,
                    lag_seconds,
                    state: if replica.handshake_complete {
                        "online"
                    } else {
                        "handshake"
                    },
                }
            })
            .collect::<Vec<_>>();
        infos.sort_by_key(|info| info.client_id);
        infos
    }

    pub fn replication_configure_master(&mut self) {
        self.replication.reset_as_master();
    }

    pub fn replication_configure_replica(&mut self, master_host: Bytes, master_port: i64) {
        self.replication.mode = ReplicationMode::Replica {
            master_host,
            master_port,
        };
    }

    pub fn replication_set_listening_port(&mut self, client_id: i64, port: i64) {
        self.replication.replica_entry_mut(client_id).listening_port = Some(port);
    }

    pub fn replication_set_ip_address(&mut self, client_id: i64, ip_address: Bytes) {
        self.replication.replica_entry_mut(client_id).ip_address = Some(ip_address);
    }

    pub fn replication_set_capabilities<I>(&mut self, client_id: i64, capabilities: I)
    where
        I: IntoIterator<Item = Bytes>,
    {
        self.replication.replica_entry_mut(client_id).capabilities =
            capabilities.into_iter().collect();
    }

    pub fn replication_set_ack_offset(&mut self, client_id: i64, ack_offset: i64) {
        let replica = self.replication.replica_entry_mut(client_id);
        replica.ack_offset = ack_offset;
        replica.ack_time_ms = Some(unix_ms_now());
        replica.handshake_complete = true;
    }

    pub fn replication_mark_psync(&mut self, client_id: i64) {
        let current_offset = self.replication.master_repl_offset;
        let replica = self.replication.replica_entry_mut(client_id);
        replica.handshake_complete = true;
        replica.ack_offset = current_offset;
        replica.ack_time_ms = Some(unix_ms_now());
    }

    pub fn replication_remove_client(&mut self, client_id: i64) {
        self.replication.replicas.remove(&client_id);
    }

    #[allow(clippy::too_many_arguments)]
    pub fn tracking_register_key(
        &mut self,
        tracker_client_id: i64,
        target_client_id: i64,
        no_loop: bool,
        db_idx: usize,
        key: Bytes,
    ) {
        self.tracking
            .track_key(tracker_client_id, target_client_id, no_loop, db_idx, key);
    }

    pub fn tracking_configure_broadcast(
        &mut self,
        tracker_client_id: i64,
        target_client_id: i64,
        no_loop: bool,
        prefixes: Vec<Bytes>,
    ) {
        self.tracking
            .configure_broadcast(tracker_client_id, target_client_id, no_loop, prefixes);
    }

    pub fn tracking_clear_tracker(&mut self, tracker_client_id: i64) {
        self.tracking.clear_tracker(tracker_client_id);
    }

    pub fn tracking_remove_client(&mut self, client_id: i64) {
        self.tracking.clear_tracker(client_id);
        let detached_trackers = self.tracking.clear_target(client_id);
        for tracker_client_id in detached_trackers {
            if self
                .client_snapshot(tracker_client_id)
                .is_some_and(|snapshot| snapshot.resp >= 3)
            {
                let _ = self.pubsub.enqueue_invalidation_message(
                    tracker_client_id,
                    PubSubMessage::TrackingRedirectBroken {
                        redirect_client_id: client_id,
                    },
                );
            }
        }
        self.blocking.remove_client(client_id);
    }

    pub fn tracking_invalidate_keys<I>(&mut self, writer_client_id: i64, db_idx: usize, keys: I)
    where
        I: IntoIterator<Item = Bytes>,
    {
        let invalidations = self
            .tracking
            .invalidate_keys(writer_client_id, db_idx, keys);
        for (client_id, keys) in invalidations {
            let _ = self.pubsub.enqueue_invalidation(client_id, keys);
        }
    }

    pub fn register_blocked_client(
        &mut self,
        client_id: i64,
        keys: Vec<(usize, Bytes)>,
    ) -> Arc<Notify> {
        self.blocking.register(client_id, keys)
    }

    pub fn clear_blocked_client(&mut self, client_id: i64) {
        self.blocking.clear(client_id);
    }

    pub fn notify_blocked_clients<I>(&self, db_idx: usize, keys: I)
    where
        I: IntoIterator<Item = Bytes>,
    {
        self.blocking.notify_keys(db_idx, keys);
    }

    pub fn upsert_client_snapshot(&mut self, snapshot: ClientSnapshot) {
        self.clients.upsert(snapshot);
    }

    pub fn client_snapshot(&self, client_id: i64) -> Option<&ClientSnapshot> {
        self.clients.get(client_id)
    }

    pub fn client_snapshots(&self) -> Vec<ClientSnapshot> {
        self.clients.list()
    }

    pub fn tracking_active_redirect(
        &self,
        tracker_client_id: i64,
        configured_redirect: i64,
    ) -> i64 {
        if configured_redirect < 0 {
            return -1;
        }
        if configured_redirect != tracker_client_id
            && !self.tracking.redirect_broken(tracker_client_id)
            && self.clients.get(configured_redirect).is_some()
        {
            configured_redirect
        } else {
            -1
        }
    }

    pub fn tracking_target_client_id(
        &self,
        tracker_client_id: i64,
        configured_redirect: i64,
    ) -> i64 {
        let active_redirect = self.tracking_active_redirect(tracker_client_id, configured_redirect);
        if active_redirect >= 0 {
            active_redirect
        } else {
            tracker_client_id
        }
    }

    pub fn tracking_redirect_broken(&self, tracker_client_id: i64) -> bool {
        self.tracking.redirect_broken(tracker_client_id)
    }

    pub fn remove_client_snapshot(&mut self, client_id: i64) {
        self.clients.remove(client_id);
    }

    pub fn set_client_blocked(&mut self, client_id: i64, blocked: bool) {
        self.clients.set_blocked(client_id, blocked);
    }

    pub fn client_is_blocked(&self, client_id: i64) -> bool {
        self.clients.is_blocked(client_id)
    }

    pub fn blocked_clients(&self) -> usize {
        self.clients.blocked_clients()
    }

    pub fn tracking_clients(&self) -> usize {
        self.clients.tracking_clients()
    }

    pub fn connected_client_snapshots(&self) -> usize {
        self.clients.len()
    }

    /// Returns the server uptime in seconds.
    pub fn uptime_seconds(&self) -> i64 {
        let now = ratatosk_core::time::now_ms();
        (now - self.started_at_ms) / 1000
    }

    /// Set the lazy-free sender channel. Called once during server startup.
    pub fn set_lazy_free_sender(&mut self, sender: LazyFreeSender) {
        self.lazy_free_tx = Some(sender);
    }

    /// Try to send a value to the lazy-free thread. Returns false if
    /// no channel or value is too small.
    pub fn try_lazy_free(&self, value: StoredValue) -> bool {
        if let Some(ref tx) = self.lazy_free_tx {
            if should_lazy_free(&value) {
                let _ = tx.try_send(value);
                return true;
            }
        }
        false
    }

    /// Access the lazy-free sender (for FLUSHDB).
    pub fn lazy_free_tx(&self) -> Option<&LazyFreeSender> {
        self.lazy_free_tx.as_ref()
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

    pub fn aof_last_error(&self) -> Option<&str> {
        match &self.aof_write_state {
            AofWriteState::Latched { last_error, .. } => Some(last_error),
            AofWriteState::Normal => None,
        }
    }

    pub fn set_aof_last_error(&mut self, error: impl Into<String>) {
        self.aof_write_state = AofWriteState::Latched {
            last_error: error.into(),
            latched_at_ms: unix_ms_now(),
        };
    }

    pub fn clear_aof_last_error(&mut self) {
        self.aof_write_state = AofWriteState::Normal;
    }

    pub fn aof_write_latched(&self) -> bool {
        matches!(self.aof_write_state, AofWriteState::Latched { .. })
    }

    /// Returns the timestamp when AOF was latched, if currently latched.
    pub fn aof_latched_at_ms(&self) -> Option<i64> {
        match &self.aof_write_state {
            AofWriteState::Latched { latched_at_ms, .. } => Some(*latched_at_ms),
            AofWriteState::Normal => None,
        }
    }

    pub fn aof_rewrite_in_progress(&self) -> bool {
        self.aof_rewrite_in_progress
    }

    pub fn set_aof_rewrite_in_progress(&mut self, value: bool) {
        self.aof_rewrite_in_progress = value;
    }

    pub fn last_aof_rewrite_status(&self) -> Option<&Result<(), String>> {
        self.last_aof_rewrite_status.as_ref()
    }

    pub fn set_last_aof_rewrite_status(&mut self, status: Result<(), String>) {
        self.last_aof_rewrite_status = Some(status);
    }

    pub fn clear_last_aof_rewrite_status(&mut self) {
        self.last_aof_rewrite_status = None;
    }

    pub fn last_aof_rewrite_time_ms(&self) -> Option<i64> {
        self.last_aof_rewrite_time_ms
    }

    pub fn set_last_aof_rewrite_time_ms(&mut self, value: i64) {
        self.last_aof_rewrite_time_ms = Some(value);
    }

    pub fn clear_last_aof_rewrite_time_ms(&mut self) {
        self.last_aof_rewrite_time_ms = None;
    }

    pub fn aof_current_path(&self) -> Option<&PathBuf> {
        self.aof_current_path.as_ref()
    }

    pub fn set_aof_current_path(&mut self, path: Option<PathBuf>) {
        self.aof_current_path = path;
    }

    pub fn aof_base_path(&self) -> Option<&PathBuf> {
        self.aof_base_path.as_ref()
    }

    pub fn set_aof_base_path(&mut self, path: Option<PathBuf>) {
        self.aof_base_path = path;
    }

    // -----------------------------------------------------------------------
    // MONITOR support
    // -----------------------------------------------------------------------

    /// Register a client as a MONITOR listener.
    pub fn register_monitor(&mut self, client_id: i64) {
        self.monitor_clients.insert(client_id);
    }

    /// Remove a client from the MONITOR set and drop its pending queue.
    pub fn unregister_monitor(&mut self, client_id: i64) {
        self.monitor_clients.remove(&client_id);
        self.monitor_pending.remove(&client_id);
        self.monitor_notifiers.remove(&client_id);
    }

    /// Returns `true` if at least one client is in MONITOR mode.
    /// This is the hot-path guard — when `false`, the command dispatch
    /// skips all formatting work.
    #[inline]
    pub fn has_monitors(&self) -> bool {
        !self.monitor_clients.is_empty()
    }

    /// Returns `true` if a specific client is in MONITOR mode.
    pub fn is_monitor_client(&self, client_id: i64) -> bool {
        self.monitor_clients.contains(&client_id)
    }

    /// Push a pre-formatted monitor line to every MONITOR client except
    /// the one that issued the command (`source_client_id`).
    pub fn broadcast_monitor_message(&mut self, source_client_id: i64, line: Bytes) {
        // Collect target client IDs first to satisfy the borrow checker.
        let targets: SmallVec<[i64; 8]> = self
            .monitor_clients
            .iter()
            .copied()
            .filter(|&cid| cid != source_client_id)
            .collect();

        for cid in &targets {
            self.monitor_pending
                .entry(*cid)
                .or_default()
                .push(line.clone());
        }
        // Wake monitor clients so their I/O loops pick up pending lines.
        for cid in &targets {
            if let Some(n) = self.monitor_notifiers.get(cid) {
                n.notify_one();
            }
        }
    }

    /// Register a Notify handle for MONITOR wake-ups and return a clone.
    pub fn register_monitor_notifier(&mut self, client_id: i64) -> Arc<tokio::sync::Notify> {
        self.monitor_notifiers
            .entry(client_id)
            .or_insert_with(|| Arc::new(tokio::sync::Notify::new()))
            .clone()
    }

    /// Drain all pending monitor messages for a client.
    pub fn drain_monitor_messages(&mut self, client_id: i64) -> Vec<Bytes> {
        self.monitor_pending.remove(&client_id).unwrap_or_default()
    }

    /// Returns the number of MONITOR clients.
    pub fn monitor_client_count(&self) -> usize {
        self.monitor_clients.len()
    }
}

// ---------------------------------------------------------------------------
// Free functions
// ---------------------------------------------------------------------------

pub fn purge_expired_key(
    db: &mut impl std::ops::DerefMut<Target = HashMap<Bytes, StoredValue>>,
    key: &Bytes,
    now_ms: i64,
) {
    if db
        .get(key.as_ref())
        .is_some_and(|v| v.expire_at_ms.is_some_and(|at| at <= now_ms))
    {
        db.remove(key.as_ref());
    }
}

pub fn purge_expired_keys(
    db: &mut impl std::ops::DerefMut<Target = HashMap<Bytes, StoredValue>>,
    now_ms: i64,
) {
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
    use std::{path::PathBuf, time::Duration};

    use bytes::Bytes;
    use tokio::time::timeout;

    use super::{AtomicStatsState, PubSubState, ServerState, StatsState, StoredValue};

    #[test]
    fn remove_client_cleans_all_subscriptions() {
        let mut ps = PubSubState::default();
        let mut _rx1 = ps.register_client(1);
        let mut _rx2 = ps.register_client(2);

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

        // ch1 should still have client 2
        assert_eq!(ps.numsub(&[Bytes::from("ch1")])[0].1, 1);
        // ch2 should be gone (no subscribers left)
        assert_eq!(ps.numsub(&[Bytes::from("ch2")])[0].1, 0);
    }

    #[test]
    fn publish_after_remove_client_returns_zero() {
        let mut ps = PubSubState::default();
        let mut _rx1 = ps.register_client(1);

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
    fn pending_queue_overflow_drops_messages_when_channel_full() {
        let mut ps = PubSubState::default();
        let mut rx = ps.register_client(1);
        let channel = Bytes::from("news");
        let payload = Bytes::from("msg");

        ps.subscribe_channel(1, channel.clone());
        let limit = ps.pending_queue_limit();
        for _ in 0..limit {
            assert_eq!(ps.publish(&channel, &payload), 1);
        }

        // The first publish past the cap drops delivery (channel full).
        assert_eq!(ps.publish(&channel, &payload), 0);

        // Once overflowed (sender removed), further deliveries are dropped.
        assert_eq!(ps.publish(&channel, &payload), 0);

        // Drain all messages that were successfully sent.
        let drained = PubSubState::drain_rx(&mut rx);
        assert_eq!(drained.len(), limit);

        // Removing and re-subscribing clears overflow bookkeeping.
        ps.remove_client(1);
        let mut rx2 = ps.register_client(1);
        ps.subscribe_channel(1, channel.clone());
        assert_eq!(ps.publish(&channel, &payload), 1);
        let drained2 = PubSubState::drain_rx(&mut rx2);
        assert_eq!(drained2.len(), 1);
    }

    #[tokio::test]
    async fn pubsub_receiver_wakes_client_for_invalidation() {
        let mut ps = PubSubState::default();
        let mut rx = ps.register_client(7);

        assert!(ps.enqueue_invalidation(7, vec![Bytes::from("tracked")]));

        let msg = timeout(Duration::from_millis(50), rx.recv())
            .await
            .expect("receiver should wake for invalidation")
            .expect("should receive a message");
        assert!(matches!(msg, super::PubSubMessage::Invalidate { .. }));
    }

    #[test]
    fn enqueue_invalidation_drops_for_missing_client() {
        let mut ps = PubSubState::default();
        assert!(!ps.enqueue_invalidation(99, vec![Bytes::from("tracked")]));
    }

    #[test]
    fn server_state_persistence_fields_default() {
        let state = ServerState::with_default_dbs();
        assert!(!state.rdb_save_in_progress());
        assert!(state.last_rdb_save_status().is_none());
        assert!(state.last_rdb_save_time_ms().is_none());
        assert!(!state.aof_enabled());
        assert!(!state.aof_write_latched());
        assert!(state.aof_last_error().is_none());
        assert!(!state.aof_rewrite_in_progress());
        assert!(state.last_aof_rewrite_status().is_none());
        assert!(state.last_aof_rewrite_time_ms().is_none());
        assert!(state.aof_current_path().is_none());
        assert!(state.aof_base_path().is_none());
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

        state.set_aof_last_error("disk full");
        assert!(state.aof_write_latched());
        assert_eq!(state.aof_last_error(), Some("disk full"));

        state.clear_aof_last_error();
        assert!(!state.aof_write_latched());
        assert!(state.aof_last_error().is_none());

        state.set_aof_rewrite_in_progress(true);
        assert!(state.aof_rewrite_in_progress());

        state.set_last_aof_rewrite_status(Ok(()));
        assert_eq!(state.last_aof_rewrite_status(), Some(&Ok(())));

        state.set_last_aof_rewrite_status(Err("rewrite failed".to_string()));
        assert_eq!(
            state.last_aof_rewrite_status(),
            Some(&Err("rewrite failed".to_string()))
        );

        state.set_last_aof_rewrite_time_ms(9_999);
        assert_eq!(state.last_aof_rewrite_time_ms(), Some(9_999));

        let current = PathBuf::from("/tmp/appendonly.aof.current");
        let base = PathBuf::from("/tmp/appendonly.aof.base");
        state.set_aof_current_path(Some(current.clone()));
        state.set_aof_base_path(Some(base.clone()));
        assert_eq!(state.aof_current_path(), Some(&current));
        assert_eq!(state.aof_base_path(), Some(&base));

        state.set_aof_rewrite_in_progress(false);
        state.clear_last_aof_rewrite_status();
        state.clear_last_aof_rewrite_time_ms();
        state.set_aof_current_path(None);
        state.set_aof_base_path(None);
        assert!(!state.aof_rewrite_in_progress());
        assert!(state.last_aof_rewrite_status().is_none());
        assert!(state.last_aof_rewrite_time_ms().is_none());
        assert!(state.aof_current_path().is_none());
        assert!(state.aof_base_path().is_none());
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
        assert_eq!(stats.total_connections_received(), 0);

        stats.mark_client_connected();
        stats.mark_client_connected();
        assert_eq!(stats.connected_clients(), 2);
        assert_eq!(stats.total_connections_received(), 2);

        stats.mark_client_disconnected();
        assert_eq!(stats.connected_clients(), 1);
        assert_eq!(stats.total_connections_received(), 2);

        stats.mark_client_disconnected();
        assert_eq!(stats.connected_clients(), 0);
        assert_eq!(stats.total_connections_received(), 2);

        stats.mark_client_disconnected();
        assert_eq!(stats.connected_clients(), 0, "should not underflow");
        assert_eq!(stats.total_connections_received(), 2);
    }

    #[test]
    fn atomic_stats_connected_clients_tracks_connect_disconnect() {
        let stats = AtomicStatsState::default();
        assert_eq!(stats.connected_clients(), 0);
        assert_eq!(stats.total_connections_received(), 0);

        stats.mark_client_connected();
        stats.mark_client_connected();
        assert_eq!(stats.connected_clients(), 2);
        assert_eq!(stats.total_connections_received(), 2);

        stats.mark_client_disconnected();
        assert_eq!(stats.connected_clients(), 1);
        assert_eq!(stats.total_connections_received(), 2);

        stats.mark_client_disconnected();
        assert_eq!(stats.connected_clients(), 0);
        assert_eq!(stats.total_connections_received(), 2);

        stats.mark_client_disconnected();
        assert_eq!(stats.connected_clients(), 0, "should not underflow");
        assert_eq!(stats.total_connections_received(), 2);
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
        assert_eq!(stats.total_connections_received(), 0);
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

    #[test]
    fn latency_sample_case_insensitive_dedup() {
        let mut stats = StatsState::default();
        stats.set_latency_tracking_enabled(true);
        stats.record_latency_sample(b"GET", 5);
        stats.record_latency_sample(b"GET", 10);
        assert_eq!(stats.latency_event_names().len(), 1);
        assert_eq!(stats.latency_event_names()[0].as_ref(), b"get");
    }

    // -----------------------------------------------------------------------
    // Phase E: Per-DB RwLock concurrency tests
    // -----------------------------------------------------------------------

    #[test]
    fn concurrent_multi_db_set_get() {
        use std::sync::Arc;

        let state = Arc::new(ServerState::with_default_dbs());
        let mut handles = Vec::new();

        // N threads, each writing to a different DB
        for db_idx in 0..4usize {
            let state = Arc::clone(&state);
            handles.push(std::thread::spawn(move || {
                for i in 0..100 {
                    let key = Bytes::from(format!("key:{i}"));
                    let val = StoredValue::string(Bytes::from(format!("val:{db_idx}:{i}")), None);
                    state.db_mut(db_idx).insert(key, val);
                }
            }));
        }

        for h in handles {
            h.join().expect("thread panicked");
        }

        // Verify each DB has its own 100 keys with correct values
        for db_idx in 0..4usize {
            let db = state.db(db_idx);
            assert_eq!(db.len(), 100, "DB {db_idx} should have 100 keys");
            for i in 0..100 {
                let key = Bytes::from(format!("key:{i}"));
                let expected = Bytes::from(format!("val:{db_idx}:{i}"));
                let entry = db.get(&key).expect("key should exist");
                assert_eq!(entry.as_string(), Some(&expected));
            }
        }
    }

    #[test]
    fn move_deadlock_freedom() {
        use std::sync::Arc;

        let state = Arc::new(ServerState::with_default_dbs());

        // Seed DB 0 and DB 1
        state.db_mut(0).insert(
            Bytes::from("a"),
            StoredValue::string(Bytes::from("0"), None),
        );
        state.db_mut(1).insert(
            Bytes::from("b"),
            StoredValue::string(Bytes::from("1"), None),
        );

        // Concurrent MOVE-like: thread 1 locks (0,1), thread 2 locks (1,0)
        // ascending order prevents deadlock
        let s1 = Arc::clone(&state);
        let s2 = Arc::clone(&state);

        let h1 = std::thread::spawn(move || {
            for _ in 0..100 {
                let (mut a, mut b) = s1.data.write_two_dbs(0, 1);
                // Move key from DB 0 → DB 1
                if let Some(v) = a.data.remove(&Bytes::from("a")) {
                    b.data.insert(Bytes::from("a"), v);
                }
                // Move it back
                if let Some(v) = b.data.remove(&Bytes::from("a")) {
                    a.data.insert(Bytes::from("a"), v);
                }
            }
        });
        let h2 = std::thread::spawn(move || {
            for _ in 0..100 {
                let (mut a, mut b) = s2.data.write_two_dbs(1, 0);
                if let Some(v) = a.data.remove(&Bytes::from("b")) {
                    b.data.insert(Bytes::from("b"), v);
                }
                if let Some(v) = b.data.remove(&Bytes::from("b")) {
                    a.data.insert(Bytes::from("b"), v);
                }
            }
        });

        h1.join().expect("thread 1 panicked");
        h2.join().expect("thread 2 panicked");
    }

    #[test]
    fn snapshot_does_not_block_writes() {
        use std::sync::{
            Arc,
            atomic::{AtomicBool, Ordering},
        };

        let state = Arc::new(ServerState::with_default_dbs());

        // Seed some data
        for i in 0..100 {
            state.db_mut(0).insert(
                Bytes::from(format!("k:{i}")),
                StoredValue::string(Bytes::from("v"), None),
            );
        }

        let write_done = Arc::new(AtomicBool::new(false));

        let s = Arc::clone(&state);
        let wd = Arc::clone(&write_done);
        let writer = std::thread::spawn(move || {
            for i in 100..200 {
                s.db_mut(0).insert(
                    Bytes::from(format!("k:{i}")),
                    StoredValue::string(Bytes::from("v"), None),
                );
            }
            wd.store(true, Ordering::Release);
        });

        // Snapshot while writer is running
        let snap = state.snapshot_dbs();
        writer.join().expect("writer panicked");

        // Snapshot should have captured some consistent state
        assert!(!snap[0].is_empty(), "snapshot should have captured data");
        assert!(write_done.load(Ordering::Acquire));
    }

    #[test]
    fn snapshot_per_db_consistency() {
        let state = ServerState::with_default_dbs();

        state.db_mut(0).insert(
            Bytes::from("x"),
            StoredValue::string(Bytes::from("before"), None),
        );

        let snap = state.snapshot_dbs();

        // Modify after snapshot
        state.db_mut(0).insert(
            Bytes::from("x"),
            StoredValue::string(Bytes::from("after"), None),
        );

        // Snapshot data should be unchanged
        let snapped = snap[0].get(&Bytes::from("x")).expect("key in snapshot");
        assert_eq!(snapped.as_string(), Some(&Bytes::from("before")));
    }

    #[test]
    fn swapdb_atomicity() {
        let state = ServerState::with_default_dbs();

        state.db_mut(0).insert(
            Bytes::from("a"),
            StoredValue::string(Bytes::from("val_a"), None),
        );
        state.db_mut(1).insert(
            Bytes::from("b"),
            StoredValue::string(Bytes::from("val_b"), None),
        );

        state.swap_dbs(0, 1);

        // DB 0 should now have key "b", DB 1 should have key "a"
        assert!(state.db(0).contains_key(&Bytes::from("b")));
        assert!(!state.db(0).contains_key(&Bytes::from("a")));
        assert!(state.db(1).contains_key(&Bytes::from("a")));
        assert!(!state.db(1).contains_key(&Bytes::from("b")));
    }

    #[test]
    fn flushall_under_concurrency() {
        use std::sync::Arc;

        let state = Arc::new(ServerState::with_default_dbs());

        // Seed multiple DBs
        for db_idx in 0..4 {
            for i in 0..50 {
                state.db_mut(db_idx).insert(
                    Bytes::from(format!("k:{i}")),
                    StoredValue::string(Bytes::from("v"), None),
                );
            }
        }

        let s = Arc::clone(&state);
        let writer = std::thread::spawn(move || {
            for i in 50..100 {
                s.db_mut(2).insert(
                    Bytes::from(format!("k:{i}")),
                    StoredValue::string(Bytes::from("v"), None),
                );
            }
        });

        state.clear_all_dbs();
        writer.join().expect("writer panicked");

        // After clear + writer, only DB 2 may have keys (from writer after clear)
        for db_idx in [0usize, 1, 3] {
            assert!(
                state.db(db_idx).is_empty(),
                "DB {db_idx} should be empty after FLUSHALL"
            );
        }
    }

    #[test]
    fn expiry_per_db_isolation() {
        let state = ServerState::with_default_dbs();

        // DB 0: expired key, DB 3: alive key
        state.db_mut(0).insert(
            Bytes::from("expired"),
            StoredValue::string(Bytes::from("v"), Some(1)),
        );
        state.db_mut(3).insert(
            Bytes::from("alive"),
            StoredValue::string(Bytes::from("v"), Some(i64::MAX)),
        );

        // Simulate expiry on DB 0 only
        {
            let mut db = state.db_mut(0);
            super::purge_expired_keys(&mut db, 1000);
        }

        assert!(
            state.db(0).is_empty(),
            "expired key should be purged from DB 0"
        );
        assert!(
            state.db(3).contains_key(&Bytes::from("alive")),
            "DB 3 key should be untouched"
        );
    }
}
