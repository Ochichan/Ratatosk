use bytes::Bytes;
use rand::Rng;

use crate::keyspace::{ServerState, StoredValue, ValueData};

// ---------------------------------------------------------------------------
// EvictionPolicy — the 8 Redis maxmemory policies
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EvictionPolicy {
    NoEviction,
    AllKeysLru,
    VolatileLru,
    AllKeysLfu,
    VolatileLfu,
    AllKeysRandom,
    VolatileRandom,
    VolatileTtl,
}

impl EvictionPolicy {
    /// Parse from a Redis CONFIG string value.
    pub fn from_config_str(s: &[u8]) -> Option<Self> {
        match s {
            b"noeviction" => Some(Self::NoEviction),
            b"allkeys-lru" => Some(Self::AllKeysLru),
            b"volatile-lru" => Some(Self::VolatileLru),
            b"allkeys-lfu" => Some(Self::AllKeysLfu),
            b"volatile-lfu" => Some(Self::VolatileLfu),
            b"allkeys-random" => Some(Self::AllKeysRandom),
            b"volatile-random" => Some(Self::VolatileRandom),
            b"volatile-ttl" => Some(Self::VolatileTtl),
            _ => None,
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::NoEviction => "noeviction",
            Self::AllKeysLru => "allkeys-lru",
            Self::VolatileLru => "volatile-lru",
            Self::AllKeysLfu => "allkeys-lfu",
            Self::VolatileLfu => "volatile-lfu",
            Self::AllKeysRandom => "allkeys-random",
            Self::VolatileRandom => "volatile-random",
            Self::VolatileTtl => "volatile-ttl",
        }
    }

    fn is_volatile(self) -> bool {
        matches!(
            self,
            Self::VolatileLru
                | Self::VolatileLfu
                | Self::VolatileRandom
                | Self::VolatileTtl
        )
    }
}

// ---------------------------------------------------------------------------
// EvictionConfig
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
pub struct EvictionConfig {
    pub policy: EvictionPolicy,
    /// Maximum memory in bytes. 0 means unlimited.
    pub maxmemory: usize,
    /// Number of random samples per eviction attempt.
    pub maxmemory_samples: usize,
}

impl Default for EvictionConfig {
    fn default() -> Self {
        Self {
            policy: EvictionPolicy::NoEviction,
            maxmemory: 0,
            maxmemory_samples: 5,
        }
    }
}

// ---------------------------------------------------------------------------
// LRU clock management
// ---------------------------------------------------------------------------

/// 24-bit wrapping LRU clock in seconds.
const LRU_CLOCK_RESOLUTION: u32 = 1;
const LRU_CLOCK_MAX: u32 = (1 << 24) - 1;

/// Compute current LRU clock value from wall-clock seconds.
pub fn lru_clock(now_sec: u32) -> u32 {
    (now_sec / LRU_CLOCK_RESOLUTION) & LRU_CLOCK_MAX
}

/// Estimate idle time in seconds from object LRU clock.
pub fn estimate_idle_time(object_clock: u32, server_clock: u32) -> u32 {
    if server_clock >= object_clock {
        (server_clock - object_clock) * LRU_CLOCK_RESOLUTION
    } else {
        // Clock wrapped around
        (LRU_CLOCK_MAX - object_clock + server_clock) * LRU_CLOCK_RESOLUTION
    }
}

/// Touch (update) the LRU clock on a value.
pub fn touch_lru_clock(value: &mut StoredValue, now_sec: u32) {
    value.lru_clock = lru_clock(now_sec);
}

// ---------------------------------------------------------------------------
// Memory estimation
// ---------------------------------------------------------------------------

/// Estimate the heap memory used by a single key-value pair.
///
/// This is an approximation — it accounts for the key, the value data,
/// and the per-entry overhead of HashMap.
pub fn estimate_object_memory(key: &Bytes, value: &StoredValue) -> usize {
    // HashMap entry overhead (approx 64 bytes per slot on 64-bit)
    const ENTRY_OVERHEAD: usize = 64;

    let key_size = key.len();
    let value_size = match &value.data {
        ValueData::String(s) => s.len(),
        ValueData::List(l) => {
            let elem_overhead = l.len() * 24; // Bytes = 24 bytes on stack
            let content: usize = l.iter().map(|b| b.len()).sum();
            elem_overhead + content
        }
        ValueData::Hash(h) => {
            let per_field = 64; // HashMap entry overhead
            h.iter()
                .map(|(k, v)| k.len() + v.value.len() + per_field)
                .sum()
        }
        ValueData::Set(s) => {
            let per_elem = 48; // HashSet entry overhead
            s.iter().map(|b| b.len() + per_elem).sum()
        }
        ValueData::SortedSet(z) => {
            // BTreeMap + HashMap dual indexing
            let per_entry = 128;
            z.len() * per_entry
        }
        ValueData::Stream { entries, groups } => {
            let entries_size: usize = entries
                .iter()
                .map(|e| {
                    32 + e.fields.iter().map(|(k, v)| k.len() + v.len() + 48).sum::<usize>()
                })
                .sum();
            let groups_size = groups.len() * 256;
            entries_size + groups_size
        }
    };

    ENTRY_OVERHEAD + key_size + value_size + std::mem::size_of::<StoredValue>()
}

// ---------------------------------------------------------------------------
// Eviction execution
// ---------------------------------------------------------------------------

/// Check whether eviction is needed based on current memory usage.
pub fn needs_eviction(used_memory: usize, config: &EvictionConfig) -> bool {
    config.maxmemory > 0 && used_memory > config.maxmemory
}

/// Estimate total memory used across all databases.
pub fn estimate_used_memory(state: &ServerState) -> usize {
    let mut total = 0usize;
    for db_idx in 0..state.db_count() {
        let db = state.db(db_idx);
        for (key, value) in db.iter() {
            total = total.saturating_add(estimate_object_memory(key, value));
        }
    }
    total
}

/// Perform eviction until memory drops below maxmemory.
///
/// Returns the number of keys evicted.
pub fn perform_eviction(state: &mut ServerState, config: &EvictionConfig) -> usize {
    if config.policy == EvictionPolicy::NoEviction || config.maxmemory == 0 {
        return 0;
    }

    let mut evicted = 0usize;
    let mut rng = rand::thread_rng();

    // Try up to 128 rounds to get below maxmemory
    for _ in 0..128 {
        let used = estimate_used_memory(state);
        if used <= config.maxmemory {
            break;
        }

        let best = select_eviction_candidate(state, config, &mut rng);
        let Some((db_idx, key)) = best else {
            break;
        };

        state.db_mut(db_idx).remove(&key);
        state.touch_key_version(db_idx, &key);
        evicted += 1;
    }

    evicted
}

/// Select the best eviction candidate via sampling.
fn select_eviction_candidate(
    state: &ServerState,
    config: &EvictionConfig,
    rng: &mut impl Rng,
) -> Option<(usize, Bytes)> {
    let samples = config.maxmemory_samples;
    let mut best_key: Option<(usize, Bytes)> = None;
    let mut best_score: u64 = 0; // higher = better candidate for eviction

    for db_idx in 0..state.db_count() {
        let db = state.db(db_idx);
        if db.is_empty() {
            continue;
        }

        let keys: Vec<&Bytes> = db.keys().collect();
        if keys.is_empty() {
            continue;
        }

        for _ in 0..samples {
            let idx = rng.gen_range(0..keys.len());
            let key = keys[idx];

            let Some(value) = db.get(key) else {
                continue;
            };

            // For volatile policies, skip keys without TTL
            if config.policy.is_volatile() && value.expire_at_ms.is_none() {
                continue;
            }

            let score = eviction_score(value, &config.policy);
            if best_key.is_none() || score > best_score {
                best_score = score;
                best_key = Some((db_idx, key.clone()));
            }
        }
    }

    best_key
}

/// Compute an eviction score for a value. Higher = more eligible for eviction.
fn eviction_score(value: &StoredValue, policy: &EvictionPolicy) -> u64 {
    match policy {
        EvictionPolicy::NoEviction => 0,
        EvictionPolicy::AllKeysLru | EvictionPolicy::VolatileLru => {
            // Higher idle time = more evictable
            // Use lru_clock directly — lower clock = older = higher score
            let clock = value.lru_clock;
            u64::from(LRU_CLOCK_MAX.wrapping_sub(clock))
        }
        EvictionPolicy::AllKeysLfu | EvictionPolicy::VolatileLfu => {
            // Lower frequency = more evictable → invert
            // lru_clock repurposed as LFU counter
            let counter = value.lru_clock as u64;
            u64::MAX.saturating_sub(counter)
        }
        EvictionPolicy::AllKeysRandom | EvictionPolicy::VolatileRandom => {
            // All keys equally eligible
            1
        }
        EvictionPolicy::VolatileTtl => {
            // Closer TTL = more evictable (lower expire_at_ms = higher score)
            match value.expire_at_ms {
                Some(ttl) => u64::MAX.saturating_sub(ttl as u64),
                None => 0,
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use hashbrown::HashMap;
    use std::collections::VecDeque;

    use super::*;
    use crate::keyspace::{HashFieldEntry, ServerState, StoredValue};

    fn make_server_with_keys(count: usize) -> ServerState {
        let mut state = ServerState::with_default_dbs();
        for i in 0..count {
            let key = Bytes::from(format!("key:{i}"));
            let val = StoredValue::string(Bytes::from(format!("val:{i}")), None);
            state.db_mut(0).insert(key, val);
        }
        state
    }

    #[test]
    fn no_eviction_policy_never_evicts() {
        let mut state = make_server_with_keys(100);
        let config = EvictionConfig {
            policy: EvictionPolicy::NoEviction,
            maxmemory: 1, // very low
            maxmemory_samples: 5,
        };
        let evicted = perform_eviction(&mut state, &config);
        assert_eq!(evicted, 0);
    }

    #[test]
    fn allkeys_random_evicts_until_below_threshold() {
        let mut state = make_server_with_keys(50);
        let used_before = estimate_used_memory(&state);
        let config = EvictionConfig {
            policy: EvictionPolicy::AllKeysRandom,
            maxmemory: used_before / 2,
            maxmemory_samples: 10,
        };

        let evicted = perform_eviction(&mut state, &config);
        assert!(evicted > 0, "should evict at least one key");

        let used_after = estimate_used_memory(&state);
        assert!(
            used_after <= config.maxmemory,
            "used {used_after} should be <= maxmemory {}",
            config.maxmemory
        );
    }

    #[test]
    fn volatile_random_skips_keys_without_ttl() {
        let mut state = ServerState::with_default_dbs();
        // Add keys without TTL
        for i in 0..20 {
            let key = Bytes::from(format!("persist:{i}"));
            state
                .db_mut(0)
                .insert(key, StoredValue::string(Bytes::from("v"), None));
        }
        // Add keys with TTL
        for i in 0..5 {
            let key = Bytes::from(format!("volatile:{i}"));
            state.db_mut(0).insert(
                key,
                StoredValue::string(Bytes::from("v"), Some(999_999_999_999)),
            );
        }

        let config = EvictionConfig {
            policy: EvictionPolicy::VolatileRandom,
            maxmemory: 1,
            maxmemory_samples: 20,
        };

        let evicted = perform_eviction(&mut state, &config);
        assert!(evicted > 0);

        // All persisted keys should remain
        for i in 0..20 {
            let key = Bytes::from(format!("persist:{i}"));
            assert!(
                state.db(0).contains_key(&key),
                "persistent key {key:?} should not be evicted"
            );
        }
    }

    #[test]
    fn volatile_ttl_prefers_closest_expiry() {
        let mut state = ServerState::with_default_dbs();
        state.db_mut(0).insert(
            Bytes::from("far"),
            StoredValue::string(Bytes::from("v"), Some(999_999_999_999)),
        );
        state.db_mut(0).insert(
            Bytes::from("near"),
            StoredValue::string(Bytes::from("v"), Some(1)),
        );

        let config = EvictionConfig {
            policy: EvictionPolicy::VolatileTtl,
            maxmemory: 1,
            maxmemory_samples: 10,
        };

        perform_eviction(&mut state, &config);
        // The "near" key with TTL=1 should be evicted first
        assert!(
            !state.db(0).contains_key(&Bytes::from("near")),
            "near-expiry key should be evicted"
        );
    }

    #[test]
    fn lru_clock_wraps_correctly() {
        assert_eq!(lru_clock(0), 0);
        assert_eq!(lru_clock(LRU_CLOCK_MAX), LRU_CLOCK_MAX);
        // Beyond 24-bit wraps
        assert_eq!(lru_clock(LRU_CLOCK_MAX + 1), 0);
    }

    #[test]
    fn idle_time_handles_wrap() {
        let idle = estimate_idle_time(100, 200);
        assert_eq!(idle, 100);

        // Wrap-around: (MAX - (MAX-10) + 5) = 15
        let idle_wrap = estimate_idle_time(LRU_CLOCK_MAX - 10, 5);
        assert_eq!(idle_wrap, 15);
    }

    #[test]
    fn estimate_memory_accounts_for_different_types() {
        let string_mem = estimate_object_memory(
            &Bytes::from("k"),
            &StoredValue::string(Bytes::from("hello"), None),
        );
        let list_mem = estimate_object_memory(
            &Bytes::from("k"),
            &StoredValue::list(
                VecDeque::from(vec![Bytes::from("a"), Bytes::from("b")]),
                None,
            ),
        );
        let hash_mem = {
            let mut fields = HashMap::new();
            fields.insert(Bytes::from("f1"), HashFieldEntry::new(Bytes::from("v1")));
            estimate_object_memory(&Bytes::from("k"), &StoredValue::hash(fields, None))
        };

        // All should be positive and reasonable
        assert!(string_mem > 0);
        assert!(list_mem > string_mem);
        assert!(hash_mem > 0);
    }

    #[test]
    fn needs_eviction_respects_zero_maxmemory() {
        let config = EvictionConfig {
            maxmemory: 0,
            ..EvictionConfig::default()
        };
        assert!(!needs_eviction(999_999, &config));
    }

    #[test]
    fn needs_eviction_triggers_above_threshold() {
        let config = EvictionConfig {
            maxmemory: 1000,
            ..EvictionConfig::default()
        };
        assert!(!needs_eviction(999, &config));
        assert!(needs_eviction(1001, &config));
    }

    #[test]
    fn eviction_policy_from_config_str() {
        assert_eq!(
            EvictionPolicy::from_config_str(b"allkeys-lru"),
            Some(EvictionPolicy::AllKeysLru)
        );
        assert_eq!(
            EvictionPolicy::from_config_str(b"volatile-ttl"),
            Some(EvictionPolicy::VolatileTtl)
        );
        assert_eq!(EvictionPolicy::from_config_str(b"unknown"), None);
    }

    #[test]
    fn eviction_policy_roundtrip() {
        let policies = [
            EvictionPolicy::NoEviction,
            EvictionPolicy::AllKeysLru,
            EvictionPolicy::VolatileLru,
            EvictionPolicy::AllKeysLfu,
            EvictionPolicy::VolatileLfu,
            EvictionPolicy::AllKeysRandom,
            EvictionPolicy::VolatileRandom,
            EvictionPolicy::VolatileTtl,
        ];
        for policy in policies {
            let s = policy.as_str();
            let parsed = EvictionPolicy::from_config_str(s.as_bytes());
            assert_eq!(parsed, Some(policy), "roundtrip failed for {s}");
        }
    }
}
