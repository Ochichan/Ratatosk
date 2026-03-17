use std::sync::atomic::{AtomicI64, Ordering};

use bytes::Bytes;
use rand::Rng;
use ratatosk_core::time::now_ms;

use crate::keyspace::ServerState;

static LAST_WALL_CLOCK_MS: AtomicI64 = AtomicI64::new(0);

pub fn detect_clock_jump() {
    let current = now_ms();
    let previous = LAST_WALL_CLOCK_MS.swap(current, Ordering::Relaxed);

    if previous > 0 {
        let delta = current - previous;
        if delta < -1000 {
            metrics::counter!("ratatosk_clock_jumps_total", "direction" => "backward").increment(1);
            tracing::warn!(
                target = "ratatosk::time",
                delta_ms = delta,
                "wall-clock jumped backward (NTP correction or manual adjustment); expiry deadlines may be affected"
            );
        } else if delta > 5000 {
            metrics::counter!("ratatosk_clock_jumps_total", "direction" => "forward").increment(1);
            tracing::warn!(
                target = "ratatosk::time",
                delta_ms = delta,
                "wall-clock jumped forward; immediate expiry may occur"
            );
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExpireCondition {
    None,
    Nx,
    Xx,
    Gt,
    Lt,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SetExpirePolicy {
    None,
    KeepTtl,
    AtMs(i64),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExpireMode {
    RelativeSeconds,
    RelativeMilliseconds,
    AbsoluteSeconds,
    AbsoluteMilliseconds,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TtlMode {
    Seconds,
    Milliseconds,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExpireTimeMode {
    Seconds,
    Milliseconds,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GetExPolicy {
    KeepTtl,
    Persist,
    AtMs(i64),
}

pub fn parse_expire_condition(raw: &Bytes) -> Option<ExpireCondition> {
    if raw.eq_ignore_ascii_case(b"NX") {
        Some(ExpireCondition::Nx)
    } else if raw.eq_ignore_ascii_case(b"XX") {
        Some(ExpireCondition::Xx)
    } else if raw.eq_ignore_ascii_case(b"GT") {
        Some(ExpireCondition::Gt)
    } else if raw.eq_ignore_ascii_case(b"LT") {
        Some(ExpireCondition::Lt)
    } else {
        None
    }
}

pub fn to_expire_target_ms(raw: i64, mode: ExpireMode, now_ms: i64) -> i64 {
    match mode {
        ExpireMode::RelativeSeconds => now_ms.saturating_add(raw.saturating_mul(1000)),
        ExpireMode::RelativeMilliseconds => now_ms.saturating_add(raw),
        ExpireMode::AbsoluteSeconds => raw.saturating_mul(1000),
        ExpireMode::AbsoluteMilliseconds => raw,
    }
}

pub fn expire_condition_matches(
    condition: ExpireCondition,
    current: Option<i64>,
    target: i64,
) -> bool {
    match condition {
        ExpireCondition::None => true,
        ExpireCondition::Nx => current.is_none(),
        ExpireCondition::Xx => current.is_some(),
        ExpireCondition::Gt => current.is_some_and(|cur| target > cur),
        ExpireCondition::Lt => current.is_none_or(|cur| target < cur),
    }
}

// ---------------------------------------------------------------------------
// Active expiry — sampling-based periodic cleanup (called by server_cron)
// ---------------------------------------------------------------------------

/// Run one cycle of active expiry across all databases.
///
/// Samples random keys with TTL set, removes those that have expired.
/// The number of keys to sample and the early-stop threshold are
/// configurable via `ConfigState::active_expire_cycle_lookups` (default 20)
/// and `ConfigState::active_expire_cycle_threshold_pct` (default 25%).
/// Returns the total number of keys expired across all databases.
pub fn active_expire_cycle(state: &mut ServerState, now_ms: i64) -> usize {
    let cycle_lookups = state.config.active_expire_cycle_lookups();
    let cycle_threshold = state.config.active_expire_cycle_threshold_pct() as f64 / 100.0;

    let mut total_expired = 0usize;
    let mut rng = rand::thread_rng();

    for db_idx in 0..state.db_count() {
        // Sampling phase: hold read guard, collect candidate keys, then release.
        let (samples_to_take, sampled_keys) = {
            let db = state.db(db_idx);
            if db.is_empty() {
                continue;
            }

            // First pass: count volatile keys without allocating
            let volatile_count = db.iter().filter(|(_, v)| v.expire_at_ms.is_some()).count();

            if volatile_count == 0 {
                continue;
            }

            // Use reservoir sampling (Algorithm R) to select random indices in O(k) space
            let samples_to_take = cycle_lookups.min(volatile_count);
            let mut sample_indices: Vec<usize> = (0..samples_to_take).collect();
            for i in samples_to_take..volatile_count {
                let j = rng.gen_range(0..i + 1);
                if j < samples_to_take {
                    sample_indices[j] = i;
                }
            }
            sample_indices.sort_unstable();

            // Second pass: collect only sampled keys (small, bounded allocation)
            let mut sampled_keys: Vec<Bytes> = Vec::with_capacity(samples_to_take);
            let mut volatile_idx = 0;
            let mut sample_cursor = 0;
            for (key, value) in db.iter() {
                if value.expire_at_ms.is_none() {
                    continue;
                }
                if sample_cursor < sample_indices.len()
                    && volatile_idx == sample_indices[sample_cursor]
                {
                    sampled_keys.push(key.clone());
                    sample_cursor += 1;
                    if sample_cursor >= sample_indices.len() {
                        break;
                    }
                }
                volatile_idx += 1;
            }

            (samples_to_take, sampled_keys)
        }; // read guard dropped here

        // Expiry phase: acquire write guard per key check+removal.
        let mut expired = 0usize;
        for key in sampled_keys {
            let mut db = state.db_mut(db_idx);
            let is_expired = db
                .get(key.as_ref())
                .is_some_and(|v| v.expire_at_ms.is_some_and(|at| at <= now_ms));

            if is_expired {
                db.remove(key.as_ref());
                drop(db); // release write guard before touch_key_version
                state.touch_key_version(db_idx, key);
                expired += 1;
            }
        }

        total_expired += expired;

        // Stop early if few keys are expiring (save CPU)
        let sampled = samples_to_take;
        if sampled > 0 && (expired as f64 / sampled as f64) < cycle_threshold {
            continue;
        }
    }

    state.stats.add_expired_keys(total_expired as u64);
    total_expired
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::keyspace::StoredValue;

    #[test]
    fn active_expire_cycle_removes_expired_keys() {
        let mut state = ServerState::with_default_dbs();
        let now_ms = 10_000i64;

        // Add keys: some expired, some not
        for i in 0..10 {
            let key = Bytes::from(format!("expired:{i}"));
            state.db_mut(0).insert(
                key,
                StoredValue::string(Bytes::from("v"), Some(now_ms - 1000)),
            );
        }
        for i in 0..10 {
            let key = Bytes::from(format!("alive:{i}"));
            state.db_mut(0).insert(
                key,
                StoredValue::string(Bytes::from("v"), Some(now_ms + 10_000)),
            );
        }
        // Keys without TTL
        for i in 0..5 {
            let key = Bytes::from(format!("persist:{i}"));
            state
                .db_mut(0)
                .insert(key, StoredValue::string(Bytes::from("v"), None));
        }

        // Run multiple cycles to ensure all expired keys are sampled
        let mut total_expired = 0;
        for _ in 0..20 {
            total_expired += active_expire_cycle(&mut state, now_ms);
        }
        assert!(total_expired > 0, "should expire at least some keys");

        // Verify expired keys are gone after multiple cycles
        for i in 0..10 {
            let key = Bytes::from(format!("expired:{i}"));
            assert!(
                !state.db(0).contains_key(&key),
                "expired key {key:?} should have been removed"
            );
        }

        // Verify alive keys remain
        for i in 0..10 {
            let key = Bytes::from(format!("alive:{i}"));
            assert!(
                state.db(0).contains_key(&key),
                "alive key {key:?} should still exist"
            );
        }

        // Verify persistent keys remain
        for i in 0..5 {
            let key = Bytes::from(format!("persist:{i}"));
            assert!(
                state.db(0).contains_key(&key),
                "persistent key {key:?} should still exist"
            );
        }
    }

    #[test]
    fn active_expire_cycle_empty_db_is_noop() {
        let mut state = ServerState::with_default_dbs();
        let expired = active_expire_cycle(&mut state, 999_999);
        assert_eq!(expired, 0);
    }

    #[test]
    fn active_expire_cycle_no_volatile_keys_is_noop() {
        let mut state = ServerState::with_default_dbs();
        for i in 0..10 {
            let key = Bytes::from(format!("key:{i}"));
            state
                .db_mut(0)
                .insert(key, StoredValue::string(Bytes::from("v"), None));
        }
        let expired = active_expire_cycle(&mut state, 999_999);
        assert_eq!(expired, 0);
    }
}
