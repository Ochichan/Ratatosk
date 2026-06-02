use std::{
    collections::VecDeque,
    sync::atomic::{AtomicU64, Ordering as AtomicOrdering},
};

use bytes::Bytes;
use hashbrown::HashMap;
use ratatosk_core::time::now_sec as unix_sec_now;

use crate::security::sanitize_slowlog_argv;

#[derive(Debug, Clone)]
pub struct SlowlogEntry {
    pub id: i64,
    pub unix_time: i64,
    pub duration_us: i64,
    pub argv: Vec<Bytes>,
}

#[derive(Debug, Default)]
struct LatencyHistory {
    samples: VecDeque<(i64, i64)>,
    max_ms: i64,
}

#[derive(Debug)]
pub struct StatsState {
    total_commands_processed: u64,
    last_save_unix_sec: i64,
    slowlog_log_slower_than_us: i64,
    slowlog_max_len: usize,
    latency_tracking_enabled: bool,
    slowlog_entries: VecDeque<SlowlogEntry>,
    next_slowlog_id: i64,
    latency_events: HashMap<Bytes, LatencyHistory>,
    connected_clients: u64,
    total_connections_received: u64,
    total_net_input_bytes: u64,
    total_net_output_bytes: u64,
    evicted_keys: u64,
    expired_keys: u64,
    keyspace_hits: u64,
    keyspace_misses: u64,
    prev_commands_snapshot: u64,
    instantaneous_ops_per_sec: u64,
    cached_memory_estimate: u64,
    last_memory_estimate_tick: u64,
    memory_estimate_age_ticks: u64,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct HotStatsSnapshot {
    pub total_commands_processed: u64,
    pub connected_clients: u64,
    pub total_connections_received: u64,
    pub total_net_input_bytes: u64,
    pub total_net_output_bytes: u64,
    pub evicted_keys: u64,
    pub expired_keys: u64,
    pub keyspace_hits: u64,
    pub keyspace_misses: u64,
    pub instantaneous_ops_per_sec: u64,
    pub cached_memory_estimate: u64,
    pub memory_estimate_age_ticks: u64,
}

impl Default for StatsState {
    fn default() -> Self {
        Self {
            total_commands_processed: 0,
            last_save_unix_sec: unix_sec_now(),
            slowlog_log_slower_than_us: -1,
            slowlog_max_len: 128,
            latency_tracking_enabled: false,
            slowlog_entries: VecDeque::new(),
            next_slowlog_id: 0,
            latency_events: HashMap::new(),
            connected_clients: 0,
            total_connections_received: 0,
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
            memory_estimate_age_ticks: 0,
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
        self.total_connections_received = 0;
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

    pub fn total_connections_received(&self) -> u64 {
        self.total_connections_received
    }

    pub fn mark_client_connected(&mut self) {
        self.connected_clients = self.connected_clients.saturating_add(1);
        self.total_connections_received = self.total_connections_received.saturating_add(1);
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

    pub fn slowlog_tracking_enabled(&self) -> bool {
        self.slowlog_log_slower_than_us >= 0 && self.slowlog_max_len > 0
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
        if threshold < 0 || duration_us < threshold || self.slowlog_max_len == 0 {
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
        if !self.latency_tracking_enabled {
            return;
        }

        let len = event.len().min(32);
        let mut buf = [0u8; 32];
        for (i, &b) in event[..len].iter().enumerate() {
            buf[i] = b.to_ascii_lowercase();
        }
        let lower = &buf[..len];

        let now_sec = unix_sec_now();
        let sample_ms = latency_ms.max(0);
        let history = self
            .latency_events
            .raw_entry_mut()
            .from_key(lower)
            .or_insert_with(|| (Bytes::copy_from_slice(lower), LatencyHistory::default()))
            .1;
        if sample_ms == 0
            && history
                .samples
                .back()
                .is_some_and(|(last_ts, last_ms)| *last_ts == now_sec && *last_ms == 0)
        {
            return;
        }

        history.samples.push_back((now_sec, sample_ms));
        if sample_ms > history.max_ms {
            history.max_ms = sample_ms;
        }
        while history.samples.len() > 160 {
            let (_, evicted_ms) = history.samples.pop_front().unwrap();
            if evicted_ms == history.max_ms {
                history.max_ms = history.samples.iter().map(|(_, ms)| *ms).max().unwrap_or(0);
            }
        }
    }

    pub fn latency_tracking_enabled(&self) -> bool {
        self.latency_tracking_enabled
    }

    pub fn set_latency_tracking_enabled(&mut self, enabled: bool) {
        self.latency_tracking_enabled = enabled;
    }

    pub fn latency_latest(&self) -> Vec<(Bytes, i64, i64, i64)> {
        let mut out = Vec::new();
        for (event, history) in &self.latency_events {
            let Some((latest_ts, latest_ms)) = history.samples.back().copied() else {
                continue;
            };
            out.push((event.clone(), latest_ts, latest_ms, history.max_ms));
        }
        out.sort_by(|a, b| a.0.cmp(&b.0));
        out
    }

    pub fn latency_history(&self, event: &Bytes) -> Vec<(i64, i64)> {
        self.latency_events
            .get(event.as_ref())
            .map(|h| h.samples.iter().copied().collect())
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
        let mut names = self.latency_events.keys().cloned().collect::<Vec<_>>();
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

    /// Cron-stamped age (in cron ticks) of the cached memory estimate as of the
    /// last cron pass. Mirrors the `ratatosk_memory_estimate_age_ticks` Prometheus
    /// gauge so `INFO memory` and Prometheus report the same drift value.
    pub fn memory_estimate_age_ticks(&self) -> u64 {
        self.memory_estimate_age_ticks
    }

    pub fn set_memory_estimate_age_ticks(&mut self, age_ticks: u64) {
        self.memory_estimate_age_ticks = age_ticks;
    }

    pub fn hot_snapshot(&self) -> HotStatsSnapshot {
        HotStatsSnapshot {
            total_commands_processed: self.total_commands_processed,
            connected_clients: self.connected_clients,
            total_connections_received: self.total_connections_received,
            total_net_input_bytes: self.total_net_input_bytes,
            total_net_output_bytes: self.total_net_output_bytes,
            evicted_keys: self.evicted_keys,
            expired_keys: self.expired_keys,
            keyspace_hits: self.keyspace_hits,
            keyspace_misses: self.keyspace_misses,
            instantaneous_ops_per_sec: self.instantaneous_ops_per_sec,
            cached_memory_estimate: self.cached_memory_estimate,
            memory_estimate_age_ticks: self.memory_estimate_age_ticks,
        }
    }

    pub fn catch_up_from_hot_snapshot(&mut self, snapshot: HotStatsSnapshot) {
        self.total_commands_processed = self
            .total_commands_processed
            .max(snapshot.total_commands_processed);
        self.connected_clients = snapshot.connected_clients;
        self.total_connections_received = self
            .total_connections_received
            .max(snapshot.total_connections_received);
        self.total_net_input_bytes = self
            .total_net_input_bytes
            .max(snapshot.total_net_input_bytes);
        self.total_net_output_bytes = self
            .total_net_output_bytes
            .max(snapshot.total_net_output_bytes);
        self.evicted_keys = self.evicted_keys.max(snapshot.evicted_keys);
        self.expired_keys = self.expired_keys.max(snapshot.expired_keys);
        self.keyspace_hits = self.keyspace_hits.max(snapshot.keyspace_hits);
        self.keyspace_misses = self.keyspace_misses.max(snapshot.keyspace_misses);
        self.instantaneous_ops_per_sec = snapshot.instantaneous_ops_per_sec;
        self.cached_memory_estimate = snapshot.cached_memory_estimate;
        self.prev_commands_snapshot = self
            .prev_commands_snapshot
            .min(self.total_commands_processed);
    }

    pub fn catch_up_from_atomic(&mut self, atomic: &AtomicStatsState) {
        self.catch_up_from_hot_snapshot(atomic.hot_snapshot());
    }
}

#[derive(Debug)]
pub struct AtomicStatsState {
    total_commands_processed: AtomicU64,
    connected_clients: AtomicU64,
    total_connections_received: AtomicU64,
    total_net_input_bytes: AtomicU64,
    total_net_output_bytes: AtomicU64,
    evicted_keys: AtomicU64,
    expired_keys: AtomicU64,
    keyspace_hits: AtomicU64,
    keyspace_misses: AtomicU64,
    instantaneous_ops_per_sec: AtomicU64,
    cached_memory_estimate: AtomicU64,
}

impl Default for AtomicStatsState {
    fn default() -> Self {
        Self {
            total_commands_processed: AtomicU64::new(0),
            connected_clients: AtomicU64::new(0),
            total_connections_received: AtomicU64::new(0),
            total_net_input_bytes: AtomicU64::new(0),
            total_net_output_bytes: AtomicU64::new(0),
            evicted_keys: AtomicU64::new(0),
            expired_keys: AtomicU64::new(0),
            keyspace_hits: AtomicU64::new(0),
            keyspace_misses: AtomicU64::new(0),
            instantaneous_ops_per_sec: AtomicU64::new(0),
            cached_memory_estimate: AtomicU64::new(0),
        }
    }
}

impl AtomicStatsState {
    fn catch_up_counter(counter: &AtomicU64, target: u64) {
        let current = counter.load(AtomicOrdering::Relaxed);
        if target > current {
            counter.fetch_add(target - current, AtomicOrdering::Relaxed);
        }
    }

    pub fn from_stats(stats: &StatsState) -> Self {
        let atomic = Self::default();
        atomic
            .total_commands_processed
            .store(stats.total_commands_processed(), AtomicOrdering::Relaxed);
        atomic
            .connected_clients
            .store(stats.connected_clients(), AtomicOrdering::Relaxed);
        atomic
            .total_connections_received
            .store(stats.total_connections_received(), AtomicOrdering::Relaxed);
        atomic
            .total_net_input_bytes
            .store(stats.total_net_input_bytes(), AtomicOrdering::Relaxed);
        atomic
            .total_net_output_bytes
            .store(stats.total_net_output_bytes(), AtomicOrdering::Relaxed);
        atomic
    }

    pub fn mark_command_processed(&self) {
        self.total_commands_processed
            .fetch_add(1, AtomicOrdering::Relaxed);
    }

    pub fn total_commands_processed(&self) -> u64 {
        self.total_commands_processed.load(AtomicOrdering::Relaxed)
    }

    pub fn adjust_commands_processed_by(&self, overcounted: u64) {
        let _ = self.total_commands_processed.fetch_update(
            AtomicOrdering::Relaxed,
            AtomicOrdering::Relaxed,
            |current| Some(current.saturating_sub(overcounted)),
        );
    }

    pub fn mark_client_connected(&self) {
        self.connected_clients.fetch_add(1, AtomicOrdering::Relaxed);
        self.total_connections_received
            .fetch_add(1, AtomicOrdering::Relaxed);
    }

    pub fn mark_client_disconnected(&self) {
        let _ = self.connected_clients.fetch_update(
            AtomicOrdering::Relaxed,
            AtomicOrdering::Relaxed,
            |current| Some(current.saturating_sub(1)),
        );
    }

    pub fn connected_clients(&self) -> u64 {
        self.connected_clients.load(AtomicOrdering::Relaxed)
    }

    pub fn total_connections_received(&self) -> u64 {
        self.total_connections_received
            .load(AtomicOrdering::Relaxed)
    }

    pub fn add_net_input_bytes(&self, bytes: u64) {
        self.total_net_input_bytes
            .fetch_add(bytes, AtomicOrdering::Relaxed);
    }

    pub fn total_net_input_bytes(&self) -> u64 {
        self.total_net_input_bytes.load(AtomicOrdering::Relaxed)
    }

    pub fn add_net_output_bytes(&self, bytes: u64) {
        self.total_net_output_bytes
            .fetch_add(bytes, AtomicOrdering::Relaxed);
    }

    pub fn total_net_output_bytes(&self) -> u64 {
        self.total_net_output_bytes.load(AtomicOrdering::Relaxed)
    }

    pub fn add_evicted_keys(&self, count: u64) {
        self.evicted_keys.fetch_add(count, AtomicOrdering::Relaxed);
    }

    pub fn evicted_keys(&self) -> u64 {
        self.evicted_keys.load(AtomicOrdering::Relaxed)
    }

    pub fn add_expired_keys(&self, count: u64) {
        self.expired_keys.fetch_add(count, AtomicOrdering::Relaxed);
    }

    pub fn expired_keys(&self) -> u64 {
        self.expired_keys.load(AtomicOrdering::Relaxed)
    }

    pub fn mark_keyspace_hit(&self) {
        self.keyspace_hits.fetch_add(1, AtomicOrdering::Relaxed);
    }

    pub fn add_keyspace_hits(&self, count: u64) {
        self.keyspace_hits.fetch_add(count, AtomicOrdering::Relaxed);
    }

    pub fn keyspace_hits(&self) -> u64 {
        self.keyspace_hits.load(AtomicOrdering::Relaxed)
    }

    pub fn mark_keyspace_miss(&self) {
        self.keyspace_misses.fetch_add(1, AtomicOrdering::Relaxed);
    }

    pub fn add_keyspace_misses(&self, count: u64) {
        self.keyspace_misses
            .fetch_add(count, AtomicOrdering::Relaxed);
    }

    pub fn keyspace_misses(&self) -> u64 {
        self.keyspace_misses.load(AtomicOrdering::Relaxed)
    }

    pub fn set_instantaneous_ops_per_sec(&self, value: u64) {
        self.instantaneous_ops_per_sec
            .store(value, AtomicOrdering::Relaxed);
    }

    pub fn instantaneous_ops_per_sec(&self) -> u64 {
        self.instantaneous_ops_per_sec.load(AtomicOrdering::Relaxed)
    }

    pub fn set_cached_memory_estimate(&self, estimate: u64) {
        self.cached_memory_estimate
            .store(estimate, AtomicOrdering::Relaxed);
    }

    pub fn cached_memory_estimate(&self) -> u64 {
        self.cached_memory_estimate.load(AtomicOrdering::Relaxed)
    }

    pub fn hot_snapshot(&self) -> HotStatsSnapshot {
        HotStatsSnapshot {
            total_commands_processed: self.total_commands_processed(),
            connected_clients: self.connected_clients(),
            total_connections_received: self.total_connections_received(),
            total_net_input_bytes: self.total_net_input_bytes(),
            total_net_output_bytes: self.total_net_output_bytes(),
            evicted_keys: self.evicted_keys(),
            expired_keys: self.expired_keys(),
            keyspace_hits: self.keyspace_hits(),
            keyspace_misses: self.keyspace_misses(),
            instantaneous_ops_per_sec: self.instantaneous_ops_per_sec(),
            cached_memory_estimate: self.cached_memory_estimate(),
            // Age is stamped by the cron on the inner StatsState only; the atomic
            // fast-path side does not track it. merged() takes it from inner.
            memory_estimate_age_ticks: 0,
        }
    }

    pub fn catch_up_from_stats(&self, stats: &StatsState) {
        Self::catch_up_counter(
            &self.total_commands_processed,
            stats.total_commands_processed(),
        );
        Self::catch_up_counter(
            &self.total_connections_received,
            stats.total_connections_received(),
        );
        Self::catch_up_counter(&self.total_net_input_bytes, stats.total_net_input_bytes());
        Self::catch_up_counter(&self.total_net_output_bytes, stats.total_net_output_bytes());
        Self::catch_up_counter(&self.evicted_keys, stats.evicted_keys());
        Self::catch_up_counter(&self.expired_keys, stats.expired_keys());
        Self::catch_up_counter(&self.keyspace_hits, stats.keyspace_hits());
        Self::catch_up_counter(&self.keyspace_misses, stats.keyspace_misses());
        self.instantaneous_ops_per_sec
            .store(stats.instantaneous_ops_per_sec(), AtomicOrdering::Relaxed);
        self.cached_memory_estimate
            .store(stats.cached_memory_estimate(), AtomicOrdering::Relaxed);
    }

    pub fn reset(&self) {
        self.total_commands_processed
            .store(0, AtomicOrdering::Relaxed);
        self.connected_clients.store(0, AtomicOrdering::Relaxed);
        self.total_connections_received
            .store(0, AtomicOrdering::Relaxed);
        self.total_net_input_bytes.store(0, AtomicOrdering::Relaxed);
        self.total_net_output_bytes
            .store(0, AtomicOrdering::Relaxed);
        self.evicted_keys.store(0, AtomicOrdering::Relaxed);
        self.expired_keys.store(0, AtomicOrdering::Relaxed);
        self.keyspace_hits.store(0, AtomicOrdering::Relaxed);
        self.keyspace_misses.store(0, AtomicOrdering::Relaxed);
        self.instantaneous_ops_per_sec
            .store(0, AtomicOrdering::Relaxed);
        self.cached_memory_estimate
            .store(0, AtomicOrdering::Relaxed);
    }
}

impl HotStatsSnapshot {
    pub fn merged(inner: &StatsState, atomic: Option<&AtomicStatsState>) -> Self {
        let inner = inner.hot_snapshot();
        let Some(atomic) = atomic else {
            return inner;
        };
        let atomic = atomic.hot_snapshot();

        Self {
            total_commands_processed: inner
                .total_commands_processed
                .max(atomic.total_commands_processed),
            connected_clients: inner.connected_clients.max(atomic.connected_clients),
            total_connections_received: inner
                .total_connections_received
                .max(atomic.total_connections_received),
            total_net_input_bytes: inner
                .total_net_input_bytes
                .max(atomic.total_net_input_bytes),
            total_net_output_bytes: inner
                .total_net_output_bytes
                .max(atomic.total_net_output_bytes),
            evicted_keys: inner.evicted_keys.max(atomic.evicted_keys),
            expired_keys: inner.expired_keys.max(atomic.expired_keys),
            keyspace_hits: inner.keyspace_hits.max(atomic.keyspace_hits),
            keyspace_misses: inner.keyspace_misses.max(atomic.keyspace_misses),
            instantaneous_ops_per_sec: if atomic.instantaneous_ops_per_sec > 0
                || inner.instantaneous_ops_per_sec == 0
            {
                atomic.instantaneous_ops_per_sec
            } else {
                inner.instantaneous_ops_per_sec
            },
            cached_memory_estimate: if atomic.cached_memory_estimate > 0
                || inner.cached_memory_estimate == 0
            {
                atomic.cached_memory_estimate
            } else {
                inner.cached_memory_estimate
            },
            // Cron stamps this on the inner StatsState only; the atomic side is 0.
            memory_estimate_age_ticks: inner.memory_estimate_age_ticks,
        }
    }
}
