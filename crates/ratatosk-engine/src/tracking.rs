use bytes::Bytes;
use hashbrown::{HashMap, HashSet};

#[derive(Debug, Clone, Default)]
pub struct ClientTrackingState {
    key_watchers: HashMap<(usize, Bytes), HashMap<i64, TrackingWatcher>>,
    tracker_keys: HashMap<i64, HashSet<(usize, Bytes)>>,
    broadcast_watchers: HashMap<i64, BroadcastWatcher>,
    broken_redirects: HashSet<i64>,
}

#[derive(Debug, Clone, Copy)]
struct TrackingWatcher {
    target_client_id: i64,
    no_loop: bool,
}

#[derive(Debug, Clone)]
struct BroadcastWatcher {
    target_client_id: i64,
    no_loop: bool,
    prefixes: Vec<Bytes>,
}

impl ClientTrackingState {
    /// True when any client tracks keys or prefixes, so writes must be
    /// reported through [`Self::invalidate_keys`].
    pub fn has_watchers(&self) -> bool {
        !self.key_watchers.is_empty() || !self.broadcast_watchers.is_empty()
    }

    #[allow(clippy::too_many_arguments)]
    pub fn track_key(
        &mut self,
        tracker_client_id: i64,
        target_client_id: i64,
        no_loop: bool,
        db_idx: usize,
        key: Bytes,
    ) {
        let tracked_key = (db_idx, key.clone());
        self.key_watchers
            .entry(tracked_key.clone())
            .or_default()
            .insert(
                tracker_client_id,
                TrackingWatcher {
                    target_client_id,
                    no_loop,
                },
            );
        self.tracker_keys
            .entry(tracker_client_id)
            .or_default()
            .insert(tracked_key);
    }

    pub fn configure_broadcast(
        &mut self,
        tracker_client_id: i64,
        target_client_id: i64,
        no_loop: bool,
        mut prefixes: Vec<Bytes>,
    ) {
        prefixes.sort();
        prefixes.dedup();
        self.broadcast_watchers.insert(
            tracker_client_id,
            BroadcastWatcher {
                target_client_id,
                no_loop,
                prefixes,
            },
        );
    }

    pub fn invalidate_keys<I>(
        &mut self,
        writer_client_id: i64,
        db_idx: usize,
        keys: I,
    ) -> HashMap<i64, Vec<Bytes>>
    where
        I: IntoIterator<Item = Bytes>,
    {
        let mut invalidations: HashMap<i64, Vec<Bytes>> = HashMap::new();

        for key in keys {
            let tracked_key = (db_idx, key.clone());
            if let Some(watchers) = self.key_watchers.remove(&tracked_key) {
                for (tracker_client_id, watcher) in watchers {
                    if !(watcher.no_loop && tracker_client_id == writer_client_id) {
                        invalidations
                            .entry(watcher.target_client_id)
                            .or_default()
                            .push(key.clone());
                    }

                    if let Some(keys) = self.tracker_keys.get_mut(&tracker_client_id) {
                        keys.remove(&tracked_key);
                        let should_remove_tracker = keys.is_empty();
                        if should_remove_tracker {
                            let _ = keys;
                            self.tracker_keys.remove(&tracker_client_id);
                        }
                    }
                }
            }

            for (tracker_client_id, watcher) in &self.broadcast_watchers {
                if watcher.no_loop && *tracker_client_id == writer_client_id {
                    continue;
                }
                if watcher.prefixes.is_empty()
                    || watcher
                        .prefixes
                        .iter()
                        .any(|prefix| key.starts_with(prefix))
                {
                    invalidations
                        .entry(watcher.target_client_id)
                        .or_default()
                        .push(key.clone());
                }
            }
        }

        for keys in invalidations.values_mut() {
            keys.sort();
            keys.dedup();
        }

        invalidations
    }

    pub fn clear_tracker(&mut self, tracker_client_id: i64) {
        self.broadcast_watchers.remove(&tracker_client_id);
        self.broken_redirects.remove(&tracker_client_id);

        if let Some(tracked_keys) = self.tracker_keys.remove(&tracker_client_id) {
            let mut empty_keys = Vec::new();
            for tracked_key in tracked_keys {
                if let Some(watchers) = self.key_watchers.get_mut(&tracked_key) {
                    watchers.remove(&tracker_client_id);
                    if watchers.is_empty() {
                        empty_keys.push(tracked_key);
                    }
                }
            }

            for tracked_key in empty_keys {
                self.key_watchers.remove(&tracked_key);
            }
        }
    }

    pub fn clear_target(&mut self, target_client_id: i64) -> Vec<i64> {
        let mut detached_trackers = HashSet::new();

        for (tracker_client_id, watcher) in &mut self.broadcast_watchers {
            if watcher.target_client_id == target_client_id {
                watcher.target_client_id = *tracker_client_id;
                detached_trackers.insert(*tracker_client_id);
            }
        }

        for watchers in self.key_watchers.values_mut() {
            for (tracker_client_id, watcher) in watchers {
                if watcher.target_client_id == target_client_id {
                    watcher.target_client_id = *tracker_client_id;
                    detached_trackers.insert(*tracker_client_id);
                }
            }
        }

        for tracker_client_id in &detached_trackers {
            self.broken_redirects.insert(*tracker_client_id);
        }

        let mut detached = detached_trackers.into_iter().collect::<Vec<_>>();
        detached.sort_unstable();
        detached
    }

    pub fn redirect_broken(&self, tracker_client_id: i64) -> bool {
        self.broken_redirects.contains(&tracker_client_id)
    }
}
