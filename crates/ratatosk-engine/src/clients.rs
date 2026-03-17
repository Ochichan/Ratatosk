use std::{collections::VecDeque, sync::Arc};

use bytes::Bytes;
use hashbrown::{HashMap, HashSet};
use tokio::sync::Notify;

#[derive(Debug, Clone, Default)]
pub struct ClientSnapshot {
    pub id: i64,
    pub addr: Bytes,
    pub laddr: Bytes,
    pub name: Option<Bytes>,
    pub age_seconds: i64,
    pub idle_seconds: i64,
    pub flags: Bytes,
    pub db: usize,
    pub sub: usize,
    pub psub: usize,
    pub ssub: usize,
    pub multi: i64,
    pub cmd: Bytes,
    pub user: Bytes,
    pub redir: i64,
    pub tracking_enabled: bool,
    pub resp: i64,
    pub lib_name: Option<Bytes>,
    pub lib_ver: Option<Bytes>,
}

#[derive(Debug, Clone, Default)]
pub struct ClientRegistry {
    clients: HashMap<i64, ClientSnapshot>,
    blocked_clients: HashSet<i64>,
}

#[derive(Debug, Default)]
pub struct BlockingState {
    key_waiters: HashMap<(usize, Bytes), VecDeque<i64>>,
    client_keys: HashMap<i64, HashSet<(usize, Bytes)>>,
    client_notifiers: HashMap<i64, Arc<Notify>>,
}

impl BlockingState {
    pub fn register(&mut self, client_id: i64, keys: Vec<(usize, Bytes)>) -> Arc<Notify> {
        self.clear(client_id);

        let notifier = self
            .client_notifiers
            .entry(client_id)
            .or_insert_with(|| Arc::new(Notify::new()))
            .clone();

        let key_set = keys.into_iter().collect::<HashSet<_>>();
        for key in &key_set {
            let waiters = self.key_waiters.entry(key.clone()).or_default();
            if !waiters.contains(&client_id) {
                waiters.push_back(client_id);
            }
        }
        if !key_set.is_empty() {
            self.client_keys.insert(client_id, key_set);
        }

        notifier
    }

    pub fn clear(&mut self, client_id: i64) {
        let Some(keys) = self.client_keys.remove(&client_id) else {
            return;
        };

        let mut empty_keys = Vec::new();
        for key in keys {
            if let Some(waiters) = self.key_waiters.get_mut(&key) {
                waiters.retain(|&id| id != client_id);
                if waiters.is_empty() {
                    empty_keys.push(key);
                }
            }
        }

        for key in empty_keys {
            self.key_waiters.remove(&key);
        }
    }

    pub fn remove_client(&mut self, client_id: i64) {
        self.clear(client_id);
        self.client_notifiers.remove(&client_id);
    }

    pub fn notify_keys<I>(&self, db_idx: usize, keys: I)
    where
        I: IntoIterator<Item = Bytes>,
    {
        let mut notified_clients = HashSet::new();
        for key in keys {
            if let Some(waiters) = self.key_waiters.get(&(db_idx, key)) {
                if let Some(&first) = waiters.front() {
                    notified_clients.insert(first);
                }
            }
        }

        for client_id in notified_clients {
            if let Some(notifier) = self.client_notifiers.get(&client_id) {
                notifier.notify_one();
            }
        }
    }
}

impl ClientRegistry {
    pub fn upsert(&mut self, snapshot: ClientSnapshot) {
        self.clients.insert(snapshot.id, snapshot);
    }

    pub fn remove(&mut self, client_id: i64) {
        self.clients.remove(&client_id);
        self.blocked_clients.remove(&client_id);
    }

    pub fn detach_redirect_target(&mut self, target_client_id: i64) {
        for snapshot in self.clients.values_mut() {
            if snapshot.redir == target_client_id {
                snapshot.redir = -1;
            }
        }
    }

    pub fn get(&self, client_id: i64) -> Option<&ClientSnapshot> {
        self.clients.get(&client_id)
    }

    pub fn list(&self) -> Vec<ClientSnapshot> {
        let mut snapshots = self.clients.values().cloned().collect::<Vec<_>>();
        snapshots.sort_by_key(|snapshot| snapshot.id);
        snapshots
    }

    pub fn set_blocked(&mut self, client_id: i64, blocked: bool) {
        if blocked {
            self.blocked_clients.insert(client_id);
        } else {
            self.blocked_clients.remove(&client_id);
        }
    }

    pub fn is_blocked(&self, client_id: i64) -> bool {
        self.blocked_clients.contains(&client_id)
    }

    pub fn blocked_clients(&self) -> usize {
        self.blocked_clients.len()
    }

    pub fn tracking_clients(&self) -> usize {
        self.clients
            .values()
            .filter(|snapshot| snapshot.tracking_enabled)
            .count()
    }

    pub fn len(&self) -> usize {
        self.clients.len()
    }

    pub fn is_empty(&self) -> bool {
        self.clients.is_empty()
    }
}
