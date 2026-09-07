use bytes::Bytes;
use hashbrown::{HashMap, HashSet};
use smallvec::SmallVec;

use crate::config::ConfigState;

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
    Invalidate {
        keys: Vec<Bytes>,
    },
    TrackingRedirectBroken {
        redirect_client_id: i64,
    },
}

pub struct PubSubState {
    channels: HashMap<Bytes, HashSet<i64>>,
    shard_channels: HashMap<Bytes, HashSet<i64>>,
    patterns: HashMap<Bytes, HashSet<i64>>,
    client_channel_subs: HashMap<i64, HashSet<Bytes>>,
    client_shard_channel_subs: HashMap<i64, HashSet<Bytes>>,
    client_pattern_subs: HashMap<i64, HashSet<Bytes>>,
    subscriber_tx: HashMap<i64, tokio::sync::mpsc::Sender<PubSubMessage>>,
    hard_limit: usize,
    soft_limit: usize,
    soft_seconds: u64,
}

impl std::fmt::Debug for PubSubState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PubSubState")
            .field("channels", &self.channels)
            .field("shard_channels", &self.shard_channels)
            .field("patterns", &self.patterns)
            .field("client_channel_subs", &self.client_channel_subs)
            .field("client_shard_channel_subs", &self.client_shard_channel_subs)
            .field("client_pattern_subs", &self.client_pattern_subs)
            .field(
                "subscriber_tx",
                &self.subscriber_tx.keys().collect::<Vec<_>>(),
            )
            .field("hard_limit", &self.hard_limit)
            .field("soft_limit", &self.soft_limit)
            .field("soft_seconds", &self.soft_seconds)
            .finish()
    }
}

impl Default for PubSubState {
    fn default() -> Self {
        Self::new(&ConfigState::default())
    }
}

impl PubSubState {
    pub fn new(config: &ConfigState) -> Self {
        Self {
            channels: HashMap::default(),
            shard_channels: HashMap::default(),
            patterns: HashMap::default(),
            client_channel_subs: HashMap::default(),
            client_shard_channel_subs: HashMap::default(),
            client_pattern_subs: HashMap::default(),
            subscriber_tx: HashMap::default(),
            hard_limit: config.pubsub_queue_hard_limit(),
            soft_limit: config.pubsub_queue_soft_limit(),
            soft_seconds: config.pubsub_queue_soft_seconds(),
        }
    }

    pub fn set_queue_limits(&mut self, hard_limit: usize, soft_limit: usize, soft_seconds: u64) {
        self.hard_limit = hard_limit;
        self.soft_limit = soft_limit;
        self.soft_seconds = soft_seconds;
    }

    pub fn register_client(
        &mut self,
        client_id: i64,
    ) -> tokio::sync::mpsc::Receiver<PubSubMessage> {
        let (tx, rx) = tokio::sync::mpsc::channel(self.hard_limit.max(1));
        self.subscriber_tx.insert(client_id, tx);
        rx
    }

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

    fn send_to_client(&mut self, client_id: i64, msg: PubSubMessage) -> bool {
        let tx = match self.subscriber_tx.get(&client_id) {
            Some(tx) => tx,
            None => {
                if self.client_total_subscriptions(client_id) == 0 {
                    metrics::counter!("ratatosk_pubsub_messages_dropped_total", "reason" => "missing_client")
                        .increment(1);
                    return false;
                }
                metrics::counter!("ratatosk_pubsub_messages_dropped_total", "reason" => "missing_client")
                    .increment(1);
                return false;
            }
        };
        match tx.try_send(msg) {
            Ok(()) => true,
            Err(tokio::sync::mpsc::error::TrySendError::Full(_)) => {
                metrics::counter!("ratatosk_pubsub_clients_overflowed_total").increment(1);
                metrics::counter!("ratatosk_pubsub_messages_dropped_total", "reason" => "queue_full")
                    .increment(1);
                tracing::warn!(
                    target = "ratatosk::pubsub",
                    client_id,
                    "PubSub client overflowed - channel full, closing sender"
                );
                self.subscriber_tx.remove(&client_id);
                false
            }
            Err(tokio::sync::mpsc::error::TrySendError::Closed(_)) => {
                metrics::counter!("ratatosk_pubsub_messages_dropped_total", "reason" => "client_disconnected")
                    .increment(1);
                self.subscriber_tx.remove(&client_id);
                false
            }
        }
    }

    pub fn pending_queue_limit(&self) -> usize {
        self.hard_limit
    }

    pub fn client_has_subscriptions(&self, client_id: i64) -> bool {
        self.client_total_subscriptions(client_id) > 0
    }

    pub fn publish(&mut self, channel: &Bytes, payload: &Bytes) -> i64 {
        let mut receivers = 0i64;

        let direct_subscribers: SmallVec<[i64; 8]> = self
            .channels
            .get(channel)
            .map(|set| set.iter().copied().collect())
            .unwrap_or_default();

        for client_id in direct_subscribers {
            if self.send_to_client(
                client_id,
                PubSubMessage::Message {
                    channel: channel.clone(),
                    payload: payload.clone(),
                },
            ) {
                receivers += 1;
            }
        }

        if !self.patterns.is_empty() {
            let channel_text = String::from_utf8_lossy(channel);
            let patterns: SmallVec<[(Bytes, SmallVec<[i64; 8]>); 4]> = self
                .patterns
                .iter()
                .filter(|(pattern, _)| {
                    glob_match::glob_match(
                        std::str::from_utf8(pattern).unwrap_or(""),
                        &channel_text,
                    )
                })
                .map(|(pattern, subscribers)| {
                    (pattern.clone(), subscribers.iter().copied().collect())
                })
                .collect();

            for (pattern, subscribers) in patterns {
                for client_id in subscribers {
                    if self.send_to_client(
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
        }

        receivers
    }

    pub fn publish_shard(&mut self, channel: &Bytes, payload: &Bytes) -> i64 {
        let mut receivers = 0i64;

        let direct_subscribers: SmallVec<[i64; 8]> = self
            .shard_channels
            .get(channel)
            .map(|set| set.iter().copied().collect())
            .unwrap_or_default();

        for client_id in direct_subscribers {
            if self.send_to_client(
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

    pub fn enqueue_invalidation(&mut self, client_id: i64, keys: Vec<Bytes>) -> bool {
        self.send_to_client(client_id, PubSubMessage::Invalidate { keys })
    }

    pub fn enqueue_invalidation_message(&mut self, client_id: i64, message: PubSubMessage) -> bool {
        self.send_to_client(client_id, message)
    }

    pub fn drain_rx(rx: &mut tokio::sync::mpsc::Receiver<PubSubMessage>) -> Vec<PubSubMessage> {
        let mut out = Vec::new();
        while let Ok(msg) = rx.try_recv() {
            out.push(msg);
        }
        out
    }

    pub fn notify_client(&self, _client_id: i64) {}

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

        self.subscriber_tx.remove(&client_id);
    }

    pub(crate) fn client_total_subscriptions(&self, client_id: i64) -> usize {
        self.client_standard_subscriptions(client_id)
            .saturating_add(self.client_shard_subscriptions(client_id))
    }

    pub(crate) fn client_standard_subscriptions(&self, client_id: i64) -> usize {
        self.client_channel_subs
            .get(&client_id)
            .map_or(0, HashSet::len)
            .saturating_add(
                self.client_pattern_subs
                    .get(&client_id)
                    .map_or(0, HashSet::len),
            )
    }

    pub(crate) fn client_shard_subscriptions(&self, client_id: i64) -> usize {
        self.client_shard_channel_subs
            .get(&client_id)
            .map_or(0, HashSet::len)
    }

    #[cfg(test)]
    pub fn assert_invariants(&self) {
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
