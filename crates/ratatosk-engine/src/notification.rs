use bytes::Bytes;

use crate::keyspace::PubSubState;

// ---------------------------------------------------------------------------
// notify-keyspace-events config flag parsing
// ---------------------------------------------------------------------------

/// Event type flags derived from the `notify-keyspace-events` config string.
///
/// K = keyspace events, E = keyevent events
/// g = generic (DEL, EXPIRE, RENAME, ...)
/// $ = string commands
/// l = list commands
/// s = set commands
/// h = hash commands
/// z = sorted set commands
/// x = expired events
/// e = evicted events (maxmemory)
/// t = stream commands
/// m = key miss events
/// A = alias for "g$lshzxet"
#[derive(Debug, Clone, Copy, Default)]
pub struct NotifyFlags {
    pub keyspace: bool,
    pub keyevent: bool,
    pub generic: bool,
    pub string: bool,
    pub list: bool,
    pub set: bool,
    pub hash: bool,
    pub sorted_set: bool,
    pub expired: bool,
    pub evicted: bool,
    pub stream: bool,
    pub key_miss: bool,
}

impl NotifyFlags {
    /// Parse the Redis `notify-keyspace-events` config string.
    pub fn from_config(config: &[u8]) -> Self {
        let mut flags = Self::default();
        for &ch in config {
            match ch {
                b'K' => flags.keyspace = true,
                b'E' => flags.keyevent = true,
                b'g' => flags.generic = true,
                b'$' => flags.string = true,
                b'l' => flags.list = true,
                b's' => flags.set = true,
                b'h' => flags.hash = true,
                b'z' => flags.sorted_set = true,
                b'x' => flags.expired = true,
                b'e' => flags.evicted = true,
                b't' => flags.stream = true,
                b'm' => flags.key_miss = true,
                b'A' => {
                    flags.generic = true;
                    flags.string = true;
                    flags.list = true;
                    flags.set = true;
                    flags.hash = true;
                    flags.sorted_set = true;
                    flags.expired = true;
                    flags.evicted = true;
                    flags.stream = true;
                }
                _ => {} // ignore unknown
            }
        }

        // Must have at least K or E, otherwise disable everything
        if !flags.keyspace && !flags.keyevent {
            return Self::default();
        }

        flags
    }

    /// Check if a particular event type character should be notified.
    pub fn should_notify(self, event_type: u8) -> bool {
        if !self.keyspace && !self.keyevent {
            return false;
        }
        match event_type {
            b'g' => self.generic,
            b'$' => self.string,
            b'l' => self.list,
            b's' => self.set,
            b'h' => self.hash,
            b'z' => self.sorted_set,
            b'x' => self.expired,
            b'e' => self.evicted,
            b't' => self.stream,
            b'm' => self.key_miss,
            _ => false,
        }
    }
}

/// Emit keyspace/keyevent notifications via the pub/sub system.
///
/// - `__keyspace@<db>__:<key>` → publishes the event name
/// - `__keyevent@<db>__:<event_name>` → publishes the key
#[allow(clippy::too_many_arguments)]
pub fn notify_keyspace_event(
    pubsub: &mut PubSubState,
    config_str: &[u8],
    event_type: u8,
    event_name: &[u8],
    db_idx: usize,
    key: &Bytes,
) {
    let flags = NotifyFlags::from_config(config_str);
    if !flags.should_notify(event_type) {
        return;
    }

    if flags.keyspace {
        let channel = Bytes::from(format!(
            "__keyspace@{db_idx}__:{}",
            String::from_utf8_lossy(key)
        ));
        let payload = Bytes::copy_from_slice(event_name);
        pubsub.publish(&channel, &payload);
    }

    if flags.keyevent {
        let channel = Bytes::from(format!(
            "__keyevent@{db_idx}__:{}",
            String::from_utf8_lossy(event_name)
        ));
        pubsub.publish(&channel, key);
    }
}

/// Convenience macro for calling `notify_keyspace_event` with less boilerplate.
#[macro_export]
macro_rules! notify {
    ($server:expr, $type:expr, $event:expr, $db:expr, $key:expr) => {
        $crate::notification::notify_keyspace_event(
            &mut $server.pubsub,
            $server.config.notify_keyspace_events(),
            $type,
            $event,
            $db,
            $key,
        )
    };
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;

    use super::*;
    use crate::keyspace::{PubSubState, ServerState};

    #[test]
    fn empty_config_disables_notifications() {
        let flags = NotifyFlags::from_config(b"");
        assert!(!flags.should_notify(b'g'));
        assert!(!flags.should_notify(b'$'));
    }

    #[test]
    fn ke_config_enables_keyspace_and_keyevent() {
        let flags = NotifyFlags::from_config(b"KE$");
        assert!(flags.keyspace);
        assert!(flags.keyevent);
        assert!(flags.string);
        assert!(flags.should_notify(b'$'));
        assert!(!flags.should_notify(b'l'));
    }

    #[test]
    fn a_shorthand_enables_all_data_types() {
        let flags = NotifyFlags::from_config(b"KEA");
        assert!(flags.should_notify(b'g'));
        assert!(flags.should_notify(b'$'));
        assert!(flags.should_notify(b'l'));
        assert!(flags.should_notify(b's'));
        assert!(flags.should_notify(b'h'));
        assert!(flags.should_notify(b'z'));
        assert!(flags.should_notify(b'x'));
        assert!(flags.should_notify(b'e'));
        assert!(flags.should_notify(b't'));
        assert!(!flags.should_notify(b'm')); // m not in A
    }

    #[test]
    fn without_k_or_e_nothing_is_enabled() {
        // Even if data flags are set, without K or E nothing happens
        let flags = NotifyFlags::from_config(b"$lsh");
        assert!(!flags.keyspace);
        assert!(!flags.keyevent);
        assert!(!flags.should_notify(b'$'));
    }

    #[test]
    fn notify_keyspace_event_publishes_to_subscribers() {
        let mut pubsub = PubSubState::default();
        let mut rx1 = pubsub.register_client(1);
        let mut rx2 = pubsub.register_client(2);

        // Subscribe to keyspace channel
        let channel = Bytes::from("__keyspace@0__:mykey");
        pubsub.subscribe_channel(1, channel.clone());

        // Subscribe to keyevent channel
        let event_channel = Bytes::from("__keyevent@0__:set");
        pubsub.subscribe_channel(2, event_channel.clone());

        // Emit notification
        notify_keyspace_event(&mut pubsub, b"KEA", b'$', b"set", 0, &Bytes::from("mykey"));

        // Client 1 should receive keyspace notification
        let msgs1 = PubSubState::drain_rx(&mut rx1);
        assert_eq!(
            msgs1.len(),
            1,
            "client 1 should receive keyspace notification"
        );

        // Client 2 should receive keyevent notification
        let msgs2 = PubSubState::drain_rx(&mut rx2);
        assert_eq!(
            msgs2.len(),
            1,
            "client 2 should receive keyevent notification"
        );
    }

    #[test]
    fn notify_with_k_only_skips_keyevent() {
        let mut pubsub = PubSubState::default();
        let mut rx1 = pubsub.register_client(1);

        let keyevent_channel = Bytes::from("__keyevent@0__:set");
        pubsub.subscribe_channel(1, keyevent_channel);

        // Only K flag, no E
        notify_keyspace_event(&mut pubsub, b"K$", b'$', b"set", 0, &Bytes::from("mykey"));

        let msgs = PubSubState::drain_rx(&mut rx1);
        assert_eq!(
            msgs.len(),
            0,
            "keyevent channel should not receive when only K is set"
        );
    }

    #[test]
    fn notify_macro_works() {
        let mut server = ServerState::with_default_dbs();
        server.config.set_notify_keyspace_events(Bytes::from("KEA"));
        let mut rx1 = server.pubsub.register_client(1);

        let channel = Bytes::from("__keyspace@0__:foo");
        server.pubsub.subscribe_channel(1, channel);

        let key = Bytes::from("foo");
        notify!(server, b'$', b"set", 0, &key);

        let msgs = PubSubState::drain_rx(&mut rx1);
        assert_eq!(msgs.len(), 1);
    }
}
