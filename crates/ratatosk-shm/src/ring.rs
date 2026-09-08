//! Single-producer / single-consumer byte ring with a Dekker-style parking
//! handshake. Sans-IO: the caller provides the storage and the doorbell.
//!
//! Index protocol (per direction):
//!
//! * `tail` is written only by the producer, `head` only by the consumer.
//!   Each side keeps its own index privately and only *publishes* it to the
//!   shared word; it never reads its own index back from shared memory.
//! * The peer-owned index is validated on every read:
//!   `tail.wrapping_sub(head) > capacity` means the peer corrupted the ring.
//!   The caller must then close the connection.
//! * Data indices are always masked, so no value taken from shared memory can
//!   produce an out-of-bounds access.
//!
//! Parking handshake (symmetric for "wait for data" and "wait for space"):
//!
//! ```text
//! waiter:                          signaller (after publishing an index):
//!   parked.store(1, SeqCst)          index.store(.., Release)
//!   fence(SeqCst)                    fence(SeqCst)
//!   if progress possible:            if parked.load(SeqCst) == 1:
//!       parked.store(0); proceed         ring doorbell
//!   else: sleep on doorbell
//! ```
//!
//! The two `SeqCst` fences form a total order, so at least one side observes
//! the other's write: either the signaller sees `parked == 1` and rings, or
//! the waiter sees the published index and does not sleep. Lost wake-ups are
//! therefore impossible; the loom model in `tests/loom.rs` checks this.

use crate::sync::{AtomicU8, AtomicU32, AtomicU64, Ordering, fence};

/// The peer wrote an index that cannot be valid. The connection must be closed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Corrupt;

impl std::fmt::Display for Corrupt {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("shared-memory ring index corrupted by peer")
    }
}

impl std::error::Error for Corrupt {}

/// Outcome of a `park_*` call.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Park {
    /// Progress became possible while arming the parked flag; the flag has been
    /// cleared again and the caller should retry immediately.
    Proceed,
    /// The parked flag is armed and no progress is possible. The caller must
    /// wait for the doorbell and then call `unpark()`.
    Sleep,
}

/// Borrowed view of one ring direction's control words and data area.
pub struct RingView<'a> {
    pub tail: &'a AtomicU64,
    pub head: &'a AtomicU64,
    pub consumer_parked: &'a AtomicU32,
    pub producer_parked: &'a AtomicU32,
    pub data: &'a [AtomicU8],
}

impl RingView<'_> {
    fn capacity(&self) -> u64 {
        self.data.len() as u64
    }

    fn mask(&self) -> u64 {
        debug_assert!(self.data.len().is_power_of_two());
        self.capacity() - 1
    }
}

/// Producer half. `tail` is the private, authoritative copy of the index.
/// The view is passed per call so the owner of the storage (an `Arc`'d
/// segment) does not have to be borrowed for the producer's whole lifetime.
pub struct Producer {
    tail: u64,
}

impl Producer {
    /// Create a producer whose private tail starts from the currently
    /// published value. Call only once per ring, before any data is exchanged.
    pub fn new(view: &RingView<'_>) -> Self {
        assert!(view.data.len().is_power_of_two() && !view.data.is_empty());
        Self {
            tail: view.tail.load(Ordering::Acquire),
        }
    }

    /// Number of bytes the peer has not consumed yet, validated.
    fn used(&self, view: &RingView<'_>, head: u64) -> Result<u64, Corrupt> {
        let used = self.tail.wrapping_sub(head);
        if used > view.capacity() {
            return Err(Corrupt);
        }
        Ok(used)
    }

    /// Copy as much of `src` as fits and publish the new tail. Returns the
    /// number of bytes written (0 when the ring is full). The caller should
    /// then check [`Producer::should_ring`] and ring the doorbell if asked.
    pub fn push(&mut self, view: &RingView<'_>, src: &[u8]) -> Result<usize, Corrupt> {
        let head = view.head.load(Ordering::Acquire);
        let free = view.capacity() - self.used(view, head)?;
        let n = (free as usize).min(src.len());
        if n == 0 {
            return Ok(0);
        }
        let mask = view.mask();
        for (i, byte) in src[..n].iter().enumerate() {
            let idx = (self.tail.wrapping_add(i as u64) & mask) as usize;
            view.data[idx].store(*byte, Ordering::Relaxed);
        }
        self.tail = self.tail.wrapping_add(n as u64);
        view.tail.store(self.tail, Ordering::Release);
        Ok(n)
    }

    /// After a successful `push`, decide whether the consumer is parked and
    /// needs the doorbell.
    pub fn should_ring(&self, view: &RingView<'_>) -> bool {
        fence(Ordering::SeqCst);
        view.consumer_parked.load(Ordering::SeqCst) == 1
    }

    /// Arm the producer's parked flag because the ring is full.
    pub fn park_for_space(&self, view: &RingView<'_>) -> Result<Park, Corrupt> {
        view.producer_parked.store(1, Ordering::SeqCst);
        fence(Ordering::SeqCst);
        let head = view.head.load(Ordering::Acquire);
        if self.used(view, head)? < view.capacity() {
            view.producer_parked.store(0, Ordering::SeqCst);
            return Ok(Park::Proceed);
        }
        Ok(Park::Sleep)
    }

    /// Clear the parked flag after the doorbell woke us.
    pub fn unpark(&self, view: &RingView<'_>) {
        view.producer_parked.store(0, Ordering::SeqCst);
    }

    /// Cheap check used by spin loops: is there room for at least one byte?
    pub fn has_space(&self, view: &RingView<'_>) -> Result<bool, Corrupt> {
        let head = view.head.load(Ordering::Acquire);
        Ok(self.used(view, head)? < view.capacity())
    }
}

/// Consumer half. `head` is the private, authoritative copy of the index.
pub struct Consumer {
    head: u64,
}

impl Consumer {
    pub fn new(view: &RingView<'_>) -> Self {
        assert!(view.data.len().is_power_of_two() && !view.data.is_empty());
        Self {
            head: view.head.load(Ordering::Acquire),
        }
    }

    /// Number of readable bytes, validated against the peer-owned tail.
    fn available(&self, view: &RingView<'_>, tail: u64) -> Result<u64, Corrupt> {
        let avail = tail.wrapping_sub(self.head);
        if avail > view.capacity() {
            return Err(Corrupt);
        }
        Ok(avail)
    }

    /// Copy up to `dst.len()` bytes out of the ring into private memory and
    /// publish the new head. Returns 0 when the ring is empty.
    pub fn pop(&mut self, view: &RingView<'_>, dst: &mut [u8]) -> Result<usize, Corrupt> {
        let tail = view.tail.load(Ordering::Acquire);
        let avail = self.available(view, tail)?;
        let n = (avail as usize).min(dst.len());
        if n == 0 {
            return Ok(0);
        }
        let mask = view.mask();
        for (i, slot) in dst[..n].iter_mut().enumerate() {
            let idx = (self.head.wrapping_add(i as u64) & mask) as usize;
            *slot = view.data[idx].load(Ordering::Relaxed);
        }
        self.head = self.head.wrapping_add(n as u64);
        view.head.store(self.head, Ordering::Release);
        Ok(n)
    }

    /// After a successful `pop`, decide whether the producer is parked waiting
    /// for space and needs the doorbell.
    pub fn should_ring(&self, view: &RingView<'_>) -> bool {
        fence(Ordering::SeqCst);
        view.producer_parked.load(Ordering::SeqCst) == 1
    }

    /// Arm the consumer's parked flag because the ring is empty.
    pub fn park_for_data(&self, view: &RingView<'_>) -> Result<Park, Corrupt> {
        view.consumer_parked.store(1, Ordering::SeqCst);
        fence(Ordering::SeqCst);
        let tail = view.tail.load(Ordering::Acquire);
        if self.available(view, tail)? > 0 {
            view.consumer_parked.store(0, Ordering::SeqCst);
            return Ok(Park::Proceed);
        }
        Ok(Park::Sleep)
    }

    pub fn unpark(&self, view: &RingView<'_>) {
        view.consumer_parked.store(0, Ordering::SeqCst);
    }

    /// Cheap check used by spin loops: is at least one byte readable?
    pub fn has_data(&self, view: &RingView<'_>) -> Result<bool, Corrupt> {
        let tail = view.tail.load(Ordering::Acquire);
        Ok(self.available(view, tail)? > 0)
    }
}

#[cfg(all(test, not(loom)))]
mod tests {
    use super::*;

    struct Storage {
        tail: AtomicU64,
        head: AtomicU64,
        consumer_parked: AtomicU32,
        producer_parked: AtomicU32,
        data: Vec<AtomicU8>,
    }

    impl Storage {
        fn new(cap: usize) -> Self {
            Self {
                tail: AtomicU64::new(0),
                head: AtomicU64::new(0),
                consumer_parked: AtomicU32::new(0),
                producer_parked: AtomicU32::new(0),
                data: (0..cap).map(|_| AtomicU8::new(0)).collect(),
            }
        }

        fn view(&self) -> RingView<'_> {
            RingView {
                tail: &self.tail,
                head: &self.head,
                consumer_parked: &self.consumer_parked,
                producer_parked: &self.producer_parked,
                data: &self.data,
            }
        }
    }

    #[test]
    fn push_pop_round_trip_with_wrap() {
        let storage = Storage::new(8);
        let mut producer = Producer::new(&storage.view());
        let mut consumer = Consumer::new(&storage.view());

        assert_eq!(producer.push(&storage.view(), b"abcdef").unwrap(), 6);
        let mut out = [0u8; 8];
        assert_eq!(consumer.pop(&storage.view(), &mut out).unwrap(), 6);
        assert_eq!(&out[..6], b"abcdef");

        // Wraps around the end of the 8-byte buffer.
        assert_eq!(producer.push(&storage.view(), b"ghijkl").unwrap(), 6);
        assert_eq!(consumer.pop(&storage.view(), &mut out).unwrap(), 6);
        assert_eq!(&out[..6], b"ghijkl");
    }

    #[test]
    fn full_ring_returns_zero_and_partial_writes() {
        let storage = Storage::new(4);
        let mut producer = Producer::new(&storage.view());
        let mut consumer = Consumer::new(&storage.view());

        assert_eq!(producer.push(&storage.view(), b"123456").unwrap(), 4);
        assert_eq!(producer.push(&storage.view(), b"x").unwrap(), 0);
        assert_eq!(
            producer.park_for_space(&storage.view()).unwrap(),
            Park::Sleep
        );
        let mut out = [0u8; 2];
        assert_eq!(consumer.pop(&storage.view(), &mut out).unwrap(), 2);
        assert!(
            consumer.should_ring(&storage.view()),
            "producer is parked; must ring"
        );
        assert_eq!(
            producer.park_for_space(&storage.view()).unwrap(),
            Park::Proceed
        );
        assert_eq!(producer.push(&storage.view(), b"x").unwrap(), 1);
    }

    #[test]
    fn empty_ring_parks_and_wakes() {
        let storage = Storage::new(4);
        let mut producer = Producer::new(&storage.view());
        let mut consumer = Consumer::new(&storage.view());

        assert_eq!(
            consumer.park_for_data(&storage.view()).unwrap(),
            Park::Sleep
        );
        assert_eq!(producer.push(&storage.view(), b"a").unwrap(), 1);
        assert!(producer.should_ring(&storage.view()));
        assert_eq!(
            consumer.park_for_data(&storage.view()).unwrap(),
            Park::Proceed
        );
        let mut out = [0u8; 1];
        assert_eq!(consumer.pop(&storage.view(), &mut out).unwrap(), 1);
        assert!(!producer.should_ring(&storage.view()));
    }

    #[test]
    fn hostile_tail_is_rejected_by_consumer() {
        let storage = Storage::new(8);
        let mut consumer = Consumer::new(&storage.view());
        storage.tail.store(9, Ordering::SeqCst);
        let mut out = [0u8; 8];
        assert_eq!(consumer.pop(&storage.view(), &mut out), Err(Corrupt));
        assert_eq!(consumer.park_for_data(&storage.view()), Err(Corrupt));
    }

    #[test]
    fn hostile_head_is_rejected_by_producer() {
        let storage = Storage::new(8);
        let mut producer = Producer::new(&storage.view());
        // head ahead of tail by more than capacity → wrapping_sub exceeds capacity
        storage.head.store(1, Ordering::SeqCst);
        assert_eq!(producer.push(&storage.view(), b"a"), Err(Corrupt));
        assert_eq!(producer.park_for_space(&storage.view()), Err(Corrupt));
    }

    #[test]
    fn masked_indices_never_escape_capacity() {
        let storage = Storage::new(4);
        let mut producer = Producer::new(&storage.view());
        let mut consumer = Consumer::new(&storage.view());
        // Drive the indices far past u32 range by cycling many times.
        let payload = [7u8; 3];
        let mut out = [0u8; 3];
        for _ in 0..10_000 {
            assert_eq!(producer.push(&storage.view(), &payload).unwrap(), 3);
            assert_eq!(consumer.pop(&storage.view(), &mut out).unwrap(), 3);
            assert_eq!(out, payload);
        }
    }
}
