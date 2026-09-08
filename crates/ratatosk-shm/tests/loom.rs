//! Loom model of the ring + parking handshake.
//!
//! Run with: `RUSTFLAGS="--cfg loom" cargo test -p ratatosk-shm --release --test loom`
//!
//! The doorbell is modelled as a mutex-protected counter plus a condvar per
//! direction, which is exactly what the Unix-socket doorbell provides: a
//! wake-up that can be coalesced but never lost once sent. A lost wake-up in
//! the handshake would show up as a loom deadlock (both threads blocked).
#![cfg(loom)]

use loom::sync::atomic::{AtomicU8, AtomicU32, AtomicU64};
use loom::sync::{Arc, Condvar, Mutex};
use loom::thread;
use ratatosk_shm::ring::{Consumer, Park, Producer, RingView};

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

struct Doorbell {
    pending: Mutex<u32>,
    cv: Condvar,
}

impl Doorbell {
    fn new() -> Self {
        Self {
            pending: Mutex::new(0),
            cv: Condvar::new(),
        }
    }

    fn ring(&self) {
        let mut pending = self.pending.lock().unwrap();
        *pending += 1;
        self.cv.notify_one();
    }

    /// Block until at least one doorbell byte is pending, then drain them all.
    fn wait_and_drain(&self) {
        let mut pending = self.pending.lock().unwrap();
        while *pending == 0 {
            pending = self.cv.wait(pending).unwrap();
        }
        *pending = 0;
    }
}

#[test]
fn producer_and_consumer_never_lose_a_wakeup() {
    loom::model(|| {
        const CAP: usize = 2;
        const TOTAL: usize = 3; // forces at least one park-for-space and one park-for-data

        let storage = Arc::new(Storage::new(CAP));
        let to_consumer = Arc::new(Doorbell::new());
        let to_producer = Arc::new(Doorbell::new());

        let producer_thread = {
            let storage = Arc::clone(&storage);
            let to_consumer = Arc::clone(&to_consumer);
            let to_producer = Arc::clone(&to_producer);
            thread::spawn(move || {
                let view = storage.view();
                let mut producer = Producer::new(&view);
                let payload: Vec<u8> = (1..=TOTAL as u8).collect();
                let mut written = 0;
                while written < TOTAL {
                    let n = producer.push(&view, &payload[written..]).unwrap();
                    if n > 0 {
                        written += n;
                        if producer.should_ring(&view) {
                            to_consumer.ring();
                        }
                        continue;
                    }
                    match producer.park_for_space(&view).unwrap() {
                        Park::Proceed => {}
                        Park::Sleep => {
                            to_producer.wait_and_drain();
                            producer.unpark(&view);
                        }
                    }
                }
            })
        };

        let consumer_thread = {
            let storage = Arc::clone(&storage);
            let to_consumer = Arc::clone(&to_consumer);
            let to_producer = Arc::clone(&to_producer);
            thread::spawn(move || {
                let view = storage.view();
                let mut consumer = Consumer::new(&view);
                let mut received = Vec::with_capacity(TOTAL);
                let mut buf = [0u8; TOTAL];
                while received.len() < TOTAL {
                    let n = consumer.pop(&view, &mut buf).unwrap();
                    if n > 0 {
                        received.extend_from_slice(&buf[..n]);
                        if consumer.should_ring(&view) {
                            to_producer.ring();
                        }
                        continue;
                    }
                    match consumer.park_for_data(&view).unwrap() {
                        Park::Proceed => {}
                        Park::Sleep => {
                            to_consumer.wait_and_drain();
                            consumer.unpark(&view);
                        }
                    }
                }
                received
            })
        };

        producer_thread.join().unwrap();
        let received = consumer_thread.join().unwrap();
        assert_eq!(received, (1..=TOTAL as u8).collect::<Vec<u8>>());
    });
}
