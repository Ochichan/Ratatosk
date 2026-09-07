//! Time utilities for Ratatosk.
//!
//! ## Clock Selection Guide
//!
//! **Wall-clock (SystemTime)** — use for:
//! - Display timestamps (INFO logs, client-facing values)
//! - Persistence (RDB/AOF timestamps, expire_at_ms)
//! - Absolute deadlines that must survive process restart
//!
//! **Monotonic clock (Instant)** — use for:
//! - Durations and elapsed time (ops/sec calculation, profiling)
//! - Relative deadlines (BLPOP timeout, client read timeout)
//! - Any measurement that must be immune to clock jumps
//!
//! **Why both?**
//! - SystemTime can jump backward (NTP correction, manual adjustment)
//! - Instant is guaranteed monotonic but not comparable across restarts
//! - Mixing them causes bugs: negative durations, missed deadlines, panics

use std::cell::Cell;
use std::time::{Instant, SystemTime, UNIX_EPOCH};

thread_local! {
    static COMMAND_TIME: Cell<Option<i64>> = const { Cell::new(None) };
}

/// Run synchronous command execution against its recorded wall clock.
/// This scope must never span an await. Nested scopes and unwinding restore
/// the caller's clock; other runtime threads retain their own real clock.
pub fn with_command_time<T>(timestamp_ms: i64, execute: impl FnOnce() -> T) -> T {
    struct Restore(Option<i64>);
    impl Drop for Restore {
        fn drop(&mut self) {
            COMMAND_TIME.set(self.0);
        }
    }
    let _restore = Restore(COMMAND_TIME.replace(Some(timestamp_ms)));
    execute()
}

/// Current wall-clock time in milliseconds since UNIX epoch.
///
/// Use for display, persistence, and absolute deadlines.
/// Do NOT use for duration measurement (use `monotonic_ms()` instead).
pub fn now_ms() -> i64 {
    if let Some(timestamp) = COMMAND_TIME.get() {
        return timestamp;
    }
    match SystemTime::now().duration_since(UNIX_EPOCH) {
        Ok(duration) => i64::try_from(duration.as_millis()).unwrap_or(i64::MAX),
        Err(_) => 0,
    }
}

/// Current wall-clock time in seconds since UNIX epoch.
///
/// Use for display, persistence, and absolute deadlines.
/// Do NOT use for duration measurement (use `monotonic_ms()` instead).
pub fn now_sec() -> i64 {
    now_ms() / 1000
}

/// Monotonic clock in milliseconds since an arbitrary epoch.
///
/// Use for durations, elapsed time, and relative deadlines.
/// Guaranteed never to go backward, immune to clock jumps.
///
/// Do NOT persist this value — it's meaningless across restarts.
pub fn monotonic_ms() -> u64 {
    static START: std::sync::OnceLock<Instant> = std::sync::OnceLock::new();
    let start = START.get_or_init(Instant::now);
    start.elapsed().as_millis() as u64
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn command_clock_is_nested_thread_local_and_unwind_safe() {
        with_command_time(12345, || {
            assert_eq!(now_ms(), 12345);
            assert_eq!(now_sec(), 12);
            with_command_time(67890, || assert_eq!(now_ms(), 67890));
            assert_eq!(now_ms(), 12345);
            assert!(std::thread::spawn(now_ms).join().expect("clock thread") > 12345);
            let _ = std::panic::catch_unwind(|| with_command_time(1, || panic!("test unwind")));
            assert_eq!(now_ms(), 12345);
        });
        assert!(now_ms() > 12345);
    }

    #[test]
    fn now_ms_returns_positive() {
        let ts = now_ms();
        assert!(ts > 0, "timestamp should be positive: {ts}");
    }

    #[test]
    fn now_sec_returns_positive() {
        let ts = now_sec();
        assert!(ts > 0, "timestamp should be positive: {ts}");
    }

    #[test]
    fn now_ms_greater_than_now_sec() {
        let ms = now_ms();
        let sec = now_sec();
        assert!(ms > sec, "ms ({ms}) should be much larger than sec ({sec})");
    }

    #[test]
    fn monotonic_ms_never_goes_backward() {
        let mut prev = monotonic_ms();
        for _ in 0..1000 {
            let current = monotonic_ms();
            assert!(
                current >= prev,
                "monotonic clock went backward: {prev} -> {current}"
            );
            prev = current;
        }
    }

    #[test]
    fn monotonic_ms_advances() {
        let start = monotonic_ms();
        std::thread::sleep(std::time::Duration::from_millis(10));
        let end = monotonic_ms();
        assert!(
            end > start,
            "monotonic clock should advance: {start} -> {end}"
        );
    }
}
