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

use std::time::{Instant, SystemTime, UNIX_EPOCH};

/// Current wall-clock time in milliseconds since UNIX epoch.
///
/// Use for display, persistence, and absolute deadlines.
/// Do NOT use for duration measurement (use `monotonic_ms()` instead).
pub fn now_ms() -> i64 {
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
    match SystemTime::now().duration_since(UNIX_EPOCH) {
        Ok(duration) => i64::try_from(duration.as_secs()).unwrap_or(i64::MAX),
        Err(_) => 0,
    }
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
