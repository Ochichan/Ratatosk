//! Connection rate limiter per IP address.
//!
//! Prevents a single client from exhausting max_clients via rapid
//! connect/disconnect cycles.

use std::collections::HashMap;
use std::net::IpAddr;
use std::time::{Duration, Instant};

/// Rate limiter for incoming connections.
pub struct ConnectionRateLimiter {
    /// Map of IP to connection attempts in the current window
    attempts: HashMap<IpAddr, Vec<Instant>>,
    /// Time window for rate limiting
    window: Duration,
    /// Maximum attempts per window
    max_attempts: usize,
    /// Cleanup interval
    last_cleanup: Instant,
}

impl ConnectionRateLimiter {
    /// Create a new rate limiter with the given window and max attempts.
    pub fn new(window: Duration, max_attempts: usize) -> Self {
        Self {
            attempts: HashMap::new(),
            window,
            max_attempts,
            last_cleanup: Instant::now(),
        }
    }

    /// Check if a connection from the given IP should be allowed.
    ///
    /// Returns `true` if the connection is within rate limits,
    /// `false` if it should be rejected.
    pub fn check_rate_limit(&mut self, addr: IpAddr) -> bool {
        let now = Instant::now();

        // Periodic cleanup of old entries
        if now.duration_since(self.last_cleanup) > self.window {
            self.cleanup(now);
            self.last_cleanup = now;
        }

        let attempts = self.attempts.entry(addr).or_default();

        // Remove old attempts outside the window
        attempts.retain(|t| now.duration_since(*t) < self.window);

        if attempts.len() >= self.max_attempts {
            tracing::warn!(
                target = "ratatosk::security",
                remote_ip = %addr,
                attempts = attempts.len(),
                max_attempts = self.max_attempts,
                window_sec = self.window.as_secs(),
                "Connection rate limit exceeded"
            );
            return false;
        }

        attempts.push(now);
        true
    }

    /// Get the number of tracked IPs (for metrics/debugging).
    pub fn tracked_ips(&self) -> usize {
        self.attempts.len()
    }

    fn cleanup(&mut self, now: Instant) {
        self.attempts.retain(|_, attempts| {
            attempts.retain(|t| now.duration_since(*t) < self.window);
            !attempts.is_empty()
        });
    }
}

impl Default for ConnectionRateLimiter {
    fn default() -> Self {
        // Default: 10 connections per 10 seconds per IP
        Self::new(Duration::from_secs(10), 10)
    }
}

/// Per-IP AUTH failure rate limiter.
///
/// Tracks AUTH failures across reconnection attempts to prevent brute-force
/// attacks that reconnect after each failure. An IP is blocked when it exceeds
/// `max_failures` within `window`.
pub struct AuthRateLimiter {
    failures: HashMap<IpAddr, Vec<Instant>>,
    window: Duration,
    max_failures: usize,
    last_cleanup: Instant,
}

impl AuthRateLimiter {
    pub fn new(window: Duration, max_failures: usize) -> Self {
        Self {
            failures: HashMap::new(),
            window,
            max_failures,
            last_cleanup: Instant::now(),
        }
    }

    /// Record an AUTH failure from the given IP. Returns `true` if the IP
    /// should now be blocked (threshold exceeded).
    pub fn record_failure(&mut self, addr: IpAddr) -> bool {
        let now = Instant::now();

        if now.duration_since(self.last_cleanup) > self.window {
            self.cleanup(now);
            self.last_cleanup = now;
        }

        let entries = self.failures.entry(addr).or_default();
        entries.retain(|t| now.duration_since(*t) < self.window);
        entries.push(now);

        if entries.len() > self.max_failures {
            tracing::warn!(
                target = "ratatosk::security",
                remote_ip = %addr,
                failures = entries.len(),
                max_failures = self.max_failures,
                window_sec = self.window.as_secs(),
                "AUTH failure rate limit exceeded for IP"
            );
            return true;
        }

        false
    }

    /// Check if the given IP is currently blocked due to excessive AUTH failures.
    pub fn is_blocked(&mut self, addr: IpAddr) -> bool {
        let now = Instant::now();
        let entries = self.failures.entry(addr).or_default();
        entries.retain(|t| now.duration_since(*t) < self.window);
        entries.len() > self.max_failures
    }

    /// Clear failure records for an IP (e.g. on successful AUTH).
    pub fn clear(&mut self, addr: IpAddr) {
        self.failures.remove(&addr);
    }

    fn cleanup(&mut self, now: Instant) {
        self.failures.retain(|_, entries| {
            entries.retain(|t| now.duration_since(*t) < self.window);
            !entries.is_empty()
        });
    }
}

impl Default for AuthRateLimiter {
    fn default() -> Self {
        // Default: 20 AUTH failures per 60 seconds per IP
        Self::new(Duration::from_secs(60), 20)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rate_limiter_allows_within_limit() {
        let mut limiter = ConnectionRateLimiter::new(Duration::from_secs(60), 5);
        let ip = IpAddr::from([127, 0, 0, 1]);

        for _ in 0..5 {
            assert!(limiter.check_rate_limit(ip), "Should allow within limit");
        }
    }

    #[test]
    fn rate_limiter_blocks_over_limit() {
        let mut limiter = ConnectionRateLimiter::new(Duration::from_secs(60), 3);
        let ip = IpAddr::from([127, 0, 0, 1]);

        for _ in 0..3 {
            assert!(limiter.check_rate_limit(ip), "Should allow within limit");
        }

        assert!(!limiter.check_rate_limit(ip), "Should block over limit");
    }

    #[test]
    fn rate_limiter_tracks_different_ips_separately() {
        let mut limiter = ConnectionRateLimiter::new(Duration::from_secs(60), 2);
        let ip1 = IpAddr::from([127, 0, 0, 1]);
        let ip2 = IpAddr::from([127, 0, 0, 2]);

        // Exhaust limit for ip1
        assert!(limiter.check_rate_limit(ip1));
        assert!(limiter.check_rate_limit(ip1));
        assert!(!limiter.check_rate_limit(ip1));

        // ip2 should still be allowed
        assert!(limiter.check_rate_limit(ip2));
        assert!(limiter.check_rate_limit(ip2));
        assert!(!limiter.check_rate_limit(ip2));
    }

    #[test]
    fn auth_rate_limiter_blocks_after_threshold() {
        let mut limiter = AuthRateLimiter::new(Duration::from_secs(60), 5);
        let ip = IpAddr::from([10, 0, 0, 1]);

        // First 5 failures should not block
        for _ in 0..5 {
            assert!(
                !limiter.record_failure(ip),
                "Should not block within threshold"
            );
        }

        // 6th failure should trigger block
        assert!(
            limiter.record_failure(ip),
            "Should block after exceeding threshold"
        );
        assert!(limiter.is_blocked(ip), "IP should be blocked");
    }

    #[test]
    fn auth_rate_limiter_clear_resets() {
        let mut limiter = AuthRateLimiter::new(Duration::from_secs(60), 3);
        let ip = IpAddr::from([10, 0, 0, 1]);

        for _ in 0..3 {
            limiter.record_failure(ip);
        }
        assert!(limiter.record_failure(ip));

        limiter.clear(ip);
        assert!(!limiter.is_blocked(ip), "Should not be blocked after clear");
    }

    #[test]
    fn auth_rate_limiter_tracks_ips_separately() {
        let mut limiter = AuthRateLimiter::new(Duration::from_secs(60), 3);
        let ip1 = IpAddr::from([10, 0, 0, 1]);
        let ip2 = IpAddr::from([10, 0, 0, 2]);

        for _ in 0..4 {
            limiter.record_failure(ip1);
        }

        assert!(limiter.is_blocked(ip1));
        assert!(
            !limiter.is_blocked(ip2),
            "Different IP should not be blocked"
        );
    }
}
