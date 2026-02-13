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
}
