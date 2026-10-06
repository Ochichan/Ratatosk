//! Process-local authentication failure limiting.

use std::collections::HashMap;
use std::net::IpAddr;
use std::time::{Duration, Instant};

const DEFAULT_MAX_TRACKED_IPS: usize = 4096;

/// Bounded per-IP authentication failure limiter.
///
/// Entries are retained while they contain failures inside the configured
/// window. When every slot is active, an untracked IP is rejected rather than
/// evicting an active entry and making the limit easy to bypass.
#[derive(Debug)]
pub struct AuthRateLimiter {
    failures: HashMap<IpAddr, Vec<Instant>>,
    window: Duration,
    max_failures: usize,
    max_tracked_ips: usize,
}

impl AuthRateLimiter {
    /// Create a limiter with the process-wide IP capacity.
    pub fn new(window: Duration, max_failures: usize) -> Self {
        Self::with_capacity(window, max_failures, DEFAULT_MAX_TRACKED_IPS)
    }

    fn with_capacity(window: Duration, max_failures: usize, max_tracked_ips: usize) -> Self {
        Self {
            failures: HashMap::new(),
            window,
            max_failures: max_failures.clamp(1, 20),
            max_tracked_ips: max_tracked_ips.max(1),
        }
    }

    /// Return whether a credential check must be rejected for this IP.
    ///
    /// This query never allocates an empty per-IP entry.
    pub fn is_blocked(&mut self, addr: IpAddr) -> bool {
        self.is_blocked_at(addr, Instant::now())
    }

    /// Record a failed credential check and return whether the IP has reached
    /// the threshold. At most `max_failures` timestamps are retained per IP.
    pub fn record_failure(&mut self, addr: IpAddr) -> bool {
        self.record_failure_at(addr, Instant::now())
    }

    /// Remove the failure history for an IP.
    ///
    /// Authentication success deliberately does not call this: a successful
    /// client must not erase failures made by other clients behind the same IP.
    pub fn clear(&mut self, addr: IpAddr) {
        self.failures.remove(&canonical_ip(addr));
    }

    /// Number of IP entries currently retained.
    pub fn tracked_ips(&self) -> usize {
        self.failures.len()
    }

    fn is_blocked_at(&mut self, addr: IpAddr, now: Instant) -> bool {
        let addr = canonical_ip(addr);
        if let Some(entries) = self.failures.get_mut(&addr) {
            retain_recent(entries, now, self.window);
            if entries.is_empty() {
                self.failures.remove(&addr);
            } else {
                return entries.len() >= self.max_failures;
            }
        }

        if self.failures.len() < self.max_tracked_ips {
            return false;
        }

        self.cleanup(now);
        self.failures.len() >= self.max_tracked_ips
    }

    fn record_failure_at(&mut self, addr: IpAddr, now: Instant) -> bool {
        let addr = canonical_ip(addr);
        if self.is_blocked_at(addr, now) {
            return true;
        }

        let entries = self.failures.entry(addr).or_default();
        if entries.len() < self.max_failures {
            entries.push(now);
        }
        entries.len() >= self.max_failures
    }

    fn cleanup(&mut self, now: Instant) {
        self.failures.retain(|_, entries| {
            retain_recent(entries, now, self.window);
            !entries.is_empty()
        });
    }
}

impl Default for AuthRateLimiter {
    fn default() -> Self {
        Self::new(Duration::from_secs(60), 20)
    }
}

fn retain_recent(entries: &mut Vec<Instant>, now: Instant, window: Duration) {
    entries.retain(|failure| now.saturating_duration_since(*failure) < window);
}

fn canonical_ip(addr: IpAddr) -> IpAddr {
    match addr {
        IpAddr::V6(addr) => addr
            .to_ipv4_mapped()
            .map(IpAddr::V4)
            .unwrap_or(IpAddr::V6(addr)),
        IpAddr::V4(addr) => IpAddr::V4(addr),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::Ipv4Addr;

    fn ip(last_octet: u8) -> IpAddr {
        IpAddr::V4(Ipv4Addr::new(192, 0, 2, last_octet))
    }

    #[test]
    fn blocks_at_threshold_and_expires_on_a_monotonic_clock() {
        let mut limiter = AuthRateLimiter::with_capacity(Duration::from_secs(60), 3, 8);
        let start = Instant::now();

        assert!(!limiter.is_blocked_at(ip(1), start));
        assert!(!limiter.record_failure_at(ip(1), start));
        assert!(!limiter.record_failure_at(ip(1), start + Duration::from_secs(1)));
        assert!(limiter.record_failure_at(ip(1), start + Duration::from_secs(2)));
        assert!(limiter.is_blocked_at(ip(1), start + Duration::from_secs(59)));
        assert!(!limiter.is_blocked_at(ip(1), start + Duration::from_secs(62)));
        assert_eq!(limiter.tracked_ips(), 0);
    }

    #[test]
    fn full_active_map_fails_closed_without_evicting_or_allocating() {
        let mut limiter = AuthRateLimiter::with_capacity(Duration::from_secs(60), 2, 2);
        let start = Instant::now();

        assert!(!limiter.record_failure_at(ip(1), start));
        assert!(!limiter.record_failure_at(ip(2), start));
        assert!(limiter.is_blocked_at(ip(3), start));
        assert_eq!(limiter.tracked_ips(), 2);
        assert!(!limiter.is_blocked_at(ip(1), start));
        assert!(!limiter.is_blocked_at(ip(2), start));

        assert!(!limiter.is_blocked_at(ip(3), start + Duration::from_secs(60)));
        assert_eq!(limiter.tracked_ips(), 0);
    }

    #[test]
    fn ipv4_mapped_ipv6_shares_the_ipv4_bucket() {
        let mut limiter = AuthRateLimiter::with_capacity(Duration::from_secs(60), 2, 8);
        let start = Instant::now();
        let v4 = Ipv4Addr::new(192, 0, 2, 9);
        let mapped = IpAddr::V6(v4.to_ipv6_mapped());

        assert!(!limiter.record_failure_at(IpAddr::V4(v4), start));
        assert!(limiter.record_failure_at(mapped, start));
        assert!(limiter.is_blocked_at(IpAddr::V4(v4), start));
        assert_eq!(limiter.tracked_ips(), 1);
    }

    #[test]
    fn queries_do_not_allocate_empty_entries_and_timestamps_stay_bounded() {
        let mut limiter = AuthRateLimiter::with_capacity(Duration::from_secs(60), 2, 8);
        let start = Instant::now();

        assert!(!limiter.is_blocked_at(ip(1), start));
        assert_eq!(limiter.tracked_ips(), 0);
        assert!(!limiter.record_failure_at(ip(1), start));
        assert!(limiter.record_failure_at(ip(1), start));
        assert!(limiter.record_failure_at(ip(1), start));
        assert_eq!(limiter.failures[&ip(1)].len(), 2);
    }
}
