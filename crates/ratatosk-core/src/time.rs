use std::time::{SystemTime, UNIX_EPOCH};

/// Current wall-clock time in milliseconds since UNIX epoch.
pub fn now_ms() -> i64 {
    match SystemTime::now().duration_since(UNIX_EPOCH) {
        Ok(duration) => i64::try_from(duration.as_millis()).unwrap_or(i64::MAX),
        Err(_) => 0,
    }
}

/// Current wall-clock time in seconds since UNIX epoch.
pub fn now_sec() -> i64 {
    match SystemTime::now().duration_since(UNIX_EPOCH) {
        Ok(duration) => i64::try_from(duration.as_secs()).unwrap_or(i64::MAX),
        Err(_) => 0,
    }
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
}
