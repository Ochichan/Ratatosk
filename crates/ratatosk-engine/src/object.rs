use std::time::{SystemTime, UNIX_EPOCH};

use bytes::Bytes;

/// Largest string value a command may build, Redis' default
/// `proto-max-bulk-len` (512 MiB).
pub const PROTO_MAX_BULK_LEN: usize = 512 * 1024 * 1024;

pub const STRING_TOO_LONG_ERR: &str =
    "ERR string exceeds maximum allowed size (proto-max-bulk-len)";

/// Wall-clock microseconds for display and sampling only. Command logic must
/// use `ratatosk_core::time::now_ms`, which honours the replay clock.
pub fn now_us() -> i64 {
    match SystemTime::now().duration_since(UNIX_EPOCH) {
        Ok(duration) => i64::try_from(duration.as_micros()).unwrap_or(i64::MAX),
        Err(_) => 0,
    }
}

pub fn parse_i64(raw: &Bytes) -> Option<i64> {
    let bytes = raw.as_ref();
    if bytes.is_empty() {
        return None;
    }

    let (start, negative) = match bytes[0] {
        b'-' => (1usize, true),
        b'+' => (1usize, false),
        _ => (0usize, false),
    };

    if start >= bytes.len() {
        return None;
    }

    let mut acc = 0i64;
    if negative {
        for &byte in &bytes[start..] {
            if !byte.is_ascii_digit() {
                return None;
            }
            let digit = i64::from(byte - b'0');
            acc = acc.checked_mul(10)?.checked_sub(digit)?;
        }
    } else {
        for &byte in &bytes[start..] {
            if !byte.is_ascii_digit() {
                return None;
            }
            let digit = i64::from(byte - b'0');
            acc = acc.checked_mul(10)?.checked_add(digit)?;
        }
    }

    Some(acc)
}

pub fn parse_usize(raw: &Bytes) -> Option<usize> {
    let bytes = raw.as_ref();
    if bytes.is_empty() {
        return None;
    }

    let mut acc = 0usize;
    for &byte in bytes {
        if !byte.is_ascii_digit() {
            return None;
        }
        let digit = usize::from(byte - b'0');
        acc = acc.checked_mul(10)?.checked_add(digit)?;
    }

    Some(acc)
}

pub fn parse_f64(raw: &Bytes) -> Option<f64> {
    std::str::from_utf8(raw).ok()?.parse::<f64>().ok()
}

pub fn format_f64_for_redis(value: f64) -> Bytes {
    // Use ryu for fast formatting, then strip trailing ".0" for integers
    // to match Redis convention ("1" not "1.0").
    let mut buf = ryu::Buffer::new();
    let s = buf.format(value);
    let trimmed = s.strip_suffix(".0").unwrap_or(s);
    Bytes::copy_from_slice(trimmed.as_bytes())
}

/// Resolves a list or sorted-set index range (`LRANGE`, `LTRIM`, `ZRANGE`).
///
/// Negative indexes count from the end. As in Redis, a range whose end is
/// still negative after that, or which starts past the last element, is
/// empty.
pub fn normalize_range(len: usize, start: i64, end: i64) -> Option<(usize, usize)> {
    let len = i64::try_from(len).ok()?;
    let start = if start < 0 {
        start.saturating_add(len).max(0)
    } else {
        start
    };
    let end = if end < 0 {
        end.saturating_add(len)
    } else {
        end
    };
    if start > end || start >= len {
        return None;
    }
    Some((start as usize, end.min(len - 1) as usize))
}

/// Resolves a `GETRANGE` byte range.
///
/// Redis treats strings differently from lists here: a still-negative end is
/// clamped to 0, so `GETRANGE s 0 -100` returns the first byte, while two
/// negative indexes in the wrong order return nothing.
pub fn normalize_string_range(len: usize, start: i64, end: i64) -> Option<(usize, usize)> {
    if start < 0 && end < 0 && start > end {
        return None;
    }
    clamp_index_range(len, start, end)
}

/// Resolves a range by clamping both ends into the value, the rule `BITPOS`
/// uses (and `GETRANGE`/`BITCOUNT` after their negative-order check).
pub fn clamp_index_range(len: usize, start: i64, end: i64) -> Option<(usize, usize)> {
    let len = i64::try_from(len).ok()?;
    if len == 0 {
        return None;
    }
    let start = if start < 0 {
        start.saturating_add(len).max(0)
    } else {
        start
    };
    let end = if end < 0 {
        end.saturating_add(len).max(0)
    } else {
        end
    }
    .min(len - 1);
    if start > end {
        return None;
    }
    Some((start as usize, end as usize))
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;

    use super::{normalize_range, normalize_string_range, parse_i64, parse_usize};

    #[test]
    fn normalize_range_deeply_negative_both_out_of_range() {
        assert_eq!(normalize_range(3, -10, -10), None);
    }

    #[test]
    fn list_ranges_are_empty_when_the_end_stays_negative() {
        assert_eq!(normalize_range(3, 0, -100), None);
        assert_eq!(normalize_range(3, -100, -100), None);
        assert_eq!(normalize_range(3, 1, 100), Some((1, 2)));
        assert_eq!(normalize_range(3, 3, 5), None);
        assert_eq!(normalize_range(0, 0, -1), None);
        assert_eq!(normalize_range(3, i64::MIN, i64::MAX), Some((0, 2)));
    }

    #[test]
    fn string_ranges_clamp_a_negative_end_like_getrange() {
        assert_eq!(normalize_string_range(3, 0, -100), Some((0, 0)));
        assert_eq!(normalize_string_range(3, -10, -10), Some((0, 0)));
        assert_eq!(normalize_string_range(3, -1, -2), None);
        assert_eq!(normalize_string_range(3, 1, 100), Some((1, 2)));
        assert_eq!(normalize_string_range(3, 5, 10), None);
        assert_eq!(normalize_string_range(0, 0, -1), None);
    }

    #[test]
    fn normalize_range_deeply_negative_end_valid() {
        assert_eq!(normalize_range(3, -10, -1), Some((0, 2)));
    }

    #[test]
    fn normalize_range_valid_negative() {
        assert_eq!(normalize_range(3, -3, -1), Some((0, 2)));
    }

    #[test]
    fn normalize_range_positive_indices() {
        assert_eq!(normalize_range(3, 0, 0), Some((0, 0)));
    }

    #[test]
    fn parse_i64_handles_bounds() {
        assert_eq!(
            parse_i64(&Bytes::from("-9223372036854775808")),
            Some(i64::MIN)
        );
        assert_eq!(
            parse_i64(&Bytes::from("9223372036854775807")),
            Some(i64::MAX)
        );
        assert_eq!(parse_i64(&Bytes::from("9223372036854775808")), None);
    }

    #[test]
    fn parse_usize_rejects_signs() {
        assert_eq!(parse_usize(&Bytes::from("123")), Some(123));
        assert_eq!(parse_usize(&Bytes::from("+123")), None);
        assert_eq!(parse_usize(&Bytes::from("-1")), None);
    }
}
