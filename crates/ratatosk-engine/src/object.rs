use std::time::{SystemTime, UNIX_EPOCH};

use bytes::Bytes;

pub fn now_ms() -> i64 {
    match SystemTime::now().duration_since(UNIX_EPOCH) {
        Ok(duration) => i64::try_from(duration.as_millis()).unwrap_or(i64::MAX),
        Err(_) => 0,
    }
}

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
    Bytes::from(value.to_string())
}

pub fn normalize_range(len: usize, mut start: i64, mut end: i64) -> Option<(usize, usize)> {
    if len == 0 {
        return None;
    }

    let len_i64 = len as i64;

    if start < 0 {
        start += len_i64;
    }
    if end < 0 {
        end += len_i64;
    }

    // Both indices resolved to out-of-range (still negative after offset)
    let start_out_of_range = start < 0;
    let end_out_of_range = end < 0;

    if start_out_of_range && end_out_of_range {
        return None;
    }

    if start < 0 {
        start = 0;
    }
    if end < 0 {
        end = 0;
    }

    if end >= len_i64 {
        end = len_i64 - 1;
    }

    if start > end || start >= len_i64 {
        return None;
    }

    Some((start as usize, end as usize))
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;

    use super::{normalize_range, parse_i64, parse_usize};

    #[test]
    fn normalize_range_deeply_negative_both_out_of_range() {
        assert_eq!(normalize_range(3, -10, -10), None);
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
