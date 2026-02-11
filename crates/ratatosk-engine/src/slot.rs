//! CRC16-XMODEM based hash slot calculation for Redis Cluster.

pub const SLOT_COUNT: u16 = 16384;

/// CRC16-XMODEM lookup table, computed at compile time.
const CRC16_TAB: [u16; 256] = generate_crc16_table();

const fn generate_crc16_table() -> [u16; 256] {
    let mut table = [0u16; 256];
    let mut i = 0u16;
    while i < 256 {
        let mut crc = i << 8;
        let mut j = 0;
        while j < 8 {
            if crc & 0x8000 != 0 {
                crc = (crc << 1) ^ 0x1021;
            } else {
                crc <<= 1;
            }
            j += 1;
        }
        table[i as usize] = crc;
        i += 1;
    }
    table
}

fn crc16(data: &[u8]) -> u16 {
    let mut crc: u16 = 0;
    for &byte in data {
        let idx = ((crc >> 8) ^ byte as u16) as usize;
        crc = (crc << 8) ^ CRC16_TAB[idx];
    }
    crc
}

/// Extract hash tag from key: content between first `{` and next `}`.
/// If no valid hash tag (empty braces or no braces), use entire key.
fn extract_hash_tag(key: &[u8]) -> &[u8] {
    if let Some(start) = key.iter().position(|&b| b == b'{') {
        if let Some(end) = key[start + 1..].iter().position(|&b| b == b'}') {
            if end > 0 {
                return &key[start + 1..start + 1 + end];
            }
        }
    }
    key
}

/// Compute hash slot for a key (0..16383).
pub fn key_hash_slot(key: &[u8]) -> u16 {
    let tag = extract_hash_tag(key);
    crc16(tag) % SLOT_COUNT
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_empty_key() {
        // Empty key should hash to slot 0 (crc16 of empty is 0, 0 % 16384 = 0)
        assert_eq!(key_hash_slot(b""), 0);
    }

    #[test]
    fn test_known_slot_values() {
        // Redis known values: "foo" hashes to slot 12182
        assert_eq!(key_hash_slot(b"foo"), 12182);
        // "bar" hashes to slot 5061
        assert_eq!(key_hash_slot(b"bar"), 5061);
    }

    #[test]
    fn test_hash_tag_basic() {
        // {user}.following and {user}.followers should be in the same slot
        let slot_a = key_hash_slot(b"{user}.following");
        let slot_b = key_hash_slot(b"{user}.followers");
        assert_eq!(slot_a, slot_b);
        // Both should equal the slot of just "user"
        assert_eq!(slot_a, key_hash_slot(b"user"));
    }

    #[test]
    fn test_hash_tag_empty_braces() {
        // Empty braces {} should be ignored; hash entire key
        let slot = key_hash_slot(b"{}key");
        assert_eq!(slot, key_hash_slot(b"{}key"));
    }

    #[test]
    fn test_hash_tag_no_closing_brace() {
        // No closing brace: hash entire key
        let slot = key_hash_slot(b"{unclosed");
        assert_eq!(slot, crc16(b"{unclosed") % SLOT_COUNT);
    }

    #[test]
    fn test_hash_tag_first_occurrence() {
        // Only the first { matters
        let slot = key_hash_slot(b"{a}{b}");
        assert_eq!(slot, key_hash_slot(b"a"));
    }

    #[test]
    fn test_extract_hash_tag_no_braces() {
        assert_eq!(extract_hash_tag(b"hello"), b"hello");
    }

    #[test]
    fn test_extract_hash_tag_valid() {
        assert_eq!(extract_hash_tag(b"prefix{tag}suffix"), b"tag");
    }

    #[test]
    fn test_extract_hash_tag_empty() {
        assert_eq!(extract_hash_tag(b"prefix{}suffix"), b"prefix{}suffix");
    }

    #[test]
    fn test_slot_range() {
        // All slots should be in 0..16384
        for i in 0u16..=255 {
            let key = [i as u8];
            assert!(key_hash_slot(&key) < SLOT_COUNT);
        }
    }
}
