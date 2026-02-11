//! HyperLogLog cardinality estimation (dense representation).
//!
//! Implements the HyperLogLog algorithm as used by Redis: 16384 6-bit registers
//! packed into a 12288-byte dense encoding, preceded by a 16-byte header that
//! stores a magic marker, encoding byte, and a cached cardinality value.

const HLL_P: u8 = 14;
const HLL_REGISTERS: usize = 1 << HLL_P; // 16384
const HLL_BITS: u8 = 6;
const HLL_REGISTER_BYTES: usize = (HLL_REGISTERS * HLL_BITS as usize).div_ceil(8); // 12288
const HLL_HEADER_SIZE: usize = 16;
const HLL_MAGIC: &[u8; 4] = b"HYLL";
const HLL_DENSE: u8 = 0;
const HLL_SEED: u64 = 0xadc83b19;

/// Total size of a serialized HLL: 16-byte header + 12288 bytes of registers.
pub const HLL_SIZE: usize = HLL_HEADER_SIZE + HLL_REGISTER_BYTES;

// ---------------------------------------------------------------------------
// MurmurHash64A
// ---------------------------------------------------------------------------

/// MurmurHash64A — the hash function Redis uses for HyperLogLog.
pub fn murmur_hash_64a(data: &[u8], seed: u64) -> u64 {
    const M: u64 = 0xc6a4_a793_5bd1_e995;
    const R: u32 = 47;

    let len = data.len();
    let mut h: u64 = seed ^ (len as u64).wrapping_mul(M);

    // Process 8-byte chunks.
    let chunks = len / 8;
    for i in 0..chunks {
        let base = i * 8;
        let mut k = u64::from_le_bytes([
            data[base],
            data[base + 1],
            data[base + 2],
            data[base + 3],
            data[base + 4],
            data[base + 5],
            data[base + 6],
            data[base + 7],
        ]);
        k = k.wrapping_mul(M);
        k ^= k >> R;
        k = k.wrapping_mul(M);

        h ^= k;
        h = h.wrapping_mul(M);
    }

    // Handle remaining bytes (fall-through style, MSB-first).
    let tail = &data[chunks * 8..];
    if tail.len() >= 7 {
        h ^= (tail[6] as u64) << 48;
    }
    if tail.len() >= 6 {
        h ^= (tail[5] as u64) << 40;
    }
    if tail.len() >= 5 {
        h ^= (tail[4] as u64) << 32;
    }
    if tail.len() >= 4 {
        h ^= (tail[3] as u64) << 24;
    }
    if tail.len() >= 3 {
        h ^= (tail[2] as u64) << 16;
    }
    if tail.len() >= 2 {
        h ^= (tail[1] as u64) << 8;
    }
    if !tail.is_empty() {
        h ^= tail[0] as u64;
        h = h.wrapping_mul(M);
    }

    // Finalization mix.
    h ^= h >> R;
    h = h.wrapping_mul(M);
    h ^= h >> R;
    h
}

// ---------------------------------------------------------------------------
// Blob lifecycle
// ---------------------------------------------------------------------------

/// Create a new empty HLL blob (16384 registers, all zero).
pub fn hll_create() -> Vec<u8> {
    let mut buf = vec![0u8; HLL_SIZE];
    buf[0..4].copy_from_slice(HLL_MAGIC);
    buf[4] = HLL_DENSE;
    // Bytes 5..8: reserved (zero).
    // Bytes 8..16: cached cardinality as little-endian i64 (-1 = dirty/invalid).
    let invalid: i64 = -1;
    buf[8..16].copy_from_slice(&invalid.to_le_bytes());
    buf
}

/// Return `true` when `data` looks like a valid dense HLL blob.
pub fn hll_is_valid(data: &[u8]) -> bool {
    data.len() == HLL_SIZE && data[0..4] == *HLL_MAGIC && data[4] == HLL_DENSE
}

// ---------------------------------------------------------------------------
// Register access (6-bit packed)
// ---------------------------------------------------------------------------

/// Read a single 6-bit register value (0..63).
fn hll_get_register(data: &[u8], index: usize) -> u8 {
    let bit_offset = HLL_HEADER_SIZE * 8 + index * HLL_BITS as usize;
    let byte_idx = bit_offset / 8;
    let bit_idx = bit_offset % 8;

    if bit_idx + HLL_BITS as usize <= 8 {
        // Entire register fits in one byte.
        (data[byte_idx] >> bit_idx) & 0x3F
    } else {
        // Register spans two bytes.
        let lo = data[byte_idx] >> bit_idx;
        let hi = data[byte_idx + 1] << (8 - bit_idx);
        (lo | hi) & 0x3F
    }
}

/// Write a single 6-bit register value (0..63).
fn hll_set_register(data: &mut [u8], index: usize, value: u8) {
    let bit_offset = HLL_HEADER_SIZE * 8 + index * HLL_BITS as usize;
    let byte_idx = bit_offset / 8;
    let bit_idx = bit_offset % 8;
    let val = value & 0x3F;

    if bit_idx + HLL_BITS as usize <= 8 {
        // Entire register fits in one byte.
        data[byte_idx] &= !(0x3F << bit_idx);
        data[byte_idx] |= val << bit_idx;
    } else {
        // Register spans two bytes.
        let bits_in_first = 8 - bit_idx;
        // Clear + set lower bits in the first byte.
        data[byte_idx] &= !(0xFF << bit_idx);
        data[byte_idx] |= val << bit_idx;
        // Clear + set upper bits in the second byte.
        let remaining = HLL_BITS as usize - bits_in_first;
        data[byte_idx + 1] &= 0xFF << remaining;
        data[byte_idx + 1] |= val >> bits_in_first;
    }
}

// ---------------------------------------------------------------------------
// Cache helpers
// ---------------------------------------------------------------------------

/// Mark the cached cardinality as invalid (set header to -1).
fn hll_invalidate_cache(data: &mut [u8]) {
    let invalid: i64 = -1;
    data[8..16].copy_from_slice(&invalid.to_le_bytes());
}

/// Read the cached cardinality from the header (-1 means invalid).
fn hll_get_cached(data: &[u8]) -> i64 {
    i64::from_le_bytes([
        data[8], data[9], data[10], data[11], data[12], data[13], data[14], data[15],
    ])
}

/// Write a cached cardinality into the header.
fn hll_set_cached(data: &mut [u8], card: i64) {
    data[8..16].copy_from_slice(&card.to_le_bytes());
}

// ---------------------------------------------------------------------------
// Add element
// ---------------------------------------------------------------------------

/// Add an element to the HLL.  Returns `true` if any register was updated
/// (i.e. the internal representation changed), `false` otherwise.
pub fn hll_add(data: &mut Vec<u8>, element: &[u8]) -> bool {
    if data.len() != HLL_SIZE {
        *data = hll_create();
    }

    let hash = murmur_hash_64a(element, HLL_SEED);

    // Lowest HLL_P (14) bits select the register index.
    let index = (hash & ((HLL_REGISTERS as u64) - 1)) as usize;

    // Remaining 50 bits are used to count the run of zeros.
    // "count" = position of the first 1-bit (from the right) + 1.
    let remaining = hash >> HLL_P;
    let count = if remaining == 0 {
        // All 50 remaining bits are zero ⇒ run length = 50 + 1 = 51.
        (64 - HLL_P) + 1
    } else {
        (remaining.trailing_zeros() as u8) + 1
    };
    // 6-bit register can store 0..63; cap at 63.
    let count = count.min(63);

    let old = hll_get_register(data, index);
    if count > old {
        hll_set_register(data, index, count);
        hll_invalidate_cache(data);
        true
    } else {
        false
    }
}

// ---------------------------------------------------------------------------
// Cardinality estimation
// ---------------------------------------------------------------------------

/// Internal: compute the raw HLL cardinality estimate from register values.
fn hll_raw_count(data: &[u8]) -> i64 {
    let m = HLL_REGISTERS as f64;
    let mut sum = 0.0_f64;
    let mut zeros = 0_u32;

    for i in 0..HLL_REGISTERS {
        let reg = hll_get_register(data, i);
        // 2^(-reg).  When reg == 0 this contributes 1.0, which also counts
        // towards the linear-counting correction below.
        sum += 1.0 / (1_u64 << reg) as f64;
        if reg == 0 {
            zeros += 1;
        }
    }

    // Alpha correction factor for m = 16384 (the standard constant).
    let alpha = 0.7213 / (1.0 + 1.079 / m);
    let mut estimate = alpha * m * m / sum;

    // Small-range correction via linear counting when many registers are zero.
    if estimate <= 5.0 * m / 2.0 && zeros > 0 {
        estimate = m * (m / zeros as f64).ln();
    }

    estimate as i64
}

/// Return the cardinality estimate, using the cached value when available.
/// This is a read-only variant: it will not update the cache in the header.
pub fn hll_count(data: &[u8]) -> i64 {
    if data.len() != HLL_SIZE {
        return 0;
    }
    let cached = hll_get_cached(data);
    if cached >= 0 {
        return cached;
    }
    hll_raw_count(data)
}

/// Return the cardinality estimate and store it in the header cache.
pub fn hll_count_and_cache(data: &mut [u8]) -> i64 {
    if data.len() != HLL_SIZE {
        return 0;
    }
    let cached = hll_get_cached(data);
    if cached >= 0 {
        return cached;
    }
    let count = hll_raw_count(data);
    hll_set_cached(data, count);
    count
}

// ---------------------------------------------------------------------------
// Merge
// ---------------------------------------------------------------------------

/// Merge one or more source HLLs into `dest` by taking the max register value
/// at every position.  Sources that are not valid HLL blobs are silently
/// skipped.  The cached cardinality of `dest` is invalidated.
pub fn hll_merge(dest: &mut Vec<u8>, sources: &[&[u8]]) {
    if dest.len() != HLL_SIZE {
        *dest = hll_create();
    }
    for source in sources {
        if source.len() != HLL_SIZE {
            continue;
        }
        for i in 0..HLL_REGISTERS {
            let src_val = hll_get_register(source, i);
            let dst_val = hll_get_register(dest, i);
            if src_val > dst_val {
                hll_set_register(dest, i, src_val);
            }
        }
    }
    hll_invalidate_cache(dest);
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_create_produces_valid_blob() {
        let blob = hll_create();
        assert_eq!(blob.len(), HLL_SIZE);
        assert!(hll_is_valid(&blob));
        assert_eq!(&blob[0..4], b"HYLL");
        assert_eq!(blob[4], HLL_DENSE);
        // Cached cardinality must be -1 (invalid).
        assert_eq!(hll_get_cached(&blob), -1);
    }

    #[test]
    fn test_is_valid_rejects_wrong_size() {
        assert!(!hll_is_valid(&[]));
        assert!(!hll_is_valid(&[0u8; 100]));
        let mut bad = hll_create();
        bad.push(0);
        assert!(!hll_is_valid(&bad));
    }

    #[test]
    fn test_is_valid_rejects_wrong_magic() {
        let mut blob = hll_create();
        blob[0] = b'X';
        assert!(!hll_is_valid(&blob));
    }

    #[test]
    fn test_register_roundtrip() {
        let mut blob = hll_create();
        for i in 0..HLL_REGISTERS {
            assert_eq!(hll_get_register(&blob, i), 0);
        }
        // Set a few registers to known values and verify.
        hll_set_register(&mut blob, 0, 63);
        hll_set_register(&mut blob, 1, 1);
        hll_set_register(&mut blob, 100, 42);
        hll_set_register(&mut blob, HLL_REGISTERS - 1, 33);

        assert_eq!(hll_get_register(&blob, 0), 63);
        assert_eq!(hll_get_register(&blob, 1), 1);
        assert_eq!(hll_get_register(&blob, 100), 42);
        assert_eq!(hll_get_register(&blob, HLL_REGISTERS - 1), 33);
        // Untouched register stays zero.
        assert_eq!(hll_get_register(&blob, 50), 0);
    }

    #[test]
    fn test_register_boundary_values() {
        let mut blob = hll_create();
        // Every possible 6-bit value at a register that spans two bytes.
        // Register index 4: bit_offset = 16*8 + 4*6 = 152 → byte 19, bit 0
        // Register index 5: bit_offset = 16*8 + 5*6 = 158 → byte 19, bit 6 → spans bytes 19..20
        for val in 0..64 {
            hll_set_register(&mut blob, 5, val);
            assert_eq!(hll_get_register(&blob, 5), val);
        }
    }

    #[test]
    fn test_add_returns_true_on_first_insert() {
        let mut blob = hll_create();
        assert!(hll_add(&mut blob, b"hello"));
    }

    #[test]
    fn test_add_returns_false_on_duplicate() {
        let mut blob = hll_create();
        hll_add(&mut blob, b"hello");
        // Second add of the same element should return false (same register,
        // same or lower count).
        assert!(!hll_add(&mut blob, b"hello"));
    }

    #[test]
    fn test_add_invalidates_cache() {
        let mut blob = hll_create();
        // Manually set a valid cache.
        hll_set_cached(&mut blob, 42);
        assert_eq!(hll_get_cached(&blob), 42);

        hll_add(&mut blob, b"world");
        // After a successful add the cache must be invalidated.
        assert_eq!(hll_get_cached(&blob), -1);
    }

    #[test]
    fn test_count_empty_hll() {
        let blob = hll_create();
        // All registers zero ⇒ cardinality should be 0.
        assert_eq!(hll_raw_count(&blob), 0);
    }

    #[test]
    fn test_count_uses_cache() {
        let mut blob = hll_create();
        hll_set_cached(&mut blob, 999);
        assert_eq!(hll_count(&blob), 999);
    }

    #[test]
    fn test_count_and_cache_stores_result() {
        let mut blob = hll_create();
        hll_add(&mut blob, b"a");
        assert_eq!(hll_get_cached(&blob), -1);
        let count = hll_count_and_cache(&mut blob);
        assert!(count >= 1);
        assert_eq!(hll_get_cached(&blob), count);
        // Calling again returns the same cached value.
        assert_eq!(hll_count_and_cache(&mut blob), count);
    }

    #[test]
    fn test_cardinality_estimate_accuracy() {
        // Add 1000 distinct elements and verify the estimate is within 10%.
        let mut blob = hll_create();
        for i in 0..1000_u32 {
            hll_add(&mut blob, &i.to_le_bytes());
        }
        let estimate = hll_raw_count(&blob);
        let error_pct = ((estimate - 1000) as f64 / 1000.0).abs();
        assert!(
            error_pct < 0.10,
            "estimate {estimate} deviates more than 10% from 1000 (error={error_pct:.3})"
        );
    }

    #[test]
    fn test_cardinality_large_set() {
        // Add 100_000 distinct elements.
        let mut blob = hll_create();
        for i in 0..100_000_u32 {
            hll_add(&mut blob, &i.to_le_bytes());
        }
        let estimate = hll_raw_count(&blob);
        let error_pct = ((estimate - 100_000) as f64 / 100_000.0).abs();
        assert!(
            error_pct < 0.05,
            "estimate {estimate} deviates more than 5% from 100000 (error={error_pct:.3})"
        );
    }

    #[test]
    fn test_merge_takes_max_registers() {
        let mut a = hll_create();
        let mut b = hll_create();

        for i in 0..500_u32 {
            hll_add(&mut a, &i.to_le_bytes());
        }
        for i in 500..1000_u32 {
            hll_add(&mut b, &i.to_le_bytes());
        }

        let mut merged = hll_create();
        hll_merge(&mut merged, &[&a, &b]);

        let count_merged = hll_raw_count(&merged);
        let error_pct = ((count_merged - 1000) as f64 / 1000.0).abs();
        assert!(
            error_pct < 0.10,
            "merged estimate {count_merged} deviates more than 10% from 1000 (error={error_pct:.3})"
        );
    }

    #[test]
    fn test_merge_skips_invalid_sources() {
        let mut dest = hll_create();
        hll_add(&mut dest, b"x");
        let before = hll_raw_count(&dest);

        let bad_source = vec![0u8; 100];
        hll_merge(&mut dest, &[&bad_source]);

        // Merge with an invalid source should not corrupt the destination.
        assert!(hll_is_valid(&dest));
        assert_eq!(hll_raw_count(&dest), before);
    }

    #[test]
    fn test_merge_idempotent_with_self() {
        let mut blob = hll_create();
        for i in 0..500_u32 {
            hll_add(&mut blob, &i.to_le_bytes());
        }
        let count_before = hll_raw_count(&blob);

        let clone = blob.clone();
        hll_merge(&mut blob, &[&clone]);

        let count_after = hll_raw_count(&blob);
        assert_eq!(count_before, count_after);
    }

    #[test]
    fn test_murmur_hash_deterministic() {
        let h1 = murmur_hash_64a(b"hello", 0);
        let h2 = murmur_hash_64a(b"hello", 0);
        assert_eq!(h1, h2);
    }

    #[test]
    fn test_murmur_hash_different_inputs() {
        let h1 = murmur_hash_64a(b"hello", 0);
        let h2 = murmur_hash_64a(b"world", 0);
        assert_ne!(h1, h2);
    }

    #[test]
    fn test_murmur_hash_empty_input() {
        // Should not panic.
        let h = murmur_hash_64a(b"", HLL_SEED);
        // The result is deterministic (seed-dependent).
        assert_eq!(h, murmur_hash_64a(b"", HLL_SEED));
    }

    #[test]
    fn test_count_invalid_blob_returns_zero() {
        assert_eq!(hll_count(&[]), 0);
        assert_eq!(hll_count(&[0u8; 100]), 0);
    }

    #[test]
    fn test_count_and_cache_invalid_blob_returns_zero() {
        let mut bad = vec![0u8; 100];
        assert_eq!(hll_count_and_cache(&mut bad), 0);
    }

    #[test]
    fn test_add_reinitializes_wrong_size_buffer() {
        let mut blob = vec![0u8; 10];
        hll_add(&mut blob, b"test");
        // After add, the buffer should have been re-created.
        assert_eq!(blob.len(), HLL_SIZE);
        assert!(hll_is_valid(&blob));
    }

    #[test]
    fn test_merge_reinitializes_wrong_size_dest() {
        let mut dest = vec![0u8; 10];
        let src = hll_create();
        hll_merge(&mut dest, &[&src]);
        assert_eq!(dest.len(), HLL_SIZE);
        assert!(hll_is_valid(&dest));
    }
}
