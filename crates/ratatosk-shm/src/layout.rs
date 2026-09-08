//! Fixed layout of the shared segment.
//!
//! ```text
//! offset 0       magic (u64)            "RTKSHM\0\1"
//! offset 8       version (u32)
//! offset 12      ring_bytes (u32)       per direction, power of two
//! offset 128     c2s.tail   (u64)       written by client (producer)
//! offset 256     c2s.head   (u64)       written by server (consumer)
//! offset 384     c2s.consumer_parked (u32)   written by server
//! offset 512     c2s.producer_parked (u32)   written by client
//! offset 640     s2c.tail   (u64)       written by server
//! offset 768     s2c.head   (u64)       written by client
//! offset 896     s2c.consumer_parked (u32)   written by client
//! offset 1024    s2c.producer_parked (u32)   written by server
//! offset 4096    c2s data  [ring_bytes]
//! offset 4096+R  s2c data  [ring_bytes]
//! ```
//!
//! Every control word sits on its own 128-byte line so producer and consumer
//! never share a cache line.

pub const MAGIC: u64 = u64::from_le_bytes(*b"RTKSHM\0\x01");
pub const VERSION: u32 = 1;

pub const HEADER_BYTES: usize = 4096;
pub const LINE: usize = 128;

pub const OFF_MAGIC: usize = 0;
pub const OFF_VERSION: usize = 8;
pub const OFF_RING_BYTES: usize = 12;

pub const OFF_C2S_TAIL: usize = LINE;
pub const OFF_C2S_HEAD: usize = 2 * LINE;
pub const OFF_C2S_CONSUMER_PARKED: usize = 3 * LINE;
pub const OFF_C2S_PRODUCER_PARKED: usize = 4 * LINE;
pub const OFF_S2C_TAIL: usize = 5 * LINE;
pub const OFF_S2C_HEAD: usize = 6 * LINE;
pub const OFF_S2C_CONSUMER_PARKED: usize = 7 * LINE;
pub const OFF_S2C_PRODUCER_PARKED: usize = 8 * LINE;

/// Smallest ring the transport accepts per direction.
pub const MIN_RING_BYTES: u32 = 4096;
/// Largest ring the transport accepts per direction (64 MiB).
pub const MAX_RING_BYTES: u32 = 64 * 1024 * 1024;
/// Default ring size per direction (1 MiB).
pub const DEFAULT_RING_BYTES: u32 = 1024 * 1024;

/// Validate a requested ring size: power of two within bounds.
pub fn validate_ring_bytes(ring_bytes: u32) -> Result<u32, LayoutError> {
    if !ring_bytes.is_power_of_two() || !(MIN_RING_BYTES..=MAX_RING_BYTES).contains(&ring_bytes) {
        return Err(LayoutError::InvalidRingBytes(ring_bytes));
    }
    Ok(ring_bytes)
}

/// Total segment size for a given per-direction ring size.
pub fn segment_bytes(ring_bytes: u32) -> usize {
    HEADER_BYTES + 2 * ring_bytes as usize
}

pub const fn off_c2s_data() -> usize {
    HEADER_BYTES
}

pub fn off_s2c_data(ring_bytes: u32) -> usize {
    HEADER_BYTES + ring_bytes as usize
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LayoutError {
    InvalidRingBytes(u32),
    BadMagic(u64),
    BadVersion(u32),
    SegmentTooSmall { expected: usize, actual: usize },
}

impl std::fmt::Display for LayoutError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InvalidRingBytes(v) => write!(
                f,
                "ring size {v} is not a power of two in [{MIN_RING_BYTES}, {MAX_RING_BYTES}]"
            ),
            Self::BadMagic(v) => write!(f, "segment magic {v:#x} does not match {MAGIC:#x}"),
            Self::BadVersion(v) => write!(f, "segment version {v} is not {VERSION}"),
            Self::SegmentTooSmall { expected, actual } => {
                write!(f, "segment is {actual} bytes, expected at least {expected}")
            }
        }
    }
}

impl std::error::Error for LayoutError {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ring_bytes_bounds() {
        assert!(validate_ring_bytes(4096).is_ok());
        assert!(validate_ring_bytes(1 << 20).is_ok());
        assert!(validate_ring_bytes(4095).is_err());
        assert!(validate_ring_bytes(3 * 4096).is_err());
        assert!(validate_ring_bytes(MAX_RING_BYTES * 2).is_err());
    }

    #[test]
    fn control_words_do_not_share_lines() {
        let offsets = [
            OFF_C2S_TAIL,
            OFF_C2S_HEAD,
            OFF_C2S_CONSUMER_PARKED,
            OFF_C2S_PRODUCER_PARKED,
            OFF_S2C_TAIL,
            OFF_S2C_HEAD,
            OFF_S2C_CONSUMER_PARKED,
            OFF_S2C_PRODUCER_PARKED,
        ];
        for pair in offsets.windows(2) {
            assert!(pair[1] - pair[0] >= LINE);
        }
        assert!(offsets[offsets.len() - 1] + LINE <= HEADER_BYTES);
    }
}
