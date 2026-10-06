use crc::{CRC_64_ECMA_182, Crc};

/// CRC-64/ECMA-182 digest over everything before the trailing checksum.
///
/// This is not Redis' CRC-64 (Jones coefficients, reflected), so Ratatosk
/// snapshots and Redis `dump.rdb` files do not verify against each other.
/// Changing the polynomial would invalidate every existing snapshot.
pub struct Crc64Digest {
    crc: Crc<u64>,
    state: u64,
}

impl Default for Crc64Digest {
    fn default() -> Self {
        Self::new()
    }
}

impl Crc64Digest {
    pub fn new() -> Self {
        Self {
            crc: Crc::<u64>::new(&CRC_64_ECMA_182),
            state: 0,
        }
    }

    pub fn update(&mut self, data: &[u8]) {
        let mut digest = self.crc.digest_with_initial(self.state);
        digest.update(data);
        self.state = digest.finalize();
    }

    pub fn finalize(self) -> u64 {
        self.state
    }

    pub fn value(&self) -> u64 {
        self.state
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn crc64_deterministic() {
        let mut d1 = Crc64Digest::new();
        d1.update(b"hello");
        d1.update(b" world");

        let mut d2 = Crc64Digest::new();
        d2.update(b"hello world");

        assert_eq!(d1.finalize(), d2.finalize());
    }

    #[test]
    fn crc64_empty_is_zero() {
        let d = Crc64Digest::new();
        assert_eq!(d.finalize(), 0);
    }

    #[test]
    fn crc64_matches_the_ecma_182_check_value() {
        // Catalogued check value of CRC-64/ECMA-182 for "123456789". Pinning
        // it keeps existing snapshots loadable across dependency upgrades.
        let mut d = Crc64Digest::new();
        d.update(b"1234");
        d.update(b"56789");
        assert_eq!(d.finalize(), 0x6c40_df5f_0b49_7347);
    }
}
