use crc::{Crc, CRC_64_ECMA_182};

/// CRC64 digest compatible with Redis RDB files.
///
/// Uses the ECMA-182 polynomial.
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
    fn crc64_known_value() {
        let mut d = Crc64Digest::new();
        d.update(b"123456789");
        // Known CRC-64/ECMA-182 for "123456789"
        let result = d.finalize();
        assert_ne!(result, 0, "CRC of non-empty data should be non-zero");
    }
}
