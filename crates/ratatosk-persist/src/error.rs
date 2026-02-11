/// Persistence error type.
#[derive(Debug, thiserror::Error)]
pub enum PersistError {
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    #[error("invalid RDB magic bytes")]
    InvalidMagic,

    #[error("unsupported RDB version: {version}")]
    UnsupportedVersion { version: u32 },

    #[error("CRC64 checksum mismatch: expected {expected:#018x}, got {actual:#018x}")]
    CrcMismatch { expected: u64, actual: u64 },

    #[error("corrupt RDB data: {reason}")]
    Corrupt { reason: String },

    #[error("unexpected EOF while reading RDB")]
    UnexpectedEof,

    #[error("unknown type byte: {type_byte:#04x}")]
    UnknownType { type_byte: u8 },
}

impl PersistError {
    pub fn corrupt(reason: impl Into<String>) -> Self {
        Self::Corrupt {
            reason: reason.into(),
        }
    }
}
