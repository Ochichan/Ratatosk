/// RDB file magic bytes: "REDIS"
pub const RDB_MAGIC: &[u8; 5] = b"REDIS";

/// RDB version we produce (compatible with Redis 7+)
pub const RDB_VERSION: u32 = 12;

/// RDB version string as 4 ASCII digits
pub const RDB_VERSION_STR: &[u8; 4] = b"0012";

// ---------------------------------------------------------------------------
// RDB type bytes — one per value type
// ---------------------------------------------------------------------------

pub const RDB_TYPE_STRING: u8 = 0;
pub const RDB_TYPE_LIST: u8 = 1;
pub const RDB_TYPE_SET: u8 = 2;
pub const RDB_TYPE_ZSET: u8 = 5;
pub const RDB_TYPE_HASH: u8 = 4;
pub const RDB_TYPE_STREAM: u8 = 19;

// ---------------------------------------------------------------------------
// RDB opcodes
// ---------------------------------------------------------------------------

/// Next key has millisecond-precision expiry (8-byte LE follows)
pub const RDB_OPCODE_EXPIRETIME_MS: u8 = 0xFC;

/// Next key has second-precision expiry (4-byte LE follows)
pub const RDB_OPCODE_EXPIRETIME: u8 = 0xFD;

/// Select DB — followed by length-encoded DB number
pub const RDB_OPCODE_SELECTDB: u8 = 0xFE;

/// End of file — followed by 8-byte CRC64
pub const RDB_OPCODE_EOF: u8 = 0xFF;

/// Resize DB hint — followed by db_size and expires_size
pub const RDB_OPCODE_RESIZEDB: u8 = 0xFB;

/// Auxiliary field (key-value metadata like redis-ver, ctime, etc.)
pub const RDB_OPCODE_AUX: u8 = 0xFA;

// ---------------------------------------------------------------------------
// Length encoding
// ---------------------------------------------------------------------------

/// RDB length encoding constants.
/// 00xxxxxx = 6-bit length (0..63)
/// 01xxxxxx xxxxxxxx = 14-bit length (0..16383)
/// 10000000 xxxxxxxx xxxxxxxx xxxxxxxx xxxxxxxx = 32-bit length
/// 10000001 xxxxxxxx*8 = 64-bit length
/// 11xxxxxx = special encoding (integer, LZF compressed)
pub const RDB_6BITLEN: u8 = 0;
pub const RDB_14BITLEN: u8 = 1;
pub const RDB_32BITLEN: u8 = 0x80;
pub const RDB_64BITLEN: u8 = 0x81;
pub const RDB_ENCVAL: u8 = 3;

/// Special encoding sub-types
pub const RDB_ENC_INT8: u8 = 0;
pub const RDB_ENC_INT16: u8 = 1;
pub const RDB_ENC_INT32: u8 = 2;
