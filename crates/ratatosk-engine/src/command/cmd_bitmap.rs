use bytes::Bytes;

use ratatosk_resp::frame::RespFrame;

use crate::keyspace::{ServerState, StoredValue, purge_expired_key};

use super::{
    ClientState, CommandOutcome, err, now_ms, parse_i64, to_uppercase_bytes, wrong_arity,
    wrong_type_response,
};

// ---------------------------------------------------------------------------
// Bit manipulation helpers
// Redis uses big-endian bit order: bit 0 = MSB of byte[0].
// ---------------------------------------------------------------------------

fn get_bit(data: &[u8], offset: usize) -> u8 {
    let byte_idx = offset / 8;
    let bit_idx = 7 - (offset % 8);
    if byte_idx >= data.len() {
        0
    } else {
        (data[byte_idx] >> bit_idx) & 1
    }
}

fn set_bit(data: &mut Vec<u8>, offset: usize, value: u8) -> u8 {
    let byte_idx = offset / 8;
    let bit_idx = 7 - (offset % 8);
    if byte_idx >= data.len() {
        data.resize(byte_idx + 1, 0);
    }
    let old = (data[byte_idx] >> bit_idx) & 1;
    if value != 0 {
        data[byte_idx] |= 1 << bit_idx;
    } else {
        data[byte_idx] &= !(1 << bit_idx);
    }
    old
}

/// Count set bits in a byte (population count).
fn popcount_byte(b: u8) -> i64 {
    b.count_ones() as i64
}

// ---------------------------------------------------------------------------
// BITFIELD helpers
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum BitfieldOverflow {
    Wrap,
    Sat,
    Fail,
}

#[derive(Debug, Clone, Copy)]
struct BitfieldEncoding {
    bits: u8,
    signed: bool,
}

#[derive(Debug, Clone, Copy)]
enum BitfieldOp {
    Get {
        encoding: BitfieldEncoding,
        offset: usize,
    },
    Set {
        encoding: BitfieldEncoding,
        offset: usize,
        value: i64,
    },
    IncrBy {
        encoding: BitfieldEncoding,
        offset: usize,
        increment: i64,
    },
}

/// Parse a bitfield encoding like "u8", "i16", "u1", "i64".
fn parse_bitfield_encoding(raw: &Bytes) -> Option<BitfieldEncoding> {
    let s = std::str::from_utf8(raw).ok()?;
    if s.is_empty() {
        return None;
    }
    let (signed, rest) = match s.as_bytes()[0] {
        b'i' => (true, &s[1..]),
        b'u' => (false, &s[1..]),
        _ => return None,
    };
    let bits: u8 = rest.parse().ok()?;
    // Signed: 1..=64, unsigned: 1..=63
    if signed {
        if !(1..=64).contains(&bits) {
            return None;
        }
    } else if !(1..=63).contains(&bits) {
        return None;
    }
    Some(BitfieldEncoding { bits, signed })
}

/// Parse a bitfield offset. If prefixed with '#', multiply by encoding bits.
fn parse_bitfield_offset(raw: &Bytes, encoding: &BitfieldEncoding) -> Option<usize> {
    let s = std::str::from_utf8(raw).ok()?;
    if s.is_empty() {
        return None;
    }
    if let Some(rest) = s.strip_prefix('#') {
        let multiplier: usize = rest.parse().ok()?;
        Some(multiplier.checked_mul(encoding.bits as usize)?)
    } else {
        let offset: usize = s.parse().ok()?;
        Some(offset)
    }
}

/// Read `bits` bits starting at `bit_offset` from the data buffer.
fn read_bits(data: &[u8], bit_offset: usize, bits: u8, signed: bool) -> i64 {
    if bits == 0 {
        return 0;
    }
    let mut value: u64 = 0;
    for i in 0..bits as usize {
        let bit = get_bit(data, bit_offset + i);
        value = (value << 1) | (bit as u64);
    }
    if signed && bits > 0 && bits < 64 {
        // Sign-extend: if MSB is set, fill upper bits with 1s
        let sign_bit = 1u64 << (bits as u32 - 1);
        if value & sign_bit != 0 {
            // Fill upper bits
            let mask = !((1u64 << bits as u32) - 1);
            value |= mask;
        }
    } else if signed && bits == 64 {
        // All 64 bits, already correct
    }
    value as i64
}

/// Write `bits` bits starting at `bit_offset` into the data buffer.
fn write_bits(data: &mut Vec<u8>, bit_offset: usize, bits: u8, value: i64) {
    let raw = value as u64;
    for i in 0..bits as usize {
        let bit_pos = bits as usize - 1 - i;
        let bit_val = ((raw >> bit_pos as u32) & 1) as u8;
        set_bit(data, bit_offset + i, bit_val);
    }
}

/// Apply overflow policy to a bitfield operation result.
/// Returns None if FAIL overflow and the value overflows.
fn apply_overflow(
    value: i64,
    encoding: BitfieldEncoding,
    overflow: BitfieldOverflow,
) -> Option<i64> {
    let bits = encoding.bits;
    if encoding.signed {
        let min = if bits == 64 {
            i64::MIN
        } else {
            -(1i64 << (bits - 1))
        };
        let max = if bits == 64 {
            i64::MAX
        } else {
            (1i64 << (bits - 1)) - 1
        };

        if value >= min && value <= max {
            return Some(value);
        }

        match overflow {
            BitfieldOverflow::Fail => None,
            BitfieldOverflow::Sat => {
                if value < min {
                    Some(min)
                } else {
                    Some(max)
                }
            }
            BitfieldOverflow::Wrap => {
                if bits == 64 {
                    // For 64-bit signed, wrapping is native i64 behavior
                    Some(value)
                } else {
                    let range = 1u64 << bits as u32;
                    let mut v = value as u64;
                    v &= range - 1;
                    // Sign-extend
                    let sign_bit = 1u64 << (bits as u32 - 1);
                    if v & sign_bit != 0 {
                        v |= !((1u64 << bits as u32) - 1);
                    }
                    Some(v as i64)
                }
            }
        }
    } else {
        let max = if bits >= 64 {
            u64::MAX
        } else {
            (1u64 << bits as u32) - 1
        };

        let unsigned_val = value as u64;
        // Check if the value fits in bits
        // For unsigned, we check whether the value when masked equals original
        if bits < 64 {
            let masked = unsigned_val & max;
            if masked == unsigned_val && value >= 0 {
                return Some(value);
            }
            // Also need to handle the case where value was negative
            // (which means overflow in unsigned context)
            if value >= 0 && unsigned_val <= max {
                return Some(value);
            }
        } else {
            // 63-bit unsigned max
            if value >= 0 {
                return Some(value);
            }
        }

        match overflow {
            BitfieldOverflow::Fail => None,
            BitfieldOverflow::Sat => {
                if value < 0 {
                    Some(0)
                } else {
                    Some(max as i64)
                }
            }
            BitfieldOverflow::Wrap => Some((unsigned_val & max) as i64),
        }
    }
}

// ---------------------------------------------------------------------------
// SETBIT key offset value
// ---------------------------------------------------------------------------

pub(super) fn cmd_setbit(
    args: &[Bytes],
    server: &mut ServerState,
    client: &ClientState,
) -> CommandOutcome {
    let [key, offset_raw, value_raw] = args else {
        return wrong_arity("setbit");
    };

    let Some(offset) = parse_i64(offset_raw) else {
        return CommandOutcome::reply(err("ERR bit offset is not an integer or out of range"));
    };
    if offset < 0 {
        return CommandOutcome::reply(err("ERR bit offset is not an integer or out of range"));
    }
    let offset = offset as usize;

    // Maximum bit offset: 2^32 - 1 (512MB string)
    if offset >= 4_294_967_296 {
        return CommandOutcome::reply(err("ERR bit offset is not an integer or out of range"));
    }

    let Some(bit_value) = parse_i64(value_raw) else {
        return CommandOutcome::reply(err("ERR bit is not an integer or out of range"));
    };
    if bit_value != 0 && bit_value != 1 {
        return CommandOutcome::reply(err("ERR bit is not an integer or out of range"));
    }

    let now = now_ms();
    let db = server.db_mut(client.selected_db);
    purge_expired_key(db, key, now);

    let (mut data, expire_at_ms) = if let Some(existing) = db.get(key) {
        let Some(s) = existing.as_string() else {
            return wrong_type_response();
        };
        (s.to_vec(), existing.expire_at_ms)
    } else {
        (Vec::new(), None)
    };

    let old = set_bit(&mut data, offset, bit_value as u8);
    db.insert(
        key.clone(),
        StoredValue::string(Bytes::from(data), expire_at_ms),
    );

    CommandOutcome::reply(RespFrame::Integer(old as i64))
}

// ---------------------------------------------------------------------------
// GETBIT key offset
// ---------------------------------------------------------------------------

pub(super) fn cmd_getbit(
    args: &[Bytes],
    server: &mut ServerState,
    client: &ClientState,
) -> CommandOutcome {
    let [key, offset_raw] = args else {
        return wrong_arity("getbit");
    };

    let Some(offset) = parse_i64(offset_raw) else {
        return CommandOutcome::reply(err("ERR bit offset is not an integer or out of range"));
    };
    if offset < 0 {
        return CommandOutcome::reply(err("ERR bit offset is not an integer or out of range"));
    }
    let offset = offset as usize;

    let now = now_ms();
    let db = server.db_mut(client.selected_db);
    purge_expired_key(db, key, now);

    let Some(entry) = db.get(key) else {
        return CommandOutcome::reply(RespFrame::Integer(0));
    };
    let Some(data) = entry.as_string() else {
        return wrong_type_response();
    };

    CommandOutcome::reply(RespFrame::Integer(get_bit(data, offset) as i64))
}

// ---------------------------------------------------------------------------
// BITCOUNT key [start end [BYTE|BIT]]
// ---------------------------------------------------------------------------

pub(super) fn cmd_bitcount(
    args: &[Bytes],
    server: &mut ServerState,
    client: &ClientState,
) -> CommandOutcome {
    if args.is_empty() || args.len() == 2 || args.len() > 4 {
        return wrong_arity("bitcount");
    }

    let key = &args[0];

    let now = now_ms();
    let db = server.db_mut(client.selected_db);
    purge_expired_key(db, key, now);

    let Some(entry) = db.get(key) else {
        return CommandOutcome::reply(RespFrame::Integer(0));
    };
    let Some(data) = entry.as_string() else {
        return wrong_type_response();
    };

    if data.is_empty() {
        return CommandOutcome::reply(RespFrame::Integer(0));
    }

    // No range given: count all bits
    if args.len() == 1 {
        let count: i64 = data.iter().map(|b| popcount_byte(*b)).sum();
        return CommandOutcome::reply(RespFrame::Integer(count));
    }

    // Range given
    let Some(start_raw) = parse_i64(&args[1]) else {
        return CommandOutcome::reply(err("ERR value is not an integer or out of range"));
    };
    let Some(end_raw) = parse_i64(&args[2]) else {
        return CommandOutcome::reply(err("ERR value is not an integer or out of range"));
    };

    let bit_mode = if args.len() == 4 {
        let upper = to_uppercase_bytes(&args[3]);
        match upper.as_slice() {
            b"BYTE" => false,
            b"BIT" => true,
            _ => return CommandOutcome::reply(err("ERR syntax error")),
        }
    } else {
        false // default is BYTE mode
    };

    if bit_mode {
        let total_bits = data.len() * 8;
        let (start, end) = resolve_range(start_raw, end_raw, total_bits);
        if start > end || start >= total_bits {
            return CommandOutcome::reply(RespFrame::Integer(0));
        }
        let end = end.min(total_bits - 1);
        let mut count: i64 = 0;
        for bit_pos in start..=end {
            count += get_bit(data, bit_pos) as i64;
        }
        CommandOutcome::reply(RespFrame::Integer(count))
    } else {
        let byte_len = data.len();
        let (start, end) = resolve_range(start_raw, end_raw, byte_len);
        if start > end || start >= byte_len {
            return CommandOutcome::reply(RespFrame::Integer(0));
        }
        let end = end.min(byte_len - 1);
        let count: i64 = data[start..=end].iter().map(|b| popcount_byte(*b)).sum();
        CommandOutcome::reply(RespFrame::Integer(count))
    }
}

/// Resolve negative indexes to positive, clamping to valid range.
fn resolve_range(mut start: i64, mut end: i64, len: usize) -> (usize, usize) {
    let len_i64 = len as i64;
    if start < 0 {
        start += len_i64;
    }
    if end < 0 {
        end += len_i64;
    }
    if start < 0 {
        start = 0;
    }
    if end < 0 {
        // Both resolved negative: empty range
        return (1, 0);
    }
    (start as usize, end as usize)
}

// ---------------------------------------------------------------------------
// BITPOS key bit [start [end [BYTE|BIT]]]
// ---------------------------------------------------------------------------

pub(super) fn cmd_bitpos(
    args: &[Bytes],
    server: &mut ServerState,
    client: &ClientState,
) -> CommandOutcome {
    if args.len() < 2 || args.len() > 5 {
        return wrong_arity("bitpos");
    }

    let key = &args[0];
    let Some(target_bit) = parse_i64(&args[1]) else {
        return CommandOutcome::reply(err("ERR value is not an integer or out of range"));
    };
    if target_bit != 0 && target_bit != 1 {
        return CommandOutcome::reply(err("ERR bit must be 0 or 1"));
    }
    let target_bit = target_bit as u8;

    let now = now_ms();
    let db = server.db_mut(client.selected_db);
    purge_expired_key(db, key, now);

    let Some(entry) = db.get(key) else {
        // Empty key: bit 0 is always at position 0, bit 1 is not found
        if target_bit == 0 {
            return CommandOutcome::reply(RespFrame::Integer(0));
        }
        return CommandOutcome::reply(RespFrame::Integer(-1));
    };
    let Some(data) = entry.as_string() else {
        return wrong_type_response();
    };

    if data.is_empty() {
        if target_bit == 0 {
            return CommandOutcome::reply(RespFrame::Integer(0));
        }
        return CommandOutcome::reply(RespFrame::Integer(-1));
    }

    let has_explicit_end = args.len() >= 4;

    let bit_mode = if args.len() == 5 {
        let upper = to_uppercase_bytes(&args[4]);
        match upper.as_slice() {
            b"BYTE" => false,
            b"BIT" => true,
            _ => return CommandOutcome::reply(err("ERR syntax error")),
        }
    } else {
        false
    };

    let start_raw = if args.len() >= 3 {
        let Some(v) = parse_i64(&args[2]) else {
            return CommandOutcome::reply(err("ERR value is not an integer or out of range"));
        };
        v
    } else {
        0
    };

    let end_raw = if args.len() >= 4 {
        let Some(v) = parse_i64(&args[3]) else {
            return CommandOutcome::reply(err("ERR value is not an integer or out of range"));
        };
        v
    } else if bit_mode {
        (data.len() * 8) as i64 - 1
    } else {
        data.len() as i64 - 1
    };

    if bit_mode {
        let total_bits = data.len() * 8;
        let (start, end) = resolve_range(start_raw, end_raw, total_bits);
        if start > end || start >= total_bits {
            return CommandOutcome::reply(RespFrame::Integer(-1));
        }
        let end = end.min(total_bits - 1);
        for pos in start..=end {
            if get_bit(data, pos) == target_bit {
                return CommandOutcome::reply(RespFrame::Integer(pos as i64));
            }
        }
        CommandOutcome::reply(RespFrame::Integer(-1))
    } else {
        let byte_len = data.len();
        let (start_byte, end_byte) = resolve_range(start_raw, end_raw, byte_len);
        if start_byte > end_byte || start_byte >= byte_len {
            return CommandOutcome::reply(RespFrame::Integer(-1));
        }
        let end_byte = end_byte.min(byte_len - 1);
        let start_bit = start_byte * 8;
        let end_bit = (end_byte + 1) * 8 - 1;

        for pos in start_bit..=end_bit {
            if get_bit(data, pos) == target_bit {
                return CommandOutcome::reply(RespFrame::Integer(pos as i64));
            }
        }

        // For bit=0 without explicit end: if all scanned bits are 1, return
        // the first bit position past the string (Redis behavior)
        if target_bit == 0 && !has_explicit_end {
            return CommandOutcome::reply(RespFrame::Integer((end_bit + 1) as i64));
        }

        CommandOutcome::reply(RespFrame::Integer(-1))
    }
}

// ---------------------------------------------------------------------------
// BITOP operation destkey key [key ...]
// ---------------------------------------------------------------------------

pub(super) fn cmd_bitop(
    args: &[Bytes],
    server: &mut ServerState,
    client: &ClientState,
) -> CommandOutcome {
    if args.len() < 3 {
        return wrong_arity("bitop");
    }

    let op_raw = to_uppercase_bytes(&args[0]);
    let dest_key = &args[1];
    let src_keys = &args[2..];

    let now = now_ms();
    let db = server.db_mut(client.selected_db);

    // NOT takes exactly 1 source key
    if op_raw.as_slice() == b"NOT" && src_keys.len() != 1 {
        return CommandOutcome::reply(err("ERR BITOP NOT requires one and only one key"));
    }

    // Gather source byte arrays
    let mut sources: Vec<Vec<u8>> = Vec::with_capacity(src_keys.len());
    let mut max_len: usize = 0;

    for src_key in src_keys {
        purge_expired_key(db, src_key, now);
        if let Some(entry) = db.get(src_key) {
            let Some(data) = entry.as_string() else {
                return wrong_type_response();
            };
            let v = data.to_vec();
            if v.len() > max_len {
                max_len = v.len();
            }
            sources.push(v);
        } else {
            sources.push(Vec::new());
        }
    }

    let mut result = vec![0u8; max_len];

    match op_raw.as_slice() {
        b"AND" => {
            for byte_idx in 0..max_len {
                let mut val = 0xFFu8;
                for src in &sources {
                    let b = if byte_idx < src.len() {
                        src[byte_idx]
                    } else {
                        0
                    };
                    val &= b;
                }
                result[byte_idx] = val;
            }
        }
        b"OR" => {
            for byte_idx in 0..max_len {
                let mut val = 0u8;
                for src in &sources {
                    let b = if byte_idx < src.len() {
                        src[byte_idx]
                    } else {
                        0
                    };
                    val |= b;
                }
                result[byte_idx] = val;
            }
        }
        b"XOR" => {
            for byte_idx in 0..max_len {
                let mut val = 0u8;
                for src in &sources {
                    let b = if byte_idx < src.len() {
                        src[byte_idx]
                    } else {
                        0
                    };
                    val ^= b;
                }
                result[byte_idx] = val;
            }
        }
        b"NOT" => {
            for byte_idx in 0..max_len {
                let b = if byte_idx < sources[0].len() {
                    sources[0][byte_idx]
                } else {
                    0
                };
                result[byte_idx] = !b;
            }
        }
        _ => {
            return CommandOutcome::reply(err("ERR BITOP requires AND, OR, XOR, or NOT"));
        }
    }

    purge_expired_key(db, dest_key, now);
    let result_len = result.len() as i64;
    db.insert(
        dest_key.clone(),
        StoredValue::string(Bytes::from(result), None),
    );

    CommandOutcome::reply(RespFrame::Integer(result_len))
}

// ---------------------------------------------------------------------------
// BITFIELD key [GET encoding offset] [SET encoding offset value]
//              [INCRBY encoding offset increment] [OVERFLOW WRAP|SAT|FAIL]
// ---------------------------------------------------------------------------

pub(super) fn cmd_bitfield(
    args: &[Bytes],
    server: &mut ServerState,
    client: &ClientState,
) -> CommandOutcome {
    if args.is_empty() {
        return wrong_arity("bitfield");
    }

    let key = &args[0];

    // Parse sub-commands
    let mut ops: Vec<(BitfieldOp, BitfieldOverflow)> = Vec::new();
    let mut current_overflow = BitfieldOverflow::Wrap;
    let mut i = 1;

    while i < args.len() {
        let upper = to_uppercase_bytes(&args[i]);
        match upper.as_slice() {
            b"GET" => {
                if i + 2 >= args.len() {
                    return CommandOutcome::reply(err("ERR syntax error"));
                }
                let Some(encoding) = parse_bitfield_encoding(&args[i + 1]) else {
                    return CommandOutcome::reply(err(
                        "ERR Invalid bitfield type. Use something like i8 u8 i16 u16 i32 u32 i64 etc.",
                    ));
                };
                let Some(offset) = parse_bitfield_offset(&args[i + 2], &encoding) else {
                    return CommandOutcome::reply(err(
                        "ERR bit offset is not an integer or out of range",
                    ));
                };
                ops.push((BitfieldOp::Get { encoding, offset }, current_overflow));
                i += 3;
            }
            b"SET" => {
                if i + 3 >= args.len() {
                    return CommandOutcome::reply(err("ERR syntax error"));
                }
                let Some(encoding) = parse_bitfield_encoding(&args[i + 1]) else {
                    return CommandOutcome::reply(err(
                        "ERR Invalid bitfield type. Use something like i8 u8 i16 u16 i32 u32 i64 etc.",
                    ));
                };
                let Some(offset) = parse_bitfield_offset(&args[i + 2], &encoding) else {
                    return CommandOutcome::reply(err(
                        "ERR bit offset is not an integer or out of range",
                    ));
                };
                let Some(value) = parse_i64(&args[i + 3]) else {
                    return CommandOutcome::reply(err(
                        "ERR value is not an integer or out of range",
                    ));
                };
                ops.push((
                    BitfieldOp::Set {
                        encoding,
                        offset,
                        value,
                    },
                    current_overflow,
                ));
                i += 4;
            }
            b"INCRBY" => {
                if i + 3 >= args.len() {
                    return CommandOutcome::reply(err("ERR syntax error"));
                }
                let Some(encoding) = parse_bitfield_encoding(&args[i + 1]) else {
                    return CommandOutcome::reply(err(
                        "ERR Invalid bitfield type. Use something like i8 u8 i16 u16 i32 u32 i64 etc.",
                    ));
                };
                let Some(offset) = parse_bitfield_offset(&args[i + 2], &encoding) else {
                    return CommandOutcome::reply(err(
                        "ERR bit offset is not an integer or out of range",
                    ));
                };
                let Some(increment) = parse_i64(&args[i + 3]) else {
                    return CommandOutcome::reply(err(
                        "ERR value is not an integer or out of range",
                    ));
                };
                ops.push((
                    BitfieldOp::IncrBy {
                        encoding,
                        offset,
                        increment,
                    },
                    current_overflow,
                ));
                i += 4;
            }
            b"OVERFLOW" => {
                if i + 1 >= args.len() {
                    return CommandOutcome::reply(err("ERR syntax error"));
                }
                let overflow_upper = to_uppercase_bytes(&args[i + 1]);
                current_overflow = match overflow_upper.as_slice() {
                    b"WRAP" => BitfieldOverflow::Wrap,
                    b"SAT" => BitfieldOverflow::Sat,
                    b"FAIL" => BitfieldOverflow::Fail,
                    _ => {
                        return CommandOutcome::reply(err(
                            "ERR Invalid OVERFLOW type (should be one of WRAP, SAT, FAIL)",
                        ));
                    }
                };
                i += 2;
            }
            _ => {
                return CommandOutcome::reply(err("ERR syntax error"));
            }
        }
    }

    let now = now_ms();
    let db = server.db_mut(client.selected_db);
    purge_expired_key(db, key, now);

    let (mut data, expire_at_ms) = if let Some(existing) = db.get(key) {
        let Some(s) = existing.as_string() else {
            return wrong_type_response();
        };
        (s.to_vec(), existing.expire_at_ms)
    } else {
        (Vec::new(), None)
    };

    let mut results: Vec<RespFrame> = Vec::with_capacity(ops.len());
    let mut modified = false;

    for (op, overflow) in &ops {
        match op {
            BitfieldOp::Get { encoding, offset } => {
                let val = read_bits(&data, *offset, encoding.bits, encoding.signed);
                results.push(RespFrame::Integer(val));
            }
            BitfieldOp::Set {
                encoding,
                offset,
                value,
            } => {
                let old_val = read_bits(&data, *offset, encoding.bits, encoding.signed);
                let clamped = apply_overflow(*value, *encoding, *overflow);
                match clamped {
                    Some(new_val) => {
                        write_bits(&mut data, *offset, encoding.bits, new_val);
                        modified = true;
                        results.push(RespFrame::Integer(old_val));
                    }
                    None => {
                        // FAIL: do not modify, return nil
                        results.push(RespFrame::BulkString(None));
                    }
                }
            }
            BitfieldOp::IncrBy {
                encoding,
                offset,
                increment,
            } => {
                let current = read_bits(&data, *offset, encoding.bits, encoding.signed);
                let new_raw = current.wrapping_add(*increment);
                let clamped = apply_overflow(new_raw, *encoding, *overflow);
                match clamped {
                    Some(new_val) => {
                        write_bits(&mut data, *offset, encoding.bits, new_val);
                        modified = true;
                        results.push(RespFrame::Integer(new_val));
                    }
                    None => {
                        // FAIL: do not modify, return nil
                        results.push(RespFrame::BulkString(None));
                    }
                }
            }
        }
    }

    if modified {
        db.insert(
            key.clone(),
            StoredValue::string(Bytes::from(data), expire_at_ms),
        );
    }

    CommandOutcome::reply(RespFrame::Array(results))
}

// ---------------------------------------------------------------------------
// BITFIELD_RO key [GET encoding offset]
// ---------------------------------------------------------------------------

pub(super) fn cmd_bitfield_ro(
    args: &[Bytes],
    server: &mut ServerState,
    client: &ClientState,
) -> CommandOutcome {
    if args.is_empty() {
        return wrong_arity("bitfield_ro");
    }

    let key = &args[0];

    // Parse sub-commands: only GET allowed
    let mut ops: Vec<BitfieldOp> = Vec::new();
    let mut i = 1;

    while i < args.len() {
        let upper = to_uppercase_bytes(&args[i]);
        match upper.as_slice() {
            b"GET" => {
                if i + 2 >= args.len() {
                    return CommandOutcome::reply(err("ERR syntax error"));
                }
                let Some(encoding) = parse_bitfield_encoding(&args[i + 1]) else {
                    return CommandOutcome::reply(err(
                        "ERR Invalid bitfield type. Use something like i8 u8 i16 u16 i32 u32 i64 etc.",
                    ));
                };
                let Some(offset) = parse_bitfield_offset(&args[i + 2], &encoding) else {
                    return CommandOutcome::reply(err(
                        "ERR bit offset is not an integer or out of range",
                    ));
                };
                ops.push(BitfieldOp::Get { encoding, offset });
                i += 3;
            }
            _ => {
                return CommandOutcome::reply(err(
                    "ERR BITFIELD_RO only supports the GET subcommand",
                ));
            }
        }
    }

    let now = now_ms();
    let db = server.db_mut(client.selected_db);
    purge_expired_key(db, key, now);

    let data = if let Some(existing) = db.get(key) {
        let Some(s) = existing.as_string() else {
            return wrong_type_response();
        };
        s.as_ref()
    } else {
        &[] as &[u8]
    };

    let mut results: Vec<RespFrame> = Vec::with_capacity(ops.len());
    for op in &ops {
        if let BitfieldOp::Get { encoding, offset } = op {
            let val = read_bits(data, *offset, encoding.bits, encoding.signed);
            results.push(RespFrame::Integer(val));
        }
    }

    CommandOutcome::reply(RespFrame::Array(results))
}
