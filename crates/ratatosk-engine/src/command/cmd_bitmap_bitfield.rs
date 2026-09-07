use bytes::Bytes;

use ratatosk_resp::frame::RespFrame;

use crate::keyspace::{ServerState, StoredValue, purge_expired_key};

use super::{
    ClientState, CommandOutcome,
    cmd_bitmap::{get_bit, set_bit},
    err, now_ms, parse_i64, to_uppercase_bytes, wrong_arity, wrong_type_response,
};

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
    if signed {
        if !(1..=64).contains(&bits) {
            return None;
        }
    } else if !(1..=63).contains(&bits) {
        return None;
    }
    Some(BitfieldEncoding { bits, signed })
}

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
        let sign_bit = 1u64 << (bits as u32 - 1);
        if value & sign_bit != 0 {
            let mask = !((1u64 << bits as u32) - 1);
            value |= mask;
        }
    }
    value as i64
}

fn write_bits(data: &mut Vec<u8>, bit_offset: usize, bits: u8, value: i64) {
    let raw = value as u64;
    for i in 0..bits as usize {
        let bit_pos = bits as usize - 1 - i;
        let bit_val = ((raw >> bit_pos as u32) & 1) as u8;
        set_bit(data, bit_offset + i, bit_val);
    }
}

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
                    Some(value)
                } else {
                    let range = 1u64 << bits as u32;
                    let mut v = value as u64;
                    v &= range - 1;
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
        if bits < 64 {
            let masked = unsigned_val & max;
            if masked == unsigned_val && value >= 0 {
                return Some(value);
            }
            if value >= 0 && unsigned_val <= max {
                return Some(value);
            }
        } else if value >= 0 {
            return Some(value);
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

pub(super) fn cmd_bitfield(
    args: &[Bytes],
    server: &mut ServerState,
    client: &ClientState,
) -> CommandOutcome {
    if args.is_empty() {
        return wrong_arity("bitfield");
    }

    let key = &args[0];
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
    let mut db = server.db_mut(client.selected_db);
    purge_expired_key(&mut db, key, now);

    let (mut data, expire_at_ms) = if let Some(existing) = db.get(key) {
        let Some(s) = existing.as_string_bytes() else {
            return wrong_type_response();
        };
        (s.to_vec(), existing.expire_at_ms())
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
                    None => results.push(RespFrame::BulkString(None)),
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
                    None => results.push(RespFrame::BulkString(None)),
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

pub(super) fn cmd_bitfield_ro(
    args: &[Bytes],
    server: &mut ServerState,
    client: &ClientState,
) -> CommandOutcome {
    if args.is_empty() {
        return wrong_arity("bitfield_ro");
    }

    let key = &args[0];
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
    let mut db = server.db_mut(client.selected_db);
    purge_expired_key(&mut db, key, now);

    let data = if let Some(existing) = db.get(key) {
        let Some(s) = existing.as_string_bytes() else {
            return wrong_type_response();
        };
        s
    } else {
        Bytes::new()
    };

    let mut results: Vec<RespFrame> = Vec::with_capacity(ops.len());
    for op in &ops {
        if let BitfieldOp::Get { encoding, offset } = op {
            let val = read_bits(&data, *offset, encoding.bits, encoding.signed);
            results.push(RespFrame::Integer(val));
        }
    }

    CommandOutcome::reply(RespFrame::Array(results))
}
