use bytes::Bytes;

use ratatosk_resp::frame::RespFrame;

use crate::keyspace::{ServerState, SortedSet, StoredValue, purge_expired_key};
use crate::object::format_f64_for_redis;

use super::{
    ClientState, CommandOutcome, err, now_ms, to_uppercase_bytes, wrong_arity, wrong_type_response,
};

// ---------------------------------------------------------------------------
// Geohash constants
// ---------------------------------------------------------------------------

const GEO_LAT_MIN: f64 = -85.05112878;
const GEO_LAT_MAX: f64 = 85.05112878;
const GEO_LON_MIN: f64 = -180.0;
const GEO_LON_MAX: f64 = 180.0;
const GEO_STEP_MAX: u8 = 26; // 52 bits total (26 bits lon + 26 bits lat)
const EARTH_RADIUS_METERS: f64 = 6372797.560856;

const BASE32: &[u8] = b"0123456789bcdefghjkmnpqrstuvwxyz";

// ---------------------------------------------------------------------------
// Geohash encoding / decoding utilities
// ---------------------------------------------------------------------------

/// Encode (lon, lat) into a 52-bit geohash stored as f64 score in sorted set.
fn geohash_encode(lon: f64, lat: f64) -> Option<f64> {
    if !(GEO_LON_MIN..=GEO_LON_MAX).contains(&lon) || !(GEO_LAT_MIN..=GEO_LAT_MAX).contains(&lat) {
        return None;
    }
    let mut lat_range = (GEO_LAT_MIN, GEO_LAT_MAX);
    let mut lon_range = (GEO_LON_MIN, GEO_LON_MAX);
    let mut hash: u64 = 0;
    for i in 0..GEO_STEP_MAX {
        // Longitude bit (even positions in interleaved hash)
        let mid = (lon_range.0 + lon_range.1) / 2.0;
        if lon >= mid {
            hash |= 1 << (51 - i as u32 * 2);
            lon_range.0 = mid;
        } else {
            lon_range.1 = mid;
        }
        // Latitude bit (odd positions in interleaved hash)
        let mid = (lat_range.0 + lat_range.1) / 2.0;
        if lat >= mid {
            hash |= 1 << (50 - i as u32 * 2);
            lat_range.0 = mid;
        } else {
            lat_range.1 = mid;
        }
    }
    Some(hash as f64)
}

/// Decode a 52-bit geohash score back to (lon, lat).
pub(super) fn geohash_decode(score: f64) -> (f64, f64) {
    let hash = score as u64;
    let mut lat_range = (GEO_LAT_MIN, GEO_LAT_MAX);
    let mut lon_range = (GEO_LON_MIN, GEO_LON_MAX);
    for i in 0..GEO_STEP_MAX {
        if hash & (1 << (51 - i as u32 * 2)) != 0 {
            lon_range.0 = (lon_range.0 + lon_range.1) / 2.0;
        } else {
            lon_range.1 = (lon_range.0 + lon_range.1) / 2.0;
        }
        if hash & (1 << (50 - i as u32 * 2)) != 0 {
            lat_range.0 = (lat_range.0 + lat_range.1) / 2.0;
        } else {
            lat_range.1 = (lat_range.0 + lat_range.1) / 2.0;
        }
    }
    (
        (lon_range.0 + lon_range.1) / 2.0,
        (lat_range.0 + lat_range.1) / 2.0,
    )
}

/// Produce an 11-character base32 geohash string from (lon, lat).
fn geohash_string(lon: f64, lat: f64) -> Option<String> {
    if !(GEO_LON_MIN..=GEO_LON_MAX).contains(&lon) || !(GEO_LAT_MIN..=GEO_LAT_MAX).contains(&lat) {
        return None;
    }
    let mut lat_range = (GEO_LAT_MIN, GEO_LAT_MAX);
    let mut lon_range = (GEO_LON_MIN, GEO_LON_MAX);
    let mut bits = 0u64;
    let total_bits: u32 = 55; // 11 chars * 5 bits
    for i in 0..total_bits {
        if i % 2 == 0 {
            // Longitude bit
            let mid = (lon_range.0 + lon_range.1) / 2.0;
            if lon >= mid {
                bits |= 1 << (total_bits - 1 - i);
                lon_range.0 = mid;
            } else {
                lon_range.1 = mid;
            }
        } else {
            // Latitude bit
            let mid = (lat_range.0 + lat_range.1) / 2.0;
            if lat >= mid {
                bits |= 1 << (total_bits - 1 - i);
                lat_range.0 = mid;
            } else {
                lat_range.1 = mid;
            }
        }
    }
    let mut result = String::with_capacity(11);
    for i in 0..11u32 {
        let idx = ((bits >> (50 - i * 5)) & 0x1F) as usize;
        result.push(BASE32[idx] as char);
    }
    Some(result)
}

/// Haversine distance in meters between two (lon, lat) points.
pub(super) fn haversine_distance(lon1: f64, lat1: f64, lon2: f64, lat2: f64) -> f64 {
    let to_rad = std::f64::consts::PI / 180.0;
    let dlat = (lat2 - lat1) * to_rad;
    let dlon = (lon2 - lon1) * to_rad;
    let a = (dlat / 2.0).sin().powi(2)
        + lat1.to_radians().cos() * lat2.to_radians().cos() * (dlon / 2.0).sin().powi(2);
    let c = 2.0 * a.sqrt().asin();
    EARTH_RADIUS_METERS * c
}

/// Parse longitude and latitude from raw Bytes, validating ranges.
pub(super) fn parse_lon_lat(lon_raw: &Bytes, lat_raw: &Bytes) -> Option<(f64, f64)> {
    let lon: f64 = std::str::from_utf8(lon_raw).ok()?.parse().ok()?;
    let lat: f64 = std::str::from_utf8(lat_raw).ok()?.parse().ok()?;
    if !(GEO_LON_MIN..=GEO_LON_MAX).contains(&lon) || !(GEO_LAT_MIN..=GEO_LAT_MAX).contains(&lat) {
        return None;
    }
    Some((lon, lat))
}

/// Parse a distance unit string, returning meters-per-unit factor.
pub(super) fn parse_unit(raw: &[u8]) -> Option<f64> {
    let upper = raw.to_ascii_uppercase();
    match upper.as_slice() {
        b"M" => Some(1.0),
        b"KM" => Some(1000.0),
        b"FT" => Some(0.3048),
        b"MI" => Some(1609.344),
        _ => None,
    }
}

/// Convert distance in meters to the given unit.
pub(super) fn meters_to_unit(meters: f64, unit_factor: f64) -> f64 {
    meters / unit_factor
}

// ---------------------------------------------------------------------------
// GEOADD key [NX|XX] [CH] longitude latitude member [lon lat member ...]
// ---------------------------------------------------------------------------

pub(super) fn cmd_geoadd(
    args: &[Bytes],
    server: &mut ServerState,
    client: &ClientState,
) -> CommandOutcome {
    if args.len() < 4 {
        return wrong_arity("geoadd");
    }

    let key = &args[0];
    let mut idx = 1usize;
    let mut nx = false;
    let mut xx = false;
    let mut ch = false;

    // Parse optional flags
    while idx < args.len() {
        let upper = to_uppercase_bytes(&args[idx]);
        match upper.as_slice() {
            b"NX" => {
                nx = true;
                idx += 1;
            }
            b"XX" => {
                xx = true;
                idx += 1;
            }
            b"CH" => {
                ch = true;
                idx += 1;
            }
            _ => break,
        }
    }

    if nx && xx {
        return CommandOutcome::reply(err(
            "ERR XX and NX options at the same time are not compatible",
        ));
    }

    // Remaining args must be triples: lon lat member
    let remaining = &args[idx..];
    if remaining.is_empty() || remaining.len() % 3 != 0 {
        return wrong_arity("geoadd");
    }

    let now = now_ms();
    let mut db = server.db_mut(client.selected_db);
    purge_expired_key(&mut db, key, now);

    // Ensure sorted set exists or create one
    if let Some(entry) = db.get(key) {
        if !entry.is_sorted_set() {
            return wrong_type_response();
        }
    }

    if !db.contains_key(key) {
        db.insert(
            key.clone(),
            StoredValue::sorted_set(SortedSet::default(), None),
        );
    }

    let entry = db.get_mut(key);
    let Some(entry) = entry else {
        return CommandOutcome::reply(RespFrame::Integer(0));
    };
    let Some(zset) = entry.as_sorted_set_mut() else {
        return wrong_type_response();
    };

    let mut added: i64 = 0;
    let mut changed: i64 = 0;

    let mut triple_idx = 0;
    while triple_idx + 2 < remaining.len() {
        let lon_raw = &remaining[triple_idx];
        let lat_raw = &remaining[triple_idx + 1];
        let member = &remaining[triple_idx + 2];
        triple_idx += 3;

        let Some((lon, lat)) = parse_lon_lat(lon_raw, lat_raw) else {
            return CommandOutcome::reply(err("ERR value is not a valid float or is out of range"));
        };

        let Some(score) = geohash_encode(lon, lat) else {
            return CommandOutcome::reply(err("ERR invalid longitude,latitude pair"));
        };

        let exists = zset.by_member.contains_key(member);

        if nx && exists {
            continue;
        }
        if xx && !exists {
            continue;
        }

        let old_score = zset.score(member);
        let is_new = zset.insert(member.clone(), score);

        if is_new {
            added += 1;
        } else if old_score.is_some_and(|old| (old - score).abs() > f64::EPSILON) {
            changed += 1;
        }
    }

    let reply = if ch { added + changed } else { added };
    CommandOutcome::reply(RespFrame::Integer(reply))
}

// ---------------------------------------------------------------------------
// GEOPOS key member [member ...]
// ---------------------------------------------------------------------------

pub(super) fn cmd_geopos(
    args: &[Bytes],
    server: &mut ServerState,
    client: &ClientState,
) -> CommandOutcome {
    if args.len() < 2 {
        return wrong_arity("geopos");
    }

    let key = &args[0];
    let members = &args[1..];

    let now = now_ms();
    let mut db = server.db_mut(client.selected_db);
    purge_expired_key(&mut db, key, now);

    let Some(entry) = db.get(key) else {
        return CommandOutcome::reply(RespFrame::Array(
            members
                .iter()
                .map(|_| RespFrame::BulkString(None))
                .collect(),
        ));
    };
    let Some(zset) = entry.as_sorted_set() else {
        return wrong_type_response();
    };

    let items: Vec<RespFrame> = members
        .iter()
        .map(|member| {
            let Some(score) = zset.score(member) else {
                return RespFrame::BulkString(None);
            };
            let (lon, lat) = geohash_decode(score);
            RespFrame::Array(vec![
                RespFrame::BulkString(Some(format_f64_for_redis(lon))),
                RespFrame::BulkString(Some(format_f64_for_redis(lat))),
            ])
        })
        .collect();

    CommandOutcome::reply(RespFrame::Array(items))
}

// ---------------------------------------------------------------------------
// GEODIST key member1 member2 [M|KM|FT|MI]
// ---------------------------------------------------------------------------

pub(super) fn cmd_geodist(
    args: &[Bytes],
    server: &mut ServerState,
    client: &ClientState,
) -> CommandOutcome {
    if args.len() < 3 || args.len() > 4 {
        return wrong_arity("geodist");
    }

    let key = &args[0];
    let member1 = &args[1];
    let member2 = &args[2];

    let unit_factor = if args.len() == 4 {
        let Some(factor) = parse_unit(&args[3]) else {
            return CommandOutcome::reply(err(
                "ERR unsupported unit provided. please use M, KM, FT, MI",
            ));
        };
        factor
    } else {
        1.0 // default: meters
    };

    let now = now_ms();
    let mut db = server.db_mut(client.selected_db);
    purge_expired_key(&mut db, key, now);

    let Some(entry) = db.get(key) else {
        return CommandOutcome::reply(RespFrame::BulkString(None));
    };
    let Some(zset) = entry.as_sorted_set() else {
        return wrong_type_response();
    };

    let Some(score1) = zset.score(member1) else {
        return CommandOutcome::reply(RespFrame::BulkString(None));
    };
    let Some(score2) = zset.score(member2) else {
        return CommandOutcome::reply(RespFrame::BulkString(None));
    };

    let (lon1, lat1) = geohash_decode(score1);
    let (lon2, lat2) = geohash_decode(score2);
    let dist = haversine_distance(lon1, lat1, lon2, lat2);
    let converted = meters_to_unit(dist, unit_factor);

    CommandOutcome::reply(RespFrame::BulkString(Some(format_f64_for_redis(converted))))
}

// ---------------------------------------------------------------------------
// GEOHASH key member [member ...]
// ---------------------------------------------------------------------------

pub(super) fn cmd_geohash(
    args: &[Bytes],
    server: &mut ServerState,
    client: &ClientState,
) -> CommandOutcome {
    if args.len() < 2 {
        return wrong_arity("geohash");
    }

    let key = &args[0];
    let members = &args[1..];

    let now = now_ms();
    let mut db = server.db_mut(client.selected_db);
    purge_expired_key(&mut db, key, now);

    let Some(entry) = db.get(key) else {
        return CommandOutcome::reply(RespFrame::Array(
            members
                .iter()
                .map(|_| RespFrame::BulkString(None))
                .collect(),
        ));
    };
    let Some(zset) = entry.as_sorted_set() else {
        return wrong_type_response();
    };

    let items: Vec<RespFrame> = members
        .iter()
        .map(|member| {
            let Some(score) = zset.score(member) else {
                return RespFrame::BulkString(None);
            };
            let (lon, lat) = geohash_decode(score);
            match geohash_string(lon, lat) {
                Some(hash_str) => RespFrame::BulkString(Some(Bytes::from(hash_str))),
                None => RespFrame::BulkString(None),
            }
        })
        .collect();

    CommandOutcome::reply(RespFrame::Array(items))
}
