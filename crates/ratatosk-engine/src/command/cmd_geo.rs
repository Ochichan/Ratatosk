use bytes::Bytes;

use ratatosk_resp::frame::RespFrame;

use crate::keyspace::{ServerState, SortedSet, StoredValue, purge_expired_key};
use crate::object::format_f64_for_redis;

use super::{
    ClientState, CommandOutcome, err, now_ms, parse_i64, to_uppercase_bytes, wrong_arity,
    wrong_type_response,
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
fn geohash_decode(score: f64) -> (f64, f64) {
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
fn haversine_distance(lon1: f64, lat1: f64, lon2: f64, lat2: f64) -> f64 {
    let to_rad = std::f64::consts::PI / 180.0;
    let dlat = (lat2 - lat1) * to_rad;
    let dlon = (lon2 - lon1) * to_rad;
    let a = (dlat / 2.0).sin().powi(2)
        + lat1.to_radians().cos() * lat2.to_radians().cos() * (dlon / 2.0).sin().powi(2);
    let c = 2.0 * a.sqrt().asin();
    EARTH_RADIUS_METERS * c
}

/// Parse longitude and latitude from raw Bytes, validating ranges.
fn parse_lon_lat(lon_raw: &Bytes, lat_raw: &Bytes) -> Option<(f64, f64)> {
    let lon: f64 = std::str::from_utf8(lon_raw).ok()?.parse().ok()?;
    let lat: f64 = std::str::from_utf8(lat_raw).ok()?.parse().ok()?;
    if !(GEO_LON_MIN..=GEO_LON_MAX).contains(&lon) || !(GEO_LAT_MIN..=GEO_LAT_MAX).contains(&lat) {
        return None;
    }
    Some((lon, lat))
}

/// Parse a distance unit string, returning meters-per-unit factor.
fn parse_unit(raw: &[u8]) -> Option<f64> {
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
fn meters_to_unit(meters: f64, unit_factor: f64) -> f64 {
    meters / unit_factor
}

// ---------------------------------------------------------------------------
// GEOSEARCH internal data types
// ---------------------------------------------------------------------------

enum GeoCenter {
    Member(Bytes),
    LonLat(f64, f64),
}

enum GeoShape {
    Radius(f64),                     // meters
    Box { width: f64, height: f64 }, // meters
}

struct GeoSearchOpts {
    center: GeoCenter,
    shape: GeoShape,
    ascending: Option<bool>, // None = no sort, Some(true) = ASC, Some(false) = DESC
    count: Option<usize>,
    count_any: bool,
    with_coord: bool,
    with_dist: bool,
    with_hash: bool,
}

struct GeoSearchResult {
    member: Bytes,
    dist: f64,
    score: f64,
    lon: f64,
    lat: f64,
}

// ---------------------------------------------------------------------------
// GEOSEARCH parsing helper
// ---------------------------------------------------------------------------

/// Parse GEOSEARCH / GEOSEARCHSTORE options starting at `start_idx` in args.
/// `has_store_dist` controls whether STOREDIST is recognized.
fn parse_geosearch_options(
    args: &[Bytes],
    start_idx: usize,
    allow_store_dist: bool,
) -> Result<(GeoSearchOpts, bool), RespFrame> {
    let mut center: Option<GeoCenter> = None;
    let mut shape: Option<GeoShape> = None;
    let mut ascending: Option<bool> = None;
    let mut count: Option<usize> = None;
    let mut count_any = false;
    let mut with_coord = false;
    let mut with_dist = false;
    let mut with_hash = false;
    let mut store_dist = false;
    let mut i = start_idx;

    while i < args.len() {
        let upper = to_uppercase_bytes(&args[i]);
        match upper.as_slice() {
            b"FROMMEMBER" => {
                if center.is_some() {
                    return Err(err("ERR syntax error"));
                }
                i += 1;
                if i >= args.len() {
                    return Err(err("ERR syntax error"));
                }
                center = Some(GeoCenter::Member(args[i].clone()));
            }
            b"FROMLONLAT" => {
                if center.is_some() {
                    return Err(err("ERR syntax error"));
                }
                if i + 2 >= args.len() {
                    return Err(err("ERR syntax error"));
                }
                let Some((lon, lat)) = parse_lon_lat(&args[i + 1], &args[i + 2]) else {
                    return Err(err("ERR value is not a valid float or is out of range"));
                };
                center = Some(GeoCenter::LonLat(lon, lat));
                i += 2;
            }
            b"BYRADIUS" => {
                if shape.is_some() {
                    return Err(err("ERR syntax error"));
                }
                if i + 2 >= args.len() {
                    return Err(err("ERR syntax error"));
                }
                let radius_str = std::str::from_utf8(&args[i + 1])
                    .map_err(|_| err("ERR value is not a valid float"))?;
                let radius: f64 = radius_str
                    .parse()
                    .map_err(|_| err("ERR value is not a valid float"))?;
                if radius < 0.0 {
                    return Err(err("ERR radius cannot be negative"));
                }
                let Some(unit_factor) = parse_unit(&args[i + 2]) else {
                    return Err(err(
                        "ERR unsupported unit provided. please use M, KM, FT, MI",
                    ));
                };
                shape = Some(GeoShape::Radius(radius * unit_factor));
                i += 2;
            }
            b"BYBOX" => {
                if shape.is_some() {
                    return Err(err("ERR syntax error"));
                }
                if i + 3 >= args.len() {
                    return Err(err("ERR syntax error"));
                }
                let width_str = std::str::from_utf8(&args[i + 1])
                    .map_err(|_| err("ERR value is not a valid float"))?;
                let width: f64 = width_str
                    .parse()
                    .map_err(|_| err("ERR value is not a valid float"))?;
                let height_str = std::str::from_utf8(&args[i + 2])
                    .map_err(|_| err("ERR value is not a valid float"))?;
                let height: f64 = height_str
                    .parse()
                    .map_err(|_| err("ERR value is not a valid float"))?;
                if width < 0.0 || height < 0.0 {
                    return Err(err("ERR width or height cannot be negative"));
                }
                let Some(unit_factor) = parse_unit(&args[i + 3]) else {
                    return Err(err(
                        "ERR unsupported unit provided. please use M, KM, FT, MI",
                    ));
                };
                shape = Some(GeoShape::Box {
                    width: width * unit_factor,
                    height: height * unit_factor,
                });
                i += 3;
            }
            b"ASC" => {
                ascending = Some(true);
            }
            b"DESC" => {
                ascending = Some(false);
            }
            b"COUNT" => {
                i += 1;
                if i >= args.len() {
                    return Err(err("ERR syntax error"));
                }
                let Some(c) = parse_i64(&args[i]) else {
                    return Err(err("ERR value is not an integer or out of range"));
                };
                if c <= 0 {
                    return Err(err("ERR COUNT must be > 0"));
                }
                count = Some(c as usize);
                // Check for optional ANY
                if i + 1 < args.len() && to_uppercase_bytes(&args[i + 1]).as_slice() == b"ANY" {
                    count_any = true;
                    i += 1;
                }
            }
            b"WITHCOORD" => {
                with_coord = true;
            }
            b"WITHDIST" => {
                with_dist = true;
            }
            b"WITHHASH" => {
                with_hash = true;
            }
            b"STOREDIST" if allow_store_dist => {
                store_dist = true;
            }
            _ => {
                return Err(err("ERR syntax error"));
            }
        }
        i += 1;
    }

    let Some(center) = center else {
        return Err(err(
            "ERR exactly one of FROMMEMBER or FROMLONLAT must be provided",
        ));
    };
    let Some(shape) = shape else {
        return Err(err("ERR exactly one of BYRADIUS or BYBOX must be provided"));
    };

    Ok((
        GeoSearchOpts {
            center,
            shape,
            ascending,
            count,
            count_any,
            with_coord,
            with_dist,
            with_hash,
        },
        store_dist,
    ))
}

/// Execute the core geosearch logic on a sorted set.
/// Returns the list of matching results, or an error frame.
fn geosearch_execute(
    zset: &SortedSet,
    opts: &GeoSearchOpts,
    unit_factor: f64,
) -> Result<Vec<GeoSearchResult>, RespFrame> {
    // Resolve center coordinates
    let (center_lon, center_lat) = match &opts.center {
        GeoCenter::LonLat(lon, lat) => (*lon, *lat),
        GeoCenter::Member(member) => {
            let Some(score) = zset.score(member) else {
                return Err(err("ERR could not decode requested zset member"));
            };
            geohash_decode(score)
        }
    };

    let mut results = Vec::new();

    // Iterate all members and filter
    for entry in zset.by_score.keys() {
        let (lon, lat) = geohash_decode(entry.score.value());
        let dist = haversine_distance(center_lon, center_lat, lon, lat);

        let inside = match &opts.shape {
            GeoShape::Radius(radius) => dist <= *radius,
            GeoShape::Box { width, height } => {
                // Box check: distance along lon and lat axes
                let dist_lon = haversine_distance(center_lon, center_lat, lon, center_lat);
                let dist_lat = haversine_distance(center_lon, center_lat, center_lon, lat);
                dist_lon <= *width / 2.0 && dist_lat <= *height / 2.0
            }
        };

        if inside {
            results.push(GeoSearchResult {
                member: entry.member.clone(),
                dist,
                score: entry.score.value(),
                lon,
                lat,
            });

            // When ANY is specified with COUNT, stop scanning early
            if opts.count_any {
                if let Some(c) = opts.count {
                    if results.len() >= c {
                        break;
                    }
                }
            }
        }
    }

    // Sort
    if let Some(asc) = opts.ascending {
        if asc {
            results.sort_by(|a, b| {
                a.dist
                    .partial_cmp(&b.dist)
                    .unwrap_or(std::cmp::Ordering::Equal)
            });
        } else {
            results.sort_by(|a, b| {
                b.dist
                    .partial_cmp(&a.dist)
                    .unwrap_or(std::cmp::Ordering::Equal)
            });
        }
    }

    // Apply COUNT
    if let Some(c) = opts.count {
        results.truncate(c);
    }

    // Convert distances to display unit
    for r in &mut results {
        r.dist = meters_to_unit(r.dist, unit_factor);
    }

    Ok(results)
}

/// Build a RESP reply array from search results.
fn geosearch_reply(
    results: &[GeoSearchResult],
    with_coord: bool,
    with_dist: bool,
    with_hash: bool,
) -> RespFrame {
    let has_extras = with_coord || with_dist || with_hash;

    let items: Vec<RespFrame> = results
        .iter()
        .map(|r| {
            if !has_extras {
                return RespFrame::BulkString(Some(r.member.clone()));
            }
            let mut parts = Vec::with_capacity(4);
            parts.push(RespFrame::BulkString(Some(r.member.clone())));
            if with_dist {
                parts.push(RespFrame::BulkString(Some(format_f64_for_redis(r.dist))));
            }
            if with_hash {
                parts.push(RespFrame::Integer(r.score as i64));
            }
            if with_coord {
                parts.push(RespFrame::Array(vec![
                    RespFrame::BulkString(Some(format_f64_for_redis(r.lon))),
                    RespFrame::BulkString(Some(format_f64_for_redis(r.lat))),
                ]));
            }
            RespFrame::Array(parts)
        })
        .collect();

    RespFrame::Array(items)
}

/// Extract the unit factor from the radius/box arguments for display purposes.
/// Scans args for BYRADIUS or BYBOX and returns the unit factor.
fn extract_unit_factor(args: &[Bytes], start: usize) -> f64 {
    let mut i = start;
    while i < args.len() {
        let upper = to_uppercase_bytes(&args[i]);
        match upper.as_slice() {
            b"BYRADIUS" => {
                // unit is at i+2
                if i + 2 < args.len() {
                    if let Some(factor) = parse_unit(&args[i + 2]) {
                        return factor;
                    }
                }
                return 1.0;
            }
            b"BYBOX" => {
                // unit is at i+3
                if i + 3 < args.len() {
                    if let Some(factor) = parse_unit(&args[i + 3]) {
                        return factor;
                    }
                }
                return 1.0;
            }
            _ => {}
        }
        i += 1;
    }
    1.0
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

// ---------------------------------------------------------------------------
// GEOSEARCH key FROMMEMBER member | FROMLONLAT lon lat
//               BYRADIUS radius M|KM|FT|MI | BYBOX width height M|KM|FT|MI
//               [ASC|DESC] [COUNT count [ANY]] [WITHCOORD] [WITHDIST] [WITHHASH]
// ---------------------------------------------------------------------------

pub(super) fn cmd_geosearch(
    args: &[Bytes],
    server: &mut ServerState,
    client: &ClientState,
) -> CommandOutcome {
    if args.len() < 4 {
        return wrong_arity("geosearch");
    }

    let key = &args[0];
    let unit_factor = extract_unit_factor(args, 1);

    let (opts, _store_dist) = match parse_geosearch_options(args, 1, false) {
        Ok(v) => v,
        Err(e) => return CommandOutcome::reply(e),
    };

    let now = now_ms();
    let mut db = server.db_mut(client.selected_db);
    purge_expired_key(&mut db, key, now);

    let Some(entry) = db.get(key) else {
        return CommandOutcome::reply(RespFrame::Array(vec![]));
    };
    let Some(zset) = entry.as_sorted_set() else {
        return wrong_type_response();
    };

    let results = match geosearch_execute(zset, &opts, unit_factor) {
        Ok(r) => r,
        Err(e) => return CommandOutcome::reply(e),
    };

    CommandOutcome::reply(geosearch_reply(
        &results,
        opts.with_coord,
        opts.with_dist,
        opts.with_hash,
    ))
}

// ---------------------------------------------------------------------------
// GEOSEARCHSTORE dest src FROMMEMBER member | FROMLONLAT lon lat
//                BYRADIUS radius unit | BYBOX width height unit
//                [ASC|DESC] [COUNT count [ANY]] [STOREDIST]
// ---------------------------------------------------------------------------

pub(super) fn cmd_geosearchstore(
    args: &[Bytes],
    server: &mut ServerState,
    client: &ClientState,
) -> CommandOutcome {
    if args.len() < 5 {
        return wrong_arity("geosearchstore");
    }

    let dest_key = &args[0];
    let src_key = &args[1];
    let unit_factor = extract_unit_factor(args, 2);

    let (opts, store_dist) = match parse_geosearch_options(args, 2, true) {
        Ok(v) => v,
        Err(e) => return CommandOutcome::reply(e),
    };

    let now = now_ms();
    let mut db = server.db_mut(client.selected_db);
    purge_expired_key(&mut db, src_key, now);
    purge_expired_key(&mut db, dest_key, now);

    let Some(entry) = db.get(src_key) else {
        // Source does not exist: remove dest if exists and return 0
        db.remove(dest_key);
        return CommandOutcome::reply(RespFrame::Integer(0));
    };
    let Some(zset) = entry.as_sorted_set() else {
        return wrong_type_response();
    };

    let results = match geosearch_execute(zset, &opts, unit_factor) {
        Ok(r) => r,
        Err(e) => return CommandOutcome::reply(e),
    };

    let count = results.len() as i64;

    if results.is_empty() {
        db.remove(dest_key);
    } else {
        let mut dest_zset = SortedSet::default();
        for r in &results {
            let score = if store_dist { r.dist } else { r.score };
            dest_zset.insert(r.member.clone(), score);
        }
        db.insert(dest_key.clone(), StoredValue::sorted_set(dest_zset, None));
    }

    CommandOutcome::reply(RespFrame::Integer(count))
}

// ---------------------------------------------------------------------------
// GEORADIUS key lon lat radius M|KM|FT|MI [WITHCOORD] [WITHDIST] [WITHHASH]
//           [COUNT count [ANY]] [ASC|DESC] [STORE key] [STOREDIST key]
// ---------------------------------------------------------------------------

pub(super) fn cmd_georadius(
    args: &[Bytes],
    server: &mut ServerState,
    client: &ClientState,
    readonly: bool,
) -> CommandOutcome {
    if args.len() < 5 {
        let name = if readonly {
            "georadius_ro"
        } else {
            "georadius"
        };
        return wrong_arity(name);
    }

    let key = &args[0];

    let Some((lon, lat)) = parse_lon_lat(&args[1], &args[2]) else {
        return CommandOutcome::reply(err("ERR value is not a valid float or is out of range"));
    };

    let radius_str = match std::str::from_utf8(&args[3]) {
        Ok(s) => s,
        Err(_) => return CommandOutcome::reply(err("ERR value is not a valid float")),
    };
    let radius: f64 = match radius_str.parse() {
        Ok(v) => v,
        Err(_) => return CommandOutcome::reply(err("ERR value is not a valid float")),
    };
    if radius < 0.0 {
        return CommandOutcome::reply(err("ERR radius cannot be negative"));
    }

    let Some(unit_factor) = parse_unit(&args[4]) else {
        return CommandOutcome::reply(err(
            "ERR unsupported unit provided. please use M, KM, FT, MI",
        ));
    };

    // Parse remaining optional arguments
    let mut with_coord = false;
    let mut with_dist = false;
    let mut with_hash = false;
    let mut ascending: Option<bool> = None;
    let mut count: Option<usize> = None;
    let mut count_any = false;
    let mut store_key: Option<Bytes> = None;
    let mut storedist_key: Option<Bytes> = None;
    let mut i = 5;

    while i < args.len() {
        let upper = to_uppercase_bytes(&args[i]);
        match upper.as_slice() {
            b"WITHCOORD" => with_coord = true,
            b"WITHDIST" => with_dist = true,
            b"WITHHASH" => with_hash = true,
            b"ASC" => ascending = Some(true),
            b"DESC" => ascending = Some(false),
            b"COUNT" => {
                i += 1;
                if i >= args.len() {
                    return CommandOutcome::reply(err("ERR syntax error"));
                }
                let Some(c) = parse_i64(&args[i]) else {
                    return CommandOutcome::reply(err(
                        "ERR value is not an integer or out of range",
                    ));
                };
                if c <= 0 {
                    return CommandOutcome::reply(err("ERR COUNT must be > 0"));
                }
                count = Some(c as usize);
                if i + 1 < args.len() && to_uppercase_bytes(&args[i + 1]).as_slice() == b"ANY" {
                    count_any = true;
                    i += 1;
                }
            }
            b"STORE" if !readonly => {
                i += 1;
                if i >= args.len() {
                    return CommandOutcome::reply(err("ERR syntax error"));
                }
                store_key = Some(args[i].clone());
            }
            b"STOREDIST" if !readonly => {
                i += 1;
                if i >= args.len() {
                    return CommandOutcome::reply(err("ERR syntax error"));
                }
                storedist_key = Some(args[i].clone());
            }
            _ => return CommandOutcome::reply(err("ERR syntax error")),
        }
        i += 1;
    }

    let opts = GeoSearchOpts {
        center: GeoCenter::LonLat(lon, lat),
        shape: GeoShape::Radius(radius * unit_factor),
        ascending,
        count,
        count_any,
        with_coord,
        with_dist,
        with_hash,
    };

    let now = now_ms();
    let mut db = server.db_mut(client.selected_db);
    purge_expired_key(&mut db, key, now);

    let Some(entry) = db.get(key) else {
        if let Some(sk) = &store_key {
            db.remove(sk);
        }
        if let Some(sdk) = &storedist_key {
            db.remove(sdk);
        }
        if store_key.is_some() || storedist_key.is_some() {
            return CommandOutcome::reply(RespFrame::Integer(0));
        }
        return CommandOutcome::reply(RespFrame::Array(vec![]));
    };
    let Some(zset) = entry.as_sorted_set() else {
        return wrong_type_response();
    };

    let results = match geosearch_execute(zset, &opts, unit_factor) {
        Ok(r) => r,
        Err(e) => return CommandOutcome::reply(e),
    };

    // STORE / STOREDIST handling
    if let Some(sk) = store_key {
        let count_val = results.len() as i64;
        if results.is_empty() {
            db.remove(&sk);
        } else {
            let mut dest_zset = SortedSet::default();
            for r in &results {
                dest_zset.insert(r.member.clone(), r.score);
            }
            db.insert(sk, StoredValue::sorted_set(dest_zset, None));
        }
        return CommandOutcome::reply(RespFrame::Integer(count_val));
    }

    if let Some(sdk) = storedist_key {
        let count_val = results.len() as i64;
        if results.is_empty() {
            db.remove(&sdk);
        } else {
            let mut dest_zset = SortedSet::default();
            for r in &results {
                dest_zset.insert(r.member.clone(), r.dist);
            }
            db.insert(sdk, StoredValue::sorted_set(dest_zset, None));
        }
        return CommandOutcome::reply(RespFrame::Integer(count_val));
    }

    CommandOutcome::reply(geosearch_reply(
        &results,
        opts.with_coord,
        opts.with_dist,
        opts.with_hash,
    ))
}

// ---------------------------------------------------------------------------
// GEORADIUS_RO key lon lat radius M|KM|FT|MI [...]
// ---------------------------------------------------------------------------

pub(super) fn cmd_georadius_ro(
    args: &[Bytes],
    server: &mut ServerState,
    client: &ClientState,
) -> CommandOutcome {
    cmd_georadius(args, server, client, true)
}

// ---------------------------------------------------------------------------
// GEORADIUSBYMEMBER key member radius M|KM|FT|MI [WITHCOORD] [WITHDIST]
//                   [WITHHASH] [COUNT count [ANY]] [ASC|DESC]
//                   [STORE key] [STOREDIST key]
// ---------------------------------------------------------------------------

pub(super) fn cmd_georadiusbymember(
    args: &[Bytes],
    server: &mut ServerState,
    client: &ClientState,
    readonly: bool,
) -> CommandOutcome {
    if args.len() < 4 {
        let name = if readonly {
            "georadiusbymember_ro"
        } else {
            "georadiusbymember"
        };
        return wrong_arity(name);
    }

    let key = &args[0];
    let member = &args[1];

    let radius_str = match std::str::from_utf8(&args[2]) {
        Ok(s) => s,
        Err(_) => return CommandOutcome::reply(err("ERR value is not a valid float")),
    };
    let radius: f64 = match radius_str.parse() {
        Ok(v) => v,
        Err(_) => return CommandOutcome::reply(err("ERR value is not a valid float")),
    };
    if radius < 0.0 {
        return CommandOutcome::reply(err("ERR radius cannot be negative"));
    }

    let Some(unit_factor) = parse_unit(&args[3]) else {
        return CommandOutcome::reply(err(
            "ERR unsupported unit provided. please use M, KM, FT, MI",
        ));
    };

    // Parse remaining optional arguments
    let mut with_coord = false;
    let mut with_dist = false;
    let mut with_hash = false;
    let mut ascending: Option<bool> = None;
    let mut count: Option<usize> = None;
    let mut count_any = false;
    let mut store_key: Option<Bytes> = None;
    let mut storedist_key: Option<Bytes> = None;
    let mut i = 4;

    while i < args.len() {
        let upper = to_uppercase_bytes(&args[i]);
        match upper.as_slice() {
            b"WITHCOORD" => with_coord = true,
            b"WITHDIST" => with_dist = true,
            b"WITHHASH" => with_hash = true,
            b"ASC" => ascending = Some(true),
            b"DESC" => ascending = Some(false),
            b"COUNT" => {
                i += 1;
                if i >= args.len() {
                    return CommandOutcome::reply(err("ERR syntax error"));
                }
                let Some(c) = parse_i64(&args[i]) else {
                    return CommandOutcome::reply(err(
                        "ERR value is not an integer or out of range",
                    ));
                };
                if c <= 0 {
                    return CommandOutcome::reply(err("ERR COUNT must be > 0"));
                }
                count = Some(c as usize);
                if i + 1 < args.len() && to_uppercase_bytes(&args[i + 1]).as_slice() == b"ANY" {
                    count_any = true;
                    i += 1;
                }
            }
            b"STORE" if !readonly => {
                i += 1;
                if i >= args.len() {
                    return CommandOutcome::reply(err("ERR syntax error"));
                }
                store_key = Some(args[i].clone());
            }
            b"STOREDIST" if !readonly => {
                i += 1;
                if i >= args.len() {
                    return CommandOutcome::reply(err("ERR syntax error"));
                }
                storedist_key = Some(args[i].clone());
            }
            _ => return CommandOutcome::reply(err("ERR syntax error")),
        }
        i += 1;
    }

    let opts = GeoSearchOpts {
        center: GeoCenter::Member(member.clone()),
        shape: GeoShape::Radius(radius * unit_factor),
        ascending,
        count,
        count_any,
        with_coord,
        with_dist,
        with_hash,
    };

    let now = now_ms();
    let mut db = server.db_mut(client.selected_db);
    purge_expired_key(&mut db, key, now);

    let Some(entry) = db.get(key) else {
        if let Some(sk) = &store_key {
            db.remove(sk);
        }
        if let Some(sdk) = &storedist_key {
            db.remove(sdk);
        }
        if store_key.is_some() || storedist_key.is_some() {
            return CommandOutcome::reply(RespFrame::Integer(0));
        }
        return CommandOutcome::reply(RespFrame::Array(vec![]));
    };
    let Some(zset) = entry.as_sorted_set() else {
        return wrong_type_response();
    };

    let results = match geosearch_execute(zset, &opts, unit_factor) {
        Ok(r) => r,
        Err(e) => return CommandOutcome::reply(e),
    };

    // STORE / STOREDIST handling
    if let Some(sk) = store_key {
        let count_val = results.len() as i64;
        if results.is_empty() {
            db.remove(&sk);
        } else {
            let mut dest_zset = SortedSet::default();
            for r in &results {
                dest_zset.insert(r.member.clone(), r.score);
            }
            db.insert(sk, StoredValue::sorted_set(dest_zset, None));
        }
        return CommandOutcome::reply(RespFrame::Integer(count_val));
    }

    if let Some(sdk) = storedist_key {
        let count_val = results.len() as i64;
        if results.is_empty() {
            db.remove(&sdk);
        } else {
            let mut dest_zset = SortedSet::default();
            for r in &results {
                dest_zset.insert(r.member.clone(), r.dist);
            }
            db.insert(sdk, StoredValue::sorted_set(dest_zset, None));
        }
        return CommandOutcome::reply(RespFrame::Integer(count_val));
    }

    CommandOutcome::reply(geosearch_reply(
        &results,
        opts.with_coord,
        opts.with_dist,
        opts.with_hash,
    ))
}

// ---------------------------------------------------------------------------
// GEORADIUSBYMEMBER_RO key member radius M|KM|FT|MI [...]
// ---------------------------------------------------------------------------

pub(super) fn cmd_georadiusbymember_ro(
    args: &[Bytes],
    server: &mut ServerState,
    client: &ClientState,
) -> CommandOutcome {
    cmd_georadiusbymember(args, server, client, true)
}
