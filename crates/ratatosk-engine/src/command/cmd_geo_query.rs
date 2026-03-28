use bytes::Bytes;

use ratatosk_resp::frame::RespFrame;

use crate::keyspace::{ServerState, SortedSet, StoredValue, purge_expired_key};
use crate::object::format_f64_for_redis;

use super::cmd_geo::{
    geohash_decode, haversine_distance, meters_to_unit, parse_lon_lat, parse_unit,
};
use super::{
    ClientState, CommandOutcome, err, now_ms, parse_i64, to_uppercase_bytes, wrong_arity,
    wrong_type_response,
};

pub(super) enum GeoCenter {
    Member(Bytes),
    LonLat(f64, f64),
}

pub(super) enum GeoShape {
    Radius(f64),                     // meters
    Box { width: f64, height: f64 }, // meters
}

pub(super) struct GeoSearchOpts {
    pub(super) center: GeoCenter,
    pub(super) shape: GeoShape,
    pub(super) ascending: Option<bool>, // None = no sort, Some(true) = ASC, Some(false) = DESC
    pub(super) count: Option<usize>,
    pub(super) count_any: bool,
    pub(super) with_coord: bool,
    pub(super) with_dist: bool,
    pub(super) with_hash: bool,
}

pub(super) struct GeoSearchResult {
    pub(super) member: Bytes,
    pub(super) dist: f64,
    pub(super) score: f64,
    pub(super) lon: f64,
    pub(super) lat: f64,
}

/// Parse GEOSEARCH / GEOSEARCHSTORE options starting at `start_idx` in args.
/// `allow_store_dist` controls whether STOREDIST is recognized.
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
pub(super) fn geosearch_execute(
    zset: &SortedSet,
    opts: &GeoSearchOpts,
    unit_factor: f64,
) -> Result<Vec<GeoSearchResult>, RespFrame> {
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

    for entry in zset.by_score.keys() {
        let (lon, lat) = geohash_decode(entry.score.value());
        let dist = haversine_distance(center_lon, center_lat, lon, lat);

        let inside = match &opts.shape {
            GeoShape::Radius(radius) => dist <= *radius,
            GeoShape::Box { width, height } => {
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

            if opts.count_any {
                if let Some(c) = opts.count {
                    if results.len() >= c {
                        break;
                    }
                }
            }
        }
    }

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

    if let Some(c) = opts.count {
        results.truncate(c);
    }

    for r in &mut results {
        r.dist = meters_to_unit(r.dist, unit_factor);
    }

    Ok(results)
}

pub(super) fn geosearch_reply(
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

fn extract_unit_factor(args: &[Bytes], start: usize) -> f64 {
    let mut i = start;
    while i < args.len() {
        let upper = to_uppercase_bytes(&args[i]);
        match upper.as_slice() {
            b"BYRADIUS" => {
                if i + 2 < args.len() {
                    if let Some(factor) = parse_unit(&args[i + 2]) {
                        return factor;
                    }
                }
                return 1.0;
            }
            b"BYBOX" => {
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
