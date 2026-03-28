use bytes::Bytes;

use ratatosk_resp::frame::RespFrame;

use crate::keyspace::{ServerState, SortedSet, StoredValue, purge_expired_key};

use super::cmd_geo::{parse_lon_lat, parse_unit};
use super::cmd_geo_query::{
    GeoCenter, GeoSearchOpts, GeoShape, geosearch_execute, geosearch_reply,
};
use super::{
    ClientState, CommandOutcome, err, now_ms, parse_i64, to_uppercase_bytes, wrong_arity,
    wrong_type_response,
};

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

pub(super) fn cmd_georadius_ro(
    args: &[Bytes],
    server: &mut ServerState,
    client: &ClientState,
) -> CommandOutcome {
    cmd_georadius(args, server, client, true)
}

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

pub(super) fn cmd_georadiusbymember_ro(
    args: &[Bytes],
    server: &mut ServerState,
    client: &ClientState,
) -> CommandOutcome {
    cmd_georadiusbymember(args, server, client, true)
}
