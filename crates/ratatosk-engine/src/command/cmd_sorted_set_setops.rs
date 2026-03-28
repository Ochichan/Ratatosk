use bytes::Bytes;

use hashbrown::{HashMap, HashSet};
use ratatosk_resp::frame::RespFrame;

use crate::keyspace::{ServerState, SortedSet, SortedSetScore, StoredValue, purge_expired_key};
use crate::object::parse_f64;

use super::cmd_sorted_set::{MAX_ZSET_NUMKEYS, entries_to_resp, sorted_entries};
use super::{
    ClientState, CommandOutcome, err, now_ms, parse_i64, to_uppercase_bytes, wrong_arity,
    wrong_type_response,
};

#[derive(Debug, Clone, Copy)]
enum Aggregate {
    Sum,
    Min,
    Max,
}

/// Parse WEIGHTS and AGGREGATE options that follow the keys in set operations.
/// `option_start` is the index in `args` where option parsing begins (after keys).
fn parse_weights_aggregate(
    args: &[Bytes],
    option_start: usize,
    numkeys: usize,
) -> Result<(Vec<f64>, Aggregate), RespFrame> {
    let mut weights = vec![1.0f64; numkeys];
    let mut aggregate = Aggregate::Sum;

    let mut idx = option_start;
    while idx < args.len() {
        let upper = to_uppercase_bytes(&args[idx]);
        match upper.as_slice() {
            b"WEIGHTS" => {
                if idx + numkeys >= args.len() {
                    return Err(err("ERR syntax error"));
                }
                for (i, w_raw) in args[idx + 1..idx + 1 + numkeys].iter().enumerate() {
                    let Some(weight) = parse_f64(w_raw) else {
                        return Err(err("ERR weight value is not a float"));
                    };
                    weights[i] = weight;
                }
                idx += 1 + numkeys;
            }
            b"AGGREGATE" => {
                if idx + 1 >= args.len() {
                    return Err(err("ERR syntax error"));
                }
                let agg_upper = to_uppercase_bytes(&args[idx + 1]);
                aggregate = match agg_upper.as_slice() {
                    b"SUM" => Aggregate::Sum,
                    b"MIN" => Aggregate::Min,
                    b"MAX" => Aggregate::Max,
                    _ => return Err(err("ERR syntax error")),
                };
                idx += 2;
            }
            _ => return Err(err("ERR syntax error")),
        }
    }

    Ok((weights, aggregate))
}

fn aggregate_score(agg: Aggregate, lhs: f64, rhs: f64) -> f64 {
    match agg {
        Aggregate::Sum => lhs + rhs,
        Aggregate::Min => lhs.min(rhs),
        Aggregate::Max => lhs.max(rhs),
    }
}

/// Read a sorted set from the db; returns an empty set if the key does not exist.
/// Returns Err if the key exists but is the wrong type.
#[allow(clippy::result_large_err)]
fn read_zset_or_empty(
    db: &HashMap<Bytes, StoredValue>,
    key: &Bytes,
) -> Result<Vec<(Bytes, f64)>, CommandOutcome> {
    let Some(entry) = db.get(key) else {
        return Ok(Vec::new());
    };
    let Some(zset) = entry.as_sorted_set() else {
        return Err(wrong_type_response());
    };
    Ok(sorted_entries(zset))
}

#[allow(clippy::result_large_err)]
fn compute_union(
    db: &HashMap<Bytes, StoredValue>,
    keys: &[Bytes],
    weights: &[f64],
    aggregate: Aggregate,
) -> Result<Vec<(Bytes, f64)>, CommandOutcome> {
    let mut result: HashMap<Bytes, f64> = HashMap::new();

    for (index, key) in keys.iter().enumerate() {
        let entries = read_zset_or_empty(db, key)?;
        let weight = weights.get(index).copied().unwrap_or(1.0);
        for (member, score) in entries {
            let weighted = score * weight;
            result
                .entry(member)
                .and_modify(|existing| *existing = aggregate_score(aggregate, *existing, weighted))
                .or_insert(weighted);
        }
    }

    let mut out: Vec<(Bytes, f64)> = result.into_iter().collect();
    out.sort_by(|(a_member, a_score), (b_member, b_score)| {
        SortedSetScore(*a_score)
            .cmp(&SortedSetScore(*b_score))
            .then_with(|| a_member.cmp(b_member))
    });
    Ok(out)
}

#[allow(clippy::result_large_err)]
fn compute_inter(
    db: &HashMap<Bytes, StoredValue>,
    keys: &[Bytes],
    weights: &[f64],
    aggregate: Aggregate,
) -> Result<Vec<(Bytes, f64)>, CommandOutcome> {
    if keys.is_empty() {
        return Ok(Vec::new());
    }

    let first_entries = read_zset_or_empty(db, &keys[0])?;
    let first_weight = weights.first().copied().unwrap_or(1.0);
    let mut result: HashMap<Bytes, f64> = first_entries
        .into_iter()
        .map(|(member, score)| (member, score * first_weight))
        .collect();

    for (index, key) in keys.iter().enumerate().skip(1) {
        let entries = read_zset_or_empty(db, key)?;
        let weight = weights.get(index).copied().unwrap_or(1.0);
        let other: HashMap<Bytes, f64> = entries
            .into_iter()
            .map(|(member, score)| (member, score * weight))
            .collect();

        result.retain(|member, existing| {
            if let Some(other_score) = other.get(member) {
                *existing = aggregate_score(aggregate, *existing, *other_score);
                true
            } else {
                false
            }
        });

        if result.is_empty() {
            break;
        }
    }

    let mut out: Vec<(Bytes, f64)> = result.into_iter().collect();
    out.sort_by(|(a_member, a_score), (b_member, b_score)| {
        SortedSetScore(*a_score)
            .cmp(&SortedSetScore(*b_score))
            .then_with(|| a_member.cmp(b_member))
    });
    Ok(out)
}

#[allow(clippy::result_large_err)]
fn compute_diff(
    db: &HashMap<Bytes, StoredValue>,
    keys: &[Bytes],
) -> Result<Vec<(Bytes, f64)>, CommandOutcome> {
    if keys.is_empty() {
        return Ok(Vec::new());
    }

    let first_entries = read_zset_or_empty(db, &keys[0])?;
    let mut result: HashMap<Bytes, f64> = first_entries.into_iter().collect();

    for key in keys.iter().skip(1) {
        let entries = read_zset_or_empty(db, key)?;
        for (member, _) in entries {
            result.remove(&member);
        }
        if result.is_empty() {
            break;
        }
    }

    let mut out: Vec<(Bytes, f64)> = result.into_iter().collect();
    out.sort_by(|(a_member, a_score), (b_member, b_score)| {
        SortedSetScore(*a_score)
            .cmp(&SortedSetScore(*b_score))
            .then_with(|| a_member.cmp(b_member))
    });
    Ok(out)
}

/// Parse numkeys + key list from args starting at a given index.
/// Returns (numkeys, keys_slice_end_index).
#[allow(clippy::result_large_err)]
fn parse_numkeys_and_keys(
    args: &[Bytes],
    start: usize,
    command_name: &str,
) -> Result<(usize, usize), CommandOutcome> {
    let Some(numkeys_raw) = args.get(start) else {
        return Err(wrong_arity(command_name));
    };
    let Some(numkeys_i64) = parse_i64(numkeys_raw) else {
        return Err(CommandOutcome::reply(err(
            "ERR value is not an integer or out of range",
        )));
    };
    if numkeys_i64 <= 0 {
        return Err(CommandOutcome::reply(err(
            "ERR numkeys should be greater than 0",
        )));
    }
    let Ok(numkeys) = usize::try_from(numkeys_i64) else {
        return Err(CommandOutcome::reply(err(
            "ERR value is not an integer or out of range",
        )));
    };
    if numkeys > MAX_ZSET_NUMKEYS {
        return Err(CommandOutcome::reply(err("ERR numkeys is out of range")));
    }
    let keys_end = start + 1 + numkeys;
    if keys_end > args.len() {
        return Err(CommandOutcome::reply(err(
            "ERR Number of keys can't be greater than number of args",
        )));
    }
    Ok((numkeys, keys_end))
}

pub(super) fn cmd_zunion(
    args: &[Bytes],
    server: &mut ServerState,
    client: &ClientState,
) -> CommandOutcome {
    if args.len() < 2 {
        return wrong_arity("zunion");
    }

    let (numkeys, keys_end) = match parse_numkeys_and_keys(args, 0, "zunion") {
        Ok(v) => v,
        Err(outcome) => return outcome,
    };
    let keys = &args[1..keys_end];

    let mut with_scores = false;
    let option_args = &args[keys_end..];

    let mut filtered_options: Vec<Bytes> = Vec::with_capacity(option_args.len());
    for option in option_args {
        if option.eq_ignore_ascii_case(b"WITHSCORES") {
            with_scores = true;
        } else {
            filtered_options.push(option.clone());
        }
    }

    let (weights, aggregate) = match parse_weights_aggregate(&filtered_options, 0, numkeys) {
        Ok(v) => v,
        Err(e) => return CommandOutcome::reply(e),
    };

    let now = now_ms();
    let mut db = server.db_mut(client.selected_db);
    for key in keys {
        purge_expired_key(&mut db, key, now);
    }

    let result = match compute_union(&db, keys, &weights, aggregate) {
        Ok(v) => v,
        Err(outcome) => return outcome,
    };

    CommandOutcome::reply(RespFrame::Array(entries_to_resp(&result, with_scores)))
}

pub(super) fn cmd_zunionstore(
    args: &[Bytes],
    server: &mut ServerState,
    client: &ClientState,
) -> CommandOutcome {
    if args.len() < 3 {
        return wrong_arity("zunionstore");
    }

    let dest = &args[0];
    let (numkeys, keys_end) = match parse_numkeys_and_keys(args, 1, "zunionstore") {
        Ok(v) => v,
        Err(outcome) => return outcome,
    };
    let keys = &args[2..keys_end];

    let (weights, aggregate) = match parse_weights_aggregate(args, keys_end, numkeys) {
        Ok(v) => v,
        Err(e) => return CommandOutcome::reply(e),
    };

    let now = now_ms();
    let mut db = server.db_mut(client.selected_db);
    for key in keys {
        purge_expired_key(&mut db, key, now);
    }

    let result = match compute_union(&db, keys, &weights, aggregate) {
        Ok(v) => v,
        Err(outcome) => return outcome,
    };

    let count = result.len() as i64;
    if result.is_empty() {
        db.remove(dest);
    } else {
        let mut zset = SortedSet::default();
        for (member, score) in &result {
            zset.insert(member.clone(), *score);
        }
        db.insert(dest.clone(), StoredValue::sorted_set(zset, None));
    }

    CommandOutcome::reply(RespFrame::Integer(count))
}

pub(super) fn cmd_zinter(
    args: &[Bytes],
    server: &mut ServerState,
    client: &ClientState,
) -> CommandOutcome {
    if args.len() < 2 {
        return wrong_arity("zinter");
    }

    let (numkeys, keys_end) = match parse_numkeys_and_keys(args, 0, "zinter") {
        Ok(v) => v,
        Err(outcome) => return outcome,
    };
    let keys = &args[1..keys_end];

    let mut with_scores = false;
    let option_args = &args[keys_end..];

    let mut filtered_options: Vec<Bytes> = Vec::with_capacity(option_args.len());
    for option in option_args {
        if option.eq_ignore_ascii_case(b"WITHSCORES") {
            with_scores = true;
        } else {
            filtered_options.push(option.clone());
        }
    }

    let (weights, aggregate) = match parse_weights_aggregate(&filtered_options, 0, numkeys) {
        Ok(v) => v,
        Err(e) => return CommandOutcome::reply(e),
    };

    let now = now_ms();
    let mut db = server.db_mut(client.selected_db);
    for key in keys {
        purge_expired_key(&mut db, key, now);
    }

    let result = match compute_inter(&db, keys, &weights, aggregate) {
        Ok(v) => v,
        Err(outcome) => return outcome,
    };

    CommandOutcome::reply(RespFrame::Array(entries_to_resp(&result, with_scores)))
}

pub(super) fn cmd_zinterstore(
    args: &[Bytes],
    server: &mut ServerState,
    client: &ClientState,
) -> CommandOutcome {
    if args.len() < 3 {
        return wrong_arity("zinterstore");
    }

    let dest = &args[0];
    let (numkeys, keys_end) = match parse_numkeys_and_keys(args, 1, "zinterstore") {
        Ok(v) => v,
        Err(outcome) => return outcome,
    };
    let keys = &args[2..keys_end];

    let (weights, aggregate) = match parse_weights_aggregate(args, keys_end, numkeys) {
        Ok(v) => v,
        Err(e) => return CommandOutcome::reply(e),
    };

    let now = now_ms();
    let mut db = server.db_mut(client.selected_db);
    for key in keys {
        purge_expired_key(&mut db, key, now);
    }

    let result = match compute_inter(&db, keys, &weights, aggregate) {
        Ok(v) => v,
        Err(outcome) => return outcome,
    };

    let count = result.len() as i64;
    if result.is_empty() {
        db.remove(dest);
    } else {
        let mut zset = SortedSet::default();
        for (member, score) in &result {
            zset.insert(member.clone(), *score);
        }
        db.insert(dest.clone(), StoredValue::sorted_set(zset, None));
    }

    CommandOutcome::reply(RespFrame::Integer(count))
}

pub(super) fn cmd_zintercard(
    args: &[Bytes],
    server: &mut ServerState,
    client: &ClientState,
) -> CommandOutcome {
    if args.len() < 2 {
        return wrong_arity("zintercard");
    }

    let (_numkeys, keys_end) = match parse_numkeys_and_keys(args, 0, "zintercard") {
        Ok(v) => v,
        Err(outcome) => return outcome,
    };
    let keys = &args[1..keys_end];

    let mut limit = 0usize;
    let mut idx = keys_end;
    while idx < args.len() {
        if args[idx].eq_ignore_ascii_case(b"LIMIT") {
            let Some(limit_raw) = args.get(idx + 1) else {
                return CommandOutcome::reply(err("ERR syntax error"));
            };
            let Some(parsed) = parse_i64(limit_raw) else {
                return CommandOutcome::reply(err("ERR value is not an integer or out of range"));
            };
            if parsed < 0 {
                return CommandOutcome::reply(err("ERR LIMIT can't be negative"));
            }
            let Ok(parsed_limit) = usize::try_from(parsed) else {
                return CommandOutcome::reply(err("ERR value is not an integer or out of range"));
            };
            limit = parsed_limit;
            idx += 2;
        } else {
            return CommandOutcome::reply(err("ERR syntax error"));
        }
    }

    let now = now_ms();
    let mut db = server.db_mut(client.selected_db);
    for key in keys {
        purge_expired_key(&mut db, key, now);
    }

    if keys.is_empty() {
        return CommandOutcome::reply(RespFrame::Integer(0));
    }

    let mut sets: Vec<Vec<(Bytes, f64)>> = Vec::with_capacity(keys.len());
    for key in keys {
        match read_zset_or_empty(&db, key) {
            Ok(entries) => {
                if entries.is_empty() {
                    return CommandOutcome::reply(RespFrame::Integer(0));
                }
                sets.push(entries);
            }
            Err(outcome) => return outcome,
        }
    }

    let mut smallest_idx = 0usize;
    let mut smallest_len = sets[0].len();
    for (index, set) in sets.iter().enumerate().skip(1) {
        if set.len() < smallest_len {
            smallest_idx = index;
            smallest_len = set.len();
        }
    }

    let mut count = 0usize;
    let candidate_members: Vec<Bytes> = sets[smallest_idx]
        .iter()
        .map(|(member, _)| member.clone())
        .collect();

    let mut member_sets: Vec<HashSet<&Bytes>> = Vec::with_capacity(sets.len());
    for set in &sets {
        member_sets.push(set.iter().map(|(member, _)| member).collect());
    }

    for member in &candidate_members {
        if member_sets.iter().all(|set| set.contains(member)) {
            count += 1;
            if limit > 0 && count >= limit {
                break;
            }
        }
    }

    CommandOutcome::reply(RespFrame::Integer(count as i64))
}

pub(super) fn cmd_zdiff(
    args: &[Bytes],
    server: &mut ServerState,
    client: &ClientState,
) -> CommandOutcome {
    if args.len() < 2 {
        return wrong_arity("zdiff");
    }

    let (_numkeys, keys_end) = match parse_numkeys_and_keys(args, 0, "zdiff") {
        Ok(v) => v,
        Err(outcome) => return outcome,
    };
    let keys = &args[1..keys_end];

    let with_scores = args
        .get(keys_end)
        .is_some_and(|option| option.eq_ignore_ascii_case(b"WITHSCORES"));

    let now = now_ms();
    let mut db = server.db_mut(client.selected_db);
    for key in keys {
        purge_expired_key(&mut db, key, now);
    }

    let result = match compute_diff(&db, keys) {
        Ok(v) => v,
        Err(outcome) => return outcome,
    };

    CommandOutcome::reply(RespFrame::Array(entries_to_resp(&result, with_scores)))
}

pub(super) fn cmd_zdiffstore(
    args: &[Bytes],
    server: &mut ServerState,
    client: &ClientState,
) -> CommandOutcome {
    if args.len() < 3 {
        return wrong_arity("zdiffstore");
    }

    let dest = &args[0];
    let (_numkeys, keys_end) = match parse_numkeys_and_keys(args, 1, "zdiffstore") {
        Ok(v) => v,
        Err(outcome) => return outcome,
    };
    let keys = &args[2..keys_end];

    let now = now_ms();
    let mut db = server.db_mut(client.selected_db);
    for key in keys {
        purge_expired_key(&mut db, key, now);
    }

    let result = match compute_diff(&db, keys) {
        Ok(v) => v,
        Err(outcome) => return outcome,
    };

    let count = result.len() as i64;
    if result.is_empty() {
        db.remove(dest);
    } else {
        let mut zset = SortedSet::default();
        for (member, score) in &result {
            zset.insert(member.clone(), *score);
        }
        db.insert(dest.clone(), StoredValue::sorted_set(zset, None));
    }

    CommandOutcome::reply(RespFrame::Integer(count))
}
