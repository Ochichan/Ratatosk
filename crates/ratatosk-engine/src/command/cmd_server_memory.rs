use bytes::Bytes;

use ratatosk_resp::frame::RespFrame;

use crate::keyspace::{ServerState, StoredValue, purge_expired_key};

use super::{ClientState, CommandOutcome, err, now_ms, parse_i64, to_uppercase_bytes, wrong_arity};

pub(super) fn cmd_memory(
    args: &[Bytes],
    server: &mut ServerState,
    client: &ClientState,
) -> CommandOutcome {
    if args.is_empty() {
        return wrong_arity("memory");
    }

    let subcommand = to_uppercase_bytes(&args[0]);
    match subcommand.as_slice() {
        b"USAGE" => cmd_memory_usage(&args[1..], server, client),
        b"STATS" => cmd_memory_stats(&args[1..], server),
        b"DOCTOR" => cmd_memory_doctor(&args[1..], server),
        b"MALLOC-STATS" => cmd_memory_malloc_stats(&args[1..]),
        b"PURGE" => cmd_memory_purge(&args[1..]),
        b"HELP" => {
            if args.len() != 1 {
                return wrong_arity("memory");
            }
            CommandOutcome::reply(RespFrame::Array(vec![
                RespFrame::bulk_str(
                    "USAGE <key> [SAMPLES <count>] -- Estimate memory usage of a key.",
                ),
                RespFrame::bulk_str("STATS -- Return allocator and dataset memory statistics."),
                RespFrame::bulk_str("DOCTOR -- Return memory health diagnosis text."),
                RespFrame::bulk_str("MALLOC-STATS -- Return allocator stats text."),
                RespFrame::bulk_str("PURGE -- Ask allocator to release free pages."),
                RespFrame::bulk_str("HELP -- Show this help."),
            ]))
        }
        _ => CommandOutcome::reply(err("ERR syntax error")),
    }
}

fn cmd_memory_usage(
    args: &[Bytes],
    server: &mut ServerState,
    client: &ClientState,
) -> CommandOutcome {
    if args.is_empty() || args.len() > 3 {
        return wrong_arity("memory");
    }

    let key = &args[0];
    if args.len() == 3 {
        if !args[1].eq_ignore_ascii_case(b"SAMPLES") {
            return CommandOutcome::reply(err("ERR syntax error"));
        }

        let Some(samples) = parse_i64(&args[2]) else {
            return CommandOutcome::reply(err("ERR value is not an integer or out of range"));
        };
        if samples < 0 {
            return CommandOutcome::reply(err("ERR value is out of range"));
        }
    } else if args.len() == 2 {
        return CommandOutcome::reply(err("ERR syntax error"));
    }

    let now = now_ms();
    let mut db = server.db_mut(client.selected_db);
    purge_expired_key(&mut db, key, now);

    let Some(value) = db.get(key) else {
        return CommandOutcome::reply(RespFrame::Null);
    };

    let estimate = estimate_value_memory_usage(key, value);
    CommandOutcome::reply(RespFrame::Integer(estimate))
}

fn estimate_value_memory_usage(key: &Bytes, value: &StoredValue) -> i64 {
    use crate::keyspace::ValueData;

    let mut total = key.len().saturating_add(64);

    match &value.data {
        ValueData::String(v) => {
            total = total.saturating_add(v.len());
        }
        ValueData::Hash(hash) => {
            total = total.saturating_add(48);
            for (field, item) in hash {
                total = total
                    .saturating_add(field.len())
                    .saturating_add(item.value.len())
                    .saturating_add(24);
            }
        }
        ValueData::List(list) => {
            total = total.saturating_add(48);
            for item in list {
                total = total.saturating_add(item.len()).saturating_add(8);
            }
        }
        ValueData::Set(set) => {
            total = total.saturating_add(48);
            for item in set {
                total = total.saturating_add(item.len()).saturating_add(16);
            }
        }
        ValueData::SortedSet(zset) => {
            total = total.saturating_add(64);
            for entry in zset.by_score.keys() {
                total = total
                    .saturating_add(entry.member.len())
                    .saturating_add(8)
                    .saturating_add(24);
            }
        }
        ValueData::Stream { entries, groups } => {
            total = total.saturating_add(64);
            for entry in entries {
                total = total.saturating_add(32);
                for (k, v) in &entry.fields {
                    total = total
                        .saturating_add(k.len())
                        .saturating_add(v.len())
                        .saturating_add(16);
                }
            }
            total = total.saturating_add(groups.len().saturating_mul(64));
        }
    }

    i64::try_from(total).unwrap_or(i64::MAX)
}

fn cmd_memory_stats(args: &[Bytes], server: &ServerState) -> CommandOutcome {
    if !args.is_empty() {
        return wrong_arity("memory");
    }

    let total_keys = (0..server.db_count())
        .map(|db_idx| server.db(db_idx).len() as i64)
        .sum::<i64>();
    let total_dataset_bytes: i64 = (0..server.db_count())
        .map(|db_idx| {
            let db = server.db(db_idx);
            db.iter()
                .map(|(key, value)| estimate_value_memory_usage(key, value))
                .sum::<i64>()
        })
        .sum();

    CommandOutcome::reply(RespFrame::Map(vec![
        (
            RespFrame::bulk_str("peak.allocated"),
            RespFrame::Integer(total_dataset_bytes),
        ),
        (
            RespFrame::bulk_str("total.allocated"),
            RespFrame::Integer(total_dataset_bytes),
        ),
        (
            RespFrame::bulk_str("dataset.bytes"),
            RespFrame::Integer(total_dataset_bytes),
        ),
        (
            RespFrame::bulk_str("dataset.keys"),
            RespFrame::Integer(total_keys),
        ),
        (
            RespFrame::bulk_str("allocator.active"),
            RespFrame::Integer(total_dataset_bytes),
        ),
    ]))
}

fn cmd_memory_doctor(args: &[Bytes], server: &ServerState) -> CommandOutcome {
    if !args.is_empty() {
        return wrong_arity("memory");
    }

    let total_keys: i64 = (0..server.db_count())
        .map(|db_idx| server.db(db_idx).len() as i64)
        .sum();
    let total_dataset_bytes: i64 = (0..server.db_count())
        .map(|db_idx| {
            let db = server.db(db_idx);
            db.iter()
                .map(|(key, value)| estimate_value_memory_usage(key, value))
                .sum::<i64>()
        })
        .sum();

    let mut concerns = Vec::new();

    if total_keys == 0 {
        concerns.push(
            "This instance has no keys loaded. It is either a fresh start or all data has been evicted/expired."
                .to_string(),
        );
    }

    if total_keys > 0 {
        let key_overhead = total_keys * 64;
        let estimated_total = total_dataset_bytes + key_overhead;
        if estimated_total > 0 && total_dataset_bytes * 100 / estimated_total < 50 {
            concerns.push(format!(
                "Dataset bytes ({total_dataset_bytes}) are less than 50% of estimated total allocation ({estimated_total}). High metadata overhead for the stored data."
            ));
        }
    }

    let report = if concerns.is_empty() {
        "Sam, I have no memory problems".to_string()
    } else {
        let mut msg = "Sam, I have a few concerns:\n\n".to_string();
        for (i, concern) in concerns.iter().enumerate() {
            msg.push_str(&format!("{}. {}\n", i + 1, concern));
        }
        msg
    };

    CommandOutcome::reply(RespFrame::bulk_str(&report))
}

fn cmd_memory_malloc_stats(args: &[Bytes]) -> CommandOutcome {
    if !args.is_empty() {
        return wrong_arity("memory");
    }

    let output = if cfg!(feature = "mimalloc") {
        "allocator:mimalloc\nstats collection unavailable in this safe build\n"
    } else {
        "allocator:system\nstats collection unavailable in this build\n"
    };

    CommandOutcome::reply(RespFrame::bulk_str(output))
}

fn cmd_memory_purge(args: &[Bytes]) -> CommandOutcome {
    if !args.is_empty() {
        return wrong_arity("memory");
    }

    CommandOutcome::reply(RespFrame::ok())
}
