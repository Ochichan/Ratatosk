use bytes::Bytes;

use ratatosk_resp::frame::RespFrame;

use crate::keyspace::{ServerState, purge_expired_key};
use crate::object::format_f64_for_redis;

use super::cmd_list_blocking::{blocking_deadline_ms, parse_blocking_timeout_seconds};
use super::cmd_sorted_set::MAX_ZSET_NUMKEYS;
use super::{
    ClientState, CommandOutcome, err, now_ms, parse_i64, to_uppercase_bytes, wrong_arity,
    wrong_type_response,
};

const MAX_ZSET_POP_COUNT: usize = 100_000;

fn blocking_watch_keys(client: &ClientState, keys: &[Bytes]) -> Vec<(usize, Bytes)> {
    keys.iter()
        .cloned()
        .map(|key| (client.selected_db(), key))
        .collect()
}

fn build_blocking_frame(command_name: &str, args: &[Bytes]) -> RespFrame {
    let mut parts = Vec::with_capacity(1 + args.len());
    parts.push(RespFrame::BulkString(Some(Bytes::copy_from_slice(
        command_name.as_bytes(),
    ))));
    for arg in args {
        parts.push(RespFrame::BulkString(Some(arg.clone())));
    }
    RespFrame::Array(parts)
}

pub(super) fn cmd_zpopmin(
    args: &[Bytes],
    server: &mut ServerState,
    client: &ClientState,
) -> CommandOutcome {
    zpop_impl(args, server, client, true, "zpopmin")
}

pub(super) fn cmd_zpopmax(
    args: &[Bytes],
    server: &mut ServerState,
    client: &ClientState,
) -> CommandOutcome {
    zpop_impl(args, server, client, false, "zpopmax")
}

fn zpop_impl(
    args: &[Bytes],
    server: &mut ServerState,
    client: &ClientState,
    pop_min: bool,
    command_name: &str,
) -> CommandOutcome {
    if args.is_empty() || args.len() > 2 {
        return wrong_arity(command_name);
    }

    let key = &args[0];
    let count = if args.len() == 2 {
        let Some(raw_count) = parse_i64(&args[1]) else {
            return CommandOutcome::reply(err("ERR value is not an integer or out of range"));
        };
        if raw_count < 0 {
            return CommandOutcome::reply(err("ERR value is out of range, must be positive"));
        }
        let Ok(parsed_count) = usize::try_from(raw_count) else {
            return CommandOutcome::reply(err("ERR value is not an integer or out of range"));
        };
        if parsed_count > MAX_ZSET_POP_COUNT {
            return CommandOutcome::reply(err("ERR count is out of range"));
        }
        parsed_count
    } else {
        1
    };

    if count == 0 {
        return CommandOutcome::reply(RespFrame::Array(vec![]));
    }

    let now = now_ms();
    let mut db = server.db_mut(client.selected_db);
    purge_expired_key(&mut db, key, now);

    let Some(entry) = db.get_mut(key) else {
        return CommandOutcome::reply(RespFrame::Array(vec![]));
    };
    let Some(zset) = entry.as_sorted_set_mut() else {
        return wrong_type_response();
    };

    let mut popped = Vec::with_capacity(count.min(zset.len()));
    for _ in 0..count {
        let entry_opt = if pop_min {
            zset.by_score.keys().next().cloned()
        } else {
            zset.by_score.keys().next_back().cloned()
        };
        let Some(entry) = entry_opt else {
            break;
        };
        popped.push((entry.member.clone(), entry.score.value()));
        zset.remove(&entry.member);
    }

    if zset.is_empty() {
        db.remove(key);
    }

    let mut out = Vec::with_capacity(popped.len().saturating_mul(2));
    for (member, score) in &popped {
        out.push(RespFrame::BulkString(Some(member.clone())));
        out.push(RespFrame::BulkString(Some(format_f64_for_redis(*score))));
    }

    CommandOutcome::reply(RespFrame::Array(out))
}

pub(super) fn cmd_zmpop(
    args: &[Bytes],
    server: &mut ServerState,
    client: &ClientState,
) -> CommandOutcome {
    zmpop_inner(args, server, client, false)
}

pub(super) fn cmd_bzmpop(
    args: &[Bytes],
    server: &mut ServerState,
    client: &ClientState,
) -> CommandOutcome {
    zmpop_inner(args, server, client, true)
}

fn zmpop_inner(
    args: &[Bytes],
    server: &mut ServerState,
    client: &ClientState,
    blocking: bool,
) -> CommandOutcome {
    let command_name = if blocking { "bzmpop" } else { "zmpop" };
    let mut idx = 0usize;
    let mut timeout_sec = 0.0f64;

    if blocking {
        if args.len() < 4 {
            return wrong_arity(command_name);
        }
        timeout_sec = match parse_blocking_timeout_seconds(&args[0]) {
            Ok(timeout) => timeout,
            Err(response) => return CommandOutcome::reply(response),
        };
        idx = 1;
    } else if args.len() < 3 {
        return wrong_arity(command_name);
    }

    let Some(numkeys_raw) = args.get(idx) else {
        return wrong_arity(command_name);
    };
    let Some(numkeys_i64) = parse_i64(numkeys_raw) else {
        return CommandOutcome::reply(err("ERR value is not an integer or out of range"));
    };
    if numkeys_i64 <= 0 {
        return CommandOutcome::reply(err("ERR numkeys should be greater than 0"));
    }

    let Ok(numkeys) = usize::try_from(numkeys_i64) else {
        return CommandOutcome::reply(err("ERR value is not an integer or out of range"));
    };
    if numkeys > MAX_ZSET_NUMKEYS {
        return CommandOutcome::reply(err("ERR numkeys is out of range"));
    }
    let first_key_idx = idx + 1;
    let side_idx = first_key_idx.saturating_add(numkeys);
    if side_idx >= args.len() {
        return wrong_arity(command_name);
    }

    let keys = args[first_key_idx..side_idx].to_vec();
    let Some(side_raw) = args.get(side_idx) else {
        return wrong_arity(command_name);
    };
    let upper_side = to_uppercase_bytes(side_raw);
    let pop_min = match upper_side.as_slice() {
        b"MIN" => true,
        b"MAX" => false,
        _ => return CommandOutcome::reply(err("ERR syntax error")),
    };

    let mut count = 1usize;
    let option_args = &args[(side_idx + 1)..];
    if !option_args.is_empty() {
        if option_args.len() != 2 || !option_args[0].eq_ignore_ascii_case(b"COUNT") {
            return CommandOutcome::reply(err("ERR syntax error"));
        }
        let Some(parsed_count) = parse_i64(&option_args[1]) else {
            return CommandOutcome::reply(err("ERR value is not an integer or out of range"));
        };
        if parsed_count <= 0 {
            return CommandOutcome::reply(err("ERR count should be greater than 0"));
        }
        let Ok(parsed_count) = usize::try_from(parsed_count) else {
            return CommandOutcome::reply(err("ERR value is not an integer or out of range"));
        };
        if parsed_count > MAX_ZSET_POP_COUNT {
            return CommandOutcome::reply(err("ERR count is out of range"));
        }
        count = parsed_count;
    }

    if !blocking {
        return try_zmpop_once(&keys, pop_min, count, server, client);
    }

    let deadline_ms = blocking_deadline_ms(timeout_sec);

    let outcome = try_zmpop_once(&keys, pop_min, count, server, client);
    if !matches!(outcome.response, RespFrame::BulkString(None)) {
        return outcome;
    }

    if let Some(deadline) = deadline_ms {
        if now_ms() >= deadline {
            return outcome;
        }
    }

    let full_frame = build_blocking_frame("BZMPOP", args);
    CommandOutcome::blocking(
        outcome.response,
        deadline_ms,
        full_frame,
        blocking_watch_keys(client, &keys),
    )
}

fn try_zmpop_once(
    keys: &[Bytes],
    pop_min: bool,
    count: usize,
    server: &mut ServerState,
    client: &ClientState,
) -> CommandOutcome {
    let now = now_ms();
    let mut db = server.db_mut(client.selected_db);

    for key in keys {
        purge_expired_key(&mut db, key, now);

        let mut remove_key = false;
        let popped = {
            let Some(entry) = db.get_mut(key) else {
                continue;
            };
            let Some(zset) = entry.as_sorted_set_mut() else {
                return wrong_type_response();
            };
            if zset.is_empty() {
                continue;
            }

            let mut items = Vec::with_capacity(count.min(zset.len()));
            for _ in 0..count {
                let entry_opt = if pop_min {
                    zset.by_score.keys().next().cloned()
                } else {
                    zset.by_score.keys().next_back().cloned()
                };
                let Some(entry) = entry_opt else {
                    break;
                };
                items.push((entry.member.clone(), entry.score.value()));
                zset.remove(&entry.member);
            }

            if zset.is_empty() {
                remove_key = true;
            }

            items
        };

        if remove_key {
            db.remove(key);
        }

        if popped.is_empty() {
            continue;
        }

        let values: Vec<RespFrame> = popped
            .into_iter()
            .map(|(member, score)| {
                RespFrame::Array(vec![
                    RespFrame::BulkString(Some(member)),
                    RespFrame::BulkString(Some(format_f64_for_redis(score))),
                ])
            })
            .collect();

        return CommandOutcome::reply(RespFrame::Array(vec![
            RespFrame::BulkString(Some(key.clone())),
            RespFrame::Array(values),
        ]));
    }

    CommandOutcome::reply(RespFrame::BulkString(None))
}

pub(super) fn cmd_bzpopmin(
    args: &[Bytes],
    server: &mut ServerState,
    client: &ClientState,
) -> CommandOutcome {
    cmd_bzpop(args, server, client, true, "bzpopmin")
}

pub(super) fn cmd_bzpopmax(
    args: &[Bytes],
    server: &mut ServerState,
    client: &ClientState,
) -> CommandOutcome {
    cmd_bzpop(args, server, client, false, "bzpopmax")
}

fn cmd_bzpop(
    args: &[Bytes],
    server: &mut ServerState,
    client: &ClientState,
    pop_min: bool,
    command_name: &str,
) -> CommandOutcome {
    if args.len() < 2 {
        return wrong_arity(command_name);
    }

    let timeout_raw = &args[args.len() - 1];
    let timeout_sec = match parse_blocking_timeout_seconds(timeout_raw) {
        Ok(timeout) => timeout,
        Err(response) => return CommandOutcome::reply(response),
    };

    let keys = &args[..args.len() - 1];
    let deadline_ms = blocking_deadline_ms(timeout_sec);

    let outcome = try_bzpop_once(keys, server, client, pop_min);
    if !matches!(outcome.response, RespFrame::BulkString(None)) {
        return outcome;
    }

    if let Some(deadline) = deadline_ms {
        if now_ms() >= deadline {
            return outcome;
        }
    }

    let full_frame = build_blocking_frame(command_name, args);
    CommandOutcome::blocking(
        outcome.response,
        deadline_ms,
        full_frame,
        blocking_watch_keys(client, keys),
    )
}

fn try_bzpop_once(
    keys: &[Bytes],
    server: &mut ServerState,
    client: &ClientState,
    pop_min: bool,
) -> CommandOutcome {
    let now = now_ms();
    let mut db = server.db_mut(client.selected_db);

    for key in keys {
        purge_expired_key(&mut db, key, now);

        let mut remove_key = false;
        let popped = {
            let Some(entry) = db.get_mut(key) else {
                continue;
            };
            let Some(zset) = entry.as_sorted_set_mut() else {
                return wrong_type_response();
            };

            let entry_opt = if pop_min {
                zset.by_score.keys().next().cloned()
            } else {
                zset.by_score.keys().next_back().cloned()
            };

            let Some(entry) = entry_opt else {
                continue;
            };

            let member = entry.member.clone();
            let score = entry.score.value();
            zset.remove(&member);

            if zset.is_empty() {
                remove_key = true;
            }

            Some((member, score))
        };

        if remove_key {
            db.remove(key);
        }

        let Some((member, score)) = popped else {
            continue;
        };

        return CommandOutcome::reply(RespFrame::Array(vec![
            RespFrame::BulkString(Some(key.clone())),
            RespFrame::BulkString(Some(member)),
            RespFrame::BulkString(Some(format_f64_for_redis(score))),
        ]));
    }

    CommandOutcome::reply(RespFrame::BulkString(None))
}
