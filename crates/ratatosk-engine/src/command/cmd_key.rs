use bytes::Bytes;

use crate::expiry::{ExpireCondition, ExpireMode, ExpireTimeMode, TtlMode};
use crate::keyspace::{ServerState, purge_expired_key};
use crate::security::next_audit_stamp;
use ratatosk_resp::frame::RespFrame;

use super::{
    ClientState, CommandOutcome, err, expire_condition_matches, now_ms, parse_expire_condition,
    parse_i64, parse_usize, to_expire_target_ms, to_uppercase_bytes, wrong_arity,
};

pub(super) fn cmd_persist(
    args: &[Bytes],
    server: &mut ServerState,
    client: &ClientState,
) -> CommandOutcome {
    let [key] = args else {
        return wrong_arity("persist");
    };

    let now = now_ms();
    let mut db = server.db_mut(client.selected_db);
    purge_expired_key(&mut db, key, now);

    let removed = db
        .get(key)
        .is_some_and(|entry| entry.expire_at_ms().is_some());
    if !db.set_key_expiry(key, None) {
        return CommandOutcome::reply(RespFrame::Integer(0));
    }

    CommandOutcome::reply(RespFrame::Integer(if removed { 1 } else { 0 }))
}

pub(super) fn cmd_del(
    args: &[Bytes],
    server: &mut ServerState,
    client: &ClientState,
) -> CommandOutcome {
    cmd_delete_like(args, server, client, "del")
}

pub(super) fn cmd_exists(
    args: &[Bytes],
    server: &mut ServerState,
    client: &ClientState,
) -> CommandOutcome {
    if args.is_empty() {
        return wrong_arity("exists");
    }

    let now = now_ms();
    let (count, total_keys) = {
        let mut db = server.db_mut(client.selected_db);
        let mut count = 0i64;
        let total_keys = args.len() as u64;
        for key in args {
            purge_expired_key(&mut db, key, now);
            if db.contains_key(key) {
                count += 1;
            }
        }
        (count, total_keys)
    };

    let hits = count as u64;
    let misses = total_keys.saturating_sub(hits);
    server.stats.add_keyspace_hits(hits);
    server.stats.add_keyspace_misses(misses);

    CommandOutcome::reply(RespFrame::Integer(count))
}

pub(super) fn cmd_touch(
    args: &[Bytes],
    server: &mut ServerState,
    client: &ClientState,
) -> CommandOutcome {
    if args.is_empty() {
        return wrong_arity("touch");
    }

    let now = now_ms();
    let mut db = server.db_mut(client.selected_db);
    let mut count = 0i64;
    for key in args {
        purge_expired_key(&mut db, key, now);
        if db.contains_key(key) {
            count += 1;
        }
    }

    CommandOutcome::reply(RespFrame::Integer(count))
}

pub(super) fn cmd_unlink(
    args: &[Bytes],
    server: &mut ServerState,
    client: &ClientState,
) -> CommandOutcome {
    cmd_delete_like(args, server, client, "unlink")
}

fn cmd_delete_like(
    args: &[Bytes],
    server: &mut ServerState,
    client: &ClientState,
    command: &str,
) -> CommandOutcome {
    if args.is_empty() {
        return wrong_arity(command);
    }

    let now = now_ms();
    let mut db = server.db_mut(client.selected_db);
    let mut removed = 0i64;
    for key in args {
        purge_expired_key(&mut db, key, now);
        if db.remove(key).is_some() {
            removed += 1;
        }
    }

    let command_upper = command.to_ascii_uppercase();
    let payload = format!(
        "event=KEY_DELETE client_id={} db={} command={} requested_keys={} removed_keys={}",
        client.id(),
        client.selected_db,
        command_upper,
        args.len(),
        removed
    );
    let stamp = next_audit_stamp("KEY_DELETE", &payload);
    tracing::info!(
        target = "ratatosk::audit",
        event = "KEY_DELETE",
        audit_seq = stamp.seq,
        audit_prev_hash = %stamp.prev_hash,
        audit_hash = %stamp.hash,
        client_id = client.id(),
        db = client.selected_db,
        command = command_upper,
        requested_keys = args.len(),
        removed_keys = removed,
        "key deletion command executed"
    );

    CommandOutcome::reply(RespFrame::Integer(removed))
}

pub(super) fn cmd_rename(
    args: &[Bytes],
    server: &mut ServerState,
    client: &ClientState,
) -> CommandOutcome {
    cmd_rename_with_mode(args, server, client, false, "rename")
}

pub(super) fn cmd_renamenx(
    args: &[Bytes],
    server: &mut ServerState,
    client: &ClientState,
) -> CommandOutcome {
    cmd_rename_with_mode(args, server, client, true, "renamenx")
}

pub(super) fn cmd_rename_with_mode(
    args: &[Bytes],
    server: &mut ServerState,
    client: &ClientState,
    only_if_missing: bool,
    command_name: &str,
) -> CommandOutcome {
    let [source, target] = args else {
        return wrong_arity(command_name);
    };

    let now = now_ms();
    let mut db = server.db_mut(client.selected_db);
    purge_expired_key(&mut db, source, now);

    if !db.contains_key(source) {
        return CommandOutcome::reply(err("ERR no such key"));
    }

    if source == target {
        return if only_if_missing {
            CommandOutcome::reply(RespFrame::Integer(0))
        } else {
            CommandOutcome::reply(RespFrame::ok())
        };
    }

    purge_expired_key(&mut db, target, now);
    if only_if_missing && db.contains_key(target) {
        return CommandOutcome::reply(RespFrame::Integer(0));
    }

    db.rename(source, target);

    if only_if_missing {
        CommandOutcome::reply(RespFrame::Integer(1))
    } else {
        CommandOutcome::reply(RespFrame::ok())
    }
}

pub(super) fn cmd_move(
    args: &[Bytes],
    server: &mut ServerState,
    client: &ClientState,
) -> CommandOutcome {
    let [key, db_index_raw] = args else {
        return wrong_arity("move");
    };

    let Some(target_db_index) = parse_usize(db_index_raw) else {
        return CommandOutcome::reply(err("ERR value is not an integer or out of range"));
    };

    if target_db_index >= server.db_count() {
        return CommandOutcome::reply(err("ERR DB index is out of range"));
    }

    if target_db_index == client.selected_db {
        return CommandOutcome::reply(err("ERR source and destination objects are the same"));
    }

    let now = now_ms();

    {
        let mut source_db = server.db_mut(client.selected_db);
        purge_expired_key(&mut source_db, key, now);
    }

    if !server.db(client.selected_db).contains_key(key) {
        return CommandOutcome::reply(RespFrame::Integer(0));
    }

    {
        let mut target_db = server.db_mut(target_db_index);
        purge_expired_key(&mut target_db, key, now);
        if target_db.contains_key(key) {
            return CommandOutcome::reply(RespFrame::Integer(0));
        }
    }

    // Estimate once while taking the value; the target database adds the
    // same figure back instead of walking the value a second time.
    let moved = {
        let mut source_db = server.db_mut(client.selected_db);
        source_db.take_with_estimate(key)
    };

    let Some((moved_value, estimate)) = moved else {
        return CommandOutcome::reply(RespFrame::Integer(0));
    };

    let mut target_db = server.db_mut(target_db_index);
    target_db.insert_with_estimate(key.clone(), moved_value, estimate);
    CommandOutcome::reply(RespFrame::Integer(1))
}

pub(super) fn cmd_copy(
    args: &[Bytes],
    server: &mut ServerState,
    client: &ClientState,
) -> CommandOutcome {
    if args.len() < 2 {
        return wrong_arity("copy");
    }

    let source_key = &args[0];
    let target_key = &args[1];

    let mut target_db_index = client.selected_db;
    let mut replace = false;

    let mut idx = 2usize;
    while idx < args.len() {
        let option = to_uppercase_bytes(&args[idx]);
        match option.as_slice() {
            b"REPLACE" => {
                replace = true;
                idx += 1;
            }
            b"DB" => {
                if idx + 1 >= args.len() {
                    return CommandOutcome::reply(err("ERR syntax error"));
                }

                let Some(parsed_db_index) = parse_usize(&args[idx + 1]) else {
                    return CommandOutcome::reply(err(
                        "ERR value is not an integer or out of range",
                    ));
                };
                target_db_index = parsed_db_index;
                idx += 2;
            }
            _ => {
                return CommandOutcome::reply(err("ERR syntax error"));
            }
        }
    }

    if target_db_index >= server.db_count() {
        return CommandOutcome::reply(err("ERR DB index is out of range"));
    }

    if target_db_index == client.selected_db && source_key == target_key {
        return CommandOutcome::reply(err("ERR source and destination objects are the same"));
    }

    let now = now_ms();

    {
        let mut source_db = server.db_mut(client.selected_db);
        purge_expired_key(&mut source_db, source_key, now);
    }

    let Some(source_value) = server.db(client.selected_db).get(source_key).cloned() else {
        return CommandOutcome::reply(RespFrame::Integer(0));
    };

    let mut target_db = server.db_mut(target_db_index);
    purge_expired_key(&mut target_db, target_key, now);

    if target_db.contains_key(target_key) && !replace {
        return CommandOutcome::reply(RespFrame::Integer(0));
    }

    target_db.insert(target_key.clone(), source_value);
    CommandOutcome::reply(RespFrame::Integer(1))
}

pub(super) fn parse_scan_cursor(raw: &Bytes) -> Result<usize, RespFrame> {
    let Some(cursor_value) = parse_i64(raw) else {
        return Err(err("ERR invalid cursor"));
    };
    if cursor_value < 0 {
        return Err(err("ERR invalid cursor"));
    }

    usize::try_from(cursor_value).map_err(|_| err("ERR invalid cursor"))
}

pub(super) fn parse_scan_match_count_options(
    options: &[Bytes],
) -> Result<(Option<String>, usize), RespFrame> {
    let mut pattern: Option<String> = None;
    let mut count: usize = 10;

    let mut idx = 0usize;
    while idx < options.len() {
        let option = &options[idx];
        if option.eq_ignore_ascii_case(b"MATCH") {
            if idx + 1 >= options.len() {
                return Err(err("ERR syntax error"));
            }
            pattern = Some(String::from_utf8_lossy(&options[idx + 1]).to_string());
            idx += 2;
            continue;
        }

        if option.eq_ignore_ascii_case(b"COUNT") {
            if idx + 1 >= options.len() {
                return Err(err("ERR syntax error"));
            }
            let Some(parsed_count) = parse_i64(&options[idx + 1]) else {
                return Err(err("ERR value is not an integer or out of range"));
            };
            if parsed_count < 1 {
                return Err(err("ERR syntax error"));
            }
            let Ok(parsed_count) = usize::try_from(parsed_count) else {
                return Err(err("ERR syntax error"));
            };
            count = parsed_count;
            idx += 2;
            continue;
        }

        return Err(err("ERR syntax error"));
    }

    Ok((pattern, count))
}

pub(super) fn scan_collect_indexes<T, F>(
    items: &[T],
    cursor: usize,
    count: usize,
    mut is_match: F,
) -> (usize, Vec<usize>)
where
    F: FnMut(&T) -> bool,
{
    if items.is_empty() {
        return (0, vec![]);
    }

    let mut idx = cursor.min(items.len());
    let mut scanned = 0usize;
    let mut matched = Vec::new();

    while idx < items.len() && scanned < count {
        if is_match(&items[idx]) {
            matched.push(idx);
        }
        idx += 1;
        scanned += 1;
    }

    let next_cursor = if idx >= items.len() { 0 } else { idx };
    (next_cursor, matched)
}

pub(super) fn scan_reply(next_cursor: usize, entries: Vec<RespFrame>) -> CommandOutcome {
    CommandOutcome::reply(RespFrame::Array(vec![
        RespFrame::BulkString(Some(Bytes::from(next_cursor.to_string()))),
        RespFrame::Array(entries),
    ]))
}

pub(super) fn cmd_select(
    args: &[Bytes],
    server: &ServerState,
    client: &mut ClientState,
) -> CommandOutcome {
    let [db_index_raw] = args else {
        return wrong_arity("select");
    };

    let Some(db_index) = parse_usize(db_index_raw) else {
        return CommandOutcome::reply(err("ERR value is not an integer or out of range"));
    };

    if db_index >= server.db_count() {
        return CommandOutcome::reply(err("ERR DB index is out of range"));
    }

    client.selected_db = db_index;
    CommandOutcome::reply(RespFrame::ok())
}

pub(super) fn cmd_expire_with_mode(
    args: &[Bytes],
    server: &mut ServerState,
    client: &ClientState,
    mode: ExpireMode,
    command_name: &str,
) -> CommandOutcome {
    if args.len() < 2 || args.len() > 3 {
        return wrong_arity(command_name);
    }

    let key = &args[0];
    let Some(timeout_raw) = parse_i64(&args[1]) else {
        return CommandOutcome::reply(err("ERR value is not an integer or out of range"));
    };

    let condition = if args.len() == 3 {
        parse_expire_condition(&args[2])
    } else {
        Some(ExpireCondition::None)
    };
    let Some(condition) = condition else {
        return CommandOutcome::reply(err("ERR syntax error"));
    };

    let now = now_ms();
    let target = to_expire_target_ms(timeout_raw, mode, now);

    let mut db = server.db_mut(client.selected_db);
    purge_expired_key(&mut db, key, now);

    let Some(current_expire) = db.get(key).map(|entry| entry.expire_at_ms()) else {
        return CommandOutcome::reply(RespFrame::Integer(0));
    };

    if !expire_condition_matches(condition, current_expire, target) {
        return CommandOutcome::reply(RespFrame::Integer(0));
    }

    if target <= now {
        db.remove(key);
        return CommandOutcome::reply(RespFrame::Integer(1));
    }

    if db.set_key_expiry(key, Some(target)) {
        return CommandOutcome::reply(RespFrame::Integer(1));
    }

    CommandOutcome::reply(RespFrame::Integer(0))
}

pub(super) fn cmd_ttl_with_mode(
    args: &[Bytes],
    server: &mut ServerState,
    client: &ClientState,
    mode: TtlMode,
    command_name: &str,
) -> CommandOutcome {
    let [key] = args else {
        return wrong_arity(command_name);
    };

    let now = now_ms();
    let mut db = server.db_mut(client.selected_db);
    purge_expired_key(&mut db, key, now);

    let Some(entry) = db.get(key) else {
        return CommandOutcome::reply(RespFrame::Integer(-2));
    };

    let Some(expire_at_ms) = entry.expire_at_ms() else {
        return CommandOutcome::reply(RespFrame::Integer(-1));
    };

    let remaining_ms = expire_at_ms.saturating_sub(now);
    if remaining_ms <= 0 {
        db.remove(key);
        return CommandOutcome::reply(RespFrame::Integer(-2));
    }

    let ttl = match mode {
        TtlMode::Seconds => remaining_ms / 1000,
        TtlMode::Milliseconds => remaining_ms,
    };
    CommandOutcome::reply(RespFrame::Integer(ttl))
}

pub(super) fn cmd_expiretime_with_mode(
    args: &[Bytes],
    server: &mut ServerState,
    client: &ClientState,
    mode: ExpireTimeMode,
    command_name: &str,
) -> CommandOutcome {
    let [key] = args else {
        return wrong_arity(command_name);
    };

    let now = now_ms();
    let mut db = server.db_mut(client.selected_db);
    purge_expired_key(&mut db, key, now);

    let Some(entry) = db.get(key) else {
        return CommandOutcome::reply(RespFrame::Integer(-2));
    };

    let Some(expire_at_ms) = entry.expire_at_ms() else {
        return CommandOutcome::reply(RespFrame::Integer(-1));
    };

    let value = match mode {
        ExpireTimeMode::Seconds => expire_at_ms / 1000,
        ExpireTimeMode::Milliseconds => expire_at_ms,
    };
    CommandOutcome::reply(RespFrame::Integer(value))
}
