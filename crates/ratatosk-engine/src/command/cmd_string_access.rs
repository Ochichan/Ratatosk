use bytes::Bytes;

use ratatosk_resp::frame::RespFrame;

use crate::expiry::{ExpireMode, GetExPolicy, SetExpirePolicy};
use crate::keyspace::{ServerState, StoredValue, purge_expired_key};

use super::{
    ClientState, CommandOutcome, err, now_ms, parse_command_expire_at_ms, parse_getex_policy,
    parse_i64, wrong_arity, wrong_type_response,
};

pub(super) fn cmd_set(
    args: &[Bytes],
    server: &mut ServerState,
    client: &ClientState,
) -> CommandOutcome {
    if args.len() < 2 {
        return wrong_arity("set");
    }

    let key = args[0].clone();
    let value = args[1].clone();

    let mut nx = false;
    let mut xx = false;
    let mut get_old = false;
    let mut expire_policy = SetExpirePolicy::None;
    let now = now_ms();

    let mut idx = 2usize;
    while idx < args.len() {
        let option = &args[idx];
        if option.eq_ignore_ascii_case(b"NX") {
            nx = true;
            idx += 1;
        } else if option.eq_ignore_ascii_case(b"XX") {
            xx = true;
            idx += 1;
        } else if option.eq_ignore_ascii_case(b"GET") {
            get_old = true;
            idx += 1;
        } else if option.eq_ignore_ascii_case(b"KEEPTTL") {
            if !matches!(expire_policy, SetExpirePolicy::None) {
                return CommandOutcome::reply(err("ERR syntax error"));
            }
            expire_policy = SetExpirePolicy::KeepTtl;
            idx += 1;
        } else if option.eq_ignore_ascii_case(b"EX")
            || option.eq_ignore_ascii_case(b"PX")
            || option.eq_ignore_ascii_case(b"EXAT")
            || option.eq_ignore_ascii_case(b"PXAT")
        {
            if idx + 1 >= args.len() || !matches!(expire_policy, SetExpirePolicy::None) {
                return CommandOutcome::reply(err("ERR syntax error"));
            }

            let Some(raw) = parse_i64(&args[idx + 1]) else {
                return CommandOutcome::reply(err("ERR value is not an integer or out of range"));
            };
            if raw <= 0 {
                return CommandOutcome::reply(err("ERR invalid expire time in 'set' command"));
            }

            let at_ms = if option.eq_ignore_ascii_case(b"EX") {
                now.saturating_add(raw.saturating_mul(1000))
            } else if option.eq_ignore_ascii_case(b"PX") {
                now.saturating_add(raw)
            } else if option.eq_ignore_ascii_case(b"EXAT") {
                raw.saturating_mul(1000)
            } else {
                raw
            };
            expire_policy = SetExpirePolicy::AtMs(at_ms);
            idx += 2;
        } else {
            return CommandOutcome::reply(err("ERR syntax error"));
        }
    }

    if nx && xx {
        return CommandOutcome::reply(err("ERR syntax error"));
    }

    let mut db = server.db_mut(client.selected_db);
    purge_expired_key(&mut db, &key, now);

    let existing_entry = db.get(&key);
    let should_set = (!nx || existing_entry.is_none()) && (!xx || existing_entry.is_some());

    if !should_set {
        if get_old {
            return match existing_entry {
                Some(entry) if entry.is_string() => {
                    CommandOutcome::reply(RespFrame::BulkString(entry.as_string().cloned()))
                }
                Some(_) => wrong_type_response(),
                None => CommandOutcome::reply(RespFrame::BulkString(None)),
            };
        }
        return CommandOutcome::reply(RespFrame::BulkString(None));
    }

    if get_old && existing_entry.is_some_and(|entry| !entry.is_string()) {
        return wrong_type_response();
    }

    let previous_value = if get_old {
        existing_entry.and_then(|entry| entry.as_string().cloned())
    } else {
        None
    };

    let expire_at_ms = match expire_policy {
        SetExpirePolicy::None => None,
        SetExpirePolicy::KeepTtl => existing_entry.and_then(|entry| entry.expire_at_ms),
        SetExpirePolicy::AtMs(ts) => Some(ts),
    };

    if let Some(expire_ts) = expire_at_ms {
        if expire_ts <= now {
            db.remove(&key);
        } else {
            db.insert(key, StoredValue::string(value, expire_at_ms));
        }
    } else {
        db.insert(key, StoredValue::string(value, expire_at_ms));
    }

    if get_old {
        return CommandOutcome::reply(RespFrame::BulkString(previous_value));
    }

    CommandOutcome::reply(RespFrame::ok())
}

pub(super) fn cmd_get(
    args: &[Bytes],
    server: &mut ServerState,
    client: &ClientState,
) -> CommandOutcome {
    let [key] = args else {
        return wrong_arity("get");
    };

    let now = now_ms();
    let result = {
        let mut db = server.db_mut(client.selected_db);
        purge_expired_key(&mut db, key, now);

        match db.get(key) {
            None => Err(false),
            Some(entry) if !entry.is_string() => Err(true),
            Some(entry) => Ok(entry.as_string().cloned()),
        }
    };

    match result {
        Err(false) => {
            server.stats.mark_keyspace_miss();
            CommandOutcome::reply(RespFrame::BulkString(None))
        }
        Err(true) => wrong_type_response(),
        Ok(value) => {
            server.stats.mark_keyspace_hit();
            CommandOutcome::reply(RespFrame::BulkString(value))
        }
    }
}

pub(super) fn cmd_setex_with_mode(
    args: &[Bytes],
    server: &mut ServerState,
    client: &ClientState,
    mode: ExpireMode,
    command_name: &str,
) -> CommandOutcome {
    let [key, timeout_raw, value] = args else {
        return wrong_arity(command_name);
    };

    let now = now_ms();
    let expire_at_ms = match parse_command_expire_at_ms(timeout_raw, mode, now, command_name) {
        Ok(v) => v,
        Err(response) => return CommandOutcome::reply(response),
    };

    let mut db = server.db_mut(client.selected_db);
    purge_expired_key(&mut db, key, now);

    if expire_at_ms <= now {
        db.remove(key);
    } else {
        db.insert(
            key.clone(),
            StoredValue::string(value.clone(), Some(expire_at_ms)),
        );
    }

    CommandOutcome::reply(RespFrame::ok())
}

pub(super) fn cmd_getdel(
    args: &[Bytes],
    server: &mut ServerState,
    client: &ClientState,
) -> CommandOutcome {
    let [key] = args else {
        return wrong_arity("getdel");
    };

    let now = now_ms();
    let mut db = server.db_mut(client.selected_db);
    purge_expired_key(&mut db, key, now);

    let Some(entry) = db.get(key) else {
        return CommandOutcome::reply(RespFrame::BulkString(None));
    };
    if !entry.is_string() {
        return wrong_type_response();
    }

    let value = db
        .remove(key)
        .and_then(|removed| removed.as_string().cloned());
    CommandOutcome::reply(RespFrame::BulkString(value))
}

pub(super) fn cmd_getex(
    args: &[Bytes],
    server: &mut ServerState,
    client: &ClientState,
) -> CommandOutcome {
    if args.is_empty() {
        return wrong_arity("getex");
    }

    let key = &args[0];
    let now = now_ms();
    let policy = match parse_getex_policy(&args[1..], now) {
        Ok(policy) => policy,
        Err(response) => return CommandOutcome::reply(response),
    };

    let mut db = server.db_mut(client.selected_db);
    purge_expired_key(&mut db, key, now);

    let Some(entry) = db.get(key) else {
        return CommandOutcome::reply(RespFrame::BulkString(None));
    };
    let Some(current_value) = entry.as_string().cloned() else {
        return wrong_type_response();
    };

    match policy {
        GetExPolicy::KeepTtl => {}
        GetExPolicy::Persist => {
            if let Some(entry) = db.get_mut(key) {
                entry.expire_at_ms = None;
            }
        }
        GetExPolicy::AtMs(expire_at_ms) => {
            if expire_at_ms <= now {
                db.remove(key);
            } else if let Some(entry) = db.get_mut(key) {
                entry.expire_at_ms = Some(expire_at_ms);
            }
        }
    }

    CommandOutcome::reply(RespFrame::BulkString(Some(current_value)))
}
