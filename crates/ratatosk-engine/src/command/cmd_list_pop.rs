use bytes::Bytes;

use ratatosk_resp::frame::RespFrame;

use crate::keyspace::{ServerState, purge_expired_key};

use super::{
    ClientState, CommandOutcome, err, now_ms, parse_i64, wrong_arity, wrong_type_response,
};

pub(super) const MAX_LIST_POP_COUNT: usize = 100_000;
pub(super) const MAX_LIST_NUMKEYS: usize = 10_000;

pub(super) fn cmd_lpop(
    args: &[Bytes],
    server: &mut ServerState,
    client: &ClientState,
) -> CommandOutcome {
    cmd_pop(args, server, client, true, "lpop")
}

pub(super) fn cmd_rpop(
    args: &[Bytes],
    server: &mut ServerState,
    client: &ClientState,
) -> CommandOutcome {
    cmd_pop(args, server, client, false, "rpop")
}

pub(super) fn cmd_pop(
    args: &[Bytes],
    server: &mut ServerState,
    client: &ClientState,
    left: bool,
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
        if parsed_count > MAX_LIST_POP_COUNT {
            return CommandOutcome::reply(err("ERR count is out of range"));
        }
        Some(parsed_count)
    } else {
        None
    };

    if matches!(count, Some(0)) {
        return CommandOutcome::reply(RespFrame::Array(vec![]));
    }

    let now = now_ms();
    let mut db = server.db_mut(client.selected_db);
    purge_expired_key(&mut db, key, now);

    if !db.contains_key(key) {
        return CommandOutcome::reply(RespFrame::BulkString(None));
    }

    let mut remove_key = false;
    let response = {
        let Some(entry) = db.get_mut(key) else {
            return CommandOutcome::reply(RespFrame::BulkString(None));
        };
        let Some(list) = entry.as_list_mut() else {
            return wrong_type_response();
        };

        match count {
            None => {
                let popped = if left {
                    list.pop_front()
                } else {
                    list.pop_back()
                };
                if list.is_empty() {
                    remove_key = true;
                }
                CommandOutcome::reply(RespFrame::BulkString(popped))
            }
            Some(count) => {
                let mut items = Vec::with_capacity(count);
                for _ in 0..count {
                    let popped = if left {
                        list.pop_front()
                    } else {
                        list.pop_back()
                    };
                    let Some(popped) = popped else {
                        break;
                    };
                    items.push(RespFrame::BulkString(Some(popped)));
                }
                if list.is_empty() {
                    remove_key = true;
                }
                CommandOutcome::reply(RespFrame::Array(items))
            }
        }
    };

    if remove_key {
        db.remove(key);
    }

    response
}
