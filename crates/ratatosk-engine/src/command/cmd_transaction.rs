use bytes::Bytes;

use ratatosk_resp::frame::RespFrame;

use crate::keyspace::{ServerState, purge_expired_key};

use super::{ClientState, CommandOutcome, WatchedKey, err, execute, now_ms, wrong_arity};

pub(super) fn cmd_multi(args: &[Bytes], client: &mut ClientState) -> CommandOutcome {
    if !args.is_empty() {
        return wrong_arity("multi");
    }

    if client.in_multi {
        return CommandOutcome::reply(err("ERR MULTI calls can not be nested"));
    }

    client.in_multi = true;
    client.tx_queue.clear();
    client.tx_error = false;
    CommandOutcome::reply(RespFrame::ok())
}

pub(super) fn cmd_exec(
    args: &[Bytes],
    server: &mut ServerState,
    client: &mut ClientState,
) -> CommandOutcome {
    if !args.is_empty() {
        return wrong_arity("exec");
    }

    if !client.in_multi {
        return CommandOutcome::reply(err("ERR EXEC without MULTI"));
    }

    if client.tx_error {
        client.in_multi = false;
        client.tx_queue.clear();
        client.tx_error = false;
        client.watched.clear();
        return CommandOutcome::reply(err(
            "EXECABORT Transaction discarded because of previous errors.",
        ));
    }

    let watched_dirty = client
        .watched
        .iter()
        .any(|watch| server.key_version(watch.db_index, &watch.key) != watch.version);

    let queued = std::mem::take(&mut client.tx_queue);
    client.in_multi = false;
    client.tx_error = false;
    client.watched.clear();

    if watched_dirty {
        return CommandOutcome::reply(RespFrame::Null);
    }

    let overcounted = queued.len() as u64;
    let mut replies = Vec::with_capacity(queued.len());
    for argv in queued {
        let frame = RespFrame::Array(
            argv.into_iter()
                .map(|value| RespFrame::BulkString(Some(value)))
                .collect(),
        );
        let outcome = execute(frame, server, client);
        replies.push(outcome.response);
    }

    // Each queued command incremented total_commands_processed via execute(),
    // but Redis counts EXEC as a single command. Subtract the overcounted amount.
    server.stats.adjust_commands_processed_by(overcounted);

    CommandOutcome::reply(RespFrame::Array(replies))
}

pub(super) fn cmd_discard(args: &[Bytes], client: &mut ClientState) -> CommandOutcome {
    if !args.is_empty() {
        return wrong_arity("discard");
    }

    if !client.in_multi {
        return CommandOutcome::reply(err("ERR DISCARD without MULTI"));
    }

    client.in_multi = false;
    client.tx_queue.clear();
    client.tx_error = false;
    client.watched.clear();
    CommandOutcome::reply(RespFrame::ok())
}

pub(super) fn cmd_watch(
    args: &[Bytes],
    server: &mut ServerState,
    client: &mut ClientState,
) -> CommandOutcome {
    if args.is_empty() {
        return wrong_arity("watch");
    }

    if client.in_multi {
        return CommandOutcome::reply(err("ERR WATCH inside MULTI is not allowed"));
    }

    let db_index = client.selected_db;
    let now = now_ms();
    {
        let db = server.db_mut(db_index);
        for key in args {
            purge_expired_key(db, key, now);
        }
    }

    for key in args {
        let version = server.key_version(db_index, key);
        if let Some(existing) = client
            .watched
            .iter_mut()
            .find(|entry| entry.db_index == db_index && entry.key == *key)
        {
            existing.version = version;
        } else {
            client.watched.push(WatchedKey {
                db_index,
                key: key.clone(),
                version,
            });
        }
    }

    CommandOutcome::reply(RespFrame::ok())
}

pub(super) fn cmd_unwatch(args: &[Bytes], client: &mut ClientState) -> CommandOutcome {
    if !args.is_empty() {
        return wrong_arity("unwatch");
    }

    client.watched.clear();
    CommandOutcome::reply(RespFrame::ok())
}
