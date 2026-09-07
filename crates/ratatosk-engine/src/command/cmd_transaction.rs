use bytes::Bytes;

use ratatosk_resp::frame::RespFrame;

use crate::keyspace::{AtomicStatsState, ServerState, purge_expired_key};

use super::{
    ClientState, CommandOutcome, DurabilityEffects, DurableCommand, ServerAccess, TransactionState,
    WatchedKey, err, execute, now_ms, wrong_arity,
};

pub(super) fn cmd_multi(args: &[Bytes], client: &mut ClientState) -> CommandOutcome {
    if !args.is_empty() {
        return wrong_arity("multi");
    }

    if client.tx_state.in_multi() {
        return CommandOutcome::reply(err("ERR MULTI calls can not be nested"));
    }

    client.tx_state = TransactionState::InTransaction {
        queue: Vec::new(),
        has_error: false,
    };
    CommandOutcome::reply(RespFrame::ok())
}

pub(super) fn cmd_exec(
    args: &[Bytes],
    server: &mut ServerState,
    client: &mut ClientState,
    atomic_stats: Option<&AtomicStatsState>,
) -> CommandOutcome {
    if !args.is_empty() {
        return wrong_arity("exec");
    }

    if !client.tx_state.in_multi() {
        return CommandOutcome::reply(err("ERR EXEC without MULTI"));
    }

    if client.tx_state.has_error() {
        client.tx_state = TransactionState::default();
        client.watched.clear();
        return CommandOutcome::reply(err(
            "EXECABORT Transaction discarded because of previous errors.",
        ));
    }

    let watched_dirty = client
        .watched
        .iter()
        .any(|((db_index, key), watch)| server.key_version(*db_index, key) != watch.version);

    let queued = match std::mem::take(&mut client.tx_state) {
        TransactionState::InTransaction { queue, .. } => queue,
        TransactionState::Normal => Vec::new(),
    };
    client.tx_state = TransactionState::default();
    client.watched.clear();

    if watched_dirty {
        return CommandOutcome::reply(RespFrame::NullArray);
    }

    let overcounted = queued.len() as u64;
    let mut replies = Vec::with_capacity(queued.len());
    let mut reply_protocol = client.protocol_version();
    let mut segment_start = 0;
    let mut durable_commands = Vec::<DurableCommand>::new();
    let mut config_dirty = false;
    let mut acl_dirty = false;
    for argv in queued {
        let frame = RespFrame::Array(
            argv.into_iter()
                .map(|value| RespFrame::BulkString(Some(value)))
                .collect(),
        );
        let mut access = ServerAccess::new_with_optional_atomic_stats(server, atomic_stats);
        let outcome = execute(frame, &mut access, client);
        config_dirty |= outcome.config_dirty;
        acl_dirty |= outcome.acl_dirty;
        if let Some(effects) = client.take_durability_effects() {
            durable_commands.extend(effects.commands);
        }
        if client.protocol_version() != reply_protocol {
            // Only the completed segment belongs to the previous version.
            // Each reply is wrapped at most once, even with repeated HELLOs.
            for reply in &mut replies[segment_start..] {
                let previous = std::mem::replace(reply, RespFrame::Null);
                *reply = RespFrame::Versioned {
                    version: reply_protocol,
                    frame: Box::new(previous),
                };
            }
            segment_start = replies.len();
            reply_protocol = client.protocol_version();
        }
        replies.push(outcome.response);
    }

    // Each queued command incremented total_commands_processed via execute(),
    // but Redis counts EXEC as a single command. Subtract the overcounted amount.
    server.stats.adjust_commands_processed_by(overcounted);
    if let Some(stats) = atomic_stats {
        stats.adjust_commands_processed_by(overcounted);
    }

    if !durable_commands.is_empty() {
        client.set_durability_effects(Some(DurabilityEffects::transaction(durable_commands)));
    }
    let mut outcome = CommandOutcome::reply(RespFrame::Array(replies));
    outcome.config_dirty = config_dirty;
    outcome.acl_dirty = acl_dirty;
    outcome
}

pub(super) fn cmd_discard(args: &[Bytes], client: &mut ClientState) -> CommandOutcome {
    if !args.is_empty() {
        return wrong_arity("discard");
    }

    if !client.tx_state.in_multi() {
        return CommandOutcome::reply(err("ERR DISCARD without MULTI"));
    }

    client.tx_state = TransactionState::default();
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

    if client.tx_state.in_multi() {
        return CommandOutcome::reply(err("ERR WATCH inside MULTI is not allowed"));
    }

    let db_index = client.selected_db;
    let now = now_ms();
    {
        let mut db = server.db_mut(db_index);
        for key in args {
            purge_expired_key(&mut db, key, now);
        }
    }

    for key in args {
        let version = server.key_version(db_index, key);
        client
            .watched
            .insert((db_index, key.clone()), WatchedKey { version });
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

#[cfg(test)]
mod tests {
    use super::*;

    fn argv(parts: &[&str]) -> Vec<Bytes> {
        parts
            .iter()
            .map(|part| Bytes::copy_from_slice(part.as_bytes()))
            .collect()
    }

    #[test]
    fn exec_aggregates_inner_config_and_acl_dirty_flags() {
        let mut server = ServerState::with_default_dbs();
        let mut client = ClientState {
            tx_state: TransactionState::InTransaction {
                queue: vec![
                    argv(&["CONFIG", "SET", "hz", "20"]),
                    argv(&["ACL", "SETUSER", "lifecycle", "on", "nopass", "+@all"]),
                ],
                has_error: false,
            },
            ..ClientState::default()
        };

        let outcome = cmd_exec(&[], &mut server, &mut client, None);

        assert!(outcome.config_dirty);
        assert!(outcome.acl_dirty);
    }
}
