use bytes::Bytes;

use ratatosk_resp::frame::RespFrame;

use crate::keyspace::ServerState;

use super::{ClientState, CommandOutcome, err, to_uppercase_bytes, wrong_arity};

pub(super) fn cmd_subscribe(
    args: &[Bytes],
    server: &mut ServerState,
    client: &mut ClientState,
) -> CommandOutcome {
    if args.is_empty() {
        return wrong_arity("subscribe");
    }

    let mut replies = Vec::with_capacity(args.len());
    for channel in args {
        let count = server.pubsub.subscribe_channel(client.id, channel.clone());
        client.set_pubsub_subscription_count(count);
        replies.push(RespFrame::Array(vec![
            RespFrame::bulk_str("subscribe"),
            RespFrame::BulkString(Some(channel.clone())),
            RespFrame::Integer(count),
        ]));
    }

    if replies.len() == 1 {
        CommandOutcome::reply(replies.pop().unwrap_or(RespFrame::Array(vec![])))
    } else {
        CommandOutcome::reply(RespFrame::Array(replies))
    }
}

pub(super) fn cmd_ssubscribe(
    args: &[Bytes],
    server: &mut ServerState,
    client: &mut ClientState,
) -> CommandOutcome {
    if args.is_empty() {
        return wrong_arity("ssubscribe");
    }

    let mut replies = Vec::with_capacity(args.len());
    for channel in args {
        let count = server
            .pubsub
            .subscribe_shard_channel(client.id, channel.clone());
        client.set_pubsub_subscription_count(count);
        replies.push(RespFrame::Array(vec![
            RespFrame::bulk_str("ssubscribe"),
            RespFrame::BulkString(Some(channel.clone())),
            RespFrame::Integer(count),
        ]));
    }

    if replies.len() == 1 {
        CommandOutcome::reply(replies.pop().unwrap_or(RespFrame::Array(vec![])))
    } else {
        CommandOutcome::reply(RespFrame::Array(replies))
    }
}

pub(super) fn cmd_psubscribe(
    args: &[Bytes],
    server: &mut ServerState,
    client: &mut ClientState,
) -> CommandOutcome {
    if args.is_empty() {
        return wrong_arity("psubscribe");
    }

    let mut replies = Vec::with_capacity(args.len());
    for pattern in args {
        let count = server.pubsub.subscribe_pattern(client.id, pattern.clone());
        client.set_pubsub_subscription_count(count);
        replies.push(RespFrame::Array(vec![
            RespFrame::bulk_str("psubscribe"),
            RespFrame::BulkString(Some(pattern.clone())),
            RespFrame::Integer(count),
        ]));
    }

    if replies.len() == 1 {
        CommandOutcome::reply(replies.pop().unwrap_or(RespFrame::Array(vec![])))
    } else {
        CommandOutcome::reply(RespFrame::Array(replies))
    }
}

pub(super) fn cmd_publish(args: &[Bytes], server: &mut ServerState) -> CommandOutcome {
    let [channel, payload] = args else {
        return wrong_arity("publish");
    };

    let receivers = server.pubsub.publish(channel, payload);
    CommandOutcome::reply(RespFrame::Integer(receivers))
}

pub(super) fn cmd_spublish(args: &[Bytes], server: &mut ServerState) -> CommandOutcome {
    let [channel, payload] = args else {
        return wrong_arity("spublish");
    };

    let receivers = server.pubsub.publish_shard(channel, payload);
    CommandOutcome::reply(RespFrame::Integer(receivers))
}

pub(super) fn cmd_unsubscribe(
    args: &[Bytes],
    server: &mut ServerState,
    client: &mut ClientState,
) -> CommandOutcome {
    let channels = if args.is_empty() {
        server.pubsub.client_channels(client.id)
    } else {
        args.to_vec()
    };

    if channels.is_empty() {
        client.set_pubsub_subscription_count(0);
        return CommandOutcome::reply(RespFrame::Array(vec![
            RespFrame::bulk_str("unsubscribe"),
            RespFrame::BulkString(None),
            RespFrame::Integer(0),
        ]));
    }

    let mut replies = Vec::with_capacity(channels.len());
    for channel in channels {
        let count = server.pubsub.unsubscribe_channel(client.id, &channel);
        client.set_pubsub_subscription_count(count);
        replies.push(RespFrame::Array(vec![
            RespFrame::bulk_str("unsubscribe"),
            RespFrame::BulkString(Some(channel)),
            RespFrame::Integer(count),
        ]));
    }

    if replies.len() == 1 {
        CommandOutcome::reply(replies.pop().unwrap_or(RespFrame::Array(vec![])))
    } else {
        CommandOutcome::reply(RespFrame::Array(replies))
    }
}

pub(super) fn cmd_punsubscribe(
    args: &[Bytes],
    server: &mut ServerState,
    client: &mut ClientState,
) -> CommandOutcome {
    let patterns = if args.is_empty() {
        server.pubsub.client_patterns(client.id)
    } else {
        args.to_vec()
    };

    if patterns.is_empty() {
        client.set_pubsub_subscription_count(0);
        return CommandOutcome::reply(RespFrame::Array(vec![
            RespFrame::bulk_str("punsubscribe"),
            RespFrame::BulkString(None),
            RespFrame::Integer(0),
        ]));
    }

    let mut replies = Vec::with_capacity(patterns.len());
    for pattern in patterns {
        let count = server.pubsub.unsubscribe_pattern(client.id, &pattern);
        client.set_pubsub_subscription_count(count);
        replies.push(RespFrame::Array(vec![
            RespFrame::bulk_str("punsubscribe"),
            RespFrame::BulkString(Some(pattern)),
            RespFrame::Integer(count),
        ]));
    }

    if replies.len() == 1 {
        CommandOutcome::reply(replies.pop().unwrap_or(RespFrame::Array(vec![])))
    } else {
        CommandOutcome::reply(RespFrame::Array(replies))
    }
}

pub(super) fn cmd_sunsubscribe(
    args: &[Bytes],
    server: &mut ServerState,
    client: &mut ClientState,
) -> CommandOutcome {
    let channels = if args.is_empty() {
        server.pubsub.client_shard_channels(client.id)
    } else {
        args.to_vec()
    };

    if channels.is_empty() {
        client.set_pubsub_subscription_count(0);
        return CommandOutcome::reply(RespFrame::Array(vec![
            RespFrame::bulk_str("sunsubscribe"),
            RespFrame::BulkString(None),
            RespFrame::Integer(0),
        ]));
    }

    let mut replies = Vec::with_capacity(channels.len());
    for channel in channels {
        let count = server.pubsub.unsubscribe_shard_channel(client.id, &channel);
        client.set_pubsub_subscription_count(count);
        replies.push(RespFrame::Array(vec![
            RespFrame::bulk_str("sunsubscribe"),
            RespFrame::BulkString(Some(channel)),
            RespFrame::Integer(count),
        ]));
    }

    if replies.len() == 1 {
        CommandOutcome::reply(replies.pop().unwrap_or(RespFrame::Array(vec![])))
    } else {
        CommandOutcome::reply(RespFrame::Array(replies))
    }
}

pub(super) fn cmd_pubsub(args: &[Bytes], server: &ServerState) -> CommandOutcome {
    if args.is_empty() {
        return wrong_arity("pubsub");
    }

    let subcommand = to_uppercase_bytes(&args[0]);
    match subcommand.as_slice() {
        b"CHANNELS" => {
            if args.len() > 2 {
                return wrong_arity("pubsub");
            }

            let pattern = args
                .get(1)
                .map(|raw| String::from_utf8_lossy(raw).to_string());
            let channels = server.pubsub.channels_matching(pattern.as_deref());
            CommandOutcome::reply(RespFrame::Array(
                channels
                    .into_iter()
                    .map(|channel| RespFrame::BulkString(Some(channel)))
                    .collect(),
            ))
        }
        b"NUMSUB" => {
            if args.len() < 2 {
                return wrong_arity("pubsub");
            }

            let pairs = server.pubsub.numsub(&args[1..]);
            let mut out = Vec::with_capacity(pairs.len().saturating_mul(2));
            for (channel, count) in pairs {
                out.push(RespFrame::BulkString(Some(channel)));
                out.push(RespFrame::Integer(count));
            }
            CommandOutcome::reply(RespFrame::Array(out))
        }
        b"NUMPAT" => {
            if args.len() != 1 {
                return wrong_arity("pubsub");
            }
            CommandOutcome::reply(RespFrame::Integer(server.pubsub.numpat()))
        }
        b"SHARDCHANNELS" => {
            if args.len() > 2 {
                return wrong_arity("pubsub");
            }

            let pattern = args
                .get(1)
                .map(|raw| String::from_utf8_lossy(raw).to_string());
            let channels = server.pubsub.shard_channels_matching(pattern.as_deref());
            CommandOutcome::reply(RespFrame::Array(
                channels
                    .into_iter()
                    .map(|channel| RespFrame::BulkString(Some(channel)))
                    .collect(),
            ))
        }
        b"SHARDNUMSUB" => {
            if args.len() < 2 {
                return wrong_arity("pubsub");
            }

            let pairs = server.pubsub.shard_numsub(&args[1..]);
            let mut out = Vec::with_capacity(pairs.len().saturating_mul(2));
            for (channel, count) in pairs {
                out.push(RespFrame::BulkString(Some(channel)));
                out.push(RespFrame::Integer(count));
            }
            CommandOutcome::reply(RespFrame::Array(out))
        }
        b"HELP" => {
            if args.len() != 1 {
                return wrong_arity("pubsub");
            }
            CommandOutcome::reply(RespFrame::Array(vec![
                RespFrame::bulk_str("CHANNELS [pattern] -- Return active channels."),
                RespFrame::bulk_str("NUMSUB <channel> [channel ...] -- Return subscriber counts."),
                RespFrame::bulk_str("NUMPAT -- Return the number of unique pattern subscriptions."),
                RespFrame::bulk_str("SHARDCHANNELS [pattern] -- Return active shard channels."),
                RespFrame::bulk_str(
                    "SHARDNUMSUB <channel> [channel ...] -- Return shard subscriber counts.",
                ),
                RespFrame::bulk_str("HELP -- Show this help."),
            ]))
        }
        _ => CommandOutcome::reply(err(
            "ERR unknown PUBSUB subcommand or wrong number of arguments",
        )),
    }
}
