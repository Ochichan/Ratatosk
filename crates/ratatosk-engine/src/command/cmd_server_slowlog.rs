use bytes::Bytes;

use ratatosk_resp::frame::RespFrame;

use crate::keyspace::ServerState;

use super::{CommandOutcome, err, parse_i64, to_uppercase_bytes, wrong_arity};

pub(super) fn cmd_slowlog(args: &[Bytes], server: &mut ServerState) -> CommandOutcome {
    if args.is_empty() {
        return wrong_arity("slowlog");
    }

    let subcommand = to_uppercase_bytes(&args[0]);
    match subcommand.as_slice() {
        b"GET" => {
            if args.len() > 2 {
                return wrong_arity("slowlog");
            }
            let count = if let Some(raw) = args.get(1) {
                let Some(value) = parse_i64(raw) else {
                    return CommandOutcome::reply(err(
                        "ERR value is not an integer or out of range",
                    ));
                };
                if value < 0 {
                    return CommandOutcome::reply(err("ERR value is out of range"));
                }
                value as usize
            } else {
                10usize
            };

            let rows = server
                .stats
                .slowlog_entries()
                .iter()
                .take(count)
                .map(|entry| {
                    RespFrame::Array(vec![
                        RespFrame::Integer(entry.id),
                        RespFrame::Integer(entry.unix_time),
                        RespFrame::Integer(entry.duration_us),
                        RespFrame::Array(
                            entry
                                .argv
                                .iter()
                                .cloned()
                                .map(|arg| RespFrame::BulkString(Some(arg)))
                                .collect::<Vec<_>>(),
                        ),
                    ])
                })
                .collect::<Vec<_>>();
            CommandOutcome::reply(RespFrame::Array(rows))
        }
        b"LEN" => {
            if args.len() != 1 {
                return wrong_arity("slowlog");
            }
            CommandOutcome::reply(RespFrame::Integer(server.stats.slowlog_len() as i64))
        }
        b"RESET" => {
            if args.len() != 1 {
                return wrong_arity("slowlog");
            }
            server.stats.slowlog_reset();
            CommandOutcome::reply(RespFrame::ok())
        }
        b"HELP" => {
            if args.len() != 1 {
                return wrong_arity("slowlog");
            }
            CommandOutcome::reply(RespFrame::Array(vec![
                RespFrame::bulk_str("GET [count] -- Return the slow log entries."),
                RespFrame::bulk_str("LEN -- Return the current number of entries in the slow log."),
                RespFrame::bulk_str("RESET -- Reset the slow log."),
                RespFrame::bulk_str("HELP -- Show this help."),
            ]))
        }
        _ => CommandOutcome::reply(err("ERR syntax error")),
    }
}
