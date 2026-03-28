use std::fmt::Write;

use bytes::Bytes;

use ratatosk_resp::frame::RespFrame;

use crate::keyspace::ServerState;

use super::{CommandOutcome, err, to_uppercase_bytes, wrong_arity};

pub(super) fn cmd_latency(args: &[Bytes], server: &mut ServerState) -> CommandOutcome {
    if args.is_empty() {
        return wrong_arity("latency");
    }

    let subcommand = to_uppercase_bytes(&args[0]);
    match subcommand.as_slice() {
        b"HELP" => {
            if args.len() != 1 {
                return wrong_arity("latency");
            }
            CommandOutcome::reply(RespFrame::Array(vec![
                RespFrame::bulk_str("DOCTOR -- Return a human readable latency report."),
                RespFrame::bulk_str("GRAPH <event> -- Return an ASCII latency graph."),
                RespFrame::bulk_str("HISTORY <event> -- Return timestamp-latency samples."),
                RespFrame::bulk_str("HISTOGRAM [event ...] -- Return latency histogram buckets."),
                RespFrame::bulk_str("LATEST -- Return the latest latency samples."),
                RespFrame::bulk_str("RESET [event ...] -- Reset latency events."),
                RespFrame::bulk_str("HELP -- Show this help."),
            ]))
        }
        b"LATEST" => {
            if args.len() != 1 {
                return wrong_arity("latency");
            }
            let rows = server
                .stats
                .latency_latest()
                .into_iter()
                .map(|(event, ts, latest_ms, max_ms)| {
                    RespFrame::Array(vec![
                        RespFrame::BulkString(Some(event)),
                        RespFrame::Integer(ts),
                        RespFrame::Integer(latest_ms),
                        RespFrame::Integer(max_ms),
                    ])
                })
                .collect::<Vec<_>>();
            CommandOutcome::reply(RespFrame::Array(rows))
        }
        b"HISTORY" => {
            let [_, event] = args else {
                return wrong_arity("latency");
            };
            let rows = server
                .stats
                .latency_history(event)
                .into_iter()
                .map(|(ts, ms)| {
                    RespFrame::Array(vec![RespFrame::Integer(ts), RespFrame::Integer(ms)])
                })
                .collect::<Vec<_>>();
            CommandOutcome::reply(RespFrame::Array(rows))
        }
        b"RESET" => {
            let removed = server.stats.latency_reset(&args[1..]);
            CommandOutcome::reply(RespFrame::Integer(removed))
        }
        b"DOCTOR" => {
            if args.len() != 1 {
                return wrong_arity("latency");
            }
            let events = server.stats.latency_event_names();
            if events.is_empty() {
                return CommandOutcome::reply(RespFrame::bulk_str(
                    "I have no latency reports to show. Be happy!",
                ));
            }

            let mut report = String::new();
            for event in &events {
                let history = server.stats.latency_history(event);
                if history.is_empty() {
                    continue;
                }
                let name = String::from_utf8_lossy(event);
                let max_ms = history.iter().map(|(_, ms)| *ms).max().unwrap_or(0);
                let min_ms = history.iter().map(|(_, ms)| *ms).min().unwrap_or(0);
                let sum: i64 = history.iter().map(|(_, ms)| *ms).sum();
                let avg_ms = sum / history.len() as i64;

                let mut sorted: Vec<i64> = history.iter().map(|(_, ms)| *ms).collect();
                sorted.sort_unstable();
                let median_ms = sorted[sorted.len() / 2];

                let _ = writeln!(
                    report,
                    "{name} - {samples} samples, median {median_ms} ms, avg {avg_ms} ms, \
                     min {min_ms} ms, max {max_ms} ms.",
                    samples = history.len()
                );

                if max_ms > 100 {
                    let _ = writeln!(
                        report,
                        "  WARNING: High latency detected for '{name}'. \
                         Consider checking slow commands or system load."
                    );
                }
            }
            CommandOutcome::reply(RespFrame::bulk_str(&report))
        }
        b"GRAPH" => {
            let [_, event] = args else {
                return wrong_arity("latency");
            };
            let history = server.stats.latency_history(event);
            if history.is_empty() {
                return CommandOutcome::reply(RespFrame::bulk_str(""));
            }

            let name = String::from_utf8_lossy(event);
            let max_ms = history.iter().map(|(_, ms)| *ms).max().unwrap_or(1);
            let min_ms = history.iter().map(|(_, ms)| *ms).min().unwrap_or(0);
            let all_time_max = max_ms;

            let graph_height: usize = 16;
            let samples: Vec<i64> = history.iter().map(|(_, ms)| *ms).collect();
            let num_cols = samples.len().min(80);
            let display_samples = &samples[samples.len().saturating_sub(num_cols)..];

            let range = (max_ms - min_ms).max(1);
            let mut graph = String::new();
            let _ = writeln!(
                graph,
                "{name} - high {max_ms} ms, low {min_ms} ms (all time high {all_time_max} ms)"
            );

            for row in (0..graph_height).rev() {
                let threshold = min_ms + (range * row as i64) / graph_height as i64;
                let mut line = String::with_capacity(num_cols + 1);
                for &val in display_samples {
                    let normalized =
                        ((val - min_ms) as usize * graph_height) / range.max(1) as usize;
                    if normalized > row {
                        line.push('#');
                    } else if normalized == row && row == 0 {
                        line.push('_');
                    } else {
                        line.push(' ');
                    }
                }
                let _ = writeln!(graph, "{line} | {threshold} ms");
            }

            let now_ts = history.last().map(|(ts, _)| *ts).unwrap_or(0);
            let display_history = &history[history.len().saturating_sub(num_cols)..];
            let mut labels = String::new();
            for (ts, _) in display_history {
                let ago = now_ts - ts;
                if ago > 60 {
                    let _ = write!(labels, "{}", ago / 60);
                } else {
                    labels.push('.');
                }
            }
            let _ = writeln!(graph, "{labels}");

            CommandOutcome::reply(RespFrame::bulk_str(&graph))
        }
        b"HISTOGRAM" => {
            let events = if args.len() == 1 {
                server.stats.latency_event_names()
            } else {
                args[1..].to_vec()
            };

            let mut out = Vec::new();
            for event in events {
                let history = server.stats.latency_history(&event);
                if history.is_empty() {
                    continue;
                }

                let mut b0 = 0i64;
                let mut b1 = 0i64;
                let mut b2 = 0i64;
                let mut b3 = 0i64;
                for (_, ms) in history {
                    if ms <= 1 {
                        b0 += 1;
                    } else if ms <= 5 {
                        b1 += 1;
                    } else if ms <= 20 {
                        b2 += 1;
                    } else {
                        b3 += 1;
                    }
                }

                out.push(RespFrame::Array(vec![
                    RespFrame::BulkString(Some(event)),
                    RespFrame::Array(vec![
                        RespFrame::Array(vec![RespFrame::bulk_str("le=1"), RespFrame::Integer(b0)]),
                        RespFrame::Array(vec![RespFrame::bulk_str("le=5"), RespFrame::Integer(b1)]),
                        RespFrame::Array(vec![
                            RespFrame::bulk_str("le=20"),
                            RespFrame::Integer(b2),
                        ]),
                        RespFrame::Array(vec![
                            RespFrame::bulk_str("gt=20"),
                            RespFrame::Integer(b3),
                        ]),
                    ]),
                ]));
            }

            CommandOutcome::reply(RespFrame::Array(out))
        }
        _ => CommandOutcome::reply(err(
            "ERR unknown LATENCY subcommand or wrong number of arguments",
        )),
    }
}
