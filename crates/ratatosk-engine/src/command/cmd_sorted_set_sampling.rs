use bytes::Bytes;

use glob_match::glob_match;
use ratatosk_resp::frame::RespFrame;

use crate::keyspace::{ServerState, purge_expired_key};
use crate::object::{format_f64_for_redis, now_us};

use super::cmd_sorted_set::{entries_to_resp, sorted_entries};
use super::{
    ClientState, CommandOutcome, err, now_ms, parse_i64, wrong_arity, wrong_type_response,
};
use super::{parse_scan_cursor, parse_scan_match_count_options, scan_collect_indexes, scan_reply};

const MAX_ZSET_RANDOM_COUNT: usize = 100_000;

pub(super) fn cmd_zrandmember(
    args: &[Bytes],
    server: &mut ServerState,
    client: &ClientState,
) -> CommandOutcome {
    if args.is_empty() || args.len() > 3 {
        return wrong_arity("zrandmember");
    }

    let key = &args[0];
    let count = if args.len() >= 2 {
        let Some(parsed) = parse_i64(&args[1]) else {
            return CommandOutcome::reply(err("ERR value is not an integer or out of range"));
        };
        Some(parsed)
    } else {
        None
    };

    let with_scores = args.len() == 3 && args[2].eq_ignore_ascii_case(b"WITHSCORES");

    let now = now_ms();
    let mut db = server.db_mut(client.selected_db);
    purge_expired_key(&mut db, key, now);

    let Some(entry) = db.get(key) else {
        return if count.is_some() {
            CommandOutcome::reply(RespFrame::Array(vec![]))
        } else {
            CommandOutcome::reply(RespFrame::BulkString(None))
        };
    };
    let Some(zset) = entry.as_sorted_set() else {
        return wrong_type_response();
    };

    let members: Vec<(Bytes, f64)> = sorted_entries(zset);
    if members.is_empty() {
        return if count.is_some() {
            CommandOutcome::reply(RespFrame::Array(vec![]))
        } else {
            CommandOutcome::reply(RespFrame::BulkString(None))
        };
    }

    let start = usize::try_from(now_us()).unwrap_or(0) % members.len();

    match count {
        None => {
            let (member, _) = &members[start];
            CommandOutcome::reply(RespFrame::BulkString(Some(member.clone())))
        }
        Some(raw_count) if raw_count >= 0 => {
            let Ok(requested) = usize::try_from(raw_count) else {
                return CommandOutcome::reply(err("ERR value is not an integer or out of range"));
            };
            if requested > MAX_ZSET_RANDOM_COUNT {
                return CommandOutcome::reply(err("ERR count is out of range"));
            }
            if requested == 0 {
                return CommandOutcome::reply(RespFrame::Array(vec![]));
            }

            let take = requested.min(members.len());
            let mut rotated = members;
            rotated.rotate_left(start);

            let selected = &rotated[..take];
            CommandOutcome::reply(RespFrame::Array(entries_to_resp(selected, with_scores)))
        }
        Some(raw_count) => {
            let Ok(requested) = usize::try_from(raw_count.saturating_neg()) else {
                return CommandOutcome::reply(err("ERR value is not an integer or out of range"));
            };
            if requested > MAX_ZSET_RANDOM_COUNT {
                return CommandOutcome::reply(err("ERR count is out of range"));
            }
            let mut out = Vec::with_capacity(if with_scores {
                requested.saturating_mul(2)
            } else {
                requested
            });
            for offset in 0..requested {
                let idx = (start + offset) % members.len();
                let (member, score) = &members[idx];
                out.push(RespFrame::BulkString(Some(member.clone())));
                if with_scores {
                    out.push(RespFrame::BulkString(Some(format_f64_for_redis(*score))));
                }
            }
            CommandOutcome::reply(RespFrame::Array(out))
        }
    }
}

pub(super) fn cmd_zscan(
    args: &[Bytes],
    server: &mut ServerState,
    client: &ClientState,
) -> CommandOutcome {
    let [key, cursor_raw, options @ ..] = args else {
        return wrong_arity("zscan");
    };

    let cursor = match parse_scan_cursor(cursor_raw) {
        Ok(cursor) => cursor,
        Err(response) => return CommandOutcome::reply(response),
    };

    let (pattern, count) = match parse_scan_match_count_options(options) {
        Ok(parsed) => parsed,
        Err(response) => return CommandOutcome::reply(response),
    };

    let now = now_ms();
    let mut db = server.db_mut(client.selected_db);
    purge_expired_key(&mut db, key, now);

    let Some(entry) = db.get(key) else {
        return scan_reply(0, vec![]);
    };
    let Some(zset) = entry.as_sorted_set() else {
        return wrong_type_response();
    };

    let mut entries: Vec<(Bytes, f64)> = zset
        .by_member
        .iter()
        .map(|(member, score)| (member.clone(), score.value()))
        .collect();
    entries.sort_by(|(a_member, _), (b_member, _)| a_member.cmp(b_member));

    let (next_cursor, matched_indexes) =
        scan_collect_indexes(&entries, cursor, count, |(member, _)| {
            if let Some(pattern) = &pattern {
                glob_match(pattern, &String::from_utf8_lossy(member))
            } else {
                true
            }
        });

    let mut out = Vec::with_capacity(matched_indexes.len().saturating_mul(2));
    for idx in matched_indexes {
        let (member, score) = &entries[idx];
        out.push(RespFrame::BulkString(Some(member.clone())));
        out.push(RespFrame::BulkString(Some(format_f64_for_redis(*score))));
    }

    scan_reply(next_cursor, out)
}
