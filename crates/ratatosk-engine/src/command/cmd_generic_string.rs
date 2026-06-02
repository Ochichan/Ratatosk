use bytes::Bytes;
use ratatosk_resp::frame::RespFrame;

use crate::keyspace::{ServerState, StoredValue, purge_expired_key};

use super::{
    ClientState, CommandOutcome, err, now_ms, parse_i64, to_uppercase_bytes, wrong_arity,
    wrong_type_response,
};

const LCS_MAX_DP_CELLS: usize = 16_000_000;

pub(super) fn cmd_lcs(
    args: &[Bytes],
    server: &mut ServerState,
    client: &ClientState,
) -> CommandOutcome {
    if args.len() < 2 {
        return wrong_arity("lcs");
    }

    let key1 = &args[0];
    let key2 = &args[1];

    let mut return_len = false;
    let mut idx = 2usize;
    while idx < args.len() {
        let option = to_uppercase_bytes(&args[idx]);
        match option.as_slice() {
            b"LEN" => {
                return_len = true;
                idx += 1;
            }
            _ => return CommandOutcome::reply(err("ERR syntax error")),
        }
    }

    let now = now_ms();
    let mut db = server.db_mut(client.selected_db);
    purge_expired_key(&mut db, key1, now);
    purge_expired_key(&mut db, key2, now);

    let left = match db.get(key1) {
        None => Bytes::new(),
        Some(entry) => match entry.as_string_bytes() {
            Some(v) => v,
            None => return wrong_type_response(),
        },
    };
    let right = match db.get(key2) {
        None => Bytes::new(),
        Some(entry) => match entry.as_string_bytes() {
            Some(v) => v,
            None => return wrong_type_response(),
        },
    };

    if left.len().saturating_mul(right.len()) > LCS_MAX_DP_CELLS {
        return CommandOutcome::reply(err("ERR LCS input is too large"));
    }

    let lcs = longest_common_subsequence(left.as_ref(), right.as_ref());
    if return_len {
        CommandOutcome::reply(RespFrame::Integer(lcs.len() as i64))
    } else {
        CommandOutcome::reply(RespFrame::BulkString(Some(Bytes::from(lcs))))
    }
}

pub(super) fn cmd_msetex(
    args: &[Bytes],
    server: &mut ServerState,
    client: &ClientState,
) -> CommandOutcome {
    if args.len() < 3 {
        return wrong_arity("msetex");
    }

    let Some(numkeys_i64) = parse_i64(&args[0]) else {
        return CommandOutcome::reply(err("ERR value is not an integer or out of range"));
    };
    if numkeys_i64 <= 0 {
        return CommandOutcome::reply(err("ERR numkeys should be greater than 0"));
    }
    let Ok(numkeys) = usize::try_from(numkeys_i64) else {
        return CommandOutcome::reply(err("ERR value is not an integer or out of range"));
    };

    let pair_tokens = numkeys.saturating_mul(2);
    if args.len() < 1 + pair_tokens {
        return wrong_arity("msetex");
    }

    let mut kvs = Vec::with_capacity(numkeys);
    let mut idx = 1usize;
    for _ in 0..numkeys {
        if idx + 1 >= args.len() {
            return wrong_arity("msetex");
        }
        kvs.push((args[idx].clone(), args[idx + 1].clone()));
        idx += 2;
    }

    let mut nx = false;
    let mut xx = false;
    let mut expire_at_ms: Option<i64> = None;
    let now = now_ms();

    while idx < args.len() {
        let option = to_uppercase_bytes(&args[idx]);
        match option.as_slice() {
            b"NX" => {
                nx = true;
                idx += 1;
            }
            b"XX" => {
                xx = true;
                idx += 1;
            }
            b"EX" | b"PX" => {
                if idx + 1 >= args.len() || expire_at_ms.is_some() {
                    return CommandOutcome::reply(err("ERR syntax error"));
                }
                let Some(raw_ttl) = parse_i64(&args[idx + 1]) else {
                    return CommandOutcome::reply(err(
                        "ERR value is not an integer or out of range",
                    ));
                };
                if raw_ttl <= 0 {
                    return CommandOutcome::reply(err(
                        "ERR invalid expire time in 'msetex' command",
                    ));
                }
                expire_at_ms = Some(if option.as_slice() == b"EX" {
                    now.saturating_add(raw_ttl.saturating_mul(1000))
                } else {
                    now.saturating_add(raw_ttl)
                });
                idx += 2;
            }
            _ => return CommandOutcome::reply(err("ERR syntax error")),
        }
    }

    if nx && xx {
        return CommandOutcome::reply(err("ERR syntax error"));
    }

    let mut db = server.db_mut(client.selected_db);
    for (key, _) in &kvs {
        purge_expired_key(&mut db, key, now);
    }

    if nx && kvs.iter().any(|(key, _)| db.contains_key(key)) {
        return CommandOutcome::reply(RespFrame::Integer(0));
    }
    if xx && kvs.iter().any(|(key, _)| !db.contains_key(key)) {
        return CommandOutcome::reply(RespFrame::Integer(0));
    }

    for (key, value) in kvs {
        db.insert(key, StoredValue::string(value, expire_at_ms));
    }

    CommandOutcome::reply(RespFrame::Integer(1))
}

pub(super) fn longest_common_subsequence(a: &[u8], b: &[u8]) -> Vec<u8> {
    if a.is_empty() || b.is_empty() {
        return Vec::new();
    }

    let m = a.len();
    let n = b.len();
    let mut dp = vec![vec![0u16; n + 1]; m + 1];

    for i in 0..m {
        for (j, bj) in b.iter().enumerate().take(n) {
            if a[i] == *bj {
                dp[i + 1][j + 1] = dp[i][j].saturating_add(1);
            } else {
                dp[i + 1][j + 1] = dp[i][j + 1].max(dp[i + 1][j]);
            }
        }
    }

    let mut i = m;
    let mut j = n;
    let mut out = Vec::with_capacity(dp[m][n] as usize);
    while i > 0 && j > 0 {
        if a[i - 1] == b[j - 1] {
            out.push(a[i - 1]);
            i -= 1;
            j -= 1;
        } else if dp[i - 1][j] >= dp[i][j - 1] {
            i -= 1;
        } else {
            j -= 1;
        }
    }

    out.reverse();
    out
}
