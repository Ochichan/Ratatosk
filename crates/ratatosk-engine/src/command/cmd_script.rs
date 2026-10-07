use bytes::Bytes;

use ratatosk_resp::frame::RespFrame;

use crate::{keyspace::ServerState, security::next_audit_stamp};

#[cfg(feature = "lua-scripting")]
use super::ClientState;
use super::{CommandOutcome, err, to_uppercase_bytes, wrong_arity};

// ---------------------------------------------------------------------------
// EVAL / EVALSHA / EVAL_RO / EVALSHA_RO
// ---------------------------------------------------------------------------

/// Parse the common `(numkeys, KEYS..., ARGV...)` suffix shared by EVAL and
/// EVALSHA. Returns `(keys, argv)` slices on success.
#[cfg(feature = "lua-scripting")]
fn parse_keys_argv<'a>(
    args: &'a [Bytes],
    cmd_name: &str,
) -> Result<(&'a [Bytes], &'a [Bytes]), RespFrame> {
    // args[0] = script/sha, args[1] = numkeys, rest = keys... args...
    if args.len() < 2 {
        return Err(err(&format!(
            "ERR wrong number of arguments for '{cmd_name}' command"
        )));
    }

    let numkeys_str = std::str::from_utf8(&args[1]).unwrap_or("");
    let numkeys: usize = match numkeys_str.parse() {
        Ok(n) => n,
        Err(_) => {
            return Err(err("ERR value is not an integer or out of range"));
        }
    };

    // Validate argument count without adding an untrusted `numkeys` to the
    // fixed prefix length. A parsed usize::MAX must be an ordinary error, not
    // an overflow panic.
    if numkeys > args.len().saturating_sub(2) {
        return Err(err(
            "ERR Number of keys can't be greater than number of args",
        ));
    }

    let keys = &args[2..2 + numkeys];
    let argv = &args[2 + numkeys..];

    Ok((keys, argv))
}

// ---- Feature: lua-scripting enabled ----

#[cfg(feature = "lua-scripting")]
pub(super) fn cmd_eval(
    args: &[Bytes],
    server: &mut ServerState,
    client: &mut ClientState,
) -> CommandOutcome {
    cmd_eval_with_mode(
        args,
        server,
        client,
        "eval",
        super::lua_runtime::ScriptMode::ReadWrite,
    )
}

#[cfg(feature = "lua-scripting")]
fn cmd_eval_with_mode(
    args: &[Bytes],
    server: &mut ServerState,
    client: &mut ClientState,
    command_name: &str,
    mode: super::lua_runtime::ScriptMode,
) -> CommandOutcome {
    let (keys, argv) = match parse_keys_argv(args, command_name) {
        Ok(pair) => pair,
        Err(response) => return CommandOutcome::reply(response),
    };

    let script = &args[0];

    // Cache the script
    let sha = sha1_hex(script);
    let sha_bytes = Bytes::copy_from_slice(sha.as_bytes());
    server
        .script_cache
        .scripts
        .insert(sha_bytes, script.clone());

    let result = super::lua_runtime::eval_script(super::lua_runtime::EvalRequest {
        source: script,
        keys,
        argv,
        server,
        client,
        mode,
    });
    client.finish_script_durability_effects();
    CommandOutcome::reply(result)
}

#[cfg(feature = "lua-scripting")]
pub(super) fn cmd_evalsha(
    args: &[Bytes],
    server: &mut ServerState,
    client: &mut ClientState,
) -> CommandOutcome {
    cmd_evalsha_with_mode(
        args,
        server,
        client,
        "evalsha",
        super::lua_runtime::ScriptMode::ReadWrite,
    )
}

#[cfg(feature = "lua-scripting")]
fn cmd_evalsha_with_mode(
    args: &[Bytes],
    server: &mut ServerState,
    client: &mut ClientState,
    command_name: &str,
    mode: super::lua_runtime::ScriptMode,
) -> CommandOutcome {
    let (keys, argv) = match parse_keys_argv(args, command_name) {
        Ok(pair) => pair,
        Err(response) => return CommandOutcome::reply(response),
    };

    let sha = &args[0];

    // Look up the script in the cache
    let script = match server.script_cache.scripts.get(sha) {
        Some(s) => s.clone(),
        None => {
            // Also try lowercase form of the SHA (Redis is case-insensitive for SHA)
            let sha_lower =
                Bytes::from(std::str::from_utf8(sha).unwrap_or("").to_ascii_lowercase());
            match server.script_cache.scripts.get(&sha_lower) {
                Some(s) => s.clone(),
                None => {
                    return CommandOutcome::reply(err("NOSCRIPT No matching script. Use EVAL."));
                }
            }
        }
    };

    let result = super::lua_runtime::eval_script(super::lua_runtime::EvalRequest {
        source: &script,
        keys,
        argv,
        server,
        client,
        mode,
    });
    client.finish_script_durability_effects();
    CommandOutcome::reply(result)
}

#[cfg(feature = "lua-scripting")]
pub(super) fn cmd_eval_ro(
    args: &[Bytes],
    server: &mut ServerState,
    client: &mut ClientState,
) -> CommandOutcome {
    cmd_eval_with_mode(
        args,
        server,
        client,
        "eval_ro",
        super::lua_runtime::ScriptMode::ReadOnly,
    )
}

#[cfg(feature = "lua-scripting")]
pub(super) fn cmd_evalsha_ro(
    args: &[Bytes],
    server: &mut ServerState,
    client: &mut ClientState,
) -> CommandOutcome {
    cmd_evalsha_with_mode(
        args,
        server,
        client,
        "evalsha_ro",
        super::lua_runtime::ScriptMode::ReadOnly,
    )
}

// ---- Feature: lua-scripting disabled (stubs) ----

#[cfg(not(feature = "lua-scripting"))]
pub(super) fn cmd_eval(args: &[Bytes]) -> CommandOutcome {
    if args.len() < 2 {
        return wrong_arity("eval");
    }
    CommandOutcome::reply(err("ERR Scripting not supported in this build"))
}

#[cfg(not(feature = "lua-scripting"))]
pub(super) fn cmd_evalsha(args: &[Bytes]) -> CommandOutcome {
    if args.len() < 2 {
        return wrong_arity("evalsha");
    }
    CommandOutcome::reply(err("NOSCRIPT No matching script. Please use EVAL."))
}

#[cfg(not(feature = "lua-scripting"))]
pub(super) fn cmd_eval_ro(args: &[Bytes]) -> CommandOutcome {
    cmd_eval(args)
}

#[cfg(not(feature = "lua-scripting"))]
pub(super) fn cmd_evalsha_ro(args: &[Bytes]) -> CommandOutcome {
    cmd_evalsha(args)
}

// ---------------------------------------------------------------------------
// SCRIPT <subcommand>
// ---------------------------------------------------------------------------

pub(super) fn cmd_script(args: &[Bytes], server: &mut ServerState) -> CommandOutcome {
    if args.is_empty() {
        return wrong_arity("script");
    }

    let sub = to_uppercase_bytes(&args[0]);
    match sub.as_slice() {
        b"LOAD" => script_load(&args[1..], server),
        b"EXISTS" => script_exists(&args[1..], server),
        b"FLUSH" => script_flush(&args[1..], server),
        b"KILL" => script_kill(&args[1..]),
        b"DEBUG" => script_debug(&args[1..]),
        b"HELP" => script_help(),
        _ => CommandOutcome::reply(err(
            "ERR unknown SCRIPT subcommand or wrong number of arguments",
        )),
    }
}

fn script_load(args: &[Bytes], server: &mut ServerState) -> CommandOutcome {
    if args.len() != 1 {
        return wrong_arity("script|load");
    }

    let sha = sha1_hex(&args[0]);
    let sha_bytes = Bytes::copy_from_slice(sha.as_bytes());
    server
        .script_cache
        .scripts
        .insert(sha_bytes.clone(), args[0].clone());
    CommandOutcome::reply(RespFrame::BulkString(Some(sha_bytes)))
}

fn script_exists(args: &[Bytes], server: &ServerState) -> CommandOutcome {
    if args.is_empty() {
        return wrong_arity("script|exists");
    }

    let results: Vec<RespFrame> = args
        .iter()
        .map(|sha| {
            if server.script_cache.scripts.contains_key(sha) {
                RespFrame::Integer(1)
            } else {
                RespFrame::Integer(0)
            }
        })
        .collect();

    CommandOutcome::reply(RespFrame::Array(results))
}

fn script_flush(args: &[Bytes], server: &mut ServerState) -> CommandOutcome {
    // Accept optional ASYNC or SYNC argument (ignored -- always synchronous)
    if args.len() > 1 {
        return wrong_arity("script|flush");
    }

    let mode = if args.is_empty() {
        "SYNC".to_string()
    } else {
        let mode = to_uppercase_bytes(&args[0]);
        if !matches!(mode.as_slice(), b"ASYNC" | b"SYNC") {
            return CommandOutcome::reply(err("ERR SCRIPT FLUSH only supports ASYNC|SYNC option"));
        }
        String::from_utf8_lossy(&mode).to_string()
    };

    server.script_cache.scripts.clear();

    let payload = format!("event=SCRIPT_FLUSH mode={mode}");
    let stamp = next_audit_stamp("SCRIPT_FLUSH", &payload);
    tracing::info!(
        target = "ratatosk::audit",
        event = "SCRIPT_FLUSH",
        audit_seq = stamp.seq,
        audit_prev_hash = %stamp.prev_hash,
        audit_hash = %stamp.hash,
        mode = %mode,
        "script cache flushed"
    );

    CommandOutcome::reply(RespFrame::ok())
}

fn script_kill(args: &[Bytes]) -> CommandOutcome {
    if !args.is_empty() {
        return wrong_arity("script|kill");
    }

    CommandOutcome::reply(err("ERR SCRIPT KILL is not supported in this build"))
}

fn script_debug(args: &[Bytes]) -> CommandOutcome {
    if args.len() != 1 {
        return wrong_arity("script|debug");
    }

    CommandOutcome::reply(err("ERR SCRIPT DEBUG is not supported in this build"))
}

fn script_help() -> CommandOutcome {
    let lines: Vec<RespFrame> = vec![
        RespFrame::BulkString(Some(Bytes::from_static(
            b"SCRIPT <subcommand> [<arg> [value] [opt] ...]. Subcommands are:",
        ))),
        RespFrame::BulkString(Some(Bytes::from_static(
            b"LOAD, EXISTS, FLUSH, and HELP are available in this build. SCRIPT DEBUG and SCRIPT KILL are unsupported.",
        ))),
        RespFrame::BulkString(Some(Bytes::from_static(b"EXISTS <sha1> [<sha1> ...]"))),
        RespFrame::BulkString(Some(Bytes::from_static(
            b"    Return information about the existence of the scripts in the script cache.",
        ))),
        RespFrame::BulkString(Some(Bytes::from_static(b"FLUSH [ASYNC|SYNC]"))),
        RespFrame::BulkString(Some(Bytes::from_static(
            b"    Flush the Lua scripts cache. Very dangerous on replicas.",
        ))),
        RespFrame::BulkString(Some(Bytes::from_static(b"LOAD <script>"))),
        RespFrame::BulkString(Some(Bytes::from_static(
            b"    Load a script into the scripts cache without executing it.",
        ))),
        RespFrame::BulkString(Some(Bytes::from_static(b"HELP"))),
        RespFrame::BulkString(Some(Bytes::from_static(b"    Print this help."))),
    ];

    CommandOutcome::reply(RespFrame::Array(lines))
}

// ---------------------------------------------------------------------------
// FCALL / FCALL_RO
// ---------------------------------------------------------------------------

pub(super) fn cmd_fcall(args: &[Bytes]) -> CommandOutcome {
    if args.len() < 2 {
        return wrong_arity("fcall");
    }
    CommandOutcome::reply(err("ERR Function not found"))
}

pub(super) fn cmd_fcall_ro(args: &[Bytes]) -> CommandOutcome {
    cmd_fcall(args)
}

pub(super) fn cmd_function(args: &[Bytes]) -> CommandOutcome {
    if args.is_empty() {
        return wrong_arity("function");
    }

    let sub = to_uppercase_bytes(&args[0]);
    match sub.as_slice() {
        b"HELP" => function_help(),
        b"LIST" => CommandOutcome::reply(RespFrame::Array(vec![])),
        b"DUMP" => CommandOutcome::reply(RespFrame::BulkString(None)),
        b"STATS" => function_stats(),
        b"LOAD" => CommandOutcome::reply(err("ERR Function not supported in this build")),
        b"DELETE" => CommandOutcome::reply(err("ERR Function not supported in this build")),
        b"FLUSH" => {
            if args.len() > 2 {
                return wrong_arity("function");
            }

            let mode = if args.len() == 1 {
                "SYNC".to_string()
            } else {
                let mode = to_uppercase_bytes(&args[1]);
                if !matches!(mode.as_slice(), b"ASYNC" | b"SYNC") {
                    return CommandOutcome::reply(err(
                        "ERR FUNCTION FLUSH only supports ASYNC|SYNC option",
                    ));
                }
                String::from_utf8_lossy(&mode).to_string()
            };

            let payload = format!("event=FUNCTION_FLUSH mode={mode}");
            let stamp = next_audit_stamp("FUNCTION_FLUSH", &payload);
            tracing::info!(
                target = "ratatosk::audit",
                event = "FUNCTION_FLUSH",
                audit_seq = stamp.seq,
                audit_prev_hash = %stamp.prev_hash,
                audit_hash = %stamp.hash,
                mode = %mode,
                "function registry flushed"
            );

            CommandOutcome::reply(RespFrame::ok())
        }
        b"RESTORE" => CommandOutcome::reply(err("ERR Function not supported in this build")),
        _ => CommandOutcome::reply(err(
            "ERR unknown FUNCTION subcommand or wrong number of arguments",
        )),
    }
}
fn function_help() -> CommandOutcome {
    let lines: Vec<RespFrame> = vec![
        RespFrame::BulkString(Some(Bytes::from_static(
            b"FUNCTION <subcommand> [<arg> [value] [opt] ...]. Subcommands are:",
        ))),
        RespFrame::BulkString(Some(Bytes::from_static(
            b"LIST, DUMP, STATS, FLUSH, and HELP are available in this build. LOAD, DELETE, and RESTORE are unsupported.",
        ))),
        RespFrame::BulkString(Some(Bytes::from_static(b"DELETE <library-name>"))),
        RespFrame::BulkString(Some(Bytes::from_static(b"    Delete a function library."))),
        RespFrame::BulkString(Some(Bytes::from_static(b"DUMP"))),
        RespFrame::BulkString(Some(Bytes::from_static(
            b"    Dump all function libraries in serialized format.",
        ))),
        RespFrame::BulkString(Some(Bytes::from_static(b"FLUSH [ASYNC|SYNC]"))),
        RespFrame::BulkString(Some(Bytes::from_static(
            b"    Delete all function libraries.",
        ))),
        RespFrame::BulkString(Some(Bytes::from_static(
            b"LIST [LIBRARYNAME <pattern>] [WITHCODE]",
        ))),
        RespFrame::BulkString(Some(Bytes::from_static(
            b"    Return information about all libraries.",
        ))),
        RespFrame::BulkString(Some(Bytes::from_static(b"LOAD [REPLACE] <function-code>"))),
        RespFrame::BulkString(Some(Bytes::from_static(
            b"    Create a new library with the given code.",
        ))),
        RespFrame::BulkString(Some(Bytes::from_static(
            b"RESTORE <serialized-value> [FLUSH|APPEND|REPLACE]",
        ))),
        RespFrame::BulkString(Some(Bytes::from_static(
            b"    Restore all libraries from a serialized dump.",
        ))),
        RespFrame::BulkString(Some(Bytes::from_static(b"STATS"))),
        RespFrame::BulkString(Some(Bytes::from_static(
            b"    Return information about the engines and function libraries.",
        ))),
        RespFrame::BulkString(Some(Bytes::from_static(b"HELP"))),
        RespFrame::BulkString(Some(Bytes::from_static(b"    Print this help."))),
    ];

    CommandOutcome::reply(RespFrame::Array(lines))
}

fn function_stats() -> CommandOutcome {
    // Return a flat array representing the map:
    //   running_script => 0 (not running)
    //   engines => (empty array representing no engines)
    let response = RespFrame::Array(vec![
        RespFrame::BulkString(Some(Bytes::from_static(b"running_script"))),
        RespFrame::Integer(0),
        RespFrame::BulkString(Some(Bytes::from_static(b"engines"))),
        RespFrame::Array(vec![]),
    ]);

    CommandOutcome::reply(response)
}

// ---------------------------------------------------------------------------
// Minimal SHA1 implementation (FIPS 180-1) -- no unsafe, no external crate.
// Only used for SCRIPT LOAD to compute the 40-char hex digest.
// ---------------------------------------------------------------------------

fn sha1_hex(data: &[u8]) -> String {
    let mut h0: u32 = 0x67452301;
    let mut h1: u32 = 0xEFCDAB89;
    let mut h2: u32 = 0x98BADCFE;
    let mut h3: u32 = 0x10325476;
    let mut h4: u32 = 0xC3D2E1F0;

    // Pre-processing: pad message
    let bit_len = (data.len() as u64).wrapping_mul(8);
    let mut padded = Vec::with_capacity(data.len() + 72);
    padded.extend_from_slice(data);
    padded.push(0x80);
    while padded.len() % 64 != 56 {
        padded.push(0);
    }
    padded.extend_from_slice(&bit_len.to_be_bytes());

    // Process each 512-bit (64-byte) block
    for chunk in padded.chunks(64) {
        let mut w = [0u32; 80];
        for i in 0..16 {
            w[i] = u32::from_be_bytes([
                chunk[i * 4],
                chunk[i * 4 + 1],
                chunk[i * 4 + 2],
                chunk[i * 4 + 3],
            ]);
        }
        for i in 16..80 {
            w[i] = (w[i - 3] ^ w[i - 8] ^ w[i - 14] ^ w[i - 16]).rotate_left(1);
        }

        let (mut a, mut b, mut c, mut d, mut e) = (h0, h1, h2, h3, h4);

        #[allow(clippy::needless_range_loop)]
        for i in 0..80 {
            let (f, k) = match i {
                0..=19 => ((b & c) | ((!b) & d), 0x5A827999u32),
                20..=39 => (b ^ c ^ d, 0x6ED9EBA1u32),
                40..=59 => ((b & c) | (b & d) | (c & d), 0x8F1BBCDCu32),
                _ => (b ^ c ^ d, 0xCA62C1D6u32),
            };
            let temp = a
                .rotate_left(5)
                .wrapping_add(f)
                .wrapping_add(e)
                .wrapping_add(k)
                .wrapping_add(w[i]);
            e = d;
            d = c;
            c = b.rotate_left(30);
            b = a;
            a = temp;
        }

        h0 = h0.wrapping_add(a);
        h1 = h1.wrapping_add(b);
        h2 = h2.wrapping_add(c);
        h3 = h3.wrapping_add(d);
        h4 = h4.wrapping_add(e);
    }

    format!("{:08x}{:08x}{:08x}{:08x}{:08x}", h0, h1, h2, h3, h4)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[cfg(feature = "lua-scripting")]
    #[test]
    fn lua_inline_replies_cannot_inject_frames_or_break_following_replies() {
        use bytes::BytesMut;
        use ratatosk_resp::{encode_to_vec, parse};

        let mut server = ServerState::with_default_dbs();
        let mut client = ClientState::default();
        for body in [
            "return redis.call('SET','forbidden','v')",
            "error('first\\r\\n+FORGED\\r\\n')",
            "return redis.error_reply('first\\r\\n+FORGED\\r\\n')",
            "return redis.status_reply('OK\\r\\n+FORGED\\r\\n')",
            "return {{err='first\\r\\n+FORGED'}, {ok='OK\\n'}}",
        ] {
            let reply = run_command(&["EVAL_RO", body, "0"], &mut server, &mut client);
            let mut encoded = Vec::new();
            encode_to_vec(&reply, &mut encoded);
            encode_to_vec(&RespFrame::pong(), &mut encoded);
            let mut wire = BytesMut::from(encoded.as_slice());
            assert_eq!(parse(&mut wire).expect("valid Lua reply"), Some(reply));
            assert_eq!(
                parse(&mut wire).expect("following PONG"),
                Some(RespFrame::pong())
            );
            assert!(wire.is_empty(), "Lua response injected extra bytes");
        }
        assert_eq!(
            run_command(
                &["EVAL_RO", "return 'a\\r\\nb'", "0"],
                &mut server,
                &mut client
            ),
            RespFrame::bulk_str("a\r\nb")
        );
        assert_eq!(
            run_command(&["GET", "forbidden"], &mut server, &mut client),
            RespFrame::BulkString(None)
        );
    }

    #[cfg(feature = "lua-scripting")]
    fn arguments(parts: &[&str]) -> Vec<Bytes> {
        parts
            .iter()
            .map(|part| Bytes::copy_from_slice(part.as_bytes()))
            .collect()
    }

    #[cfg(feature = "lua-scripting")]
    fn assert_error_contains(response: &RespFrame, expected: &str) {
        let RespFrame::Error(message) = response else {
            panic!("expected error containing {expected:?}, got {response:?}");
        };
        assert!(
            String::from_utf8_lossy(message).contains(expected),
            "unexpected error: {}",
            String::from_utf8_lossy(message)
        );
    }

    #[cfg(feature = "lua-scripting")]
    fn run_command(
        parts: &[&str],
        server: &mut ServerState,
        client: &mut ClientState,
    ) -> RespFrame {
        let frame = RespFrame::Array(parts.iter().map(|part| RespFrame::bulk_str(part)).collect());
        let mut access = super::super::ServerAccess::new_inline(server);
        super::super::execute(frame, &mut access, client).response
    }

    #[test]
    fn test_sha1_empty() {
        // SHA1("") = da39a3ee5e6b4b0d3255bfef95601890afd80709
        assert_eq!(sha1_hex(b""), "da39a3ee5e6b4b0d3255bfef95601890afd80709");
    }

    #[test]
    fn test_sha1_hello() {
        // SHA1("hello") = aaf4c61ddcc5e8a2dabede0f3b482cd9aea9434d
        assert_eq!(
            sha1_hex(b"hello"),
            "aaf4c61ddcc5e8a2dabede0f3b482cd9aea9434d"
        );
    }

    #[test]
    fn test_sha1_abc() {
        // SHA1("abc") = a9993e364706816aba3e25717850c26c9cd0d89d
        assert_eq!(sha1_hex(b"abc"), "a9993e364706816aba3e25717850c26c9cd0d89d");
    }

    #[test]
    fn test_sha1_redis_script() {
        // Known Redis script: "return 1"
        // SHA1("return 1") = e0e1f9fabfc9d4800c877a703b823ac0578ff8db
        assert_eq!(
            sha1_hex(b"return 1"),
            "e0e1f9fabfc9d4800c877a703b823ac0578ff8db"
        );
    }

    #[cfg(feature = "lua-scripting")]
    #[test]
    fn parse_keys_argv_rejects_usize_max_without_overflow() {
        let numkeys = usize::MAX.to_string();
        let args = vec![Bytes::from_static(b"return 1"), Bytes::from(numkeys)];

        let error = parse_keys_argv(&args, "eval").unwrap_err();
        assert_error_contains(
            &error,
            "Number of keys can't be greater than number of args",
        );
    }

    #[cfg(feature = "lua-scripting")]
    fn run_argv(
        server: &mut ServerState,
        client: &mut ClientState,
        parts: &[&str],
    ) -> (RespFrame, Option<crate::command::DurabilityEffects>) {
        let argv = arguments(parts);
        let mut access = crate::command::ServerAccess::new_inline(server);
        let response = crate::command::execute_argv(&argv, &mut access, client).response;
        (response, client.take_durability_effects())
    }

    #[cfg(feature = "lua-scripting")]
    fn durable_strings(effects: &crate::command::DurabilityEffects) -> Vec<(usize, Vec<String>)> {
        effects
            .commands
            .iter()
            .map(|command| {
                (
                    command.db_index,
                    command
                        .argv
                        .iter()
                        .map(|arg| String::from_utf8_lossy(arg).into_owned())
                        .collect(),
                )
            })
            .collect()
    }

    #[cfg(feature = "lua-scripting")]
    #[test]
    fn eval_writes_become_one_atomic_effect_transaction_that_replays_identically() {
        let script = "redis.call('SET','s','v') \
            redis.call('INCR','n') \
            redis.call('SADD','set','a') \
            redis.call('XADD','x','*','f','1') \
            redis.call('HSET','h','f','v') \
            redis.pcall('LPUSH','s','bad') \
            redis.call('GET','s') \
            local popped = redis.call('SPOP','set') \
            redis.call('SELECT','1') \
            redis.call('SET','other','1') \
            return popped";
        let mut server = ServerState::with_default_dbs();
        let mut client = ClientState::default();
        client.set_durability_capture_enabled(true);
        let (response, effects) = run_argv(&mut server, &mut client, &["EVAL", script, "0"]);
        assert_eq!(response, RespFrame::bulk_str("a"));
        let effects = effects.expect("script writes must be durable");
        assert!(effects.transaction);
        let strings = durable_strings(&effects);
        let names: Vec<(usize, &str)> = strings
            .iter()
            .map(|(db, argv)| (*db, argv[0].as_str()))
            .collect();
        // The failed LPUSH and the read-only GET log nothing. SPOP becomes DEL
        // because it emptied the set, and XADD carries its generated ID.
        assert_eq!(
            names,
            vec![
                (0, "SET"),
                (0, "INCR"),
                (0, "SADD"),
                (0, "XADD"),
                (0, "HSET"),
                (0, "DEL"),
                (1, "SET"),
            ]
        );
        assert_ne!(strings[3].1[2], "*");

        let mut replayed = ServerState::with_default_dbs();
        let mut replay_client = ClientState::default();
        for (db, argv) in &strings {
            let select = db.to_string();
            run_argv(&mut replayed, &mut replay_client, &["SELECT", &select]);
            let parts: Vec<&str> = argv.iter().map(String::as_str).collect();
            let (reply, _) = run_argv(&mut replayed, &mut replay_client, &parts);
            assert!(!matches!(reply, RespFrame::Error(_)), "{argv:?}: {reply:?}");
        }
        for db in 0..2 {
            assert_eq!(
                server.db(db).len(),
                replayed.db(db).len(),
                "db {db} key count"
            );
        }
        let mut probe = ClientState::default();
        for parts in [
            &["GET", "s"][..],
            &["GET", "n"],
            &["SMEMBERS", "set"],
            &["XRANGE", "x", "-", "+"],
            &["HGETALL", "h"],
        ] {
            let (live, _) = run_argv(&mut server, &mut probe, parts);
            let (again, _) = run_argv(&mut replayed, &mut probe, parts);
            assert_eq!(live, again, "{parts:?}");
        }
        run_argv(&mut server, &mut probe, &["SELECT", "1"]);
        run_argv(&mut replayed, &mut probe, &["SELECT", "1"]);
        let (live, _) = run_argv(&mut server, &mut probe, &["GET", "other"]);
        let (again, _) = run_argv(&mut replayed, &mut probe, &["GET", "other"]);
        assert_eq!(live, again);
        assert_eq!(live, RespFrame::bulk_str("1"));
    }

    #[cfg(feature = "lua-scripting")]
    #[test]
    fn read_only_failed_and_ro_scripts_log_nothing() {
        let mut server = ServerState::with_default_dbs();
        let mut client = ClientState::default();
        client.set_durability_capture_enabled(true);
        run_argv(&mut server, &mut client, &["SET", "k", "v"]);

        let (_, effects) = run_argv(
            &mut server,
            &mut client,
            &["EVAL", "return redis.call('GET','k')", "0"],
        );
        assert!(effects.is_none());
        let (_, effects) = run_argv(
            &mut server,
            &mut client,
            &["EVAL", "return redis.pcall('LPUSH','k','x')", "0"],
        );
        assert!(effects.is_none(), "failed write must not be logged");
        let (reply, effects) = run_argv(
            &mut server,
            &mut client,
            &["EVAL_RO", "return redis.pcall('SET','k','x')", "0"],
        );
        assert!(matches!(reply, RespFrame::Error(_)));
        assert!(effects.is_none());
        let (_, effects) = run_argv(
            &mut server,
            &mut client,
            &["SCRIPT", "LOAD", "return redis.call('SET','k','x')"],
        );
        assert!(effects.is_none(), "scripts are not persisted, as in Redis");
        // Stale effects from an earlier script must not leak into the next.
        run_argv(
            &mut server,
            &mut client,
            &["EVAL", "return redis.call('SET','a','1')", "0"],
        );
        let (_, effects) = run_argv(&mut server, &mut client, &["PING"]);
        assert!(effects.is_none());
    }

    #[cfg(feature = "lua-scripting")]
    #[test]
    fn writes_before_a_script_error_are_still_logged() {
        let mut server = ServerState::with_default_dbs();
        let mut client = ClientState::default();
        client.set_durability_capture_enabled(true);
        let (reply, effects) = run_argv(
            &mut server,
            &mut client,
            &[
                "EVAL",
                "redis.call('SET','a','1') return redis.call('LPUSH','a','x')",
                "0",
            ],
        );
        assert!(matches!(reply, RespFrame::Error(_)));
        let effects = durable_strings(&effects.expect("SET happened"));
        assert_eq!(
            effects,
            vec![(0, vec!["SET".into(), "a".into(), "1".into()])]
        );
    }

    #[cfg(feature = "lua-scripting")]
    #[test]
    fn script_select_is_undone_on_every_exit_path() {
        let mut server = ServerState::with_default_dbs();
        let mut client = ClientState::default();
        client.set_durability_capture_enabled(true);
        run_argv(&mut server, &mut client, &["SELECT", "2"]);
        let scripts = [
            "redis.call('SELECT','1') return redis.call('SET','a','1')",
            "redis.call('SELECT','1') return redis.call('LPUSH','nokey')",
            "redis.call('SELECT','1') return redis.pcall('NOSUCHCMD')",
            "redis.call('SELECT','1') error('boom')",
            "redis.call('SELECT','1') return redis.call('NOSUCHCMD')",
            "redis.call('SELECT','1') while true do end",
        ];
        for script in scripts {
            for cmd in ["EVAL", "EVAL_RO"] {
                run_argv(&mut server, &mut client, &[cmd, script, "0"]);
                assert_eq!(client.selected_db(), 2, "{cmd} {script}");
            }
        }
        let (_, effects) = run_argv(
            &mut server,
            &mut client,
            &[
                "EVAL",
                "redis.call('SELECT','1') return redis.call('SET','b','1')",
                "0",
            ],
        );
        assert_eq!(
            durable_strings(&effects.expect("effects")),
            vec![(1, vec!["SET".into(), "b".into(), "1".into()])]
        );
        assert_eq!(client.selected_db(), 2);
        run_argv(
            &mut server,
            &mut client,
            &["EVALSHA", "0000000000000000000000000000000000000000", "0"],
        );
        assert_eq!(client.selected_db(), 2);
    }

    #[cfg(feature = "lua-scripting")]
    #[test]
    fn queued_commands_after_eval_in_multi_run_in_the_original_db() {
        let mut server = ServerState::with_default_dbs();
        let mut client = ClientState::default();
        client.set_durability_capture_enabled(true);
        run_argv(&mut server, &mut client, &["SELECT", "2"]);
        run_argv(&mut server, &mut client, &["MULTI"]);
        run_argv(
            &mut server,
            &mut client,
            &[
                "EVAL",
                "redis.call('SELECT','1') return redis.call('SET','in1','x')",
                "0",
            ],
        );
        run_argv(&mut server, &mut client, &["SET", "in2", "y"]);
        let (_, effects) = run_argv(&mut server, &mut client, &["EXEC"]);
        assert_eq!(
            durable_strings(&effects.expect("effects"))
                .iter()
                .map(|(db, a)| (*db, a[1].clone()))
                .collect::<Vec<_>>(),
            vec![(1, "in1".to_string()), (2, "in2".to_string())]
        );
        assert_eq!(client.selected_db(), 2);
    }

    #[cfg(feature = "lua-scripting")]
    #[test]
    fn eval_inside_multi_exec_joins_the_exec_transaction() {
        let mut server = ServerState::with_default_dbs();
        let mut client = ClientState::default();
        client.set_durability_capture_enabled(true);
        run_argv(&mut server, &mut client, &["MULTI"]);
        run_argv(&mut server, &mut client, &["SET", "before", "1"]);
        run_argv(
            &mut server,
            &mut client,
            &["EVAL", "return redis.call('SET','in-script','2')", "0"],
        );
        let (reply, effects) = run_argv(&mut server, &mut client, &["EXEC"]);
        assert!(matches!(reply, RespFrame::Array(_)));
        let effects = effects.expect("EXEC effects");
        assert!(effects.transaction);
        let strings = durable_strings(&effects);
        assert_eq!(
            strings
                .iter()
                .map(|(_, a)| a[1].as_str())
                .collect::<Vec<_>>(),
            vec!["before", "in-script"]
        );
    }

    #[cfg(feature = "lua-scripting")]
    #[test]
    fn readonly_aliases_reject_call_and_pcall_without_dataset_or_durable_effects() {
        let mut server = ServerState::with_default_dbs();
        let mut client = ClientState::default();
        client.set_durability_capture_enabled(true);

        let call = arguments(&["return redis.call('SET','call-write','value')", "0"]);
        let response = cmd_eval_ro(&call, &mut server, &mut client).response;
        assert_error_contains(&response, "read-only scripts");
        assert!(
            !server
                .db(0)
                .contains_key(&Bytes::from_static(b"call-write"))
        );
        assert!(client.take_durability_effects().is_none());

        let script = Bytes::from_static(b"return redis.pcall('SET','pcall-write','value')");
        let RespFrame::BulkString(Some(sha)) =
            script_load(std::slice::from_ref(&script), &mut server).response
        else {
            panic!("SCRIPT LOAD did not return a SHA");
        };
        let response =
            cmd_evalsha_ro(&[sha, Bytes::from_static(b"0")], &mut server, &mut client).response;
        assert_error_contains(&response, "read-only scripts");
        assert!(
            !server
                .db(0)
                .contains_key(&Bytes::from_static(b"pcall-write"))
        );
        assert!(client.take_durability_effects().is_none());
    }

    #[cfg(feature = "lua-scripting")]
    #[test]
    fn readonly_reads_succeed_and_mode_is_scoped_to_one_invocation() {
        let mut server = ServerState::with_default_dbs();
        let mut client = ClientState::default();

        let write = arguments(&["return redis.call('SET','seed','value')", "0"]);
        assert_eq!(
            cmd_eval(&write, &mut server, &mut client).response,
            RespFrame::ok()
        );

        let mixed_case_read = arguments(&["return redis.call('gEt','seed')", "0"]);
        assert_eq!(
            cmd_eval_ro(&mixed_case_read, &mut server, &mut client).response,
            RespFrame::bulk_str("value")
        );

        let rejected = arguments(&["return redis.call('sEt','blocked','value')", "0"]);
        assert_error_contains(
            &cmd_eval_ro(&rejected, &mut server, &mut client).response,
            "read-only scripts",
        );

        let following_write = arguments(&["return redis.call('SET','after-ro','value')", "0"]);
        assert_eq!(
            cmd_eval(&following_write, &mut server, &mut client).response,
            RespFrame::ok()
        );
        assert!(server.db(0).contains_key(&Bytes::from_static(b"after-ro")));
    }

    #[cfg(feature = "lua-scripting")]
    #[test]
    fn os_library_is_not_exposed_in_writable_or_readonly_scripts() {
        let mut server = ServerState::with_default_dbs();
        let mut client = ClientState::default();
        let check = arguments(&["return type(os)", "0"]);

        assert_eq!(
            cmd_eval(&check, &mut server, &mut client).response,
            RespFrame::bulk_str("nil")
        );
        assert_eq!(
            cmd_eval_ro(&check, &mut server, &mut client).response,
            RespFrame::bulk_str("nil")
        );
    }

    #[cfg(feature = "lua-scripting")]
    #[test]
    fn readonly_rejections_precede_server_connection_transaction_and_pubsub_effects() {
        let mut server = ServerState::with_default_dbs();
        let mut client = ClientState::new(42);
        client.set_durability_capture_enabled(true);
        let mut receiver = server.pubsub.register_client(99);
        server
            .pubsub
            .subscribe_channel(99, Bytes::from_static(b"events"));
        server
            .pubsub
            .subscribe_shard_channel(99, Bytes::from_static(b"events"));

        let forbidden = [
            "return redis.pcall('PUBLISH','events','payload')",
            "return redis.pcall('SPUBLISH','events','payload')",
            "return redis.pcall('SUBSCRIBE','events')",
            "return redis.pcall('CONFIG','SET','maxmemory','1')",
            "return redis.pcall('CLIENT','SETNAME','lua')",
            "return redis.pcall('SELECT','1')",
            "return redis.pcall('RESET')",
            "return redis.pcall('MULTI')",
            "return redis.pcall('EXEC')",
            "return redis.pcall('EVAL','return 1','0')",
        ];

        for source in forbidden {
            let response =
                cmd_eval_ro(&arguments(&[source, "0"]), &mut server, &mut client).response;
            assert!(
                matches!(response, RespFrame::Error(_)),
                "{source}: {response:?}"
            );
        }

        assert_eq!(server.config.maxmemory(), 0);
        assert_eq!(client.selected_db(), 0);
        assert!(!client.in_multi());
        assert_eq!(client.acl_user(), &Bytes::from_static(b"default"));
        assert_eq!(
            server.pubsub.numsub(&[Bytes::from_static(b"events")])[0].1,
            1
        );
        assert_eq!(
            server.pubsub.shard_numsub(&[Bytes::from_static(b"events")])[0].1,
            1
        );
        assert!(
            matches!(
                receiver.try_recv(),
                Err(tokio::sync::mpsc::error::TryRecvError::Empty)
            ),
            "a rejected publish reached the subscriber"
        );
        assert!(client.take_durability_effects().is_none());
    }

    #[cfg(feature = "lua-scripting")]
    #[test]
    fn all_scripts_reject_auth_hello_and_noscript_commands_before_dispatch() {
        let mut server = ServerState::with_default_dbs();
        let mut client = ClientState::new(43);
        let forbidden = [
            "return redis.pcall('AUTH','bad-password')",
            "return redis.pcall('HELLO','3','AUTH','default','bad-password')",
            "return redis.pcall('CONFIG','GET','maxmemory')",
            "return redis.pcall('CLIENT','GETNAME')",
            "return redis.pcall('MULTI')",
        ];

        for source in forbidden {
            let response = cmd_eval(&arguments(&[source, "0"]), &mut server, &mut client).response;
            assert_error_contains(&response, "not allowed from script");
        }

        assert!(!client.is_authenticated());
        assert_eq!(client.auth_failure_count(), 0);
        assert_eq!(client.protocol_version(), 2);
        assert!(!client.in_multi());
    }

    #[cfg(feature = "lua-scripting")]
    #[test]
    fn readonly_script_queued_in_multi_is_rejected_by_exec_without_mutation() {
        let mut server = ServerState::with_default_dbs();
        let mut client = ClientState::default();
        client.set_durability_capture_enabled(true);

        assert_eq!(
            run_command(&["MULTI"], &mut server, &mut client),
            RespFrame::ok()
        );
        assert_eq!(
            run_command(
                &[
                    "EVAL_RO",
                    "return redis.call('SET','queued-write','value')",
                    "0",
                ],
                &mut server,
                &mut client,
            ),
            RespFrame::SimpleString(Bytes::from_static(b"QUEUED"))
        );
        let response = run_command(&["EXEC"], &mut server, &mut client);
        let RespFrame::Array(items) = response else {
            panic!("expected EXEC array, got {response:?}");
        };
        assert_eq!(items.len(), 1);
        assert_error_contains(&items[0], "read-only scripts");
        assert!(
            !server
                .db(0)
                .contains_key(&Bytes::from_static(b"queued-write"))
        );
        assert!(client.take_durability_effects().is_none());
    }
}
