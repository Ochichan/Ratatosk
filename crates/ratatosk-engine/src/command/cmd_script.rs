use bytes::Bytes;

use ratatosk_resp::frame::RespFrame;

use crate::keyspace::ServerState;

use super::{CommandOutcome, err, to_uppercase_bytes, wrong_arity};

// ---------------------------------------------------------------------------
// EVAL / EVALSHA / EVAL_RO / EVALSHA_RO
// ---------------------------------------------------------------------------

pub(super) fn cmd_eval(args: &[Bytes]) -> CommandOutcome {
    if args.len() < 2 {
        return wrong_arity("eval");
    }
    CommandOutcome::reply(err("ERR Scripting not supported in this build"))
}

pub(super) fn cmd_evalsha(args: &[Bytes]) -> CommandOutcome {
    if args.len() < 2 {
        return wrong_arity("evalsha");
    }
    CommandOutcome::reply(err("NOSCRIPT No matching script. Please use EVAL."))
}

pub(super) fn cmd_eval_ro(args: &[Bytes]) -> CommandOutcome {
    cmd_eval(args)
}

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

    if args.len() == 1 {
        let mode = to_uppercase_bytes(&args[0]);
        if !matches!(mode.as_slice(), b"ASYNC" | b"SYNC") {
            return CommandOutcome::reply(err("ERR SCRIPT FLUSH only supports ASYNC|SYNC option"));
        }
    }

    server.script_cache.scripts.clear();
    CommandOutcome::reply(RespFrame::ok())
}

fn script_help() -> CommandOutcome {
    let lines: Vec<RespFrame> = vec![
        RespFrame::BulkString(Some(Bytes::from_static(
            b"SCRIPT <subcommand> [<arg> [value] [opt] ...]. Subcommands are:",
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
// FCALL / FCALL_RO / FUNCTION
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
        b"FLUSH" => CommandOutcome::reply(RespFrame::ok()),
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
}
