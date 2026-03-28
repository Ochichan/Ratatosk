use bytes::Bytes;

use ratatosk_resp::frame::RespFrame;

use super::{CommandOutcome, err, parse_i64, parse_usize, to_uppercase_bytes, wrong_arity};

pub(super) fn cmd_debug(args: &[Bytes]) -> CommandOutcome {
    if args.is_empty() {
        return wrong_arity("debug");
    }

    let subcommand = to_uppercase_bytes(&args[0]);
    match subcommand.as_slice() {
        b"HELP" => {
            if args.len() != 1 {
                return wrong_arity("debug");
            }
            CommandOutcome::reply(RespFrame::Array(vec![
                RespFrame::bulk_str("HELP -- Show this help."),
                RespFrame::bulk_str(
                    "OBJECT <key> -- Return debug metadata for a key (not supported in baseline).",
                ),
            ]))
        }
        _ => CommandOutcome::reply(err("ERR DEBUG subcommand is not supported")),
    }
}

pub(super) fn cmd_lolwut(args: &[Bytes]) -> CommandOutcome {
    if args.is_empty() {
        return CommandOutcome::reply(RespFrame::BulkString(Some(Bytes::from_static(
            b"Ratatosk says hi from standalone mode.",
        ))));
    }

    if args.len() == 2 && args[0].eq_ignore_ascii_case(b"VERSION") {
        let Some(version) = parse_i64(&args[1]) else {
            return CommandOutcome::reply(err("ERR value is not an integer or out of range"));
        };
        return CommandOutcome::reply(RespFrame::BulkString(Some(Bytes::from(format!(
            "Ratatosk says hi from standalone mode (version {version})."
        )))));
    }

    CommandOutcome::reply(err("ERR syntax error"))
}

pub(super) fn cmd_module(args: &[Bytes]) -> CommandOutcome {
    if args.is_empty() {
        return wrong_arity("module");
    }

    let subcommand = to_uppercase_bytes(&args[0]);
    match subcommand.as_slice() {
        b"HELP" => {
            if args.len() != 1 {
                return wrong_arity("module");
            }
            CommandOutcome::reply(RespFrame::Array(vec![
                RespFrame::bulk_str("LIST -- Return loaded modules."),
                RespFrame::bulk_str("LOAD path [arg ...] -- Load a module (unsupported)."),
                RespFrame::bulk_str(
                    "LOADEX path [CONFIG name value ...] [ARGS ...] -- Load module with options (unsupported).",
                ),
                RespFrame::bulk_str("UNLOAD name -- Unload module (unsupported)."),
                RespFrame::bulk_str("HELP -- Show this help."),
            ]))
        }
        b"LIST" => {
            if args.len() != 1 {
                return wrong_arity("module");
            }
            CommandOutcome::reply(RespFrame::Array(vec![]))
        }
        b"LOAD" | b"LOADEX" | b"UNLOAD" => {
            CommandOutcome::reply(err("ERR MODULE command is not supported in this build"))
        }
        _ => CommandOutcome::reply(err("ERR unknown subcommand for MODULE")),
    }
}

pub(super) fn cmd_hotkeys(args: &[Bytes]) -> CommandOutcome {
    if args.is_empty() {
        return wrong_arity("hotkeys");
    }

    let subcommand = to_uppercase_bytes(&args[0]);
    match subcommand.as_slice() {
        b"GET" => {
            if args.len() != 1 {
                return wrong_arity("hotkeys");
            }
            CommandOutcome::reply(RespFrame::Array(vec![]))
        }
        b"RESET" | b"START" | b"STOP" => {
            if args.len() != 1 {
                return wrong_arity("hotkeys");
            }
            CommandOutcome::reply(RespFrame::ok())
        }
        b"HELP" => {
            if args.len() != 1 {
                return wrong_arity("hotkeys");
            }
            CommandOutcome::reply(RespFrame::Array(vec![
                RespFrame::bulk_str("GET -- Return hot key samples."),
                RespFrame::bulk_str("RESET -- Reset collected hot key samples."),
                RespFrame::bulk_str("START -- Start hot key sampling."),
                RespFrame::bulk_str("STOP -- Stop hot key sampling."),
                RespFrame::bulk_str("HELP -- Show this help."),
            ]))
        }
        _ => CommandOutcome::reply(err("ERR unknown subcommand for HOTKEYS")),
    }
}

pub(super) fn cmd_failover(_args: &[Bytes]) -> CommandOutcome {
    CommandOutcome::reply(err("ERR FAILOVER is not supported in standalone mode"))
}

pub(super) fn cmd_shutdown(_args: &[Bytes]) -> CommandOutcome {
    CommandOutcome::reply(err("ERR SHUTDOWN is not supported in this build"))
}

pub(super) fn cmd_trimslots(args: &[Bytes]) -> CommandOutcome {
    let [slot] = args else {
        return wrong_arity("trimslots");
    };

    let Some(_slot_id) = parse_usize(slot) else {
        return CommandOutcome::reply(err("ERR value is not an integer or out of range"));
    };

    CommandOutcome::reply(RespFrame::Integer(0))
}
