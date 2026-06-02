use bytes::Bytes;

use ratatosk_resp::frame::RespFrame;

use super::{
    CommandOutcome, CommandSpec, all_command_specs, command_spec_count, err, find_command_spec,
    find_command_spec_parts, to_uppercase_bytes, wrong_arity,
};

pub(super) fn cmd_command(args: &[Bytes]) -> CommandOutcome {
    if args.is_empty() {
        return CommandOutcome::reply(command_full_reply());
    }

    let subcommand = to_uppercase_bytes(&args[0]);
    match subcommand.as_slice() {
        b"COUNT" => {
            if args.len() != 1 {
                return wrong_arity("command");
            }
            CommandOutcome::reply(RespFrame::Integer(command_spec_count() as i64))
        }
        b"LIST" => {
            if args.len() != 1 {
                return wrong_arity("command");
            }
            CommandOutcome::reply(command_list_reply())
        }
        b"INFO" => cmd_command_info(&args[1..]),
        b"DOCS" => cmd_command_docs(&args[1..]),
        b"GETKEYS" => cmd_command_getkeys(&args[1..], false),
        b"GETKEYSANDFLAGS" => cmd_command_getkeys(&args[1..], true),
        b"HELP" => {
            if args.len() != 1 {
                return wrong_arity("command");
            }
            CommandOutcome::reply(RespFrame::Array(vec![
                RespFrame::bulk_str("COUNT -- Return the total number of commands."),
                RespFrame::bulk_str("LIST -- Return the command names."),
                RespFrame::bulk_str(
                    "INFO command-name [command-name ...] -- Return command details.",
                ),
                RespFrame::bulk_str(
                    "DOCS [command-name ...] -- Return command docs map including Ratatosk capability tier.",
                ),
                RespFrame::bulk_str("HELP -- Show this help."),
            ]))
        }
        _ => CommandOutcome::reply(err("ERR unsupported COMMAND subcommand")),
    }
}

fn cmd_command_info(args: &[Bytes]) -> CommandOutcome {
    if args.is_empty() {
        return wrong_arity("command");
    }

    let mut frames = Vec::with_capacity(args.len());
    for spec in requested_command_specs(args) {
        if let Some(spec) = spec {
            frames.push(command_spec_frame(spec));
        } else {
            frames.push(RespFrame::Null);
        }
    }

    CommandOutcome::reply(RespFrame::Array(frames))
}

fn cmd_command_docs(args: &[Bytes]) -> CommandOutcome {
    let specs = if args.is_empty() {
        all_command_specs().collect::<Vec<_>>()
    } else {
        requested_command_specs(args)
            .into_iter()
            .flatten()
            .collect::<Vec<_>>()
    };

    let mut rows = Vec::with_capacity(specs.len());
    for spec in specs {
        let capability_tier = command_capability_tier(spec);
        let flags = spec
            .flags
            .iter()
            .map(|flag| RespFrame::bulk_str(flag))
            .collect::<Vec<_>>();
        rows.push((
            RespFrame::BulkString(Some(Bytes::from(spec.name.to_ascii_lowercase()))),
            RespFrame::Map(vec![
                (
                    RespFrame::bulk_str("summary"),
                    RespFrame::bulk_str(command_capability_summary(capability_tier)),
                ),
                (
                    RespFrame::bulk_str("arity"),
                    RespFrame::Integer(i64::from(spec.arity)),
                ),
                (RespFrame::bulk_str("flags"), RespFrame::Array(flags)),
                (
                    RespFrame::bulk_str("ratatosk_capability_tier"),
                    RespFrame::bulk_str(capability_tier),
                ),
            ]),
        ));
    }

    CommandOutcome::reply(RespFrame::Map(rows))
}

fn requested_command_specs(args: &[Bytes]) -> Vec<Option<CommandSpec>> {
    let max_name_parts = all_command_specs()
        .map(|spec| spec.name.split(' ').count())
        .max()
        .unwrap_or(1);

    let mut specs = Vec::with_capacity(args.len());
    let mut idx = 0usize;
    while idx < args.len() {
        let remaining = args.len() - idx;
        let mut matched = None;
        for len in (1..=remaining.min(max_name_parts)).rev() {
            if let Some(spec) = find_command_spec_parts(&args[idx..idx + len]) {
                matched = Some((len, spec));
                break;
            }
        }

        if let Some((len, spec)) = matched {
            specs.push(Some(spec));
            idx += len;
        } else {
            specs.push(find_command_spec(&args[idx]));
            idx += 1;
        }
    }

    specs
}

/// Resolve the most specific command spec for the leading command in `argv`,
/// folding a container command and its subcommand (e.g. `CLIENT PAUSE`,
/// `CLUSTER SETSLOT`) into a single spec when one exists. Trailing arguments are
/// ignored — only leading tokens that form a known command name are matched, so
/// a key literally named after a subcommand (`GET CLIENT`) never misresolves.
pub(super) fn resolve_leading_spec(argv: &[Bytes]) -> Option<CommandSpec> {
    if argv.is_empty() {
        return None;
    }
    let max_name_parts = all_command_specs()
        .map(|spec| spec.name.split(' ').count())
        .max()
        .unwrap_or(1);
    let remaining = argv.len();
    for len in (1..=remaining.min(max_name_parts)).rev() {
        if let Some(spec) = find_command_spec_parts(&argv[0..len]) {
            return Some(spec);
        }
    }
    None
}

/// Strict compatibility-mode gate.
///
/// Returns a structured error frame when `argv` names a command that strict mode
/// must reject: any `unsupported`/`syntax_only` capability tier, plus the
/// durability/replication metadata commands (`WAIT`/`WAITAOF`) whose Redis
/// contract a single-node server cannot honour. Returns `None` — meaning "allow"
/// — for every command Ratatosk genuinely implements. The caller only consults
/// this when `compatibility-mode strict` is active.
pub(super) fn strict_mode_error(argv: &[Bytes]) -> Option<RespFrame> {
    let spec = resolve_leading_spec(argv)?;
    let name = spec.name;
    let reason = if matches!(name, "WAIT" | "WAITAOF") {
        "requires replica-backed acknowledgement that a single-node server cannot provide"
    } else {
        match command_capability_tier(spec) {
            "unsupported" => "command is not supported in this single-node Ratatosk build",
            "syntax_only" => {
                "command is only accepted syntactically and has no Redis-equivalent operational effect"
            }
            _ => return None,
        }
    };
    Some(err(&format!(
        "ERR command {name} is not supported in Ratatosk strict compatibility mode; reason={reason}"
    )))
}

pub(super) fn command_capability_tier(spec: CommandSpec) -> &'static str {
    let name = spec.name;

    if matches!(
        name,
        "SYNC"
            | "SENTINEL"
            | "EVAL"
            | "EVALSHA"
            | "EVAL_RO"
            | "EVALSHA_RO"
            | "FCALL"
            | "FCALL_RO"
            | "CLIENT PAUSE"
            | "CLIENT UNPAUSE"
            | "MODULE LOAD"
            | "MODULE LOADEX"
            | "MODULE UNLOAD"
            | "SCRIPT KILL"
            | "SCRIPT DEBUG"
            | "FUNCTION LOAD"
            | "FUNCTION DELETE"
            | "FUNCTION RESTORE"
            // Handlers below always return a "not supported" error on a single
            // node, so the tier must say so (audited 2026-06-02).
            | "DEBUG"
            | "FAILOVER"
            | "SHUTDOWN"
    ) {
        return "unsupported";
    }

    if name == "SENTINEL HELP" {
        return "syntax_only";
    }
    if name.starts_with("SENTINEL ") {
        return "unsupported";
    }

    if name.starts_with("CLUSTER ")
        && !matches!(
            name,
            "CLUSTER COUNTKEYSINSLOT"
                | "CLUSTER GETKEYSINSLOT"
                | "CLUSTER HELP"
                | "CLUSTER INFO"
                | "CLUSTER KEYSLOT"
                | "CLUSTER LINKS"
                | "CLUSTER MYID"
                | "CLUSTER SHARDS"
                | "CLUSTER SLOTS"
        )
    {
        return "unsupported";
    }

    if matches!(
        name,
        // SFLUSH parses its mode and returns OK without flushing anything.
        "ASKING" | "READONLY" | "READWRITE" | "CLIENT UNBLOCK" | "SFLUSH"
    ) {
        return "syntax_only";
    }

    if matches!(
        name,
        "ROLE"
            | "REPLCONF"
            | "PSYNC"
            | "REPLICAOF"
            | "SLAVEOF"
            | "WAIT"
            | "WAITAOF"
            | "CLIENT"
            | "CLIENT CACHING"
            | "CLIENT GETREDIR"
            | "CLIENT INFO"
            | "CLIENT KILL"
            | "CLIENT LIST"
            | "CLIENT NO-EVICT"
            | "CLIENT NO-TOUCH"
            | "CLIENT REPLY"
            | "CLIENT SETINFO"
            | "CLIENT TRACKING"
            | "CLIENT TRACKINGINFO"
            | "CLUSTER"
            | "CLUSTER COUNTKEYSINSLOT"
            | "CLUSTER GETKEYSINSLOT"
            | "CLUSTER HELP"
            | "CLUSTER INFO"
            | "CLUSTER KEYSLOT"
            | "CLUSTER LINKS"
            | "CLUSTER MYID"
            | "CLUSTER SHARDS"
            | "CLUSTER SLOTS"
            | "CONFIG"
            | "CONFIG GET"
            | "CONFIG HELP"
            | "CONFIG RESETSTAT"
            | "CONFIG REWRITE"
            | "CONFIG SET"
            | "INFO"
            | "MONITOR"
            | "LATENCY"
            | "LATENCY DOCTOR"
            | "LATENCY GRAPH"
            | "LATENCY HELP"
            | "LATENCY HISTOGRAM"
            | "LATENCY HISTORY"
            | "LATENCY LATEST"
            | "LATENCY RESET"
            | "MEMORY"
            | "MEMORY DOCTOR"
            | "MEMORY HELP"
            | "MEMORY MALLOC-STATS"
            | "MEMORY PURGE"
            | "MEMORY STATS"
            | "MEMORY USAGE"
            | "HOTKEYS"
            | "HOTKEYS GET"
            | "HOTKEYS RESET"
            | "HOTKEYS START"
            | "HOTKEYS STOP"
            | "ACL LOAD"
            | "ACL SAVE"
            | "ACL DRYRUN"
            | "LOLWUT"
            | "TRIMSLOTS"
            | "MODULE"
            | "MODULE HELP"
            | "MODULE LIST"
            | "FUNCTION"
            | "FUNCTION HELP"
            | "FUNCTION LIST"
            | "FUNCTION DUMP"
            | "FUNCTION FLUSH"
            | "FUNCTION STATS"
            | "SCRIPT"
            | "SCRIPT LOAD"
            | "SCRIPT EXISTS"
            | "SCRIPT FLUSH"
            | "SCRIPT HELP"
    ) {
        return "baseline_local";
    }

    if matches!(
        name,
        "BGSAVE"
            | "BGREWRITEAOF"
            | "BLMOVE"
            | "BLMPOP"
            | "BLPOP"
            | "BRPOP"
            | "BRPOPLPUSH"
            | "FLUSHALL"
            | "FLUSHDB"
            | "FUNCTION KILL"
            | "SAVE"
            | "XREAD"
            | "XREADGROUP"
    ) {
        return "behavioral_subset";
    }

    "behavioral_subset"
}

fn command_capability_summary(capability_tier: &str) -> &'static str {
    match capability_tier {
        "unsupported" => "Not supported in the current Ratatosk build.",
        "syntax_only" => {
            "Parses or acknowledges syntax in standalone mode without full Redis side effects."
        }
        "baseline_local" => {
            "Implements a standalone-local baseline and does not claim distributed Redis parity."
        }
        "behavioral_subset" => {
            "Implements useful Redis-compatible behavior, but may omit some edge semantics or distributed contracts."
        }
        "distributed_parity" => {
            "Implements the expected Redis behavior including distributed contracts."
        }
        _ => "Capability tier is unknown.",
    }
}

fn cmd_command_getkeys(args: &[Bytes], with_flags: bool) -> CommandOutcome {
    if args.is_empty() {
        return wrong_arity("command");
    }

    let Some(spec) = find_command_spec(&args[0]) else {
        let name = String::from_utf8_lossy(&args[0]).to_string();
        return CommandOutcome::reply(err(&format!(
            "ERR Invalid command specified, or key spec not found for '{name}'"
        )));
    };

    let mut keys = Vec::new();
    for_each_command_key_position(spec, args.len(), |pos| {
        let Some(key) = args.get(pos) else {
            return;
        };
        if with_flags {
            keys.push(RespFrame::Array(vec![
                RespFrame::BulkString(Some(key.clone())),
                RespFrame::Array(vec![
                    RespFrame::bulk_str("RW"),
                    RespFrame::bulk_str("access"),
                    RespFrame::bulk_str("update"),
                ]),
            ]));
        } else {
            keys.push(RespFrame::BulkString(Some(key.clone())));
        }
    });

    CommandOutcome::reply(RespFrame::Array(keys))
}

pub(super) fn command_arity_matches(arity: i16, argc: usize) -> bool {
    if arity >= 0 {
        argc == arity as usize
    } else {
        argc >= (-arity) as usize
    }
}

pub(super) fn for_each_command_key_position(
    spec: CommandSpec,
    argv_len: usize,
    mut f: impl FnMut(usize),
) {
    if spec.first_key == 0 {
        return;
    }

    let first = spec.first_key as usize;
    let last = if spec.last_key < 0 {
        argv_len.saturating_sub(1)
    } else {
        spec.last_key as usize
    };
    let step = spec.key_step.max(1) as usize;

    if first >= argv_len {
        return;
    }

    let mut pos = first;
    while pos <= last && pos < argv_len {
        f(pos);
        pos += step;
    }
}

fn command_full_reply() -> RespFrame {
    let specs = all_command_specs().collect::<Vec<_>>();
    let frames = specs.iter().map(|spec| command_spec_frame(*spec)).collect();
    RespFrame::Array(frames)
}

fn command_list_reply() -> RespFrame {
    let names = all_command_specs()
        .map(|spec| RespFrame::BulkString(Some(Bytes::from(spec.name.to_ascii_lowercase()))))
        .collect();
    RespFrame::Array(names)
}

fn command_spec_frame(spec: CommandSpec) -> RespFrame {
    let flags = spec
        .flags
        .iter()
        .map(|flag| RespFrame::bulk_str(flag))
        .collect::<Vec<_>>();

    RespFrame::Array(vec![
        RespFrame::BulkString(Some(Bytes::from(spec.name.to_ascii_lowercase()))),
        RespFrame::Integer(i64::from(spec.arity)),
        RespFrame::Array(flags),
        RespFrame::Integer(spec.first_key),
        RespFrame::Integer(spec.last_key),
        RespFrame::Integer(spec.key_step),
    ])
}
