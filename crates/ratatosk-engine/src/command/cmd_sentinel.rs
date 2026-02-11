use bytes::Bytes;

use ratatosk_resp::frame::RespFrame;

use super::{CommandOutcome, err, to_uppercase_bytes, wrong_arity};

// ---------------------------------------------------------------------------
// SENTINEL <subcommand>
// ---------------------------------------------------------------------------

pub(super) fn cmd_sentinel(args: &[Bytes]) -> CommandOutcome {
    if args.is_empty() {
        return wrong_arity("sentinel");
    }

    let sub = to_uppercase_bytes(&args[0]);
    match sub.as_slice() {
        b"HELP" => sentinel_help(),
        _ => CommandOutcome::reply(err("ERR This instance is not configured as a Sentinel")),
    }
}

fn sentinel_help() -> CommandOutcome {
    let lines: Vec<RespFrame> = vec![
        RespFrame::BulkString(Some(Bytes::from_static(
            b"SENTINEL <subcommand> [<arg> [value] [opt] ...]. Subcommands are:",
        ))),
        RespFrame::BulkString(Some(Bytes::from_static(b"MASTERS"))),
        RespFrame::BulkString(Some(Bytes::from_static(
            b"    Show a list of monitored masters and their state.",
        ))),
        RespFrame::BulkString(Some(Bytes::from_static(b"MASTER <name>"))),
        RespFrame::BulkString(Some(Bytes::from_static(
            b"    Show the state and info of the specified master.",
        ))),
        RespFrame::BulkString(Some(Bytes::from_static(b"REPLICAS <name>"))),
        RespFrame::BulkString(Some(Bytes::from_static(
            b"    Show a list of replicas for the specified master and their state.",
        ))),
        RespFrame::BulkString(Some(Bytes::from_static(b"SENTINELS <name>"))),
        RespFrame::BulkString(Some(Bytes::from_static(
            b"    Show a list of Sentinel instances for the specified master and their state.",
        ))),
        RespFrame::BulkString(Some(Bytes::from_static(b"GET-MASTER-ADDR-BY-NAME <name>"))),
        RespFrame::BulkString(Some(Bytes::from_static(
            b"    Return the ip and port number of the master with that name.",
        ))),
        RespFrame::BulkString(Some(Bytes::from_static(b"RESET <pattern>"))),
        RespFrame::BulkString(Some(Bytes::from_static(
            b"    Reset masters matching <pattern>.",
        ))),
        RespFrame::BulkString(Some(Bytes::from_static(b"FAILOVER <name>"))),
        RespFrame::BulkString(Some(Bytes::from_static(
            b"    Force a failover for the specified master.",
        ))),
        RespFrame::BulkString(Some(Bytes::from_static(b"CKQUORUM <name>"))),
        RespFrame::BulkString(Some(Bytes::from_static(
            b"    Check if the current Sentinel configuration is able to reach the quorum.",
        ))),
        RespFrame::BulkString(Some(Bytes::from_static(b"FLUSHCONFIG"))),
        RespFrame::BulkString(Some(Bytes::from_static(
            b"    Force Sentinel to rewrite its configuration on disk.",
        ))),
        RespFrame::BulkString(Some(Bytes::from_static(
            b"MONITOR <name> <ip> <port> <quorum>",
        ))),
        RespFrame::BulkString(Some(Bytes::from_static(
            b"    Start monitoring a new master with the specified name, ip, port and quorum.",
        ))),
        RespFrame::BulkString(Some(Bytes::from_static(b"REMOVE <name>"))),
        RespFrame::BulkString(Some(Bytes::from_static(
            b"    Remove master from Sentinel's monitor list.",
        ))),
        RespFrame::BulkString(Some(Bytes::from_static(
            b"SET <name> <option> <value> [<option> <value> ...]",
        ))),
        RespFrame::BulkString(Some(Bytes::from_static(
            b"    Set configuration parameters for the specified master.",
        ))),
        RespFrame::BulkString(Some(Bytes::from_static(
            b"IS-MASTER-DOWN-BY-ADDR <ip> <port> <current-epoch> <runid>",
        ))),
        RespFrame::BulkString(Some(Bytes::from_static(
            b"    Check if the master specified by ip:port is down from the current Sentinel's point of view.",
        ))),
        RespFrame::BulkString(Some(Bytes::from_static(b"SIMULATE-FAILURE <flag> [<flag> ...]"))),
        RespFrame::BulkString(Some(Bytes::from_static(
            b"    Simulate Sentinel crash or partition for testing.",
        ))),
        RespFrame::BulkString(Some(Bytes::from_static(b"PENDING-SCRIPTS"))),
        RespFrame::BulkString(Some(Bytes::from_static(
            b"    Get a list of pending scripts.",
        ))),
        RespFrame::BulkString(Some(Bytes::from_static(b"INFO-CACHE <name>"))),
        RespFrame::BulkString(Some(Bytes::from_static(
            b"    Return cached INFO output from masters and replicas.",
        ))),
        RespFrame::BulkString(Some(Bytes::from_static(b"MYID"))),
        RespFrame::BulkString(Some(Bytes::from_static(
            b"    Return the ID of the Sentinel instance.",
        ))),
        RespFrame::BulkString(Some(Bytes::from_static(
            b"CONFIG SET <name> <option> <value> [<option> <value> ...]",
        ))),
        RespFrame::BulkString(Some(Bytes::from_static(
            b"    Set global Sentinel configuration parameter.",
        ))),
        RespFrame::BulkString(Some(Bytes::from_static(
            b"CONFIG GET <name> [<option> ...]",
        ))),
        RespFrame::BulkString(Some(Bytes::from_static(
            b"    Get global Sentinel configuration parameter.",
        ))),
        RespFrame::BulkString(Some(Bytes::from_static(b"DEBUG <param> [<param> ...]"))),
        RespFrame::BulkString(Some(Bytes::from_static(
            b"    Change Sentinel debug parameters.",
        ))),
        RespFrame::BulkString(Some(Bytes::from_static(b"HELP"))),
        RespFrame::BulkString(Some(Bytes::from_static(b"    Print this help."))),
    ];

    CommandOutcome::reply(RespFrame::Array(lines))
}
