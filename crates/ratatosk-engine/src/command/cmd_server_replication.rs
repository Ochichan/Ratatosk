use bytes::Bytes;

use ratatosk_resp::frame::RespFrame;

use crate::keyspace::{ReplicationMode, ServerState};

use super::{ClientState, CommandOutcome, err, now_ms, parse_i64, to_uppercase_bytes, wrong_arity};

pub(super) fn cmd_role(args: &[Bytes], server: &ServerState) -> CommandOutcome {
    if !args.is_empty() {
        return wrong_arity("role");
    }

    let reply = match server.replication_mode() {
        ReplicationMode::Master => {
            let replicas = server
                .replication_replica_infos(now_ms())
                .into_iter()
                .map(|replica| {
                    RespFrame::Array(vec![
                        RespFrame::BulkString(Some(replica.ip_address)),
                        RespFrame::Integer(replica.listening_port),
                        RespFrame::Integer(replica.ack_offset),
                    ])
                })
                .collect::<Vec<_>>();
            RespFrame::Array(vec![
                RespFrame::bulk_str("master"),
                RespFrame::Integer(server.replication_offset()),
                RespFrame::Array(replicas),
            ])
        }
        ReplicationMode::Replica {
            master_host,
            master_port,
        } => RespFrame::Array(vec![
            RespFrame::bulk_str("slave"),
            RespFrame::BulkString(Some(master_host.clone())),
            RespFrame::Integer(*master_port),
            RespFrame::bulk_str("connected"),
            RespFrame::Integer(server.replication_offset()),
        ]),
    };

    CommandOutcome::reply(reply)
}

pub(super) fn cmd_replconf(
    args: &[Bytes],
    server: &mut ServerState,
    client: &ClientState,
) -> CommandOutcome {
    if args.len() < 2 {
        return wrong_arity("replconf");
    }

    let option = to_uppercase_bytes(&args[0]);
    match option.as_slice() {
        b"LISTENING-PORT" => {
            if args.len() != 2 {
                return wrong_arity("replconf");
            }
            let Some(port) = parse_i64(&args[1]) else {
                return CommandOutcome::reply(err("ERR value is not an integer or out of range"));
            };
            if !(0..=65535).contains(&port) {
                return CommandOutcome::reply(err("ERR value is out of range"));
            }
            server.replication_set_listening_port(client.id(), port);
            CommandOutcome::reply(RespFrame::ok())
        }
        b"CAPA" => {
            if args.len() < 2 {
                return wrong_arity("replconf");
            }
            server.replication_set_capabilities(client.id(), args[1..].iter().cloned());
            CommandOutcome::reply(RespFrame::ok())
        }
        b"ACK" => {
            if args.len() != 2 {
                return wrong_arity("replconf");
            }
            let Some(offset) = parse_i64(&args[1]) else {
                return CommandOutcome::reply(err("ERR value is not an integer or out of range"));
            };
            server.replication_set_ack_offset(client.id(), offset);
            CommandOutcome::reply(RespFrame::ok())
        }
        b"GETACK" => {
            if args.len() != 2 {
                return wrong_arity("replconf");
            }
            if !args[1].eq_ignore_ascii_case(b"*") {
                return CommandOutcome::reply(err("ERR syntax error"));
            }
            CommandOutcome::reply(RespFrame::Array(vec![
                RespFrame::bulk_str("REPLCONF"),
                RespFrame::bulk_str("ACK"),
                RespFrame::BulkString(Some(Bytes::from(server.replication_offset().to_string()))),
            ]))
        }
        b"IP-ADDRESS" => {
            if args.len() != 2 {
                return wrong_arity("replconf");
            }
            server.replication_set_ip_address(client.id(), args[1].clone());
            CommandOutcome::reply(RespFrame::ok())
        }
        _ => CommandOutcome::reply(err("ERR Unknown REPLCONF option")),
    }
}

pub(super) fn cmd_sync(args: &[Bytes]) -> CommandOutcome {
    if !args.is_empty() {
        return wrong_arity("sync");
    }

    CommandOutcome::reply(err("ERR SYNC is not supported in standalone mode"))
}

pub(super) fn cmd_psync(
    args: &[Bytes],
    _server: &mut ServerState,
    _client: &ClientState,
) -> CommandOutcome {
    if args.len() != 2 {
        return wrong_arity("psync");
    }

    CommandOutcome::reply(err(
        "ERR PSYNC is not supported; Ratatosk runs in standalone mode",
    ))
}

pub(super) fn cmd_replicaof(args: &[Bytes], server: &mut ServerState) -> CommandOutcome {
    let [host, port_raw] = args else {
        return wrong_arity("replicaof");
    };

    if host.eq_ignore_ascii_case(b"NO") && port_raw.eq_ignore_ascii_case(b"ONE") {
        server.replication_configure_master();
        return CommandOutcome::reply(RespFrame::ok());
    }

    CommandOutcome::reply(err(
        "ERR REPLICAOF is not supported; Ratatosk runs in standalone mode. Use REPLICAOF NO ONE to confirm standalone.",
    ))
}

pub(super) fn append_info_replication_section(out: &mut String, server: &ServerState) {
    out.push_str("# Replication\r\n");

    match server.replication_mode() {
        ReplicationMode::Master => {
            out.push_str("role:master\r\n");
            out.push_str(&format!(
                "connected_slaves:{}\r\n",
                server.replication_connected_replicas()
            ));
            for (idx, replica) in server
                .replication_replica_infos(now_ms())
                .into_iter()
                .enumerate()
            {
                out.push_str(&format!(
                    "slave{idx}:ip={},port={},state={},offset={},lag={}\r\n",
                    String::from_utf8_lossy(&replica.ip_address),
                    replica.listening_port,
                    replica.state,
                    replica.ack_offset,
                    replica.lag_seconds
                ));
            }
            out.push_str(&format!(
                "master_replid:{}\r\n",
                String::from_utf8_lossy(server.replication_primary_replid())
            ));
            out.push_str(&format!(
                "master_repl_offset:{}\r\n",
                server.replication_offset()
            ));
            out.push_str("second_repl_offset:-1\r\n");
        }
        ReplicationMode::Replica {
            master_host,
            master_port,
        } => {
            out.push_str("role:slave\r\n");
            out.push_str(&format!(
                "master_host:{}\r\n",
                String::from_utf8_lossy(master_host)
            ));
            out.push_str(&format!("master_port:{master_port}\r\n"));
            out.push_str("master_link_status:connected\r\n");
            out.push_str("master_last_io_seconds_ago:0\r\n");
            out.push_str("master_sync_in_progress:0\r\n");
            out.push_str(&format!(
                "slave_repl_offset:{}\r\n",
                server.replication_offset()
            ));
            out.push_str(&format!(
                "slave_read_repl_offset:{}\r\n",
                server.replication_offset()
            ));
        }
    }

    out.push_str("repl_backlog_active:0\r\n");
    out.push_str("repl_backlog_size:0\r\n");
    out.push_str("repl_backlog_first_byte_offset:0\r\n");
    out.push_str("repl_backlog_histlen:0\r\n");
    out.push_str("\r\n");
}
