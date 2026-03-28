use std::fmt::Write;

use bytes::Bytes;

use ratatosk_resp::frame::RespFrame;

use crate::keyspace::{ClientSnapshot, ServerState};

use super::{ClientState, CommandOutcome, err, parse_i64};

pub(super) fn cmd_client_info(server: &ServerState, client: &ClientState) -> CommandOutcome {
    let line = server
        .client_snapshot(client.id())
        .map(|snapshot| {
            format_client_snapshot_line(snapshot, server.client_is_blocked(snapshot.id))
        })
        .unwrap_or_else(|| format_client_info_line(client));
    CommandOutcome::reply(RespFrame::bulk_str(&line))
}

pub(super) fn cmd_client_list(
    args: &[Bytes],
    server: &ServerState,
    client: &ClientState,
) -> CommandOutcome {
    let filter = match parse_client_list_filter(args) {
        Ok(filter) => filter,
        Err(response) => return CommandOutcome::reply(response),
    };

    let mut payload = String::new();
    let snapshots = server.client_snapshots();
    if snapshots.is_empty() {
        if client_snapshot_matches_filter(
            &filter,
            &client.snapshot(
                Bytes::from_static(b"127.0.0.1:0"),
                Bytes::from_static(b"127.0.0.1:0"),
            ),
            false,
        ) {
            payload.push_str(&format_client_info_line(client));
            payload.push('\n');
        }
    } else {
        for snapshot in snapshots {
            let blocked = server.client_is_blocked(snapshot.id);
            if client_snapshot_matches_filter(&filter, &snapshot, blocked) {
                payload.push_str(&format_client_snapshot_line(&snapshot, blocked));
                payload.push('\n');
            }
        }
    }

    CommandOutcome::reply(RespFrame::bulk_str(&payload))
}

#[derive(Default)]
struct ClientListFilter {
    type_filter: Option<Bytes>,
    id_filter: Option<i64>,
}

fn parse_client_list_filter(args: &[Bytes]) -> Result<ClientListFilter, RespFrame> {
    let mut filter = ClientListFilter::default();
    let mut idx = 0usize;

    while idx < args.len() {
        let option = super::to_uppercase_stack(&args[idx]);
        match option.as_slice() {
            b"TYPE" => {
                if idx + 1 >= args.len() {
                    return Err(err("ERR syntax error"));
                }

                let type_filter = super::to_uppercase_stack(&args[idx + 1]);
                filter.type_filter = Some(Bytes::copy_from_slice(type_filter.as_slice()));
                idx += 2;
            }
            b"ID" => {
                if idx + 1 >= args.len() {
                    return Err(err("ERR syntax error"));
                }

                let Some(id_filter) = parse_i64(&args[idx + 1]) else {
                    return Err(err("ERR value is not an integer or out of range"));
                };

                filter.id_filter = Some(id_filter);
                idx += 2;
            }
            _ => return Err(err("ERR syntax error")),
        }
    }

    Ok(filter)
}

fn client_snapshot_matches_filter(
    filter: &ClientListFilter,
    snapshot: &ClientSnapshot,
    _blocked: bool,
) -> bool {
    if let Some(id_filter) = filter.id_filter {
        if snapshot.id != id_filter {
            return false;
        }
    }

    if let Some(type_filter) = &filter.type_filter {
        match type_filter.as_ref() {
            b"NORMAL" => {
                if snapshot.sub > 0 {
                    return false;
                }
            }
            b"PUBSUB" => {
                if snapshot.sub == 0 {
                    return false;
                }
            }
            b"MASTER" | b"REPLICA" => return false,
            _ => return false,
        }
    }

    true
}

fn format_client_info_line(client: &ClientState) -> String {
    let snapshot = client.snapshot(
        Bytes::from_static(b"127.0.0.1:0"),
        Bytes::from_static(b"127.0.0.1:0"),
    );
    format_client_snapshot_line(&snapshot, false)
}

fn format_client_snapshot_line(snapshot: &ClientSnapshot, blocked: bool) -> String {
    const ESTIMATED_CAPACITY: usize = 256;
    let mut out = String::with_capacity(ESTIMATED_CAPACITY);

    let name = snapshot
        .name
        .as_ref()
        .map(|v| String::from_utf8_lossy(v).into_owned())
        .unwrap_or_default();
    let mut flags = snapshot.flags.to_vec();
    if blocked && !flags.contains(&b'b') {
        flags.push(b'b');
    }
    let flags = String::from_utf8_lossy(&flags).into_owned();
    let cmd = String::from_utf8_lossy(&snapshot.cmd).to_ascii_lowercase();
    let user = String::from_utf8_lossy(&snapshot.user).into_owned();

    let _ = write!(
        out,
        "id={} addr={} laddr={} fd=-1 name={} age={} idle={} flags={} db={} sub={} psub={} ssub={} multi={} qbuf=0 qbuf-free=0 argv-mem=0 multi-mem=0 rbs=0 rbp=0 obl=0 oll=0 omem=0 tot-mem=0 events=r cmd={} user={} lib-name={} lib-ver={} redir={} resp={}",
        snapshot.id,
        String::from_utf8_lossy(&snapshot.addr),
        String::from_utf8_lossy(&snapshot.laddr),
        name,
        snapshot.age_seconds,
        snapshot.idle_seconds,
        flags,
        snapshot.db,
        snapshot.sub,
        snapshot.psub,
        snapshot.ssub,
        snapshot.multi,
        cmd,
        user,
        snapshot
            .lib_name
            .as_ref()
            .map(|v| String::from_utf8_lossy(v).into_owned())
            .unwrap_or_default(),
        snapshot
            .lib_ver
            .as_ref()
            .map(|v| String::from_utf8_lossy(v).into_owned())
            .unwrap_or_default(),
        snapshot.redir,
        snapshot.resp
    );
    out
}
