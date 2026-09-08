use std::io;

use bytes::Bytes;
use ratatosk_engine::command::ClientState;
use ratatosk_resp::{RespFrame, RespVersion, encode_to_vec_for_version, encoded_len_for_version};

use crate::{metrics, transport::ConnInfo};

use super::SharedServerState;

fn is_benign_disconnect(error: &io::Error) -> bool {
    matches!(
        error.kind(),
        io::ErrorKind::BrokenPipe
            | io::ErrorKind::ConnectionReset
            | io::ErrorKind::ConnectionAborted
            | io::ErrorKind::TimedOut
            | io::ErrorKind::UnexpectedEof
    )
}

pub(super) fn disconnect_reason_for_result(result: &io::Result<()>) -> &'static str {
    match result {
        Ok(()) => "closed",
        Err(error) if is_benign_disconnect(error) => "client_disconnect",
        Err(_) => "error",
    }
}

pub(super) fn register_client_connection(
    info: &ConnInfo,
    server_state: &SharedServerState,
) -> (i64, String) {
    let client_id = server_state.alloc_client_id();
    server_state.stats.mark_client_connected();
    metrics::set_active_connections(server_state.stats.connected_clients() as usize);
    metrics::record_connection_event("accepted");

    (client_id, info.remote_display.clone())
}

pub(super) async fn finish_client_connection(
    server_state: &SharedServerState,
    client_id: i64,
    disconnect_reason: &str,
) {
    {
        let mut server = server_state.meta.lock().await;
        server.stats.mark_client_disconnected();
        server.pubsub.remove_client(client_id);
        server.replication_remove_client(client_id);
        server.tracking_remove_client(client_id);
        server.unregister_monitor(client_id);
        server.remove_client_snapshot(client_id);
    }

    server_state.stats.mark_client_disconnected();
    metrics::set_active_connections(server_state.stats.connected_clients() as usize);
    metrics::record_connection_event(disconnect_reason);
}

pub(super) async fn refresh_client_snapshot(
    server_state: &SharedServerState,
    client_state: &ClientState,
    addr: &Bytes,
    laddr: &Bytes,
    blocked: bool,
) {
    let mut server = server_state.meta.lock().await;
    server.stats.catch_up_from_atomic(&server_state.stats);
    server.upsert_client_snapshot(client_state.snapshot_with_redirect(
        addr.clone(),
        laddr.clone(),
        client_state.tracking_redirect(),
    ));
    server.set_client_blocked(client_state.id(), blocked);
}

pub(super) async fn flush_pending_input_bytes(
    server_state: &SharedServerState,
    pending_input_bytes: &mut u64,
) {
    if *pending_input_bytes == 0 {
        return;
    }

    let bytes = std::mem::take(pending_input_bytes);
    let mut server = server_state.meta.lock().await;
    server.stats.add_net_input_bytes(bytes);
}

pub(super) fn append_encoded_frame(
    output: &mut Vec<u8>,
    frame: &RespFrame,
    output_limit_bytes: usize,
    protocol_version: i64,
) -> bool {
    let version = RespVersion::from_protocol_version(protocol_version);
    let frame_len = encoded_len_for_version(frame, version);
    if output.len().saturating_add(frame_len) > output_limit_bytes {
        return false;
    }

    output.reserve(frame_len);
    encode_to_vec_for_version(frame, output, version);
    true
}

pub(super) fn frame_to_argv_for_persistence(frame: &RespFrame) -> Option<Vec<Bytes>> {
    let RespFrame::Array(items) = frame else {
        return None;
    };

    let mut out = Vec::with_capacity(items.len());
    for item in items {
        match item {
            RespFrame::BulkString(Some(value)) => out.push(value.clone()),
            RespFrame::SimpleString(value) => out.push(value.clone()),
            RespFrame::Integer(value) => out.push(Bytes::from(value.to_string())),
            RespFrame::BulkString(None) => return None,
            _ => return None,
        }
    }

    Some(out)
}

#[cfg(test)]
mod tests {
    use ratatosk_resp::{RespVersion, encode_for_version};

    use super::*;

    #[test]
    fn output_limit_uses_the_projected_resp2_wire_length() {
        let frame = RespFrame::Map(
            (0..7)
                .map(|index| {
                    (
                        RespFrame::bulk_str(&format!("key{index}")),
                        RespFrame::Integer(index),
                    )
                })
                .collect(),
        );
        let expected = encode_for_version(&frame, RespVersion::Resp2);
        let mut output = Vec::new();

        assert!(
            !append_encoded_frame(&mut output, &frame, expected.len() - 1, 2),
            "the RESP2 map projection must not bypass the output limit"
        );
        assert!(output.is_empty());

        assert!(append_encoded_frame(&mut output, &frame, expected.len(), 2));
        assert_eq!(output.as_slice(), expected.as_ref());
    }
}
