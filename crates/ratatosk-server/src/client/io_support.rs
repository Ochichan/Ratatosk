use super::shared_support::append_encoded_frame;
use super::*;
use crate::transport::SessionStream;

/// Result of the select-based I/O wait: either a pubsub message arrived,
/// a monitor notification arrived, or network data was read.
pub(super) enum WaitResult {
    PubSubMsg(PubSubMessage),
    PubSubClosed,
    MonitorWake,
    NetworkRead(usize),
}

pub(super) async fn wait_for_async_push_or_input<S: SessionStream>(
    stream: &mut S,
    input: &mut BytesMut,
    pubsub_rx: &mut tokio::sync::mpsc::Receiver<PubSubMessage>,
    monitor_notifier: &Notify,
) -> io::Result<WaitResult> {
    tokio::select! {
        msg = pubsub_rx.recv() => match msg {
            Some(m) => Ok(WaitResult::PubSubMsg(m)),
            None => Ok(WaitResult::PubSubClosed),
        },
        _ = monitor_notifier.notified() => Ok(WaitResult::MonitorWake),
        result = stream.read_buf(input) => result.map(WaitResult::NetworkRead),
    }
}

pub(super) async fn write_all_with_timeout<S: SessionStream>(
    stream: &mut S,
    payload: &[u8],
    write_timeout: Duration,
) -> io::Result<()> {
    if payload.is_empty() {
        return Ok(());
    }

    match timeout(write_timeout, stream.write_all(payload)).await {
        Ok(result) => result,
        Err(_) => Err(io::Error::new(
            io::ErrorKind::TimedOut,
            "socket write timeout",
        )),
    }
}

pub(super) fn load_connection_runtime_config(
    server_state: &SharedServerState,
) -> (usize, usize, Duration) {
    let config = server_state.config_cache.load();
    (
        config.query_buffer_limit(),
        config.output_buffer_flush_threshold(),
        Duration::from_secs(config.client_write_timeout_sec()),
    )
}

pub(super) fn reload_connection_runtime_config(
    server_state: &SharedServerState,
    query_buffer_limit: &mut usize,
    output_buffer_flush_threshold: &mut usize,
    write_timeout: &mut Duration,
) {
    let (query_limit, output_threshold, timeout) = load_connection_runtime_config(server_state);
    *query_buffer_limit = query_limit;
    *output_buffer_flush_threshold = output_threshold;
    *write_timeout = timeout;
}

/// Encode a single PubSubMessage into the output buffer.
/// Returns `false` if the output buffer would exceed the limit.
pub(super) fn encode_pubsub_message(
    message: PubSubMessage,
    output: &mut Vec<u8>,
    output_limit_bytes: usize,
    protocol_version: i64,
) -> bool {
    encode_pubsub_messages(vec![message], output, output_limit_bytes, protocol_version)
}

fn encode_pubsub_messages(
    messages: Vec<PubSubMessage>,
    output: &mut Vec<u8>,
    output_limit_bytes: usize,
    protocol_version: i64,
) -> bool {
    for message in messages {
        let frame = match message {
            PubSubMessage::Message { channel, payload } => {
                let inner = vec![
                    RespFrame::bulk_str("message"),
                    RespFrame::BulkString(Some(channel)),
                    RespFrame::BulkString(Some(payload)),
                ];
                RespFrame::Push(inner)
            }
            PubSubMessage::SMessage { channel, payload } => {
                let inner = vec![
                    RespFrame::bulk_str("smessage"),
                    RespFrame::BulkString(Some(channel)),
                    RespFrame::BulkString(Some(payload)),
                ];
                RespFrame::Push(inner)
            }
            PubSubMessage::PMessage {
                pattern,
                channel,
                payload,
            } => {
                let inner = vec![
                    RespFrame::bulk_str("pmessage"),
                    RespFrame::BulkString(Some(pattern)),
                    RespFrame::BulkString(Some(channel)),
                    RespFrame::BulkString(Some(payload)),
                ];
                RespFrame::Push(inner)
            }
            PubSubMessage::Invalidate { keys } => {
                let inner = vec![
                    RespFrame::bulk_str("invalidate"),
                    RespFrame::Array(
                        keys.into_iter()
                            .map(|key| RespFrame::BulkString(Some(key)))
                            .collect(),
                    ),
                ];
                RespFrame::Push(inner)
            }
            PubSubMessage::TrackingRedirectBroken { redirect_client_id } => RespFrame::Push(vec![
                RespFrame::bulk_str("tracking-redir-broken"),
                RespFrame::Integer(redirect_client_id),
            ]),
        };
        if !append_encoded_frame(output, &frame, output_limit_bytes, protocol_version) {
            return false;
        }
    }

    true
}
