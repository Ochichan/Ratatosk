use super::shared_support::{append_encoded_frame, flush_pending_input_bytes};
use super::*;

pub(super) struct ProtocolCommandOutcome {
    outcome: CommandOutcome,
    protocol_version: i64,
}

async fn write_output_limit_error(
    stream: &mut TcpStream,
    write_timeout: Duration,
) -> io::Result<()> {
    let response = encode(&RespFrame::error_str(OUTPUT_BUFFER_LIMIT_ERR));
    write_all_with_timeout(stream, &response, write_timeout).await
}

async fn flush_output_buffer(
    stream: &mut TcpStream,
    server_state: &SharedServerState,
    output: &mut Vec<u8>,
    write_timeout: Duration,
) -> io::Result<()> {
    if output.is_empty() {
        return Ok(());
    }

    let out_len = output.len() as u64;
    write_all_with_timeout(stream, output, write_timeout).await?;
    output.clear();
    server_state.stats.add_net_output_bytes(out_len);
    let mut server = server_state.meta.lock().await;
    server.stats.add_net_output_bytes(out_len);
    Ok(())
}

#[allow(clippy::too_many_arguments)]
pub(super) async fn drain_preloop_async_output(
    stream: &mut TcpStream,
    server_state: &SharedServerState,
    client_state: &ClientState,
    pubsub_rx: &mut tokio::sync::mpsc::Receiver<PubSubMessage>,
    output: &mut Vec<u8>,
    output_limit_bytes: usize,
    write_timeout: Duration,
) -> io::Result<bool> {
    let mut had_pubsub = false;
    while let Ok(msg) = pubsub_rx.try_recv() {
        had_pubsub = true;
        if !encode_pubsub_message(
            msg,
            output,
            output_limit_bytes,
            client_state.protocol_version(),
        ) {
            tracing::warn!(
                client_id = client_state.id(),
                output_limit_bytes = output_limit_bytes,
                "disconnecting client: pubsub output frame exceeded buffer limit"
            );
            write_output_limit_error(stream, write_timeout).await?;
            return Ok(true);
        }
    }
    if had_pubsub {
        flush_output_buffer(stream, server_state, output, write_timeout).await?;
    }

    let monitor_pending = {
        let mut server = server_state.meta.lock().await;
        server.drain_monitor_messages(client_state.id())
    };
    if !monitor_pending.is_empty() {
        for line in monitor_pending {
            let frame = RespFrame::SimpleString(line);
            if !append_encoded_frame(
                output,
                &frame,
                output_limit_bytes,
                client_state.protocol_version(),
            ) {
                tracing::warn!(
                    client_id = client_state.id(),
                    output_limit_bytes = output_limit_bytes,
                    "disconnecting monitor client: output buffer limit exceeded"
                );
                write_output_limit_error(stream, write_timeout).await?;
                return Ok(true);
            }
        }
        flush_output_buffer(stream, server_state, output, write_timeout).await?;
    }

    Ok(false)
}

#[allow(clippy::too_many_arguments)]
pub(super) async fn collect_parsed_frames(
    stream: &mut TcpStream,
    server_state: &SharedServerState,
    client_state: &ClientState,
    input: &mut BytesMut,
    pending_input_bytes: &mut u64,
    query_buffer_limit: usize,
    write_timeout: Duration,
) -> io::Result<Option<Vec<RespFrame>>> {
    if input.len() > query_buffer_limit {
        flush_pending_input_bytes(server_state, pending_input_bytes).await;
        let frame = RespFrame::error_str("ERR query buffer limit exceeded");
        write_all_with_timeout(stream, &encode(&frame), write_timeout).await?;
        return Ok(None);
    }

    let mut parsed_frames = Vec::new();
    loop {
        match parse(input) {
            Ok(Some(frame)) => parsed_frames.push(frame),
            Ok(None) => break,
            Err(error) => {
                flush_pending_input_bytes(server_state, pending_input_bytes).await;
                tracing::warn!(
                    target = "ratatosk::protocol",
                    client_id = client_state.id(),
                    input_len = input.len(),
                    error = %error,
                    "protocol parse error; closing client connection"
                );
                let response = encode(&RespFrame::error_str("ERR protocol error"));
                write_all_with_timeout(stream, &response, write_timeout).await?;
                return Ok(None);
            }
        }
    }

    flush_pending_input_bytes(server_state, pending_input_bytes).await;
    Ok(Some(parsed_frames))
}

#[allow(clippy::too_many_arguments)]
pub(super) async fn execute_client_pipeline(
    parsed_frames: Vec<RespFrame>,
    server_state: &SharedServerState,
    persistence: &Arc<PersistenceRuntime>,
    client_state: &mut ClientState,
    stream: &TcpStream,
    addr: &Bytes,
    laddr: &Bytes,
) -> io::Result<Vec<ProtocolCommandOutcome>> {
    match try_run_readonly_batch(parsed_frames, server_state, client_state).await {
        Ok(outcomes) => Ok(outcomes
            .into_iter()
            .map(|outcome| ProtocolCommandOutcome {
                outcome,
                protocol_version: client_state.protocol_version(),
            })
            .collect()),
        Err(frames) => {
            let mut outcomes = Vec::with_capacity(frames.len());
            for frame in frames {
                let outcome = run_with_blocking_retry(
                    frame,
                    server_state,
                    persistence,
                    client_state,
                    stream,
                    addr,
                    laddr,
                )
                .await
                .map_err(|error| {
                    io::Error::new(
                        error.kind(),
                        format!(
                            "executing command pipeline for client_id={}: {}",
                            client_state.id(),
                            error
                        ),
                    )
                })?;
                outcomes.push(ProtocolCommandOutcome {
                    outcome,
                    protocol_version: client_state.protocol_version(),
                });
            }
            Ok(outcomes)
        }
    }
}

#[allow(clippy::too_many_arguments)]
pub(super) async fn apply_command_outcomes(
    outcomes: Vec<ProtocolCommandOutcome>,
    stream: &mut TcpStream,
    server_state: &SharedServerState,
    client_state: &mut ClientState,
    output: &mut Vec<u8>,
    output_limit_bytes: usize,
    output_buffer_flush_threshold: &mut usize,
    query_buffer_limit: &mut usize,
    write_timeout: &mut Duration,
) -> io::Result<Option<bool>> {
    let mut should_close = false;
    for ProtocolCommandOutcome {
        outcome,
        protocol_version,
    } in outcomes
    {
        if outcome.config_dirty {
            reload_connection_runtime_config(
                server_state,
                query_buffer_limit,
                output_buffer_flush_threshold,
                write_timeout,
            );
        }

        if let Some(delay_ms) = outcome.delay_ms {
            tokio::time::sleep(std::time::Duration::from_millis(delay_ms)).await;
        }

        let reply_mode = client_state.reply_mode().clone();
        let suppress = match reply_mode.as_ref() {
            b"off" => true,
            b"skip" => {
                client_state.set_reply_mode_on();
                true
            }
            _ => false,
        };

        if !suppress {
            if !append_encoded_frame(
                output,
                &outcome.response,
                output_limit_bytes,
                protocol_version,
            ) {
                tracing::warn!(
                    client_id = client_state.id(),
                    output_limit_bytes = output_limit_bytes,
                    "disconnecting client: command response exceeded output buffer limit"
                );
                write_output_limit_error(stream, *write_timeout).await?;
                return Ok(None);
            }

            if output.len() >= *output_buffer_flush_threshold {
                flush_output_buffer(stream, server_state, output, *write_timeout).await?;
            }
        }

        if outcome.close {
            should_close = true;
            break;
        }
    }

    Ok(Some(should_close))
}

#[allow(clippy::too_many_arguments)]
pub(super) async fn drain_post_command_async_output(
    stream: &mut TcpStream,
    server_state: &SharedServerState,
    client_state: &ClientState,
    pubsub_rx: &mut tokio::sync::mpsc::Receiver<PubSubMessage>,
    output: &mut Vec<u8>,
    output_limit_bytes: usize,
    write_timeout: Duration,
) -> io::Result<bool> {
    let should_poll_pubsub =
        client_state.has_pubsub_subscriptions() || client_state.tracking_enabled();
    if should_poll_pubsub {
        while let Ok(msg) = pubsub_rx.try_recv() {
            if !encode_pubsub_message(
                msg,
                output,
                output_limit_bytes,
                client_state.protocol_version(),
            ) {
                tracing::warn!(
                    client_id = client_state.id(),
                    output_limit_bytes = output_limit_bytes,
                    "disconnecting client: pubsub output frame exceeded buffer limit"
                );
                write_output_limit_error(stream, write_timeout).await?;
                return Ok(true);
            }
        }
    }

    if client_state.is_monitor() {
        let monitor_msgs = {
            let mut server = server_state.meta.lock().await;
            server.drain_monitor_messages(client_state.id())
        };
        for line in monitor_msgs {
            let frame = RespFrame::SimpleString(line);
            if !append_encoded_frame(
                output,
                &frame,
                output_limit_bytes,
                client_state.protocol_version(),
            ) {
                tracing::warn!(
                    client_id = client_state.id(),
                    output_limit_bytes = output_limit_bytes,
                    "disconnecting monitor client: output buffer limit exceeded"
                );
                write_output_limit_error(stream, write_timeout).await?;
                return Ok(true);
            }
        }
    }

    flush_output_buffer(stream, server_state, output, write_timeout).await?;
    Ok(false)
}
