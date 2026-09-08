use std::sync::Arc;

use tokio::sync::Notify;

use super::*;
use crate::transport::{ConnInfo, SessionStream};

pub(super) struct SessionRuntime {
    pub(super) input: BytesMut,
    /// Bytes read into `input` while a blocking command was parked; the loop
    /// treats them as a completed read on its next iteration.
    pub(super) prefetched_input_bytes: usize,
    pub(super) output: Vec<u8>,
    pub(super) pending_input_bytes: u64,
    pub(super) pubsub_rx: tokio::sync::mpsc::Receiver<PubSubMessage>,
    pub(super) monitor_notifier: Arc<Notify>,
    pub(super) query_buffer_limit: usize,
    pub(super) output_buffer_flush_threshold: usize,
    pub(super) write_timeout: Duration,
    pub(super) addr: Bytes,
    pub(super) laddr: Bytes,
}

enum SessionLoopAction {
    Continue,
    Close,
    Read(usize),
}

pub(super) async fn initialize_session_runtime(
    info: &ConnInfo,
    server_state: &SharedServerState,
    client_id: i64,
    client_state: &ClientState,
) -> SessionRuntime {
    let addr = info.addr.clone();
    let laddr = info.laddr.clone();
    let (
        pubsub_rx,
        monitor_notifier,
        query_buffer_limit,
        output_buffer_flush_threshold,
        write_timeout,
    ) = {
        let mut server = server_state.meta.lock().await;
        server.stats.mark_client_connected();
        let rx = server.pubsub.register_client(client_id);
        let mn = server.register_monitor_notifier(client_id);
        let (qbl, obft, wt) = load_connection_runtime_config(server_state);
        (rx, mn, qbl, obft, wt)
    };

    refresh_client_snapshot(server_state, client_state, &addr, &laddr, false).await;

    SessionRuntime {
        input: BytesMut::with_capacity(4096),
        prefetched_input_bytes: 0,
        output: Vec::with_capacity(4096),
        pending_input_bytes: 0,
        pubsub_rx,
        monitor_notifier,
        query_buffer_limit,
        output_buffer_flush_threshold,
        write_timeout,
        addr,
        laddr,
    }
}

async fn wait_for_session_activity<S: SessionStream>(
    stream: &mut S,
    client_state: &ClientState,
    runtime: &mut SessionRuntime,
    io_limits: ClientIoLimits,
) -> io::Result<SessionLoopAction> {
    let client_accepts_async_push = client_state.has_pubsub_subscriptions()
        || client_state.tracking_enabled()
        || client_state.is_monitor();

    let wait_result = if io_limits.client_read_timeout_sec > 0 && !client_accepts_async_push {
        let idle_duration = Duration::from_secs(io_limits.client_read_timeout_sec);
        tokio::select! {
            msg = runtime.pubsub_rx.recv() => match msg {
                Some(message) => Ok(WaitResult::PubSubMsg(message)),
                None => Ok(WaitResult::PubSubClosed),
            },
            _ = runtime.monitor_notifier.notified() => Ok(WaitResult::MonitorWake),
            result = timeout(idle_duration, stream.read_buf(&mut runtime.input)) => match result {
                Ok(result) => result.map(WaitResult::NetworkRead),
                Err(_) => {
                    tracing::debug!(
                        client_id = client_state.id(),
                        timeout_sec = io_limits.client_read_timeout_sec,
                        "disconnecting idle client: read timeout"
                    );
                    return Ok(SessionLoopAction::Close);
                }
            }
        }?
    } else {
        wait_for_async_push_or_input(
            stream,
            &mut runtime.input,
            &mut runtime.pubsub_rx,
            runtime.monitor_notifier.as_ref(),
        )
        .await?
    };

    match wait_result {
        WaitResult::PubSubMsg(message) => {
            if !encode_pubsub_message(
                message,
                &mut runtime.output,
                io_limits.output_buffer_limit_bytes,
                client_state.protocol_version(),
            ) {
                let response = encode(&RespFrame::error_str(OUTPUT_BUFFER_LIMIT_ERR));
                write_all_with_timeout(stream, &response, runtime.write_timeout).await?;
                return Ok(SessionLoopAction::Close);
            }
            write_all_with_timeout(stream, &runtime.output, runtime.write_timeout).await?;
            runtime.output.clear();
            Ok(SessionLoopAction::Continue)
        }
        WaitResult::PubSubClosed => {
            tracing::warn!(
                client_id = client_state.id(),
                "disconnecting pubsub client: push channel closed (overflow)"
            );
            let response = encode(&RespFrame::error_str(
                "ERR pubsub pending output buffer limit exceeded",
            ));
            write_all_with_timeout(stream, &response, runtime.write_timeout).await?;
            Ok(SessionLoopAction::Close)
        }
        WaitResult::MonitorWake => Ok(SessionLoopAction::Continue),
        WaitResult::NetworkRead(0) => Ok(SessionLoopAction::Close),
        WaitResult::NetworkRead(read) => Ok(SessionLoopAction::Read(read)),
    }
}

#[allow(clippy::too_many_arguments)]
pub(super) async fn run_client_session_loop<S: SessionStream>(
    stream: &mut S,
    server_state: &SharedServerState,
    persistence: &Arc<PersistenceRuntime>,
    client_state: &mut ClientState,
    runtime: &mut SessionRuntime,
    io_limits: ClientIoLimits,
) -> io::Result<()> {
    loop {
        if drain_preloop_async_output(
            stream,
            server_state,
            client_state,
            &mut runtime.pubsub_rx,
            &mut runtime.output,
            io_limits.output_buffer_limit_bytes,
            runtime.write_timeout,
        )
        .await?
        {
            return Ok(());
        }

        let read = if runtime.prefetched_input_bytes > 0 {
            std::mem::take(&mut runtime.prefetched_input_bytes)
        } else {
            match wait_for_session_activity(stream, client_state, runtime, io_limits).await? {
                SessionLoopAction::Continue => continue,
                SessionLoopAction::Close => return Ok(()),
                SessionLoopAction::Read(read) => read,
            }
        };

        server_state.stats.add_net_input_bytes(read as u64);
        runtime.pending_input_bytes = runtime.pending_input_bytes.saturating_add(read as u64);

        let Some(parsed_frames) = collect_parsed_frames(
            stream,
            server_state,
            client_state,
            &mut runtime.input,
            &mut runtime.pending_input_bytes,
            runtime.query_buffer_limit,
            runtime.write_timeout,
        )
        .await?
        else {
            return Ok(());
        };

        let outcomes = execute_client_pipeline(
            parsed_frames,
            server_state,
            persistence,
            client_state,
            stream,
            &mut runtime.input,
            runtime.query_buffer_limit,
            &mut runtime.prefetched_input_bytes,
            &runtime.addr,
            &runtime.laddr,
        )
        .await?;

        let Some(should_close) = apply_command_outcomes(
            outcomes,
            stream,
            server_state,
            client_state,
            &mut runtime.output,
            io_limits.output_buffer_limit_bytes,
            &mut runtime.output_buffer_flush_threshold,
            &mut runtime.query_buffer_limit,
            &mut runtime.write_timeout,
        )
        .await?
        else {
            return Ok(());
        };

        if drain_post_command_async_output(
            stream,
            server_state,
            client_state,
            &mut runtime.pubsub_rx,
            &mut runtime.output,
            io_limits.output_buffer_limit_bytes,
            runtime.write_timeout,
        )
        .await?
        {
            return Ok(());
        }

        refresh_client_snapshot(
            server_state,
            client_state,
            &runtime.addr,
            &runtime.laddr,
            false,
        )
        .await;

        if should_close {
            return Ok(());
        }
    }
}
