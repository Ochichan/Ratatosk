use std::{io, sync::Arc, time::Duration};

use bytes::{Bytes, BytesMut};
use ratatosk_engine::{
    command::{ClientState, CommandOutcome, execute, is_write_command},
    keyspace::{PubSubMessage, ServerState},
};
use ratatosk_resp::{RespFrame, encode, encode_to_vec, encoded_len, parse};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
    sync::Mutex,
    time::timeout,
};

use crate::breadcrumbs;
use crate::config::DEFAULT_OUTPUT_BUFFER_LIMIT_BYTES;
use crate::metrics;
use crate::persistence::{PersistenceRuntime, run_save, start_bgsave};

const QUERY_BUFFER_LIMIT: usize = 1024 * 1024;
const OUTPUT_BUFFER_FLUSH_THRESHOLD: usize = 16 * 1024;
const PUBSUB_POLL_INTERVAL: Duration = Duration::from_millis(20);
const WRITE_TIMEOUT: Duration = Duration::from_secs(5);
const BLOCKING_RETRY_BACKOFF_INITIAL: Duration = Duration::from_millis(10);
const BLOCKING_RETRY_BACKOFF_MAX: Duration = Duration::from_millis(200);
const AOF_LOCK_ACQUIRE_TIMEOUT: Duration = Duration::from_secs(2);
const AOF_APPEND_SLOW_THRESHOLD: Duration = Duration::from_secs(3);
const OUTPUT_BUFFER_LIMIT_ERR: &str = "ERR output buffer limit exceeded";

pub type SharedServerState = Arc<Mutex<ServerState>>;

/// Check if an error represents an expected client disconnect (not a server error).
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

#[derive(Debug, Clone, Copy)]
pub struct ClientIoLimits {
    pub output_buffer_limit_bytes: usize,
    pub client_read_timeout_sec: u64,
}

impl Default for ClientIoLimits {
    fn default() -> Self {
        Self {
            output_buffer_limit_bytes: DEFAULT_OUTPUT_BUFFER_LIMIT_BYTES,
            client_read_timeout_sec: 0,
        }
    }
}

fn append_encoded_frame(
    output: &mut Vec<u8>,
    frame: &RespFrame,
    output_limit_bytes: usize,
) -> bool {
    let frame_len = encoded_len(frame);
    if output.len().saturating_add(frame_len) > output_limit_bytes {
        return false;
    }

    output.reserve(frame_len);
    encode_to_vec(frame, output);
    true
}

fn next_retry_backoff(current: Duration) -> Duration {
    current
        .checked_mul(2)
        .unwrap_or(BLOCKING_RETRY_BACKOFF_MAX)
        .min(BLOCKING_RETRY_BACKOFF_MAX)
}

async fn wait_for_disconnect_or_timeout(
    stream: &TcpStream,
    wait_for: Duration,
) -> io::Result<bool> {
    if wait_for.is_zero() {
        return Ok(false);
    }

    match timeout(wait_for, stream.readable()).await {
        Err(_) => Ok(false),
        Ok(Ok(())) => {
            let mut probe = [0u8; 1];
            match stream.peek(&mut probe).await {
                Ok(0) => Ok(true),
                Ok(_) => Ok(false),
                Err(error)
                    if matches!(
                        error.kind(),
                        io::ErrorKind::WouldBlock | io::ErrorKind::Interrupted
                    ) =>
                {
                    Ok(false)
                }
                Err(error) => Err(error),
            }
        }
        Ok(Err(error))
            if matches!(
                error.kind(),
                io::ErrorKind::WouldBlock | io::ErrorKind::Interrupted
            ) =>
        {
            Ok(false)
        }
        Ok(Err(error)) => Err(error),
    }
}

async fn write_all_with_timeout(stream: &mut TcpStream, payload: &[u8]) -> io::Result<()> {
    if payload.is_empty() {
        return Ok(());
    }

    match timeout(WRITE_TIMEOUT, stream.write_all(payload)).await {
        Ok(result) => result,
        Err(_) => Err(io::Error::new(
            io::ErrorKind::TimedOut,
            "socket write timeout",
        )),
    }
}

async fn run_with_blocking_retry(
    frame: RespFrame,
    server_state: &SharedServerState,
    persistence: &Arc<PersistenceRuntime>,
    client_state: &mut ClientState,
    stream: &TcpStream,
) -> io::Result<CommandOutcome> {
    let start = std::time::Instant::now();
    let first_argv = frame_to_argv_for_persistence(&frame);
    let first_db = client_state.selected_db();

    // Extract command name for metrics
    let command_name = first_argv
        .as_ref()
        .and_then(|argv| {
            argv.first()
                .map(|cmd| String::from_utf8_lossy(cmd).to_ascii_uppercase())
        })
        .unwrap_or_else(|| "UNKNOWN".to_string());

    breadcrumbs::record_command(
        client_state.id(),
        &command_name,
        first_db,
        0,
        "execute",
    );

    let mut outcome = {
        let mut server = server_state.lock().await;
        execute(frame, &mut server, client_state)
    };

    // Record command metrics
    let duration = start.elapsed();
    let success = !matches!(outcome.response, ratatosk_resp::RespFrame::Error(_));
    metrics::record_command(&command_name, success, duration.as_secs_f64());

    // Log slow commands (> 1ms)
    if duration.as_millis() > 1 {
        tracing::debug!(
            target = "ratatosk::slow_command",
            command = %command_name,
            duration_ms = duration.as_micros() as f64 / 1000.0,
            "slow command detected"
        );
    }

    let Some(retry) = outcome.retry_blocking.clone() else {
        apply_post_execute_persistence(
            server_state,
            persistence,
            first_db,
            first_argv,
            &mut outcome,
        )
        .await;
        return Ok(outcome);
    };

    let deadline_ms = retry.deadline_ms;
    let mut frame = retry.frame;
    let mut last_response = outcome.response;
    let mut backoff = BLOCKING_RETRY_BACKOFF_INITIAL;
    let mut retry_attempts = 0u64;

    loop {
        let now_ms = ratatosk_core::time::monotonic_ms();
        if let Some(deadline) = deadline_ms {
            let deadline_u64 = u64::try_from(deadline).unwrap_or(u64::MAX);
            if now_ms >= deadline_u64 {
                metrics::record_blocking_retry_deadline_exhausted(&command_name);
                metrics::record_blocking_retry_completed(&command_name, retry_attempts);
                return Ok(CommandOutcome {
                    response: last_response,
                    close: false,
                    retry_blocking: None,
                });
            }
        }

        let wait_for = if let Some(deadline) = deadline_ms {
            let deadline_u64 = u64::try_from(deadline).unwrap_or(u64::MAX);
            let remaining_ms = deadline_u64.saturating_sub(now_ms);
            Duration::from_millis(remaining_ms).min(backoff)
        } else {
            backoff
        };

        retry_attempts = retry_attempts.saturating_add(1);
        breadcrumbs::record_command(
            client_state.id(),
            &command_name,
            client_state.selected_db(),
            retry_attempts,
            "retry",
        );
        metrics::record_blocking_retry_iteration(&command_name);
        metrics::record_blocking_retry_wait_ms(&command_name, wait_for.as_millis() as f64);

        if wait_for_disconnect_or_timeout(stream, wait_for)
            .await
            .map_err(|error| {
                io::Error::new(
                    error.kind(),
                    format!(
                        "waiting for blocking command retry readiness (command={}, attempt={}): {}",
                        command_name, retry_attempts, error
                    ),
                )
            })?
        {
            return Err(io::Error::new(
                io::ErrorKind::ConnectionReset,
                "client disconnected while waiting for blocking command",
            ));
        }

        let outcome = {
            let argv = frame_to_argv_for_persistence(&frame);
            let db = client_state.selected_db();
            let mut server = server_state.lock().await;
            let mut outcome = execute(frame, &mut server, client_state);
            if outcome.retry_blocking.is_none() {
                drop(server);
                apply_post_execute_persistence(server_state, persistence, db, argv, &mut outcome)
                    .await;
            }
            outcome
        };

        let Some(retry) = outcome.retry_blocking else {
            metrics::record_blocking_retry_completed(&command_name, retry_attempts);
            return Ok(outcome);
        };
        last_response = outcome.response;
        frame = retry.frame;
        backoff = next_retry_backoff(backoff);
    }
}

fn frame_to_argv_for_persistence(frame: &RespFrame) -> Option<Vec<Bytes>> {
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

fn is_queued_response(frame: &RespFrame) -> bool {
    match frame {
        RespFrame::SimpleString(text) => text.eq_ignore_ascii_case(b"QUEUED"),
        _ => false,
    }
}

async fn apply_post_execute_persistence(
    server_state: &SharedServerState,
    persistence: &Arc<PersistenceRuntime>,
    selected_db: usize,
    argv: Option<Vec<Bytes>>,
    outcome: &mut CommandOutcome,
) {
    let Some(argv) = argv else {
        return;
    };
    if argv.is_empty() || matches!(outcome.response, RespFrame::Error(_)) {
        return;
    }

    let command = argv[0].to_ascii_uppercase();

    if command == b"SAVE" && !matches!(outcome.response, RespFrame::Error(_)) {
        if let Err(error) = run_save(server_state, &persistence.rdb_path).await {
            outcome.response = RespFrame::error_str(&format!("ERR SAVE failed: {error}"));
        }
        return;
    }

    if command == b"BGSAVE" && !matches!(outcome.response, RespFrame::Error(_)) {
        if !start_bgsave(Arc::clone(server_state), persistence.rdb_path.clone()).await {
            outcome.response = RespFrame::error_str("ERR Background save already in progress");
        }
        return;
    }

    if is_queued_response(&outcome.response) || !is_write_command(&argv) {
        return;
    }

    let Some(writer) = &persistence.aof_writer else {
        return;
    };

    let fsync_policy = {
        let server = server_state.lock().await;
        String::from_utf8_lossy(server.config.appendfsync()).to_string()
    };

    let lock_start = std::time::Instant::now();
    let mut writer = match timeout(AOF_LOCK_ACQUIRE_TIMEOUT, writer.lock()).await {
        Ok(guard) => {
            let wait_ms = lock_start.elapsed().as_secs_f64() * 1000.0;
            metrics::record_aof_lock_wait_ms(wait_ms);
            guard
        }
        Err(_) => {
            metrics::record_aof_append_timeout("lock");
            outcome.response = RespFrame::error_str(
                "ERR AOF append failed: timeout acquiring append lock",
            );
            tracing::error!(
                target = "ratatosk::aof",
                selected_db = selected_db,
                command = %String::from_utf8_lossy(&argv[0]),
                timeout_ms = AOF_LOCK_ACQUIRE_TIMEOUT.as_millis(),
                "AOF append lock acquisition timed out"
            );
            return;
        }
    };

    let append_start = std::time::Instant::now();
    match writer.append_command(selected_db, &argv) {
        Ok(()) => {
            let elapsed_ms = append_start.elapsed().as_secs_f64() * 1000.0;
            if append_start.elapsed() > AOF_APPEND_SLOW_THRESHOLD {
                metrics::record_aof_append_timeout("append_slow");
                tracing::warn!(
                    target = "ratatosk::aof",
                    selected_db = selected_db,
                    command = %String::from_utf8_lossy(&argv[0]),
                    elapsed_ms = elapsed_ms,
                    threshold_ms = AOF_APPEND_SLOW_THRESHOLD.as_millis(),
                    "AOF append exceeded slow threshold"
                );
            }
            metrics::record_aof_write(&fsync_policy);
            metrics::record_aof_append_duration_ms(elapsed_ms, "ok");
        }
        Err(error) => {
            let elapsed_ms = append_start.elapsed().as_secs_f64() * 1000.0;
            metrics::record_aof_append_duration_ms(elapsed_ms, "error");
            outcome.response = RespFrame::error_str(&format!("ERR AOF append failed: {error}"));
            tracing::error!(
                target = "ratatosk::aof",
                selected_db = selected_db,
                command = %String::from_utf8_lossy(&argv[0]),
                elapsed_ms = elapsed_ms,
                error = %error,
                "AOF append failed"
            );
        }
    }
}

fn encode_pubsub_messages(
    messages: Vec<PubSubMessage>,
    output: &mut Vec<u8>,
    output_limit_bytes: usize,
) -> bool {
    for message in messages {
        let frame = match message {
            PubSubMessage::Message { channel, payload } => RespFrame::Array(vec![
                RespFrame::bulk_str("message"),
                RespFrame::BulkString(Some(channel)),
                RespFrame::BulkString(Some(payload)),
            ]),
            PubSubMessage::SMessage { channel, payload } => RespFrame::Array(vec![
                RespFrame::bulk_str("smessage"),
                RespFrame::BulkString(Some(channel)),
                RespFrame::BulkString(Some(payload)),
            ]),
            PubSubMessage::PMessage {
                pattern,
                channel,
                payload,
            } => RespFrame::Array(vec![
                RespFrame::bulk_str("pmessage"),
                RespFrame::BulkString(Some(pattern)),
                RespFrame::BulkString(Some(channel)),
                RespFrame::BulkString(Some(payload)),
            ]),
        };
        if !append_encoded_frame(output, &frame, output_limit_bytes) {
            return false;
        }
    }

    true
}

pub async fn handle_client(stream: TcpStream, server_state: SharedServerState) -> io::Result<()> {
    let persistence = Arc::new(
        PersistenceRuntime::from_config(&crate::config::ServerConfig::default()).map_err(
            |error| {
                io::Error::new(
                    error.kind(),
                    format!("creating default persistence runtime for client handler: {error}"),
                )
            },
        )?,
    );
    handle_client_with_limits(stream, server_state, persistence, ClientIoLimits::default()).await
}

pub async fn handle_client_with_limits(
    stream: TcpStream,
    server_state: SharedServerState,
    persistence: Arc<PersistenceRuntime>,
    io_limits: ClientIoLimits,
) -> io::Result<()> {
    let (client_id, remote_addr) = {
        let mut server = server_state.lock().await;
        let id = server.alloc_client_id();
        server.stats.mark_client_connected();
        let active = server.stats.connected_clients();
        metrics::set_active_connections(active as usize);
        metrics::record_connection_event("accepted");
        let addr = stream
            .peer_addr()
            .map(|a| a.to_string())
            .unwrap_or_else(|_| "unknown".to_string());
        (id, addr)
    };

    // Create a tracing span for this client session
    let client_span = tracing::info_span!(
        "client_session",
        client_id = client_id,
        remote_addr = %remote_addr,
    );

    let _span_enter = client_span.enter();

    tracing::info!(
        target = "ratatosk::client",
        client_id = client_id,
        remote_addr = %remote_addr,
        "client connected"
    );

    let result = handle_client_inner(stream, &server_state, &persistence, client_id, io_limits)
        .await
        .map_err(|error| {
            io::Error::new(
                error.kind(),
                format!(
                    "handling client I/O (client_id={}, remote_addr={}): {}",
                    client_id, remote_addr, error
                ),
            )
        });

    let disconnect_reason = match &result {
        Ok(()) => "closed",
        Err(e) if is_benign_disconnect(e) => "client_disconnect",
        Err(_) => "error",
    };

    {
        let mut server = server_state.lock().await;
        server.pubsub.remove_client(client_id);
        server.stats.mark_client_disconnected();
        let active = server.stats.connected_clients();
        metrics::set_active_connections(active as usize);
        metrics::record_connection_event(disconnect_reason);
    }

    tracing::info!(
        target = "ratatosk::client",
        client_id = client_id,
        reason = disconnect_reason,
        "client disconnected"
    );

    result
}

async fn handle_client_inner(
    mut stream: TcpStream,
    server_state: &SharedServerState,
    persistence: &Arc<PersistenceRuntime>,
    client_id: i64,
    io_limits: ClientIoLimits,
) -> io::Result<()> {
    if let Err(error) = stream.set_nodelay(true) {
        tracing::debug!(error = %error, "failed to enable TCP_NODELAY");
    }

    let mut input = BytesMut::with_capacity(4096);
    let mut output = Vec::with_capacity(4096);
    let mut client_state = ClientState::new(client_id);
    let mut poll_pubsub_pending = false;

    loop {
        let client_has_subscriptions = client_state.has_pubsub_subscriptions();
        let (pending_overflow, pending) = if client_has_subscriptions || poll_pubsub_pending {
            let mut server = server_state.lock().await;
            (
                server.pubsub.take_overflowed_client(client_state.id()),
                server.pubsub.drain_messages(client_state.id()),
            )
        } else {
            (false, Vec::new())
        };
        poll_pubsub_pending = client_has_subscriptions || pending_overflow || !pending.is_empty();
        if pending_overflow {
            tracing::warn!(
                client_id = client_state.id(),
                "disconnecting pubsub client: pending output buffer limit exceeded"
            );
            let response = encode(&RespFrame::error_str(
                "ERR pubsub pending output buffer limit exceeded",
            ));
            write_all_with_timeout(&mut stream, &response).await?;
            return Ok(());
        }

        if !pending.is_empty() {
            if !encode_pubsub_messages(pending, &mut output, io_limits.output_buffer_limit_bytes) {
                tracing::warn!(
                    client_id = client_state.id(),
                    output_limit_bytes = io_limits.output_buffer_limit_bytes,
                    "disconnecting client: pubsub output frame exceeded buffer limit"
                );
                let response = encode(&RespFrame::error_str(OUTPUT_BUFFER_LIMIT_ERR));
                write_all_with_timeout(&mut stream, &response).await?;
                return Ok(());
            }
            write_all_with_timeout(&mut stream, &output).await?;
            output.clear();
        }

        let read = if client_has_subscriptions {
            match timeout(PUBSUB_POLL_INTERVAL, stream.read_buf(&mut input)).await {
                Ok(result) => result?,
                Err(_) => continue,
            }
        } else if io_limits.client_read_timeout_sec > 0 {
            let idle_duration = Duration::from_secs(io_limits.client_read_timeout_sec);
            match timeout(idle_duration, stream.read_buf(&mut input)).await {
                Ok(result) => result?,
                Err(_) => {
                    tracing::info!(
                        client_id = client_id,
                        timeout_sec = io_limits.client_read_timeout_sec,
                        "disconnecting idle client: read timeout"
                    );
                    return Ok(());
                }
            }
        } else {
            stream.read_buf(&mut input).await?
        };

        if read == 0 {
            return Ok(());
        }

        {
            let mut server = server_state.lock().await;
            server.stats.add_net_input_bytes(read as u64);
        }

        if input.len() > QUERY_BUFFER_LIMIT {
            let frame = RespFrame::error_str("ERR query buffer limit exceeded");
            write_all_with_timeout(&mut stream, &encode(&frame)).await?;
            return Ok(());
        }

        let mut should_close = false;
        loop {
            let frame = match parse(&mut input) {
                Ok(Some(frame)) => frame,
                Ok(None) => break,
                Err(_) => {
                    let response = encode(&RespFrame::error_str("ERR protocol error"));
                    write_all_with_timeout(&mut stream, &response).await?;
                    return Ok(());
                }
            };

            let outcome = run_with_blocking_retry(
                frame,
                server_state,
                persistence,
                &mut client_state,
                &stream,
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
            if !append_encoded_frame(
                &mut output,
                &outcome.response,
                io_limits.output_buffer_limit_bytes,
            ) {
                tracing::warn!(
                    client_id = client_state.id(),
                    output_limit_bytes = io_limits.output_buffer_limit_bytes,
                    "disconnecting client: command response exceeded output buffer limit"
                );
                let response = encode(&RespFrame::error_str(OUTPUT_BUFFER_LIMIT_ERR));
                write_all_with_timeout(&mut stream, &response).await?;
                return Ok(());
            }

            if output.len() >= OUTPUT_BUFFER_FLUSH_THRESHOLD {
                let out_len = output.len() as u64;
                write_all_with_timeout(&mut stream, &output).await?;
                output.clear();
                let mut server = server_state.lock().await;
                server.stats.add_net_output_bytes(out_len);
            }

            if outcome.close {
                should_close = true;
                break;
            }
        }

        let should_poll_pubsub = poll_pubsub_pending || client_state.has_pubsub_subscriptions();
        if should_poll_pubsub {
            let pending = {
                let mut server = server_state.lock().await;
                server.pubsub.drain_messages(client_state.id())
            };
            let had_pending = !pending.is_empty();
            if had_pending
                && !encode_pubsub_messages(
                    pending,
                    &mut output,
                    io_limits.output_buffer_limit_bytes,
                )
            {
                tracing::warn!(
                    client_id = client_state.id(),
                    output_limit_bytes = io_limits.output_buffer_limit_bytes,
                    "disconnecting client: pubsub output frame exceeded buffer limit"
                );
                let response = encode(&RespFrame::error_str(OUTPUT_BUFFER_LIMIT_ERR));
                write_all_with_timeout(&mut stream, &response).await?;
                return Ok(());
            }
            poll_pubsub_pending = client_state.has_pubsub_subscriptions() || had_pending;
        } else {
            poll_pubsub_pending = false;
        }

        if !output.is_empty() {
            let out_len = output.len() as u64;
            write_all_with_timeout(&mut stream, &output).await?;
            output.clear();
            let mut server = server_state.lock().await;
            server.stats.add_net_output_bytes(out_len);
        }

        if should_close {
            return Ok(());
        }
    }
}
#[cfg(test)]
mod tests {
    use ratatosk_engine::keyspace::ServerState;
    use std::{sync::Arc, time::Duration};
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::{TcpListener, TcpStream},
        sync::Mutex,
        time::timeout,
    };

    use super::{ClientIoLimits, handle_client, handle_client_with_limits};
    use crate::config::DEFAULT_OUTPUT_BUFFER_LIMIT_BYTES;
    use crate::persistence::PersistenceRuntime;

    async fn setup_client_server() -> (TcpStream, tokio::task::JoinHandle<()>) {
        setup_client_server_with_limits(ClientIoLimits::default()).await
    }

    async fn setup_client_server_with_limits(
        io_limits: ClientIoLimits,
    ) -> (TcpStream, tokio::task::JoinHandle<()>) {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind listener");
        let addr = listener.local_addr().expect("local addr");
        let shared = Arc::new(Mutex::new(ServerState::with_default_dbs()));

        let server_task = tokio::spawn(async move {
            let (socket, _) = listener.accept().await.expect("accept");
            let persistence = Arc::new(
                PersistenceRuntime::from_config(&crate::config::ServerConfig::default())
                    .expect("persistence runtime"),
            );
            handle_client_with_limits(socket, shared, persistence, io_limits)
                .await
                .expect("handle client");
        });

        let client = TcpStream::connect(addr).await.expect("connect client");
        (client, server_task)
    }

    async fn setup_client_server_with_persistence(
        io_limits: ClientIoLimits,
        persistence: Arc<PersistenceRuntime>,
    ) -> (TcpStream, tokio::task::JoinHandle<()>) {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind listener");
        let addr = listener.local_addr().expect("local addr");
        let shared = Arc::new(Mutex::new(ServerState::with_default_dbs()));

        let server_task = tokio::spawn(async move {
            let (socket, _) = listener.accept().await.expect("accept");
            handle_client_with_limits(socket, shared, persistence, io_limits)
                .await
                .expect("handle client");
        });

        let client = TcpStream::connect(addr).await.expect("connect client");
        (client, server_task)
    }

    async fn read_reply(stream: &mut TcpStream) -> Vec<u8> {
        let mut buf = vec![0u8; 4096];
        let n = timeout(Duration::from_secs(1), stream.read(&mut buf))
            .await
            .expect("read timeout")
            .expect("read reply");
        buf.truncate(n);
        buf
    }

    async fn read_exact_reply(stream: &mut TcpStream, expected_len: usize) -> Vec<u8> {
        let mut out = Vec::with_capacity(expected_len);
        while out.len() < expected_len {
            let chunk = read_reply(stream).await;
            out.extend_from_slice(&chunk);
        }
        out
    }

    #[tokio::test]
    async fn inline_ping_and_echo() {
        let (mut client, server_task) = setup_client_server().await;

        client.write_all(b"PING\r\n").await.expect("write ping");
        let ping = read_reply(&mut client).await;
        assert_eq!(ping, b"+PONG\r\n");

        client.write_all(b"ECHO hi\r\n").await.expect("write echo");
        let echo = read_reply(&mut client).await;
        assert_eq!(echo, b"$2\r\nhi\r\n");

        client.write_all(b"QUIT\r\n").await.expect("write quit");
        let quit = read_reply(&mut client).await;
        assert_eq!(quit, b"+OK\r\n");

        let mut eof = [0u8; 1];
        let n = client.read(&mut eof).await.expect("read eof");
        assert_eq!(n, 0);

        server_task.await.expect("server task complete");
    }

    #[tokio::test]
    async fn m1_set_get_exists_del_select() {
        let (mut client, server_task) = setup_client_server().await;

        client
            .write_all(b"*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n")
            .await
            .expect("write set");
        assert_eq!(read_reply(&mut client).await, b"+OK\r\n");

        client
            .write_all(b"*2\r\n$3\r\nGET\r\n$3\r\nfoo\r\n")
            .await
            .expect("write get");
        assert_eq!(read_reply(&mut client).await, b"$3\r\nbar\r\n");

        client
            .write_all(b"*2\r\n$6\r\nSELECT\r\n$1\r\n1\r\n")
            .await
            .expect("write select");
        assert_eq!(read_reply(&mut client).await, b"+OK\r\n");

        client
            .write_all(b"*2\r\n$3\r\nGET\r\n$3\r\nfoo\r\n")
            .await
            .expect("write get db1");
        assert_eq!(read_reply(&mut client).await, b"$-1\r\n");

        client.write_all(b"QUIT\r\n").await.expect("write quit");
        let _ = read_reply(&mut client).await;

        server_task.await.expect("server task complete");
    }

    #[tokio::test]
    async fn pipelined_commands_return_batched_replies() {
        let (mut client, server_task) = setup_client_server().await;

        client
            .write_all(b"PING\r\nECHO hi\r\nQUIT\r\n")
            .await
            .expect("write pipelined commands");

        let expected = b"+PONG\r\n$2\r\nhi\r\n+OK\r\n";
        let reply = read_exact_reply(&mut client, expected.len()).await;
        assert_eq!(reply, expected);

        let mut eof = [0u8; 1];
        let n = client.read(&mut eof).await.expect("read eof");
        assert_eq!(n, 0);

        server_task.await.expect("server task complete");
    }
    #[tokio::test]
    async fn oversized_response_disconnects_client() {
        let limits = ClientIoLimits {
            output_buffer_limit_bytes: 256,
            client_read_timeout_sec: 0,
        };
        let (mut client, server_task) = setup_client_server_with_limits(limits).await;

        let payload = "x".repeat(1024);
        let command = format!("ECHO {payload}\r\n");
        client
            .write_all(command.as_bytes())
            .await
            .expect("write oversized echo");

        let reply = read_reply(&mut client).await;
        assert_eq!(reply, b"-ERR output buffer limit exceeded\r\n");

        let mut eof = [0u8; 1];
        let n = client.read(&mut eof).await.expect("read eof");
        assert_eq!(n, 0);

        server_task.await.expect("server task complete");
    }

    #[tokio::test]
    async fn pubsub_cross_client_fanout() {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind listener");
        let addr = listener.local_addr().expect("local addr");
        let shared = Arc::new(Mutex::new(ServerState::with_default_dbs()));

        let shared_for_accept = Arc::clone(&shared);
        let accept_task = tokio::spawn(async move {
            let (sock_a, _) = listener.accept().await.expect("accept a");
            let (sock_b, _) = listener.accept().await.expect("accept b");

            let shared_a = Arc::clone(&shared_for_accept);
            let task_a = tokio::spawn(async move {
                handle_client(sock_a, shared_a).await.expect("handle a");
            });
            let shared_b = Arc::clone(&shared_for_accept);
            let task_b = tokio::spawn(async move {
                handle_client(sock_b, shared_b).await.expect("handle b");
            });

            task_a.await.expect("join a");
            task_b.await.expect("join b");
        });

        let mut sub = TcpStream::connect(addr).await.expect("connect sub");
        let mut pubc = TcpStream::connect(addr).await.expect("connect pub");

        sub.write_all(b"*2\r\n$9\r\nSUBSCRIBE\r\n$4\r\nnews\r\n")
            .await
            .expect("subscribe");
        let sub_ack = read_reply(&mut sub).await;
        assert!(
            sub_ack
                .windows(b"subscribe".len())
                .any(|w| w == b"subscribe")
        );

        pubc.write_all(b"*3\r\n$7\r\nPUBLISH\r\n$4\r\nnews\r\n$5\r\nhello\r\n")
            .await
            .expect("publish");
        let pub_reply = read_reply(&mut pubc).await;
        assert_eq!(pub_reply, b":1\r\n");

        let pushed = read_reply(&mut sub).await;
        assert!(pushed.windows(b"message".len()).any(|w| w == b"message"));
        assert!(pushed.windows(b"news".len()).any(|w| w == b"news"));
        assert!(pushed.windows(b"hello".len()).any(|w| w == b"hello"));

        sub.write_all(b"*2\r\n$10\r\nSSUBSCRIBE\r\n$6\r\nshard1\r\n")
            .await
            .expect("ssubscribe");
        let ssub_ack = read_reply(&mut sub).await;
        assert!(
            ssub_ack
                .windows(b"ssubscribe".len())
                .any(|w| w == b"ssubscribe")
        );

        pubc.write_all(b"*3\r\n$8\r\nSPUBLISH\r\n$6\r\nshard1\r\n$5\r\nworld\r\n")
            .await
            .expect("spublish");
        let spub_reply = read_reply(&mut pubc).await;
        assert_eq!(spub_reply, b":1\r\n");

        let shard_pushed = read_reply(&mut sub).await;
        assert!(
            shard_pushed
                .windows(b"smessage".len())
                .any(|w| w == b"smessage")
        );
        assert!(
            shard_pushed
                .windows(b"shard1".len())
                .any(|w| w == b"shard1")
        );
        assert!(shard_pushed.windows(b"world".len()).any(|w| w == b"world"));

        sub.write_all(b"QUIT\r\n").await.expect("quit sub");
        let _ = read_reply(&mut sub).await;
        pubc.write_all(b"QUIT\r\n").await.expect("quit pub");
        let _ = read_reply(&mut pubc).await;

        accept_task.await.expect("accept task join");
    }

    #[tokio::test]
    async fn client_read_timeout_disconnects_idle_client() {
        let limits = ClientIoLimits {
            output_buffer_limit_bytes: DEFAULT_OUTPUT_BUFFER_LIMIT_BYTES,
            client_read_timeout_sec: 1,
        };
        let (mut client, server_task) = setup_client_server_with_limits(limits).await;

        client.write_all(b"PING\r\n").await.expect("write ping");
        let ping = read_reply(&mut client).await;
        assert_eq!(ping, b"+PONG\r\n");

        tokio::time::sleep(Duration::from_secs(2)).await;

        let mut buf = [0u8; 1];
        let n = client.read(&mut buf).await.expect("read after timeout");
        assert_eq!(n, 0, "expected EOF after timeout");

        server_task.await.expect("server task complete");
    }

    #[tokio::test]
    async fn write_commands_append_to_aof() {
        let dir = tempfile::tempdir().expect("tmpdir");
        let mut config = crate::config::ServerConfig::default();
        config.dir = dir.path().to_path_buf();
        config.appendonly = true;
        config.appendfsync = "always".to_string();
        let persistence = Arc::new(PersistenceRuntime::from_config(&config).expect("runtime"));

        let (mut client, server_task) =
            setup_client_server_with_persistence(ClientIoLimits::default(), persistence).await;

        client
            .write_all(b"*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n")
            .await
            .expect("write set");
        assert_eq!(read_reply(&mut client).await, b"+OK\r\n");

        client.write_all(b"QUIT\r\n").await.expect("quit");
        let _ = read_reply(&mut client).await;
        server_task.await.expect("server task complete");

        let aof = std::fs::read_to_string(dir.path().join("appendonly.aof")).expect("read aof");
        assert!(aof.contains("SET"));
        assert!(aof.contains("foo"));
        assert!(aof.contains("bar"));
    }
}
