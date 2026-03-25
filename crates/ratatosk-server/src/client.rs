use std::{
    io,
    sync::{Arc, OnceLock},
    time::Duration,
};

use bytes::{Bytes, BytesMut};
use ratatosk_engine::{
    command::{ClientState, CommandOutcome, ServerAccess, execute, is_write_command},
    keyspace::{PubSubMessage, SharedState},
};
use ratatosk_resp::{RespFrame, encode, encode_to_vec, encoded_len, parse};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
    sync::Notify,
    time::timeout,
};

use crate::breadcrumbs;
use crate::config::DEFAULT_OUTPUT_BUFFER_LIMIT_BYTES;
use crate::metrics;
use crate::persistence::{
    PersistenceRuntime, append_aof_command, run_save, start_bgrewriteaof, start_bgsave,
};

// Fallback defaults — runtime values are read from ConfigState at connection start.
#[allow(dead_code)]
const QUERY_BUFFER_LIMIT: usize = 1024 * 1024;
#[allow(dead_code)]
const OUTPUT_BUFFER_FLUSH_THRESHOLD: usize = 16 * 1024;
#[allow(dead_code)]
const WRITE_TIMEOUT: Duration = Duration::from_secs(5);
// Maximum wait between blocking retries.  With FIFO wake-one semantics only
// the front-of-queue waiter is notified, so exponential backoff is no longer
// needed — this constant caps the wait that guards against lost notifications.
const BLOCKING_RETRY_POLL_CAP: Duration = Duration::from_millis(500);
const AOF_APPEND_SLOW_THRESHOLD: Duration = Duration::from_secs(3);
const OUTPUT_BUFFER_LIMIT_ERR: &str = "ERR output buffer limit exceeded";
const AOF_WRITE_LATCH_ERR_PREFIX: &str =
    "MISCONF writes are blocked because AOF persistence is in an error state";
const READONLY_BATCH_ENV: &str = "RATATOSK_PIPELINE_READONLY_BATCH_LOCK";
static READONLY_BATCH_ENABLED: OnceLock<bool> = OnceLock::new();

pub type SharedServerState = Arc<SharedState>;

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

fn socket_addr_bytes(stream: &TcpStream) -> (Bytes, Bytes) {
    let addr = stream
        .peer_addr()
        .map(|addr| Bytes::from(addr.to_string()))
        .unwrap_or_else(|_| Bytes::from_static(b"127.0.0.1:0"));
    let laddr = stream
        .local_addr()
        .map(|addr| Bytes::from(addr.to_string()))
        .unwrap_or_else(|_| Bytes::from_static(b"127.0.0.1:0"));
    (addr, laddr)
}

async fn refresh_client_snapshot(
    server_state: &SharedServerState,
    client_state: &ClientState,
    addr: &Bytes,
    laddr: &Bytes,
    blocked: bool,
) {
    let mut server = server_state.meta.lock().await;
    server.upsert_client_snapshot(client_state.snapshot_with_redirect(
        addr.clone(),
        laddr.clone(),
        client_state.tracking_redirect(),
    ));
    server.set_client_blocked(client_state.id(), blocked);
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

fn readonly_batch_enabled() -> bool {
    *READONLY_BATCH_ENABLED.get_or_init(|| match std::env::var(READONLY_BATCH_ENV) {
        Ok(value) => {
            !(value == "0"
                || value.eq_ignore_ascii_case("false")
                || value.eq_ignore_ascii_case("no")
                || value.eq_ignore_ascii_case("off"))
        }
        Err(_) => true,
    })
}

fn is_readonly_batch_command(command: &[u8]) -> bool {
    command.eq_ignore_ascii_case(b"PING")
        || command.eq_ignore_ascii_case(b"ECHO")
        || command.eq_ignore_ascii_case(b"TIME")
        || command.eq_ignore_ascii_case(b"DBSIZE")
}

async fn try_run_readonly_batch(
    frames: Vec<RespFrame>,
    server_state: &SharedServerState,
    client_state: &mut ClientState,
) -> Result<Vec<CommandOutcome>, Vec<RespFrame>> {
    if !readonly_batch_enabled() || frames.len() < 2 {
        return Err(frames);
    }

    let mut command_names = Vec::with_capacity(frames.len());
    for frame in &frames {
        let Some(argv) = frame_to_argv_for_persistence(frame) else {
            return Err(frames);
        };
        let Some(command) = argv.first() else {
            return Err(frames);
        };
        if is_write_command(&argv) || !is_readonly_batch_command(command) {
            return Err(frames);
        }
        command_names.push(String::from_utf8_lossy(command).to_ascii_uppercase());
    }

    let lock_wait_start = std::time::Instant::now();
    let mut server = server_state.meta.lock().await;
    metrics::record_server_state_lock_wait_ms(
        "batch_execute_readonly",
        lock_wait_start.elapsed().as_secs_f64() * 1000.0,
    );

    let lock_hold_start = std::time::Instant::now();
    let mut outcomes = Vec::with_capacity(command_names.len());
    for (frame, command_name) in frames.into_iter().zip(command_names.iter()) {
        breadcrumbs::record_command(
            client_state.id(),
            command_name,
            client_state.selected_db(),
            0,
            "batch_execute",
        );

        let start = std::time::Instant::now();
        let outcome = {
            let mut access = ServerAccess::new_inline(&mut server);
            execute(frame, &mut access, client_state)
        };
        let duration = start.elapsed();
        let success = !matches!(outcome.response, ratatosk_resp::RespFrame::Error(_));
        metrics::record_command(command_name, success, duration.as_secs_f64());

        if duration.as_millis() > 1 {
            tracing::debug!(
                target = "ratatosk::slow_command",
                command = %command_name,
                duration_ms = duration.as_micros() as f64 / 1000.0,
                "slow command detected in readonly batch"
            );
        }
        outcomes.push(outcome);
    }
    metrics::record_server_state_lock_hold_ms(
        "batch_execute_readonly",
        lock_hold_start.elapsed().as_secs_f64() * 1000.0,
    );

    Ok(outcomes)
}

fn aof_write_latch_error(detail: &str) -> RespFrame {
    RespFrame::error_str(&format!(
        "{AOF_WRITE_LATCH_ERR_PREFIX}; last_error={detail}"
    ))
}

async fn set_aof_write_latch(server_state: &SharedServerState, error: String) {
    let mut server = server_state.meta.lock().await;
    server.set_aof_last_error(error.clone());
    drop(server);

    metrics::set_aof_write_latched(true);
    tracing::error!(
        target = "ratatosk::aof",
        error = %error,
        "AOF write latch engaged; write commands will be rejected"
    );
}

async fn clear_aof_write_latch_if_set(server_state: &SharedServerState) {
    let mut server = server_state.meta.lock().await;
    let was_latched = server.aof_write_latched();
    if was_latched {
        server.clear_aof_last_error();
    }
    drop(server);

    if was_latched {
        metrics::set_aof_write_latched(false);
        tracing::warn!(
            target = "ratatosk::aof",
            "AOF write latch cleared after successful append"
        );
    }
}

async fn wait_for_blocking_ready(
    stream: &TcpStream,
    wait_for: Duration,
    notifier: &Notify,
) -> io::Result<bool> {
    if wait_for.is_zero() {
        return Ok(false);
    }

    let sleep = tokio::time::sleep(wait_for);
    tokio::pin!(sleep);
    tokio::select! {
        _ = notifier.notified() => Ok(false),
        _ = &mut sleep => Ok(false),
        result = stream.readable() => match result {
            Ok(()) => {
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
}

/// Result of the select-based I/O wait: either a pubsub message arrived,
/// a monitor notification arrived, or network data was read.
enum WaitResult {
    PubSubMsg(PubSubMessage),
    PubSubClosed,
    MonitorWake,
    NetworkRead(usize),
}

async fn wait_for_async_push_or_input(
    stream: &mut TcpStream,
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

async fn write_all_with_timeout(
    stream: &mut TcpStream,
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

fn load_connection_runtime_config(server_state: &SharedServerState) -> (usize, usize, Duration) {
    let config = server_state.config_cache.load();
    (
        config.query_buffer_limit(),
        config.output_buffer_flush_threshold(),
        Duration::from_secs(config.client_write_timeout_sec()),
    )
}

fn reload_connection_runtime_config(
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

#[allow(clippy::too_many_arguments)]
async fn run_with_blocking_retry(
    frame: RespFrame,
    server_state: &SharedServerState,
    persistence: &Arc<PersistenceRuntime>,
    client_state: &mut ClientState,
    stream: &TcpStream,
    addr: &Bytes,
    laddr: &Bytes,
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

    let is_write_operation = first_argv.as_deref().is_some_and(is_write_command);

    breadcrumbs::record_command(client_state.id(), &command_name, first_db, 0, "execute");

    let mut outcome = {
        let lock_wait_start = std::time::Instant::now();
        let mut server = server_state.meta.lock().await;
        metrics::record_server_state_lock_wait_ms(
            "execute",
            lock_wait_start.elapsed().as_secs_f64() * 1000.0,
        );

        let lock_hold_start = std::time::Instant::now();
        let aof_latched_error = if is_write_operation && server.aof_enabled() {
            server.aof_last_error().map(str::to_owned)
        } else {
            None
        };

        let outcome = if let Some(aof_error) = aof_latched_error {
            metrics::record_aof_write_rejected("latched");
            CommandOutcome {
                response: aof_write_latch_error(&aof_error),
                close: false,
                retry_blocking: None,
                delay_ms: None,
                config_dirty: false,
            }
        } else {
            let mut access = ServerAccess::new_inline(&mut server);
            execute(frame, &mut access, client_state)
        };
        if outcome.config_dirty {
            server_state.update_config_cache(&server.config);
        }
        metrics::record_server_state_lock_hold_ms(
            "execute",
            lock_hold_start.elapsed().as_secs_f64() * 1000.0,
        );
        outcome
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
        refresh_client_snapshot(server_state, client_state, addr, laddr, false).await;
        return Ok(outcome);
    };

    refresh_client_snapshot(server_state, client_state, addr, laddr, true).await;
    let mut notifier = {
        let mut server = server_state.meta.lock().await;
        server.register_blocked_client(client_state.id(), retry.watch_keys.clone())
    };

    let deadline_ms = retry.deadline_ms;
    let mut frame = retry.frame;
    let mut last_response = outcome.response;
    let mut retry_attempts = 0u64;

    loop {
        let now_ms = ratatosk_core::time::monotonic_ms();
        if let Some(deadline) = deadline_ms {
            let deadline_u64 = u64::try_from(deadline).unwrap_or(u64::MAX);
            if now_ms >= deadline_u64 {
                metrics::record_blocking_retry_deadline_exhausted(&command_name);
                metrics::record_blocking_retry_completed(&command_name, retry_attempts);
                {
                    let mut server = server_state.meta.lock().await;
                    server.clear_blocked_client(client_state.id());
                }
                refresh_client_snapshot(server_state, client_state, addr, laddr, false).await;
                return Ok(CommandOutcome {
                    response: last_response,
                    close: false,
                    retry_blocking: None,
                    delay_ms: None,
                    config_dirty: false,
                });
            }
        }

        let wait_for = if let Some(deadline) = deadline_ms {
            let deadline_u64 = u64::try_from(deadline).unwrap_or(u64::MAX);
            let remaining_ms = deadline_u64.saturating_sub(now_ms);
            Duration::from_millis(remaining_ms).min(BLOCKING_RETRY_POLL_CAP)
        } else {
            BLOCKING_RETRY_POLL_CAP
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

        if wait_for_blocking_ready(stream, wait_for, notifier.as_ref())
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
            let is_retry_write = argv.as_deref().is_some_and(is_write_command);

            let lock_wait_start = std::time::Instant::now();
            let mut server = server_state.meta.lock().await;
            metrics::record_server_state_lock_wait_ms(
                "retry_execute",
                lock_wait_start.elapsed().as_secs_f64() * 1000.0,
            );

            let lock_hold_start = std::time::Instant::now();
            let aof_latched_error = if is_retry_write && server.aof_enabled() {
                server.aof_last_error().map(str::to_owned)
            } else {
                None
            };
            let mut outcome = if let Some(aof_error) = aof_latched_error {
                metrics::record_aof_write_rejected("latched_retry");
                CommandOutcome {
                    response: aof_write_latch_error(&aof_error),
                    close: false,
                    retry_blocking: None,
                    delay_ms: None,
                    config_dirty: false,
                }
            } else {
                let mut access = ServerAccess::new_inline(&mut server);
                execute(frame, &mut access, client_state)
            };
            if outcome.config_dirty {
                server_state.update_config_cache(&server.config);
            }
            metrics::record_server_state_lock_hold_ms(
                "retry_execute",
                lock_hold_start.elapsed().as_secs_f64() * 1000.0,
            );

            if outcome.retry_blocking.is_none() {
                drop(server);
                apply_post_execute_persistence(server_state, persistence, db, argv, &mut outcome)
                    .await;
            }
            outcome
        };

        let Some(retry) = outcome.retry_blocking else {
            metrics::record_blocking_retry_completed(&command_name, retry_attempts);
            {
                let mut server = server_state.meta.lock().await;
                server.clear_blocked_client(client_state.id());
            }
            refresh_client_snapshot(server_state, client_state, addr, laddr, false).await;
            return Ok(outcome);
        };
        last_response = outcome.response;
        frame = retry.frame;
        notifier = {
            let mut server = server_state.meta.lock().await;
            server.register_blocked_client(client_state.id(), retry.watch_keys.clone())
        };
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

    if command == b"BGREWRITEAOF" && !matches!(outcome.response, RespFrame::Error(_)) {
        if !start_bgrewriteaof(Arc::clone(server_state), Arc::clone(persistence)).await {
            outcome.response =
                RespFrame::error_str("ERR BGREWRITEAOF failed: appendonly is disabled");
        }
        return;
    }

    if is_queued_response(&outcome.response) || !is_write_command(&argv) {
        return;
    }

    // Lock-free config read via ArcSwap.
    let fsync_policy = {
        let config = server_state.config_cache.load();
        String::from_utf8_lossy(config.appendfsync()).to_string()
    };
    let command_name = String::from_utf8_lossy(&argv[0]).to_string();

    let append_start = std::time::Instant::now();
    match append_aof_command(persistence, selected_db, argv).await {
        Ok(()) => {
            let elapsed_ms = append_start.elapsed().as_secs_f64() * 1000.0;
            if append_start.elapsed() > AOF_APPEND_SLOW_THRESHOLD {
                metrics::record_aof_append_timeout("append_slow");
                tracing::warn!(
                    target = "ratatosk::aof",
                    selected_db = selected_db,
                    command = %command_name,
                    elapsed_ms = elapsed_ms,
                    threshold_ms = AOF_APPEND_SLOW_THRESHOLD.as_millis(),
                    "AOF append exceeded slow threshold"
                );
            }
            metrics::record_aof_write(&fsync_policy);
            metrics::record_aof_append_duration_ms(elapsed_ms, "ok");
            clear_aof_write_latch_if_set(server_state).await;
        }
        Err(error) => {
            let elapsed_ms = append_start.elapsed().as_secs_f64() * 1000.0;
            metrics::record_aof_append_duration_ms(elapsed_ms, "error");
            metrics::record_aof_write_error();
            if error.kind() == io::ErrorKind::TimedOut {
                metrics::record_aof_append_timeout("worker");
            }

            let latch_error = format!("AOF append failed for command {}: {}", command_name, error);
            set_aof_write_latch(server_state, latch_error.clone()).await;
            outcome.response = aof_write_latch_error(&latch_error);
            tracing::error!(
                target = "ratatosk::aof",
                selected_db = selected_db,
                command = %command_name,
                elapsed_ms = elapsed_ms,
                error = %error,
                "AOF append failed"
            );
        }
    }
}

/// Encode a single PubSubMessage into the output buffer.
/// Returns `false` if the output buffer would exceed the limit.
fn encode_pubsub_message(
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
    let use_push = protocol_version >= 3;

    for message in messages {
        let frame = match message {
            PubSubMessage::Message { channel, payload } => {
                let inner = vec![
                    RespFrame::bulk_str("message"),
                    RespFrame::BulkString(Some(channel)),
                    RespFrame::BulkString(Some(payload)),
                ];
                if use_push {
                    RespFrame::Push(inner)
                } else {
                    RespFrame::Array(inner)
                }
            }
            PubSubMessage::SMessage { channel, payload } => {
                let inner = vec![
                    RespFrame::bulk_str("smessage"),
                    RespFrame::BulkString(Some(channel)),
                    RespFrame::BulkString(Some(payload)),
                ];
                if use_push {
                    RespFrame::Push(inner)
                } else {
                    RespFrame::Array(inner)
                }
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
                if use_push {
                    RespFrame::Push(inner)
                } else {
                    RespFrame::Array(inner)
                }
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
                if use_push {
                    RespFrame::Push(inner)
                } else {
                    RespFrame::Array(inner)
                }
            }
            PubSubMessage::TrackingRedirectBroken { redirect_client_id } => {
                if use_push {
                    RespFrame::Push(vec![
                        RespFrame::bulk_str("tracking-redir-broken"),
                        RespFrame::Integer(redirect_client_id),
                    ])
                } else {
                    RespFrame::Array(vec![
                        RespFrame::bulk_str("tracking-redir-broken"),
                        RespFrame::Integer(redirect_client_id),
                    ])
                }
            }
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
        // Lock-free: atomic client ID allocation and stats update.
        let id = server_state.alloc_client_id();
        server_state.stats.mark_client_connected();
        let active = server_state.stats.connected_clients();
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

    tracing::debug!(
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
        let mut server = server_state.meta.lock().await;
        server.stats.mark_client_disconnected();
        server.pubsub.remove_client(client_id);
        server.replication_remove_client(client_id);
        server.tracking_remove_client(client_id);
        server.unregister_monitor(client_id);
        server.remove_client_snapshot(client_id);
    }
    // Lock-free stats update after releasing the inner lock.
    server_state.stats.mark_client_disconnected();
    let active = server_state.stats.connected_clients();
    metrics::set_active_connections(active as usize);
    metrics::record_connection_event(disconnect_reason);

    tracing::debug!(
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
    let (addr, laddr) = socket_addr_bytes(&stream);
    let (
        mut pubsub_rx,
        monitor_notifier,
        mut query_buffer_limit,
        mut output_buffer_flush_threshold,
        mut write_timeout,
    ) = {
        let mut server = server_state.meta.lock().await;
        server.stats.mark_client_connected();
        let rx = server.pubsub.register_client(client_id);
        let mn = server.register_monitor_notifier(client_id);
        let (qbl, obft, wt) = load_connection_runtime_config(server_state);
        (rx, mn, qbl, obft, wt)
    };
    refresh_client_snapshot(server_state, &client_state, &addr, &laddr, false).await;
    loop {
        let client_accepts_async_push = client_state.has_pubsub_subscriptions()
            || client_state.tracking_enabled()
            || client_state.is_monitor();

        // Drain any already-buffered pubsub messages (lock-free via mpsc).
        {
            let mut had_pubsub = false;
            while let Ok(msg) = pubsub_rx.try_recv() {
                had_pubsub = true;
                if !encode_pubsub_message(
                    msg,
                    &mut output,
                    io_limits.output_buffer_limit_bytes,
                    client_state.protocol_version(),
                ) {
                    tracing::warn!(
                        client_id = client_state.id(),
                        output_limit_bytes = io_limits.output_buffer_limit_bytes,
                        "disconnecting client: pubsub output frame exceeded buffer limit"
                    );
                    let response = encode(&RespFrame::error_str(OUTPUT_BUFFER_LIMIT_ERR));
                    write_all_with_timeout(&mut stream, &response, write_timeout).await?;
                    return Ok(());
                }
            }
            if had_pubsub {
                write_all_with_timeout(&mut stream, &output, write_timeout).await?;
                output.clear();
            }
        }

        // Drain MONITOR messages — each line is sent as a RESP simple string.
        {
            let monitor_pending = {
                let mut server = server_state.meta.lock().await;
                server.drain_monitor_messages(client_state.id())
            };
            if !monitor_pending.is_empty() {
                for line in monitor_pending {
                    let frame = RespFrame::SimpleString(line);
                    if !append_encoded_frame(
                        &mut output,
                        &frame,
                        io_limits.output_buffer_limit_bytes,
                    ) {
                        tracing::warn!(
                            client_id = client_state.id(),
                            output_limit_bytes = io_limits.output_buffer_limit_bytes,
                            "disconnecting monitor client: output buffer limit exceeded"
                        );
                        let response = encode(&RespFrame::error_str(OUTPUT_BUFFER_LIMIT_ERR));
                        write_all_with_timeout(&mut stream, &response, write_timeout).await?;
                        return Ok(());
                    }
                }
                write_all_with_timeout(&mut stream, &output, write_timeout).await?;
                output.clear();
            }
        }

        // Wait for either: a pubsub push message, a monitor notification,
        // or network input from the client.
        let wait_result = if io_limits.client_read_timeout_sec > 0 && !client_accepts_async_push {
            let idle_duration = Duration::from_secs(io_limits.client_read_timeout_sec);
            tokio::select! {
                msg = pubsub_rx.recv() => match msg {
                    Some(m) => Ok(WaitResult::PubSubMsg(m)),
                    None => Ok(WaitResult::PubSubClosed),
                },
                _ = monitor_notifier.notified() => Ok(WaitResult::MonitorWake),
                result = timeout(idle_duration, stream.read_buf(&mut input)) => match result {
                    Ok(result) => result.map(WaitResult::NetworkRead),
                    Err(_) => {
                        tracing::debug!(
                            client_id = client_id,
                            timeout_sec = io_limits.client_read_timeout_sec,
                            "disconnecting idle client: read timeout"
                        );
                        return Ok(());
                    }
                }
            }?
        } else {
            wait_for_async_push_or_input(
                &mut stream,
                &mut input,
                &mut pubsub_rx,
                monitor_notifier.as_ref(),
            )
            .await?
        };

        let read = match wait_result {
            WaitResult::PubSubMsg(msg) => {
                if !encode_pubsub_message(
                    msg,
                    &mut output,
                    io_limits.output_buffer_limit_bytes,
                    client_state.protocol_version(),
                ) {
                    let response = encode(&RespFrame::error_str(OUTPUT_BUFFER_LIMIT_ERR));
                    write_all_with_timeout(&mut stream, &response, write_timeout).await?;
                    return Ok(());
                }
                write_all_with_timeout(&mut stream, &output, write_timeout).await?;
                output.clear();
                continue;
            }
            WaitResult::PubSubClosed => {
                tracing::warn!(
                    client_id = client_id,
                    "disconnecting pubsub client: push channel closed (overflow)"
                );
                let response = encode(&RespFrame::error_str(
                    "ERR pubsub pending output buffer limit exceeded",
                ));
                write_all_with_timeout(&mut stream, &response, write_timeout).await?;
                return Ok(());
            }
            WaitResult::MonitorWake => {
                // Loop back to drain monitor messages at the top.
                continue;
            }
            WaitResult::NetworkRead(n) => n,
        };

        if read == 0 {
            return Ok(());
        }

        // Lock-free stats update for network I/O bytes.
        server_state.stats.add_net_input_bytes(read as u64);

        if input.len() > query_buffer_limit {
            let frame = RespFrame::error_str("ERR query buffer limit exceeded");
            write_all_with_timeout(&mut stream, &encode(&frame), write_timeout).await?;
            return Ok(());
        }

        let mut parsed_frames = Vec::new();
        loop {
            match parse(&mut input) {
                Ok(Some(frame)) => parsed_frames.push(frame),
                Ok(None) => break,
                Err(error) => {
                    tracing::warn!(
                        target = "ratatosk::protocol",
                        client_id = client_state.id(),
                        input_len = input.len(),
                        error = %error,
                        "protocol parse error; closing client connection"
                    );
                    let response = encode(&RespFrame::error_str("ERR protocol error"));
                    write_all_with_timeout(&mut stream, &response, write_timeout).await?;
                    return Ok(());
                }
            }
        }

        let outcomes =
            match try_run_readonly_batch(parsed_frames, server_state, &mut client_state).await {
                Ok(outcomes) => outcomes,
                Err(frames) => {
                    let mut outcomes = Vec::with_capacity(frames.len());
                    for frame in frames {
                        let outcome = run_with_blocking_retry(
                            frame,
                            server_state,
                            persistence,
                            &mut client_state,
                            &stream,
                            &addr,
                            &laddr,
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
                        outcomes.push(outcome);
                    }
                    outcomes
                }
            };

        let mut should_close = false;
        for outcome in outcomes {
            if outcome.config_dirty {
                reload_connection_runtime_config(
                    server_state,
                    &mut query_buffer_limit,
                    &mut output_buffer_flush_threshold,
                    &mut write_timeout,
                );
            }

            // Apply progressive delay (e.g. AUTH failure backoff) before sending response.
            if let Some(delay_ms) = outcome.delay_ms {
                tokio::time::sleep(std::time::Duration::from_millis(delay_ms)).await;
            }

            // Enforce CLIENT REPLY mode: suppress responses when off/skip.
            // Push notifications (pub/sub, invalidation) are unaffected.
            let reply_mode = client_state.reply_mode().clone();
            let suppress = match reply_mode.as_ref() {
                b"off" => true,
                b"skip" => {
                    // Skip this one response, then reset to "on"
                    client_state.set_reply_mode_on();
                    true
                }
                _ => false,
            };

            if !suppress {
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
                    write_all_with_timeout(&mut stream, &response, write_timeout).await?;
                    return Ok(());
                }

                if output.len() >= output_buffer_flush_threshold {
                    let out_len = output.len() as u64;
                    write_all_with_timeout(&mut stream, &output, write_timeout).await?;
                    output.clear();
                    let mut server = server_state.meta.lock().await;
                    server.stats.add_net_output_bytes(out_len);
                }
            }

            if outcome.close {
                should_close = true;
                break;
            }
        }

        // After command execution, drain any pubsub messages that arrived
        // during processing (lock-free via mpsc try_recv).
        let should_poll_pubsub =
            client_state.has_pubsub_subscriptions() || client_state.tracking_enabled();
        if should_poll_pubsub {
            while let Ok(msg) = pubsub_rx.try_recv() {
                if !encode_pubsub_message(
                    msg,
                    &mut output,
                    io_limits.output_buffer_limit_bytes,
                    client_state.protocol_version(),
                ) {
                    tracing::warn!(
                        client_id = client_state.id(),
                        output_limit_bytes = io_limits.output_buffer_limit_bytes,
                        "disconnecting client: pubsub output frame exceeded buffer limit"
                    );
                    let response = encode(&RespFrame::error_str(OUTPUT_BUFFER_LIMIT_ERR));
                    write_all_with_timeout(&mut stream, &response, write_timeout).await?;
                    return Ok(());
                }
            }
        }

        // Drain MONITOR messages accumulated during command execution.
        if client_state.is_monitor() {
            let monitor_msgs = {
                let mut server = server_state.meta.lock().await;
                server.drain_monitor_messages(client_state.id())
            };
            for line in monitor_msgs {
                let frame = RespFrame::SimpleString(line);
                if !append_encoded_frame(&mut output, &frame, io_limits.output_buffer_limit_bytes) {
                    tracing::warn!(
                        client_id = client_state.id(),
                        output_limit_bytes = io_limits.output_buffer_limit_bytes,
                        "disconnecting monitor client: output buffer limit exceeded"
                    );
                    let response = encode(&RespFrame::error_str(OUTPUT_BUFFER_LIMIT_ERR));
                    write_all_with_timeout(&mut stream, &response, write_timeout).await?;
                    return Ok(());
                }
            }
        }

        if !output.is_empty() {
            let out_len = output.len() as u64;
            write_all_with_timeout(&mut stream, &output, write_timeout).await?;
            output.clear();
            let mut server = server_state.meta.lock().await;
            server.stats.add_net_output_bytes(out_len);
        }

        refresh_client_snapshot(server_state, &client_state, &addr, &laddr, false).await;

        if should_close {
            return Ok(());
        }
    }
}
#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use ratatosk_engine::keyspace::{ServerState, SharedState};
    use std::{sync::Arc, time::Duration};
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::{TcpListener, TcpStream},
        time::timeout,
    };

    use super::{ClientIoLimits, handle_client, handle_client_with_limits};
    use crate::config::DEFAULT_OUTPUT_BUFFER_LIMIT_BYTES;
    use crate::persistence::PersistenceRuntime;

    async fn setup_client_server() -> (TcpStream, tokio::task::JoinHandle<()>) {
        setup_client_server_with_limits(ClientIoLimits::default()).await
    }

    async fn setup_client_server_with_shared(
        io_limits: ClientIoLimits,
    ) -> (TcpStream, Arc<SharedState>, tokio::task::JoinHandle<()>) {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind listener");
        let addr = listener.local_addr().expect("local addr");
        let shared = Arc::new(SharedState::new(ServerState::with_default_dbs()));
        let shared_for_server = Arc::clone(&shared);

        let server_task = tokio::spawn(async move {
            let (socket, _) = listener.accept().await.expect("accept");
            let persistence = Arc::new(
                PersistenceRuntime::from_config(&crate::config::ServerConfig::default())
                    .expect("persistence runtime"),
            );
            handle_client_with_limits(socket, shared_for_server, persistence, io_limits)
                .await
                .expect("handle client");
        });

        let client = TcpStream::connect(addr).await.expect("connect client");
        (client, shared, server_task)
    }

    async fn setup_client_server_with_limits(
        io_limits: ClientIoLimits,
    ) -> (TcpStream, tokio::task::JoinHandle<()>) {
        let (client, _shared, server_task) = setup_client_server_with_shared(io_limits).await;
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
        let shared = Arc::new(SharedState::new(ServerState::with_default_dbs()));

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

    /// Search the temp dir for any AOF file and return its concatenated contents.
    fn find_aof_content(dir: &std::path::Path) -> String {
        let mut content = String::new();
        for entry in std::fs::read_dir(dir).expect("read dir") {
            let entry = entry.expect("dir entry");
            let name = entry.file_name();
            let name_str = name.to_string_lossy();
            if name_str.ends_with(".aof") && !name_str.ends_with(".manifest") {
                let text = std::fs::read_to_string(entry.path()).unwrap_or_default();
                content.push_str(&text);
            }
        }
        assert!(
            !content.is_empty(),
            "no AOF file found in {}",
            dir.display()
        );
        content
    }

    fn parse_integer_reply(reply: &[u8]) -> i64 {
        let text = std::str::from_utf8(reply).expect("valid integer reply utf8");
        text.trim_start_matches(':')
            .trim()
            .parse::<i64>()
            .expect("integer reply")
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
    async fn peer_close_after_ping_cleans_up_connection_state() {
        let (mut client, shared, server_task) =
            setup_client_server_with_shared(ClientIoLimits::default()).await;

        client.write_all(b"PING\r\n").await.expect("write ping");
        assert_eq!(read_reply(&mut client).await, b"+PONG\r\n");

        drop(client);

        timeout(Duration::from_secs(1), server_task)
            .await
            .expect("server task timeout")
            .expect("server task complete");

        assert_eq!(shared.stats.connected_clients(), 0);
        assert_eq!(shared.stats.total_connections_received(), 1);

        let server = shared.meta.lock().await;
        assert_eq!(server.stats.connected_clients(), 0);
        assert_eq!(server.stats.total_connections_received(), 1);
        assert_eq!(server.connected_client_snapshots(), 0);
        assert_eq!(server.blocked_clients(), 0);
        assert_eq!(server.tracking_clients(), 0);
        assert_eq!(server.monitor_client_count(), 0);
        assert!(server.client_snapshot(1).is_none());
        assert!(server.pubsub.client_channels(1).is_empty());
        assert!(server.pubsub.client_shard_channels(1).is_empty());
        assert!(server.pubsub.client_patterns(1).is_empty());
    }

    #[tokio::test]
    async fn peer_close_after_subscribe_cleans_up_pubsub_state() {
        let (mut client, shared, server_task) =
            setup_client_server_with_shared(ClientIoLimits::default()).await;

        client
            .write_all(b"*2\r\n$9\r\nSUBSCRIBE\r\n$4\r\nnews\r\n")
            .await
            .expect("subscribe");
        let subscribe_reply = read_reply(&mut client).await;
        assert!(
            subscribe_reply
                .windows(b"subscribe".len())
                .any(|window| window == b"subscribe")
        );

        drop(client);

        timeout(Duration::from_secs(1), server_task)
            .await
            .expect("server task timeout")
            .expect("server task complete");

        let server = shared.meta.lock().await;
        assert_eq!(server.connected_client_snapshots(), 0);
        assert!(server.client_snapshot(1).is_none());
        assert!(server.pubsub.client_channels(1).is_empty());
        assert_eq!(server.pubsub.numsub(&[Bytes::from("news")])[0].1, 0);
    }

    #[tokio::test]
    async fn repeated_peer_closes_do_not_leave_connected_clients_behind() {
        const CONNECTIONS: usize = 8;

        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind listener");
        let addr = listener.local_addr().expect("local addr");
        let shared = Arc::new(SharedState::new(ServerState::with_default_dbs()));
        let shared_for_accept = Arc::clone(&shared);

        let accept_task = tokio::spawn(async move {
            let persistence = Arc::new(
                PersistenceRuntime::from_config(&crate::config::ServerConfig::default())
                    .expect("persistence runtime"),
            );
            let mut tasks = Vec::with_capacity(CONNECTIONS);

            for _ in 0..CONNECTIONS {
                let (socket, _) = listener.accept().await.expect("accept");
                let shared = Arc::clone(&shared_for_accept);
                let persistence = Arc::clone(&persistence);
                tasks.push(tokio::spawn(async move {
                    handle_client_with_limits(
                        socket,
                        shared,
                        persistence,
                        ClientIoLimits::default(),
                    )
                    .await
                    .expect("handle client");
                }));
            }

            for task in tasks {
                task.await.expect("join client task");
            }
        });

        for _ in 0..CONNECTIONS {
            let mut client = TcpStream::connect(addr).await.expect("connect client");
            client.write_all(b"PING\r\n").await.expect("write ping");
            assert_eq!(read_reply(&mut client).await, b"+PONG\r\n");
            drop(client);
        }

        timeout(Duration::from_secs(2), accept_task)
            .await
            .expect("accept task timeout")
            .expect("accept task complete");

        assert_eq!(shared.stats.connected_clients(), 0);
        assert_eq!(
            shared.stats.total_connections_received(),
            CONNECTIONS as u64
        );

        let server = shared.meta.lock().await;
        assert_eq!(server.stats.connected_clients(), 0);
        assert_eq!(
            server.stats.total_connections_received(),
            CONNECTIONS as u64
        );
        assert_eq!(server.connected_client_snapshots(), 0);
    }

    #[tokio::test]
    async fn info_stats_reports_total_connections_received_from_runtime_stats() {
        let (mut client, server_task) = setup_client_server().await;

        client
            .write_all(b"INFO stats\r\n")
            .await
            .expect("write info stats");
        let info = read_reply(&mut client).await;
        let info_text = String::from_utf8_lossy(&info);
        assert!(
            info_text.contains("total_connections_received:1"),
            "{info_text}"
        );

        client.write_all(b"QUIT\r\n").await.expect("quit");
        let _ = read_reply(&mut client).await;
        server_task.await.expect("server task complete");
    }

    #[tokio::test]
    async fn config_set_query_buffer_limit_applies_to_current_connection() {
        let (mut client, shared, server_task) =
            setup_client_server_with_shared(ClientIoLimits::default()).await;

        client
            .write_all(b"CONFIG SET query-buffer-limit 1024\r\n")
            .await
            .expect("set query buffer limit");
        assert_eq!(read_reply(&mut client).await, b"+OK\r\n");
        assert_eq!(shared.config_cache.load().query_buffer_limit(), 1024);

        let payload = "x".repeat(1100);
        let command = format!("ECHO {payload}\r\n");
        client
            .write_all(command.as_bytes())
            .await
            .expect("write oversized query");

        let reply = read_reply(&mut client).await;
        assert_eq!(reply, b"-ERR query buffer limit exceeded\r\n");

        let mut eof = [0u8; 1];
        let n = client.read(&mut eof).await.expect("read eof");
        assert_eq!(n, 0);

        server_task.await.expect("server task complete");
    }

    #[tokio::test]
    async fn config_set_hz_round_trips_and_updates_config_cache() {
        let (mut client, shared, server_task) =
            setup_client_server_with_shared(ClientIoLimits::default()).await;

        client
            .write_all(b"CONFIG SET hz 25\r\n")
            .await
            .expect("set hz");
        assert_eq!(read_reply(&mut client).await, b"+OK\r\n");
        assert_eq!(shared.config_cache.load().hz(), 25);

        client
            .write_all(b"CONFIG GET hz\r\n")
            .await
            .expect("get hz");
        assert_eq!(
            read_reply(&mut client).await,
            b"*2\r\n$2\r\nhz\r\n$2\r\n25\r\n"
        );

        client.write_all(b"QUIT\r\n").await.expect("quit");
        let _ = read_reply(&mut client).await;
        server_task.await.expect("server task complete");
    }

    #[tokio::test]
    async fn pubsub_cross_client_fanout() {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind listener");
        let addr = listener.local_addr().expect("local addr");
        let shared = Arc::new(SharedState::new(ServerState::with_default_dbs()));

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
    async fn client_tracking_pushes_invalidation_cross_client() {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind listener");
        let addr = listener.local_addr().expect("local addr");
        let shared = Arc::new(SharedState::new(ServerState::with_default_dbs()));

        let shared_for_accept = Arc::clone(&shared);
        let accept_task = tokio::spawn(async move {
            let (sock_tracker, _) = listener.accept().await.expect("accept tracker");
            let (sock_writer, _) = listener.accept().await.expect("accept writer");

            let shared_tracker = Arc::clone(&shared_for_accept);
            let task_tracker = tokio::spawn(async move {
                handle_client(sock_tracker, shared_tracker)
                    .await
                    .expect("handle tracker");
            });
            let shared_writer = Arc::clone(&shared_for_accept);
            let task_writer = tokio::spawn(async move {
                handle_client(sock_writer, shared_writer)
                    .await
                    .expect("handle writer");
            });

            task_tracker.await.expect("join tracker");
            task_writer.await.expect("join writer");
        });

        let mut tracker = TcpStream::connect(addr).await.expect("connect tracker");
        let mut writer = TcpStream::connect(addr).await.expect("connect writer");

        writer
            .write_all(b"*3\r\n$3\r\nSET\r\n$7\r\ntracked\r\n$2\r\nv1\r\n")
            .await
            .expect("seed tracked key");
        assert_eq!(read_reply(&mut writer).await, b"+OK\r\n");

        tracker
            .write_all(b"*3\r\n$6\r\nCLIENT\r\n$8\r\nTRACKING\r\n$2\r\nON\r\n")
            .await
            .expect("enable tracking");
        assert_eq!(read_reply(&mut tracker).await, b"+OK\r\n");

        tracker
            .write_all(b"*2\r\n$3\r\nGET\r\n$7\r\ntracked\r\n")
            .await
            .expect("read tracked key");
        assert_eq!(read_reply(&mut tracker).await, b"$2\r\nv1\r\n");

        writer
            .write_all(b"*3\r\n$3\r\nSET\r\n$7\r\ntracked\r\n$2\r\nv2\r\n")
            .await
            .expect("update tracked key");
        assert_eq!(read_reply(&mut writer).await, b"+OK\r\n");

        let invalidation = read_reply(&mut tracker).await;
        assert!(
            invalidation
                .windows(b"invalidate".len())
                .any(|w| w == b"invalidate"),
            "expected invalidate push, got {:?}",
            String::from_utf8_lossy(&invalidation)
        );
        assert!(
            invalidation
                .windows(b"tracked".len())
                .any(|w| w == b"tracked"),
            "expected tracked key in invalidate push, got {:?}",
            String::from_utf8_lossy(&invalidation)
        );

        tracker.write_all(b"QUIT\r\n").await.expect("quit tracker");
        let _ = read_reply(&mut tracker).await;
        writer.write_all(b"QUIT\r\n").await.expect("quit writer");
        let _ = read_reply(&mut writer).await;

        accept_task.await.expect("accept task join");
    }

    #[tokio::test]
    async fn client_tracking_bcast_prefix_pushes_matching_invalidation() {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind listener");
        let addr = listener.local_addr().expect("local addr");
        let shared = Arc::new(SharedState::new(ServerState::with_default_dbs()));

        let shared_for_accept = Arc::clone(&shared);
        let accept_task = tokio::spawn(async move {
            let (sock_tracker, _) = listener.accept().await.expect("accept tracker");
            let (sock_writer, _) = listener.accept().await.expect("accept writer");

            let shared_tracker = Arc::clone(&shared_for_accept);
            let task_tracker = tokio::spawn(async move {
                handle_client(sock_tracker, shared_tracker)
                    .await
                    .expect("handle tracker");
            });
            let shared_writer = Arc::clone(&shared_for_accept);
            let task_writer = tokio::spawn(async move {
                handle_client(sock_writer, shared_writer)
                    .await
                    .expect("handle writer");
            });

            task_tracker.await.expect("join tracker");
            task_writer.await.expect("join writer");
        });

        let mut tracker = TcpStream::connect(addr).await.expect("connect tracker");
        let mut writer = TcpStream::connect(addr).await.expect("connect writer");

        tracker
            .write_all(
                b"*6\r\n$6\r\nCLIENT\r\n$8\r\nTRACKING\r\n$2\r\nON\r\n$5\r\nBCAST\r\n$6\r\nPREFIX\r\n$5\r\nuser:\r\n",
            )
            .await
            .expect("enable bcast prefix tracking");
        assert_eq!(read_reply(&mut tracker).await, b"+OK\r\n");

        writer
            .write_all(b"*3\r\n$3\r\nSET\r\n$6\r\nother:\r\n$2\r\nv1\r\n")
            .await
            .expect("write non matching key");
        assert_eq!(read_reply(&mut writer).await, b"+OK\r\n");

        writer
            .write_all(b"*3\r\n$3\r\nSET\r\n$6\r\nuser:1\r\n$2\r\nv2\r\n")
            .await
            .expect("write matching key");
        assert_eq!(read_reply(&mut writer).await, b"+OK\r\n");

        let invalidation = read_reply(&mut tracker).await;
        assert!(
            invalidation
                .windows(b"invalidate".len())
                .any(|w| w == b"invalidate"),
            "expected invalidate push, got {:?}",
            String::from_utf8_lossy(&invalidation)
        );
        assert!(
            invalidation
                .windows(b"user:1".len())
                .any(|w| w == b"user:1"),
            "expected matching key in invalidate push, got {:?}",
            String::from_utf8_lossy(&invalidation)
        );
        assert!(
            !invalidation
                .windows(b"other:".len())
                .any(|w| w == b"other:"),
            "unexpected non-matching key in invalidate push, got {:?}",
            String::from_utf8_lossy(&invalidation)
        );

        tracker.write_all(b"QUIT\r\n").await.expect("quit tracker");
        let _ = read_reply(&mut tracker).await;
        writer.write_all(b"QUIT\r\n").await.expect("quit writer");
        let _ = read_reply(&mut writer).await;

        accept_task.await.expect("accept task join");
    }

    #[tokio::test]
    async fn client_tracking_redirect_pushes_invalidation_to_target() {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind listener");
        let addr = listener.local_addr().expect("local addr");
        let shared = Arc::new(SharedState::new(ServerState::with_default_dbs()));

        let shared_for_accept = Arc::clone(&shared);
        let accept_task = tokio::spawn(async move {
            let (sock_tracker, _) = listener.accept().await.expect("accept tracker");
            let (sock_target, _) = listener.accept().await.expect("accept target");
            let (sock_writer, _) = listener.accept().await.expect("accept writer");

            let shared_tracker = Arc::clone(&shared_for_accept);
            let task_tracker = tokio::spawn(async move {
                handle_client(sock_tracker, shared_tracker)
                    .await
                    .expect("handle tracker");
            });
            let shared_target = Arc::clone(&shared_for_accept);
            let task_target = tokio::spawn(async move {
                handle_client(sock_target, shared_target)
                    .await
                    .expect("handle target");
            });
            let shared_writer = Arc::clone(&shared_for_accept);
            let task_writer = tokio::spawn(async move {
                handle_client(sock_writer, shared_writer)
                    .await
                    .expect("handle writer");
            });

            task_tracker.await.expect("join tracker");
            task_target.await.expect("join target");
            task_writer.await.expect("join writer");
        });

        let mut tracker = TcpStream::connect(addr).await.expect("connect tracker");
        let mut target = TcpStream::connect(addr).await.expect("connect target");
        let mut writer = TcpStream::connect(addr).await.expect("connect writer");

        target
            .write_all(b"*2\r\n$6\r\nCLIENT\r\n$2\r\nID\r\n")
            .await
            .expect("request target id");
        let target_id = parse_integer_reply(&read_reply(&mut target).await);

        let tracking_command = format!(
            "*5\r\n$6\r\nCLIENT\r\n$8\r\nTRACKING\r\n$2\r\nON\r\n$8\r\nREDIRECT\r\n${}\r\n{}\r\n",
            target_id.to_string().len(),
            target_id
        );
        tracker
            .write_all(tracking_command.as_bytes())
            .await
            .expect("enable redirect tracking");
        assert_eq!(read_reply(&mut tracker).await, b"+OK\r\n");

        writer
            .write_all(b"*3\r\n$3\r\nSET\r\n$7\r\ntracked\r\n$2\r\nv1\r\n")
            .await
            .expect("seed tracked key");
        assert_eq!(read_reply(&mut writer).await, b"+OK\r\n");

        tracker
            .write_all(b"*2\r\n$3\r\nGET\r\n$7\r\ntracked\r\n")
            .await
            .expect("read tracked key");
        assert_eq!(read_reply(&mut tracker).await, b"$2\r\nv1\r\n");

        writer
            .write_all(b"*3\r\n$3\r\nSET\r\n$7\r\ntracked\r\n$2\r\nv2\r\n")
            .await
            .expect("update tracked key");
        assert_eq!(read_reply(&mut writer).await, b"+OK\r\n");

        let invalidation = read_reply(&mut target).await;
        assert!(
            invalidation
                .windows(b"invalidate".len())
                .any(|w| w == b"invalidate"),
            "expected invalidate push, got {:?}",
            String::from_utf8_lossy(&invalidation)
        );
        assert!(
            invalidation
                .windows(b"tracked".len())
                .any(|w| w == b"tracked"),
            "expected tracked key in invalidate push, got {:?}",
            String::from_utf8_lossy(&invalidation)
        );

        tracker.write_all(b"QUIT\r\n").await.expect("quit tracker");
        let _ = read_reply(&mut tracker).await;
        target.write_all(b"QUIT\r\n").await.expect("quit target");
        let _ = read_reply(&mut target).await;
        writer.write_all(b"QUIT\r\n").await.expect("quit writer");
        let _ = read_reply(&mut writer).await;

        accept_task.await.expect("accept task join");
    }

    #[tokio::test]
    async fn client_tracking_redirect_disconnect_marks_broken_redirect_and_falls_back() {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind listener");
        let addr = listener.local_addr().expect("local addr");
        let shared = Arc::new(SharedState::new(ServerState::with_default_dbs()));

        let shared_for_accept = Arc::clone(&shared);
        let accept_task = tokio::spawn(async move {
            let (sock_tracker, _) = listener.accept().await.expect("accept tracker");
            let (sock_target, _) = listener.accept().await.expect("accept target");
            let (sock_writer, _) = listener.accept().await.expect("accept writer");

            let shared_tracker = Arc::clone(&shared_for_accept);
            let task_tracker = tokio::spawn(async move {
                handle_client(sock_tracker, shared_tracker)
                    .await
                    .expect("handle tracker");
            });
            let shared_target = Arc::clone(&shared_for_accept);
            let task_target = tokio::spawn(async move {
                handle_client(sock_target, shared_target)
                    .await
                    .expect("handle target");
            });
            let shared_writer = Arc::clone(&shared_for_accept);
            let task_writer = tokio::spawn(async move {
                handle_client(sock_writer, shared_writer)
                    .await
                    .expect("handle writer");
            });

            task_tracker.await.expect("join tracker");
            task_target.await.expect("join target");
            task_writer.await.expect("join writer");
        });

        let mut tracker = TcpStream::connect(addr).await.expect("connect tracker");
        let mut target = TcpStream::connect(addr).await.expect("connect target");
        let mut writer = TcpStream::connect(addr).await.expect("connect writer");

        tracker
            .write_all(b"*2\r\n$5\r\nHELLO\r\n$1\r\n3\r\n")
            .await
            .expect("switch tracker to resp3");
        let hello = read_reply(&mut tracker).await;
        assert!(
            hello.windows(b"proto".len()).any(|w| w == b"proto"),
            "expected HELLO map, got {:?}",
            String::from_utf8_lossy(&hello)
        );

        target
            .write_all(b"*2\r\n$6\r\nCLIENT\r\n$2\r\nID\r\n")
            .await
            .expect("request target id");
        let target_id = parse_integer_reply(&read_reply(&mut target).await);

        let tracking_command = format!(
            "*5\r\n$6\r\nCLIENT\r\n$8\r\nTRACKING\r\n$2\r\nON\r\n$8\r\nREDIRECT\r\n${}\r\n{}\r\n",
            target_id.to_string().len(),
            target_id
        );
        tracker
            .write_all(tracking_command.as_bytes())
            .await
            .expect("enable redirect tracking");
        assert_eq!(read_reply(&mut tracker).await, b"+OK\r\n");

        writer
            .write_all(b"*3\r\n$3\r\nSET\r\n$7\r\ntracked\r\n$2\r\nv1\r\n")
            .await
            .expect("seed tracked key");
        assert_eq!(read_reply(&mut writer).await, b"+OK\r\n");

        tracker
            .write_all(b"*2\r\n$3\r\nGET\r\n$7\r\ntracked\r\n")
            .await
            .expect("read tracked key");
        assert_eq!(read_reply(&mut tracker).await, b"$2\r\nv1\r\n");

        target.write_all(b"QUIT\r\n").await.expect("quit target");
        let _ = read_reply(&mut target).await;
        let target_id_text = target_id.to_string();
        let broken_redirect = read_reply(&mut tracker).await;
        assert_eq!(broken_redirect.first().copied(), Some(b'>'));
        assert!(
            broken_redirect
                .windows(b"tracking-redir-broken".len())
                .any(|w| w == b"tracking-redir-broken"),
            "expected tracking-redir-broken push, got {:?}",
            String::from_utf8_lossy(&broken_redirect)
        );
        assert!(
            broken_redirect
                .windows(target_id_text.len())
                .any(|w| w == target_id_text.as_bytes()),
            "expected broken redirect id in push, got {:?}",
            String::from_utf8_lossy(&broken_redirect)
        );

        tracker
            .write_all(b"*2\r\n$6\r\nCLIENT\r\n$8\r\nGETREDIR\r\n")
            .await
            .expect("request active redirect");
        assert_eq!(
            read_reply(&mut tracker).await,
            format!(":{}\r\n", target_id).as_bytes()
        );

        writer
            .write_all(b"*3\r\n$3\r\nSET\r\n$7\r\ntracked\r\n$2\r\nv2\r\n")
            .await
            .expect("update tracked key");
        assert_eq!(read_reply(&mut writer).await, b"+OK\r\n");

        let invalidation = read_reply(&mut tracker).await;
        assert!(
            invalidation
                .windows(b"invalidate".len())
                .any(|w| w == b"invalidate"),
            "expected invalidate push, got {:?}",
            String::from_utf8_lossy(&invalidation)
        );
        assert!(
            invalidation
                .windows(b"tracked".len())
                .any(|w| w == b"tracked"),
            "expected tracked key in invalidate push, got {:?}",
            String::from_utf8_lossy(&invalidation)
        );

        tracker.write_all(b"QUIT\r\n").await.expect("quit tracker");
        let _ = read_reply(&mut tracker).await;
        writer.write_all(b"QUIT\r\n").await.expect("quit writer");
        let _ = read_reply(&mut writer).await;

        accept_task.await.expect("accept task join");
    }

    #[tokio::test]
    async fn client_registry_reports_blocked_and_tracking_clients() {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind listener");
        let addr = listener.local_addr().expect("local addr");
        let shared = Arc::new(SharedState::new(ServerState::with_default_dbs()));

        let shared_for_accept = Arc::clone(&shared);
        let accept_task = tokio::spawn(async move {
            let (sock_blocked, _) = listener.accept().await.expect("accept blocked");
            let (sock_observer, _) = listener.accept().await.expect("accept observer");

            let shared_blocked = Arc::clone(&shared_for_accept);
            let task_blocked = tokio::spawn(async move {
                handle_client(sock_blocked, shared_blocked)
                    .await
                    .expect("handle blocked");
            });
            let shared_observer = Arc::clone(&shared_for_accept);
            let task_observer = tokio::spawn(async move {
                handle_client(sock_observer, shared_observer)
                    .await
                    .expect("handle observer");
            });

            task_blocked.await.expect("join blocked");
            task_observer.await.expect("join observer");
        });

        let mut blocked = TcpStream::connect(addr).await.expect("connect blocked");
        let mut observer = TcpStream::connect(addr).await.expect("connect observer");

        observer
            .write_all(b"*3\r\n$6\r\nCLIENT\r\n$8\r\nTRACKING\r\n$2\r\nON\r\n")
            .await
            .expect("enable tracking");
        assert_eq!(read_reply(&mut observer).await, b"+OK\r\n");

        blocked
            .write_all(b"*3\r\n$5\r\nBLPOP\r\n$7\r\nmissing\r\n$1\r\n1\r\n")
            .await
            .expect("start blocking pop");
        tokio::time::sleep(Duration::from_millis(50)).await;

        observer
            .write_all(b"*2\r\n$4\r\nINFO\r\n$7\r\nclients\r\n")
            .await
            .expect("info clients");
        let info = read_reply(&mut observer).await;
        let info_text = String::from_utf8_lossy(&info);
        assert!(info_text.contains("connected_clients:2"), "{info_text}");
        assert!(info_text.contains("blocked_clients:1"), "{info_text}");
        assert!(info_text.contains("tracking_clients:1"), "{info_text}");

        observer
            .write_all(b"*2\r\n$6\r\nCLIENT\r\n$4\r\nLIST\r\n")
            .await
            .expect("client list");
        let list = read_reply(&mut observer).await;
        let list_text = String::from_utf8_lossy(&list);
        assert!(list_text.contains("flags=Nt"), "{list_text}");
        assert!(list_text.contains("flags=Nb"), "{list_text}");

        let blocked_reply = read_reply(&mut blocked).await;
        assert!(
            blocked_reply == b"*-1\r\n" || blocked_reply == b"$-1\r\n",
            "unexpected BLPOP timeout reply: {:?}",
            String::from_utf8_lossy(&blocked_reply)
        );

        blocked.write_all(b"QUIT\r\n").await.expect("quit blocked");
        let _ = read_reply(&mut blocked).await;
        observer
            .write_all(b"QUIT\r\n")
            .await
            .expect("quit observer");
        let _ = read_reply(&mut observer).await;

        accept_task.await.expect("accept task join");
    }

    #[tokio::test]
    async fn blocking_list_pop_wakes_on_matching_write() {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind listener");
        let addr = listener.local_addr().expect("local addr");
        let shared = Arc::new(SharedState::new(ServerState::with_default_dbs()));

        let shared_for_accept = Arc::clone(&shared);
        let accept_task = tokio::spawn(async move {
            let (sock_blocked, _) = listener.accept().await.expect("accept blocked");
            let (sock_writer, _) = listener.accept().await.expect("accept writer");

            let shared_blocked = Arc::clone(&shared_for_accept);
            let task_blocked = tokio::spawn(async move {
                handle_client(sock_blocked, shared_blocked)
                    .await
                    .expect("handle blocked");
            });
            let shared_writer = Arc::clone(&shared_for_accept);
            let task_writer = tokio::spawn(async move {
                handle_client(sock_writer, shared_writer)
                    .await
                    .expect("handle writer");
            });

            task_blocked.await.expect("join blocked");
            task_writer.await.expect("join writer");
        });

        let mut blocked = TcpStream::connect(addr).await.expect("connect blocked");
        let mut writer = TcpStream::connect(addr).await.expect("connect writer");

        blocked
            .write_all(b"*3\r\n$5\r\nBLPOP\r\n$7\r\nwake-me\r\n$1\r\n5\r\n")
            .await
            .expect("start blocking pop");
        tokio::time::sleep(Duration::from_millis(50)).await;

        writer
            .write_all(b"*3\r\n$5\r\nLPUSH\r\n$7\r\nwake-me\r\n$7\r\npayload\r\n")
            .await
            .expect("push payload");
        assert_eq!(read_reply(&mut writer).await, b":1\r\n");

        let reply = read_reply(&mut blocked).await;
        let reply_text = String::from_utf8_lossy(&reply);
        assert!(reply_text.contains("wake-me"), "{reply_text}");
        assert!(reply_text.contains("payload"), "{reply_text}");

        blocked.write_all(b"QUIT\r\n").await.expect("quit blocked");
        let _ = read_reply(&mut blocked).await;
        writer.write_all(b"QUIT\r\n").await.expect("quit writer");
        let _ = read_reply(&mut writer).await;

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
        let config = crate::config::ServerConfig {
            dir: dir.path().to_path_buf(),
            appendonly: true,
            appendfsync: "always".to_string(),
            ..crate::config::ServerConfig::default()
        };
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

        // Find the actual AOF file written — may be the legacy filename or a
        // manifest-managed incremental file depending on bootstrap layout.
        let aof_content = find_aof_content(dir.path());
        assert!(
            aof_content.contains("SET"),
            "AOF should contain SET command"
        );
        assert!(aof_content.contains("foo"), "AOF should contain key 'foo'");
        assert!(
            aof_content.contains("bar"),
            "AOF should contain value 'bar'"
        );
    }
}
