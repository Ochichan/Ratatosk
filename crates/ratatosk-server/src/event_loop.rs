use std::{io, sync::Arc, time::Duration};

#[cfg(target_os = "linux")]
use std::fs;

use bytes::Bytes;
use ratatosk_core::time::now_ms;
use ratatosk_engine::{
    acl::AclState,
    eviction::{
        EvictionConfig, EvictionPolicy, estimate_used_memory, needs_eviction, perform_eviction,
    },
    expiry::{active_expire_cycle, detect_clock_jump},
    keyspace::{ServerState, SharedState},
};
use tokio::{
    net::TcpListener,
    sync::{OwnedSemaphorePermit, Semaphore},
    task::JoinSet,
    time::timeout,
};

use crate::{
    client::{ClientIoLimits, handle_client_with_limits},
    config::ServerConfig,
    persistence::{
        PersistenceRuntime, apply_server_persistence_config, drain_bgrewriteaof_tasks,
        drain_bgsave_tasks, flush_aof, load_startup_data, start_bgsave, sync_server_aof_file_info,
    },
    rate_limiter::ConnectionRateLimiter,
};

const ACCEPT_ERROR_BACKOFF_INITIAL: Duration = Duration::from_millis(50);
const ACCEPT_ERROR_BACKOFF_MAX: Duration = Duration::from_secs(2);
const MEMORY_ESTIMATE_INTERVAL: u64 = 10;
const DEFAULT_CONN_RATE_LIMIT_WINDOW_SECS: u64 = 10;
const DEFAULT_CONN_RATE_LIMIT_MAX_ATTEMPTS: usize = 10;
const ALLOW_DEFAULT_USER_NOPASS_ENV: &str = "RATATOSK_ALLOW_DEFAULT_USER_NOPASS";
const DEFAULT_USER_PASSWORD_ENV: &str = "RATATOSK_DEFAULT_USER_PASSWORD";
const DEFAULT_USER_PASSWORD_HASH_ENV: &str = "RATATOSK_DEFAULT_USER_PASSWORD_HASH";
const SHUTDOWN_BEST_EFFORT_ENV: &str = "RATATOSK_SHUTDOWN_BEST_EFFORT";

#[derive(Debug, Clone, Copy)]
enum ShutdownSignal {
    Interrupt,
    Terminate,
    #[cfg(not(unix))]
    CtrlC,
}

fn is_transient_accept_error(error: &io::Error) -> bool {
    if matches!(
        error.kind(),
        io::ErrorKind::Interrupted
            | io::ErrorKind::WouldBlock
            | io::ErrorKind::ConnectionAborted
            | io::ErrorKind::TimedOut
    ) {
        return true;
    }

    matches!(error.raw_os_error(), Some(11 | 23 | 24))
}

fn next_backoff(current: Duration) -> Duration {
    current
        .checked_mul(2)
        .unwrap_or(ACCEPT_ERROR_BACKOFF_MAX)
        .min(ACCEPT_ERROR_BACKOFF_MAX)
}

fn connection_rate_limit_window_from_env() -> Duration {
    std::env::var("RATATOSK_CONN_RATE_LIMIT_WINDOW_SEC")
        .ok()
        .and_then(|raw| raw.parse::<u64>().ok())
        .filter(|value| *value > 0)
        .map(Duration::from_secs)
        .unwrap_or_else(|| Duration::from_secs(DEFAULT_CONN_RATE_LIMIT_WINDOW_SECS))
}

fn connection_rate_limit_max_attempts_from_env() -> usize {
    std::env::var("RATATOSK_CONN_RATE_LIMIT_MAX_ATTEMPTS")
        .ok()
        .and_then(|raw| raw.parse::<usize>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(DEFAULT_CONN_RATE_LIMIT_MAX_ATTEMPTS)
}

fn env_truthy(name: &str) -> bool {
    std::env::var(name).is_ok_and(|value| {
        value == "1"
            || value.eq_ignore_ascii_case("true")
            || value.eq_ignore_ascii_case("yes")
            || value.eq_ignore_ascii_case("on")
    })
}

fn load_acl_state_from_disk(
    initial_state: &mut ServerState,
    config: &ServerConfig,
) -> io::Result<()> {
    let path = AclState::file_path(&config.dir);
    match AclState::load_from_file(&path)? {
        Some(loaded_acl) => {
            initial_state.acl = loaded_acl;
            tracing::info!(
                target = "ratatosk::startup",
                path = %path.display(),
                "loaded ACL state from disk"
            );
        }
        None => {
            tracing::debug!(
                target = "ratatosk::startup",
                path = %path.display(),
                "no ACL file found; using in-memory defaults"
            );
        }
    }

    Ok(())
}

fn bootstrap_default_user_for_bind(
    initial_state: &mut ServerState,
    config: &ServerConfig,
) -> io::Result<()> {
    if config.binds_to_loopback() {
        return Ok(());
    }

    if initial_state.acl.default_user_is_password_protected() {
        tracing::info!(
            target = "ratatosk::security",
            bind = %config.bind,
            "using persisted ACL state for password-protected default user"
        );
        return Ok(());
    }

    if env_truthy(ALLOW_DEFAULT_USER_NOPASS_ENV) {
        tracing::warn!(
            target = "ratatosk::security",
            bind = %config.bind,
            env = ALLOW_DEFAULT_USER_NOPASS_ENV,
            "default ACL user remains nopass on a non-loopback bind by explicit operator opt-in"
        );
        return Ok(());
    }

    let password_hash = if let Ok(hash) = std::env::var(DEFAULT_USER_PASSWORD_HASH_ENV) {
        if hash.is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("{DEFAULT_USER_PASSWORD_HASH_ENV} must not be empty"),
            ));
        }
        Bytes::from(hash)
    } else if let Ok(password) = std::env::var(DEFAULT_USER_PASSWORD_ENV) {
        if password.is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("{DEFAULT_USER_PASSWORD_ENV} must not be empty"),
            ));
        }
        AclState::hash_password(password.as_bytes()).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("failed to hash {DEFAULT_USER_PASSWORD_ENV}"),
            )
        })?
    } else {
        return Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            format!(
                "non-loopback bind '{}' requires {} or {} (or {}=true for an explicitly insecure deployment)",
                config.bind,
                DEFAULT_USER_PASSWORD_ENV,
                DEFAULT_USER_PASSWORD_HASH_ENV,
                ALLOW_DEFAULT_USER_NOPASS_ENV,
            ),
        ));
    };

    let default_user = initial_state
        .acl
        .get_or_create_user_mut(&Bytes::from_static(b"default"));
    default_user.enabled = true;
    default_user.nopass = false;
    default_user.passwords.clear();
    default_user.passwords.insert(password_hash);
    default_user.allow_all_commands = true;

    tracing::info!(
        target = "ratatosk::security",
        bind = %config.bind,
        "configured default ACL user to require a password on non-loopback bind"
    );

    Ok(())
}

fn shutdown_best_effort_enabled() -> bool {
    std::env::var(SHUTDOWN_BEST_EFFORT_ENV).is_ok_and(|value| {
        value == "1"
            || value.eq_ignore_ascii_case("true")
            || value.eq_ignore_ascii_case("yes")
            || value.eq_ignore_ascii_case("on")
    })
}

fn io_error_kind_label(error: &io::Error) -> String {
    format!("{:?}", error.kind()).to_ascii_lowercase()
}

fn is_expected_client_disconnect(error: &io::Error) -> bool {
    matches!(
        error.kind(),
        io::ErrorKind::BrokenPipe
            | io::ErrorKind::ConnectionReset
            | io::ErrorKind::ConnectionAborted
            | io::ErrorKind::TimedOut
            | io::ErrorKind::UnexpectedEof
    )
}

#[cfg(target_os = "linux")]
fn linux_open_fd_metrics() -> Option<(u64, u64)> {
    let open_fds = fs::read_dir("/proc/self/fd").ok()?.count() as u64;
    let limits = fs::read_to_string("/proc/self/limits").ok()?;
    let mut soft_limit = None;
    for line in limits.lines() {
        if !line.starts_with("Max open files") {
            continue;
        }

        let rest = line.trim_start_matches("Max open files").trim();
        let token = rest.split_whitespace().next()?;
        if token.eq_ignore_ascii_case("unlimited") {
            soft_limit = Some(u64::MAX);
        } else {
            soft_limit = token.parse::<u64>().ok();
        }
        break;
    }

    soft_limit.map(|limit| (open_fds, limit))
}

fn emit_fd_metrics() {
    #[cfg(target_os = "linux")]
    if let Some((open_fds, limit)) = linux_open_fd_metrics() {
        crate::metrics::set_open_fds(open_fds, limit);
    }
}

#[cfg(unix)]
async fn wait_for_shutdown_signal() -> io::Result<ShutdownSignal> {
    use tokio::signal::unix::{SignalKind, signal};

    let mut sigint = signal(SignalKind::interrupt())?;
    let mut sigterm = signal(SignalKind::terminate())?;

    tokio::select! {
        _ = sigint.recv() => Ok(ShutdownSignal::Interrupt),
        _ = sigterm.recv() => Ok(ShutdownSignal::Terminate),
    }
}

#[cfg(not(unix))]
async fn wait_for_shutdown_signal() -> io::Result<ShutdownSignal> {
    tokio::signal::ctrl_c().await.map_err(io::Error::other)?;
    Ok(ShutdownSignal::CtrlC)
}

async fn drain_client_tasks(tasks: &mut JoinSet<()>, grace_period: Duration) {
    if tasks.is_empty() {
        return;
    }

    let start = std::time::Instant::now();
    let initial_count = tasks.len();

    tracing::info!(
        target = "ratatosk::shutdown",
        active_clients = initial_count,
        grace_ms = grace_period.as_millis(),
        "beginning client drain"
    );

    let drain = async {
        while let Some(result) = tasks.join_next().await {
            if let Err(error) = result {
                if error.is_cancelled() {
                    tracing::debug!(
                        target = "ratatosk::shutdown",
                        "client task cancelled during shutdown"
                    );
                } else {
                    tracing::warn!(target = "ratatosk::shutdown", error = %error, "client task join failure during shutdown");
                }
            }
        }
    };

    if timeout(grace_period, drain).await.is_ok() {
        tracing::info!(
            target = "ratatosk::shutdown",
            drained_clients = initial_count,
            elapsed_ms = start.elapsed().as_millis(),
            "all client handlers drained successfully"
        );
        return;
    }

    let remaining = tasks.len();
    let aborted = initial_count - remaining;

    tracing::warn!(
        target = "ratatosk::shutdown",
        remaining_clients = remaining,
        aborted_clients = aborted,
        elapsed_ms = start.elapsed().as_millis(),
        "shutdown grace period expired; aborting remaining client handlers"
    );

    crate::metrics::record_shutdown_clients_aborted(remaining as u64);

    tasks.abort_all();

    while let Some(result) = tasks.join_next().await {
        if let Err(error) = result {
            if !error.is_cancelled() {
                tracing::warn!(target = "ratatosk::shutdown", error = %error, "client task join failure after abort");
            }
        }
    }
}

async fn flush_persistence_before_shutdown(
    appendonly: bool,
    persistence: &PersistenceRuntime,
) -> io::Result<bool> {
    if !appendonly {
        crate::metrics::record_shutdown_aof_flush_result("skipped", "appendonly_disabled");
        return Ok(false);
    }

    let started_at = std::time::Instant::now();
    let best_effort = shutdown_best_effort_enabled();

    match flush_aof(persistence).await {
        Ok(()) => {
            let duration_ms = started_at.elapsed().as_secs_f64() * 1000.0;
            crate::metrics::record_shutdown_aof_flush_duration_ms(duration_ms, "success");
            crate::metrics::record_shutdown_aof_flush_result("success", "ok");
            tracing::info!(
                target = "ratatosk::shutdown",
                duration_ms = duration_ms,
                "AOF flushed before shutdown"
            );
            Ok(true)
        }
        Err(error) => {
            let duration_ms = started_at.elapsed().as_secs_f64() * 1000.0;
            let error_kind = io_error_kind_label(&error);
            crate::metrics::record_shutdown_aof_flush_duration_ms(duration_ms, "error");
            crate::metrics::record_shutdown_aof_flush_result(
                if best_effort {
                    "error_best_effort"
                } else {
                    "error_fatal"
                },
                &error_kind,
            );

            if best_effort {
                tracing::warn!(
                    target = "ratatosk::shutdown",
                    error = %error,
                    error_kind = %error_kind,
                    duration_ms = duration_ms,
                    best_effort = true,
                    override_env = SHUTDOWN_BEST_EFFORT_ENV,
                    "failed to flush AOF before shutdown; continuing due to best-effort mode"
                );
                Ok(false)
            } else {
                tracing::error!(
                    target = "ratatosk::shutdown",
                    error = %error,
                    error_kind = %error_kind,
                    duration_ms = duration_ms,
                    best_effort = false,
                    override_env = SHUTDOWN_BEST_EFFORT_ENV,
                    "failed to flush AOF before shutdown"
                );
                Err(io::Error::new(
                    error.kind(),
                    format!("failed to flush AOF before shutdown: {error}"),
                ))
            }
        }
    }
}

fn finalize_shutdown_result(
    fatal_error: Option<io::Error>,
    shutdown_flush_error: Option<io::Error>,
) -> io::Result<()> {
    match (fatal_error, shutdown_flush_error) {
        (Some(fatal), Some(flush)) => Err(io::Error::other(format!(
            "shutdown encountered multiple errors: fatal_runtime_error={fatal}; shutdown_flush_error={flush}"
        ))),
        (Some(fatal), None) => Err(fatal),
        (None, Some(flush)) => Err(flush),
        (None, None) => Ok(()),
    }
}

/// Build eviction config from current server config state.
fn build_eviction_config(state: &ServerState) -> EvictionConfig {
    let policy = EvictionPolicy::from_config_str(state.config.maxmemory_policy())
        .unwrap_or(EvictionPolicy::NoEviction);
    EvictionConfig {
        policy,
        maxmemory: state.config.maxmemory(),
        maxmemory_samples: state.config.maxmemory_samples(),
    }
}

/// server_cron housekeeping — called at `hz` frequency.
///
/// 1. Active expiry cycle (sampling-based)
/// 2. Eviction check (maxmemory)
/// 3. Ops/sec sampling (every `ops_sec_interval` ticks)
async fn server_cron(server_state: &Arc<SharedState>, cron_tick: &mut u64, ops_sec_interval: u64) {
    detect_clock_jump();

    let mut server = server_state.meta.lock().await;
    let current_ms = now_ms();

    // 1. Active expiry cycle
    let expired = active_expire_cycle(&mut server, current_ms);
    if expired > 0 {
        tracing::debug!(expired, "active expiry cycle removed keys");
    }

    // 2. Eviction check (with cached memory estimate)
    let eviction_config = build_eviction_config(&server);
    if eviction_config.maxmemory > 0 {
        let used = if *cron_tick % MEMORY_ESTIMATE_INTERVAL == 0 {
            let estimate = estimate_used_memory(&server);
            server
                .stats
                .set_cached_memory_estimate(estimate as u64, *cron_tick);
            // Record memory metric
            crate::metrics::set_memory_used(estimate as u64);
            estimate
        } else {
            server.stats.cached_memory_estimate() as usize
        };

        if needs_eviction(used, &eviction_config) {
            let evicted = perform_eviction(&mut server, &eviction_config);
            if evicted > 0 {
                tracing::info!(
                    target = "ratatosk::eviction",
                    evicted,
                    policy = eviction_config.policy.as_str(),
                    "eviction cycle removed keys"
                );
            }
        }
    }

    let memory_estimate_age_ticks =
        (*cron_tick).saturating_sub(server.stats.last_memory_estimate_tick());
    crate::metrics::set_memory_estimate_age_ticks(memory_estimate_age_ticks);

    // 3. Ops/sec sampling
    *cron_tick = cron_tick.wrapping_add(1);
    if *cron_tick % ops_sec_interval == 0 {
        server.stats.catch_up_from_atomic(&server_state.stats);
        let interval_secs =
            ops_sec_interval.saturating_mul(1000) / u64::from(server.config.hz().max(1)) / 1000;
        server.stats.sample_ops_per_sec(interval_secs.max(1));
    }

    server_state.stats.catch_up_from_stats(&server.stats);
}

pub async fn run(config: ServerConfig) -> io::Result<()> {
    let listen_addr = config.listen_addr();
    let listener = TcpListener::bind(&listen_addr).await.map_err(|error| {
        io::Error::new(
            error.kind(),
            format!("binding TCP listener on {listen_addr}: {error}"),
        )
    })?;

    if config.port == 6379 {
        tracing::warn!(
            target = "ratatosk::startup",
            port = 6379,
            "listening on the default Redis port (6379); this may conflict with a co-located Redis instance. \
             Consider RATATOSK_PORT=6380 for coexistence."
        );
    }

    emit_fd_metrics();

    // Lazy-free background thread for async deletion of large values
    const LAZY_FREE_CHANNEL_SIZE: usize = 4096;
    let (lazy_free_tx, lazy_free_rx) = crossbeam_channel::bounded::<
        ratatosk_engine::keyspace::StoredValue,
    >(LAZY_FREE_CHANNEL_SIZE);
    let lazy_free_shutdown = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let lazy_free_flag = Arc::clone(&lazy_free_shutdown);
    let lazy_free_handle = std::thread::spawn(move || {
        while !lazy_free_flag.load(std::sync::atomic::Ordering::Acquire) {
            match lazy_free_rx.recv_timeout(Duration::from_millis(100)) {
                Ok(value) => drop(value),
                Err(crossbeam_channel::RecvTimeoutError::Timeout) => {}
                Err(crossbeam_channel::RecvTimeoutError::Disconnected) => break,
            }
        }
        // Drain remaining values
        for value in lazy_free_rx.try_iter() {
            drop(value);
        }
    });

    // Spawn lazy-free channel monitor
    let lazy_free_monitor_handle = tokio::spawn({
        let lazy_free_tx = lazy_free_tx.clone();
        async move {
            let mut interval = tokio::time::interval(Duration::from_secs(10));
            loop {
                interval.tick().await;

                // Estimate channel utilization (approximate)
                let capacity = LAZY_FREE_CHANNEL_SIZE as f64;
                let available = lazy_free_tx.capacity().unwrap_or(0) as f64;
                let utilization = 1.0 - (available / capacity);

                crate::metrics::set_lazyfree_queue_utilization(utilization);

                if utilization > 0.9 {
                    tracing::warn!(
                        target = "ratatosk::memory",
                        utilization = utilization,
                        capacity = LAZY_FREE_CHANNEL_SIZE,
                        "lazy-free channel nearing capacity"
                    );
                }
            }
        }
    });

    let mut initial_state = ServerState::with_default_dbs();
    load_acl_state_from_disk(&mut initial_state, &config)?;
    bootstrap_default_user_for_bind(&mut initial_state, &config)?;
    initial_state.set_lazy_free_sender(lazy_free_tx);
    let server_state = Arc::new(SharedState::new(initial_state));
    apply_server_persistence_config(&server_state, &config).await;

    let persistence = Arc::new(PersistenceRuntime::from_config(&config).map_err(|error| {
        io::Error::new(
            error.kind(),
            format!(
                "initializing persistence runtime (dir={}, appendonly={}): {}",
                config.dir.display(),
                config.appendonly,
                error
            ),
        )
    })?);
    sync_server_aof_file_info(&server_state, &persistence)
        .await
        .map_err(|error| {
            io::Error::new(
                error.kind(),
                format!("syncing AOF file info after persistence init: {error}"),
            )
        })?;
    load_startup_data(&server_state, &persistence, config.appendonly)
        .await
        .map_err(|error| {
            io::Error::new(
                error.kind(),
                format!("loading startup persistence data: {error}"),
            )
        })?;

    let client_permits = Arc::new(Semaphore::new(config.max_clients));
    crate::metrics::set_semaphore_available_permits(client_permits.available_permits());

    let io_limits = ClientIoLimits {
        output_buffer_limit_bytes: config.output_buffer_limit_bytes,
        client_read_timeout_sec: config.client_timeout_sec,
    };

    let mut shutdown = Box::pin(wait_for_shutdown_signal());
    let mut client_tasks: JoinSet<()> = JoinSet::new();
    let mut accept_backoff = ACCEPT_ERROR_BACKOFF_INITIAL;
    let mut fatal_error: Option<io::Error> = None;

    let rate_limit_window = connection_rate_limit_window_from_env();
    let rate_limit_max_attempts = connection_rate_limit_max_attempts_from_env();
    let mut rate_limiter = ConnectionRateLimiter::new(rate_limit_window, rate_limit_max_attempts);
    tracing::info!(
        target = "ratatosk::startup",
        rate_limit_window_sec = rate_limit_window.as_secs(),
        rate_limit_max_attempts,
        "connection rate limiter configured"
    );

    // server_cron timer — default 10 Hz (lock-free config read)
    let mut cron_hz = server_state.config_cache.load().hz().max(1);
    let mut cron_period = Duration::from_millis(1000 / u64::from(cron_hz));
    let mut cron_interval = tokio::time::interval(cron_period);
    cron_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut cron_tick: u64 = 0;
    let mut ops_sec_interval = u64::from(cron_hz);

    // SIGUSR1 signal handler (Unix only) for triggering RDB save
    #[cfg(unix)]
    let mut sigusr1 = {
        use tokio::signal::unix::{SignalKind, signal};
        signal(SignalKind::user_defined1()).map_err(|error| {
            io::Error::new(error.kind(), format!("installing SIGUSR1 handler: {error}"))
        })?
    };

    loop {
        let configured_hz = server_state.config_cache.load().hz().max(1);
        if configured_hz != cron_hz {
            cron_hz = configured_hz;
            cron_period = Duration::from_millis(1000 / u64::from(cron_hz));
            cron_interval = tokio::time::interval(cron_period);
            cron_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            ops_sec_interval = u64::from(cron_hz);
            tracing::info!(
                target = "ratatosk::config",
                hz = cron_hz,
                period_ms = cron_period.as_millis(),
                "server cron frequency updated"
            );
        }

        // Wrap SIGUSR1 as a future that resolves to a flag.
        // On non-unix, use a pending future that never resolves.
        #[cfg(unix)]
        let sigusr1_recv = sigusr1.recv();
        #[cfg(not(unix))]
        let sigusr1_recv = std::future::pending::<Option<()>>();

        tokio::select! {
            signal = &mut shutdown => {
                match signal {
                    Ok(kind) => {
                        tracing::info!(
                            ?kind,
                            grace_ms = config.shutdown_grace_period_ms,
                            "shutdown signal received"
                        );
                    }
                    Err(error) => {
                        tracing::warn!(
                            error = %error,
                            grace_ms = config.shutdown_grace_period_ms,
                            "failed to wait for shutdown signal; initiating shutdown"
                        );
                    }
                }
                break;
            }
            accepted = listener.accept() => {
                let (stream, addr) = match accepted {
                    Ok(accepted) => {
                        accept_backoff = ACCEPT_ERROR_BACKOFF_INITIAL;
                        accepted
                    }
                    Err(error) if is_transient_accept_error(&error) => {
                        let kind = io_error_kind_label(&error);
                        crate::metrics::record_accept_error(&kind, true);
                        crate::metrics::record_accept_backoff(accept_backoff.as_millis() as f64);
                        tracing::debug!(
                            error = %error,
                            backoff_ms = accept_backoff.as_millis(),
                            "transient accept error; retrying"
                        );
                        tokio::time::sleep(accept_backoff).await;
                        accept_backoff = next_backoff(accept_backoff);
                        continue;
                    }
                    Err(error) => {
                        let kind = io_error_kind_label(&error);
                        crate::metrics::record_accept_error(&kind, false);
                        let wrapped = io::Error::new(
                            error.kind(),
                            format!("accepting TCP connection on {listen_addr}: {error}"),
                        );
                        tracing::error!(
                            target = "ratatosk::network",
                            error = %wrapped,
                            kind = %kind,
                            "fatal listener accept error; initiating graceful shutdown"
                        );
                        fatal_error = Some(wrapped);
                        break;
                    }
                };

                // Rate limiting check
                let within_rate_limit = rate_limiter.check_rate_limit(addr.ip());
                crate::metrics::set_rate_limiter_tracked_ips(rate_limiter.tracked_ips());
                if !within_rate_limit {
                    crate::metrics::record_connection_rejected("rate_limited");
                    tracing::warn!(
                        target = "ratatosk::security",
                        remote_addr = %addr,
                        "rejecting connection: rate limit exceeded"
                    );
                    drop(stream);
                    continue;
                }

                let permit: OwnedSemaphorePermit = match Arc::clone(&client_permits).try_acquire_owned() {
                    Ok(permit) => permit,
                    Err(_) => {
                        crate::metrics::record_connection_rejected("max_clients");
                        crate::metrics::set_semaphore_available_permits(client_permits.available_permits());
                        tracing::warn!(
                            remote_addr = %addr,
                            max_clients = config.max_clients,
                            "rejecting connection: max concurrent client limit reached"
                        );
                        drop(stream);
                        continue;
                    }
                };
                crate::metrics::set_semaphore_available_permits(client_permits.available_permits());

                let state = Arc::clone(&server_state);
                let persistence = Arc::clone(&persistence);
                let remote_addr = addr;
                client_tasks.spawn(async move {
                    let _permit = permit;
                    if let Err(error) = handle_client_with_limits(stream, state, persistence, io_limits).await {
                        if is_expected_client_disconnect(&error) {
                            tracing::debug!(remote_addr = %remote_addr, error = %error, "client disconnected");
                        } else {
                            tracing::warn!(remote_addr = %remote_addr, error = %error, "client handler error");
                        }
                    }
                });
            }
            _ = cron_interval.tick() => {
                crate::metrics::set_semaphore_available_permits(client_permits.available_permits());
                emit_fd_metrics();
                server_cron(&server_state, &mut cron_tick, ops_sec_interval).await;
            }
            _ = sigusr1_recv => {
                if start_bgsave(Arc::clone(&server_state), persistence.rdb_path.clone()).await {
                    tracing::info!("SIGUSR1 received: background RDB save started");
                } else {
                    tracing::warn!("SIGUSR1 received but RDB save already in progress");
                }
            }
        }
    }

    client_permits.close();
    crate::metrics::set_semaphore_available_permits(client_permits.available_permits());
    let grace_period = Duration::from_millis(config.shutdown_grace_period_ms);
    drain_client_tasks(&mut client_tasks, grace_period).await;
    let mut shutdown_flush_error = None;

    let (bgsave_completed, bgsave_aborted) = drain_bgsave_tasks(grace_period).await;
    if bgsave_aborted > 0 {
        tracing::warn!(
            target = "ratatosk::shutdown",
            completed = bgsave_completed,
            aborted = bgsave_aborted,
            "background save tasks exceeded shutdown deadline"
        );
    } else if bgsave_completed > 0 {
        tracing::info!(
            target = "ratatosk::shutdown",
            completed = bgsave_completed,
            "background save tasks drained"
        );
    }
    let (rewrite_completed, rewrite_aborted) = drain_bgrewriteaof_tasks(grace_period).await;
    if rewrite_aborted > 0 {
        tracing::warn!(
            target = "ratatosk::shutdown",
            completed = rewrite_completed,
            aborted = rewrite_aborted,
            "background AOF rewrite tasks exceeded shutdown deadline"
        );
    } else if rewrite_completed > 0 {
        tracing::info!(
            target = "ratatosk::shutdown",
            completed = rewrite_completed,
            "background AOF rewrite tasks drained"
        );
    }

    if let Err(error) = flush_persistence_before_shutdown(config.appendonly, &persistence).await {
        shutdown_flush_error = Some(error);
    }

    // Shut down lazy-free background thread
    lazy_free_shutdown.store(true, std::sync::atomic::Ordering::Release);
    if let Err(error) = lazy_free_handle.join() {
        tracing::warn!("lazy-free thread panicked: {error:?}");
    }

    // Abort lazy-free monitor task
    lazy_free_monitor_handle.abort();

    if fatal_error.is_none() && shutdown_flush_error.is_none() {
        tracing::info!("server shutdown complete");
    } else {
        tracing::warn!(
            target = "ratatosk::shutdown",
            "server shutdown completed with errors"
        );
    }

    finalize_shutdown_result(fatal_error, shutdown_flush_error)
}

#[cfg(test)]
mod tests {
    use std::{
        io,
        sync::{Mutex, OnceLock},
        time::{Duration, Instant},
    };

    use bytes::Bytes;
    use ratatosk_engine::{acl::AclState, keyspace::ServerState};
    use tokio::task::JoinSet;

    use super::{
        ALLOW_DEFAULT_USER_NOPASS_ENV, DEFAULT_USER_PASSWORD_ENV, SHUTDOWN_BEST_EFFORT_ENV,
        ServerConfig, bootstrap_default_user_for_bind, drain_client_tasks,
        finalize_shutdown_result, load_acl_state_from_disk, shutdown_best_effort_enabled,
    };

    fn env_guard() -> std::sync::MutexGuard<'static, ()> {
        static ENV_LOCK: OnceLock<Mutex<()>> = OnceLock::new();
        ENV_LOCK
            .get_or_init(|| Mutex::new(()))
            .lock()
            .expect("env lock")
    }

    #[tokio::test]
    async fn drain_client_tasks_completes_ready_tasks() {
        let mut tasks: JoinSet<()> = JoinSet::new();
        tasks.spawn(async {});
        tasks.spawn(async {});

        drain_client_tasks(&mut tasks, Duration::from_millis(100)).await;

        assert!(tasks.is_empty());
    }

    #[tokio::test]
    async fn drain_client_tasks_aborts_stuck_tasks_after_grace() {
        let mut tasks: JoinSet<()> = JoinSet::new();
        tasks.spawn(async {
            tokio::time::sleep(Duration::from_secs(60)).await;
        });

        let started_at = Instant::now();
        drain_client_tasks(&mut tasks, Duration::from_millis(20)).await;

        assert!(tasks.is_empty());
        assert!(started_at.elapsed() < Duration::from_secs(1));
    }

    #[test]
    fn shutdown_best_effort_defaults_to_false() {
        // SAFETY: test-only env isolation for this process.
        unsafe { std::env::remove_var(SHUTDOWN_BEST_EFFORT_ENV) };
        assert!(!shutdown_best_effort_enabled());
    }

    #[test]
    fn shutdown_best_effort_reads_truthy_env() {
        // SAFETY: test-only env isolation for this process.
        unsafe { std::env::set_var(SHUTDOWN_BEST_EFFORT_ENV, "true") };
        assert!(shutdown_best_effort_enabled());
        // SAFETY: test-only env isolation for this process.
        unsafe { std::env::remove_var(SHUTDOWN_BEST_EFFORT_ENV) };
    }

    #[test]
    fn finalize_shutdown_result_combines_runtime_and_flush_errors() {
        let result = finalize_shutdown_result(
            Some(io::Error::other("listener failure")),
            Some(io::Error::new(io::ErrorKind::TimedOut, "flush timeout")),
        )
        .expect_err("combined shutdown errors should fail");

        let text = result.to_string();
        assert!(
            text.contains("fatal_runtime_error=listener failure"),
            "{text}"
        );
        assert!(
            text.contains("shutdown_flush_error=failed to flush AOF before shutdown")
                || text.contains("shutdown_flush_error=flush timeout"),
            "{text}"
        );
    }

    #[test]
    fn non_loopback_bind_requires_bootstrap_password_by_default() {
        let _guard = env_guard();
        // SAFETY: test-only env isolation guarded by a process-wide mutex.
        unsafe {
            std::env::remove_var(DEFAULT_USER_PASSWORD_ENV);
            std::env::remove_var(ALLOW_DEFAULT_USER_NOPASS_ENV);
        }

        let mut server = ServerState::with_default_dbs();
        let config = ServerConfig {
            bind: "0.0.0.0".to_string(),
            ..ServerConfig::default()
        };

        let error =
            bootstrap_default_user_for_bind(&mut server, &config).expect_err("expected error");
        assert_eq!(error.kind(), io::ErrorKind::PermissionDenied);
    }

    #[test]
    fn non_loopback_bind_bootstraps_default_user_password() {
        let _guard = env_guard();
        // SAFETY: test-only env isolation guarded by a process-wide mutex.
        unsafe {
            std::env::set_var(DEFAULT_USER_PASSWORD_ENV, "bootstrap-secret");
            std::env::remove_var(ALLOW_DEFAULT_USER_NOPASS_ENV);
        }

        let mut server = ServerState::with_default_dbs();
        let config = ServerConfig {
            bind: "0.0.0.0".to_string(),
            ..ServerConfig::default()
        };
        bootstrap_default_user_for_bind(&mut server, &config).expect("bootstrap password");

        let default_user = server
            .acl
            .get_user(&Bytes::from_static(b"default"))
            .expect("default user");
        assert!(!default_user.nopass);
        assert_eq!(default_user.passwords.len(), 1);

        // SAFETY: test-only env isolation guarded by a process-wide mutex.
        unsafe {
            std::env::remove_var(DEFAULT_USER_PASSWORD_ENV);
        }
    }

    #[test]
    fn load_acl_state_from_disk_restores_saved_acl_users() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = AclState::file_path(dir.path());

        let mut acl = AclState::default();
        let user = acl.get_or_create_user_mut(&Bytes::from_static(b"alice"));
        user.enabled = true;
        user.nopass = false;
        user.passwords
            .insert(Bytes::from_static(b"$argon2id$v=19$m=19456,t=2,p=1$hash"));
        user.allow_all_commands = false;
        user.allowed_categories.insert(Bytes::from_static(b"read"));
        acl.save_to_file(&path).expect("persist ACL");

        let config = ServerConfig {
            dir: dir.path().to_path_buf(),
            ..ServerConfig::default()
        };
        let mut server = ServerState::with_default_dbs();
        load_acl_state_from_disk(&mut server, &config).expect("load ACL");

        let alice = server
            .acl
            .get_user(&Bytes::from_static(b"alice"))
            .expect("alice should be restored");
        assert!(alice.enabled);
        assert!(!alice.nopass);
        assert!(!alice.allow_all_commands);
        assert!(
            alice
                .passwords
                .contains(b"$argon2id$v=19$m=19456,t=2,p=1$hash" as &[u8])
        );
        assert!(alice.allowed_categories.contains(b"read" as &[u8]));
    }

    #[test]
    fn non_loopback_bind_accepts_preloaded_password_protected_default_user() {
        let _guard = env_guard();
        // SAFETY: test-only env isolation guarded by a process-wide mutex.
        unsafe {
            std::env::remove_var(DEFAULT_USER_PASSWORD_ENV);
            std::env::remove_var(ALLOW_DEFAULT_USER_NOPASS_ENV);
        }

        let mut server = ServerState::with_default_dbs();
        let default_user = server
            .acl
            .get_or_create_user_mut(&Bytes::from_static(b"default"));
        default_user.enabled = true;
        default_user.nopass = false;
        default_user
            .passwords
            .insert(Bytes::from_static(b"$argon2id$v=19$m=19456,t=2,p=1$hash"));

        let config = ServerConfig {
            bind: "0.0.0.0".to_string(),
            ..ServerConfig::default()
        };

        bootstrap_default_user_for_bind(&mut server, &config)
            .expect("preloaded ACL should satisfy non-loopback security check");
    }
}
