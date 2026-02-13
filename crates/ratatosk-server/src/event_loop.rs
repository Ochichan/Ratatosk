use std::{io, sync::Arc, time::Duration};

use ratatosk_core::time::now_ms;
use ratatosk_engine::{
    eviction::{EvictionConfig, EvictionPolicy, estimate_used_memory, needs_eviction, perform_eviction},
    expiry::{active_expire_cycle, detect_clock_jump},
    keyspace::ServerState,
};
use tokio::{
    net::TcpListener,
    sync::{Mutex, OwnedSemaphorePermit, Semaphore},
    task::JoinSet,
    time::timeout,
};

use crate::{
    client::{ClientIoLimits, handle_client_with_limits},
    config::ServerConfig,
    persistence::{
        PersistenceRuntime, apply_server_persistence_config, flush_aof, load_startup_data,
        start_bgsave,
    },
    rate_limiter::ConnectionRateLimiter,
};

const ACCEPT_ERROR_BACKOFF_INITIAL: Duration = Duration::from_millis(50);
const ACCEPT_ERROR_BACKOFF_MAX: Duration = Duration::from_secs(2);
const MEMORY_ESTIMATE_INTERVAL: u64 = 10;

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
                    tracing::debug!(target = "ratatosk::shutdown", "client task cancelled during shutdown");
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
async fn server_cron(
    server_state: &Arc<Mutex<ServerState>>,
    cron_tick: &mut u64,
    ops_sec_interval: u64,
) {
    detect_clock_jump();
    
    let mut server = server_state.lock().await;
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
            server.stats.set_cached_memory_estimate(estimate as u64, *cron_tick);
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

    // 3. Ops/sec sampling
    *cron_tick = cron_tick.wrapping_add(1);
    if *cron_tick % ops_sec_interval == 0 {
        let interval_secs = ops_sec_interval.saturating_mul(1000)
            / u64::from(server.config.hz().max(1))
            / 1000;
        server.stats.sample_ops_per_sec(interval_secs.max(1));
    }
}

pub async fn run(config: ServerConfig) -> io::Result<()> {
    let listener = TcpListener::bind(config.listen_addr()).await?;

    // Lazy-free background thread for async deletion of large values
    const LAZY_FREE_CHANNEL_SIZE: usize = 4096;
    let (lazy_free_tx, lazy_free_rx) =
        crossbeam_channel::bounded::<ratatosk_engine::keyspace::StoredValue>(LAZY_FREE_CHANNEL_SIZE);
    let lazy_free_shutdown = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let lazy_free_flag = Arc::clone(&lazy_free_shutdown);
    let lazy_free_handle = std::thread::spawn(move || {
        while !lazy_free_flag.load(std::sync::atomic::Ordering::Relaxed) {
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
    initial_state.set_lazy_free_sender(lazy_free_tx);
    let server_state = Arc::new(Mutex::new(initial_state));
    apply_server_persistence_config(&server_state, &config).await;

    let persistence = Arc::new(PersistenceRuntime::from_config(&config)?);
    load_startup_data(&server_state, &persistence, config.appendonly).await?;

    let client_permits = Arc::new(Semaphore::new(config.max_clients));
    let io_limits = ClientIoLimits {
        output_buffer_limit_bytes: config.output_buffer_limit_bytes,
        client_read_timeout_sec: config.client_timeout_sec,
    };

    let mut shutdown = Box::pin(wait_for_shutdown_signal());
    let mut client_tasks: JoinSet<()> = JoinSet::new();
    let mut accept_backoff = ACCEPT_ERROR_BACKOFF_INITIAL;
    
    // Connection rate limiter (10 connections per 10 seconds per IP)
    let mut rate_limiter = ConnectionRateLimiter::default();

    // server_cron timer — default 10 Hz
    let cron_hz = {
        let server = server_state.lock().await;
        server.config.hz()
    };
    let cron_period = Duration::from_millis(1000 / u64::from(cron_hz.max(1)));
    let mut cron_interval = tokio::time::interval(cron_period);
    cron_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut cron_tick: u64 = 0;
    let ops_sec_interval = u64::from(cron_hz.max(1));

    // SIGUSR1 signal handler (Unix only) for triggering RDB save
    #[cfg(unix)]
    let mut sigusr1 = {
        use tokio::signal::unix::{SignalKind, signal};
        signal(SignalKind::user_defined1())?
    };

    loop {
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
                        tracing::warn!(
                            error = %error,
                            backoff_ms = accept_backoff.as_millis(),
                            "transient accept error; retrying"
                        );
                        tokio::time::sleep(accept_backoff).await;
                        accept_backoff = next_backoff(accept_backoff);
                        continue;
                    }
                    Err(error) => return Err(error),
                };

                // Rate limiting check
                if !rate_limiter.check_rate_limit(addr.ip()) {
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
                        tracing::warn!(
                            remote_addr = %addr,
                            max_clients = config.max_clients,
                            "rejecting connection: max concurrent client limit reached"
                        );
                        drop(stream);
                        continue;
                    }
                };

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
    let grace_period = Duration::from_millis(config.shutdown_grace_period_ms);
    drain_client_tasks(&mut client_tasks, grace_period).await;

    if config.appendonly {
        if let Err(error) = flush_aof(&persistence).await {
            tracing::warn!(error = %error, "failed to flush AOF before shutdown");
        }
    }
    tracing::info!("persistence flushed before shutdown");

    // Shut down lazy-free background thread
    lazy_free_shutdown.store(true, std::sync::atomic::Ordering::Relaxed);
    if let Err(error) = lazy_free_handle.join() {
        tracing::warn!("lazy-free thread panicked: {error:?}");
    }

    // Abort lazy-free monitor task
    lazy_free_monitor_handle.abort();

    tracing::info!("server shutdown complete");

    Ok(())
}

#[cfg(test)]
mod tests {
    use std::time::{Duration, Instant};

    use tokio::task::JoinSet;

    use super::drain_client_tasks;

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
}
