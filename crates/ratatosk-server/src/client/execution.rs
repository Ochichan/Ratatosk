use super::shared_support::{
    frame_to_argv_for_persistence, is_queued_response, refresh_client_snapshot,
};
use super::*;

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

#[allow(clippy::too_many_arguments)]
pub(super) async fn run_with_blocking_retry(
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

    let command_name = first_argv
        .as_ref()
        .and_then(|argv| {
            argv.first()
                .map(|cmd| String::from_utf8_lossy(cmd).to_ascii_uppercase())
        })
        .unwrap_or_else(|| "UNKNOWN".to_string());

    let is_write_operation = first_argv.as_deref().is_some_and(is_write_command);

    breadcrumbs::record_command(client_state.id(), &command_name, first_db, 0, "execute");

    let mut outcome = if let Some(argv) = first_argv.as_deref() {
        let fast_started = std::time::Instant::now();
        if let Some(outcome) = try_execute_lock_free_fast_command(argv, server_state, client_state)
        {
            let lock_wait_start = std::time::Instant::now();
            let mut server = server_state.meta.lock().await;
            metrics::record_server_state_lock_wait_ms(
                "execute_lock_free_post",
                lock_wait_start.elapsed().as_secs_f64() * 1000.0,
            );

            let lock_hold_start = std::time::Instant::now();
            server.stats.catch_up_from_atomic(&server_state.stats);
            let (track_slowlog, track_latency) = post_execute_tracking_flags(&server, argv);
            let elapsed_us = i64::try_from(fast_started.elapsed().as_micros()).unwrap_or(i64::MAX);
            apply_post_execute_side_effects(
                &mut server,
                client_state,
                argv,
                &outcome.response,
                Some(elapsed_us),
                track_slowlog,
                track_latency,
            );
            metrics::record_server_state_lock_hold_ms(
                "execute_lock_free_post",
                lock_hold_start.elapsed().as_secs_f64() * 1000.0,
            );
            outcome
        } else {
            let precheck = precheck_execute_argv_with_default_acl(
                argv,
                server_state.default_acl_policy(),
                client_state,
            );
            if let ExecuteArgvPrecheck::Reject(outcome) = precheck {
                outcome
            } else {
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
                        acl_dirty: false,
                    }
                } else {
                    let mut access = ServerAccess::new_with_runtime_caches(
                        &mut server,
                        &server_state.stats,
                        Some(server_state.default_acl_policy()),
                    );
                    execute_argv(argv, &mut access, client_state)
                };
                if outcome.config_dirty {
                    server_state.update_config_cache(&server.config);
                }
                if outcome.acl_dirty {
                    server_state.update_acl_policy_cache(&server.acl);
                }
                metrics::record_server_state_lock_hold_ms(
                    "execute",
                    lock_hold_start.elapsed().as_secs_f64() * 1000.0,
                );
                outcome
            }
        }
    } else {
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
                acl_dirty: false,
            }
        } else {
            let mut access = ServerAccess::new_with_runtime_caches(
                &mut server,
                &server_state.stats,
                Some(server_state.default_acl_policy()),
            );
            execute(frame, &mut access, client_state)
        };
        if outcome.config_dirty {
            server_state.update_config_cache(&server.config);
        }
        if outcome.acl_dirty {
            server_state.update_acl_policy_cache(&server.acl);
        }
        metrics::record_server_state_lock_hold_ms(
            "execute",
            lock_hold_start.elapsed().as_secs_f64() * 1000.0,
        );
        outcome
    };

    let duration = start.elapsed();
    let success = !matches!(outcome.response, ratatosk_resp::RespFrame::Error(_));
    metrics::record_command(&command_name, success, duration.as_secs_f64());

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
                    acl_dirty: false,
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
                    acl_dirty: false,
                }
            } else {
                let mut access = ServerAccess::new_with_runtime_caches(
                    &mut server,
                    &server_state.stats,
                    Some(server_state.default_acl_policy()),
                );
                if let Some(argv) = argv.as_deref() {
                    execute_argv(argv, &mut access, client_state)
                } else {
                    execute(frame, &mut access, client_state)
                }
            };
            if outcome.config_dirty {
                server_state.update_config_cache(&server.config);
            }
            if outcome.acl_dirty {
                server_state.update_acl_policy_cache(&server.acl);
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
