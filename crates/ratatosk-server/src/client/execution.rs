use super::shared_support::{frame_to_argv_for_persistence, refresh_client_snapshot};
use super::*;
use ratatosk_persist::aof::FsyncPolicy;

#[derive(Clone, Copy)]
enum AofRuntimeConfigSource {
    DirectConfigSet,
    Exec,
}

#[derive(Clone)]
struct AofRuntimeConfigBefore {
    source: AofRuntimeConfigSource,
    appendonly: bool,
    appendfsync: Bytes,
    writer_active: bool,
    writer_policy: FsyncPolicy,
}

fn aof_runtime_config_source(argv: &[Bytes]) -> Option<AofRuntimeConfigSource> {
    if argv
        .first()
        .is_some_and(|command| command.eq_ignore_ascii_case(b"EXEC"))
    {
        return Some(AofRuntimeConfigSource::Exec);
    }

    if argv.len() < 4
        || !argv[0].eq_ignore_ascii_case(b"CONFIG")
        || !argv[1].eq_ignore_ascii_case(b"SET")
        || (argv.len() - 2) % 2 != 0
    {
        return None;
    }

    for pair in argv[2..].chunks_exact(2) {
        if pair[0].eq_ignore_ascii_case(b"appendonly")
            || pair[0].eq_ignore_ascii_case(b"appendfsync")
        {
            return Some(AofRuntimeConfigSource::DirectConfigSet);
        }
    }

    None
}

fn aof_write_latch_error(detail: &str) -> RespFrame {
    RespFrame::error_str(&format!(
        "{AOF_WRITE_LATCH_ERR_PREFIX}; last_error={detail}"
    ))
}

fn refresh_server_aof_runtime_state(server: &mut ServerState, persistence: &PersistenceRuntime) {
    let writer_active = persistence.aof_sender().is_some();
    server.config.set_appendonly(writer_active);
    server
        .config
        .set_appendfsync(Bytes::from(persistence.aof_policy().as_str()));
    server.set_aof_enabled(writer_active);
    server.set_aof_current_path(persistence.aof_active_path());
    match persistence.aof_base_path() {
        Ok(base_path) => server.set_aof_base_path(base_path),
        Err(error) => {
            tracing::warn!(
                target = "ratatosk::aof",
                error = %error,
                "AOF lifecycle changed but the manifest path could not be refreshed"
            );
        }
    }
}

async fn reconcile_runtime_aof_config_change(
    server: &mut ServerState,
    persistence: &PersistenceRuntime,
    before: &AofRuntimeConfigBefore,
    effects: Option<DurabilityEffects>,
    outcome: &mut CommandOutcome,
) -> Option<DurabilityEffects> {
    if matches!(outcome.response, RespFrame::Error(_)) {
        return effects;
    }

    let desired_policy = match FsyncPolicy::from_config_str(server.config.appendfsync().as_ref()) {
        Some(policy) => policy,
        None => {
            refresh_server_aof_runtime_state(server, persistence);
            outcome.response = RespFrame::error_str("ERR invalid appendfsync policy");
            return None;
        }
    };
    let desired_enabled = server.config.appendonly();
    let aof_settings_changed = before.appendonly != desired_enabled
        || before.appendfsync.as_ref() != server.config.appendfsync().as_ref();
    if matches!(before.source, AofRuntimeConfigSource::Exec) && !aof_settings_changed {
        return effects;
    }

    let mut effects = effects;
    let writer_active = persistence.aof_sender().is_some();
    if before.writer_active != writer_active {
        tracing::debug!(
            target = "ratatosk::aof",
            before_active = before.writer_active,
            current_active = writer_active,
            "AOF writer state changed while reconciling runtime CONFIG"
        );
    }
    let enabling = desired_enabled && !writer_active;
    let disabling = !desired_enabled && writer_active;
    let policy_changed = desired_policy != before.writer_policy;

    // A policy change must finish before CONFIG is acknowledged. In
    // particular, `set_aof_fsync_policy` waits without a reply timeout after
    // the command is accepted, so the actual writer cannot mutate after a
    // rolled-back CONFIG value.
    if !enabling && (policy_changed || desired_policy != persistence.aof_policy()) {
        if let Err(error) = set_aof_fsync_policy(persistence, desired_policy).await {
            // The transaction's data commands already committed in memory.
            // Preserve them in the still-live old writer before reporting the
            // configuration failure; a closed writer is latched by the append
            // helper instead.
            let appended = append_durability_effects_while_locked(
                server,
                persistence,
                effects.take(),
                outcome,
            )
            .await;
            refresh_server_aof_runtime_state(server, persistence);
            if appended {
                outcome.response =
                    RespFrame::error_str(&format!("ERR CONFIG SET AOF failed: {error}"));
            }
            return None;
        }
    }

    if enabling {
        // Snapshot after every queued command has completed. The prior writer
        // was absent, so the snapshot alone represents the committed EXEC and
        // must not be followed by an incremental replay of its effects.
        let snapshot = server.data.snapshot_all();
        if let Err(error) = enable_aof_from_snapshot(persistence, &snapshot, desired_policy) {
            refresh_server_aof_runtime_state(server, persistence);
            outcome.response = RespFrame::error_str(&format!("ERR CONFIG SET AOF failed: {error}"));
            return None;
        }
        if persistence.aof_sender().is_none() {
            refresh_server_aof_runtime_state(server, persistence);
            outcome.response = RespFrame::error_str(
                "ERR CONFIG SET AOF failed: writer lifecycle did not reach the requested state",
            );
            return None;
        }

        // A fresh BASE and a live replacement writer are the recovery point
        // for a latched storage fault. Do not clear the latch before both have
        // succeeded.
        server.clear_aof_last_error();
        metrics::set_aof_write_latched(false);
        server.set_aof_rewrite_in_progress(false);
        server.set_last_aof_rewrite_status(Ok(()));
        server.set_last_aof_rewrite_time_ms(ratatosk_core::time::now_ms());
        refresh_server_aof_runtime_state(server, persistence);
        return None;
    }

    if disabling {
        // Durability effects from this EXEC were captured while the old writer
        // was live. Append them before Shutdown, whose forced fsync then makes
        // the committed transaction durable before the worker exits.
        if !append_durability_effects_while_locked(server, persistence, effects.take(), outcome)
            .await
        {
            refresh_server_aof_runtime_state(server, persistence);
            return None;
        }

        if let Err(error) = disable_aof(persistence).await {
            // Shutdown may have completed (and removed the writer) even when
            // its fsync reports an error. Refresh from the actual runtime
            // state instead of restoring a stale enabled setting.
            refresh_server_aof_runtime_state(server, persistence);
            outcome.response = RespFrame::error_str(&format!("ERR CONFIG SET AOF failed: {error}"));
            return None;
        }
        server.set_aof_rewrite_in_progress(false);
        refresh_server_aof_runtime_state(server, persistence);
        return None;
    }

    refresh_server_aof_runtime_state(server, persistence);
    effects
}

async fn append_durability_effects_while_locked(
    server: &mut ServerState,
    persistence: &PersistenceRuntime,
    effects: Option<DurabilityEffects>,
    outcome: &mut CommandOutcome,
) -> bool {
    let Some(effects) = effects else {
        return true;
    };
    if effects.is_empty()
        || !server.aof_enabled()
        || matches!(outcome.response, RespFrame::Error(_))
    {
        return true;
    }
    if persistence.aof_sender().is_none() {
        let latch_error = "AOF append failed: active writer is unavailable".to_string();
        metrics::record_aof_write_error();
        server.set_aof_last_error(latch_error.clone());
        metrics::set_aof_write_latched(true);
        outcome.response = aof_write_latch_error(&latch_error);
        return false;
    }

    let fsync_policy = String::from_utf8_lossy(server.config.appendfsync()).to_string();
    let append_start = std::time::Instant::now();
    match append_aof_effects(persistence, effects).await {
        Ok(()) => {
            let elapsed_ms = append_start.elapsed().as_secs_f64() * 1000.0;
            if append_start.elapsed() > AOF_APPEND_SLOW_THRESHOLD {
                metrics::record_aof_append_timeout("append_slow");
                tracing::warn!(
                    target = "ratatosk::aof",
                    elapsed_ms = elapsed_ms,
                    threshold_ms = AOF_APPEND_SLOW_THRESHOLD.as_millis(),
                    "AOF append exceeded slow threshold"
                );
            }
            metrics::record_aof_write(&fsync_policy);
            metrics::record_aof_append_duration_ms(elapsed_ms, "ok");
            true
        }
        Err(error) => {
            let elapsed_ms = append_start.elapsed().as_secs_f64() * 1000.0;
            metrics::record_aof_append_duration_ms(elapsed_ms, "error");
            metrics::record_aof_write_error();
            if error.kind() == io::ErrorKind::TimedOut {
                metrics::record_aof_append_timeout("worker");
            }
            let latch_error = format!("AOF append failed: {error}");
            server.set_aof_last_error(latch_error.clone());
            metrics::set_aof_write_latched(true);
            outcome.response = aof_write_latch_error(&latch_error);
            false
        }
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

    let is_write_operation = first_argv.as_deref().is_some_and(is_write_command)
        || (command_name == "EXEC" && client_state.has_queued_writes());

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

                let config_source = aof_runtime_config_source(argv);
                let config_before = config_source.map(|source| AofRuntimeConfigBefore {
                    source,
                    appendonly: server.config.appendonly(),
                    appendfsync: server.config.appendfsync().clone(),
                    writer_active: persistence.aof_sender().is_some(),
                    writer_policy: persistence.aof_policy(),
                });

                let mut outcome = if let Some(aof_error) = aof_latched_error {
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
                    client_state.set_durability_capture_enabled(server.aof_enabled());
                    let mut access = ServerAccess::new_with_runtime_caches(
                        &mut server,
                        &server_state.stats,
                        Some(server_state.default_acl_policy()),
                    );
                    execute_argv(argv, &mut access, client_state)
                };
                let effects = client_state.take_durability_effects();
                let effects = if let Some(before) = config_before.as_ref() {
                    reconcile_runtime_aof_config_change(
                        &mut server,
                        persistence,
                        before,
                        effects,
                        &mut outcome,
                    )
                    .await
                } else {
                    effects
                };
                let _ = append_durability_effects_while_locked(
                    &mut server,
                    persistence,
                    effects,
                    &mut outcome,
                )
                .await;
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

        let mut outcome = if let Some(aof_error) = aof_latched_error {
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
            client_state.set_durability_capture_enabled(server.aof_enabled());
            let mut access = ServerAccess::new_with_runtime_caches(
                &mut server,
                &server_state.stats,
                Some(server_state.default_acl_policy()),
            );
            execute(frame, &mut access, client_state)
        };
        let effects = client_state.take_durability_effects();
        let _ =
            append_durability_effects_while_locked(&mut server, persistence, effects, &mut outcome)
                .await;
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
        apply_post_execute_persistence(server_state, persistence, first_argv, &mut outcome).await;
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
                client_state.set_durability_capture_enabled(server.aof_enabled());
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
            let effects = client_state.take_durability_effects();
            let _ = append_durability_effects_while_locked(
                &mut server,
                persistence,
                effects,
                &mut outcome,
            )
            .await;
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

    if command == b"BGREWRITEAOF"
        && !matches!(outcome.response, RespFrame::Error(_))
        && !start_bgrewriteaof(Arc::clone(server_state), Arc::clone(persistence)).await
    {
        outcome.response = RespFrame::error_str("ERR BGREWRITEAOF failed: appendonly is disabled");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::ServerConfig;
    use ratatosk_engine::command::{ClientState, ServerAccess, execute_argv};

    fn argv(parts: &[&str]) -> Vec<Bytes> {
        parts
            .iter()
            .map(|part| Bytes::copy_from_slice(part.as_bytes()))
            .collect()
    }

    #[tokio::test]
    async fn exec_enable_uses_one_final_snapshot_and_updates_fsync_policy() {
        let dir = tempfile::tempdir().expect("tmpdir");
        let config = ServerConfig {
            dir: dir.path().to_path_buf(),
            appendonly: false,
            appendfsync: "everysec".to_string(),
            ..ServerConfig::default()
        };
        let persistence = PersistenceRuntime::from_config(&config).expect("persistence runtime");
        let mut server = ServerState::with_default_dbs();
        server.set_aof_enabled(false);
        server.config.set_appendonly(false);
        server
            .config
            .set_appendfsync(Bytes::from_static(b"everysec"));
        server.set_aof_last_error("previous storage fault");
        let before = AofRuntimeConfigBefore {
            source: AofRuntimeConfigSource::Exec,
            appendonly: server.config.appendonly(),
            appendfsync: server.config.appendfsync().clone(),
            writer_active: persistence.aof_sender().is_some(),
            writer_policy: persistence.aof_policy(),
        };
        let mut client = ClientState::default();
        client.set_durability_capture_enabled(false);

        let (mut outcome, effects) = {
            let mut access = ServerAccess::new_inline(&mut server);
            assert_eq!(
                execute_argv(&argv(&["MULTI"]), &mut access, &mut client).response,
                RespFrame::ok()
            );
            assert_eq!(
                execute_argv(&argv(&["INCR", "counter"]), &mut access, &mut client).response,
                RespFrame::queued()
            );
            assert_eq!(
                execute_argv(
                    &argv(&["CONFIG", "SET", "appendonly", "yes"]),
                    &mut access,
                    &mut client,
                )
                .response,
                RespFrame::queued()
            );
            assert_eq!(
                execute_argv(
                    &argv(&["CONFIG", "SET", "appendfsync", "always"]),
                    &mut access,
                    &mut client,
                )
                .response,
                RespFrame::queued()
            );
            assert_eq!(
                execute_argv(&argv(&["INCR", "counter"]), &mut access, &mut client).response,
                RespFrame::queued()
            );
            let outcome = execute_argv(&argv(&["EXEC"]), &mut access, &mut client);
            let effects = client.take_durability_effects();
            (outcome, effects)
        };

        assert!(outcome.config_dirty);
        assert!(
            reconcile_runtime_aof_config_change(
                &mut server,
                &persistence,
                &before,
                effects,
                &mut outcome,
            )
            .await
            .is_none()
        );
        assert_eq!(persistence.aof_policy(), FsyncPolicy::Always);
        assert!(server.aof_enabled());
        assert!(!server.aof_write_latched());

        let base_path = persistence
            .aof_base_path()
            .expect("load AOF manifest")
            .expect("fresh BASE path");
        let snapshot = ratatosk_persist::rdb::loader::load(&base_path).expect("load BASE");
        assert_eq!(
            snapshot[0]
                .get(&Bytes::from_static(b"counter"))
                .and_then(|entry| entry.as_string_bytes()),
            Some(Bytes::from_static(b"2"))
        );

        disable_aof(&persistence)
            .await
            .expect("stop test AOF writer");
    }

    #[tokio::test]
    async fn aborted_exec_does_not_change_aof_lifecycle() {
        let dir = tempfile::tempdir().expect("tmpdir");
        let config = ServerConfig {
            dir: dir.path().to_path_buf(),
            appendonly: false,
            ..ServerConfig::default()
        };
        let persistence = PersistenceRuntime::from_config(&config).expect("persistence runtime");
        let mut server = ServerState::with_default_dbs();
        let before = AofRuntimeConfigBefore {
            source: AofRuntimeConfigSource::Exec,
            appendonly: server.config.appendonly(),
            appendfsync: server.config.appendfsync().clone(),
            writer_active: false,
            writer_policy: persistence.aof_policy(),
        };
        let mut client = ClientState::default();

        let mut access = ServerAccess::new_inline(&mut server);
        assert_eq!(
            execute_argv(&argv(&["MULTI"]), &mut access, &mut client).response,
            RespFrame::ok()
        );
        assert_eq!(
            execute_argv(
                &argv(&["CONFIG", "SET", "appendonly", "yes"]),
                &mut access,
                &mut client,
            )
            .response,
            RespFrame::queued()
        );
        assert!(matches!(
            execute_argv(&argv(&["NO_SUCH_COMMAND"]), &mut access, &mut client).response,
            RespFrame::Error(_)
        ));
        let mut outcome = execute_argv(&argv(&["EXEC"]), &mut access, &mut client);

        assert!(matches!(outcome.response, RespFrame::Error(_)));
        assert!(
            reconcile_runtime_aof_config_change(
                &mut server,
                &persistence,
                &before,
                client.take_durability_effects(),
                &mut outcome,
            )
            .await
            .is_none()
        );
        assert!(!server.config.appendonly());
        assert!(persistence.aof_sender().is_none());
    }
}
