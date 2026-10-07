use std::{
    env, io,
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};

use bytes::Bytes;
#[cfg(test)]
use ratatosk_engine::keyspace::ServerState;
use ratatosk_engine::{
    command::DurabilityEffects,
    keyspace::{DbSnapshot, SharedState},
};
use ratatosk_persist::aof::{
    AofManifest, AofRecovery, AofWriter, DEFAULT_AOF_MANIFEST_FILENAME, FsyncPolicy,
    commit_manifest_switch,
};
use tokio::sync::{mpsc, oneshot};

use super::PersistenceRuntime;
use super::util::env_truthy;

const DEFAULT_AOF_QUEUE_CAPACITY: usize = 4096;
/// How often the worker checks a background fsync for completion while idle.
const AOF_FSYNC_POLL_INTERVAL: Duration = Duration::from_millis(10);
const AOF_APPEND_QUEUE_TIMEOUT: Duration = Duration::from_secs(2);
const AOF_APPEND_REPLY_TIMEOUT: Duration = Duration::from_secs(5);
const AOF_FLUSH_REPLY_TIMEOUT: Duration = Duration::from_secs(5);
const AOF_REWRITE_REPLY_TIMEOUT: Duration = Duration::from_secs(900);
const ALLOW_INCOMPLETE_AOF_CHAIN_ENV: &str = "RATATOSK_ALLOW_INCOMPLETE_AOF_CHAIN";

pub(crate) enum AofWorkerCommand {
    Append {
        db_index: usize,
        argv: Vec<Bytes>,
        timestamp_ms: i64,
        reply: oneshot::Sender<Result<(), String>>,
    },
    AppendTransaction {
        commands: Vec<(usize, Vec<Bytes>)>,
        timestamp_ms: i64,
        reply: oneshot::Sender<Result<(), String>>,
    },
    Flush {
        reply: oneshot::Sender<Result<(), String>>,
    },
    Rewrite {
        snapshot: DbSnapshot,
        reply: oneshot::Sender<Result<PathBuf, String>>,
    },
    SetPolicy {
        policy: FsyncPolicy,
        reply: oneshot::Sender<Result<(), String>>,
    },
    /// Periodic `appendfsync everysec` housekeeping. Collects a finished
    /// background fsync and starts the next one when due. Does not wait for
    /// the disk and is a no-op under other policies.
    EverySecTick {
        reply: oneshot::Sender<Result<(), String>>,
    },
    Shutdown {
        reply: oneshot::Sender<Result<(), String>>,
    },
}

/// A rewrite acknowledgement bound to the writer lineage that accepted it.
///
/// The worker can finish after a CONFIG SET appendonly no/yes sequence has
/// installed a replacement writer. Carrying this identity to completion keeps
/// the old worker from publishing its path into the new lineage.
pub(crate) struct AofRewriteRequest {
    generation: u64,
    reply: oneshot::Receiver<Result<PathBuf, String>>,
}

impl AofRewriteRequest {
    pub(crate) fn generation(&self) -> u64 {
        self.generation
    }
}

pub(crate) fn spawn_aof_worker(
    aof_path: PathBuf,
    manifest_path: Option<PathBuf>,
    writer: AofWriter,
    policy: FsyncPolicy,
    queue_capacity: usize,
) -> mpsc::Sender<AofWorkerCommand> {
    let (tx, mut rx) = mpsc::channel::<AofWorkerCommand>(queue_capacity);
    crate::metrics::set_aof_queue_depth(0);

    tokio::spawn(async move {
        enum Exit {
            GracefulShutdown,
            ShutdownFsyncFailed(String),
            ChannelClosed,
        }

        let mut writer = writer;
        let mut active_aof_path = aof_path;
        let mut manifest_path = manifest_path;
        let mut policy = policy;
        let mut delayed_reported = 0u64;
        let exit = loop {
            let delayed = writer.delayed_fsync_count();
            if delayed < delayed_reported {
                // A rewrite replaced the writer and its counter restarted.
                delayed_reported = 0;
            }
            if delayed > delayed_reported {
                crate::metrics::record_aof_delayed_fsync(delayed - delayed_reported);
                delayed_reported = delayed;
            }
            // While an everysec fsync runs, wake briefly so records postponed
            // behind it are written soon after it finishes even if no client
            // appends again.
            let received = if writer.fsync_in_flight() {
                match tokio::time::timeout(AOF_FSYNC_POLL_INTERVAL, rx.recv()).await {
                    Ok(received) => received,
                    Err(_) => {
                        writer.poll_background();
                        continue;
                    }
                }
            } else {
                rx.recv().await
            };
            let Some(command) = received else {
                break Exit::ChannelClosed;
            };
            crate::metrics::set_aof_queue_depth(rx.len());
            match command {
                AofWorkerCommand::Append {
                    db_index,
                    argv,
                    timestamp_ms,
                    reply,
                } => {
                    let result = writer
                        .append_command_at(db_index, &argv, timestamp_ms)
                        .map_err(|error| format!("appending AOF command: {error}"));
                    let _ = reply.send(result);
                }
                AofWorkerCommand::AppendTransaction {
                    commands,
                    timestamp_ms,
                    reply,
                } => {
                    let result = writer
                        .append_transaction_at(&commands, timestamp_ms)
                        .map_err(|error| format!("appending AOF transaction: {error}"));
                    let _ = reply.send(result);
                }
                AofWorkerCommand::Flush { reply } => {
                    let result = writer
                        .force_fsync()
                        .map_err(|error| format!("flushing AOF: {error}"));
                    let _ = reply.send(result);
                }
                AofWorkerCommand::Rewrite { snapshot, reply } => {
                    let result = rewrite_aof_and_reopen(
                        &mut active_aof_path,
                        manifest_path.as_deref(),
                        policy,
                        &mut writer,
                        &snapshot,
                    );
                    if result.is_ok() && manifest_path.is_none() {
                        manifest_path = active_aof_path
                            .parent()
                            .map(|dir| dir.join(DEFAULT_AOF_MANIFEST_FILENAME));
                    }
                    let _ = reply.send(result);
                }
                AofWorkerCommand::SetPolicy {
                    policy: next_policy,
                    reply,
                } => {
                    let result = writer
                        .set_policy(next_policy)
                        .map_err(|error| format!("draining AOF before policy change: {error}"));
                    if result.is_ok() {
                        policy = next_policy;
                    }
                    let _ = reply.send(result);
                }
                AofWorkerCommand::EverySecTick { reply } => {
                    let result = writer
                        .everysec_tick()
                        .map_err(|error| format!("periodic AOF fsync: {error}"));
                    let _ = reply.send(result);
                }
                AofWorkerCommand::Shutdown { reply } => {
                    let result = writer
                        .force_fsync()
                        .map_err(|error| format!("flushing AOF before shutdown: {error}"));
                    let exit = match &result {
                        Ok(()) => Exit::GracefulShutdown,
                        Err(error) => Exit::ShutdownFsyncFailed(error.clone()),
                    };
                    let _ = reply.send(result);
                    break exit;
                }
            }
        };

        crate::metrics::set_aof_queue_depth(0);
        match exit {
            Exit::GracefulShutdown => {
                tracing::info!(
                    target = "ratatosk::aof",
                    path = %active_aof_path.display(),
                    "AOF worker shut down gracefully"
                );
            }
            Exit::ShutdownFsyncFailed(error) => {
                tracing::error!(
                    target = "ratatosk::aof",
                    path = %active_aof_path.display(),
                    error = %error,
                    "AOF worker shutdown fsync failed"
                );
            }
            Exit::ChannelClosed => {
                tracing::warn!(
                    target = "ratatosk::aof",
                    path = %active_aof_path.display(),
                    "AOF worker channel closed; worker exiting"
                );
            }
        }
    });

    tx
}

fn rewrite_aof_and_reopen(
    active_aof_path: &mut PathBuf,
    manifest_path: Option<&Path>,
    policy: FsyncPolicy,
    writer: &mut AofWriter,
    snapshot: &DbSnapshot,
) -> Result<PathBuf, String> {
    writer
        .force_fsync()
        .map_err(|error| format!("forcing fsync before rewrite: {error}"))?;

    let dir = active_aof_path.parent().ok_or_else(|| {
        format!(
            "active AOF path '{}' has no parent directory",
            active_aof_path.display()
        )
    })?;
    let manifest_path = manifest_path
        .map(Path::to_path_buf)
        .unwrap_or_else(|| dir.join(DEFAULT_AOF_MANIFEST_FILENAME));
    let (next_incr_path, reopened) = materialize_aof_base(dir, &manifest_path, snapshot, policy)?;

    *writer = reopened;
    *active_aof_path = next_incr_path.clone();
    Ok(next_incr_path)
}

fn materialize_aof_base(
    dir: &Path,
    manifest_path: &Path,
    snapshot: &DbSnapshot,
    policy: FsyncPolicy,
) -> Result<(PathBuf, AofWriter), String> {
    let mut manifest = if manifest_path.exists() {
        AofManifest::load_from_file(manifest_path)
            .map_err(|error| format!("loading AOF manifest before materializing base: {error}"))?
    } else {
        AofManifest::new(dir)
    };

    let base_path = manifest.new_base_file();
    ratatosk_persist::rdb::saver::save_atomic(snapshot, &base_path)
        .map_err(|error| format!("writing materialized AOF BASE snapshot: {error}"))?;
    let base_name = base_path
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or_else(|| {
            format!(
                "AOF BASE path '{}' has no valid filename",
                base_path.display()
            )
        })?
        .to_string();

    manifest.set_base_after_rewrite(base_name);
    let next_incr_path = manifest.new_incr_file();
    let mut writer = AofWriter::open(&next_incr_path, policy)
        .map_err(|error| format!("opening incremental AOF after materialized base: {error}"))?;
    writer.force_fsync().map_err(|error| {
        format!("fsyncing incremental AOF before materialized BASE switch: {error}")
    })?;
    // Do not delete older lineage files here.  A manifest is the authority;
    // preserving orphaned files keeps rollback and forensic recovery safe.
    commit_manifest_switch(manifest_path, &manifest, &[])
        .map_err(|error| format!("committing materialized AOF BASE switch: {error}"))?;

    Ok((next_incr_path, writer))
}

fn advance_aof_generation(generation: &mut u64) {
    *generation = generation.wrapping_add(1);
    if *generation == 0 {
        *generation = 1;
    }
}

/// Clear a sender only when it still belongs to the operation that observed
/// it. A late failure from an old worker must never remove a newer writer.
fn clear_aof_sender_if_current(runtime: &PersistenceRuntime, generation: u64) {
    let mut state = runtime
        .aof
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    if state.sender.is_some() && state.generation == generation {
        state.sender = None;
        advance_aof_generation(&mut state.generation);
    }
}

pub async fn append_aof_command(
    runtime: &PersistenceRuntime,
    db_index: usize,
    argv: Vec<Bytes>,
) -> io::Result<()> {
    let Some((sender, generation)) = runtime.aof_sender_with_generation() else {
        return Ok(());
    };

    observe_aof_queue_depth(&sender);
    let (reply_tx, reply_rx) = oneshot::channel();

    match tokio::time::timeout(
        AOF_APPEND_QUEUE_TIMEOUT,
        sender.send(AofWorkerCommand::Append {
            db_index,
            argv,
            timestamp_ms: ratatosk_core::time::now_ms(),
            reply: reply_tx,
        }),
    )
    .await
    {
        Err(_) => {
            crate::metrics::record_aof_worker_enqueue_timeout("append");
            return Err(io::Error::new(
                io::ErrorKind::TimedOut,
                "timed out enqueueing AOF append request",
            ));
        }
        Ok(Err(_)) => {
            clear_aof_sender_if_current(runtime, generation);
            return Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "AOF worker channel closed while enqueueing append",
            ));
        }
        Ok(Ok(())) => {}
    }

    match tokio::time::timeout(AOF_APPEND_REPLY_TIMEOUT, reply_rx).await {
        Err(_) => {
            crate::metrics::record_aof_append_timeout("worker_reply");
            Err(io::Error::new(
                io::ErrorKind::TimedOut,
                "timed out waiting for AOF append completion",
            ))
        }
        Ok(Err(_)) => {
            clear_aof_sender_if_current(runtime, generation);
            Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "AOF worker dropped append completion channel",
            ))
        }
        Ok(Ok(Ok(()))) => Ok(()),
        Ok(Ok(Err(error))) => Err(io::Error::other(error)),
    }
}

/// Append effects captured by the engine rather than the client's original
/// argv.  This preserves absolute TTLs and resolved stream IDs, and lets a
/// committed transaction become one recoverable AOF unit.
pub async fn append_aof_effects(
    runtime: &PersistenceRuntime,
    effects: DurabilityEffects,
) -> io::Result<()> {
    if effects.is_empty() {
        return Ok(());
    }

    let Some((sender, generation)) = runtime.aof_sender_with_generation() else {
        return Ok(());
    };
    observe_aof_queue_depth(&sender);

    let (reply_tx, reply_rx) = oneshot::channel();
    let command = if effects.transaction {
        AofWorkerCommand::AppendTransaction {
            commands: effects
                .commands
                .into_iter()
                .map(|command| (command.db_index, command.argv))
                .collect(),
            timestamp_ms: effects.timestamp_ms,
            reply: reply_tx,
        }
    } else {
        let mut commands = effects.commands.into_iter();
        let Some(command) = commands.next() else {
            return Ok(());
        };
        if let Some(extra) = commands.next() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!(
                    "non-transaction durability capture unexpectedly contained multiple commands (first db={}, second db={})",
                    command.db_index, extra.db_index
                ),
            ));
        }
        AofWorkerCommand::Append {
            db_index: command.db_index,
            argv: command.argv,
            timestamp_ms: effects.timestamp_ms,
            reply: reply_tx,
        }
    };

    match tokio::time::timeout(AOF_APPEND_QUEUE_TIMEOUT, sender.send(command)).await {
        Err(_) => {
            crate::metrics::record_aof_worker_enqueue_timeout("append_effects");
            return Err(io::Error::new(
                io::ErrorKind::TimedOut,
                "timed out enqueueing AOF durability effects",
            ));
        }
        Ok(Err(_)) => {
            clear_aof_sender_if_current(runtime, generation);
            return Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "AOF worker channel closed while enqueueing durability effects",
            ));
        }
        Ok(Ok(())) => {}
    }

    match tokio::time::timeout(AOF_APPEND_REPLY_TIMEOUT, reply_rx).await {
        Err(_) => Err(io::Error::new(
            io::ErrorKind::TimedOut,
            "timed out waiting for AOF durability effects completion",
        )),
        Ok(Err(_)) => {
            clear_aof_sender_if_current(runtime, generation);
            Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "AOF worker dropped durability effects completion channel",
            ))
        }
        Ok(Ok(Ok(()))) => Ok(()),
        Ok(Ok(Err(error))) => Err(io::Error::other(error)),
    }
}

pub async fn flush_aof(runtime: &PersistenceRuntime) -> io::Result<()> {
    let Some((sender, generation)) = runtime.aof_sender_with_generation() else {
        return Ok(());
    };

    observe_aof_queue_depth(&sender);
    let (reply_tx, reply_rx) = oneshot::channel();

    match tokio::time::timeout(
        AOF_APPEND_QUEUE_TIMEOUT,
        sender.send(AofWorkerCommand::Flush { reply: reply_tx }),
    )
    .await
    {
        Err(_) => {
            crate::metrics::record_aof_worker_enqueue_timeout("flush");
            return Err(io::Error::new(
                io::ErrorKind::TimedOut,
                "timed out enqueueing AOF flush request",
            ));
        }
        Ok(Err(_)) => {
            clear_aof_sender_if_current(runtime, generation);
            return Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "AOF worker channel closed while enqueueing flush",
            ));
        }
        Ok(Ok(())) => {}
    }

    match tokio::time::timeout(AOF_FLUSH_REPLY_TIMEOUT, reply_rx).await {
        Err(_) => Err(io::Error::new(
            io::ErrorKind::TimedOut,
            "timed out waiting for AOF flush completion",
        )),
        Ok(Err(_)) => {
            clear_aof_sender_if_current(runtime, generation);
            Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "AOF worker dropped flush completion channel",
            ))
        }
        Ok(Ok(Ok(()))) => {
            crate::metrics::record_aof_write("always");
            Ok(())
        }
        Ok(Ok(Err(error))) => {
            crate::metrics::record_aof_write_error();
            Err(io::Error::other(error))
        }
    }
}

/// Drive the `everysec` background fsync from the server timer.
///
/// The worker answers immediately: it never waits for the disk here, so this
/// is safe to call without holding the server `meta` lock. It is a no-op when
/// the worker's policy is not `everysec`.
pub async fn tick_aof_everysec(runtime: &PersistenceRuntime) -> Result<(), (u64, io::Error)> {
    let Some((sender, generation)) = runtime.aof_sender_with_generation() else {
        return Ok(());
    };
    tick_aof_everysec_inner(runtime, sender, generation)
        .await
        .map_err(|error| (generation, error))
}

async fn tick_aof_everysec_inner(
    runtime: &PersistenceRuntime,
    sender: mpsc::Sender<AofWorkerCommand>,
    generation: u64,
) -> io::Result<()> {
    let (reply_tx, reply_rx) = oneshot::channel();
    match tokio::time::timeout(
        AOF_APPEND_QUEUE_TIMEOUT,
        sender.send(AofWorkerCommand::EverySecTick { reply: reply_tx }),
    )
    .await
    {
        Err(_) => {
            crate::metrics::record_aof_worker_enqueue_timeout("everysec_tick");
            return Err(io::Error::new(
                io::ErrorKind::TimedOut,
                "timed out enqueueing AOF everysec tick",
            ));
        }
        Ok(Err(_)) => {
            clear_aof_sender_if_current(runtime, generation);
            return Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "AOF worker channel closed while enqueueing everysec tick",
            ));
        }
        Ok(Ok(())) => {}
    }

    match tokio::time::timeout(AOF_FLUSH_REPLY_TIMEOUT, reply_rx).await {
        Err(_) => Err(io::Error::new(
            io::ErrorKind::TimedOut,
            "timed out waiting for AOF everysec tick",
        )),
        Ok(Err(_)) => {
            clear_aof_sender_if_current(runtime, generation);
            Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "AOF worker dropped everysec tick completion channel",
            ))
        }
        Ok(Ok(Ok(()))) => Ok(()),
        Ok(Ok(Err(error))) => {
            crate::metrics::record_aof_write_error();
            Err(io::Error::other(error))
        }
    }
}

pub(crate) fn enable_aof_from_snapshot(
    runtime: &PersistenceRuntime,
    snapshot: &DbSnapshot,
    policy: FsyncPolicy,
) -> io::Result<()> {
    if runtime.aof_sender().is_some() {
        return Ok(());
    }

    let dir = runtime.aof_path.parent().ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            format!(
                "AOF path '{}' has no parent directory",
                runtime.aof_path.display()
            ),
        )
    })?;
    let manifest_path = dir.join(DEFAULT_AOF_MANIFEST_FILENAME);
    let (active_path, writer) =
        materialize_aof_base(dir, &manifest_path, snapshot, policy).map_err(io::Error::other)?;
    let sender = spawn_aof_worker(
        active_path.clone(),
        Some(manifest_path.clone()),
        writer,
        policy,
        aof_queue_capacity_from_env(),
    );

    let mut state = runtime
        .aof
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    advance_aof_generation(&mut state.generation);
    state.sender = Some(sender);
    state.active_path = active_path;
    state.manifest_path = Some(manifest_path);
    state.policy = policy;
    Ok(())
}

pub(crate) async fn disable_aof(runtime: &PersistenceRuntime) -> io::Result<()> {
    disable_aof_with_reply_timeout(runtime, None, false).await
}

impl PersistenceRuntime {
    /// Stop the AOF worker during terminal server shutdown.
    ///
    /// Unlike CONFIG SET appendonly no, process exit has no configuration
    /// rollback to reconcile, so the final worker acknowledgement is bounded
    /// by the same deadline previously used for the terminal flush request.
    pub(crate) async fn shutdown_aof_for_exit(&self) -> io::Result<()> {
        disable_aof_with_reply_timeout(self, Some(AOF_FLUSH_REPLY_TIMEOUT), true).await
    }
}

async fn disable_aof_with_reply_timeout(
    runtime: &PersistenceRuntime,
    reply_timeout: Option<Duration>,
    record_fsync_metrics: bool,
) -> io::Result<()> {
    let Some((sender, generation)) = runtime.aof_sender_with_generation() else {
        return Ok(());
    };

    observe_aof_queue_depth(&sender);
    let (reply_tx, reply_rx) = oneshot::channel();
    match tokio::time::timeout(
        AOF_APPEND_QUEUE_TIMEOUT,
        sender.send(AofWorkerCommand::Shutdown { reply: reply_tx }),
    )
    .await
    {
        Err(_) => {
            crate::metrics::record_aof_worker_enqueue_timeout("shutdown");
            return Err(io::Error::new(
                io::ErrorKind::TimedOut,
                "timed out enqueueing AOF shutdown request",
            ));
        }
        Ok(Err(_)) => {
            clear_aof_sender_if_current(runtime, generation);
            return Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "AOF worker channel closed while enqueueing shutdown",
            ));
        }
        Ok(Ok(())) => {}
    }

    let reply = if let Some(reply_timeout) = reply_timeout {
        match tokio::time::timeout(reply_timeout, reply_rx).await {
            Ok(reply) => reply,
            Err(_) => {
                clear_aof_sender_if_current(runtime, generation);
                return Err(io::Error::new(
                    io::ErrorKind::TimedOut,
                    "timed out waiting for AOF shutdown completion",
                ));
            }
        }
    } else {
        // Once Shutdown has entered the worker queue it is not safe for CONFIG
        // SET appendonly no to time out: the worker may still flush and exit
        // after the caller rolls configuration back. Wait for the definitive
        // result so configuration follows the known writer lifecycle.
        reply_rx.await
    };

    match reply {
        Err(_) => {
            clear_aof_sender_if_current(runtime, generation);
            Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "AOF worker dropped shutdown completion channel",
            ))
        }
        Ok(Err(error)) => {
            // Shutdown always exits the worker after reporting its fsync
            // result, even if the flush failed. Do not leave a stale sender
            // that makes INFO/configuration claim persistence is still live.
            clear_aof_sender_if_current(runtime, generation);
            if record_fsync_metrics {
                crate::metrics::record_aof_write_error();
            }
            Err(io::Error::other(error))
        }
        Ok(Ok(())) => {
            clear_aof_sender_if_current(runtime, generation);
            if record_fsync_metrics {
                crate::metrics::record_aof_write("always");
            }
            Ok(())
        }
    }
}

pub(crate) async fn set_aof_fsync_policy(
    runtime: &PersistenceRuntime,
    policy: FsyncPolicy,
) -> io::Result<()> {
    let Some((sender, generation)) = runtime.aof_sender_with_generation() else {
        runtime
            .aof
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .policy = policy;
        return Ok(());
    };

    observe_aof_queue_depth(&sender);
    let (reply_tx, reply_rx) = oneshot::channel();
    match tokio::time::timeout(
        AOF_APPEND_QUEUE_TIMEOUT,
        sender.send(AofWorkerCommand::SetPolicy {
            policy,
            reply: reply_tx,
        }),
    )
    .await
    {
        Err(_) => {
            crate::metrics::record_aof_worker_enqueue_timeout("set_policy");
            return Err(io::Error::new(
                io::ErrorKind::TimedOut,
                "timed out enqueueing AOF fsync policy update",
            ));
        }
        Ok(Err(_)) => {
            clear_aof_sender_if_current(runtime, generation);
            return Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "AOF worker channel closed while updating fsync policy",
            ));
        }
        Ok(Ok(())) => {}
    }

    // A queued policy change can take effect after an arbitrary worker delay.
    // Do not let CONFIG roll back while the worker later applies it.
    match reply_rx.await {
        Err(_) => {
            clear_aof_sender_if_current(runtime, generation);
            Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "AOF worker dropped fsync policy completion channel",
            ))
        }
        Ok(Err(error)) => Err(io::Error::other(error)),
        Ok(Ok(())) => {
            let mut state = runtime
                .aof
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            if state.sender.is_none() || state.generation != generation {
                return Err(io::Error::new(
                    io::ErrorKind::BrokenPipe,
                    "AOF worker changed while updating fsync policy",
                ));
            }
            state.policy = policy;
            Ok(())
        }
    }
}

pub(crate) async fn replace_aof_with_snapshot(
    runtime: &PersistenceRuntime,
    snapshot: &DbSnapshot,
    policy: FsyncPolicy,
) -> io::Result<()> {
    disable_aof(runtime).await?;
    enable_aof_from_snapshot(runtime, snapshot, policy)
}

pub(crate) async fn enqueue_aof_rewrite(
    runtime: &PersistenceRuntime,
    snapshot: DbSnapshot,
) -> io::Result<AofRewriteRequest> {
    let Some((sender, generation)) = runtime.aof_sender_with_generation() else {
        return Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "AOF rewrite requested while appendonly is disabled",
        ));
    };

    observe_aof_queue_depth(&sender);
    let (reply_tx, reply_rx) = oneshot::channel();

    match tokio::time::timeout(
        AOF_APPEND_QUEUE_TIMEOUT,
        sender.send(AofWorkerCommand::Rewrite {
            snapshot,
            reply: reply_tx,
        }),
    )
    .await
    {
        Err(_) => {
            crate::metrics::record_aof_worker_enqueue_timeout("rewrite");
            return Err(io::Error::new(
                io::ErrorKind::TimedOut,
                "timed out enqueueing AOF rewrite request",
            ));
        }
        Ok(Err(_)) => {
            clear_aof_sender_if_current(runtime, generation);
            return Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "AOF worker channel closed while enqueueing rewrite",
            ));
        }
        Ok(Ok(())) => {}
    }

    Ok(AofRewriteRequest {
        generation,
        reply: reply_rx,
    })
}

pub(crate) async fn await_aof_rewrite(
    runtime: &PersistenceRuntime,
    request: AofRewriteRequest,
) -> io::Result<bool> {
    let AofRewriteRequest { generation, reply } = request;
    match tokio::time::timeout(AOF_REWRITE_REPLY_TIMEOUT, reply).await {
        Err(_) => Err(io::Error::new(
            io::ErrorKind::TimedOut,
            "timed out waiting for AOF rewrite completion",
        )),
        Ok(Err(_)) => {
            clear_aof_sender_if_current(runtime, generation);
            Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "AOF worker dropped rewrite completion channel",
            ))
        }
        Ok(Ok(Ok(active_path))) => {
            let manifest_path = active_path
                .parent()
                .map(|dir| dir.join(DEFAULT_AOF_MANIFEST_FILENAME));
            let mut state = runtime
                .aof
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            if state.sender.is_none() || state.generation != generation {
                return Ok(false);
            }
            state.active_path = active_path;
            state.manifest_path = manifest_path;
            Ok(true)
        }
        Ok(Ok(Err(error))) => Err(io::Error::other(error)),
    }
}

pub(crate) async fn replay_startup_aof_file(
    server_state: &Arc<SharedState>,
    path: &Path,
    may_truncate: bool,
) -> io::Result<()> {
    let result = {
        let mut state = server_state.meta.lock().await;
        AofRecovery::replay_file(path, &mut state)
            .map_err(|error| io::Error::other(format!("replaying AOF: {error}")))?
    };

    if result.corruption_detected {
        tracing::warn!(
            target = "ratatosk::startup",
            path = %path.display(),
            commands_replayed = result.commands_replayed,
            bytes_processed = result.bytes_processed,
            truncated_at = result.truncated_at,
            "AOF replay completed with corruption detected"
        );
    } else {
        tracing::info!(
            target = "ratatosk::startup",
            path = %path.display(),
            commands_replayed = result.commands_replayed,
            bytes_processed = result.bytes_processed,
            "AOF replay completed successfully"
        );
    }

    if result.version_mismatch {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "AOF file {} has incompatible or unknown version header; refusing startup to protect rollback safety",
                path.display()
            ),
        ));
    }

    if result.legacy_format && !env_truthy("RATATOSK_ALLOW_LEGACY_AOF") {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "AOF file {} is in legacy headerless format; refusing startup by default (set RATATOSK_ALLOW_LEGACY_AOF=true to bypass)",
                path.display()
            ),
        ));
    }

    if result.legacy_format {
        tracing::warn!(
            target = "ratatosk::startup",
            path = %path.display(),
            "AOF replay accepted legacy headerless format due to RATATOSK_ALLOW_LEGACY_AOF"
        );
    }

    if let Some(boundary) = result.truncated_at {
        if !may_truncate {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "AOF file {} is damaged at byte {boundary} but is not the last file of the manifest chain; refusing startup because truncating it would drop writes that later files depend on",
                    path.display()
                ),
            ));
        }
        // Repair before admitting writes. Leaving the tail in place would
        // swallow later acknowledged writes or attach them to an old MULTI.
        let file = std::fs::OpenOptions::new().write(true).open(path)?;
        file.set_len(boundary as u64)?;
        file.sync_all()?;
        tracing::warn!(target = "ratatosk::startup", path = %path.display(),
            boundary, "truncated AOF to its verified committed prefix");
    }

    Ok(())
}

pub(crate) fn startup_aof_recovery_paths(runtime: &PersistenceRuntime) -> io::Result<Vec<PathBuf>> {
    let Some(manifest_path) = runtime.aof_manifest_path.as_ref() else {
        return Ok(runtime
            .aof_path
            .exists()
            .then(|| runtime.aof_path.clone())
            .into_iter()
            .collect());
    };

    if !manifest_path.exists() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "AOF manifest '{}' is missing; refusing startup to avoid recovering an unverified partial lineage",
                manifest_path.display()
            ),
        ));
    }

    let manifest = AofManifest::load_from_file(manifest_path)?;
    let mut recovery_files = Vec::new();
    let mut missing_files = Vec::new();
    for path in manifest.recovery_files() {
        if path.exists() {
            recovery_files.push(path);
        } else {
            missing_files.push(path);
        }
    }

    if !missing_files.is_empty() {
        if env_truthy(ALLOW_INCOMPLETE_AOF_CHAIN_ENV) {
            for path in &missing_files {
                tracing::warn!(
                    target = "ratatosk::startup",
                    path = %path.display(),
                    manifest = %manifest_path.display(),
                    override_env = ALLOW_INCOMPLETE_AOF_CHAIN_ENV,
                    "AOF manifest references a missing recovery file; continuing because incomplete-chain override is enabled"
                );
            }
        } else {
            let missing = missing_files
                .iter()
                .map(|path| path.display().to_string())
                .collect::<Vec<_>>()
                .join(", ");
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "AOF manifest '{}' references missing recovery file(s): {}. Refusing startup to avoid partial recovery (set {}=true to bypass)",
                    manifest_path.display(),
                    missing,
                    ALLOW_INCOMPLETE_AOF_CHAIN_ENV
                ),
            ));
        }
    }

    tracing::info!(
        target = "ratatosk::startup",
        manifest = %manifest_path.display(),
        files = recovery_files.len(),
        "loaded AOF manifest for startup recovery"
    );

    Ok(recovery_files)
}

/// Check whether a legacy single-file AOF exists without a manifest.
///
/// Returns the path to the legacy file if one is found and no manifest
/// is present, otherwise `None`.
pub(crate) fn detect_legacy_aof(legacy_aof_path: &Path, manifest_path: &Path) -> Option<PathBuf> {
    if legacy_aof_path.exists() && !manifest_path.exists() {
        Some(legacy_aof_path.to_path_buf())
    } else {
        None
    }
}

/// Migrate a legacy single-file AOF to manifest-based format in-place.
///
/// Creates a manifest that wraps the existing legacy file as the first
/// incremental segment, then creates a new incremental file for future
/// writes.  The legacy file is **not** removed or renamed — the manifest
/// simply references it.
fn migrate_legacy_aof_to_manifest(
    dir: &Path,
    legacy_aof_path: &Path,
    manifest_path: &Path,
) -> io::Result<(PathBuf, Option<PathBuf>)> {
    let legacy_filename = legacy_aof_path
        .file_name()
        .and_then(|f| f.to_str())
        .ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                format!(
                    "legacy AOF path has no valid filename: {}",
                    legacy_aof_path.display()
                ),
            )
        })?;

    let mut manifest = AofManifest::new(dir);
    // Register the legacy file as the base so it is included in
    // recovery ordering before any new incremental files.
    manifest.set_base_after_rewrite(legacy_filename.to_string());
    let active_path = manifest.new_incr_file();
    manifest.save_to_file(manifest_path)?;

    tracing::info!(
        target = "ratatosk::startup",
        legacy_path = %legacy_aof_path.display(),
        manifest_path = %manifest_path.display(),
        active_incr = %active_path.display(),
        "migrated legacy single-file AOF to manifest-based format"
    );

    Ok((active_path, Some(manifest_path.to_path_buf())))
}

pub(crate) fn bootstrap_aof_layout(
    dir: &Path,
    legacy_aof_path: &Path,
    manifest_path: &Path,
) -> io::Result<(PathBuf, Option<PathBuf>)> {
    if manifest_path.exists() {
        let mut manifest = AofManifest::load_from_file(manifest_path)?;
        if let Some(path) = manifest.current_incr_path() {
            if !path.exists() && !env_truthy(ALLOW_INCOMPLETE_AOF_CHAIN_ENV) {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!(
                        "AOF manifest '{}' references missing active incremental file '{}'; refusing startup to avoid partial recovery",
                        manifest_path.display(),
                        path.display()
                    ),
                ));
            }
            return Ok((path, Some(manifest_path.to_path_buf())));
        }

        let active_path = manifest.new_incr_file();
        manifest.save_to_file(manifest_path)?;
        return Ok((active_path, Some(manifest_path.to_path_buf())));
    }

    if let Some(legacy_path) = detect_legacy_aof(legacy_aof_path, manifest_path) {
        tracing::warn!(
            target = "ratatosk::startup",
            path = %legacy_path.display(),
            "Legacy single-file AOF detected at {}. \
             Set RATATOSK_MIGRATE_AOF=true to auto-convert to manifest-based format.",
            legacy_path.display()
        );

        if env_truthy("RATATOSK_MIGRATE_AOF") {
            return migrate_legacy_aof_to_manifest(dir, &legacy_path, manifest_path);
        }

        return Ok((legacy_path, None));
    }

    let mut manifest = AofManifest::new(dir);
    let active_path = manifest.new_incr_file();
    manifest.save_to_file(manifest_path)?;
    Ok((active_path, Some(manifest_path.to_path_buf())))
}

pub(crate) fn aof_queue_capacity_from_env() -> usize {
    env::var("RATATOSK_AOF_QUEUE_CAPACITY")
        .ok()
        .and_then(|raw| raw.parse::<usize>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(DEFAULT_AOF_QUEUE_CAPACITY)
}

pub(crate) fn observe_aof_queue_depth(sender: &mpsc::Sender<AofWorkerCommand>) {
    let depth = sender.max_capacity().saturating_sub(sender.capacity());
    crate::metrics::set_aof_queue_depth(depth);
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::persistence::load_startup_data;
    use bytes::Bytes;
    use ratatosk_engine::keyspace::StoredValue;
    use ratatosk_persist::aof::DEFAULT_AOF_MANIFEST_FILENAME;
    use ratatosk_persist::rdb;
    use std::{
        ffi::OsString,
        io::Write,
        sync::{Mutex, MutexGuard, OnceLock},
    };

    use crate::config::ServerConfig;

    #[derive(Clone)]
    struct CapturedLogs(Arc<Mutex<Vec<u8>>>);

    struct CapturedLogWriter(Arc<Mutex<Vec<u8>>>);

    impl Write for CapturedLogWriter {
        fn write(&mut self, buffer: &[u8]) -> io::Result<usize> {
            self.0
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .extend_from_slice(buffer);
            Ok(buffer.len())
        }

        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    impl<'writer> tracing_subscriber::fmt::MakeWriter<'writer> for CapturedLogs {
        type Writer = CapturedLogWriter;

        fn make_writer(&'writer self) -> Self::Writer {
            CapturedLogWriter(Arc::clone(&self.0))
        }
    }

    fn aof_log_subscriber(logs: Arc<Mutex<Vec<u8>>>) -> impl tracing::Subscriber + Send + Sync {
        tracing_subscriber::fmt()
            .with_ansi(false)
            .without_time()
            .with_max_level(tracing::Level::TRACE)
            .with_writer(CapturedLogs(logs))
            .finish()
    }

    /// Install `logs` as this thread's subscriber. Rebuilding the callsite
    /// interest cache makes log statements that already registered while no
    /// subscriber was installed (by tests in other threads) become enabled
    /// again, so log-asserting tests do not depend on test order.
    fn install_log_subscriber(logs: Arc<Mutex<Vec<u8>>>) -> tracing::subscriber::DefaultGuard {
        let guard = tracing::subscriber::set_default(aof_log_subscriber(logs));
        tracing::callsite::rebuild_interest_cache();
        guard
    }

    async fn wait_for_aof_log(logs: &Arc<Mutex<Vec<u8>>>, path: &Path, message: &str) -> String {
        let path = path.display().to_string();
        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                let output = {
                    let bytes = logs.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
                    String::from_utf8_lossy(&bytes).into_owned()
                };
                if output
                    .lines()
                    .any(|line| line.contains(&path) && line.contains(message))
                {
                    return output;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap_or_else(|_| panic!("timed out waiting for '{message}' for {path}"))
    }

    fn env_test_lock() -> MutexGuard<'static, ()> {
        static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
        LOCK.get_or_init(|| Mutex::new(()))
            .lock()
            .expect("env test lock poisoned")
    }

    struct ScopedEnvVar {
        key: &'static str,
        previous: Option<OsString>,
    }

    impl ScopedEnvVar {
        fn set(key: &'static str, value: &str) -> Self {
            let previous = env::var_os(key);
            // SAFETY: tests hold env_test_lock() while mutating process env.
            unsafe { env::set_var(key, value) };
            Self { key, previous }
        }

        fn remove(key: &'static str) -> Self {
            let previous = env::var_os(key);
            // SAFETY: tests hold env_test_lock() while mutating process env.
            unsafe { env::remove_var(key) };
            Self { key, previous }
        }
    }

    impl Drop for ScopedEnvVar {
        fn drop(&mut self) {
            if let Some(value) = self.previous.as_ref() {
                // SAFETY: tests hold env_test_lock() while mutating process env.
                unsafe { env::set_var(self.key, value) };
            } else {
                // SAFETY: tests hold env_test_lock() while mutating process env.
                unsafe { env::remove_var(self.key) };
            }
        }
    }

    fn runtime_without_writer(
        rdb_path: PathBuf,
        aof_path: PathBuf,
        aof_manifest_path: Option<PathBuf>,
    ) -> PersistenceRuntime {
        PersistenceRuntime {
            rdb_path,
            aof_path: aof_path.clone(),
            aof_manifest_path: aof_manifest_path.clone(),
            aof: Arc::new(Mutex::new(super::super::AofRuntimeState {
                sender: None,
                generation: 0,
                active_path: aof_path,
                manifest_path: aof_manifest_path,
                policy: FsyncPolicy::EverySec,
            })),
        }
    }

    #[tokio::test]
    async fn from_config_bootstraps_manifest_backed_aof_on_empty_dir() {
        let dir = tempfile::tempdir().expect("tmpdir");
        let config = ServerConfig {
            dir: dir.path().to_path_buf(),
            appendonly: true,
            ..ServerConfig::default()
        };

        let runtime = PersistenceRuntime::from_config(&config).expect("runtime");
        let manifest_path = runtime
            .aof_manifest_path
            .as_ref()
            .expect("manifest-backed runtime");
        let manifest = AofManifest::load_from_file(manifest_path).expect("load manifest");

        assert_eq!(
            runtime.aof_path,
            manifest.current_incr_path().expect("active incr")
        );
        assert!(manifest_path.exists());
    }

    #[tokio::test]
    async fn from_config_keeps_legacy_single_file_aof_as_fallback() {
        use ratatosk_persist::aof::DEFAULT_SINGLE_FILE_AOF_FILENAME;

        let dir = tempfile::tempdir().expect("tmpdir");
        let legacy_path = dir.path().join(DEFAULT_SINGLE_FILE_AOF_FILENAME);
        std::fs::write(&legacy_path, b"").expect("write legacy aof");

        let config = ServerConfig {
            dir: dir.path().to_path_buf(),
            appendonly: true,
            ..ServerConfig::default()
        };

        let runtime = PersistenceRuntime::from_config(&config).expect("runtime");

        assert_eq!(runtime.aof_path, legacy_path);
        assert!(runtime.aof_manifest_path.is_none());
    }

    #[tokio::test]
    async fn from_config_repairs_manifest_without_active_incr() {
        let dir = tempfile::tempdir().expect("tmpdir");
        let manifest_path = dir.path().join(DEFAULT_AOF_MANIFEST_FILENAME);
        AofManifest::new(dir.path())
            .save_to_file(&manifest_path)
            .expect("save empty manifest");

        let config = ServerConfig {
            dir: dir.path().to_path_buf(),
            appendonly: true,
            ..ServerConfig::default()
        };

        let runtime = PersistenceRuntime::from_config(&config).expect("runtime");
        let manifest = AofManifest::load_from_file(&manifest_path).expect("load manifest");

        assert_eq!(runtime.aof_manifest_path, Some(manifest_path));
        assert_eq!(
            runtime.aof_path,
            manifest.current_incr_path().expect("active incr")
        );
    }

    #[test]
    fn from_config_refuses_to_create_a_missing_active_manifest_segment() {
        let dir = tempfile::tempdir().expect("tmpdir");
        let manifest_path = dir.path().join(DEFAULT_AOF_MANIFEST_FILENAME);
        let mut manifest = AofManifest::new(dir.path());
        let missing_active = manifest.new_incr_file();
        manifest
            .save_to_file(&manifest_path)
            .expect("save manifest");

        let config = ServerConfig {
            dir: dir.path().to_path_buf(),
            appendonly: true,
            ..ServerConfig::default()
        };

        let error = match PersistenceRuntime::from_config(&config) {
            Ok(_) => panic!("missing active AOF segment must fail closed"),
            Err(error) => error,
        };
        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
        assert!(
            error
                .to_string()
                .contains(missing_active.to_string_lossy().as_ref())
        );
        assert!(!missing_active.exists());
    }

    #[tokio::test]
    async fn startup_bootstraps_existing_rdb_into_a_fresh_aof_lineage() {
        let dir = tempfile::tempdir().expect("tmpdir");
        let config = ServerConfig {
            dir: dir.path().to_path_buf(),
            appendonly: true,
            appendfsync: "always".to_string(),
            ..ServerConfig::default()
        };
        let rdb_path = dir.path().join(&config.dbfilename);
        let rdb_state = ServerState::with_default_dbs();
        rdb_state.db_mut(0).insert(
            Bytes::from("from-rdb"),
            StoredValue::string(Bytes::from("snapshot"), None),
        );
        rdb::saver::save(&rdb_state.snapshot_dbs(), &rdb_path).expect("save source rdb");

        let runtime = PersistenceRuntime::from_config(&config).expect("runtime");
        let shared = Arc::new(SharedState::new(ServerState::with_default_dbs()));
        load_startup_data(&shared, &runtime, true)
            .await
            .expect("bootstrap existing rdb");

        append_aof_command(
            &runtime,
            0,
            vec![
                Bytes::from("SET"),
                Bytes::from("after"),
                Bytes::from("append"),
            ],
        )
        .await
        .expect("append after bootstrap");
        flush_aof(&runtime).await.expect("flush append");

        let manifest_path = runtime
            .aof_manifest_path
            .as_ref()
            .expect("manifest-backed runtime");
        let manifest = AofManifest::load_from_file(manifest_path).expect("load manifest");
        assert!(
            manifest
                .base_path()
                .is_some_and(|path| path.extension().is_some_and(|extension| extension == "rdb"))
        );
        disable_aof(&runtime).await.expect("stop first writer");

        let restarted_runtime = PersistenceRuntime::from_config(&config).expect("restart runtime");
        let restarted = Arc::new(SharedState::new(ServerState::with_default_dbs()));
        load_startup_data(&restarted, &restarted_runtime, true)
            .await
            .expect("recover AOF lineage");
        let loaded = restarted.meta.lock().await;
        assert_eq!(
            loaded
                .db(0)
                .get(&Bytes::from("from-rdb"))
                .and_then(|entry| entry.as_string_bytes()),
            Some(Bytes::from("snapshot"))
        );
        assert_eq!(
            loaded
                .db(0)
                .get(&Bytes::from("after"))
                .and_then(|entry| entry.as_string_bytes()),
            Some(Bytes::from("append"))
        );
    }

    #[tokio::test]
    async fn startup_load_uses_nonempty_aof_as_the_authoritative_lineage() {
        let dir = tempfile::tempdir().expect("tmpdir");
        let config = ServerConfig {
            dir: dir.path().to_path_buf(),
            appendonly: true,
            appendfsync: "always".to_string(),
            ..ServerConfig::default()
        };

        let runtime = PersistenceRuntime::from_config(&config).expect("runtime");

        let state = ServerState::with_default_dbs();
        state.db_mut(0).insert(
            Bytes::from("from-rdb"),
            StoredValue::string(Bytes::from("1"), None),
        );
        rdb::saver::save(&state.snapshot_dbs(), &runtime.rdb_path).expect("save rdb");

        append_aof_command(
            &runtime,
            0,
            vec![
                Bytes::from("SET"),
                Bytes::from("from-aof"),
                Bytes::from("2"),
            ],
        )
        .await
        .expect("append aof command");
        flush_aof(&runtime).await.expect("flush aof");

        let shared = Arc::new(SharedState::new(ServerState::with_default_dbs()));
        load_startup_data(&shared, &runtime, true)
            .await
            .expect("load startup");

        let loaded = shared.meta.lock().await;
        assert!(loaded.db(0).get(&Bytes::from("from-rdb")).is_none());
        assert_eq!(
            loaded
                .db(0)
                .get(&Bytes::from("from-aof"))
                .and_then(|v| v.as_string_bytes()),
            Some(Bytes::from("2"))
        );
    }

    #[tokio::test]
    async fn startup_load_replays_manifest_chain_without_an_independent_rdb() {
        use ratatosk_persist::aof::DEFAULT_SINGLE_FILE_AOF_FILENAME;

        let dir = tempfile::tempdir().expect("tmpdir");
        let runtime = runtime_without_writer(
            dir.path().join("dump.rdb"),
            dir.path().join(DEFAULT_SINGLE_FILE_AOF_FILENAME),
            Some(dir.path().join(DEFAULT_AOF_MANIFEST_FILENAME)),
        );

        let rdb_state = ServerState::with_default_dbs();
        rdb_state.db_mut(0).insert(
            Bytes::from("from-rdb"),
            StoredValue::string(Bytes::from("1"), None),
        );
        rdb::saver::save(&rdb_state.snapshot_dbs(), &runtime.rdb_path).expect("save rdb");

        let mut manifest = AofManifest::new(dir.path());
        manifest.set_base_after_rewrite("appendonly.aof.base.aof".into());
        let base_path = manifest.base_path().expect("base path");
        let incr_one = manifest.new_incr_file();
        let incr_two = manifest.new_incr_file();
        manifest
            .save_to_file(
                runtime
                    .aof_manifest_path
                    .as_ref()
                    .expect("manifest path should exist"),
            )
            .expect("save manifest");

        for (path, key, value) in [
            (&base_path, "from-base", "2"),
            (&incr_one, "from-incr-1", "3"),
            (&incr_two, "from-incr-2", "4"),
        ] {
            let mut writer = AofWriter::open(path, FsyncPolicy::Always).expect("open aof writer");
            writer
                .append_command(
                    0,
                    &[Bytes::from("SET"), Bytes::from(key), Bytes::from(value)],
                )
                .expect("append aof command");
            writer.force_fsync().expect("fsync");
        }

        let shared = Arc::new(SharedState::new(ServerState::with_default_dbs()));
        load_startup_data(&shared, &runtime, true)
            .await
            .expect("load startup");

        let loaded = shared.meta.lock().await;
        assert!(loaded.db(0).get(&Bytes::from("from-rdb")).is_none());
        for (key, value) in [
            ("from-base", "2"),
            ("from-incr-1", "3"),
            ("from-incr-2", "4"),
        ] {
            assert_eq!(
                loaded
                    .db(0)
                    .get(&Bytes::from(key))
                    .and_then(|entry| entry.as_string_bytes()),
                Some(Bytes::from(value))
            );
        }
    }

    #[tokio::test]
    async fn startup_refuses_to_truncate_a_damaged_earlier_chain_segment() {
        use ratatosk_persist::aof::DEFAULT_SINGLE_FILE_AOF_FILENAME;

        let dir = tempfile::tempdir().expect("tmpdir");
        let manifest_path = dir.path().join(DEFAULT_AOF_MANIFEST_FILENAME);
        let runtime = runtime_without_writer(
            dir.path().join("dump.rdb"),
            dir.path().join(DEFAULT_SINGLE_FILE_AOF_FILENAME),
            Some(manifest_path.clone()),
        );

        let mut manifest = AofManifest::new(dir.path());
        let incr_one = manifest.new_incr_file();
        let incr_two = manifest.new_incr_file();
        manifest
            .save_to_file(&manifest_path)
            .expect("save manifest");
        for (path, key) in [(&incr_one, "one"), (&incr_two, "two")] {
            let mut writer = AofWriter::open(path, FsyncPolicy::Always).expect("open aof writer");
            writer
                .append_command(0, &[Bytes::from("SET"), Bytes::from(key), Bytes::from("v")])
                .expect("append aof command");
        }
        let damaged_len = std::fs::metadata(&incr_one).expect("incr one").len();
        std::fs::OpenOptions::new()
            .append(true)
            .open(&incr_one)
            .and_then(|mut file| file.write_all(b"*3\r\n$3\r\nSET\r\n$2\r\nlo"))
            .expect("tear the first segment");

        let shared = Arc::new(SharedState::new(ServerState::with_default_dbs()));
        let error = load_startup_data(&shared, &runtime, true)
            .await
            .expect_err("a damaged earlier segment must stop startup");
        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
        assert!(error.to_string().contains("not the last file"), "{error}");
        assert!(
            std::fs::metadata(&incr_one).expect("incr one").len() > damaged_len,
            "the damaged segment must be left for inspection"
        );

        // The same tear on the final segment is still repaired.
        std::fs::OpenOptions::new()
            .write(true)
            .open(&incr_one)
            .and_then(|file| file.set_len(damaged_len))
            .expect("repair first segment");
        std::fs::OpenOptions::new()
            .append(true)
            .open(&incr_two)
            .and_then(|mut file| file.write_all(b"*3\r\n$3\r\nSET"))
            .expect("tear the final segment");
        let shared = Arc::new(SharedState::new(ServerState::with_default_dbs()));
        load_startup_data(&shared, &runtime, true)
            .await
            .expect("a torn final segment is truncated");
        {
            let loaded = shared.meta.lock().await;
            assert!(loaded.db(0).contains_key(b"one".as_slice()));
            assert!(loaded.db(0).contains_key(b"two".as_slice()));
        }

        // A header-only file after the damaged one, like the INCR file the
        // writer opens before replay, adds no dependent writes.
        std::fs::write(&incr_two, b"REDIS-AOF-002\n").expect("empty final segment");
        std::fs::OpenOptions::new()
            .append(true)
            .open(&incr_one)
            .and_then(|mut file| file.write_all(b"*3\r\n$3\r\nSET"))
            .expect("tear the last segment with data");
        let shared = Arc::new(SharedState::new(ServerState::with_default_dbs()));
        load_startup_data(&shared, &runtime, true)
            .await
            .expect("the last segment with data is truncated");
        assert_eq!(
            std::fs::metadata(&incr_one).expect("incr one").len(),
            damaged_len
        );
        assert!(
            shared
                .meta
                .lock()
                .await
                .db(0)
                .contains_key(b"one".as_slice())
        );
    }

    #[test]
    fn detect_legacy_aof_returns_path_when_legacy_exists_without_manifest() {
        use ratatosk_persist::aof::DEFAULT_SINGLE_FILE_AOF_FILENAME;

        let dir = tempfile::tempdir().expect("tmpdir");
        let legacy_path = dir.path().join(DEFAULT_SINGLE_FILE_AOF_FILENAME);
        let manifest_path = dir.path().join(DEFAULT_AOF_MANIFEST_FILENAME);

        // No legacy file, no manifest -> None
        assert!(detect_legacy_aof(&legacy_path, &manifest_path).is_none());

        // Legacy file exists, no manifest -> Some
        std::fs::write(&legacy_path, b"").expect("write legacy");
        assert_eq!(
            detect_legacy_aof(&legacy_path, &manifest_path),
            Some(legacy_path.clone())
        );

        // Both legacy file and manifest exist -> None (manifest takes precedence)
        let mut manifest = AofManifest::new(dir.path());
        manifest.new_incr_file();
        manifest
            .save_to_file(&manifest_path)
            .expect("save manifest");
        assert!(detect_legacy_aof(&legacy_path, &manifest_path).is_none());
    }

    #[test]
    fn migrate_legacy_aof_creates_manifest_with_base() {
        use ratatosk_persist::aof::DEFAULT_SINGLE_FILE_AOF_FILENAME;

        let dir = tempfile::tempdir().expect("tmpdir");
        let legacy_path = dir.path().join(DEFAULT_SINGLE_FILE_AOF_FILENAME);
        let manifest_path = dir.path().join(DEFAULT_AOF_MANIFEST_FILENAME);

        std::fs::write(&legacy_path, b"*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$1\r\nv\r\n")
            .expect("write legacy");

        let (active_path, manifest_opt) =
            super::migrate_legacy_aof_to_manifest(dir.path(), &legacy_path, &manifest_path)
                .expect("migrate");

        assert!(manifest_opt.is_some());
        assert_eq!(manifest_opt.unwrap(), manifest_path);

        let manifest = AofManifest::load_from_file(&manifest_path).expect("load manifest");
        assert_eq!(manifest.base_file(), Some(DEFAULT_SINGLE_FILE_AOF_FILENAME));
        assert_eq!(manifest.incr_files().len(), 1);
        assert_eq!(
            manifest.current_incr_path().expect("active incr"),
            active_path
        );
        // Legacy file is preserved
        assert!(legacy_path.exists());
    }

    #[test]
    fn bootstrap_aof_layout_warns_but_keeps_legacy_without_migrate_env() {
        use ratatosk_persist::aof::DEFAULT_SINGLE_FILE_AOF_FILENAME;

        let _env_lock = env_test_lock();
        let dir = tempfile::tempdir().expect("tmpdir");
        let legacy_path = dir.path().join(DEFAULT_SINGLE_FILE_AOF_FILENAME);
        let manifest_path = dir.path().join(DEFAULT_AOF_MANIFEST_FILENAME);

        std::fs::write(&legacy_path, b"").expect("write legacy");

        let _migrate_env = ScopedEnvVar::remove("RATATOSK_MIGRATE_AOF");

        let (aof_path, manifest_opt) =
            bootstrap_aof_layout(dir.path(), &legacy_path, &manifest_path).expect("bootstrap");

        assert_eq!(aof_path, legacy_path);
        assert!(manifest_opt.is_none());
        assert!(!manifest_path.exists());
    }

    #[test]
    fn startup_aof_recovery_paths_fails_closed_when_manifest_file_is_missing() {
        let _env_lock = env_test_lock();
        let dir = tempfile::tempdir().expect("tmpdir");
        let manifest_path = dir.path().join(DEFAULT_AOF_MANIFEST_FILENAME);
        let mut manifest = AofManifest::new(dir.path());
        manifest.set_base_after_rewrite("appendonly.aof.base.aof".into());
        let _incr_path = manifest.new_incr_file();
        manifest
            .save_to_file(&manifest_path)
            .expect("save manifest");

        let runtime = runtime_without_writer(
            dir.path().join("dump.rdb"),
            dir.path().join("appendonly.aof.1.incr.aof"),
            Some(manifest_path.clone()),
        );

        let _override_env = ScopedEnvVar::remove(ALLOW_INCOMPLETE_AOF_CHAIN_ENV);

        let error = startup_aof_recovery_paths(&runtime).expect_err("startup should fail closed");
        let text = error.to_string();
        assert!(matches!(error.kind(), io::ErrorKind::InvalidData));
        assert!(text.contains("missing recovery file(s)"), "{text}");
        assert!(text.contains(ALLOW_INCOMPLETE_AOF_CHAIN_ENV), "{text}");
    }

    #[test]
    fn startup_aof_recovery_paths_allows_missing_manifest_file_with_override() {
        let _env_lock = env_test_lock();
        let dir = tempfile::tempdir().expect("tmpdir");
        let manifest_path = dir.path().join(DEFAULT_AOF_MANIFEST_FILENAME);
        let mut manifest = AofManifest::new(dir.path());
        manifest.set_base_after_rewrite("appendonly.aof.base.aof".into());
        let existing_incr = manifest.new_incr_file();
        let _missing_incr = manifest.new_incr_file();
        manifest
            .save_to_file(&manifest_path)
            .expect("save manifest");

        std::fs::write(&existing_incr, b"REDIS-AOF-001\n").expect("write existing incr");

        let runtime = runtime_without_writer(
            dir.path().join("dump.rdb"),
            existing_incr.clone(),
            Some(manifest_path),
        );

        let _override_env = ScopedEnvVar::set(ALLOW_INCOMPLETE_AOF_CHAIN_ENV, "true");

        let files = startup_aof_recovery_paths(&runtime).expect("override should allow startup");
        assert_eq!(files, vec![existing_incr]);
    }

    #[tokio::test]
    async fn rewrite_aof_via_worker_keeps_writer_usable() {
        let dir = tempfile::tempdir().expect("tmpdir");
        let config = ServerConfig {
            dir: dir.path().to_path_buf(),
            appendonly: true,
            appendfsync: "always".to_string(),
            ..ServerConfig::default()
        };

        let runtime = PersistenceRuntime::from_config(&config).expect("runtime");

        append_aof_command(
            &runtime,
            0,
            vec![
                Bytes::from("SET"),
                Bytes::from("pre"),
                Bytes::from("rewrite"),
            ],
        )
        .await
        .expect("append before rewrite");
        flush_aof(&runtime).await.expect("flush before rewrite");

        let snapshot_state = ServerState::with_default_dbs();
        snapshot_state.db_mut(0).insert(
            Bytes::from("pre"),
            StoredValue::string(Bytes::from("rewrite"), None),
        );
        let reply = enqueue_aof_rewrite(&runtime, snapshot_state.snapshot_dbs())
            .await
            .expect("enqueue rewrite request");
        // Queue an increment before awaiting the rewrite completion. FIFO
        // worker ordering must put it after the materialized BASE rather than
        // losing it with the retired history.
        append_aof_command(
            &runtime,
            0,
            vec![
                Bytes::from("SET"),
                Bytes::from("post"),
                Bytes::from("rewrite"),
            ],
        )
        .await
        .expect("append after rewrite");
        flush_aof(&runtime).await.expect("flush after rewrite");
        assert!(
            await_aof_rewrite(&runtime, reply)
                .await
                .expect("rewrite request should succeed")
        );

        let manifest_path = runtime
            .aof_manifest_path
            .as_ref()
            .expect("manifest path should exist");
        let manifest_after_rewrite =
            AofManifest::load_from_file(manifest_path).expect("load manifest after rewrite");
        assert_eq!(manifest_after_rewrite.incr_files().len(), 1);
        assert_ne!(
            runtime.aof_active_path().expect("current incr path"),
            runtime.aof_path
        );

        let shared = Arc::new(SharedState::new(ServerState::with_default_dbs()));
        load_startup_data(&shared, &runtime, true)
            .await
            .expect("load startup after rewrite");
        let loaded = shared.meta.lock().await;

        assert_eq!(
            loaded
                .db(0)
                .get(&Bytes::from("pre"))
                .and_then(|v| v.as_string_bytes()),
            Some(Bytes::from("rewrite"))
        );
        assert_eq!(
            loaded
                .db(0)
                .get(&Bytes::from("post"))
                .and_then(|v| v.as_string_bytes()),
            Some(Bytes::from("rewrite"))
        );
    }

    #[tokio::test]
    async fn stale_rewrite_completion_does_not_replace_new_writer_path() {
        let dir = tempfile::tempdir().expect("tmpdir");
        let old_path = dir.path().join("old.incr.aof");
        let new_path = dir.path().join("new.incr.aof");
        let runtime = runtime_without_writer(
            dir.path().join("dump.rdb"),
            old_path.clone(),
            Some(dir.path().join(DEFAULT_AOF_MANIFEST_FILENAME)),
        );

        let (old_sender, _old_receiver) = mpsc::channel(1);
        let (new_sender, _new_receiver) = mpsc::channel(1);
        {
            let mut state = runtime
                .aof
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            state.sender = Some(old_sender);
            state.generation = 1;
            state.active_path = old_path;
        }

        let (reply_tx, reply_rx) = oneshot::channel();
        reply_tx
            .send(Ok(dir.path().join("rewritten-old.incr.aof")))
            .expect("send rewrite result");

        {
            let mut state = runtime
                .aof
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            state.sender = Some(new_sender);
            state.generation = 2;
            state.active_path = new_path.clone();
        }

        assert!(
            !await_aof_rewrite(
                &runtime,
                AofRewriteRequest {
                    generation: 1,
                    reply: reply_rx,
                },
            )
            .await
            .expect("stale completion should be ignored")
        );
        assert_eq!(runtime.aof_active_path(), Some(new_path));
    }

    #[tokio::test]
    async fn closed_control_channels_clear_the_active_writer() {
        let dir = tempfile::tempdir().expect("tmpdir");
        let runtime = runtime_without_writer(
            dir.path().join("dump.rdb"),
            dir.path().join("appendonly.aof"),
            None,
        );

        let (policy_sender, policy_receiver) = mpsc::channel(1);
        drop(policy_receiver);
        {
            let mut state = runtime
                .aof
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            state.sender = Some(policy_sender);
            state.generation = 1;
        }
        assert!(
            set_aof_fsync_policy(&runtime, FsyncPolicy::Always)
                .await
                .is_err()
        );
        assert!(runtime.aof_sender().is_none());

        let (shutdown_sender, shutdown_receiver) = mpsc::channel(1);
        drop(shutdown_receiver);
        {
            let mut state = runtime
                .aof
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            state.sender = Some(shutdown_sender);
            state.generation = 3;
        }
        assert!(disable_aof(&runtime).await.is_err());
        assert!(runtime.aof_sender().is_none());
    }

    #[tokio::test(flavor = "current_thread")]
    async fn explicit_worker_shutdown_is_graceful_without_a_warning() {
        let logs = Arc::new(Mutex::new(Vec::new()));
        let _subscriber = install_log_subscriber(Arc::clone(&logs));
        let dir = tempfile::tempdir().expect("tmpdir");
        let path = dir.path().join("graceful-worker.aof");
        let writer = AofWriter::open(&path, FsyncPolicy::No).expect("open AOF writer");
        let sender = spawn_aof_worker(path.clone(), None, writer, FsyncPolicy::No, 1);
        let (reply_tx, reply_rx) = oneshot::channel();

        sender
            .send(AofWorkerCommand::Shutdown { reply: reply_tx })
            .await
            .expect("enqueue shutdown");
        reply_rx
            .await
            .expect("worker shutdown reply")
            .expect("worker shutdown fsync");

        let output = wait_for_aof_log(&logs, &path, "AOF worker shut down gracefully").await;
        assert!(
            output
                .lines()
                .filter(|line| line.contains(&path.display().to_string()))
                .all(|line| !line.contains(" WARN ")),
            "graceful worker shutdown emitted a warning:\n{output}"
        );
    }

    async fn worker_append(sender: &mpsc::Sender<AofWorkerCommand>, key: &str) {
        let (reply, reply_rx) = oneshot::channel();
        sender
            .send(AofWorkerCommand::Append {
                db_index: 0,
                argv: vec![
                    Bytes::from("SET"),
                    Bytes::from(key.to_string()),
                    Bytes::from("v"),
                ],
                timestamp_ms: 1,
                reply,
            })
            .await
            .expect("enqueue append");
        reply_rx
            .await
            .expect("append reply")
            .expect("append acknowledged");
    }

    fn aof_has_keys_in_order(path: &Path, keys: &[&str]) -> bool {
        let text = String::from_utf8_lossy(&std::fs::read(path).expect("read aof")).into_owned();
        let mut from = 0;
        for key in keys {
            let needle = format!("${}\r\n{key}\r\n", key.len());
            match text[from..].find(&needle) {
                Some(at) => from += at + needle.len(),
                None => return false,
            }
        }
        true
    }

    #[tokio::test]
    async fn everysec_worker_keeps_order_across_background_fsync_and_drains_on_controls() {
        // Own subscriber so worker log callsites register as enabled; without
        // one they can cache "disabled" and starve the log-asserting tests.
        let _subscriber = install_log_subscriber(Arc::new(Mutex::new(Vec::new())));
        let dir = tempfile::tempdir().expect("tmpdir");
        let path = dir.path().join("everysec-worker.aof");
        let writer = AofWriter::open(&path, FsyncPolicy::EverySec).expect("open AOF writer");
        let sender = spawn_aof_worker(path.clone(), None, writer, FsyncPolicy::EverySec, 64);

        worker_append(&sender, "k1").await;
        // Make an fsync due so the next append starts the background fsync.
        tokio::time::sleep(Duration::from_millis(1100)).await;
        worker_append(&sender, "k2").await;
        worker_append(&sender, "k3").await;

        let (reply, reply_rx) = oneshot::channel();
        sender
            .send(AofWorkerCommand::Flush { reply })
            .await
            .expect("enqueue flush");
        reply_rx.await.expect("flush reply").expect("flush");
        assert!(aof_has_keys_in_order(&path, &["k1", "k2", "k3"]));

        // A tick with nothing due is accepted and changes nothing.
        let (reply, reply_rx) = oneshot::channel();
        sender
            .send(AofWorkerCommand::EverySecTick { reply })
            .await
            .expect("enqueue tick");
        reply_rx.await.expect("tick reply").expect("tick");

        tokio::time::sleep(Duration::from_millis(1100)).await;
        worker_append(&sender, "k4").await;
        worker_append(&sender, "k5").await;
        let (reply, reply_rx) = oneshot::channel();
        sender
            .send(AofWorkerCommand::SetPolicy {
                policy: FsyncPolicy::No,
                reply,
            })
            .await
            .expect("enqueue set policy");
        reply_rx
            .await
            .expect("set policy reply")
            .expect("set policy");
        assert!(aof_has_keys_in_order(
            &path,
            &["k1", "k2", "k3", "k4", "k5"]
        ));

        worker_append(&sender, "k6").await;
        let (reply, reply_rx) = oneshot::channel();
        sender
            .send(AofWorkerCommand::Shutdown { reply })
            .await
            .expect("enqueue shutdown");
        reply_rx.await.expect("shutdown reply").expect("shutdown");
        assert!(aof_has_keys_in_order(
            &path,
            &["k1", "k2", "k3", "k4", "k5", "k6"]
        ));
    }

    /// An everysec worker whose first background fsync fails, with that
    /// failure already collected and stored by the writer.
    async fn worker_with_failed_background_fsync(path: &Path) -> mpsc::Sender<AofWorkerCommand> {
        let mut writer = AofWriter::open(path, FsyncPolicy::EverySec).expect("open AOF writer");
        let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        writer.set_fsync_hook_for_tests(Arc::new(move |file: &std::fs::File| {
            if calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst) == 0 {
                Err(io::Error::other("synthetic background fsync failure"))
            } else {
                file.sync_all()
            }
        }));
        let sender = spawn_aof_worker(path.to_path_buf(), None, writer, FsyncPolicy::EverySec, 64);
        worker_append(&sender, "k1").await;
        tokio::time::sleep(Duration::from_millis(1100)).await;
        // Due: this append starts the failing background fsync.
        worker_append(&sender, "k2").await;
        // The worker polls every 10 ms and collects the failure.
        tokio::time::sleep(Duration::from_millis(200)).await;
        sender
    }

    async fn worker_append_result(
        sender: &mpsc::Sender<AofWorkerCommand>,
        key: &str,
    ) -> Result<(), String> {
        let (reply, reply_rx) = oneshot::channel();
        sender
            .send(AofWorkerCommand::Append {
                db_index: 0,
                argv: vec![
                    Bytes::from("SET"),
                    Bytes::from(key.to_string()),
                    Bytes::from("v"),
                ],
                timestamp_ms: 1,
                reply,
            })
            .await
            .expect("enqueue append");
        reply_rx.await.expect("append reply")
    }

    /// Stop the worker explicitly so it never logs the channel-closed warning,
    /// which would pollute the tracing callsite cache that
    /// `worker_channel_disconnect_remains_a_warning` depends on.
    async fn worker_shutdown(sender: &mpsc::Sender<AofWorkerCommand>) {
        let (reply, reply_rx) = oneshot::channel();
        sender
            .send(AofWorkerCommand::Shutdown { reply })
            .await
            .expect("enqueue shutdown");
        let _ = reply_rx.await;
    }

    #[tokio::test]
    async fn config_set_does_not_swallow_a_background_fsync_failure() {
        // Own subscriber so worker log callsites register as enabled; without
        // one they can cache "disabled" and starve the log-asserting tests.
        let _subscriber = install_log_subscriber(Arc::new(Mutex::new(Vec::new())));
        let dir = tempfile::tempdir().expect("tmpdir");
        let path = dir.path().join("bg-fail-policy.aof");
        let sender = worker_with_failed_background_fsync(&path).await;

        let (reply, reply_rx) = oneshot::channel();
        sender
            .send(AofWorkerCommand::SetPolicy {
                policy: FsyncPolicy::Always,
                reply,
            })
            .await
            .expect("enqueue set policy");
        reply_rx
            .await
            .expect("set policy reply")
            .expect("CONFIG SET itself must succeed");

        let error = worker_append_result(&sender, "k3")
            .await
            .expect_err("the stored failure must reach the next append, which latches it");
        assert!(
            error.contains("synthetic background fsync failure"),
            "{error}"
        );
        worker_append_result(&sender, "k4")
            .await
            .expect("the failure is delivered once");
        worker_shutdown(&sender).await;
    }

    #[tokio::test]
    async fn rewrite_reports_a_background_fsync_failure_and_keeps_it_for_the_latch() {
        // Own subscriber so worker log callsites register as enabled; without
        // one they can cache "disabled" and starve the log-asserting tests.
        let _subscriber = install_log_subscriber(Arc::new(Mutex::new(Vec::new())));
        let dir = tempfile::tempdir().expect("tmpdir");
        let path = dir.path().join("bg-fail-rewrite.aof");
        let sender = worker_with_failed_background_fsync(&path).await;

        let (reply, reply_rx) = oneshot::channel();
        sender
            .send(AofWorkerCommand::Rewrite {
                snapshot: ServerState::with_default_dbs().snapshot_dbs(),
                reply,
            })
            .await
            .expect("enqueue rewrite");
        let error = reply_rx
            .await
            .expect("rewrite reply")
            .expect_err("rewrite must not succeed over a failed fsync");
        assert!(
            error.contains("synthetic background fsync failure"),
            "{error}"
        );

        let error = worker_append_result(&sender, "k3")
            .await
            .expect_err("append still latches the failure");
        assert!(
            error.contains("synthetic background fsync failure"),
            "{error}"
        );
    }

    #[tokio::test]
    async fn shutdown_reports_a_background_fsync_failure_after_fsyncing() {
        // Own subscriber so worker log callsites register as enabled; without
        // one they can cache "disabled" and starve the log-asserting tests.
        let _subscriber = install_log_subscriber(Arc::new(Mutex::new(Vec::new())));
        let dir = tempfile::tempdir().expect("tmpdir");
        let path = dir.path().join("bg-fail-shutdown.aof");
        let sender = worker_with_failed_background_fsync(&path).await;

        let (reply, reply_rx) = oneshot::channel();
        sender
            .send(AofWorkerCommand::Shutdown { reply })
            .await
            .expect("enqueue shutdown");
        let error = reply_rx
            .await
            .expect("shutdown reply")
            .expect_err("shutdown must surface the failure");
        assert!(
            error.contains("synthetic background fsync failure"),
            "{error}"
        );
        assert!(
            aof_has_keys_in_order(&path, &["k1", "k2"]),
            "held-back records are still written"
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn worker_channel_disconnect_remains_a_warning() {
        let logs = Arc::new(Mutex::new(Vec::new()));
        let _subscriber = install_log_subscriber(Arc::clone(&logs));
        let dir = tempfile::tempdir().expect("tmpdir");
        let path = dir.path().join("disconnected-worker.aof");
        let writer = AofWriter::open(&path, FsyncPolicy::No).expect("open AOF writer");
        let sender = spawn_aof_worker(path.clone(), None, writer, FsyncPolicy::No, 1);

        drop(sender);

        let output =
            wait_for_aof_log(&logs, &path, "AOF worker channel closed; worker exiting").await;
        assert!(
            output.lines().any(|line| {
                line.contains(&path.display().to_string())
                    && line.contains(" WARN ")
                    && line.contains("AOF worker channel closed; worker exiting")
            }),
            "unexpected worker disconnect was not logged as a warning:\n{output}"
        );
    }

    #[tokio::test]
    async fn terminal_shutdown_bounds_the_acknowledgement_wait() {
        let dir = tempfile::tempdir().expect("tmpdir");
        let runtime = runtime_without_writer(
            dir.path().join("dump.rdb"),
            dir.path().join("timed-out-shutdown.aof"),
            None,
        );
        let (sender, mut receiver) = mpsc::channel(1);
        {
            let mut state = runtime.aof.lock().expect("runtime state");
            state.sender = Some(sender);
            state.generation = 1;
        }

        let shutdown_task = tokio::spawn({
            let runtime = runtime.clone();
            async move {
                disable_aof_with_reply_timeout(&runtime, Some(Duration::from_millis(20)), true)
                    .await
            }
        });
        let AofWorkerCommand::Shutdown { reply } =
            receiver.recv().await.expect("accepted shutdown")
        else {
            panic!("expected shutdown control")
        };

        let error = shutdown_task
            .await
            .expect("shutdown task")
            .expect_err("missing acknowledgement must time out");
        assert_eq!(error.kind(), io::ErrorKind::TimedOut);
        assert!(
            error
                .to_string()
                .contains("timed out waiting for AOF shutdown completion")
        );
        assert!(runtime.aof_sender().is_none());
        drop(reply);
    }

    #[tokio::test]
    async fn terminal_shutdown_surfaces_the_worker_fsync_error() {
        let dir = tempfile::tempdir().expect("tmpdir");
        let runtime = runtime_without_writer(
            dir.path().join("dump.rdb"),
            dir.path().join("failed-shutdown.aof"),
            None,
        );
        let (sender, mut receiver) = mpsc::channel(1);
        {
            let mut state = runtime.aof.lock().expect("runtime state");
            state.sender = Some(sender);
            state.generation = 1;
        }

        let shutdown_task = tokio::spawn({
            let runtime = runtime.clone();
            async move {
                disable_aof_with_reply_timeout(&runtime, Some(Duration::from_secs(1)), true).await
            }
        });
        let AofWorkerCommand::Shutdown { reply } =
            receiver.recv().await.expect("accepted shutdown")
        else {
            panic!("expected shutdown control")
        };
        reply
            .send(Err("synthetic shutdown fsync failure".to_string()))
            .expect("send worker failure");

        let error = shutdown_task
            .await
            .expect("shutdown task")
            .expect_err("worker fsync failure must be returned");
        assert!(
            error
                .to_string()
                .contains("synthetic shutdown fsync failure")
        );
        assert!(runtime.aof_sender().is_none());
    }

    #[tokio::test]
    async fn accepted_controls_wait_past_the_old_reply_deadline() {
        let dir = tempfile::tempdir().expect("tmpdir");
        let policy_runtime = runtime_without_writer(
            dir.path().join("dump.rdb"),
            dir.path().join("policy.aof"),
            None,
        );
        let shutdown_runtime = runtime_without_writer(
            dir.path().join("dump.rdb"),
            dir.path().join("shutdown.aof"),
            None,
        );
        let (policy_sender, mut policy_receiver) = mpsc::channel(1);
        let (shutdown_sender, mut shutdown_receiver) = mpsc::channel(1);
        for (runtime, sender) in [
            (&policy_runtime, policy_sender),
            (&shutdown_runtime, shutdown_sender),
        ] {
            let mut state = runtime.aof.lock().expect("runtime state");
            state.sender = Some(sender);
            state.generation = 1;
        }
        let policy_task = tokio::spawn({
            let runtime = policy_runtime.clone();
            async move { set_aof_fsync_policy(&runtime, FsyncPolicy::No).await }
        });
        let shutdown_task = tokio::spawn({
            let runtime = shutdown_runtime.clone();
            async move { disable_aof(&runtime).await }
        });
        let AofWorkerCommand::SetPolicy {
            reply: policy_reply,
            ..
        } = policy_receiver.recv().await.expect("accepted policy")
        else {
            panic!("expected policy control")
        };
        let AofWorkerCommand::Shutdown {
            reply: shutdown_reply,
        } = shutdown_receiver.recv().await.expect("accepted shutdown")
        else {
            panic!("expected shutdown control")
        };
        tokio::time::sleep(AOF_APPEND_REPLY_TIMEOUT + Duration::from_millis(150)).await;
        assert!(
            !policy_task.is_finished(),
            "accepted policy must not time out before worker completion"
        );
        assert!(
            !shutdown_task.is_finished(),
            "accepted shutdown must not time out before worker completion"
        );
        policy_reply.send(Ok(())).expect("policy completion");
        shutdown_reply.send(Ok(())).expect("shutdown completion");
        policy_task
            .await
            .expect("policy task")
            .expect("policy result");
        shutdown_task
            .await
            .expect("shutdown task")
            .expect("shutdown result");
        assert_eq!(policy_runtime.aof_policy(), FsyncPolicy::No);
        assert!(shutdown_runtime.aof_sender().is_none());
    }
}
