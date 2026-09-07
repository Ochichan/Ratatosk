mod aof;
mod rdb;
mod util;

use std::{
    io,
    path::PathBuf,
    sync::{Arc, Mutex, OnceLock},
    time::Duration,
};

use ratatosk_core::time::now_ms;
use ratatosk_engine::keyspace::SharedState;
use ratatosk_persist::aof::{
    AofManifest, AofWriter, DEFAULT_AOF_MANIFEST_FILENAME, DEFAULT_SINGLE_FILE_AOF_FILENAME,
    FsyncPolicy,
};
use tokio::sync::mpsc;

use crate::config::ServerConfig;

use self::aof::{
    AofWorkerCommand, aof_queue_capacity_from_env, await_aof_rewrite, bootstrap_aof_layout,
    enqueue_aof_rewrite, replace_aof_with_snapshot, replay_startup_aof_file, spawn_aof_worker,
    startup_aof_recovery_paths,
};
use self::util::{check_disk_space, validate_aof_file, validate_working_directory};

// Public re-exports to maintain the existing API surface.
pub use self::aof::{append_aof_command, append_aof_effects, flush_aof};
pub(crate) use self::aof::{disable_aof, enable_aof_from_snapshot, set_aof_fsync_policy};
pub use self::rdb::run_save;

static BGSAVE_TASKS: OnceLock<std::sync::Mutex<Vec<tokio::task::JoinHandle<()>>>> = OnceLock::new();
static BGREWRITEAOF_TASKS: OnceLock<std::sync::Mutex<Vec<tokio::task::JoinHandle<()>>>> =
    OnceLock::new();

fn bgsave_tasks() -> &'static std::sync::Mutex<Vec<tokio::task::JoinHandle<()>>> {
    BGSAVE_TASKS.get_or_init(|| std::sync::Mutex::new(Vec::new()))
}

fn bgrewriteaof_tasks() -> &'static std::sync::Mutex<Vec<tokio::task::JoinHandle<()>>> {
    BGREWRITEAOF_TASKS.get_or_init(|| std::sync::Mutex::new(Vec::new()))
}

#[derive(Clone)]
pub struct PersistenceRuntime {
    pub rdb_path: PathBuf,
    pub aof_path: PathBuf,
    pub aof_manifest_path: Option<PathBuf>,
    aof: Arc<Mutex<AofRuntimeState>>,
}

pub(crate) struct AofRuntimeState {
    pub(crate) sender: Option<mpsc::Sender<AofWorkerCommand>>,
    /// Changes whenever the active writer is replaced or removed. Async
    /// rewrite completions use it to avoid publishing stale lineage paths.
    pub(crate) generation: u64,
    pub(crate) active_path: PathBuf,
    pub(crate) manifest_path: Option<PathBuf>,
    pub(crate) policy: FsyncPolicy,
}

impl PersistenceRuntime {
    pub fn from_config(config: &ServerConfig) -> io::Result<Self> {
        validate_working_directory(&config.dir)?;
        check_disk_space(&config.dir, 100 * 1024 * 1024)?;

        let rdb_path = config.dir.join(&config.dbfilename);
        let legacy_aof_path = config.dir.join(DEFAULT_SINGLE_FILE_AOF_FILENAME);
        let manifest_path = config.dir.join(DEFAULT_AOF_MANIFEST_FILENAME);
        let (aof_path, aof_manifest_path) = if config.appendonly {
            bootstrap_aof_layout(&config.dir, &legacy_aof_path, &manifest_path)?
        } else {
            (legacy_aof_path.clone(), None)
        };

        if config.appendonly && aof_path.exists() {
            validate_aof_file(&aof_path)?;
        }

        let policy = FsyncPolicy::from_config_str(config.appendfsync.as_bytes())
            .unwrap_or(FsyncPolicy::EverySec);
        let aof_tx = if config.appendonly {
            let writer = AofWriter::open(&aof_path, policy)
                .map_err(|e| io::Error::other(format!("opening AOF writer: {e}")))?;
            let queue_capacity = aof_queue_capacity_from_env();
            Some(spawn_aof_worker(
                aof_path.clone(),
                aof_manifest_path.clone(),
                writer,
                policy,
                queue_capacity,
            ))
        } else {
            None
        };

        let active_path = aof_path.clone();
        let active_manifest_path = aof_manifest_path.clone();
        let aof_generation = u64::from(aof_tx.is_some());

        Ok(Self {
            rdb_path,
            aof_path,
            aof_manifest_path,
            aof: Arc::new(Mutex::new(AofRuntimeState {
                sender: aof_tx,
                generation: aof_generation,
                active_path,
                manifest_path: active_manifest_path,
                policy,
            })),
        })
    }

    pub(crate) fn aof_sender(&self) -> Option<mpsc::Sender<AofWorkerCommand>> {
        self.aof_sender_with_generation()
            .map(|(sender, _generation)| sender)
    }

    pub(crate) fn aof_sender_with_generation(
        &self,
    ) -> Option<(mpsc::Sender<AofWorkerCommand>, u64)> {
        let state = self
            .aof
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        state
            .sender
            .as_ref()
            .map(|sender| (sender.clone(), state.generation))
    }

    pub(crate) fn aof_generation_is_current(&self, generation: u64) -> bool {
        let state = self
            .aof
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        state.sender.is_some() && state.generation == generation
    }

    pub(crate) fn aof_policy(&self) -> FsyncPolicy {
        self.aof
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .policy
    }

    pub(crate) fn aof_active_path(&self) -> Option<PathBuf> {
        let state = self
            .aof
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        state.sender.as_ref().map(|_| state.active_path.clone())
    }

    pub(crate) fn aof_base_path(&self) -> io::Result<Option<PathBuf>> {
        let manifest_path = self
            .aof
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .manifest_path
            .clone();
        match manifest_path {
            Some(path) if path.exists() => {
                AofManifest::load_from_file(&path).map(|m| m.base_path())
            }
            _ => Ok(None),
        }
    }
}

fn runtime_aof_file_paths(
    runtime: &PersistenceRuntime,
) -> io::Result<(Option<PathBuf>, Option<PathBuf>)> {
    let current_path = runtime.aof_active_path();
    let base_path = runtime.aof_base_path()?;

    Ok((current_path, base_path))
}

pub async fn sync_server_aof_file_info(
    server_state: &Arc<SharedState>,
    runtime: &PersistenceRuntime,
) -> io::Result<()> {
    let (current_path, base_path) = runtime_aof_file_paths(runtime)?;
    let mut state = server_state.meta.lock().await;
    state.set_aof_current_path(current_path);
    state.set_aof_base_path(base_path);
    Ok(())
}

/// Publish AOF file paths only if the writer that produced them is still the
/// active lineage. The server-state lock is acquired before looking up runtime
/// paths, matching the lifecycle mutation lock order and preventing an old
/// BGREWRITE completion from racing a disable/enable transition.
async fn sync_server_aof_file_info_if_current_generation(
    server_state: &Arc<SharedState>,
    runtime: &PersistenceRuntime,
    generation: u64,
) -> io::Result<bool> {
    let mut state = server_state.meta.lock().await;
    if !runtime.aof_generation_is_current(generation) {
        return Ok(false);
    }

    let (current_path, base_path) = runtime_aof_file_paths(runtime)?;
    state.set_aof_current_path(current_path);
    state.set_aof_base_path(base_path);
    Ok(true)
}

pub async fn load_startup_data(
    server_state: &Arc<SharedState>,
    runtime: &PersistenceRuntime,
    appendonly: bool,
) -> io::Result<()> {
    if appendonly {
        let recovery_paths = startup_aof_recovery_paths(runtime)?;
        if aof_chain_has_authoritative_data(&recovery_paths)? {
            for path in recovery_paths {
                if path.extension().is_some_and(|extension| extension == "rdb") {
                    let snapshot = ratatosk_persist::rdb::loader::load(&path).map_err(|error| {
                        io::Error::other(format!("loading AOF BASE snapshot: {error}"))
                    })?;
                    let key_count: usize = snapshot.iter().map(|db| db.len()).sum();
                    let state = server_state.meta.lock().await;
                    state.load_from_rdb(snapshot);
                    tracing::info!(path = %path.display(), keys = key_count, "loaded authoritative AOF BASE snapshot");
                } else {
                    replay_startup_aof_file(server_state, &path).await?;
                }
            }
            return Ok(());
        }
    }

    let loaded_rdb = if runtime.rdb_path.exists() {
        let snapshot = ratatosk_persist::rdb::loader::load(&runtime.rdb_path)
            .map_err(|error| io::Error::other(format!("loading RDB snapshot: {error}")))?;
        let key_count: usize = snapshot.iter().map(|db| db.len()).sum();
        {
            let state = server_state.meta.lock().await;
            state.load_from_rdb(snapshot);
        }
        tracing::info!(keys = key_count, "loaded RDB snapshot");
        true
    } else {
        tracing::info!(path = %runtime.rdb_path.display(), "no RDB file found, starting empty");
        false
    };

    // An empty AOF has no history.  When it is enabled beside an existing RDB,
    // turn that RDB into the first authoritative AOF BASE before accepting
    // writes.  This avoids both losing the snapshot and replaying it twice.
    if appendonly && loaded_rdb {
        let snapshot = server_state.data.snapshot_all();
        replace_aof_with_snapshot(runtime, &snapshot, runtime.aof_policy()).await?;
        sync_server_aof_file_info(server_state, runtime).await?;
    }

    Ok(())
}

fn aof_chain_has_authoritative_data(paths: &[PathBuf]) -> io::Result<bool> {
    const AOF_HEADER_LEN: u64 = b"REDIS-AOF-001\n".len() as u64;

    for path in paths {
        if path.extension().is_some_and(|extension| extension == "rdb") {
            return Ok(true);
        }

        let metadata = std::fs::metadata(path).map_err(|error| {
            io::Error::new(
                error.kind(),
                format!(
                    "reading AOF recovery metadata '{}': {error}",
                    path.display()
                ),
            )
        })?;
        if metadata.len() == 0 {
            continue;
        }

        let header = std::fs::read(path).map_err(|error| {
            io::Error::new(
                error.kind(),
                format!("reading AOF recovery file '{}': {error}", path.display()),
            )
        })?;
        if (header.starts_with(b"REDIS-AOF-001\n") || header.starts_with(b"REDIS-AOF-002\n"))
            && metadata.len() <= AOF_HEADER_LEN
        {
            continue;
        }
        return Ok(true);
    }

    Ok(false)
}

pub async fn start_bgsave(server_state: Arc<SharedState>, rdb_path: PathBuf) -> bool {
    {
        let mut state = server_state.meta.lock().await;
        if state.rdb_save_in_progress() {
            return false;
        }
        state.set_rdb_save_in_progress(true);
    }

    let snapshot = server_state.data.snapshot_all();

    crate::metrics::record_rdb_save(true);

    let handle = tokio::spawn(async move {
        let path_for_log = rdb_path.display().to_string();
        let save_result = tokio::task::spawn_blocking(move || {
            ratatosk_persist::rdb::saver::save(&snapshot, &rdb_path)
        })
        .await
        .map_err(|error| format!("joining BGSAVE worker: {error}"))
        .and_then(|result| result.map_err(|error| error.to_string()));

        let mut state = server_state.meta.lock().await;
        state.set_rdb_save_in_progress(false);

        match save_result {
            Ok(()) => {
                state.stats.mark_last_save_now();
                state.set_last_rdb_save_time_ms(now_ms());
                state.set_last_rdb_save_status(Ok(()));
                tracing::info!(path = %path_for_log, "background RDB save completed");
            }
            Err(error) => {
                state.set_last_rdb_save_status(Err(error.clone()));
                crate::metrics::record_rdb_save_error();
                tracing::warn!(error = %error, path = %path_for_log, "background RDB save failed");
            }
        }
    });

    if let Ok(mut tasks) = bgsave_tasks().lock() {
        tasks.push(handle);
    }

    true
}

pub async fn drain_bgsave_tasks(grace: Duration) -> (usize, usize) {
    let (completed, aborted) = drain_task_handles(
        bgsave_tasks(),
        grace,
        crate::metrics::record_bgsave_task_timeout,
    )
    .await;

    if aborted > 0 {
        crate::metrics::record_bgsave_tasks_aborted(aborted as u64);
    }

    (completed, aborted)
}

pub async fn start_bgrewriteaof(
    server_state: Arc<SharedState>,
    runtime: Arc<PersistenceRuntime>,
) -> bool {
    // Acquire the same state lock used by mutations before snapshotting and
    // enqueue the rewrite before releasing it.  Existing writes are already
    // ahead of this worker message; later writes can only enqueue after it,
    // so the rewritten BASE and following INCR remain one ordered lineage.
    let rewrite_reply = {
        let mut state = server_state.meta.lock().await;
        if runtime.aof_sender().is_none() {
            state.set_aof_rewrite_in_progress(false);
            state.set_last_aof_rewrite_status(Err("appendonly is disabled".to_string()));
            state.set_last_aof_rewrite_time_ms(now_ms());
            return false;
        }

        let snapshot = server_state.data.snapshot_all();
        match enqueue_aof_rewrite(&runtime, snapshot).await {
            Ok(reply) => reply,
            Err(error) => {
                state.set_aof_rewrite_in_progress(false);
                state.set_last_aof_rewrite_status(Err(error.to_string()));
                state.set_last_aof_rewrite_time_ms(now_ms());
                crate::metrics::record_aof_rewrite("error");
                tracing::warn!(
                    target = "ratatosk::aof",
                    error = %error,
                    "failed to enqueue background AOF rewrite"
                );
                return false;
            }
        }
    };

    crate::metrics::record_aof_rewrite("requested");
    let rewrite_generation = rewrite_reply.generation();

    let handle = tokio::spawn(async move {
        let result = await_aof_rewrite(&runtime, rewrite_reply).await;
        if !runtime.aof_generation_is_current(rewrite_generation) {
            tracing::debug!(
                target = "ratatosk::aof",
                generation = rewrite_generation,
                "discarding stale background AOF rewrite completion"
            );
            return;
        }

        let sync_result = sync_server_aof_file_info_if_current_generation(
            &server_state,
            &runtime,
            rewrite_generation,
        )
        .await;
        match sync_result {
            Ok(true) => {}
            Ok(false) => return,
            Err(error) => {
                tracing::warn!(
                    target = "ratatosk::aof",
                    error = %error,
                    "failed to refresh AOF file info after rewrite"
                );
                return;
            }
        }

        let mut state = server_state.meta.lock().await;
        if !runtime.aof_generation_is_current(rewrite_generation) {
            return;
        }
        state.set_aof_rewrite_in_progress(false);
        state.set_last_aof_rewrite_time_ms(now_ms());

        match result {
            Ok(true) => {
                state.set_last_aof_rewrite_status(Ok(()));
                crate::metrics::record_aof_rewrite("success");
                tracing::info!(target = "ratatosk::aof", "background AOF rewrite completed");
            }
            Ok(false) => (),
            Err(error) => {
                state.set_last_aof_rewrite_status(Err(error.to_string()));
                crate::metrics::record_aof_rewrite("error");
                tracing::warn!(
                    target = "ratatosk::aof",
                    error = %error,
                    "background AOF rewrite failed"
                );
            }
        }
    });

    if let Ok(mut tasks) = bgrewriteaof_tasks().lock() {
        tasks.push(handle);
    }

    true
}

pub async fn drain_bgrewriteaof_tasks(grace: Duration) -> (usize, usize) {
    let (completed, aborted) = drain_task_handles(
        bgrewriteaof_tasks(),
        grace,
        crate::metrics::record_bgrewriteaof_task_timeout,
    )
    .await;

    if aborted > 0 {
        crate::metrics::record_bgrewriteaof_tasks_aborted(aborted as u64);
    }

    (completed, aborted)
}

async fn drain_task_handles(
    tasks_lock: &'static std::sync::Mutex<Vec<tokio::task::JoinHandle<()>>>,
    grace: Duration,
    record_timeout: fn(),
) -> (usize, usize) {
    let tasks = match tasks_lock.lock() {
        Ok(mut guard) => std::mem::take(&mut *guard),
        Err(poisoned) => std::mem::take(&mut *poisoned.into_inner()),
    };

    if tasks.is_empty() {
        return (0, 0);
    }

    let deadline = tokio::time::Instant::now() + grace;
    let mut completed = 0usize;
    let mut aborted = 0usize;

    for mut task in tasks {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        if remaining.is_zero() {
            task.abort();
            aborted = aborted.saturating_add(1);
            record_timeout();
            continue;
        }

        match tokio::time::timeout(remaining, &mut task).await {
            Ok(Ok(())) => {
                completed = completed.saturating_add(1);
            }
            Ok(Err(error)) => {
                completed = completed.saturating_add(1);
                tracing::warn!(error = %error, "background task finished with join error");
            }
            Err(_) => {
                task.abort();
                aborted = aborted.saturating_add(1);
                record_timeout();
                tracing::warn!("background task did not complete before shutdown deadline");
            }
        }
    }

    (completed, aborted)
}
