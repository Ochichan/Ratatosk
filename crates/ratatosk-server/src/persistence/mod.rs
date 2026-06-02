mod aof;
mod rdb;
mod util;

use std::{
    io,
    path::PathBuf,
    sync::{Arc, OnceLock},
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
    AofWorkerCommand, aof_queue_capacity_from_env, bootstrap_aof_layout, replay_startup_aof_file,
    request_aof_rewrite, spawn_aof_worker, startup_aof_recovery_paths,
};
use self::util::{check_disk_space, validate_aof_file, validate_working_directory};

// Public re-exports to maintain the existing API surface.
pub use self::aof::{append_aof_command, flush_aof};
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
    aof_tx: Option<mpsc::Sender<AofWorkerCommand>>,
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

        let aof_tx = if config.appendonly {
            let policy = FsyncPolicy::from_config_str(config.appendfsync.as_bytes())
                .unwrap_or(FsyncPolicy::EverySec);
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

        Ok(Self {
            rdb_path,
            aof_path,
            aof_manifest_path,
            aof_tx,
        })
    }

    pub(crate) fn aof_sender(&self) -> Option<&mpsc::Sender<AofWorkerCommand>> {
        self.aof_tx.as_ref()
    }
}

fn runtime_aof_file_paths(
    runtime: &PersistenceRuntime,
) -> io::Result<(Option<PathBuf>, Option<PathBuf>)> {
    let current_path = runtime
        .aof_sender()
        .is_some()
        .then(|| runtime.aof_path.clone());

    let base_path = match runtime.aof_manifest_path.as_ref() {
        Some(manifest_path) if manifest_path.exists() => {
            AofManifest::load_from_file(manifest_path)?.base_path()
        }
        _ => None,
    };

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

pub async fn load_startup_data(
    server_state: &Arc<SharedState>,
    runtime: &PersistenceRuntime,
    appendonly: bool,
) -> io::Result<()> {
    if runtime.rdb_path.exists() {
        let snapshot = ratatosk_persist::rdb::loader::load(&runtime.rdb_path)
            .map_err(|error| io::Error::other(format!("loading RDB snapshot: {error}")))?;
        let key_count: usize = snapshot.iter().map(|db| db.len()).sum();
        {
            let state = server_state.meta.lock().await;
            state.load_from_rdb(snapshot);
        }
        tracing::info!(keys = key_count, "loaded RDB snapshot");
    } else {
        tracing::info!(path = %runtime.rdb_path.display(), "no RDB file found, starting empty");
    }

    if appendonly {
        for path in startup_aof_recovery_paths(runtime)? {
            replay_startup_aof_file(server_state, &path).await?;
        }
    }

    Ok(())
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
    if runtime.aof_sender().is_none() {
        let mut state = server_state.meta.lock().await;
        state.set_aof_rewrite_in_progress(false);
        state.set_last_aof_rewrite_status(Err("appendonly is disabled".to_string()));
        state.set_last_aof_rewrite_time_ms(now_ms());
        return false;
    }

    crate::metrics::record_aof_rewrite("requested");

    let handle = tokio::spawn(async move {
        let result = request_aof_rewrite(&runtime).await;
        let sync_result = sync_server_aof_file_info(&server_state, &runtime).await;

        let mut state = server_state.meta.lock().await;
        state.set_aof_rewrite_in_progress(false);
        state.set_last_aof_rewrite_time_ms(now_ms());

        match result {
            Ok(()) => {
                state.set_last_aof_rewrite_status(Ok(()));
                crate::metrics::record_aof_rewrite("success");
                tracing::info!(target = "ratatosk::aof", "background AOF rewrite completed");
            }
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

        if let Err(error) = sync_result {
            tracing::warn!(
                target = "ratatosk::aof",
                error = %error,
                "failed to refresh AOF file info after rewrite"
            );
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
