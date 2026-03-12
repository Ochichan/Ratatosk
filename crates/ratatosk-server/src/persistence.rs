use std::{
    env, io,
    path::{Path, PathBuf},
    sync::{Arc, OnceLock},
    time::Duration,
};

use bytes::Bytes;
use fs2::available_space;
use ratatosk_core::time::now_ms;
use ratatosk_engine::keyspace::ServerState;
use ratatosk_persist::{
    aof::{
        AofManifest, AofRecovery, AofWriter, DEFAULT_AOF_MANIFEST_FILENAME,
        DEFAULT_SINGLE_FILE_AOF_FILENAME, FsyncPolicy, commit_manifest_switch,
        rewrite_single_file_in_place,
    },
    rdb,
};
use tokio::sync::{Mutex, mpsc, oneshot};

use crate::config::ServerConfig;

const DEFAULT_AOF_QUEUE_CAPACITY: usize = 4096;
const AOF_APPEND_QUEUE_TIMEOUT: Duration = Duration::from_secs(2);
const AOF_APPEND_REPLY_TIMEOUT: Duration = Duration::from_secs(5);
const AOF_FLUSH_REPLY_TIMEOUT: Duration = Duration::from_secs(5);
const AOF_REWRITE_REPLY_TIMEOUT: Duration = Duration::from_secs(900);

static BGSAVE_TASKS: OnceLock<std::sync::Mutex<Vec<tokio::task::JoinHandle<()>>>> = OnceLock::new();
static BGREWRITEAOF_TASKS: OnceLock<std::sync::Mutex<Vec<tokio::task::JoinHandle<()>>>> =
    OnceLock::new();

fn bgsave_tasks() -> &'static std::sync::Mutex<Vec<tokio::task::JoinHandle<()>>> {
    BGSAVE_TASKS.get_or_init(|| std::sync::Mutex::new(Vec::new()))
}

fn bgrewriteaof_tasks() -> &'static std::sync::Mutex<Vec<tokio::task::JoinHandle<()>>> {
    BGREWRITEAOF_TASKS.get_or_init(|| std::sync::Mutex::new(Vec::new()))
}

enum AofWorkerCommand {
    Append {
        db_index: usize,
        argv: Vec<Bytes>,
        reply: oneshot::Sender<Result<(), String>>,
    },
    Flush {
        reply: oneshot::Sender<Result<(), String>>,
    },
    Rewrite {
        reply: oneshot::Sender<Result<(), String>>,
    },
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

    fn aof_sender(&self) -> Option<&mpsc::Sender<AofWorkerCommand>> {
        self.aof_tx.as_ref()
    }
}

fn bootstrap_aof_layout(
    dir: &Path,
    legacy_aof_path: &Path,
    manifest_path: &Path,
) -> io::Result<(PathBuf, Option<PathBuf>)> {
    if manifest_path.exists() {
        let mut manifest = AofManifest::load_from_file(manifest_path)?;
        if let Some(path) = manifest.current_incr_path() {
            return Ok((path, Some(manifest_path.to_path_buf())));
        }

        let active_path = manifest.new_incr_file();
        manifest.save_to_file(manifest_path)?;
        return Ok((active_path, Some(manifest_path.to_path_buf())));
    }

    if legacy_aof_path.exists() {
        return Ok((legacy_aof_path.to_path_buf(), None));
    }

    let mut manifest = AofManifest::new(dir);
    let active_path = manifest.new_incr_file();
    manifest.save_to_file(manifest_path)?;
    Ok((active_path, Some(manifest_path.to_path_buf())))
}

fn aof_queue_capacity_from_env() -> usize {
    env::var("RATATOSK_AOF_QUEUE_CAPACITY")
        .ok()
        .and_then(|raw| raw.parse::<usize>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(DEFAULT_AOF_QUEUE_CAPACITY)
}

fn observe_aof_queue_depth(sender: &mpsc::Sender<AofWorkerCommand>) {
    let depth = sender.max_capacity().saturating_sub(sender.capacity());
    crate::metrics::set_aof_queue_depth(depth);
}

fn spawn_aof_worker(
    aof_path: PathBuf,
    manifest_path: Option<PathBuf>,
    writer: AofWriter,
    policy: FsyncPolicy,
    queue_capacity: usize,
) -> mpsc::Sender<AofWorkerCommand> {
    let (tx, mut rx) = mpsc::channel::<AofWorkerCommand>(queue_capacity);
    crate::metrics::set_aof_queue_depth(0);

    tokio::spawn(async move {
        let mut writer = writer;
        let mut active_aof_path = aof_path;
        while let Some(command) = rx.recv().await {
            crate::metrics::set_aof_queue_depth(rx.len());
            match command {
                AofWorkerCommand::Append {
                    db_index,
                    argv,
                    reply,
                } => {
                    let result = writer
                        .append_command(db_index, &argv)
                        .map_err(|error| format!("appending AOF command: {error}"));
                    let _ = reply.send(result);
                }
                AofWorkerCommand::Flush { reply } => {
                    let result = writer
                        .force_fsync()
                        .map_err(|error| format!("flushing AOF: {error}"));
                    let _ = reply.send(result);
                }
                AofWorkerCommand::Rewrite { reply } => {
                    let result = rewrite_aof_and_reopen(
                        &mut active_aof_path,
                        manifest_path.as_deref(),
                        policy,
                        &mut writer,
                    );
                    let _ = reply.send(result);
                }
            }
        }

        crate::metrics::set_aof_queue_depth(0);
        tracing::warn!(
            target = "ratatosk::aof",
            path = %active_aof_path.display(),
            "AOF worker channel closed; worker exiting"
        );
    });

    tx
}

fn rewrite_aof_and_reopen(
    active_aof_path: &mut PathBuf,
    manifest_path: Option<&Path>,
    policy: FsyncPolicy,
    writer: &mut AofWriter,
) -> Result<(), String> {
    writer
        .force_fsync()
        .map_err(|error| format!("forcing fsync before rewrite: {error}"))?;

    rewrite_single_file_in_place(active_aof_path)
        .map_err(|error| format!("rewriting AOF file: {error}"))?;

    if let Some(manifest_path) = manifest_path {
        let mut manifest = AofManifest::load_from_file(manifest_path)
            .map_err(|error| format!("loading AOF manifest before switch: {error}"))?;
        let current_incr = manifest.current_incr_path().ok_or_else(|| {
            "manifest-backed AOF rewrite requires an active incremental file".to_string()
        })?;
        if current_incr != *active_aof_path {
            return Err(format!(
                "manifest current incremental path '{}' did not match active writer path '{}'",
                current_incr.display(),
                active_aof_path.display()
            ));
        }

        let next_incr_path = manifest.new_incr_file();
        let mut reopened = AofWriter::open(&next_incr_path, policy)
            .map_err(|error| format!("opening next incremental AOF writer: {error}"))?;
        reopened.force_fsync().map_err(|error| {
            format!("fsyncing next incremental AOF before manifest switch: {error}")
        })?;
        commit_manifest_switch(manifest_path, &manifest, &[])
            .map_err(|error| format!("committing manifest switch after rewrite: {error}"))?;
        *writer = reopened;
        *active_aof_path = next_incr_path;
    } else {
        let reopened = AofWriter::open(active_aof_path, policy)
            .map_err(|error| format!("reopening rewritten AOF writer: {error}"))?;
        *writer = reopened;
    }
    Ok(())
}

fn validate_working_directory(dir: &Path) -> io::Result<()> {
    if !dir.exists() {
        return Err(io::Error::new(
            io::ErrorKind::NotFound,
            format!("working directory does not exist: {}", dir.display()),
        ));
    }

    if !dir.is_dir() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!(
                "working directory path is not a directory: {}",
                dir.display()
            ),
        ));
    }

    let test_file = dir.join(".ratatosk_write_test");
    match std::fs::File::create(&test_file) {
        Ok(_) => {
            let _ = std::fs::remove_file(&test_file);
        }
        Err(error) => {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                format!(
                    "working directory is not writable: {} (error: {})",
                    dir.display(),
                    error
                ),
            ));
        }
    }

    Ok(())
}

fn check_disk_space(dir: &Path, min_bytes: u64) -> io::Result<()> {
    let available = available_space(dir).map_err(|error| {
        io::Error::new(
            error.kind(),
            format!(
                "failed to query available disk space in {}: {error}",
                dir.display()
            ),
        )
    })?;

    if available < min_bytes {
        return Err(io::Error::new(
            io::ErrorKind::StorageFull,
            format!(
                "insufficient disk space in {}: available={} bytes, required={} bytes",
                dir.display(),
                available,
                min_bytes
            ),
        ));
    }

    tracing::info!(
        target = "ratatosk::startup",
        dir = %dir.display(),
        available_bytes = available,
        min_bytes_required = min_bytes,
        "disk space validation passed"
    );

    Ok(())
}

fn validate_aof_file(path: &Path) -> io::Result<()> {
    match std::fs::File::open(path) {
        Ok(_) => {
            tracing::info!(
                target = "ratatosk::startup",
                path = %path.display(),
                "AOF file exists and is readable"
            );
            Ok(())
        }
        Err(error) => Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "AOF file exists but cannot be read: {} (error: {})",
                path.display(),
                error
            ),
        )),
    }
}

fn env_truthy(name: &str) -> bool {
    env::var(name).is_ok_and(|value| {
        value == "1"
            || value.eq_ignore_ascii_case("true")
            || value.eq_ignore_ascii_case("yes")
            || value.eq_ignore_ascii_case("on")
    })
}

pub async fn apply_server_persistence_config(
    server_state: &Arc<Mutex<ServerState>>,
    config: &ServerConfig,
) {
    let mut state = server_state.lock().await;
    state.config.set_dir(config.dir.clone());
    state.config.set_dbfilename(config.dbfilename.clone());
    state
        .config
        .set_appendfsync(bytes::Bytes::from(config.appendfsync.clone()));
    state.config.set_appendonly(config.appendonly);
    state.set_aof_enabled(config.appendonly);

    if !config.appendonly {
        state.clear_aof_last_error();
        state.set_aof_rewrite_in_progress(false);
        state.clear_last_aof_rewrite_status();
        state.clear_last_aof_rewrite_time_ms();
    }

    crate::metrics::set_aof_write_latched(state.aof_write_latched());
}

pub async fn load_startup_data(
    server_state: &Arc<Mutex<ServerState>>,
    runtime: &PersistenceRuntime,
    appendonly: bool,
) -> io::Result<()> {
    if runtime.rdb_path.exists() {
        let snapshot = rdb::loader::load(&runtime.rdb_path)
            .map_err(|error| io::Error::other(format!("loading RDB snapshot: {error}")))?;
        let key_count: usize = snapshot.iter().map(|db| db.len()).sum();
        {
            let mut state = server_state.lock().await;
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

fn startup_aof_recovery_paths(runtime: &PersistenceRuntime) -> io::Result<Vec<PathBuf>> {
    let Some(manifest_path) = runtime.aof_manifest_path.as_ref() else {
        return Ok(runtime
            .aof_path
            .exists()
            .then(|| runtime.aof_path.clone())
            .into_iter()
            .collect());
    };

    if !manifest_path.exists() {
        return Ok(runtime
            .aof_path
            .exists()
            .then(|| runtime.aof_path.clone())
            .into_iter()
            .collect());
    }

    let manifest = AofManifest::load_from_file(manifest_path)?;
    let mut recovery_files = Vec::new();
    for path in manifest.recovery_files() {
        if path.exists() {
            recovery_files.push(path);
        } else {
            tracing::warn!(
                target = "ratatosk::startup",
                path = %path.display(),
                manifest = %manifest_path.display(),
                "AOF manifest references a missing recovery file; skipping"
            );
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

async fn replay_startup_aof_file(
    server_state: &Arc<Mutex<ServerState>>,
    path: &Path,
) -> io::Result<()> {
    let result = {
        let mut state = server_state.lock().await;
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

    Ok(())
}

pub async fn start_bgsave(server_state: Arc<Mutex<ServerState>>, rdb_path: PathBuf) -> bool {
    let snapshot = {
        let mut state = server_state.lock().await;
        if state.rdb_save_in_progress() {
            return false;
        }
        state.set_rdb_save_in_progress(true);
        state.snapshot_dbs()
    };

    crate::metrics::record_rdb_save(true);

    let handle = tokio::spawn(async move {
        let path_for_log = rdb_path.display().to_string();
        let save_result =
            tokio::task::spawn_blocking(move || rdb::saver::save(&snapshot, &rdb_path))
                .await
                .map_err(|error| format!("joining BGSAVE worker: {error}"))
                .and_then(|result| result.map_err(|error| error.to_string()));

        let mut state = server_state.lock().await;
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
    server_state: Arc<Mutex<ServerState>>,
    runtime: Arc<PersistenceRuntime>,
) -> bool {
    if runtime.aof_sender().is_none() {
        let mut state = server_state.lock().await;
        state.set_aof_rewrite_in_progress(false);
        state.set_last_aof_rewrite_status(Err("appendonly is disabled".to_string()));
        state.set_last_aof_rewrite_time_ms(now_ms());
        return false;
    }

    crate::metrics::record_aof_rewrite("requested");

    let handle = tokio::spawn(async move {
        let result = request_aof_rewrite(&runtime).await;

        let mut state = server_state.lock().await;
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

pub async fn run_save(server_state: &Arc<Mutex<ServerState>>, rdb_path: &Path) -> io::Result<()> {
    let snapshot = {
        let state = server_state.lock().await;
        if state.rdb_save_in_progress() {
            return Err(io::Error::other("background save already in progress"));
        }
        state.snapshot_dbs()
    };

    crate::metrics::record_rdb_save(false);

    let result = rdb::saver::save(&snapshot, rdb_path);
    let mut state = server_state.lock().await;
    match &result {
        Ok(()) => {
            state.stats.mark_last_save_now();
            state.set_last_rdb_save_time_ms(now_ms());
            state.set_last_rdb_save_status(Ok(()));
        }
        Err(error) => {
            state.set_last_rdb_save_status(Err(error.to_string()));
            crate::metrics::record_rdb_save_error();
        }
    }
    result
}

pub async fn append_aof_command(
    runtime: &PersistenceRuntime,
    db_index: usize,
    argv: Vec<Bytes>,
) -> io::Result<()> {
    let Some(sender) = runtime.aof_sender() else {
        return Ok(());
    };

    observe_aof_queue_depth(sender);
    let (reply_tx, reply_rx) = oneshot::channel();

    match tokio::time::timeout(
        AOF_APPEND_QUEUE_TIMEOUT,
        sender.send(AofWorkerCommand::Append {
            db_index,
            argv,
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
        Ok(Err(_)) => Err(io::Error::new(
            io::ErrorKind::BrokenPipe,
            "AOF worker dropped append completion channel",
        )),
        Ok(Ok(Ok(()))) => Ok(()),
        Ok(Ok(Err(error))) => Err(io::Error::other(error)),
    }
}

async fn request_aof_rewrite(runtime: &PersistenceRuntime) -> io::Result<()> {
    let Some(sender) = runtime.aof_sender() else {
        return Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "AOF rewrite requested while appendonly is disabled",
        ));
    };

    observe_aof_queue_depth(sender);
    let (reply_tx, reply_rx) = oneshot::channel();

    match tokio::time::timeout(
        AOF_APPEND_QUEUE_TIMEOUT,
        sender.send(AofWorkerCommand::Rewrite { reply: reply_tx }),
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
            return Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "AOF worker channel closed while enqueueing rewrite",
            ));
        }
        Ok(Ok(())) => {}
    }

    match tokio::time::timeout(AOF_REWRITE_REPLY_TIMEOUT, reply_rx).await {
        Err(_) => Err(io::Error::new(
            io::ErrorKind::TimedOut,
            "timed out waiting for AOF rewrite completion",
        )),
        Ok(Err(_)) => Err(io::Error::new(
            io::ErrorKind::BrokenPipe,
            "AOF worker dropped rewrite completion channel",
        )),
        Ok(Ok(Ok(()))) => Ok(()),
        Ok(Ok(Err(error))) => Err(io::Error::other(error)),
    }
}

pub async fn flush_aof(runtime: &PersistenceRuntime) -> io::Result<()> {
    let Some(sender) = runtime.aof_sender() else {
        return Ok(());
    };

    observe_aof_queue_depth(sender);
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
        Ok(Err(_)) => Err(io::Error::new(
            io::ErrorKind::BrokenPipe,
            "AOF worker dropped flush completion channel",
        )),
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

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use ratatosk_engine::keyspace::StoredValue;

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

    #[tokio::test]
    async fn startup_load_replays_rdb_then_aof() {
        let dir = tempfile::tempdir().expect("tmpdir");
        let config = ServerConfig {
            dir: dir.path().to_path_buf(),
            appendonly: true,
            appendfsync: "always".to_string(),
            ..ServerConfig::default()
        };

        let runtime = PersistenceRuntime::from_config(&config).expect("runtime");

        let mut state = ServerState::with_default_dbs();
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

        let shared = Arc::new(Mutex::new(ServerState::with_default_dbs()));
        load_startup_data(&shared, &runtime, true)
            .await
            .expect("load startup");

        let loaded = shared.lock().await;
        assert_eq!(
            loaded
                .db(0)
                .get(&Bytes::from("from-rdb"))
                .and_then(|v| v.as_string()),
            Some(&Bytes::from("1"))
        );
        assert_eq!(
            loaded
                .db(0)
                .get(&Bytes::from("from-aof"))
                .and_then(|v| v.as_string()),
            Some(&Bytes::from("2"))
        );
    }

    #[tokio::test]
    async fn startup_load_replays_manifest_recovery_chain() {
        let dir = tempfile::tempdir().expect("tmpdir");
        let runtime = PersistenceRuntime {
            rdb_path: dir.path().join("dump.rdb"),
            aof_path: dir.path().join(DEFAULT_SINGLE_FILE_AOF_FILENAME),
            aof_manifest_path: Some(dir.path().join(DEFAULT_AOF_MANIFEST_FILENAME)),
            aof_tx: None,
        };

        let mut rdb_state = ServerState::with_default_dbs();
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

        let shared = Arc::new(Mutex::new(ServerState::with_default_dbs()));
        load_startup_data(&shared, &runtime, true)
            .await
            .expect("load startup");

        let loaded = shared.lock().await;
        for (key, value) in [
            ("from-rdb", "1"),
            ("from-base", "2"),
            ("from-incr-1", "3"),
            ("from-incr-2", "4"),
        ] {
            assert_eq!(
                loaded
                    .db(0)
                    .get(&Bytes::from(key))
                    .and_then(|entry| entry.as_string()),
                Some(&Bytes::from(value))
            );
        }
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

        request_aof_rewrite(&runtime)
            .await
            .expect("rewrite request should succeed");

        let manifest_path = runtime
            .aof_manifest_path
            .as_ref()
            .expect("manifest path should exist");
        let manifest_after_rewrite =
            AofManifest::load_from_file(manifest_path).expect("load manifest after rewrite");
        assert_eq!(manifest_after_rewrite.incr_files().len(), 2);
        assert_ne!(
            manifest_after_rewrite
                .current_incr_path()
                .expect("current incr path"),
            runtime.aof_path
        );

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

        let shared = Arc::new(Mutex::new(ServerState::with_default_dbs()));
        load_startup_data(&shared, &runtime, true)
            .await
            .expect("load startup after rewrite");
        let loaded = shared.lock().await;

        assert_eq!(
            loaded
                .db(0)
                .get(&Bytes::from("pre"))
                .and_then(|v| v.as_string()),
            Some(&Bytes::from("rewrite"))
        );
        assert_eq!(
            loaded
                .db(0)
                .get(&Bytes::from("post"))
                .and_then(|v| v.as_string()),
            Some(&Bytes::from("rewrite"))
        );
    }
}
