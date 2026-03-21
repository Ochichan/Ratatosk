use std::{
    env, io,
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};

use bytes::Bytes;
#[cfg(test)]
use ratatosk_engine::keyspace::ServerState;
use ratatosk_engine::keyspace::SharedState;
use ratatosk_persist::aof::{
    AofManifest, AofRecovery, AofWriter, FsyncPolicy, commit_manifest_switch,
    rewrite_single_file_in_place,
};
use tokio::sync::{mpsc, oneshot};

use super::PersistenceRuntime;
use super::util::env_truthy;

const DEFAULT_AOF_QUEUE_CAPACITY: usize = 4096;
const AOF_APPEND_QUEUE_TIMEOUT: Duration = Duration::from_secs(2);
const AOF_APPEND_REPLY_TIMEOUT: Duration = Duration::from_secs(5);
const AOF_FLUSH_REPLY_TIMEOUT: Duration = Duration::from_secs(5);
const AOF_REWRITE_REPLY_TIMEOUT: Duration = Duration::from_secs(900);
const ALLOW_INCOMPLETE_AOF_CHAIN_ENV: &str = "RATATOSK_ALLOW_INCOMPLETE_AOF_CHAIN";

pub(crate) enum AofWorkerCommand {
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

pub(crate) async fn request_aof_rewrite(runtime: &PersistenceRuntime) -> io::Result<()> {
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

pub(crate) async fn replay_startup_aof_file(
    server_state: &Arc<SharedState>,
    path: &Path,
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
        return Ok(runtime
            .aof_path
            .exists()
            .then(|| runtime.aof_path.clone())
            .into_iter()
            .collect());
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

    use crate::config::ServerConfig;

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
        use ratatosk_persist::aof::DEFAULT_SINGLE_FILE_AOF_FILENAME;

        let dir = tempfile::tempdir().expect("tmpdir");
        let runtime = PersistenceRuntime {
            rdb_path: dir.path().join("dump.rdb"),
            aof_path: dir.path().join(DEFAULT_SINGLE_FILE_AOF_FILENAME),
            aof_manifest_path: Some(dir.path().join(DEFAULT_AOF_MANIFEST_FILENAME)),
            aof_tx: None,
        };

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

        let dir = tempfile::tempdir().expect("tmpdir");
        let legacy_path = dir.path().join(DEFAULT_SINGLE_FILE_AOF_FILENAME);
        let manifest_path = dir.path().join(DEFAULT_AOF_MANIFEST_FILENAME);

        std::fs::write(&legacy_path, b"").expect("write legacy");

        // Ensure RATATOSK_MIGRATE_AOF is not set
        // SAFETY: test-only; no other threads depend on this env var in this test.
        unsafe { env::remove_var("RATATOSK_MIGRATE_AOF") };

        let (aof_path, manifest_opt) =
            bootstrap_aof_layout(dir.path(), &legacy_path, &manifest_path).expect("bootstrap");

        assert_eq!(aof_path, legacy_path);
        assert!(manifest_opt.is_none());
        assert!(!manifest_path.exists());
    }

    #[test]
    fn startup_aof_recovery_paths_fails_closed_when_manifest_file_is_missing() {
        let dir = tempfile::tempdir().expect("tmpdir");
        let manifest_path = dir.path().join(DEFAULT_AOF_MANIFEST_FILENAME);
        let mut manifest = AofManifest::new(dir.path());
        manifest.set_base_after_rewrite("appendonly.aof.base.aof".into());
        let _incr_path = manifest.new_incr_file();
        manifest
            .save_to_file(&manifest_path)
            .expect("save manifest");

        let runtime = PersistenceRuntime {
            rdb_path: dir.path().join("dump.rdb"),
            aof_path: dir.path().join("appendonly.aof.1.incr.aof"),
            aof_manifest_path: Some(manifest_path.clone()),
            aof_tx: None,
        };

        // SAFETY: test-only env isolation for this process.
        unsafe { env::remove_var(ALLOW_INCOMPLETE_AOF_CHAIN_ENV) };

        let error = startup_aof_recovery_paths(&runtime).expect_err("startup should fail closed");
        let text = error.to_string();
        assert!(matches!(error.kind(), io::ErrorKind::InvalidData));
        assert!(text.contains("missing recovery file(s)"), "{text}");
        assert!(text.contains(ALLOW_INCOMPLETE_AOF_CHAIN_ENV), "{text}");
    }

    #[test]
    fn startup_aof_recovery_paths_allows_missing_manifest_file_with_override() {
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

        let runtime = PersistenceRuntime {
            rdb_path: dir.path().join("dump.rdb"),
            aof_path: existing_incr.clone(),
            aof_manifest_path: Some(manifest_path),
            aof_tx: None,
        };

        // SAFETY: test-only env isolation for this process.
        unsafe { env::set_var(ALLOW_INCOMPLETE_AOF_CHAIN_ENV, "true") };

        let files = startup_aof_recovery_paths(&runtime).expect("override should allow startup");
        assert_eq!(files, vec![existing_incr]);

        // SAFETY: test-only env isolation for this process.
        unsafe { env::remove_var(ALLOW_INCOMPLETE_AOF_CHAIN_ENV) };
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

        let shared = Arc::new(SharedState::new(ServerState::with_default_dbs()));
        load_startup_data(&shared, &runtime, true)
            .await
            .expect("load startup after rewrite");
        let loaded = shared.meta.lock().await;

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
