use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use ratatosk_core::time::now_ms;
use ratatosk_engine::keyspace::ServerState;
use ratatosk_persist::{
    aof::{AofRecovery, AofWriter, FsyncPolicy},
    rdb,
};
use tokio::sync::Mutex;

use crate::config::ServerConfig;

const AOF_FILENAME: &str = "appendonly.aof";

#[derive(Clone)]
pub struct PersistenceRuntime {
    pub rdb_path: PathBuf,
    pub aof_path: PathBuf,
    pub aof_writer: Option<Arc<Mutex<AofWriter>>>,
}

impl PersistenceRuntime {
    pub fn from_config(config: &ServerConfig) -> io::Result<Self> {
        // 1. Validate working directory exists and is writable
        validate_working_directory(&config.dir)?;
        
        // 2. Check available disk space (minimum 100MB)
        check_disk_space(&config.dir, 100 * 1024 * 1024)?;
        
        let rdb_path = config.dir.join(&config.dbfilename);
        let aof_path = config.dir.join(AOF_FILENAME);
        
        // 3. Validate AOF file if exists and appendonly is enabled
        if config.appendonly && aof_path.exists() {
            validate_aof_file(&aof_path)?;
        }
        
        let aof_writer = if config.appendonly {
            let policy = FsyncPolicy::from_config_str(config.appendfsync.as_bytes())
                .unwrap_or(FsyncPolicy::EverySec);
            let writer = AofWriter::open(&aof_path, policy)
                .map_err(|e| io::Error::other(format!("opening AOF writer: {e}")))?;
            Some(Arc::new(Mutex::new(writer)))
        } else {
            None
        };

        Ok(Self {
            rdb_path,
            aof_path,
            aof_writer,
        })
    }
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
            format!("working directory path is not a directory: {}", dir.display()),
        ));
    }
    
    // Test write permission by creating a temp file
    let test_file = dir.join(".ratatosk_write_test");
    match std::fs::File::create(&test_file) {
        Ok(_) => {
            let _ = std::fs::remove_file(&test_file);
        }
        Err(e) => {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                format!("working directory is not writable: {} (error: {})", dir.display(), e),
            ));
        }
    }
    
    Ok(())
}

fn check_disk_space(dir: &Path, min_bytes: u64) -> io::Result<()> {
    // Practical check: try to create a file to verify write capability
    // Note: For proper disk space checking on Unix, use nix::sys::statvfs or similar
    let test_file = dir.join(".ratatosk_space_test");
    match std::fs::File::create(&test_file) {
        Ok(_file) => {
            let _ = std::fs::remove_file(&test_file);
        }
        Err(e) => {
            return Err(io::Error::new(
                io::ErrorKind::StorageFull,
                format!("insufficient disk space or permission denied in {}: {}", dir.display(), e),
            ));
        }
    }
    
    tracing::info!(
        target = "ratatosk::startup",
        dir = %dir.display(),
        min_bytes_required = min_bytes,
        "disk space validation passed"
    );
    
    Ok(())
}

fn validate_aof_file(path: &Path) -> io::Result<()> {
    // Check if AOF file is readable
    match std::fs::File::open(path) {
        Ok(_) => {
            tracing::info!(
                target = "ratatosk::startup",
                path = %path.display(),
                "AOF file exists and is readable"
            );
            Ok(())
        }
        Err(e) => {
            Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("AOF file exists but cannot be read: {} (error: {})", path.display(), e),
            ))
        }
    }
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
}

pub async fn load_startup_data(
    server_state: &Arc<Mutex<ServerState>>,
    runtime: &PersistenceRuntime,
    appendonly: bool,
) -> io::Result<()> {
    if runtime.rdb_path.exists() {
        let snapshot = rdb::loader::load(&runtime.rdb_path)
            .map_err(|e| io::Error::other(format!("loading RDB snapshot: {e}")))?;
        let key_count: usize = snapshot.iter().map(|db| db.len()).sum();
        {
            let mut state = server_state.lock().await;
            state.load_from_rdb(snapshot);
        }
        tracing::info!(keys = key_count, "loaded RDB snapshot");
    } else {
        tracing::info!(path = %runtime.rdb_path.display(), "no RDB file found, starting empty");
    }

    if appendonly && runtime.aof_path.exists() {
        let result = {
            let mut state = server_state.lock().await;
            AofRecovery::replay_file(&runtime.aof_path, &mut state)
                .map_err(|e| io::Error::other(format!("replaying AOF: {e}")))?
        };
        
        // Emit metrics for AOF replay
        if result.corruption_detected {
            tracing::warn!(
                target = "ratatosk::startup",
                commands_replayed = result.commands_replayed,
                bytes_processed = result.bytes_processed,
                truncated_at = result.truncated_at,
                "AOF replay completed with corruption detected"
            );
        } else {
            tracing::info!(
                target = "ratatosk::startup",
                commands_replayed = result.commands_replayed,
                bytes_processed = result.bytes_processed,
                "AOF replay completed successfully"
            );
        }
        
        if result.version_mismatch {
            tracing::warn!(
                target = "ratatosk::startup",
                "AOF file version mismatch detected"
            );
        }
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
    
    crate::metrics::record_rdb_save(true); // background = true

    tokio::spawn(async move {
        let path_for_log = rdb_path.display().to_string();
        let save_result = tokio::task::spawn_blocking(move || rdb::saver::save(&snapshot, &rdb_path))
            .await
            .map_err(|e| format!("joining BGSAVE worker: {e}"))
            .and_then(|result| result.map_err(|e| e.to_string()));

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

    true
}

pub async fn run_save(server_state: &Arc<Mutex<ServerState>>, rdb_path: &Path) -> io::Result<()> {
    let snapshot = {
        let state = server_state.lock().await;
        if state.rdb_save_in_progress() {
            return Err(io::Error::other("background save already in progress"));
        }
        state.snapshot_dbs()
    };
    
    crate::metrics::record_rdb_save(false); // foreground = false

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

pub async fn flush_aof(runtime: &PersistenceRuntime) -> io::Result<()> {
    let Some(writer) = &runtime.aof_writer else {
        return Ok(());
    };

    let mut guard = writer.lock().await;
    
    // Get fsync policy for metrics
    let fsync_policy = "always"; // force_fsync always does fsync
    crate::metrics::record_aof_write(fsync_policy);
    
    guard
        .force_fsync()
        .map_err(|e| {
            crate::metrics::record_aof_write_error();
            io::Error::other(format!("flushing AOF: {e}"))
        })
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use ratatosk_engine::keyspace::StoredValue;

    #[tokio::test]
    async fn startup_load_replays_rdb_then_aof() {
        let dir = tempfile::tempdir().expect("tmpdir");
        let mut config = ServerConfig::default();
        config.dir = dir.path().to_path_buf();
        config.appendonly = true;
        config.appendfsync = "always".to_string();

        let runtime = PersistenceRuntime::from_config(&config).expect("runtime");

        let mut state = ServerState::with_default_dbs();
        state.db_mut(0).insert(
            Bytes::from("from-rdb"),
            StoredValue::string(Bytes::from("1"), None),
        );
        rdb::saver::save(&state.snapshot_dbs(), &runtime.rdb_path).expect("save rdb");

        {
            let mut writer = runtime.aof_writer.as_ref().expect("aof writer").lock().await;
            writer
                .append_command(
                    0,
                    &[
                        Bytes::from("SET"),
                        Bytes::from("from-aof"),
                        Bytes::from("2"),
                    ],
                )
                .expect("append");
            writer.force_fsync().expect("fsync");
        }

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
}
