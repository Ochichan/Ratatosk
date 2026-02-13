#[cfg(feature = "mimalloc")]
use mimalloc::MiMalloc;

#[cfg(feature = "mimalloc")]
#[global_allocator]
static GLOBAL_ALLOCATOR: MiMalloc = MiMalloc;

use std::{
    ffi::OsStr,
    io,
    path::{Path, PathBuf},
    time::SystemTime,
};

use anyhow::Context;
use clap::Parser;
use ratatosk_server::{config::ServerConfig, event_loop, metrics};

const DEFAULT_CRASH_MAX_FILES: usize = 64;
const DEFAULT_CRASH_MAX_TOTAL_BYTES: u64 = 64 * 1024 * 1024;

#[derive(Parser)]
#[command(version = concat!(env!("CARGO_PKG_VERSION"), " (", env!("GIT_HASH"), ")"))]
struct Args {}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let _args = Args::parse();

    setup_panic_hook();

    tracing_subscriber::fmt::init();

    if let Err(error) = prune_crash_files_startup() {
        tracing::warn!(
            target = "ratatosk::startup",
            error = %error,
            "failed to rotate existing crash files"
        );
    }

    // Initialize metrics exporter
    if let Err(error) = metrics::init_metrics_default() {
        tracing::warn!(
            target = "ratatosk::startup",
            error = %error,
            "failed to initialize metrics exporter, continuing without metrics"
        );
    }

    let config =
        ServerConfig::from_env().context("loading server configuration from environment")?;

    log_startup_config(&config);

    event_loop::run(config)
        .await
        .context("running ratatosk event loop")?;

    Ok(())
}

fn setup_panic_hook() {
    std::panic::set_hook(Box::new(|info| {
        let timestamp = format_timestamp();
        let backtrace = std::backtrace::Backtrace::force_capture();
        let pid = std::process::id();
        let version = env!("GIT_HASH");

        eprintln!(
            "[{}] FATAL PANIC: {}\nPID: {}\nVersion: {}\n\n{}",
            timestamp, info, pid, version, backtrace
        );

        if let Err(error) = write_crash_file(&timestamp, info, &backtrace, pid, version) {
            eprintln!("Failed to write crash file: {error}");
        }

        tracing::error!(
            target = "ratatosk::panic",
            %info,
            pid,
            version,
            %backtrace,
            "process panicking - will abort"
        );
    }));
}

fn format_timestamp() -> String {
    let now_ms = ratatosk_core::time::now_ms();
    let secs = now_ms / 1000;
    let millis = (now_ms % 1000) as u32;

    format!("{}.{:03}", secs, millis)
}

fn write_crash_file(
    timestamp: &str,
    info: &std::panic::PanicHookInfo,
    backtrace: &std::backtrace::Backtrace,
    pid: u32,
    version: &str,
) -> io::Result<()> {
    let crash_dir = crash_dir_from_env();
    let (max_files, max_total_bytes) = crash_limits_from_env();

    std::fs::create_dir_all(&crash_dir)?;

    let filename = crash_dir.join(format!("ratatosk-crash-{timestamp}-{pid}.json"));

    let crash_info = serde_json::json!({
        "timestamp": timestamp,
        "timestamp_ms": ratatosk_core::time::now_ms(),
        "panic_info": info.to_string(),
        "backtrace": backtrace.to_string(),
        "pid": pid,
        "version": version,
        "cargo_version": env!("CARGO_PKG_VERSION"),
    });

    std::fs::write(&filename, crash_info.to_string())?;

    prune_crash_files(&crash_dir, max_files, max_total_bytes)?;

    eprintln!("Crash info written to: {}", filename.display());

    Ok(())
}

fn crash_dir_from_env() -> PathBuf {
    std::env::var("RATATOSK_CRASH_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|_| PathBuf::from("/tmp"))
}

fn crash_limits_from_env() -> (usize, u64) {
    let max_files = parse_env_usize("RATATOSK_CRASH_MAX_FILES", DEFAULT_CRASH_MAX_FILES);
    let max_total_bytes = parse_env_u64(
        "RATATOSK_CRASH_MAX_TOTAL_BYTES",
        DEFAULT_CRASH_MAX_TOTAL_BYTES,
    );
    (max_files, max_total_bytes)
}

fn parse_env_usize(name: &str, default: usize) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|raw| raw.parse::<usize>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(default)
}

fn parse_env_u64(name: &str, default: u64) -> u64 {
    std::env::var(name)
        .ok()
        .and_then(|raw| raw.parse::<u64>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(default)
}

fn prune_crash_files_startup() -> io::Result<()> {
    let crash_dir = crash_dir_from_env();
    if !crash_dir.exists() {
        return Ok(());
    }

    let (max_files, max_total_bytes) = crash_limits_from_env();
    prune_crash_files(&crash_dir, max_files, max_total_bytes)
}

fn prune_crash_files(dir: &Path, max_files: usize, max_total_bytes: u64) -> io::Result<()> {
    if max_files == 0 || max_total_bytes == 0 {
        return Ok(());
    }

    struct CrashFile {
        path: PathBuf,
        modified: SystemTime,
        size: u64,
    }

    let mut crash_files = Vec::new();

    for entry in std::fs::read_dir(dir)? {
        let entry = entry?;
        let path = entry.path();

        let filename = match path.file_name().and_then(OsStr::to_str) {
            Some(filename) => filename,
            None => continue,
        };

        if !filename.starts_with("ratatosk-crash-") || !filename.ends_with(".json") {
            continue;
        }

        let metadata = entry.metadata()?;
        if !metadata.is_file() {
            continue;
        }

        crash_files.push(CrashFile {
            path,
            modified: metadata.modified().unwrap_or(SystemTime::UNIX_EPOCH),
            size: metadata.len(),
        });
    }

    crash_files.sort_by(|left, right| right.modified.cmp(&left.modified));

    let mut kept_files = 0usize;
    let mut kept_bytes = 0u64;

    for crash_file in crash_files {
        let within_file_limit = kept_files < max_files;
        let next_total_bytes = kept_bytes.saturating_add(crash_file.size);
        let within_byte_limit = next_total_bytes <= max_total_bytes;

        if within_file_limit && within_byte_limit {
            kept_files += 1;
            kept_bytes = next_total_bytes;
            continue;
        }

        let _ = std::fs::remove_file(&crash_file.path);
    }

    Ok(())
}

fn log_startup_config(config: &ServerConfig) {
    tracing::info!(
        target = "ratatosk::startup",
        version = env!("CARGO_PKG_VERSION"),
        git_hash = env!("GIT_HASH"),
        bind = %config.bind,
        port = config.port,
        pid = std::process::id(),
        max_clients = config.max_clients,
        output_buffer_limit_bytes = config.output_buffer_limit_bytes,
        shutdown_grace_period_ms = config.shutdown_grace_period_ms,
        client_timeout_sec = config.client_timeout_sec,
        working_dir = %config.dir.display(),
        dbfilename = %config.dbfilename,
        appendonly = config.appendonly,
        appendfsync = %config.appendfsync,
        "Ratatosk server configuration loaded"
    );
}
