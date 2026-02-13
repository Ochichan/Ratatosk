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

use anyhow::{Context, anyhow};
use clap::Parser;
use ratatosk_server::{breadcrumbs, config::ServerConfig, event_loop, metrics};
use tracing_subscriber::EnvFilter;

const DEFAULT_CRASH_MAX_FILES: usize = 64;
const DEFAULT_CRASH_MAX_TOTAL_BYTES: u64 = 64 * 1024 * 1024;
const DEFAULT_FD_HEADROOM: usize = 128;

#[derive(Parser)]
#[command(version = concat!(
    env!("CARGO_PKG_VERSION"),
    " (",
    env!("GIT_HASH"),
    ")"
))]
struct Args {}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let _args = Args::parse();

    setup_panic_hook();
    init_tracing();

    if let Err(error) = prune_crash_files_startup() {
        tracing::warn!(
            target = "ratatosk::startup",
            error = %error,
            "failed to rotate existing crash files"
        );
    }

    init_metrics_exporter()?;

    let config =
        ServerConfig::from_env().context("loading server configuration from environment")?;
    run_startup_preflight(&config).context("running startup preflight checks")?;

    log_startup_config(&config);

    event_loop::run(config)
        .await
        .context("running ratatosk event loop")?;

    Ok(())
}

fn init_tracing() {
    let env_filter =
        EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info,ratatosk=info"));

    tracing_subscriber::fmt()
        .with_env_filter(env_filter)
        .json()
        .with_current_span(true)
        .with_span_list(true)
        .init();
}

fn init_metrics_exporter() -> anyhow::Result<()> {
    let bind_addr = metrics::metrics_bind_addr_from_env();
    let allow_without_metrics = env_truthy("RATATOSK_ALLOW_NO_METRICS");

    if let Err(error) = metrics::init_metrics(&bind_addr) {
        if allow_without_metrics {
            tracing::warn!(
                target = "ratatosk::startup",
                error = %error,
                bind_addr = %bind_addr,
                "failed to initialize metrics exporter; continuing due to RATATOSK_ALLOW_NO_METRICS"
            );
            return Ok(());
        }

        return Err(anyhow!(
            "failed to initialize metrics exporter on {}: {} (set RATATOSK_ALLOW_NO_METRICS=true to bypass)",
            bind_addr,
            error
        ));
    }

    Ok(())
}

fn env_truthy(name: &str) -> bool {
    std::env::var(name).is_ok_and(|value| {
        value == "1"
            || value.eq_ignore_ascii_case("true")
            || value.eq_ignore_ascii_case("yes")
            || value.eq_ignore_ascii_case("on")
    })
}

fn setup_panic_hook() {
    std::panic::set_hook(Box::new(|info| {
        let timestamp = format_timestamp();
        let backtrace = std::backtrace::Backtrace::force_capture();
        let pid = std::process::id();
        let version = env!("GIT_HASH");
        let build_unix_ts = env!("BUILD_UNIX_TS");

        eprintln!(
            "[{}] FATAL PANIC: {}\nPID: {}\nVersion: {}\nBuildUnixTs: {}\n\n{}",
            timestamp, info, pid, version, build_unix_ts, backtrace
        );

        if let Err(error) = write_crash_file(&timestamp, info, &backtrace, pid, version) {
            eprintln!("Failed to write crash file: {error}");
        }

        tracing::error!(
            target = "ratatosk::panic",
            %info,
            pid,
            version,
            build_unix_ts,
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
    let last_commands = breadcrumbs::snapshot(64);

    let crash_info = serde_json::json!({
        "timestamp": timestamp,
        "timestamp_ms": ratatosk_core::time::now_ms(),
        "panic_info": info.to_string(),
        "backtrace": backtrace.to_string(),
        "pid": pid,
        "version": version,
        "cargo_version": env!("CARGO_PKG_VERSION"),
        "build_unix_ts": env!("BUILD_UNIX_TS"),
        "breadcrumbs": last_commands,
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
        build_unix_ts = env!("BUILD_UNIX_TS"),
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

fn run_startup_preflight(config: &ServerConfig) -> anyhow::Result<()> {
    validate_fd_headroom(config)?;
    validate_persistence_dir_access(config)?;
    validate_audit_log_access()?;
    Ok(())
}

fn validate_persistence_dir_access(config: &ServerConfig) -> anyhow::Result<()> {
    if !config.dir.exists() {
        return Err(anyhow!(
            "persistence directory does not exist: {}",
            config.dir.display()
        ));
    }

    if !config.dir.is_dir() {
        return Err(anyhow!(
            "persistence directory path is not a directory: {}",
            config.dir.display()
        ));
    }

    let probe = config.dir.join(".ratatosk_startup_probe");
    std::fs::write(&probe, b"ok").with_context(|| {
        format!(
            "writing startup probe file in persistence directory: {}",
            probe.display()
        )
    })?;
    let _ = std::fs::remove_file(&probe);

    tracing::info!(
        target = "ratatosk::startup",
        dir = %config.dir.display(),
        "persistence directory preflight passed"
    );

    Ok(())
}

fn validate_audit_log_access() -> anyhow::Result<()> {
    let (audit_log_path, audit_state_path) = ratatosk_engine::security::audit_paths_from_env();

    validate_appendable_file_path("audit log", &audit_log_path)?;
    validate_appendable_file_path("audit chain state", &audit_state_path)?;

    tracing::info!(
        target = "ratatosk::startup",
        audit_log_path = %audit_log_path.display(),
        audit_state_path = %audit_state_path.display(),
        "audit preflight passed"
    );

    Ok(())
}

fn validate_appendable_file_path(label: &str, path: &Path) -> anyhow::Result<()> {
    let parent = path.parent().unwrap_or(Path::new("."));
    std::fs::create_dir_all(parent).with_context(|| {
        format!(
            "creating parent directory for {}: {}",
            label,
            parent.display()
        )
    })?;

    std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .with_context(|| format!("opening {} path for append: {}", label, path.display()))?;

    Ok(())
}

fn validate_fd_headroom(config: &ServerConfig) -> anyhow::Result<()> {
    #[cfg(target_os = "linux")]
    {
        let Some(soft_limit) = linux_soft_nofile_limit() else {
            tracing::warn!(
                target = "ratatosk::startup",
                "could not parse /proc/self/limits for open files; skipping fd headroom validation"
            );
            return Ok(());
        };

        let required = config.max_clients.saturating_add(DEFAULT_FD_HEADROOM);
        if soft_limit < required as u64 {
            return Err(anyhow!(
                "insufficient open-file limit: soft_limit={} required_at_least={} (max_clients={} + headroom={})",
                soft_limit,
                required,
                config.max_clients,
                DEFAULT_FD_HEADROOM
            ));
        }

        tracing::info!(
            target = "ratatosk::startup",
            fd_soft_limit = soft_limit,
            fd_required = required,
            "file descriptor headroom preflight passed"
        );

        Ok(())
    }

    #[cfg(not(target_os = "linux"))]
    {
        tracing::warn!(
            target = "ratatosk::startup",
            "fd headroom preflight is only implemented on Linux; skipping"
        );
        let _ = config;
        Ok(())
    }
}

#[cfg(target_os = "linux")]
fn linux_soft_nofile_limit() -> Option<u64> {
    let limits = std::fs::read_to_string("/proc/self/limits").ok()?;
    for line in limits.lines() {
        if !line.starts_with("Max open files") {
            continue;
        }

        let rest = line.trim_start_matches("Max open files").trim();
        let mut parts = rest.split_whitespace();
        let soft = parts.next()?;
        if soft.eq_ignore_ascii_case("unlimited") {
            return Some(u64::MAX);
        }
        return soft.parse::<u64>().ok();
    }

    None
}
