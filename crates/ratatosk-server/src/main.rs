#[cfg(feature = "mimalloc")]
use mimalloc::MiMalloc;

#[cfg(feature = "mimalloc")]
#[global_allocator]
static GLOBAL_ALLOCATOR: MiMalloc = MiMalloc;

use clap::Parser;
use ratatosk_server::{config::ServerConfig, event_loop, metrics};

#[derive(Parser)]
#[command(version = concat!(env!("CARGO_PKG_VERSION"), " (", env!("GIT_HASH"), ")"))]
struct Args {}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let _args = Args::parse();

    setup_panic_hook();

    tracing_subscriber::fmt::init();

    // Initialize metrics exporter
    if let Err(e) = metrics::init_metrics_default() {
        tracing::warn!(
            target = "ratatosk::startup",
            error = %e,
            "Failed to initialize metrics exporter, continuing without metrics"
        );
    }

    let config = ServerConfig::from_env()?;

    log_startup_config(&config);

    event_loop::run(config).await?;

    Ok(())
}

fn setup_panic_hook() {
    std::panic::set_hook(Box::new(|info| {
        let timestamp = format_timestamp();
        let backtrace = std::backtrace::Backtrace::force_capture();
        let pid = std::process::id();
        let version = env!("GIT_HASH");

        // stderr에 즉시 출력
        eprintln!(
            "[{}] FATAL PANIC: {}\nPID: {}\nVersion: {}\n\n{}",
            timestamp, info, pid, version, backtrace
        );

        // crash 파일 저장
        if let Err(e) = write_crash_file(&timestamp, info, &backtrace, pid, version) {
            eprintln!("Failed to write crash file: {}", e);
        }

        // tracing 로그
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
    
    // ISO 8601 형식: 20240115T123456.789Z
    // time crate 없이는 간단하게 unix timestamp + millis로 표현
    format!("{}.{:03}", secs, millis)
}

fn write_crash_file(
    timestamp: &str,
    info: &std::panic::PanicInfo,
    backtrace: &std::backtrace::Backtrace,
    pid: u32,
    version: &str,
) -> std::io::Result<()> {
    let crash_dir = std::env::var("RATATOSK_CRASH_DIR")
        .unwrap_or_else(|_| "/tmp".to_string());
    
    let filename = format!("{}/ratatosk-crash-{}-{}.json", crash_dir, timestamp, pid);
    
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
    
    eprintln!("Crash info written to: {}", filename);
    
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
