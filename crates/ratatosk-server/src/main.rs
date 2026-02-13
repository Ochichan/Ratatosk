#[cfg(feature = "mimalloc")]
use mimalloc::MiMalloc;

#[cfg(feature = "mimalloc")]
#[global_allocator]
static GLOBAL_ALLOCATOR: MiMalloc = MiMalloc;

use clap::Parser;
use ratatosk_server::{config::ServerConfig, event_loop};

#[derive(Parser)]
#[command(version = concat!(env!("CARGO_PKG_VERSION"), " (", env!("GIT_HASH"), ")"))]
struct Args {}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let _args = Args::parse();

    std::panic::set_hook(Box::new(|info| {
        eprintln!("FATAL PANIC: {info}");
        tracing::error!(%info, "process panicking — will abort");
    }));

    tracing_subscriber::fmt::init();

    let config = ServerConfig::from_env()?;

    tracing::info!(
        bind = %config.bind,
        port = config.port,
        pid = std::process::id(),
        version = env!("CARGO_PKG_VERSION"),
        max_clients = config.max_clients,
        "Ratatosk server starting"
    );

    event_loop::run(config).await?;

    Ok(())
}
