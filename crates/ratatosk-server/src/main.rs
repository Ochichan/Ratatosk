#[cfg(feature = "mimalloc")]
use mimalloc::MiMalloc;

#[cfg(feature = "mimalloc")]
#[global_allocator]
static GLOBAL_ALLOCATOR: MiMalloc = MiMalloc;

use ratatosk_server::{config::ServerConfig, event_loop};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();

    let config = ServerConfig::from_env()?;
    event_loop::run(config).await?;

    Ok(())
}
