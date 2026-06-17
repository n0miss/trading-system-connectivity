use anyhow::Result;
use clap::Parser;
use tracing::info;

#[derive(Parser)]
#[command(name = "connector", about = "Crypto CEX market data connector")]
struct Args {
    #[arg(short, long, default_value = "config/default.toml")]
    config: String,

    #[arg(long, default_value = "0")]
    shard_id: u32,

    #[arg(long, default_value = "1")]
    total_shards: u32,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    let args = Args::parse();

    info!(
        config = %args.config,
        shard_id = args.shard_id,
        total_shards = args.total_shards,
        "connector starting"
    );

    tokio::signal::ctrl_c().await?;

    info!("shutdown signal received");
    Ok(())
}
