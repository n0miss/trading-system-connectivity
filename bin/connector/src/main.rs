use anyhow::Result;
use axum::{Router, routing::get, http::StatusCode, response::IntoResponse};
use clap::Parser;
use connector_metrics::MetricsHandle;
use std::sync::Arc;
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

async fn metrics_handler(
    axum::extract::State(metrics): axum::extract::State<MetricsHandle>,
) -> impl IntoResponse {
    (
        StatusCode::OK,
        [("content-type", "text/plain; version=0.0.4")],
        metrics.render_prometheus(),
    )
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

    let cfg: connector_config::ConnectorConfig = config::Config::builder()
        .add_source(config::File::with_name(&args.config))
        .add_source(config::Environment::with_prefix("CONNECTOR").separator("__"))
        .build()?
        .try_deserialize()?;

    info!(
        config = %args.config,
        shard_id = args.shard_id,
        total_shards = args.total_shards,
        "connector starting"
    );

    let metrics: MetricsHandle = Arc::new(connector_metrics::ConnectorMetrics::new());

    let prometheus_port = cfg.metrics.prometheus_port;
    let metrics_for_server = metrics.clone();
    let app = Router::new()
        .route("/metrics", get(metrics_handler))
        .with_state(metrics_for_server);

    let listener = tokio::net::TcpListener::bind(("0.0.0.0", prometheus_port)).await?;
    info!(port = prometheus_port, "metrics server listening");

    tokio::select! {
        result = axum::serve(listener, app) => {
            result?;
        }
        _ = tokio::signal::ctrl_c() => {
            info!("shutdown signal received");
        }
    }

    Ok(())
}
