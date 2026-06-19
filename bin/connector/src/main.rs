use anyhow::Result;
use axum::{Router, http::StatusCode, response::IntoResponse, routing::get};
use clap::Parser;
use connector_core::{MarketType, VenueId};
use connector_metrics::MetricsHandle;
use connector_refdata::RefDataService;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{mpsc, watch};
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

fn parse_venue(venue: &str, market: &str) -> Result<(VenueId, MarketType)> {
    let venue_id = match venue {
        "binance_spot"    => VenueId::BinanceSpot,
        "binance_futures" => VenueId::BinanceFutures,
        other => anyhow::bail!("unknown venue: {other}"),
    };
    let market_type = match market {
        "spot"          => MarketType::Spot,
        "usdm_futures"  => MarketType::UsdmFutures,
        other => anyhow::bail!("unknown market: {other}"),
    };
    Ok((venue_id, market_type))
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
        config  = %args.config,
        shard_id = args.shard_id,
        total_shards = args.total_shards,
        venue   = %cfg.instance.venue,
        market  = %cfg.instance.market,
        "connector starting"
    );

    let (venue_id, market_type) = parse_venue(&cfg.instance.venue, &cfg.instance.market)?;
    let metrics: MetricsHandle = Arc::new(connector_metrics::ConnectorMetrics::new());
    let (shutdown_tx, shutdown_rx) = watch::channel(false);

    // ── Metrics HTTP server ───────────────────────────────────────────────────
    let prometheus_port = cfg.metrics.prometheus_port;
    let app = Router::new()
        .route("/metrics", get(metrics_handler))
        .with_state(metrics.clone());
    let listener = tokio::net::TcpListener::bind(("0.0.0.0", prometheus_port)).await?;
    info!(port = prometheus_port, "metrics server listening");

    // ── Symbol discovery ──────────────────────────────────────────────────────
    let mut refdata = RefDataService::new(
        &cfg.rest.base_url,
        venue_id,
        market_type,
        cfg.instance.id,
        Duration::from_secs(300),
    );
    let events = refdata.refresh().await?;

    let universe: Vec<String> = if cfg.symbols.universe.is_empty() {
        events
            .iter()
            .filter(|e| e.definition().is_trading)
            .map(|e| e.definition().symbol.clone())
            .collect()
    } else {
        cfg.symbols.universe.clone()
    };
    info!(count = universe.len(), "symbol universe established");

    // ── WebSocket connections ─────────────────────────────────────────────────
    let max_streams = cfg.websocket.max_streams_per_connection as usize;

    let streams: Vec<String> = match venue_id {
        VenueId::BinanceSpot => universe
            .iter()
            .flat_map(|sym| {
                [
                    binance_spot_adapter::SpotStream::BookTicker.stream_name(sym),
                    binance_spot_adapter::SpotStream::Depth { update_speed_ms: 100 }.stream_name(sym),
                ]
            })
            .collect(),
        VenueId::BinanceFutures => universe
            .iter()
            .flat_map(|sym| {
                [
                    binance_futures_adapter::FuturesStream::BookTicker.stream_name(sym),
                    binance_futures_adapter::FuturesStream::Depth { update_speed_ms: 100 }.stream_name(sym),
                ]
            })
            .collect(),
    };

    let mut conn_tasks = Vec::new();
    for (i, chunk) in streams.chunks(max_streams).enumerate() {
        let url = match venue_id {
            VenueId::BinanceSpot =>
                binance_spot_adapter::build_url(&cfg.websocket.url, &chunk.to_vec()),
            VenueId::BinanceFutures =>
                binance_futures_adapter::build_url(&cfg.websocket.url, &chunk.to_vec()),
        };
        info!(connection = i, streams = chunk.len(), "starting WebSocket connection");

        let sd = shutdown_rx.clone();
        let m  = metrics.clone();
        let ws = cfg.websocket.clone();

        let task = match venue_id {
            VenueId::BinanceSpot => {
                tokio::spawn(async move {
                    let mgr = binance_spot_adapter::ConnectionManager::new(ws)
                        .with_metrics(m);
                    let (tx, mut rx) = mpsc::channel(4096);
                    tokio::spawn(async move { while rx.recv().await.is_some() {} });
                    mgr.run(&url, tx, sd).await;
                })
            }
            VenueId::BinanceFutures => {
                tokio::spawn(async move {
                    let mgr = binance_futures_adapter::ConnectionManager::new(ws);
                    let (tx, mut rx) = mpsc::channel(4096);
                    tokio::spawn(async move { while rx.recv().await.is_some() {} });
                    mgr.run(&url, tx, sd).await;
                })
            }
        };
        conn_tasks.push(task);
    }

    // ── Main loop ─────────────────────────────────────────────────────────────
    tokio::select! {
        result = axum::serve(listener, app) => { result?; }
        _ = tokio::signal::ctrl_c() => {
            info!("shutdown signal received");
            let _ = shutdown_tx.send(true);
        }
    }

    for task in conn_tasks {
        let _ = task.await;
    }

    Ok(())
}
