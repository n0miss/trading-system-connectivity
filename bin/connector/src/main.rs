use anyhow::Result;
use axum::{Router, http::StatusCode, response::IntoResponse, routing::get};
use clap::Parser;
use connector_core::{InstrumentDefinition, MarketType, NormalizedMessage, VenueId};
use connector_metrics::MetricsHandle;
use connector_refdata::RefDataService;
use protocol_json::{FuturesEvent, SpotEvent, parse_futures_message, parse_spot_message};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::watch;
use tracing::{info, warn};

#[derive(Parser)]
#[command(name = "connector", about = "Crypto CEX market data connector")]
struct Args {
    #[arg(short, long, default_value = "config/default.toml")]
    config: String,
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

fn spot_symbol(event: &SpotEvent) -> Option<&str> {
    match event {
        SpotEvent::BookTicker(bt) => Some(&bt.symbol),
        SpotEvent::DepthUpdate(du) => Some(&du.symbol),
        SpotEvent::Trade(tr)       => Some(&tr.symbol),
        SpotEvent::Unknown(_)      => None,
    }
}

fn futures_symbol(event: &FuturesEvent) -> Option<&str> {
    match event {
        FuturesEvent::BookTicker(bt) => Some(&bt.symbol),
        FuturesEvent::DepthUpdate(du) => Some(&du.symbol),
        FuturesEvent::AggTrade(at)   => Some(&at.symbol),
        FuturesEvent::MarkPrice(mp)  => Some(&mp.symbol),
        FuturesEvent::ForceOrder(fo) => Some(&fo.order.symbol),
        FuturesEvent::Unknown(_)     => None,
    }
}

/// Build a [`DynShardedPublisher`] for the given shard.
///
/// Retries up to `cfg.connect_retries` times with `cfg.connect_retry_delay_ms`
/// between each attempt.  Falls back to a null (no-op) publisher if all
/// attempts fail, so the connector keeps running for testing without a live
/// Aeron deployment.
async fn build_publisher(
    cfg:      &connector_config::AeronConfig,
    shard_id: u32,
    shutdown: watch::Receiver<bool>,
) -> connector_aeron::DynShardedPublisher {
    connector_aeron::build_aeron_with_retry(
        cfg,
        &[shard_id],
        Duration::from_millis(cfg.connect_retry_delay_ms),
        shutdown,
    ).await
}

/// Encode one normalized message, stamp `local_publish_ts`, and offer it.
///
/// `local_publish_ts` is patched in-place at byte offset 48 of the header
/// (see [`connector_core::header`] wire layout) so the publish timestamp is
/// accurate without re-encoding the full message.
///
/// When the publication has been closed by the media driver (`Closed` return
/// code), a synchronous reconnect is attempted via [`connector_aeron::reconnect_sync`].
/// This blocks the calling thread while reconnecting, but at that point messages
/// are already being lost, so the delay is acceptable.
fn publish_one(
    msg:       &NormalizedMessage,
    shard_id:  u32,
    publisher: &mut connector_aeron::DynShardedPublisher,
    aeron_cfg: &connector_config::AeronConfig,
    metrics:   &connector_metrics::ConnectorMetrics,
    buf:       &mut [u8],
) {
    let len = match msg.encode_into(buf) {
        Ok(n)  => n,
        Err(e) => { warn!("encode error: {e}"); return; }
    };
    let hdr        = msg.header();
    let publish_ts = binance_spot_adapter::record_publish(
        metrics,
        hdr.exchange_event_ts,
        hdr.local_recv_ts,
    );
    // Patch local_publish_ts (offset 48, 8 bytes, little-endian).
    buf[48..56].copy_from_slice(&publish_ts.to_le_bytes());

    let offer = publisher
        .offer(shard_id, &buf[..len])
        .unwrap_or(connector_aeron::OfferResult::Closed);
    if !offer.is_ok() {
        binance_spot_adapter::record_offer_failure(metrics);
        if matches!(offer, connector_aeron::OfferResult::Closed) {
            tracing::error!(shard_id, "Aeron publication closed — attempting sync reconnect");
            connector_aeron::reconnect_sync(aeron_cfg, shard_id, publisher);
        }
    }
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
        config      = %args.config,
        instance_id = cfg.instance.id,
        total       = cfg.instance.total,
        venue       = %cfg.instance.venue,
        market      = %cfg.instance.market,
        "connector starting"
    );

    let (venue_id, market_type) = parse_venue(&cfg.instance.venue, &cfg.instance.market)?;
    let metrics: MetricsHandle   = Arc::new(connector_metrics::ConnectorMetrics::new());
    let (shutdown_tx, shutdown_rx) = watch::channel(false);

    // ── Metrics HTTP server ───────────────────────────────────────────────────
    let prometheus_port = cfg.metrics.prometheus_port;
    let app = Router::new()
        .route("/metrics", get(metrics_handler))
        .with_state(metrics.clone());
    let listener = tokio::net::TcpListener::bind(("0.0.0.0", prometheus_port)).await?;
    info!(port = prometheus_port, "metrics server listening");

    // ── Symbol discovery ──────────────────────────────────────────────────────
    let rest_base_url = match venue_id {
        VenueId::BinanceSpot    => cfg.rest.spot_base_url.as_str(),
        VenueId::BinanceFutures => cfg.rest.futures_base_url.as_str(),
    };
    let mut refdata = RefDataService::new(
        rest_base_url,
        venue_id,
        market_type,
        cfg.instance.id,
        Duration::from_secs(300),
    );
    let events = refdata.refresh().await?;

    // Full trading universe for this venue/market.
    let all_symbols: Vec<String> = if cfg.symbols.universe.is_empty() {
        events
            .iter()
            .filter(|e| e.definition().is_trading)
            .map(|e| e.definition().symbol.clone())
            .collect()
    } else {
        cfg.symbols.universe.clone()
    };
    info!(count = all_symbols.len(), "symbol universe established");

    // Filter to only the symbols whose logical shard is owned by this instance.
    // Ownership: shard_for(symbol) % instance.total == instance.id
    let universe: Vec<String> = cfg
        .filter_owned_symbols(venue_id, market_type, all_symbols.iter().map(String::as_str))
        .into_iter()
        .map(str::to_owned)
        .collect();
    info!(
        instance_id  = cfg.instance.id,
        total        = cfg.instance.total,
        owned        = universe.len(),
        total_shards = cfg.sharding.total_logical_shards,
        "shard assignment complete"
    );

    // Shared instrument map — one Arc clone per connection task.
    let inst_map: Arc<HashMap<String, InstrumentDefinition>> = Arc::new(
        events
            .iter()
            .map(|e| (e.definition().symbol.clone(), e.definition().clone()))
            .collect(),
    );

    // ── WebSocket connections ─────────────────────────────────────────────────
    let instance_id = cfg.instance.id;
    let shard_id    = instance_id;
    let max_streams = cfg.websocket.max_streams_per_connection as usize;

    let streams: Vec<String> = match venue_id {
        VenueId::BinanceSpot => universe
            .iter()
            .flat_map(|sym| [
                binance_spot_adapter::SpotStream::BookTicker.stream_name(sym),
                binance_spot_adapter::SpotStream::Depth { update_speed_ms: 100 }.stream_name(sym),
                binance_spot_adapter::SpotStream::Trade.stream_name(sym),
            ])
            .collect(),
        VenueId::BinanceFutures => universe
            .iter()
            .flat_map(|sym| [
                binance_futures_adapter::FuturesStream::BookTicker.stream_name(sym),
                binance_futures_adapter::FuturesStream::Depth { update_speed_ms: 100 }.stream_name(sym),
                binance_futures_adapter::FuturesStream::AggTrade.stream_name(sym),
                binance_futures_adapter::FuturesStream::MarkPrice { update_interval_secs: 3 }.stream_name(sym),
            ])
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

        let sd      = shutdown_rx.clone();
        let m       = metrics.clone();
        let ws      = cfg.websocket.clone();
        let imap    = inst_map.clone();
        let conn_id = i as u32;

        let aeron_cfg = cfg.aeron.clone();
        let task = match venue_id {
            VenueId::BinanceSpot => tokio::spawn(async move {
                let mgr = binance_spot_adapter::ConnectionManager::new(ws)
                    .with_metrics(m.clone());

                let ctx = binance_spot_adapter::NormalizeCtx {
                    venue_id,
                    market_type,
                    instance_id,
                    connection_id: conn_id,
                };
                let mut publisher = build_publisher(&aeron_cfg, shard_id, sd.clone()).await;
                let mut buf = vec![0u8; 65_536];
                let mut seq = 0u64;

                // Process each frame inline — no channel hop, no task wakeup.
                mgr.run(&url, move |frame| {
                    let event = match parse_spot_message(&frame.payload) {
                        Ok(e)  => e,
                        Err(e) => { warn!("spot JSON: {e}"); return; }
                    };
                    let symbol = match spot_symbol(&event) {
                        Some(s) => s,
                        None    => return,
                    };
                    let inst = match imap.get(symbol) {
                        Some(i) => i,
                        None    => { warn!(symbol, "unknown instrument"); return; }
                    };
                    let msg = match binance_spot_adapter::normalize_spot_event(
                        &event, inst, &ctx, &mut seq, frame.recv_ts,
                    ) {
                        Ok(Some(m)) => m,
                        Ok(None)    => return,
                        Err(e)      => { warn!("normalize: {e}"); return; }
                    };
                    publish_one(&msg, shard_id, &mut publisher, &aeron_cfg, &m, &mut buf);
                }, sd).await;
            }),

            VenueId::BinanceFutures => tokio::spawn(async move {
                let mgr = binance_futures_adapter::ConnectionManager::new(ws)
                    .with_metrics(m.clone());

                let ctx = binance_futures_adapter::NormalizeCtx {
                    venue_id,
                    market_type,
                    instance_id,
                    connection_id: conn_id,
                };
                let mut publisher = build_publisher(&aeron_cfg, shard_id, sd.clone()).await;
                let mut buf = vec![0u8; 65_536];
                let mut seq = 0u64;

                // Process each frame inline — no channel hop, no task wakeup.
                mgr.run(&url, move |frame| {
                    let event = match parse_futures_message(&frame.payload) {
                        Ok(e)  => e,
                        Err(e) => { warn!("futures JSON: {e}"); return; }
                    };
                    let symbol = match futures_symbol(&event) {
                        Some(s) => s,
                        None    => return,
                    };
                    let inst = match imap.get(symbol) {
                        Some(i) => i,
                        None    => { warn!(symbol, "unknown instrument"); return; }
                    };
                    let msgs = match binance_futures_adapter::normalize_futures_event(
                        &event, inst, &ctx, &mut seq, frame.recv_ts,
                    ) {
                        Ok(v)  => v,
                        Err(e) => { warn!("normalize: {e}"); return; }
                    };
                    for msg in &msgs {
                        publish_one(msg, shard_id, &mut publisher, &aeron_cfg, &m, &mut buf);
                    }
                }, sd).await;
            }),
        };

        conn_tasks.push(task);
    }

    // ── Open interest polling (futures only) ─────────────────────────────────
    if venue_id == VenueId::BinanceFutures && cfg.rest.open_interest_poll_secs > 0 {
        let poll_interval = Duration::from_secs(cfg.rest.open_interest_poll_secs);
        let owned_instruments: Vec<InstrumentDefinition> = universe
            .iter()
            .filter_map(|sym| inst_map.get(sym).cloned())
            .collect();

        info!(
            symbols       = owned_instruments.len(),
            interval_secs = cfg.rest.open_interest_poll_secs,
            "starting open interest poller"
        );

        let oi_client = connector_refdata::RestClient::new(rest_base_url);
        let mut oi_poller = connector_refdata::OpenInterestPoller::new(
            oi_client,
            owned_instruments,
            poll_interval,
        );

        let sd        = shutdown_rx.clone();
        let aeron_cfg = cfg.aeron.clone();
        let m         = metrics.clone();
        let oi_task = tokio::spawn(async move {
            let mut publisher = build_publisher(&aeron_cfg, shard_id, sd.clone()).await;
            let mut buf = vec![0u8; 65_536];
            oi_poller.run(sd, |msg| {
                let nm = NormalizedMessage::OpenInterest(msg);
                publish_one(&nm, shard_id, &mut publisher, &aeron_cfg, &m, &mut buf);
            }).await.ok();
        });
        conn_tasks.push(oi_task);
    }

    // ── Main loop ─────────────────────────────────────────────────────────────
    tokio::select! {
        result = axum::serve(listener, app) => { result?; }
        _ = tokio::signal::ctrl_c() => {
            info!("shutdown signal received");
            let _ = shutdown_tx.send(true);
        }
    }

    // Wait up to 5 s for async tasks to honour the shutdown signal.
    let drain = async {
        for task in conn_tasks {
            let _ = task.await;
        }
    };
    match tokio::time::timeout(Duration::from_secs(5), drain).await {
        Ok(_)  => info!("all tasks stopped gracefully"),
        Err(_) => warn!("graceful shutdown timed out after 5 s"),
    }

    // Always force-exit rather than returning Ok(()) and letting the tokio
    // runtime drop.  The runtime drop waits for spawn_blocking threads that
    // may still be mid-execution in Aeron native C code (10-s heartbeat
    // timeout), which causes a segfault during teardown.  process::exit skips
    // all destructors and lets the OS clean up file descriptors and sockets.
    std::process::exit(0);
}
