//! Stage 2.11 — End-to-end smoke test: live Binance Spot → normalizer → order book.
//!
//! Pipeline:
//!   Binance REST  →  InstrumentDefinition (price/qty scales)
//!   Binance WS    →  RawFrame
//!                 →  parse_spot_message   (SpotEvent)
//!                 →  normalize_spot_event (NormalizedMessage)
//!                 →  OrderBook::apply_delta
//!                 →  ShardedPublisher::offer  (NullPublication — counts without I/O)
//!
//! Subscribes to BTCUSDT bookTicker, depth@100ms, and trade streams for 30 s
//! (or Ctrl-C), printing per-second stats to stdout.
//!
//!   cargo run --example spot_e2e

use std::time::Duration;

use anyhow::{anyhow, Context};
use tokio::sync::{mpsc, watch};

use binance_spot_adapter::{
    build_url as ws_build_url, normalize_spot_event, ConnectionManager, NormalizeCtx, RawFrame,
    SpotStream,
};
use connector_aeron::build_null;
use connector_config::WebSocketConfig;
use connector_core::{MarketType, NormalizedMessage, VenueId};
use connector_order_book::OrderBook;
use connector_refdata::RestClient;
use protocol_json::parse_spot_message;

const SYMBOL:    &str = "BTCUSDT";
const RUN_SECS:  u64  = 30;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter("info")
        .with_target(false)
        .compact()
        .init();

    // -------------------------------------------------------------------------
    // 1. Fetch BTCUSDT instrument definition (price_scale, qty_scale, id).
    // -------------------------------------------------------------------------
    tracing::info!("fetching instrument definition for {SYMBOL}");

    let rest = RestClient::new("https://api.binance.com");
    let defs = rest
        .fetch_exchange_info(VenueId::BinanceSpot, MarketType::Spot, 0, 0)
        .await
        .context("fetch_exchange_info")?;

    let inst = defs
        .into_iter()
        .find(|d| d.symbol == SYMBOL)
        .ok_or_else(|| anyhow!("{SYMBOL} not found in exchange info"))?;

    tracing::info!(
        symbol        = %inst.symbol,
        price_scale   = inst.price_scale,
        qty_scale     = inst.qty_scale,
        instrument_id = inst.header.instrument_id,
        "instrument ready",
    );

    // -------------------------------------------------------------------------
    // 2. Build the combined-stream WebSocket URL.
    // -------------------------------------------------------------------------
    let streams = vec![
        SpotStream::BookTicker.stream_name(SYMBOL),
        SpotStream::Depth { update_speed_ms: 100 }.stream_name(SYMBOL),
        SpotStream::Trade.stream_name(SYMBOL),
    ];
    let ws_url = ws_build_url("wss://stream.binance.com:9443", &streams);
    tracing::info!(%ws_url, "WebSocket URL");

    // -------------------------------------------------------------------------
    // 3. Construct pipeline components.
    // -------------------------------------------------------------------------
    let ws_cfg = WebSocketConfig {
        url:                        "wss://stream.binance.com:9443".into(),
        ping_interval_secs:         20,
        max_streams_per_connection: 1024,
        reconnect_delay_ms:         500,
        forced_reconnect_secs:      86_400,
    };

    let ctx = NormalizeCtx {
        venue_id:      VenueId::BinanceSpot,
        market_type:   MarketType::Spot,
        instance_id:   0,
        connection_id: 0,
    };

    let (frame_tx, mut frame_rx) = mpsc::channel::<RawFrame>(1024);
    let (shutdown_tx, shutdown_rx) = watch::channel(false);

    // Single shard (shard 0) for this smoke test.
    let mut publisher   = build_null(&[0]);
    let mut book        = OrderBook::new(SYMBOL);
    let mut seq         = 0u64;
    let mut encode_buf  = vec![0u8; 8 * 1024];

    // -------------------------------------------------------------------------
    // 4. Spawn the WebSocket connection manager.
    // -------------------------------------------------------------------------
    let mgr        = ConnectionManager::new(ws_cfg);
    let mgr_handle = tokio::spawn({
        let url = ws_url.clone();
        async move { mgr.run(&url, frame_tx, shutdown_rx).await }
    });

    // -------------------------------------------------------------------------
    // 5. Main loop — process frames, print stats every second.
    // -------------------------------------------------------------------------
    let mut total_msgs:    u64          = 0;
    let mut msgs_this_sec: u64          = 0;
    let mut trade_count:   u64          = 0;
    let mut parse_errors:  u64          = 0;
    let mut bbo:           Option<Bbo>  = None;

    let mut ticker = tokio::time::interval(Duration::from_secs(1));
    ticker.tick().await; // skip the immediate first tick

    let deadline = tokio::time::sleep(Duration::from_secs(RUN_SECS));
    tokio::pin!(deadline);

    println!(
        "\nRunning for {RUN_SECS}s — Ctrl-C to stop early\n\
         {:-<80}",
        "",
    );

    loop {
        tokio::select! {
            biased;

            _ = tokio::signal::ctrl_c() => {
                tracing::info!("Ctrl-C received");
                break;
            }

            _ = &mut deadline => {
                tracing::info!("{RUN_SECS}s elapsed");
                break;
            }

            _ = ticker.tick() => {
                let pub0 = publisher.publication(0).unwrap();
                let book_bid = book.best_bid();
                let book_ask = book.best_ask();

                println!(
                    "{:>6} msg/s | {:>8} total | {} | trades {:>5} | \
                     book bid={} ask={} ({}/{}lvls) | \
                     pub {}/{} B",
                    msgs_this_sec,
                    total_msgs,
                    bbo.as_ref().map(|b| b.to_string()).as_deref().unwrap_or("BBO –"),
                    trade_count,
                    book_bid.map(|l| fmt(l.price, inst.price_scale)).as_deref().unwrap_or("–"),
                    book_ask.map(|l| fmt(l.price, inst.price_scale)).as_deref().unwrap_or("–"),
                    book.bid_depth(),
                    book.ask_depth(),
                    pub0.messages_offered,
                    pub0.bytes_offered,
                );

                msgs_this_sec = 0;
            }

            frame = frame_rx.recv() => {
                let Some(frame) = frame else { break };

                // --- parse ---
                let event = match parse_spot_message(&frame.payload) {
                    Ok(e)  => e,
                    Err(e) => {
                        parse_errors += 1;
                        tracing::warn!("parse error #{parse_errors}: {e}");
                        continue;
                    }
                };

                // --- normalize ---
                let msg = match normalize_spot_event(&event, &inst, &ctx, &mut seq, frame.recv_ts) {
                    Ok(Some(m)) => m,
                    Ok(None)    => continue,
                    Err(e)      => {
                        tracing::warn!("normalize error: {e}");
                        continue;
                    }
                };

                // --- apply to book / capture stats ---
                match &msg {
                    NormalizedMessage::BookDelta(bd) => {
                        book.apply_delta(bd);
                    }
                    NormalizedMessage::BestBidOffer(b) => {
                        bbo = Some(Bbo {
                            bid_price: b.bid_price,
                            bid_qty:   b.bid_qty,
                            ask_price: b.ask_price,
                            ask_qty:   b.ask_qty,
                            p_scale:   inst.price_scale,
                            q_scale:   inst.qty_scale,
                        });
                    }
                    NormalizedMessage::Trade(_) => {
                        trade_count += 1;
                    }
                    _ => {}
                }

                // --- encode + offer (NullPublication) ---
                if let Ok(len) = msg.encode_into(&mut encode_buf) {
                    let _ = publisher.offer(0, &encode_buf[..len]);
                }

                total_msgs    += 1;
                msgs_this_sec += 1;
            }
        }
    }

    // -------------------------------------------------------------------------
    // 6. Shut down and print final summary.
    // -------------------------------------------------------------------------
    let _ = shutdown_tx.send(true);
    let _ = mgr_handle.await;

    let pub0 = publisher.publication(0).unwrap();

    println!("\n{:-<80}", "");
    println!("Final summary");
    println!("  messages processed : {total_msgs}");
    println!("  trades             : {trade_count}");
    println!("  parse errors       : {parse_errors}");
    println!("  book bid levels    : {}", book.bid_depth());
    println!("  book ask levels    : {}", book.ask_depth());
    if let Some(l) = book.best_bid() {
        println!("  best bid           : {}  qty {}", fmt(l.price, inst.price_scale), fmt(l.qty, inst.qty_scale));
    }
    if let Some(l) = book.best_ask() {
        println!("  best ask           : {}  qty {}", fmt(l.price, inst.price_scale), fmt(l.qty, inst.qty_scale));
    }
    println!("  publisher          : {} messages, {} bytes", pub0.messages_offered, pub0.bytes_offered);

    Ok(())
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Render a scaled integer as a decimal string.
fn fmt(value: i64, scale: u32) -> String {
    if scale == 0 {
        return value.to_string();
    }
    let divisor = 10_i64.pow(scale);
    let int_part  = value / divisor;
    let frac_part = (value % divisor).abs();
    format!("{int_part}.{frac_part:0>width$}", width = scale as usize)
}

/// Compact BBO display (avoids float formatting).
struct Bbo {
    bid_price: i64,
    bid_qty:   i64,
    ask_price: i64,
    ask_qty:   i64,
    p_scale:   u32,
    q_scale:   u32,
}

impl std::fmt::Display for Bbo {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "BBO bid={}@{} ask={}@{}",
            fmt(self.bid_price, self.p_scale),
            fmt(self.bid_qty,   self.q_scale),
            fmt(self.ask_price, self.p_scale),
            fmt(self.ask_qty,   self.q_scale),
        )
    }
}
