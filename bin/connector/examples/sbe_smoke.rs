/// Live smoke test for the Binance SBE WebSocket feed (§7.30).
///
/// Connects to `wss://stream-sbe.binance.com`, subscribes to trade +
/// bookTicker + depth for one symbol, and prints each decoded SBE frame
/// until `--count` messages have been received or `--secs` have elapsed.
///
/// # Usage
///
/// ```
/// BINANCE_API_KEY=<ed25519-api-key> \
///   cargo run --example sbe_smoke -- --symbol BTCUSDT --count 20
/// ```
///
/// Exit codes:
///   0  — received at least one SBE frame successfully
///   1  — timed out or received no SBE frames
use std::time::Duration;

use tokio::sync::{mpsc, watch};
use tracing::{error, info, warn};
use tracing_subscriber::EnvFilter;

use binance_spot_adapter::{
    build_url, decode_raw_frame, ConnectionManager, DecodedFrame, RawFrame,
};
use connector_config::WebSocketConfig;
use protocol_sbe::SbeMessage;

const SBE_BASE: &str = "wss://stream-sbe.binance.com";

// ---------------------------------------------------------------------------
// CLI
// ---------------------------------------------------------------------------

struct Args {
    symbol: String,
    count:  usize,
    secs:   u64,
}

fn parse_args() -> Args {
    let mut symbol = "BTCUSDT".to_string();
    let mut count  = 20usize;
    let mut secs   = 30u64;

    let mut it = std::env::args().skip(1);
    while let Some(flag) = it.next() {
        match flag.as_str() {
            "--symbol" => { if let Some(v) = it.next() { symbol = v; } }
            "--count"  => { if let Some(v) = it.next() { count  = v.parse().unwrap_or(count); } }
            "--secs"   => { if let Some(v) = it.next() { secs   = v.parse().unwrap_or(secs);  } }
            _ => {}
        }
    }
    Args { symbol, count, secs }
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

#[tokio::main]
async fn main() -> std::process::ExitCode {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env()
            .add_directive("sbe_smoke=info".parse().unwrap()))
        .with_target(false)
        .init();

    let args = parse_args();
    let sym  = args.symbol.to_lowercase();

    let api_key = match std::env::var("BINANCE_API_KEY") {
        Ok(k) if !k.is_empty() => {
            info!("BINANCE_API_KEY found — will send X-MBX-APIKEY header");
            Some(k)
        }
        _ => {
            warn!("BINANCE_API_KEY not set — connection will likely be rejected (SBE requires auth)");
            None
        }
    };

    let streams: Vec<String> = vec![
        format!("{sym}@trade"),
        format!("{sym}@bookTicker"),
        format!("{sym}@depth@100ms"),
    ];

    let url = build_url(SBE_BASE, &streams);
    info!(%url, "connecting to Binance SBE endpoint");

    let config = WebSocketConfig {
        url:                    SBE_BASE.to_string(),
        api_key,
        ping_interval_secs:     20,
        max_streams_per_connection: 1024,
        reconnect_delay_ms:     500,
        forced_reconnect_secs:  86_400,
    };

    let (tx, mut rx) = mpsc::channel::<RawFrame>(256);
    let (shutdown_tx, shutdown_rx) = watch::channel(false);

    let mgr = ConnectionManager::new(config);
    let mgr_task = tokio::spawn(async move {
        mgr.run(&url, tx, shutdown_rx).await;
    });

    let deadline = tokio::time::sleep(Duration::from_secs(args.secs));
    tokio::pin!(deadline);

    let mut total      = 0usize;
    let mut sbe_count  = 0usize;
    let mut json_count = 0usize;  // text frames (control messages from exchange)
    let mut err_count  = 0usize;

    loop {
        tokio::select! {
            biased;

            _ = &mut deadline => {
                info!(secs = args.secs, "timeout reached");
                break;
            }

            frame = rx.recv() => {
                let Some(frame) = frame else { break };

                total += 1;
                let frame_kind = if frame.is_binary { "binary" } else { "text" };

                match decode_raw_frame(&frame) {
                    Ok(DecodedFrame::Sbe(msg)) => {
                        sbe_count += 1;
                        print_sbe(&msg);
                    }
                    Ok(DecodedFrame::Json(ev)) => {
                        // Text frames from the SBE endpoint carry control
                        // messages (subscribe confirmations, server shutdown).
                        json_count += 1;
                        info!(kind = frame_kind, ?ev, "control / JSON frame");
                    }
                    Err(e) => {
                        err_count += 1;
                        warn!(kind = frame_kind, "decode error: {e}");
                    }
                }

                if total >= args.count {
                    info!(total, "target count reached");
                    break;
                }
            }
        }
    }

    let _ = shutdown_tx.send(true);
    let _ = mgr_task.await;

    info!(
        total,
        sbe   = sbe_count,
        json  = json_count,
        errors = err_count,
        "done",
    );

    if sbe_count > 0 {
        std::process::ExitCode::SUCCESS
    } else {
        error!("received 0 SBE frames — check API key and endpoint");
        std::process::ExitCode::FAILURE
    }
}

// ---------------------------------------------------------------------------
// Pretty-printers
// ---------------------------------------------------------------------------

fn print_sbe(msg: &SbeMessage) {
    use protocol_sbe::AggressorSide;
    match msg {
        SbeMessage::Trade(t) => {
            info!(
                template = "Trade",
                symbol   = %t.symbol,
                trade_id = t.trade_id,
                price_mantissa = t.price.mantissa,
                qty_mantissa   = t.quantity.mantissa,
                aggressor = ?t.aggressor_side,
                event_time_us = t.event_time,
                "SBE frame",
            );
        }
        SbeMessage::Bbo(b) => {
            info!(
                template     = "BBO",
                symbol       = %b.symbol,
                bid_mantissa = b.best_bid_price.mantissa,
                ask_mantissa = b.best_ask_price.mantissa,
                event_time_us = b.event_time,
                "SBE frame",
            );
        }
        SbeMessage::DepthDiff(d) => {
            info!(
                template        = "DepthDiff",
                symbol          = %d.symbol,
                first_update_id = d.first_update_id,
                final_update_id = d.final_update_id,
                bids            = d.bids.len(),
                asks            = d.asks.len(),
                event_time_us   = d.event_time,
                "SBE frame",
            );
        }
        SbeMessage::DepthSnapshot(s) => {
            info!(
                template       = "DepthSnapshot",
                symbol         = %s.symbol,
                last_update_id = s.last_update_id,
                bids           = s.bids.len(),
                asks           = s.asks.len(),
                event_time_us  = s.event_time,
                "SBE frame",
            );
        }
    }
}
