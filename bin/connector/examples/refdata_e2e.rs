/// Stage 6.28 — Reference data end-to-end pipeline.
///
/// Fetches Binance Spot and USDT-M Futures exchange info via `RefDataService`,
/// broadcasts each `RefDataEvent` through `RefDataPublisher<NullPublication>`
/// (2 shards), and prints per-venue stats.
///
/// Demonstrates:
///   - First refresh → Added events for every symbol
///   - Second refresh → zero events (idempotency via business-field diffing)
///   - Messages per event: 2 (InstrumentDefinition + TradingStatus) × N shards
///   - Binary round-trip for the first instrument
///
///   cargo run --example refdata_e2e
use std::time::Duration;

use connector_aeron::build_null;
use connector_core::{InstrumentDefinition, MarketType, VenueId};
use connector_refdata::{RefDataPublisher, RefDataService};

// ---------------------------------------------------------------------------
// Per-venue stats
// ---------------------------------------------------------------------------

#[derive(Default)]
struct VenueStats {
    instruments: u32,
    trading: u32,
    added: u32,
    updated: u32,
    /// Total Aeron offers across all shards (1–2 messages × N shards per event).
    offers: u32,
}

// ---------------------------------------------------------------------------
// run_venue: fetch, publish, summarise
// ---------------------------------------------------------------------------

async fn run_venue(
    name: &str,
    base_url: &str,
    venue_id: VenueId,
    market_type: MarketType,
) -> VenueStats {
    const SHARDS: &[u32] = &[0, 1];

    let mut svc = RefDataService::new(
        base_url,
        venue_id,
        market_type,
        0,
        Duration::from_secs(3600), // no periodic refresh in this example
    );
    let mut pub_ = RefDataPublisher::new(build_null(SHARDS));

    println!("=== {name} ===");
    println!("  Fetching {}…", base_url);

    // --- first refresh ---------------------------------------------------
    let events = svc.refresh().await.expect("fetch_exchange_info failed");

    let mut stats = VenueStats::default();
    stats.instruments = events.len() as u32;

    for event in &events {
        if event.is_added() {
            stats.added += 1;
        }
        if event.is_updated() {
            stats.updated += 1;
        }
        if event.definition().is_trading {
            stats.trading += 1;
        }

        stats.offers += pub_.broadcast(event).expect("broadcast failed");
    }

    println!("  Instruments        : {}", stats.instruments);
    println!("  Added / Updated    : {} / {}", stats.added, stats.updated,);
    println!(
        "  Trading / Halted   : {} / {}",
        stats.trading,
        stats.instruments - stats.trading,
    );
    println!(
        "  Aeron offers       : {} ({} shards × {} events × ~2 msgs)",
        stats.offers,
        SHARDS.len(),
        stats.instruments,
    );

    // --- second refresh (idempotency) ------------------------------------
    let second = svc.refresh().await.expect("second fetch failed");
    println!(
        "  Second refresh     : {} events (expected 0 — no business-field changes)",
        second.len(),
    );

    // --- binary round-trip check -----------------------------------------
    if let Some(event) = events.first() {
        let def = event.definition();
        let mut buf = vec![0u8; 4096];
        let n = def.encode_into(&mut buf).expect("encode failed");
        let decoded = InstrumentDefinition::decode(&buf[..n]).expect("decode failed");

        assert_eq!(def.symbol, decoded.symbol);
        assert_eq!(def.base_asset, decoded.base_asset);
        assert_eq!(def.price_scale, decoded.price_scale);
        assert_eq!(def.qty_scale, decoded.qty_scale);
        assert_eq!(def.tick_size, decoded.tick_size);
        assert_eq!(def.step_size, decoded.step_size);
        assert_eq!(def.min_qty, decoded.min_qty);
        assert_eq!(def.min_notional, decoded.min_notional);
        assert_eq!(def.is_trading, decoded.is_trading);

        println!("  Round-trip check   : {} OK", decoded.symbol);
    }

    println!();
    stats
}

// ---------------------------------------------------------------------------
// main
// ---------------------------------------------------------------------------

#[tokio::main]
async fn main() {
    let spot = run_venue(
        "Binance Spot",
        "https://api.binance.com",
        VenueId::BinanceSpot,
        MarketType::Spot,
    )
    .await;

    let futures = run_venue(
        "Binance USDT-M Futures",
        "https://fapi.binance.com",
        VenueId::BinanceFutures,
        MarketType::UsdmFutures,
    )
    .await;

    println!("=== Summary ===");
    println!(
        "  Total instruments  : {} (Spot: {}, Futures: {})",
        spot.instruments + futures.instruments,
        spot.instruments,
        futures.instruments,
    );
    println!(
        "  Total Aeron offers : {} (Spot: {}, Futures: {})",
        spot.offers + futures.offers,
        spot.offers,
        futures.offers,
    );
}
