//! shadow-compare — compare active vs shadow connector BBO output.
//!
//! # Production use (Aeron)
//!
//! When real Aeron subscriptions are available, replace the mpsc channels
//! below with Aeron image handlers that forward frames to `SyncSender<Vec<u8>>`.
//! The `Comparator` is transport-agnostic; only the channel wiring changes.
//!
//! Active shards:  Aeron stream IDs 1..=N      (shard_stream_id(shard))
//! Shadow shards:  Aeron stream IDs 1001..=N+1000
//!
//! # Demo mode
//!
//! Pass `--demo` to run a self-contained synthetic comparison that exercises
//! all three `Verdict` states without needing live connectors.

use std::sync::mpsc;

use clap::Parser;
use tracing::info;

use shadow_compare::{CompareConfig, Comparator, SHADOW_STREAM_ID_OFFSET};

// ---------------------------------------------------------------------------
// CLI
// ---------------------------------------------------------------------------

#[derive(Parser)]
#[command(
    name = "shadow-compare",
    about = "Compare active vs shadow connector BBO output for migration safety"
)]
struct Args {
    /// Maximum allowed price divergence in basis points (1 bps = 0.01%).
    #[arg(long, default_value_t = 1)]
    tolerance_bps: i64,

    /// Minimum number of per-symbol comparison samples required before
    /// a Stable verdict can be issued.
    #[arg(long, default_value_t = 60)]
    min_samples: u64,

    /// Maximum allowed divergence rate per symbol as a percentage.
    /// 0.0 = no divergences permitted.
    #[arg(long, default_value_t = 0.0)]
    max_divergence_pct: f64,

    /// Run a synthetic demo that exercises the comparator without live connectors.
    #[arg(long)]
    demo: bool,
}

// ---------------------------------------------------------------------------
// Entry point
// ---------------------------------------------------------------------------

fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    let args = Args::parse();

    let cfg = CompareConfig {
        tolerance_bps:      args.tolerance_bps,
        min_samples:        args.min_samples,
        max_divergence_pct: args.max_divergence_pct,
    };

    if args.demo {
        run_demo(cfg);
    } else {
        print_production_instructions(&args);
    }
}

// ---------------------------------------------------------------------------
// Demo mode
// ---------------------------------------------------------------------------

fn run_demo(cfg: CompareConfig) {
    use connector_core::{
        BestBidOffer, MessageHeader, MessageType, VenueId, MarketType, SCHEMA_VERSION, TS_NONE,
    };

    info!("=== shadow-compare demo mode ===");
    info!(
        tolerance_bps = cfg.tolerance_bps,
        min_samples   = cfg.min_samples,
        "config"
    );

    let (active_tx, active_rx) = mpsc::sync_channel::<Vec<u8>>(4096);
    let (shadow_tx, shadow_rx) = mpsc::sync_channel::<Vec<u8>>(4096);
    let mut cmp = Comparator::new(cfg.clone(), active_rx, shadow_rx);

    let header = MessageHeader {
        schema_version:    SCHEMA_VERSION,
        message_type:      MessageType::BestBidOffer,
        venue_id:          VenueId::BinanceSpot,
        market_type:       MarketType::Spot,
        instrument_id:     1,
        connection_id:     0,
        instance_id:       0,
        sequence_number:   0,
        exchange_event_ts: 0,
        exchange_tx_ts:    TS_NONE,
        local_recv_ts:     0,
        local_publish_ts:  0,
    };

    let send_bbo =
        |tx: &mpsc::SyncSender<Vec<u8>>, symbol: &str, bid: i64, ask: i64, seq: u64| {
            let mut h = header;
            h.sequence_number = seq;
            let msg = BestBidOffer {
                header:      h,
                symbol:      symbol.to_string(),
                price_scale: 2,
                qty_scale:   3,
                bid_price:   bid,
                bid_qty:     1_000_000,
                ask_price:   ask,
                ask_qty:     500_000,
                update_id:   seq,
            };
            let mut buf = vec![0u8; 512];
            let len = msg.encode_into(&mut buf).unwrap();
            buf.truncate(len);
            tx.send(buf).unwrap();
        };

    // ── Phase 1: matching prices (should accumulate toward Stable) ──────────
    println!("\n[Phase 1] Sending {} matching BBO pairs...", cfg.min_samples);
    let prices: &[(&str, i64, i64)] = &[
        ("BTCUSDT", 6_400_000_00, 6_400_100_00),
        ("ETHUSDT", 1_700_000_00, 1_700_100_00),
        ("BNBUSDT",   600_000_00,   600_100_00),
        ("SOLUSDT",    70_000_00,    70_100_00),
    ];
    for seq in 0..cfg.min_samples {
        for &(sym, bid, ask) in prices {
            send_bbo(&active_tx, sym, bid, ask, seq);
            send_bbo(&shadow_tx, sym, bid, ask, seq);
        }
        cmp.tick();
    }
    print_report(&cmp, "After phase 1 (matching)");

    // ── Phase 2: inject divergence into one symbol ──────────────────────────
    println!("\n[Phase 2] Injecting a diverging BBO for ETHUSDT...");
    // Shadow sends a price 50 bps higher on ETHUSDT (stale or lagged book)
    send_bbo(&active_tx, "ETHUSDT", 1_700_000_00, 1_700_100_00, 9999);
    send_bbo(&shadow_tx, "ETHUSDT", 1_708_500_00, 1_708_600_00, 9999); // +50 bps
    cmp.tick();
    print_report(&cmp, "After phase 2 (diverging ETHUSDT)");

    // ── Phase 3: shadow recovers, prices re-align ───────────────────────────
    println!("\n[Phase 3] Shadow recovers; prices re-align...");
    for seq in 10_000..10_005 {
        for &(sym, bid, ask) in prices {
            send_bbo(&active_tx, sym, bid, ask, seq);
            send_bbo(&shadow_tx, sym, bid, ask, seq);
        }
        cmp.tick();
    }
    print_report(&cmp, "After phase 3 (recovered)");

    // ── Final verdict ───────────────────────────────────────────────────────
    let verdict = cmp.verdict();
    println!("\n=== Final verdict: {:?}", verdict);
    println!("    exit code: {}", verdict.exit_code());
    std::process::exit(verdict.exit_code());
}

fn print_report(cmp: &Comparator, label: &str) {
    println!("\n  [{label}]  total_samples={}", cmp.total_samples());
    let mut rows: Vec<_> = cmp.stats().values().collect();
    rows.sort_by_key(|s| s.symbol.as_str());
    for s in rows {
        println!(
            "    {:10}  samples={:4}  divergences={:2}  div%={:.1}  max_bid={} bps  max_ask={} bps",
            s.symbol, s.samples, s.divergences, s.divergence_pct(),
            s.max_bid_diff_bps, s.max_ask_diff_bps
        );
    }
    println!("  verdict → {:?}", cmp.verdict());
}

// ---------------------------------------------------------------------------
// Production instructions
// ---------------------------------------------------------------------------

fn print_production_instructions(args: &Args) {
    println!(
        r#"
shadow-compare — production wiring notes
=========================================

This binary is ready for production use once Aeron subscriptions are wired in.

Current config:
  --tolerance-bps      {tol}
  --min-samples        {min}
  --max-divergence-pct {div}

Aeron stream ID convention:
  Active shard k  → stream ID  k + 1
  Shadow shard k  → stream ID  k + 1 + {offset}  (SHADOW_STREAM_ID_OFFSET = {offset})

Wiring steps (when real Aeron is integrated):
  1. Open an Aeron context pointing to /dev/shm/aeron (or the configured dir).
  2. For each active shard stream ID 1..=N:
       create a Subscription and, for each image, spawn a task that reads
       fragments and forwards them to `active_tx: SyncSender<Vec<u8>>`.
  3. Do the same for shadow stream IDs (1001..=1000+N) → `shadow_tx`.
  4. Pass `active_rx` and `shadow_rx` to `Comparator::new(...)` and
       call `cmp.tick()` in a loop every 100 ms.
  5. Exit with `std::process::exit(cmp.verdict().exit_code())` after
       `min_samples` is reached.

Exit codes:
  0  STABLE    — all symbols match within tolerance for the full window
  1  DIVERGING — at least one symbol exceeded tolerance
  2  TIMEOUT   — min_samples not reached within the observation window

Run `shadow-compare --demo` to test the comparison logic with synthetic data.
See deploy/runbook.md §5 for the full migration procedure.
"#,
        tol    = args.tolerance_bps,
        min    = args.min_samples,
        div    = args.max_divergence_pct,
        offset = SHADOW_STREAM_ID_OFFSET,
    );
}
