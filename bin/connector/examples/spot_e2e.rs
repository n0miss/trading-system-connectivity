//! Stage 4.19 — Multi-symbol sharded order-book engine.
//!
//! Pipeline (per shard):
//!   Binance REST  → InstrumentDefinition list (price/qty scales)
//!   shard_for_symbol → partition symbols across TOTAL_SHARDS logical shards
//!   per-shard tokio task:
//!     Binance WS (combined stream)
//!     → RawFrame
//!     → parse_spot_message  (SpotEvent)
//!     → symbol routing via symbol_from_event
//!     → normalize_spot_event (NormalizedMessage)
//!     → SequenceValidator  (U/u rules §2.2)
//!     → OrderBook::apply_delta / mark_stale
//!     → BboValidator (§3.16) / check_snapshot (§3.17)
//!     → apply_spot_snapshot on gap (Send-safe: fetch before re-borrow)
//!     → ShardedPublisher::offer (NullPublication)
//!
//! Symbols: BTCUSDT, ETHUSDT, SOLUSDT, BNBUSDT — split across 2 shards.
//! Runs for RUN_SECS seconds (or Ctrl-C), printing per-symbol stats.
//!
//!   cargo run --example spot_e2e

use std::collections::HashMap;
use std::time::Duration;

use anyhow::Context;
use clap::Parser;
use tokio::sync::{mpsc, watch};

use binance_spot_adapter::{
    apply_spot_snapshot, build_url as ws_build_url, check_snapshot, normalize_spot_event,
    BboCheckResult, CircuitState, ConnectionManager, NormalizeCtx, OverflowReason, PushResult,
    RawFrame, RecoveryBuffer, ShardEngine, SnapshotCheckResult, SnapshotValidatorConfig,
    SpotStream, ValidateResult, SNAPSHOT_INTERVAL_SECS, SPOT_WS_BASE,
};
use connector_aeron::{build_null, Heartbeater, NullPublication, ShardedPublisher};
use connector_config::{shard_for_symbol, WebSocketConfig};
use connector_core::{
    BookRecovered, BookStale, BookStaleReason, FeedState, FeedStatus, GapDetected, Heartbeat,
    MarketType, MessageHeader, MessageType, NormalizedMessage, VenueId, SCHEMA_VERSION, TS_NONE,
};
use connector_refdata::RestClient;
use protocol_json::{parse_spot_message, SpotEvent};

const SYMBOLS: &[&str] = &["BTCUSDT", "ETHUSDT", "SOLUSDT", "BNBUSDT"];

// ---------------------------------------------------------------------------
// CLI
// ---------------------------------------------------------------------------

#[derive(Parser)]
#[command(about = "Binance Spot multi-symbol sharded order-book demo")]
struct Args {
    /// Number of shards (WebSocket connections).  Each shard handles a subset
    /// of symbols.  Must be between 1 and the number of symbols (4).
    #[arg(long, default_value_t = 2)]
    shards: u32,

    /// How long to run before exiting (seconds).
    #[arg(long, default_value_t = 30)]
    secs: u64,
}

// ---------------------------------------------------------------------------
// Entry point
// ---------------------------------------------------------------------------

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter("info")
        .with_target(false)
        .compact()
        .init();

    let args = Args::parse();
    let total_shards = args.shards.max(1);
    let run_secs = args.secs;

    // Fetch exchange info for all symbols at once.
    let rest = RestClient::new("https://api.binance.com");
    tracing::info!("fetching exchange info for {} symbols", SYMBOLS.len());
    let all_defs = rest
        .fetch_exchange_info(VenueId::BinanceSpot, MarketType::Spot, 0, 0)
        .await
        .context("fetch_exchange_info")?;

    // Keep only the symbols we care about.
    let insts: Vec<_> = all_defs
        .into_iter()
        .filter(|d| SYMBOLS.contains(&d.symbol.as_str()))
        .collect();

    if insts.len() != SYMBOLS.len() {
        let found: Vec<&str> = insts.iter().map(|d| d.symbol.as_str()).collect();
        tracing::warn!(?found, "some requested symbols not found in exchange info");
    }

    // Partition instruments by shard.
    let mut shard_map: HashMap<u32, Vec<_>> = HashMap::new();
    for inst in insts {
        let shard = shard_for_symbol(
            VenueId::BinanceSpot,
            MarketType::Spot,
            &inst.symbol,
            total_shards,
        );
        tracing::info!(symbol = %inst.symbol, shard, "assigned to shard");
        shard_map.entry(shard).or_default().push(inst);
    }

    // Spawn one task per shard.
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let mut join_set = tokio::task::JoinSet::new();

    for (shard_id, instruments) in shard_map {
        let mut engine = ShardEngine::new(shard_id);
        for inst in &instruments {
            engine.add_symbol(inst.clone());
        }
        let publisher = build_null(&[shard_id]);
        let rest_c = rest.clone();
        let shutdown = shutdown_rx.clone();
        join_set.spawn(run_shard(shard_id, engine, rest_c, shutdown, publisher));
    }

    println!(
        "\nRunning for {run_secs}s ({total_shards} shard(s)) — Ctrl-C to stop early\n{:-<80}",
        ""
    );

    tokio::select! {
        _ = tokio::signal::ctrl_c() => { tracing::info!("Ctrl-C received"); }
        _ = tokio::time::sleep(Duration::from_secs(run_secs)) => {
            tracing::info!("{run_secs}s elapsed");
        }
    }

    let _ = shutdown_tx.send(true);
    while let Some(res) = join_set.join_next().await {
        if let Err(e) = res {
            tracing::error!("shard task panicked: {e}");
        }
    }

    Ok(())
}

// ---------------------------------------------------------------------------
// Per-shard task
// ---------------------------------------------------------------------------

async fn run_shard(
    shard_id: u32,
    mut engine: ShardEngine,
    rest: RestClient,
    mut shutdown: watch::Receiver<bool>,
    mut publisher: ShardedPublisher<NullPublication>,
) {
    // Build combined-stream WebSocket URL for this shard's symbols.
    let mut streams = Vec::new();
    for (sym, _) in engine.symbols() {
        streams.push(SpotStream::BookTicker.stream_name(sym));
        streams.push(
            SpotStream::Depth {
                update_speed_ms: 100,
            }
            .stream_name(sym),
        );
        streams.push(SpotStream::Trade.stream_name(sym));
    }
    let ws_url = ws_build_url(SPOT_WS_BASE, &streams);

    tracing::info!(shard_id, symbols = engine.symbol_count(), %ws_url, "shard starting");

    let ctx = NormalizeCtx {
        venue_id: VenueId::BinanceSpot,
        market_type: MarketType::Spot,
        instance_id: 0,
        connection_id: shard_id,
    };

    let mut seq = 0u64;
    let mut encode_buf = vec![0u8; 8 * 1024];
    let mut heartbeater = Heartbeater::new();

    // Per-symbol counters are kept separate from protocol state so we can borrow
    // `engine` mutably while also reading/writing `counters`.
    let mut counters: HashMap<String, SymbolCounters> = engine
        .symbol_names()
        .map(|s| (s.to_string(), SymbolCounters::default()))
        .collect();

    let ws_cfg = WebSocketConfig {
        url: SPOT_WS_BASE.into(),
        futures_url: "wss://fstream.binance.com:443".into(),
        api_key: None,
        ping_interval_secs: 20,
        max_streams_per_connection: 1024,
        reconnect_delay_ms: 500,
        forced_reconnect_secs: 86_400,
    };

    let (frame_tx, mut frame_rx) = mpsc::channel::<RawFrame>(1024);
    let mgr = ConnectionManager::new(ws_cfg);
    let mgr_handle = tokio::spawn({
        let url = ws_url.clone();
        let shutdown_m = shutdown.clone();
        async move {
            mgr.run(
                &url,
                move |frame| {
                    let _ = frame_tx.try_send(frame);
                },
                shutdown_m,
            )
            .await
        }
    });

    let mut ticker = tokio::time::interval(Duration::from_secs(1));
    let mut validation_ticker = tokio::time::interval(Duration::from_secs(SNAPSHOT_INTERVAL_SECS));
    ticker.tick().await;
    validation_ticker.tick().await;

    loop {
        tokio::select! {
            biased;

            _ = shutdown.changed() => {
                if *shutdown.borrow() { break; }
            }

            // -----------------------------------------------------------------
            // 1-second stats tick + stale-book recovery retry
            // -----------------------------------------------------------------
            _ = ticker.tick() => {
                let now = now_nanos();

                // Collect stale symbols before we start borrowing engine mutably.
                let stale_syms: Vec<String> = engine
                    .symbols()
                    .filter(|(_, s)| s.is_stale())
                    .map(|(sym, _)| sym.to_string())
                    .collect();

                for sym in &stale_syms {
                    // Check circuit (releases borrow before any await).
                    let circuit_closed = {
                        let state = engine.get_mut(sym).unwrap();
                        matches!(state.circuit.check(now), CircuitState::Closed)
                    };
                    if !circuit_closed { continue; }

                    // Clone inst so we hold no engine borrow across the await.
                    let inst = engine.get(sym).unwrap().inst.clone();

                    {
                        let state = engine.get_mut(sym).unwrap();
                        set_feed_state(
                            FeedState::Recovering, shard_id, &mut state.feed_state,
                            &mut encode_buf, &mut publisher, &ctx, &state.inst,
                            &mut seq, now,
                        );
                    }

                    // ← no engine borrow alive here
                    let fetch_result = rest.fetch_spot_depth_snapshot(&inst, now).await;

                    let state = engine.get_mut(sym).unwrap();
                    match fetch_result {
                        Ok(snapshot) => {
                            match apply_spot_snapshot(
                                &snapshot,
                                &mut state.book, &mut state.validator, &mut state.recovery_buf,
                            ) {
                                Ok(outcome) => {
                                    state.circuit.record_success();
                                    state.bbo_validator.clear();
                                    counters.entry(sym.clone()).or_default().recover_count += 1;
                                    let msg = BookRecovered {
                                        header: ctrl_hdr(
                                            MessageType::BookRecovered, &ctx, &state.inst,
                                            &mut seq, now,
                                        ),
                                        symbol:             state.inst.symbol.clone(),
                                        snapshot_update_id: outcome.snapshot_id,
                                    };
                                    if let Ok(n) = msg.encode_into(&mut encode_buf) {
                                        let _ = publisher.offer(shard_id, &encode_buf[..n]);
                                    }
                                    set_feed_state(
                                        FeedState::Live, shard_id, &mut state.feed_state,
                                        &mut encode_buf, &mut publisher, &ctx, &state.inst,
                                        &mut seq, now,
                                    );
                                    tracing::info!(
                                        shard_id, %sym,
                                        snapshot_id = outcome.snapshot_id,
                                        replayed    = outcome.replayed,
                                        "tick recovery succeeded",
                                    );
                                }
                                Err(e) => {
                                    let opened = state.circuit.record_failure(now);
                                    tracing::warn!(shard_id, %sym, "tick recovery apply failed: {e}");
                                    if opened {
                                        set_feed_state(
                                            FeedState::Degraded, shard_id, &mut state.feed_state,
                                            &mut encode_buf, &mut publisher, &ctx, &state.inst,
                                            &mut seq, now,
                                        );
                                    }
                                }
                            }
                        }
                        Err(e) => {
                            let opened = state.circuit.record_failure(now);
                            tracing::warn!(shard_id, %sym, "tick recovery fetch failed: {e}");
                            if opened {
                                set_feed_state(
                                    FeedState::Degraded, shard_id, &mut state.feed_state,
                                    &mut encode_buf, &mut publisher, &ctx, &state.inst,
                                    &mut seq, now,
                                );
                            }
                        }
                    }
                }

                // Print per-symbol stats.
                let default_c = SymbolCounters::default();
                for (sym, state) in engine.symbols() {
                    let c        = counters.get(sym).unwrap_or(&default_c);
                    let book_bid = state.book.best_bid();
                    let book_ask = state.book.best_ask();
                    let ps       = state.inst.price_scale;
                    let qs       = state.inst.qty_scale;
                    println!(
                        "shard={shard_id} {sym:>8} | {:>5}/s {:>8}tot | {} | \
                         bid={} ask={} ({}/{}lvls) [{:?}] | \
                         gaps {} bbo {} snap {} rcvr {} ovfl {} cb {}/{}",
                        c.msgs_this_sec,
                        c.total_msgs,
                        c.bbo.as_ref().map(|b| b.to_string()).as_deref().unwrap_or("BBO –    "),
                        book_bid.map(|l| fmt(l.price, ps)).as_deref().unwrap_or("–"),
                        book_ask.map(|l| fmt(l.price, ps)).as_deref().unwrap_or("–"),
                        state.book.bid_depth(),
                        state.book.ask_depth(),
                        state.feed_state,
                        c.gap_count,
                        c.bbo_stale_count,
                        c.snap_incompat,
                        c.recover_count,
                        c.overflow_count,
                        state.circuit.failures(),
                        state.circuit.max_attempts(),
                    );
                    let _ = qs; // suppress if unused in fmt
                }
                for c in counters.values_mut() { c.msgs_this_sec = 0; }

                // Publish a shard-level heartbeat so downstream consumers can
                // detect feed staleness even during quiet markets.
                if heartbeater.is_due(now) {
                    let hb = Heartbeat { header: shard_hdr(MessageType::Heartbeat, &ctx, &mut seq, now) };
                    if let Ok(n) = hb.encode_into(&mut encode_buf) {
                        let _ = publisher.offer(shard_id, &encode_buf[..n]);
                    }
                    heartbeater.record_beat(now);
                    tracing::debug!(shard_id, "heartbeat published");
                }
            }

            // -----------------------------------------------------------------
            // Periodic REST snapshot validation (§3.17)
            // -----------------------------------------------------------------
            _ = validation_ticker.tick() => {
                let live_syms: Vec<String> = engine
                    .symbols()
                    .filter(|(_, s)| !s.is_stale() && s.book.bid_depth() > 0 && s.book.ask_depth() > 0)
                    .map(|(sym, _)| sym.to_string())
                    .collect();

                for sym in &live_syms {
                    let now  = now_nanos();
                    let inst = engine.get(sym).unwrap().inst.clone();

                    // ← no engine borrow alive here
                    let fetch_result = rest.fetch_spot_depth_snapshot(&inst, now).await;

                    match fetch_result {
                        Ok(snapshot) => {
                            let state = engine.get_mut(sym).unwrap();
                            let cfg   = SnapshotValidatorConfig::default();
                            match check_snapshot(state.book.bids(), state.book.asks(), &snapshot, &cfg)
                            {
                                SnapshotCheckResult::Compatible => {
                                    tracing::debug!(
                                        shard_id, %sym,
                                        snapshot_id = snapshot.update_id,
                                        "periodic snapshot: compatible",
                                    );
                                }
                                SnapshotCheckResult::Incompatible {
                                    bid_mismatches, ask_mismatches, snapshot_id,
                                } => {
                                    counters.entry(sym.clone()).or_default().snap_incompat += 1;
                                    tracing::warn!(
                                        shard_id, %sym,
                                        bid_mismatches, ask_mismatches, snapshot_id,
                                        "periodic snapshot incompatible — marking stale",
                                    );
                                    state.book.mark_stale(BookStaleReason::SnapshotIncompatible);
                                    let stale_msg = BookStale {
                                        header: ctrl_hdr(
                                            MessageType::BookStale, &ctx, &state.inst,
                                            &mut seq, now,
                                        ),
                                        symbol: state.inst.symbol.clone(),
                                        reason: BookStaleReason::SnapshotIncompatible,
                                    };
                                    if let Ok(n) = stale_msg.encode_into(&mut encode_buf) {
                                        let _ = publisher.offer(shard_id, &encode_buf[..n]);
                                    }
                                    set_feed_state(
                                        FeedState::Stale, shard_id, &mut state.feed_state,
                                        &mut encode_buf, &mut publisher, &ctx, &state.inst,
                                        &mut seq, now,
                                    );
                                }
                            }
                        }
                        Err(e) => {
                            tracing::warn!(shard_id, %sym, "periodic snapshot fetch failed: {e}");
                        }
                    }
                }
            }

            // -----------------------------------------------------------------
            // WebSocket frame
            // -----------------------------------------------------------------
            frame = frame_rx.recv() => {
                let Some(frame) = frame else { break };

                // Parse.
                let event = match parse_spot_message(&frame.payload) {
                    Ok(e)  => e,
                    Err(e) => { tracing::warn!(shard_id, "parse error: {e}"); continue; }
                };

                // Route by symbol.  Ignore events for symbols not in this shard.
                let symbol = match symbol_from_event(&event) {
                    Some(s) => s.to_string(),
                    None    => continue,
                };
                if !engine.contains_symbol(&symbol) { continue; }

                // Normalize — hold an immutable borrow for the inst reference,
                // then release it before any mutable access.
                let msg = {
                    let state = engine.get(&symbol).unwrap();
                    match normalize_spot_event(&event, &state.inst, &ctx, &mut seq, frame.recv_ts) {
                        Ok(Some(m)) => m,
                        Ok(None)    => continue,
                        Err(e)      => {
                            tracing::warn!(shard_id, %symbol, "normalize error: {e}");
                            continue;
                        }
                    }
                }; // immutable borrow of engine released here

                // Encode (encode_buf is independent of engine).
                let encoded_len = match msg.encode_into(&mut encode_buf) {
                    Ok(n)  => n,
                    Err(e) => { tracing::warn!(shard_id, %symbol, "encode error: {e}"); continue; }
                };

                // Update message counters (counters is independent of engine).
                {
                    let c = counters.entry(symbol.clone()).or_default();
                    c.total_msgs    += 1;
                    c.msgs_this_sec += 1;
                }

                // Dispatch by message type.
                match &msg {
                    NormalizedMessage::BookDelta(bd) => {
                        // Startup seed: treat first delta as if we just took a snapshot
                        // at U-1 so the very first event bridges into Active state.
                        {
                            let state = engine.get_mut(&symbol).unwrap();
                            if state.validator.last_valid_id().is_none() {
                                state.validator.on_snapshot(bd.first_update_id.saturating_sub(1));
                            }
                        }

                        // Validate.  Collect the result without holding state across
                        // any potential await below.
                        let validate_result = {
                            let state = engine.get_mut(&symbol).unwrap();
                            state.validator.validate(bd.first_update_id, bd.final_update_id)
                        };

                        match validate_result {
                            ValidateResult::Apply => {
                                let was_connecting = {
                                    let state = engine.get_mut(&symbol).unwrap();
                                    let c = state.feed_state == FeedState::Connecting;
                                    state.book.apply_delta(bd);
                                    c
                                };
                                if was_connecting {
                                    let state = engine.get_mut(&symbol).unwrap();
                                    set_feed_state(
                                        FeedState::Live, shard_id, &mut state.feed_state,
                                        &mut encode_buf, &mut publisher, &ctx, &state.inst,
                                        &mut seq, frame.recv_ts,
                                    );
                                }
                            }
                            ValidateResult::Discard => {}
                            ValidateResult::Buffering => {
                                let now   = now_nanos();
                                let state = engine.get_mut(&symbol).unwrap();
                                let c     = counters.entry(symbol.clone()).or_default();
                                let overflowed = push_to_recovery(
                                    &mut state.recovery_buf, bd, frame.recv_ts,
                                    encoded_len, &mut c.overflow_count,
                                );
                                if overflowed {
                                    let opened = state.circuit.record_failure(now);
                                    if opened {
                                        set_feed_state(
                                            FeedState::Degraded, shard_id, &mut state.feed_state,
                                            &mut encode_buf, &mut publisher, &ctx, &state.inst,
                                            &mut seq, now,
                                        );
                                        tracing::error!(shard_id, %symbol, "circuit opened on buffer overflow — DEGRADED");
                                    }
                                }
                            }
                            ValidateResult::Gap { expected, actual, last_valid } => {
                                let now = now_nanos();

                                // Sync work: publish events, buffer delta, check circuit.
                                // Held in an explicit block so the borrow ends before await.
                                let (circuit_open, inst) = {
                                    let state = engine.get_mut(&symbol).unwrap();
                                    let c     = counters.entry(symbol.clone()).or_default();
                                    c.gap_count += 1;

                                    let gap_msg = GapDetected {
                                        header: ctrl_hdr(
                                            MessageType::GapDetected, &ctx, &state.inst,
                                            &mut seq, frame.recv_ts,
                                        ),
                                        symbol:             state.inst.symbol.clone(),
                                        expected_update_id: expected,
                                        received_update_id: actual,
                                    };
                                    if let Ok(n) = gap_msg.encode_into(&mut encode_buf) {
                                        let _ = publisher.offer(shard_id, &encode_buf[..n]);
                                    }

                                    state.book.mark_stale(BookStaleReason::SequenceGap);
                                    let stale_msg = BookStale {
                                        header: ctrl_hdr(
                                            MessageType::BookStale, &ctx, &state.inst,
                                            &mut seq, frame.recv_ts,
                                        ),
                                        symbol: state.inst.symbol.clone(),
                                        reason: BookStaleReason::SequenceGap,
                                    };
                                    if let Ok(n) = stale_msg.encode_into(&mut encode_buf) {
                                        let _ = publisher.offer(shard_id, &encode_buf[..n]);
                                    }

                                    tracing::warn!(
                                        shard_id, %symbol, expected, actual,
                                        gap  = actual.saturating_sub(expected),
                                        last_valid,
                                        "sequence gap",
                                    );

                                    let overflowed = push_to_recovery(
                                        &mut state.recovery_buf, bd, frame.recv_ts,
                                        encoded_len, &mut c.overflow_count,
                                    );
                                    if overflowed {
                                        let opened = state.circuit.record_failure(now);
                                        if opened {
                                            set_feed_state(
                                                FeedState::Degraded, shard_id,
                                                &mut state.feed_state, &mut encode_buf,
                                                &mut publisher, &ctx, &state.inst,
                                                &mut seq, now,
                                            );
                                        }
                                    }

                                    let circuit_open =
                                        matches!(state.circuit.check(now), CircuitState::Open { .. });
                                    let inst = state.inst.clone();
                                    (circuit_open, inst)
                                }; // engine borrow released

                                if circuit_open {
                                    let state = engine.get_mut(&symbol).unwrap();
                                    set_feed_state(
                                        FeedState::Degraded, shard_id, &mut state.feed_state,
                                        &mut encode_buf, &mut publisher, &ctx, &state.inst,
                                        &mut seq, now,
                                    );
                                    tracing::warn!(shard_id, %symbol, "circuit open — deferring recovery");
                                } else {
                                    {
                                        let state = engine.get_mut(&symbol).unwrap();
                                        set_feed_state(
                                            FeedState::Recovering, shard_id, &mut state.feed_state,
                                            &mut encode_buf, &mut publisher, &ctx, &state.inst,
                                            &mut seq, now,
                                        );
                                    }

                                    // ← no engine borrow alive across this await
                                    let fetch_result =
                                        rest.fetch_spot_depth_snapshot(&inst, now).await;

                                    let state = engine.get_mut(&symbol).unwrap();
                                    let c     = counters.entry(symbol.clone()).or_default();
                                    match fetch_result {
                                        Ok(snapshot) => {
                                            match apply_spot_snapshot(
                                                &snapshot,
                                                &mut state.book,
                                                &mut state.validator,
                                                &mut state.recovery_buf,
                                            ) {
                                                Ok(outcome) => {
                                                    c.recover_count += 1;
                                                    state.circuit.record_success();
                                                    state.bbo_validator.clear();
                                                    let msg = BookRecovered {
                                                        header: ctrl_hdr(
                                                            MessageType::BookRecovered, &ctx,
                                                            &state.inst, &mut seq, now,
                                                        ),
                                                        symbol:             state.inst.symbol.clone(),
                                                        snapshot_update_id: outcome.snapshot_id,
                                                    };
                                                    if let Ok(n) = msg.encode_into(&mut encode_buf) {
                                                        let _ = publisher.offer(shard_id, &encode_buf[..n]);
                                                    }
                                                    set_feed_state(
                                                        FeedState::Live, shard_id,
                                                        &mut state.feed_state, &mut encode_buf,
                                                        &mut publisher, &ctx, &state.inst,
                                                        &mut seq, now,
                                                    );
                                                    tracing::info!(
                                                        shard_id, %symbol,
                                                        snapshot_id = outcome.snapshot_id,
                                                        replayed    = outcome.replayed,
                                                        discarded   = outcome.discarded,
                                                        "book recovered after gap",
                                                    );
                                                }
                                                Err(e) => {
                                                    let opened = state.circuit.record_failure(now);
                                                    tracing::warn!(shard_id, %symbol, "gap recovery apply failed: {e}");
                                                    if opened {
                                                        set_feed_state(
                                                            FeedState::Degraded, shard_id,
                                                            &mut state.feed_state, &mut encode_buf,
                                                            &mut publisher, &ctx, &state.inst,
                                                            &mut seq, now,
                                                        );
                                                        tracing::error!(
                                                            shard_id, %symbol,
                                                            cooldown_s = state.circuit.cooldown_ns() / 1_000_000_000,
                                                            "circuit opened — DEGRADED",
                                                        );
                                                    } else {
                                                        set_feed_state(
                                                            FeedState::Stale, shard_id,
                                                            &mut state.feed_state, &mut encode_buf,
                                                            &mut publisher, &ctx, &state.inst,
                                                            &mut seq, now,
                                                        );
                                                    }
                                                }
                                            }
                                        }
                                        Err(e) => {
                                            let opened = state.circuit.record_failure(now);
                                            tracing::warn!(shard_id, %symbol, "gap recovery fetch failed: {e}");
                                            if opened {
                                                set_feed_state(
                                                    FeedState::Degraded, shard_id,
                                                    &mut state.feed_state, &mut encode_buf,
                                                    &mut publisher, &ctx, &state.inst,
                                                    &mut seq, now,
                                                );
                                                tracing::error!(
                                                    shard_id, %symbol,
                                                    cooldown_s = state.circuit.cooldown_ns() / 1_000_000_000,
                                                    "circuit opened — DEGRADED",
                                                );
                                            } else {
                                                set_feed_state(
                                                    FeedState::Stale, shard_id,
                                                    &mut state.feed_state, &mut encode_buf,
                                                    &mut publisher, &ctx, &state.inst,
                                                    &mut seq, now,
                                                );
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }

                    NormalizedMessage::BestBidOffer(b) => {
                        // Record BBO for display.
                        {
                            let state = engine.get(&symbol).unwrap();
                            let c     = counters.entry(symbol.clone()).or_default();
                            c.bbo = Some(Bbo {
                                bid_price: b.bid_price,
                                bid_qty:   b.bid_qty,
                                ask_price: b.ask_price,
                                ask_qty:   b.ask_qty,
                                p_scale:   state.inst.price_scale,
                                q_scale:   state.inst.qty_scale,
                            });
                        }

                        // Validate top-of-book only when the book is live.
                        let book_is_stale = engine.get(&symbol).unwrap().book.is_stale();
                        if book_is_stale {
                            engine.get_mut(&symbol).unwrap().bbo_validator.clear();
                        } else {
                            let (book_bid, book_ask) = {
                                let state = engine.get(&symbol).unwrap();
                                (
                                    state.book.best_bid().map(|l| l.price),
                                    state.book.best_ask().map(|l| l.price),
                                )
                            };
                            let bbo_result = {
                                let state = engine.get_mut(&symbol).unwrap();
                                state.bbo_validator.check(
                                    frame.recv_ts, book_bid, book_ask,
                                    b.bid_price, b.ask_price,
                                )
                            };
                            match bbo_result {
                                BboCheckResult::Ok => {}
                                BboCheckResult::Degrade { mismatch_ns } => {
                                    let state = engine.get_mut(&symbol).unwrap();
                                    tracing::warn!(
                                        shard_id, %symbol,
                                        mismatch_ms = mismatch_ns / 1_000_000,
                                        "BBO mismatch — degrading",
                                    );
                                    set_feed_state(
                                        FeedState::Degraded, shard_id, &mut state.feed_state,
                                        &mut encode_buf, &mut publisher, &ctx, &state.inst,
                                        &mut seq, frame.recv_ts,
                                    );
                                }
                                BboCheckResult::MarkStale { mismatch_ns } => {
                                    counters.entry(symbol.clone()).or_default().bbo_stale_count += 1;
                                    let state = engine.get_mut(&symbol).unwrap();
                                    state.bbo_validator.clear();
                                    tracing::warn!(
                                        shard_id, %symbol,
                                        mismatch_ms = mismatch_ns / 1_000_000,
                                        "BBO mismatch exceeded 1 s — marking stale",
                                    );
                                    state.book.mark_stale(BookStaleReason::BboMismatch);
                                    let stale_msg = BookStale {
                                        header: ctrl_hdr(
                                            MessageType::BookStale, &ctx, &state.inst,
                                            &mut seq, frame.recv_ts,
                                        ),
                                        symbol: state.inst.symbol.clone(),
                                        reason: BookStaleReason::BboMismatch,
                                    };
                                    if let Ok(n) = stale_msg.encode_into(&mut encode_buf) {
                                        let _ = publisher.offer(shard_id, &encode_buf[..n]);
                                    }
                                    set_feed_state(
                                        FeedState::Stale, shard_id, &mut state.feed_state,
                                        &mut encode_buf, &mut publisher, &ctx, &state.inst,
                                        &mut seq, frame.recv_ts,
                                    );
                                }
                            }
                        }
                    }

                    NormalizedMessage::Trade(_) => {
                        counters.entry(symbol.clone()).or_default().trade_count += 1;
                    }

                    _ => {}
                }

                // Offer the encoded bytes to the shard's publication.
                let _ = publisher.offer(shard_id, &encode_buf[..encoded_len]);
            }
        }
    }

    mgr_handle.abort();
    tracing::info!(shard_id, "shard stopped");
}

// ---------------------------------------------------------------------------
// Per-symbol display counters
// ---------------------------------------------------------------------------

#[derive(Default)]
struct SymbolCounters {
    total_msgs: u64,
    msgs_this_sec: u64,
    trade_count: u64,
    gap_count: u64,
    overflow_count: u64,
    recover_count: u64,
    bbo_stale_count: u64,
    snap_incompat: u64,
    bbo: Option<Bbo>,
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn now_nanos() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos() as i64
}

fn ctrl_hdr(
    msg_type: MessageType,
    ctx: &NormalizeCtx,
    inst: &connector_core::InstrumentDefinition,
    seq: &mut u64,
    ts: i64,
) -> MessageHeader {
    *seq += 1;
    MessageHeader {
        schema_version: SCHEMA_VERSION,
        message_type: msg_type,
        venue_id: ctx.venue_id,
        market_type: ctx.market_type,
        instrument_id: inst.header.instrument_id,
        connection_id: ctx.connection_id,
        instance_id: ctx.instance_id,
        sequence_number: *seq,
        exchange_event_ts: TS_NONE,
        exchange_tx_ts: TS_NONE,
        local_recv_ts: ts,
        local_publish_ts: ts,
    }
}

/// Build a shard-level `MessageHeader` (instrument_id = 0) for messages that
/// cover the whole shard rather than a single instrument (e.g. `Heartbeat`).
fn shard_hdr(msg_type: MessageType, ctx: &NormalizeCtx, seq: &mut u64, ts: i64) -> MessageHeader {
    *seq += 1;
    MessageHeader {
        schema_version: SCHEMA_VERSION,
        message_type: msg_type,
        venue_id: ctx.venue_id,
        market_type: ctx.market_type,
        instrument_id: 0,
        connection_id: ctx.connection_id,
        instance_id: ctx.instance_id,
        sequence_number: *seq,
        exchange_event_ts: TS_NONE,
        exchange_tx_ts: TS_NONE,
        local_recv_ts: ts,
        local_publish_ts: ts,
    }
}

fn set_feed_state(
    new: FeedState,
    shard_id: u32,
    current: &mut FeedState,
    encode_buf: &mut Vec<u8>,
    publisher: &mut ShardedPublisher<NullPublication>,
    ctx: &NormalizeCtx,
    inst: &connector_core::InstrumentDefinition,
    seq: &mut u64,
    ts: i64,
) {
    if *current == new {
        return;
    }
    *current = new;
    let msg = FeedStatus {
        header: ctrl_hdr(MessageType::FeedStatus, ctx, inst, seq, ts),
        state: new,
    };
    if let Ok(n) = msg.encode_into(encode_buf) {
        let _ = publisher.offer(shard_id, &encode_buf[..n]);
    }
    tracing::info!(?new, "feed state → {new:?}");
}

fn push_to_recovery(
    buf: &mut RecoveryBuffer,
    delta: &connector_core::BookDelta,
    recv_ts: i64,
    encoded_size: usize,
    overflow_count: &mut u64,
) -> bool {
    match buf.push(delta.clone(), recv_ts, encoded_size) {
        PushResult::Accepted => false,
        PushResult::Overflow(reason) => {
            *overflow_count += 1;
            let reason_str = match reason {
                OverflowReason::Age => "age",
                OverflowReason::EventCount => "event_count",
                OverflowReason::ByteSize => "byte_size",
            };
            tracing::error!(reason = reason_str, "recovery buffer overflow — cleared");
            buf.clear();
            true
        }
    }
}

fn fmt(value: i64, scale: u32) -> String {
    if scale == 0 {
        return value.to_string();
    }
    let divisor = 10_i64.pow(scale);
    let int_part = value / divisor;
    let frac_part = (value % divisor).abs();
    format!("{int_part}.{frac_part:0>width$}", width = scale as usize)
}

fn symbol_from_event(event: &SpotEvent) -> Option<&str> {
    match event {
        SpotEvent::BookTicker(b) => Some(&b.symbol),
        SpotEvent::DepthUpdate(d) => Some(&d.symbol),
        SpotEvent::Trade(t) => Some(&t.symbol),
        SpotEvent::Unknown(_) => None,
    }
}

struct Bbo {
    bid_price: i64,
    bid_qty: i64,
    ask_price: i64,
    ask_qty: i64,
    p_scale: u32,
    q_scale: u32,
}

impl std::fmt::Display for Bbo {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "BBO bid={}@{} ask={}@{}",
            fmt(self.bid_price, self.p_scale),
            fmt(self.bid_qty, self.q_scale),
            fmt(self.ask_price, self.p_scale),
            fmt(self.ask_qty, self.q_scale),
        )
    }
}
