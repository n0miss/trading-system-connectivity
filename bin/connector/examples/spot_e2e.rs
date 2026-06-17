//! Stage 2.11 / 3.12 — End-to-end smoke test: live Binance Spot → normalizer → order book.
//!
//! Pipeline:
//!   Binance REST  →  InstrumentDefinition (price/qty scales)
//!   Binance WS    →  RawFrame
//!                 →  parse_spot_message    (SpotEvent)
//!                 →  normalize_spot_event  (NormalizedMessage)
//!                 →  SequenceValidator     (U/u rules §2.2)
//!                 →  OrderBook::apply_delta / mark_stale
//!                 →  ShardedPublisher::offer  (NullPublication — counts without I/O)
//!
//! Subscribes to BTCUSDT bookTicker, depth@100ms, and trade streams for 30 s
//! (or Ctrl-C), printing per-second stats to stdout.
//!
//! NOTE (Stage 3.12): The sequence validator is seeded with the first delta's
//! `first_update_id - 1` as a synthetic snapshot so the smoke test runs without
//! a REST depth-snapshot fetch.  Proper initialisation (REST snapshot → bridge)
//! is implemented in Stage 3.14.
//!
//!   cargo run --example spot_e2e

use std::time::Duration;

use anyhow::{anyhow, Context};
use tokio::sync::{mpsc, watch};

use binance_spot_adapter::{
    build_url as ws_build_url, normalize_spot_event, run_spot_recovery,
    BboCheckResult, BboValidator,
    check_snapshot, SnapshotCheckResult, SnapshotValidatorConfig, SNAPSHOT_INTERVAL_SECS,
    CircuitBreaker, CircuitState,
    ConnectionManager, NormalizeCtx, OverflowReason, PushResult, RawFrame,
    RecoveryBuffer, SequenceValidator, SpotStream, ValidateResult,
};
use connector_aeron::{build_null, NullPublication, ShardedPublisher};
use connector_config::WebSocketConfig;
use connector_core::{
    BookRecovered, BookStale, BookStaleReason, FeedState, FeedStatus, GapDetected,
    MarketType, MessageHeader, MessageType, NormalizedMessage, VenueId,
    SCHEMA_VERSION, TS_NONE,
};
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
    let mut publisher    = build_null(&[0]);
    let mut book         = OrderBook::new(SYMBOL);
    let mut validator    = SequenceValidator::new();
    let mut recovery_buf = RecoveryBuffer::new();
    let mut circuit      = CircuitBreaker::new();
    let mut bbo_validator = BboValidator::new();
    let mut feed_state   = FeedState::Connecting;
    let mut seq          = 0u64;
    let mut encode_buf   = vec![0u8; 8 * 1024];

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
    let mut total_msgs:      u64          = 0;
    let mut msgs_this_sec:   u64          = 0;
    let mut trade_count:     u64          = 0;
    let mut parse_errors:    u64          = 0;
    let mut gap_count:       u64          = 0;
    let mut overflow_count:  u64          = 0;
    let mut recover_count:   u64          = 0;
    let mut bbo_stale_count:  u64          = 0;
    let mut snap_incompat:    u64          = 0;
    let mut bbo:              Option<Bbo>  = None;

    let mut ticker            = tokio::time::interval(Duration::from_secs(1));
    let mut validation_ticker = tokio::time::interval(Duration::from_secs(SNAPSHOT_INTERVAL_SECS));
    ticker.tick().await;            // skip the immediate first tick
    validation_ticker.tick().await; // skip the immediate first tick

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
                let now = now_nanos();

                // If the book is stale and the circuit is closed, retry recovery.
                if book.is_stale() {
                    match circuit.check(now) {
                        CircuitState::Closed => {
                            tracing::info!(
                                failures = circuit.failures(),
                                "book stale — retrying recovery on tick",
                            );
                            set_feed_state(
                                FeedState::Recovering, &mut feed_state,
                                &mut encode_buf, &mut publisher, &ctx, &inst,
                                &mut seq, now,
                            );
                            match run_spot_recovery(
                                &rest, &inst, now,
                                &mut book, &mut validator, &mut recovery_buf,
                            ).await {
                                Ok(outcome) => {
                                    recover_count += 1;
                                    circuit.record_success();
                                    bbo_validator.clear();
                                    let msg = BookRecovered {
                                        header: ctrl_hdr(
                                            MessageType::BookRecovered, &ctx, &inst,
                                            &mut seq, now,
                                        ),
                                        symbol:             inst.symbol.clone(),
                                        snapshot_update_id: outcome.snapshot_id,
                                    };
                                    if let Ok(n) = msg.encode_into(&mut encode_buf) {
                                        let _ = publisher.offer(0, &encode_buf[..n]);
                                    }
                                    set_feed_state(
                                        FeedState::Live, &mut feed_state,
                                        &mut encode_buf, &mut publisher, &ctx, &inst,
                                        &mut seq, now,
                                    );
                                    tracing::info!(
                                        snapshot_id = outcome.snapshot_id,
                                        replayed    = outcome.replayed,
                                        "tick recovery succeeded",
                                    );
                                }
                                Err(e) => {
                                    let opened = circuit.record_failure(now);
                                    tracing::warn!("tick recovery failed: {e}");
                                    if opened {
                                        set_feed_state(
                                            FeedState::Degraded, &mut feed_state,
                                            &mut encode_buf, &mut publisher, &ctx, &inst,
                                            &mut seq, now,
                                        );
                                        tracing::error!(
                                            cooldown_s = circuit.cooldown_ns() / 1_000_000_000,
                                            "circuit opened — DEGRADED",
                                        );
                                    }
                                }
                            }
                        }
                        CircuitState::Open { retry_after_ns } => {
                            let remaining_s = (retry_after_ns - now).max(0) / 1_000_000_000;
                            tracing::debug!(remaining_s, "circuit open — skipping recovery tick");
                        }
                    }
                }

                let pub0     = publisher.publication(0).unwrap();
                let book_bid = book.best_bid();
                let book_ask = book.best_ask();
                let state_tag = format!(" [{feed_state:?}]");

                println!(
                    "{:>6} msg/s | {:>8} total | {} | trades {:>5} | \
                     book bid={} ask={} ({}/{}lvls){} | \
                     gaps {} bbo {} snap {} rcvr {} ovfl {} cb {}/{} | \
                     buf {}/{} | pub {}/{} B",
                    msgs_this_sec,
                    total_msgs,
                    bbo.as_ref().map(|b| b.to_string()).as_deref().unwrap_or("BBO –"),
                    trade_count,
                    book_bid.map(|l| fmt(l.price, inst.price_scale)).as_deref().unwrap_or("–"),
                    book_ask.map(|l| fmt(l.price, inst.price_scale)).as_deref().unwrap_or("–"),
                    book.bid_depth(),
                    book.ask_depth(),
                    state_tag,
                    gap_count,
                    bbo_stale_count,
                    snap_incompat,
                    recover_count,
                    overflow_count,
                    circuit.failures(),
                    circuit.max_attempts(),
                    recovery_buf.len(),
                    recovery_buf.total_bytes(),
                    pub0.messages_offered,
                    pub0.bytes_offered,
                );

                msgs_this_sec = 0;
            }

            _ = validation_ticker.tick() => {
                // Periodic REST depth snapshot validation (§3.17).
                // Only run when the book is live and has data to compare.
                if !book.is_stale() && book.bid_depth() > 0 && book.ask_depth() > 0 {
                    let now = now_nanos();
                    tracing::debug!("periodic snapshot validation — fetching REST snapshot");
                    match rest.fetch_spot_depth_snapshot(&inst, now).await {
                        Ok(snapshot) => {
                            let cfg = SnapshotValidatorConfig::default();
                            match check_snapshot(
                                book.bids(), book.asks(), &snapshot, &cfg,
                            ) {
                                SnapshotCheckResult::Compatible => {
                                    tracing::debug!(
                                        snapshot_id = snapshot.update_id,
                                        "periodic snapshot: compatible",
                                    );
                                }
                                SnapshotCheckResult::Incompatible {
                                    bid_mismatches, ask_mismatches, snapshot_id,
                                } => {
                                    snap_incompat += 1;
                                    tracing::warn!(
                                        bid_mismatches,
                                        ask_mismatches,
                                        snapshot_id,
                                        "periodic snapshot incompatible — marking book stale",
                                    );
                                    book.mark_stale(BookStaleReason::SnapshotIncompatible);
                                    let stale_msg = BookStale {
                                        header: ctrl_hdr(
                                            MessageType::BookStale, &ctx, &inst,
                                            &mut seq, now,
                                        ),
                                        symbol: inst.symbol.clone(),
                                        reason: BookStaleReason::SnapshotIncompatible,
                                    };
                                    if let Ok(n) = stale_msg.encode_into(&mut encode_buf) {
                                        let _ = publisher.offer(0, &encode_buf[..n]);
                                    }
                                    set_feed_state(
                                        FeedState::Stale, &mut feed_state,
                                        &mut encode_buf, &mut publisher,
                                        &ctx, &inst, &mut seq, now,
                                    );
                                }
                            }
                        }
                        Err(e) => {
                            tracing::warn!("periodic snapshot fetch failed: {e}");
                        }
                    }
                }
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

                // --- encode (gives us byte size for the recovery buffer) ---
                let encoded_len = match msg.encode_into(&mut encode_buf) {
                    Ok(len) => len,
                    Err(e) => {
                        tracing::warn!("encode error: {e}");
                        continue;
                    }
                };

                // --- apply to book / capture stats ---
                match &msg {
                    NormalizedMessage::BookDelta(bd) => {
                        // Startup simplification: seed the validator with the first
                        // delta's U-1 so the very first event bridges into Active
                        // state.  After a gap, run_spot_recovery replaces this with
                        // a proper REST depth snapshot (Stage 3.14).
                        if validator.last_valid_id().is_none() {
                            validator.on_snapshot(bd.first_update_id.saturating_sub(1));
                        }

                        match validator.validate(bd.first_update_id, bd.final_update_id) {
                            ValidateResult::Apply => {
                                book.apply_delta(bd);
                            }

                            ValidateResult::Gap { expected, actual, last_valid } => {
                                gap_count += 1;

                                // Publish GapDetected.
                                let gap_msg = GapDetected {
                                    header: ctrl_hdr(
                                        MessageType::GapDetected, &ctx, &inst,
                                        &mut seq, frame.recv_ts,
                                    ),
                                    symbol:             inst.symbol.clone(),
                                    expected_update_id: expected,
                                    received_update_id: actual,
                                };
                                if let Ok(n) = gap_msg.encode_into(&mut encode_buf) {
                                    let _ = publisher.offer(0, &encode_buf[..n]);
                                }

                                // Publish BookStale and mark the book.
                                book.mark_stale(BookStaleReason::SequenceGap);
                                let stale_msg = BookStale {
                                    header: ctrl_hdr(
                                        MessageType::BookStale, &ctx, &inst,
                                        &mut seq, frame.recv_ts,
                                    ),
                                    symbol: inst.symbol.clone(),
                                    reason: BookStaleReason::SequenceGap,
                                };
                                if let Ok(n) = stale_msg.encode_into(&mut encode_buf) {
                                    let _ = publisher.offer(0, &encode_buf[..n]);
                                }

                                tracing::warn!(
                                    expected,
                                    actual,
                                    last_valid,
                                    gap = actual - expected,
                                    "sequence gap",
                                );

                                // Buffer the gap-causing delta (it may bridge after the
                                // REST snapshot is applied).
                                let now = now_nanos();
                                let overflowed = push_to_recovery(
                                    &mut recovery_buf, bd, frame.recv_ts,
                                    encoded_len, &mut overflow_count,
                                );
                                if overflowed {
                                    // Buffer overflow counts as a recovery failure.
                                    let opened = circuit.record_failure(now);
                                    if opened {
                                        set_feed_state(
                                            FeedState::Degraded, &mut feed_state,
                                            &mut encode_buf, &mut publisher,
                                            &ctx, &inst, &mut seq, now,
                                        );
                                        tracing::error!("circuit opened on buffer overflow — DEGRADED");
                                    }
                                }

                                // Attempt recovery if circuit is closed.
                                // Frames arriving during the REST call accumulate in
                                // frame_rx (capacity 1 024) and are processed after.
                                match circuit.check(now) {
                                    CircuitState::Open { retry_after_ns } => {
                                        let remaining_s =
                                            (retry_after_ns - now).max(0) / 1_000_000_000;
                                        tracing::warn!(
                                            remaining_s,
                                            "circuit open — deferring recovery",
                                        );
                                        set_feed_state(
                                            FeedState::Degraded, &mut feed_state,
                                            &mut encode_buf, &mut publisher,
                                            &ctx, &inst, &mut seq, now,
                                        );
                                    }
                                    CircuitState::Closed => {
                                        set_feed_state(
                                            FeedState::Recovering, &mut feed_state,
                                            &mut encode_buf, &mut publisher,
                                            &ctx, &inst, &mut seq, now,
                                        );
                                        match run_spot_recovery(
                                            &rest, &inst, now,
                                            &mut book, &mut validator, &mut recovery_buf,
                                        ).await {
                                            Ok(outcome) => {
                                                recover_count += 1;
                                                circuit.record_success();
                                                bbo_validator.clear();
                                                let msg = BookRecovered {
                                                    header: ctrl_hdr(
                                                        MessageType::BookRecovered, &ctx,
                                                        &inst, &mut seq, now,
                                                    ),
                                                    symbol:             inst.symbol.clone(),
                                                    snapshot_update_id: outcome.snapshot_id,
                                                };
                                                if let Ok(n) = msg.encode_into(&mut encode_buf) {
                                                    let _ = publisher.offer(0, &encode_buf[..n]);
                                                }
                                                set_feed_state(
                                                    FeedState::Live, &mut feed_state,
                                                    &mut encode_buf, &mut publisher,
                                                    &ctx, &inst, &mut seq, now,
                                                );
                                                tracing::info!(
                                                    snapshot_id = outcome.snapshot_id,
                                                    replayed    = outcome.replayed,
                                                    discarded   = outcome.discarded,
                                                    "book recovered",
                                                );
                                            }
                                            Err(e) => {
                                                let opened = circuit.record_failure(now);
                                                tracing::warn!("recovery failed: {e}");
                                                if opened {
                                                    set_feed_state(
                                                        FeedState::Degraded, &mut feed_state,
                                                        &mut encode_buf, &mut publisher,
                                                        &ctx, &inst, &mut seq, now,
                                                    );
                                                    tracing::error!(
                                                        cooldown_s = circuit.cooldown_ns()
                                                            / 1_000_000_000,
                                                        "circuit opened — DEGRADED",
                                                    );
                                                } else {
                                                    set_feed_state(
                                                        FeedState::Stale, &mut feed_state,
                                                        &mut encode_buf, &mut publisher,
                                                        &ctx, &inst, &mut seq, now,
                                                    );
                                                }
                                            }
                                        }
                                    }
                                }
                            }

                            ValidateResult::Buffering => {
                                // Already stale — accumulate for replay once recovery fires.
                                let overflowed = push_to_recovery(
                                    &mut recovery_buf, bd, frame.recv_ts,
                                    encoded_len, &mut overflow_count,
                                );
                                if overflowed {
                                    let now = now_nanos();
                                    let opened = circuit.record_failure(now);
                                    if opened {
                                        set_feed_state(
                                            FeedState::Degraded, &mut feed_state,
                                            &mut encode_buf, &mut publisher,
                                            &ctx, &inst, &mut seq, now,
                                        );
                                        tracing::error!("circuit opened on buffer overflow — DEGRADED");
                                    }
                                }
                            }

                            ValidateResult::Discard => {}
                        }
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

                        // Validate top-of-book only when the book is live.
                        if !book.is_stale() {
                            let book_bid = book.best_bid().map(|l| l.price);
                            let book_ask = book.best_ask().map(|l| l.price);
                            match bbo_validator.check(
                                frame.recv_ts,
                                book_bid, book_ask,
                                b.bid_price, b.ask_price,
                            ) {
                                BboCheckResult::Ok => {}
                                BboCheckResult::Degrade { mismatch_ns } => {
                                    tracing::warn!(
                                        mismatch_ms = mismatch_ns / 1_000_000,
                                        "BBO mismatch — degrading",
                                    );
                                    set_feed_state(
                                        FeedState::Degraded, &mut feed_state,
                                        &mut encode_buf, &mut publisher,
                                        &ctx, &inst, &mut seq, frame.recv_ts,
                                    );
                                }
                                BboCheckResult::MarkStale { mismatch_ns } => {
                                    bbo_stale_count += 1;
                                    bbo_validator.clear();
                                    tracing::warn!(
                                        mismatch_ms = mismatch_ns / 1_000_000,
                                        "BBO mismatch exceeded 1 s — marking book stale",
                                    );
                                    book.mark_stale(BookStaleReason::BboMismatch);
                                    let stale_msg = BookStale {
                                        header: ctrl_hdr(
                                            MessageType::BookStale, &ctx, &inst,
                                            &mut seq, frame.recv_ts,
                                        ),
                                        symbol: inst.symbol.clone(),
                                        reason: BookStaleReason::BboMismatch,
                                    };
                                    if let Ok(n) = stale_msg.encode_into(&mut encode_buf) {
                                        let _ = publisher.offer(0, &encode_buf[..n]);
                                    }
                                    set_feed_state(
                                        FeedState::Stale, &mut feed_state,
                                        &mut encode_buf, &mut publisher,
                                        &ctx, &inst, &mut seq, frame.recv_ts,
                                    );
                                }
                            }
                        } else {
                            // Book is already stale — don't accumulate a spurious timer.
                            bbo_validator.clear();
                        }
                    }
                    NormalizedMessage::Trade(_) => {
                        trade_count += 1;
                    }
                    _ => {}
                }

                // --- offer encoded bytes (NullPublication) ---
                let _ = publisher.offer(0, &encode_buf[..encoded_len]);

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
    println!("  sequence gaps      : {gap_count}");
    println!("  BBO stale events   : {bbo_stale_count}");
    println!("  snapshot incompatible: {snap_incompat}");
    println!("  recoveries         : {recover_count}");
    println!("  buffer overflows   : {overflow_count}");
    println!("  book stale         : {}", book.is_stale());
    println!("  recovery buf       : {} events / {} bytes", recovery_buf.len(), recovery_buf.total_bytes());
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

/// Current time in nanoseconds since the Unix epoch.
fn now_nanos() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos() as i64
}

/// Build a `MessageHeader` for a control-plane message (GapDetected, BookStale, …).
fn ctrl_hdr(
    msg_type: MessageType,
    ctx:      &NormalizeCtx,
    inst:     &connector_core::InstrumentDefinition,
    seq:      &mut u64,
    ts:       i64,
) -> MessageHeader {
    *seq += 1;
    MessageHeader {
        schema_version:    SCHEMA_VERSION,
        message_type:      msg_type,
        venue_id:          ctx.venue_id,
        market_type:       ctx.market_type,
        instrument_id:     inst.header.instrument_id,
        connection_id:     ctx.connection_id,
        instance_id:       ctx.instance_id,
        sequence_number:   *seq,
        exchange_event_ts: TS_NONE,
        exchange_tx_ts:    TS_NONE,
        local_recv_ts:     ts,
        local_publish_ts:  ts,
    }
}

/// Transition `current` to `new`, publish a `FeedStatus` message, and log.
/// No-ops if the state is unchanged.
fn set_feed_state(
    new:        FeedState,
    current:    &mut FeedState,
    encode_buf: &mut Vec<u8>,
    publisher:  &mut ShardedPublisher<NullPublication>,
    ctx:        &NormalizeCtx,
    inst:       &connector_core::InstrumentDefinition,
    seq:        &mut u64,
    ts:         i64,
) {
    if *current == new { return; }
    *current = new;
    let msg = FeedStatus {
        header: ctrl_hdr(MessageType::FeedStatus, ctx, inst, seq, ts),
        state:  new,
    };
    if let Ok(n) = msg.encode_into(encode_buf) {
        let _ = publisher.offer(0, &encode_buf[..n]);
    }
    tracing::info!(?new, "feed state → {new:?}");
}

/// Push `delta` to `buf`; on overflow, clear the buffer and increment the counter.
/// Returns `true` if an overflow occurred so the caller can engage the circuit breaker.
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
                OverflowReason::Age        => "age",
                OverflowReason::EventCount => "event_count",
                OverflowReason::ByteSize   => "byte_size",
            };
            tracing::error!(reason = reason_str, "recovery buffer overflow — cleared");
            buf.clear();
            true
        }
    }
}

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
