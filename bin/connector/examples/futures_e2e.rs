//! Stage 5.25 — Binance USDT-M Futures multi-symbol sharded pipeline.
//!
//! Pipeline (per shard):
//!   Binance REST  → InstrumentDefinition list (price/qty scales, UsdmFutures)
//!   shard_for_symbol → partition symbols across TOTAL_SHARDS logical shards
//!   per-shard tokio task:
//!     Binance Futures WS (combined stream)
//!     → RawFrame
//!     → parse_futures_message (FuturesEvent)
//!     → symbol routing via symbol_from_futures_event
//!     → normalize_futures_event (Vec<NormalizedMessage>)
//!     → for BookDelta: FuturesSequenceValidator (pu-based gap detection §5.24)
//!                      → OrderBook::apply_delta / mark_stale
//!                      → on Gap: fetch_futures_depth_snapshot (Send-safe)
//!                                → apply_futures_snapshot
//!     → for MarkPrice/FundingRate/Trade/Liquidation: publish directly
//!     → ShardedPublisher::offer (NullPublication)
//!
//! Symbols: BTCUSDT, ETHUSDT, SOLUSDT, BNBUSDT — split across 2 shards.
//! Streams per symbol: bookTicker + depth@100ms + aggTrade + markPrice@1s + forceOrder.
//! Runs for RUN_SECS seconds (or Ctrl-C), printing per-symbol stats.
//!
//!   cargo run --example futures_e2e

use std::collections::HashMap;
use std::time::Duration;
use std::time::SystemTime;

use anyhow::Context;
use tokio::sync::{mpsc, watch};

use binance_futures_adapter::{
    apply_futures_snapshot, build_url, normalize_futures_event, ConnectionManager,
    FuturesShardEngine, FuturesStream, NormalizeCtx, OverflowReason, PushResult, RawFrame,
    ValidateResult, ValidationState, FUTURES_WS_BASE,
};
use connector_aeron::{build_null, Heartbeater, NullPublication, ShardedPublisher};
use connector_config::{shard_for_symbol, WebSocketConfig};
use connector_core::{
    BookRecovered, BookStale, BookStaleReason, FeedState, FeedStatus, GapDetected, Heartbeat,
    MarketType, MessageHeader, MessageType, NormalizedMessage, VenueId, SCHEMA_VERSION, TS_NONE,
};
use connector_refdata::RestClient;
use protocol_json::{parse_futures_message, FuturesEvent};

const SYMBOLS: &[&str] = &["BTCUSDT", "ETHUSDT", "SOLUSDT", "BNBUSDT"];
const TOTAL_SHARDS: u32 = 2;
const RUN_SECS: u64 = 30;
const FUTURES_REST_URL: &str = "https://fapi.binance.com";

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

    let rest = RestClient::new(FUTURES_REST_URL);
    tracing::info!(
        "fetching futures exchange info for {} symbols",
        SYMBOLS.len()
    );

    let all_defs = rest
        .fetch_exchange_info(VenueId::BinanceFutures, MarketType::UsdmFutures, 0, 0)
        .await
        .context("fetch_exchange_info")?;

    let insts: Vec<_> = all_defs
        .into_iter()
        .filter(|d| SYMBOLS.contains(&d.symbol.as_str()))
        .collect();

    if insts.len() != SYMBOLS.len() {
        let found: Vec<&str> = insts.iter().map(|d| d.symbol.as_str()).collect();
        tracing::warn!(?found, "some requested symbols not found in exchange info");
    }

    // Partition instruments across shards.
    let mut shard_insts: Vec<Vec<_>> = vec![vec![]; TOTAL_SHARDS as usize];
    for inst in insts {
        let shard = shard_for_symbol(
            VenueId::BinanceFutures,
            MarketType::UsdmFutures,
            &inst.symbol,
            TOTAL_SHARDS,
        );
        shard_insts[shard as usize].push(inst);
    }

    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let mut join_set = tokio::task::JoinSet::new();

    for (shard_id, insts) in shard_insts.into_iter().enumerate() {
        if insts.is_empty() {
            continue;
        }

        let shard_id = shard_id as u32;
        let mut engine = FuturesShardEngine::new(shard_id);
        for inst in insts {
            engine.add_symbol(inst);
        }

        let rest_clone = rest.clone();
        let shutdown_shard = shutdown_rx.clone();
        let publisher = build_null(&[shard_id]);

        join_set.spawn(async move {
            run_shard(shard_id, engine, rest_clone, shutdown_shard, publisher).await;
        });
    }

    tracing::info!("all shards started — running for {RUN_SECS}s (Ctrl-C to stop early)");
    tokio::select! {
        biased;
        _ = tokio::signal::ctrl_c()       => { tracing::info!("Ctrl-C received"); }
        _ = tokio::time::sleep(Duration::from_secs(RUN_SECS)) => {}
    }
    let _ = shutdown_tx.send(true);

    while let Some(res) = join_set.join_next().await {
        if let Err(e) = res {
            tracing::error!("shard task panicked: {e}");
        }
    }
    tracing::info!("all shards stopped");
    Ok(())
}

// ---------------------------------------------------------------------------
// Per-shard pipeline
// ---------------------------------------------------------------------------

#[derive(Default)]
struct SymbolCounters {
    bbo: u64,
    book_deltas: u64,
    trades: u64,
    mark_prices: u64,
    funding_rates: u64,
    liquidations: u64,
    gaps: u64,
    recoveries: u64,
}

async fn run_shard(
    shard_id: u32,
    mut engine: FuturesShardEngine,
    rest: RestClient,
    mut shutdown: watch::Receiver<bool>,
    mut publisher: ShardedPublisher<NullPublication>,
) {
    let ctx = NormalizeCtx {
        venue_id: VenueId::BinanceFutures,
        market_type: MarketType::UsdmFutures,
        instance_id: 0,
        connection_id: shard_id,
    };

    // Build combined-stream URL for all symbols in this shard.
    let streams: Vec<String> = engine
        .symbol_names()
        .flat_map(|sym| {
            [
                FuturesStream::BookTicker.stream_name(sym),
                FuturesStream::Depth {
                    update_speed_ms: 100,
                }
                .stream_name(sym),
                FuturesStream::AggTrade.stream_name(sym),
                FuturesStream::MarkPrice {
                    update_interval_secs: 1,
                }
                .stream_name(sym),
                FuturesStream::ForceOrder.stream_name(sym),
            ]
        })
        .collect();

    let ws_url = build_url(FUTURES_WS_BASE, &streams);
    tracing::info!(shard_id, symbols = engine.symbol_count(), %ws_url, "shard connecting");

    let ws_config = WebSocketConfig {
        url: FUTURES_WS_BASE.to_string(),
        futures_url: FUTURES_WS_BASE.to_string(),
        api_key: None,
        ping_interval_secs: 20,
        max_streams_per_connection: 1024,
        reconnect_delay_ms: 500,
        forced_reconnect_secs: 86_400,
    };
    let mgr = ConnectionManager::new(ws_config);

    let (frame_tx, mut frame_rx) = mpsc::channel::<RawFrame>(4096);
    let shutdown_ws = shutdown.clone();
    let ws_url_clone = ws_url.clone();
    tokio::spawn(async move {
        mgr.run(
            &ws_url_clone,
            move |frame| {
                let _ = frame_tx.try_send(frame);
            },
            shutdown_ws,
        )
        .await;
    });

    let mut seq = 0u64;
    let mut heartbeater = Heartbeater::new();
    let mut encode_buf = vec![0u8; 64_000];
    let mut counters: HashMap<String, SymbolCounters> = engine
        .symbol_names()
        .map(|s| (s.to_string(), SymbolCounters::default()))
        .collect();

    set_feed_state(
        shard_id,
        FeedState::Connecting,
        &ctx,
        &mut seq,
        now_nanos(),
        &mut encode_buf,
        &mut publisher,
    );

    loop {
        tokio::select! {
            biased;

            _ = shutdown.changed() => {
                if *shutdown.borrow() { break; }
            }

            maybe_frame = frame_rx.recv() => {
                let frame = match maybe_frame {
                    Some(f) => f,
                    None    => break,
                };

                // ── parse ─────────────────────────────────────────────────
                let event = match parse_futures_message(&frame.payload) {
                    Ok(e)  => e,
                    Err(e) => {
                        tracing::warn!(shard_id, "parse error: {e}");
                        continue;
                    }
                };

                // ── route by symbol ───────────────────────────────────────
                let Some(sym_str) = symbol_from_futures_event(&event) else { continue };
                if !engine.contains_symbol(sym_str) { continue; }
                let sym = sym_str.to_string();

                // ── normalize ─────────────────────────────────────────────
                let msgs = {
                    let state = engine.get_mut(&sym).unwrap();
                    match normalize_futures_event(&event, &state.inst, &ctx, &mut seq, frame.recv_ts) {
                        Ok(v)  => v,
                        Err(e) => { tracing::warn!(shard_id, %sym, "normalize error: {e}"); continue; }
                    }
                };

                // ── dispatch each normalized message ──────────────────────
                for msg in msgs {
                    let now = frame.recv_ts;
                    match &msg {
                        NormalizedMessage::BookDelta(bd) => {
                            let bd = bd.clone();
                            let ctr = counters.get_mut(&sym).unwrap();

                            // Encode the delta now (needed for recovery_buf push).
                            let encoded_size = bd.encode_into(&mut encode_buf)
                                .unwrap_or(0);

                            // If stale: buffer for replay.
                            if engine.get(&sym).unwrap().is_stale() {
                                let state = engine.get_mut(&sym).unwrap();
                                match state.recovery_buf.push(bd, now, encoded_size) {
                                    PushResult::Accepted => {}
                                    PushResult::Overflow(reason) => {
                                        let why = match reason {
                                            OverflowReason::Age        => BookStaleReason::StaleTimeout,
                                            OverflowReason::EventCount => BookStaleReason::BufferOverflow,
                                            OverflowReason::ByteSize   => BookStaleReason::BufferOverflow,
                                        };
                                        state.recovery_buf.clear();
                                        publish_book_stale(shard_id, &state.inst, why, &ctx,
                                            &mut seq, now, &mut encode_buf, &mut publisher);
                                    }
                                }
                                continue;
                            }

                            let validate_result = {
                                let state = engine.get_mut(&sym).unwrap();
                                match state.validator.state() {
                                    ValidationState::AwaitingSnapshot => {
                                        let push = state.recovery_buf.push(bd, now, encoded_size);
                                        if let PushResult::Overflow(_) = push {
                                            state.recovery_buf.clear();
                                        }
                                        continue;
                                    }
                                    _ => state.validator.validate(
                                        bd.first_update_id,
                                        bd.final_update_id,
                                        bd.prev_update_id,
                                    ),
                                }
                            };

                            match validate_result {
                                ValidateResult::Apply => {
                                    let state = engine.get_mut(&sym).unwrap();
                                    state.book.apply_delta(&bd);
                                    ctr.book_deltas += 1;
                                    // Re-encode and publish with updated local_publish_ts.
                                    if let Ok(n) = bd.encode_into(&mut encode_buf) {
                                        let _ = publisher.offer(shard_id, &encode_buf[..n]);
                                    }
                                }
                                ValidateResult::Discard => {}
                                ValidateResult::Buffering => {
                                    let state = engine.get_mut(&sym).unwrap();
                                    let _ = state.recovery_buf.push(bd, now, encoded_size);
                                }
                                ValidateResult::Gap { expected_pu, actual_pu, last_valid } => {
                                    ctr.gaps += 1;
                                    tracing::warn!(
                                        shard_id, %sym,
                                        expected_pu, actual_pu, last_valid,
                                        "depth sequence gap — starting recovery",
                                    );

                                    let (inst, circuit_ok) = {
                                        let state = engine.get_mut(&sym).unwrap();
                                        state.book.mark_stale(BookStaleReason::SequenceGap);
                                        publish_gap_detected(shard_id, &state.inst, expected_pu,
                                            actual_pu, last_valid, &ctx, &mut seq, now,
                                            &mut encode_buf, &mut publisher);
                                        publish_book_stale(shard_id, &state.inst,
                                            BookStaleReason::SequenceGap, &ctx, &mut seq, now,
                                            &mut encode_buf, &mut publisher);
                                        state.feed_state = FeedState::Recovering;
                                        let ok = !state.circuit.check(now).is_open();
                                        if !ok { let _ = state.circuit.record_failure(now); }
                                        (state.inst.clone(), ok)
                                    }; // borrow released before await

                                    if circuit_ok {
                                        let snap = rest.fetch_futures_depth_snapshot(&inst, now).await;
                                        match snap {
                                            Ok(snapshot) => {
                                                let state = engine.get_mut(&sym).unwrap();
                                                match apply_futures_snapshot(
                                                    &snapshot,
                                                    &mut state.book,
                                                    &mut state.validator,
                                                    &mut state.recovery_buf,
                                                ) {
                                                    Ok(outcome) => {
                                                        state.circuit.record_success();
                                                        state.feed_state = FeedState::Live;
                                                        ctr.recoveries += 1;
                                                        tracing::info!(
                                                            shard_id, %sym,
                                                            snapshot_id = outcome.snapshot_id,
                                                            replayed = outcome.replayed,
                                                            "recovery complete",
                                                        );
                                                        publish_book_recovered(shard_id, &state.inst,
                                                            outcome.snapshot_id, &ctx, &mut seq, now,
                                                            &mut encode_buf, &mut publisher);
                                                    }
                                                    Err(e) => {
                                                        tracing::error!(shard_id, %sym, "recovery apply failed: {e}");
                                                        let _ = state.circuit.record_failure(now);
                                                    }
                                                }
                                            }
                                            Err(e) => {
                                                tracing::error!(shard_id, %sym, "snapshot fetch failed: {e}");
                                                let state = engine.get_mut(&sym).unwrap();
                                                let _ = state.circuit.record_failure(now);
                                            }
                                        }
                                    }
                                }
                            }
                        }

                        NormalizedMessage::BestBidOffer(_) => {
                            counters.get_mut(&sym).unwrap().bbo += 1;
                            if let Ok(n) = msg.encode_into(&mut encode_buf) {
                                let _ = publisher.offer(shard_id, &encode_buf[..n]);
                            }
                        }

                        NormalizedMessage::Trade(_) => {
                            counters.get_mut(&sym).unwrap().trades += 1;
                            if let Ok(n) = msg.encode_into(&mut encode_buf) {
                                let _ = publisher.offer(shard_id, &encode_buf[..n]);
                            }
                        }

                        NormalizedMessage::MarkPrice(_) => {
                            counters.get_mut(&sym).unwrap().mark_prices += 1;
                            if let Ok(n) = msg.encode_into(&mut encode_buf) {
                                let _ = publisher.offer(shard_id, &encode_buf[..n]);
                            }
                        }

                        NormalizedMessage::FundingRate(_) => {
                            counters.get_mut(&sym).unwrap().funding_rates += 1;
                            if let Ok(n) = msg.encode_into(&mut encode_buf) {
                                let _ = publisher.offer(shard_id, &encode_buf[..n]);
                            }
                        }

                        NormalizedMessage::Liquidation(_) => {
                            counters.get_mut(&sym).unwrap().liquidations += 1;
                            if let Ok(n) = msg.encode_into(&mut encode_buf) {
                                let _ = publisher.offer(shard_id, &encode_buf[..n]);
                            }
                        }

                        _ => {}
                    }

                    // Heartbeat check on every frame.
                    let now = now_nanos();
                    if heartbeater.is_due(now) {
                        let hb = Heartbeat {
                            header: shard_hdr(MessageType::Heartbeat, &ctx, &mut seq, now),
                        };
                        if let Ok(n) = hb.encode_into(&mut encode_buf) {
                            let _ = publisher.offer(shard_id, &encode_buf[..n]);
                        }
                        heartbeater.record_beat(now);
                        tracing::debug!(shard_id, "heartbeat published");
                    }
                }
            }
        }
    }

    // Print per-symbol stats.
    let mut syms: Vec<&str> = counters.keys().map(|s| s.as_str()).collect();
    syms.sort_unstable();
    for sym in syms {
        let c = &counters[sym];
        tracing::info!(
            shard_id,
            sym,
            bbo = c.bbo,
            book_deltas = c.book_deltas,
            trades = c.trades,
            mark_prices = c.mark_prices,
            funding_rates = c.funding_rates,
            liquidations = c.liquidations,
            gaps = c.gaps,
            recoveries = c.recoveries,
            "symbol stats",
        );
    }
    tracing::info!(shard_id, "shard stopped");
}

// ---------------------------------------------------------------------------
// Symbol routing
// ---------------------------------------------------------------------------

fn symbol_from_futures_event(event: &FuturesEvent) -> Option<&str> {
    match event {
        FuturesEvent::BookTicker(bt) => Some(&bt.symbol),
        FuturesEvent::DepthUpdate(du) => Some(&du.symbol),
        FuturesEvent::AggTrade(at) => Some(&at.symbol),
        FuturesEvent::MarkPrice(mp) => Some(&mp.symbol),
        FuturesEvent::ForceOrder(fo) => Some(&fo.order.symbol),
        FuturesEvent::Unknown(_) => None,
    }
}

// ---------------------------------------------------------------------------
// Message helpers
// ---------------------------------------------------------------------------

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

fn inst_hdr(
    msg_type: MessageType,
    inst: &connector_core::InstrumentDefinition,
    ctx: &NormalizeCtx,
    seq: &mut u64,
    recv_ts: i64,
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
        local_recv_ts: recv_ts,
        local_publish_ts: recv_ts,
    }
}

fn set_feed_state(
    shard_id: u32,
    state: FeedState,
    ctx: &NormalizeCtx,
    seq: &mut u64,
    ts: i64,
    buf: &mut [u8],
    pub_: &mut ShardedPublisher<NullPublication>,
) {
    let fs = FeedStatus {
        header: shard_hdr(MessageType::FeedStatus, ctx, seq, ts),
        state,
    };
    if let Ok(n) = fs.encode_into(buf) {
        let _ = pub_.offer(shard_id, &buf[..n]);
    }
}

#[allow(clippy::too_many_arguments)]
fn publish_gap_detected(
    shard_id: u32,
    inst: &connector_core::InstrumentDefinition,
    expected: u64,
    actual: u64,
    _last_valid: u64,
    ctx: &NormalizeCtx,
    seq: &mut u64,
    ts: i64,
    buf: &mut [u8],
    pub_: &mut ShardedPublisher<NullPublication>,
) {
    let gd = GapDetected {
        header: inst_hdr(MessageType::GapDetected, inst, ctx, seq, ts),
        symbol: inst.symbol.clone(),
        expected_update_id: expected,
        received_update_id: actual,
    };
    if let Ok(n) = gd.encode_into(buf) {
        let _ = pub_.offer(shard_id, &buf[..n]);
    }
}

#[allow(clippy::too_many_arguments)]
fn publish_book_stale(
    shard_id: u32,
    inst: &connector_core::InstrumentDefinition,
    reason: BookStaleReason,
    ctx: &NormalizeCtx,
    seq: &mut u64,
    ts: i64,
    buf: &mut [u8],
    pub_: &mut ShardedPublisher<NullPublication>,
) {
    let bs = BookStale {
        header: inst_hdr(MessageType::BookStale, inst, ctx, seq, ts),
        symbol: inst.symbol.clone(),
        reason,
    };
    if let Ok(n) = bs.encode_into(buf) {
        let _ = pub_.offer(shard_id, &buf[..n]);
    }
}

#[allow(clippy::too_many_arguments)]
fn publish_book_recovered(
    shard_id: u32,
    inst: &connector_core::InstrumentDefinition,
    snapshot_id: u64,
    ctx: &NormalizeCtx,
    seq: &mut u64,
    ts: i64,
    buf: &mut [u8],
    pub_: &mut ShardedPublisher<NullPublication>,
) {
    let br = BookRecovered {
        header: inst_hdr(MessageType::BookRecovered, inst, ctx, seq, ts),
        symbol: inst.symbol.clone(),
        snapshot_update_id: snapshot_id,
    };
    if let Ok(n) = br.encode_into(buf) {
        let _ = pub_.offer(shard_id, &buf[..n]);
    }
}

fn now_nanos() -> i64 {
    SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos() as i64
}
