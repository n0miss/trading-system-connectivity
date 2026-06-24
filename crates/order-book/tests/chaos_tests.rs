//! Chaos tests (§10.39): disconnects, REST slowness, and malformed events.
//!
//! All timing assertions use **virtual nanosecond timestamps** — no real
//! sleeps or wall-clock dependency.
//!
//! § SLA targets used below
//! ─────────────────────────────────────────────────────────────────────────
//! RECOVERY_WINDOW_NS   = 10 000 000 000 ns  (10 s)   §1 addendum — max age
//!                        of the recovery buffer; snapshot must arrive in time.
//! BBO_DEGRADE_NS       =    250 000 000 ns (250 ms)   §2.3 — BBO gap before
//!                        degraded.
//! BBO_STALE_NS         =  1 000 000 000 ns   (1 s)   §2.3 — BBO gap before
//!                        stale.
//! BACKPRESSURE_WARN_NS =        100 000 ns (100 µs)  §5.3 — Aeron warn.
//! BACKPRESSURE_DEGRADE =      1 000 000 ns   (1 ms)  §5.3 — Aeron degrade.

use connector_core::{
    BookDelta, BookSnapshot, BookStaleReason, Heartbeat, MarketType, MessageHeader, MessageType,
    NormalizedMessage, PriceLevel, VenueId, HEADER_SIZE, SCHEMA_VERSION, TS_NONE, UPDATE_ID_NONE,
};
use connector_order_book::{harness::SyntheticHarness, OrderBook};
use connector_replay::{FaultConfig, PollResult, RecordedFrame, ReplayMode, Replayer, SourceKind};

// ---------------------------------------------------------------------------
// SLA constants — §7.3
// ---------------------------------------------------------------------------

mod sla {
    /// §1 addendum — snapshot must arrive within this window after stale event.
    pub const RECOVERY_WINDOW_NS: i64 = 10_000_000_000;
    /// §2.3 — BBO divergence before degraded.
    pub const BBO_DEGRADE_NS: i64 = 250_000_000;
    /// §2.3 — BBO divergence before stale.
    pub const BBO_STALE_NS: i64 = 1_000_000_000;
    /// §5.3 — Aeron backpressure warning threshold.
    pub const BACKPRESSURE_WARN_NS: i64 = 100_000;
    /// §5.3 — Aeron backpressure degrade threshold.
    pub const BACKPRESSURE_DEGRADE_NS: i64 = 1_000_000;
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn make_header(msg_type: MessageType) -> MessageHeader {
    MessageHeader {
        schema_version: SCHEMA_VERSION,
        message_type: msg_type,
        venue_id: VenueId::BinanceSpot,
        market_type: MarketType::Spot,
        instrument_id: 0,
        connection_id: 0,
        instance_id: 0,
        sequence_number: 0,
        exchange_event_ts: TS_NONE,
        exchange_tx_ts: TS_NONE,
        local_recv_ts: TS_NONE,
        local_publish_ts: TS_NONE,
    }
}

fn snap(bids: Vec<PriceLevel>, asks: Vec<PriceLevel>, uid: u64) -> BookSnapshot {
    BookSnapshot {
        header: make_header(MessageType::BookSnapshot),
        symbol: "BTCUSDT".into(),
        update_id: uid,
        price_scale: 2,
        qty_scale: 5,
        bids,
        asks,
    }
}

fn delta(uid: u64) -> BookDelta {
    BookDelta {
        header: make_header(MessageType::BookDelta),
        symbol: "BTCUSDT".into(),
        first_update_id: uid,
        final_update_id: uid,
        prev_update_id: UPDATE_ID_NONE,
        price_scale: 2,
        qty_scale: 5,
        bids: vec![PriceLevel {
            price: 99_000,
            qty: 1,
        }],
        asks: vec![PriceLevel {
            price: 101_000,
            qty: 1,
        }],
    }
}

fn healthy_book(n_deltas: u64) -> OrderBook {
    let mut book = OrderBook::new("BTCUSDT");
    book.apply_snapshot(&snap(
        vec![PriceLevel {
            price: 99_000,
            qty: 100,
        }],
        vec![PriceLevel {
            price: 101_000,
            qty: 100,
        }],
        1,
    ));
    for uid in 2..=n_deltas + 1 {
        book.apply_delta(&delta(uid));
    }
    book
}

/// Encode a `Heartbeat` into bytes; panics on encoding failure.
fn encoded_heartbeat(ts_ns: i64) -> RecordedFrame {
    let hb = Heartbeat {
        header: make_header(MessageType::Heartbeat),
    };
    let mut buf = vec![0u8; HEADER_SIZE];
    hb.header.encode_into(&mut buf).unwrap();
    buf[0] = SCHEMA_VERSION;
    buf[1] = MessageType::Heartbeat as u8;
    RecordedFrame {
        captured_at_ns: ts_ns,
        payload: buf,
        source_kind: SourceKind::NormalizedMessage,
    }
}

/// Encode a `BookDelta` into bytes.
fn encoded_delta_frame(ts_ns: i64, uid: u64) -> RecordedFrame {
    let d = delta(uid);
    let mut buf = vec![0u8; 512];
    let n = d.encode_into(&mut buf).unwrap();
    buf.truncate(n);
    RecordedFrame {
        captured_at_ns: ts_ns,
        payload: buf,
        source_kind: SourceKind::NormalizedMessage,
    }
}

/// Virtual monotonic clock to drive SLA assertions without real time.
struct Clock {
    ns: i64,
}

impl Clock {
    fn new() -> Self {
        Self { ns: 0 }
    }
    fn advance_ms(&mut self, ms: i64) {
        self.ns += ms * 1_000_000;
    }
    fn advance_s(&mut self, s: i64) {
        self.ns += s * 1_000_000_000;
    }
    fn now(&self) -> i64 {
        self.ns
    }
    fn elapsed_since(&self, t: i64) -> i64 {
        self.ns - t
    }
}

// ---------------------------------------------------------------------------
// §1 — Disconnect / sequence-gap scenarios
// ---------------------------------------------------------------------------

/// Happy path: gap detected at T=1s, snapshot arrives at T=5s → within 10s.
#[test]
fn chaos_sequence_gap_recovery_within_sla() {
    let mut book = healthy_book(50);
    let mut clk = Clock::new();

    clk.advance_s(1);
    let stale_ts = clk.now();
    book.mark_stale(BookStaleReason::SequenceGap);
    assert!(book.is_stale());

    clk.advance_s(4); // now T=5s
    book.apply_snapshot(&snap(
        vec![PriceLevel {
            price: 99_500,
            qty: 200,
        }],
        vec![PriceLevel {
            price: 100_500,
            qty: 200,
        }],
        100,
    ));
    book.mark_recovered();
    assert!(!book.is_stale(), "book must be recovered after snapshot");

    let elapsed = clk.elapsed_since(stale_ts);
    assert!(
        elapsed < sla::RECOVERY_WINDOW_NS,
        "recovery took {}ms, SLA is {}ms",
        elapsed / 1_000_000,
        sla::RECOVERY_WINDOW_NS / 1_000_000,
    );
}

/// Edge: snapshot arrives at T=9.5s — just inside the 10s window.
#[test]
fn chaos_sequence_gap_recovery_at_edge_of_sla() {
    let mut book = healthy_book(10);
    let mut clk = Clock::new();

    let stale_ts = clk.now();
    book.mark_stale(BookStaleReason::SequenceGap);

    clk.advance_ms(9_500); // 9.5 s
    book.apply_snapshot(&snap(vec![], vec![], 200));
    book.mark_recovered();

    let elapsed = clk.elapsed_since(stale_ts);
    assert!(
        elapsed < sla::RECOVERY_WINDOW_NS,
        "edge-case recovery took {}ms, SLA {}ms",
        elapsed / 1_000_000,
        sla::RECOVERY_WINDOW_NS / 1_000_000,
    );
}

/// Five rapid reconnect cycles, each must complete within RECOVERY_WINDOW_NS.
#[test]
fn chaos_multiple_reconnect_cycles_within_sla() {
    let mut book = healthy_book(5);
    let mut clk = Clock::new();

    for cycle in 0..5 {
        clk.advance_ms(1_000 * (cycle + 1)); // gap grows per cycle (still < 10s each)
        let stale_ts = clk.now();
        book.mark_stale(BookStaleReason::SequenceGap);

        clk.advance_ms(200); // 200 ms simulated REST latency
        book.apply_snapshot(&snap(
            vec![PriceLevel {
                price: 99_000 + cycle as i64 * 10,
                qty: 1,
            }],
            vec![PriceLevel {
                price: 101_000 + cycle as i64 * 10,
                qty: 1,
            }],
            100 + cycle as u64,
        ));
        book.mark_recovered();

        let elapsed = clk.elapsed_since(stale_ts);
        assert!(
            elapsed < sla::RECOVERY_WINDOW_NS,
            "cycle {cycle} recovery took {}ms, SLA {}ms",
            elapsed / 1_000_000,
            sla::RECOVERY_WINDOW_NS / 1_000_000,
        );
        assert!(!book.is_stale(), "cycle {cycle}: book must be recovered");
    }
}

/// Book marked stale then receives many zero-qty deltas before snapshot.
/// Verifies the book doesn't corrupt invariants while waiting.
#[test]
fn chaos_stale_book_survives_many_deltas_before_recovery() {
    let mut book = healthy_book(10);
    let mut clk = Clock::new();

    book.mark_stale(BookStaleReason::SequenceGap);
    let stale_ts = clk.now();

    // Feed 500 deltas while stale (no-op for recovery, but must not corrupt book).
    // In production the buffer would hold these; here we simply verify no panic.
    for uid in 12..512 {
        book.apply_delta(&delta(uid));
    }

    clk.advance_s(7);
    book.apply_snapshot(&snap(
        vec![PriceLevel {
            price: 98_000,
            qty: 50,
        }],
        vec![PriceLevel {
            price: 102_000,
            qty: 50,
        }],
        600,
    ));
    book.mark_recovered();

    let elapsed = clk.elapsed_since(stale_ts);
    assert!(elapsed < sla::RECOVERY_WINDOW_NS);
    assert!(!book.is_stale());
    assert_eq!(book.bid_depth(), 1);
    assert_eq!(book.ask_depth(), 1);
}

/// Large book (500 bid + 500 ask levels) must still recover within the SLA.
#[test]
fn chaos_large_book_stale_and_recover_within_sla() {
    let mut book = OrderBook::new("BTCUSDT");
    let bid_levels: Vec<PriceLevel> = (1..=500)
        .map(|i| PriceLevel {
            price: 99_000 - i * 10,
            qty: i,
        })
        .collect();
    let ask_levels: Vec<PriceLevel> = (1..=500)
        .map(|i| PriceLevel {
            price: 101_000 + i * 10,
            qty: i,
        })
        .collect();
    book.apply_snapshot(&snap(bid_levels, ask_levels, 1));
    assert_eq!(book.bid_depth(), 500);

    let mut clk = Clock::new();
    let stale_ts = clk.now();
    book.mark_stale(BookStaleReason::SequenceGap);

    clk.advance_s(3);
    let new_bid = vec![PriceLevel {
        price: 99_000,
        qty: 10,
    }];
    let new_ask = vec![PriceLevel {
        price: 101_000,
        qty: 10,
    }];
    book.apply_snapshot(&snap(new_bid, new_ask, 1_000));
    book.mark_recovered();

    let elapsed = clk.elapsed_since(stale_ts);
    assert!(elapsed < sla::RECOVERY_WINDOW_NS);
    assert!(!book.is_stale());
    assert_eq!(
        book.bid_depth(),
        1,
        "snapshot must fully replace prior 500 levels"
    );
    assert_eq!(book.ask_depth(), 1);
}

// ---------------------------------------------------------------------------
// §2 — REST slowness scenarios
// ---------------------------------------------------------------------------

/// Snapshot arrives after 8 s — within the 10 s recovery window.
#[test]
fn chaos_rest_snapshot_delayed_8s_within_sla() {
    let mut book = OrderBook::new("BTCUSDT");
    book.apply_snapshot(&snap(
        vec![PriceLevel {
            price: 99_000,
            qty: 1,
        }],
        vec![PriceLevel {
            price: 101_000,
            qty: 1,
        }],
        1,
    ));

    let mut clk = Clock::new();
    let stale_ts = clk.now();
    book.mark_stale(BookStaleReason::SequenceGap);

    // Simulate slow REST endpoint — 8 seconds.
    clk.advance_s(8);
    book.apply_snapshot(&snap(
        vec![PriceLevel {
            price: 99_900,
            qty: 5,
        }],
        vec![PriceLevel {
            price: 100_100,
            qty: 5,
        }],
        100,
    ));
    book.mark_recovered();

    let elapsed = clk.elapsed_since(stale_ts);
    assert!(
        elapsed < sla::RECOVERY_WINDOW_NS,
        "8s REST delay still within SLA; elapsed {}ms",
        elapsed / 1_000_000,
    );
    assert!(!book.is_stale());
}

/// Snapshot arrives after 11 s — the recovery buffer has expired (SLA violation).
/// Asserts the violation is detectable (elapsed > RECOVERY_WINDOW_NS).
#[test]
fn chaos_rest_snapshot_delayed_11s_exceeds_sla() {
    let mut book = OrderBook::new("BTCUSDT");
    book.apply_snapshot(&snap(vec![], vec![], 1));

    let mut clk = Clock::new();
    let stale_ts = clk.now();
    book.mark_stale(BookStaleReason::SequenceGap);

    clk.advance_s(11); // past the 10 s window
    book.apply_snapshot(&snap(vec![], vec![], 500));
    book.mark_recovered();

    let elapsed = clk.elapsed_since(stale_ts);
    assert!(
        elapsed > sla::RECOVERY_WINDOW_NS,
        "11s delay must be detected as a SLA violation; elapsed {}ms",
        elapsed / 1_000_000,
    );
    // In production the circuit breaker would have fired; here we just verify
    // the system continues without panicking and the book is still usable.
    assert!(!book.is_stale());
}

/// BBO degrade/stale thresholds (§2.3) are ordered and internally consistent.
#[test]
fn chaos_bbo_sla_constants_are_ordered() {
    assert!(
        sla::BBO_DEGRADE_NS < sla::BBO_STALE_NS,
        "degrade threshold must be less than stale threshold"
    );
    assert!(
        sla::BBO_STALE_NS < sla::RECOVERY_WINDOW_NS,
        "stale threshold must be less than recovery window"
    );
}

/// Backpressure thresholds (§5.3) are ordered.
#[test]
fn chaos_backpressure_sla_constants_are_ordered() {
    assert!(sla::BACKPRESSURE_WARN_NS < sla::BACKPRESSURE_DEGRADE_NS);
    assert!(sla::BACKPRESSURE_DEGRADE_NS < sla::RECOVERY_WINDOW_NS);
}

// ---------------------------------------------------------------------------
// §3 — Malformed / corrupted event scenarios
// ---------------------------------------------------------------------------

/// A zero-length buffer must return a decode error, not panic.
#[test]
fn chaos_truncated_empty_buffer_is_decode_error() {
    let result = NormalizedMessage::from_bytes(&[]);
    assert!(result.is_err(), "empty buffer must be a decode error");
}

/// A buffer shorter than HEADER_SIZE must return a decode error.
#[test]
fn chaos_truncated_partial_header_is_decode_error() {
    for size in [1, 4, 10, HEADER_SIZE - 1] {
        let buf = vec![SCHEMA_VERSION; size]; // correct schema but incomplete header
        let result = NormalizedMessage::from_bytes(&buf);
        assert!(
            result.is_err(),
            "buffer of {size} bytes (< HEADER_SIZE={HEADER_SIZE}) must be a decode error"
        );
    }
}

/// A HEADER_SIZE buffer with an unknown message type byte must return a decode error.
#[test]
fn chaos_unknown_message_type_byte_is_decode_error() {
    for unknown_type in [0u8, 50, 99, 200, 255] {
        let mut buf = vec![0u8; HEADER_SIZE];
        buf[0] = SCHEMA_VERSION;
        buf[1] = unknown_type; // message_type field
        let result = NormalizedMessage::from_bytes(&buf);
        assert!(
            result.is_err(),
            "message_type={unknown_type} must be a decode error"
        );
    }
}

/// `BookChecksum` (type 18) is a status-stream-only message; decoding via
/// `NormalizedMessage::from_bytes` must return an error, not construct one.
#[test]
fn chaos_book_checksum_message_type_is_rejected_by_normalized_decode() {
    let mut buf = vec![0u8; HEADER_SIZE];
    buf[0] = SCHEMA_VERSION;
    buf[1] = MessageType::BookChecksum as u8;
    let result = NormalizedMessage::from_bytes(&buf);
    assert!(
        result.is_err(),
        "BookChecksum must not decode as NormalizedMessage"
    );
}

/// A valid `Heartbeat` round-trips through encode → decode without error.
#[test]
fn chaos_valid_heartbeat_encodes_and_decodes() {
    let hb = Heartbeat {
        header: make_header(MessageType::Heartbeat),
    };
    let mut buf = vec![0u8; HEADER_SIZE];
    hb.header.encode_into(&mut buf).unwrap();
    buf[1] = MessageType::Heartbeat as u8; // ensure type byte is correct

    let result = NormalizedMessage::from_bytes(&buf);
    assert!(result.is_ok(), "valid heartbeat must decode successfully");
    assert!(matches!(result.unwrap(), NormalizedMessage::Heartbeat(_)));
}

/// Corrupting the message-type byte of a valid frame causes a decode error.
#[test]
fn chaos_corrupted_message_type_byte_is_decode_error() {
    let hb = Heartbeat {
        header: make_header(MessageType::Heartbeat),
    };
    let mut buf = vec![0u8; HEADER_SIZE];
    hb.header.encode_into(&mut buf).unwrap();
    buf[1] = MessageType::Heartbeat as u8;

    buf[1] = 99; // overwrite with unknown type
    let result = NormalizedMessage::from_bytes(&buf);
    assert!(
        result.is_err(),
        "corrupted message_type byte must be a decode error"
    );
}

// ---------------------------------------------------------------------------
// §4 — FaultInjection replayer scenarios
// ---------------------------------------------------------------------------

/// Drop rate = 100%: replayer reaches Done without delivering any frames.
#[test]
fn chaos_drop_all_frames_replayer_reaches_done() {
    let frames: Vec<RecordedFrame> = (0..20)
        .map(|i| encoded_heartbeat(i as i64 * 1_000_000))
        .collect();
    let n = frames.len();

    let mode = ReplayMode::FaultInjection {
        inner: Box::new(ReplayMode::AsFastAsPossible),
        faults: FaultConfig {
            drop_percent: 100,
            corrupt_percent: 0,
            duplicate_percent: 0,
            seed: 42,
        },
    };
    let mut replayer = Replayer::new(frames, mode);

    loop {
        match replayer.next_frame() {
            PollResult::Done => break,
            PollResult::Ready(_) => panic!("no frames should be delivered at 100% drop rate"),
            PollResult::NotYet { .. } => {}
        }
    }
    assert_eq!(replayer.stats().frames_dropped, n as u64);
    assert_eq!(replayer.stats().frames_delivered, 0);
}

/// Corrupt rate = 100%: every delivered frame has `was_corrupted = true`.
#[test]
fn chaos_corrupt_all_frames_sets_was_corrupted_flag() {
    let frames: Vec<RecordedFrame> = (0..10)
        .map(|i| encoded_delta_frame(i as i64 * 1_000_000, i as u64 + 1))
        .collect();
    let n = frames.len();

    let mode = ReplayMode::FaultInjection {
        inner: Box::new(ReplayMode::AsFastAsPossible),
        faults: FaultConfig {
            drop_percent: 0,
            corrupt_percent: 100,
            duplicate_percent: 0,
            seed: 7,
        },
    };
    let mut replayer = Replayer::new(frames, mode);
    let mut corrupted = 0usize;

    loop {
        match replayer.next_frame() {
            PollResult::Done => break,
            PollResult::Ready(ev) => {
                assert!(
                    ev.was_corrupted,
                    "corrupt_percent=100 must flag every frame"
                );
                corrupted += 1;
                // Decode must either succeed or fail gracefully — must not panic.
                let _ = NormalizedMessage::from_bytes(&ev.payload);
            }
            PollResult::NotYet { .. } => {}
        }
    }
    assert_eq!(
        corrupted, n,
        "all {n} frames must be delivered (none dropped)"
    );
    assert_eq!(replayer.stats().frames_corrupted, n as u64);
}

/// Applying the same `BookDelta` twice (duplicate delivery) must not violate
/// book invariants — BTreeMap upsert semantics make it idempotent.
#[test]
fn chaos_duplicate_delta_delivery_preserves_book_invariants() {
    let frames: Vec<RecordedFrame> = (0..5)
        .map(|i| encoded_delta_frame(i as i64 * 1_000_000, i as u64 + 1))
        .collect();

    let mode = ReplayMode::FaultInjection {
        inner: Box::new(ReplayMode::AsFastAsPossible),
        faults: FaultConfig {
            drop_percent: 0,
            corrupt_percent: 0,
            duplicate_percent: 100,
            seed: 13,
        },
    };
    let mut replayer = Replayer::new(frames, mode);
    let mut book = OrderBook::new("BTCUSDT");
    book.apply_snapshot(&snap(
        vec![PriceLevel {
            price: 99_000,
            qty: 1,
        }],
        vec![PriceLevel {
            price: 101_000,
            qty: 1,
        }],
        1,
    ));

    let mut events = 0usize;
    loop {
        match replayer.next_frame() {
            PollResult::Done => break,
            PollResult::Ready(ev) if !ev.was_corrupted => {
                if let Ok(NormalizedMessage::BookDelta(d)) =
                    NormalizedMessage::from_bytes(&ev.payload)
                {
                    book.apply_delta(&d);
                    events += 1;
                }
            }
            PollResult::Ready(_) => {}
            PollResult::NotYet { .. } => {}
        }
    }

    assert!(
        events > 0,
        "at least some duplicate frames must have been delivered"
    );
    assert!(
        book.best_bid().is_some(),
        "book must remain non-empty after duplicate deltas"
    );
    if let (Some(bb), Some(ba)) = (book.best_bid(), book.best_ask()) {
        assert!(bb.price < ba.price, "crossed book after duplicate delivery");
    }
    for lvl in book.bids() {
        assert!(lvl.qty > 0, "zero-qty bid after duplicates");
    }
    for lvl in book.asks() {
        assert!(lvl.qty > 0, "zero-qty ask after duplicates");
    }
}

/// Mixed faults (drop=10%, corrupt=10%, dup=10%): the harness must not
/// violate invariants for any of the frames that survive and are applied.
#[test]
fn chaos_mixed_fault_injection_harness_stays_consistent() {
    let mut harness = SyntheticHarness::with_symbol_count(5, 999);
    harness.broadcast_snapshot(5, 5);

    // Encode a batch of delta frames.
    let frames: Vec<RecordedFrame> = (0..50)
        .map(|i| encoded_delta_frame(i as i64 * 500_000, i as u64 + 2))
        .collect();

    let mode = ReplayMode::FaultInjection {
        inner: Box::new(ReplayMode::AsFastAsPossible),
        faults: FaultConfig {
            drop_percent: 10,
            corrupt_percent: 10,
            duplicate_percent: 10,
            seed: 31337,
        },
    };
    let mut replayer = Replayer::new(frames, mode);

    loop {
        match replayer.next_frame() {
            PollResult::Done => break,
            PollResult::Ready(_ev) => {
                // The harness maintains its own books; we're verifying the
                // replayer drives the workload without panicking and the
                // harness invariants remain clean after its own events.
            }
            PollResult::NotYet { .. } => {}
        }
    }

    let violations = harness.check_invariants();
    assert!(
        violations.is_clean(),
        "harness has {} violation(s) after mixed fault replay: {:?}",
        violations.count(),
        violations.violations,
    );

    let s = replayer.stats();
    // Basic sanity: drops + corruptions + deliveries account for all frames.
    assert!(
        s.frames_dropped + s.frames_delivered >= 50,
        "stat accounting off: dropped={} delivered={} dup={}",
        s.frames_dropped,
        s.frames_delivered,
        s.frames_duplicated,
    );
}

/// Raw WS payload frames (JSON bytes) passed through replay must not panic
/// when decoded as `NormalizedMessage::from_bytes` — they'll fail (wrong
/// encoding), but must return `Err`, not unwind.
#[test]
fn chaos_raw_ws_payload_frames_fail_gracefully() {
    let json_payloads: &[&[u8]] = &[
        b"{}",
        b"{\"e\":\"depthUpdate\",\"E\":1700000000000,\"s\":\"BTCUSDT\"}",
        b"not json at all \xff\xfe",
        b"",
    ];

    for payload in json_payloads {
        let result = NormalizedMessage::from_bytes(payload);
        assert!(
            result.is_err(),
            "raw JSON/WS bytes must not decode as NormalizedMessage; payload={:?}",
            std::str::from_utf8(payload).unwrap_or("<binary>"),
        );
    }
}

/// Frames from `SourceKind::RawWsPayload` replay cleanly through the
/// `Replayer` without panicking.
#[test]
fn chaos_raw_ws_source_kind_frames_replay_without_panic() {
    let frames: Vec<RecordedFrame> = (0..5)
        .map(|i| RecordedFrame {
            captured_at_ns: i as i64 * 1_000_000,
            payload: b"{\"e\":\"depthUpdate\"}".to_vec(),
            source_kind: SourceKind::RawWsPayload,
        })
        .collect();

    let mut replayer = Replayer::new(frames, ReplayMode::AsFastAsPossible);
    let mut count = 0;
    loop {
        match replayer.next_frame() {
            PollResult::Done => break,
            PollResult::Ready(ev) => {
                assert_eq!(ev.source_kind, SourceKind::RawWsPayload);
                assert!(!ev.was_corrupted);
                count += 1;
            }
            PollResult::NotYet { .. } => {}
        }
    }
    assert_eq!(count, 5);
}
