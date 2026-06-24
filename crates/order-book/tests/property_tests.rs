//! Property tests for `OrderBook` invariants (§10.38).
//!
//! Three invariants are verified for all reachable book states:
//!
//! 1. **Bid < Ask** — when both sides are non-empty, `best_bid.price` is
//!    strictly less than `best_ask.price`.
//! 2. **No non-positive quantities** — every level retained in the book has
//!    `qty > 0`; zero-qty deltas must remove the level, not store it.
//! 3. **Monotonic `last_update_id`** — applying deltas in sequence never
//!    causes `last_update_id` to decrease.
//!
//! Additional structural properties (iterator order, `best_bid/best_ask`
//! extremum values, snapshot replacement, checksum stability) are included
//! because they underpin the invariants above.

use std::collections::HashSet;

use connector_core::{
    BookDelta, BookSnapshot, MarketType, MessageHeader, MessageType, PriceLevel, VenueId,
    SCHEMA_VERSION, TS_NONE, UPDATE_ID_NONE,
};
use connector_order_book::{harness::SyntheticHarness, OrderBook};
use proptest::prelude::*;

// ---------------------------------------------------------------------------
// Price domain
// ---------------------------------------------------------------------------
//
// Bids are generated strictly below MID; asks strictly above MID.
// This mirrors the exchange guarantee: by the time a depth update reaches us,
// the matching engine has already executed any crossing orders as trades.

const MID: i64 = 100_000;

fn bid_price() -> impl Strategy<Value = i64> {
    1i64..MID
}
fn ask_price() -> impl Strategy<Value = i64> {
    (MID + 1)..=200_000i64
}
fn pos_qty() -> impl Strategy<Value = i64> {
    1i64..=10_000i64
}

fn bid_level() -> impl Strategy<Value = PriceLevel> {
    (bid_price(), pos_qty()).prop_map(|(price, qty)| PriceLevel { price, qty })
}

fn ask_level() -> impl Strategy<Value = PriceLevel> {
    (ask_price(), pos_qty()).prop_map(|(price, qty)| PriceLevel { price, qty })
}

fn bid_levels(max: usize) -> impl Strategy<Value = Vec<PriceLevel>> {
    proptest::collection::vec(bid_level(), 0..=max)
}

fn ask_levels(max: usize) -> impl Strategy<Value = Vec<PriceLevel>> {
    proptest::collection::vec(ask_level(), 0..=max)
}

fn nonempty_bid_levels(max: usize) -> impl Strategy<Value = Vec<PriceLevel>> {
    proptest::collection::vec(bid_level(), 1..=max)
}

fn nonempty_ask_levels(max: usize) -> impl Strategy<Value = Vec<PriceLevel>> {
    proptest::collection::vec(ask_level(), 1..=max)
}

// ---------------------------------------------------------------------------
// Message builders
// ---------------------------------------------------------------------------

fn hdr(msg_type: MessageType) -> MessageHeader {
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
        header: hdr(MessageType::BookSnapshot),
        symbol: "T".into(),
        update_id: uid,
        price_scale: 2,
        qty_scale: 5,
        bids,
        asks,
    }
}

fn delta(bids: Vec<PriceLevel>, asks: Vec<PriceLevel>, uid: u64) -> BookDelta {
    BookDelta {
        header: hdr(MessageType::BookDelta),
        symbol: "T".into(),
        first_update_id: uid,
        final_update_id: uid,
        prev_update_id: UPDATE_ID_NONE,
        price_scale: 2,
        qty_scale: 5,
        bids,
        asks,
    }
}

// Return the distinct-price count in a level list (mirrors BTreeMap dedup).
fn distinct_prices(levels: &[PriceLevel]) -> usize {
    levels.iter().map(|l| l.price).collect::<HashSet<_>>().len()
}

// ---------------------------------------------------------------------------
// Invariant 1 — Bid < Ask
// ---------------------------------------------------------------------------

proptest! {
    /// For any valid snapshot (bids below MID, asks above MID), the book's
    /// best bid is strictly less than its best ask.
    #[test]
    fn inv1_bid_never_crosses_ask_for_valid_snapshot(
        bids in bid_levels(20),
        asks in ask_levels(20),
    ) {
        let mut book = OrderBook::new("T");
        book.apply_snapshot(&snap(bids, asks, 1));
        if let (Some(bb), Some(ba)) = (book.best_bid(), book.best_ask()) {
            prop_assert!(
                bb.price < ba.price,
                "crossed: bid {} >= ask {}", bb.price, ba.price
            );
        }
    }

    /// Applying a sequence of valid deltas never crosses the book.
    #[test]
    fn inv1_bid_never_crosses_ask_after_delta_sequence(
        bids_seq in proptest::collection::vec(bid_levels(10), 1..=10),
        asks_seq in proptest::collection::vec(ask_levels(10), 1..=10),
    ) {
        let mut book = OrderBook::new("T");
        let n = bids_seq.len().min(asks_seq.len());
        for i in 0..n {
            book.apply_delta(&delta(bids_seq[i].clone(), asks_seq[i].clone(), i as u64 + 1));
            if let (Some(bb), Some(ba)) = (book.best_bid(), book.best_ask()) {
                prop_assert!(
                    bb.price < ba.price,
                    "crossed after delta {i}: bid {} >= ask {}", bb.price, ba.price
                );
            }
        }
    }

    /// A snapshot followed by further deltas never crosses the book.
    #[test]
    fn inv1_bid_never_crosses_ask_after_snapshot_then_deltas(
        snap_bids  in bid_levels(10),
        snap_asks  in ask_levels(10),
        delta_bids in proptest::collection::vec(bid_levels(5), 0..=5),
        delta_asks in proptest::collection::vec(ask_levels(5), 0..=5),
    ) {
        let mut book = OrderBook::new("T");
        book.apply_snapshot(&snap(snap_bids, snap_asks, 1));
        let n = delta_bids.len().min(delta_asks.len());
        for i in 0..n {
            book.apply_delta(&delta(delta_bids[i].clone(), delta_asks[i].clone(), i as u64 + 2));
            if let (Some(bb), Some(ba)) = (book.best_bid(), book.best_ask()) {
                prop_assert!(
                    bb.price < ba.price,
                    "crossed after delta {i}: bid {} >= ask {}", bb.price, ba.price
                );
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Invariant 2 — No non-positive quantities
// ---------------------------------------------------------------------------

proptest! {
    /// Every bid level retained after a valid delta has strictly positive qty.
    #[test]
    fn inv2_no_nonpositive_qty_in_bids_after_valid_delta(
        bids in bid_levels(30),
    ) {
        let mut book = OrderBook::new("T");
        book.apply_delta(&delta(bids, vec![], 1));
        for lvl in book.bids() {
            prop_assert!(lvl.qty > 0,
                "bid qty {} <= 0 at price {}", lvl.qty, lvl.price);
        }
    }

    /// Every ask level retained after a valid delta has strictly positive qty.
    #[test]
    fn inv2_no_nonpositive_qty_in_asks_after_valid_delta(
        asks in ask_levels(30),
    ) {
        let mut book = OrderBook::new("T");
        book.apply_delta(&delta(vec![], asks, 1));
        for lvl in book.asks() {
            prop_assert!(lvl.qty > 0,
                "ask qty {} <= 0 at price {}", lvl.qty, lvl.price);
        }
    }

    /// Every level retained after a snapshot has strictly positive qty.
    #[test]
    fn inv2_no_nonpositive_qty_after_snapshot(
        bids in bid_levels(20),
        asks in ask_levels(20),
    ) {
        let mut book = OrderBook::new("T");
        book.apply_snapshot(&snap(bids, asks, 1));
        for lvl in book.bids() {
            prop_assert!(lvl.qty > 0, "bid qty {}", lvl.qty);
        }
        for lvl in book.asks() {
            prop_assert!(lvl.qty > 0, "ask qty {}", lvl.qty);
        }
    }

    /// A zero-qty delta for a bid level removes it from the book entirely —
    /// no zero-qty entry is ever retained.
    #[test]
    fn inv2_zero_qty_bid_delta_removes_level(
        price in bid_price(),
        qty   in pos_qty(),
    ) {
        let mut book = OrderBook::new("T");
        book.apply_delta(&delta(vec![PriceLevel { price, qty }], vec![], 1));
        prop_assert_eq!(book.bid_depth(), 1);

        book.apply_delta(&delta(vec![PriceLevel { price, qty: 0 }], vec![], 2));
        prop_assert_eq!(book.bid_depth(), 0,
            "level at price {} must be gone after qty=0 delta", price);
        prop_assert!(book.best_bid().is_none());
    }

    /// A zero-qty delta for an ask level removes it from the book entirely.
    #[test]
    fn inv2_zero_qty_ask_delta_removes_level(
        price in ask_price(),
        qty   in pos_qty(),
    ) {
        let mut book = OrderBook::new("T");
        book.apply_delta(&delta(vec![], vec![PriceLevel { price, qty }], 1));
        prop_assert_eq!(book.ask_depth(), 1);

        book.apply_delta(&delta(vec![], vec![PriceLevel { price, qty: 0 }], 2));
        prop_assert_eq!(book.ask_depth(), 0);
        prop_assert!(book.best_ask().is_none());
    }

    /// Removing a level that was never present is a no-op; qty invariant holds.
    #[test]
    fn inv2_zero_qty_for_absent_level_is_noop(
        price in bid_price(),
        other in bid_price(),
        qty   in pos_qty(),
    ) {
        prop_assume!(price != other);
        let mut book = OrderBook::new("T");
        book.apply_delta(&delta(vec![PriceLevel { price, qty }], vec![], 1));

        // Remove a level that was never inserted.
        book.apply_delta(&delta(vec![PriceLevel { price: other, qty: 0 }], vec![], 2));

        // Original level is unaffected.
        prop_assert_eq!(book.bid_depth(), 1);
        let bb = book.best_bid().unwrap();
        prop_assert_eq!(bb.price, price);
        prop_assert!(bb.qty > 0);
    }
}

// ---------------------------------------------------------------------------
// Invariant 3 — Monotonic last_update_id
// ---------------------------------------------------------------------------

proptest! {
    /// Applying `n` consecutive deltas with increasing UIDs keeps
    /// `last_update_id` monotonically non-decreasing throughout.
    #[test]
    fn inv3_last_update_id_never_regresses_across_deltas(
        n in 1usize..=50,
    ) {
        let mut book = OrderBook::new("T");
        let mut prev  = 0u64;
        for i in 0..n {
            let uid = i as u64 + 1;
            book.apply_delta(&delta(vec![], vec![], uid));
            let current = book.last_update_id();
            prop_assert!(
                current >= prev,
                "last_update_id regressed from {prev} to {current} at step {i}"
            );
            prev = current;
        }
    }

    /// After applying a snapshot (which sets `last_update_id` to `snap.update_id`),
    /// further deltas only advance `last_update_id`.
    #[test]
    fn inv3_last_update_id_advances_after_snapshot(
        snap_uid    in 1u64..=1_000u64,
        delta_count in 1usize..=20,
    ) {
        let mut book = OrderBook::new("T");
        book.apply_snapshot(&snap(vec![], vec![], snap_uid));
        prop_assert_eq!(book.last_update_id(), snap_uid);

        let mut prev = snap_uid;
        for i in 0..delta_count {
            let uid = snap_uid + i as u64 + 1;
            book.apply_delta(&delta(vec![], vec![], uid));
            let current = book.last_update_id();
            prop_assert!(
                current >= prev,
                "last_update_id regressed from {prev} to {current}"
            );
            prev = current;
        }
    }

    /// Two snapshots applied in order keep `last_update_id` moving forward.
    #[test]
    fn inv3_second_snapshot_uid_greater_than_first(
        uid1 in 1u64..=1_000u64,
        gap  in 1u64..=500u64,
    ) {
        let uid2 = uid1 + gap;
        let mut book = OrderBook::new("T");
        book.apply_snapshot(&snap(vec![], vec![], uid1));
        prop_assert_eq!(book.last_update_id(), uid1);
        book.apply_snapshot(&snap(vec![], vec![], uid2));
        prop_assert!(book.last_update_id() >= uid1,
            "second snapshot must not regress last_update_id");
    }
}

// ---------------------------------------------------------------------------
// Structural properties (underpin the three invariants)
// ---------------------------------------------------------------------------

proptest! {
    /// The bid iterator always yields levels in strictly descending price order.
    #[test]
    fn struct_bid_iterator_is_strictly_descending(
        bids in bid_levels(20),
    ) {
        let mut book = OrderBook::new("T");
        book.apply_snapshot(&snap(bids, vec![], 1));
        let prices: Vec<i64> = book.bids().map(|l| l.price).collect();
        for w in prices.windows(2) {
            prop_assert!(w[0] > w[1],
                "bid prices not strictly descending: {} then {}", w[0], w[1]);
        }
    }

    /// The ask iterator always yields levels in strictly ascending price order.
    #[test]
    fn struct_ask_iterator_is_strictly_ascending(
        asks in ask_levels(20),
    ) {
        let mut book = OrderBook::new("T");
        book.apply_snapshot(&snap(vec![], asks, 1));
        let prices: Vec<i64> = book.asks().map(|l| l.price).collect();
        for w in prices.windows(2) {
            prop_assert!(w[0] < w[1],
                "ask prices not strictly ascending: {} then {}", w[0], w[1]);
        }
    }

    /// `best_bid()` always returns the maximum price in the bid side.
    #[test]
    fn struct_best_bid_is_maximum_bid_price(
        bids in nonempty_bid_levels(20),
    ) {
        let expected = bids.iter().map(|l| l.price).max().unwrap();
        let mut book  = OrderBook::new("T");
        book.apply_snapshot(&snap(bids, vec![], 1));
        let bb = book.best_bid().expect("non-empty bid side must have a best bid");
        prop_assert_eq!(bb.price, expected,
            "best_bid should be {}, got {}", expected, bb.price);
    }

    /// `best_ask()` always returns the minimum price in the ask side.
    #[test]
    fn struct_best_ask_is_minimum_ask_price(
        asks in nonempty_ask_levels(20),
    ) {
        let expected = asks.iter().map(|l| l.price).min().unwrap();
        let mut book  = OrderBook::new("T");
        book.apply_snapshot(&snap(vec![], asks, 1));
        let ba = book.best_ask().expect("non-empty ask side must have a best ask");
        prop_assert_eq!(ba.price, expected,
            "best_ask should be {}, got {}", expected, ba.price);
    }

    /// Applying a second snapshot fully replaces the first: depth equals the
    /// number of distinct prices in the new snapshot (positive-qty levels only).
    #[test]
    fn struct_snapshot_fully_replaces_prior_state(
        bids1 in bid_levels(10),
        asks1 in ask_levels(10),
        bids2 in bid_levels(10),
        asks2 in ask_levels(10),
    ) {
        let mut book = OrderBook::new("T");
        book.apply_snapshot(&snap(bids1, asks1, 1));
        let exp_bid = distinct_prices(&bids2);
        let exp_ask = distinct_prices(&asks2);
        book.apply_snapshot(&snap(bids2, asks2, 2));
        prop_assert_eq!(book.bid_depth(), exp_bid,
            "bid depth after second snapshot: expected {}, got {}", exp_bid, book.bid_depth());
        prop_assert_eq!(book.ask_depth(), exp_ask,
            "ask depth after second snapshot: expected {}, got {}", exp_ask, book.ask_depth());
    }

    /// An empty snapshot clears the book completely, regardless of prior state.
    #[test]
    fn struct_empty_snapshot_clears_all_levels(
        bids in bid_levels(15),
        asks in ask_levels(15),
    ) {
        let mut book = OrderBook::new("T");
        book.apply_snapshot(&snap(bids, asks, 1));
        book.apply_snapshot(&snap(vec![], vec![], 2)); // delisting
        prop_assert!(book.is_empty(),
            "book must be empty after empty snapshot; bid_depth={}, ask_depth={}",
            book.bid_depth(), book.ask_depth());
        prop_assert!(book.best_bid().is_none());
        prop_assert!(book.best_ask().is_none());
    }

    /// Two books built from the same sequence of operations have identical
    /// checksums.
    #[test]
    fn struct_checksum_stable_for_identical_state(
        bids in bid_levels(15),
        asks in ask_levels(15),
    ) {
        let mut a = OrderBook::new("T");
        let mut b = OrderBook::new("T");
        let s = snap(bids, asks, 1);
        a.apply_snapshot(&s);
        b.apply_snapshot(&s);
        prop_assert_eq!(a.checksum(), b.checksum(),
            "identical books must have identical checksums");
    }

    /// Changing any level's qty changes the checksum.
    #[test]
    fn struct_checksum_differs_after_qty_change(
        price in bid_price(),
        qty1  in 1i64..=5_000i64,
        qty2  in 5_001i64..=10_000i64,
    ) {
        let mut a = OrderBook::new("T");
        let mut b = OrderBook::new("T");
        a.apply_snapshot(&snap(vec![PriceLevel { price, qty: qty1 }], vec![], 1));
        b.apply_snapshot(&snap(vec![PriceLevel { price, qty: qty2 }], vec![], 1));
        prop_assert_ne!(a.checksum(), b.checksum(),
            "different qty ({} vs {}) must produce different checksums", qty1, qty2);
    }

    /// The `SyntheticHarness` never produces invariant violations for any
    /// combination of symbol count, operation count, and seed.
    #[test]
    fn struct_harness_never_violates_invariants(
        symbol_count in 1usize..=8,
        delta_count  in 1usize..=15,
        seed         in any::<u64>(),
    ) {
        let mut h = SyntheticHarness::with_symbol_count(symbol_count, seed);
        h.broadcast_snapshot(5, 5);
        for _ in 0..delta_count {
            h.broadcast_delta(2, 2);
        }
        let v = h.check_invariants();
        prop_assert!(
            v.is_clean(),
            "harness produced {n} violation(s): {v:?}",
            n = v.count(),
            v = v.violations
        );
    }

    /// Bid depth never exceeds the number of distinct prices in the applied
    /// updates (BTreeMap deduplication).
    #[test]
    fn struct_bid_depth_bounded_by_distinct_prices(
        bids in bid_levels(20),
    ) {
        let expected_max = distinct_prices(&bids);
        let mut book = OrderBook::new("T");
        book.apply_snapshot(&snap(bids, vec![], 1));
        prop_assert!(
            book.bid_depth() <= expected_max,
            "bid_depth {} > distinct prices {}", book.bid_depth(), expected_max
        );
    }

    /// Ask depth never exceeds the number of distinct prices in the applied
    /// updates.
    #[test]
    fn struct_ask_depth_bounded_by_distinct_prices(
        asks in ask_levels(20),
    ) {
        let expected_max = distinct_prices(&asks);
        let mut book = OrderBook::new("T");
        book.apply_snapshot(&snap(vec![], asks, 1));
        prop_assert!(
            book.ask_depth() <= expected_max,
            "ask_depth {} > distinct prices {}", book.ask_depth(), expected_max
        );
    }
}
