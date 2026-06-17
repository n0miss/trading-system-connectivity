use std::collections::BTreeMap;

use connector_core::{BookDelta, BookSnapshot, BookStaleReason, PriceLevel};

// ---------------------------------------------------------------------------
// OrderBook
// ---------------------------------------------------------------------------

/// Single-symbol in-memory L2 order book.
///
/// Bids are stored as a `BTreeMap<price, qty>` iterated in descending order
/// (best bid = highest price = `max_key`). Asks are ascending (best ask =
/// lowest price = `min_key`). A `qty` of zero signals level removal; the book
/// enforces this invariant — zero-qty entries are never retained.
///
/// Staleness is tracked separately from the level data so the recovery
/// procedure (Stage 3.14) can apply a snapshot and then explicitly call
/// [`mark_recovered`] when all conditions are met.
///
/// [`mark_recovered`]: OrderBook::mark_recovered
pub struct OrderBook {
    /// price → qty, best bid = maximum key.
    bids: BTreeMap<i64, i64>,
    /// price → qty, best ask = minimum key.
    asks: BTreeMap<i64, i64>,
    symbol:         String,
    last_update_id: u64,
    stale:          bool,
    stale_reason:   Option<BookStaleReason>,
}

impl OrderBook {
    pub fn new(symbol: impl Into<String>) -> Self {
        Self {
            bids:           BTreeMap::new(),
            asks:           BTreeMap::new(),
            symbol:         symbol.into(),
            last_update_id: 0,
            stale:          false,
            stale_reason:   None,
        }
    }

    // --- Accessors ---

    pub fn symbol(&self)         -> &str { &self.symbol }
    pub fn last_update_id(&self) -> u64  { self.last_update_id }
    pub fn bid_depth(&self)      -> usize { self.bids.len() }
    pub fn ask_depth(&self)      -> usize { self.asks.len() }
    pub fn is_empty(&self)       -> bool  { self.bids.is_empty() && self.asks.is_empty() }
    pub fn is_stale(&self)       -> bool  { self.stale }
    pub fn stale_reason(&self)   -> Option<BookStaleReason> { self.stale_reason }

    /// Best (highest-price) bid level, or `None` if the bid side is empty.
    pub fn best_bid(&self) -> Option<PriceLevel> {
        self.bids.iter().next_back().map(|(&price, &qty)| PriceLevel { price, qty })
    }

    /// Best (lowest-price) ask level, or `None` if the ask side is empty.
    pub fn best_ask(&self) -> Option<PriceLevel> {
        self.asks.iter().next().map(|(&price, &qty)| PriceLevel { price, qty })
    }

    /// Iterate bids in descending price order (best first).
    pub fn bids(&self) -> impl Iterator<Item = PriceLevel> + '_ {
        self.bids.iter().rev().map(|(&price, &qty)| PriceLevel { price, qty })
    }

    /// Iterate asks in ascending price order (best first).
    pub fn asks(&self) -> impl Iterator<Item = PriceLevel> + '_ {
        self.asks.iter().map(|(&price, &qty)| PriceLevel { price, qty })
    }

    // --- Staleness ---

    /// Mark the book as stale with the given reason.
    ///
    /// The level data is preserved so recovery (Stage 3.14) can inspect it,
    /// but callers should not rely on the levels being accurate while stale.
    pub fn mark_stale(&mut self, reason: BookStaleReason) {
        self.stale        = true;
        self.stale_reason = Some(reason);
    }

    /// Clear the stale flag.  Call this after the recovery procedure has
    /// successfully re-synchronised the book (Stage 3.14).
    pub fn mark_recovered(&mut self) {
        self.stale        = false;
        self.stale_reason = None;
    }

    // --- Mutations ---

    /// Replace the full book with a REST snapshot.
    /// Existing levels are discarded; zero-qty snapshot entries are skipped.
    pub fn apply_snapshot(&mut self, snap: &BookSnapshot) {
        self.bids.clear();
        self.asks.clear();
        for lvl in &snap.bids {
            if lvl.qty > 0 {
                self.bids.insert(lvl.price, lvl.qty);
            }
        }
        for lvl in &snap.asks {
            if lvl.qty > 0 {
                self.asks.insert(lvl.price, lvl.qty);
            }
        }
        self.last_update_id = snap.update_id;
    }

    /// Apply an incremental depth-update delta.
    /// qty > 0 → insert/update level; qty == 0 → remove level.
    /// `last_update_id` advances to `delta.final_update_id`.
    pub fn apply_delta(&mut self, delta: &BookDelta) {
        apply_levels(&mut self.bids, &delta.bids);
        apply_levels(&mut self.asks, &delta.asks);
        self.last_update_id = delta.final_update_id;
    }
}

// ---------------------------------------------------------------------------
// Internal helpers
// ---------------------------------------------------------------------------

fn apply_levels(side: &mut BTreeMap<i64, i64>, levels: &[PriceLevel]) {
    for lvl in levels {
        if lvl.qty == 0 {
            side.remove(&lvl.price);
        } else {
            side.insert(lvl.price, lvl.qty);
        }
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use connector_core::{
        BookStaleReason, MarketType, MessageHeader, MessageType, VenueId,
        SCHEMA_VERSION, TS_NONE, UPDATE_ID_NONE,
    };

    fn hdr(msg_type: MessageType) -> MessageHeader {
        MessageHeader {
            schema_version:    SCHEMA_VERSION,
            message_type:      msg_type,
            venue_id:          VenueId::BinanceSpot,
            market_type:       MarketType::Spot,
            instrument_id:     1,
            connection_id:     0,
            instance_id:       0,
            sequence_number:   0,
            exchange_event_ts: TS_NONE,
            exchange_tx_ts:    TS_NONE,
            local_recv_ts:     TS_NONE,
            local_publish_ts:  TS_NONE,
        }
    }

    fn level(price: i64, qty: i64) -> PriceLevel {
        PriceLevel { price, qty }
    }

    fn delta(bids: Vec<PriceLevel>, asks: Vec<PriceLevel>, last_id: u64) -> BookDelta {
        BookDelta {
            header:          hdr(MessageType::BookDelta),
            symbol:          "BTCUSDT".to_string(),
            first_update_id: last_id,
            final_update_id: last_id,
            prev_update_id:  UPDATE_ID_NONE,
            bids,
            asks,
        }
    }

    fn snapshot(bids: Vec<PriceLevel>, asks: Vec<PriceLevel>, update_id: u64) -> BookSnapshot {
        BookSnapshot {
            header:    hdr(MessageType::BookSnapshot),
            symbol:    "BTCUSDT".to_string(),
            update_id,
            bids,
            asks,
        }
    }

    // --- empty book ---

    #[test]
    fn empty_book_best_bid_is_none() {
        let book = OrderBook::new("BTCUSDT");
        assert!(book.best_bid().is_none());
    }

    #[test]
    fn empty_book_best_ask_is_none() {
        let book = OrderBook::new("BTCUSDT");
        assert!(book.best_ask().is_none());
    }

    #[test]
    fn new_book_is_empty() {
        let book = OrderBook::new("ETHUSDT");
        assert!(book.is_empty());
        assert_eq!(book.bid_depth(), 0);
        assert_eq!(book.ask_depth(), 0);
    }

    // --- apply_delta: inserts ---

    #[test]
    fn apply_delta_adds_bid_and_ask_levels() {
        let mut book = OrderBook::new("BTCUSDT");
        book.apply_delta(&delta(
            vec![level(96_500_00, 100), level(96_499_00, 200)],
            vec![level(96_501_00, 50)],
            1,
        ));
        assert_eq!(book.bid_depth(), 2);
        assert_eq!(book.ask_depth(), 1);
        assert!(!book.is_empty());
    }

    #[test]
    fn apply_delta_updates_existing_level() {
        let mut book = OrderBook::new("BTCUSDT");
        book.apply_delta(&delta(vec![level(100, 10)], vec![], 1));
        book.apply_delta(&delta(vec![level(100, 99)], vec![], 2));
        assert_eq!(book.bid_depth(), 1);
        assert_eq!(book.best_bid().unwrap().qty, 99);
    }

    // --- apply_delta: removals ---

    #[test]
    fn apply_delta_zero_qty_removes_level() {
        let mut book = OrderBook::new("BTCUSDT");
        book.apply_delta(&delta(vec![level(100, 10)], vec![], 1));
        assert_eq!(book.bid_depth(), 1);
        book.apply_delta(&delta(vec![level(100, 0)], vec![], 2));
        assert_eq!(book.bid_depth(), 0);
        assert!(book.best_bid().is_none());
    }

    #[test]
    fn apply_delta_remove_nonexistent_level_is_noop() {
        let mut book = OrderBook::new("BTCUSDT");
        book.apply_delta(&delta(vec![level(999, 0)], vec![], 1));
        assert!(book.is_empty());
    }

    // --- best bid / ask ordering ---

    #[test]
    fn best_bid_is_highest_price() {
        let mut book = OrderBook::new("BTCUSDT");
        book.apply_delta(&delta(
            vec![level(100, 1), level(200, 1), level(150, 1)],
            vec![],
            1,
        ));
        assert_eq!(book.best_bid().unwrap().price, 200);
    }

    #[test]
    fn best_ask_is_lowest_price() {
        let mut book = OrderBook::new("BTCUSDT");
        book.apply_delta(&delta(
            vec![],
            vec![level(300, 1), level(200, 1), level(250, 1)],
            1,
        ));
        assert_eq!(book.best_ask().unwrap().price, 200);
    }

    // --- iterators ---

    #[test]
    fn bids_iterator_is_descending() {
        let mut book = OrderBook::new("BTCUSDT");
        book.apply_delta(&delta(
            vec![level(100, 1), level(300, 1), level(200, 1)],
            vec![],
            1,
        ));
        let prices: Vec<i64> = book.bids().map(|l| l.price).collect();
        assert_eq!(prices, vec![300, 200, 100]);
    }

    #[test]
    fn asks_iterator_is_ascending() {
        let mut book = OrderBook::new("BTCUSDT");
        book.apply_delta(&delta(
            vec![],
            vec![level(300, 1), level(100, 1), level(200, 1)],
            1,
        ));
        let prices: Vec<i64> = book.asks().map(|l| l.price).collect();
        assert_eq!(prices, vec![100, 200, 300]);
    }

    // --- apply_snapshot ---

    #[test]
    fn apply_snapshot_populates_book() {
        let mut book = OrderBook::new("BTCUSDT");
        book.apply_snapshot(&snapshot(
            vec![level(100, 10), level(90, 20)],
            vec![level(110, 5)],
            42,
        ));
        assert_eq!(book.bid_depth(), 2);
        assert_eq!(book.ask_depth(), 1);
        assert_eq!(book.last_update_id(), 42);
    }

    #[test]
    fn apply_snapshot_replaces_existing_book() {
        let mut book = OrderBook::new("BTCUSDT");
        book.apply_delta(&delta(
            vec![level(100, 10), level(200, 5), level(300, 1)],
            vec![level(400, 3)],
            5,
        ));
        assert_eq!(book.bid_depth(), 3);

        book.apply_snapshot(&snapshot(
            vec![level(101, 7)],
            vec![level(102, 2)],
            99,
        ));
        assert_eq!(book.bid_depth(), 1);
        assert_eq!(book.ask_depth(), 1);
        assert_eq!(book.best_bid().unwrap().price, 101);
        assert_eq!(book.last_update_id(), 99);
    }

    #[test]
    fn apply_snapshot_skips_zero_qty_entries() {
        let mut book = OrderBook::new("BTCUSDT");
        book.apply_snapshot(&snapshot(
            vec![level(100, 0), level(90, 5)],
            vec![level(110, 0), level(120, 3)],
            1,
        ));
        assert_eq!(book.bid_depth(), 1);
        assert_eq!(book.ask_depth(), 1);
        assert_eq!(book.best_bid().unwrap().price, 90);
        assert_eq!(book.best_ask().unwrap().price, 120);
    }

    // --- last_update_id ---

    #[test]
    fn last_update_id_tracks_final_update_id() {
        let mut book = OrderBook::new("BTCUSDT");
        assert_eq!(book.last_update_id(), 0);
        book.apply_delta(&delta(vec![], vec![], 17));
        assert_eq!(book.last_update_id(), 17);
        book.apply_delta(&delta(vec![], vec![], 42));
        assert_eq!(book.last_update_id(), 42);
    }

    // --- symbol ---

    #[test]
    fn symbol_is_preserved() {
        let book = OrderBook::new("SOLUSDT");
        assert_eq!(book.symbol(), "SOLUSDT");
    }

    // --- staleness ---

    #[test]
    fn new_book_is_not_stale() {
        let book = OrderBook::new("BTCUSDT");
        assert!(!book.is_stale());
        assert_eq!(book.stale_reason(), None);
    }

    #[test]
    fn mark_stale_sets_flag_and_reason() {
        let mut book = OrderBook::new("BTCUSDT");
        book.mark_stale(BookStaleReason::SequenceGap);
        assert!(book.is_stale());
        assert_eq!(book.stale_reason(), Some(BookStaleReason::SequenceGap));
    }

    #[test]
    fn mark_recovered_clears_stale_flag() {
        let mut book = OrderBook::new("BTCUSDT");
        book.mark_stale(BookStaleReason::SequenceGap);
        book.mark_recovered();
        assert!(!book.is_stale());
        assert_eq!(book.stale_reason(), None);
    }

    #[test]
    fn mark_stale_does_not_erase_level_data() {
        let mut book = OrderBook::new("BTCUSDT");
        book.apply_delta(&delta(vec![level(100, 10)], vec![level(101, 5)], 1));
        book.mark_stale(BookStaleReason::WebSocketReconnect);
        assert_eq!(book.bid_depth(), 1);
        assert_eq!(book.ask_depth(), 1);
    }

    #[test]
    fn mark_stale_can_be_overwritten_with_different_reason() {
        let mut book = OrderBook::new("BTCUSDT");
        book.mark_stale(BookStaleReason::SequenceGap);
        book.mark_stale(BookStaleReason::BboMismatch);
        assert_eq!(book.stale_reason(), Some(BookStaleReason::BboMismatch));
    }
}
