/// Periodic REST depth snapshot validation (§3.17).
///
/// At a configurable interval (default 60 s) the caller fetches a fresh REST
/// depth snapshot and passes both it and the top levels of the in-memory order
/// book to [`check_snapshot`].  If more than `max_mismatched` levels on either
/// side diverge in price by more than `price_tolerance`, the function returns
/// [`SnapshotCheckResult::Incompatible`] and the caller should mark the book
/// stale with [`connector_core::BookStaleReason::SnapshotIncompatible`] and
/// trigger recovery.
///
/// # Level ordering
///
/// Both sides must be supplied **best-first**:
/// * bids — descending price (best bid at index 0)
/// * asks — ascending  price (best ask at index 0)
///
/// This matches what [`connector_order_book::OrderBook::bids`] /
/// [`connector_order_book::OrderBook::asks`] and Binance REST depth responses
/// produce.

use connector_core::{BookSnapshot, PriceLevel};

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

/// Number of price levels to compare on each side (default).
pub const CHECK_LEVELS: usize = 5;
/// Maximum diverged levels on either side before flagging incompatible (default).
pub const MAX_MISMATCHED: usize = 0;
/// Maximum per-level price divergence in raw scaled ticks (default = exact match).
pub const PRICE_TOLERANCE: i64 = 0;
/// How often the periodic check should run (seconds).
pub const INTERVAL_SECS: u64 = 60;

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

/// Configuration for [`check_snapshot`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SnapshotValidatorConfig {
    /// Number of top levels to compare on each side.
    pub check_levels: usize,
    /// How many levels on either side may diverge before the book is considered
    /// incompatible.  Zero means every compared level must be within tolerance.
    pub max_mismatched: usize,
    /// Maximum absolute price difference (in raw scaled i64 units) for a level
    /// to be considered "matching".  Zero means prices must be identical.
    pub price_tolerance: i64,
}

impl Default for SnapshotValidatorConfig {
    fn default() -> Self {
        Self {
            check_levels:    CHECK_LEVELS,
            max_mismatched:  MAX_MISMATCHED,
            price_tolerance: PRICE_TOLERANCE,
        }
    }
}

/// Result of a single periodic snapshot comparison.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SnapshotCheckResult {
    /// Top-of-book levels are within tolerance of the REST snapshot.
    Compatible,
    /// Too many levels diverge; the book should be marked stale and recovered.
    Incompatible {
        bid_mismatches: usize,
        ask_mismatches: usize,
        /// `lastUpdateId` from the REST snapshot that was used for comparison.
        snapshot_id: u64,
    },
}

// ---------------------------------------------------------------------------
// Core comparison
// ---------------------------------------------------------------------------

/// Compare the top-of-book levels from the in-memory book against a REST
/// depth snapshot, applying the configured tolerances.
///
/// Returns [`SnapshotCheckResult::Compatible`] when the book has no levels
/// yet (nothing to compare) to avoid false positives during startup.
pub fn check_snapshot(
    book_bids: impl Iterator<Item = PriceLevel>,
    book_asks: impl Iterator<Item = PriceLevel>,
    snapshot:  &BookSnapshot,
    cfg:       &SnapshotValidatorConfig,
) -> SnapshotCheckResult {
    let bid_mismatches = count_price_mismatches(
        book_bids,
        snapshot.bids.iter().copied(),
        cfg.check_levels,
        cfg.price_tolerance,
    );
    let ask_mismatches = count_price_mismatches(
        book_asks,
        snapshot.asks.iter().copied(),
        cfg.check_levels,
        cfg.price_tolerance,
    );

    if bid_mismatches > cfg.max_mismatched || ask_mismatches > cfg.max_mismatched {
        SnapshotCheckResult::Incompatible {
            bid_mismatches,
            ask_mismatches,
            snapshot_id: snapshot.update_id,
        }
    } else {
        SnapshotCheckResult::Compatible
    }
}

/// Count the number of level pairs (up to `check_levels`) whose prices differ
/// by more than `price_tolerance`.  Stops at the shorter of the two iterators.
fn count_price_mismatches(
    book_levels: impl Iterator<Item = PriceLevel>,
    snap_levels: impl Iterator<Item = PriceLevel>,
    check_levels: usize,
    price_tolerance: i64,
) -> usize {
    book_levels
        .take(check_levels)
        .zip(snap_levels.take(check_levels))
        .filter(|(b, s)| (b.price - s.price).abs() > price_tolerance)
        .count()
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use connector_core::{
        MarketType, MessageHeader, MessageType, PriceLevel, VenueId,
        SCHEMA_VERSION, TS_NONE,
    };

    use super::*;

    // Build a minimal BookSnapshot for tests.
    fn snap(update_id: u64, bids: Vec<PriceLevel>, asks: Vec<PriceLevel>) -> BookSnapshot {
        BookSnapshot {
            header: MessageHeader {
                schema_version:    SCHEMA_VERSION,
                message_type:      MessageType::BookSnapshot,
                venue_id:          VenueId::BinanceSpot,
                market_type:       MarketType::Spot,
                instrument_id:     0,
                connection_id:     0,
                instance_id:       0,
                sequence_number:   0,
                exchange_event_ts: TS_NONE,
                exchange_tx_ts:    TS_NONE,
                local_recv_ts:     0,
                local_publish_ts:  0,
            },
            symbol:    "BTCUSDT".into(),
            update_id,
            bids,
            asks,
        }
    }

    fn lvl(price: i64, qty: i64) -> PriceLevel {
        PriceLevel { price, qty }
    }

    fn cfg() -> SnapshotValidatorConfig {
        SnapshotValidatorConfig::default()
    }

    fn cfg_with(check_levels: usize, max_mismatched: usize, price_tolerance: i64)
        -> SnapshotValidatorConfig
    {
        SnapshotValidatorConfig { check_levels, max_mismatched, price_tolerance }
    }

    // --- Defaults ---

    #[test]
    fn default_config_has_spec_values() {
        let c = SnapshotValidatorConfig::default();
        assert_eq!(c.check_levels,    CHECK_LEVELS);
        assert_eq!(c.max_mismatched,  MAX_MISMATCHED);
        assert_eq!(c.price_tolerance, PRICE_TOLERANCE);
    }

    // --- Exact match ---

    #[test]
    fn identical_levels_return_compatible() {
        let bids = vec![lvl(100, 10), lvl(99, 5), lvl(98, 3)];
        let asks = vec![lvl(101, 8), lvl(102, 4), lvl(103, 2)];
        let s    = snap(1, bids.clone(), asks.clone());
        assert_eq!(
            check_snapshot(bids.into_iter(), asks.into_iter(), &s, &cfg()),
            SnapshotCheckResult::Compatible,
        );
    }

    #[test]
    fn single_level_match_is_compatible() {
        let bids = vec![lvl(100, 10)];
        let asks = vec![lvl(101, 8)];
        let s    = snap(1, bids.clone(), asks.clone());
        assert_eq!(
            check_snapshot(bids.into_iter(), asks.into_iter(), &s, &cfg()),
            SnapshotCheckResult::Compatible,
        );
    }

    // --- Empty book ---

    #[test]
    fn empty_book_returns_compatible() {
        let bids: Vec<PriceLevel> = vec![];
        let asks: Vec<PriceLevel> = vec![];
        let s = snap(1, vec![lvl(100, 10)], vec![lvl(101, 8)]);
        assert_eq!(
            check_snapshot(bids.into_iter(), asks.into_iter(), &s, &cfg()),
            SnapshotCheckResult::Compatible,
        );
    }

    #[test]
    fn empty_snapshot_returns_compatible() {
        let bids = vec![lvl(100, 10)];
        let asks = vec![lvl(101, 8)];
        let s    = snap(1, vec![], vec![]);
        assert_eq!(
            check_snapshot(bids.into_iter(), asks.into_iter(), &s, &cfg()),
            SnapshotCheckResult::Compatible,
        );
    }

    // --- Bid mismatch ---

    #[test]
    fn bid_price_mismatch_returns_incompatible() {
        let book_bids = vec![lvl(100, 10)];
        let book_asks = vec![lvl(101, 8)];
        let s = snap(1, vec![lvl(200, 10)], vec![lvl(101, 8)]);
        let r = check_snapshot(book_bids.into_iter(), book_asks.into_iter(), &s, &cfg());
        assert_eq!(
            r,
            SnapshotCheckResult::Incompatible {
                bid_mismatches: 1,
                ask_mismatches: 0,
                snapshot_id:    1,
            },
        );
    }

    // --- Ask mismatch ---

    #[test]
    fn ask_price_mismatch_returns_incompatible() {
        let book_bids = vec![lvl(100, 10)];
        let book_asks = vec![lvl(101, 8)];
        let s = snap(1, vec![lvl(100, 10)], vec![lvl(999, 8)]);
        let r = check_snapshot(book_bids.into_iter(), book_asks.into_iter(), &s, &cfg());
        assert_eq!(
            r,
            SnapshotCheckResult::Incompatible {
                bid_mismatches: 0,
                ask_mismatches: 1,
                snapshot_id:    1,
            },
        );
    }

    // --- Both sides mismatch ---

    #[test]
    fn both_sides_mismatched_reports_counts() {
        let book_bids = vec![lvl(100, 10), lvl(99, 5)];
        let book_asks = vec![lvl(101, 8), lvl(102, 4)];
        let s = snap(
            7,
            vec![lvl(200, 10), lvl(198, 5)],
            vec![lvl(201, 8),  lvl(202, 4)],
        );
        let r = check_snapshot(book_bids.into_iter(), book_asks.into_iter(), &s, &cfg());
        assert_eq!(
            r,
            SnapshotCheckResult::Incompatible {
                bid_mismatches: 2,
                ask_mismatches: 2,
                snapshot_id:    7,
            },
        );
    }

    // --- check_levels cap ---

    #[test]
    fn only_top_n_levels_are_compared() {
        // 5 levels total; first 5 (=check_levels) match, 6th differs
        let book_bids = vec![lvl(100,1), lvl(99,1), lvl(98,1), lvl(97,1), lvl(96,1), lvl(50,1)];
        let snap_bids = vec![lvl(100,1), lvl(99,1), lvl(98,1), lvl(97,1), lvl(96,1), lvl(99,1)];
        let book_asks = vec![lvl(101, 1)];
        let snap_asks = vec![lvl(101, 1)];
        let s = snap(1, snap_bids, snap_asks);
        assert_eq!(
            check_snapshot(book_bids.into_iter(), book_asks.into_iter(), &s, &cfg()),
            SnapshotCheckResult::Compatible,
        );
    }

    #[test]
    fn custom_check_levels_one_compares_only_best() {
        // Second level differs but check_levels=1 → only best is compared.
        let book_bids = vec![lvl(100, 1), lvl(50, 1)];
        let snap_bids = vec![lvl(100, 1), lvl(99, 1)];
        let book_asks = vec![lvl(101, 1)];
        let snap_asks = vec![lvl(101, 1)];
        let s = snap(1, snap_bids, snap_asks);
        let c = cfg_with(1, 0, 0);
        assert_eq!(
            check_snapshot(book_bids.into_iter(), book_asks.into_iter(), &s, &c),
            SnapshotCheckResult::Compatible,
        );
    }

    // --- price_tolerance ---

    #[test]
    fn price_within_tolerance_returns_compatible() {
        let book_bids = vec![lvl(100, 10)];
        let book_asks = vec![lvl(103, 8)];
        // book bid=100, snap bid=102 — diff=2, tolerance=2 → compatible
        let s = snap(1, vec![lvl(102, 10)], vec![lvl(103, 8)]);
        let c = cfg_with(5, 0, 2);
        assert_eq!(
            check_snapshot(book_bids.into_iter(), book_asks.into_iter(), &s, &c),
            SnapshotCheckResult::Compatible,
        );
    }

    #[test]
    fn price_exceeding_tolerance_by_one_returns_incompatible() {
        let book_bids = vec![lvl(100, 10)];
        let book_asks = vec![lvl(103, 8)];
        // book bid=100, snap bid=103 — diff=3, tolerance=2 → incompatible
        let s = snap(1, vec![lvl(103, 10)], vec![lvl(103, 8)]);
        let c = cfg_with(5, 0, 2);
        let r = check_snapshot(book_bids.into_iter(), book_asks.into_iter(), &s, &c);
        assert!(matches!(r, SnapshotCheckResult::Incompatible { bid_mismatches: 1, .. }));
    }

    #[test]
    fn price_exactly_at_tolerance_boundary_is_compatible() {
        // diff == tolerance → not > tolerance → compatible.
        let book_bids = vec![lvl(100, 10)];
        let book_asks = vec![lvl(101, 8)];
        let s = snap(1, vec![lvl(105, 10)], vec![lvl(101, 8)]);
        let c = cfg_with(5, 0, 5); // tolerance = 5, diff = 5 → compatible
        assert_eq!(
            check_snapshot(book_bids.into_iter(), book_asks.into_iter(), &s, &c),
            SnapshotCheckResult::Compatible,
        );
    }

    // --- max_mismatched ---

    #[test]
    fn one_mismatch_within_max_allowed_is_compatible() {
        let book_bids = vec![lvl(100, 1), lvl(99, 1)];
        let book_asks = vec![lvl(101, 1), lvl(102, 1)];
        // First bid matches, second doesn't.
        let s = snap(1, vec![lvl(100, 1), lvl(50, 1)], vec![lvl(101, 1), lvl(102, 1)]);
        let c = cfg_with(5, 1, 0); // allow 1 mismatch
        assert_eq!(
            check_snapshot(book_bids.into_iter(), book_asks.into_iter(), &s, &c),
            SnapshotCheckResult::Compatible,
        );
    }

    #[test]
    fn two_mismatches_exceeding_max_returns_incompatible() {
        let book_bids = vec![lvl(100, 1), lvl(99, 1)];
        let book_asks = vec![lvl(101, 1), lvl(102, 1)];
        let s = snap(1, vec![lvl(50, 1), lvl(49, 1)], vec![lvl(101, 1), lvl(102, 1)]);
        let c = cfg_with(5, 1, 0); // allow 1 mismatch, but 2 diverge
        let r = check_snapshot(book_bids.into_iter(), book_asks.into_iter(), &s, &c);
        assert!(matches!(
            r,
            SnapshotCheckResult::Incompatible { bid_mismatches: 2, .. }
        ));
    }

    // --- snapshot_id propagated ---

    #[test]
    fn incompatible_result_carries_snapshot_id() {
        let book_bids = vec![lvl(100, 1)];
        let book_asks = vec![lvl(101, 1)];
        let s = snap(42_000, vec![lvl(999, 1)], vec![lvl(101, 1)]);
        match check_snapshot(book_bids.into_iter(), book_asks.into_iter(), &s, &cfg()) {
            SnapshotCheckResult::Incompatible { snapshot_id, .. } => {
                assert_eq!(snapshot_id, 42_000);
            }
            other => panic!("expected Incompatible, got {other:?}"),
        }
    }

    // --- qty differences do not trigger incompatibility ---

    #[test]
    fn qty_difference_alone_does_not_trigger_incompatible() {
        // Same prices, different qtys — only prices are checked.
        let book_bids = vec![lvl(100, 999)];
        let book_asks = vec![lvl(101, 888)];
        let s = snap(1, vec![lvl(100, 1)], vec![lvl(101, 1)]);
        assert_eq!(
            check_snapshot(book_bids.into_iter(), book_asks.into_iter(), &s, &cfg()),
            SnapshotCheckResult::Compatible,
        );
    }
}
