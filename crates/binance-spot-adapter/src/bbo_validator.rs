//! Top-of-book BBO validation (§3.16).
//!
//! Each time a `BestBidOffer` event arrives, the caller compares the order
//! book's best bid/ask *price* against the BBO stream's prices and passes the
//! result to [`BboValidator::check`].  The validator tracks how long a mismatch
//! has persisted and returns the appropriate action:
//!
//! | Mismatch duration | Action            |
//! |-------------------|-------------------|
//! | < 250 ms          | none (wait)       |
//! | ≥ 250 ms          | [`BboCheckResult::Degrade`]    |
//! | ≥ 1 s             | [`BboCheckResult::MarkStale`] |
//!
//! A mismatch timer is started on the *first* mis-matching BBO event and
//! cancelled on the first matching one.  Calling `check` with `None` book
//! prices (book not yet populated) is treated as a match — the empty-book
//! period must not trigger a false positive.

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

pub const DEGRADE_NS: i64 = 250_000_000; // 250 ms
pub const STALE_NS: i64 = 1_000_000_000; // 1 s

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

/// Action returned by [`BboValidator::check`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BboCheckResult {
    /// Prices match (or book is not yet populated); no action required.
    Ok,
    /// Mismatch has persisted ≥ 250 ms — caller should transition to Degraded.
    Degrade { mismatch_ns: i64 },
    /// Mismatch has persisted ≥ 1 s — caller should mark the book Stale with
    /// [`connector_core::BookStaleReason::BboMismatch`].
    MarkStale { mismatch_ns: i64 },
}

// ---------------------------------------------------------------------------
// BboValidator
// ---------------------------------------------------------------------------

/// Tracks how long the order book's top-of-book has been diverging from the
/// BBO stream and signals when action thresholds are crossed.
#[derive(Debug)]
pub struct BboValidator {
    /// Nanosecond timestamp of the first BBO event that disagreed with the
    /// book.  `None` when prices are in sync.
    mismatch_since: Option<i64>,
    degrade_ns: i64,
    stale_ns: i64,
}

impl BboValidator {
    /// Create a validator with the §3.16 defaults (250 ms degrade / 1 s stale).
    pub fn new() -> Self {
        Self::with_thresholds(DEGRADE_NS, STALE_NS)
    }

    /// Create a validator with custom thresholds (useful for tests).
    pub fn with_thresholds(degrade_ns: i64, stale_ns: i64) -> Self {
        Self {
            mismatch_since: None,
            degrade_ns,
            stale_ns,
        }
    }

    // --- Core check ---------------------------------------------------------

    /// Compare book top-of-book prices against the latest BBO prices and
    /// return the appropriate action.
    ///
    /// * `now_ns`          — current nanosecond timestamp (usually the BBO frame's `recv_ts`).
    /// * `book_bid_price`  — `None` if the book is empty.
    /// * `book_ask_price`  — `None` if the book is empty.
    /// * `bbo_bid_price`   — best bid price from the BBO stream (scaled i64).
    /// * `bbo_ask_price`   — best ask price from the BBO stream (scaled i64).
    pub fn check(
        &mut self,
        now_ns: i64,
        book_bid_price: Option<i64>,
        book_ask_price: Option<i64>,
        bbo_bid_price: i64,
        bbo_ask_price: i64,
    ) -> BboCheckResult {
        // Empty book — no data to compare; treat as OK and reset the timer.
        let (Some(bb), Some(ba)) = (book_bid_price, book_ask_price) else {
            self.mismatch_since = None;
            return BboCheckResult::Ok;
        };

        let prices_match = bb == bbo_bid_price && ba == bbo_ask_price;

        if prices_match {
            self.mismatch_since = None;
            return BboCheckResult::Ok;
        }

        // Mismatch — start or continue the timer.
        let since = *self.mismatch_since.get_or_insert(now_ns);
        let duration_ns = now_ns.saturating_sub(since);

        if duration_ns >= self.stale_ns {
            BboCheckResult::MarkStale {
                mismatch_ns: duration_ns,
            }
        } else if duration_ns >= self.degrade_ns {
            BboCheckResult::Degrade {
                mismatch_ns: duration_ns,
            }
        } else {
            BboCheckResult::Ok
        }
    }

    // --- Reset --------------------------------------------------------------

    /// Clear the mismatch timer (call after successful recovery or on reconnect).
    pub fn clear(&mut self) {
        self.mismatch_since = None;
    }

    // --- Accessors ----------------------------------------------------------

    pub fn mismatch_since(&self) -> Option<i64> {
        self.mismatch_since
    }
    pub fn degrade_ns(&self) -> i64 {
        self.degrade_ns
    }
    pub fn stale_ns(&self) -> i64 {
        self.stale_ns
    }
}

impl Default for BboValidator {
    fn default() -> Self {
        Self::new()
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    const MS: i64 = 1_000_000;
    const SEC: i64 = 1_000_000_000;

    fn bv() -> BboValidator {
        BboValidator::with_thresholds(250 * MS, SEC)
    }

    // --- Matching prices ---

    #[test]
    fn matching_prices_return_ok() {
        let mut v = bv();
        assert_eq!(
            v.check(0, Some(100), Some(101), 100, 101),
            BboCheckResult::Ok,
        );
    }

    #[test]
    fn match_clears_mismatch_timer() {
        let mut v = bv();
        // Start a mismatch.
        v.check(0, Some(100), Some(101), 200, 201);
        assert!(v.mismatch_since().is_some());
        // Then a match — timer should clear.
        v.check(100 * MS, Some(100), Some(101), 100, 101);
        assert_eq!(v.mismatch_since(), None);
    }

    #[test]
    fn after_match_subsequent_mismatch_restarts_timer() {
        let mut v = bv();
        v.check(0, Some(100), Some(101), 200, 201);
        v.check(100 * MS, Some(100), Some(101), 100, 101); // match — clears
        v.check(200 * MS, Some(100), Some(101), 300, 301); // new mismatch
        assert_eq!(v.mismatch_since(), Some(200 * MS));
    }

    // --- Empty book ---

    #[test]
    fn empty_book_bid_returns_ok() {
        let mut v = bv();
        assert_eq!(v.check(0, None, Some(101), 100, 101), BboCheckResult::Ok,);
    }

    #[test]
    fn empty_book_ask_returns_ok() {
        let mut v = bv();
        assert_eq!(v.check(0, Some(100), None, 100, 101), BboCheckResult::Ok,);
    }

    #[test]
    fn both_none_returns_ok() {
        let mut v = bv();
        assert_eq!(v.check(0, None, None, 100, 101), BboCheckResult::Ok,);
    }

    #[test]
    fn empty_book_clears_existing_mismatch_timer() {
        let mut v = bv();
        v.check(0, Some(100), Some(101), 200, 201); // mismatch starts
        v.check(100 * MS, None, None, 200, 201); // book empty — clears timer
        assert_eq!(v.mismatch_since(), None);
    }

    // --- Fresh mismatch (< 250 ms) ---

    #[test]
    fn fresh_mismatch_returns_ok() {
        let mut v = bv();
        let r = v.check(0, Some(100), Some(101), 200, 201);
        assert_eq!(r, BboCheckResult::Ok);
    }

    #[test]
    fn mismatch_at_249ms_returns_ok() {
        let mut v = bv();
        v.check(0, Some(100), Some(101), 200, 201); // start timer
        let r = v.check(249 * MS, Some(100), Some(101), 200, 201);
        assert_eq!(r, BboCheckResult::Ok);
    }

    // --- Degrade threshold (≥ 250 ms, < 1 s) ---

    #[test]
    fn mismatch_at_250ms_returns_degrade() {
        let mut v = bv();
        v.check(0, Some(100), Some(101), 200, 201);
        let r = v.check(250 * MS, Some(100), Some(101), 200, 201);
        assert_eq!(
            r,
            BboCheckResult::Degrade {
                mismatch_ns: 250 * MS
            }
        );
    }

    #[test]
    fn mismatch_at_999ms_returns_degrade() {
        let mut v = bv();
        v.check(0, Some(100), Some(101), 200, 201);
        let r = v.check(999 * MS, Some(100), Some(101), 200, 201);
        assert_eq!(
            r,
            BboCheckResult::Degrade {
                mismatch_ns: 999 * MS
            }
        );
    }

    #[test]
    fn degrade_mismatch_ns_is_elapsed_time() {
        let mut v = bv();
        v.check(1000, Some(100), Some(101), 200, 201); // start at t=1000ns
        let r = v.check(1000 + 300 * MS, Some(100), Some(101), 200, 201);
        match r {
            BboCheckResult::Degrade { mismatch_ns } => assert_eq!(mismatch_ns, 300 * MS),
            other => panic!("expected Degrade, got {other:?}"),
        }
    }

    // --- MarkStale threshold (≥ 1 s) ---

    #[test]
    fn mismatch_at_1s_returns_mark_stale() {
        let mut v = bv();
        v.check(0, Some(100), Some(101), 200, 201);
        let r = v.check(SEC, Some(100), Some(101), 200, 201);
        assert_eq!(r, BboCheckResult::MarkStale { mismatch_ns: SEC });
    }

    #[test]
    fn mismatch_at_2s_returns_mark_stale() {
        let mut v = bv();
        v.check(0, Some(100), Some(101), 200, 201);
        let r = v.check(2 * SEC, Some(100), Some(101), 200, 201);
        assert_eq!(
            r,
            BboCheckResult::MarkStale {
                mismatch_ns: 2 * SEC
            }
        );
    }

    #[test]
    fn mark_stale_mismatch_ns_is_elapsed_time() {
        let mut v = bv();
        v.check(500, Some(100), Some(101), 200, 201);
        let r = v.check(500 + SEC, Some(100), Some(101), 200, 201);
        match r {
            BboCheckResult::MarkStale { mismatch_ns } => assert_eq!(mismatch_ns, SEC),
            other => panic!("expected MarkStale, got {other:?}"),
        }
    }

    // --- clear ---

    #[test]
    fn clear_resets_mismatch_since() {
        let mut v = bv();
        v.check(0, Some(100), Some(101), 200, 201);
        v.clear();
        assert_eq!(v.mismatch_since(), None);
    }

    #[test]
    fn after_clear_fresh_mismatch_restarts_timer_from_zero() {
        let mut v = bv();
        v.check(0, Some(100), Some(101), 200, 201);
        v.clear();
        // A new mismatch right at the degrade threshold should be Ok
        // (timer restarts, so elapsed = 0).
        let r = v.check(300 * MS, Some(100), Some(101), 200, 201);
        assert_eq!(r, BboCheckResult::Ok, "timer should have restarted");
    }

    // --- Bid-only mismatch ---

    #[test]
    fn bid_mismatch_alone_triggers_degrade() {
        let mut v = bv();
        v.check(0, Some(100), Some(101), 200, 101); // bid wrong, ask ok
        let r = v.check(250 * MS, Some(100), Some(101), 200, 101);
        assert_eq!(
            r,
            BboCheckResult::Degrade {
                mismatch_ns: 250 * MS
            }
        );
    }

    // --- Ask-only mismatch ---

    #[test]
    fn ask_mismatch_alone_triggers_degrade() {
        let mut v = bv();
        v.check(0, Some(100), Some(101), 100, 200); // ask wrong, bid ok
        let r = v.check(250 * MS, Some(100), Some(101), 100, 200);
        assert_eq!(
            r,
            BboCheckResult::Degrade {
                mismatch_ns: 250 * MS
            }
        );
    }

    // --- default / accessors ---

    #[test]
    fn default_matches_new() {
        let v = BboValidator::default();
        assert_eq!(v.degrade_ns(), DEGRADE_NS);
        assert_eq!(v.stale_ns(), STALE_NS);
        assert_eq!(v.mismatch_since(), None);
    }

    #[test]
    fn new_validator_has_no_mismatch() {
        let v = BboValidator::new();
        assert_eq!(v.mismatch_since(), None);
    }
}
