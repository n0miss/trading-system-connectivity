//! Binance USDT-M Futures depth-update sequence validator (§5.24).
//!
//! # Key difference vs Spot
//!
//! In Active state the Spot validator checks `U == last_final + 1` (the
//! first-update-id of the new event must immediately follow the last accepted
//! final-update-id).  Futures events carry a `pu` (prev_final_update_id) field
//! that Binance sets to the exact `u` of the previous event, so the check
//! becomes `pu == last_final`.  This is more robust because Futures update IDs
//! do not increment strictly by one — there can be events with no changes that
//! still advance the ID by more than one.
//!
//! # State machine
//!
//! ```text
//!   AwaitingSnapshot ──on_snapshot(last_update_id)──► Bridging{snapshot_id}
//!
//!   Bridging{snapshot_id}:
//!     u ≤ snapshot_id                   → Discard   (predates snapshot)
//!     U ≤ snapshot_id + 1, u > snapshot → Active{last_final = u}   (Apply)
//!     U >  snapshot_id + 1              → Stale (gap right after snapshot)
//!
//!   Active{last_final}:
//!     pu == last_final → Active{last_final = u}   (Apply)
//!     pu <  last_final → Discard  (duplicate / old event)
//!     pu >  last_final → Stale    (gap)
//!
//!   Stale ──validate()──► Stale  (Buffering)
//!   Stale ──on_snapshot()──► Bridging  (recovery path)
//!   *     ──reset()──►     AwaitingSnapshot  (WebSocket reconnect)
//! ```
//!
//! # Parameters for `validate`
//!
//! Pass the three depth-event fields verbatim:
//! - `first` → `U` (first update ID in this event)
//! - `final_` → `u` (final update ID in this event)
//! - `prev_final` → `pu` (final update ID of the previous event)

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

/// Internal state of the futures sequence validator.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ValidationState {
    /// No snapshot applied yet; all deltas should be buffered.
    AwaitingSnapshot,
    /// Snapshot applied with `snapshot_id`; waiting for a delta that bridges
    /// into the stream (U ≤ snapshot_id + 1, u > snapshot_id).
    Bridging { snapshot_id: u64 },
    /// Stream is contiguous; `last_final` is the `u` field of the last accepted
    /// delta.  Gap detection uses `pu == last_final`.
    Active { last_final: u64 },
    /// A gap was detected; validator halts until a new snapshot is applied.
    Stale { last_valid: u64 },
}

/// Result of validating a single depth delta.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ValidateResult {
    /// Delta is in sequence — apply it to the order book.
    Apply,
    /// Delta predates the current position — discard silently.
    Discard,
    /// A sequence gap was detected before this delta.
    ///
    /// The validator transitions to [`ValidationState::Stale`].
    /// The caller should:
    ///  1. Publish a `GapDetected` message.
    ///  2. Mark the book stale.
    ///  3. Start the recovery procedure (fetch snapshot, re-bridge).
    Gap {
        /// The `pu` value we expected (`== last_final`).
        expected_pu: u64,
        /// The `pu` value we actually received (`> last_final`).
        actual_pu: u64,
        /// The last valid `u` before the gap.
        last_valid: u64,
    },
    /// Validator is not ready (awaiting snapshot or stale).
    /// The caller should buffer the delta for replay.
    Buffering,
}

// ---------------------------------------------------------------------------
// FuturesSequenceValidator
// ---------------------------------------------------------------------------

/// Validates the `U` / `u` / `pu` sequence numbers of Binance USDT-M Futures
/// depth updates.
///
/// Create one instance per symbol.  Call [`on_snapshot`] after applying a REST
/// depth snapshot, then pass every depth delta to [`validate`].
///
/// [`on_snapshot`]: FuturesSequenceValidator::on_snapshot
/// [`validate`]: FuturesSequenceValidator::validate
#[derive(Debug)]
pub struct FuturesSequenceValidator {
    state: ValidationState,
}

impl FuturesSequenceValidator {
    pub fn new() -> Self {
        Self {
            state: ValidationState::AwaitingSnapshot,
        }
    }

    /// Call after applying a REST depth snapshot with `last_update_id`.
    ///
    /// Transitions to [`ValidationState::Bridging`] so the next delta can
    /// bridge from the snapshot into the live stream.
    pub fn on_snapshot(&mut self, last_update_id: u64) {
        self.state = ValidationState::Bridging {
            snapshot_id: last_update_id,
        };
    }

    /// Validate an incoming depth delta.
    ///
    /// * `first`      — the `U` field (first update ID in this event)
    /// * `final_`     — the `u` field (final update ID in this event)
    /// * `prev_final` — the `pu` field (final update ID of the previous event)
    pub fn validate(&mut self, first: u64, final_: u64, prev_final: u64) -> ValidateResult {
        match self.state {
            ValidationState::AwaitingSnapshot => ValidateResult::Buffering,

            ValidationState::Bridging { snapshot_id } => {
                if final_ <= snapshot_id {
                    // Entire event predates the snapshot — already incorporated.
                    ValidateResult::Discard
                } else if first <= snapshot_id + 1 {
                    // U ≤ snapshot_id + 1 AND u > snapshot_id → valid bridge.
                    self.state = ValidationState::Active { last_final: final_ };
                    ValidateResult::Apply
                } else {
                    // Gap immediately after snapshot.
                    let last_valid = snapshot_id;
                    self.state = ValidationState::Stale { last_valid };
                    ValidateResult::Gap {
                        expected_pu: last_valid,
                        actual_pu: prev_final,
                        last_valid,
                    }
                }
            }

            ValidationState::Active { last_final } => {
                if prev_final == last_final {
                    // Futures gap check: pu must equal the previous event's u.
                    self.state = ValidationState::Active { last_final: final_ };
                    ValidateResult::Apply
                } else if prev_final < last_final {
                    // Old event — pu points to an earlier position than where we are.
                    ValidateResult::Discard
                } else {
                    // pu > last_final: event skips ahead — gap.
                    self.state = ValidationState::Stale {
                        last_valid: last_final,
                    };
                    ValidateResult::Gap {
                        expected_pu: last_final,
                        actual_pu: prev_final,
                        last_valid: last_final,
                    }
                }
            }

            ValidationState::Stale { .. } => ValidateResult::Buffering,
        }
    }

    /// Reset to `AwaitingSnapshot` (e.g., on WebSocket reconnect).
    pub fn reset(&mut self) {
        self.state = ValidationState::AwaitingSnapshot;
    }

    pub fn state(&self) -> ValidationState {
        self.state
    }

    /// Last accepted final-update-id, or `None` in `AwaitingSnapshot`.
    pub fn last_valid_id(&self) -> Option<u64> {
        match self.state {
            ValidationState::AwaitingSnapshot => None,
            ValidationState::Bridging { snapshot_id } => Some(snapshot_id),
            ValidationState::Active { last_final } => Some(last_final),
            ValidationState::Stale { last_valid } => Some(last_valid),
        }
    }
}

impl Default for FuturesSequenceValidator {
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

    // --- initial state ---

    #[test]
    fn new_validator_is_awaiting_snapshot() {
        let v = FuturesSequenceValidator::new();
        assert_eq!(v.state(), ValidationState::AwaitingSnapshot);
    }

    #[test]
    fn validate_before_snapshot_returns_buffering() {
        let mut v = FuturesSequenceValidator::new();
        // pu=0 is meaningless before snapshot
        assert_eq!(v.validate(1, 5, 0), ValidateResult::Buffering);
        assert_eq!(v.state(), ValidationState::AwaitingSnapshot);
    }

    #[test]
    fn last_valid_id_is_none_before_snapshot() {
        let v = FuturesSequenceValidator::new();
        assert_eq!(v.last_valid_id(), None);
    }

    // --- on_snapshot transitions ---

    #[test]
    fn on_snapshot_transitions_to_bridging() {
        let mut v = FuturesSequenceValidator::new();
        v.on_snapshot(1000);
        assert_eq!(v.state(), ValidationState::Bridging { snapshot_id: 1000 });
        assert_eq!(v.last_valid_id(), Some(1000));
    }

    // --- bridging: discard ---

    #[test]
    fn bridge_event_final_at_snapshot_discarded() {
        let mut v = FuturesSequenceValidator::new();
        v.on_snapshot(100);
        // u == 100 ≤ snapshot_id → predates snapshot
        assert_eq!(v.validate(95, 100, 94), ValidateResult::Discard);
        assert_eq!(v.state(), ValidationState::Bridging { snapshot_id: 100 });
    }

    #[test]
    fn bridge_event_final_below_snapshot_discarded() {
        let mut v = FuturesSequenceValidator::new();
        v.on_snapshot(200);
        assert_eq!(v.validate(180, 199, 179), ValidateResult::Discard);
    }

    // --- bridging: apply ---

    #[test]
    fn bridge_exact_start_applies() {
        let mut v = FuturesSequenceValidator::new();
        v.on_snapshot(100);
        // U == 101 == snapshot_id + 1 (exact bridge)
        assert_eq!(v.validate(101, 105, 100), ValidateResult::Apply);
        assert_eq!(v.state(), ValidationState::Active { last_final: 105 });
    }

    #[test]
    fn bridge_overlapping_start_applies() {
        let mut v = FuturesSequenceValidator::new();
        v.on_snapshot(100);
        // U == 98 < snapshot_id + 1, u == 103 > snapshot_id → valid bridge
        assert_eq!(v.validate(98, 103, 95), ValidateResult::Apply);
        assert_eq!(v.state(), ValidationState::Active { last_final: 103 });
    }

    #[test]
    fn bridge_u_equals_snapshot_applies() {
        let mut v = FuturesSequenceValidator::new();
        v.on_snapshot(100);
        // U == 100 ≤ 101, u == 101 > 100 → valid bridge
        assert_eq!(v.validate(100, 101, 99), ValidateResult::Apply);
    }

    // --- bridging: gap ---

    #[test]
    fn bridge_gap_after_snapshot_marks_stale() {
        let mut v = FuturesSequenceValidator::new();
        v.on_snapshot(100);
        // U == 103 > 101 → gap
        let r = v.validate(103, 107, 102);
        assert_eq!(
            r,
            ValidateResult::Gap {
                expected_pu: 100,
                actual_pu: 102,
                last_valid: 100
            },
        );
        assert_eq!(v.state(), ValidationState::Stale { last_valid: 100 });
    }

    // --- active: apply via pu ---

    #[test]
    fn active_exact_pu_match_applies() {
        let mut v = FuturesSequenceValidator::new();
        v.on_snapshot(10);
        v.validate(11, 20, 10); // bridge → Active { last_final: 20 }
                                // Next event: pu == 20 == last_final
        assert_eq!(v.validate(21, 30, 20), ValidateResult::Apply);
        assert_eq!(v.state(), ValidationState::Active { last_final: 30 });
    }

    #[test]
    fn active_chain_of_three_events_all_apply() {
        let mut v = FuturesSequenceValidator::new();
        v.on_snapshot(0);
        assert_eq!(v.validate(1, 10, 0), ValidateResult::Apply); // bridge
        assert_eq!(v.validate(11, 25, 10), ValidateResult::Apply);
        assert_eq!(v.validate(26, 40, 25), ValidateResult::Apply);
        assert_eq!(v.state(), ValidationState::Active { last_final: 40 });
    }

    #[test]
    fn active_futures_id_can_jump_by_more_than_one_and_still_apply() {
        // Futures IDs may skip values between events — only pu matters.
        let mut v = FuturesSequenceValidator::new();
        v.on_snapshot(100);
        v.validate(101, 200, 100); // bridge → Active { last_final: 200 }
                                   // Next event skips U from 201 to 300 — valid because pu == 200
        assert_eq!(v.validate(300, 500, 200), ValidateResult::Apply);
    }

    // --- active: gap ---

    #[test]
    fn active_pu_ahead_of_last_final_is_a_gap() {
        let mut v = FuturesSequenceValidator::new();
        v.on_snapshot(10);
        v.validate(11, 20, 10); // → Active { last_final: 20 }
                                // pu == 25 > 20 → gap
        let r = v.validate(26, 35, 25);
        assert_eq!(
            r,
            ValidateResult::Gap {
                expected_pu: 20,
                actual_pu: 25,
                last_valid: 20
            },
        );
        assert_eq!(v.state(), ValidationState::Stale { last_valid: 20 });
    }

    // --- active: discard ---

    #[test]
    fn active_pu_below_last_final_discards() {
        let mut v = FuturesSequenceValidator::new();
        v.on_snapshot(10);
        v.validate(11, 20, 10); // → Active { last_final: 20 }
                                // pu == 15 < 20 → old event
        assert_eq!(v.validate(16, 22, 15), ValidateResult::Discard);
        // State must not change
        assert_eq!(v.state(), ValidationState::Active { last_final: 20 });
    }

    #[test]
    fn active_pu_equals_zero_when_last_final_nonzero_discards() {
        let mut v = FuturesSequenceValidator::new();
        v.on_snapshot(5);
        v.validate(6, 10, 5); // → Active { last_final: 10 }
        assert_eq!(v.validate(1, 3, 0), ValidateResult::Discard);
    }

    // --- stale ---

    #[test]
    fn stale_returns_buffering_for_any_delta() {
        let mut v = FuturesSequenceValidator::new();
        v.on_snapshot(10);
        v.validate(11, 20, 10);
        v.validate(22, 30, 25); // gap → Stale
        assert_eq!(v.validate(31, 40, 30), ValidateResult::Buffering);
        assert_eq!(v.validate(41, 50, 40), ValidateResult::Buffering);
    }

    #[test]
    fn stale_last_valid_id_preserved() {
        let mut v = FuturesSequenceValidator::new();
        v.on_snapshot(10);
        v.validate(11, 20, 10);
        v.validate(22, 30, 25); // gap → Stale { last_valid: 20 }
        assert_eq!(v.last_valid_id(), Some(20));
    }

    #[test]
    fn new_snapshot_after_stale_goes_to_bridging() {
        let mut v = FuturesSequenceValidator::new();
        v.on_snapshot(10);
        v.validate(11, 20, 10);
        v.validate(22, 30, 25); // → Stale
        v.on_snapshot(28);
        assert_eq!(v.state(), ValidationState::Bridging { snapshot_id: 28 });
    }

    // --- reset ---

    #[test]
    fn reset_from_active_returns_to_awaiting_snapshot() {
        let mut v = FuturesSequenceValidator::new();
        v.on_snapshot(10);
        v.validate(11, 20, 10); // → Active
        v.reset();
        assert_eq!(v.state(), ValidationState::AwaitingSnapshot);
        assert_eq!(v.last_valid_id(), None);
    }

    #[test]
    fn reset_from_stale_returns_to_awaiting_snapshot() {
        let mut v = FuturesSequenceValidator::new();
        v.on_snapshot(10);
        v.validate(11, 20, 10);
        v.validate(22, 30, 25); // → Stale
        v.reset();
        assert_eq!(v.state(), ValidationState::AwaitingSnapshot);
    }

    // --- default ---

    #[test]
    fn default_matches_new() {
        let v = FuturesSequenceValidator::default();
        assert_eq!(v.state(), ValidationState::AwaitingSnapshot);
    }
}
