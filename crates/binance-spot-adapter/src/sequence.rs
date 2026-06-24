/// Binance Spot depth-update sequence validator (U/u rules, §2.2).
///
/// # State machine
///
/// ```text
///   AwaitingSnapshot ──on_snapshot()──► Bridging{snapshot_id}
///                                            │
///                         final_ ≤ snapshot_id → Discard (stale buffer event)
///                         first ≤ snapshot_id+1 → Active{last_final=final_}  (Apply)
///                         first >  snapshot_id+1 → Stale (Gap)
///
///   Active{last_final} ──validate(first, final_)──►
///                         first == last_final+1 → Active{last_final=final_}  (Apply)
///                         first >  last_final+1 → Stale (Gap)
///                         first <  last_final+1 → Active unchanged            (Discard)
///
///   Stale ──validate()──► Stale  (Buffering — caller stores for recovery)
///   Stale ──on_snapshot()──► Bridging  (recovery path)
///   * ──reset()──► AwaitingSnapshot  (WebSocket reconnect)
/// ```

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

/// Internal state of the sequence validator.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ValidationState {
    /// No snapshot has been applied yet; all deltas should be buffered.
    AwaitingSnapshot,
    /// Snapshot applied with the given `lastUpdateId`; waiting for the first
    /// delta that bridges into the stream (`U ≤ snapshot_id + 1`).
    Bridging { snapshot_id: u64 },
    /// Stream is contiguous; `last_final` is the `u` field of the last accepted delta.
    Active { last_final: u64 },
    /// A gap was detected; `last_valid` is the `u` field before the gap.
    Stale { last_valid: u64 },
}

/// Result of validating a single depth delta.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ValidateResult {
    /// Delta is valid and in sequence — apply to the order book.
    Apply,
    /// Delta predates the current position — discard silently.
    Discard,
    /// A sequence gap was detected before this delta.
    ///
    /// The validator has transitioned to [`ValidationState::Stale`].
    /// The caller should:
    ///   1. Publish a `GapDetected` message.
    ///   2. Mark the book stale (`BookStale`).
    ///   3. Start the recovery procedure (Stage 3.14).
    Gap {
        /// The first-update-id we expected (`last_final + 1`).
        expected: u64,
        /// The first-update-id we received (`> expected`).
        actual: u64,
        /// The last valid final-update-id before the gap.
        last_valid: u64,
    },
    /// Validator is not ready (awaiting snapshot or already stale).
    /// The caller should buffer the delta for replay.
    Buffering,
}

// ---------------------------------------------------------------------------
// SequenceValidator
// ---------------------------------------------------------------------------

/// Validates the `U` / `u` sequence numbers of Binance Spot depth updates.
///
/// Create one instance per symbol.  Call [`on_snapshot`] whenever a REST
/// depth snapshot is applied to the book, then pass every depth delta to
/// [`validate`] before applying it.
///
/// [`on_snapshot`]: SequenceValidator::on_snapshot
/// [`validate`]: SequenceValidator::validate
#[derive(Debug)]
pub struct SequenceValidator {
    state: ValidationState,
}

impl SequenceValidator {
    pub fn new() -> Self {
        Self {
            state: ValidationState::AwaitingSnapshot,
        }
    }

    /// Call this after applying a REST depth snapshot with `last_update_id`.
    ///
    /// Transitions the validator to [`ValidationState::Bridging`] so the next
    /// delta can bridge from the snapshot into the live stream.
    pub fn on_snapshot(&mut self, last_update_id: u64) {
        self.state = ValidationState::Bridging {
            snapshot_id: last_update_id,
        };
    }

    /// Validate an incoming depth delta with Binance fields `U` (`first`) and
    /// `u` (`final_`).
    ///
    /// Returns a [`ValidateResult`] and updates internal state accordingly.
    pub fn validate(&mut self, first: u64, final_: u64) -> ValidateResult {
        match self.state {
            ValidationState::AwaitingSnapshot => ValidateResult::Buffering,

            ValidationState::Bridging { snapshot_id } => {
                if final_ <= snapshot_id {
                    // Entire event predates the snapshot — already incorporated.
                    ValidateResult::Discard
                } else if first <= snapshot_id + 1 {
                    // Bridges: U ≤ snapshot_id+1  AND  u > snapshot_id. §2.2 rule.
                    self.state = ValidationState::Active { last_final: final_ };
                    ValidateResult::Apply
                } else {
                    // Gap immediately after snapshot.
                    let last_valid = snapshot_id;
                    self.state = ValidationState::Stale { last_valid };
                    ValidateResult::Gap {
                        expected: snapshot_id + 1,
                        actual: first,
                        last_valid,
                    }
                }
            }

            ValidationState::Active { last_final } => {
                let expected = last_final + 1;
                if first == expected {
                    // Strict continuity: U == prev_u + 1.
                    self.state = ValidationState::Active { last_final: final_ };
                    ValidateResult::Apply
                } else if first > expected {
                    // Gap.
                    self.state = ValidationState::Stale {
                        last_valid: last_final,
                    };
                    ValidateResult::Gap {
                        expected,
                        actual: first,
                        last_valid: last_final,
                    }
                } else {
                    // Duplicate / overlapping event (first < expected).
                    // State does not change; the caller may discard or inspect.
                    ValidateResult::Discard
                }
            }

            ValidationState::Stale { .. } => ValidateResult::Buffering,
        }
    }

    /// The last valid final-update-id seen before transitioning to Stale,
    /// or the snapshot_id if still in Bridging state, or the last applied
    /// final-update-id in Active state.  Returns `None` in AwaitingSnapshot.
    pub fn last_valid_id(&self) -> Option<u64> {
        match self.state {
            ValidationState::AwaitingSnapshot => None,
            ValidationState::Bridging { snapshot_id } => Some(snapshot_id),
            ValidationState::Active { last_final } => Some(last_final),
            ValidationState::Stale { last_valid } => Some(last_valid),
        }
    }

    /// Reset to `AwaitingSnapshot` (e.g., on WebSocket reconnect).
    pub fn reset(&mut self) {
        self.state = ValidationState::AwaitingSnapshot;
    }

    pub fn state(&self) -> ValidationState {
        self.state
    }
}

impl Default for SequenceValidator {
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
        let v = SequenceValidator::new();
        assert_eq!(v.state(), ValidationState::AwaitingSnapshot);
    }

    #[test]
    fn validate_before_snapshot_returns_buffering() {
        let mut v = SequenceValidator::new();
        assert_eq!(v.validate(1, 5), ValidateResult::Buffering);
        assert_eq!(v.state(), ValidationState::AwaitingSnapshot);
    }

    #[test]
    fn last_valid_id_is_none_before_snapshot() {
        let v = SequenceValidator::new();
        assert_eq!(v.last_valid_id(), None);
    }

    // --- on_snapshot transitions ---

    #[test]
    fn on_snapshot_transitions_to_bridging() {
        let mut v = SequenceValidator::new();
        v.on_snapshot(100);
        assert_eq!(v.state(), ValidationState::Bridging { snapshot_id: 100 });
        assert_eq!(v.last_valid_id(), Some(100));
    }

    // --- bridging: discard ---

    #[test]
    fn bridge_event_entirely_before_snapshot_discarded() {
        let mut v = SequenceValidator::new();
        v.on_snapshot(100);
        // final_ == 100 → still within snapshot range
        assert_eq!(v.validate(95, 100), ValidateResult::Discard);
        assert_eq!(v.state(), ValidationState::Bridging { snapshot_id: 100 });
    }

    #[test]
    fn bridge_event_final_below_snapshot_discarded() {
        let mut v = SequenceValidator::new();
        v.on_snapshot(200);
        assert_eq!(v.validate(180, 199), ValidateResult::Discard);
    }

    // --- bridging: apply (various U values) ---

    #[test]
    fn bridge_event_exact_start_applies() {
        let mut v = SequenceValidator::new();
        v.on_snapshot(100);
        // U == snapshot_id + 1 (exact bridge)
        assert_eq!(v.validate(101, 105), ValidateResult::Apply);
        assert_eq!(v.state(), ValidationState::Active { last_final: 105 });
    }

    #[test]
    fn bridge_event_overlapping_start_applies() {
        let mut v = SequenceValidator::new();
        v.on_snapshot(100);
        // U < snapshot_id + 1 but u > snapshot_id
        assert_eq!(v.validate(98, 103), ValidateResult::Apply);
        assert_eq!(v.state(), ValidationState::Active { last_final: 103 });
    }

    #[test]
    fn bridge_event_u_equals_snapshot_applies() {
        let mut v = SequenceValidator::new();
        v.on_snapshot(100);
        // U == snapshot_id (≤ snapshot_id + 1), u == 101 (> snapshot_id) → valid bridge
        assert_eq!(v.validate(100, 101), ValidateResult::Apply);
    }

    // --- bridging: gap ---

    #[test]
    fn bridge_event_gap_after_snapshot_marks_stale() {
        let mut v = SequenceValidator::new();
        v.on_snapshot(100);
        // U == 103 > 101 → gap
        let r = v.validate(103, 107);
        assert_eq!(
            r,
            ValidateResult::Gap {
                expected: 101,
                actual: 103,
                last_valid: 100
            },
        );
        assert_eq!(v.state(), ValidationState::Stale { last_valid: 100 });
    }

    // --- active: apply ---

    #[test]
    fn active_exact_continuity_applies() {
        let mut v = SequenceValidator::new();
        v.on_snapshot(10);
        v.validate(11, 15); // bridge → Active { last_final: 15 }
        assert_eq!(v.validate(16, 20), ValidateResult::Apply);
        assert_eq!(v.state(), ValidationState::Active { last_final: 20 });
    }

    #[test]
    fn active_chain_of_three_deltas_applies_all() {
        let mut v = SequenceValidator::new();
        v.on_snapshot(0);
        assert_eq!(v.validate(1, 10), ValidateResult::Apply);
        assert_eq!(v.validate(11, 20), ValidateResult::Apply);
        assert_eq!(v.validate(21, 30), ValidateResult::Apply);
        assert_eq!(v.state(), ValidationState::Active { last_final: 30 });
    }

    // --- active: gap ---

    #[test]
    fn active_gap_marks_stale() {
        let mut v = SequenceValidator::new();
        v.on_snapshot(10);
        v.validate(11, 20); // bridge → Active { last_final: 20 }
        let r = v.validate(23, 30); // expected 21, got 23
        assert_eq!(
            r,
            ValidateResult::Gap {
                expected: 21,
                actual: 23,
                last_valid: 20
            },
        );
        assert_eq!(v.state(), ValidationState::Stale { last_valid: 20 });
    }

    // --- active: discard ---

    #[test]
    fn active_duplicate_event_discarded() {
        let mut v = SequenceValidator::new();
        v.on_snapshot(10);
        v.validate(11, 20); // Active { last_final: 20 }
                            // first == 15 < 21 → duplicate
        let r = v.validate(15, 20);
        assert_eq!(r, ValidateResult::Discard);
        // State should not change
        assert_eq!(v.state(), ValidationState::Active { last_final: 20 });
    }

    #[test]
    fn active_overlapping_event_discarded() {
        let mut v = SequenceValidator::new();
        v.on_snapshot(10);
        v.validate(11, 50); // Active { last_final: 50 }
                            // first == 40, expected 51 → old event
        assert_eq!(v.validate(40, 55), ValidateResult::Discard);
        assert_eq!(v.state(), ValidationState::Active { last_final: 50 });
    }

    // --- stale ---

    #[test]
    fn stale_returns_buffering_for_any_delta() {
        let mut v = SequenceValidator::new();
        v.on_snapshot(10);
        v.validate(11, 20);
        v.validate(25, 30); // gap → Stale
        assert_eq!(v.validate(31, 40), ValidateResult::Buffering);
        assert_eq!(v.validate(41, 50), ValidateResult::Buffering);
    }

    #[test]
    fn stale_last_valid_id_is_preserved() {
        let mut v = SequenceValidator::new();
        v.on_snapshot(10);
        v.validate(11, 20);
        v.validate(25, 30); // gap → Stale { last_valid: 20 }
        assert_eq!(v.last_valid_id(), Some(20));
    }

    #[test]
    fn new_snapshot_after_stale_goes_to_bridging() {
        let mut v = SequenceValidator::new();
        v.on_snapshot(10);
        v.validate(11, 20);
        v.validate(25, 30); // → Stale
                            // Recovery: new snapshot
        v.on_snapshot(28);
        assert_eq!(v.state(), ValidationState::Bridging { snapshot_id: 28 });
    }

    // --- reset ---

    #[test]
    fn reset_from_active_returns_to_awaiting_snapshot() {
        let mut v = SequenceValidator::new();
        v.on_snapshot(10);
        v.validate(11, 20); // → Active
        v.reset();
        assert_eq!(v.state(), ValidationState::AwaitingSnapshot);
        assert_eq!(v.last_valid_id(), None);
    }

    #[test]
    fn reset_from_stale_returns_to_awaiting_snapshot() {
        let mut v = SequenceValidator::new();
        v.on_snapshot(10);
        v.validate(11, 20);
        v.validate(25, 30); // → Stale
        v.reset();
        assert_eq!(v.state(), ValidationState::AwaitingSnapshot);
    }

    // --- default ---

    #[test]
    fn default_matches_new() {
        let v = SequenceValidator::default();
        assert_eq!(v.state(), ValidationState::AwaitingSnapshot);
    }
}
