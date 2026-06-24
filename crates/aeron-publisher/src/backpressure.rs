/// Aeron back-pressure escalation policy (§5.3).
///
/// When a [`Publication::offer`] call returns [`OfferResult::BackPressured`]
/// or [`OfferResult::AdminAction`], the caller must retry.  If the publication
/// remains congested, the policy escalates:
///
/// | Elapsed since first back-pressure | Action              |
/// |------------------------------------|---------------------|
/// | < 100 µs                           | Spin-retry silently |
/// | ≥ 100 µs                           | Log a warning once  |
/// | ≥ 1 ms                             | Degrade the circuit |
/// | ≥ 10 ms                            | Restart / failover  |
///
/// This module exposes two types:
/// * [`OfferOutcome`] — the result of one `try_offer` call.
/// * [`BackpressureGuard`] — per-publication state machine.
use tracing::warn;

use crate::publication::{OfferResult, Publication};

// ---------------------------------------------------------------------------
// Wall clock (production path only)
// ---------------------------------------------------------------------------

fn now_nanos() -> i64 {
    use std::time::SystemTime;
    SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos() as i64
}

// ---------------------------------------------------------------------------
// OfferOutcome
// ---------------------------------------------------------------------------

/// Outcome of one [`BackpressureGuard::try_offer`] call.
///
/// The escalation order matches §5.3:
/// `Accepted` — happy path;
/// `Retrying` → `Warned` → `Degrade` → `Restart` — escalating back-pressure;
/// `Closed` — terminal failure.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OfferOutcome {
    /// Message accepted by the publication. Inner value is the new stream position.
    Accepted(i64),
    /// Back-pressured; elapsed < 100 µs. Caller should spin and retry.
    Retrying,
    /// Back-pressured ≥ 100 µs. A warning has been logged once. Keep retrying.
    Warned,
    /// Back-pressured ≥ 1 ms. Caller should degrade the circuit and keep retrying.
    Degrade,
    /// Back-pressured ≥ 10 ms. Caller should restart the shard or trigger failover.
    Restart,
    /// Publication returned `Closed` or `MaxPositionExceeded`. Do not retry.
    Closed,
}

impl OfferOutcome {
    /// `true` when the message was accepted.
    pub fn is_accepted(self) -> bool {
        matches!(self, Self::Accepted(_))
    }

    /// `true` when the caller should call `try_offer` again with the same message.
    ///
    /// `Degrade` is included: the caller should degrade its state *and* continue
    /// retrying until `Restart` or `Closed` signals that the attempt is abandoned.
    pub fn should_retry(self) -> bool {
        matches!(self, Self::Retrying | Self::Warned | Self::Degrade)
    }

    /// `true` when the caller must give up — no further retry is appropriate.
    pub fn is_terminal(self) -> bool {
        matches!(self, Self::Restart | Self::Closed)
    }
}

// ---------------------------------------------------------------------------
// Default thresholds (§5.3)
// ---------------------------------------------------------------------------

/// Elapsed nanoseconds before a warning is logged (100 µs).
pub const DEFAULT_WARN_NS: i64 = 100_000;
/// Elapsed nanoseconds before returning `Degrade` (1 ms).
pub const DEFAULT_DEGRADE_NS: i64 = 1_000_000;
/// Elapsed nanoseconds before returning `Restart` (10 ms).
pub const DEFAULT_RESTART_NS: i64 = 10_000_000;

// ---------------------------------------------------------------------------
// BackpressureGuard
// ---------------------------------------------------------------------------

/// Per-publication back-pressure state machine.
///
/// Tracks when back-pressure started and escalates the [`OfferOutcome`] over
/// time according to §5.3.  The guard never sleeps — the caller controls the
/// retry loop and is free to busy-spin, yield, or sleep between `try_offer`
/// calls.
///
/// # Typical hot-path usage
///
/// ```rust
/// use connector_aeron::{BackpressureGuard, NullPublication, OfferOutcome};
///
/// let mut guard = BackpressureGuard::new();
/// let mut pub_  = NullPublication::default();
///
/// let outcome = loop {
///     let outcome = guard.try_offer(&mut pub_, b"payload");
///     if !outcome.should_retry() { break outcome; }
///     std::hint::spin_loop();
/// };
///
/// assert!(outcome.is_accepted());
/// ```
///
/// # Test usage
///
/// Use [`try_offer_at`] to inject a synthetic timestamp so tests can advance
/// the clock without sleeping:
///
/// ```rust
/// use connector_aeron::{BackpressureGuard, OfferOutcome};
/// use connector_aeron::publication::Publication;
///
/// struct AlwaysBP;
/// impl Publication for AlwaysBP {
///     fn offer(&mut self, _: &[u8]) -> connector_aeron::OfferResult {
///         connector_aeron::OfferResult::BackPressured
///     }
///     fn is_connected(&self) -> bool { true }
/// }
///
/// let mut guard = BackpressureGuard::new();
/// let mut pub_  = AlwaysBP;
///
/// // t = 0: first back-pressure → Retrying
/// assert_eq!(guard.try_offer_at(&mut pub_, b"x", 0), OfferOutcome::Retrying);
/// // t = 100 µs: exactly at warn threshold
/// assert_eq!(guard.try_offer_at(&mut pub_, b"x", 100_000), OfferOutcome::Warned);
/// // t = 1 ms: degrade
/// assert_eq!(guard.try_offer_at(&mut pub_, b"x", 1_000_000), OfferOutcome::Degrade);
/// // t = 10 ms: restart
/// assert_eq!(guard.try_offer_at(&mut pub_, b"x", 10_000_000), OfferOutcome::Restart);
/// ```
///
/// [`try_offer_at`]: BackpressureGuard::try_offer_at
pub struct BackpressureGuard {
    /// Nanosecond timestamp when this back-pressure window started, or `None`.
    bp_start_ns: Option<i64>,
    /// `true` once the warn-threshold log has been emitted for the current window.
    warned: bool,
    /// Warn threshold in nanoseconds (§5.3 default: 100 000).
    pub warn_ns: i64,
    /// Degrade threshold in nanoseconds (§5.3 default: 1 000 000).
    pub degrade_ns: i64,
    /// Restart threshold in nanoseconds (§5.3 default: 10 000 000).
    pub restart_ns: i64,
}

impl BackpressureGuard {
    /// Create a guard with the §5.3 default thresholds.
    ///
    /// `const` so it can be embedded in a larger `const`/`static` struct.
    pub const fn new() -> Self {
        Self::with_thresholds(DEFAULT_WARN_NS, DEFAULT_DEGRADE_NS, DEFAULT_RESTART_NS)
    }

    /// Create a guard with custom thresholds.
    ///
    /// Useful for tuning on specific hardware or for deterministic testing
    /// with tight thresholds (e.g., `warn_ns = 10`, `degrade_ns = 20`, ...).
    pub const fn with_thresholds(warn_ns: i64, degrade_ns: i64, restart_ns: i64) -> Self {
        Self {
            bp_start_ns: None,
            warned: false,
            warn_ns,
            degrade_ns,
            restart_ns,
        }
    }

    // -----------------------------------------------------------------------
    // Production path
    // -----------------------------------------------------------------------

    /// Try to offer `bytes` to `pub_`, applying the §5.3 escalation policy.
    ///
    /// Reads the wall clock via `SystemTime::now()`.
    /// Use [`try_offer_at`] in tests to control time deterministically.
    ///
    /// [`try_offer_at`]: BackpressureGuard::try_offer_at
    #[inline]
    pub fn try_offer<P: Publication>(&mut self, pub_: &mut P, bytes: &[u8]) -> OfferOutcome {
        self.try_offer_at(pub_, bytes, now_nanos())
    }

    // -----------------------------------------------------------------------
    // Testable inner core
    // -----------------------------------------------------------------------

    /// Identical to [`try_offer`] but uses the caller-supplied `now_ns` instead
    /// of the wall clock.  Enables deterministic unit tests without `sleep`.
    ///
    /// [`try_offer`]: BackpressureGuard::try_offer
    pub fn try_offer_at<P: Publication>(
        &mut self,
        pub_: &mut P,
        bytes: &[u8],
        now_ns: i64,
    ) -> OfferOutcome {
        match pub_.offer(bytes) {
            OfferResult::Ok(pos) => {
                self.reset();
                OfferOutcome::Accepted(pos)
            }

            // Both are retryable; AdminAction is typically brief but we still
            // track it in the escalation window so a stalled media driver doesn't
            // block indefinitely.
            OfferResult::BackPressured | OfferResult::AdminAction => {
                // Record the start of this window on the first failure.
                let start = *self.bp_start_ns.get_or_insert(now_ns);
                let elapsed = now_ns.saturating_sub(start);

                if elapsed >= self.restart_ns {
                    // Do NOT reset: the window remains active so the caller can
                    // inspect elapsed_ns() after Restart if needed.
                    OfferOutcome::Restart
                } else if elapsed >= self.degrade_ns {
                    OfferOutcome::Degrade
                } else if elapsed >= self.warn_ns {
                    if !self.warned {
                        warn!(
                            elapsed_ns = elapsed,
                            warn_threshold_ns = self.warn_ns,
                            degrade_threshold_ns = self.degrade_ns,
                            restart_threshold_ns = self.restart_ns,
                            "Aeron back-pressure exceeded warn threshold",
                        );
                        self.warned = true;
                    }
                    OfferOutcome::Warned
                } else {
                    OfferOutcome::Retrying
                }
            }

            // Terminal failures: reset so the guard is ready for the next message.
            OfferResult::Closed | OfferResult::MaxPositionExceeded => {
                self.reset();
                OfferOutcome::Closed
            }
        }
    }

    // -----------------------------------------------------------------------
    // State accessors
    // -----------------------------------------------------------------------

    /// Reset back-pressure state.
    ///
    /// Called automatically when `try_offer` returns `Accepted` or `Closed`.
    /// Call manually to abandon a window after a `Restart` result before
    /// attempting the next message.
    pub fn reset(&mut self) {
        self.bp_start_ns = None;
        self.warned = false;
    }

    /// `true` when the guard is currently tracking a back-pressure window.
    pub fn is_in_backpressure(&self) -> bool {
        self.bp_start_ns.is_some()
    }

    /// Nanoseconds elapsed since back-pressure started, given `now_ns`.
    ///
    /// Returns `None` when not in a back-pressure window.
    pub fn elapsed_ns(&self, now_ns: i64) -> Option<i64> {
        self.bp_start_ns.map(|start| now_ns.saturating_sub(start))
    }
}

impl Default for BackpressureGuard {
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
    use crate::publication::{NullPublication, OfferResult, Publication};

    // -----------------------------------------------------------------------
    // Test-only publication stubs
    // -----------------------------------------------------------------------

    /// Always returns BackPressured.
    struct AlwaysBP;
    impl Publication for AlwaysBP {
        fn offer(&mut self, _: &[u8]) -> OfferResult {
            OfferResult::BackPressured
        }
        fn is_connected(&self) -> bool {
            true
        }
    }

    /// Always returns AdminAction.
    struct AlwaysAdmin;
    impl Publication for AlwaysAdmin {
        fn offer(&mut self, _: &[u8]) -> OfferResult {
            OfferResult::AdminAction
        }
        fn is_connected(&self) -> bool {
            true
        }
    }

    /// Always returns Closed.
    struct AlwaysClosed;
    impl Publication for AlwaysClosed {
        fn offer(&mut self, _: &[u8]) -> OfferResult {
            OfferResult::Closed
        }
        fn is_connected(&self) -> bool {
            false
        }
    }

    /// Returns MaxPositionExceeded.
    struct AlwaysMaxPos;
    impl Publication for AlwaysMaxPos {
        fn offer(&mut self, _: &[u8]) -> OfferResult {
            OfferResult::MaxPositionExceeded
        }
        fn is_connected(&self) -> bool {
            true
        }
    }

    /// Fails for the first `fail_count` calls, then succeeds at position `fail_count + 1`.
    struct FailThenOk {
        remaining: u32,
    }
    impl FailThenOk {
        fn new(n: u32) -> Self {
            Self { remaining: n }
        }
    }
    impl Publication for FailThenOk {
        fn offer(&mut self, _: &[u8]) -> OfferResult {
            if self.remaining > 0 {
                self.remaining -= 1;
                OfferResult::BackPressured
            } else {
                OfferResult::Ok(42)
            }
        }
        fn is_connected(&self) -> bool {
            true
        }
    }

    fn guard() -> BackpressureGuard {
        BackpressureGuard::new()
    }

    // -----------------------------------------------------------------------
    // OfferOutcome helpers
    // -----------------------------------------------------------------------

    #[test]
    fn accepted_is_accepted_and_not_terminal_or_retry() {
        let o = OfferOutcome::Accepted(1);
        assert!(o.is_accepted());
        assert!(!o.should_retry());
        assert!(!o.is_terminal());
    }

    #[test]
    fn retrying_should_retry_only() {
        let o = OfferOutcome::Retrying;
        assert!(!o.is_accepted());
        assert!(o.should_retry());
        assert!(!o.is_terminal());
    }

    #[test]
    fn warned_should_retry_only() {
        assert!(OfferOutcome::Warned.should_retry());
        assert!(!OfferOutcome::Warned.is_terminal());
    }

    #[test]
    fn degrade_should_retry_and_not_terminal() {
        assert!(OfferOutcome::Degrade.should_retry());
        assert!(!OfferOutcome::Degrade.is_terminal());
    }

    #[test]
    fn restart_is_terminal_and_not_retry() {
        assert!(OfferOutcome::Restart.is_terminal());
        assert!(!OfferOutcome::Restart.should_retry());
    }

    #[test]
    fn closed_is_terminal_and_not_retry() {
        assert!(OfferOutcome::Closed.is_terminal());
        assert!(!OfferOutcome::Closed.should_retry());
    }

    // -----------------------------------------------------------------------
    // BackpressureGuard — initial state
    // -----------------------------------------------------------------------

    #[test]
    fn new_guard_is_not_in_backpressure() {
        let g = guard();
        assert!(!g.is_in_backpressure());
    }

    #[test]
    fn new_guard_elapsed_is_none() {
        let g = guard();
        assert!(g.elapsed_ns(1_000_000).is_none());
    }

    // -----------------------------------------------------------------------
    // Happy path
    // -----------------------------------------------------------------------

    #[test]
    fn accepted_returns_correct_position() {
        let mut g = guard();
        let mut p = NullPublication::default();
        p.offer(b"seed"); // position = 4
        let outcome = g.try_offer(&mut p, b"hello");
        assert_eq!(outcome, OfferOutcome::Accepted(9));
    }

    #[test]
    fn accepted_keeps_guard_in_clean_state() {
        let mut g = guard();
        let mut p = NullPublication::default();
        g.try_offer(&mut p, b"msg");
        assert!(!g.is_in_backpressure());
        assert!(g.elapsed_ns(0).is_none());
    }

    // -----------------------------------------------------------------------
    // Escalation ladder (time injected via try_offer_at)
    // -----------------------------------------------------------------------

    #[test]
    fn retrying_below_warn_threshold() {
        let mut g = guard();
        let mut p = AlwaysBP;
        // t=0: start of window
        assert_eq!(g.try_offer_at(&mut p, b"x", 0), OfferOutcome::Retrying);
        // t=99µs: still below 100µs
        assert_eq!(g.try_offer_at(&mut p, b"x", 99_999), OfferOutcome::Retrying);
    }

    #[test]
    fn warned_at_exactly_warn_threshold() {
        let mut g = guard();
        let mut p = AlwaysBP;
        g.try_offer_at(&mut p, b"x", 0); // starts window
                                         // exactly 100µs elapsed
        assert_eq!(g.try_offer_at(&mut p, b"x", 100_000), OfferOutcome::Warned);
    }

    #[test]
    fn warned_second_call_at_same_elapsed_returns_warned_again() {
        let mut g = guard();
        let mut p = AlwaysBP;
        g.try_offer_at(&mut p, b"x", 0);
        g.try_offer_at(&mut p, b"x", 100_000); // first Warned
                                               // second call at same elapsed — still Warned, warn not re-logged
        assert_eq!(g.try_offer_at(&mut p, b"x", 100_000), OfferOutcome::Warned);
    }

    #[test]
    fn degrade_at_exactly_degrade_threshold() {
        let mut g = guard();
        let mut p = AlwaysBP;
        g.try_offer_at(&mut p, b"x", 0);
        assert_eq!(
            g.try_offer_at(&mut p, b"x", 1_000_000),
            OfferOutcome::Degrade
        );
    }

    #[test]
    fn degrade_between_degrade_and_restart_threshold() {
        let mut g = guard();
        let mut p = AlwaysBP;
        g.try_offer_at(&mut p, b"x", 0);
        assert_eq!(
            g.try_offer_at(&mut p, b"x", 5_000_000),
            OfferOutcome::Degrade
        );
    }

    #[test]
    fn restart_at_exactly_restart_threshold() {
        let mut g = guard();
        let mut p = AlwaysBP;
        g.try_offer_at(&mut p, b"x", 0);
        assert_eq!(
            g.try_offer_at(&mut p, b"x", 10_000_000),
            OfferOutcome::Restart
        );
    }

    #[test]
    fn restart_beyond_restart_threshold() {
        let mut g = guard();
        let mut p = AlwaysBP;
        g.try_offer_at(&mut p, b"x", 0);
        assert_eq!(
            g.try_offer_at(&mut p, b"x", 50_000_000),
            OfferOutcome::Restart
        );
    }

    #[test]
    fn full_escalation_sequence() {
        let mut g = guard();
        let mut p = AlwaysBP;

        assert_eq!(g.try_offer_at(&mut p, b"x", 0), OfferOutcome::Retrying);
        assert_eq!(g.try_offer_at(&mut p, b"x", 50_000), OfferOutcome::Retrying);
        assert_eq!(g.try_offer_at(&mut p, b"x", 100_000), OfferOutcome::Warned);
        assert_eq!(g.try_offer_at(&mut p, b"x", 500_000), OfferOutcome::Warned);
        assert_eq!(
            g.try_offer_at(&mut p, b"x", 1_000_000),
            OfferOutcome::Degrade
        );
        assert_eq!(
            g.try_offer_at(&mut p, b"x", 5_000_000),
            OfferOutcome::Degrade
        );
        assert_eq!(
            g.try_offer_at(&mut p, b"x", 10_000_000),
            OfferOutcome::Restart
        );
        assert_eq!(
            g.try_offer_at(&mut p, b"x", 100_000_000),
            OfferOutcome::Restart
        );
    }

    // -----------------------------------------------------------------------
    // Back-pressure window start time
    // -----------------------------------------------------------------------

    #[test]
    fn start_time_recorded_at_first_failure() {
        let mut g = guard();
        let mut p = AlwaysBP;
        g.try_offer_at(&mut p, b"x", 1_000); // start = 1_000
        g.try_offer_at(&mut p, b"x", 2_000); // should NOT move start
                                             // elapsed relative to start=1_000 at now=3_000 is 2_000, not 1_000
        assert_eq!(g.elapsed_ns(3_000), Some(2_000));
    }

    #[test]
    fn elapsed_ns_tracks_window_duration() {
        let mut g = guard();
        let mut p = AlwaysBP;
        g.try_offer_at(&mut p, b"x", 0);
        assert_eq!(g.elapsed_ns(500_000), Some(500_000));
    }

    // -----------------------------------------------------------------------
    // State accessors
    // -----------------------------------------------------------------------

    #[test]
    fn is_in_backpressure_true_after_first_bp_result() {
        let mut g = guard();
        let mut p = AlwaysBP;
        g.try_offer_at(&mut p, b"x", 0);
        assert!(g.is_in_backpressure());
    }

    #[test]
    fn is_in_backpressure_false_after_manual_reset() {
        let mut g = guard();
        let mut p = AlwaysBP;
        g.try_offer_at(&mut p, b"x", 0);
        g.reset();
        assert!(!g.is_in_backpressure());
    }

    #[test]
    fn manual_reset_clears_warned_flag() {
        let mut g = guard();
        let mut p = AlwaysBP;
        g.try_offer_at(&mut p, b"x", 0);
        g.try_offer_at(&mut p, b"x", 200_000); // sets warned = true
        g.reset();
        // After reset, a fresh window starts; below warn threshold → Retrying (not Warned)
        g.try_offer_at(&mut p, b"x", 0);
        assert_eq!(g.try_offer_at(&mut p, b"x", 50_000), OfferOutcome::Retrying);
    }

    #[test]
    fn accepted_after_backpressure_resets_guard() {
        let mut g = guard();
        let mut p = FailThenOk::new(3);
        g.try_offer_at(&mut p, b"x", 0);
        g.try_offer_at(&mut p, b"x", 50_000);
        g.try_offer_at(&mut p, b"x", 100_000);
        // 4th call succeeds
        let outcome = g.try_offer_at(&mut p, b"x", 200_000);
        assert!(outcome.is_accepted());
        assert!(!g.is_in_backpressure());
        assert!(g.elapsed_ns(200_000).is_none());
    }

    // -----------------------------------------------------------------------
    // Terminal results
    // -----------------------------------------------------------------------

    #[test]
    fn closed_publication_returns_closed() {
        let mut g = guard();
        let mut p = AlwaysClosed;
        let outcome = g.try_offer(&mut p, b"x");
        assert_eq!(outcome, OfferOutcome::Closed);
        assert!(outcome.is_terminal());
    }

    #[test]
    fn closed_publication_resets_guard() {
        let mut g = guard();
        let mut p = AlwaysClosed;
        g.try_offer(&mut p, b"x");
        // Guard should be clean (reset called internally)
        assert!(!g.is_in_backpressure());
    }

    #[test]
    fn max_position_exceeded_returns_closed() {
        let mut g = guard();
        let mut p = AlwaysMaxPos;
        assert_eq!(g.try_offer(&mut p, b"x"), OfferOutcome::Closed);
    }

    // -----------------------------------------------------------------------
    // AdminAction
    // -----------------------------------------------------------------------

    #[test]
    fn admin_action_enters_backpressure_window() {
        let mut g = guard();
        let mut p = AlwaysAdmin;
        g.try_offer_at(&mut p, b"x", 0);
        assert!(g.is_in_backpressure());
    }

    #[test]
    fn admin_action_escalates_like_backpressure() {
        let mut g = guard();
        let mut p = AlwaysAdmin;
        g.try_offer_at(&mut p, b"x", 0);
        assert_eq!(
            g.try_offer_at(&mut p, b"x", 10_000_000),
            OfferOutcome::Restart
        );
    }

    // -----------------------------------------------------------------------
    // Custom thresholds
    // -----------------------------------------------------------------------

    #[test]
    fn custom_thresholds_respected() {
        // Tight thresholds for unit-test convenience
        let mut g = BackpressureGuard::with_thresholds(10, 20, 30);
        let mut p = AlwaysBP;
        assert_eq!(g.try_offer_at(&mut p, b"x", 0), OfferOutcome::Retrying); // elapsed 0 < 10
        assert_eq!(g.try_offer_at(&mut p, b"x", 10), OfferOutcome::Warned); // elapsed 10 ≥ 10
        assert_eq!(g.try_offer_at(&mut p, b"x", 20), OfferOutcome::Degrade); // elapsed 20 ≥ 20
        assert_eq!(g.try_offer_at(&mut p, b"x", 30), OfferOutcome::Restart); // elapsed 30 ≥ 30
    }

    #[test]
    fn default_and_new_have_same_thresholds() {
        let a = BackpressureGuard::new();
        let b = BackpressureGuard::default();
        assert_eq!(a.warn_ns, b.warn_ns);
        assert_eq!(a.degrade_ns, b.degrade_ns);
        assert_eq!(a.restart_ns, b.restart_ns);
    }

    #[test]
    fn default_thresholds_match_spec() {
        let g = BackpressureGuard::new();
        assert_eq!(g.warn_ns, DEFAULT_WARN_NS);
        assert_eq!(g.degrade_ns, DEFAULT_DEGRADE_NS);
        assert_eq!(g.restart_ns, DEFAULT_RESTART_NS);
        assert_eq!(DEFAULT_WARN_NS, 100_000);
        assert_eq!(DEFAULT_DEGRADE_NS, 1_000_000);
        assert_eq!(DEFAULT_RESTART_NS, 10_000_000);
    }

    // -----------------------------------------------------------------------
    // Integration: full spin loop with FailThenOk
    // -----------------------------------------------------------------------

    #[test]
    fn spin_loop_accepts_after_transient_backpressure() {
        let mut g = BackpressureGuard::new();
        let mut p = FailThenOk::new(5);

        let mut t: i64 = 0;
        let outcome = loop {
            let o = g.try_offer_at(&mut p, b"msg", t);
            if !o.should_retry() {
                break o;
            }
            t += 10_000; // advance 10µs per attempt
        };

        // 5 failures × 10µs = 50µs total — below the 100µs warn threshold
        assert!(outcome.is_accepted(), "expected Accepted, got {outcome:?}");
        assert!(!g.is_in_backpressure());
    }

    #[test]
    fn spin_loop_escalates_through_all_stages() {
        // Use very large publication backlog so it never succeeds.
        let mut g = BackpressureGuard::new();
        let mut p = FailThenOk::new(u32::MAX);
        let mut outcomes = Vec::new();

        // Phase 1: spin below warn threshold (50µs steps)
        let mut t: i64 = 0;
        for _ in 0..2 {
            outcomes.push(g.try_offer_at(&mut p, b"m", t));
            t += 50_000;
        }
        // Phase 2: cross warn threshold (100µs total elapsed)
        outcomes.push(g.try_offer_at(&mut p, b"m", t));
        // Phase 3: cross degrade threshold (jump to 1ms elapsed)
        t = 1_000_000;
        outcomes.push(g.try_offer_at(&mut p, b"m", t));
        // Phase 4: cross restart threshold (jump to 10ms elapsed)
        t = 10_000_000;
        outcomes.push(g.try_offer_at(&mut p, b"m", t));

        assert!(
            outcomes.contains(&OfferOutcome::Retrying),
            "expected Retrying: {outcomes:?}"
        );
        assert!(
            outcomes.contains(&OfferOutcome::Warned),
            "expected Warned: {outcomes:?}"
        );
        assert!(
            outcomes.contains(&OfferOutcome::Degrade),
            "expected Degrade: {outcomes:?}"
        );
        assert!(
            outcomes.contains(&OfferOutcome::Restart),
            "expected Restart: {outcomes:?}"
        );
    }
}
