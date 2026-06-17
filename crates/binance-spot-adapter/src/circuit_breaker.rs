/// Recovery circuit breaker (§3.15).
///
/// Limits how many consecutive recovery attempts can fail before the channel
/// is suspended.  After `max_attempts` consecutive failures the circuit opens
/// and no further recovery is attempted until the `cooldown_ns` window expires.
/// A single success resets the failure counter and closes the circuit.
///
/// Defaults match the SPEC: **5 attempts / 30 s cooldown**.

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

pub const MAX_ATTEMPTS: u32 = 5;
pub const COOLDOWN_NS:  i64 = 30_000_000_000; // 30 s

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

/// Observable circuit state returned by [`CircuitBreaker::check`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CircuitState {
    /// Circuit is closed; a recovery attempt is allowed.
    Closed,
    /// Circuit is open; no recovery until `retry_after_ns` (nanosecond epoch).
    Open { retry_after_ns: i64 },
}

impl CircuitState {
    pub fn is_open(self) -> bool {
        matches!(self, Self::Open { .. })
    }
}

// ---------------------------------------------------------------------------
// CircuitBreaker
// ---------------------------------------------------------------------------

/// Consecutive-failure circuit breaker for the Spot recovery path.
///
/// Create one per symbol (or one per channel in a multi-symbol setup).
#[derive(Debug)]
pub struct CircuitBreaker {
    failures:     u32,
    max_attempts: u32,
    cooldown_ns:  i64,
    /// Nanosecond epoch timestamp at which the cooldown expires.
    /// `None` while the circuit is closed.
    open_until:   Option<i64>,
}

impl CircuitBreaker {
    /// Create a circuit breaker with the §3.15 defaults (5 attempts / 30 s).
    pub fn new() -> Self {
        Self::with_limits(MAX_ATTEMPTS, COOLDOWN_NS)
    }

    /// Create a circuit breaker with custom limits (useful for tests).
    pub fn with_limits(max_attempts: u32, cooldown_ns: i64) -> Self {
        Self {
            failures:     0,
            max_attempts,
            cooldown_ns,
            open_until:   None,
        }
    }

    // --- State query --------------------------------------------------------

    /// Return the current circuit state.
    ///
    /// If the circuit is open but the cooldown has expired, it is
    /// **automatically reset** to `Closed` and the failure counter is cleared.
    ///
    /// Call this before every recovery attempt.
    pub fn check(&mut self, now_ns: i64) -> CircuitState {
        if let Some(until) = self.open_until {
            if now_ns < until {
                return CircuitState::Open { retry_after_ns: until };
            }
            // Cooldown expired — auto-reset.
            self.failures   = 0;
            self.open_until = None;
        }
        CircuitState::Closed
    }

    // --- Outcome recording --------------------------------------------------

    /// Record a successful recovery attempt.
    ///
    /// Resets the failure counter and closes the circuit unconditionally.
    pub fn record_success(&mut self) {
        self.failures   = 0;
        self.open_until = None;
    }

    /// Record a failed recovery attempt.
    ///
    /// Returns `true` if this failure **just opened** the circuit (i.e., the
    /// circuit was closed and now transitions to open).  Returns `false` if the
    /// circuit was already open or if the failure count is still below the
    /// threshold.
    pub fn record_failure(&mut self, now_ns: i64) -> bool {
        if self.open_until.is_some() {
            // Already open — don't restart the cooldown timer.
            return false;
        }
        self.failures += 1;
        if self.failures >= self.max_attempts {
            self.open_until = Some(now_ns + self.cooldown_ns);
            true
        } else {
            false
        }
    }

    // --- Accessors ----------------------------------------------------------

    pub fn failures(&self)     -> u32       { self.failures }
    pub fn max_attempts(&self) -> u32       { self.max_attempts }
    pub fn cooldown_ns(&self)  -> i64       { self.cooldown_ns }
    pub fn open_until(&self)   -> Option<i64> { self.open_until }
}

impl Default for CircuitBreaker {
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

    const SEC: i64 = 1_000_000_000;

    // --- Initial state ---

    #[test]
    fn new_circuit_is_closed() {
        let mut cb = CircuitBreaker::new();
        assert_eq!(cb.check(0), CircuitState::Closed);
    }

    #[test]
    fn new_circuit_has_zero_failures() {
        let cb = CircuitBreaker::new();
        assert_eq!(cb.failures(), 0);
    }

    #[test]
    fn new_circuit_has_no_open_until() {
        let cb = CircuitBreaker::new();
        assert_eq!(cb.open_until(), None);
    }

    // --- record_failure: below threshold ---

    #[test]
    fn failures_below_max_do_not_open_circuit() {
        let mut cb = CircuitBreaker::with_limits(5, 30 * SEC);
        for _ in 0..4 {
            let opened = cb.record_failure(0);
            assert!(!opened, "should not open before max_attempts");
        }
        assert_eq!(cb.check(0), CircuitState::Closed);
    }

    #[test]
    fn record_failure_below_max_returns_false() {
        let mut cb = CircuitBreaker::with_limits(5, 30 * SEC);
        assert!(!cb.record_failure(0));
        assert!(!cb.record_failure(0));
        assert!(!cb.record_failure(0));
        assert!(!cb.record_failure(0));
    }

    // --- record_failure: at threshold ---

    #[test]
    fn fifth_failure_opens_circuit_and_returns_true() {
        let mut cb = CircuitBreaker::with_limits(5, 30 * SEC);
        for _ in 0..4 {
            cb.record_failure(0);
        }
        let opened = cb.record_failure(1000);
        assert!(opened, "5th failure should open the circuit");
    }

    #[test]
    fn circuit_open_after_max_attempts() {
        let mut cb = CircuitBreaker::with_limits(3, 30 * SEC);
        for _ in 0..3 {
            cb.record_failure(0);
        }
        assert_eq!(
            cb.check(0),
            CircuitState::Open { retry_after_ns: 30 * SEC },
        );
    }

    #[test]
    fn circuit_open_until_is_now_plus_cooldown() {
        let mut cb = CircuitBreaker::with_limits(2, 10 * SEC);
        cb.record_failure(5 * SEC);
        cb.record_failure(5 * SEC); // opens at t=5s
        assert_eq!(cb.open_until(), Some(15 * SEC));
    }

    // --- check while open ---

    #[test]
    fn check_returns_open_within_cooldown() {
        let mut cb = CircuitBreaker::with_limits(1, 30 * SEC);
        cb.record_failure(0);
        let state = cb.check(1 * SEC); // 1s later, still in cooldown
        assert!(state.is_open());
    }

    #[test]
    fn check_open_returns_correct_retry_after() {
        let mut cb = CircuitBreaker::with_limits(1, 30 * SEC);
        cb.record_failure(0);
        match cb.check(0) {
            CircuitState::Open { retry_after_ns } => {
                assert_eq!(retry_after_ns, 30 * SEC);
            }
            CircuitState::Closed => panic!("expected Open"),
        }
    }

    // --- check auto-reset after cooldown ---

    #[test]
    fn check_auto_resets_after_cooldown_expires() {
        let mut cb = CircuitBreaker::with_limits(1, 10 * SEC);
        cb.record_failure(0);
        // Cooldown expires at 10s; check at 10s (not strictly greater, so still open)
        // At exactly `until`, the condition is `now_ns < until` → false → resets.
        assert_eq!(cb.check(10 * SEC), CircuitState::Closed);
        assert_eq!(cb.failures(), 0);
        assert_eq!(cb.open_until(), None);
    }

    #[test]
    fn check_still_open_one_ns_before_cooldown() {
        let mut cb = CircuitBreaker::with_limits(1, 10 * SEC);
        cb.record_failure(0);
        assert!(cb.check(10 * SEC - 1).is_open());
    }

    #[test]
    fn after_auto_reset_new_failures_can_reopen() {
        let mut cb = CircuitBreaker::with_limits(2, 10 * SEC);
        cb.record_failure(0);
        cb.record_failure(0); // opens at t=0
        cb.check(10 * SEC);   // auto-reset
        cb.record_failure(10 * SEC);
        let opened = cb.record_failure(10 * SEC);
        assert!(opened); // circuit re-opens
    }

    // --- record_failure when already open ---

    #[test]
    fn record_failure_when_open_returns_false_and_does_not_restart_timer() {
        let mut cb = CircuitBreaker::with_limits(1, 30 * SEC);
        cb.record_failure(0); // opens, until = 30s
        let opened = cb.record_failure(1 * SEC); // already open
        assert!(!opened, "already open — should return false");
        // Timer should NOT be reset to 1s + 30s = 31s.
        assert_eq!(cb.open_until(), Some(30 * SEC));
    }

    // --- record_success ---

    #[test]
    fn record_success_resets_failures_to_zero() {
        let mut cb = CircuitBreaker::with_limits(5, 30 * SEC);
        cb.record_failure(0);
        cb.record_failure(0);
        cb.record_failure(0);
        cb.record_success();
        assert_eq!(cb.failures(), 0);
    }

    #[test]
    fn record_success_closes_open_circuit() {
        let mut cb = CircuitBreaker::with_limits(1, 30 * SEC);
        cb.record_failure(0); // opens
        cb.record_success();
        assert_eq!(cb.check(0), CircuitState::Closed);
        assert_eq!(cb.open_until(), None);
    }

    #[test]
    fn record_success_allows_fresh_failures_to_open_again() {
        let mut cb = CircuitBreaker::with_limits(2, 30 * SEC);
        cb.record_failure(0);
        cb.record_failure(0); // opens
        cb.record_success();  // closes
        cb.record_failure(1 * SEC);
        let opened = cb.record_failure(1 * SEC);
        assert!(opened);
    }

    // --- default ---

    #[test]
    fn default_matches_new() {
        let cb = CircuitBreaker::default();
        assert_eq!(cb.max_attempts(), MAX_ATTEMPTS);
        assert_eq!(cb.cooldown_ns(),  COOLDOWN_NS);
    }

    // --- state machine integration ---

    #[test]
    fn full_lifecycle_gap_recover_gap_degrade_reset() {
        let mut cb = CircuitBreaker::with_limits(3, 10 * SEC);

        // 3 failures → DEGRADED
        assert!(!cb.record_failure(0));
        assert!(!cb.record_failure(0));
        assert!(cb.record_failure(0)); // just opened

        // During cooldown: check returns Open
        assert!(cb.check(5 * SEC).is_open());

        // Cooldown expires: check resets and returns Closed
        assert_eq!(cb.check(10 * SEC), CircuitState::Closed);

        // One more failure then a success
        cb.record_failure(10 * SEC);
        cb.record_success();
        assert_eq!(cb.check(11 * SEC), CircuitState::Closed);
        assert_eq!(cb.failures(), 0);
    }
}
