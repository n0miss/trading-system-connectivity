//! Recovery circuit breaker for Binance USDT-M Futures (§5.25).
//!
//! Limits consecutive recovery failures before suspending the channel.
//! Defaults: 5 attempts / 30 s cooldown — identical to the Spot version.

pub const MAX_ATTEMPTS: u32 = 5;
pub const COOLDOWN_NS: i64 = 30_000_000_000;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CircuitState {
    Closed,
    Open { retry_after_ns: i64 },
}

impl CircuitState {
    pub fn is_open(self) -> bool {
        matches!(self, Self::Open { .. })
    }
}

#[derive(Debug)]
pub struct CircuitBreaker {
    failures: u32,
    max_attempts: u32,
    cooldown_ns: i64,
    open_until: Option<i64>,
}

impl CircuitBreaker {
    pub fn new() -> Self {
        Self::with_limits(MAX_ATTEMPTS, COOLDOWN_NS)
    }

    pub fn with_limits(max_attempts: u32, cooldown_ns: i64) -> Self {
        Self {
            failures: 0,
            max_attempts,
            cooldown_ns,
            open_until: None,
        }
    }

    /// Return the current circuit state, auto-resetting if cooldown has expired.
    pub fn check(&mut self, now_ns: i64) -> CircuitState {
        if let Some(until) = self.open_until {
            if now_ns < until {
                return CircuitState::Open {
                    retry_after_ns: until,
                };
            }
            self.failures = 0;
            self.open_until = None;
        }
        CircuitState::Closed
    }

    pub fn record_success(&mut self) {
        self.failures = 0;
        self.open_until = None;
    }

    /// Returns `true` if this failure just opened the circuit.
    pub fn record_failure(&mut self, now_ns: i64) -> bool {
        if self.open_until.is_some() {
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

    pub fn failures(&self) -> u32 {
        self.failures
    }
    pub fn max_attempts(&self) -> u32 {
        self.max_attempts
    }
    pub fn cooldown_ns(&self) -> i64 {
        self.cooldown_ns
    }
    pub fn open_until(&self) -> Option<i64> {
        self.open_until
    }
}

impl Default for CircuitBreaker {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    const SEC: i64 = 1_000_000_000;

    #[test]
    fn new_circuit_is_closed() {
        let mut cb = CircuitBreaker::new();
        assert_eq!(cb.check(0), CircuitState::Closed);
    }

    #[test]
    fn failures_below_max_do_not_open() {
        let mut cb = CircuitBreaker::with_limits(5, 30 * SEC);
        for _ in 0..4 {
            assert!(!cb.record_failure(0));
        }
        assert_eq!(cb.check(0), CircuitState::Closed);
    }

    #[test]
    fn max_failures_opens_circuit() {
        let mut cb = CircuitBreaker::with_limits(3, 30 * SEC);
        for _ in 0..2 {
            cb.record_failure(0);
        }
        assert!(cb.record_failure(0));
        assert!(cb.check(0).is_open());
    }

    #[test]
    fn cooldown_auto_resets() {
        let mut cb = CircuitBreaker::with_limits(1, 10 * SEC);
        cb.record_failure(0);
        assert_eq!(cb.check(10 * SEC), CircuitState::Closed);
        assert_eq!(cb.failures(), 0);
    }

    #[test]
    fn record_success_closes_circuit() {
        let mut cb = CircuitBreaker::with_limits(1, 30 * SEC);
        cb.record_failure(0);
        cb.record_success();
        assert_eq!(cb.check(0), CircuitState::Closed);
        assert_eq!(cb.failures(), 0);
    }

    #[test]
    fn record_failure_when_open_does_not_restart_timer() {
        let mut cb = CircuitBreaker::with_limits(1, 30 * SEC);
        cb.record_failure(0); // opens; until = 30s
        assert!(!cb.record_failure(SEC));
        assert_eq!(cb.open_until(), Some(30 * SEC)); // timer not restarted
    }
}
