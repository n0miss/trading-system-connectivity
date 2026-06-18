/// Replay timing modes and fault-injection configuration.

// ---------------------------------------------------------------------------
// FaultConfig
// ---------------------------------------------------------------------------

/// Parameters controlling fault injection.
///
/// Each `*_percent` field is an independent probability (0–100).  On every
/// frame that passes the timing gate, the replayer rolls the PRNG three times:
/// drop → corrupt → duplicate (in that order).  Drop is checked first so that
/// dropped frames are never corrupted or duplicated.
///
/// All randomness is **deterministic**: the same `seed` always produces the
/// same fault sequence, making test failures reproducible.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FaultConfig {
    /// Probability (0–100) that a frame is silently dropped.
    pub drop_percent: u8,
    /// Probability (0–100) that a frame's payload has one byte flipped.
    pub corrupt_percent: u8,
    /// Probability (0–100) that a frame is emitted twice.
    pub duplicate_percent: u8,
    /// Seed for the deterministic PRNG.
    pub seed: u64,
}

impl Default for FaultConfig {
    fn default() -> Self {
        Self { drop_percent: 0, corrupt_percent: 0, duplicate_percent: 0, seed: 0 }
    }
}

// ---------------------------------------------------------------------------
// ReplayMode
// ---------------------------------------------------------------------------

/// Controls *when* the replayer emits frames and *whether* faults are injected.
///
/// The five modes correspond to the five use-cases in §10.36:
///
/// | Mode              | Use case                                    |
/// |-------------------|---------------------------------------------|
/// | `AsFastAsPossible`| Throughput benchmarks, bulk unit tests      |
/// | `OriginalTiming`  | Realistic latency simulation                |
/// | `Scaled`          | Accelerated soak tests, slow-motion debug   |
/// | `Deterministic`   | Property tests, fully reproducible replays  |
/// | `FaultInjection`  | Chaos tests (stage 39)                      |
#[derive(Debug, Clone)]
pub enum ReplayMode {
    /// Emit frames as fast as the consumer can accept them — no delay.
    AsFastAsPossible,

    /// Preserve the exact inter-message gaps from the recording.
    ///
    /// The first frame is always immediately ready; subsequent frames are
    /// held until `now - replay_start >= captured_at - first_captured_at`.
    OriginalTiming,

    /// Stretch or compress recorded delays by `num / den`.
    ///
    /// * `num > den` — slower than recorded (e.g. `num=2, den=1` → 2× slower)
    /// * `num < den` — faster than recorded (e.g. `num=1, den=2` → 2× faster)
    /// * `num == den` — identical to `OriginalTiming`
    ///
    /// `den` must not be zero; the replayer clamps it to 1.
    Scaled {
        num: u32,
        den: u32,
    },

    /// Tick-based virtual clock — no wall-clock dependency.
    ///
    /// Every call to `next_frame_at()` returns the next frame immediately,
    /// regardless of the `now_ns` argument.  The `virtual_ts_ns` field in
    /// [`crate::replayer::ReplayEvent`] advances by the recorded
    /// inter-frame delta so downstream components see realistic timestamps.
    ///
    /// Use this mode in unit and property tests for perfect reproducibility.
    Deterministic,

    /// Apply `faults` to frames emitted according to `inner`'s timing.
    ///
    /// Fault decisions are made **after** the timing gate passes, so the
    /// timing semantics of `inner` are fully preserved.
    ///
    /// ```
    /// use connector_replay::mode::{FaultConfig, ReplayMode};
    ///
    /// // 20% drop rate on top of original timing:
    /// let mode = ReplayMode::FaultInjection {
    ///     inner:  Box::new(ReplayMode::OriginalTiming),
    ///     faults: FaultConfig { drop_percent: 20, seed: 42, ..Default::default() },
    /// };
    /// ```
    FaultInjection {
        inner:  Box<ReplayMode>,
        faults: FaultConfig,
    },
}

impl ReplayMode {
    /// Walk the `FaultInjection` wrapper to find the base timing mode.
    pub(crate) fn base_timing(&self) -> &ReplayMode {
        match self {
            ReplayMode::FaultInjection { inner, .. } => inner.base_timing(),
            other => other,
        }
    }

    /// Return the `FaultConfig` if this mode (or any wrapper) contains one.
    pub(crate) fn fault_config(&self) -> Option<&FaultConfig> {
        match self {
            ReplayMode::FaultInjection { faults, .. } => Some(faults),
            _ => None,
        }
    }
}

// ---------------------------------------------------------------------------
// Prng — xorshift64
// ---------------------------------------------------------------------------

/// Deterministic pseudo-random number generator (xorshift64).
///
/// Used exclusively for fault-injection decisions.  The same seed always
/// produces the same sequence.
pub(crate) struct Prng {
    state: u64,
}

impl Prng {
    pub(crate) fn new(seed: u64) -> Self {
        // xorshift64 must not have a zero state.
        Self { state: if seed == 0 { 0xDEAD_BEEF_CAFE_F00D } else { seed } }
    }

    pub(crate) fn next(&mut self) -> u64 {
        let mut x = self.state;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.state = x;
        x
    }

    /// Return `true` with probability `pct / 100`.
    pub(crate) fn percent_chance(&mut self, pct: u8) -> bool {
        pct > 0 && self.next() % 100 < pct as u64
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn prng_is_deterministic() {
        let mut a = Prng::new(42);
        let mut b = Prng::new(42);
        for _ in 0..100 {
            assert_eq!(a.next(), b.next());
        }
    }

    #[test]
    fn prng_zero_seed_does_not_lock_up() {
        let mut p = Prng::new(0);
        for _ in 0..1000 {
            p.next(); // must not loop infinitely
        }
    }

    #[test]
    fn percent_chance_zero_never_fires() {
        let mut p = Prng::new(1);
        for _ in 0..1000 {
            assert!(!p.percent_chance(0));
        }
    }

    #[test]
    fn percent_chance_hundred_always_fires() {
        let mut p = Prng::new(1);
        for _ in 0..1000 {
            assert!(p.percent_chance(100));
        }
    }

    #[test]
    fn base_timing_unwraps_fault_injection() {
        let mode = ReplayMode::FaultInjection {
            inner:  Box::new(ReplayMode::Deterministic),
            faults: FaultConfig::default(),
        };
        assert!(matches!(mode.base_timing(), ReplayMode::Deterministic));
    }

    #[test]
    fn fault_config_present_in_fault_injection_mode() {
        let fc = FaultConfig { drop_percent: 10, ..Default::default() };
        let mode = ReplayMode::FaultInjection {
            inner:  Box::new(ReplayMode::AsFastAsPossible),
            faults: fc.clone(),
        };
        assert_eq!(mode.fault_config(), Some(&fc));
    }

    #[test]
    fn fault_config_absent_in_non_fault_mode() {
        assert!(ReplayMode::AsFastAsPossible.fault_config().is_none());
        assert!(ReplayMode::OriginalTiming.fault_config().is_none());
        assert!(ReplayMode::Deterministic.fault_config().is_none());
    }
}
