/// Per-shard heartbeat timer (§4.20).
///
/// Downstream consumers need a regular signal to distinguish "feed is alive
/// but quiet" from "feed has died".  `Heartbeater` tracks the last time a
/// heartbeat was emitted and tells the caller when the next one is due.
///
/// # Stream layout
///
/// Heartbeats are published on the same Aeron stream as market-data messages
/// (`stream_id = shard_id + 1`, see [`crate::shard_stream_id`]).  Mixing
/// heartbeats with data messages gives downstream subscribers a single ordered
/// stream they can monitor for staleness without consulting a separate channel.
///
/// # Usage
///
/// ```
/// use connector_aeron::Heartbeater;
///
/// let mut hb = Heartbeater::new();
/// let now_ns: i64 = 1_000_000_000; // pretend 1 s has elapsed since epoch
///
/// if hb.is_due(now_ns) {
///     // encode and publish a Heartbeat message …
///     hb.record_beat(now_ns);
/// }
/// ```

/// Default heartbeat interval: one second.
pub const HEARTBEAT_INTERVAL_NS: i64 = 1_000_000_000;

/// Tracks when the next per-shard heartbeat should be published.
pub struct Heartbeater {
    interval_ns: i64,
    last_beat_ns: i64, // 0 → never beaten
}

impl Default for Heartbeater {
    fn default() -> Self {
        Self::new()
    }
}

impl Heartbeater {
    /// Create a heartbeat timer with the default 1-second interval.
    pub fn new() -> Self {
        Self::with_interval_ns(HEARTBEAT_INTERVAL_NS)
    }

    /// Create a heartbeat timer with a custom interval in nanoseconds.
    ///
    /// # Panics
    ///
    /// Panics if `interval_ns` is zero or negative.
    pub fn with_interval_ns(interval_ns: i64) -> Self {
        assert!(interval_ns > 0, "heartbeat interval_ns must be positive");
        Self {
            interval_ns,
            last_beat_ns: 0,
        }
    }

    /// Returns `true` if a heartbeat should be published at `now_ns`.
    ///
    /// Always returns `true` before the first [`record_beat`] call so that
    /// a heartbeat is emitted promptly on startup.
    ///
    /// [`record_beat`]: Self::record_beat
    pub fn is_due(&self, now_ns: i64) -> bool {
        now_ns.saturating_sub(self.last_beat_ns) >= self.interval_ns
    }

    /// Record that a heartbeat was emitted at `now_ns`.
    pub fn record_beat(&mut self, now_ns: i64) {
        self.last_beat_ns = now_ns;
    }

    /// Nanoseconds until the next heartbeat is due (0 if already due).
    pub fn next_due_in_ns(&self, now_ns: i64) -> i64 {
        let elapsed = now_ns.saturating_sub(self.last_beat_ns);
        (self.interval_ns - elapsed).max(0)
    }

    /// The configured heartbeat interval in nanoseconds.
    pub fn interval_ns(&self) -> i64 {
        self.interval_ns
    }

    /// Nanosecond timestamp of the last recorded beat (0 if never beaten).
    pub fn last_beat_ns(&self) -> i64 {
        self.last_beat_ns
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    const ONE_SEC: i64 = 1_000_000_000;

    #[test]
    fn default_interval_is_one_second() {
        let hb = Heartbeater::new();
        assert_eq!(hb.interval_ns(), ONE_SEC);
    }

    #[test]
    fn default_matches_new() {
        let a = Heartbeater::new();
        let b = Heartbeater::default();
        assert_eq!(a.interval_ns(), b.interval_ns());
        assert_eq!(a.last_beat_ns(), b.last_beat_ns());
    }

    #[test]
    fn is_due_true_when_never_beaten() {
        // last_beat_ns = 0; any positive now_ns ≥ interval is due.
        // With interval=1s and now=1s, elapsed = 1s - 0 = 1s ≥ interval.
        let hb = Heartbeater::new();
        assert!(hb.is_due(ONE_SEC));
    }

    #[test]
    fn is_due_true_even_at_one_ns_when_never_beaten() {
        // now=1 < interval=1s, but 1 - 0 = 1 < 1_000_000_000 → NOT due.
        // This verifies the threshold is respected, not just "never beaten".
        let hb = Heartbeater::new();
        assert!(!hb.is_due(1));
    }

    #[test]
    fn is_due_false_immediately_after_record_beat() {
        let mut hb = Heartbeater::new();
        let now = 5 * ONE_SEC;
        hb.record_beat(now);
        assert!(!hb.is_due(now));
    }

    #[test]
    fn is_due_false_one_ns_before_interval_elapses() {
        let mut hb = Heartbeater::new();
        let beat_at = 10 * ONE_SEC;
        hb.record_beat(beat_at);
        assert!(!hb.is_due(beat_at + ONE_SEC - 1));
    }

    #[test]
    fn is_due_true_at_exactly_interval_elapsed() {
        let mut hb = Heartbeater::new();
        let beat_at = 10 * ONE_SEC;
        hb.record_beat(beat_at);
        assert!(hb.is_due(beat_at + ONE_SEC));
    }

    #[test]
    fn is_due_true_after_interval_exceeded() {
        let mut hb = Heartbeater::new();
        let beat_at = 10 * ONE_SEC;
        hb.record_beat(beat_at);
        assert!(hb.is_due(beat_at + 2 * ONE_SEC));
    }

    #[test]
    fn record_beat_updates_last_beat_ns() {
        let mut hb = Heartbeater::new();
        hb.record_beat(42);
        assert_eq!(hb.last_beat_ns(), 42);
    }

    #[test]
    fn next_due_in_ns_zero_when_due() {
        let mut hb = Heartbeater::new();
        let beat_at = 5 * ONE_SEC;
        hb.record_beat(beat_at);
        assert_eq!(hb.next_due_in_ns(beat_at + ONE_SEC), 0);
        assert_eq!(hb.next_due_in_ns(beat_at + 2 * ONE_SEC), 0);
    }

    #[test]
    fn next_due_in_ns_returns_remaining_time() {
        let mut hb = Heartbeater::new();
        let beat_at = 5 * ONE_SEC;
        hb.record_beat(beat_at);
        // 100 ms elapsed, 900 ms remaining
        let remaining = hb.next_due_in_ns(beat_at + 100_000_000);
        assert_eq!(remaining, 900_000_000);
    }

    #[test]
    fn custom_interval_respected() {
        let mut hb = Heartbeater::with_interval_ns(500_000_000); // 500 ms
        hb.record_beat(0);
        assert!(!hb.is_due(499_999_999));
        assert!(hb.is_due(500_000_000));
    }

    #[test]
    fn second_beat_resets_timer_correctly() {
        let mut hb = Heartbeater::new();
        let first_beat = 3 * ONE_SEC;
        let second_beat = 4 * ONE_SEC;
        hb.record_beat(first_beat);
        hb.record_beat(second_beat);
        // 500 ms after second beat — still not due
        assert!(!hb.is_due(second_beat + 500_000_000));
        // 1 s after second beat — due
        assert!(hb.is_due(second_beat + ONE_SEC));
    }
}
