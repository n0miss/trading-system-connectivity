//! Client order ID generation.
//!
//! Format: `cc-{instance:04x}-{counter:016x}` (24 characters).
//!
//! * `cc`       — 2-char prefix identifying this system ("connector").
//! * `instance` — 4 hex chars from the gateway's `instance_id` (0..=65535).
//! * `counter`  — 16 hex chars, monotonically increasing per instance.
//!
//! This format is well within Binance's 36-character client order ID limit and
//! is lexicographically sortable by (instance, counter).

const PREFIX: &str = "cc";

/// A validated client order ID.
///
/// Cheaply cloneable (`String` under the hood).  Use [`std::fmt::Display`] or
/// [`as_str`][Self::as_str] to obtain the string form for exchange submission.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct ClientOrderId(String);

impl ClientOrderId {
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Extract the monotonic counter embedded in this cloid.
    ///
    /// Returns `None` when the string does not match the `cc-XXXX-XXXXXXXXXXXXXXXX`
    /// format (e.g. for externally-assigned IDs).
    /// Construct from raw string — for use within this crate only (journal decoder).
    pub(crate) fn new_raw(s: String) -> Self {
        Self(s)
    }

    pub fn parse_counter(&self) -> Option<u64> {
        let mut parts = self.0.splitn(3, '-');
        let prefix = parts.next()?;
        let _inst = parts.next()?;
        let ctr = parts.next()?;
        if prefix != PREFIX {
            return None;
        }
        u64::from_str_radix(ctr, 16).ok()
    }

    /// Extract the instance ID embedded in this cloid.
    pub fn parse_instance(&self) -> Option<u32> {
        let mut parts = self.0.splitn(3, '-');
        let prefix = parts.next()?;
        let inst = parts.next()?;
        if prefix != PREFIX {
            return None;
        }
        u32::from_str_radix(inst, 16).ok()
    }
}

impl std::fmt::Display for ClientOrderId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

/// Generates monotonically increasing `ClientOrderId` values for one gateway instance.
///
/// Create via [`new`][Self::new] for a fresh start, or via
/// [`with_start_after`][Self::with_start_after] to resume after recovering the
/// journal (pass the highest counter seen in any recovered `OrderRequested` entry).
pub struct ClientOrderIdGenerator {
    instance_id: u32,
    counter: u64,
}

impl ClientOrderIdGenerator {
    /// Create a generator starting at counter = 0.
    pub fn new(instance_id: u32) -> Self {
        Self {
            instance_id,
            counter: 0,
        }
    }

    /// Create a generator that resumes after `last_counter`.
    ///
    /// Use during recovery to guarantee no counter is reused even across restarts.
    pub fn with_start_after(instance_id: u32, last_counter: u64) -> Self {
        Self {
            instance_id,
            counter: last_counter.saturating_add(1),
        }
    }

    /// Issue the next `ClientOrderId` and advance the internal counter.
    pub fn next(&mut self) -> ClientOrderId {
        let s = format!("{}-{:04x}-{:016x}", PREFIX, self.instance_id, self.counter);
        self.counter += 1;
        ClientOrderId(s)
    }

    /// The next counter value that will be issued (pre-increment view).
    pub fn counter(&self) -> u64 {
        self.counter
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn counter_starts_at_zero() {
        let gen = ClientOrderIdGenerator::new(0);
        assert_eq!(gen.counter(), 0);
    }

    #[test]
    fn next_increments_counter() {
        let mut gen = ClientOrderIdGenerator::new(0);
        gen.next();
        gen.next();
        assert_eq!(gen.counter(), 2);
    }

    #[test]
    fn cloid_format_is_exactly_24_chars() {
        let mut gen = ClientOrderIdGenerator::new(0);
        let cloid = gen.next();
        assert_eq!(
            cloid.as_str().len(),
            24,
            "cloid must be exactly 24 chars, got: {:?}",
            cloid.as_str()
        );
    }

    #[test]
    fn cloid_within_binance_36_char_limit() {
        let mut gen = ClientOrderIdGenerator::new(u16::MAX as u32);
        for _ in 0..100 {
            let cloid = gen.next();
            assert!(
                cloid.as_str().len() <= 36,
                "cloid too long: {}",
                cloid.as_str().len()
            );
        }
    }

    #[test]
    fn cloid_contains_only_valid_characters() {
        let mut gen = ClientOrderIdGenerator::new(42);
        for _ in 0..20 {
            let cloid = gen.next();
            for ch in cloid.as_str().chars() {
                assert!(
                    ch.is_ascii_hexdigit() || ch == '-',
                    "invalid char {ch:?} in cloid {:?}",
                    cloid.as_str()
                );
            }
        }
    }

    #[test]
    fn parse_counter_round_trips() {
        let mut gen = ClientOrderIdGenerator::new(7);
        for expected in 0u64..10 {
            let cloid = gen.next();
            assert_eq!(
                cloid.parse_counter(),
                Some(expected),
                "counter round-trip failed for {:?}",
                cloid.as_str()
            );
        }
    }

    #[test]
    fn parse_instance_round_trips() {
        for inst in [0u32, 1, 255, 0xabcd] {
            let mut gen = ClientOrderIdGenerator::new(inst);
            let cloid = gen.next();
            assert_eq!(cloid.parse_instance(), Some(inst));
        }
    }

    #[test]
    fn with_start_after_resumes_correctly() {
        let mut gen = ClientOrderIdGenerator::with_start_after(0, 99);
        let cloid = gen.next();
        assert_eq!(cloid.parse_counter(), Some(100));
    }

    #[test]
    fn different_instances_produce_different_cloids() {
        let mut gen_a = ClientOrderIdGenerator::new(0);
        let mut gen_b = ClientOrderIdGenerator::new(1);
        let a = gen_a.next();
        let b = gen_b.next();
        assert_ne!(a, b);
    }

    #[test]
    fn same_instance_produces_unique_cloids() {
        let mut gen = ClientOrderIdGenerator::new(0);
        let ids: Vec<_> = (0..1000).map(|_| gen.next()).collect();
        let unique: std::collections::HashSet<_> = ids.iter().collect();
        assert_eq!(unique.len(), 1000, "all generated cloids must be unique");
    }

    #[test]
    fn cloid_display_equals_as_str() {
        let mut gen = ClientOrderIdGenerator::new(3);
        let cloid = gen.next();
        assert_eq!(format!("{cloid}"), cloid.as_str());
    }

    #[test]
    fn parse_counter_returns_none_for_foreign_id() {
        let foreign = ClientOrderId("binance-generated-123".to_string());
        assert!(foreign.parse_counter().is_none());
    }
}
