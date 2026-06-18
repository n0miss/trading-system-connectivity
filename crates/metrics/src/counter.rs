use std::sync::atomic::{AtomicU64, Ordering};

/// A monotonically increasing lock-free counter.
///
/// All methods are wait-free and allocation-free.  The constructor is
/// `const` so `Counter` values can be placed in `static` storage.
pub struct Counter {
    pub(crate) name: &'static str,
    pub(crate) help: &'static str,
    value:           AtomicU64,
}

impl Counter {
    /// Create a new counter initialised to zero.
    pub const fn new(name: &'static str, help: &'static str) -> Self {
        Self { name, help, value: AtomicU64::new(0) }
    }

    /// Increment by one.
    #[inline]
    pub fn increment(&self) {
        self.value.fetch_add(1, Ordering::Relaxed);
    }

    /// Increment by `n`.
    #[inline]
    pub fn add(&self, n: u64) {
        self.value.fetch_add(n, Ordering::Relaxed);
    }

    /// Read the current value.
    #[inline]
    pub fn get(&self) -> u64 {
        self.value.load(Ordering::Relaxed)
    }

    /// Name used in Prometheus output (without the `_total` suffix).
    pub fn name(&self) -> &'static str { self.name }
    /// Help text used in Prometheus output.
    pub fn help(&self) -> &'static str { self.help }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn new_counter_is_zero() {
        let c = Counter::new("test", "help");
        assert_eq!(c.get(), 0);
    }

    #[test]
    fn increment_adds_one() {
        let c = Counter::new("test", "help");
        c.increment();
        assert_eq!(c.get(), 1);
    }

    #[test]
    fn multiple_increments_accumulate() {
        let c = Counter::new("test", "help");
        for _ in 0..10 { c.increment(); }
        assert_eq!(c.get(), 10);
    }

    #[test]
    fn add_by_n() {
        let c = Counter::new("test", "help");
        c.add(42);
        assert_eq!(c.get(), 42);
    }

    #[test]
    fn add_is_cumulative() {
        let c = Counter::new("test", "help");
        c.add(100);
        c.add(23);
        assert_eq!(c.get(), 123);
    }

    #[test]
    fn add_zero_is_noop() {
        let c = Counter::new("test", "help");
        c.add(0);
        assert_eq!(c.get(), 0);
    }

    #[test]
    fn name_and_help_are_preserved() {
        let c = Counter::new("my_metric", "my help text");
        assert_eq!(c.name(), "my_metric");
        assert_eq!(c.help(), "my help text");
    }

    #[test]
    fn counter_can_be_static() {
        static C: Counter = Counter::new("static_counter", "static help");
        C.increment();
        assert!(C.get() >= 1);
    }
}
