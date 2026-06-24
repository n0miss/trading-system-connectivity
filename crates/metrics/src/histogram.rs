use std::sync::atomic::{AtomicU64, Ordering};

// ---------------------------------------------------------------------------
// Bucket boundaries (nanoseconds)
// ---------------------------------------------------------------------------

/// Upper bound of each histogram bucket in nanoseconds.
///
/// Fine-grained below 1 ms for co-located latency; coarser buckets up to 5 s
/// so wire latency from non-colocated machines (internet RTT 50–500 ms)
/// lands in a labelled bucket rather than `+Inf`.
pub const BUCKET_BOUNDS: &[u64] = &[
    1_000,         // 1 µs
    5_000,         // 5 µs
    10_000,        // 10 µs
    25_000,        // 25 µs
    50_000,        // 50 µs
    100_000,       // 100 µs
    250_000,       // 250 µs
    500_000,       // 500 µs
    1_000_000,     // 1 ms
    2_500_000,     // 2.5 ms
    5_000_000,     // 5 ms
    10_000_000,    // 10 ms
    25_000_000,    // 25 ms
    50_000_000,    // 50 ms
    100_000_000,   // 100 ms
    200_000_000,   // 200 ms
    500_000_000,   // 500 ms
    1_000_000_000, // 1 s
    5_000_000_000, // 5 s
];

pub const NUM_BOUNDS: usize = 19;
/// Total buckets: one per bound plus the `+Inf` catch-all.
pub const NUM_BUCKETS: usize = NUM_BOUNDS + 1;

const ZERO: AtomicU64 = AtomicU64::new(0);

// ---------------------------------------------------------------------------
// Histogram
// ---------------------------------------------------------------------------

/// Cumulative-bucket latency histogram for nanosecond-resolution measurements.
///
/// Records how many samples fall into each exponential bucket, plus a running
/// sum and count.  All `record` calls are wait-free and allocation-free.
///
/// The constructor is `const` so `Histogram` values can live in `static`
/// storage without a heap allocation.
///
/// # Percentiles
///
/// [`percentile`] returns the upper bound of the first bucket whose
/// cumulative count covers the requested fraction.  This is the same
/// approximation Prometheus uses: cheap to compute, accurate when samples
/// cluster well within a single bucket.
///
/// [`percentile`]: Histogram::percentile
pub struct Histogram {
    pub(crate) name: &'static str,
    pub(crate) help: &'static str,
    /// One slot per bound + one `+Inf` slot.  Non-cumulative (raw per-bucket).
    pub(crate) buckets: [AtomicU64; NUM_BUCKETS],
    pub(crate) sum: AtomicU64,
    pub(crate) count: AtomicU64,
}

impl Histogram {
    pub const fn new(name: &'static str, help: &'static str) -> Self {
        Self {
            name,
            help,
            buckets: [ZERO; NUM_BUCKETS],
            sum: AtomicU64::new(0),
            count: AtomicU64::new(0),
        }
    }

    /// Record one sample.
    ///
    /// Negative values (e.g. from clock skew) are silently ignored.
    /// The call is wait-free and never allocates.
    #[inline]
    pub fn record(&self, nanos: i64) {
        if nanos < 0 {
            return;
        }
        let v = nanos as u64;
        let idx = BUCKET_BOUNDS.partition_point(|&b| b < v).min(NUM_BOUNDS);
        self.buckets[idx].fetch_add(1, Ordering::Relaxed);
        self.sum.fetch_add(v, Ordering::Relaxed);
        self.count.fetch_add(1, Ordering::Relaxed);
    }

    /// Return the total number of recorded samples.
    pub fn count(&self) -> u64 {
        self.count.load(Ordering::Relaxed)
    }

    /// Return the sum of all recorded values in nanoseconds.
    pub fn sum(&self) -> u64 {
        self.sum.load(Ordering::Relaxed)
    }

    /// Return the approximate `p`-th percentile in nanoseconds, or `None`
    /// when no samples have been recorded.
    ///
    /// `p` must be in `(0.0, 1.0]`.  Returns the upper bound of the bucket
    /// that first covers the target rank, or `u64::MAX` when samples fall in
    /// the `+Inf` overflow bucket.
    pub fn percentile(&self, p: f64) -> Option<u64> {
        let total = self.count.load(Ordering::Relaxed);
        if total == 0 {
            return None;
        }
        let target = ((total as f64) * p).ceil() as u64;
        let mut cumulative = 0u64;
        for (i, &upper) in BUCKET_BOUNDS.iter().enumerate() {
            cumulative += self.buckets[i].load(Ordering::Relaxed);
            if cumulative >= target {
                return Some(upper);
            }
        }
        Some(u64::MAX) // in the +Inf bucket
    }

    /// Approximate 50th percentile in nanoseconds.
    pub fn p50(&self) -> Option<u64> {
        self.percentile(0.5)
    }

    /// Approximate 99th percentile in nanoseconds.
    pub fn p99(&self) -> Option<u64> {
        self.percentile(0.99)
    }

    pub fn name(&self) -> &'static str {
        self.name
    }
    pub fn help(&self) -> &'static str {
        self.help
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn hist() -> Histogram {
        Histogram::new("test_latency_ns", "test help")
    }

    // --- record ---

    #[test]
    fn record_negative_is_ignored() {
        let h = hist();
        h.record(-1);
        assert_eq!(h.count(), 0);
        assert_eq!(h.sum(), 0);
    }

    #[test]
    fn record_zero_goes_to_first_bucket() {
        let h = hist();
        h.record(0);
        // 0 < 1_000 → bucket index 0 (le=1µs)
        assert_eq!(h.buckets[0].load(Ordering::Relaxed), 1);
        assert_eq!(h.count(), 1);
        assert_eq!(h.sum(), 0);
    }

    #[test]
    fn record_exact_bound_goes_to_that_bucket() {
        let h = hist();
        h.record(1_000); // exactly le=1µs
                         // partition_point(|&b| b < 1000) = 0 → bucket 0
        assert_eq!(h.buckets[0].load(Ordering::Relaxed), 1);
    }

    #[test]
    fn record_just_above_bound_goes_to_next_bucket() {
        let h = hist();
        h.record(1_001); // just above 1µs → bucket 1 (le=5µs)
        assert_eq!(h.buckets[0].load(Ordering::Relaxed), 0);
        assert_eq!(h.buckets[1].load(Ordering::Relaxed), 1);
    }

    #[test]
    fn record_1ms_goes_to_correct_bucket() {
        let h = hist();
        h.record(1_000_000);
        // BUCKET_BOUNDS[8] = 1_000_000 → partition_point(|&b| b < 1_000_000) = 8
        assert_eq!(h.buckets[8].load(Ordering::Relaxed), 1);
    }

    #[test]
    fn record_above_max_bound_goes_to_inf_bucket() {
        let h = hist();
        h.record(10_000_000_000); // 10 s > 5 s (last bound) → +Inf
        assert_eq!(h.buckets[NUM_BUCKETS - 1].load(Ordering::Relaxed), 1);
    }

    #[test]
    fn sum_accumulates() {
        let h = hist();
        h.record(1_000);
        h.record(5_000);
        h.record(10_000);
        assert_eq!(h.sum(), 16_000);
        assert_eq!(h.count(), 3);
    }

    // --- percentile ---

    #[test]
    fn percentile_on_empty_returns_none() {
        let h = hist();
        assert!(h.p50().is_none());
        assert!(h.p99().is_none());
    }

    #[test]
    fn p50_returns_bucket_upper_bound() {
        let h = hist();
        // Record 100 samples all in the 1ms bucket.
        for _ in 0..100 {
            h.record(1_000_000);
        }
        // Median should be upper bound of the 1ms bucket.
        assert_eq!(h.p50(), Some(1_000_000));
    }

    #[test]
    fn p99_separates_from_p50_when_distribution_spans_buckets() {
        let h = hist();
        // 99 samples in 1µs bucket, 1 sample in 1ms bucket.
        for _ in 0..99 {
            h.record(500);
        }
        h.record(1_000_000);
        assert_eq!(h.p50(), Some(1_000)); // 50th % → 1µs bucket
        assert_eq!(h.p99(), Some(1_000)); // 99th % → still 1µs bucket (99/100)
        assert_eq!(h.percentile(1.0), Some(1_000_000)); // 100th % → 1ms bucket
    }

    #[test]
    fn p99_in_overflow_when_large_value_is_the_99th() {
        let h = hist();
        for _ in 0..99 {
            h.record(500_000);
        }
        h.record(10_000_000_000); // 10 s — beyond all bounds (max is 5 s)
                                  // 100th % (rank 100) → only reached after overflow bucket
        assert_eq!(h.percentile(1.0), Some(u64::MAX));
    }

    #[test]
    fn histogram_can_be_static() {
        static H: Histogram = Histogram::new("static_hist_ns", "static help");
        H.record(50_000);
        assert!(H.count() >= 1);
    }

    #[test]
    fn name_and_help_are_preserved() {
        let h = Histogram::new("my_lat_ns", "my help");
        assert_eq!(h.name(), "my_lat_ns");
        assert_eq!(h.help(), "my help");
    }
}
