use crate::counter::Counter;
use crate::histogram::Histogram;
use crate::render::render_prometheus;

// ---------------------------------------------------------------------------
// ConnectorMetrics
// ---------------------------------------------------------------------------

/// All metrics for one connector instance.
///
/// Every field is a lock-free atomic; the struct is `Send + Sync` and can be
/// placed in a `static` (all constructors are `const`).
///
/// # Example
///
/// ```rust
/// use connector_metrics::ConnectorMetrics;
///
/// static METRICS: ConnectorMetrics = ConnectorMetrics::new();
///
/// fn on_reconnect() {
///     METRICS.reconnects.increment();
/// }
/// ```
pub struct ConnectorMetrics {
    // ---- Latency histograms (nanoseconds) ---------------------------------
    /// Time between the exchange-side event timestamp and local socket receive.
    /// Measures network + OS scheduling overhead.
    pub wire_latency: Histogram,

    /// Time between local socket receive and Aeron offer.
    /// Measures decode + normalization + sequence-validation overhead.
    pub processing_latency: Histogram,

    /// Time between the exchange-side event timestamp and Aeron offer.
    /// The sum of wire and processing latency.
    pub end_to_end_latency: Histogram,

    // ---- Throughput counters ----------------------------------------------
    /// Raw WebSocket frames received from the exchange (both SBE and JSON).
    pub messages_in: Counter,

    /// Normalized messages offered to Aeron (one per decoded market event).
    pub messages_out: Counter,

    // ---- Health counters --------------------------------------------------
    /// Number of WebSocket reconnect attempts (any cause).
    pub reconnects: Counter,

    /// Number of times an order book was marked stale.
    pub stale_books: Counter,

    /// Number of sequence-number gaps detected.
    pub sequence_gaps: Counter,

    /// Number of Aeron offer calls that returned a back-pressure or error code.
    pub offer_failures: Counter,

    /// Number of frames that failed both SBE and JSON decoding.
    pub decode_errors: Counter,
}

impl ConnectorMetrics {
    /// Create a new registry with all metrics zeroed.
    ///
    /// `const` — safe to use as a `static` initializer.
    pub const fn new() -> Self {
        Self {
            wire_latency: Histogram::new(
                "connector_wire_latency_ns",
                "Exchange event timestamp to local socket receive, nanoseconds.",
            ),
            processing_latency: Histogram::new(
                "connector_processing_latency_ns",
                "Local socket receive to Aeron offer (decode + normalise + validate), nanoseconds.",
            ),
            end_to_end_latency: Histogram::new(
                "connector_end_to_end_latency_ns",
                "Exchange event timestamp to Aeron offer (wire + processing), nanoseconds.",
            ),

            messages_in: Counter::new(
                "connector_messages_in",
                "Raw WebSocket frames received from the exchange.",
            ),
            messages_out: Counter::new(
                "connector_messages_out",
                "Normalized messages offered to Aeron.",
            ),
            reconnects: Counter::new("connector_reconnects", "WebSocket reconnect attempts."),
            stale_books: Counter::new("connector_stale_books", "Order books marked stale."),
            sequence_gaps: Counter::new(
                "connector_sequence_gaps",
                "Sequence-number gaps detected.",
            ),
            offer_failures: Counter::new(
                "connector_offer_failures",
                "Aeron offer calls that returned back-pressure or an error.",
            ),
            decode_errors: Counter::new(
                "connector_decode_errors",
                "WebSocket frames that failed both SBE and JSON decoding.",
            ),
        }
    }

    /// Render the full registry in Prometheus text format.
    ///
    /// Allocates a `String`; call only on the scrape path.
    pub fn render_prometheus(&self) -> String {
        render_prometheus(self)
    }

    // -----------------------------------------------------------------------
    // Iterator helpers (used by render.rs)
    // -----------------------------------------------------------------------

    pub(crate) fn counters(&self) -> [&Counter; 7] {
        [
            &self.messages_in,
            &self.messages_out,
            &self.reconnects,
            &self.stale_books,
            &self.sequence_gaps,
            &self.offer_failures,
            &self.decode_errors,
        ]
    }

    pub(crate) fn histograms(&self) -> [&Histogram; 3] {
        [
            &self.wire_latency,
            &self.processing_latency,
            &self.end_to_end_latency,
        ]
    }
}

impl Default for ConnectorMetrics {
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

    #[test]
    fn new_registry_all_counters_zero() {
        let m = ConnectorMetrics::new();
        assert_eq!(m.messages_in.get(), 0);
        assert_eq!(m.messages_out.get(), 0);
        assert_eq!(m.reconnects.get(), 0);
        assert_eq!(m.stale_books.get(), 0);
        assert_eq!(m.sequence_gaps.get(), 0);
        assert_eq!(m.offer_failures.get(), 0);
        assert_eq!(m.decode_errors.get(), 0);
    }

    #[test]
    fn new_registry_all_histograms_empty() {
        let m = ConnectorMetrics::new();
        assert_eq!(m.wire_latency.count(), 0);
        assert_eq!(m.processing_latency.count(), 0);
        assert_eq!(m.end_to_end_latency.count(), 0);
    }

    #[test]
    fn registry_as_static() {
        static M: ConnectorMetrics = ConnectorMetrics::new();
        M.reconnects.increment();
        assert!(M.reconnects.get() >= 1);
    }

    #[test]
    fn default_matches_new() {
        let a = ConnectorMetrics::new();
        let b = ConnectorMetrics::default();
        assert_eq!(a.reconnects.get(), b.reconnects.get());
    }

    #[test]
    fn counter_fields_work_independently() {
        let m = ConnectorMetrics::new();
        m.reconnects.increment();
        m.stale_books.add(3);
        m.sequence_gaps.add(2);
        assert_eq!(m.reconnects.get(), 1);
        assert_eq!(m.stale_books.get(), 3);
        assert_eq!(m.sequence_gaps.get(), 2);
        assert_eq!(m.offer_failures.get(), 0); // untouched
    }

    #[test]
    fn latency_hops_recorded_independently() {
        let m = ConnectorMetrics::new();
        m.wire_latency.record(10_000); // 10µs
        m.processing_latency.record(50_000); // 50µs
        m.end_to_end_latency.record(60_000); // 60µs

        assert_eq!(m.wire_latency.count(), 1);
        assert_eq!(m.processing_latency.count(), 1);
        assert_eq!(m.end_to_end_latency.count(), 1);
        assert_eq!(m.wire_latency.sum(), 10_000);
        assert_eq!(m.processing_latency.sum(), 50_000);
        assert_eq!(m.end_to_end_latency.sum(), 60_000);
    }

    #[test]
    fn p50_and_p99_accessible_via_registry() {
        let m = ConnectorMetrics::new();
        for _ in 0..100 {
            m.wire_latency.record(50_000); // all samples in 50µs bucket
        }
        assert_eq!(m.wire_latency.p50(), Some(50_000));
        assert_eq!(m.wire_latency.p99(), Some(50_000));
    }

    #[test]
    fn render_prometheus_is_non_empty() {
        let m = ConnectorMetrics::new();
        m.reconnects.increment();
        let out = m.render_prometheus();
        assert!(!out.is_empty());
        assert!(out.contains("connector_reconnects_total 1"));
    }

    #[test]
    fn counters_iter_length() {
        let m = ConnectorMetrics::new();
        assert_eq!(m.counters().len(), 7);
    }

    #[test]
    fn histograms_iter_length() {
        let m = ConnectorMetrics::new();
        assert_eq!(m.histograms().len(), 3);
    }
}
