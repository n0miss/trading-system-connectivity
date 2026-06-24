/// Hot-path instrumentation helpers.
///
/// All public functions are wait-free and allocation-free.  They accept a
/// shared `&ConnectorMetrics` reference (suitable for a `static` registry or
/// an `Arc`-deref) and update the relevant atomic counters / histograms.
///
/// # Timestamp conventions
///
/// All timestamps are nanoseconds since the Unix epoch, matching
/// `MessageHeader` fields:
///
/// | Field              | Description                                      |
/// |--------------------|--------------------------------------------------|
/// | `exchange_event_ts`| When the exchange generated the event.           |
/// | `local_recv_ts`    | When the local socket received the raw bytes.    |
/// | `local_publish_ts` | Stamped by `record_publish` just before publish. |
///
/// `TS_NONE = 0` is the sentinel for "timestamp not available".  Latencies
/// are only recorded when both source timestamps are non-zero.
use connector_core::TS_NONE;
use connector_metrics::ConnectorMetrics;

// ---------------------------------------------------------------------------
// Shared clock
// ---------------------------------------------------------------------------

/// Wall-clock nanoseconds since the Unix epoch.
///
/// Used exclusively by [`record_publish`] and [`ConnectionManager`].
/// Single definition here; callers import this module.
#[inline]
pub(crate) fn now_nanos() -> i64 {
    use std::time::SystemTime;
    SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos() as i64
}

// ---------------------------------------------------------------------------
// Hot-path instrumentation functions
// ---------------------------------------------------------------------------

/// Stamp `local_publish_ts` and record the three latency hops.
///
/// Call this immediately before offering a normalized message to Aeron.
///
/// # Arguments
///
/// * `m`        — The shared metrics registry.
/// * `event_ts` — `MessageHeader::exchange_event_ts` of the message being published.
/// * `recv_ts`  — `MessageHeader::local_recv_ts` of the message being published.
///
/// # Returns
///
/// The nanosecond timestamp used as `local_publish_ts`.  Patch it into the
/// encoded buffer's header before the Aeron offer.
///
/// # Latency accounting
///
/// | Metric               | Delta                                  | Requires exchange ts? |
/// |----------------------|----------------------------------------|-----------------------|
/// | `processing_latency` | `local_publish_ts − local_recv_ts`     | No — always recorded  |
/// | `wire_latency`       | `local_recv_ts − exchange_event_ts`    | Yes                   |
/// | `end_to_end_latency` | `local_publish_ts − exchange_event_ts` | Yes                   |
///
/// `processing_latency` is recorded for every message (including bookTicker,
/// which carries no exchange timestamp).  `wire_latency` and `end_to_end_latency`
/// are only recorded when `exchange_event_ts != TS_NONE`.
#[inline]
pub fn record_publish(m: &ConnectorMetrics, event_ts: i64, recv_ts: i64) -> i64 {
    let publish_ts = now_nanos();
    m.messages_out.increment();
    if recv_ts != TS_NONE {
        m.processing_latency
            .record(publish_ts.saturating_sub(recv_ts));
    }
    if event_ts != TS_NONE && recv_ts != TS_NONE {
        m.wire_latency.record(recv_ts.saturating_sub(event_ts));
        m.end_to_end_latency
            .record(publish_ts.saturating_sub(event_ts));
    }
    publish_ts
}

/// Increment `sequence_gaps`.  Call when a sequence gap is detected.
#[inline]
pub fn record_sequence_gap(m: &ConnectorMetrics) {
    m.sequence_gaps.increment();
}

/// Increment `stale_books`.  Call when a book is marked stale.
#[inline]
pub fn record_book_stale(m: &ConnectorMetrics) {
    m.stale_books.increment();
}

/// Increment `offer_failures`.  Call when an Aeron offer returns a
/// back-pressure or error result code.
#[inline]
pub fn record_offer_failure(m: &ConnectorMetrics) {
    m.offer_failures.increment();
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn nanos_ago(ns: i64) -> i64 {
        now_nanos() - ns
    }

    // -----------------------------------------------------------------------
    // record_publish
    // -----------------------------------------------------------------------

    #[test]
    fn record_publish_increments_messages_out() {
        let m = ConnectorMetrics::new();
        let event_ts = nanos_ago(1_000_000); // 1 ms ago
        let recv_ts = nanos_ago(100_000); // 100 µs ago
        record_publish(&m, event_ts, recv_ts);
        assert_eq!(m.messages_out.get(), 1);
    }

    #[test]
    fn record_publish_accumulates_over_multiple_calls() {
        let m = ConnectorMetrics::new();
        let event_ts = nanos_ago(1_000_000);
        let recv_ts = nanos_ago(100_000);
        for _ in 0..100 {
            record_publish(&m, event_ts, recv_ts);
        }
        assert_eq!(m.messages_out.get(), 100);
    }

    #[test]
    fn record_publish_records_all_three_latency_hops() {
        let m = ConnectorMetrics::new();
        let event_ts = nanos_ago(1_000_000); // 1 ms ago
        let recv_ts = nanos_ago(100_000); // 100 µs ago
        record_publish(&m, event_ts, recv_ts);

        assert_eq!(m.wire_latency.count(), 1, "wire_latency not recorded");
        assert_eq!(
            m.processing_latency.count(),
            1,
            "processing_latency not recorded"
        );
        assert_eq!(
            m.end_to_end_latency.count(),
            1,
            "end_to_end_latency not recorded"
        );
    }

    #[test]
    fn record_publish_skips_latencies_when_event_ts_is_none() {
        let m = ConnectorMetrics::new();
        let recv_ts = nanos_ago(100_000);
        record_publish(&m, TS_NONE, recv_ts);

        // messages_out still increments — we did publish the message
        assert_eq!(m.messages_out.get(), 1);
        // processing_latency is recorded (only needs recv_ts, e.g. bookTicker)
        assert_eq!(m.processing_latency.count(), 1);
        // wire and e2e require a valid exchange timestamp — stay empty
        assert_eq!(m.wire_latency.count(), 0);
        assert_eq!(m.end_to_end_latency.count(), 0);
    }

    #[test]
    fn record_publish_skips_latencies_when_recv_ts_is_none() {
        let m = ConnectorMetrics::new();
        let event_ts = nanos_ago(1_000_000);
        record_publish(&m, event_ts, TS_NONE);

        assert_eq!(m.messages_out.get(), 1);
        assert_eq!(m.wire_latency.count(), 0);
        assert_eq!(m.processing_latency.count(), 0);
        assert_eq!(m.end_to_end_latency.count(), 0);
    }

    #[test]
    fn record_publish_skips_latencies_when_both_ts_none() {
        let m = ConnectorMetrics::new();
        record_publish(&m, TS_NONE, TS_NONE);

        assert_eq!(m.messages_out.get(), 1);
        assert_eq!(m.wire_latency.count(), 0);
    }

    #[test]
    fn record_publish_wire_latency_lands_in_correct_bucket() {
        // wire_latency = recv_ts - event_ts = 900 µs → bucket le=1_000_000 (1ms)
        let m = ConnectorMetrics::new();
        let event_ts = nanos_ago(1_000_000); // 1 ms ago
        let recv_ts = nanos_ago(100_000); // 100 µs ago → wire = 900 µs
        for _ in 0..100 {
            record_publish(&m, event_ts, recv_ts);
        }
        // p50 and p99 should both land in the 1ms bucket (900 µs < 1 ms)
        let p50 = m.wire_latency.p50().expect("p50 should be Some");
        let p99 = m.wire_latency.p99().expect("p99 should be Some");
        assert_eq!(p50, p99, "all samples are identical so p50 == p99");
        assert!(
            p50 <= 1_000_000,
            "900 µs should land at or below the 1ms bucket: got {p50}"
        );
    }

    /// End-to-end demo: simulate 1 000 frames through the hot path and verify
    /// that p50/p99 latency numbers are non-None.
    #[test]
    fn p50_and_p99_produced_after_simulated_pipeline() {
        let m = ConnectorMetrics::new();

        // Wire latency ≈ 900 µs, processing ≈ 100 µs (approximation; actual
        // processing_latency depends on real clock, so we only verify it is recorded)
        let event_ts = nanos_ago(1_000_000);
        let recv_ts = nanos_ago(100_000);

        for _ in 0..1_000 {
            record_publish(&m, event_ts, recv_ts);
        }

        assert_eq!(m.messages_out.get(), 1_000);
        assert_eq!(m.wire_latency.count(), 1_000);

        let p50 = m.wire_latency.p50();
        let p99 = m.wire_latency.p99();
        assert!(p50.is_some(), "p50 must be Some after 1000 samples");
        assert!(p99.is_some(), "p99 must be Some after 1000 samples");

        let e2e_p99 = m.end_to_end_latency.p99();
        assert!(
            e2e_p99.is_some(),
            "end-to-end p99 must be Some after 1000 samples"
        );
    }

    #[test]
    fn record_publish_returns_publish_ts_after_recv_ts() {
        let recv_ts = now_nanos();
        let m = ConnectorMetrics::new();
        let publish_ts = record_publish(&m, recv_ts - 1_000_000, recv_ts);
        assert!(
            publish_ts >= recv_ts,
            "publish_ts ({publish_ts}) must be >= recv_ts ({recv_ts})"
        );
    }

    // -----------------------------------------------------------------------
    // Helper counters
    // -----------------------------------------------------------------------

    #[test]
    fn record_sequence_gap_increments_counter() {
        let m = ConnectorMetrics::new();
        record_sequence_gap(&m);
        record_sequence_gap(&m);
        assert_eq!(m.sequence_gaps.get(), 2);
    }

    #[test]
    fn record_book_stale_increments_counter() {
        let m = ConnectorMetrics::new();
        record_book_stale(&m);
        assert_eq!(m.stale_books.get(), 1);
    }

    #[test]
    fn record_offer_failure_increments_counter() {
        let m = ConnectorMetrics::new();
        record_offer_failure(&m);
        record_offer_failure(&m);
        record_offer_failure(&m);
        assert_eq!(m.offer_failures.get(), 3);
    }
}
