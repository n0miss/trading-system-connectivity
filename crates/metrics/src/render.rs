use std::fmt::Write as FmtWrite;
use std::sync::atomic::Ordering;

use crate::counter::Counter;
use crate::histogram::{Histogram, BUCKET_BOUNDS};
use crate::registry::ConnectorMetrics;

// ---------------------------------------------------------------------------
// Public entry point
// ---------------------------------------------------------------------------

/// Render all metrics in `registry` as a Prometheus text-format scrape body.
///
/// Allocates a `String`; only called on the scrape path, never on the hot path.
pub fn render_prometheus(m: &ConnectorMetrics) -> String {
    let mut out = String::with_capacity(8192);

    // Counters
    for c in m.counters() {
        write_counter(&mut out, c);
    }

    // Histograms
    for h in m.histograms() {
        write_histogram(&mut out, h);
    }

    out
}

// ---------------------------------------------------------------------------
// Per-metric renderers
// ---------------------------------------------------------------------------

fn write_counter(out: &mut String, c: &Counter) {
    // Prometheus convention: counter metric names end with `_total`.
    let name = c.name();
    let full = if name.ends_with("_total") {
        name.to_string()
    } else {
        format!("{name}_total")
    };
    let _ = writeln!(out, "# HELP {full} {}", c.help());
    let _ = writeln!(out, "# TYPE {full} counter");
    let _ = writeln!(out, "{full} {}", c.get());
}

fn write_histogram(out: &mut String, h: &Histogram) {
    let name = h.name();
    let _ = writeln!(out, "# HELP {name} {}", h.help());
    let _ = writeln!(out, "# TYPE {name} histogram");

    // Buckets are cumulative (Prometheus convention).
    let mut cumulative = 0u64;
    for (i, &upper) in BUCKET_BOUNDS.iter().enumerate() {
        cumulative += h.buckets[i].load(Ordering::Relaxed);
        let _ = writeln!(out, r#"{name}_bucket{{le="{upper}"}} {cumulative}"#);
    }
    // +Inf bucket
    let total = h.count.load(Ordering::Relaxed);
    let _ = writeln!(out, r#"{name}_bucket{{le="+Inf"}} {total}"#);
    let _ = writeln!(out, "{name}_sum {}", h.sum.load(Ordering::Relaxed));
    let _ = writeln!(out, "{name}_count {total}");
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::counter::Counter;
    use crate::histogram::{Histogram, NUM_BOUNDS};
    use crate::registry::ConnectorMetrics;

    // -----------------------------------------------------------------------
    // Counter rendering
    // -----------------------------------------------------------------------

    #[test]
    fn counter_renders_help_and_type() {
        let c = Counter::new("test_events", "Count of test events.");
        let mut out = String::new();
        write_counter(&mut out, &c);
        assert!(out.contains("# HELP test_events_total Count of test events."));
        assert!(out.contains("# TYPE test_events_total counter"));
    }

    #[test]
    fn counter_renders_value() {
        let c = Counter::new("test_events", "help");
        c.add(7);
        let mut out = String::new();
        write_counter(&mut out, &c);
        assert!(out.contains("test_events_total 7"));
    }

    #[test]
    fn counter_with_total_suffix_not_doubled() {
        let c = Counter::new("already_total", "help");
        let mut out = String::new();
        write_counter(&mut out, &c);
        assert!(out.contains("already_total "));
        assert!(!out.contains("already_total_total"));
    }

    // -----------------------------------------------------------------------
    // Histogram rendering
    // -----------------------------------------------------------------------

    #[test]
    fn histogram_renders_help_and_type() {
        let h = Histogram::new("my_latency_ns", "Latency in ns.");
        let mut out = String::new();
        write_histogram(&mut out, &h);
        assert!(out.contains("# HELP my_latency_ns Latency in ns."));
        assert!(out.contains("# TYPE my_latency_ns histogram"));
    }

    #[test]
    fn histogram_renders_correct_bucket_count() {
        let h = Histogram::new("lat_ns", "help");
        let mut out = String::new();
        write_histogram(&mut out, &h);
        // NUM_BOUNDS explicit buckets + 1 +Inf bucket
        let bucket_lines = out.lines().filter(|l| l.contains("_bucket{")).count();
        assert_eq!(bucket_lines, NUM_BOUNDS + 1);
    }

    #[test]
    fn histogram_renders_inf_bucket() {
        let h = Histogram::new("lat_ns", "help");
        let mut out = String::new();
        write_histogram(&mut out, &h);
        assert!(out.contains(r#"lat_ns_bucket{le="+Inf"} 0"#));
    }

    #[test]
    fn histogram_buckets_are_cumulative() {
        let h = Histogram::new("lat_ns", "help");
        h.record(500); // → bucket 0 (le=1000)
        h.record(3_000); // → bucket 1 (le=5000)
        let mut out = String::new();
        write_histogram(&mut out, &h);
        // le=1000 bucket should show 1 (just the 500ns sample)
        assert!(out.contains(r#"lat_ns_bucket{le="1000"} 1"#));
        // le=5000 bucket should show 2 (cumulative)
        assert!(out.contains(r#"lat_ns_bucket{le="5000"} 2"#));
        // +Inf should equal total count = 2
        assert!(out.contains(r#"lat_ns_bucket{le="+Inf"} 2"#));
    }

    #[test]
    fn histogram_renders_sum_and_count() {
        let h = Histogram::new("lat_ns", "help");
        h.record(1_000);
        h.record(2_000);
        let mut out = String::new();
        write_histogram(&mut out, &h);
        assert!(out.contains("lat_ns_sum 3000"));
        assert!(out.contains("lat_ns_count 2"));
    }

    // -----------------------------------------------------------------------
    // Full registry render
    // -----------------------------------------------------------------------

    #[test]
    fn registry_render_contains_all_counter_names() {
        let m = ConnectorMetrics::new();
        let out = render_prometheus(&m);
        for name in [
            "connector_messages_in_total",
            "connector_messages_out_total",
            "connector_reconnects_total",
            "connector_stale_books_total",
            "connector_sequence_gaps_total",
            "connector_offer_failures_total",
            "connector_decode_errors_total",
        ] {
            assert!(out.contains(name), "missing metric: {name}");
        }
    }

    #[test]
    fn registry_render_contains_all_histogram_names() {
        let m = ConnectorMetrics::new();
        let out = render_prometheus(&m);
        for name in [
            "connector_wire_latency_ns",
            "connector_processing_latency_ns",
            "connector_end_to_end_latency_ns",
        ] {
            assert!(out.contains(name), "missing histogram: {name}");
        }
    }

    #[test]
    fn registry_render_all_values_zero_on_fresh_registry() {
        let m = ConnectorMetrics::new();
        let out = render_prometheus(&m);
        // Every counter should be 0 on a fresh registry.
        assert!(out.contains("connector_reconnects_total 0"));
        assert!(out.contains("connector_stale_books_total 0"));
        assert!(out.contains(r#"connector_wire_latency_ns_bucket{le="+Inf"} 0"#));
    }
}
