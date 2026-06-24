/// Connector metrics: lock-free counters and latency histograms, Prometheus export.
///
/// # Design goals
///
/// - **No allocations on the hot path.** [`Counter::increment`], [`Counter::add`],
///   and [`Histogram::record`] are all wait-free and never allocate.
/// - **`static`-safe.** Both [`Counter`] and [`Histogram`] have `const` constructors
///   so a [`ConnectorMetrics`] instance can live in a `static` without a heap
///   allocation or a `once_cell`/`lazy_static`.
/// - **Prometheus-compatible export.** [`ConnectorMetrics::render_prometheus`]
///   produces the standard text format that a `/metrics` HTTP endpoint can
///   serve directly to a Prometheus scraper.
///
/// # Tracked metrics
///
/// | Metric | Type | Description |
/// |--------|------|-------------|
/// | `connector_wire_latency_ns` | histogram | Exchange event ts → local recv |
/// | `connector_processing_latency_ns` | histogram | Local recv → Aeron offer |
/// | `connector_end_to_end_latency_ns` | histogram | Exchange event ts → Aeron offer |
/// | `connector_messages_in_total` | counter | Raw WebSocket frames in |
/// | `connector_messages_out_total` | counter | Normalized messages offered to Aeron |
/// | `connector_reconnects_total` | counter | WebSocket reconnect attempts |
/// | `connector_stale_books_total` | counter | Books marked stale |
/// | `connector_sequence_gaps_total` | counter | Sequence gaps detected |
/// | `connector_offer_failures_total` | counter | Aeron offer failures |
/// | `connector_decode_errors_total` | counter | Frames that failed decode |
///
/// # Example
///
/// ```rust
/// use connector_metrics::ConnectorMetrics;
///
/// static METRICS: ConnectorMetrics = ConnectorMetrics::new();
///
/// fn on_frame_received(recv_ts: i64, event_ts: i64) {
///     METRICS.messages_in.increment();
///     METRICS.wire_latency.record(recv_ts - event_ts);
/// }
///
/// fn scrape_handler() -> String {
///     METRICS.render_prometheus()
/// }
/// ```
mod counter;
mod histogram;
mod registry;
mod render;

pub use counter::Counter;
pub use histogram::{Histogram, BUCKET_BOUNDS, NUM_BOUNDS, NUM_BUCKETS};
pub use registry::ConnectorMetrics;
pub use render::render_prometheus;

/// Convenience alias for a heap-allocated, shareable metrics registry.
///
/// Use this to thread a single [`ConnectorMetrics`] instance through multiple
/// components (connection manager, normalizer, publisher) without copying.
///
/// ```rust
/// use connector_metrics::{ConnectorMetrics, MetricsHandle};
/// use std::sync::Arc;
///
/// let metrics: MetricsHandle = Arc::new(ConnectorMetrics::new());
/// metrics.reconnects.increment();
/// ```
pub type MetricsHandle = std::sync::Arc<ConnectorMetrics>;
