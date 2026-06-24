/// Per-symbol recovery buffer — holds depth deltas while the book is stale.
///
/// Three hard limits (addendum §1):
///   - **2,048 events**
///   - **4 MiB** cumulative encoded size
///   - **10 s** age of the oldest buffered event
///
/// All three are checked on every [`push`] call in the order: age → count → bytes.
/// If any limit is exceeded the delta is **not** buffered and
/// [`PushResult::Overflow`] is returned — the caller must call [`clear`] and
/// transition the channel to DEGRADED (Stage 3.15).
///
/// [`push`]:  RecoveryBuffer::push
/// [`clear`]: RecoveryBuffer::clear
use std::collections::VecDeque;

use connector_core::BookDelta;

// ---------------------------------------------------------------------------
// Constants (addendum §1)
// ---------------------------------------------------------------------------

pub const MAX_EVENTS: usize = 2_048;
pub const MAX_BYTES: usize = 4 * 1_024 * 1_024; // 4 MiB
pub const MAX_AGE_NS: i64 = 10_000_000_000; // 10 s

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

/// A depth delta held inside the recovery buffer.
#[derive(Debug, Clone)]
pub struct BufferedDelta {
    /// The normalised depth delta, ready to replay through the sequence validator.
    pub delta: BookDelta,
    /// Nanosecond timestamp when the WebSocket frame was received.
    pub recv_ts: i64,
    /// Binary-encoded byte size; counts toward the byte-budget limit.
    pub encoded_size: usize,
}

/// Outcome of a [`RecoveryBuffer::push`] call.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PushResult {
    /// Delta was accepted and is now buffered.
    Accepted,
    /// A limit was exceeded; the delta was **not** buffered.
    ///
    /// The caller must call [`RecoveryBuffer::clear`] and transition to DEGRADED.
    Overflow(OverflowReason),
}

/// Which limit caused the overflow.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OverflowReason {
    /// Oldest buffered event is more than 10 s old.
    Age,
    /// Buffer holds 2,048 events already.
    EventCount,
    /// Adding this event would exceed 4 MiB.
    ByteSize,
}

// ---------------------------------------------------------------------------
// RecoveryBuffer
// ---------------------------------------------------------------------------

#[derive(Debug)]
pub struct RecoveryBuffer {
    events: VecDeque<BufferedDelta>,
    total_bytes: usize,
    max_events: usize,
    max_bytes: usize,
    max_age_ns: i64,
}

impl RecoveryBuffer {
    /// Create a buffer with the addendum §1 defaults.
    pub fn new() -> Self {
        Self::with_limits(MAX_EVENTS, MAX_BYTES, MAX_AGE_NS)
    }

    /// Create a buffer with custom limits — primarily for unit tests.
    pub fn with_limits(max_events: usize, max_bytes: usize, max_age_ns: i64) -> Self {
        Self {
            events: VecDeque::new(),
            total_bytes: 0,
            max_events,
            max_bytes,
            max_age_ns,
        }
    }

    // --- Push ---------------------------------------------------------------

    /// Buffer a depth delta.
    ///
    /// `recv_ts` is the nanosecond receive timestamp from the WebSocket frame.
    /// `encoded_size` is the binary-encoded byte length (from `encode_into`).
    ///
    /// Limits are evaluated in order: **age → count → bytes**.
    /// On `Overflow`, the delta is NOT stored — call [`clear`] then start DEGRADED.
    ///
    /// [`clear`]: RecoveryBuffer::clear
    pub fn push(&mut self, delta: BookDelta, recv_ts: i64, encoded_size: usize) -> PushResult {
        // 1. Age: has the oldest buffered event been waiting too long?
        if let Some(oldest) = self.events.front() {
            if recv_ts.saturating_sub(oldest.recv_ts) > self.max_age_ns {
                return PushResult::Overflow(OverflowReason::Age);
            }
        }

        // 2. Event count.
        if self.events.len() >= self.max_events {
            return PushResult::Overflow(OverflowReason::EventCount);
        }

        // 3. Byte budget.
        if self.total_bytes.saturating_add(encoded_size) > self.max_bytes {
            return PushResult::Overflow(OverflowReason::ByteSize);
        }

        self.total_bytes += encoded_size;
        self.events.push_back(BufferedDelta {
            delta,
            recv_ts,
            encoded_size,
        });
        PushResult::Accepted
    }

    // --- Recovery drain -----------------------------------------------------

    /// Drain events after applying a REST snapshot with `last_update_id = snapshot_id`.
    ///
    /// Events where `final_update_id ≤ snapshot_id` are silently discarded
    /// (they are already incorporated in the snapshot).  The rest are returned
    /// in chronological order for replay through the sequence validator.
    ///
    /// The buffer is left empty and the byte counter is reset.
    pub fn drain_after(&mut self, snapshot_id: u64) -> Vec<BufferedDelta> {
        let events = std::mem::take(&mut self.events);
        self.total_bytes = 0;
        events
            .into_iter()
            .filter(|e| e.delta.final_update_id > snapshot_id)
            .collect()
    }

    // --- Reset --------------------------------------------------------------

    /// Discard all buffered events.
    ///
    /// Called on buffer overflow (before transitioning to DEGRADED, Stage 3.15)
    /// or on WebSocket reconnect.
    pub fn clear(&mut self) {
        self.events.clear();
        self.total_bytes = 0;
    }

    // --- Accessors ----------------------------------------------------------

    pub fn len(&self) -> usize {
        self.events.len()
    }
    pub fn total_bytes(&self) -> usize {
        self.total_bytes
    }
    pub fn is_empty(&self) -> bool {
        self.events.is_empty()
    }
    pub fn max_events(&self) -> usize {
        self.max_events
    }
    pub fn max_bytes(&self) -> usize {
        self.max_bytes
    }
    pub fn max_age_ns(&self) -> i64 {
        self.max_age_ns
    }

    /// Receive timestamp of the oldest buffered event, or `None` if empty.
    pub fn oldest_recv_ts(&self) -> Option<i64> {
        self.events.front().map(|e| e.recv_ts)
    }

    /// Iterate buffered events in chronological order (oldest first).
    pub fn events(&self) -> impl Iterator<Item = &BufferedDelta> {
        self.events.iter()
    }
}

impl Default for RecoveryBuffer {
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
    use connector_core::{
        BookDelta, MarketType, MessageHeader, MessageType, VenueId, SCHEMA_VERSION, TS_NONE,
        UPDATE_ID_NONE,
    };

    // -----------------------------------------------------------------------
    // Helpers
    // -----------------------------------------------------------------------

    fn make_delta(first: u64, final_: u64) -> BookDelta {
        BookDelta {
            header: MessageHeader {
                schema_version: SCHEMA_VERSION,
                message_type: MessageType::BookDelta,
                venue_id: VenueId::BinanceSpot,
                market_type: MarketType::Spot,
                instrument_id: 1,
                connection_id: 0,
                instance_id: 0,
                sequence_number: 0,
                exchange_event_ts: TS_NONE,
                exchange_tx_ts: TS_NONE,
                local_recv_ts: TS_NONE,
                local_publish_ts: TS_NONE,
            },
            symbol: "BTCUSDT".to_string(),
            price_scale: 2,
            qty_scale: 3,
            first_update_id: first,
            final_update_id: final_,
            prev_update_id: UPDATE_ID_NONE,
            bids: vec![],
            asks: vec![],
        }
    }

    const SEC: i64 = 1_000_000_000;

    // -----------------------------------------------------------------------
    // Initial state
    // -----------------------------------------------------------------------

    #[test]
    fn new_buffer_is_empty() {
        let buf = RecoveryBuffer::new();
        assert!(buf.is_empty());
        assert_eq!(buf.len(), 0);
        assert_eq!(buf.total_bytes(), 0);
        assert_eq!(buf.oldest_recv_ts(), None);
    }

    #[test]
    fn new_buffer_has_default_limits() {
        let buf = RecoveryBuffer::new();
        assert_eq!(buf.max_events(), MAX_EVENTS);
        assert_eq!(buf.max_bytes(), MAX_BYTES);
        assert_eq!(buf.max_age_ns(), MAX_AGE_NS);
    }

    // -----------------------------------------------------------------------
    // Push — happy path
    // -----------------------------------------------------------------------

    #[test]
    fn push_single_event_is_accepted() {
        let mut buf = RecoveryBuffer::new();
        let r = buf.push(make_delta(1, 10), 0, 256);
        assert_eq!(r, PushResult::Accepted);
        assert_eq!(buf.len(), 1);
        assert_eq!(buf.total_bytes(), 256);
    }

    #[test]
    fn push_accumulates_byte_count() {
        let mut buf = RecoveryBuffer::new();
        buf.push(make_delta(1, 10), 0, 100);
        buf.push(make_delta(11, 20), SEC, 200);
        buf.push(make_delta(21, 30), 2 * SEC, 300);
        assert_eq!(buf.len(), 3);
        assert_eq!(buf.total_bytes(), 600);
    }

    #[test]
    fn oldest_recv_ts_is_first_pushed() {
        let mut buf = RecoveryBuffer::new();
        buf.push(make_delta(1, 10), 1_000, 100);
        buf.push(make_delta(11, 20), 2_000, 100);
        assert_eq!(buf.oldest_recv_ts(), Some(1_000));
    }

    // -----------------------------------------------------------------------
    // Push — overflow: age
    // -----------------------------------------------------------------------

    #[test]
    fn push_overflows_when_oldest_event_exceeds_max_age() {
        let mut buf = RecoveryBuffer::with_limits(100, 10 * 1024 * 1024, 5 * SEC);
        buf.push(make_delta(1, 10), 0, 100); // recv_ts = 0

        // 6 s later: oldest event is 6 s old > 5 s limit
        let r = buf.push(make_delta(11, 20), 6 * SEC, 100);
        assert_eq!(r, PushResult::Overflow(OverflowReason::Age));
    }

    #[test]
    fn push_at_exact_age_limit_is_not_overflow() {
        let mut buf = RecoveryBuffer::with_limits(100, 10 * 1024 * 1024, 5 * SEC);
        buf.push(make_delta(1, 10), 0, 100);

        // Exactly at the limit: not yet exceeded (strict >).
        let r = buf.push(make_delta(11, 20), 5 * SEC, 100);
        assert_eq!(r, PushResult::Accepted);
    }

    #[test]
    fn age_overflow_does_not_buffer_the_event() {
        let mut buf = RecoveryBuffer::with_limits(100, 10 * 1024 * 1024, 5 * SEC);
        buf.push(make_delta(1, 10), 0, 100);
        buf.push(make_delta(11, 20), 6 * SEC, 100); // overflow
        assert_eq!(buf.len(), 1); // only the first event is buffered
    }

    // -----------------------------------------------------------------------
    // Push — overflow: event count
    // -----------------------------------------------------------------------

    #[test]
    fn push_overflows_when_event_count_reached() {
        let mut buf = RecoveryBuffer::with_limits(3, MAX_BYTES, MAX_AGE_NS);
        buf.push(make_delta(1, 10), 0, 10);
        buf.push(make_delta(11, 20), 0, 10);
        buf.push(make_delta(21, 30), 0, 10);
        // Fourth push: count == max_events
        let r = buf.push(make_delta(31, 40), 0, 10);
        assert_eq!(r, PushResult::Overflow(OverflowReason::EventCount));
    }

    #[test]
    fn push_at_capacity_minus_one_is_accepted() {
        let mut buf = RecoveryBuffer::with_limits(3, MAX_BYTES, MAX_AGE_NS);
        buf.push(make_delta(1, 10), 0, 10);
        buf.push(make_delta(11, 20), 0, 10);
        let r = buf.push(make_delta(21, 30), 0, 10);
        assert_eq!(r, PushResult::Accepted);
        assert_eq!(buf.len(), 3);
    }

    // -----------------------------------------------------------------------
    // Push — overflow: byte size
    // -----------------------------------------------------------------------

    #[test]
    fn push_overflows_when_byte_budget_exceeded() {
        let mut buf = RecoveryBuffer::with_limits(MAX_EVENTS, 500, MAX_AGE_NS);
        buf.push(make_delta(1, 10), 0, 300);
        // 300 + 300 = 600 > 500
        let r = buf.push(make_delta(11, 20), 0, 300);
        assert_eq!(r, PushResult::Overflow(OverflowReason::ByteSize));
    }

    #[test]
    fn push_byte_overflow_does_not_advance_total_bytes() {
        let mut buf = RecoveryBuffer::with_limits(MAX_EVENTS, 500, MAX_AGE_NS);
        buf.push(make_delta(1, 10), 0, 300);
        buf.push(make_delta(11, 20), 0, 300); // overflow, not stored
        assert_eq!(buf.total_bytes(), 300);
        assert_eq!(buf.len(), 1);
    }

    // -----------------------------------------------------------------------
    // Push — overflow priority order (age checked before count before bytes)
    // -----------------------------------------------------------------------

    #[test]
    fn overflow_priority_age_before_count() {
        // Buffer is also at event limit, but age fires first.
        let mut buf = RecoveryBuffer::with_limits(1, MAX_BYTES, 5 * SEC);
        buf.push(make_delta(1, 10), 0, 100); // fills the 1-event buffer
        let r = buf.push(make_delta(11, 20), 6 * SEC, 100);
        // Would be EventCount (len==1 ≥ max_events==1) AND Age, but Age comes first.
        assert_eq!(r, PushResult::Overflow(OverflowReason::Age));
    }

    // -----------------------------------------------------------------------
    // drain_after
    // -----------------------------------------------------------------------

    #[test]
    fn drain_after_discards_events_at_or_before_snapshot() {
        let mut buf = RecoveryBuffer::new();
        buf.push(make_delta(1, 10), 0, 100); // final == 10  ≤ 15 → discard
        buf.push(make_delta(11, 15), 0, 100); // final == 15  ≤ 15 → discard
        buf.push(make_delta(16, 20), 0, 100); // final == 20  > 15 → keep
        buf.push(make_delta(21, 25), 0, 100); // final == 25  > 15 → keep

        let kept = buf.drain_after(15);
        assert_eq!(kept.len(), 2);
        assert_eq!(kept[0].delta.final_update_id, 20);
        assert_eq!(kept[1].delta.final_update_id, 25);
    }

    #[test]
    fn drain_after_empties_buffer_and_resets_bytes() {
        let mut buf = RecoveryBuffer::new();
        buf.push(make_delta(1, 10), 0, 200);
        buf.push(make_delta(11, 20), 0, 300);

        let _kept = buf.drain_after(5);
        assert!(buf.is_empty());
        assert_eq!(buf.total_bytes(), 0);
    }

    #[test]
    fn drain_after_empty_buffer_returns_empty_vec() {
        let mut buf = RecoveryBuffer::new();
        let kept = buf.drain_after(100);
        assert!(kept.is_empty());
    }

    #[test]
    fn drain_after_all_stale_returns_empty() {
        let mut buf = RecoveryBuffer::new();
        buf.push(make_delta(1, 5), 0, 100);
        buf.push(make_delta(6, 10), 0, 100);
        let kept = buf.drain_after(100);
        assert!(kept.is_empty());
    }

    // -----------------------------------------------------------------------
    // clear
    // -----------------------------------------------------------------------

    #[test]
    fn clear_resets_buffer_to_empty() {
        let mut buf = RecoveryBuffer::new();
        buf.push(make_delta(1, 10), 0, 400);
        buf.push(make_delta(11, 20), 0, 400);
        buf.clear();
        assert!(buf.is_empty());
        assert_eq!(buf.total_bytes(), 0);
        assert_eq!(buf.oldest_recv_ts(), None);
    }

    #[test]
    fn clear_allows_fresh_push_after_overflow() {
        let mut buf = RecoveryBuffer::with_limits(2, MAX_BYTES, MAX_AGE_NS);
        buf.push(make_delta(1, 10), 0, 10);
        buf.push(make_delta(11, 20), 0, 10);
        let r = buf.push(make_delta(21, 30), 0, 10);
        assert_eq!(r, PushResult::Overflow(OverflowReason::EventCount));

        buf.clear();
        let r2 = buf.push(make_delta(21, 30), 0, 10);
        assert_eq!(r2, PushResult::Accepted);
    }

    // -----------------------------------------------------------------------
    // events iterator
    // -----------------------------------------------------------------------

    #[test]
    fn events_iterator_is_chronological() {
        let mut buf = RecoveryBuffer::new();
        buf.push(make_delta(1, 10), 0, 100);
        buf.push(make_delta(11, 20), 0, 200);
        buf.push(make_delta(21, 30), 0, 300);

        let finals: Vec<u64> = buf.events().map(|e| e.delta.final_update_id).collect();
        assert_eq!(finals, vec![10, 20, 30]);
    }

    // -----------------------------------------------------------------------
    // Default
    // -----------------------------------------------------------------------

    #[test]
    fn default_matches_new() {
        let buf = RecoveryBuffer::default();
        assert_eq!(buf.max_events(), MAX_EVENTS);
        assert_eq!(buf.max_bytes(), MAX_BYTES);
        assert_eq!(buf.max_age_ns(), MAX_AGE_NS);
    }
}
