/// Per-symbol recovery buffer for Binance USDT-M Futures (§5.25).
///
/// Holds depth deltas while the book is stale (gap detected or awaiting
/// initial snapshot).  Same three hard limits as the Spot version:
///   - 2,048 events
///   - 4 MiB cumulative encoded size
///   - 10 s age of the oldest buffered event
use std::collections::VecDeque;

use connector_core::BookDelta;

pub const MAX_EVENTS: usize = 2_048;
pub const MAX_BYTES: usize = 4 * 1_024 * 1_024;
pub const MAX_AGE_NS: i64 = 10_000_000_000;

#[derive(Debug, Clone)]
pub struct BufferedDelta {
    pub delta: BookDelta,
    pub recv_ts: i64,
    pub encoded_size: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PushResult {
    Accepted,
    Overflow(OverflowReason),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OverflowReason {
    Age,
    EventCount,
    ByteSize,
}

#[derive(Debug)]
pub struct RecoveryBuffer {
    events: VecDeque<BufferedDelta>,
    total_bytes: usize,
    max_events: usize,
    max_bytes: usize,
    max_age_ns: i64,
}

impl RecoveryBuffer {
    pub fn new() -> Self {
        Self::with_limits(MAX_EVENTS, MAX_BYTES, MAX_AGE_NS)
    }

    pub fn with_limits(max_events: usize, max_bytes: usize, max_age_ns: i64) -> Self {
        Self {
            events: VecDeque::new(),
            total_bytes: 0,
            max_events,
            max_bytes,
            max_age_ns,
        }
    }

    /// Buffer a depth delta.  Limits evaluated in order: age → count → bytes.
    pub fn push(&mut self, delta: BookDelta, recv_ts: i64, encoded_size: usize) -> PushResult {
        if let Some(oldest) = self.events.front() {
            if recv_ts.saturating_sub(oldest.recv_ts) > self.max_age_ns {
                return PushResult::Overflow(OverflowReason::Age);
            }
        }
        if self.events.len() >= self.max_events {
            return PushResult::Overflow(OverflowReason::EventCount);
        }
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

    /// Drain events whose `final_update_id > snapshot_id`.
    ///
    /// Events at or before the snapshot are discarded (already incorporated).
    /// The buffer is left empty and the byte counter reset.
    pub fn drain_after(&mut self, snapshot_id: u64) -> Vec<BufferedDelta> {
        let events = std::mem::take(&mut self.events);
        self.total_bytes = 0;
        events
            .into_iter()
            .filter(|e| e.delta.final_update_id > snapshot_id)
            .collect()
    }

    pub fn clear(&mut self) {
        self.events.clear();
        self.total_bytes = 0;
    }

    pub fn len(&self) -> usize {
        self.events.len()
    }
    pub fn is_empty(&self) -> bool {
        self.events.is_empty()
    }
    pub fn total_bytes(&self) -> usize {
        self.total_bytes
    }
    pub fn oldest_recv_ts(&self) -> Option<i64> {
        self.events.front().map(|e| e.recv_ts)
    }
    pub fn events(&self) -> impl Iterator<Item = &BufferedDelta> {
        self.events.iter()
    }
}

impl Default for RecoveryBuffer {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use connector_core::{
        BookDelta, MarketType, MessageHeader, MessageType, VenueId, SCHEMA_VERSION, TS_NONE,
        UPDATE_ID_NONE,
    };

    fn make_delta(first: u64, final_: u64) -> BookDelta {
        BookDelta {
            header: MessageHeader {
                schema_version: SCHEMA_VERSION,
                message_type: MessageType::BookDelta,
                venue_id: VenueId::BinanceFutures,
                market_type: MarketType::UsdmFutures,
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

    #[test]
    fn new_buffer_is_empty() {
        let buf = RecoveryBuffer::new();
        assert!(buf.is_empty());
        assert_eq!(buf.total_bytes(), 0);
        assert_eq!(buf.oldest_recv_ts(), None);
    }

    #[test]
    fn push_accepted_and_len_grows() {
        let mut buf = RecoveryBuffer::new();
        assert_eq!(buf.push(make_delta(1, 10), 0, 200), PushResult::Accepted);
        assert_eq!(buf.len(), 1);
        assert_eq!(buf.total_bytes(), 200);
    }

    #[test]
    fn age_overflow_fires_first() {
        let mut buf = RecoveryBuffer::with_limits(1, MAX_BYTES, 5 * SEC);
        buf.push(make_delta(1, 10), 0, 100);
        // event count would also overflow (len == max_events), but age is checked first
        assert_eq!(
            buf.push(make_delta(11, 20), 6 * SEC, 100),
            PushResult::Overflow(OverflowReason::Age)
        );
    }

    #[test]
    fn event_count_overflow() {
        let mut buf = RecoveryBuffer::with_limits(2, MAX_BYTES, MAX_AGE_NS);
        buf.push(make_delta(1, 10), 0, 10);
        buf.push(make_delta(11, 20), 0, 10);
        assert_eq!(
            buf.push(make_delta(21, 30), 0, 10),
            PushResult::Overflow(OverflowReason::EventCount)
        );
    }

    #[test]
    fn byte_size_overflow() {
        let mut buf = RecoveryBuffer::with_limits(MAX_EVENTS, 500, MAX_AGE_NS);
        buf.push(make_delta(1, 10), 0, 300);
        assert_eq!(
            buf.push(make_delta(11, 20), 0, 300),
            PushResult::Overflow(OverflowReason::ByteSize)
        );
        assert_eq!(buf.total_bytes(), 300); // overflow event not counted
    }

    #[test]
    fn drain_after_keeps_events_beyond_snapshot() {
        let mut buf = RecoveryBuffer::new();
        buf.push(make_delta(1, 10), 0, 100); // final 10 ≤ 15 → discard
        buf.push(make_delta(11, 15), 0, 100); // final 15 ≤ 15 → discard
        buf.push(make_delta(16, 20), 0, 100); // final 20 > 15 → keep
        let kept = buf.drain_after(15);
        assert_eq!(kept.len(), 1);
        assert_eq!(kept[0].delta.final_update_id, 20);
        assert!(buf.is_empty());
    }

    #[test]
    fn clear_empties_buffer() {
        let mut buf = RecoveryBuffer::new();
        buf.push(make_delta(1, 10), 0, 400);
        buf.clear();
        assert!(buf.is_empty());
        assert_eq!(buf.total_bytes(), 0);
    }
}
