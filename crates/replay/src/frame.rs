// ---------------------------------------------------------------------------
// SourceKind
// ---------------------------------------------------------------------------

/// Describes the encoding of a [`RecordedFrame`]'s payload.
///
/// The caller decides which decoder to apply; the replayer is agnostic to
/// the encoding.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SourceKind {
    /// Raw bytes as they arrived off the WebSocket (typically JSON text
    /// for Binance Spot/Futures streams).  Pass through the exchange-specific
    /// JSON parser → normalizer pipeline.
    RawWsPayload,

    /// Internal binary-encoded [`connector_core::NormalizedMessage`].
    /// Can be fed directly into the order-book or Aeron publisher.
    NormalizedMessage,

    /// Bytes sourced from an Aeron Archive recording.  The layout matches
    /// the framed binary format written by the Aeron publisher.
    AeronArchive,
}

// ---------------------------------------------------------------------------
// RecordedFrame
// ---------------------------------------------------------------------------

/// A single captured event, ready for replay.
#[derive(Debug, Clone)]
pub struct RecordedFrame {
    /// Unix nanoseconds at the moment this event was first observed.
    pub captured_at_ns: i64,
    /// Raw payload bytes.
    pub payload: Vec<u8>,
    /// How `payload` should be decoded.
    pub source_kind: SourceKind,
}

impl RecordedFrame {
    pub fn new(captured_at_ns: i64, payload: Vec<u8>, source_kind: SourceKind) -> Self {
        Self {
            captured_at_ns,
            payload,
            source_kind,
        }
    }

    /// Convenience constructor for raw WS payloads.
    pub fn raw_ws(captured_at_ns: i64, payload: Vec<u8>) -> Self {
        Self::new(captured_at_ns, payload, SourceKind::RawWsPayload)
    }

    /// Convenience constructor for normalized binary messages.
    pub fn normalized(captured_at_ns: i64, payload: Vec<u8>) -> Self {
        Self::new(captured_at_ns, payload, SourceKind::NormalizedMessage)
    }

    /// Convenience constructor for Aeron Archive frames.
    pub fn aeron_archive(captured_at_ns: i64, payload: Vec<u8>) -> Self {
        Self::new(captured_at_ns, payload, SourceKind::AeronArchive)
    }
}
