use std::sync::mpsc;

// ---------------------------------------------------------------------------
// OfferResult
// ---------------------------------------------------------------------------

/// Result of a single `Publication::offer` call.
///
/// Models the Aeron C-client return-value convention so that a real Aeron
/// `Publication` can implement the trait with zero additional ceremony.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OfferResult {
    /// Message accepted. Inner value is the new stream position.
    Ok(i64),
    /// Publication buffer is full; caller should retry.
    BackPressured,
    /// Aeron media driver is performing an admin action; retry immediately.
    AdminAction,
    /// Publication has been closed.
    Closed,
    /// Stream position limit exceeded (rare; indicates a stalled consumer).
    MaxPositionExceeded,
}

impl OfferResult {
    /// Returns `true` for the happy path.
    pub fn is_ok(self) -> bool {
        matches!(self, Self::Ok(_))
    }

    /// Returns `true` when the caller should retry the same offer.
    pub fn should_retry(self) -> bool {
        matches!(self, Self::BackPressured | Self::AdminAction)
    }
}

// ---------------------------------------------------------------------------
// Publication trait
// ---------------------------------------------------------------------------

/// A single Aeron publication (one logical stream).
///
/// Implementations are `!Sync` by design — only one writer per stream.
/// For a real Aeron binding, implement this trait over the native
/// `aeron_publication_t *` and call `aeron_publication_offer`.
pub trait Publication {
    /// Offer `bytes` to the stream.
    ///
    /// Non-blocking. Returns [`OfferResult`] directly without panicking so the
    /// caller can apply its own backpressure policy (Stage 9).
    fn offer(&mut self, bytes: &[u8]) -> OfferResult;

    /// Whether at least one subscriber is connected to this publication.
    fn is_connected(&self) -> bool;
}

// ---------------------------------------------------------------------------
// NullPublication
// ---------------------------------------------------------------------------

/// A publication that discards every message and accumulates counters.
///
/// Useful for benchmarking the upstream pipeline without an Aeron media
/// driver, and for tests that verify routing logic without I/O.
#[derive(Debug, Default)]
pub struct NullPublication {
    pub messages_offered: u64,
    pub bytes_offered:    u64,
}

impl Publication for NullPublication {
    fn offer(&mut self, bytes: &[u8]) -> OfferResult {
        self.messages_offered += 1;
        self.bytes_offered    += bytes.len() as u64;
        OfferResult::Ok(self.bytes_offered as i64)
    }

    fn is_connected(&self) -> bool { true }
}

// ---------------------------------------------------------------------------
// ChannelPublication
// ---------------------------------------------------------------------------

/// A publication that forwards encoded frames to an mpsc channel.
///
/// Each encoded message is heap-copied once into a `Vec<u8>` and sent.
/// The receiver end is typically held by a test or a downstream consumer.
/// Returns [`OfferResult::BackPressured`] when the channel is full and
/// [`OfferResult::Closed`] when the receiver has been dropped.
pub struct ChannelPublication {
    tx: mpsc::SyncSender<Vec<u8>>,
    position: i64,
}

impl ChannelPublication {
    /// Create a new channel-backed publication with the given buffer capacity.
    /// Returns `(publication, receiver)`.
    pub fn new(capacity: usize) -> (Self, mpsc::Receiver<Vec<u8>>) {
        let (tx, rx) = mpsc::sync_channel(capacity);
        (Self { tx, position: 0 }, rx)
    }
}

impl Publication for ChannelPublication {
    fn offer(&mut self, bytes: &[u8]) -> OfferResult {
        match self.tx.try_send(bytes.to_vec()) {
            Ok(()) => {
                self.position += bytes.len() as i64;
                OfferResult::Ok(self.position)
            }
            Err(mpsc::TrySendError::Full(_))         => OfferResult::BackPressured,
            Err(mpsc::TrySendError::Disconnected(_)) => OfferResult::Closed,
        }
    }

    fn is_connected(&self) -> bool { true }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn null_publication_counts_messages_and_bytes() {
        let mut pub_ = NullPublication::default();
        pub_.offer(b"hello");
        pub_.offer(b"world!");
        assert_eq!(pub_.messages_offered, 2);
        assert_eq!(pub_.bytes_offered, 11);
    }

    #[test]
    fn null_publication_is_always_connected() {
        let pub_ = NullPublication::default();
        assert!(pub_.is_connected());
    }

    #[test]
    fn null_publication_offer_result_is_ok() {
        let mut pub_ = NullPublication::default();
        let result = pub_.offer(b"test");
        assert!(result.is_ok());
        assert!(!result.should_retry());
    }

    #[test]
    fn channel_publication_receiver_gets_bytes() {
        let (mut pub_, rx) = ChannelPublication::new(4);
        let payload = b"encoded_message";
        let result  = pub_.offer(payload);
        assert!(result.is_ok());
        assert_eq!(rx.recv().unwrap(), payload.as_slice());
    }

    #[test]
    fn channel_publication_back_pressured_when_full() {
        let (mut pub_, _rx) = ChannelPublication::new(1);
        pub_.offer(b"a");
        let result = pub_.offer(b"b");
        assert_eq!(result, OfferResult::BackPressured);
        assert!(result.should_retry());
    }

    #[test]
    fn channel_publication_closed_when_receiver_dropped() {
        let (mut pub_, rx) = ChannelPublication::new(1);
        drop(rx);
        let result = pub_.offer(b"orphan");
        assert_eq!(result, OfferResult::Closed);
        assert!(!result.should_retry());
    }

    #[test]
    fn offer_result_ok_is_ok_and_not_retry() {
        assert!(OfferResult::Ok(42).is_ok());
        assert!(!OfferResult::Ok(42).should_retry());
    }

    #[test]
    fn offer_result_back_pressured_is_not_ok_but_retry() {
        assert!(!OfferResult::BackPressured.is_ok());
        assert!(OfferResult::BackPressured.should_retry());
    }

    #[test]
    fn offer_result_admin_action_should_retry() {
        assert!(OfferResult::AdminAction.should_retry());
    }

    #[test]
    fn offer_result_closed_is_not_ok_and_not_retry() {
        assert!(!OfferResult::Closed.is_ok());
        assert!(!OfferResult::Closed.should_retry());
    }
}
