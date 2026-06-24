/// Passive-instance checksum publisher.
///
/// [`ChecksumPublisher`] sits at the end of the passive instance's processing
/// pipeline.  After each [`OrderBook`] update it encodes a [`BookChecksum`]
/// message into a fixed stack buffer and offers it to the underlying
/// [`Publication`] (typically an Aeron IPC publication targeting the status
/// stream).
///
/// The encoding and offer are both on the hot path, so no heap allocation
/// is performed.  The caller keeps a single `ChecksumPublisher` per shard.
use tracing::warn;

use connector_aeron::publication::{OfferResult, Publication};
use connector_core::{BookChecksum, MessageHeader, MessageType};
use connector_order_book::OrderBook;

// ---------------------------------------------------------------------------
// Buffer sizing
// ---------------------------------------------------------------------------

/// Maximum encoded size of a `BookChecksum` message.
///
/// Header:    56 bytes
/// Symbol:     2 (u16 len) + 32 (max Binance symbol) = 34 bytes
/// update_id:  8 bytes
/// bid_depth:  4 bytes
/// ask_depth:  4 bytes
/// checksum:   8 bytes
/// Total:    114 bytes — we round up to 256 for headroom.
pub const MAX_CHECKSUM_MSG_BYTES: usize = 256;

// ---------------------------------------------------------------------------
// ChecksumPublisher
// ---------------------------------------------------------------------------

/// Encodes and publishes [`BookChecksum`] messages for the passive instance.
///
/// # Example
///
/// ```rust
/// use connector_aeron::NullPublication;
/// use connector_order_book::OrderBook;
/// use connector_core::{MarketType, MessageHeader, MessageType, VenueId, SCHEMA_VERSION, TS_NONE};
/// use connector_redundancy::ChecksumPublisher;
///
/// let pub_ = NullPublication::default();
/// let mut cp = ChecksumPublisher::new(pub_);
///
/// let mut book = OrderBook::new("BTCUSDT");
///
/// let header = MessageHeader {
///     schema_version:    SCHEMA_VERSION,
///     message_type:      MessageType::BookChecksum,
///     venue_id:          VenueId::BinanceSpot,
///     market_type:       MarketType::Spot,
///     instrument_id:     1,
///     connection_id:     0,
///     instance_id:       1,   // passive instance
///     sequence_number:   0,
///     exchange_event_ts: TS_NONE,
///     exchange_tx_ts:    TS_NONE,
///     local_recv_ts:     TS_NONE,
///     local_publish_ts:  TS_NONE,
/// };
///
/// let result = cp.publish(&book, header);
/// assert!(result.is_ok());
/// ```
pub struct ChecksumPublisher<P: Publication> {
    publication: P,
    buf: [u8; MAX_CHECKSUM_MSG_BYTES],
    /// Running count of successful offers (for diagnostics).
    pub published: u64,
    /// Running count of failed offers (back-pressure + errors).
    pub failed: u64,
}

impl<P: Publication> ChecksumPublisher<P> {
    /// Wrap a publication.  The publication should target the **status stream**
    /// (a separate Aeron stream from the market-data stream).
    pub fn new(publication: P) -> Self {
        Self {
            publication,
            buf: [0u8; MAX_CHECKSUM_MSG_BYTES],
            published: 0,
            failed: 0,
        }
    }

    /// Compute the book checksum and offer a [`BookChecksum`] message.
    ///
    /// `header` should be pre-filled with the correct `venue_id`,
    /// `market_type`, `instrument_id`, `instance_id`, `sequence_number`, and
    /// timestamp fields.  The `message_type` field is always overwritten with
    /// [`MessageType::BookChecksum`].
    ///
    /// Returns the raw [`OfferResult`] from the underlying publication so the
    /// caller can apply its own back-pressure policy ([`BackpressureGuard`]).
    ///
    /// [`BackpressureGuard`]: connector_aeron::BackpressureGuard
    pub fn publish(&mut self, book: &OrderBook, mut header: MessageHeader) -> OfferResult {
        header.message_type = MessageType::BookChecksum;

        let msg = BookChecksum {
            header,
            symbol: book.symbol().to_owned(),
            update_id: book.last_update_id(),
            bid_depth: book.bid_depth() as u32,
            ask_depth: book.ask_depth() as u32,
            checksum: book.checksum(),
        };

        let n = match msg.encode_into(&mut self.buf) {
            Ok(n) => n,
            Err(e) => {
                // Encoding can only fail if the symbol exceeds 65 535 bytes,
                // which is impossible for any real exchange symbol.
                warn!("BookChecksum encode error (should never happen): {e}");
                self.failed += 1;
                return OfferResult::Closed;
            }
        };

        let result = self.publication.offer(&self.buf[..n]);
        match result {
            OfferResult::Ok(_) => self.published += 1,
            _ => self.failed += 1,
        }
        result
    }

    /// Access the wrapped publication (e.g. to read stats from `NullPublication`).
    pub fn publication(&self) -> &P {
        &self.publication
    }

    /// Unwrap the inner publication.
    pub fn into_inner(self) -> P {
        self.publication
    }

    /// Whether the underlying publication reports at least one connected subscriber.
    pub fn is_connected(&self) -> bool {
        self.publication.is_connected()
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use connector_aeron::publication::{ChannelPublication, NullPublication, OfferResult};
    use connector_core::{BookChecksum, MarketType, MessageType, VenueId, SCHEMA_VERSION, TS_NONE};
    use connector_order_book::OrderBook;

    fn test_header(instance_id: u32) -> MessageHeader {
        MessageHeader {
            schema_version: SCHEMA_VERSION,
            message_type: MessageType::BookChecksum,
            venue_id: VenueId::BinanceSpot,
            market_type: MarketType::Spot,
            instrument_id: 1,
            connection_id: 0,
            instance_id,
            sequence_number: 0,
            exchange_event_ts: TS_NONE,
            exchange_tx_ts: TS_NONE,
            local_recv_ts: TS_NONE,
            local_publish_ts: TS_NONE,
        }
    }

    fn empty_book() -> OrderBook {
        OrderBook::new("BTCUSDT")
    }

    // -----------------------------------------------------------------------
    // Basic publish behaviour
    // -----------------------------------------------------------------------

    #[test]
    fn publish_on_null_publication_returns_ok() {
        let mut cp = ChecksumPublisher::new(NullPublication::default());
        let result = cp.publish(&empty_book(), test_header(1));
        assert!(result.is_ok());
    }

    #[test]
    fn publish_increments_published_counter_on_success() {
        let mut cp = ChecksumPublisher::new(NullPublication::default());
        cp.publish(&empty_book(), test_header(1));
        cp.publish(&empty_book(), test_header(1));
        assert_eq!(cp.published, 2);
        assert_eq!(cp.failed, 0);
    }

    #[test]
    fn publish_increments_failed_on_back_pressure() {
        let (pub_, _rx) = ChannelPublication::new(0); // capacity 0 → always back-pressured
        let mut cp = ChecksumPublisher::new(pub_);
        let result = cp.publish(&empty_book(), test_header(1));
        assert_eq!(result, OfferResult::BackPressured);
        assert_eq!(cp.failed, 1);
        assert_eq!(cp.published, 0);
    }

    // -----------------------------------------------------------------------
    // Encoded message is decodable
    // -----------------------------------------------------------------------

    #[test]
    fn published_message_decodes_correctly() {
        let (pub_, rx) = ChannelPublication::new(8);
        let mut cp = ChecksumPublisher::new(pub_);

        let mut book = OrderBook::new("ETHUSDT");
        // Apply some levels so the checksum is non-trivial.
        use connector_core::{BookDelta, PriceLevel, UPDATE_ID_NONE};
        let delta = BookDelta {
            header: test_header(1),
            symbol: "ETHUSDT".into(),
            price_scale: 2,
            qty_scale: 3,
            first_update_id: 100,
            final_update_id: 100,
            prev_update_id: UPDATE_ID_NONE,
            bids: vec![PriceLevel {
                price: 3000_00,
                qty: 10,
            }],
            asks: vec![PriceLevel {
                price: 3001_00,
                qty: 5,
            }],
        };
        book.apply_delta(&delta);

        let hdr = {
            let mut h = test_header(1);
            h.sequence_number = 42;
            h
        };
        cp.publish(&book, hdr);

        let bytes = rx.recv().unwrap();
        let decoded = BookChecksum::decode(&bytes).expect("decode must succeed");

        assert_eq!(decoded.symbol, "ETHUSDT");
        assert_eq!(decoded.update_id, 100);
        assert_eq!(decoded.bid_depth, 1);
        assert_eq!(decoded.ask_depth, 1);
        assert_eq!(decoded.checksum, book.checksum());
        assert_eq!(decoded.header.sequence_number, 42);
        assert_eq!(decoded.header.message_type, MessageType::BookChecksum);
    }

    #[test]
    fn message_type_is_always_book_checksum_regardless_of_header_input() {
        let (pub_, rx) = ChannelPublication::new(8);
        let mut cp = ChecksumPublisher::new(pub_);

        // Pass in a header with the wrong message type — publish() must override it.
        let mut hdr = test_header(1);
        hdr.message_type = MessageType::Trade; // wrong — should be corrected

        cp.publish(&empty_book(), hdr);

        let bytes = rx.recv().unwrap();
        let decoded = BookChecksum::decode(&bytes).unwrap();
        assert_eq!(decoded.header.message_type, MessageType::BookChecksum);
    }

    // -----------------------------------------------------------------------
    // Checksum content
    // -----------------------------------------------------------------------

    #[test]
    fn passive_instance_id_is_preserved_in_encoded_message() {
        let (pub_, rx) = ChannelPublication::new(8);
        let mut cp = ChecksumPublisher::new(pub_);

        cp.publish(&empty_book(), test_header(1)); // instance 1 = passive

        let bytes = rx.recv().unwrap();
        let decoded = BookChecksum::decode(&bytes).unwrap();
        assert_eq!(decoded.header.instance_id, 1);
    }

    #[test]
    fn checksum_reflects_book_state() {
        let (pub_, rx) = ChannelPublication::new(8);
        let mut cp = ChecksumPublisher::new(pub_);

        let book = empty_book();
        cp.publish(&book, test_header(1));

        let bytes = rx.recv().unwrap();
        let decoded = BookChecksum::decode(&bytes).unwrap();
        assert_eq!(decoded.checksum, book.checksum());
    }

    #[test]
    fn consecutive_publishes_reflect_different_checksums_after_update() {
        let (pub_, rx) = ChannelPublication::new(8);
        let mut cp = ChecksumPublisher::new(pub_);

        let mut book = empty_book();

        // First publish — empty book
        cp.publish(&book, test_header(1));
        let bytes_1 = rx.recv().unwrap();
        let decoded_1 = BookChecksum::decode(&bytes_1).unwrap();

        // Apply a delta
        use connector_core::{BookDelta, PriceLevel, UPDATE_ID_NONE};
        let d = BookDelta {
            header: test_header(1),
            symbol: "BTCUSDT".into(),
            price_scale: 2,
            qty_scale: 3,
            first_update_id: 1,
            final_update_id: 1,
            prev_update_id: UPDATE_ID_NONE,
            bids: vec![PriceLevel { price: 100, qty: 5 }],
            asks: vec![],
        };
        book.apply_delta(&d);

        // Second publish — book has changed
        cp.publish(&book, test_header(1));
        let bytes_2 = rx.recv().unwrap();
        let decoded_2 = BookChecksum::decode(&bytes_2).unwrap();

        assert_ne!(
            decoded_1.checksum, decoded_2.checksum,
            "checksum must differ after book update"
        );
        assert_eq!(decoded_2.bid_depth, 1);
        assert_eq!(decoded_2.update_id, 1);
    }

    // -----------------------------------------------------------------------
    // Accessors
    // -----------------------------------------------------------------------

    #[test]
    fn is_connected_reflects_publication() {
        let cp = ChecksumPublisher::new(NullPublication::default());
        assert!(cp.is_connected());
    }

    #[test]
    fn into_inner_returns_publication() {
        let cp = ChecksumPublisher::new(NullPublication::default());
        let pub_ = cp.into_inner();
        assert_eq!(pub_.messages_offered, 0);
    }

    #[test]
    fn publication_accessor_readable() {
        let mut cp = ChecksumPublisher::new(NullPublication::default());
        cp.publish(&empty_book(), test_header(1));
        assert_eq!(cp.publication().messages_offered, 1);
    }
}
