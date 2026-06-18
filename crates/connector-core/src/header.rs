use crate::{Error, MarketType, MessageType, VenueId};

pub const SCHEMA_VERSION: u8 = 1;

/// Sentinel: exchange_tx_ts field is absent for this message.
pub const TS_NONE: i64 = 0;

/// Wire layout (all integers little-endian):
///
///  Offset  Len  Field
///  0       1    schema_version (u8)
///  1       1    message_type   (u8, MessageType repr)
///  2       1    venue_id       (u8, VenueId repr)
///  3       1    market_type    (u8, MarketType repr)
///  4       4    instrument_id  (u32)
///  8       4    connection_id  (u32)
///  12      4    instance_id    (u32)
///  16      8    sequence_number (u64)
///  24      8    exchange_event_ts (i64, nanoseconds since Unix epoch)
///  32      8    exchange_tx_ts    (i64, nanoseconds; TS_NONE = not present)
///  40      8    local_recv_ts     (i64, nanoseconds since Unix epoch)
///  48      8    local_publish_ts  (i64, nanoseconds since Unix epoch)
///  --- total ---
pub const HEADER_SIZE: usize = 1 + 1 + 1 + 1   // version, msg_type, venue, market
                             + 4 + 4 + 4        // instrument_id, connection_id, instance_id
                             + 8                // sequence_number
                             + 8 + 8 + 8 + 8;  // timestamps

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MessageHeader {
    pub schema_version:   u8,
    pub message_type:     MessageType,
    pub venue_id:         VenueId,
    pub market_type:      MarketType,
    pub instrument_id:    u32,
    pub connection_id:    u32,
    pub instance_id:      u32,
    pub sequence_number:  u64,
    /// Nanoseconds since Unix epoch. TS_NONE when the exchange does not provide it.
    pub exchange_event_ts: i64,
    /// Nanoseconds since Unix epoch. TS_NONE when the exchange does not provide it.
    pub exchange_tx_ts:   i64,
    /// Nanoseconds since Unix epoch: when the raw bytes arrived at the local socket.
    pub local_recv_ts:    i64,
    /// Nanoseconds since Unix epoch: stamped immediately before the Aeron offer.
    pub local_publish_ts: i64,
}

impl MessageHeader {
    /// Encode into the first [`HEADER_SIZE`] bytes of `buf`.
    pub fn encode_into(&self, buf: &mut [u8]) -> Result<(), Error> {
        if buf.len() < HEADER_SIZE {
            return Err(Error::BufferTooShort { needed: HEADER_SIZE, have: buf.len() });
        }
        buf[0] = self.schema_version;
        buf[1] = self.message_type as u8;
        buf[2] = self.venue_id as u8;
        buf[3] = self.market_type as u8;
        buf[4..8].copy_from_slice(&self.instrument_id.to_le_bytes());
        buf[8..12].copy_from_slice(&self.connection_id.to_le_bytes());
        buf[12..16].copy_from_slice(&self.instance_id.to_le_bytes());
        buf[16..24].copy_from_slice(&self.sequence_number.to_le_bytes());
        buf[24..32].copy_from_slice(&self.exchange_event_ts.to_le_bytes());
        buf[32..40].copy_from_slice(&self.exchange_tx_ts.to_le_bytes());
        buf[40..48].copy_from_slice(&self.local_recv_ts.to_le_bytes());
        buf[48..56].copy_from_slice(&self.local_publish_ts.to_le_bytes());
        Ok(())
    }

    /// Decode from the first [`HEADER_SIZE`] bytes of `buf`. Trailing bytes are ignored.
    pub fn decode(buf: &[u8]) -> Result<Self, Error> {
        if buf.len() < HEADER_SIZE {
            return Err(Error::BufferTooShort { needed: HEADER_SIZE, have: buf.len() });
        }
        // Safety: buf.len() >= 56 so all fixed slices below are in-bounds.
        Ok(Self {
            schema_version:   buf[0],
            message_type:     MessageType::try_from(buf[1])?,
            venue_id:         VenueId::try_from(buf[2])?,
            market_type:      MarketType::try_from(buf[3])?,
            instrument_id:    u32::from_le_bytes(buf[4..8].try_into().unwrap()),
            connection_id:    u32::from_le_bytes(buf[8..12].try_into().unwrap()),
            instance_id:      u32::from_le_bytes(buf[12..16].try_into().unwrap()),
            sequence_number:  u64::from_le_bytes(buf[16..24].try_into().unwrap()),
            exchange_event_ts: i64::from_le_bytes(buf[24..32].try_into().unwrap()),
            exchange_tx_ts:   i64::from_le_bytes(buf[32..40].try_into().unwrap()),
            local_recv_ts:    i64::from_le_bytes(buf[40..48].try_into().unwrap()),
            local_publish_ts: i64::from_le_bytes(buf[48..56].try_into().unwrap()),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample() -> MessageHeader {
        MessageHeader {
            schema_version:   SCHEMA_VERSION,
            message_type:     MessageType::BookDelta,
            venue_id:         VenueId::BinanceSpot,
            market_type:      MarketType::Spot,
            instrument_id:    42,
            connection_id:    7,
            instance_id:      1,
            sequence_number:  1_000_000,
            exchange_event_ts: 1_700_000_000_123_456_789,
            exchange_tx_ts:   TS_NONE,
            local_recv_ts:    1_700_000_000_123_500_000,
            local_publish_ts: 1_700_000_000_123_510_000,
        }
    }

    #[test]
    fn round_trip() {
        let hdr = sample();
        let mut buf = [0u8; HEADER_SIZE];
        hdr.encode_into(&mut buf).unwrap();
        let decoded = MessageHeader::decode(&buf).unwrap();
        assert_eq!(hdr, decoded);
    }

    #[test]
    fn round_trip_with_trailing_bytes() {
        let hdr = sample();
        let mut buf = [0u8; HEADER_SIZE + 32];
        hdr.encode_into(&mut buf).unwrap();
        let decoded = MessageHeader::decode(&buf).unwrap();
        assert_eq!(hdr, decoded);
    }

    #[test]
    fn encode_buffer_too_short() {
        let hdr = sample();
        let mut buf = [0u8; HEADER_SIZE - 1];
        let err = hdr.encode_into(&mut buf).unwrap_err();
        assert_eq!(err, Error::BufferTooShort { needed: HEADER_SIZE, have: HEADER_SIZE - 1 });
    }

    #[test]
    fn decode_buffer_too_short() {
        let buf = [0u8; HEADER_SIZE - 1];
        let err = MessageHeader::decode(&buf).unwrap_err();
        assert_eq!(err, Error::BufferTooShort { needed: HEADER_SIZE, have: HEADER_SIZE - 1 });
    }

    #[test]
    fn decode_unknown_venue_id() {
        let hdr = sample();
        let mut buf = [0u8; HEADER_SIZE];
        hdr.encode_into(&mut buf).unwrap();
        buf[2] = 255;
        let err = MessageHeader::decode(&buf).unwrap_err();
        assert_eq!(err, Error::UnknownVenueId(255));
    }

    #[test]
    fn decode_unknown_market_type() {
        let hdr = sample();
        let mut buf = [0u8; HEADER_SIZE];
        hdr.encode_into(&mut buf).unwrap();
        buf[3] = 99;
        let err = MessageHeader::decode(&buf).unwrap_err();
        assert_eq!(err, Error::UnknownMarketType(99));
    }

    #[test]
    fn decode_unknown_message_type() {
        let hdr = sample();
        let mut buf = [0u8; HEADER_SIZE];
        hdr.encode_into(&mut buf).unwrap();
        buf[1] = 200;
        let err = MessageHeader::decode(&buf).unwrap_err();
        assert_eq!(err, Error::UnknownMessageType(200));
    }

    #[test]
    fn exchange_tx_ts_none_survives_round_trip() {
        let mut hdr = sample();
        hdr.exchange_tx_ts = TS_NONE;
        let mut buf = [0u8; HEADER_SIZE];
        hdr.encode_into(&mut buf).unwrap();
        let decoded = MessageHeader::decode(&buf).unwrap();
        assert_eq!(decoded.exchange_tx_ts, TS_NONE);
    }

    #[test]
    fn futures_venue_round_trip() {
        let mut hdr = sample();
        hdr.venue_id    = VenueId::BinanceFutures;
        hdr.market_type = MarketType::UsdmFutures;
        let mut buf = [0u8; HEADER_SIZE];
        hdr.encode_into(&mut buf).unwrap();
        let decoded = MessageHeader::decode(&buf).unwrap();
        assert_eq!(decoded.venue_id,    VenueId::BinanceFutures);
        assert_eq!(decoded.market_type, MarketType::UsdmFutures);
    }

    #[test]
    fn all_message_types_round_trip() {
        use MessageType::*;
        let all = [
            InstrumentDefinition, TradingStatus, BookSnapshot, BookDelta,
            BestBidOffer, Trade, MarkPrice, FundingRate, Liquidation,
            OpenInterest, AccountUpdate, OrderUpdate, Heartbeat,
            FeedStatus, GapDetected, BookStale, BookRecovered, BookChecksum,
        ];
        for mt in all {
            let mut hdr = sample();
            hdr.message_type = mt;
            let mut buf = [0u8; HEADER_SIZE];
            hdr.encode_into(&mut buf).unwrap();
            let decoded = MessageHeader::decode(&buf).unwrap();
            assert_eq!(decoded.message_type, mt, "round-trip failed for {mt:?}");
        }
    }

    #[test]
    fn header_size_is_56() {
        assert_eq!(HEADER_SIZE, 56);
    }

    #[test]
    fn sequence_number_max_round_trip() {
        let mut hdr = sample();
        hdr.sequence_number = u64::MAX;
        let mut buf = [0u8; HEADER_SIZE];
        hdr.encode_into(&mut buf).unwrap();
        assert_eq!(MessageHeader::decode(&buf).unwrap().sequence_number, u64::MAX);
    }
}
