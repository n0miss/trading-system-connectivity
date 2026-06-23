use crate::{
    codec::{Decoder, Encoder},
    header::{MessageHeader, HEADER_SIZE},
    types::{AggressorSide, BookStaleReason, FeedState, MessageType},
    Error,
};

/// Sentinel: update_id field is absent for this message.
pub const UPDATE_ID_NONE: u64 = 0;

// ---------------------------------------------------------------------------
// Shared domain type
// ---------------------------------------------------------------------------

/// A single price level. Prices and quantities are scaled integers — no floats.
/// The scale factors live in the corresponding InstrumentDefinition.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PriceLevel {
    /// Signed to allow removal signals (qty == 0 removes the level).
    pub price: i64,
    pub qty:   i64,
}

fn put_levels(enc: &mut Encoder, levels: &[PriceLevel]) -> Result<(), Error> {
    if levels.len() > u32::MAX as usize {
        return Err(Error::VecTooLong { count: levels.len(), max: u32::MAX as usize });
    }
    enc.put_u32(levels.len() as u32)?;
    for lvl in levels {
        enc.put_i64(lvl.price)?;
        enc.put_i64(lvl.qty)?;
    }
    Ok(())
}

fn get_levels(dec: &mut Decoder) -> Result<Vec<PriceLevel>, Error> {
    let count = dec.get_u32()? as usize;
    let mut levels = Vec::with_capacity(count);
    for _ in 0..count {
        levels.push(PriceLevel { price: dec.get_i64()?, qty: dec.get_i64()? });
    }
    Ok(levels)
}

// ---------------------------------------------------------------------------
// Macro to generate the type-check guard in every decode()
// ---------------------------------------------------------------------------

macro_rules! check_message_type {
    ($header:expr, $expected:expr) => {
        if $header.message_type != $expected {
            return Err(Error::MessageTypeMismatch {
                got:      $header.message_type,
                expected: $expected,
            });
        }
    };
}

// ---------------------------------------------------------------------------
// Message structs
// ---------------------------------------------------------------------------

/// Instrument reference data fetched from REST and published at startup / on change.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InstrumentDefinition {
    pub header:        MessageHeader,
    pub symbol:        String,
    pub base_asset:    String,
    pub quote_asset:   String,
    /// Divisor: actual_price = price_mantissa / 10^price_scale
    pub price_scale:   u32,
    pub qty_scale:     u32,
    pub tick_size:     i64,
    pub step_size:     i64,
    pub min_qty:       i64,
    pub min_notional:  i64,
    /// Contract size in qty_scale units; 0 for spot.
    pub contract_size: i64,
    pub is_trading:    bool,
}

impl InstrumentDefinition {
    pub fn encode_into(&self, buf: &mut [u8]) -> Result<usize, Error> {
        self.header.encode_into(buf)?;
        let mut enc = Encoder::new(&mut buf[HEADER_SIZE..]);
        enc.put_str(&self.symbol)?;
        enc.put_str(&self.base_asset)?;
        enc.put_str(&self.quote_asset)?;
        enc.put_u32(self.price_scale)?;
        enc.put_u32(self.qty_scale)?;
        enc.put_i64(self.tick_size)?;
        enc.put_i64(self.step_size)?;
        enc.put_i64(self.min_qty)?;
        enc.put_i64(self.min_notional)?;
        enc.put_i64(self.contract_size)?;
        enc.put_bool(self.is_trading)?;
        Ok(HEADER_SIZE + enc.finish())
    }

    pub fn decode(buf: &[u8]) -> Result<Self, Error> {
        let header = MessageHeader::decode(buf)?;
        check_message_type!(header, MessageType::InstrumentDefinition);
        let mut dec = Decoder::new(&buf[HEADER_SIZE..]);
        Ok(Self {
            header,
            symbol:        dec.get_str()?,
            base_asset:    dec.get_str()?,
            quote_asset:   dec.get_str()?,
            price_scale:   dec.get_u32()?,
            qty_scale:     dec.get_u32()?,
            tick_size:     dec.get_i64()?,
            step_size:     dec.get_i64()?,
            min_qty:       dec.get_i64()?,
            min_notional:  dec.get_i64()?,
            contract_size: dec.get_i64()?,
            is_trading:    dec.get_bool()?,
        })
    }
}

// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TradingStatus {
    pub header:     MessageHeader,
    pub symbol:     String,
    pub is_trading: bool,
}

impl TradingStatus {
    pub fn encode_into(&self, buf: &mut [u8]) -> Result<usize, Error> {
        self.header.encode_into(buf)?;
        let mut enc = Encoder::new(&mut buf[HEADER_SIZE..]);
        enc.put_str(&self.symbol)?;
        enc.put_bool(self.is_trading)?;
        Ok(HEADER_SIZE + enc.finish())
    }

    pub fn decode(buf: &[u8]) -> Result<Self, Error> {
        let header = MessageHeader::decode(buf)?;
        check_message_type!(header, MessageType::TradingStatus);
        let mut dec = Decoder::new(&buf[HEADER_SIZE..]);
        Ok(Self { header, symbol: dec.get_str()?, is_trading: dec.get_bool()? })
    }
}

// ---------------------------------------------------------------------------

/// Full depth snapshot used during book initialisation and recovery.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BookSnapshot {
    pub header:      MessageHeader,
    pub symbol:      String,
    pub price_scale: u8,
    pub qty_scale:   u8,
    pub update_id:   u64,
    pub bids:        Vec<PriceLevel>,
    pub asks:        Vec<PriceLevel>,
}

impl BookSnapshot {
    pub fn encode_into(&self, buf: &mut [u8]) -> Result<usize, Error> {
        self.header.encode_into(buf)?;
        let mut enc = Encoder::new(&mut buf[HEADER_SIZE..]);
        enc.put_str(&self.symbol)?;
        enc.put_u8(self.price_scale)?;
        enc.put_u8(self.qty_scale)?;
        enc.put_u64(self.update_id)?;
        put_levels(&mut enc, &self.bids)?;
        put_levels(&mut enc, &self.asks)?;
        Ok(HEADER_SIZE + enc.finish())
    }

    pub fn decode(buf: &[u8]) -> Result<Self, Error> {
        let header = MessageHeader::decode(buf)?;
        check_message_type!(header, MessageType::BookSnapshot);
        let mut dec = Decoder::new(&buf[HEADER_SIZE..]);
        Ok(Self {
            header,
            symbol:      dec.get_str()?,
            price_scale: dec.get_u8()?,
            qty_scale:   dec.get_u8()?,
            update_id:   dec.get_u64()?,
            bids:        get_levels(&mut dec)?,
            asks:        get_levels(&mut dec)?,
        })
    }
}

// ---------------------------------------------------------------------------

/// Incremental L2 order book update.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BookDelta {
    pub header:           MessageHeader,
    pub symbol:           String,
    pub price_scale:      u8,
    pub qty_scale:        u8,
    pub first_update_id:  u64,
    pub final_update_id:  u64,
    /// Previous final update id for sequence continuity; UPDATE_ID_NONE if unavailable.
    pub prev_update_id:   u64,
    pub bids:             Vec<PriceLevel>,
    pub asks:             Vec<PriceLevel>,
}

impl BookDelta {
    pub fn encode_into(&self, buf: &mut [u8]) -> Result<usize, Error> {
        self.header.encode_into(buf)?;
        let mut enc = Encoder::new(&mut buf[HEADER_SIZE..]);
        enc.put_str(&self.symbol)?;
        enc.put_u8(self.price_scale)?;
        enc.put_u8(self.qty_scale)?;
        enc.put_u64(self.first_update_id)?;
        enc.put_u64(self.final_update_id)?;
        enc.put_u64(self.prev_update_id)?;
        put_levels(&mut enc, &self.bids)?;
        put_levels(&mut enc, &self.asks)?;
        Ok(HEADER_SIZE + enc.finish())
    }

    pub fn decode(buf: &[u8]) -> Result<Self, Error> {
        let header = MessageHeader::decode(buf)?;
        check_message_type!(header, MessageType::BookDelta);
        let mut dec = Decoder::new(&buf[HEADER_SIZE..]);
        Ok(Self {
            header,
            symbol:           dec.get_str()?,
            price_scale:      dec.get_u8()?,
            qty_scale:        dec.get_u8()?,
            first_update_id:  dec.get_u64()?,
            final_update_id:  dec.get_u64()?,
            prev_update_id:   dec.get_u64()?,
            bids:             get_levels(&mut dec)?,
            asks:             get_levels(&mut dec)?,
        })
    }
}

// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BestBidOffer {
    pub header:      MessageHeader,
    pub symbol:      String,
    pub price_scale: u8,
    pub qty_scale:   u8,
    pub bid_price:   i64,
    pub bid_qty:     i64,
    pub ask_price:   i64,
    pub ask_qty:     i64,
    /// Exchange update id; UPDATE_ID_NONE if the exchange does not provide one.
    pub update_id:   u64,
}

impl BestBidOffer {
    pub fn encode_into(&self, buf: &mut [u8]) -> Result<usize, Error> {
        self.header.encode_into(buf)?;
        let mut enc = Encoder::new(&mut buf[HEADER_SIZE..]);
        enc.put_str(&self.symbol)?;
        enc.put_u8(self.price_scale)?;
        enc.put_u8(self.qty_scale)?;
        enc.put_i64(self.bid_price)?;
        enc.put_i64(self.bid_qty)?;
        enc.put_i64(self.ask_price)?;
        enc.put_i64(self.ask_qty)?;
        enc.put_u64(self.update_id)?;
        Ok(HEADER_SIZE + enc.finish())
    }

    pub fn decode(buf: &[u8]) -> Result<Self, Error> {
        let header = MessageHeader::decode(buf)?;
        check_message_type!(header, MessageType::BestBidOffer);
        let mut dec = Decoder::new(&buf[HEADER_SIZE..]);
        Ok(Self {
            header,
            symbol:      dec.get_str()?,
            price_scale: dec.get_u8()?,
            qty_scale:   dec.get_u8()?,
            bid_price:   dec.get_i64()?,
            bid_qty:     dec.get_i64()?,
            ask_price:   dec.get_i64()?,
            ask_qty:     dec.get_i64()?,
            update_id:   dec.get_u64()?,
        })
    }
}

// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Trade {
    pub header:         MessageHeader,
    pub symbol:         String,
    pub price_scale:    u8,
    pub qty_scale:      u8,
    pub trade_id:       u64,
    pub price:          i64,
    pub qty:            i64,
    pub trade_ts:       i64,
    pub is_buyer_maker: bool,
    pub aggressor_side: AggressorSide,
}

impl Trade {
    pub fn encode_into(&self, buf: &mut [u8]) -> Result<usize, Error> {
        self.header.encode_into(buf)?;
        let mut enc = Encoder::new(&mut buf[HEADER_SIZE..]);
        enc.put_str(&self.symbol)?;
        enc.put_u8(self.price_scale)?;
        enc.put_u8(self.qty_scale)?;
        enc.put_u64(self.trade_id)?;
        enc.put_i64(self.price)?;
        enc.put_i64(self.qty)?;
        enc.put_i64(self.trade_ts)?;
        enc.put_bool(self.is_buyer_maker)?;
        enc.put_u8(self.aggressor_side as u8)?;
        Ok(HEADER_SIZE + enc.finish())
    }

    pub fn decode(buf: &[u8]) -> Result<Self, Error> {
        let header = MessageHeader::decode(buf)?;
        check_message_type!(header, MessageType::Trade);
        let mut dec = Decoder::new(&buf[HEADER_SIZE..]);
        Ok(Self {
            header,
            symbol:         dec.get_str()?,
            price_scale:    dec.get_u8()?,
            qty_scale:      dec.get_u8()?,
            trade_id:       dec.get_u64()?,
            price:          dec.get_i64()?,
            qty:            dec.get_i64()?,
            trade_ts:       dec.get_i64()?,
            is_buyer_maker: dec.get_bool()?,
            aggressor_side: AggressorSide::try_from(dec.get_u8()?)?,
        })
    }
}

// ---------------------------------------------------------------------------

/// Mark price and index price for a futures symbol.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MarkPrice {
    pub header:      MessageHeader,
    pub symbol:      String,
    pub price_scale: u8,
    pub mark_price:  i64,
    pub index_price: i64,
}

impl MarkPrice {
    pub fn encode_into(&self, buf: &mut [u8]) -> Result<usize, Error> {
        self.header.encode_into(buf)?;
        let mut enc = Encoder::new(&mut buf[HEADER_SIZE..]);
        enc.put_str(&self.symbol)?;
        enc.put_u8(self.price_scale)?;
        enc.put_i64(self.mark_price)?;
        enc.put_i64(self.index_price)?;
        Ok(HEADER_SIZE + enc.finish())
    }

    pub fn decode(buf: &[u8]) -> Result<Self, Error> {
        let header = MessageHeader::decode(buf)?;
        check_message_type!(header, MessageType::MarkPrice);
        let mut dec = Decoder::new(&buf[HEADER_SIZE..]);
        Ok(Self {
            header,
            symbol:      dec.get_str()?,
            price_scale: dec.get_u8()?,
            mark_price:  dec.get_i64()?,
            index_price: dec.get_i64()?,
        })
    }
}

// ---------------------------------------------------------------------------

/// Funding rate for a futures symbol.
/// `funding_rate` is scaled as `rate * 10^funding_rate_scale`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FundingRate {
    pub header:              MessageHeader,
    pub symbol:              String,
    pub funding_rate_scale:  u8,
    pub funding_rate:        i64,
    pub next_funding_time:   i64,
}

impl FundingRate {
    pub fn encode_into(&self, buf: &mut [u8]) -> Result<usize, Error> {
        self.header.encode_into(buf)?;
        let mut enc = Encoder::new(&mut buf[HEADER_SIZE..]);
        enc.put_str(&self.symbol)?;
        enc.put_u8(self.funding_rate_scale)?;
        enc.put_i64(self.funding_rate)?;
        enc.put_i64(self.next_funding_time)?;
        Ok(HEADER_SIZE + enc.finish())
    }

    pub fn decode(buf: &[u8]) -> Result<Self, Error> {
        let header = MessageHeader::decode(buf)?;
        check_message_type!(header, MessageType::FundingRate);
        let mut dec = Decoder::new(&buf[HEADER_SIZE..]);
        Ok(Self {
            header,
            symbol:             dec.get_str()?,
            funding_rate_scale: dec.get_u8()?,
            funding_rate:       dec.get_i64()?,
            next_funding_time:  dec.get_i64()?,
        })
    }
}

// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Liquidation {
    pub header:          MessageHeader,
    pub symbol:          String,
    pub price_scale:     u8,
    pub qty_scale:       u8,
    pub side:            AggressorSide,
    pub price:           i64,
    pub qty:             i64,
    pub avg_price:       i64,
    pub last_filled_qty: i64,
}

impl Liquidation {
    pub fn encode_into(&self, buf: &mut [u8]) -> Result<usize, Error> {
        self.header.encode_into(buf)?;
        let mut enc = Encoder::new(&mut buf[HEADER_SIZE..]);
        enc.put_str(&self.symbol)?;
        enc.put_u8(self.price_scale)?;
        enc.put_u8(self.qty_scale)?;
        enc.put_u8(self.side as u8)?;
        enc.put_i64(self.price)?;
        enc.put_i64(self.qty)?;
        enc.put_i64(self.avg_price)?;
        enc.put_i64(self.last_filled_qty)?;
        Ok(HEADER_SIZE + enc.finish())
    }

    pub fn decode(buf: &[u8]) -> Result<Self, Error> {
        let header = MessageHeader::decode(buf)?;
        check_message_type!(header, MessageType::Liquidation);
        let mut dec = Decoder::new(&buf[HEADER_SIZE..]);
        Ok(Self {
            header,
            symbol:          dec.get_str()?,
            price_scale:     dec.get_u8()?,
            qty_scale:       dec.get_u8()?,
            side:            AggressorSide::try_from(dec.get_u8()?)?,
            price:           dec.get_i64()?,
            qty:             dec.get_i64()?,
            avg_price:       dec.get_i64()?,
            last_filled_qty: dec.get_i64()?,
        })
    }
}

// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OpenInterest {
    pub header:        MessageHeader,
    pub symbol:        String,
    pub qty_scale:     u8,
    pub open_interest: i64,
}

impl OpenInterest {
    pub fn encode_into(&self, buf: &mut [u8]) -> Result<usize, Error> {
        self.header.encode_into(buf)?;
        let mut enc = Encoder::new(&mut buf[HEADER_SIZE..]);
        enc.put_str(&self.symbol)?;
        enc.put_u8(self.qty_scale)?;
        enc.put_i64(self.open_interest)?;
        Ok(HEADER_SIZE + enc.finish())
    }

    pub fn decode(buf: &[u8]) -> Result<Self, Error> {
        let header = MessageHeader::decode(buf)?;
        check_message_type!(header, MessageType::OpenInterest);
        let mut dec = Decoder::new(&buf[HEADER_SIZE..]);
        Ok(Self {
            header,
            symbol:        dec.get_str()?,
            qty_scale:     dec.get_u8()?,
            open_interest: dec.get_i64()?,
        })
    }
}

// ---------------------------------------------------------------------------

/// Keepalive signal. Body is empty — only the header is encoded.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Heartbeat {
    pub header: MessageHeader,
}

impl Heartbeat {
    pub fn encode_into(&self, buf: &mut [u8]) -> Result<usize, Error> {
        self.header.encode_into(buf)?;
        Ok(HEADER_SIZE)
    }

    pub fn decode(buf: &[u8]) -> Result<Self, Error> {
        let header = MessageHeader::decode(buf)?;
        check_message_type!(header, MessageType::Heartbeat);
        Ok(Self { header })
    }
}

// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FeedStatus {
    pub header: MessageHeader,
    pub state:  FeedState,
}

impl FeedStatus {
    pub fn encode_into(&self, buf: &mut [u8]) -> Result<usize, Error> {
        self.header.encode_into(buf)?;
        let mut enc = Encoder::new(&mut buf[HEADER_SIZE..]);
        enc.put_u8(self.state as u8)?;
        Ok(HEADER_SIZE + enc.finish())
    }

    pub fn decode(buf: &[u8]) -> Result<Self, Error> {
        let header = MessageHeader::decode(buf)?;
        check_message_type!(header, MessageType::FeedStatus);
        let mut dec = Decoder::new(&buf[HEADER_SIZE..]);
        Ok(Self { header, state: FeedState::try_from(dec.get_u8()?)? })
    }
}

// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GapDetected {
    pub header:               MessageHeader,
    pub symbol:               String,
    pub expected_update_id:   u64,
    pub received_update_id:   u64,
}

impl GapDetected {
    pub fn encode_into(&self, buf: &mut [u8]) -> Result<usize, Error> {
        self.header.encode_into(buf)?;
        let mut enc = Encoder::new(&mut buf[HEADER_SIZE..]);
        enc.put_str(&self.symbol)?;
        enc.put_u64(self.expected_update_id)?;
        enc.put_u64(self.received_update_id)?;
        Ok(HEADER_SIZE + enc.finish())
    }

    pub fn decode(buf: &[u8]) -> Result<Self, Error> {
        let header = MessageHeader::decode(buf)?;
        check_message_type!(header, MessageType::GapDetected);
        let mut dec = Decoder::new(&buf[HEADER_SIZE..]);
        Ok(Self {
            header,
            symbol:             dec.get_str()?,
            expected_update_id: dec.get_u64()?,
            received_update_id: dec.get_u64()?,
        })
    }
}

// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BookStale {
    pub header: MessageHeader,
    pub symbol: String,
    pub reason: BookStaleReason,
}

impl BookStale {
    pub fn encode_into(&self, buf: &mut [u8]) -> Result<usize, Error> {
        self.header.encode_into(buf)?;
        let mut enc = Encoder::new(&mut buf[HEADER_SIZE..]);
        enc.put_str(&self.symbol)?;
        enc.put_u8(self.reason as u8)?;
        Ok(HEADER_SIZE + enc.finish())
    }

    pub fn decode(buf: &[u8]) -> Result<Self, Error> {
        let header = MessageHeader::decode(buf)?;
        check_message_type!(header, MessageType::BookStale);
        let mut dec = Decoder::new(&buf[HEADER_SIZE..]);
        Ok(Self {
            header,
            symbol: dec.get_str()?,
            reason: BookStaleReason::try_from(dec.get_u8()?)?,
        })
    }
}

// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BookRecovered {
    pub header:              MessageHeader,
    pub symbol:              String,
    pub snapshot_update_id:  u64,
}

impl BookRecovered {
    pub fn encode_into(&self, buf: &mut [u8]) -> Result<usize, Error> {
        self.header.encode_into(buf)?;
        let mut enc = Encoder::new(&mut buf[HEADER_SIZE..]);
        enc.put_str(&self.symbol)?;
        enc.put_u64(self.snapshot_update_id)?;
        Ok(HEADER_SIZE + enc.finish())
    }

    pub fn decode(buf: &[u8]) -> Result<Self, Error> {
        let header = MessageHeader::decode(buf)?;
        check_message_type!(header, MessageType::BookRecovered);
        let mut dec = Decoder::new(&buf[HEADER_SIZE..]);
        Ok(Self { header, symbol: dec.get_str()?, snapshot_update_id: dec.get_u64()? })
    }
}

// ---------------------------------------------------------------------------
// BookChecksum — redundancy / status stream (§9.34)
// ---------------------------------------------------------------------------

/// Deterministic book-state fingerprint published by passive instances.
///
/// After each [`BookDelta`] or [`BookSnapshot`] is applied, the passive
/// instance computes an FNV-1a checksum over the full book state and sends
/// this message to the **status stream**.  The cross-instance comparator
/// (§9.35) reads both the active and passive status streams and triggers
/// failover when the checksums diverge for the same `update_id`.
///
/// # Wire layout (after the 56-byte `MessageHeader`)
///
/// | Field       | Type | Bytes | Notes                         |
/// |-------------|------|-------|-------------------------------|
/// | `symbol`    | str  | 2+n   | u16 length-prefix, UTF-8      |
/// | `update_id` | u64  | 8     | `last_update_id` at checksum time |
/// | `bid_depth` | u32  | 4     | number of bid price levels    |
/// | `ask_depth` | u32  | 4     | number of ask price levels    |
/// | `checksum`  | u64  | 8     | FNV-1a 64-bit hash            |
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BookChecksum {
    pub header:    MessageHeader,
    /// Symbol this checksum covers (e.g. `"BTCUSDT"`).
    pub symbol:    String,
    /// The `last_update_id` of the book at checksum time.
    pub update_id: u64,
    /// Number of bid price levels in the book at checksum time.
    pub bid_depth: u32,
    /// Number of ask price levels in the book at checksum time.
    pub ask_depth: u32,
    /// FNV-1a 64-bit hash over `update_id || bids desc || asks asc`.
    pub checksum:  u64,
}

impl BookChecksum {
    pub fn encode_into(&self, buf: &mut [u8]) -> Result<usize, Error> {
        self.header.encode_into(buf)?;
        let mut enc = Encoder::new(&mut buf[HEADER_SIZE..]);
        enc.put_str(&self.symbol)?;
        enc.put_u64(self.update_id)?;
        enc.put_u32(self.bid_depth)?;
        enc.put_u32(self.ask_depth)?;
        enc.put_u64(self.checksum)?;
        Ok(HEADER_SIZE + enc.finish())
    }

    pub fn decode(buf: &[u8]) -> Result<Self, Error> {
        let header = MessageHeader::decode(buf)?;
        check_message_type!(header, MessageType::BookChecksum);
        let mut dec = Decoder::new(&buf[HEADER_SIZE..]);
        Ok(Self {
            header,
            symbol:    dec.get_str()?,
            update_id: dec.get_u64()?,
            bid_depth: dec.get_u32()?,
            ask_depth: dec.get_u32()?,
            checksum:  dec.get_u64()?,
        })
    }
}

// ---------------------------------------------------------------------------
// Phase 1.5 stubs — body intentionally empty until the execution layer is built
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AccountUpdate {
    pub header: MessageHeader,
}

impl AccountUpdate {
    pub fn encode_into(&self, buf: &mut [u8]) -> Result<usize, Error> {
        self.header.encode_into(buf)?;
        Ok(HEADER_SIZE)
    }

    pub fn decode(buf: &[u8]) -> Result<Self, Error> {
        let header = MessageHeader::decode(buf)?;
        check_message_type!(header, MessageType::AccountUpdate);
        Ok(Self { header })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OrderUpdate {
    pub header: MessageHeader,
}

impl OrderUpdate {
    pub fn encode_into(&self, buf: &mut [u8]) -> Result<usize, Error> {
        self.header.encode_into(buf)?;
        Ok(HEADER_SIZE)
    }

    pub fn decode(buf: &[u8]) -> Result<Self, Error> {
        let header = MessageHeader::decode(buf)?;
        check_message_type!(header, MessageType::OrderUpdate);
        Ok(Self { header })
    }
}

// ---------------------------------------------------------------------------
// Sum type
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NormalizedMessage {
    InstrumentDefinition(InstrumentDefinition),
    TradingStatus(TradingStatus),
    BookSnapshot(BookSnapshot),
    BookDelta(BookDelta),
    BestBidOffer(BestBidOffer),
    Trade(Trade),
    MarkPrice(MarkPrice),
    FundingRate(FundingRate),
    Liquidation(Liquidation),
    OpenInterest(OpenInterest),
    AccountUpdate(AccountUpdate),
    OrderUpdate(OrderUpdate),
    Heartbeat(Heartbeat),
    FeedStatus(FeedStatus),
    GapDetected(GapDetected),
    BookStale(BookStale),
    BookRecovered(BookRecovered),
}

impl NormalizedMessage {
    pub fn from_bytes(buf: &[u8]) -> Result<Self, Error> {
        let header = MessageHeader::decode(buf)?;
        match header.message_type {
            MessageType::InstrumentDefinition => Ok(Self::InstrumentDefinition(InstrumentDefinition::decode(buf)?)),
            MessageType::TradingStatus        => Ok(Self::TradingStatus(TradingStatus::decode(buf)?)),
            MessageType::BookSnapshot         => Ok(Self::BookSnapshot(BookSnapshot::decode(buf)?)),
            MessageType::BookDelta            => Ok(Self::BookDelta(BookDelta::decode(buf)?)),
            MessageType::BestBidOffer         => Ok(Self::BestBidOffer(BestBidOffer::decode(buf)?)),
            MessageType::Trade                => Ok(Self::Trade(Trade::decode(buf)?)),
            MessageType::MarkPrice            => Ok(Self::MarkPrice(MarkPrice::decode(buf)?)),
            MessageType::FundingRate          => Ok(Self::FundingRate(FundingRate::decode(buf)?)),
            MessageType::Liquidation          => Ok(Self::Liquidation(Liquidation::decode(buf)?)),
            MessageType::OpenInterest         => Ok(Self::OpenInterest(OpenInterest::decode(buf)?)),
            MessageType::AccountUpdate        => Ok(Self::AccountUpdate(AccountUpdate::decode(buf)?)),
            MessageType::OrderUpdate          => Ok(Self::OrderUpdate(OrderUpdate::decode(buf)?)),
            MessageType::Heartbeat            => Ok(Self::Heartbeat(Heartbeat::decode(buf)?)),
            MessageType::FeedStatus           => Ok(Self::FeedStatus(FeedStatus::decode(buf)?)),
            MessageType::GapDetected          => Ok(Self::GapDetected(GapDetected::decode(buf)?)),
            MessageType::BookStale            => Ok(Self::BookStale(BookStale::decode(buf)?)),
            MessageType::BookRecovered        => Ok(Self::BookRecovered(BookRecovered::decode(buf)?)),
            // BookChecksum is a status-stream message, not a market-data message.
            // Route it through BookChecksum::decode() directly, not NormalizedMessage.
            MessageType::BookChecksum => Err(Error::UnknownMessageType(MessageType::BookChecksum as u8)),
        }
    }

    pub fn encode_into(&self, buf: &mut [u8]) -> Result<usize, Error> {
        match self {
            Self::InstrumentDefinition(m) => m.encode_into(buf),
            Self::TradingStatus(m)        => m.encode_into(buf),
            Self::BookSnapshot(m)         => m.encode_into(buf),
            Self::BookDelta(m)            => m.encode_into(buf),
            Self::BestBidOffer(m)         => m.encode_into(buf),
            Self::Trade(m)                => m.encode_into(buf),
            Self::MarkPrice(m)            => m.encode_into(buf),
            Self::FundingRate(m)          => m.encode_into(buf),
            Self::Liquidation(m)          => m.encode_into(buf),
            Self::OpenInterest(m)         => m.encode_into(buf),
            Self::AccountUpdate(m)        => m.encode_into(buf),
            Self::OrderUpdate(m)          => m.encode_into(buf),
            Self::Heartbeat(m)            => m.encode_into(buf),
            Self::FeedStatus(m)           => m.encode_into(buf),
            Self::GapDetected(m)          => m.encode_into(buf),
            Self::BookStale(m)            => m.encode_into(buf),
            Self::BookRecovered(m)        => m.encode_into(buf),
        }
    }

    pub fn header(&self) -> &MessageHeader {
        match self {
            Self::InstrumentDefinition(m) => &m.header,
            Self::TradingStatus(m)        => &m.header,
            Self::BookSnapshot(m)         => &m.header,
            Self::BookDelta(m)            => &m.header,
            Self::BestBidOffer(m)         => &m.header,
            Self::Trade(m)                => &m.header,
            Self::MarkPrice(m)            => &m.header,
            Self::FundingRate(m)          => &m.header,
            Self::Liquidation(m)          => &m.header,
            Self::OpenInterest(m)         => &m.header,
            Self::AccountUpdate(m)        => &m.header,
            Self::OrderUpdate(m)          => &m.header,
            Self::Heartbeat(m)            => &m.header,
            Self::FeedStatus(m)           => &m.header,
            Self::GapDetected(m)          => &m.header,
            Self::BookStale(m)            => &m.header,
            Self::BookRecovered(m)        => &m.header,
        }
    }
}
