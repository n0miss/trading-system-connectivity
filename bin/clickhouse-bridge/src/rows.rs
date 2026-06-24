use clickhouse::Row;
use connector_core::{
    BestBidOffer, BookDelta, FundingRate, InstrumentDefinition, Liquidation, MarkPrice,
    OpenInterest, Trade,
};
use serde::Serialize;

// ---------------------------------------------------------------------------
// Scale conversion
//
// The wire protocol stores all prices and quantities as scaled integers:
//   actual_value = mantissa / 10^scale
//
// ClickHouse Decimal(18, 8) is stored on the wire as a raw Int64 with an
// implicit scale of 8.  To write the correct value we rescale the mantissa:
//   decimal8_int = mantissa * 10^(8 - wire_scale)
//
// Example: ETHUSDT price 1654.77, wire scale=2 → mantissa=165477
//   to_d8(165477, 2) = 165477 * 10^6 = 165477_000000
//   ClickHouse reads 165477000000 as Decimal(18,8) → 1654.77000000 ✓
//
// Funding rate uses a hardcoded wire scale of 9.  We use Decimal(18, 9) for
// that column so the mantissa maps directly with no conversion.
// ---------------------------------------------------------------------------

fn to_d8(mantissa: i64, scale: u8) -> i64 {
    match 8i32 - scale as i32 {
        0 => mantissa,
        n if n > 0 => mantissa * 10i64.pow(n as u32),
        n => mantissa / 10i64.pow((-n) as u32),
    }
}

// ---------------------------------------------------------------------------
// Trades
// ---------------------------------------------------------------------------

#[derive(Row, Serialize)]
pub struct TradeRow {
    pub exchange_event_ts: i64,
    pub local_recv_ts: i64,
    pub local_publish_ts: i64,
    pub venue_id: u8,
    pub market_type: u8,
    pub instrument_id: u32,
    pub symbol: String,
    pub sequence_number: u64,
    pub trade_id: u64,
    pub price: i64, // Decimal(18, 8)
    pub qty: i64,   // Decimal(18, 8)
    pub trade_ts: i64,
    pub is_buyer_maker: u8,
    pub aggressor_side: u8,
}

impl From<&Trade> for TradeRow {
    fn from(m: &Trade) -> Self {
        Self {
            exchange_event_ts: m.header.exchange_event_ts,
            local_recv_ts: m.header.local_recv_ts,
            local_publish_ts: m.header.local_publish_ts,
            venue_id: m.header.venue_id as u8,
            market_type: m.header.market_type as u8,
            instrument_id: m.header.instrument_id,
            symbol: m.symbol.clone(),
            sequence_number: m.header.sequence_number,
            trade_id: m.trade_id,
            price: to_d8(m.price, m.price_scale),
            qty: to_d8(m.qty, m.qty_scale),
            trade_ts: m.trade_ts,
            is_buyer_maker: m.is_buyer_maker as u8,
            aggressor_side: m.aggressor_side as u8,
        }
    }
}

// ---------------------------------------------------------------------------
// Best bid/offer
// ---------------------------------------------------------------------------

#[derive(Row, Serialize)]
pub struct BboRow {
    pub exchange_event_ts: i64,
    pub local_recv_ts: i64,
    pub local_publish_ts: i64,
    pub venue_id: u8,
    pub market_type: u8,
    pub instrument_id: u32,
    pub symbol: String,
    pub sequence_number: u64,
    pub bid_price: i64, // Decimal(18, 8)
    pub bid_qty: i64,   // Decimal(18, 8)
    pub ask_price: i64, // Decimal(18, 8)
    pub ask_qty: i64,   // Decimal(18, 8)
    pub update_id: u64,
}

impl From<&BestBidOffer> for BboRow {
    fn from(m: &BestBidOffer) -> Self {
        Self {
            exchange_event_ts: m.header.exchange_event_ts,
            local_recv_ts: m.header.local_recv_ts,
            local_publish_ts: m.header.local_publish_ts,
            venue_id: m.header.venue_id as u8,
            market_type: m.header.market_type as u8,
            instrument_id: m.header.instrument_id,
            symbol: m.symbol.clone(),
            sequence_number: m.header.sequence_number,
            bid_price: to_d8(m.bid_price, m.price_scale),
            bid_qty: to_d8(m.bid_qty, m.qty_scale),
            ask_price: to_d8(m.ask_price, m.price_scale),
            ask_qty: to_d8(m.ask_qty, m.qty_scale),
            update_id: m.update_id,
        }
    }
}

// ---------------------------------------------------------------------------
// Mark price
// ---------------------------------------------------------------------------

#[derive(Row, Serialize)]
pub struct MarkPriceRow {
    pub exchange_event_ts: i64,
    pub local_recv_ts: i64,
    pub local_publish_ts: i64,
    pub venue_id: u8,
    pub market_type: u8,
    pub instrument_id: u32,
    pub symbol: String,
    pub sequence_number: u64,
    pub mark_price: i64,  // Decimal(18, 8)
    pub index_price: i64, // Decimal(18, 8)
}

impl From<&MarkPrice> for MarkPriceRow {
    fn from(m: &MarkPrice) -> Self {
        Self {
            exchange_event_ts: m.header.exchange_event_ts,
            local_recv_ts: m.header.local_recv_ts,
            local_publish_ts: m.header.local_publish_ts,
            venue_id: m.header.venue_id as u8,
            market_type: m.header.market_type as u8,
            instrument_id: m.header.instrument_id,
            symbol: m.symbol.clone(),
            sequence_number: m.header.sequence_number,
            mark_price: to_d8(m.mark_price, m.price_scale),
            index_price: to_d8(m.index_price, m.price_scale),
        }
    }
}

// ---------------------------------------------------------------------------
// Funding rate
//
// The normalizer hardcodes scale=9 for funding_rate (Binance returns 8
// significant decimal digits e.g. "0.00010000").  We use Decimal(18, 9) in
// ClickHouse so the mantissa maps directly with no conversion.
// ---------------------------------------------------------------------------

#[derive(Row, Serialize)]
pub struct FundingRateRow {
    pub exchange_event_ts: i64,
    pub local_recv_ts: i64,
    pub local_publish_ts: i64,
    pub venue_id: u8,
    pub market_type: u8,
    pub instrument_id: u32,
    pub symbol: String,
    pub sequence_number: u64,
    pub funding_rate: i64, // Decimal(18, 9) — wire scale=9, no conversion
    pub next_funding_time: i64,
}

impl From<&FundingRate> for FundingRateRow {
    fn from(m: &FundingRate) -> Self {
        Self {
            exchange_event_ts: m.header.exchange_event_ts,
            local_recv_ts: m.header.local_recv_ts,
            local_publish_ts: m.header.local_publish_ts,
            venue_id: m.header.venue_id as u8,
            market_type: m.header.market_type as u8,
            instrument_id: m.header.instrument_id,
            symbol: m.symbol.clone(),
            sequence_number: m.header.sequence_number,
            funding_rate: m.funding_rate, // already at scale=9
            next_funding_time: m.next_funding_time,
        }
    }
}

// ---------------------------------------------------------------------------
// Liquidation
// ---------------------------------------------------------------------------

#[derive(Row, Serialize)]
pub struct LiquidationRow {
    pub exchange_event_ts: i64,
    pub local_recv_ts: i64,
    pub local_publish_ts: i64,
    pub venue_id: u8,
    pub market_type: u8,
    pub instrument_id: u32,
    pub symbol: String,
    pub sequence_number: u64,
    pub side: u8,
    pub price: i64,           // Decimal(18, 8)
    pub qty: i64,             // Decimal(18, 8)
    pub avg_price: i64,       // Decimal(18, 8)
    pub last_filled_qty: i64, // Decimal(18, 8)
}

impl From<&Liquidation> for LiquidationRow {
    fn from(m: &Liquidation) -> Self {
        Self {
            exchange_event_ts: m.header.exchange_event_ts,
            local_recv_ts: m.header.local_recv_ts,
            local_publish_ts: m.header.local_publish_ts,
            venue_id: m.header.venue_id as u8,
            market_type: m.header.market_type as u8,
            instrument_id: m.header.instrument_id,
            symbol: m.symbol.clone(),
            sequence_number: m.header.sequence_number,
            side: m.side as u8,
            price: to_d8(m.price, m.price_scale),
            qty: to_d8(m.qty, m.qty_scale),
            avg_price: to_d8(m.avg_price, m.price_scale),
            last_filled_qty: to_d8(m.last_filled_qty, m.qty_scale),
        }
    }
}

// ---------------------------------------------------------------------------
// Open interest
// ---------------------------------------------------------------------------

#[derive(Row, Serialize)]
pub struct OpenInterestRow {
    pub exchange_event_ts: i64,
    pub local_recv_ts: i64,
    pub local_publish_ts: i64,
    pub venue_id: u8,
    pub market_type: u8,
    pub instrument_id: u32,
    pub symbol: String,
    pub sequence_number: u64,
    pub open_interest: i64, // Decimal(18, 8)
}

impl From<&OpenInterest> for OpenInterestRow {
    fn from(m: &OpenInterest) -> Self {
        Self {
            exchange_event_ts: m.header.exchange_event_ts,
            local_recv_ts: m.header.local_recv_ts,
            local_publish_ts: m.header.local_publish_ts,
            venue_id: m.header.venue_id as u8,
            market_type: m.header.market_type as u8,
            instrument_id: m.header.instrument_id,
            symbol: m.symbol.clone(),
            sequence_number: m.header.sequence_number,
            open_interest: to_d8(m.open_interest, m.qty_scale),
        }
    }
}

// ---------------------------------------------------------------------------
// Book delta
//
// Each delta is one row with Array columns for the price levels.
// ---------------------------------------------------------------------------

#[derive(Row, Serialize)]
pub struct BookDeltaRow {
    pub exchange_event_ts: i64,
    pub local_recv_ts: i64,
    pub local_publish_ts: i64,
    pub venue_id: u8,
    pub market_type: u8,
    pub instrument_id: u32,
    pub symbol: String,
    pub sequence_number: u64,
    pub first_update_id: u64,
    pub final_update_id: u64,
    pub prev_update_id: u64,
    pub bid_prices: Vec<i64>, // Array(Decimal(18, 8))
    pub bid_qtys: Vec<i64>,   // Array(Decimal(18, 8))
    pub ask_prices: Vec<i64>, // Array(Decimal(18, 8))
    pub ask_qtys: Vec<i64>,   // Array(Decimal(18, 8))
}

impl From<&BookDelta> for BookDeltaRow {
    fn from(m: &BookDelta) -> Self {
        let p = m.price_scale;
        let q = m.qty_scale;
        Self {
            exchange_event_ts: m.header.exchange_event_ts,
            local_recv_ts: m.header.local_recv_ts,
            local_publish_ts: m.header.local_publish_ts,
            venue_id: m.header.venue_id as u8,
            market_type: m.header.market_type as u8,
            instrument_id: m.header.instrument_id,
            symbol: m.symbol.clone(),
            sequence_number: m.header.sequence_number,
            first_update_id: m.first_update_id,
            final_update_id: m.final_update_id,
            prev_update_id: m.prev_update_id,
            bid_prices: m.bids.iter().map(|l| to_d8(l.price, p)).collect(),
            bid_qtys: m.bids.iter().map(|l| to_d8(l.qty, q)).collect(),
            ask_prices: m.asks.iter().map(|l| to_d8(l.price, p)).collect(),
            ask_qtys: m.asks.iter().map(|l| to_d8(l.qty, q)).collect(),
        }
    }
}

// ---------------------------------------------------------------------------
// Instrument reference data
// ---------------------------------------------------------------------------

#[derive(Row, Serialize)]
pub struct InstrumentRow {
    pub local_recv_ts: i64,
    pub venue_id: u8,
    pub market_type: u8,
    pub instrument_id: u32,
    pub symbol: String,
    pub base_asset: String,
    pub quote_asset: String,
    pub price_scale: u32,
    pub qty_scale: u32,
    pub tick_size: i64,     // Decimal(18, 8)
    pub step_size: i64,     // Decimal(18, 8)
    pub min_qty: i64,       // Decimal(18, 8)
    pub min_notional: i64,  // Decimal(18, 8)
    pub contract_size: i64, // Decimal(18, 8)
    pub is_trading: u8,
}

impl From<&InstrumentDefinition> for InstrumentRow {
    fn from(m: &InstrumentDefinition) -> Self {
        let ps = m.price_scale as u8;
        let qs = m.qty_scale as u8;
        Self {
            local_recv_ts: m.header.local_recv_ts,
            venue_id: m.header.venue_id as u8,
            market_type: m.header.market_type as u8,
            instrument_id: m.header.instrument_id,
            symbol: m.symbol.clone(),
            base_asset: m.base_asset.clone(),
            quote_asset: m.quote_asset.clone(),
            price_scale: m.price_scale,
            qty_scale: m.qty_scale,
            tick_size: to_d8(m.tick_size, ps),
            step_size: to_d8(m.step_size, qs),
            min_qty: to_d8(m.min_qty, qs),
            min_notional: to_d8(m.min_notional, ps),
            contract_size: to_d8(m.contract_size, qs),
            is_trading: m.is_trading as u8,
        }
    }
}
