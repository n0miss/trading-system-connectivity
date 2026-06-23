use clickhouse::Row;
use connector_core::{
    BestBidOffer, BookDelta, FundingRate, InstrumentDefinition, Liquidation, MarkPrice,
    OpenInterest, Trade,
};
use serde::Serialize;

// ---------------------------------------------------------------------------
// Common header fields embedded in every row.  Storing them flat (not nested)
// keeps ClickHouse queries simple: `WHERE symbol = 'BTCUSDT'` with no dot notation.
// ---------------------------------------------------------------------------

#[derive(Row, Serialize)]
pub struct TradeRow {
    pub exchange_event_ts: i64,
    pub local_recv_ts:     i64,
    pub local_publish_ts:  i64,
    pub venue_id:          u8,
    pub market_type:       u8,
    pub instrument_id:     u32,
    pub symbol:            String,
    pub sequence_number:   u64,
    pub trade_id:          u64,
    pub price:             i64,
    pub qty:               i64,
    pub trade_ts:          i64,
    pub is_buyer_maker:    u8,   // Bool in ClickHouse, serialised as UInt8
    pub aggressor_side:    u8,
}

impl From<&Trade> for TradeRow {
    fn from(m: &Trade) -> Self {
        Self {
            exchange_event_ts: m.header.exchange_event_ts,
            local_recv_ts:     m.header.local_recv_ts,
            local_publish_ts:  m.header.local_publish_ts,
            venue_id:          m.header.venue_id as u8,
            market_type:       m.header.market_type as u8,
            instrument_id:     m.header.instrument_id,
            symbol:            m.symbol.clone(),
            sequence_number:   m.header.sequence_number,
            trade_id:          m.trade_id,
            price:             m.price,
            qty:               m.qty,
            trade_ts:          m.trade_ts,
            is_buyer_maker:    m.is_buyer_maker as u8,
            aggressor_side:    m.aggressor_side as u8,
        }
    }
}

// ---------------------------------------------------------------------------

#[derive(Row, Serialize)]
pub struct BboRow {
    pub exchange_event_ts: i64,
    pub local_recv_ts:     i64,
    pub local_publish_ts:  i64,
    pub venue_id:          u8,
    pub market_type:       u8,
    pub instrument_id:     u32,
    pub symbol:            String,
    pub sequence_number:   u64,
    pub bid_price:         i64,
    pub bid_qty:           i64,
    pub ask_price:         i64,
    pub ask_qty:           i64,
    pub update_id:         u64,
}

impl From<&BestBidOffer> for BboRow {
    fn from(m: &BestBidOffer) -> Self {
        Self {
            exchange_event_ts: m.header.exchange_event_ts,
            local_recv_ts:     m.header.local_recv_ts,
            local_publish_ts:  m.header.local_publish_ts,
            venue_id:          m.header.venue_id as u8,
            market_type:       m.header.market_type as u8,
            instrument_id:     m.header.instrument_id,
            symbol:            m.symbol.clone(),
            sequence_number:   m.header.sequence_number,
            bid_price:         m.bid_price,
            bid_qty:           m.bid_qty,
            ask_price:         m.ask_price,
            ask_qty:           m.ask_qty,
            update_id:         m.update_id,
        }
    }
}

// ---------------------------------------------------------------------------

#[derive(Row, Serialize)]
pub struct MarkPriceRow {
    pub exchange_event_ts: i64,
    pub local_recv_ts:     i64,
    pub local_publish_ts:  i64,
    pub venue_id:          u8,
    pub market_type:       u8,
    pub instrument_id:     u32,
    pub symbol:            String,
    pub sequence_number:   u64,
    pub mark_price:        i64,
    pub index_price:       i64,
}

impl From<&MarkPrice> for MarkPriceRow {
    fn from(m: &MarkPrice) -> Self {
        Self {
            exchange_event_ts: m.header.exchange_event_ts,
            local_recv_ts:     m.header.local_recv_ts,
            local_publish_ts:  m.header.local_publish_ts,
            venue_id:          m.header.venue_id as u8,
            market_type:       m.header.market_type as u8,
            instrument_id:     m.header.instrument_id,
            symbol:            m.symbol.clone(),
            sequence_number:   m.header.sequence_number,
            mark_price:        m.mark_price,
            index_price:       m.index_price,
        }
    }
}

// ---------------------------------------------------------------------------

#[derive(Row, Serialize)]
pub struct FundingRateRow {
    pub exchange_event_ts:  i64,
    pub local_recv_ts:      i64,
    pub local_publish_ts:   i64,
    pub venue_id:           u8,
    pub market_type:        u8,
    pub instrument_id:      u32,
    pub symbol:             String,
    pub sequence_number:    u64,
    pub funding_rate:       i64,
    pub next_funding_time:  i64,
}

impl From<&FundingRate> for FundingRateRow {
    fn from(m: &FundingRate) -> Self {
        Self {
            exchange_event_ts: m.header.exchange_event_ts,
            local_recv_ts:     m.header.local_recv_ts,
            local_publish_ts:  m.header.local_publish_ts,
            venue_id:          m.header.venue_id as u8,
            market_type:       m.header.market_type as u8,
            instrument_id:     m.header.instrument_id,
            symbol:            m.symbol.clone(),
            sequence_number:   m.header.sequence_number,
            funding_rate:      m.funding_rate,
            next_funding_time: m.next_funding_time,
        }
    }
}

// ---------------------------------------------------------------------------

#[derive(Row, Serialize)]
pub struct LiquidationRow {
    pub exchange_event_ts: i64,
    pub local_recv_ts:     i64,
    pub local_publish_ts:  i64,
    pub venue_id:          u8,
    pub market_type:       u8,
    pub instrument_id:     u32,
    pub symbol:            String,
    pub sequence_number:   u64,
    pub side:              u8,
    pub price:             i64,
    pub qty:               i64,
    pub avg_price:         i64,
    pub last_filled_qty:   i64,
}

impl From<&Liquidation> for LiquidationRow {
    fn from(m: &Liquidation) -> Self {
        Self {
            exchange_event_ts: m.header.exchange_event_ts,
            local_recv_ts:     m.header.local_recv_ts,
            local_publish_ts:  m.header.local_publish_ts,
            venue_id:          m.header.venue_id as u8,
            market_type:       m.header.market_type as u8,
            instrument_id:     m.header.instrument_id,
            symbol:            m.symbol.clone(),
            sequence_number:   m.header.sequence_number,
            side:              m.side as u8,
            price:             m.price,
            qty:               m.qty,
            avg_price:         m.avg_price,
            last_filled_qty:   m.last_filled_qty,
        }
    }
}

// ---------------------------------------------------------------------------

#[derive(Row, Serialize)]
pub struct OpenInterestRow {
    pub exchange_event_ts: i64,
    pub local_recv_ts:     i64,
    pub local_publish_ts:  i64,
    pub venue_id:          u8,
    pub market_type:       u8,
    pub instrument_id:     u32,
    pub symbol:            String,
    pub sequence_number:   u64,
    pub open_interest:     i64,
}

impl From<&OpenInterest> for OpenInterestRow {
    fn from(m: &OpenInterest) -> Self {
        Self {
            exchange_event_ts: m.header.exchange_event_ts,
            local_recv_ts:     m.header.local_recv_ts,
            local_publish_ts:  m.header.local_publish_ts,
            venue_id:          m.header.venue_id as u8,
            market_type:       m.header.market_type as u8,
            instrument_id:     m.header.instrument_id,
            symbol:            m.symbol.clone(),
            sequence_number:   m.header.sequence_number,
            open_interest:     m.open_interest,
        }
    }
}

// ---------------------------------------------------------------------------
// Instrument reference data
// ---------------------------------------------------------------------------

#[derive(Row, Serialize)]
pub struct InstrumentRow {
    pub local_recv_ts:  i64,
    pub venue_id:       u8,
    pub market_type:    u8,
    pub instrument_id:  u32,
    pub symbol:         String,
    pub base_asset:     String,
    pub quote_asset:    String,
    pub price_scale:    u32,
    pub qty_scale:      u32,
    pub tick_size:      i64,
    pub step_size:      i64,
    pub min_qty:        i64,
    pub min_notional:   i64,
    pub contract_size:  i64,
    pub is_trading:     u8,
}

impl From<&InstrumentDefinition> for InstrumentRow {
    fn from(m: &InstrumentDefinition) -> Self {
        Self {
            local_recv_ts:  m.header.local_recv_ts,
            venue_id:       m.header.venue_id as u8,
            market_type:    m.header.market_type as u8,
            instrument_id:  m.header.instrument_id,
            symbol:         m.symbol.clone(),
            base_asset:     m.base_asset.clone(),
            quote_asset:    m.quote_asset.clone(),
            price_scale:    m.price_scale,
            qty_scale:      m.qty_scale,
            tick_size:      m.tick_size,
            step_size:      m.step_size,
            min_qty:        m.min_qty,
            min_notional:   m.min_notional,
            contract_size:  m.contract_size,
            is_trading:     m.is_trading as u8,
        }
    }
}

// ---------------------------------------------------------------------------
// BookDelta uses Array(Int64) columns for the price levels so each delta is
// one row (not one row per level).  This keeps insert volume manageable while
// still storing full depth information.
// ---------------------------------------------------------------------------

#[derive(Row, Serialize)]
pub struct BookDeltaRow {
    pub exchange_event_ts: i64,
    pub local_recv_ts:     i64,
    pub local_publish_ts:  i64,
    pub venue_id:          u8,
    pub market_type:       u8,
    pub instrument_id:     u32,
    pub symbol:            String,
    pub sequence_number:   u64,
    pub first_update_id:   u64,
    pub final_update_id:   u64,
    pub prev_update_id:    u64,
    pub bid_prices:        Vec<i64>,
    pub bid_qtys:          Vec<i64>,
    pub ask_prices:        Vec<i64>,
    pub ask_qtys:          Vec<i64>,
}

impl From<&BookDelta> for BookDeltaRow {
    fn from(m: &BookDelta) -> Self {
        Self {
            exchange_event_ts: m.header.exchange_event_ts,
            local_recv_ts:     m.header.local_recv_ts,
            local_publish_ts:  m.header.local_publish_ts,
            venue_id:          m.header.venue_id as u8,
            market_type:       m.header.market_type as u8,
            instrument_id:     m.header.instrument_id,
            symbol:            m.symbol.clone(),
            sequence_number:   m.header.sequence_number,
            first_update_id:   m.first_update_id,
            final_update_id:   m.final_update_id,
            prev_update_id:    m.prev_update_id,
            bid_prices:        m.bids.iter().map(|l| l.price).collect(),
            bid_qtys:          m.bids.iter().map(|l| l.qty).collect(),
            ask_prices:        m.asks.iter().map(|l| l.price).collect(),
            ask_qtys:          m.asks.iter().map(|l| l.qty).collect(),
        }
    }
}
