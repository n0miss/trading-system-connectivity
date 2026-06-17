use std::collections::HashMap;

use connector_core::{
    InstrumentDefinition, MarketType, MessageHeader, MessageType, TradingStatus, VenueId,
    SCHEMA_VERSION, TS_NONE,
};

use crate::normalizer::symbol_instrument_id;

/// In-memory store of all known instrument definitions.
///
/// Tracks per-symbol trading status so callers can detect changes and
/// publish `TradingStatus` messages when a symbol goes live or halts.
pub struct InstrumentRegistry {
    instruments: HashMap<String, InstrumentDefinition>,
    venue_id:    VenueId,
    market_type: MarketType,
    instance_id: u32,
    next_seq:    u64,
}

impl InstrumentRegistry {
    pub fn new(venue_id: VenueId, market_type: MarketType, instance_id: u32) -> Self {
        Self {
            instruments: HashMap::new(),
            venue_id,
            market_type,
            instance_id,
            next_seq: 0,
        }
    }

    /// Insert or update a definition.
    ///
    /// Returns `Some(TradingStatus)` when `is_trading` changed for an existing symbol,
    /// or when a symbol is seen for the first time (so downstream can publish the initial state).
    pub fn upsert(&mut self, def: InstrumentDefinition) -> Option<TradingStatus> {
        let seq = self.next_seq;
        self.next_seq += 1;

        let symbol = def.symbol.clone();
        let new_trading = def.is_trading;

        let changed = match self.instruments.get(&symbol) {
            None       => true,                           // new symbol
            Some(prev) => prev.is_trading != new_trading, // status flipped
        };

        self.instruments.insert(symbol.clone(), def);

        if changed {
            Some(TradingStatus {
                header: MessageHeader {
                    schema_version:    SCHEMA_VERSION,
                    message_type:      MessageType::TradingStatus,
                    venue_id:          self.venue_id,
                    market_type:       self.market_type,
                    instrument_id:     symbol_instrument_id(&symbol),
                    connection_id:     0,
                    instance_id:       self.instance_id,
                    sequence_number:   seq,
                    exchange_event_ts: TS_NONE,
                    exchange_tx_ts:    TS_NONE,
                    local_recv_ts:     TS_NONE,
                    local_publish_ts:  TS_NONE,
                },
                symbol,
                is_trading: new_trading,
            })
        } else {
            None
        }
    }

    /// Apply a full batch of definitions (e.g. a fresh exchangeInfo response).
    /// Returns every `TradingStatus` change produced by the batch.
    pub fn apply_batch(&mut self, defs: Vec<InstrumentDefinition>) -> Vec<TradingStatus> {
        defs.into_iter().filter_map(|d| self.upsert(d)).collect()
    }

    pub fn get(&self, symbol: &str) -> Option<&InstrumentDefinition> {
        self.instruments.get(symbol)
    }

    pub fn iter(&self) -> impl Iterator<Item = &InstrumentDefinition> {
        self.instruments.values()
    }

    pub fn len(&self) -> usize {
        self.instruments.len()
    }

    pub fn is_empty(&self) -> bool {
        self.instruments.is_empty()
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use connector_core::{MarketType, MessageType, VenueId};

    use crate::normalizer::{parse_exchange_info, SPOT_JSON_FOR_TESTS};

    fn make_registry() -> InstrumentRegistry {
        InstrumentRegistry::new(VenueId::BinanceSpot, MarketType::Spot, 1)
    }

    fn btc_def(is_trading: bool) -> InstrumentDefinition {
        let mut defs = parse_exchange_info(
            SPOT_JSON_FOR_TESTS, VenueId::BinanceSpot, MarketType::Spot, 1, 0,
        ).unwrap();
        let btc = defs.remove(defs.iter().position(|d| d.symbol == "BTCUSDT").unwrap());
        InstrumentDefinition { is_trading, ..btc }
    }

    #[test]
    fn first_insert_always_produces_status() {
        let mut reg = make_registry();
        let status = reg.upsert(btc_def(true));
        assert!(status.is_some());
        let s = status.unwrap();
        assert_eq!(s.symbol, "BTCUSDT");
        assert!(s.is_trading);
        assert_eq!(s.header.message_type, MessageType::TradingStatus);
    }

    #[test]
    fn update_without_status_change_returns_none() {
        let mut reg = make_registry();
        reg.upsert(btc_def(true));
        let status = reg.upsert(btc_def(true)); // same is_trading
        assert!(status.is_none());
    }

    #[test]
    fn trading_to_break_produces_status() {
        let mut reg = make_registry();
        reg.upsert(btc_def(true));
        let status = reg.upsert(btc_def(false));
        assert!(status.is_some());
        assert!(!status.unwrap().is_trading);
    }

    #[test]
    fn break_to_trading_produces_status() {
        let mut reg = make_registry();
        reg.upsert(btc_def(false));
        let status = reg.upsert(btc_def(true));
        assert!(status.is_some());
        assert!(status.unwrap().is_trading);
    }

    #[test]
    fn apply_batch_counts_changes() {
        let defs = parse_exchange_info(
            SPOT_JSON_FOR_TESTS, VenueId::BinanceSpot, MarketType::Spot, 1, 0,
        ).unwrap();
        let count = defs.len();

        let mut reg = make_registry();
        let changes = reg.apply_batch(defs.clone());
        // All symbols are new → all produce a status
        assert_eq!(changes.len(), count);
        assert_eq!(reg.len(), count);

        // Second application with same data → no changes
        let changes2 = reg.apply_batch(defs);
        assert_eq!(changes2.len(), 0);
    }

    #[test]
    fn registry_get_returns_latest_definition() {
        let mut reg = make_registry();
        reg.upsert(btc_def(true));
        let def = reg.get("BTCUSDT").unwrap();
        assert_eq!(def.base_asset, "BTC");
    }

    #[test]
    fn status_change_header_fields() {
        let mut reg = InstrumentRegistry::new(VenueId::BinanceFutures, MarketType::UsdmFutures, 42);
        let status = reg.upsert(btc_def(true)).unwrap();
        assert_eq!(status.header.venue_id,    VenueId::BinanceFutures);
        assert_eq!(status.header.market_type, MarketType::UsdmFutures);
        assert_eq!(status.header.instance_id, 42);
    }

    #[test]
    fn sequence_numbers_increment_across_upserts() {
        let mut reg = make_registry();
        let s1 = reg.upsert(btc_def(true)).unwrap();
        // second upsert with same value → no status returned, but seq still advances
        reg.upsert(btc_def(true));
        let s3 = reg.upsert(btc_def(false)).unwrap();
        // s3 seq must be > s1 seq
        assert!(s3.header.sequence_number > s1.header.sequence_number);
    }
}
