/// Per-shard symbol registry for Binance USDT-M Futures (§5.25).
///
/// `FuturesShardEngine` owns the [`FuturesSymbolState`] for every symbol
/// assigned to one logical shard.  It is the single source of mutable
/// per-symbol state inside a shard task.

use std::collections::HashMap;

use connector_core::InstrumentDefinition;

use crate::symbol_state::FuturesSymbolState;

pub struct FuturesShardEngine {
    shard_id: u32,
    symbols:  HashMap<String, FuturesSymbolState>,
}

impl FuturesShardEngine {
    pub fn new(shard_id: u32) -> Self {
        Self { shard_id, symbols: HashMap::new() }
    }

    /// Add `inst` to the engine.  No-op if the symbol is already registered.
    pub fn add_symbol(&mut self, inst: InstrumentDefinition) {
        let key = inst.symbol.clone();
        self.symbols.entry(key).or_insert_with(|| FuturesSymbolState::new(inst));
    }

    pub fn get(&self, symbol: &str) -> Option<&FuturesSymbolState> {
        self.symbols.get(symbol)
    }

    pub fn get_mut(&mut self, symbol: &str) -> Option<&mut FuturesSymbolState> {
        self.symbols.get_mut(symbol)
    }

    pub fn contains_symbol(&self, symbol: &str) -> bool {
        self.symbols.contains_key(symbol)
    }

    pub fn remove_symbol(&mut self, symbol: &str) -> Option<FuturesSymbolState> {
        self.symbols.remove(symbol)
    }

    pub fn shard_id(&self)     -> u32   { self.shard_id }
    pub fn symbol_count(&self) -> usize { self.symbols.len() }
    pub fn is_empty(&self)     -> bool  { self.symbols.is_empty() }

    pub fn symbols(&self) -> impl Iterator<Item = (&str, &FuturesSymbolState)> {
        self.symbols.iter().map(|(k, v)| (k.as_str(), v))
    }

    pub fn symbols_mut(&mut self) -> impl Iterator<Item = (&str, &mut FuturesSymbolState)> {
        self.symbols.iter_mut().map(|(k, v)| (k.as_str(), v))
    }

    pub fn symbol_names(&self) -> impl Iterator<Item = &str> {
        self.symbols.keys().map(|k| k.as_str())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use connector_core::{
        MarketType, MessageHeader, MessageType, VenueId, SCHEMA_VERSION, TS_NONE,
    };

    fn make_inst(symbol: &str) -> InstrumentDefinition {
        InstrumentDefinition {
            header: MessageHeader {
                schema_version:    SCHEMA_VERSION,
                message_type:      MessageType::InstrumentDefinition,
                venue_id:          VenueId::BinanceFutures,
                market_type:       MarketType::UsdmFutures,
                instrument_id:     1,
                connection_id:     0,
                instance_id:       0,
                sequence_number:   0,
                exchange_event_ts: TS_NONE,
                exchange_tx_ts:    TS_NONE,
                local_recv_ts:     TS_NONE,
                local_publish_ts:  TS_NONE,
            },
            symbol:        symbol.to_string(),
            base_asset:    "BTC".to_string(),
            quote_asset:   "USDT".to_string(),
            price_scale:   2,
            qty_scale:     3,
            tick_size:     100,
            step_size:     10,
            min_qty:       10,
            min_notional:  10_000_000_000,
            contract_size: 0,
            is_trading:    true,
        }
    }

    #[test]
    fn new_engine_is_empty() {
        let eng = FuturesShardEngine::new(0);
        assert!(eng.is_empty());
        assert_eq!(eng.symbol_count(), 0);
        assert_eq!(eng.shard_id(), 0);
    }

    #[test]
    fn add_and_get_symbol() {
        let mut eng = FuturesShardEngine::new(1);
        eng.add_symbol(make_inst("BTCUSDT"));
        assert!(eng.contains_symbol("BTCUSDT"));
        assert_eq!(eng.get("BTCUSDT").unwrap().symbol(), "BTCUSDT");
    }

    #[test]
    fn add_same_symbol_twice_keeps_original() {
        let mut eng = FuturesShardEngine::new(0);
        eng.add_symbol(make_inst("ETHUSDT"));
        eng.add_symbol(make_inst("ETHUSDT")); // no-op
        assert_eq!(eng.symbol_count(), 1);
    }

    #[test]
    fn get_mut_allows_modification() {
        let mut eng = FuturesShardEngine::new(0);
        eng.add_symbol(make_inst("SOLUSDT"));
        let state = eng.get_mut("SOLUSDT").unwrap();
        state.feed_state = connector_core::FeedState::Live;
        assert_eq!(eng.get("SOLUSDT").unwrap().feed_state, connector_core::FeedState::Live);
    }

    #[test]
    fn remove_symbol() {
        let mut eng = FuturesShardEngine::new(0);
        eng.add_symbol(make_inst("BNBUSDT"));
        assert!(eng.remove_symbol("BNBUSDT").is_some());
        assert!(!eng.contains_symbol("BNBUSDT"));
    }

    #[test]
    fn symbol_names_yields_all() {
        let mut eng = FuturesShardEngine::new(0);
        eng.add_symbol(make_inst("BTCUSDT"));
        eng.add_symbol(make_inst("ETHUSDT"));
        let mut names: Vec<&str> = eng.symbol_names().collect();
        names.sort_unstable();
        assert_eq!(names, vec!["BTCUSDT", "ETHUSDT"]);
    }
}
