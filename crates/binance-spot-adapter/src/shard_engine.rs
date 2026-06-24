/// Per-shard symbol registry (§4.19).
///
/// `ShardEngine` owns the `SymbolState` for every symbol assigned to
/// one logical shard.  It is the single source of mutable per-symbol
/// state inside a shard task.
use std::collections::HashMap;

use connector_core::InstrumentDefinition;

use crate::symbol_state::SymbolState;

pub struct ShardEngine {
    shard_id: u32,
    symbols: HashMap<String, SymbolState>,
}

impl ShardEngine {
    pub fn new(shard_id: u32) -> Self {
        Self {
            shard_id,
            symbols: HashMap::new(),
        }
    }

    /// Add `inst` to the engine.  No-op if the symbol is already registered
    /// (original state is preserved).
    pub fn add_symbol(&mut self, inst: InstrumentDefinition) {
        let key = inst.symbol.clone();
        self.symbols
            .entry(key)
            .or_insert_with(|| SymbolState::new(inst));
    }

    pub fn get(&self, symbol: &str) -> Option<&SymbolState> {
        self.symbols.get(symbol)
    }

    pub fn get_mut(&mut self, symbol: &str) -> Option<&mut SymbolState> {
        self.symbols.get_mut(symbol)
    }

    pub fn contains_symbol(&self, symbol: &str) -> bool {
        self.symbols.contains_key(symbol)
    }

    pub fn remove_symbol(&mut self, symbol: &str) -> Option<SymbolState> {
        self.symbols.remove(symbol)
    }

    pub fn shard_id(&self) -> u32 {
        self.shard_id
    }
    pub fn symbol_count(&self) -> usize {
        self.symbols.len()
    }
    pub fn is_empty(&self) -> bool {
        self.symbols.is_empty()
    }

    pub fn symbols(&self) -> impl Iterator<Item = (&str, &SymbolState)> {
        self.symbols.iter().map(|(k, v)| (k.as_str(), v))
    }

    pub fn symbols_mut(&mut self) -> impl Iterator<Item = (&str, &mut SymbolState)> {
        self.symbols.iter_mut().map(|(k, v)| (k.as_str(), v))
    }

    pub fn symbol_names(&self) -> impl Iterator<Item = &str> {
        self.symbols.keys().map(|k| k.as_str())
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use connector_core::{
        BookStaleReason, MarketType, MessageHeader, MessageType, VenueId, SCHEMA_VERSION, TS_NONE,
    };

    fn test_inst(symbol: &str) -> InstrumentDefinition {
        InstrumentDefinition {
            header: MessageHeader {
                schema_version: SCHEMA_VERSION,
                message_type: MessageType::InstrumentDefinition,
                venue_id: VenueId::BinanceSpot,
                market_type: MarketType::Spot,
                instrument_id: 0,
                connection_id: 0,
                instance_id: 0,
                sequence_number: 0,
                exchange_event_ts: TS_NONE,
                exchange_tx_ts: TS_NONE,
                local_recv_ts: 0,
                local_publish_ts: 0,
            },
            symbol: symbol.to_string(),
            base_asset: "BTC".into(),
            quote_asset: "USDT".into(),
            price_scale: 2,
            qty_scale: 5,
            tick_size: 1,
            step_size: 1,
            min_qty: 1,
            min_notional: 0,
            contract_size: 0,
            is_trading: true,
        }
    }

    #[test]
    fn new_engine_is_empty() {
        let eng = ShardEngine::new(0);
        assert!(eng.is_empty());
        assert_eq!(eng.symbol_count(), 0);
        assert_eq!(eng.shard_id(), 0);
    }

    #[test]
    fn add_symbol_stores_state() {
        let mut eng = ShardEngine::new(1);
        eng.add_symbol(test_inst("BTCUSDT"));
        assert!(!eng.is_empty());
        assert_eq!(eng.symbol_count(), 1);
        assert!(eng.contains_symbol("BTCUSDT"));
    }

    #[test]
    fn get_returns_none_for_unknown_symbol() {
        let eng = ShardEngine::new(0);
        assert!(eng.get("BTCUSDT").is_none());
    }

    #[test]
    fn get_returns_state_for_known_symbol() {
        let mut eng = ShardEngine::new(0);
        eng.add_symbol(test_inst("ETHUSDT"));
        let state = eng.get("ETHUSDT");
        assert!(state.is_some());
        assert_eq!(state.unwrap().symbol(), "ETHUSDT");
    }

    #[test]
    fn get_mut_allows_state_modification() {
        let mut eng = ShardEngine::new(0);
        eng.add_symbol(test_inst("SOLUSDT"));
        eng.get_mut("SOLUSDT")
            .unwrap()
            .book
            .mark_stale(BookStaleReason::SequenceGap);
        assert!(eng.get("SOLUSDT").unwrap().book.is_stale());
    }

    #[test]
    fn add_same_symbol_twice_keeps_original() {
        let mut eng = ShardEngine::new(0);
        eng.add_symbol(test_inst("BTCUSDT"));
        eng.get_mut("BTCUSDT")
            .unwrap()
            .book
            .mark_stale(BookStaleReason::SequenceGap);
        eng.add_symbol(test_inst("BTCUSDT")); // should not replace
        assert!(
            eng.get("BTCUSDT").unwrap().is_stale(),
            "original should be preserved"
        );
    }

    #[test]
    fn remove_symbol_drops_state() {
        let mut eng = ShardEngine::new(0);
        eng.add_symbol(test_inst("BNBUSDT"));
        let removed = eng.remove_symbol("BNBUSDT");
        assert!(removed.is_some());
        assert!(!eng.contains_symbol("BNBUSDT"));
    }

    #[test]
    fn remove_unknown_symbol_returns_none() {
        let mut eng = ShardEngine::new(0);
        assert!(eng.remove_symbol("XRPUSDT").is_none());
    }

    #[test]
    fn symbol_names_yields_all() {
        let mut eng = ShardEngine::new(0);
        eng.add_symbol(test_inst("BTCUSDT"));
        eng.add_symbol(test_inst("ETHUSDT"));
        let mut names: Vec<&str> = eng.symbol_names().collect();
        names.sort_unstable();
        assert_eq!(names, vec!["BTCUSDT", "ETHUSDT"]);
    }

    #[test]
    fn multiple_symbols_each_accessible() {
        let mut eng = ShardEngine::new(2);
        for sym in ["BTCUSDT", "ETHUSDT", "SOLUSDT", "BNBUSDT"] {
            eng.add_symbol(test_inst(sym));
        }
        assert_eq!(eng.symbol_count(), 4);
        for sym in ["BTCUSDT", "ETHUSDT", "SOLUSDT", "BNBUSDT"] {
            assert!(eng.contains_symbol(sym));
            assert_eq!(eng.get(sym).unwrap().symbol(), sym);
        }
    }
}
