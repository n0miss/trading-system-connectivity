use std::collections::HashMap;

use connector_core::InstrumentDefinition;

use crate::event::{RefDataEvent, business_fields_differ, make_trading_status};

/// In-memory store of all known instrument definitions.
///
/// Emits a [`RefDataEvent`] whenever a symbol is added or its business fields
/// change.  Callers derive [`TradingStatus`] messages from the event when
/// `is_trading` changes.
///
/// [`TradingStatus`]: connector_core::TradingStatus
pub struct InstrumentRegistry {
    instruments: HashMap<String, InstrumentDefinition>,
    /// Monotonically increasing counter for `TradingStatus` sequence numbers.
    next_seq:    u64,
}

impl InstrumentRegistry {
    pub fn new() -> Self {
        Self {
            instruments: HashMap::new(),
            next_seq:    0,
        }
    }

    /// Insert or update a definition.
    ///
    /// Returns `Some(RefDataEvent)` when the symbol is new or any business
    /// field changed.  Returns `None` when the incoming definition is
    /// identical to the stored one (header differences are ignored).
    ///
    /// `next_seq` is always advanced regardless of whether an event is emitted,
    /// so any subsequent event is guaranteed a higher sequence number.
    pub fn upsert(&mut self, def: InstrumentDefinition) -> Option<RefDataEvent> {
        let seq = self.next_seq;
        self.next_seq += 1;

        match self.instruments.get(&def.symbol) {
            None => {
                let status = make_trading_status(&def, seq);
                let ev = RefDataEvent::Added { def: def.clone(), status };
                self.instruments.insert(def.symbol.clone(), def);
                Some(ev)
            }
            Some(prev) => {
                if business_fields_differ(prev, &def) {
                    let status = if prev.is_trading != def.is_trading {
                        Some(make_trading_status(&def, seq))
                    } else {
                        None
                    };
                    let old = prev.clone();
                    self.instruments.insert(def.symbol.clone(), def.clone());
                    Some(RefDataEvent::Updated { old, new: def, status })
                } else {
                    None
                }
            }
        }
    }

    /// Apply a full batch of definitions (e.g. a fresh exchangeInfo response).
    ///
    /// Returns every [`RefDataEvent`] produced by the batch.
    pub fn apply_batch(&mut self, defs: Vec<InstrumentDefinition>) -> Vec<RefDataEvent> {
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

impl Default for InstrumentRegistry {
    fn default() -> Self {
        Self::new()
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
        InstrumentRegistry::new()
    }

    fn btc_def(is_trading: bool) -> InstrumentDefinition {
        let mut defs = parse_exchange_info(
            SPOT_JSON_FOR_TESTS, VenueId::BinanceSpot, MarketType::Spot, 1, 0,
        ).unwrap();
        let pos = defs.iter().position(|d| d.symbol == "BTCUSDT").unwrap();
        let btc = defs.remove(pos);
        InstrumentDefinition { is_trading, ..btc }
    }

    // --- upsert: Added events ---

    #[test]
    fn first_insert_always_produces_added_event() {
        let mut reg = make_registry();
        let event = reg.upsert(btc_def(true)).unwrap();
        assert!(event.is_added());
        assert_eq!(event.definition().symbol, "BTCUSDT");
        let ts = event.trading_status().unwrap();
        assert_eq!(ts.header.message_type, MessageType::TradingStatus);
        assert!(ts.is_trading);
    }

    #[test]
    fn added_event_carries_correct_is_trading() {
        let mut reg = make_registry();
        let event = reg.upsert(btc_def(false)).unwrap();
        let ts = event.trading_status().unwrap();
        assert_eq!(ts.symbol, "BTCUSDT");
        assert!(!ts.is_trading);
    }

    // --- upsert: no-change ---

    #[test]
    fn update_without_any_field_change_returns_none() {
        let mut reg = make_registry();
        reg.upsert(btc_def(true));
        let event = reg.upsert(btc_def(true));
        assert!(event.is_none());
    }

    // --- upsert: Updated events for is_trading ---

    #[test]
    fn trading_to_break_produces_updated_with_status() {
        let mut reg = make_registry();
        reg.upsert(btc_def(true));
        let event = reg.upsert(btc_def(false)).unwrap();
        assert!(event.is_updated());
        let ts = event.trading_status().unwrap();
        assert!(!ts.is_trading);
    }

    #[test]
    fn break_to_trading_produces_updated_with_status() {
        let mut reg = make_registry();
        reg.upsert(btc_def(false));
        let event = reg.upsert(btc_def(true)).unwrap();
        assert!(event.is_updated());
        let ts = event.trading_status().unwrap();
        assert!(ts.is_trading);
    }

    // --- upsert: Updated events for non-status fields ---

    #[test]
    fn tick_size_change_produces_updated_without_status() {
        let mut reg = make_registry();
        reg.upsert(btc_def(true));

        let mut changed = btc_def(true);
        changed.tick_size += 1;

        let event = reg.upsert(changed).unwrap();
        assert!(event.is_updated());
        assert!(event.trading_status().is_none(), "is_trading did not change");
        assert!(!event.trading_changed());
    }

    #[test]
    fn price_scale_change_produces_updated_event() {
        let mut reg = make_registry();
        reg.upsert(btc_def(true));

        let mut changed = btc_def(true);
        changed.price_scale += 1;

        let event = reg.upsert(changed).unwrap();
        assert!(event.is_updated());
        assert!(event.trading_status().is_none());
    }

    #[test]
    fn min_notional_change_produces_updated_event() {
        let mut reg = make_registry();
        reg.upsert(btc_def(true));

        let mut changed = btc_def(true);
        changed.min_notional += 1_000;

        let event = reg.upsert(changed).unwrap();
        assert!(event.is_updated());
    }

    #[test]
    fn updated_old_field_holds_previous_definition() {
        let mut reg = make_registry();
        reg.upsert(btc_def(true));

        let mut changed = btc_def(true);
        changed.tick_size = 99999;

        let event = reg.upsert(changed.clone()).unwrap();
        if let RefDataEvent::Updated { old, new, .. } = &event {
            assert_ne!(old.tick_size, new.tick_size);
            assert_eq!(new.tick_size, 99999);
        } else {
            panic!("expected Updated variant");
        }
    }

    // --- apply_batch ---

    #[test]
    fn apply_batch_emits_added_event_per_new_symbol() {
        let defs = parse_exchange_info(
            SPOT_JSON_FOR_TESTS, VenueId::BinanceSpot, MarketType::Spot, 1, 0,
        ).unwrap();
        let count = defs.len();

        let mut reg = make_registry();
        let events = reg.apply_batch(defs.clone());

        assert_eq!(events.len(), count, "every new symbol produces an Added event");
        assert_eq!(reg.len(), count);
        assert!(events.iter().all(|e| e.is_added()));
    }

    #[test]
    fn apply_batch_second_pass_with_same_data_emits_no_events() {
        let defs = parse_exchange_info(
            SPOT_JSON_FOR_TESTS, VenueId::BinanceSpot, MarketType::Spot, 1, 0,
        ).unwrap();

        let mut reg = make_registry();
        reg.apply_batch(defs.clone());
        let events = reg.apply_batch(defs);
        assert_eq!(events.len(), 0);
    }

    // --- get / iter ---

    #[test]
    fn get_returns_latest_definition() {
        let mut reg = make_registry();
        reg.upsert(btc_def(true));
        let def = reg.get("BTCUSDT").unwrap();
        assert_eq!(def.base_asset, "BTC");
    }

    // --- sequence numbers ---

    #[test]
    fn sequence_numbers_increase_across_upserts() {
        let mut reg = make_registry();
        let e1 = reg.upsert(btc_def(true)).unwrap();
        reg.upsert(btc_def(true)); // no change — advances seq internally
        let e3 = reg.upsert(btc_def(false)).unwrap();

        let seq1 = e1.trading_status().unwrap().header.sequence_number;
        let seq3 = e3.trading_status().unwrap().header.sequence_number;
        assert!(seq3 > seq1, "seq3={seq3} must be > seq1={seq1}");
    }

    // --- trading_status inherits venue from definition ---

    #[test]
    fn trading_status_inherits_venue_from_definition() {
        let mut reg = make_registry();
        let event = reg.upsert(btc_def(true)).unwrap();
        let ts = event.trading_status().unwrap();
        // btc_def uses VenueId::BinanceSpot, MarketType::Spot, instance_id=1
        assert_eq!(ts.header.venue_id,    VenueId::BinanceSpot);
        assert_eq!(ts.header.market_type, MarketType::Spot);
        assert_eq!(ts.header.instance_id, 1);
    }
}
