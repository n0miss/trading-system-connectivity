use connector_core::{
    InstrumentDefinition, MessageHeader, MessageType, TradingStatus, SCHEMA_VERSION, TS_NONE,
};

/// A change event emitted by [`InstrumentRegistry`] when a symbol is added
/// or any of its business fields change.
///
/// Use [`definition`] to obtain the current instrument state and
/// [`trading_status`] to obtain a publishable `TradingStatus` message when
/// `is_trading` changed.
///
/// [`InstrumentRegistry`]: crate::registry::InstrumentRegistry
/// [`definition`]:         RefDataEvent::definition
/// [`trading_status`]:     RefDataEvent::trading_status
#[derive(Debug, Clone)]
pub enum RefDataEvent {
    /// The symbol was seen for the first time in this session.
    Added {
        def: InstrumentDefinition,
        /// Initial trading status (always present for new symbols).
        status: TradingStatus,
    },
    /// The symbol already existed and ≥1 business field changed.
    Updated {
        old: InstrumentDefinition,
        new: InstrumentDefinition,
        /// `Some` only when `is_trading` changed; `None` for other field changes.
        status: Option<TradingStatus>,
    },
}

impl RefDataEvent {
    /// The current (new or initial) [`InstrumentDefinition`].
    pub fn definition(&self) -> &InstrumentDefinition {
        match self {
            Self::Added { def, .. } => def,
            Self::Updated { new, .. } => new,
        }
    }

    /// The associated [`TradingStatus`] event, if any.
    ///
    /// Always `Some` for [`Added`]; `Some` for [`Updated`] only when
    /// `is_trading` flipped.
    ///
    /// [`Added`]:   RefDataEvent::Added
    /// [`Updated`]: RefDataEvent::Updated
    pub fn trading_status(&self) -> Option<&TradingStatus> {
        match self {
            Self::Added { status, .. } => Some(status),
            Self::Updated { status, .. } => status.as_ref(),
        }
    }

    /// `true` when this event represents a brand-new symbol.
    pub fn is_added(&self) -> bool {
        matches!(self, Self::Added { .. })
    }

    /// `true` when this event represents a field change on an existing symbol.
    pub fn is_updated(&self) -> bool {
        matches!(self, Self::Updated { .. })
    }

    /// `true` when `is_trading` changed (always `true` for `Added` events).
    pub fn trading_changed(&self) -> bool {
        match self {
            Self::Added { .. } => true,
            Self::Updated { old, new, .. } => old.is_trading != new.is_trading,
        }
    }
}

// ---------------------------------------------------------------------------
// pub(crate) helpers used by registry.rs
// ---------------------------------------------------------------------------

/// Build a `TradingStatus` message from an `InstrumentDefinition`, inheriting
/// venue/market/instance from the definition's header.
pub(crate) fn make_trading_status(def: &InstrumentDefinition, seq: u64) -> TradingStatus {
    TradingStatus {
        header: MessageHeader {
            schema_version: SCHEMA_VERSION,
            message_type: MessageType::TradingStatus,
            venue_id: def.header.venue_id,
            market_type: def.header.market_type,
            instrument_id: def.header.instrument_id,
            connection_id: 0,
            instance_id: def.header.instance_id,
            sequence_number: seq,
            exchange_event_ts: TS_NONE,
            exchange_tx_ts: TS_NONE,
            local_recv_ts: TS_NONE,
            local_publish_ts: TS_NONE,
        },
        symbol: def.symbol.clone(),
        is_trading: def.is_trading,
    }
}

/// Returns `true` when any business field differs between `a` and `b`.
///
/// The `header` field is deliberately excluded — it changes on every REST fetch
/// (sequence number, timestamps) and does not represent a meaningful change.
pub(crate) fn business_fields_differ(a: &InstrumentDefinition, b: &InstrumentDefinition) -> bool {
    a.base_asset != b.base_asset
        || a.quote_asset != b.quote_asset
        || a.price_scale != b.price_scale
        || a.qty_scale != b.qty_scale
        || a.tick_size != b.tick_size
        || a.step_size != b.step_size
        || a.min_qty != b.min_qty
        || a.min_notional != b.min_notional
        || a.contract_size != b.contract_size
        || a.is_trading != b.is_trading
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use connector_core::MessageHeader;
    use connector_core::{MarketType, MessageType, VenueId, SCHEMA_VERSION, TS_NONE};

    fn make_def(symbol: &str, is_trading: bool, tick_size: i64) -> InstrumentDefinition {
        InstrumentDefinition {
            header: MessageHeader {
                schema_version: SCHEMA_VERSION,
                message_type: MessageType::InstrumentDefinition,
                venue_id: VenueId::BinanceSpot,
                market_type: MarketType::Spot,
                instrument_id: 42,
                connection_id: 0,
                instance_id: 1,
                sequence_number: 0,
                exchange_event_ts: TS_NONE,
                exchange_tx_ts: TS_NONE,
                local_recv_ts: TS_NONE,
                local_publish_ts: TS_NONE,
            },
            symbol: symbol.to_string(),
            base_asset: "BTC".to_string(),
            quote_asset: "USDT".to_string(),
            price_scale: 2,
            qty_scale: 3,
            tick_size,
            step_size: 10,
            min_qty: 10,
            min_notional: 10_000,
            contract_size: 0,
            is_trading,
        }
    }

    // --- RefDataEvent accessors ---

    #[test]
    fn added_definition_returns_the_def() {
        let def = make_def("BTCUSDT", true, 100);
        let status = make_trading_status(&def, 0);
        let event = RefDataEvent::Added {
            def: def.clone(),
            status,
        };
        assert_eq!(event.definition().symbol, "BTCUSDT");
        assert!(event.is_added());
        assert!(!event.is_updated());
        assert!(event.trading_changed());
    }

    #[test]
    fn updated_definition_returns_new_def() {
        let old = make_def("BTCUSDT", true, 100);
        let new = make_def("BTCUSDT", true, 200);
        let event = RefDataEvent::Updated {
            old,
            new,
            status: None,
        };
        assert_eq!(event.definition().tick_size, 200);
        assert!(event.is_updated());
        assert!(!event.is_added());
    }

    #[test]
    fn trading_status_is_always_some_for_added() {
        let def = make_def("BTCUSDT", true, 100);
        let status = make_trading_status(&def, 0);
        let event = RefDataEvent::Added { def, status };
        assert!(event.trading_status().is_some());
    }

    #[test]
    fn trading_status_some_when_is_trading_flips_in_update() {
        let old = make_def("BTCUSDT", true, 100);
        let new = make_def("BTCUSDT", false, 100);
        let ts = make_trading_status(&new, 1);
        let event = RefDataEvent::Updated {
            old,
            new,
            status: Some(ts),
        };
        assert!(event.trading_status().is_some());
        assert!(event.trading_changed());
        assert!(!event.trading_status().unwrap().is_trading);
    }

    #[test]
    fn trading_status_none_when_only_non_status_field_changed() {
        let old = make_def("BTCUSDT", true, 100);
        let new = make_def("BTCUSDT", true, 200);
        let event = RefDataEvent::Updated {
            old,
            new,
            status: None,
        };
        assert!(event.trading_status().is_none());
        assert!(!event.trading_changed());
    }

    // --- business_fields_differ ---

    #[test]
    fn detects_is_trading_change() {
        let a = make_def("X", true, 100);
        let b = make_def("X", false, 100);
        assert!(business_fields_differ(&a, &b));
    }

    #[test]
    fn detects_tick_size_change() {
        let a = make_def("X", true, 100);
        let b = make_def("X", true, 200);
        assert!(business_fields_differ(&a, &b));
    }

    #[test]
    fn ignores_header_difference() {
        let mut a = make_def("X", true, 100);
        let b = make_def("X", true, 100);
        a.header.sequence_number = 999; // header-only diff
        assert!(!business_fields_differ(&a, &b));
    }

    #[test]
    fn equal_defs_return_false() {
        let a = make_def("X", true, 100);
        let b = make_def("X", true, 100);
        assert!(!business_fields_differ(&a, &b));
    }

    // --- make_trading_status ---

    #[test]
    fn trading_status_inherits_venue_from_def_header() {
        let def = make_def("BTCUSDT", true, 100);
        let ts = make_trading_status(&def, 7);
        assert_eq!(ts.header.venue_id, VenueId::BinanceSpot);
        assert_eq!(ts.header.market_type, MarketType::Spot);
        assert_eq!(ts.header.instance_id, 1);
        assert_eq!(ts.header.instrument_id, 42);
        assert_eq!(ts.header.sequence_number, 7);
        assert_eq!(ts.header.message_type, MessageType::TradingStatus);
        assert_eq!(ts.symbol, "BTCUSDT");
        assert!(ts.is_trading);
    }
}
