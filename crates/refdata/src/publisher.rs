use connector_aeron::{Publication, PublisherError, ShardedPublisher};
use connector_core::Error as CoreError;
use thiserror::Error;

use crate::event::RefDataEvent;

// ---------------------------------------------------------------------------
// Error
// ---------------------------------------------------------------------------

#[derive(Debug, Error)]
pub enum RefDataPublishError {
    #[error("codec error: {0}")]
    Encode(#[from] CoreError),
    #[error("aeron error: {0}")]
    Aeron(#[from] PublisherError),
}

// ---------------------------------------------------------------------------
// RefDataPublisher
// ---------------------------------------------------------------------------

/// Serialises [`RefDataEvent`]s and offers them to a [`ShardedPublisher`].
///
/// Reference data (instrument definitions, trading status) must be known by
/// every consumer regardless of which market-data shard it subscribes to.
/// [`broadcast`] therefore sends each event to all registered shards.
///
/// # Message ordering
///
/// For each shard, `InstrumentDefinition` is always offered before
/// `TradingStatus` so a consumer that processes messages in order can index
/// the definition before it handles the status event.
///
/// [`broadcast`]: RefDataPublisher::broadcast
pub struct RefDataPublisher<P> {
    publisher: ShardedPublisher<P>,
    buf: Vec<u8>,
}

impl<P: Publication> RefDataPublisher<P> {
    pub fn new(publisher: ShardedPublisher<P>) -> Self {
        Self {
            publisher,
            buf: vec![0u8; 4096],
        }
    }

    /// Offer `event` to every registered shard (broadcast semantics).
    ///
    /// Always publishes the `InstrumentDefinition`; additionally publishes
    /// `TradingStatus` when `event.trading_status()` is `Some`.
    ///
    /// Returns the total number of Aeron offers made across all shards.
    /// Returns `Err` on the first encode or publish failure.
    pub fn broadcast(&mut self, event: &RefDataEvent) -> Result<u32, RefDataPublishError> {
        let shard_ids: Vec<u32> = self.publisher.shard_ids().collect();
        let mut total = 0u32;
        for shard_id in shard_ids {
            total += self.offer_to_shard(shard_id, event)?;
        }
        Ok(total)
    }

    /// Offer `event` to a single shard.
    ///
    /// Returns the number of Aeron offers made (1 when no `TradingStatus`, 2
    /// when `is_trading` changed or the symbol is new).
    pub fn offer_to_shard(
        &mut self,
        shard_id: u32,
        event: &RefDataEvent,
    ) -> Result<u32, RefDataPublishError> {
        let def = event.definition();
        let n = def.encode_into(&mut self.buf)?;
        self.publisher.offer(shard_id, &self.buf[..n])?;
        let mut count = 1u32;

        if let Some(ts) = event.trading_status() {
            let n = ts.encode_into(&mut self.buf)?;
            self.publisher.offer(shard_id, &self.buf[..n])?;
            count += 1;
        }
        Ok(count)
    }

    /// Number of shards this publisher owns.
    pub fn shard_count(&self) -> usize {
        self.publisher.shard_count()
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use connector_aeron::build_channel;
    use connector_core::{
        InstrumentDefinition, MarketType, MessageHeader, MessageType, TradingStatus, VenueId,
        SCHEMA_VERSION, TS_NONE,
    };

    use crate::event::{make_trading_status, RefDataEvent};

    fn make_def(symbol: &str, is_trading: bool) -> InstrumentDefinition {
        InstrumentDefinition {
            header: MessageHeader {
                schema_version: SCHEMA_VERSION,
                message_type: MessageType::InstrumentDefinition,
                venue_id: VenueId::BinanceSpot,
                market_type: MarketType::Spot,
                instrument_id: 1,
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
            tick_size: 100,
            step_size: 10,
            min_qty: 10,
            min_notional: 10_000,
            contract_size: 0,
            is_trading,
        }
    }

    fn added_event(symbol: &str, is_trading: bool) -> RefDataEvent {
        let def = make_def(symbol, is_trading);
        let status = make_trading_status(&def, 0);
        RefDataEvent::Added { def, status }
    }

    fn updated_no_status(symbol: &str) -> RefDataEvent {
        let old = make_def(symbol, true);
        let mut new = make_def(symbol, true);
        new.tick_size = 200;
        RefDataEvent::Updated {
            old,
            new,
            status: None,
        }
    }

    fn updated_with_status(symbol: &str) -> RefDataEvent {
        let old = make_def(symbol, true);
        let new = make_def(symbol, false);
        let ts = make_trading_status(&new, 1);
        RefDataEvent::Updated {
            old,
            new,
            status: Some(ts),
        }
    }

    // --- message counts ---

    #[test]
    fn added_event_publishes_two_messages_per_shard() {
        let (pub_, _rxs) = build_channel(&[0, 1], 16);
        let mut rp = RefDataPublisher::new(pub_);
        let count = rp.broadcast(&added_event("BTCUSDT", true)).unwrap();
        // 2 shards × 2 messages (def + status)
        assert_eq!(count, 4);
    }

    #[test]
    fn updated_without_status_publishes_one_message_per_shard() {
        let (pub_, _rxs) = build_channel(&[0, 1], 16);
        let mut rp = RefDataPublisher::new(pub_);
        let count = rp.broadcast(&updated_no_status("ETHUSDT")).unwrap();
        // 2 shards × 1 message (def only)
        assert_eq!(count, 2);
    }

    #[test]
    fn updated_with_status_publishes_two_messages_per_shard() {
        let (pub_, _rxs) = build_channel(&[0, 1], 16);
        let mut rp = RefDataPublisher::new(pub_);
        let count = rp.broadcast(&updated_with_status("SOLUSDT")).unwrap();
        assert_eq!(count, 4);
    }

    // --- offer_to_shard routing ---

    #[test]
    fn offer_to_shard_only_writes_to_that_shard() {
        let (pub_, mut rxs) = build_channel(&[0, 1], 16);
        let mut rp = RefDataPublisher::new(pub_);
        rp.offer_to_shard(0, &added_event("BTCUSDT", true)).unwrap();

        // Shard 0 has messages, shard 1 is empty
        assert!(rxs.get_mut(&0).unwrap().try_recv().is_ok());
        assert!(rxs.get_mut(&1).unwrap().try_recv().is_err());
    }

    #[test]
    fn offer_to_shard_returns_two_for_added() {
        let (pub_, _rxs) = build_channel(&[0], 8);
        let mut rp = RefDataPublisher::new(pub_);
        let n = rp.offer_to_shard(0, &added_event("BTCUSDT", true)).unwrap();
        assert_eq!(n, 2);
    }

    #[test]
    fn offer_to_shard_returns_one_for_updated_without_status() {
        let (pub_, _rxs) = build_channel(&[0], 8);
        let mut rp = RefDataPublisher::new(pub_);
        let n = rp.offer_to_shard(0, &updated_no_status("BTCUSDT")).unwrap();
        assert_eq!(n, 1);
    }

    // --- round-trip integrity ---

    #[test]
    fn instrument_definition_round_trips_through_channel() {
        let (pub_, mut rxs) = build_channel(&[0], 8);
        let mut rp = RefDataPublisher::new(pub_);
        let event = added_event("BTCUSDT", true);
        rp.offer_to_shard(0, &event).unwrap();

        let rx = rxs.get_mut(&0).unwrap();
        let def_bytes = rx.try_recv().unwrap();
        let decoded = InstrumentDefinition::decode(&def_bytes).unwrap();
        assert_eq!(decoded.symbol, "BTCUSDT");
        assert_eq!(decoded.price_scale, 2);
        assert_eq!(decoded.tick_size, 100);
        assert!(decoded.is_trading);
    }

    #[test]
    fn trading_status_round_trips_through_channel() {
        let (pub_, mut rxs) = build_channel(&[0], 8);
        let mut rp = RefDataPublisher::new(pub_);
        let event = added_event("BTCUSDT", true);
        rp.offer_to_shard(0, &event).unwrap();

        let rx = rxs.get_mut(&0).unwrap();
        rx.try_recv().unwrap(); // consume InstrumentDefinition
        let ts_bytes = rx.try_recv().unwrap();
        let decoded = TradingStatus::decode(&ts_bytes).unwrap();
        assert_eq!(decoded.symbol, "BTCUSDT");
        assert!(decoded.is_trading);
        assert_eq!(decoded.header.venue_id, VenueId::BinanceSpot);
    }

    #[test]
    fn definition_published_before_status() {
        let (pub_, mut rxs) = build_channel(&[0], 8);
        let mut rp = RefDataPublisher::new(pub_);
        let event = added_event("ETHUSDT", false);
        rp.offer_to_shard(0, &event).unwrap();

        let rx = rxs.get_mut(&0).unwrap();
        // First message must be InstrumentDefinition
        let first = rx.try_recv().unwrap();
        let def = InstrumentDefinition::decode(&first).unwrap();
        assert_eq!(def.symbol, "ETHUSDT");

        // Second message must be TradingStatus
        let second = rx.try_recv().unwrap();
        let ts = TradingStatus::decode(&second).unwrap();
        assert_eq!(ts.symbol, "ETHUSDT");
        assert!(!ts.is_trading);
    }

    // --- shard_count ---

    #[test]
    fn shard_count_reflects_registered_shards() {
        let (pub_, _rxs) = build_channel(&[0, 1, 2], 4);
        let rp = RefDataPublisher::new(pub_);
        assert_eq!(rp.shard_count(), 3);
    }
}
