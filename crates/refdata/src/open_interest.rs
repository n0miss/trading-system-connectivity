use std::time::Duration;

use connector_core::{InstrumentDefinition, OpenInterest};
use tokio::sync::watch;
use tokio::time::MissedTickBehavior;
use tracing::{info, warn};

use crate::{
    client::RestClient,
    error::RefDataError,
    normalizer::normalize_open_interest,
};

/// Periodically polls `/fapi/v1/openInterest` for a set of symbols and yields
/// normalised [`OpenInterest`] messages.
///
/// Symbols are polled sequentially on each tick. Individual request failures are
/// logged and skipped so one bad symbol does not block the rest.
pub struct OpenInterestPoller {
    client:   RestClient,
    symbols:  Vec<InstrumentDefinition>,
    interval: Duration,
    next_seq: u64,
}

impl OpenInterestPoller {
    pub fn new(
        client:   RestClient,
        symbols:  Vec<InstrumentDefinition>,
        interval: Duration,
    ) -> Self {
        Self { client, symbols, interval, next_seq: 0 }
    }

    /// Run the polling loop until the shutdown signal fires.
    ///
    /// Polls all symbols immediately on entry, then repeats every `interval`.
    /// Each message is passed to `on_msg`; errors are logged and do not stop
    /// the loop.
    pub async fn run(
        &mut self,
        mut shutdown: watch::Receiver<bool>,
        mut on_msg:   impl FnMut(OpenInterest),
    ) -> Result<(), RefDataError> {
        info!(
            symbols       = self.symbols.len(),
            interval_secs = self.interval.as_secs(),
            "open interest poller starting"
        );

        // Poll immediately on startup.
        self.poll_all(&mut on_msg).await;

        let mut ticker = tokio::time::interval(self.interval);
        ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);
        ticker.tick().await; // consume the immediate first tick

        loop {
            tokio::select! {
                _ = ticker.tick() => {
                    self.poll_all(&mut on_msg).await;
                }
                _ = shutdown.changed() => {
                    info!("open interest poller shutting down");
                    break;
                }
            }
        }
        Ok(())
    }

    async fn poll_all(&mut self, on_msg: &mut impl FnMut(OpenInterest)) {
        // Use index-based iteration: cloning the symbol string before the .await
        // avoids holding a borrow of self.symbols across the suspension point,
        // which would conflict with the mutable borrow of self.next_seq after.
        for i in 0..self.symbols.len() {
            let symbol = self.symbols[i].symbol.clone();
            let result = self.client.fetch_futures_open_interest(&symbol).await;
            match result {
                Ok(resp) => {
                    match normalize_open_interest(&resp, &self.symbols[i], self.next_seq) {
                        Ok(msg) => {
                            self.next_seq += 1;
                            on_msg(msg);
                        }
                        Err(e) => warn!(
                            %symbol,
                            error = %e,
                            "open interest normalize failed"
                        ),
                    }
                }
                Err(e) => warn!(
                    %symbol,
                    error = %e,
                    "open interest fetch failed"
                ),
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::normalizer::{normalize_open_interest, OpenInterestResponse};
    use connector_core::{MarketType, MessageHeader, MessageType, VenueId, SCHEMA_VERSION, TS_NONE};

    fn btcusdt_def() -> InstrumentDefinition {
        InstrumentDefinition {
            header: MessageHeader {
                schema_version:    SCHEMA_VERSION,
                message_type:      MessageType::InstrumentDefinition,
                venue_id:          VenueId::BinanceFutures,
                market_type:       MarketType::UsdmFutures,
                instrument_id:     42,
                connection_id:     0,
                instance_id:       0,
                sequence_number:   0,
                exchange_event_ts: TS_NONE,
                exchange_tx_ts:    TS_NONE,
                local_recv_ts:     TS_NONE,
                local_publish_ts:  TS_NONE,
            },
            symbol:        "BTCUSDT".into(),
            base_asset:    "BTC".into(),
            quote_asset:   "USDT".into(),
            price_scale:   2,
            qty_scale:     3,
            tick_size:     10,
            step_size:     1,
            min_qty:       1,
            min_notional:  10_000,
            contract_size: 1,
            is_trading:    true,
        }
    }

    #[test]
    fn normalize_scales_open_interest_by_qty_scale() {
        let inst = btcusdt_def(); // qty_scale = 3
        let resp = OpenInterestResponse {
            symbol:        "BTCUSDT".into(),
            open_interest: "12345.678".into(),
            time_ms:       1_700_000_000_000,
        };
        let msg = normalize_open_interest(&resp, &inst, 0).unwrap();
        // 12345.678 * 10^3 = 12_345_678
        assert_eq!(msg.open_interest, 12_345_678);
        assert_eq!(msg.symbol, "BTCUSDT");
        assert_eq!(msg.header.sequence_number, 0);
    }

    #[test]
    fn normalize_converts_time_ms_to_ns() {
        let inst = btcusdt_def();
        let resp = OpenInterestResponse {
            symbol:        "BTCUSDT".into(),
            open_interest: "1.000".into(),
            time_ms:       1_700_000_000_000,
        };
        let msg = normalize_open_interest(&resp, &inst, 0).unwrap();
        assert_eq!(msg.header.exchange_event_ts, 1_700_000_000_000 * 1_000_000);
    }

    #[test]
    fn normalize_inherits_header_fields_from_instrument() {
        let inst = btcusdt_def();
        let resp = OpenInterestResponse {
            symbol:        "BTCUSDT".into(),
            open_interest: "1.000".into(),
            time_ms:       0,
        };
        let msg = normalize_open_interest(&resp, &inst, 7).unwrap();
        let hdr = &msg.header;
        assert_eq!(hdr.venue_id,          VenueId::BinanceFutures);
        assert_eq!(hdr.market_type,       MarketType::UsdmFutures);
        assert_eq!(hdr.instrument_id,     42);
        assert_eq!(hdr.instance_id,       0);
        assert_eq!(hdr.message_type,      MessageType::OpenInterest);
        assert_eq!(hdr.connection_id,     0);
        assert_eq!(hdr.sequence_number,   7);
    }

    #[test]
    fn normalize_invalid_decimal_returns_error() {
        let inst = btcusdt_def();
        let resp = OpenInterestResponse {
            symbol:        "BTCUSDT".into(),
            open_interest: "not_a_number".into(),
            time_ms:       0,
        };
        assert!(normalize_open_interest(&resp, &inst, 0).is_err());
    }

    #[test]
    fn poller_new_starts_at_seq_zero() {
        let client = RestClient::new("https://fapi.binance.com");
        let poller = OpenInterestPoller::new(client, vec![], Duration::from_secs(60));
        assert_eq!(poller.next_seq, 0);
        assert!(poller.symbols.is_empty());
    }
}
