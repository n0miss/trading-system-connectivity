mod client;
mod error;
mod normalizer;
mod registry;

pub use client::RestClient;
pub use error::RefDataError;
pub use normalizer::{derive_scale, parse_exchange_info, parse_scaled, symbol_instrument_id};
pub use registry::InstrumentRegistry;

use connector_core::{InstrumentDefinition, MarketType, TradingStatus, VenueId};
use std::time::Duration;
use tokio::sync::watch;
use tracing::{info, warn};

// ---------------------------------------------------------------------------
// RefDataService
// ---------------------------------------------------------------------------

/// Orchestrates periodic refresh of instrument definitions and status changes.
///
/// On each refresh it calls `RestClient::fetch_exchange_info`, applies the batch
/// to the `InstrumentRegistry`, and returns any `TradingStatus` events produced
/// by status changes.  The caller is responsible for publishing those events.
pub struct RefDataService {
    client:           RestClient,
    pub registry:     InstrumentRegistry,
    refresh_interval: Duration,
    venue_id:         VenueId,
    market_type:      MarketType,
    instance_id:      u32,
    next_seq:         u64,
}

impl RefDataService {
    pub fn new(
        base_url:         impl Into<String>,
        venue_id:         VenueId,
        market_type:      MarketType,
        instance_id:      u32,
        refresh_interval: Duration,
    ) -> Self {
        Self {
            client:           RestClient::new(base_url),
            registry:         InstrumentRegistry::new(venue_id, market_type, instance_id),
            refresh_interval,
            venue_id,
            market_type,
            instance_id,
            next_seq:         0,
        }
    }

    /// Fetch once and apply to the registry. Returns any status-change events.
    pub async fn refresh(&mut self) -> Result<Vec<TradingStatus>, RefDataError> {
        let seq  = self.next_seq;
        let defs = self.client
            .fetch_exchange_info(self.venue_id, self.market_type, self.instance_id, seq)
            .await?;
        self.next_seq += defs.len() as u64;
        info!(count = defs.len(), "exchange info refreshed");
        Ok(self.registry.apply_batch(defs))
    }

    /// Run the refresh loop until the shutdown signal fires.
    ///
    /// The initial fetch runs immediately; subsequent fetches run on `refresh_interval`.
    /// Any `TradingStatus` changes are passed to `on_status_change`.
    pub async fn run(
        &mut self,
        mut shutdown: watch::Receiver<bool>,
        mut on_status_change: impl FnMut(TradingStatus),
    ) -> Result<(), RefDataError> {
        // Initial fetch
        for status in self.refresh().await? {
            on_status_change(status);
        }

        let mut ticker = tokio::time::interval(self.refresh_interval);
        ticker.tick().await; // consume the immediate first tick

        loop {
            tokio::select! {
                _ = ticker.tick() => {
                    match self.refresh().await {
                        Ok(changes) => {
                            for status in changes {
                                on_status_change(status);
                            }
                        }
                        Err(e) => warn!("exchange info refresh failed: {e}"),
                    }
                }
                _ = shutdown.changed() => {
                    info!("refdata service shutting down");
                    break;
                }
            }
        }
        Ok(())
    }

    /// Convenience: get a definition from the registry.
    pub fn get(&self, symbol: &str) -> Option<&InstrumentDefinition> {
        self.registry.get(symbol)
    }
}
