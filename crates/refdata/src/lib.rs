mod client;
mod error;
mod event;
mod normalizer;
mod open_interest;
mod publisher;
mod registry;

pub use client::RestClient;
pub use error::RefDataError;
pub use event::RefDataEvent;
pub use normalizer::{derive_scale, parse_depth_snapshot, parse_exchange_info, parse_scaled, symbol_instrument_id};
pub use open_interest::OpenInterestPoller;
pub use publisher::{RefDataPublishError, RefDataPublisher};
pub use registry::InstrumentRegistry;

use connector_core::{InstrumentDefinition, MarketType, VenueId};
use std::time::Duration;
use tokio::sync::watch;
use tracing::{info, warn};

// ---------------------------------------------------------------------------
// RefDataService
// ---------------------------------------------------------------------------

/// Orchestrates periodic refresh of instrument definitions and status changes.
///
/// On each refresh it calls [`RestClient::fetch_exchange_info`], applies the
/// batch to the [`InstrumentRegistry`], and returns every [`RefDataEvent`]
/// produced by the batch.  Callers inspect the event to decide whether to
/// publish an [`InstrumentDefinition`] message, a [`TradingStatus`] message,
/// or both.
///
/// [`InstrumentDefinition`]: connector_core::InstrumentDefinition
/// [`TradingStatus`]:        connector_core::TradingStatus
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
            registry:         InstrumentRegistry::new(),
            refresh_interval,
            venue_id,
            market_type,
            instance_id,
            next_seq:         0,
        }
    }

    /// Fetch once and apply to the registry.
    ///
    /// Returns every [`RefDataEvent`] produced — one per new or changed symbol.
    pub async fn refresh(&mut self) -> Result<Vec<RefDataEvent>, RefDataError> {
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
    /// The initial fetch runs immediately; subsequent fetches run on
    /// `refresh_interval`.  Every [`RefDataEvent`] is passed to `on_event`.
    pub async fn run(
        &mut self,
        mut shutdown: watch::Receiver<bool>,
        mut on_event: impl FnMut(RefDataEvent),
    ) -> Result<(), RefDataError> {
        for event in self.refresh().await? {
            on_event(event);
        }

        let mut ticker = tokio::time::interval(self.refresh_interval);
        ticker.tick().await; // consume the immediate first tick

        loop {
            tokio::select! {
                _ = ticker.tick() => {
                    match self.refresh().await {
                        Ok(events) => {
                            for event in events {
                                on_event(event);
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
