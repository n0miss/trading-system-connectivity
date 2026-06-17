use connector_core::{InstrumentDefinition, MarketType, VenueId};
use reqwest::Client;
use tracing::debug;

use crate::{
    error::RefDataError,
    normalizer::parse_exchange_info,
};

/// REST client for fetching Binance exchange info.
pub struct RestClient {
    http:     Client,
    base_url: String,
}

impl RestClient {
    pub fn new(base_url: impl Into<String>) -> Self {
        Self {
            http:     Client::new(),
            base_url: base_url.into(),
        }
    }

    /// Fetch and normalize exchangeInfo for the given venue and market type.
    pub async fn fetch_exchange_info(
        &self,
        venue_id:    VenueId,
        market_type: MarketType,
        instance_id: u32,
        first_seq:   u64,
    ) -> Result<Vec<InstrumentDefinition>, RefDataError> {
        let path = exchange_info_path(venue_id, market_type);
        let url  = format!("{}{}", self.base_url, path);

        debug!(%url, "fetching exchange info");

        let bytes = self.http
            .get(&url)
            .send()
            .await?
            .error_for_status()?
            .bytes()
            .await?;

        parse_exchange_info(&bytes, venue_id, market_type, instance_id, first_seq)
    }
}

fn exchange_info_path(venue_id: VenueId, market_type: MarketType) -> &'static str {
    match (venue_id, market_type) {
        (VenueId::BinanceSpot,    MarketType::Spot)        => "/api/v3/exchangeInfo",
        (VenueId::BinanceFutures, MarketType::UsdmFutures) => "/fapi/v1/exchangeInfo",
        _ => "/api/v3/exchangeInfo",
    }
}
