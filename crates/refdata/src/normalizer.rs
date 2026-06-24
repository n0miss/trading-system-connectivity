use connector_core::{
    BookSnapshot, InstrumentDefinition, MarketType, MessageHeader, MessageType, PriceLevel,
    VenueId, SCHEMA_VERSION, TS_NONE,
};
use serde::Deserialize;

use crate::error::RefDataError;

// ---------------------------------------------------------------------------
// Serde types for Binance exchange info JSON
// ---------------------------------------------------------------------------

#[derive(Deserialize)]
pub(crate) struct ExchangeInfoResponse {
    pub symbols: Vec<SymbolInfo>,
}

#[derive(Deserialize)]
pub(crate) struct SymbolInfo {
    pub symbol: String,
    pub status: String,
    #[serde(rename = "baseAsset")]
    pub base_asset: String,
    #[serde(rename = "quoteAsset")]
    pub quote_asset: String,
    #[serde(default)]
    pub filters: Vec<RawFilter>,
}

/// Flat representation of any Binance filter object.
/// Unknown fields are ignored by serde; missing optional fields default to None.
#[derive(Deserialize, Default)]
pub(crate) struct RawFilter {
    #[serde(rename = "filterType", default)]
    pub filter_type: String,
    #[serde(rename = "tickSize")]
    pub tick_size: Option<String>,
    #[serde(rename = "stepSize")]
    pub step_size: Option<String>,
    #[serde(rename = "minQty")]
    pub min_qty: Option<String>,
    /// Spot NOTIONAL filter uses "minNotional"; some futures also use it.
    #[serde(rename = "minNotional")]
    pub min_notional: Option<String>,
    /// Futures MIN_NOTIONAL filter uses "notional".
    #[serde(rename = "notional")]
    pub notional: Option<String>,
}

// ---------------------------------------------------------------------------
// Scale utilities
// ---------------------------------------------------------------------------

/// Counts the number of decimal places in a numeric string.
///
/// "0.01000000" → 8,  "0.001" → 3,  "1" → 0,  "100" → 0
pub fn derive_scale(s: &str) -> u32 {
    match s.find('.') {
        Some(dot) => (s.len() - dot - 1) as u32,
        None => 0,
    }
}

/// Converts a decimal string to a scaled i64 without using floating point.
///
/// `parse_scaled("0.01000000", 8)` → 1_000_000
/// `parse_scaled("0.001", 8)`      → 100_000  (padded to 8 decimal places)
/// `parse_scaled("5", 8)`          → 500_000_000
pub fn parse_scaled(s: &str, scale: u32) -> Result<i64, RefDataError> {
    let s = s.trim();
    let err = |v: &str| RefDataError::InvalidNumeric {
        value: v.to_owned(),
        field: "numeric",
    };

    let (int_str, frac_str) = match s.find('.') {
        Some(dot) => (&s[..dot], &s[dot + 1..]),
        None => (s, ""),
    };

    let int_part: i64 = int_str.parse().map_err(|_| err(s))?;

    // Adjust fractional part to exactly `scale` decimal places.
    let mut frac = frac_str.to_string();
    frac.truncate(scale as usize);
    while frac.len() < scale as usize {
        frac.push('0');
    }
    let frac_part: i64 = if frac.is_empty() {
        0
    } else {
        frac.parse().map_err(|_| err(s))?
    };

    let multiplier = 10_i64.pow(scale);
    Ok(int_part * multiplier + frac_part)
}

// ---------------------------------------------------------------------------
// Instrument id
// ---------------------------------------------------------------------------

/// Deterministic FNV-1a 32-bit hash of the symbol string used as instrument_id.
/// Stable across restarts and consistent with the sharding hash in §4.2.
pub fn symbol_instrument_id(symbol: &str) -> u32 {
    const BASIS: u32 = 2_166_136_261;
    const PRIME: u32 = 16_777_619;
    let mut h = BASIS;
    for b in symbol.bytes() {
        h ^= b as u32;
        h = h.wrapping_mul(PRIME);
    }
    h
}

// ---------------------------------------------------------------------------
// Normalization
// ---------------------------------------------------------------------------

fn now_nanos() -> i64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos() as i64
}

fn make_header(
    message_type: MessageType,
    venue_id: VenueId,
    market_type: MarketType,
    instrument_id: u32,
    instance_id: u32,
    seq: u64,
) -> MessageHeader {
    let ts = now_nanos();
    MessageHeader {
        schema_version: SCHEMA_VERSION,
        message_type,
        venue_id,
        market_type,
        instrument_id,
        connection_id: 0,
        instance_id,
        sequence_number: seq,
        exchange_event_ts: TS_NONE,
        exchange_tx_ts: TS_NONE,
        local_recv_ts: ts,
        local_publish_ts: ts,
    }
}

/// Normalise one `SymbolInfo` into an `InstrumentDefinition`.
///
/// Returns `None` if the symbol is not currently in a tradeable state and
/// has never been seen before (callers can choose to skip or include it).
pub fn normalize_symbol(
    info: &SymbolInfo,
    venue_id: VenueId,
    market_type: MarketType,
    instance_id: u32,
    seq: u64,
) -> Result<InstrumentDefinition, RefDataError> {
    let symbol = &info.symbol;

    // --- extract filters -------------------------------------------------
    let price_filter = info
        .filters
        .iter()
        .find(|f| f.filter_type == "PRICE_FILTER");
    let lot_filter = info.filters.iter().find(|f| f.filter_type == "LOT_SIZE");

    let tick_size_str = price_filter
        .and_then(|f| f.tick_size.as_deref())
        .unwrap_or("0.00000001");

    let step_size_str = lot_filter
        .and_then(|f| f.step_size.as_deref())
        .unwrap_or("0.00000001");

    let min_qty_str = lot_filter.and_then(|f| f.min_qty.as_deref()).unwrap_or("0");

    // min_notional: Spot uses NOTIONAL/minNotional, Futures uses MIN_NOTIONAL/notional
    let min_notional_str = info
        .filters
        .iter()
        .find(|f| f.filter_type == "NOTIONAL" || f.filter_type == "MIN_NOTIONAL")
        .and_then(|f| f.min_notional.as_deref().or(f.notional.as_deref()))
        .unwrap_or("0");

    // --- derive scales ---------------------------------------------------
    let price_scale = derive_scale(tick_size_str);
    let qty_scale = derive_scale(step_size_str);

    // --- convert to scaled integers (no floats) --------------------------
    let tick_size = parse_scaled(tick_size_str, price_scale)?;
    let step_size = parse_scaled(step_size_str, qty_scale)?;
    let min_qty = parse_scaled(min_qty_str, qty_scale)?;
    let min_notional = parse_scaled(min_notional_str, price_scale)?;

    let contract_size = match market_type {
        MarketType::UsdmFutures => 1,
        MarketType::Spot => 0,
    };

    let is_trading = info.status == "TRADING";
    let instr_id = symbol_instrument_id(symbol);

    Ok(InstrumentDefinition {
        header: make_header(
            MessageType::InstrumentDefinition,
            venue_id,
            market_type,
            instr_id,
            instance_id,
            seq,
        ),
        symbol: symbol.clone(),
        base_asset: info.base_asset.clone(),
        quote_asset: info.quote_asset.clone(),
        price_scale,
        qty_scale,
        tick_size,
        step_size,
        min_qty,
        min_notional,
        contract_size,
        is_trading,
    })
}

/// Parse a Binance `exchangeInfo` JSON response and return all normalized definitions.
pub fn parse_exchange_info(
    json: &[u8],
    venue_id: VenueId,
    market_type: MarketType,
    instance_id: u32,
    first_seq: u64,
) -> Result<Vec<InstrumentDefinition>, RefDataError> {
    let resp: ExchangeInfoResponse = serde_json::from_slice(json)?;
    let mut out = Vec::with_capacity(resp.symbols.len());
    for (i, sym) in resp.symbols.iter().enumerate() {
        out.push(normalize_symbol(
            sym,
            venue_id,
            market_type,
            instance_id,
            first_seq + i as u64,
        )?);
    }
    Ok(out)
}

// ---------------------------------------------------------------------------
// Open interest
// ---------------------------------------------------------------------------

/// Response from `GET /fapi/v1/openInterest?symbol=X`.
#[derive(serde::Deserialize)]
pub struct OpenInterestResponse {
    pub symbol: String,
    #[serde(rename = "openInterest")]
    pub open_interest: String,
    /// Exchange timestamp in milliseconds.
    #[serde(rename = "time")]
    pub time_ms: i64,
}

/// Build a normalised [`connector_core::OpenInterest`] from a REST response.
///
/// `open_interest` is scaled by `inst.qty_scale`.
/// `exchange_event_ts` is converted from milliseconds to nanoseconds.
pub fn normalize_open_interest(
    resp: &OpenInterestResponse,
    inst: &InstrumentDefinition,
    seq: u64,
) -> Result<connector_core::OpenInterest, RefDataError> {
    use connector_core::{MessageType, OpenInterest, SCHEMA_VERSION, TS_NONE};

    let open_interest = parse_scaled(&resp.open_interest, inst.qty_scale)?;
    let exchange_event_ts = resp.time_ms * 1_000_000; // ms → ns
    let ts = now_nanos();
    Ok(OpenInterest {
        header: MessageHeader {
            schema_version: SCHEMA_VERSION,
            message_type: MessageType::OpenInterest,
            venue_id: inst.header.venue_id,
            market_type: inst.header.market_type,
            instrument_id: inst.header.instrument_id,
            connection_id: 0,
            instance_id: inst.header.instance_id,
            sequence_number: seq,
            exchange_event_ts,
            exchange_tx_ts: TS_NONE,
            local_recv_ts: ts,
            local_publish_ts: ts,
        },
        symbol: inst.symbol.clone(),
        qty_scale: inst.qty_scale as u8,
        open_interest,
    })
}

// ---------------------------------------------------------------------------
// Depth snapshot parsing
// ---------------------------------------------------------------------------

#[derive(serde::Deserialize)]
pub(crate) struct DepthSnapshotResponse {
    #[serde(rename = "lastUpdateId")]
    pub last_update_id: u64,
    pub bids: Vec<[String; 2]>,
    pub asks: Vec<[String; 2]>,
}

/// Parse a Binance `/api/v3/depth` JSON response into a [`BookSnapshot`].
///
/// `recv_ts` is the nanosecond timestamp when the HTTP response was received.
pub fn parse_depth_snapshot(
    json: &[u8],
    inst: &InstrumentDefinition,
    recv_ts: i64,
) -> Result<BookSnapshot, RefDataError> {
    let resp: DepthSnapshotResponse = serde_json::from_slice(json)?;

    let header = MessageHeader {
        schema_version: SCHEMA_VERSION,
        message_type: MessageType::BookSnapshot,
        venue_id: inst.header.venue_id,
        market_type: inst.header.market_type,
        instrument_id: inst.header.instrument_id,
        connection_id: 0,
        instance_id: inst.header.instance_id,
        sequence_number: 0,
        exchange_event_ts: TS_NONE,
        exchange_tx_ts: TS_NONE,
        local_recv_ts: recv_ts,
        local_publish_ts: recv_ts,
    };

    let bids = parse_price_levels(&resp.bids, inst.price_scale, inst.qty_scale)?;
    let asks = parse_price_levels(&resp.asks, inst.price_scale, inst.qty_scale)?;

    Ok(BookSnapshot {
        header,
        symbol: inst.symbol.clone(),
        price_scale: inst.price_scale as u8,
        qty_scale: inst.qty_scale as u8,
        update_id: resp.last_update_id,
        bids,
        asks,
    })
}

fn parse_price_levels(
    levels: &[[String; 2]],
    price_scale: u32,
    qty_scale: u32,
) -> Result<Vec<PriceLevel>, RefDataError> {
    levels
        .iter()
        .map(|[price_str, qty_str]| {
            let price = parse_scaled(price_str, price_scale)?;
            let qty = parse_scaled(qty_str, qty_scale)?;
            Ok(PriceLevel { price, qty })
        })
        .collect()
}

// ---------------------------------------------------------------------------
// Test fixture — shared with registry tests
// ---------------------------------------------------------------------------

#[cfg(test)]
pub(crate) const SPOT_JSON_FOR_TESTS: &[u8] = br#"{
    "symbols": [
        {
            "symbol": "BTCUSDT",
            "status": "TRADING",
            "baseAsset": "BTC",
            "quoteAsset": "USDT",
            "baseAssetPrecision": 8,
            "filters": [
                {"filterType": "PRICE_FILTER", "tickSize": "0.01000000"},
                {"filterType": "LOT_SIZE",  "minQty": "0.00001000", "stepSize": "0.00001000"},
                {"filterType": "NOTIONAL",  "minNotional": "5.00000000"}
            ]
        },
        {
            "symbol": "ETHUSDT",
            "status": "TRADING",
            "baseAsset": "ETH",
            "quoteAsset": "USDT",
            "baseAssetPrecision": 8,
            "filters": [
                {"filterType": "PRICE_FILTER", "tickSize": "0.01000000"},
                {"filterType": "LOT_SIZE",  "minQty": "0.00010000", "stepSize": "0.00010000"},
                {"filterType": "NOTIONAL",  "minNotional": "5.00000000"}
            ]
        },
        {
            "symbol": "XRPUSDT",
            "status": "BREAK",
            "baseAsset": "XRP",
            "quoteAsset": "USDT",
            "baseAssetPrecision": 8,
            "filters": [
                {"filterType": "PRICE_FILTER", "tickSize": "0.00010000"},
                {"filterType": "LOT_SIZE",  "minQty": "1.00000000", "stepSize": "1.00000000"},
                {"filterType": "NOTIONAL",  "minNotional": "1.00000000"}
            ]
        }
    ]
}"#;

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    // ---- scale utilities ------------------------------------------------

    #[test]
    fn derive_scale_eight_decimal_places() {
        assert_eq!(derive_scale("0.01000000"), 8);
        assert_eq!(derive_scale("0.00000001"), 8);
        assert_eq!(derive_scale("1.00000000"), 8);
    }

    #[test]
    fn derive_scale_fewer_places() {
        assert_eq!(derive_scale("0.001"), 3);
        assert_eq!(derive_scale("0.1"), 1);
        assert_eq!(derive_scale("0.10"), 2);
    }

    #[test]
    fn derive_scale_integer() {
        assert_eq!(derive_scale("1"), 0);
        assert_eq!(derive_scale("100"), 0);
    }

    #[test]
    fn parse_scaled_eight_places() {
        assert_eq!(parse_scaled("0.01000000", 8).unwrap(), 1_000_000);
        assert_eq!(parse_scaled("0.00000001", 8).unwrap(), 1);
        assert_eq!(parse_scaled("1.00000000", 8).unwrap(), 100_000_000);
        assert_eq!(
            parse_scaled("43000.00000000", 8).unwrap(),
            4_300_000_000_000
        );
    }

    #[test]
    fn parse_scaled_short_string_padded() {
        // "0.001" with scale=8: pad to "00100000" → 100_000
        assert_eq!(parse_scaled("0.001", 8).unwrap(), 100_000);
        // "5" with scale=8 → 500_000_000
        assert_eq!(parse_scaled("5", 8).unwrap(), 500_000_000);
    }

    #[test]
    fn parse_scaled_matches_own_scale() {
        // "0.001" with its own scale=3 → 1
        assert_eq!(parse_scaled("0.001", 3).unwrap(), 1);
        // "0.01000000" with scale=8 → 1_000_000
        assert_eq!(parse_scaled("0.01000000", 8).unwrap(), 1_000_000);
    }

    #[test]
    fn parse_scaled_zero() {
        assert_eq!(parse_scaled("0", 8).unwrap(), 0);
        assert_eq!(parse_scaled("0.00000000", 8).unwrap(), 0);
    }

    #[test]
    fn parse_scaled_integer_no_dot() {
        assert_eq!(parse_scaled("100", 0).unwrap(), 100);
    }

    #[test]
    fn parse_scaled_invalid_returns_error() {
        assert!(parse_scaled("abc", 8).is_err());
        assert!(parse_scaled("1.2.3", 8).is_err());
    }

    #[test]
    fn symbol_instrument_id_is_deterministic() {
        assert_eq!(
            symbol_instrument_id("BTCUSDT"),
            symbol_instrument_id("BTCUSDT")
        );
        assert_ne!(
            symbol_instrument_id("BTCUSDT"),
            symbol_instrument_id("ETHUSDT")
        );
    }

    // ---- Spot exchange info parsing -------------------------------------

    const SPOT_JSON: &[u8] = br#"{
        "timezone": "UTC",
        "serverTime": 1700000000000,
        "symbols": [
            {
                "symbol": "BTCUSDT",
                "status": "TRADING",
                "baseAsset": "BTC",
                "quoteAsset": "USDT",
                "baseAssetPrecision": 8,
                "quotePrecision": 8,
                "isSpotTradingAllowed": true,
                "filters": [
                    {
                        "filterType": "PRICE_FILTER",
                        "minPrice": "0.01000000",
                        "maxPrice": "1000000.00000000",
                        "tickSize": "0.01000000"
                    },
                    {
                        "filterType": "LOT_SIZE",
                        "minQty": "0.00001000",
                        "maxQty": "9000.00000000",
                        "stepSize": "0.00001000"
                    },
                    {
                        "filterType": "NOTIONAL",
                        "minNotional": "5.00000000",
                        "applyMinToMarket": true
                    }
                ]
            },
            {
                "symbol": "ETHUSDT",
                "status": "TRADING",
                "baseAsset": "ETH",
                "quoteAsset": "USDT",
                "baseAssetPrecision": 8,
                "quotePrecision": 8,
                "isSpotTradingAllowed": true,
                "filters": [
                    {
                        "filterType": "PRICE_FILTER",
                        "tickSize": "0.01000000"
                    },
                    {
                        "filterType": "LOT_SIZE",
                        "minQty": "0.00010000",
                        "stepSize": "0.00010000"
                    },
                    {
                        "filterType": "NOTIONAL",
                        "minNotional": "5.00000000"
                    }
                ]
            },
            {
                "symbol": "XRPUSDT",
                "status": "BREAK",
                "baseAsset": "XRP",
                "quoteAsset": "USDT",
                "baseAssetPrecision": 8,
                "quotePrecision": 8,
                "isSpotTradingAllowed": false,
                "filters": [
                    {
                        "filterType": "PRICE_FILTER",
                        "tickSize": "0.00010000"
                    },
                    {
                        "filterType": "LOT_SIZE",
                        "minQty": "1.00000000",
                        "stepSize": "1.00000000"
                    },
                    {
                        "filterType": "NOTIONAL",
                        "minNotional": "1.00000000"
                    }
                ]
            }
        ]
    }"#;

    #[test]
    fn parse_spot_exchange_info_count() {
        let defs =
            parse_exchange_info(SPOT_JSON, VenueId::BinanceSpot, MarketType::Spot, 1, 0).unwrap();
        assert_eq!(defs.len(), 3);
    }

    #[test]
    fn parse_spot_btcusdt_fields() {
        let defs =
            parse_exchange_info(SPOT_JSON, VenueId::BinanceSpot, MarketType::Spot, 1, 0).unwrap();
        let btc = defs.iter().find(|d| d.symbol == "BTCUSDT").unwrap();

        assert_eq!(btc.base_asset, "BTC");
        assert_eq!(btc.quote_asset, "USDT");
        assert_eq!(btc.price_scale, 8);
        assert_eq!(btc.qty_scale, 8);
        assert_eq!(btc.tick_size, 1_000_000); // 0.01 * 10^8
        assert_eq!(btc.step_size, 1_000); // 0.00001 * 10^8
        assert_eq!(btc.min_qty, 1_000); // 0.00001 * 10^8
        assert_eq!(btc.min_notional, 500_000_000); // 5 * 10^8
        assert_eq!(btc.contract_size, 0);
        assert!(btc.is_trading);
    }

    #[test]
    fn parse_spot_break_status_not_trading() {
        let defs =
            parse_exchange_info(SPOT_JSON, VenueId::BinanceSpot, MarketType::Spot, 1, 0).unwrap();
        let xrp = defs.iter().find(|d| d.symbol == "XRPUSDT").unwrap();
        assert!(!xrp.is_trading);
    }

    #[test]
    fn parse_spot_header_fields() {
        let defs =
            parse_exchange_info(SPOT_JSON, VenueId::BinanceSpot, MarketType::Spot, 7, 100).unwrap();
        let btc = defs.iter().find(|d| d.symbol == "BTCUSDT").unwrap();

        assert_eq!(btc.header.venue_id, VenueId::BinanceSpot);
        assert_eq!(btc.header.market_type, MarketType::Spot);
        assert_eq!(btc.header.message_type, MessageType::InstrumentDefinition);
        assert_eq!(btc.header.instrument_id, symbol_instrument_id("BTCUSDT"));
        assert_eq!(btc.header.instance_id, 7);
        // sequence numbers are assigned consecutively starting at first_seq
        assert!(btc.header.sequence_number >= 100);
    }

    // ---- Futures exchange info parsing ----------------------------------

    const FUTURES_JSON: &[u8] = br#"{
        "timezone": "UTC",
        "serverTime": 1700000000000,
        "symbols": [
            {
                "symbol": "BTCUSDT",
                "pair": "BTCUSDT",
                "contractType": "PERPETUAL",
                "status": "TRADING",
                "baseAsset": "BTC",
                "quoteAsset": "USDT",
                "marginAsset": "USDT",
                "pricePrecision": 2,
                "quantityPrecision": 3,
                "baseAssetPrecision": 8,
                "quotePrecision": 8,
                "filters": [
                    {
                        "filterType": "PRICE_FILTER",
                        "minPrice": "556.80",
                        "maxPrice": "4529764",
                        "tickSize": "0.10000000"
                    },
                    {
                        "filterType": "LOT_SIZE",
                        "minQty": "0.001",
                        "maxQty": "1000",
                        "stepSize": "0.001"
                    },
                    {
                        "filterType": "MIN_NOTIONAL",
                        "notional": "100"
                    }
                ]
            },
            {
                "symbol": "ETHUSDT",
                "pair": "ETHUSDT",
                "contractType": "PERPETUAL",
                "status": "TRADING",
                "baseAsset": "ETH",
                "quoteAsset": "USDT",
                "marginAsset": "USDT",
                "pricePrecision": 2,
                "quantityPrecision": 3,
                "baseAssetPrecision": 8,
                "quotePrecision": 8,
                "filters": [
                    {
                        "filterType": "PRICE_FILTER",
                        "tickSize": "0.01000000"
                    },
                    {
                        "filterType": "LOT_SIZE",
                        "minQty": "0.001",
                        "stepSize": "0.001"
                    },
                    {
                        "filterType": "MIN_NOTIONAL",
                        "notional": "5"
                    }
                ]
            }
        ]
    }"#;

    #[test]
    fn parse_futures_btcusdt_fields() {
        let defs = parse_exchange_info(
            FUTURES_JSON,
            VenueId::BinanceFutures,
            MarketType::UsdmFutures,
            1,
            0,
        )
        .unwrap();
        let btc = defs.iter().find(|d| d.symbol == "BTCUSDT").unwrap();

        assert_eq!(btc.base_asset, "BTC");
        assert_eq!(btc.quote_asset, "USDT");
        assert_eq!(btc.price_scale, 8); // "0.10000000" → 8 decimal places
        assert_eq!(btc.qty_scale, 3); // "0.001" → 3 decimal places
        assert_eq!(btc.tick_size, 10_000_000); // 0.1 * 10^8
        assert_eq!(btc.step_size, 1); // 0.001 * 10^3
        assert_eq!(btc.min_qty, 1); // 0.001 * 10^3
                                    // min_notional = "100" with price_scale=8 → 100 * 10^8 = 10_000_000_000
        assert_eq!(btc.min_notional, 10_000_000_000);
        assert_eq!(btc.contract_size, 1);
        assert!(btc.is_trading);
        assert_eq!(btc.header.venue_id, VenueId::BinanceFutures);
        assert_eq!(btc.header.market_type, MarketType::UsdmFutures);
    }

    #[test]
    fn parse_futures_eth_min_notional_from_notional_field() {
        let defs = parse_exchange_info(
            FUTURES_JSON,
            VenueId::BinanceFutures,
            MarketType::UsdmFutures,
            1,
            0,
        )
        .unwrap();
        let eth = defs.iter().find(|d| d.symbol == "ETHUSDT").unwrap();
        // min_notional = "5" with price_scale=8 → 500_000_000
        assert_eq!(eth.min_notional, 500_000_000);
    }

    #[test]
    fn missing_price_filter_uses_default() {
        let json = br#"{
            "symbols": [{
                "symbol": "TESTUSDT",
                "status": "TRADING",
                "baseAsset": "TEST",
                "quoteAsset": "USDT",
                "filters": []
            }]
        }"#;
        let defs = parse_exchange_info(json, VenueId::BinanceSpot, MarketType::Spot, 1, 0).unwrap();
        assert_eq!(defs.len(), 1);
        // Default tick_size "0.00000001" → price_scale = 8, tick_size = 1
        assert_eq!(defs[0].price_scale, 8);
        assert_eq!(defs[0].tick_size, 1);
    }

    #[test]
    fn sequence_numbers_are_consecutive() {
        let defs =
            parse_exchange_info(SPOT_JSON, VenueId::BinanceSpot, MarketType::Spot, 1, 50).unwrap();
        let seqs: Vec<u64> = defs.iter().map(|d| d.header.sequence_number).collect();
        assert_eq!(seqs, vec![50, 51, 52]);
    }

    // ---- depth snapshot ------------------------------------------------

    fn btcusdt_def() -> InstrumentDefinition {
        parse_exchange_info(
            SPOT_JSON_FOR_TESTS,
            VenueId::BinanceSpot,
            MarketType::Spot,
            1,
            0,
        )
        .unwrap()
        .into_iter()
        .find(|d| d.symbol == "BTCUSDT")
        .unwrap()
    }

    const DEPTH_JSON: &[u8] = br#"{
        "lastUpdateId": 1234567890,
        "bids": [
            ["96500.00", "0.50000000"],
            ["96499.00", "1.00000000"]
        ],
        "asks": [
            ["96501.00", "0.25000000"],
            ["96502.00", "0.75000000"]
        ]
    }"#;

    #[test]
    fn parse_depth_snapshot_update_id() {
        let inst = btcusdt_def();
        let snap = parse_depth_snapshot(DEPTH_JSON, &inst, 0).unwrap();
        assert_eq!(snap.update_id, 1_234_567_890);
    }

    #[test]
    fn parse_depth_snapshot_level_counts() {
        let inst = btcusdt_def();
        let snap = parse_depth_snapshot(DEPTH_JSON, &inst, 0).unwrap();
        assert_eq!(snap.bids.len(), 2);
        assert_eq!(snap.asks.len(), 2);
    }

    #[test]
    fn parse_depth_snapshot_bid_price_scaled() {
        let inst = btcusdt_def();
        let snap = parse_depth_snapshot(DEPTH_JSON, &inst, 0).unwrap();
        // price_scale=8 (tickSize "0.01000000" → 8 decimal places)
        // "96500.00" → 96500 * 10^8 = 9_650_000_000_000
        assert_eq!(snap.bids[0].price, 9_650_000_000_000);
        // "96499.00" → 96499 * 10^8 = 9_649_900_000_000
        assert_eq!(snap.bids[1].price, 9_649_900_000_000);
    }

    #[test]
    fn parse_depth_snapshot_qty_scaled() {
        let inst = btcusdt_def();
        let snap = parse_depth_snapshot(DEPTH_JSON, &inst, 0).unwrap();
        // qty_scale=8 (stepSize "0.00001000" → 8 decimal places)
        // "0.50000000" → 50_000_000
        assert_eq!(snap.bids[0].qty, 50_000_000);
    }

    #[test]
    fn parse_depth_snapshot_header_fields() {
        let inst = btcusdt_def();
        let recv_ts = 9_999_999_999_i64;
        let snap = parse_depth_snapshot(DEPTH_JSON, &inst, recv_ts).unwrap();
        assert_eq!(snap.header.message_type, MessageType::BookSnapshot);
        assert_eq!(snap.header.venue_id, VenueId::BinanceSpot);
        assert_eq!(snap.header.market_type, MarketType::Spot);
        assert_eq!(snap.header.local_recv_ts, recv_ts);
        assert_eq!(snap.symbol, "BTCUSDT");
    }

    #[test]
    fn parse_depth_snapshot_invalid_json_errors() {
        let inst = btcusdt_def();
        assert!(parse_depth_snapshot(b"not json", &inst, 0).is_err());
    }
}
