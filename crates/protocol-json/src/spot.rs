use serde::Deserialize;

use crate::error::JsonError;

// ---------------------------------------------------------------------------
// Exchange-native structs
// All price/quantity fields are kept as strings — the Normalizer (Stage 2.8)
// applies per-instrument scale factors to convert them to scaled i64.
// ---------------------------------------------------------------------------

/// Binance Spot `{symbol}@bookTicker` payload.
///
/// Example:
/// ```json
/// {"u":400900217,"s":"BTCUSDT","b":"96500.00","B":"1.23","a":"96501.00","A":"0.50"}
/// ```
#[derive(Debug, Deserialize)]
pub struct SpotBookTicker {
    #[serde(rename = "u")]
    pub update_id: u64,
    #[serde(rename = "s")]
    pub symbol: String,
    #[serde(rename = "b")]
    pub bid_price: String,
    #[serde(rename = "B")]
    pub bid_qty: String,
    #[serde(rename = "a")]
    pub ask_price: String,
    #[serde(rename = "A")]
    pub ask_qty: String,
}

/// Binance Spot `{symbol}@depth@{speed}ms` payload.
///
/// Bids and asks are `[price, qty]` string pairs; qty `"0"` means remove the level.
///
/// Example:
/// ```json
/// {"e":"depthUpdate","E":1748000000000,"s":"BTCUSDT","U":50000001,"u":50000005,
///  "b":[["96500.00","2.50"],["96499.00","0"]],"a":[["96501.00","1.00"]]}
/// ```
#[derive(Debug, Deserialize)]
pub struct SpotDepthUpdate {
    #[serde(rename = "E")]
    pub event_time_ms: i64,
    #[serde(rename = "s")]
    pub symbol: String,
    /// First update ID in this batch (inclusive).
    #[serde(rename = "U")]
    pub first_update_id: u64,
    /// Final update ID in this batch (inclusive).
    #[serde(rename = "u")]
    pub last_update_id: u64,
    /// `[price, qty]` pairs. qty "0" → remove the price level.
    #[serde(rename = "b")]
    pub bids: Vec<[String; 2]>,
    #[serde(rename = "a")]
    pub asks: Vec<[String; 2]>,
}

/// Binance Spot `{symbol}@trade` payload.
///
/// `is_buyer_maker = true` → buyer is the market maker → seller is the aggressor.
///
/// Example:
/// ```json
/// {"e":"trade","E":1748000000001,"s":"BTCUSDT","t":3000001,
///  "p":"96500.50","q":"0.015","T":1748000000000,"m":false,"M":true}
/// ```
#[derive(Debug, Deserialize)]
pub struct SpotTrade {
    #[serde(rename = "E")]
    pub event_time_ms: i64,
    #[serde(rename = "s")]
    pub symbol: String,
    #[serde(rename = "t")]
    pub trade_id: u64,
    #[serde(rename = "p")]
    pub price: String,
    #[serde(rename = "q")]
    pub qty: String,
    /// Trade execution timestamp in milliseconds.
    #[serde(rename = "T")]
    pub trade_time_ms: i64,
    /// `true` → buyer is maker → seller aggressed.
    #[serde(rename = "m")]
    pub is_buyer_maker: bool,
}

// ---------------------------------------------------------------------------
// Dispatch
// ---------------------------------------------------------------------------

/// Parsed result of a Binance combined-stream message.
#[derive(Debug)]
pub enum SpotEvent {
    BookTicker(SpotBookTicker),
    DepthUpdate(SpotDepthUpdate),
    Trade(SpotTrade),
    /// Unrecognized stream type — stream name is preserved for logging.
    Unknown(String),
}

/// Parse a raw Binance combined-stream WebSocket frame into a [`SpotEvent`].
///
/// The frame must be the JSON combined-stream wrapper:
/// `{"stream":"btcusdt@bookTicker","data":{...}}`.
pub fn parse_spot_message(bytes: &[u8]) -> Result<SpotEvent, JsonError> {
    // Two-phase parse: decode the wrapper to get the stream name and a
    // zero-copy pointer to the `data` value, then dispatch on stream kind.
    #[derive(Deserialize)]
    struct Wrapper<'a> {
        stream: String,
        #[serde(borrow)]
        data: &'a serde_json::value::RawValue,
    }

    let wrapper: Wrapper = serde_json::from_slice(bytes)?;
    let data = wrapper.data.get(); // `&str` pointing into `bytes`

    match stream_kind(&wrapper.stream) {
        StreamKind::BookTicker => {
            let msg: SpotBookTicker = serde_json::from_str(data)?;
            Ok(SpotEvent::BookTicker(msg))
        }
        StreamKind::DepthUpdate => {
            let msg: SpotDepthUpdate = serde_json::from_str(data)?;
            Ok(SpotEvent::DepthUpdate(msg))
        }
        StreamKind::Trade => {
            let msg: SpotTrade = serde_json::from_str(data)?;
            Ok(SpotEvent::Trade(msg))
        }
        StreamKind::Unknown => Ok(SpotEvent::Unknown(wrapper.stream)),
    }
}

// ---------------------------------------------------------------------------
// Internal helpers
// ---------------------------------------------------------------------------

enum StreamKind {
    BookTicker,
    DepthUpdate,
    Trade,
    Unknown,
}

/// Classify a stream name like `"btcusdt@bookTicker"` or `"btcusdt@depth@100ms"`.
/// Only the segment after the first `@` matters.
fn stream_kind(stream: &str) -> StreamKind {
    let kind = stream.splitn(3, '@').nth(1).unwrap_or("");
    match kind {
        "bookTicker" => StreamKind::BookTicker,
        "depth" => StreamKind::DepthUpdate,
        "trade" => StreamKind::Trade,
        _ => StreamKind::Unknown,
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    // Real-shape Binance combined-stream frames.
    const BOOK_TICKER_MSG: &[u8] = br#"{
        "stream": "btcusdt@bookTicker",
        "data": {
            "u": 400900217,
            "s": "BTCUSDT",
            "b": "96500.00000000",
            "B": "1.23000000",
            "a": "96501.00000000",
            "A": "0.50000000"
        }
    }"#;

    const DEPTH_UPDATE_MSG: &[u8] = br#"{
        "stream": "btcusdt@depth@100ms",
        "data": {
            "e": "depthUpdate",
            "E": 1748000000000,
            "s": "BTCUSDT",
            "U": 50000001,
            "u": 50000005,
            "b": [["96500.00000000", "2.50000000"], ["96499.00000000", "0.00000000"]],
            "a": [["96501.00000000", "1.00000000"], ["96502.00000000", "3.14000000"]]
        }
    }"#;

    const TRADE_MSG: &[u8] = br#"{
        "stream": "btcusdt@trade",
        "data": {
            "e": "trade",
            "E": 1748000000001,
            "s": "BTCUSDT",
            "t": 3000001,
            "p": "96500.50000000",
            "q": "0.01500000",
            "T": 1748000000000,
            "m": false,
            "M": true
        }
    }"#;

    // A trade where the buyer is the maker (seller is aggressor).
    const TRADE_BUYER_MAKER_MSG: &[u8] = br#"{
        "stream": "ethusdt@trade",
        "data": {
            "e": "trade",
            "E": 1748000000002,
            "s": "ETHUSDT",
            "t": 9999,
            "p": "3500.00000000",
            "q": "0.50000000",
            "T": 1748000000001,
            "m": true,
            "M": true
        }
    }"#;

    const UNKNOWN_STREAM_MSG: &[u8] = br#"{
        "stream": "btcusdt@aggTrade",
        "data": {"x": 1}
    }"#;

    // --- bookTicker ---

    #[test]
    fn parse_book_ticker() {
        let event = parse_spot_message(BOOK_TICKER_MSG).unwrap();
        let SpotEvent::BookTicker(bt) = event else {
            panic!("wrong variant")
        };
        assert_eq!(bt.update_id, 400_900_217);
        assert_eq!(bt.symbol, "BTCUSDT");
        assert_eq!(bt.bid_price, "96500.00000000");
        assert_eq!(bt.bid_qty, "1.23000000");
        assert_eq!(bt.ask_price, "96501.00000000");
        assert_eq!(bt.ask_qty, "0.50000000");
    }

    // --- depth update ---

    #[test]
    fn parse_depth_update() {
        let event = parse_spot_message(DEPTH_UPDATE_MSG).unwrap();
        let SpotEvent::DepthUpdate(du) = event else {
            panic!("wrong variant")
        };
        assert_eq!(du.symbol, "BTCUSDT");
        assert_eq!(du.event_time_ms, 1_748_000_000_000);
        assert_eq!(du.first_update_id, 50_000_001);
        assert_eq!(du.last_update_id, 50_000_005);

        assert_eq!(du.bids.len(), 2);
        assert_eq!(du.bids[0], ["96500.00000000", "2.50000000"]);
        assert_eq!(du.bids[1], ["96499.00000000", "0.00000000"]);

        assert_eq!(du.asks.len(), 2);
        assert_eq!(du.asks[0], ["96501.00000000", "1.00000000"]);
        assert_eq!(du.asks[1], ["96502.00000000", "3.14000000"]);
    }

    #[test]
    fn parse_depth_update_empty_sides() {
        let msg = br#"{
            "stream": "ethusdt@depth@100ms",
            "data": {
                "e": "depthUpdate",
                "E": 1748000000000,
                "s": "ETHUSDT",
                "U": 1,
                "u": 1,
                "b": [],
                "a": [["3500.00", "10.00"]]
            }
        }"#;
        let event = parse_spot_message(msg).unwrap();
        let SpotEvent::DepthUpdate(du) = event else {
            panic!("wrong variant")
        };
        assert!(du.bids.is_empty());
        assert_eq!(du.asks.len(), 1);
    }

    // --- trade ---

    #[test]
    fn parse_trade_buyer_is_aggressor() {
        let event = parse_spot_message(TRADE_MSG).unwrap();
        let SpotEvent::Trade(tr) = event else {
            panic!("wrong variant")
        };
        assert_eq!(tr.symbol, "BTCUSDT");
        assert_eq!(tr.trade_id, 3_000_001);
        assert_eq!(tr.price, "96500.50000000");
        assert_eq!(tr.qty, "0.01500000");
        assert_eq!(tr.event_time_ms, 1_748_000_000_001);
        assert_eq!(tr.trade_time_ms, 1_748_000_000_000);
        assert!(!tr.is_buyer_maker); // buyer aggressed → m=false
    }

    #[test]
    fn parse_trade_seller_is_aggressor() {
        let event = parse_spot_message(TRADE_BUYER_MAKER_MSG).unwrap();
        let SpotEvent::Trade(tr) = event else {
            panic!("wrong variant")
        };
        assert_eq!(tr.symbol, "ETHUSDT");
        assert!(tr.is_buyer_maker); // seller aggressed → m=true
    }

    // --- dispatch ---

    #[test]
    fn unknown_stream_type_returns_unknown_variant() {
        let event = parse_spot_message(UNKNOWN_STREAM_MSG).unwrap();
        let SpotEvent::Unknown(name) = event else {
            panic!("wrong variant")
        };
        assert_eq!(name, "btcusdt@aggTrade");
    }

    #[test]
    fn malformed_json_returns_error() {
        let result = parse_spot_message(b"not json at all");
        assert!(result.is_err());
    }

    #[test]
    fn wrong_data_shape_returns_error() {
        // bookTicker wrapper but data is missing required fields
        let bad = br#"{"stream":"btcusdt@bookTicker","data":{"x":1}}"#;
        let result = parse_spot_message(bad);
        assert!(result.is_err());
    }

    #[test]
    fn stream_kind_depth_with_speed_suffix() {
        // Verify that "btcusdt@depth@250ms" and "btcusdt@depth@500ms" are also recognized.
        let msg_250 = format!(
            r#"{{"stream":"btcusdt@depth@250ms","data":{{"e":"depthUpdate","E":1,"s":"BTCUSDT","U":1,"u":2,"b":[],"a":[]}}}}"#
        );
        let event = parse_spot_message(msg_250.as_bytes()).unwrap();
        assert!(matches!(event, SpotEvent::DepthUpdate(_)));
    }
}
