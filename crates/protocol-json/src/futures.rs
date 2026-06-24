/// Binance USD-M Futures WebSocket JSON parser (§5.21).
///
/// Handles the combined-stream wrapper `{"stream":"…","data":{…}}` and
/// dispatches on the stream-name suffix to produce typed event structs.
///
/// All price and quantity fields are left as raw strings; the normalizer
/// (§5.22) applies per-instrument scale factors to produce scaled `i64` values.
use serde::Deserialize;

use crate::error::JsonError;

// ---------------------------------------------------------------------------
// Exchange-native structs
// ---------------------------------------------------------------------------

/// Binance USDT-M Futures `{symbol}@bookTicker` payload.
///
/// Layout is identical to the Spot `bookTicker` stream.
///
/// ```json
/// {"u":400900217,"s":"BTCUSDT","b":"96500.00","B":"1.23","a":"96501.00","A":"0.50"}
/// ```
#[derive(Debug, Deserialize)]
pub struct FuturesBookTicker {
    /// Order book update ID.
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

/// Binance USDT-M Futures `{symbol}@depth@{speed}ms` payload.
///
/// Differs from the Spot stream by:
/// * `"T"` — transaction time (milliseconds)
/// * `"pu"` — previous final update ID (used by the sequence validator to
///   detect gaps without a Spot-style U/u handshake)
///
/// ```json
/// {"e":"depthUpdate","E":1748000000000,"T":1748000000001,
///  "s":"BTCUSDT","U":50000001,"u":50000005,"pu":50000000,
///  "b":[["96500.00","2.50"]],"a":[["96501.00","1.00"]]}
/// ```
#[derive(Debug, Deserialize)]
pub struct FuturesDepthUpdate {
    #[serde(rename = "E")]
    pub event_time_ms: i64,
    /// Transaction time in milliseconds.  Omitted on some exchange variants.
    #[serde(rename = "T", default)]
    pub transaction_time_ms: i64,
    #[serde(rename = "s")]
    pub symbol: String,
    /// First update ID in this event (inclusive).
    #[serde(rename = "U")]
    pub first_update_id: u64,
    /// Final update ID in this event (inclusive).
    #[serde(rename = "u")]
    pub last_update_id: u64,
    /// Final update ID of the previous depth event — enables gap detection
    /// without a REST snapshot handshake at startup.  Absent on some snapshots.
    #[serde(rename = "pu", default)]
    pub prev_final_update_id: u64,
    /// `[price, qty]` pairs; qty `"0"` means remove the level.
    #[serde(rename = "b")]
    pub bids: Vec<[String; 2]>,
    #[serde(rename = "a")]
    pub asks: Vec<[String; 2]>,
}

/// Binance USDT-M Futures `{symbol}@aggTrade` payload.
///
/// Aggregated trades bundle multiple individual fill events into one message.
///
/// ```json
/// {"e":"aggTrade","E":1748000000000,"s":"BTCUSDT",
///  "a":26129,"p":"96500.50","q":"0.015",
///  "f":100,"l":105,"T":1748000000000,"m":false}
/// ```
#[derive(Debug, Deserialize)]
pub struct FuturesAggTrade {
    #[serde(rename = "E")]
    pub event_time_ms: i64,
    #[serde(rename = "s")]
    pub symbol: String,
    /// Aggregate trade ID.
    #[serde(rename = "a")]
    pub agg_trade_id: u64,
    #[serde(rename = "p")]
    pub price: String,
    #[serde(rename = "q")]
    pub qty: String,
    /// First individual trade ID in this aggregate.
    #[serde(rename = "f")]
    pub first_trade_id: u64,
    /// Last individual trade ID in this aggregate.
    #[serde(rename = "l")]
    pub last_trade_id: u64,
    /// Execution time of the last fill in milliseconds.  Absent on some variants.
    #[serde(rename = "T", default)]
    pub trade_time_ms: i64,
    /// `true` → buyer is maker → seller is the aggressor.
    #[serde(rename = "m")]
    pub is_buyer_maker: bool,
}

/// Binance USDT-M Futures `{symbol}@markPrice` payload.
///
/// Published every ~3 seconds or every 1 second for `@markPrice@1s`.
/// `index_price` and `funding_rate` may be empty strings on rare market events;
/// treat `""` as unavailable in the normalizer.
///
/// ```json
/// {"e":"markPriceUpdate","E":1748000000000,"s":"BTCUSDT",
///  "p":"96500.50","i":"96501.00","P":"96498.00",
///  "r":"0.00010000","T":1749600000000}
/// ```
#[derive(Debug, Deserialize)]
pub struct FuturesMarkPrice {
    #[serde(rename = "E")]
    pub event_time_ms: i64,
    #[serde(rename = "s")]
    pub symbol: String,
    /// Mark price string.
    #[serde(rename = "p")]
    pub mark_price: String,
    /// Spot index price string.  Empty string `""` when not available.
    #[serde(rename = "i", default)]
    pub index_price: String,
    /// Funding rate string.  Empty string `""` between settlement windows.
    #[serde(rename = "r", default)]
    pub funding_rate: String,
    /// Next funding settlement time in milliseconds.  Absent on delivery contracts.
    #[serde(rename = "T", default)]
    pub next_funding_time_ms: i64,
}

/// Binance USDT-M Futures `{symbol}@forceOrder` payload.
///
/// Published when a position is liquidated and a market order is placed on
/// behalf of the liquidated account.
///
/// ```json
/// {"e":"forceOrder","E":1748000000000,
///  "o":{"s":"BTCUSDT","S":"SELL","o":"LIMIT","f":"IOC",
///       "q":"0.014","p":"9910","ap":"9910","X":"FILLED",
///       "l":"0.014","z":"0.014","T":1748000000000}}
/// ```
#[derive(Debug, Deserialize)]
pub struct FuturesForceOrder {
    #[serde(rename = "E")]
    pub event_time_ms: i64,
    /// The liquidation order details.
    #[serde(rename = "o")]
    pub order: FuturesLiquidationOrder,
}

/// Inner order object within a [`FuturesForceOrder`] event.
#[derive(Debug, Deserialize)]
pub struct FuturesLiquidationOrder {
    #[serde(rename = "s")]
    pub symbol: String,
    /// Order side: `"BUY"` or `"SELL"`.
    #[serde(rename = "S")]
    pub side: String,
    /// Original quantity.
    #[serde(rename = "q")]
    pub qty: String,
    /// Order price.
    #[serde(rename = "p")]
    pub price: String,
    /// Average fill price.
    #[serde(rename = "ap")]
    pub avg_price: String,
    /// Last filled quantity.
    #[serde(rename = "l")]
    pub last_filled_qty: String,
    /// Trade time in milliseconds.  Absent on some liquidation order variants.
    #[serde(rename = "T", default)]
    pub trade_time_ms: i64,
}

// ---------------------------------------------------------------------------
// Dispatch
// ---------------------------------------------------------------------------

/// Parsed result of a Binance USDT-M Futures combined-stream message.
#[derive(Debug)]
pub enum FuturesEvent {
    BookTicker(FuturesBookTicker),
    DepthUpdate(FuturesDepthUpdate),
    AggTrade(FuturesAggTrade),
    MarkPrice(FuturesMarkPrice),
    ForceOrder(FuturesForceOrder),
    /// Unrecognized stream type — stream name is preserved for logging.
    Unknown(String),
}

/// Parse a raw Binance USDT-M Futures combined-stream frame into a
/// [`FuturesEvent`].
///
/// The frame must use the combined-stream envelope:
/// `{"stream":"btcusdt@bookTicker","data":{...}}`.
pub fn parse_futures_message(bytes: &[u8]) -> Result<FuturesEvent, JsonError> {
    #[derive(Deserialize)]
    struct Wrapper<'a> {
        stream: String,
        #[serde(borrow)]
        data: &'a serde_json::value::RawValue,
    }

    let wrapper: Wrapper = serde_json::from_slice(bytes)?;
    let data = wrapper.data.get();

    match stream_kind(&wrapper.stream) {
        StreamKind::BookTicker => {
            let msg: FuturesBookTicker = serde_json::from_str(data)?;
            Ok(FuturesEvent::BookTicker(msg))
        }
        StreamKind::DepthUpdate => {
            let msg: FuturesDepthUpdate = serde_json::from_str(data)?;
            Ok(FuturesEvent::DepthUpdate(msg))
        }
        StreamKind::AggTrade => {
            let msg: FuturesAggTrade = serde_json::from_str(data)?;
            Ok(FuturesEvent::AggTrade(msg))
        }
        StreamKind::MarkPrice => {
            let msg: FuturesMarkPrice = serde_json::from_str(data)?;
            Ok(FuturesEvent::MarkPrice(msg))
        }
        StreamKind::ForceOrder => {
            let msg: FuturesForceOrder = serde_json::from_str(data)?;
            Ok(FuturesEvent::ForceOrder(msg))
        }
        StreamKind::Unknown => Ok(FuturesEvent::Unknown(wrapper.stream)),
    }
}

// ---------------------------------------------------------------------------
// Internal helpers
// ---------------------------------------------------------------------------

enum StreamKind {
    BookTicker,
    DepthUpdate,
    AggTrade,
    MarkPrice,
    ForceOrder,
    Unknown,
}

/// Classify a futures stream name by the segment after the first `@`.
///
/// Examples: `"btcusdt@bookTicker"` → `BookTicker`;
/// `"btcusdt@depth@100ms"` → `DepthUpdate`;
/// `"btcusdt@markPrice@1s"` → `MarkPrice`.
fn stream_kind(stream: &str) -> StreamKind {
    let kind = stream.splitn(3, '@').nth(1).unwrap_or("");
    match kind {
        "bookTicker" => StreamKind::BookTicker,
        "depth" => StreamKind::DepthUpdate,
        "aggTrade" => StreamKind::AggTrade,
        "markPrice" => StreamKind::MarkPrice,
        "forceOrder" => StreamKind::ForceOrder,
        _ => StreamKind::Unknown,
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    const BOOK_TICKER_MSG: &[u8] = br#"{
        "stream": "btcusdt@bookTicker",
        "data": {
            "u": 400900217,
            "s": "BTCUSDT",
            "b": "96500.00",
            "B": "1.23",
            "a": "96501.00",
            "A": "0.50"
        }
    }"#;

    const DEPTH_UPDATE_MSG: &[u8] = br#"{
        "stream": "btcusdt@depth@100ms",
        "data": {
            "e": "depthUpdate",
            "E": 1748000000000,
            "T": 1748000000001,
            "s": "BTCUSDT",
            "U": 50000001,
            "u": 50000005,
            "pu": 50000000,
            "b": [["96500.00", "2.50"], ["96499.00", "0.00"]],
            "a": [["96501.00", "1.00"]]
        }
    }"#;

    const AGG_TRADE_MSG: &[u8] = br#"{
        "stream": "btcusdt@aggTrade",
        "data": {
            "e": "aggTrade",
            "E": 1748000000000,
            "s": "BTCUSDT",
            "a": 26129,
            "p": "96500.50",
            "q": "0.01500000",
            "f": 100,
            "l": 105,
            "T": 1748000000000,
            "m": false
        }
    }"#;

    const AGG_TRADE_BUYER_MAKER_MSG: &[u8] = br#"{
        "stream": "ethusdt@aggTrade",
        "data": {
            "e": "aggTrade",
            "E": 1748000000002,
            "s": "ETHUSDT",
            "a": 9999,
            "p": "3500.00",
            "q": "0.50",
            "f": 200,
            "l": 200,
            "T": 1748000000001,
            "m": true
        }
    }"#;

    const MARK_PRICE_MSG: &[u8] = br#"{
        "stream": "btcusdt@markPrice",
        "data": {
            "e": "markPriceUpdate",
            "E": 1748000000000,
            "s": "BTCUSDT",
            "p": "96500.50",
            "i": "96501.00",
            "P": "96498.00",
            "r": "0.00010000",
            "T": 1749600000000
        }
    }"#;

    const MARK_PRICE_1S_MSG: &[u8] = br#"{
        "stream": "btcusdt@markPrice@1s",
        "data": {
            "e": "markPriceUpdate",
            "E": 1748000001000,
            "s": "BTCUSDT",
            "p": "96502.00",
            "i": "96503.00",
            "P": "96500.00",
            "r": "0.00010000",
            "T": 1749600000000
        }
    }"#;

    const FORCE_ORDER_MSG: &[u8] = br#"{
        "stream": "btcusdt@forceOrder",
        "data": {
            "e": "forceOrder",
            "E": 1748000000000,
            "o": {
                "s": "BTCUSDT",
                "S": "SELL",
                "o": "LIMIT",
                "f": "IOC",
                "q": "0.014",
                "p": "9910",
                "ap": "9910",
                "X": "FILLED",
                "l": "0.014",
                "z": "0.014",
                "T": 1748000000000
            }
        }
    }"#;

    const UNKNOWN_STREAM_MSG: &[u8] = br#"{
        "stream": "btcusdt@kline_1m",
        "data": {"x": 1}
    }"#;

    // --- bookTicker ---

    #[test]
    fn parse_book_ticker() {
        let event = parse_futures_message(BOOK_TICKER_MSG).unwrap();
        let FuturesEvent::BookTicker(bt) = event else {
            panic!("wrong variant")
        };
        assert_eq!(bt.update_id, 400_900_217);
        assert_eq!(bt.symbol, "BTCUSDT");
        assert_eq!(bt.bid_price, "96500.00");
        assert_eq!(bt.bid_qty, "1.23");
        assert_eq!(bt.ask_price, "96501.00");
        assert_eq!(bt.ask_qty, "0.50");
    }

    // --- depth update ---

    #[test]
    fn parse_depth_update() {
        let event = parse_futures_message(DEPTH_UPDATE_MSG).unwrap();
        let FuturesEvent::DepthUpdate(d) = event else {
            panic!("wrong variant")
        };
        assert_eq!(d.symbol, "BTCUSDT");
        assert_eq!(d.first_update_id, 50_000_001);
        assert_eq!(d.last_update_id, 50_000_005);
        assert_eq!(d.prev_final_update_id, 50_000_000);
        assert_eq!(d.event_time_ms, 1_748_000_000_000);
        assert_eq!(d.transaction_time_ms, 1_748_000_000_001);
    }

    #[test]
    fn depth_update_bids_and_asks_parsed() {
        let FuturesEvent::DepthUpdate(d) = parse_futures_message(DEPTH_UPDATE_MSG).unwrap() else {
            panic!("wrong variant")
        };
        assert_eq!(d.bids.len(), 2);
        assert_eq!(d.asks.len(), 1);
        assert_eq!(d.bids[0], ["96500.00".to_string(), "2.50".to_string()]);
        assert_eq!(d.asks[0], ["96501.00".to_string(), "1.00".to_string()]);
    }

    #[test]
    fn depth_update_speed_suffix_dispatches_correctly() {
        // @depth@500ms should still route to DepthUpdate
        let msg = br#"{"stream":"ethusdt@depth@500ms","data":{"e":"depthUpdate","E":1,"T":2,"s":"ETHUSDT","U":1,"u":2,"pu":0,"b":[],"a":[]}}"#;
        let event = parse_futures_message(msg).unwrap();
        assert!(matches!(event, FuturesEvent::DepthUpdate(_)));
    }

    // --- aggTrade ---

    #[test]
    fn parse_agg_trade_seller_aggressor() {
        let event = parse_futures_message(AGG_TRADE_MSG).unwrap();
        let FuturesEvent::AggTrade(t) = event else {
            panic!("wrong variant")
        };
        assert_eq!(t.symbol, "BTCUSDT");
        assert_eq!(t.agg_trade_id, 26_129);
        assert_eq!(t.price, "96500.50");
        assert_eq!(t.qty, "0.01500000");
        assert_eq!(t.first_trade_id, 100);
        assert_eq!(t.last_trade_id, 105);
        assert_eq!(t.trade_time_ms, 1_748_000_000_000);
        assert!(!t.is_buyer_maker);
    }

    #[test]
    fn parse_agg_trade_buyer_maker() {
        let FuturesEvent::AggTrade(t) = parse_futures_message(AGG_TRADE_BUYER_MAKER_MSG).unwrap()
        else {
            panic!("wrong variant")
        };
        assert!(t.is_buyer_maker);
        assert_eq!(t.symbol, "ETHUSDT");
    }

    // --- markPrice ---

    #[test]
    fn parse_mark_price() {
        let event = parse_futures_message(MARK_PRICE_MSG).unwrap();
        let FuturesEvent::MarkPrice(m) = event else {
            panic!("wrong variant")
        };
        assert_eq!(m.symbol, "BTCUSDT");
        assert_eq!(m.mark_price, "96500.50");
        assert_eq!(m.index_price, "96501.00");
        assert_eq!(m.funding_rate, "0.00010000");
        assert_eq!(m.next_funding_time_ms, 1_749_600_000_000);
        assert_eq!(m.event_time_ms, 1_748_000_000_000);
    }

    #[test]
    fn mark_price_1s_suffix_dispatches_correctly() {
        let event = parse_futures_message(MARK_PRICE_1S_MSG).unwrap();
        let FuturesEvent::MarkPrice(m) = event else {
            panic!("wrong variant")
        };
        assert_eq!(m.symbol, "BTCUSDT");
        assert_eq!(m.mark_price, "96502.00");
    }

    #[test]
    fn mark_price_missing_index_price_defaults_to_empty() {
        let msg = br#"{"stream":"btcusdt@markPrice","data":{
            "e":"markPriceUpdate","E":1748000000000,"s":"BTCUSDT",
            "p":"96500.50","r":"0.00010000","T":1749600000000
        }}"#;
        let FuturesEvent::MarkPrice(m) = parse_futures_message(msg).unwrap() else {
            panic!()
        };
        assert_eq!(m.index_price, "");
    }

    // --- forceOrder ---

    #[test]
    fn parse_force_order() {
        let event = parse_futures_message(FORCE_ORDER_MSG).unwrap();
        let FuturesEvent::ForceOrder(fo) = event else {
            panic!("wrong variant")
        };
        assert_eq!(fo.event_time_ms, 1_748_000_000_000);
        assert_eq!(fo.order.symbol, "BTCUSDT");
        assert_eq!(fo.order.side, "SELL");
        assert_eq!(fo.order.price, "9910");
        assert_eq!(fo.order.avg_price, "9910");
        assert_eq!(fo.order.qty, "0.014");
        assert_eq!(fo.order.last_filled_qty, "0.014");
        assert_eq!(fo.order.trade_time_ms, 1_748_000_000_000);
    }

    // --- unknown / error ---

    #[test]
    fn unknown_stream_type_returns_unknown_variant() {
        let FuturesEvent::Unknown(stream) = parse_futures_message(UNKNOWN_STREAM_MSG).unwrap()
        else {
            panic!("expected Unknown variant")
        };
        assert_eq!(stream, "btcusdt@kline_1m");
    }

    #[test]
    fn malformed_json_returns_error() {
        let result = parse_futures_message(b"not json");
        assert!(result.is_err());
    }

    #[test]
    fn wrong_data_shape_returns_error() {
        // Valid wrapper but data shape wrong for bookTicker (missing required fields)
        let msg = br#"{"stream":"btcusdt@bookTicker","data":{"wrong":1}}"#;
        assert!(parse_futures_message(msg).is_err());
    }
}
