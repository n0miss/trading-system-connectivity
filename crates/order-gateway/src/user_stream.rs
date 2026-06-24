//! Raw Binance user data stream JSON types.
//!
//! Parsed with serde; no business logic here.  The [`parse`] function
//! dispatches on the `e` (event-type) field and returns the appropriate typed
//! variant.  Unrecognised event types are represented as
//! [`RawUserDataEvent::Unknown`] rather than errors so that new Binance event
//! types added in future do not crash the stream listener.
//!
//! All single-char Binance field names are mapped to readable Rust names via
//! `#[serde(rename = "...")]`.  Extra fields present in the JSON payload (e.g.
//! `"I"`, `"w"`, `"M"`, `"O"`, `"Z"`, `"V"`) are silently ignored.

use serde::Deserialize;

// ---------------------------------------------------------------------------
// Error
// ---------------------------------------------------------------------------

#[derive(Debug, thiserror::Error)]
pub enum ParseError {
    #[error("json: {0}")]
    Json(#[from] serde_json::Error),
}

// ---------------------------------------------------------------------------
// Raw types
// ---------------------------------------------------------------------------

/// Fields captured from a Binance `executionReport` event (Spot).
///
/// Only the fields needed by the normalizer are captured; all others are
/// silently discarded by serde.
#[derive(Debug, Deserialize, PartialEq)]
pub struct RawExecutionReport {
    /// Event time (unix ms).
    #[serde(rename = "E")]
    pub event_time_ms: i64,
    /// Symbol (e.g. "BTCUSDT").
    #[serde(rename = "s")]
    pub symbol: String,
    /// Client order ID (our `cc-XXXX-...` format for orders we placed).
    #[serde(rename = "c")]
    pub client_order_id: String,
    /// Order side: `"BUY"` or `"SELL"`.
    #[serde(rename = "S")]
    pub side_raw: String,
    /// Order type: `"LIMIT"`, `"MARKET"`, etc.
    #[serde(rename = "o")]
    pub order_type_raw: String,
    /// Time in force: `"GTC"`, `"IOC"`, `"FOK"`, `"GTX"`.
    #[serde(rename = "f")]
    pub time_in_force_raw: String,
    /// Original order quantity (decimal string).
    #[serde(rename = "q")]
    pub order_qty_raw: String,
    /// Original order price (decimal string; `"0"` for MARKET orders).
    #[serde(rename = "p")]
    pub order_price_raw: String,
    /// Execution type (the "verb"): `NEW | CANCELED | REPLACED | REJECTED | TRADE | EXPIRED | TRADE_PREVENTION`.
    #[serde(rename = "x")]
    pub exec_type_raw: String,
    /// Current order status: `NEW | PARTIALLY_FILLED | FILLED | CANCELED | REJECTED | EXPIRED | …`
    #[serde(rename = "X")]
    pub order_status_raw: String,
    /// Reject reason (`"NONE"` when the order was not rejected).
    #[serde(rename = "r")]
    pub reject_reason_raw: String,
    /// Exchange-assigned order ID.
    #[serde(rename = "i")]
    pub exchange_order_id: u64,
    /// Last executed quantity (decimal string; `"0.00000000"` for non-TRADE events).
    #[serde(rename = "l")]
    pub last_fill_qty_raw: String,
    /// Cumulative filled quantity (decimal string).
    #[serde(rename = "z")]
    pub cum_fill_qty_raw: String,
    /// Last executed price (decimal string; `"0.00000000"` for non-TRADE events).
    #[serde(rename = "L")]
    pub last_fill_price_raw: String,
    /// Transaction time (unix ms).
    #[serde(rename = "T")]
    pub transaction_time_ms: i64,
    /// Trade ID (`-1` when the event is not a trade fill).
    #[serde(rename = "t")]
    pub trade_id: i64,
    /// Commission amount (decimal string).
    #[serde(rename = "n")]
    pub commission_raw: String,
    /// Commission asset (`null` / absent when commission is zero).
    #[serde(rename = "N", default)]
    pub commission_asset: Option<String>,
    /// Whether this fill was on the maker side of the book.
    #[serde(rename = "m")]
    pub is_maker: bool,
}

/// One asset entry in an `outboundAccountPosition` event.
#[derive(Debug, Deserialize, PartialEq)]
pub struct RawAssetBalance {
    /// Asset name (e.g. `"BTC"`).
    #[serde(rename = "a")]
    pub asset: String,
    /// Free (unlocked) balance (decimal string).
    #[serde(rename = "f")]
    pub free: String,
    /// Locked balance — reserved in open orders (decimal string).
    #[serde(rename = "l")]
    pub locked: String,
}

/// Binance `outboundAccountPosition` — pushed whenever an account balance changes.
#[derive(Debug, Deserialize, PartialEq)]
pub struct RawBalancePosition {
    /// Event time (unix ms).
    #[serde(rename = "E")]
    pub event_time_ms: i64,
    /// Time of last account update (unix ms).
    #[serde(rename = "u")]
    pub last_update_ms: i64,
    /// Changed asset balances.
    #[serde(rename = "B")]
    pub balances: Vec<RawAssetBalance>,
}

/// Binance `balanceUpdate` — pushed on deposits, withdrawals, and dust sweeps.
#[derive(Debug, Deserialize, PartialEq)]
pub struct RawBalanceUpdate {
    /// Event time (unix ms).
    #[serde(rename = "E")]
    pub event_time_ms: i64,
    /// Asset name.
    #[serde(rename = "a")]
    pub asset: String,
    /// Signed balance delta (decimal string; negative for debits).
    #[serde(rename = "d")]
    pub delta_raw: String,
    /// Clear / transaction time (unix ms).
    #[serde(rename = "T")]
    pub clear_time_ms: i64,
}

// ---------------------------------------------------------------------------
// Dispatch enum
// ---------------------------------------------------------------------------

/// A single parsed message from the Binance user data stream.
#[derive(Debug, PartialEq)]
pub enum RawUserDataEvent {
    ExecutionReport(RawExecutionReport),
    OutboundAccountPosition(RawBalancePosition),
    BalanceUpdate(RawBalanceUpdate),
    /// An event type not recognised by this parser.
    ///
    /// Stored so callers can log or metric on unknown event types without
    /// losing the type string.
    Unknown {
        event_type: String,
    },
}

/// Parse one JSON message received from the Binance user data WebSocket.
///
/// Returns `RawUserDataEvent::Unknown` for unrecognised `e` values instead of
/// an error, so future Binance event types do not break the listener.
///
/// Returns `Err(ParseError::Json)` when the bytes are not valid JSON or the
/// required `e` field is absent/malformed.
pub fn parse(bytes: &[u8]) -> Result<RawUserDataEvent, ParseError> {
    #[derive(Deserialize)]
    struct Discriminant {
        e: String,
    }
    let disc: Discriminant = serde_json::from_slice(bytes)?;
    match disc.e.as_str() {
        "executionReport" => Ok(RawUserDataEvent::ExecutionReport(serde_json::from_slice(
            bytes,
        )?)),
        "outboundAccountPosition" => Ok(RawUserDataEvent::OutboundAccountPosition(
            serde_json::from_slice(bytes)?,
        )),
        "balanceUpdate" => Ok(RawUserDataEvent::BalanceUpdate(serde_json::from_slice(
            bytes,
        )?)),
        _ => Ok(RawUserDataEvent::Unknown { event_type: disc.e }),
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    // Realistic Binance payload fixtures (extra fields are intentionally kept
    // to verify that serde silently discards them).

    const NEW_EXEC_REPORT: &str = r#"{
        "e":"executionReport","E":1499405658658,"s":"BTCUSDT",
        "c":"cc-0001-0000000000000042","S":"BUY","o":"LIMIT","f":"GTC",
        "q":"0.10000000","p":"50000.00000000","P":"0.00000000","F":"0.00000000",
        "g":-1,"C":"","x":"NEW","X":"NEW","r":"NONE","i":4293153,
        "l":"0.00000000","z":"0.00000000","L":"0.00000000","n":"0","N":null,
        "T":1499405658657,"t":-1,"I":8641984,"w":true,"m":false,"M":false,
        "O":1499405658657,"Z":"0.00000000","Y":"0.00000000","Q":"0.00000000",
        "W":1499405658657,"V":"NONE"
    }"#;

    const TRADE_EXEC_REPORT: &str = r#"{
        "e":"executionReport","E":1499405658659,"s":"BTCUSDT",
        "c":"cc-0001-0000000000000042","S":"BUY","o":"LIMIT","f":"GTC",
        "q":"0.10000000","p":"50000.00000000","P":"0.00000000","F":"0.00000000",
        "g":-1,"C":"","x":"TRADE","X":"FILLED","r":"NONE","i":4293153,
        "l":"0.10000000","z":"0.10000000","L":"50000.00000000",
        "n":"0.00002500","N":"BTC","T":1499405658657,"t":9876543,
        "I":8641985,"w":false,"m":false,"M":true,
        "O":1499405658657,"Z":"5000.00000000","Y":"5000.00000000",
        "Q":"0.00000000","W":1499405658657,"V":"NONE"
    }"#;

    const CANCELED_EXEC_REPORT: &str = r#"{
        "e":"executionReport","E":1499405658700,"s":"BTCUSDT",
        "c":"cc-0001-0000000000000042","S":"SELL","o":"LIMIT","f":"GTC",
        "q":"0.05000000","p":"51000.00000000","P":"0.00000000","F":"0.00000000",
        "g":-1,"C":"cc-0001-0000000000000042",
        "x":"CANCELED","X":"CANCELED","r":"NONE","i":4293154,
        "l":"0.00000000","z":"0.00000000","L":"0.00000000","n":"0","N":null,
        "T":1499405658699,"t":-1,"I":8641986,"w":false,"m":false,"M":false,
        "O":1499405658600,"Z":"0.00000000","Y":"0.00000000","Q":"0.00000000",
        "W":1499405658600,"V":"NONE"
    }"#;

    const REJECTED_EXEC_REPORT: &str = r#"{
        "e":"executionReport","E":1499405658800,"s":"BTCUSDT",
        "c":"cc-0001-0000000000000043","S":"BUY","o":"LIMIT","f":"GTC",
        "q":"0.00010000","p":"1.00000000","P":"0.00000000","F":"0.00000000",
        "g":-1,"C":"","x":"REJECTED","X":"REJECTED","r":"PRICE_FILTER","i":0,
        "l":"0.00000000","z":"0.00000000","L":"0.00000000","n":"0","N":null,
        "T":1499405658799,"t":-1,"I":0,"w":false,"m":false,"M":false,
        "O":1499405658800,"Z":"0.00000000","Y":"0.00000000","Q":"0.00000000",
        "W":0,"V":"NONE"
    }"#;

    const ACCOUNT_POSITION: &str = r#"{
        "e":"outboundAccountPosition","E":1564034571105,"u":1564034571073,
        "B":[
            {"a":"ETH","f":"10.50000000","l":"0.00000000"},
            {"a":"BTC","f":"0.00100000","l":"0.00050000"}
        ]
    }"#;

    const BALANCE_UPDATE: &str = r#"{
        "e":"balanceUpdate","E":1573200697110,"a":"BTC","d":"100.00000000",
        "T":1573200697068
    }"#;

    #[test]
    fn parse_execution_report_new() {
        let ev = parse(NEW_EXEC_REPORT.as_bytes()).unwrap();
        let RawUserDataEvent::ExecutionReport(rep) = ev else {
            panic!("expected ExecutionReport")
        };
        assert_eq!(rep.symbol, "BTCUSDT");
        assert_eq!(rep.client_order_id, "cc-0001-0000000000000042");
        assert_eq!(rep.side_raw, "BUY");
        assert_eq!(rep.exec_type_raw, "NEW");
        assert_eq!(rep.order_status_raw, "NEW");
        assert_eq!(rep.exchange_order_id, 4293153);
        assert_eq!(rep.trade_id, -1);
        assert!(rep.commission_asset.is_none());
        assert!(!rep.is_maker);
        assert_eq!(rep.event_time_ms, 1499405658658);
        assert_eq!(rep.transaction_time_ms, 1499405658657);
    }

    #[test]
    fn parse_execution_report_trade() {
        let ev = parse(TRADE_EXEC_REPORT.as_bytes()).unwrap();
        let RawUserDataEvent::ExecutionReport(rep) = ev else {
            panic!("expected ExecutionReport")
        };
        assert_eq!(rep.exec_type_raw, "TRADE");
        assert_eq!(rep.order_status_raw, "FILLED");
        assert_eq!(rep.trade_id, 9876543);
        assert_eq!(rep.last_fill_qty_raw, "0.10000000");
        assert_eq!(rep.last_fill_price_raw, "50000.00000000");
        assert_eq!(rep.commission_asset, Some("BTC".to_string()));
        assert!(!rep.is_maker);
    }

    #[test]
    fn parse_execution_report_canceled() {
        let ev = parse(CANCELED_EXEC_REPORT.as_bytes()).unwrap();
        let RawUserDataEvent::ExecutionReport(rep) = ev else {
            panic!("expected ExecutionReport")
        };
        assert_eq!(rep.exec_type_raw, "CANCELED");
        assert_eq!(rep.side_raw, "SELL");
        assert_eq!(rep.trade_id, -1);
    }

    #[test]
    fn parse_execution_report_rejected() {
        let ev = parse(REJECTED_EXEC_REPORT.as_bytes()).unwrap();
        let RawUserDataEvent::ExecutionReport(rep) = ev else {
            panic!("expected ExecutionReport")
        };
        assert_eq!(rep.exec_type_raw, "REJECTED");
        assert_eq!(rep.reject_reason_raw, "PRICE_FILTER");
        assert_eq!(rep.trade_id, -1);
    }

    #[test]
    fn parse_outbound_account_position() {
        let ev = parse(ACCOUNT_POSITION.as_bytes()).unwrap();
        let RawUserDataEvent::OutboundAccountPosition(pos) = ev else {
            panic!("expected OutboundAccountPosition")
        };
        assert_eq!(pos.event_time_ms, 1564034571105);
        assert_eq!(pos.last_update_ms, 1564034571073);
        assert_eq!(pos.balances.len(), 2);
        assert_eq!(pos.balances[0].asset, "ETH");
        assert_eq!(pos.balances[0].free, "10.50000000");
        assert_eq!(pos.balances[0].locked, "0.00000000");
        assert_eq!(pos.balances[1].asset, "BTC");
        assert_eq!(pos.balances[1].locked, "0.00050000");
    }

    #[test]
    fn parse_balance_update() {
        let ev = parse(BALANCE_UPDATE.as_bytes()).unwrap();
        let RawUserDataEvent::BalanceUpdate(upd) = ev else {
            panic!("expected BalanceUpdate")
        };
        assert_eq!(upd.asset, "BTC");
        assert_eq!(upd.delta_raw, "100.00000000");
        assert_eq!(upd.event_time_ms, 1573200697110);
        assert_eq!(upd.clear_time_ms, 1573200697068);
    }

    #[test]
    fn parse_unknown_event_type() {
        let json = r#"{"e":"listStatus","E":1234567890000,"s":"BTCUSDT"}"#;
        let ev = parse(json.as_bytes()).unwrap();
        assert_eq!(
            ev,
            RawUserDataEvent::Unknown {
                event_type: "listStatus".to_string()
            }
        );
    }

    #[test]
    fn parse_malformed_json_is_error() {
        assert!(parse(b"{not valid json}").is_err());
    }

    #[test]
    fn parse_empty_bytes_is_error() {
        assert!(parse(b"").is_err());
    }
}
