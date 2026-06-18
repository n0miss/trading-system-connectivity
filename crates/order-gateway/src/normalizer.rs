//! Normalized user data stream types and the [`Normalizer`] that converts raw
//! Binance JSON events (from [`crate::user_stream`]) into them.
//!
//! # Usage
//!
//! ```rust,ignore
//! use connector_order_gateway::{Normalizer, NormalizedEvent, parse as parse_raw};
//!
//! let normalizer = Normalizer::new(100_000_000, 100_000_000);
//! let raw = parse_raw(ws_bytes)?;
//! match normalizer.normalize(raw)? {
//!     NormalizedEvent::OrderUpdate(upd)   => { /* feed into StateMachineEngine */ }
//!     NormalizedEvent::AccountUpdate(upd) => { /* update local balance cache   */ }
//!     NormalizedEvent::BalanceDelta(d)    => { /* apply delta to balance cache  */ }
//!     NormalizedEvent::Unknown { .. }     => { /* log / ignore                  */ }
//! }
//! ```
//!
//! # Scale convention
//!
//! Prices and quantities are stored as scaled integers (no floats).  Pass the
//! appropriate power-of-10 scale when constructing the [`Normalizer`].  For
//! most Binance Spot pairs, both price and qty use 8 decimal places
//! (`scale = 100_000_000`).  Symbol-specific overrides can be registered with
//! [`Normalizer::register_symbol`].
//!
//! Asset balances always use [`BALANCE_SCALE`] (10^8) regardless of symbol.

use std::collections::HashMap;

use crate::user_stream::RawUserDataEvent;
use crate::{ClientOrderId, OrderSide, OrderType, TimeInForce};

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

/// Fixed scale for asset balances: Binance always returns 8 decimal places.
pub const BALANCE_SCALE: i64 = 100_000_000;

/// How often a listen key should be renewed via `PUT /api/v3/userDataStream`.
/// Renewal resets the 60-minute expiry timer.
pub const LISTEN_KEY_RENEW_INTERVAL_NS: i64 = 30 * 60 * 1_000_000_000; // 30 min

/// Binance invalidates a listen key if it has not been renewed for 60 minutes.
pub const LISTEN_KEY_EXPIRY_NS: i64 = 60 * 60 * 1_000_000_000; // 60 min

// ---------------------------------------------------------------------------
// Error
// ---------------------------------------------------------------------------

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum NormalizerError {
    #[error("unknown order side: {0:?}")]
    UnknownSide(String),
    #[error("unknown order type: {0:?}")]
    UnknownOrderType(String),
    #[error("unknown time in force: {0:?}")]
    UnknownTimeInForce(String),
    #[error("invalid decimal: {0:?}")]
    InvalidDecimal(String),
}

// ---------------------------------------------------------------------------
// Execution type
// ---------------------------------------------------------------------------

/// What happened in this execution report (the "verb", Binance field `x`).
///
/// The SM feeds off `ExecutionType` to decide which [`crate::SmInput`] to
/// generate, rather than on the order-status field (`X`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ExecutionType {
    New,
    Canceled,
    Replaced,
    Rejected,
    Trade,
    Expired,
    TradePrevention,
    Unknown(String),
}

impl ExecutionType {
    pub fn from_raw(s: &str) -> Self {
        match s {
            "NEW"              => Self::New,
            "CANCELED"         => Self::Canceled,
            "REPLACED"         => Self::Replaced,
            "REJECTED"         => Self::Rejected,
            "TRADE"            => Self::Trade,
            "EXPIRED"          => Self::Expired,
            "TRADE_PREVENTION" => Self::TradePrevention,
            other              => Self::Unknown(other.to_string()),
        }
    }
}

// ---------------------------------------------------------------------------
// Normalized output types
// ---------------------------------------------------------------------------

/// Normalized order execution update (derived from `executionReport`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OrderUpdate {
    /// Exchange-assigned order ID.
    pub exchange_id: u64,
    /// Parsed `ClientOrderId` when the `c` field matches our `cc-XXXX-...`
    /// format; `None` for orders placed outside this system.
    pub cloid: Option<ClientOrderId>,
    /// Raw client order ID string (always present for correlation / logging).
    pub raw_cloid: String,
    pub symbol: String,
    pub side: OrderSide,
    pub order_type: OrderType,
    pub time_in_force: TimeInForce,
    /// What happened in this report.
    pub exec_type: ExecutionType,
    /// Raw `X` (order-status) string from Binance, passed through for callers
    /// that need the full status rather than just the execution verb.
    pub order_status_raw: String,
    /// Last fill quantity (scaled).  Zero for non-TRADE events.
    pub last_fill_qty: i64,
    /// Last fill price (scaled).  Zero for non-TRADE events.
    pub last_fill_price: i64,
    /// Cumulative filled quantity across all fills (scaled).
    pub cum_fill_qty: i64,
    /// Trade ID.  `Some(id)` only when `exec_type == Trade`.
    pub trade_id: Option<u64>,
    /// Reject reason.  `Some(reason)` when `exec_type == Rejected` and the
    /// reason string is not `"NONE"` or empty.
    pub reject_reason: Option<String>,
    /// Whether this fill was placed on the maker side of the order book.
    pub is_maker: bool,
    /// Event timestamp in nanoseconds (derived from Binance ms field `E`).
    pub event_time_ns: i64,
    /// Transaction timestamp in nanoseconds (derived from Binance ms field `T`).
    pub transaction_time_ns: i64,
}

/// Single-asset balance snapshot from an `outboundAccountPosition` event.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AssetBalance {
    pub asset: String,
    /// Free (unlocked) balance, scaled by [`BALANCE_SCALE`].
    pub free_scaled: i64,
    /// Locked balance (reserved in open orders), scaled by [`BALANCE_SCALE`].
    pub locked_scaled: i64,
}

/// Normalized `outboundAccountPosition` — pushed after any balance change.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AccountUpdate {
    pub balances: Vec<AssetBalance>,
    pub event_time_ns: i64,
    pub last_update_ns: i64,
}

/// Normalized `balanceUpdate` — deposit, withdrawal, or dust-sweep delta.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BalanceDelta {
    pub asset: String,
    /// Signed delta scaled by [`BALANCE_SCALE`].  Positive = credit, negative = debit.
    pub delta_scaled: i64,
    pub event_time_ns: i64,
    pub transaction_time_ns: i64,
}

/// All possible normalized outputs from a user data stream message.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NormalizedEvent {
    OrderUpdate(OrderUpdate),
    AccountUpdate(AccountUpdate),
    BalanceDelta(BalanceDelta),
    Unknown { event_type: String },
}

// ---------------------------------------------------------------------------
// Symbol scales
// ---------------------------------------------------------------------------

/// Price and quantity scale factors for one symbol.
#[derive(Debug, Clone)]
pub struct SymbolScales {
    pub price_scale: i64,
    pub qty_scale:   i64,
}

// ---------------------------------------------------------------------------
// Normalizer
// ---------------------------------------------------------------------------

/// Converts raw Binance user data stream events into normalized internal types.
///
/// Construct with [`Normalizer::new`] and pass a default scale for symbols
/// that have not been registered explicitly.  For production use, call
/// [`register_symbol`][Self::register_symbol] with per-symbol scales obtained
/// from the refdata crate after exchangeInfo is fetched.
pub struct Normalizer {
    default_price_scale: i64,
    default_qty_scale:   i64,
    symbol_scales:       HashMap<String, SymbolScales>,
}

impl Normalizer {
    pub fn new(default_price_scale: i64, default_qty_scale: i64) -> Self {
        Self {
            default_price_scale,
            default_qty_scale,
            symbol_scales: HashMap::new(),
        }
    }

    /// Register per-symbol scale factors.  Takes precedence over the defaults.
    pub fn register_symbol(
        &mut self,
        symbol: impl Into<String>,
        price_scale: i64,
        qty_scale: i64,
    ) {
        self.symbol_scales.insert(symbol.into(), SymbolScales { price_scale, qty_scale });
    }

    fn scales_for(&self, symbol: &str) -> SymbolScales {
        self.symbol_scales.get(symbol).cloned().unwrap_or(SymbolScales {
            price_scale: self.default_price_scale,
            qty_scale:   self.default_qty_scale,
        })
    }

    /// Normalize one raw user data stream event.
    pub fn normalize(&self, event: RawUserDataEvent) -> Result<NormalizedEvent, NormalizerError> {
        match event {
            RawUserDataEvent::ExecutionReport(rep) => {
                let scales = self.scales_for(&rep.symbol);

                let side       = parse_side(&rep.side_raw)?;
                let order_type = parse_order_type(&rep.order_type_raw)?;
                let tif        = parse_tif(&rep.time_in_force_raw)?;
                let exec_type  = ExecutionType::from_raw(&rep.exec_type_raw);

                let last_fill_qty   = parse_scaled(&rep.last_fill_qty_raw,   scales.qty_scale)?;
                let last_fill_price = parse_scaled(&rep.last_fill_price_raw, scales.price_scale)?;
                let cum_fill_qty    = parse_scaled(&rep.cum_fill_qty_raw,    scales.qty_scale)?;

                let trade_id = if rep.trade_id >= 0 {
                    Some(rep.trade_id as u64)
                } else {
                    None
                };

                let reject_reason = if rep.reject_reason_raw.is_empty()
                    || rep.reject_reason_raw == "NONE"
                {
                    None
                } else {
                    Some(rep.reject_reason_raw)
                };

                // Detect our cloid format: if parse_counter() succeeds it matches cc-XXXX-...
                let cloid_str = rep.client_order_id;
                let cand = ClientOrderId::new_raw(cloid_str.clone());
                let cloid = if cand.parse_counter().is_some() { Some(cand) } else { None };

                Ok(NormalizedEvent::OrderUpdate(OrderUpdate {
                    exchange_id:         rep.exchange_order_id,
                    cloid,
                    raw_cloid:           cloid_str,
                    symbol:              rep.symbol,
                    side,
                    order_type,
                    time_in_force:       tif,
                    exec_type,
                    order_status_raw:    rep.order_status_raw,
                    last_fill_qty,
                    last_fill_price,
                    cum_fill_qty,
                    trade_id,
                    reject_reason,
                    is_maker:            rep.is_maker,
                    event_time_ns:       rep.event_time_ms * 1_000_000,
                    transaction_time_ns: rep.transaction_time_ms * 1_000_000,
                }))
            }

            RawUserDataEvent::OutboundAccountPosition(pos) => {
                let balances = pos.balances.iter().map(|b| {
                    let free_scaled   = parse_scaled(&b.free,   BALANCE_SCALE)?;
                    let locked_scaled = parse_scaled(&b.locked, BALANCE_SCALE)?;
                    Ok(AssetBalance { asset: b.asset.clone(), free_scaled, locked_scaled })
                }).collect::<Result<Vec<_>, NormalizerError>>()?;

                Ok(NormalizedEvent::AccountUpdate(AccountUpdate {
                    balances,
                    event_time_ns:  pos.event_time_ms * 1_000_000,
                    last_update_ns: pos.last_update_ms * 1_000_000,
                }))
            }

            RawUserDataEvent::BalanceUpdate(upd) => {
                let delta_scaled = parse_scaled(&upd.delta_raw, BALANCE_SCALE)?;
                Ok(NormalizedEvent::BalanceDelta(BalanceDelta {
                    asset:               upd.asset,
                    delta_scaled,
                    event_time_ns:       upd.event_time_ms * 1_000_000,
                    transaction_time_ns: upd.clear_time_ms  * 1_000_000,
                }))
            }

            RawUserDataEvent::Unknown { event_type } => {
                Ok(NormalizedEvent::Unknown { event_type })
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Listen key lifecycle
// ---------------------------------------------------------------------------

/// Tracks the lifecycle of a Binance user data stream listen key.
///
/// Binance invalidates a listen key after 60 minutes without a renewal PUT.
/// This struct tracks when the key was last renewed and exposes two predicates:
///
/// | Predicate       | Meaning                                              | Action                                   |
/// |-----------------|------------------------------------------------------|------------------------------------------|
/// | `needs_renewal` | ≥ 30 min since last renewal                          | `PUT /api/v3/userDataStream?listenKey=…` |
/// | `is_expired`    | ≥ 60 min since last renewal (key likely invalidated) | Obtain a new key and reconnect WS        |
#[derive(Debug, Clone)]
pub struct ListenKeyState {
    key:             String,
    last_renewed_ns: i64,
}

impl ListenKeyState {
    /// Create state for a freshly-issued listen key (`now_ns` is the issue time).
    pub fn new(key: impl Into<String>, now_ns: i64) -> Self {
        Self { key: key.into(), last_renewed_ns: now_ns }
    }

    /// The listen key string (embed in the WS URL: `.../ws/{key}`).
    pub fn key(&self) -> &str {
        &self.key
    }

    /// True when the key should be renewed (`PUT /api/v3/userDataStream`).
    pub fn needs_renewal(&self, now_ns: i64) -> bool {
        now_ns.saturating_sub(self.last_renewed_ns) >= LISTEN_KEY_RENEW_INTERVAL_NS
    }

    /// True when the key has likely expired (no renewal for ≥ 60 min).
    ///
    /// When this fires, obtain a **new** listen key and reconnect the WebSocket
    /// rather than just renewing.
    pub fn is_expired(&self, now_ns: i64) -> bool {
        now_ns.saturating_sub(self.last_renewed_ns) >= LISTEN_KEY_EXPIRY_NS
    }

    /// Call after a successful `PUT /api/v3/userDataStream` response.
    pub fn on_renewed(&mut self, now_ns: i64) {
        self.last_renewed_ns = now_ns;
    }

    /// Elapsed time since the last renewal, in nanoseconds.
    pub fn age_ns(&self, now_ns: i64) -> i64 {
        now_ns.saturating_sub(self.last_renewed_ns)
    }
}

// ---------------------------------------------------------------------------
// Helpers (pub so callers can reuse the decimal parser)
// ---------------------------------------------------------------------------

fn parse_side(raw: &str) -> Result<OrderSide, NormalizerError> {
    match raw {
        "BUY"  => Ok(OrderSide::Buy),
        "SELL" => Ok(OrderSide::Sell),
        other  => Err(NormalizerError::UnknownSide(other.to_string())),
    }
}

fn parse_order_type(raw: &str) -> Result<OrderType, NormalizerError> {
    match raw {
        "LIMIT"  => Ok(OrderType::Limit),
        "MARKET" => Ok(OrderType::Market),
        other    => Err(NormalizerError::UnknownOrderType(other.to_string())),
    }
}

fn parse_tif(raw: &str) -> Result<TimeInForce, NormalizerError> {
    match raw {
        "GTC" => Ok(TimeInForce::GoodTillCancel),
        "IOC" => Ok(TimeInForce::ImmediateOrCancel),
        "FOK" => Ok(TimeInForce::FillOrKill),
        other => Err(NormalizerError::UnknownTimeInForce(other.to_string())),
    }
}

/// Parse a decimal string into a scaled integer.
///
/// `scale` must be a power of 10 (e.g. `100_000_000` for 8 decimal places).
/// Fractional digits beyond the scale precision are **truncated** (not rounded).
///
/// ```
/// # use connector_order_gateway::parse_scaled;
/// assert_eq!(parse_scaled("50000.00000000", 100_000_000).unwrap(), 5_000_000_000_000);
/// assert_eq!(parse_scaled("0.00500000",     100_000_000).unwrap(), 500_000);
/// assert_eq!(parse_scaled("1.5",            100_000_000).unwrap(), 150_000_000);
/// ```
pub fn parse_scaled(decimal: &str, scale: i64) -> Result<i64, NormalizerError> {
    let s = decimal.trim();
    if s.is_empty() {
        return Err(NormalizerError::InvalidDecimal(decimal.to_string()));
    }

    let (int_str, frac_str) = match s.find('.') {
        Some(pos) => (&s[..pos], &s[pos + 1..]),
        None      => (s, ""),
    };

    let int_val: i64 = if int_str.is_empty() {
        0
    } else {
        int_str.parse::<i64>().map_err(|_| NormalizerError::InvalidDecimal(decimal.to_string()))?
    };

    // Number of decimal digits the scale represents (scale must be a power of 10).
    let mut digits = 0usize;
    let mut tmp = scale;
    while tmp > 1 { tmp /= 10; digits += 1; }

    let frac_val: i64 = if frac_str.is_empty() {
        0
    } else {
        let available = frac_str.len().min(digits);
        let padding   = digits - available;
        let mut frac_s = frac_str[..available].to_string();
        for _ in 0..padding { frac_s.push('0'); }
        frac_s.parse::<i64>().map_err(|_| NormalizerError::InvalidDecimal(decimal.to_string()))?
    };

    Ok(int_val * scale + frac_val)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::user_stream::parse as parse_raw;

    fn norm() -> Normalizer {
        Normalizer::new(100_000_000, 100_000_000)
    }

    fn normalize(json: &str) -> NormalizedEvent {
        let raw = parse_raw(json.as_bytes()).unwrap();
        norm().normalize(raw).unwrap()
    }

    // -----------------------------------------------------------------------
    // parse_scaled
    // -----------------------------------------------------------------------

    #[test]
    fn parse_scaled_integer_only() {
        assert_eq!(parse_scaled("1", 100_000_000).unwrap(), 100_000_000);
    }

    #[test]
    fn parse_scaled_zero_decimal() {
        assert_eq!(parse_scaled("0.00000000", 100_000_000).unwrap(), 0);
    }

    #[test]
    fn parse_scaled_8_decimals() {
        assert_eq!(parse_scaled("1.00000000", 100_000_000).unwrap(), 100_000_000);
    }

    #[test]
    fn parse_scaled_fractional() {
        assert_eq!(parse_scaled("0.00500000", 100_000_000).unwrap(), 500_000);
    }

    #[test]
    fn parse_scaled_large_integer() {
        // 50000 * 10^8 = 5_000_000_000_000
        assert_eq!(parse_scaled("50000.00000000", 100_000_000).unwrap(), 5_000_000_000_000);
    }

    #[test]
    fn parse_scaled_truncates_extra_decimals() {
        // "1.123456789" has 9 dp; scale is 10^8 → truncate last digit → 112_345_678
        assert_eq!(parse_scaled("1.123456789", 100_000_000).unwrap(), 112_345_678);
    }

    #[test]
    fn parse_scaled_pads_short_fraction() {
        // "1.5" with scale 10^8 → "15000000" → 150_000_000
        assert_eq!(parse_scaled("1.5", 100_000_000).unwrap(), 150_000_000);
    }

    #[test]
    fn parse_scaled_empty_string_is_error() {
        assert!(parse_scaled("", 100_000_000).is_err());
    }

    // -----------------------------------------------------------------------
    // ExecutionType
    // -----------------------------------------------------------------------

    #[test]
    fn execution_type_all_variants() {
        assert_eq!(ExecutionType::from_raw("NEW"),              ExecutionType::New);
        assert_eq!(ExecutionType::from_raw("CANCELED"),         ExecutionType::Canceled);
        assert_eq!(ExecutionType::from_raw("REPLACED"),         ExecutionType::Replaced);
        assert_eq!(ExecutionType::from_raw("REJECTED"),         ExecutionType::Rejected);
        assert_eq!(ExecutionType::from_raw("TRADE"),            ExecutionType::Trade);
        assert_eq!(ExecutionType::from_raw("EXPIRED"),          ExecutionType::Expired);
        assert_eq!(ExecutionType::from_raw("TRADE_PREVENTION"), ExecutionType::TradePrevention);
        assert_eq!(
            ExecutionType::from_raw("FUTURE_TYPE"),
            ExecutionType::Unknown("FUTURE_TYPE".to_string()),
        );
    }

    // -----------------------------------------------------------------------
    // executionReport normalization
    // -----------------------------------------------------------------------

    // Compact execution report fixtures (extra Binance fields present to
    // ensure serde ignores them gracefully).
    const NEW_EXEC: &str = r#"{"e":"executionReport","E":1499405658658,"s":"BTCUSDT",
        "c":"cc-0001-0000000000000042","S":"BUY","o":"LIMIT","f":"GTC",
        "q":"0.10000000","p":"50000.00000000","P":"0","F":"0","g":-1,"C":"",
        "x":"NEW","X":"NEW","r":"NONE","i":4293153,"l":"0.00000000",
        "z":"0.00000000","L":"0.00000000","n":"0","N":null,"T":1499405658657,
        "t":-1,"I":0,"w":true,"m":false,"M":false,"O":0,"Z":"0","Y":"0",
        "Q":"0","W":0,"V":"NONE"}"#;

    const TRADE_EXEC: &str = r#"{"e":"executionReport","E":1499405658659,"s":"BTCUSDT",
        "c":"cc-0001-0000000000000042","S":"BUY","o":"LIMIT","f":"GTC",
        "q":"0.10000000","p":"50000.00000000","P":"0","F":"0","g":-1,"C":"",
        "x":"TRADE","X":"FILLED","r":"NONE","i":4293153,"l":"0.10000000",
        "z":"0.10000000","L":"50000.00000000","n":"0.00002500","N":"BTC",
        "T":1499405658657,"t":9876543,"I":0,"w":false,"m":false,"M":true,
        "O":0,"Z":"5000","Y":"5000","Q":"0","W":0,"V":"NONE"}"#;

    const REJECTED_EXEC: &str = r#"{"e":"executionReport","E":1499405658800,"s":"BTCUSDT",
        "c":"cc-0001-0000000000000043","S":"BUY","o":"LIMIT","f":"GTC",
        "q":"0.00010000","p":"1.00000000","P":"0","F":"0","g":-1,"C":"",
        "x":"REJECTED","X":"REJECTED","r":"PRICE_FILTER","i":0,
        "l":"0.00000000","z":"0.00000000","L":"0.00000000","n":"0","N":null,
        "T":1499405658799,"t":-1,"I":0,"w":false,"m":false,"M":false,
        "O":0,"Z":"0","Y":"0","Q":"0","W":0,"V":"NONE"}"#;

    const CANCELED_EXEC: &str = r#"{"e":"executionReport","E":1499405658700,"s":"BTCUSDT",
        "c":"cc-0001-0000000000000042","S":"SELL","o":"LIMIT","f":"GTC",
        "q":"0.05000000","p":"51000.00000000","P":"0","F":"0","g":-1,
        "C":"cc-0001-0000000000000042","x":"CANCELED","X":"CANCELED","r":"NONE",
        "i":4293154,"l":"0.00000000","z":"0.00000000","L":"0.00000000",
        "n":"0","N":null,"T":1499405658699,"t":-1,"I":0,"w":false,"m":false,
        "M":false,"O":0,"Z":"0","Y":"0","Q":"0","W":0,"V":"NONE"}"#;

    const EXPIRED_EXEC: &str = r#"{"e":"executionReport","E":1499405659000,"s":"BTCUSDT",
        "c":"cc-0001-0000000000000044","S":"BUY","o":"LIMIT","f":"IOC",
        "q":"1.00000000","p":"50000.00000000","P":"0","F":"0","g":-1,"C":"",
        "x":"EXPIRED","X":"EXPIRED","r":"NONE","i":4293155,"l":"0.00000000",
        "z":"0.00000000","L":"0.00000000","n":"0","N":null,"T":1499405659000,
        "t":-1,"I":0,"w":false,"m":false,"M":false,"O":0,"Z":"0","Y":"0",
        "Q":"0","W":0,"V":"NONE"}"#;

    #[test]
    fn normalize_new_exec_produces_order_update() {
        let NormalizedEvent::OrderUpdate(upd) = normalize(NEW_EXEC) else { panic!() };
        assert_eq!(upd.exec_type, ExecutionType::New);
        assert_eq!(upd.exchange_id, 4293153);
        assert_eq!(upd.symbol, "BTCUSDT");
        assert_eq!(upd.side, OrderSide::Buy);
        assert_eq!(upd.order_type, OrderType::Limit);
        assert_eq!(upd.time_in_force, TimeInForce::GoodTillCancel);
        assert_eq!(upd.last_fill_qty,   0);
        assert_eq!(upd.last_fill_price, 0);
        assert_eq!(upd.cum_fill_qty,    0);
        assert!(upd.trade_id.is_none());
        assert!(upd.reject_reason.is_none());
        assert_eq!(upd.event_time_ns,       1499405658658 * 1_000_000);
        assert_eq!(upd.transaction_time_ns, 1499405658657 * 1_000_000);
    }

    #[test]
    fn normalize_trade_exec_has_fill_and_trade_id() {
        let NormalizedEvent::OrderUpdate(upd) = normalize(TRADE_EXEC) else { panic!() };
        assert_eq!(upd.exec_type, ExecutionType::Trade);
        assert_eq!(upd.last_fill_qty,   10_000_000);    // 0.1 * 10^8
        assert_eq!(upd.last_fill_price, 5_000_000_000_000); // 50000 * 10^8
        assert_eq!(upd.cum_fill_qty,    10_000_000);
        assert_eq!(upd.trade_id, Some(9876543));
        assert!(upd.reject_reason.is_none());
    }

    #[test]
    fn normalize_rejected_exec_has_reason_and_no_trade_id() {
        let NormalizedEvent::OrderUpdate(upd) = normalize(REJECTED_EXEC) else { panic!() };
        assert_eq!(upd.exec_type, ExecutionType::Rejected);
        assert_eq!(upd.reject_reason, Some("PRICE_FILTER".to_string()));
        assert!(upd.trade_id.is_none());
    }

    #[test]
    fn normalize_canceled_exec_sets_side_and_no_trade_id() {
        let NormalizedEvent::OrderUpdate(upd) = normalize(CANCELED_EXEC) else { panic!() };
        assert_eq!(upd.exec_type, ExecutionType::Canceled);
        assert_eq!(upd.side, OrderSide::Sell);
        assert!(upd.trade_id.is_none());
        assert!(upd.reject_reason.is_none());
    }

    #[test]
    fn normalize_expired_exec_uses_ioc_tif() {
        let NormalizedEvent::OrderUpdate(upd) = normalize(EXPIRED_EXEC) else { panic!() };
        assert_eq!(upd.exec_type, ExecutionType::Expired);
        assert_eq!(upd.time_in_force, TimeInForce::ImmediateOrCancel);
    }

    #[test]
    fn cloid_is_parsed_for_our_format() {
        let NormalizedEvent::OrderUpdate(upd) = normalize(NEW_EXEC) else { panic!() };
        assert!(upd.cloid.is_some());
        assert_eq!(upd.raw_cloid, "cc-0001-0000000000000042");
    }

    #[test]
    fn cloid_is_none_for_external_orders() {
        let json = TRADE_EXEC.replace("cc-0001-0000000000000042", "binance-web-ui-abc123");
        let raw = parse_raw(json.as_bytes()).unwrap();
        let NormalizedEvent::OrderUpdate(upd) = norm().normalize(raw).unwrap() else { panic!() };
        assert!(upd.cloid.is_none());
        assert_eq!(upd.raw_cloid, "binance-web-ui-abc123");
    }

    #[test]
    fn normalizer_uses_registered_symbol_scales() {
        let mut n = Normalizer::new(100_000_000, 100_000_000);
        // Override BTCUSDT price scale to 10^2 (contrived but tests dispatch).
        n.register_symbol("BTCUSDT", 100, 100_000_000);
        let raw = parse_raw(TRADE_EXEC.as_bytes()).unwrap();
        let NormalizedEvent::OrderUpdate(upd) = n.normalize(raw).unwrap() else { panic!() };
        // 50000.00 * 100 = 5_000_000
        assert_eq!(upd.last_fill_price, 5_000_000);
    }

    // -----------------------------------------------------------------------
    // outboundAccountPosition
    // -----------------------------------------------------------------------

    const ACCOUNT_POS: &str = r#"{"e":"outboundAccountPosition","E":1564034571105,
        "u":1564034571073,"B":[
            {"a":"ETH","f":"10.50000000","l":"0.00000000"},
            {"a":"BTC","f":"0.00100000","l":"0.00050000"}
        ]}"#;

    #[test]
    fn normalize_account_position_parses_balances() {
        let NormalizedEvent::AccountUpdate(upd) = normalize(ACCOUNT_POS) else { panic!() };
        assert_eq!(upd.balances.len(), 2);
        // ETH: free 10.5 → 10.5 * 10^8 = 1_050_000_000
        assert_eq!(upd.balances[0].asset, "ETH");
        assert_eq!(upd.balances[0].free_scaled,   1_050_000_000);
        assert_eq!(upd.balances[0].locked_scaled, 0);
        // BTC: free 0.001 → 100_000; locked 0.0005 → 50_000
        assert_eq!(upd.balances[1].asset, "BTC");
        assert_eq!(upd.balances[1].free_scaled,   100_000);
        assert_eq!(upd.balances[1].locked_scaled,  50_000);
        assert_eq!(upd.event_time_ns,  1564034571105 * 1_000_000);
        assert_eq!(upd.last_update_ns, 1564034571073 * 1_000_000);
    }

    // -----------------------------------------------------------------------
    // balanceUpdate
    // -----------------------------------------------------------------------

    const BALANCE_UPD: &str = r#"{"e":"balanceUpdate","E":1573200697110,
        "a":"BTC","d":"100.00000000","T":1573200697068}"#;

    #[test]
    fn normalize_balance_update_produces_delta() {
        let NormalizedEvent::BalanceDelta(d) = normalize(BALANCE_UPD) else { panic!() };
        assert_eq!(d.asset, "BTC");
        assert_eq!(d.delta_scaled, 10_000_000_000); // 100 * 10^8
        assert_eq!(d.event_time_ns,       1573200697110 * 1_000_000);
        assert_eq!(d.transaction_time_ns, 1573200697068 * 1_000_000);
    }

    // -----------------------------------------------------------------------
    // Unknown event passthrough
    // -----------------------------------------------------------------------

    #[test]
    fn normalize_unknown_event_is_passthrough() {
        let json = r#"{"e":"listStatus","E":1234567890000}"#;
        let raw = parse_raw(json.as_bytes()).unwrap();
        let ev = norm().normalize(raw).unwrap();
        assert_eq!(ev, NormalizedEvent::Unknown { event_type: "listStatus".to_string() });
    }

    // -----------------------------------------------------------------------
    // ListenKeyState
    // -----------------------------------------------------------------------

    const T0: i64 = 0;

    #[test]
    fn listen_key_does_not_need_renewal_immediately() {
        let state = ListenKeyState::new("test-key", T0);
        assert!(!state.needs_renewal(T0));
        assert!(!state.is_expired(T0));
    }

    #[test]
    fn listen_key_needs_renewal_at_interval_boundary() {
        let state = ListenKeyState::new("test-key", T0);
        assert!(state.needs_renewal(T0 + LISTEN_KEY_RENEW_INTERVAL_NS));
    }

    #[test]
    fn listen_key_does_not_need_renewal_one_ns_before() {
        let state = ListenKeyState::new("test-key", T0);
        assert!(!state.needs_renewal(T0 + LISTEN_KEY_RENEW_INTERVAL_NS - 1));
    }

    #[test]
    fn listen_key_is_expired_at_60_min() {
        let state = ListenKeyState::new("test-key", T0);
        assert!(state.is_expired(T0 + LISTEN_KEY_EXPIRY_NS));
    }

    #[test]
    fn listen_key_is_not_expired_one_ns_before_60_min() {
        let state = ListenKeyState::new("test-key", T0);
        assert!(!state.is_expired(T0 + LISTEN_KEY_EXPIRY_NS - 1));
    }

    #[test]
    fn listen_key_renewal_resets_timer() {
        let mut state = ListenKeyState::new("test-key", T0);
        let t_renew = T0 + LISTEN_KEY_RENEW_INTERVAL_NS;
        state.on_renewed(t_renew);
        // At original 60-min mark the key is NOT yet expired (timer was reset).
        assert!(!state.is_expired(T0 + LISTEN_KEY_EXPIRY_NS));
        // It IS expired 60 min after the renewal time.
        assert!(state.is_expired(t_renew + LISTEN_KEY_EXPIRY_NS));
    }

    #[test]
    fn listen_key_accessor_returns_key_string() {
        let state = ListenKeyState::new("my-listen-key-xyz", T0);
        assert_eq!(state.key(), "my-listen-key-xyz");
    }

    #[test]
    fn listen_key_age_ns_equals_elapsed_time() {
        let state = ListenKeyState::new("k", T0);
        assert_eq!(state.age_ns(T0 + 1_000_000_000), 1_000_000_000);
    }
}
