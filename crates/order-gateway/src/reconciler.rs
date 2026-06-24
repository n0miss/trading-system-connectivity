//! REST reconciliation — raw Binance REST response types, a [`Reconciler`] that
//! converts them into normalized actions/outcomes, and a
//! [`ReconciliationScheduler`] that tracks when each reconciliation trigger
//! (startup, reconnect, periodic) is due.
//!
//! # Trigger → action mapping
//!
//! | Trigger         | What to fetch                         | What to call                                    |
//! |-----------------|---------------------------------------|-------------------------------------------------|
//! | Startup         | `GET /api/v3/openOrders`, account     | `reconcile_open_orders`, `reconcile_account`    |
//! | Reconnect       | Same as startup                       | Same as startup                                 |
//! | Unknown status  | `GET /api/v3/order?origClientOrderId` | `reconcile_order_status` → `SmInput::StatusCheckResult` |
//! | Periodic        | `GET /api/v3/account`                 | `reconcile_account`                             |
//!
//! The actual HTTP calls are the caller's responsibility; this module only
//! parses the responses and produces typed actions.
//!
//! # Integration sketch
//!
//! ```rust,ignore
//! // 1. Startup
//! let raw_orders: Vec<RawRestOrder> = serde_json::from_slice(&http_get("/openOrders"))?;
//! let raw_account: RawRestAccount   = serde_json::from_slice(&http_get("/account"))?;
//! let tracked = gateway.non_terminal_orders().map(|o| o.cloid.clone()).collect();
//! for action in reconciler.reconcile_open_orders(&raw_orders, &tracked)? {
//!     match action { ... }
//! }
//!
//! // 2. Unknown-status check (driven by engine.tick())
//! let raw_order: RawRestOrder = serde_json::from_slice(&http_get("/order?origClientOrderId=..."))?;
//! let outcome = reconciler.reconcile_order_status(&raw_order, None)?;
//! engine.process(&cloid, SmInput::StatusCheckResult { outcome, now_ns })?;
//! ```

use std::collections::{HashMap, HashSet};

use serde::Deserialize;

use crate::machine::StatusCheckOutcome;
use crate::normalizer::{parse_scaled, AssetBalance, NormalizerError, SymbolScales};
use crate::ClientOrderId;

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

/// Default periodic reconciliation interval.
pub const DEFAULT_PERIODIC_INTERVAL_NS: i64 = 60 * 1_000_000_000; // 60 s

// ---------------------------------------------------------------------------
// Raw REST JSON types
// ---------------------------------------------------------------------------

/// One order returned by `GET /api/v3/openOrders` or `GET /api/v3/order`.
///
/// Binance uses camelCase for all REST field names.  Extra fields not captured
/// here (e.g. `stopPrice`, `icebergQty`, `selfTradePreventionMode`) are
/// silently discarded by serde.
#[derive(Debug, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct RawRestOrder {
    pub symbol: String,
    /// Exchange-assigned numeric order ID.
    pub order_id: u64,
    /// Client order ID (our `cc-XXXX-…` format if we placed it).
    pub client_order_id: String,
    /// Original order quantity (decimal string).
    pub orig_qty: String,
    /// Cumulative executed quantity (decimal string).
    pub executed_qty: String,
    /// Current order status: `"NEW" | "PARTIALLY_FILLED" | "FILLED" | "CANCELED" | …`
    pub status: String,
    /// Time in force: `"GTC" | "IOC" | "FOK"`.
    pub time_in_force: String,
    /// Order type: `"LIMIT" | "MARKET"`.
    #[serde(rename = "type")]
    pub order_type: String,
    /// Order side: `"BUY" | "SELL"`.
    pub side: String,
    /// Order creation time (unix ms).
    #[serde(rename = "time")]
    pub created_ms: i64,
    /// Last update time (unix ms).
    pub update_time: i64,
}

/// One trade returned by `GET /api/v3/myTrades`.
#[derive(Debug, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct RawRestTrade {
    pub symbol: String,
    /// Exchange trade ID.
    pub id: u64,
    /// Exchange order ID this trade belongs to.
    pub order_id: u64,
    /// Fill price (decimal string).
    pub price: String,
    /// Fill quantity (decimal string).
    pub qty: String,
    /// Trade time (unix ms).
    #[serde(rename = "time")]
    pub time_ms: i64,
    pub is_buyer: bool,
    pub is_maker: bool,
}

/// One balance entry in `GET /api/v3/account`.
#[derive(Debug, Deserialize, PartialEq)]
pub struct RawRestBalance {
    pub asset: String,
    pub free: String,
    pub locked: String,
}

/// Response from `GET /api/v3/account`.
#[derive(Debug, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct RawRestAccount {
    pub update_time: i64,
    pub balances: Vec<RawRestBalance>,
}

// ---------------------------------------------------------------------------
// REST order status
// ---------------------------------------------------------------------------

/// Normalized representation of the `status` field in a REST order object.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RestOrderStatus {
    New,
    PartiallyFilled,
    Filled,
    Canceled,
    PendingCancel,
    Rejected,
    Expired,
    ExpiredInMatch,
    Unknown(String),
}

impl RestOrderStatus {
    pub fn from_raw(s: &str) -> Self {
        match s {
            "NEW" => Self::New,
            "PARTIALLY_FILLED" => Self::PartiallyFilled,
            "FILLED" => Self::Filled,
            "CANCELED" => Self::Canceled,
            "PENDING_CANCEL" => Self::PendingCancel,
            "REJECTED" => Self::Rejected,
            "EXPIRED" => Self::Expired,
            "EXPIRED_IN_MATCH" => Self::ExpiredInMatch,
            other => Self::Unknown(other.to_string()),
        }
    }

    /// True when the order is still open / active on the exchange.
    pub fn is_live(&self) -> bool {
        matches!(
            self,
            Self::New | Self::PartiallyFilled | Self::PendingCancel
        )
    }

    /// True when the order has reached a final state.
    pub fn is_terminal(&self) -> bool {
        matches!(
            self,
            Self::Filled | Self::Canceled | Self::Rejected | Self::Expired | Self::ExpiredInMatch
        )
    }
}

// ---------------------------------------------------------------------------
// Normalized order (produced from a REST response)
// ---------------------------------------------------------------------------

/// A REST order parsed and normalized into scaled integers.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReconcileOrder {
    /// Exchange-assigned order ID.
    pub exchange_id: u64,
    /// Parsed `ClientOrderId` if `client_order_id` matches our `cc-XXXX-…` format.
    pub cloid: Option<ClientOrderId>,
    /// Raw client order ID string (always set for logging/correlation).
    pub raw_cloid: String,
    pub symbol: String,
    pub status: RestOrderStatus,
    /// Original order quantity (scaled).
    pub orig_qty: i64,
    /// Cumulative executed quantity (scaled).
    pub executed_qty: i64,
    /// Order creation timestamp (ns).
    pub created_ns: i64,
    /// Last-update timestamp (ns).
    pub updated_ns: i64,
}

/// A fill parsed from `GET /api/v3/myTrades`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReconcileFill {
    pub trade_id: u64,
    pub exchange_order_id: u64,
    pub symbol: String,
    /// Fill quantity (scaled).
    pub qty: i64,
    /// Fill price (scaled).
    pub price: i64,
    pub time_ns: i64,
}

// ---------------------------------------------------------------------------
// Reconcile actions
// ---------------------------------------------------------------------------

/// Actions produced by comparing the exchange's open-order list with the
/// gateway's tracked orders.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReconcileAction {
    /// A tracked order is confirmed still live on the exchange.
    OrderStillLive {
        cloid: ClientOrderId,
        exchange_id: u64,
    },

    /// An open order found on the exchange that the gateway is not tracking.
    ///
    /// This may be:
    /// - An orphan from a previous process run (`cloid.is_some()` if in our format).
    /// - An order placed externally via UI or API (`cloid.is_none()`).
    UnexpectedOrder(ReconcileOrder),

    /// A non-terminal order tracked by the gateway is absent from the exchange's
    /// open-order list, meaning it completed without a WS notification being
    /// received.  Caller should query the individual order for its final status.
    OrderVanished { cloid: ClientOrderId },
}

// ---------------------------------------------------------------------------
// Reconciliation scheduler
// ---------------------------------------------------------------------------

/// Identifies the kind of reconciliation that is due.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReconcileRequest {
    /// Full reconciliation at process startup.
    Startup,
    /// Full reconciliation after a WS reconnect.
    Reconnect,
    /// Lightweight periodic balance/order check.
    Periodic,
}

/// Tracks which reconciliation triggers are pending and when the next periodic
/// check is due.
///
/// # Usage pattern
///
/// ```rust,ignore
/// let mut sched = ReconciliationScheduler::new(DEFAULT_PERIODIC_INTERVAL_NS);
/// sched.on_startup(now_ns);
///
/// loop {
///     let now_ns = clock.now_ns();
///     for req in sched.tick(now_ns) {
///         if !in_flight.contains(&req) {
///             in_flight.insert(req);
///             spawn_reconciliation(req);
///         }
///     }
///     // When a reconciliation finishes:
///     sched.on_completed(req, now_ns);
///     in_flight.remove(&req);
/// }
/// ```
///
/// [`tick`][Self::tick] returns the same request on every call until
/// [`on_completed`][Self::on_completed] is called.  Callers are responsible
/// for not dispatching duplicate in-flight requests.
pub struct ReconciliationScheduler {
    periodic_interval_ns: i64,
    startup_pending: bool,
    reconnect_pending: bool,
    /// When the next periodic check is due (None = timer not yet started).
    next_periodic_ns: Option<i64>,
}

impl ReconciliationScheduler {
    pub fn new(periodic_interval_ns: i64) -> Self {
        Self {
            periodic_interval_ns,
            startup_pending: false,
            reconnect_pending: false,
            next_periodic_ns: None,
        }
    }

    /// Call once at process startup.
    ///
    /// Marks a startup reconciliation as pending and starts the periodic timer.
    pub fn on_startup(&mut self, now_ns: i64) {
        self.startup_pending = true;
        self.next_periodic_ns = Some(now_ns + self.periodic_interval_ns);
    }

    /// Call each time the WebSocket reconnects.
    ///
    /// Marks a reconnect reconciliation as pending.  If the periodic timer has
    /// not been started yet (startup hasn't been called), this starts it.
    pub fn on_reconnect(&mut self, now_ns: i64) {
        self.reconnect_pending = true;
        if self.next_periodic_ns.is_none() {
            self.next_periodic_ns = Some(now_ns + self.periodic_interval_ns);
        }
    }

    /// Returns the set of reconciliation requests that are currently due.
    ///
    /// Does **not** clear the requests — call [`on_completed`][Self::on_completed]
    /// after each reconciliation finishes.
    pub fn tick(&self, now_ns: i64) -> Vec<ReconcileRequest> {
        let mut requests = Vec::new();
        if self.startup_pending {
            requests.push(ReconcileRequest::Startup);
        }
        if self.reconnect_pending {
            requests.push(ReconcileRequest::Reconnect);
        }
        if let Some(due_ns) = self.next_periodic_ns {
            if now_ns >= due_ns {
                requests.push(ReconcileRequest::Periodic);
            }
        }
        requests
    }

    /// Mark a reconciliation as complete.
    ///
    /// - `Startup` / `Reconnect` — clears the pending flag.
    /// - `Periodic` — resets the timer so the next periodic fires
    ///   `periodic_interval_ns` after `now_ns`.
    pub fn on_completed(&mut self, kind: ReconcileRequest, now_ns: i64) {
        match kind {
            ReconcileRequest::Startup => self.startup_pending = false,
            ReconcileRequest::Reconnect => self.reconnect_pending = false,
            ReconcileRequest::Periodic => {
                self.next_periodic_ns = Some(now_ns + self.periodic_interval_ns);
            }
        }
    }

    pub fn startup_pending(&self) -> bool {
        self.startup_pending
    }
    pub fn reconnect_pending(&self) -> bool {
        self.reconnect_pending
    }
    pub fn periodic_interval_ns(&self) -> i64 {
        self.periodic_interval_ns
    }
}

// ---------------------------------------------------------------------------
// Reconciler
// ---------------------------------------------------------------------------

/// Converts raw Binance REST responses into reconciliation actions and SM inputs.
pub struct Reconciler {
    default_price_scale: i64,
    default_qty_scale: i64,
    symbol_scales: HashMap<String, SymbolScales>,
}

impl Reconciler {
    pub fn new(default_price_scale: i64, default_qty_scale: i64) -> Self {
        Self {
            default_price_scale,
            default_qty_scale,
            symbol_scales: HashMap::new(),
        }
    }

    pub fn register_symbol(&mut self, symbol: impl Into<String>, price_scale: i64, qty_scale: i64) {
        self.symbol_scales.insert(
            symbol.into(),
            SymbolScales {
                price_scale,
                qty_scale,
            },
        );
    }

    fn scales_for(&self, symbol: &str) -> SymbolScales {
        self.symbol_scales
            .get(symbol)
            .cloned()
            .unwrap_or(SymbolScales {
                price_scale: self.default_price_scale,
                qty_scale: self.default_qty_scale,
            })
    }

    // -----------------------------------------------------------------------
    // Open-order reconciliation (startup / reconnect)
    // -----------------------------------------------------------------------

    /// Compare the exchange's open-order list against the gateway's tracked orders.
    ///
    /// `raw_orders` should be from `GET /api/v3/openOrders` (all symbols or one symbol).
    /// `tracked` should be all non-terminal cloids from the gateway, scoped to the
    /// same symbol universe as `raw_orders`.
    ///
    /// # Returns
    ///
    /// - [`ReconcileAction::OrderStillLive`] for each exchange order that matches a
    ///   tracked cloid (both sides know about it).
    /// - [`ReconcileAction::UnexpectedOrder`] for each exchange order not in `tracked`
    ///   (orphan from previous run or external order).
    /// - [`ReconcileAction::OrderVanished`] for each tracked cloid absent from the
    ///   exchange list (completed without WS notification — caller should query it).
    pub fn reconcile_open_orders(
        &self,
        raw_orders: &[RawRestOrder],
        tracked: &HashSet<ClientOrderId>,
    ) -> Result<Vec<ReconcileAction>, NormalizerError> {
        let mut actions = Vec::new();
        let mut seen = HashSet::new();

        for raw in raw_orders {
            let order = self.normalize_order(raw)?;
            if let Some(ref cloid) = order.cloid {
                if tracked.contains(cloid) {
                    seen.insert(cloid.clone());
                    actions.push(ReconcileAction::OrderStillLive {
                        cloid: cloid.clone(),
                        exchange_id: order.exchange_id,
                    });
                    continue;
                }
            }
            actions.push(ReconcileAction::UnexpectedOrder(order));
        }

        for cloid in tracked {
            if !seen.contains(cloid) {
                actions.push(ReconcileAction::OrderVanished {
                    cloid: cloid.clone(),
                });
            }
        }

        Ok(actions)
    }

    // -----------------------------------------------------------------------
    // Single-order status check (unknown-status path)
    // -----------------------------------------------------------------------

    /// Derive a [`StatusCheckOutcome`] from a `GET /api/v3/order` response.
    ///
    /// Pass `raw_trades` (from `GET /api/v3/myTrades` filtered to this order's
    /// `orderId`) when the order is FILLED and you need fill details in the
    /// outcome.  Pass `None` to produce `Filled { fills: [] }` and let the SM
    /// rely on fills already buffered from the WS stream.
    pub fn reconcile_order_status(
        &self,
        raw_order: &RawRestOrder,
        raw_trades: Option<&[RawRestTrade]>,
    ) -> Result<StatusCheckOutcome, NormalizerError> {
        let exchange_id = raw_order.order_id;
        let status = RestOrderStatus::from_raw(&raw_order.status);
        match status {
            RestOrderStatus::New
            | RestOrderStatus::PartiallyFilled
            | RestOrderStatus::PendingCancel => Ok(StatusCheckOutcome::Live { exchange_id }),

            RestOrderStatus::Filled => {
                let fills = if let Some(trades) = raw_trades {
                    self.parse_fills(trades)?
                } else {
                    vec![]
                };
                Ok(StatusCheckOutcome::Filled { exchange_id, fills })
            }

            RestOrderStatus::Canceled => Ok(StatusCheckOutcome::Cancelled { exchange_id }),

            RestOrderStatus::Rejected => Ok(StatusCheckOutcome::Rejected {
                reason: "order rejected by exchange (reason unavailable from REST)".into(),
            }),

            RestOrderStatus::Expired | RestOrderStatus::ExpiredInMatch => {
                Ok(StatusCheckOutcome::Expired { exchange_id })
            }

            RestOrderStatus::Unknown(s) => Ok(StatusCheckOutcome::Error {
                reason: format!("unrecognised REST order status: {s}"),
            }),
        }
    }

    // -----------------------------------------------------------------------
    // Account reconciliation (balances)
    // -----------------------------------------------------------------------

    /// Parse account balances from `GET /api/v3/account` using [`BALANCE_SCALE`].
    ///
    /// [`BALANCE_SCALE`]: crate::normalizer::BALANCE_SCALE
    pub fn reconcile_account(
        &self,
        raw: &RawRestAccount,
    ) -> Result<Vec<AssetBalance>, NormalizerError> {
        use crate::normalizer::BALANCE_SCALE;
        raw.balances
            .iter()
            .map(|b| {
                let free_scaled = parse_scaled(&b.free, BALANCE_SCALE)?;
                let locked_scaled = parse_scaled(&b.locked, BALANCE_SCALE)?;
                Ok(AssetBalance {
                    asset: b.asset.clone(),
                    free_scaled,
                    locked_scaled,
                })
            })
            .collect()
    }

    // -----------------------------------------------------------------------
    // Trade reconciliation (fills)
    // -----------------------------------------------------------------------

    /// Parse fill data from `GET /api/v3/myTrades`.
    pub fn reconcile_trades(
        &self,
        raw_trades: &[RawRestTrade],
    ) -> Result<Vec<ReconcileFill>, NormalizerError> {
        raw_trades
            .iter()
            .map(|t| {
                let scales = self.scales_for(&t.symbol);
                let qty = parse_scaled(&t.qty, scales.qty_scale)?;
                let price = parse_scaled(&t.price, scales.price_scale)?;
                Ok(ReconcileFill {
                    trade_id: t.id,
                    exchange_order_id: t.order_id,
                    symbol: t.symbol.clone(),
                    qty,
                    price,
                    time_ns: t.time_ms * 1_000_000,
                })
            })
            .collect()
    }

    // -----------------------------------------------------------------------
    // Private helpers
    // -----------------------------------------------------------------------

    fn normalize_order(&self, raw: &RawRestOrder) -> Result<ReconcileOrder, NormalizerError> {
        let scales = self.scales_for(&raw.symbol);
        let cloid_str = raw.client_order_id.clone();
        let cand = ClientOrderId::new_raw(cloid_str.clone());
        let cloid = if cand.parse_counter().is_some() {
            Some(cand)
        } else {
            None
        };
        Ok(ReconcileOrder {
            exchange_id: raw.order_id,
            cloid,
            raw_cloid: cloid_str,
            symbol: raw.symbol.clone(),
            status: RestOrderStatus::from_raw(&raw.status),
            orig_qty: parse_scaled(&raw.orig_qty, scales.qty_scale)?,
            executed_qty: parse_scaled(&raw.executed_qty, scales.qty_scale)?,
            created_ns: raw.created_ms * 1_000_000,
            updated_ns: raw.update_time * 1_000_000,
        })
    }

    /// Build the fills vector for `StatusCheckOutcome::Filled`.
    fn parse_fills(
        &self,
        raw_trades: &[RawRestTrade],
    ) -> Result<Vec<(u64, i64, i64)>, NormalizerError> {
        raw_trades
            .iter()
            .map(|t| {
                let scales = self.scales_for(&t.symbol);
                let qty = parse_scaled(&t.qty, scales.qty_scale)?;
                let price = parse_scaled(&t.price, scales.price_scale)?;
                Ok((t.id, qty, price))
            })
            .collect()
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn rec() -> Reconciler {
        Reconciler::new(100_000_000, 100_000_000)
    }

    // -----------------------------------------------------------------------
    // JSON fixtures
    // -----------------------------------------------------------------------

    const ORDER_NEW: &str = r#"{
        "symbol":"BTCUSDT","orderId":4293153,"orderListId":-1,
        "clientOrderId":"cc-0001-0000000000000042",
        "price":"50000.00000000","origQty":"0.10000000","executedQty":"0.00000000",
        "cummulativeQuoteQty":"0.00000000","status":"NEW","timeInForce":"GTC",
        "type":"LIMIT","side":"BUY","stopPrice":"0.00000000","icebergQty":"0.00000000",
        "time":1499827319559,"updateTime":1499827319559,"isWorking":true,
        "origQuoteOrderQty":"0.00000000","workingTime":1499827319559,
        "selfTradePreventionMode":"NONE"
    }"#;

    const ORDER_PARTIALLY_FILLED: &str = r#"{
        "symbol":"BTCUSDT","orderId":4293153,"orderListId":-1,
        "clientOrderId":"cc-0001-0000000000000042",
        "price":"50000.00000000","origQty":"0.10000000","executedQty":"0.05000000",
        "cummulativeQuoteQty":"2500.00000000","status":"PARTIALLY_FILLED",
        "timeInForce":"GTC","type":"LIMIT","side":"BUY","stopPrice":"0.00000000",
        "icebergQty":"0.00000000","time":1499827319559,"updateTime":1499827400000,
        "isWorking":true,"origQuoteOrderQty":"0.00000000","workingTime":1499827319559,
        "selfTradePreventionMode":"NONE"
    }"#;

    const ORDER_FILLED: &str = r#"{
        "symbol":"BTCUSDT","orderId":4293153,"orderListId":-1,
        "clientOrderId":"cc-0001-0000000000000042",
        "price":"50000.00000000","origQty":"0.10000000","executedQty":"0.10000000",
        "cummulativeQuoteQty":"5000.00000000","status":"FILLED",
        "timeInForce":"GTC","type":"LIMIT","side":"BUY","stopPrice":"0.00000000",
        "icebergQty":"0.00000000","time":1499827319559,"updateTime":1499827500000,
        "isWorking":false,"origQuoteOrderQty":"0.00000000","workingTime":1499827319559,
        "selfTradePreventionMode":"NONE"
    }"#;

    const ORDER_CANCELED: &str = r#"{
        "symbol":"BTCUSDT","orderId":4293154,"orderListId":-1,
        "clientOrderId":"cc-0001-0000000000000043",
        "price":"51000.00000000","origQty":"0.05000000","executedQty":"0.00000000",
        "cummulativeQuoteQty":"0.00000000","status":"CANCELED",
        "timeInForce":"GTC","type":"LIMIT","side":"SELL","stopPrice":"0.00000000",
        "icebergQty":"0.00000000","time":1499827319600,"updateTime":1499827600000,
        "isWorking":false,"origQuoteOrderQty":"0.00000000","workingTime":1499827319600,
        "selfTradePreventionMode":"NONE"
    }"#;

    const ORDER_REJECTED: &str = r#"{
        "symbol":"BTCUSDT","orderId":0,"orderListId":-1,
        "clientOrderId":"cc-0001-0000000000000044",
        "price":"1.00000000","origQty":"0.00010000","executedQty":"0.00000000",
        "cummulativeQuoteQty":"0.00000000","status":"REJECTED",
        "timeInForce":"GTC","type":"LIMIT","side":"BUY","stopPrice":"0.00000000",
        "icebergQty":"0.00000000","time":1499827319700,"updateTime":1499827319700,
        "isWorking":false,"origQuoteOrderQty":"0.00000000","workingTime":0,
        "selfTradePreventionMode":"NONE"
    }"#;

    const ORDER_EXPIRED: &str = r#"{
        "symbol":"BTCUSDT","orderId":4293155,"orderListId":-1,
        "clientOrderId":"cc-0001-0000000000000045",
        "price":"50000.00000000","origQty":"1.00000000","executedQty":"0.00000000",
        "cummulativeQuoteQty":"0.00000000","status":"EXPIRED",
        "timeInForce":"IOC","type":"LIMIT","side":"BUY","stopPrice":"0.00000000",
        "icebergQty":"0.00000000","time":1499827319800,"updateTime":1499827319800,
        "isWorking":false,"origQuoteOrderQty":"0.00000000","workingTime":1499827319800,
        "selfTradePreventionMode":"NONE"
    }"#;

    const ACCOUNT_JSON: &str = r#"{
        "makerCommission":15,"takerCommission":15,"buyerCommission":0,"sellerCommission":0,
        "canTrade":true,"canWithdraw":true,"canDeposit":true,"brokered":false,
        "requireSelfTradePrevention":false,"preventSor":false,
        "updateTime":1499827319559,"accountType":"SPOT",
        "balances":[
            {"asset":"BTC","free":"4.72384689","locked":"0.00000000"},
            {"asset":"ETH","free":"0.50000000","locked":"0.25000000"}
        ],
        "permissions":["SPOT"],"uid":354937868
    }"#;

    const TRADE_JSON: &str = r#"{
        "symbol":"BTCUSDT","id":28457,"orderId":4293153,"orderListId":-1,
        "price":"50000.00000000","qty":"0.05000000","quoteQty":"2500.00000000",
        "commission":"0.00000100","commissionAsset":"BTC","time":1499865549590,
        "isBuyer":true,"isMaker":false,"isBestMatch":true
    }"#;

    fn make_cloid(hex: u64) -> ClientOrderId {
        ClientOrderId::new_raw(format!("cc-0001-{:016x}", hex))
    }

    fn tracked(ids: &[u64]) -> HashSet<ClientOrderId> {
        ids.iter().map(|&n| make_cloid(n)).collect()
    }

    // -----------------------------------------------------------------------
    // Raw type parsing
    // -----------------------------------------------------------------------

    #[test]
    fn parse_rest_order_new() {
        let order: RawRestOrder = serde_json::from_str(ORDER_NEW).unwrap();
        assert_eq!(order.symbol, "BTCUSDT");
        assert_eq!(order.order_id, 4293153);
        assert_eq!(order.client_order_id, "cc-0001-0000000000000042");
        assert_eq!(order.status, "NEW");
        assert_eq!(order.executed_qty, "0.00000000");
        assert_eq!(order.created_ms, 1499827319559);
    }

    #[test]
    fn parse_rest_order_partially_filled() {
        let order: RawRestOrder = serde_json::from_str(ORDER_PARTIALLY_FILLED).unwrap();
        assert_eq!(order.status, "PARTIALLY_FILLED");
        assert_eq!(order.executed_qty, "0.05000000");
    }

    #[test]
    fn parse_rest_order_canceled() {
        let order: RawRestOrder = serde_json::from_str(ORDER_CANCELED).unwrap();
        assert_eq!(order.status, "CANCELED");
        assert_eq!(order.side, "SELL");
    }

    #[test]
    fn parse_rest_account_with_balances() {
        let acct: RawRestAccount = serde_json::from_str(ACCOUNT_JSON).unwrap();
        assert_eq!(acct.update_time, 1499827319559);
        assert_eq!(acct.balances.len(), 2);
        assert_eq!(acct.balances[0].asset, "BTC");
        assert_eq!(acct.balances[0].free, "4.72384689");
        assert_eq!(acct.balances[1].asset, "ETH");
        assert_eq!(acct.balances[1].locked, "0.25000000");
    }

    #[test]
    fn parse_rest_trade() {
        let trade: RawRestTrade = serde_json::from_str(TRADE_JSON).unwrap();
        assert_eq!(trade.id, 28457);
        assert_eq!(trade.order_id, 4293153);
        assert_eq!(trade.symbol, "BTCUSDT");
        assert_eq!(trade.price, "50000.00000000");
        assert_eq!(trade.qty, "0.05000000");
        assert_eq!(trade.time_ms, 1499865549590);
        assert!(trade.is_buyer);
        assert!(!trade.is_maker);
    }

    // -----------------------------------------------------------------------
    // reconcile_order_status
    // -----------------------------------------------------------------------

    fn rest_order(status: &str) -> RawRestOrder {
        let json = ORDER_NEW.replace("\"NEW\"", &format!("\"{}\"", status));
        serde_json::from_str(&json).unwrap()
    }

    #[test]
    fn reconcile_order_status_new_is_live() {
        let outcome = rec()
            .reconcile_order_status(&rest_order("NEW"), None)
            .unwrap();
        assert_eq!(
            outcome,
            StatusCheckOutcome::Live {
                exchange_id: 4293153
            }
        );
    }

    #[test]
    fn reconcile_order_status_partially_filled_is_live() {
        let outcome = rec()
            .reconcile_order_status(&rest_order("PARTIALLY_FILLED"), None)
            .unwrap();
        assert_eq!(
            outcome,
            StatusCheckOutcome::Live {
                exchange_id: 4293153
            }
        );
    }

    #[test]
    fn reconcile_order_status_pending_cancel_is_live() {
        let outcome = rec()
            .reconcile_order_status(&rest_order("PENDING_CANCEL"), None)
            .unwrap();
        assert_eq!(
            outcome,
            StatusCheckOutcome::Live {
                exchange_id: 4293153
            }
        );
    }

    #[test]
    fn reconcile_order_status_filled_no_trades_has_empty_fills() {
        let order: RawRestOrder = serde_json::from_str(ORDER_FILLED).unwrap();
        let outcome = rec().reconcile_order_status(&order, None).unwrap();
        assert_eq!(
            outcome,
            StatusCheckOutcome::Filled {
                exchange_id: 4293153,
                fills: vec![]
            },
        );
    }

    #[test]
    fn reconcile_order_status_filled_with_trades_has_fills() {
        let order: RawRestOrder = serde_json::from_str(ORDER_FILLED).unwrap();
        let trade: RawRestTrade = serde_json::from_str(TRADE_JSON).unwrap();

        let outcome = rec()
            .reconcile_order_status(&order, Some(&[trade]))
            .unwrap();
        let StatusCheckOutcome::Filled { exchange_id, fills } = outcome else {
            panic!("expected Filled");
        };
        assert_eq!(exchange_id, 4293153);
        assert_eq!(fills.len(), 1);
        // trade_id=28457, qty=0.05*10^8=5_000_000, price=50000*10^8=5_000_000_000_000
        assert_eq!(fills[0].0, 28457);
        assert_eq!(fills[0].1, 5_000_000);
        assert_eq!(fills[0].2, 5_000_000_000_000);
    }

    #[test]
    fn reconcile_order_status_canceled_is_cancelled() {
        let order: RawRestOrder = serde_json::from_str(ORDER_CANCELED).unwrap();
        let outcome = rec().reconcile_order_status(&order, None).unwrap();
        assert_eq!(
            outcome,
            StatusCheckOutcome::Cancelled {
                exchange_id: 4293154
            }
        );
    }

    #[test]
    fn reconcile_order_status_rejected_is_rejected() {
        let order: RawRestOrder = serde_json::from_str(ORDER_REJECTED).unwrap();
        let outcome = rec().reconcile_order_status(&order, None).unwrap();
        assert!(matches!(outcome, StatusCheckOutcome::Rejected { .. }));
    }

    #[test]
    fn reconcile_order_status_expired_is_expired() {
        let order: RawRestOrder = serde_json::from_str(ORDER_EXPIRED).unwrap();
        let outcome = rec().reconcile_order_status(&order, None).unwrap();
        assert_eq!(
            outcome,
            StatusCheckOutcome::Expired {
                exchange_id: 4293155
            }
        );
    }

    #[test]
    fn reconcile_order_status_unknown_status_is_error() {
        let outcome = rec()
            .reconcile_order_status(&rest_order("SOME_FUTURE_STATUS"), None)
            .unwrap();
        assert!(matches!(outcome, StatusCheckOutcome::Error { .. }));
    }

    // -----------------------------------------------------------------------
    // reconcile_open_orders
    // -----------------------------------------------------------------------

    fn open_order_json(cloid: &str, exchange_id: u64) -> String {
        format!(
            r#"{{
            "symbol":"BTCUSDT","orderId":{exchange_id},"orderListId":-1,
            "clientOrderId":"{cloid}","price":"50000.00000000",
            "origQty":"0.10000000","executedQty":"0.00000000",
            "cummulativeQuoteQty":"0.00","status":"NEW","timeInForce":"GTC",
            "type":"LIMIT","side":"BUY","stopPrice":"0","icebergQty":"0",
            "time":1499827319559,"updateTime":1499827319559,"isWorking":true,
            "origQuoteOrderQty":"0","workingTime":1499827319559,"selfTradePreventionMode":"NONE"
        }}"#
        )
    }

    fn parse_orders(jsons: &[String]) -> Vec<RawRestOrder> {
        jsons
            .iter()
            .map(|j| serde_json::from_str(j).unwrap())
            .collect()
    }

    #[test]
    fn reconcile_open_orders_tracked_order_still_live() {
        let cloid = make_cloid(0x42);
        let orders = parse_orders(&[open_order_json(cloid.as_str(), 1001)]);
        let t = tracked(&[0x42]);
        let actions = rec().reconcile_open_orders(&orders, &t).unwrap();
        assert_eq!(actions.len(), 1);
        assert!(matches!(
            &actions[0],
            ReconcileAction::OrderStillLive {
                exchange_id: 1001,
                ..
            }
        ));
    }

    #[test]
    fn reconcile_open_orders_unexpected_external_order() {
        // An order with a foreign cloid (not in our format)
        let orders = parse_orders(&[open_order_json("external-order-abc", 9999)]);
        let t = tracked(&[]);
        let actions = rec().reconcile_open_orders(&orders, &t).unwrap();
        assert_eq!(actions.len(), 1);
        let ReconcileAction::UnexpectedOrder(ref o) = actions[0] else {
            panic!()
        };
        assert!(o.cloid.is_none());
        assert_eq!(o.exchange_id, 9999);
    }

    #[test]
    fn reconcile_open_orders_our_cloid_not_tracked_is_unexpected() {
        // In our format but not in the tracked set (e.g. previous run's orphan)
        let orders = parse_orders(&[open_order_json("cc-0001-0000000000000099", 8888)]);
        let t = tracked(&[]); // empty tracked set
        let actions = rec().reconcile_open_orders(&orders, &t).unwrap();
        assert_eq!(actions.len(), 1);
        let ReconcileAction::UnexpectedOrder(ref o) = actions[0] else {
            panic!()
        };
        assert!(
            o.cloid.is_some(),
            "orphan in our format should have cloid set"
        );
        assert_eq!(o.exchange_id, 8888);
    }

    #[test]
    fn reconcile_open_orders_tracked_order_vanished() {
        // We track 0x42, but exchange has no open orders
        let t = tracked(&[0x42]);
        let actions = rec().reconcile_open_orders(&[], &t).unwrap();
        assert_eq!(actions.len(), 1);
        assert!(matches!(&actions[0], ReconcileAction::OrderVanished { .. }));
    }

    #[test]
    fn reconcile_open_orders_empty_exchange_all_tracked_vanish() {
        let t = tracked(&[0x01, 0x02, 0x03]);
        let mut actions = rec().reconcile_open_orders(&[], &t).unwrap();
        // Sort by cloid string for deterministic ordering in test
        actions.sort_by_key(|a| match a {
            ReconcileAction::OrderVanished { cloid } => cloid.as_str().to_string(),
            _ => String::new(),
        });
        assert_eq!(actions.len(), 3);
        assert!(actions
            .iter()
            .all(|a| matches!(a, ReconcileAction::OrderVanished { .. })));
    }

    #[test]
    fn reconcile_open_orders_mixed_scenario() {
        let cloid_live = make_cloid(0x10);
        let cloid_vanished = make_cloid(0x20);
        // Exchange has: cloid_live (ours, tracked) + external order
        let orders = parse_orders(&[
            open_order_json(cloid_live.as_str(), 1000),
            open_order_json("ui-placed-order", 2000),
        ]);
        let t: HashSet<ClientOrderId> = [cloid_live.clone(), cloid_vanished.clone()]
            .into_iter()
            .collect();
        let actions = rec().reconcile_open_orders(&orders, &t).unwrap();
        assert_eq!(actions.len(), 3); // Still-live + Unexpected + Vanished
        assert!(actions
            .iter()
            .any(|a| matches!(a, ReconcileAction::OrderStillLive { .. })));
        assert!(actions
            .iter()
            .any(|a| matches!(a, ReconcileAction::UnexpectedOrder(_))));
        assert!(actions
            .iter()
            .any(|a| matches!(a, ReconcileAction::OrderVanished { .. })));
    }

    // -----------------------------------------------------------------------
    // reconcile_account
    // -----------------------------------------------------------------------

    #[test]
    fn reconcile_account_correct_scale() {
        let acct: RawRestAccount = serde_json::from_str(ACCOUNT_JSON).unwrap();
        let balances = rec().reconcile_account(&acct).unwrap();
        assert_eq!(balances.len(), 2);
        // BTC: free="4.72384689" * 10^8 = 472_384_689
        assert_eq!(balances[0].asset, "BTC");
        assert_eq!(balances[0].free_scaled, 472_384_689);
        assert_eq!(balances[0].locked_scaled, 0);
        // ETH: free="0.50000000" * 10^8 = 50_000_000; locked="0.25000000" = 25_000_000
        assert_eq!(balances[1].asset, "ETH");
        assert_eq!(balances[1].free_scaled, 50_000_000);
        assert_eq!(balances[1].locked_scaled, 25_000_000);
    }

    // -----------------------------------------------------------------------
    // reconcile_trades
    // -----------------------------------------------------------------------

    #[test]
    fn reconcile_trades_parses_fills() {
        let trade: RawRestTrade = serde_json::from_str(TRADE_JSON).unwrap();
        let fills = rec().reconcile_trades(&[trade]).unwrap();
        assert_eq!(fills.len(), 1);
        assert_eq!(fills[0].trade_id, 28457);
        assert_eq!(fills[0].exchange_order_id, 4293153);
        assert_eq!(fills[0].symbol, "BTCUSDT");
        assert_eq!(fills[0].qty, 5_000_000); // 0.05 * 10^8
        assert_eq!(fills[0].price, 5_000_000_000_000); // 50000 * 10^8
        assert_eq!(fills[0].time_ns, 1499865549590 * 1_000_000);
    }

    #[test]
    fn reconcile_trades_empty_list_returns_empty() {
        let fills = rec().reconcile_trades(&[]).unwrap();
        assert!(fills.is_empty());
    }

    // -----------------------------------------------------------------------
    // ReconciliationScheduler
    // -----------------------------------------------------------------------

    const INTERVAL: i64 = 60_000_000_000; // 60 s

    fn sched() -> ReconciliationScheduler {
        ReconciliationScheduler::new(INTERVAL)
    }

    #[test]
    fn scheduler_tick_before_any_trigger_returns_nothing() {
        let s = sched();
        assert!(s.tick(0).is_empty());
    }

    #[test]
    fn scheduler_on_startup_causes_startup_request() {
        let mut s = sched();
        s.on_startup(0);
        let r = s.tick(0);
        assert!(r.contains(&ReconcileRequest::Startup));
    }

    #[test]
    fn scheduler_startup_not_repeated_after_completion() {
        let mut s = sched();
        s.on_startup(0);
        s.on_completed(ReconcileRequest::Startup, 0);
        assert!(!s.tick(0).contains(&ReconcileRequest::Startup));
    }

    #[test]
    fn scheduler_on_reconnect_causes_reconnect_request() {
        let mut s = sched();
        s.on_reconnect(0);
        assert!(s.tick(0).contains(&ReconcileRequest::Reconnect));
    }

    #[test]
    fn scheduler_reconnect_not_repeated_after_completion() {
        let mut s = sched();
        s.on_reconnect(0);
        s.on_completed(ReconcileRequest::Reconnect, 0);
        assert!(!s.tick(0).contains(&ReconcileRequest::Reconnect));
    }

    #[test]
    fn scheduler_periodic_fires_after_interval() {
        let mut s = sched();
        s.on_startup(0);
        // Just before the interval — periodic not yet due.
        assert!(!s.tick(INTERVAL - 1).contains(&ReconcileRequest::Periodic));
        // At the interval boundary — periodic is due.
        assert!(s.tick(INTERVAL).contains(&ReconcileRequest::Periodic));
    }

    #[test]
    fn scheduler_periodic_does_not_fire_before_interval() {
        let mut s = sched();
        s.on_startup(0);
        assert!(!s.tick(INTERVAL / 2).contains(&ReconcileRequest::Periodic));
    }

    #[test]
    fn scheduler_periodic_resets_after_completion() {
        let mut s = sched();
        s.on_startup(0);
        // Complete the first periodic at t=INTERVAL.
        s.on_completed(ReconcileRequest::Periodic, INTERVAL);
        // Immediately after, the next is not yet due.
        assert!(!s.tick(INTERVAL + 1).contains(&ReconcileRequest::Periodic));
        // After another full interval it is due again.
        assert!(s
            .tick(INTERVAL + INTERVAL)
            .contains(&ReconcileRequest::Periodic));
    }

    #[test]
    fn scheduler_multiple_reconnects_while_pending_are_deduplicated() {
        let mut s = sched();
        s.on_reconnect(0);
        s.on_reconnect(1_000_000_000);
        s.on_reconnect(2_000_000_000);
        let r = s.tick(2_000_000_000);
        // Only one Reconnect request despite three calls.
        assert_eq!(
            r.iter()
                .filter(|&&x| x == ReconcileRequest::Reconnect)
                .count(),
            1
        );
    }

    #[test]
    fn scheduler_startup_also_starts_periodic_timer() {
        let mut s = sched();
        s.on_startup(0);
        // Periodic should fire at t=INTERVAL even if we only called on_startup.
        assert!(s.tick(INTERVAL).contains(&ReconcileRequest::Periodic));
    }
}
