//! Order state machine (§6.3).
//!
//! Sits above the `OrderGateway` and handles three edge cases that raw
//! exchange event delivery creates:
//!
//! 1. **Out-of-order acks / fills** — Binance can deliver fill execution reports
//!    before the initial NEW acknowledgement.  The SM buffers fills received
//!    while an order is `Pending` and flushes them the moment the ACK arrives.
//!
//! 2. **Unknown-status timeout** — if no ACK or reject arrives within
//!    `timeout_ns` (default 5 s), the order is moved to `UnknownStatus` and
//!    the caller is told to schedule a REST status check.  Additional fills
//!    received during this window are buffered until the check resolves.
//!
//! 3. **Duplicate execution reports** — Binance's WS can redeliver the same
//!    fill event (identified by `trade_id`) after reconnects.  Each fill is
//!    checked against a per-order `seen_trade_ids` set; duplicates return
//!    `SmAction::Ignored`.
//!
//! # Integration pattern
//!
//! ```text
//! // 1. Submit
//! let cloid = gateway.enqueue(req, now_ns)?;
//! engine.track(cloid.clone(), req.qty, now_ns);
//!
//! // 2. WS event arrives
//! let actions = engine.process(&cloid, SmInput::FillReceived { .. })?;
//! for action in actions {
//!     match action {
//!         SmAction::ApplyAck   { exchange_id }          => gateway.on_ack(&cloid, exchange_id, now_ns)?,
//!         SmAction::ApplyFill  { fill_qty, fill_price } => gateway.on_fill(&cloid, fill_qty, fill_price, now_ns)?,
//!         SmAction::ApplyCancel                         => gateway.on_cancel(&cloid, now_ns)?,
//!         SmAction::ApplyReject { reason }              => gateway.on_reject(&cloid, &reason, now_ns)?,
//!         SmAction::ApplyExpire                         => gateway.on_expire(&cloid, now_ns)?,
//!         SmAction::ScheduleStatusCheck                 => { /* trigger REST GET /api/v3/order */ }
//!         SmAction::Ignored                             => {}
//!     }
//! }
//!
//! // 3. Periodic timer
//! for (cloid, action) in engine.tick(now_ns) {
//!     // action will be SmAction::ScheduleStatusCheck for timed-out Pending orders
//! }
//! ```

use std::collections::{HashMap, HashSet};

use crate::ClientOrderId;

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

/// Default time after submission before a non-acked order enters `UnknownStatus`.
pub const DEFAULT_UNKNOWN_TIMEOUT_NS: i64 = 5_000_000_000; // 5 s

// ---------------------------------------------------------------------------
// State
// ---------------------------------------------------------------------------

/// The state machine's view of an order's lifecycle.
///
/// This mirrors [`crate::OrderStatus`] but adds `UnknownStatus` for the
/// post-timeout waiting state and omits `Cancelling` (cancel-request tracking
/// is handled at the gateway layer, not here).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum SmStatus {
    /// Submitted to the gateway; awaiting exchange ACK.
    Pending,
    /// Exchange acknowledged; order is live on the book.
    New,
    /// At least one fill received; more expected.
    PartiallyFilled,
    /// No ACK received within the timeout; REST status check in flight.
    UnknownStatus,
    // Terminal ──────────────────────────────────────────────────────────────
    Filled,
    Cancelled,
    Rejected,
    Expired,
}

impl SmStatus {
    pub fn is_terminal(self) -> bool {
        matches!(
            self,
            Self::Filled | Self::Cancelled | Self::Rejected | Self::Expired
        )
    }

    /// True when the order is live and accepting fills.
    pub fn is_live(self) -> bool {
        matches!(self, Self::New | Self::PartiallyFilled)
    }
}

impl std::fmt::Display for SmStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::Pending => "Pending",
            Self::New => "New",
            Self::PartiallyFilled => "PartiallyFilled",
            Self::UnknownStatus => "UnknownStatus",
            Self::Filled => "Filled",
            Self::Cancelled => "Cancelled",
            Self::Rejected => "Rejected",
            Self::Expired => "Expired",
        })
    }
}

// ---------------------------------------------------------------------------
// Inputs
// ---------------------------------------------------------------------------

/// Events fed into the state machine from the WS user-data stream or REST.
#[derive(Debug, Clone)]
pub enum SmInput {
    /// Exchange issued a NEW acknowledgement.
    Acknowledged { exchange_id: u64, now_ns: i64 },

    /// A fill execution report arrived from the exchange.
    FillReceived {
        trade_id: u64,
        fill_qty: i64,
        fill_price: i64,
        now_ns: i64,
    },

    /// Exchange confirmed the cancel (CANCELED status on WS or REST).
    CancelConfirmed { now_ns: i64 },

    /// Exchange rejected the order (REJECTED status).
    Rejected { reason: String, now_ns: i64 },

    /// Order expired (IOC no-match, GTC past date).
    Expired { now_ns: i64 },

    /// Result of a REST `GET /api/v3/order` status check, triggered by the
    /// unknown-status timeout path.
    StatusCheckResult {
        outcome: StatusCheckOutcome,
        now_ns: i64,
    },
}

/// Outcome of a REST order status check.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StatusCheckOutcome {
    /// Exchange reports the order is NEW / PARTIALLY_FILLED — still live.
    Live { exchange_id: u64 },
    /// Exchange reports the order is fully filled.
    Filled {
        exchange_id: u64,
        fills: Vec<(u64, i64, i64)>,
    },
    /// Exchange reports the order is cancelled.
    Cancelled { exchange_id: u64 },
    /// Exchange reports the order was rejected (e.g. PRICE_FILTER, MIN_NOTIONAL).
    Rejected { reason: String },
    /// Exchange reports the order expired (IOC/GTD time-in-force exhausted).
    Expired { exchange_id: u64 },
    /// Exchange has no record of this order (never received or already purged).
    NotFound,
    /// REST call failed; the unknown-status check will be retried.
    Error { reason: String },
}

// ---------------------------------------------------------------------------
// Actions
// ---------------------------------------------------------------------------

/// Actions the state machine asks the caller to perform.
///
/// The typical pattern is to dispatch each action to the [`OrderGateway`][crate::OrderGateway]
/// method named in the doc comment.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SmAction {
    /// Call `gateway.on_ack(&cloid, exchange_id, now_ns)`.
    ApplyAck { exchange_id: u64 },
    /// Call `gateway.on_fill(&cloid, fill_qty, fill_price, now_ns)`.
    ApplyFill { fill_qty: i64, fill_price: i64 },
    /// Call `gateway.on_cancel(&cloid, now_ns)`.
    ApplyCancel,
    /// Call `gateway.on_reject(&cloid, &reason, now_ns)`.
    ApplyReject { reason: String },
    /// Call `gateway.on_expire(&cloid, now_ns)`.
    ApplyExpire,
    /// Trigger a REST `GET /api/v3/order` check for this order.
    ScheduleStatusCheck,
    /// Event was a no-op (duplicate trade_id or already in terminal state).
    Ignored,
}

// ---------------------------------------------------------------------------
// Error
// ---------------------------------------------------------------------------

#[derive(Debug, thiserror::Error)]
pub enum SmError {
    #[error("order not tracked: {cloid}")]
    OrderNotTracked { cloid: String },
}

// ---------------------------------------------------------------------------
// Per-order state (private)
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
struct BufferedFill {
    fill_qty: i64,
    fill_price: i64,
}

struct OrderSm {
    cloid: ClientOrderId,
    status: SmStatus,
    total_qty: i64,
    filled_qty: i64,
    submitted_ns: i64,
    buffered_fills: Vec<BufferedFill>,
    seen_trade_ids: HashSet<u64>,
}

impl OrderSm {
    fn new(cloid: ClientOrderId, total_qty: i64, submitted_ns: i64) -> Self {
        Self {
            cloid,
            status: SmStatus::Pending,
            total_qty,
            filled_qty: 0,
            submitted_ns,
            buffered_fills: Vec::new(),
            seen_trade_ids: HashSet::new(),
        }
    }

    /// Process one input event, returning the list of actions the caller must perform.
    fn process_input(&mut self, input: SmInput) -> Vec<SmAction> {
        if self.status.is_terminal() {
            return vec![SmAction::Ignored];
        }

        match self.status {
            SmStatus::Pending => self.process_pending(input),
            SmStatus::New | SmStatus::PartiallyFilled => self.process_live(input),
            SmStatus::UnknownStatus => self.process_unknown(input),
            _ => vec![SmAction::Ignored],
        }
    }

    // --- Pending state ------------------------------------------------------

    fn process_pending(&mut self, input: SmInput) -> Vec<SmAction> {
        match input {
            SmInput::Acknowledged { exchange_id, .. } => {
                self.status = SmStatus::New;
                let mut actions = vec![SmAction::ApplyAck { exchange_id }];
                // Flush all fills that arrived before the ACK.
                let fills: Vec<_> = std::mem::take(&mut self.buffered_fills);
                for f in fills {
                    self.filled_qty += f.fill_qty;
                    actions.push(SmAction::ApplyFill {
                        fill_qty: f.fill_qty,
                        fill_price: f.fill_price,
                    });
                }
                self.status = if self.filled_qty >= self.total_qty {
                    SmStatus::Filled
                } else if self.filled_qty > 0 {
                    SmStatus::PartiallyFilled
                } else {
                    SmStatus::New
                };
                actions
            }

            SmInput::FillReceived {
                trade_id,
                fill_qty,
                fill_price,
                ..
            } => {
                if self.seen_trade_ids.contains(&trade_id) {
                    return vec![SmAction::Ignored];
                }
                self.seen_trade_ids.insert(trade_id);
                // Buffer: cannot apply to gateway without prior ACK.
                self.buffered_fills.push(BufferedFill {
                    fill_qty,
                    fill_price,
                });
                // Return empty — no gateway action yet; fill will be applied on ACK.
                vec![]
            }

            SmInput::Rejected { reason, .. } => {
                self.status = SmStatus::Rejected;
                vec![SmAction::ApplyReject { reason }]
            }

            SmInput::CancelConfirmed { .. } => {
                // Rare: cancel was accepted before ACK (order never made it to the book).
                self.status = SmStatus::Cancelled;
                vec![SmAction::ApplyCancel]
            }

            _ => vec![SmAction::Ignored],
        }
    }

    // --- New / PartiallyFilled (live) state ---------------------------------

    fn process_live(&mut self, input: SmInput) -> Vec<SmAction> {
        match input {
            SmInput::FillReceived {
                trade_id,
                fill_qty,
                fill_price,
                ..
            } => {
                if self.seen_trade_ids.contains(&trade_id) {
                    return vec![SmAction::Ignored];
                }
                self.seen_trade_ids.insert(trade_id);
                self.filled_qty += fill_qty;
                self.status = if self.filled_qty >= self.total_qty {
                    SmStatus::Filled
                } else {
                    SmStatus::PartiallyFilled
                };
                vec![SmAction::ApplyFill {
                    fill_qty,
                    fill_price,
                }]
            }

            SmInput::CancelConfirmed { .. } => {
                self.status = SmStatus::Cancelled;
                vec![SmAction::ApplyCancel]
            }

            SmInput::Expired { .. } => {
                self.status = SmStatus::Expired;
                vec![SmAction::ApplyExpire]
            }

            // Very late reject (shouldn't happen in practice, but handle it).
            SmInput::Rejected { reason, .. } => {
                self.status = SmStatus::Rejected;
                vec![SmAction::ApplyReject { reason }]
            }

            // Duplicate ACK — idempotent, nothing to do.
            SmInput::Acknowledged { .. } => vec![SmAction::Ignored],

            _ => vec![SmAction::Ignored],
        }
    }

    // --- UnknownStatus state ------------------------------------------------

    fn process_unknown(&mut self, input: SmInput) -> Vec<SmAction> {
        match input {
            // Late ACK arrived before the REST check result — treat as resolution.
            SmInput::Acknowledged { exchange_id, .. } => {
                self.status = SmStatus::New;
                let mut actions = vec![SmAction::ApplyAck { exchange_id }];
                let fills: Vec<_> = std::mem::take(&mut self.buffered_fills);
                for f in fills {
                    self.filled_qty += f.fill_qty;
                    actions.push(SmAction::ApplyFill {
                        fill_qty: f.fill_qty,
                        fill_price: f.fill_price,
                    });
                }
                self.status = if self.filled_qty >= self.total_qty {
                    SmStatus::Filled
                } else if self.filled_qty > 0 {
                    SmStatus::PartiallyFilled
                } else {
                    SmStatus::New
                };
                actions
            }

            // Buffer fills received while the REST check is in flight.
            SmInput::FillReceived {
                trade_id,
                fill_qty,
                fill_price,
                ..
            } => {
                if self.seen_trade_ids.contains(&trade_id) {
                    return vec![SmAction::Ignored];
                }
                self.seen_trade_ids.insert(trade_id);
                self.buffered_fills.push(BufferedFill {
                    fill_qty,
                    fill_price,
                });
                vec![]
            }

            SmInput::CancelConfirmed { .. } => {
                self.status = SmStatus::Cancelled;
                vec![SmAction::ApplyCancel]
            }

            SmInput::StatusCheckResult { outcome, .. } => self.resolve_from_check(outcome),

            _ => vec![SmAction::Ignored],
        }
    }

    /// Apply a REST status-check result to resolve `UnknownStatus`.
    fn resolve_from_check(&mut self, outcome: StatusCheckOutcome) -> Vec<SmAction> {
        match outcome {
            StatusCheckOutcome::Live { exchange_id } => {
                self.status = SmStatus::New;
                let mut actions = vec![SmAction::ApplyAck { exchange_id }];
                let fills: Vec<_> = std::mem::take(&mut self.buffered_fills);
                for f in fills {
                    self.filled_qty += f.fill_qty;
                    actions.push(SmAction::ApplyFill {
                        fill_qty: f.fill_qty,
                        fill_price: f.fill_price,
                    });
                }
                self.status = if self.filled_qty >= self.total_qty {
                    SmStatus::Filled
                } else if self.filled_qty > 0 {
                    SmStatus::PartiallyFilled
                } else {
                    SmStatus::New
                };
                actions
            }

            StatusCheckOutcome::Filled { exchange_id, fills } => {
                // Emit ACK first, then all fills from REST (deduped against what we
                // already saw via WS while in UnknownStatus).
                let mut actions = vec![SmAction::ApplyAck { exchange_id }];
                for (trade_id, fill_qty, fill_price) in fills {
                    if !self.seen_trade_ids.contains(&trade_id) {
                        self.seen_trade_ids.insert(trade_id);
                        self.filled_qty += fill_qty;
                        actions.push(SmAction::ApplyFill {
                            fill_qty,
                            fill_price,
                        });
                    }
                }
                // Also flush any WS fills buffered while in UnknownStatus.
                let pending: Vec<_> = std::mem::take(&mut self.buffered_fills);
                for f in pending {
                    self.filled_qty += f.fill_qty;
                    actions.push(SmAction::ApplyFill {
                        fill_qty: f.fill_qty,
                        fill_price: f.fill_price,
                    });
                }
                self.status = SmStatus::Filled;
                actions
            }

            StatusCheckOutcome::Cancelled { .. } => {
                self.status = SmStatus::Cancelled;
                vec![SmAction::ApplyCancel]
            }

            StatusCheckOutcome::Rejected { reason } => {
                self.status = SmStatus::Rejected;
                vec![SmAction::ApplyReject { reason }]
            }

            StatusCheckOutcome::Expired { .. } => {
                self.status = SmStatus::Expired;
                vec![SmAction::ApplyExpire]
            }

            StatusCheckOutcome::NotFound => {
                self.status = SmStatus::Rejected;
                vec![SmAction::ApplyReject {
                    reason: "order not found by REST status check".into(),
                }]
            }

            StatusCheckOutcome::Error { .. } => {
                // Stay in UnknownStatus; caller should retry.
                vec![SmAction::ScheduleStatusCheck]
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Engine
// ---------------------------------------------------------------------------

/// Multi-order state machine engine.
///
/// One `StateMachineEngine` manages all in-flight orders for a single gateway
/// instance.  It is single-threaded and synchronous; no locks or channels.
pub struct StateMachineEngine {
    orders: HashMap<ClientOrderId, OrderSm>,
    timeout_ns: i64,
}

impl Default for StateMachineEngine {
    fn default() -> Self {
        Self::new()
    }
}

impl StateMachineEngine {
    /// Create an engine with the default 5-second unknown-status timeout.
    pub fn new() -> Self {
        Self::with_timeout_ns(DEFAULT_UNKNOWN_TIMEOUT_NS)
    }

    /// Create an engine with a custom unknown-status timeout.
    ///
    /// Useful in tests where 5 s is inconvenient.
    pub fn with_timeout_ns(timeout_ns: i64) -> Self {
        Self {
            orders: HashMap::new(),
            timeout_ns,
        }
    }

    // -----------------------------------------------------------------------
    // Lifecycle
    // -----------------------------------------------------------------------

    /// Start tracking a newly submitted order.
    ///
    /// Call this immediately after `OrderGateway::enqueue` succeeds.
    ///
    /// * `total_qty` — the original requested quantity; used to detect full fill.
    /// * `submitted_ns` — virtual/wall nanosecond timestamp of submission.
    pub fn track(&mut self, cloid: ClientOrderId, total_qty: i64, submitted_ns: i64) {
        self.orders
            .insert(cloid.clone(), OrderSm::new(cloid, total_qty, submitted_ns));
    }

    /// Stop tracking an order (e.g. after it reaches a terminal state or is
    /// pruned from the in-memory book during startup reconciliation).
    pub fn untrack(&mut self, cloid: &ClientOrderId) {
        self.orders.remove(cloid);
    }

    // -----------------------------------------------------------------------
    // Event processing
    // -----------------------------------------------------------------------

    /// Feed one exchange event into the state machine for `cloid`.
    ///
    /// Returns the list of actions the caller must perform (in order).
    /// Returns `Err(SmError::OrderNotTracked)` if the `cloid` is unknown.
    pub fn process(
        &mut self,
        cloid: &ClientOrderId,
        input: SmInput,
    ) -> Result<Vec<SmAction>, SmError> {
        // Take ownership so we can mutate and conditionally drop on terminal.
        let mut sm = self
            .orders
            .remove(cloid)
            .ok_or_else(|| SmError::OrderNotTracked {
                cloid: cloid.to_string(),
            })?;

        let actions = sm.process_input(input);

        // Re-insert only if non-terminal; terminal orders are auto-untracked.
        if !sm.status.is_terminal() {
            self.orders.insert(sm.cloid.clone(), sm);
        }

        Ok(actions)
    }

    /// Advance the virtual clock to `now_ns` and return actions for any
    /// `Pending` orders that have exceeded the unknown-status timeout.
    ///
    /// Call this on a periodic timer (e.g. every 100 ms).
    pub fn tick(&mut self, now_ns: i64) -> Vec<(ClientOrderId, SmAction)> {
        let mut results = Vec::new();
        for sm in self.orders.values_mut() {
            if sm.status == SmStatus::Pending
                && now_ns.saturating_sub(sm.submitted_ns) > self.timeout_ns
            {
                sm.status = SmStatus::UnknownStatus;
                results.push((sm.cloid.clone(), SmAction::ScheduleStatusCheck));
            }
        }
        results
    }

    // -----------------------------------------------------------------------
    // Queries
    // -----------------------------------------------------------------------

    pub fn status(&self, cloid: &ClientOrderId) -> Option<SmStatus> {
        self.orders.get(cloid).map(|sm| sm.status)
    }

    pub fn is_tracked(&self, cloid: &ClientOrderId) -> bool {
        self.orders.contains_key(cloid)
    }

    pub fn order_count(&self) -> usize {
        self.orders.len()
    }

    pub fn timeout_ns(&self) -> i64 {
        self.timeout_ns
    }

    /// Number of fills buffered for `cloid` awaiting an ACK.
    #[cfg(test)]
    pub(crate) fn buffered_fill_count(&self, cloid: &ClientOrderId) -> usize {
        self.orders
            .get(cloid)
            .map(|sm| sm.buffered_fills.len())
            .unwrap_or(0)
    }

    /// Number of trade IDs seen for `cloid`.
    #[cfg(test)]
    pub(crate) fn seen_trade_id_count(&self, cloid: &ClientOrderId) -> usize {
        self.orders
            .get(cloid)
            .map(|sm| sm.seen_trade_ids.len())
            .unwrap_or(0)
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ClientOrderIdGenerator;

    const TIMEOUT_NS: i64 = 2_000_000_000; // 2 s for tests

    fn engine() -> StateMachineEngine {
        StateMachineEngine::with_timeout_ns(TIMEOUT_NS)
    }

    fn next_cloid() -> ClientOrderId {
        static CTR: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        let n = CTR.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let mut gen = ClientOrderIdGenerator::new(0);
        // Fast-forward the generator to produce a unique cloid per call.
        for _ in 0..n {
            gen.next();
        }
        gen.next()
    }

    fn setup() -> (StateMachineEngine, ClientOrderId) {
        let mut eng = engine();
        let cloid = next_cloid();
        eng.track(cloid.clone(), 100, 0);
        (eng, cloid)
    }

    fn ack(exchange_id: u64) -> SmInput {
        SmInput::Acknowledged {
            exchange_id,
            now_ns: 1,
        }
    }

    fn fill(trade_id: u64, qty: i64, price: i64) -> SmInput {
        SmInput::FillReceived {
            trade_id,
            fill_qty: qty,
            fill_price: price,
            now_ns: 1,
        }
    }

    fn cancel() -> SmInput {
        SmInput::CancelConfirmed { now_ns: 1 }
    }
    fn reject(r: &str) -> SmInput {
        SmInput::Rejected {
            reason: r.into(),
            now_ns: 1,
        }
    }
    fn expire() -> SmInput {
        SmInput::Expired { now_ns: 1 }
    }

    // -----------------------------------------------------------------------
    // Basic lifecycle
    // -----------------------------------------------------------------------

    #[test]
    fn track_starts_in_pending() {
        let (eng, cloid) = setup();
        assert_eq!(eng.status(&cloid), Some(SmStatus::Pending));
        assert!(eng.is_tracked(&cloid));
    }

    #[test]
    fn ack_transitions_pending_to_new() {
        let (mut eng, cloid) = setup();
        let actions = eng.process(&cloid, ack(42)).unwrap();
        assert_eq!(actions, vec![SmAction::ApplyAck { exchange_id: 42 }]);
        assert_eq!(eng.status(&cloid), Some(SmStatus::New));
    }

    #[test]
    fn full_fill_after_ack_transitions_to_filled_and_untracks() {
        let (mut eng, cloid) = setup();
        eng.process(&cloid, ack(1)).unwrap();
        let actions = eng.process(&cloid, fill(1, 100, 50_000)).unwrap();
        assert_eq!(
            actions,
            vec![SmAction::ApplyFill {
                fill_qty: 100,
                fill_price: 50_000
            }]
        );
        assert_eq!(eng.status(&cloid), None); // auto-untracked
        assert!(!eng.is_tracked(&cloid));
    }

    #[test]
    fn partial_fill_stays_in_partially_filled() {
        let (mut eng, cloid) = setup();
        eng.process(&cloid, ack(1)).unwrap();
        eng.process(&cloid, fill(1, 40, 50_000)).unwrap();
        assert_eq!(eng.status(&cloid), Some(SmStatus::PartiallyFilled));
    }

    #[test]
    fn multiple_partial_fills_accumulate_to_filled() {
        let (mut eng, cloid) = setup();
        eng.process(&cloid, ack(1)).unwrap();
        eng.process(&cloid, fill(1, 40, 50_000)).unwrap();
        eng.process(&cloid, fill(2, 40, 50_100)).unwrap();
        assert_eq!(eng.status(&cloid), Some(SmStatus::PartiallyFilled));
        eng.process(&cloid, fill(3, 20, 50_200)).unwrap();
        assert!(!eng.is_tracked(&cloid)); // filled = 100 = total_qty → Filled → auto-untracked
    }

    #[test]
    fn cancel_confirmed_transitions_to_cancelled() {
        let (mut eng, cloid) = setup();
        eng.process(&cloid, ack(1)).unwrap();
        let actions = eng.process(&cloid, cancel()).unwrap();
        assert_eq!(actions, vec![SmAction::ApplyCancel]);
        assert!(!eng.is_tracked(&cloid));
    }

    #[test]
    fn reject_from_pending_transitions_to_rejected() {
        let (mut eng, cloid) = setup();
        let actions = eng.process(&cloid, reject("PRICE_FILTER")).unwrap();
        assert_eq!(
            actions,
            vec![SmAction::ApplyReject {
                reason: "PRICE_FILTER".into()
            }]
        );
        assert!(!eng.is_tracked(&cloid));
    }

    #[test]
    fn expire_from_live_transitions_to_expired() {
        let (mut eng, cloid) = setup();
        eng.process(&cloid, ack(1)).unwrap();
        let actions = eng.process(&cloid, expire()).unwrap();
        assert_eq!(actions, vec![SmAction::ApplyExpire]);
        assert!(!eng.is_tracked(&cloid));
    }

    #[test]
    fn cancel_from_pending_transitions_to_cancelled() {
        let (mut eng, cloid) = setup();
        let actions = eng.process(&cloid, cancel()).unwrap();
        assert_eq!(actions, vec![SmAction::ApplyCancel]);
        assert!(!eng.is_tracked(&cloid));
    }

    // -----------------------------------------------------------------------
    // Out-of-order: fill before ack
    // -----------------------------------------------------------------------

    #[test]
    fn fill_before_ack_is_buffered_not_applied() {
        let (mut eng, cloid) = setup();
        let actions = eng.process(&cloid, fill(1, 40, 50_000)).unwrap();
        assert!(
            actions.is_empty(),
            "fill before ack must produce no gateway actions"
        );
        assert_eq!(eng.status(&cloid), Some(SmStatus::Pending));
        assert_eq!(eng.buffered_fill_count(&cloid), 1);
    }

    #[test]
    fn buffered_fill_is_applied_when_ack_arrives() {
        let (mut eng, cloid) = setup();
        eng.process(&cloid, fill(1, 40, 50_000)).unwrap();

        let actions = eng.process(&cloid, ack(99)).unwrap();
        assert_eq!(
            actions,
            vec![
                SmAction::ApplyAck { exchange_id: 99 },
                SmAction::ApplyFill {
                    fill_qty: 40,
                    fill_price: 50_000
                },
            ]
        );
        assert_eq!(eng.status(&cloid), Some(SmStatus::PartiallyFilled));
        assert_eq!(eng.buffered_fill_count(&cloid), 0);
    }

    #[test]
    fn multiple_buffered_fills_flushed_in_order_on_ack() {
        let (mut eng, cloid) = setup();
        eng.process(&cloid, fill(1, 30, 50_000)).unwrap();
        eng.process(&cloid, fill(2, 30, 50_100)).unwrap();

        let actions = eng.process(&cloid, ack(1)).unwrap();
        assert_eq!(
            actions,
            vec![
                SmAction::ApplyAck { exchange_id: 1 },
                SmAction::ApplyFill {
                    fill_qty: 30,
                    fill_price: 50_000
                },
                SmAction::ApplyFill {
                    fill_qty: 30,
                    fill_price: 50_100
                },
            ]
        );
        assert_eq!(eng.status(&cloid), Some(SmStatus::PartiallyFilled));
    }

    #[test]
    fn buffered_fills_that_fully_fill_transition_to_filled() {
        let (mut eng, cloid) = setup();
        eng.process(&cloid, fill(1, 60, 50_000)).unwrap();
        eng.process(&cloid, fill(2, 40, 50_100)).unwrap();

        eng.process(&cloid, ack(1)).unwrap();
        assert!(
            !eng.is_tracked(&cloid),
            "full fill on ACK flush must auto-untrack"
        );
    }

    // -----------------------------------------------------------------------
    // Duplicate execution report deduplication
    // -----------------------------------------------------------------------

    #[test]
    fn duplicate_trade_id_after_ack_is_ignored() {
        let (mut eng, cloid) = setup();
        eng.process(&cloid, ack(1)).unwrap();
        eng.process(&cloid, fill(42, 50, 50_000)).unwrap();

        let actions = eng.process(&cloid, fill(42, 50, 50_000)).unwrap();
        assert_eq!(actions, vec![SmAction::Ignored]);
        // filled_qty must not have increased twice
        assert_eq!(eng.status(&cloid), Some(SmStatus::PartiallyFilled));
    }

    #[test]
    fn duplicate_trade_id_before_ack_is_ignored() {
        let (mut eng, cloid) = setup();
        eng.process(&cloid, fill(7, 30, 49_000)).unwrap();

        let actions = eng.process(&cloid, fill(7, 30, 49_000)).unwrap();
        assert_eq!(actions, vec![SmAction::Ignored]);
        assert_eq!(
            eng.buffered_fill_count(&cloid),
            1,
            "only one fill should be buffered"
        );
        assert_eq!(eng.seen_trade_id_count(&cloid), 1);
    }

    #[test]
    fn same_trade_id_buffered_then_seen_live_is_ignored() {
        let (mut eng, cloid) = setup();
        // Fill arrives before ACK (trade_id=5).
        eng.process(&cloid, fill(5, 20, 48_000)).unwrap();
        // ACK flushes buffered fill.
        eng.process(&cloid, ack(1)).unwrap();
        // Same trade_id arrives again via WS (e.g. duplicate delivery).
        let actions = eng.process(&cloid, fill(5, 20, 48_000)).unwrap();
        assert_eq!(actions, vec![SmAction::Ignored]);
    }

    // -----------------------------------------------------------------------
    // Unknown-status timeout
    // -----------------------------------------------------------------------

    #[test]
    fn tick_before_timeout_produces_no_actions() {
        let (mut eng, _cloid) = setup();
        let timed_out = eng.tick(TIMEOUT_NS - 1);
        assert!(timed_out.is_empty());
    }

    #[test]
    fn tick_after_timeout_transitions_to_unknown_and_schedules_check() {
        let (mut eng, cloid) = setup();
        let result = eng.tick(TIMEOUT_NS + 1);
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].0, cloid);
        assert_eq!(result[0].1, SmAction::ScheduleStatusCheck);
        assert_eq!(eng.status(&cloid), Some(SmStatus::UnknownStatus));
    }

    #[test]
    fn tick_only_times_out_pending_orders_not_live() {
        let mut eng = engine();
        let c_pending = next_cloid();
        let c_live = next_cloid();
        eng.track(c_pending.clone(), 100, 0);
        eng.track(c_live.clone(), 100, 0);
        eng.process(&c_live, ack(1)).unwrap(); // c_live is now New

        let result = eng.tick(TIMEOUT_NS + 1);
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].0, c_pending);
        // c_live must still be New
        assert_eq!(eng.status(&c_live), Some(SmStatus::New));
    }

    #[test]
    fn fill_during_unknown_status_is_buffered() {
        let (mut eng, cloid) = setup();
        eng.tick(TIMEOUT_NS + 1); // → UnknownStatus

        let actions = eng.process(&cloid, fill(9, 50, 50_000)).unwrap();
        assert!(actions.is_empty());
        assert_eq!(eng.buffered_fill_count(&cloid), 1);
        assert_eq!(eng.status(&cloid), Some(SmStatus::UnknownStatus));
    }

    #[test]
    fn late_ack_resolves_unknown_status() {
        let (mut eng, cloid) = setup();
        eng.tick(TIMEOUT_NS + 1); // → UnknownStatus
        eng.process(&cloid, fill(1, 30, 50_000)).unwrap(); // buffered

        let actions = eng.process(&cloid, ack(77)).unwrap();
        assert_eq!(
            actions,
            vec![
                SmAction::ApplyAck { exchange_id: 77 },
                SmAction::ApplyFill {
                    fill_qty: 30,
                    fill_price: 50_000
                },
            ]
        );
        assert_eq!(eng.status(&cloid), Some(SmStatus::PartiallyFilled));
    }

    // -----------------------------------------------------------------------
    // Status check outcomes
    // -----------------------------------------------------------------------

    fn check(outcome: StatusCheckOutcome) -> SmInput {
        SmInput::StatusCheckResult { outcome, now_ns: 1 }
    }

    #[test]
    fn status_check_live_resolves_to_new() {
        let (mut eng, cloid) = setup();
        eng.tick(TIMEOUT_NS + 1);

        let actions = eng
            .process(&cloid, check(StatusCheckOutcome::Live { exchange_id: 55 }))
            .unwrap();
        assert_eq!(actions, vec![SmAction::ApplyAck { exchange_id: 55 }]);
        assert_eq!(eng.status(&cloid), Some(SmStatus::New));
    }

    #[test]
    fn status_check_filled_applies_fills_and_deduplicates() {
        let (mut eng, cloid) = setup();
        eng.tick(TIMEOUT_NS + 1);
        // trade_id=1 already buffered via WS.
        eng.process(&cloid, fill(1, 40, 50_000)).unwrap();

        let actions = eng
            .process(
                &cloid,
                check(StatusCheckOutcome::Filled {
                    exchange_id: 88,
                    fills: vec![(1, 40, 50_000), (2, 60, 50_100)], // trade_id=1 is duplicate
                }),
            )
            .unwrap();

        // Must deduplicate trade_id=1; apply trade_id=2 only (then flush the WS buffer).
        let expected = vec![
            SmAction::ApplyAck { exchange_id: 88 },
            SmAction::ApplyFill {
                fill_qty: 60,
                fill_price: 50_100,
            }, // REST fill (trade_id=2)
            SmAction::ApplyFill {
                fill_qty: 40,
                fill_price: 50_000,
            }, // buffered WS fill (trade_id=1)
        ];
        assert_eq!(actions, expected);
        assert!(!eng.is_tracked(&cloid), "Filled → auto-untracked");
    }

    #[test]
    fn status_check_cancelled_resolves_and_untracks() {
        let (mut eng, cloid) = setup();
        eng.tick(TIMEOUT_NS + 1);

        let actions = eng
            .process(
                &cloid,
                check(StatusCheckOutcome::Cancelled { exchange_id: 3 }),
            )
            .unwrap();
        assert_eq!(actions, vec![SmAction::ApplyCancel]);
        assert!(!eng.is_tracked(&cloid));
    }

    #[test]
    fn status_check_not_found_rejects_and_untracks() {
        let (mut eng, cloid) = setup();
        eng.tick(TIMEOUT_NS + 1);

        let actions = eng
            .process(&cloid, check(StatusCheckOutcome::NotFound))
            .unwrap();
        assert!(matches!(actions[0], SmAction::ApplyReject { .. }));
        assert!(!eng.is_tracked(&cloid));
    }

    #[test]
    fn status_check_error_reschedules_check() {
        let (mut eng, cloid) = setup();
        eng.tick(TIMEOUT_NS + 1);

        let actions = eng
            .process(
                &cloid,
                check(StatusCheckOutcome::Error {
                    reason: "timeout".into(),
                }),
            )
            .unwrap();
        assert_eq!(actions, vec![SmAction::ScheduleStatusCheck]);
        assert_eq!(eng.status(&cloid), Some(SmStatus::UnknownStatus)); // still in unknown
    }

    // -----------------------------------------------------------------------
    // Terminal state behaviour
    // -----------------------------------------------------------------------

    #[test]
    fn terminal_order_ignores_all_further_inputs() {
        let (mut eng, cloid) = setup();
        eng.process(&cloid, reject("PRICE_FILTER")).unwrap(); // → Rejected (terminal, untracked)

        let err = eng.process(&cloid, ack(1));
        assert!(matches!(err, Err(SmError::OrderNotTracked { .. })));
    }

    #[test]
    fn duplicate_ack_on_live_order_is_ignored() {
        let (mut eng, cloid) = setup();
        eng.process(&cloid, ack(1)).unwrap();
        let actions = eng.process(&cloid, ack(1)).unwrap();
        assert_eq!(actions, vec![SmAction::Ignored]);
    }

    #[test]
    fn untrack_removes_from_engine() {
        let (mut eng, cloid) = setup();
        assert!(eng.is_tracked(&cloid));
        eng.untrack(&cloid);
        assert!(!eng.is_tracked(&cloid));
        assert_eq!(eng.order_count(), 0);
    }

    #[test]
    fn process_untracked_order_returns_err() {
        let mut eng = engine();
        let cloid = next_cloid();
        let err = eng.process(&cloid, ack(1));
        assert!(matches!(err, Err(SmError::OrderNotTracked { .. })));
    }

    #[test]
    fn multiple_orders_are_independent() {
        let mut eng = engine();
        let c1 = next_cloid();
        let c2 = next_cloid();
        eng.track(c1.clone(), 100, 0);
        eng.track(c2.clone(), 200, 0);

        eng.process(&c1, ack(1)).unwrap();
        eng.process(&c2, reject("INSUFFICIENT_FUNDS")).unwrap();

        assert_eq!(eng.status(&c1), Some(SmStatus::New));
        assert!(!eng.is_tracked(&c2), "c2 rejected → auto-untracked");
        assert_eq!(eng.order_count(), 1);
    }

    #[test]
    fn engine_order_count_reflects_tracking() {
        let mut eng = engine();
        assert_eq!(eng.order_count(), 0);
        let c1 = next_cloid();
        let c2 = next_cloid();
        eng.track(c1.clone(), 10, 0);
        eng.track(c2.clone(), 10, 0);
        assert_eq!(eng.order_count(), 2);
        eng.process(&c1, fill(1, 10, 100)).unwrap(); // pre-ack fill buffered
        eng.process(&c1, ack(1)).unwrap(); // → Filled → untracked
        assert_eq!(eng.order_count(), 1);
    }
}
