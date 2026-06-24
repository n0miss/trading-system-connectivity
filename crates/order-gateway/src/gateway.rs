use std::collections::HashMap;
use std::path::Path;

use crate::{
    ClientOrderId, ClientOrderIdGenerator, Error, Journal, JournalEntry, OrderRequest, OrderStatus,
    PendingOrder,
};

/// The order gateway — pre-trade component that assigns client order IDs,
/// persists all lifecycle events, and tracks in-flight order state.
///
/// The gateway does **not** send HTTP/WS requests itself; that is the
/// responsibility of the venue adapter (stage 43).  The typical call sequence
/// from the adapter is:
///
/// ```text
/// cloid = gateway.enqueue(request, now_ns)?   // before REST call
/// // … send to exchange …
/// gateway.on_ack(&cloid, exchange_id, now_ns)?
/// gateway.on_fill(&cloid, qty, price, now_ns)?
/// // … or …
/// gateway.on_reject(&cloid, reason, now_ns)?
/// ```
pub struct OrderGateway {
    instance_id: u32,
    cloid_gen: ClientOrderIdGenerator,
    pub(crate) journal: Journal,
    orders: HashMap<ClientOrderId, PendingOrder>,
}

impl OrderGateway {
    /// Open a file-backed gateway, replaying the journal to restore prior order state.
    pub fn open(instance_id: u32, journal_path: &Path) -> Result<Self, Error> {
        let (recovered, journal) = Journal::open(journal_path)?;
        let mut gw = Self {
            instance_id,
            cloid_gen: ClientOrderIdGenerator::new(instance_id),
            journal,
            orders: HashMap::new(),
        };
        gw.replay_journal(recovered)?;
        Ok(gw)
    }

    /// Create an in-memory (non-persistent) gateway.  Use in tests and benchmarks.
    pub fn in_memory(instance_id: u32) -> Self {
        Self {
            instance_id,
            cloid_gen: ClientOrderIdGenerator::new(instance_id),
            journal: Journal::in_memory(),
            orders: HashMap::new(),
        }
    }

    // ---------------------------------------------------------------------------
    // Pre-trade
    // ---------------------------------------------------------------------------

    /// Assign a `ClientOrderId`, write an `OrderRequested` journal entry, and
    /// track the order as `Pending`.  The caller is responsible for sending the
    /// order to the exchange after this returns.
    pub fn enqueue(&mut self, request: OrderRequest, now_ns: i64) -> Result<ClientOrderId, Error> {
        let cloid = self.cloid_gen.next();

        let entry = JournalEntry::OrderRequested {
            timestamp_ns: now_ns,
            cloid: cloid.clone(),
            request: request.clone(),
        };
        self.journal.append(&entry)?;

        let order = PendingOrder {
            cloid: cloid.clone(),
            request,
            status: OrderStatus::Pending,
            exchange_id: None,
            filled_qty: 0,
            submitted_ns: now_ns,
            last_updated_ns: now_ns,
        };
        self.orders.insert(cloid.clone(), order);
        Ok(cloid)
    }

    // ---------------------------------------------------------------------------
    // Lifecycle events from the exchange
    // ---------------------------------------------------------------------------

    /// Exchange acknowledged the order (`NEW` status or equivalent).
    pub fn on_ack(
        &mut self,
        cloid: &ClientOrderId,
        exchange_id: u64,
        now_ns: i64,
    ) -> Result<(), Error> {
        let order = self.get_order_mut(cloid)?;
        if order.status == OrderStatus::New {
            return Ok(()); // idempotent — ignore duplicate ack
        }
        self.require_non_terminal(cloid, OrderStatus::New)?;
        let order = self.orders.get_mut(cloid).unwrap();
        order.exchange_id = Some(exchange_id);
        order.status = OrderStatus::New;
        order.last_updated_ns = now_ns;

        let entry = JournalEntry::OrderAcknowledged {
            timestamp_ns: now_ns,
            cloid: cloid.clone(),
            exchange_id,
        };
        self.journal.append(&entry)
    }

    /// A fill event arrived from the exchange.
    ///
    /// Pass `fill_qty` as the quantity matched in this single fill event (not
    /// cumulative).  The gateway accumulates fills internally.
    pub fn on_fill(
        &mut self,
        cloid: &ClientOrderId,
        fill_qty: i64,
        fill_price: i64,
        now_ns: i64,
    ) -> Result<(), Error> {
        {
            let order = self.get_order_mut(cloid)?;
            if order.status.is_terminal() {
                return Err(Error::InvalidTransition {
                    cloid: cloid.to_string(),
                    from: order.status,
                    to: OrderStatus::PartiallyFilled,
                });
            }
        }

        let is_final = {
            let order = self.orders.get_mut(cloid).unwrap();
            order.filled_qty += fill_qty;
            order.last_updated_ns = now_ns;
            let fully_filled = order.filled_qty >= order.request.qty;
            order.status = if fully_filled {
                OrderStatus::Filled
            } else {
                OrderStatus::PartiallyFilled
            };
            fully_filled
        };

        let entry = JournalEntry::OrderFilled {
            timestamp_ns: now_ns,
            cloid: cloid.clone(),
            fill_qty,
            fill_price,
            is_final,
        };
        self.journal.append(&entry)
    }

    /// The exchange cancelled the order (response to a cancel request, or
    /// automatic expiry confirmed as cancelled).
    pub fn on_cancel(&mut self, cloid: &ClientOrderId, now_ns: i64) -> Result<(), Error> {
        {
            let order = self.get_order_mut(cloid)?;
            if order.status == OrderStatus::Cancelled {
                return Ok(()); // idempotent
            }
            if order.status.is_terminal() {
                return Err(Error::InvalidTransition {
                    cloid: cloid.to_string(),
                    from: order.status,
                    to: OrderStatus::Cancelled,
                });
            }
        }
        let order = self.orders.get_mut(cloid).unwrap();
        order.status = OrderStatus::Cancelled;
        order.last_updated_ns = now_ns;

        let entry = JournalEntry::OrderCancelled {
            timestamp_ns: now_ns,
            cloid: cloid.clone(),
        };
        self.journal.append(&entry)
    }

    /// The exchange rejected the order (e.g. filter violation).
    pub fn on_reject(
        &mut self,
        cloid: &ClientOrderId,
        reason: &str,
        now_ns: i64,
    ) -> Result<(), Error> {
        {
            let order = self.get_order_mut(cloid)?;
            if order.status.is_terminal() {
                return Err(Error::InvalidTransition {
                    cloid: cloid.to_string(),
                    from: order.status,
                    to: OrderStatus::Rejected,
                });
            }
        }
        let order = self.orders.get_mut(cloid).unwrap();
        order.status = OrderStatus::Rejected;
        order.last_updated_ns = now_ns;

        let entry = JournalEntry::OrderRejected {
            timestamp_ns: now_ns,
            cloid: cloid.clone(),
            reason: reason.to_string(),
        };
        self.journal.append(&entry)
    }

    /// The order expired (IOC not fully matched, GTC past date, etc.).
    pub fn on_expire(&mut self, cloid: &ClientOrderId, now_ns: i64) -> Result<(), Error> {
        {
            let order = self.get_order_mut(cloid)?;
            if order.status.is_terminal() {
                return Err(Error::InvalidTransition {
                    cloid: cloid.to_string(),
                    from: order.status,
                    to: OrderStatus::Expired,
                });
            }
        }
        let order = self.orders.get_mut(cloid).unwrap();
        order.status = OrderStatus::Expired;
        order.last_updated_ns = now_ns;

        let entry = JournalEntry::OrderExpired {
            timestamp_ns: now_ns,
            cloid: cloid.clone(),
        };
        self.journal.append(&entry)
    }

    // ---------------------------------------------------------------------------
    // Queries
    // ---------------------------------------------------------------------------

    pub fn get_order(&self, cloid: &ClientOrderId) -> Option<&PendingOrder> {
        self.orders.get(cloid)
    }

    pub fn pending_count(&self) -> usize {
        self.orders
            .values()
            .filter(|o| !o.status.is_terminal())
            .count()
    }

    pub fn order_count(&self) -> usize {
        self.orders.len()
    }

    pub fn non_terminal_orders(&self) -> impl Iterator<Item = &PendingOrder> {
        self.orders.values().filter(|o| !o.status.is_terminal())
    }

    pub fn orders(&self) -> impl Iterator<Item = &PendingOrder> {
        self.orders.values()
    }

    pub fn instance_id(&self) -> u32 {
        self.instance_id
    }

    // ---------------------------------------------------------------------------
    // Internals
    // ---------------------------------------------------------------------------

    fn get_order_mut(&mut self, cloid: &ClientOrderId) -> Result<&mut PendingOrder, Error> {
        self.orders
            .get_mut(cloid)
            .ok_or_else(|| Error::OrderNotFound {
                cloid: cloid.to_string(),
            })
    }

    /// Check that the order is not in a terminal state (for transitions that
    /// should only happen while the order is still live).
    fn require_non_terminal(&self, cloid: &ClientOrderId, to: OrderStatus) -> Result<(), Error> {
        let order = self.orders.get(cloid).ok_or_else(|| Error::OrderNotFound {
            cloid: cloid.to_string(),
        })?;
        if order.status.is_terminal() {
            Err(Error::InvalidTransition {
                cloid: cloid.to_string(),
                from: order.status,
                to,
            })
        } else {
            Ok(())
        }
    }

    /// Replay recovered journal entries to rebuild in-memory order state and
    /// advance the cloid generator past the last issued counter.
    fn replay_journal(&mut self, entries: Vec<JournalEntry>) -> Result<(), Error> {
        let mut max_counter: Option<u64> = None;

        for entry in entries {
            match entry {
                JournalEntry::OrderRequested {
                    timestamp_ns,
                    cloid,
                    request,
                } => {
                    if let Some(ctr) = cloid.parse_counter() {
                        max_counter = Some(max_counter.map_or(ctr, |m: u64| m.max(ctr)));
                    }
                    self.orders.insert(
                        cloid.clone(),
                        PendingOrder {
                            cloid,
                            request,
                            status: OrderStatus::Pending,
                            exchange_id: None,
                            filled_qty: 0,
                            submitted_ns: timestamp_ns,
                            last_updated_ns: timestamp_ns,
                        },
                    );
                }
                JournalEntry::OrderAcknowledged {
                    timestamp_ns,
                    cloid,
                    exchange_id,
                } => {
                    if let Some(o) = self.orders.get_mut(&cloid) {
                        if !o.status.is_terminal() {
                            o.status = OrderStatus::New;
                            o.exchange_id = Some(exchange_id);
                            o.last_updated_ns = timestamp_ns;
                        }
                    }
                }
                JournalEntry::OrderFilled {
                    timestamp_ns,
                    cloid,
                    fill_qty,
                    fill_price: _,
                    is_final,
                } => {
                    if let Some(o) = self.orders.get_mut(&cloid) {
                        if !o.status.is_terminal() {
                            o.filled_qty += fill_qty;
                            o.last_updated_ns = timestamp_ns;
                            o.status = if is_final {
                                OrderStatus::Filled
                            } else {
                                OrderStatus::PartiallyFilled
                            };
                        }
                    }
                }
                JournalEntry::OrderCancelled {
                    timestamp_ns,
                    cloid,
                } => {
                    if let Some(o) = self.orders.get_mut(&cloid) {
                        if !o.status.is_terminal() {
                            o.status = OrderStatus::Cancelled;
                            o.last_updated_ns = timestamp_ns;
                        }
                    }
                }
                JournalEntry::OrderRejected {
                    timestamp_ns,
                    cloid,
                    reason: _,
                } => {
                    if let Some(o) = self.orders.get_mut(&cloid) {
                        if !o.status.is_terminal() {
                            o.status = OrderStatus::Rejected;
                            o.last_updated_ns = timestamp_ns;
                        }
                    }
                }
                JournalEntry::OrderExpired {
                    timestamp_ns,
                    cloid,
                } => {
                    if let Some(o) = self.orders.get_mut(&cloid) {
                        if !o.status.is_terminal() {
                            o.status = OrderStatus::Expired;
                            o.last_updated_ns = timestamp_ns;
                        }
                    }
                }
            }
        }

        // Advance the generator past the highest counter seen, so no cloid is reused.
        if let Some(max) = max_counter {
            self.cloid_gen = ClientOrderIdGenerator::with_start_after(self.instance_id, max);
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{OrderSide, OrderType, TimeInForce};

    fn req() -> OrderRequest {
        OrderRequest {
            symbol: "BTCUSDT".into(),
            side: OrderSide::Buy,
            order_type: OrderType::Limit,
            qty: 100,
            limit_price: Some(50_000_000),
            time_in_force: TimeInForce::GoodTillCancel,
        }
    }

    fn gw() -> OrderGateway {
        OrderGateway::in_memory(0)
    }

    #[test]
    fn enqueue_returns_unique_cloids() {
        let mut gw = gw();
        let c1 = gw.enqueue(req(), 0).unwrap();
        let c2 = gw.enqueue(req(), 0).unwrap();
        assert_ne!(c1, c2);
    }

    #[test]
    fn enqueue_sets_status_pending() {
        let mut gw = gw();
        let cloid = gw.enqueue(req(), 0).unwrap();
        let order = gw.get_order(&cloid).unwrap();
        assert_eq!(order.status, OrderStatus::Pending);
    }

    #[test]
    fn enqueue_stores_request_fields() {
        let mut gw = gw();
        let r = req();
        let cloid = gw.enqueue(r.clone(), 1_000).unwrap();
        let order = gw.get_order(&cloid).unwrap();
        assert_eq!(order.request.symbol, "BTCUSDT");
        assert_eq!(order.request.qty, 100);
        assert_eq!(order.submitted_ns, 1_000);
    }

    #[test]
    fn on_ack_transitions_pending_to_new() {
        let mut gw = gw();
        let cloid = gw.enqueue(req(), 0).unwrap();
        gw.on_ack(&cloid, 42, 1).unwrap();
        let order = gw.get_order(&cloid).unwrap();
        assert_eq!(order.status, OrderStatus::New);
        assert_eq!(order.exchange_id, Some(42));
    }

    #[test]
    fn on_ack_is_idempotent() {
        let mut gw = gw();
        let cloid = gw.enqueue(req(), 0).unwrap();
        gw.on_ack(&cloid, 1, 0).unwrap();
        gw.on_ack(&cloid, 1, 0).unwrap(); // must not error
        assert_eq!(gw.get_order(&cloid).unwrap().status, OrderStatus::New);
    }

    #[test]
    fn on_ack_unknown_cloid_returns_err() {
        let mut gw = gw();
        let fake = ClientOrderIdGenerator::new(99).next();
        let err = gw.on_ack(&fake, 1, 0);
        assert!(matches!(err, Err(Error::OrderNotFound { .. })));
    }

    #[test]
    fn on_fill_partial_stays_partially_filled() {
        let mut gw = gw();
        let cloid = gw.enqueue(req(), 0).unwrap();
        gw.on_ack(&cloid, 1, 1).unwrap();
        gw.on_fill(&cloid, 30, 50_000_000, 2).unwrap();
        let order = gw.get_order(&cloid).unwrap();
        assert_eq!(order.status, OrderStatus::PartiallyFilled);
        assert_eq!(order.filled_qty, 30);
    }

    #[test]
    fn on_fill_full_transitions_to_filled() {
        let mut gw = gw();
        let cloid = gw.enqueue(req(), 0).unwrap();
        gw.on_ack(&cloid, 1, 1).unwrap();
        gw.on_fill(&cloid, 100, 50_000_000, 2).unwrap();
        assert_eq!(gw.get_order(&cloid).unwrap().status, OrderStatus::Filled);
    }

    #[test]
    fn on_fill_accumulates_across_partial_fills() {
        let mut gw = gw();
        let cloid = gw.enqueue(req(), 0).unwrap();
        gw.on_ack(&cloid, 1, 1).unwrap();
        gw.on_fill(&cloid, 40, 49_900_000, 2).unwrap();
        gw.on_fill(&cloid, 40, 50_000_000, 3).unwrap();
        gw.on_fill(&cloid, 20, 50_100_000, 4).unwrap();
        let order = gw.get_order(&cloid).unwrap();
        assert_eq!(order.filled_qty, 100);
        assert_eq!(order.status, OrderStatus::Filled);
    }

    #[test]
    fn on_cancel_transitions_to_cancelled() {
        let mut gw = gw();
        let cloid = gw.enqueue(req(), 0).unwrap();
        gw.on_ack(&cloid, 1, 1).unwrap();
        gw.on_cancel(&cloid, 2).unwrap();
        assert_eq!(gw.get_order(&cloid).unwrap().status, OrderStatus::Cancelled);
    }

    #[test]
    fn on_cancel_is_idempotent() {
        let mut gw = gw();
        let cloid = gw.enqueue(req(), 0).unwrap();
        gw.on_cancel(&cloid, 1).unwrap();
        gw.on_cancel(&cloid, 2).unwrap(); // must not error
        assert_eq!(gw.get_order(&cloid).unwrap().status, OrderStatus::Cancelled);
    }

    #[test]
    fn on_reject_transitions_to_rejected() {
        let mut gw = gw();
        let cloid = gw.enqueue(req(), 0).unwrap();
        gw.on_reject(&cloid, "PRICE_FILTER", 1).unwrap();
        assert_eq!(gw.get_order(&cloid).unwrap().status, OrderStatus::Rejected);
    }

    #[test]
    fn on_expire_transitions_to_expired() {
        let mut gw = gw();
        let cloid = gw.enqueue(req(), 0).unwrap();
        gw.on_ack(&cloid, 1, 1).unwrap();
        gw.on_expire(&cloid, 2).unwrap();
        assert_eq!(gw.get_order(&cloid).unwrap().status, OrderStatus::Expired);
    }

    #[test]
    fn on_ack_after_filled_is_invalid_transition() {
        let mut gw = gw();
        let cloid = gw.enqueue(req(), 0).unwrap();
        gw.on_ack(&cloid, 1, 1).unwrap();
        gw.on_fill(&cloid, 100, 50_000_000, 2).unwrap();
        let err = gw.on_ack(&cloid, 1, 3);
        assert!(matches!(err, Err(Error::InvalidTransition { .. })));
    }

    #[test]
    fn on_fill_after_cancelled_is_invalid_transition() {
        let mut gw = gw();
        let cloid = gw.enqueue(req(), 0).unwrap();
        gw.on_cancel(&cloid, 1).unwrap();
        let err = gw.on_fill(&cloid, 10, 50_000_000, 2);
        assert!(matches!(err, Err(Error::InvalidTransition { .. })));
    }

    #[test]
    fn pending_count_tracks_correctly() {
        let mut gw = gw();
        assert_eq!(gw.pending_count(), 0);
        let c1 = gw.enqueue(req(), 0).unwrap();
        let c2 = gw.enqueue(req(), 0).unwrap();
        assert_eq!(gw.pending_count(), 2);
        gw.on_fill(&c1, 100, 50_000_000, 1).unwrap();
        assert_eq!(gw.pending_count(), 1, "c1 is terminal after full fill");
        gw.on_cancel(&c2, 2).unwrap();
        assert_eq!(gw.pending_count(), 0);
    }

    #[test]
    fn non_terminal_orders_excludes_terminal() {
        let mut gw = gw();
        let c1 = gw.enqueue(req(), 0).unwrap();
        let c2 = gw.enqueue(req(), 0).unwrap();
        gw.on_reject(&c1, "x", 0).unwrap();

        let live: Vec<_> = gw.non_terminal_orders().collect();
        assert_eq!(live.len(), 1);
        assert_eq!(live[0].cloid, c2);
    }

    #[test]
    fn gateway_recovery_from_journal() {
        // Build a journal with one order in New state.
        let mut gw1 = OrderGateway::in_memory(0);
        let cloid = gw1.enqueue(req(), 0).unwrap();
        gw1.on_ack(&cloid, 9999, 1).unwrap();

        // Recover entries from the in-memory journal.
        let recovered_entries = gw1.journal.read_all_in_memory().unwrap();

        // Replay into a fresh gateway.
        let mut gw2 = OrderGateway {
            instance_id: 0,
            cloid_gen: ClientOrderIdGenerator::new(0),
            journal: Journal::in_memory(),
            orders: HashMap::new(),
        };
        gw2.replay_journal(recovered_entries).unwrap();

        // Verify recovered state.
        let order = gw2.get_order(&cloid).expect("order must survive replay");
        assert_eq!(order.status, OrderStatus::New);
        assert_eq!(order.exchange_id, Some(9999));
    }

    #[test]
    fn recovery_advances_cloid_generator_past_last_counter() {
        let mut gw1 = OrderGateway::in_memory(0);
        for _ in 0..5 {
            gw1.enqueue(req(), 0).unwrap();
        }
        let recovered_entries = gw1.journal.read_all_in_memory().unwrap();

        let mut gw2 = OrderGateway {
            instance_id: 0,
            cloid_gen: ClientOrderIdGenerator::new(0),
            journal: Journal::in_memory(),
            orders: HashMap::new(),
        };
        gw2.replay_journal(recovered_entries).unwrap();

        // Next cloid must not collide with any previously issued cloid.
        let next_cloid = gw2.cloid_gen.next();
        let next_ctr = next_cloid.parse_counter().unwrap();
        assert!(
            next_ctr >= 5,
            "generator must resume after counter 4; got {next_ctr}"
        );
    }
}
