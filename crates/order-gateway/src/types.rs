use crate::ClientOrderId;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum OrderSide {
    Buy = 0,
    Sell = 1,
}

impl TryFrom<u8> for OrderSide {
    type Error = u8;
    fn try_from(v: u8) -> Result<Self, u8> {
        match v {
            0 => Ok(Self::Buy),
            1 => Ok(Self::Sell),
            x => Err(x),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum OrderType {
    Limit = 0,
    Market = 1,
}

impl TryFrom<u8> for OrderType {
    type Error = u8;
    fn try_from(v: u8) -> Result<Self, u8> {
        match v {
            0 => Ok(Self::Limit),
            1 => Ok(Self::Market),
            x => Err(x),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum TimeInForce {
    GoodTillCancel = 0,
    ImmediateOrCancel = 1,
    FillOrKill = 2,
}

impl TryFrom<u8> for TimeInForce {
    type Error = u8;
    fn try_from(v: u8) -> Result<Self, u8> {
        match v {
            0 => Ok(Self::GoodTillCancel),
            1 => Ok(Self::ImmediateOrCancel),
            2 => Ok(Self::FillOrKill),
            x => Err(x),
        }
    }
}

/// All states an order can be in from the gateway's perspective.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum OrderStatus {
    /// Journaled but not yet dispatched to the exchange.
    Pending,
    /// Exchange acknowledged; waiting for fills or cancel.
    New,
    /// At least one fill received; more expected.
    PartiallyFilled,
    /// Fully filled — terminal.
    Filled,
    /// Cancel sent to exchange; waiting for confirmation.
    Cancelling,
    /// Exchange confirmed cancel — terminal.
    Cancelled,
    /// Exchange rejected the order — terminal.
    Rejected,
    /// Order expired (IOC not matched, GTC past expiry) — terminal.
    Expired,
}

impl OrderStatus {
    pub fn is_terminal(self) -> bool {
        matches!(
            self,
            Self::Filled | Self::Cancelled | Self::Rejected | Self::Expired
        )
    }
}

impl std::fmt::Display for OrderStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::Pending => "Pending",
            Self::New => "New",
            Self::PartiallyFilled => "PartiallyFilled",
            Self::Filled => "Filled",
            Self::Cancelling => "Cancelling",
            Self::Cancelled => "Cancelled",
            Self::Rejected => "Rejected",
            Self::Expired => "Expired",
        })
    }
}

/// An order request as presented to the gateway before it is sent to the exchange.
/// The gateway assigns the `ClientOrderId` and records this in the journal.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OrderRequest {
    /// Instrument symbol in exchange notation (e.g. "BTCUSDT").
    pub symbol: String,
    pub side: OrderSide,
    pub order_type: OrderType,
    /// Scaled quantity (no floats).
    pub qty: i64,
    /// Scaled limit price. `None` for `Market` orders.
    pub limit_price: Option<i64>,
    pub time_in_force: TimeInForce,
}

/// An order tracked by the gateway after being enqueued.
#[derive(Debug, Clone)]
pub struct PendingOrder {
    pub cloid: ClientOrderId,
    pub request: OrderRequest,
    pub status: OrderStatus,
    /// Exchange-assigned order ID, set on first acknowledgement.
    pub exchange_id: Option<u64>,
    /// Cumulative filled quantity across all partial fill events.
    pub filled_qty: i64,
    pub submitted_ns: i64,
    pub last_updated_ns: i64,
}

impl PendingOrder {
    pub fn unfilled_qty(&self) -> i64 {
        (self.request.qty - self.filled_qty).max(0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn terminal_statuses() {
        assert!(OrderStatus::Filled.is_terminal());
        assert!(OrderStatus::Cancelled.is_terminal());
        assert!(OrderStatus::Rejected.is_terminal());
        assert!(OrderStatus::Expired.is_terminal());
    }

    #[test]
    fn non_terminal_statuses() {
        assert!(!OrderStatus::Pending.is_terminal());
        assert!(!OrderStatus::New.is_terminal());
        assert!(!OrderStatus::PartiallyFilled.is_terminal());
        assert!(!OrderStatus::Cancelling.is_terminal());
    }

    #[test]
    fn order_side_round_trips() {
        assert_eq!(OrderSide::try_from(0), Ok(OrderSide::Buy));
        assert_eq!(OrderSide::try_from(1), Ok(OrderSide::Sell));
        assert!(OrderSide::try_from(99).is_err());
    }

    #[test]
    fn order_type_round_trips() {
        assert_eq!(OrderType::try_from(0), Ok(OrderType::Limit));
        assert_eq!(OrderType::try_from(1), Ok(OrderType::Market));
        assert!(OrderType::try_from(5).is_err());
    }

    #[test]
    fn time_in_force_round_trips() {
        assert_eq!(TimeInForce::try_from(0), Ok(TimeInForce::GoodTillCancel));
        assert_eq!(TimeInForce::try_from(1), Ok(TimeInForce::ImmediateOrCancel));
        assert_eq!(TimeInForce::try_from(2), Ok(TimeInForce::FillOrKill));
        assert!(TimeInForce::try_from(3).is_err());
    }

    #[test]
    fn unfilled_qty_tracks_fills() {
        let cloid = crate::ClientOrderIdGenerator::new(0).next();
        let mut order = PendingOrder {
            cloid,
            request: OrderRequest {
                symbol: "BTCUSDT".into(),
                side: OrderSide::Buy,
                order_type: OrderType::Limit,
                qty: 100,
                limit_price: Some(50_000),
                time_in_force: TimeInForce::GoodTillCancel,
            },
            status: OrderStatus::New,
            exchange_id: None,
            filled_qty: 0,
            submitted_ns: 0,
            last_updated_ns: 0,
        };
        assert_eq!(order.unfilled_qty(), 100);
        order.filled_qty = 40;
        assert_eq!(order.unfilled_qty(), 60);
        order.filled_qty = 100;
        assert_eq!(order.unfilled_qty(), 0);
    }
}
