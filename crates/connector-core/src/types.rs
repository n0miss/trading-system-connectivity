use crate::Error;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum VenueId {
    BinanceSpot = 1,
    BinanceFutures = 2,
}

impl TryFrom<u8> for VenueId {
    type Error = Error;
    fn try_from(v: u8) -> Result<Self, Self::Error> {
        match v {
            1 => Ok(Self::BinanceSpot),
            2 => Ok(Self::BinanceFutures),
            _ => Err(Error::UnknownVenueId(v)),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum MarketType {
    Spot = 1,
    UsdmFutures = 2,
}

impl TryFrom<u8> for MarketType {
    type Error = Error;
    fn try_from(v: u8) -> Result<Self, Self::Error> {
        match v {
            1 => Ok(Self::Spot),
            2 => Ok(Self::UsdmFutures),
            _ => Err(Error::UnknownMarketType(v)),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum MessageType {
    InstrumentDefinition = 1,
    TradingStatus = 2,
    BookSnapshot = 3,
    BookDelta = 4,
    BestBidOffer = 5,
    Trade = 6,
    MarkPrice = 7,
    FundingRate = 8,
    Liquidation = 9,
    OpenInterest = 10,
    AccountUpdate = 11,
    OrderUpdate = 12,
    Heartbeat = 13,
    FeedStatus = 14,
    GapDetected = 15,
    BookStale = 16,
    BookRecovered = 17,
    /// Published by passive instances to the status stream (§9.34).
    BookChecksum = 18,
}

impl TryFrom<u8> for MessageType {
    type Error = Error;
    fn try_from(v: u8) -> Result<Self, Self::Error> {
        match v {
            1 => Ok(Self::InstrumentDefinition),
            2 => Ok(Self::TradingStatus),
            3 => Ok(Self::BookSnapshot),
            4 => Ok(Self::BookDelta),
            5 => Ok(Self::BestBidOffer),
            6 => Ok(Self::Trade),
            7 => Ok(Self::MarkPrice),
            8 => Ok(Self::FundingRate),
            9 => Ok(Self::Liquidation),
            10 => Ok(Self::OpenInterest),
            11 => Ok(Self::AccountUpdate),
            12 => Ok(Self::OrderUpdate),
            13 => Ok(Self::Heartbeat),
            14 => Ok(Self::FeedStatus),
            15 => Ok(Self::GapDetected),
            16 => Ok(Self::BookStale),
            17 => Ok(Self::BookRecovered),
            18 => Ok(Self::BookChecksum),
            _ => Err(Error::UnknownMessageType(v)),
        }
    }
}

// ---------------------------------------------------------------------------
// InstanceRole
// ---------------------------------------------------------------------------

/// Whether this connector process is the active (primary) or passive (shadow) instance.
///
/// Both roles run the full pipeline and maintain their own order books.
/// The difference is where their output goes:
///
/// * **Active** → publishes [`NormalizedMessage`]s to the main market-data Aeron stream.
/// * **Passive** → publishes [`BookChecksum`] messages to the status stream so the
///   cross-instance comparator (§9.35) can detect divergence and trigger failover.
///
/// Convention: instance 0 is `Active`; all others are `Passive`.
/// For a standard two-instance deployment: `id = 0` → Active, `id = 1` → Passive.
///
/// [`NormalizedMessage`]: crate::NormalizedMessage
/// [`BookChecksum`]: crate::BookChecksum
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum InstanceRole {
    Active,
    Passive,
}

impl InstanceRole {
    /// Derive a role from a zero-based `instance_id`.
    ///
    /// Instance 0 is always Active.  This is intentionally simple: the
    /// arbiter (§9.35) handles the failover logic when the active instance
    /// diverges or goes silent.
    pub fn from_instance_id(id: u32) -> Self {
        if id == 0 {
            Self::Active
        } else {
            Self::Passive
        }
    }

    pub fn is_active(self) -> bool {
        self == Self::Active
    }
    pub fn is_passive(self) -> bool {
        self == Self::Passive
    }
}

impl std::fmt::Display for InstanceRole {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Active => f.write_str("active"),
            Self::Passive => f.write_str("passive"),
        }
    }
}

/// Feed health state, published in FeedStatus messages.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum FeedState {
    Connecting = 1,
    Live = 2,
    Degraded = 3,
    Stale = 4,
    Recovering = 5,
    Recovered = 6,
    Failed = 7,
}

impl TryFrom<u8> for FeedState {
    type Error = Error;
    fn try_from(v: u8) -> Result<Self, Self::Error> {
        match v {
            1 => Ok(Self::Connecting),
            2 => Ok(Self::Live),
            3 => Ok(Self::Degraded),
            4 => Ok(Self::Stale),
            5 => Ok(Self::Recovering),
            6 => Ok(Self::Recovered),
            7 => Ok(Self::Failed),
            _ => Err(Error::UnknownFeedState(v)),
        }
    }
}

/// Aggressor side in a trade.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum AggressorSide {
    Unknown = 0,
    Buy = 1,
    Sell = 2,
}

impl TryFrom<u8> for AggressorSide {
    type Error = Error;
    fn try_from(v: u8) -> Result<Self, Self::Error> {
        match v {
            0 => Ok(Self::Unknown),
            1 => Ok(Self::Buy),
            2 => Ok(Self::Sell),
            _ => Err(Error::UnknownAggressorSide(v)),
        }
    }
}

/// Reason a book was marked stale, carried in BookStale messages.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum BookStaleReason {
    SequenceGap = 1,
    WebSocketReconnect = 2,
    SnapshotIncompatible = 3,
    StaleTimeout = 4,
    MalformedEvent = 5,
    ExchangeShutdown = 6,
    BboMismatch = 7,
    BufferOverflow = 8,
}

impl TryFrom<u8> for BookStaleReason {
    type Error = Error;
    fn try_from(v: u8) -> Result<Self, Self::Error> {
        match v {
            1 => Ok(Self::SequenceGap),
            2 => Ok(Self::WebSocketReconnect),
            3 => Ok(Self::SnapshotIncompatible),
            4 => Ok(Self::StaleTimeout),
            5 => Ok(Self::MalformedEvent),
            6 => Ok(Self::ExchangeShutdown),
            7 => Ok(Self::BboMismatch),
            8 => Ok(Self::BufferOverflow),
            _ => Err(Error::UnknownBookStaleReason(v)),
        }
    }
}
