use crate::Error;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum VenueId {
    BinanceSpot    = 1,
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
    Spot        = 1,
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

/// All normalized outbound message types.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum MessageType {
    InstrumentDefinition = 1,
    TradingStatus        = 2,
    BookSnapshot         = 3,
    BookDelta            = 4,
    BestBidOffer         = 5,
    Trade                = 6,
    MarkPrice            = 7,
    FundingRate          = 8,
    Liquidation          = 9,
    OpenInterest         = 10,
    AccountUpdate        = 11,
    OrderUpdate          = 12,
    Heartbeat            = 13,
    FeedStatus           = 14,
    GapDetected          = 15,
    BookStale            = 16,
    BookRecovered        = 17,
}

impl TryFrom<u8> for MessageType {
    type Error = Error;
    fn try_from(v: u8) -> Result<Self, Self::Error> {
        match v {
            1  => Ok(Self::InstrumentDefinition),
            2  => Ok(Self::TradingStatus),
            3  => Ok(Self::BookSnapshot),
            4  => Ok(Self::BookDelta),
            5  => Ok(Self::BestBidOffer),
            6  => Ok(Self::Trade),
            7  => Ok(Self::MarkPrice),
            8  => Ok(Self::FundingRate),
            9  => Ok(Self::Liquidation),
            10 => Ok(Self::OpenInterest),
            11 => Ok(Self::AccountUpdate),
            12 => Ok(Self::OrderUpdate),
            13 => Ok(Self::Heartbeat),
            14 => Ok(Self::FeedStatus),
            15 => Ok(Self::GapDetected),
            16 => Ok(Self::BookStale),
            17 => Ok(Self::BookRecovered),
            _  => Err(Error::UnknownMessageType(v)),
        }
    }
}
