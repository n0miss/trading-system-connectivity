mod error;
mod futures;
mod spot;

pub use error::JsonError;
pub use futures::{
    parse_futures_message, FuturesAggTrade, FuturesBookTicker, FuturesDepthUpdate, FuturesEvent,
    FuturesForceOrder, FuturesLiquidationOrder, FuturesMarkPrice,
};
pub use spot::{parse_spot_message, SpotBookTicker, SpotDepthUpdate, SpotEvent, SpotTrade};
