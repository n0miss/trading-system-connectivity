mod error;
mod spot;

pub use error::JsonError;
pub use spot::{
    parse_spot_message,
    SpotBookTicker,
    SpotDepthUpdate,
    SpotEvent,
    SpotTrade,
};
