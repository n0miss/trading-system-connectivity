use thiserror::Error;

#[derive(Debug, Error, PartialEq, Eq)]
pub enum Error {
    #[error("buffer too short: need {needed} bytes, have {have}")]
    BufferTooShort { needed: usize, have: usize },

    #[error("unknown venue id: {0}")]
    UnknownVenueId(u8),

    #[error("unknown market type: {0}")]
    UnknownMarketType(u8),

    #[error("unknown message type: {0}")]
    UnknownMessageType(u8),
}
