use thiserror::Error;

use crate::types::MessageType;

#[derive(Debug, Error, PartialEq, Eq)]
pub enum Error {
    // --- codec ---
    #[error("buffer too short: need {needed} bytes, have {have}")]
    BufferTooShort { needed: usize, have: usize },

    #[error("string exceeds max length: {len} bytes, max {max}")]
    StringTooLong { len: usize, max: usize },

    #[error("vec too long: {count} items, max {max}")]
    VecTooLong { count: usize, max: usize },

    #[error("invalid UTF-8 in string field")]
    InvalidUtf8,

    // --- type discriminants ---
    #[error("unknown venue id: {0}")]
    UnknownVenueId(u8),

    #[error("unknown market type: {0}")]
    UnknownMarketType(u8),

    #[error("unknown message type: {0}")]
    UnknownMessageType(u8),

    #[error("unknown feed state: {0}")]
    UnknownFeedState(u8),

    #[error("unknown aggressor side: {0}")]
    UnknownAggressorSide(u8),

    #[error("unknown book stale reason: {0}")]
    UnknownBookStaleReason(u8),

    // --- structural ---
    #[error("wrong message type in header: got {got:?}, expected {expected:?}")]
    MessageTypeMismatch {
        got: MessageType,
        expected: MessageType,
    },
}
