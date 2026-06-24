use crate::OrderStatus;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("order not found: {cloid}")]
    OrderNotFound { cloid: String },

    #[error("invalid state transition for {cloid}: {from:?} → {to:?}")]
    InvalidTransition {
        cloid: String,
        from: OrderStatus,
        to: OrderStatus,
    },

    #[error("journal I/O: {0}")]
    Io(#[from] std::io::Error),

    #[error("journal corrupt at byte {offset}: {reason}")]
    JournalCorrupt { offset: usize, reason: String },

    #[error("buffer too short: need {needed}, have {have}")]
    BufferTooShort { needed: usize, have: usize },
}
