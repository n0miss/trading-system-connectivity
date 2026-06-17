pub mod connection_manager;
mod error;
pub mod normalizer;
pub mod sequence;
pub mod stream;

pub use connection_manager::{ConnectionManager, RawFrame};
pub use error::FuturesAdapterError;
pub use normalizer::{NormalizeCtx, NormalizeError, normalize_futures_event};
pub use sequence::{FuturesSequenceValidator, ValidateResult, ValidationState};
pub use stream::{FuturesStream, FUTURES_WS_BASE, build_url};
