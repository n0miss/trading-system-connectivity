mod error;
mod stream;
mod connection_manager;
mod normalizer;

pub use error::AdapterError;
pub use stream::{SpotStream, build_url};
pub use connection_manager::{ConnectionManager, RawFrame};
pub use normalizer::{NormalizeCtx, NormalizeError, normalize_spot_event};
