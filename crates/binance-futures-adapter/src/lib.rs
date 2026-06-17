mod error;
pub mod stream;

pub use error::FuturesAdapterError;
pub use stream::{FuturesStream, FUTURES_WS_BASE, build_url};
