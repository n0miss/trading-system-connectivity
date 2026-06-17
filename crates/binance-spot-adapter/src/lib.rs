mod error;
mod stream;
mod connection_manager;

pub use error::AdapterError;
pub use stream::{SpotStream, build_url};
pub use connection_manager::{ConnectionManager, RawFrame};
