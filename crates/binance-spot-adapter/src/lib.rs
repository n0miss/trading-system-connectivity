mod error;
mod stream;
mod connection_manager;
mod normalizer;
pub mod sequence;
pub mod recovery_buffer;

pub use error::AdapterError;
pub use stream::{SpotStream, build_url};
pub use connection_manager::{ConnectionManager, RawFrame};
pub use normalizer::{NormalizeCtx, NormalizeError, normalize_spot_event};
pub use sequence::{SequenceValidator, ValidateResult, ValidationState};
pub use recovery_buffer::{BufferedDelta, OverflowReason, PushResult, RecoveryBuffer};
