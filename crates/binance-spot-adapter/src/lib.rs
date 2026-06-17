mod error;
mod stream;
mod connection_manager;
mod normalizer;
pub mod sequence;
pub mod recovery_buffer;
pub mod recovery;
pub mod circuit_breaker;
pub mod bbo_validator;

pub use error::AdapterError;
pub use stream::{SpotStream, build_url};
pub use connection_manager::{ConnectionManager, RawFrame};
pub use normalizer::{NormalizeCtx, NormalizeError, normalize_spot_event};
pub use sequence::{SequenceValidator, ValidateResult, ValidationState};
pub use recovery_buffer::{BufferedDelta, OverflowReason, PushResult, RecoveryBuffer};
pub use recovery::{RecoveryError, RecoveryOutcome, run_spot_recovery};
pub use circuit_breaker::{CircuitBreaker, CircuitState};
pub use bbo_validator::{BboValidator, BboCheckResult};
