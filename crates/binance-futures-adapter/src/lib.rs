pub mod circuit_breaker;
pub mod connection_manager;
mod error;
pub mod normalizer;
pub mod recovery;
pub mod recovery_buffer;
pub mod sequence;
pub mod shard_engine;
pub mod stream;
pub mod symbol_state;

pub use circuit_breaker::{CircuitBreaker, CircuitState};
pub use connection_manager::{ConnectionManager, RawFrame};
pub use error::FuturesAdapterError;
pub use normalizer::{normalize_futures_event, NormalizeCtx, NormalizeError};
pub use recovery::{apply_futures_snapshot, run_futures_recovery, RecoveryError, RecoveryOutcome};
pub use recovery_buffer::{
    BufferedDelta, OverflowReason, PushResult, RecoveryBuffer, MAX_AGE_NS, MAX_BYTES, MAX_EVENTS,
};
pub use sequence::{FuturesSequenceValidator, ValidateResult, ValidationState};
pub use shard_engine::FuturesShardEngine;
pub use stream::{build_url, FuturesStream, FUTURES_WS_BASE};
pub use symbol_state::FuturesSymbolState;
