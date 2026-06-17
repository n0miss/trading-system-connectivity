pub mod circuit_breaker;
pub mod connection_manager;
mod error;
pub mod normalizer;
pub mod recovery;
pub mod recovery_buffer;
pub mod shard_engine;
pub mod sequence;
pub mod stream;
pub mod symbol_state;

pub use circuit_breaker::{CircuitBreaker, CircuitState};
pub use connection_manager::{ConnectionManager, RawFrame};
pub use error::FuturesAdapterError;
pub use normalizer::{NormalizeCtx, NormalizeError, normalize_futures_event};
pub use recovery::{RecoveryError, RecoveryOutcome, apply_futures_snapshot, run_futures_recovery};
pub use recovery_buffer::{
    BufferedDelta, OverflowReason, PushResult, RecoveryBuffer,
    MAX_AGE_NS, MAX_BYTES, MAX_EVENTS,
};
pub use shard_engine::FuturesShardEngine;
pub use sequence::{FuturesSequenceValidator, ValidateResult, ValidationState};
pub use stream::{FuturesStream, FUTURES_WS_BASE, build_url};
pub use symbol_state::FuturesSymbolState;
