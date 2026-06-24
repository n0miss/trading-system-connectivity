pub mod bbo_validator;
pub mod circuit_breaker;
mod connection_manager;
mod error;
pub mod feed;
pub mod instrument;
mod normalizer;
pub mod recovery;
pub mod recovery_buffer;
mod sbe_normalizer;
pub mod sequence;
pub mod shard_engine;
pub mod snapshot_validator;
mod stream;
pub mod symbol_state;

pub use bbo_validator::{BboCheckResult, BboValidator};
pub use circuit_breaker::{CircuitBreaker, CircuitState};
pub use connection_manager::{ConnectionManager, RawFrame};
pub use error::AdapterError;
pub use feed::{decode_raw_frame, DecodedFrame, FeedError, FeedKind};
pub use instrument::{
    record_book_stale, record_offer_failure, record_publish, record_sequence_gap,
};
pub use normalizer::{normalize_spot_event, NormalizeCtx, NormalizeError};
pub use recovery::{apply_spot_snapshot, run_spot_recovery, RecoveryError, RecoveryOutcome};
pub use recovery_buffer::{BufferedDelta, OverflowReason, PushResult, RecoveryBuffer};
pub use sbe_normalizer::normalize_sbe_message;
pub use sequence::{SequenceValidator, ValidateResult, ValidationState};
pub use shard_engine::ShardEngine;
pub use snapshot_validator::{
    check_snapshot, SnapshotCheckResult, SnapshotValidatorConfig,
    INTERVAL_SECS as SNAPSHOT_INTERVAL_SECS,
};
pub use stream::{build_url, SpotStream, SPOT_WS_BASE};
pub use symbol_state::SymbolState;
