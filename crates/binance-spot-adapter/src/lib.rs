mod error;
mod stream;
mod connection_manager;
mod normalizer;
mod sbe_normalizer;
pub mod feed;
pub mod instrument;
pub mod sequence;
pub mod recovery_buffer;
pub mod recovery;
pub mod circuit_breaker;
pub mod bbo_validator;
pub mod snapshot_validator;
pub mod symbol_state;
pub mod shard_engine;

pub use error::AdapterError;
pub use stream::{SpotStream, build_url, SPOT_WS_BASE};
pub use connection_manager::{ConnectionManager, RawFrame};
pub use normalizer::{NormalizeCtx, NormalizeError, normalize_spot_event};
pub use sbe_normalizer::normalize_sbe_message;
pub use feed::{decode_raw_frame, DecodedFrame, FeedError, FeedKind};
pub use instrument::{
    record_publish, record_sequence_gap, record_book_stale, record_offer_failure,
};
pub use sequence::{SequenceValidator, ValidateResult, ValidationState};
pub use recovery_buffer::{BufferedDelta, OverflowReason, PushResult, RecoveryBuffer};
pub use recovery::{RecoveryError, RecoveryOutcome, apply_spot_snapshot, run_spot_recovery};
pub use circuit_breaker::{CircuitBreaker, CircuitState};
pub use bbo_validator::{BboValidator, BboCheckResult};
pub use snapshot_validator::{
    check_snapshot, SnapshotCheckResult, SnapshotValidatorConfig,
    INTERVAL_SECS as SNAPSHOT_INTERVAL_SECS,
};
pub use symbol_state::SymbolState;
pub use shard_engine::ShardEngine;
