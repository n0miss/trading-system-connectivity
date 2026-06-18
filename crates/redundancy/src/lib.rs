/// Active/passive redundancy support (§9.34).
///
/// # Overview
///
/// Both the active and passive connector instances run the full WebSocket
/// pipeline and maintain their own order books.  They differ only in where
/// their output goes:
///
/// * **Active** — publishes [`NormalizedMessage`]s to the main market-data
///   Aeron stream (one per shard).
/// * **Passive** — publishes [`BookChecksum`] messages to the *status stream*
///   after each book update so the cross-instance comparator (§9.35) can
///   detect divergence and trigger failover.
///
/// This crate provides:
///
/// * [`InstanceRole`] re-export for convenience.
/// * [`ChecksumPublisher`] — encodes and offers `BookChecksum` messages to a
///   caller-supplied [`Publication`].
///
/// [`NormalizedMessage`]: connector_core::NormalizedMessage
/// [`BookChecksum`]: connector_core::BookChecksum
/// [`InstanceRole`]: connector_core::InstanceRole
/// [`Publication`]: connector_aeron::Publication

pub mod arbiter;
mod publisher;

pub use connector_core::InstanceRole;
pub use arbiter::{
    process, ArbiterVerdict, ChecksumArbiter, FailoverTrigger, LogOnlyTrigger,
    DEFAULT_WINDOW,
};
pub use publisher::ChecksumPublisher;
