mod backpressure;
mod error;
mod heartbeat;
pub mod publication;
mod publisher;

pub use backpressure::{
    BackpressureGuard,
    OfferOutcome,
    DEFAULT_WARN_NS, DEFAULT_DEGRADE_NS, DEFAULT_RESTART_NS,
};
pub use error::PublisherError;
pub use heartbeat::{Heartbeater, HEARTBEAT_INTERVAL_NS};
pub use publication::{ChannelPublication, NullPublication, OfferResult, Publication};
pub use publisher::{
    ShardedPublisher, DynShardedPublisher,
    build_channel, build_null, build_null_boxed,
    channel_from_config,
    ipc_channel, udp_channel,
    shard_stream_id,
};

#[cfg(feature = "aeron")]
pub use publisher::build_aeron;
