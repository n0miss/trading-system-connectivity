use thiserror::Error;

#[derive(Debug, Error, PartialEq, Eq)]
pub enum PublisherError {
    #[error("no publication registered for shard {0}")]
    UnknownShard(u32),

    #[error("failed to connect to Aeron media driver: {0}")]
    AeronConnect(String),
}
