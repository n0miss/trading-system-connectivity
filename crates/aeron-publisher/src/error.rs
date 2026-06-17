use thiserror::Error;

#[derive(Debug, Error, PartialEq, Eq)]
pub enum PublisherError {
    #[error("no publication registered for shard {0}")]
    UnknownShard(u32),
}
