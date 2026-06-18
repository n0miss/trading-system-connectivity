use thiserror::Error;

#[derive(Debug, Error, PartialEq, Eq)]
pub enum SbeError {
    #[error("buffer too short: need {needed} bytes, have {have}")]
    BufferTooShort { needed: usize, have: usize },

    #[error("schema ID mismatch: expected {expected}, got {actual}")]
    SchemaMismatch { expected: u16, actual: u16 },

    #[error("schema version unsupported: max {max}, got {actual}")]
    VersionTooNew { max: u16, actual: u16 },

    #[error("unknown template ID: {0}")]
    UnknownTemplateId(u16),

    #[error("invalid UTF-8 in string field")]
    InvalidUtf8,

    #[error("group entry count {count} exceeds limit {limit}")]
    GroupOverflow { count: usize, limit: usize },
}
