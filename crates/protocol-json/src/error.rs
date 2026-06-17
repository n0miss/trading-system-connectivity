use thiserror::Error;

#[derive(Debug, Error)]
pub enum JsonError {
    #[error("JSON parse error: {0}")]
    Parse(#[from] serde_json::Error),
}
