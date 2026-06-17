use thiserror::Error;

#[derive(Debug, Error)]
pub enum RefDataError {
    #[error("HTTP error: {0}")]
    Http(#[from] reqwest::Error),

    #[error("JSON parse error: {0}")]
    Json(#[from] serde_json::Error),

    #[error("missing required filter \"{filter}\" for symbol \"{symbol}\"")]
    MissingFilter { symbol: String, filter: &'static str },

    #[error("invalid numeric string \"{value}\" for field \"{field}\"")]
    InvalidNumeric { value: String, field: &'static str },
}
