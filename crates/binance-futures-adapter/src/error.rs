use thiserror::Error;

use protocol_json::JsonError;

#[derive(Debug, Error)]
pub enum FuturesAdapterError {
    #[error("WebSocket error: {0}")]
    WebSocket(#[from] tokio_tungstenite::tungstenite::Error),

    #[error("JSON parse error: {0}")]
    Json(#[from] JsonError),

    #[error("normalize error: {0}")]
    Normalize(String),
}
