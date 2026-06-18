use thiserror::Error;

#[derive(Debug, Error)]
pub enum AdapterError {
    #[error("WebSocket error: {0}")]
    WebSocket(#[from] tokio_tungstenite::tungstenite::Error),
    #[error("WebSocket connect timed out")]
    ConnectTimeout,
}
