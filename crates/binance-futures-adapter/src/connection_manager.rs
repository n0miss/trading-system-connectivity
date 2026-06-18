/// WebSocket connection manager for Binance USDT-M Futures streams (§5.23).
///
/// Owns the reconnect loop, ping/pong keepalive, and forced 24-hour rotation.
/// Raw frames are forwarded over the caller-supplied mpsc channel.
use std::time::Duration;

use connector_config::WebSocketConfig;
use futures_util::{SinkExt, StreamExt};
use tokio::sync::{mpsc, watch};
use tokio_tungstenite::{connect_async, tungstenite::Message};
use tracing::{info, warn};

use crate::error::FuturesAdapterError;

// ---------------------------------------------------------------------------
// Public types
// ---------------------------------------------------------------------------

/// A raw WebSocket frame received from Binance Futures.
pub struct RawFrame {
    /// Nanoseconds since Unix epoch when the frame arrived at the local socket.
    pub recv_ts: i64,
    /// Raw payload bytes (UTF-8 JSON for text frames).
    pub payload: Vec<u8>,
}

// ---------------------------------------------------------------------------
// Internal types
// ---------------------------------------------------------------------------

#[derive(Debug)]
enum DisconnectReason {
    Shutdown,
    ForcedRotation,
    PeerClosed,
}

// ---------------------------------------------------------------------------
// ConnectionManager
// ---------------------------------------------------------------------------

/// WebSocket connection manager for Binance USDT-M Futures streams.
///
/// Owns the reconnect loop, ping/pong keepalive, and forced 24-hour rotation.
/// Raw frames are forwarded over the caller-supplied mpsc channel.
pub struct ConnectionManager {
    config: WebSocketConfig,
}

impl ConnectionManager {
    pub fn new(config: WebSocketConfig) -> Self {
        Self { config }
    }

    /// Connect to `url` and forward raw frames to `tx`.
    ///
    /// Reconnects automatically on disconnect (exponential backoff) and after
    /// `config.forced_reconnect_secs` seconds (planned rotation, no backoff).
    /// Returns when the shutdown signal fires.
    pub async fn run(
        &self,
        url: &str,
        tx: mpsc::Sender<RawFrame>,
        mut shutdown: watch::Receiver<bool>,
    ) {
        let mut reconnect_count = 0u32;
        loop {
            if *shutdown.borrow() {
                break;
            }

            info!(%url, reconnect_count, "connecting to Binance Futures WebSocket");

            let result = connect_and_run(url, &self.config, &tx, &mut shutdown).await;

            match result {
                Ok(DisconnectReason::Shutdown) => break,
                Ok(DisconnectReason::ForcedRotation) => {
                    info!("24h rotation — reconnecting immediately");
                    reconnect_count += 1;
                }
                Ok(DisconnectReason::PeerClosed) | Err(_) => {
                    reconnect_count += 1;
                    let delay = backoff_delay(reconnect_count, self.config.reconnect_delay_ms);
                    warn!(
                        reconnect_count,
                        delay_ms = delay.as_millis() as u64,
                        "disconnected — reconnecting after backoff",
                    );
                    tokio::select! {
                        biased;
                        _ = shutdown.changed() => break,
                        _ = tokio::time::sleep(delay) => {}
                    }
                }
            }
        }
        info!("futures connection manager stopped");
    }
}

// ---------------------------------------------------------------------------
// Session logic
// ---------------------------------------------------------------------------

/// Establish one WebSocket session and drive it until it ends for any reason.
async fn connect_and_run(
    url: &str,
    config: &WebSocketConfig,
    tx: &mpsc::Sender<RawFrame>,
    shutdown: &mut watch::Receiver<bool>,
) -> Result<DisconnectReason, FuturesAdapterError> {
    let (ws, _) = match connect_async(url).await {
        Ok(pair) => pair,
        Err(e) => {
            warn!("WebSocket connect failed: {e}");
            return Err(FuturesAdapterError::WebSocket(e));
        }
    };
    info!("Futures WebSocket session established");

    let (mut write, mut read) = ws.split();

    // Channel for outbound messages (pong responses + proactive pings).
    // Bounded to 32; if the sender task falls behind we backpressure rather than allocate unboundedly.
    let (out_tx, mut out_rx) = mpsc::channel::<Message>(32);

    let send_task = tokio::spawn(async move {
        while let Some(msg) = out_rx.recv().await {
            if write.send(msg).await.is_err() {
                break;
            }
        }
    });

    // Proactive pings start after the first full interval (not immediately).
    let ping_dur = Duration::from_secs(config.ping_interval_secs as u64);
    let mut ping_interval = tokio::time::interval_at(
        tokio::time::Instant::now() + ping_dur,
        ping_dur,
    );
    ping_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    let rotation = tokio::time::sleep(Duration::from_secs(config.forced_reconnect_secs));
    tokio::pin!(rotation);

    let mut waiting_for_pong = false;

    let reason = loop {
        tokio::select! {
            biased;

            _ = shutdown.changed() => {
                break DisconnectReason::Shutdown;
            }

            _ = &mut rotation => {
                info!("24h connection lifetime reached — rotating");
                break DisconnectReason::ForcedRotation;
            }

            _ = ping_interval.tick() => {
                if waiting_for_pong {
                    warn!("no pong received after ping — connection appears dead");
                    break DisconnectReason::PeerClosed;
                }
                let _ = out_tx.send(Message::Ping(vec![])).await;
                waiting_for_pong = true;
            }

            msg = read.next() => {
                match msg {
                    None => {
                        info!("WebSocket stream closed by peer");
                        break DisconnectReason::PeerClosed;
                    }
                    Some(Err(e)) => {
                        warn!("WebSocket read error: {e}");
                        break DisconnectReason::PeerClosed;
                    }
                    Some(Ok(Message::Text(text))) => {
                        let frame = RawFrame {
                            recv_ts: now_nanos(),
                            payload: text.into_bytes(),
                        };
                        if tx.send(frame).await.is_err() {
                            break DisconnectReason::Shutdown;
                        }
                    }
                    Some(Ok(Message::Binary(data))) => {
                        let frame = RawFrame { recv_ts: now_nanos(), payload: data };
                        if tx.send(frame).await.is_err() {
                            break DisconnectReason::Shutdown;
                        }
                    }
                    Some(Ok(Message::Ping(data))) => {
                        let _ = out_tx.send(Message::Pong(data)).await;
                    }
                    Some(Ok(Message::Pong(_))) => {
                        waiting_for_pong = false;
                    }
                    Some(Ok(Message::Close(_))) => {
                        info!("WebSocket close frame received");
                        break DisconnectReason::PeerClosed;
                    }
                    Some(Ok(_)) => {} // Frame or future variants — ignore
                }
            }
        }
    };

    drop(out_tx);
    let _ = send_task.await;

    Ok(reason)
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Capped exponential backoff: `base_ms × 2^(attempt−1)`, hard cap at 30 s.
pub(crate) fn backoff_delay(attempt: u32, base_ms: u64) -> Duration {
    let shift = attempt.saturating_sub(1).min(6); // 2^6 = 64×; 30 s cap takes over
    let ms = base_ms.saturating_mul(1u64 << shift).min(30_000);
    Duration::from_millis(ms)
}

fn now_nanos() -> i64 {
    use std::time::SystemTime;
    SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos() as i64
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn backoff_delay_first_attempt_equals_base() {
        assert_eq!(backoff_delay(1, 500), Duration::from_millis(500));
    }

    #[test]
    fn backoff_delay_grows_exponentially() {
        assert_eq!(backoff_delay(2, 500), Duration::from_millis(1_000));
        assert_eq!(backoff_delay(3, 500), Duration::from_millis(2_000));
        assert_eq!(backoff_delay(4, 500), Duration::from_millis(4_000));
        assert_eq!(backoff_delay(5, 500), Duration::from_millis(8_000));
        assert_eq!(backoff_delay(6, 500), Duration::from_millis(16_000));
    }

    #[test]
    fn backoff_delay_caps_at_30s() {
        // attempt 7 would be 500 × 64 = 32_000 ms, capped to 30_000 ms
        assert_eq!(backoff_delay(7, 500), Duration::from_millis(30_000));
        assert_eq!(backoff_delay(20, 500), Duration::from_millis(30_000));
    }

    #[test]
    fn backoff_delay_zero_attempt_equals_base() {
        // attempt=0: shift = saturating_sub(0, 1) = 0, so 1 << 0 = 1
        assert_eq!(backoff_delay(0, 500), Duration::from_millis(500));
    }

    #[test]
    fn now_nanos_is_reasonable() {
        let ts = now_nanos();
        // After 2020-01-01 00:00 UTC in nanos
        assert!(ts > 1_577_836_800_000_000_000_i64);
        // Before 2100-01-01 00:00 UTC in nanos
        assert!(ts < 4_102_444_800_000_000_000_i64);
    }

    /// Integration test: connect to live Binance Futures and receive at least one frame.
    /// Requires network access. Run with `cargo test -- --ignored`.
    #[tokio::test]
    #[ignore]
    async fn integration_connect_and_receive_book_ticker() {
        use crate::stream::{FuturesStream, FUTURES_WS_BASE, build_url};
        use connector_config::WebSocketConfig;

        let config = WebSocketConfig {
            url: FUTURES_WS_BASE.to_string(),
            api_key: None,
            ping_interval_secs: 20,
            max_streams_per_connection: 1024,
            reconnect_delay_ms: 500,
            forced_reconnect_secs: 86_400,
        };

        let streams = vec![FuturesStream::BookTicker.stream_name("BTCUSDT")];
        let url = build_url(FUTURES_WS_BASE, &streams);

        let (tx, mut rx) = mpsc::channel::<RawFrame>(16);
        let (shutdown_tx, shutdown_rx) = watch::channel(false);

        let mgr = ConnectionManager::new(config);

        tokio::select! {
            _ = mgr.run(&url, tx, shutdown_rx) => {}
            frame = rx.recv() => {
                let frame = frame.expect("channel closed before first frame");
                let json = std::str::from_utf8(&frame.payload).expect("non-UTF-8 frame");
                assert!(json.contains("bookTicker"), "unexpected payload: {json}");
                println!("recv_ts={} payload={}", frame.recv_ts, json);
                let _ = shutdown_tx.send(true);
            }
        }
    }
}
