use std::sync::Arc;
use std::time::Duration;

use connector_config::WebSocketConfig;
use connector_metrics::ConnectorMetrics;
use futures_util::{SinkExt, StreamExt};
use tokio::sync::{mpsc, watch};
use tokio_tungstenite::{
    connect_async,
    tungstenite::{
        client::IntoClientRequest,
        http::header::{HeaderName, HeaderValue},
        Message,
    },
};
use tracing::{info, warn};

use crate::error::AdapterError;
use crate::instrument::now_nanos;

// ---------------------------------------------------------------------------
// Public types
// ---------------------------------------------------------------------------

/// A raw WebSocket frame received from Binance.
pub struct RawFrame {
    /// Nanoseconds since Unix epoch when the frame arrived at the socket.
    pub recv_ts: i64,
    /// Raw payload bytes (UTF-8 JSON for text frames; SBE binary for binary frames).
    pub payload: Vec<u8>,
    /// `true` when the WebSocket frame was received as a binary message (SBE);
    /// `false` for text messages (JSON).
    pub is_binary: bool,
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

/// WebSocket connection manager for Binance Spot streams.
///
/// Owns the reconnect loop, ping/pong keepalive, and forced 24-hour rotation.
/// Raw frames are forwarded over the caller-supplied mpsc channel.
///
/// Attach a metrics registry with [`with_metrics`] to count `messages_in`,
/// `reconnects`, and `decode_errors` on the hot path without any allocations.
///
/// [`with_metrics`]: ConnectionManager::with_metrics
pub struct ConnectionManager {
    config:  WebSocketConfig,
    metrics: Option<Arc<ConnectorMetrics>>,
}

impl ConnectionManager {
    pub fn new(config: WebSocketConfig) -> Self {
        Self { config, metrics: None }
    }

    /// Attach a metrics registry.  Returns `self` for builder-style chaining.
    ///
    /// When set, every received frame increments `messages_in` and every
    /// reconnect cycle increments `reconnects`.
    pub fn with_metrics(mut self, m: Arc<ConnectorMetrics>) -> Self {
        self.metrics = Some(m);
        self
    }

    /// Connect to `url` and invoke `on_frame` for every received data frame.
    ///
    /// `on_frame` is called **inline** in the WebSocket read loop — no channel,
    /// no task wakeup, no scheduling latency between receipt and processing.
    ///
    /// Reconnects automatically on disconnect (exponential backoff) and after
    /// `config.forced_reconnect_secs` seconds (planned rotation, no backoff).
    /// Returns when the shutdown signal fires.
    pub async fn run<F: FnMut(RawFrame)>(
        &self,
        url: &str,
        mut on_frame: F,
        mut shutdown: watch::Receiver<bool>,
    ) {
        let mut reconnect_count = 0u32;
        loop {
            if *shutdown.borrow() {
                break;
            }

            let base_url = url.split('?').next().unwrap_or(url);
            info!(url = base_url, reconnect_count, "connecting to Binance WebSocket");

            let result = connect_and_run(
                url, &self.config, &mut on_frame, &mut shutdown, self.metrics.as_deref(),
            ).await;

            match result {
                Ok(DisconnectReason::Shutdown) => break,
                Ok(DisconnectReason::ForcedRotation) => {
                    info!("24h rotation — reconnecting immediately");
                    reconnect_count += 1;
                    if let Some(m) = &self.metrics { m.reconnects.increment(); }
                }
                Ok(DisconnectReason::PeerClosed) | Err(_) => {
                    reconnect_count += 1;
                    if let Some(m) = &self.metrics { m.reconnects.increment(); }
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
        info!("connection manager stopped");
    }
}

// ---------------------------------------------------------------------------
// Session logic
// ---------------------------------------------------------------------------

/// Establish one WebSocket session and drive it until it ends for any reason.
async fn connect_and_run<F: FnMut(RawFrame)>(
    url:      &str,
    config:   &WebSocketConfig,
    on_frame: &mut F,
    shutdown: &mut watch::Receiver<bool>,
    metrics:  Option<&ConnectorMetrics>,
) -> Result<DisconnectReason, AdapterError> {
    let mut request = url
        .into_client_request()
        .map_err(|e| { warn!("failed to build request: {e}"); AdapterError::WebSocket(e) })?;
    if let Some(key) = &config.api_key {
        if let Ok(value) = HeaderValue::from_str(key) {
            request.headers_mut().insert(
                HeaderName::from_static("x-mbx-apikey"),
                value,
            );
        } else {
            warn!("api_key contains non-ASCII characters — X-MBX-APIKEY header skipped");
        }
    }

    let connect_result = tokio::select! {
        r = tokio::time::timeout(Duration::from_secs(10), connect_async(request)) => r,
        _ = shutdown.changed() => return Ok(DisconnectReason::Shutdown),
    };
    let (ws, _) = match connect_result {
        Ok(Ok(pair)) => pair,
        Ok(Err(e)) => {
            warn!("WebSocket connect failed: {e}");
            return Err(AdapterError::WebSocket(e));
        }
        Err(_) => {
            warn!("WebSocket connect timed out after 10s");
            return Err(AdapterError::ConnectTimeout);
        }
    };
    info!("WebSocket session established");

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
                        if let Some(m) = metrics { m.messages_in.increment(); }
                        on_frame(RawFrame {
                            recv_ts:   now_nanos(),
                            payload:   text.into_bytes(),
                            is_binary: false,
                        });
                    }
                    Some(Ok(Message::Binary(data))) => {
                        if let Some(m) = metrics { m.messages_in.increment(); }
                        on_frame(RawFrame {
                            recv_ts:   now_nanos(),
                            payload:   data,
                            is_binary: true,
                        });
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
    let shift = attempt.saturating_sub(1).min(6); // up to 2^6 = 64×, then the 30 s cap kicks in
    let ms = base_ms.saturating_mul(1u64 << shift).min(30_000);
    Duration::from_millis(ms)
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
        // attempt 7 would be 500 × 64 = 32_000ms, capped to 30_000ms
        assert_eq!(backoff_delay(7, 500), Duration::from_millis(30_000));
        // subsequent attempts stay at cap
        assert_eq!(backoff_delay(20, 500), Duration::from_millis(30_000));
    }

    #[test]
    fn backoff_delay_zero_attempt_equals_base() {
        // attempt=0: shift = saturating_sub(0,1) = 0, so 1<<0 = 1
        assert_eq!(backoff_delay(0, 500), Duration::from_millis(500));
    }

    /// Verify now_nanos returns a plausible nanosecond timestamp.
    #[test]
    fn now_nanos_is_reasonable() {
        let ts = now_nanos();
        // After 2020-01-01 00:00 UTC in nanos
        assert!(ts > 1_577_836_800_000_000_000_i64);
        // Before 2100-01-01 00:00 UTC in nanos
        assert!(ts < 4_102_444_800_000_000_000_i64);
    }

    /// Integration test: connect to live Binance and receive at least one frame.
    /// Requires network access. Run with `cargo test -- --ignored`.
    #[tokio::test]
    #[ignore]
    async fn integration_connect_and_receive_bbticker() {
        use crate::stream::{SpotStream, build_url};
        use connector_config::WebSocketConfig;

        let config = WebSocketConfig {
            url: "wss://stream.binance.com:443".to_string(),
            futures_url: "wss://fstream.binance.com:443".to_string(),
            api_key: None,
            ping_interval_secs: 20,
            max_streams_per_connection: 1024,
            reconnect_delay_ms: 500,
            forced_reconnect_secs: 86_400,
        };

        let streams = vec![SpotStream::BookTicker.stream_name("BTCUSDT")];
        let url = build_url(&config.url, &streams);

        let (tx, mut rx) = mpsc::channel::<RawFrame>(16);
        let (shutdown_tx, shutdown_rx) = watch::channel(false);

        let mgr = ConnectionManager::new(config);

        // Receive one frame then shut down.
        tokio::select! {
            _ = mgr.run(&url, move |frame| { let _ = tx.try_send(frame); }, shutdown_rx) => {}
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
