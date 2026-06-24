/// Canonical base URL for the Binance Spot WebSocket combined-stream endpoint.
///
/// Port 443 is preferred over 9443 because standard HTTPS/WSS traffic is
/// accepted by most firewalls and proxies whereas 9443 is often blocked.
pub const SPOT_WS_BASE: &str = "wss://stream.binance.com:443";

/// A Binance Spot WebSocket stream subscription.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SpotStream {
    BookTicker,
    /// `update_speed_ms` must be 100, 250, or 500.
    Depth {
        update_speed_ms: u16,
    },
    Trade,
    AggTrade,
}

impl SpotStream {
    /// Return the per-symbol stream name, e.g. `"btcusdt@bookTicker"`.
    pub fn stream_name(&self, symbol: &str) -> String {
        let sym = symbol.to_lowercase();
        match self {
            Self::BookTicker => format!("{}@bookTicker", sym),
            Self::Depth { update_speed_ms } => format!("{}@depth@{}ms", sym, update_speed_ms),
            Self::Trade => format!("{}@trade", sym),
            Self::AggTrade => format!("{}@aggTrade", sym),
        }
    }
}

/// Build a Binance combined-stream URL.
///
/// Always uses `/stream?streams=` so every message arrives with the
/// `{"stream":"…","data":{…}}` wrapper regardless of how many streams there are.
pub fn build_url(base_url: &str, streams: &[String]) -> String {
    format!("{}/stream?streams={}", base_url, streams.join("/"))
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn spot_ws_base_uses_port_443() {
        assert!(
            SPOT_WS_BASE.contains(":443"),
            "SPOT_WS_BASE must use port 443"
        );
    }

    #[test]
    fn stream_name_book_ticker() {
        assert_eq!(
            SpotStream::BookTicker.stream_name("BTCUSDT"),
            "btcusdt@bookTicker"
        );
        assert_eq!(
            SpotStream::BookTicker.stream_name("btcusdt"),
            "btcusdt@bookTicker"
        );
    }

    #[test]
    fn stream_name_depth() {
        assert_eq!(
            SpotStream::Depth {
                update_speed_ms: 100
            }
            .stream_name("ETHUSDT"),
            "ethusdt@depth@100ms",
        );
        assert_eq!(
            SpotStream::Depth {
                update_speed_ms: 500
            }
            .stream_name("ETHUSDT"),
            "ethusdt@depth@500ms",
        );
    }

    #[test]
    fn stream_name_trade() {
        assert_eq!(SpotStream::Trade.stream_name("BNBUSDT"), "bnbusdt@trade");
    }

    #[test]
    fn stream_name_agg_trade() {
        assert_eq!(
            SpotStream::AggTrade.stream_name("SOLUSDT"),
            "solusdt@aggTrade"
        );
    }

    #[test]
    fn build_url_single_stream() {
        let url = build_url(SPOT_WS_BASE, &["btcusdt@bookTicker".to_string()]);
        assert_eq!(
            url,
            "wss://stream.binance.com:443/stream?streams=btcusdt@bookTicker"
        );
    }

    #[test]
    fn build_url_multiple_streams() {
        let streams = vec![
            "btcusdt@bookTicker".to_string(),
            "btcusdt@depth@100ms".to_string(),
            "ethusdt@trade".to_string(),
        ];
        let url = build_url(SPOT_WS_BASE, &streams);
        assert_eq!(
            url,
            "wss://stream.binance.com:443/stream?streams=btcusdt@bookTicker/btcusdt@depth@100ms/ethusdt@trade",
        );
    }
}
