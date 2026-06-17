/// Binance USDT-M Futures WebSocket stream subscriptions (§5.21).
///
/// Stream names follow the Binance Futures combined-stream convention.
/// The canonical base URL is [`FUTURES_WS_BASE`].

/// WebSocket base URL for Binance USDT-M Futures.
pub const FUTURES_WS_BASE: &str = "wss://fstream.binance.com:443";

/// A Binance USDT-M Futures WebSocket stream subscription.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FuturesStream {
    /// Best bid/ask: `{symbol}@bookTicker`.
    BookTicker,
    /// Incremental depth updates: `{symbol}@depth@{speed}ms`.
    ///
    /// `update_speed_ms` must be 100, 250, or 500.
    Depth { update_speed_ms: u16 },
    /// Aggregated trades: `{symbol}@aggTrade`.
    AggTrade,
    /// Mark price and funding rate.
    ///
    /// `update_interval_secs = 1` → `@markPrice@1s`; any other value →
    /// `@markPrice` (default ~3 s interval).
    MarkPrice { update_interval_secs: u16 },
    /// Liquidation (force) orders: `{symbol}@forceOrder`.
    ForceOrder,
}

impl FuturesStream {
    /// Return the per-symbol stream name, e.g. `"btcusdt@bookTicker"`.
    pub fn stream_name(&self, symbol: &str) -> String {
        let sym = symbol.to_lowercase();
        match self {
            Self::BookTicker => format!("{sym}@bookTicker"),
            Self::Depth { update_speed_ms } => {
                format!("{sym}@depth@{update_speed_ms}ms")
            }
            Self::AggTrade => format!("{sym}@aggTrade"),
            Self::MarkPrice { update_interval_secs } => {
                if *update_interval_secs == 1 {
                    format!("{sym}@markPrice@1s")
                } else {
                    format!("{sym}@markPrice")
                }
            }
            Self::ForceOrder => format!("{sym}@forceOrder"),
        }
    }
}

/// Build a Binance Futures combined-stream URL.
///
/// Uses the `/stream?streams=` path so every message arrives wrapped as
/// `{"stream":"…","data":{…}}` regardless of stream count.
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
    fn stream_name_book_ticker() {
        assert_eq!(FuturesStream::BookTicker.stream_name("BTCUSDT"), "btcusdt@bookTicker");
        assert_eq!(FuturesStream::BookTicker.stream_name("btcusdt"), "btcusdt@bookTicker");
    }

    #[test]
    fn stream_name_depth_100ms() {
        assert_eq!(
            FuturesStream::Depth { update_speed_ms: 100 }.stream_name("ETHUSDT"),
            "ethusdt@depth@100ms",
        );
    }

    #[test]
    fn stream_name_depth_500ms() {
        assert_eq!(
            FuturesStream::Depth { update_speed_ms: 500 }.stream_name("SOLUSDT"),
            "solusdt@depth@500ms",
        );
    }

    #[test]
    fn stream_name_agg_trade() {
        assert_eq!(FuturesStream::AggTrade.stream_name("BNBUSDT"), "bnbusdt@aggTrade");
    }

    #[test]
    fn stream_name_mark_price_default() {
        assert_eq!(
            FuturesStream::MarkPrice { update_interval_secs: 3 }.stream_name("BTCUSDT"),
            "btcusdt@markPrice",
        );
        assert_eq!(
            FuturesStream::MarkPrice { update_interval_secs: 0 }.stream_name("BTCUSDT"),
            "btcusdt@markPrice",
        );
    }

    #[test]
    fn stream_name_mark_price_one_second() {
        assert_eq!(
            FuturesStream::MarkPrice { update_interval_secs: 1 }.stream_name("ETHUSDT"),
            "ethusdt@markPrice@1s",
        );
    }

    #[test]
    fn stream_name_force_order() {
        assert_eq!(FuturesStream::ForceOrder.stream_name("BTCUSDT"), "btcusdt@forceOrder");
    }

    #[test]
    fn stream_name_lowercases_symbol() {
        assert_eq!(FuturesStream::AggTrade.stream_name("SOLUSDT"), "solusdt@aggTrade");
        assert_eq!(FuturesStream::ForceOrder.stream_name("XRPUSDT"), "xrpusdt@forceOrder");
    }

    #[test]
    fn build_url_single_stream() {
        let url = build_url(
            FUTURES_WS_BASE,
            &["btcusdt@bookTicker".to_string()],
        );
        assert_eq!(
            url,
            "wss://fstream.binance.com:443/stream?streams=btcusdt@bookTicker",
        );
    }

    #[test]
    fn build_url_multiple_streams() {
        let streams = vec![
            "btcusdt@bookTicker".to_string(),
            "btcusdt@depth@100ms".to_string(),
            "ethusdt@markPrice@1s".to_string(),
        ];
        let url = build_url(FUTURES_WS_BASE, &streams);
        assert_eq!(
            url,
            "wss://fstream.binance.com:443/stream?streams=btcusdt@bookTicker/btcusdt@depth@100ms/ethusdt@markPrice@1s",
        );
    }

    #[test]
    fn futures_ws_base_is_fstream() {
        assert!(FUTURES_WS_BASE.contains("fstream.binance.com"));
    }
}
