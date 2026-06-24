use connector_core::{
    AggressorSide, BestBidOffer, BookDelta, InstrumentDefinition, MarketType, MessageHeader,
    MessageType, NormalizedMessage, PriceLevel, Trade, VenueId, SCHEMA_VERSION, TS_NONE,
    UPDATE_ID_NONE,
};
use protocol_json::{SpotBookTicker, SpotDepthUpdate, SpotEvent, SpotTrade};
use thiserror::Error;

// ---------------------------------------------------------------------------
// Public types
// ---------------------------------------------------------------------------

/// Runtime context shared across all normalizer calls for one connection.
#[derive(Debug, Clone, Copy)]
pub struct NormalizeCtx {
    pub venue_id: VenueId,
    pub market_type: MarketType,
    /// Zero-based instance index (written into every message header).
    pub instance_id: u32,
    /// WebSocket connection id (written into every message header).
    pub connection_id: u32,
}

#[derive(Debug, Error)]
pub enum NormalizeError {
    #[error("invalid decimal \"{value}\" (scale {scale}): {source}")]
    InvalidDecimal {
        value: String,
        scale: u32,
        source: std::num::ParseIntError,
    },
}

// ---------------------------------------------------------------------------
// Entry point
// ---------------------------------------------------------------------------

/// Convert a [`SpotEvent`] into a normalized [`NormalizedMessage`].
///
/// Returns `Ok(None)` for unrecognized stream types instead of an error so
/// the caller can silently skip them without breaking the pipeline.
///
/// `seq` is incremented by one for each successfully produced message.
/// `recv_ts` is the nanosecond timestamp when the raw frame arrived at the
/// local socket (from [`crate::connection_manager::RawFrame::recv_ts`]).
pub fn normalize_spot_event(
    event: &SpotEvent,
    inst: &InstrumentDefinition,
    ctx: &NormalizeCtx,
    seq: &mut u64,
    recv_ts: i64,
) -> Result<Option<NormalizedMessage>, NormalizeError> {
    match event {
        SpotEvent::BookTicker(bt) => {
            let msg = normalize_book_ticker(bt, inst, ctx, take_seq(seq), recv_ts)?;
            Ok(Some(NormalizedMessage::BestBidOffer(msg)))
        }
        SpotEvent::DepthUpdate(du) => {
            let msg = normalize_depth_update(du, inst, ctx, take_seq(seq), recv_ts)?;
            Ok(Some(NormalizedMessage::BookDelta(msg)))
        }
        SpotEvent::Trade(tr) => {
            let msg = normalize_trade(tr, inst, ctx, take_seq(seq), recv_ts)?;
            Ok(Some(NormalizedMessage::Trade(msg)))
        }
        SpotEvent::Unknown(_) => Ok(None),
    }
}

// ---------------------------------------------------------------------------
// Per-message conversion
// ---------------------------------------------------------------------------

fn normalize_book_ticker(
    bt: &SpotBookTicker,
    inst: &InstrumentDefinition,
    ctx: &NormalizeCtx,
    seq: u64,
    recv_ts: i64,
) -> Result<BestBidOffer, NormalizeError> {
    let p = inst.price_scale;
    let q = inst.qty_scale;
    Ok(BestBidOffer {
        // bookTicker carries no exchange timestamp.
        header: make_header(MessageType::BestBidOffer, inst, ctx, seq, TS_NONE, recv_ts),
        symbol: inst.symbol.clone(),
        price_scale: p as u8,
        qty_scale: q as u8,
        bid_price: parse_scaled(&bt.bid_price, p)?,
        bid_qty: parse_scaled(&bt.bid_qty, q)?,
        ask_price: parse_scaled(&bt.ask_price, p)?,
        ask_qty: parse_scaled(&bt.ask_qty, q)?,
        update_id: bt.update_id,
    })
}

fn normalize_depth_update(
    du: &SpotDepthUpdate,
    inst: &InstrumentDefinition,
    ctx: &NormalizeCtx,
    seq: u64,
    recv_ts: i64,
) -> Result<BookDelta, NormalizeError> {
    let p = inst.price_scale;
    let q = inst.qty_scale;

    let bids = du
        .bids
        .iter()
        .map(|[price, qty]| {
            Ok(PriceLevel {
                price: parse_scaled(price, p)?,
                qty: parse_scaled(qty, q)?,
            })
        })
        .collect::<Result<Vec<_>, NormalizeError>>()?;

    let asks = du
        .asks
        .iter()
        .map(|[price, qty]| {
            Ok(PriceLevel {
                price: parse_scaled(price, p)?,
                qty: parse_scaled(qty, q)?,
            })
        })
        .collect::<Result<Vec<_>, NormalizeError>>()?;

    Ok(BookDelta {
        header: make_header(
            MessageType::BookDelta,
            inst,
            ctx,
            seq,
            ms_to_ns(du.event_time_ms),
            recv_ts,
        ),
        symbol: inst.symbol.clone(),
        price_scale: p as u8,
        qty_scale: q as u8,
        first_update_id: du.first_update_id,
        final_update_id: du.last_update_id,
        // Stage 3 fills this in during sequence validation.
        prev_update_id: UPDATE_ID_NONE,
        bids,
        asks,
    })
}

fn normalize_trade(
    tr: &SpotTrade,
    inst: &InstrumentDefinition,
    ctx: &NormalizeCtx,
    seq: u64,
    recv_ts: i64,
) -> Result<Trade, NormalizeError> {
    // m=true  → buyer is maker → seller aggressed
    // m=false → seller is maker → buyer aggressed
    let aggressor_side = if tr.is_buyer_maker {
        AggressorSide::Sell
    } else {
        AggressorSide::Buy
    };

    Ok(Trade {
        header: make_header(
            MessageType::Trade,
            inst,
            ctx,
            seq,
            ms_to_ns(tr.event_time_ms),
            recv_ts,
        ),
        symbol: inst.symbol.clone(),
        price_scale: inst.price_scale as u8,
        qty_scale: inst.qty_scale as u8,
        trade_id: tr.trade_id,
        price: parse_scaled(&tr.price, inst.price_scale)?,
        qty: parse_scaled(&tr.qty, inst.qty_scale)?,
        trade_ts: ms_to_ns(tr.trade_time_ms),
        is_buyer_maker: tr.is_buyer_maker,
        aggressor_side,
    })
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn make_header(
    msg_type: MessageType,
    inst: &InstrumentDefinition,
    ctx: &NormalizeCtx,
    seq: u64,
    exchange_event_ts: i64,
    recv_ts: i64,
) -> MessageHeader {
    MessageHeader {
        schema_version: SCHEMA_VERSION,
        message_type: msg_type,
        venue_id: ctx.venue_id,
        market_type: ctx.market_type,
        instrument_id: inst.header.instrument_id,
        connection_id: ctx.connection_id,
        instance_id: ctx.instance_id,
        sequence_number: seq,
        exchange_event_ts,
        exchange_tx_ts: TS_NONE,
        local_recv_ts: recv_ts,
        local_publish_ts: TS_NONE,
    }
}

fn take_seq(seq: &mut u64) -> u64 {
    let n = *seq;
    *seq = seq.wrapping_add(1);
    n
}

fn ms_to_ns(ms: i64) -> i64 {
    ms.saturating_mul(1_000_000)
}

/// Convert a Binance decimal string to a scaled integer with no floating-point arithmetic.
///
/// `"96500.50000000"` with `scale=8` → `9_650_050_000_000`
/// `"0.00000000"` with `scale=8` → `0` (price-level removal marker)
/// `"0"` with `scale=8` → `0`
fn parse_scaled(s: &str, scale: u32) -> Result<i64, NormalizeError> {
    let s = s.trim();

    let (int_s, frac_s) = match s.find('.') {
        None => (s, ""),
        Some(dot) => (&s[..dot], &s[dot + 1..]),
    };

    let int_val: i64 = if int_s.is_empty() {
        0
    } else {
        int_s.parse().map_err(|e| NormalizeError::InvalidDecimal {
            value: s.to_string(),
            scale,
            source: e,
        })?
    };

    let frac_val: i64 = if scale == 0 {
        0
    } else {
        let n = scale as usize;
        // Right-pad with zeros to `scale` digits, then take exactly `scale` chars.
        let padded = format!("{:0<width$}", frac_s, width = n);
        padded[..n]
            .parse()
            .map_err(|e| NormalizeError::InvalidDecimal {
                value: s.to_string(),
                scale,
                source: e,
            })?
    };

    Ok(int_val * 10_i64.pow(scale) + frac_val)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use connector_core::{MarketType, MessageType, VenueId, TS_NONE};
    use protocol_json::parse_spot_message;

    fn test_ctx() -> NormalizeCtx {
        NormalizeCtx {
            venue_id: VenueId::BinanceSpot,
            market_type: MarketType::Spot,
            instance_id: 0,
            connection_id: 1,
        }
    }

    fn test_inst(instrument_id: u32, price_scale: u32, qty_scale: u32) -> InstrumentDefinition {
        InstrumentDefinition {
            header: MessageHeader {
                schema_version: SCHEMA_VERSION,
                message_type: MessageType::InstrumentDefinition,
                venue_id: VenueId::BinanceSpot,
                market_type: MarketType::Spot,
                instrument_id,
                connection_id: 0,
                instance_id: 0,
                sequence_number: 0,
                exchange_event_ts: TS_NONE,
                exchange_tx_ts: TS_NONE,
                local_recv_ts: TS_NONE,
                local_publish_ts: TS_NONE,
            },
            symbol: "BTCUSDT".to_string(),
            base_asset: "BTC".to_string(),
            quote_asset: "USDT".to_string(),
            price_scale,
            qty_scale,
            tick_size: 100,
            step_size: 100_000,
            min_qty: 100_000,
            min_notional: 10_000_000_000,
            contract_size: 0,
            is_trading: true,
        }
    }

    // --- parse_scaled ---

    #[test]
    fn parse_scaled_full_decimal() {
        // 96500.50000000 * 10^8 = 9_650_050_000_000
        assert_eq!(
            parse_scaled("96500.50000000", 8).unwrap(),
            9_650_050_000_000
        );
    }

    #[test]
    fn parse_scaled_zero_with_decimals() {
        assert_eq!(parse_scaled("0.00000000", 8).unwrap(), 0);
    }

    #[test]
    fn parse_scaled_integer_only_string() {
        assert_eq!(parse_scaled("0", 8).unwrap(), 0);
        assert_eq!(parse_scaled("100", 2).unwrap(), 10_000);
    }

    #[test]
    fn parse_scaled_scale_zero() {
        assert_eq!(parse_scaled("42.999", 0).unwrap(), 42);
    }

    #[test]
    fn parse_scaled_short_frac_is_right_padded() {
        // "1.5" scale=4 → int=1, frac="5"→"5000", result = 10000 + 5000 = 15000
        assert_eq!(parse_scaled("1.5", 4).unwrap(), 15_000);
    }

    #[test]
    fn parse_scaled_frac_truncated_to_scale() {
        // "1.123456789" scale=8 → 1 * 10^8 + 12_345_678 = 112_345_678 (9th digit dropped)
        assert_eq!(parse_scaled("1.123456789", 8).unwrap(), 112_345_678);
        // "0.12345678901234" scale=8 → frac truncated to "12345678"
        assert_eq!(parse_scaled("0.12345678901234", 8).unwrap(), 12_345_678);
    }

    #[test]
    fn parse_scaled_invalid_string_returns_error() {
        assert!(parse_scaled("not_a_number", 8).is_err());
    }

    // --- BestBidOffer from bookTicker ---

    #[test]
    fn normalize_book_ticker_fields() {
        let raw = br#"{
            "stream":"btcusdt@bookTicker",
            "data":{"u":400900217,"s":"BTCUSDT","b":"96500.00000000","B":"1.23000000","a":"96501.00000000","A":"0.50000000"}
        }"#;
        let event = parse_spot_message(raw).unwrap();
        let inst = test_inst(12345, 8, 8);
        let ctx = test_ctx();
        let mut seq = 0u64;

        let msg = normalize_spot_event(&event, &inst, &ctx, &mut seq, 999_000_000)
            .unwrap()
            .unwrap();
        let NormalizedMessage::BestBidOffer(bbo) = msg else {
            panic!("wrong variant")
        };

        assert_eq!(bbo.header.instrument_id, 12345);
        assert_eq!(bbo.header.sequence_number, 0);
        assert_eq!(bbo.header.exchange_event_ts, TS_NONE); // no timestamp in bookTicker
        assert_eq!(bbo.header.local_recv_ts, 999_000_000);
        assert_eq!(bbo.symbol, "BTCUSDT");
        assert_eq!(bbo.bid_price, 9_650_000_000_000); // 96500.0 * 10^8
        assert_eq!(bbo.bid_qty, 123_000_000); // 1.23 * 10^8
        assert_eq!(bbo.ask_price, 9_650_100_000_000); // 96501.0 * 10^8
        assert_eq!(bbo.ask_qty, 50_000_000); // 0.50 * 10^8
        assert_eq!(bbo.update_id, 400_900_217);
    }

    // --- BookDelta from depth update ---

    #[test]
    fn normalize_depth_update_fields() {
        let raw = br#"{
            "stream":"btcusdt@depth@100ms",
            "data":{
                "e":"depthUpdate","E":1748000000000,"s":"BTCUSDT",
                "U":50000001,"u":50000005,
                "b":[["96500.00000000","2.50000000"],["96499.00000000","0.00000000"]],
                "a":[["96501.00000000","1.00000000"]]
            }
        }"#;
        let event = parse_spot_message(raw).unwrap();
        let inst = test_inst(99, 8, 8);
        let ctx = test_ctx();
        let mut seq = 10u64;

        let msg = normalize_spot_event(&event, &inst, &ctx, &mut seq, 42)
            .unwrap()
            .unwrap();
        assert_eq!(seq, 11, "seq must advance by 1");

        let NormalizedMessage::BookDelta(bd) = msg else {
            panic!("wrong variant")
        };

        assert_eq!(bd.header.sequence_number, 10);
        assert_eq!(
            bd.header.exchange_event_ts,
            1_748_000_000_000_i64 * 1_000_000
        );
        assert_eq!(bd.header.local_recv_ts, 42);
        assert_eq!(bd.first_update_id, 50_000_001);
        assert_eq!(bd.final_update_id, 50_000_005);
        assert_eq!(bd.prev_update_id, UPDATE_ID_NONE);

        assert_eq!(bd.bids.len(), 2);
        assert_eq!(bd.bids[0].price, 9_650_000_000_000); // 96500.0 * 10^8
        assert_eq!(bd.bids[0].qty, 250_000_000); // 2.50 * 10^8
                                                 // qty "0.00000000" → 0 signals level removal
        assert_eq!(bd.bids[1].price, 9_649_900_000_000);
        assert_eq!(bd.bids[1].qty, 0);

        assert_eq!(bd.asks.len(), 1);
        assert_eq!(bd.asks[0].price, 9_650_100_000_000);
        assert_eq!(bd.asks[0].qty, 100_000_000);
    }

    // --- Trade ---

    #[test]
    fn normalize_trade_buyer_aggresses() {
        let raw = br#"{
            "stream":"btcusdt@trade",
            "data":{"e":"trade","E":1748000000001,"s":"BTCUSDT","t":3000001,
                    "p":"96500.50000000","q":"0.01500000","T":1748000000000,"m":false,"M":true}
        }"#;
        let event = parse_spot_message(raw).unwrap();
        let inst = test_inst(7, 8, 8);
        let ctx = test_ctx();
        let mut seq = 5u64;

        let msg = normalize_spot_event(&event, &inst, &ctx, &mut seq, 1)
            .unwrap()
            .unwrap();
        let NormalizedMessage::Trade(tr) = msg else {
            panic!("wrong variant")
        };

        assert_eq!(tr.header.sequence_number, 5);
        assert_eq!(
            tr.header.exchange_event_ts,
            1_748_000_000_001_i64 * 1_000_000
        );
        assert_eq!(tr.trade_id, 3_000_001);
        assert_eq!(tr.price, 9_650_050_000_000); // 96500.50 * 10^8
        assert_eq!(tr.qty, 1_500_000); // 0.015 * 10^8
        assert_eq!(tr.trade_ts, 1_748_000_000_000_i64 * 1_000_000);
        assert!(!tr.is_buyer_maker);
        assert_eq!(tr.aggressor_side, AggressorSide::Buy);
    }

    #[test]
    fn normalize_trade_seller_aggresses() {
        let raw = br#"{
            "stream":"ethusdt@trade",
            "data":{"e":"trade","E":1748000000002,"s":"ETHUSDT","t":9999,
                    "p":"3500.00000000","q":"0.50000000","T":1748000000001,"m":true,"M":true}
        }"#;
        let event = parse_spot_message(raw).unwrap();
        let inst = test_inst(88, 8, 8);
        let ctx = test_ctx();
        let mut seq = 0u64;

        let msg = normalize_spot_event(&event, &inst, &ctx, &mut seq, 1)
            .unwrap()
            .unwrap();
        let NormalizedMessage::Trade(tr) = msg else {
            panic!("wrong variant")
        };

        assert!(tr.is_buyer_maker);
        assert_eq!(tr.aggressor_side, AggressorSide::Sell);
    }

    // --- Unknown / sequence ---

    #[test]
    fn unknown_event_returns_none_and_does_not_advance_seq() {
        let event = SpotEvent::Unknown("btcusdt@aggTrade".to_string());
        let inst = test_inst(1, 8, 8);
        let ctx = test_ctx();
        let mut seq = 7u64;

        let result = normalize_spot_event(&event, &inst, &ctx, &mut seq, 0).unwrap();
        assert!(result.is_none());
        assert_eq!(seq, 7, "seq must not advance for unknown events");
    }

    #[test]
    fn sequence_increments_per_produced_message() {
        let bbo_raw = br#"{"stream":"btcusdt@bookTicker","data":{"u":1,"s":"BTCUSDT","b":"100.0","B":"1.0","a":"101.0","A":"1.0"}}"#;
        let inst = test_inst(1, 2, 2);
        let ctx = test_ctx();
        let mut seq = 0u64;

        for expected_seq in 0..5u64 {
            let event = parse_spot_message(bbo_raw).unwrap();
            let msg = normalize_spot_event(&event, &inst, &ctx, &mut seq, 0)
                .unwrap()
                .unwrap();
            assert_eq!(msg.header().sequence_number, expected_seq);
        }
        assert_eq!(seq, 5);
    }
}
