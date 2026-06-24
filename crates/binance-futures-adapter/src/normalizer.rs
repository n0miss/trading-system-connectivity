/// Binance USDT-M Futures event normalizer (§5.22).
///
/// Converts [`FuturesEvent`] variants into zero-float [`NormalizedMessage`]
/// values using per-instrument scale factors from [`InstrumentDefinition`].
///
/// Unlike the spot normalizer, this function returns a `Vec` because a single
/// `markPriceUpdate` event produces two messages: [`MarkPrice`] and,
/// when the funding rate is present, [`FundingRate`].
use connector_core::{
    AggressorSide, BestBidOffer, BookDelta, FundingRate, InstrumentDefinition, Liquidation,
    MarkPrice, MarketType, MessageHeader, MessageType, NormalizedMessage, PriceLevel, Trade,
    VenueId, SCHEMA_VERSION, TS_NONE,
};
use protocol_json::{
    FuturesAggTrade, FuturesBookTicker, FuturesDepthUpdate, FuturesEvent, FuturesForceOrder,
    FuturesMarkPrice,
};
use thiserror::Error;

// ---------------------------------------------------------------------------
// Public types
// ---------------------------------------------------------------------------

/// Runtime context shared across all normalizer calls for one connection.
#[derive(Debug, Clone, Copy)]
pub struct NormalizeCtx {
    pub venue_id: VenueId,
    pub market_type: MarketType,
    /// Zero-based instance index written into every message header.
    pub instance_id: u32,
    /// WebSocket connection ID written into every message header.
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
    #[error("unknown liquidation side \"{0}\"")]
    UnknownSide(String),
}

// ---------------------------------------------------------------------------
// Entry point
// ---------------------------------------------------------------------------

/// Convert a [`FuturesEvent`] into zero or more normalized messages.
///
/// Returns:
/// - A single-element vec for BookTicker, DepthUpdate, AggTrade, ForceOrder.
/// - A two-element vec for MarkPrice (MarkPrice + FundingRate), or one element
///   when the funding rate field is absent (empty string).
/// - An empty vec for [`FuturesEvent::Unknown`].
///
/// `seq` is incremented by one for each message produced.
/// `recv_ts` is the nanosecond-precision local socket receive timestamp.
pub fn normalize_futures_event(
    event: &FuturesEvent,
    inst: &InstrumentDefinition,
    ctx: &NormalizeCtx,
    seq: &mut u64,
    recv_ts: i64,
) -> Result<Vec<NormalizedMessage>, NormalizeError> {
    match event {
        FuturesEvent::BookTicker(bt) => {
            let msg = normalize_book_ticker(bt, inst, ctx, take_seq(seq), recv_ts)?;
            Ok(vec![NormalizedMessage::BestBidOffer(msg)])
        }
        FuturesEvent::DepthUpdate(du) => {
            let msg = normalize_depth_update(du, inst, ctx, take_seq(seq), recv_ts)?;
            Ok(vec![NormalizedMessage::BookDelta(msg)])
        }
        FuturesEvent::AggTrade(at) => {
            let msg = normalize_agg_trade(at, inst, ctx, take_seq(seq), recv_ts)?;
            Ok(vec![NormalizedMessage::Trade(msg)])
        }
        FuturesEvent::MarkPrice(mp) => normalize_mark_price(mp, inst, ctx, seq, recv_ts),
        FuturesEvent::ForceOrder(fo) => {
            let msg = normalize_force_order(fo, inst, ctx, take_seq(seq), recv_ts)?;
            Ok(vec![NormalizedMessage::Liquidation(msg)])
        }
        FuturesEvent::Unknown(_) => Ok(vec![]),
    }
}

// ---------------------------------------------------------------------------
// Per-message conversion
// ---------------------------------------------------------------------------

fn normalize_book_ticker(
    bt: &FuturesBookTicker,
    inst: &InstrumentDefinition,
    ctx: &NormalizeCtx,
    seq: u64,
    recv_ts: i64,
) -> Result<BestBidOffer, NormalizeError> {
    let p = inst.price_scale;
    let q = inst.qty_scale;
    Ok(BestBidOffer {
        // Futures bookTicker carries no exchange timestamp.
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
    du: &FuturesDepthUpdate,
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
        // Futures depth events carry pu (prev_final_update_id) directly —
        // the sequence validator uses this instead of a snapshot handshake.
        prev_update_id: du.prev_final_update_id,
        bids,
        asks,
    })
}

fn normalize_agg_trade(
    at: &FuturesAggTrade,
    inst: &InstrumentDefinition,
    ctx: &NormalizeCtx,
    seq: u64,
    recv_ts: i64,
) -> Result<Trade, NormalizeError> {
    // m=true  → buyer is maker → seller is the aggressor
    // m=false → seller is maker → buyer is the aggressor
    let aggressor_side = if at.is_buyer_maker {
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
            ms_to_ns(at.event_time_ms),
            recv_ts,
        ),
        symbol: inst.symbol.clone(),
        price_scale: inst.price_scale as u8,
        qty_scale: inst.qty_scale as u8,
        trade_id: at.agg_trade_id,
        price: parse_scaled(&at.price, inst.price_scale)?,
        qty: parse_scaled(&at.qty, inst.qty_scale)?,
        trade_ts: ms_to_ns(at.trade_time_ms),
        is_buyer_maker: at.is_buyer_maker,
        aggressor_side,
    })
}

/// Produces up to two messages: always a `MarkPrice`, and a `FundingRate`
/// only when the funding rate field is present and non-empty.
fn normalize_mark_price(
    mp: &FuturesMarkPrice,
    inst: &InstrumentDefinition,
    ctx: &NormalizeCtx,
    seq: &mut u64,
    recv_ts: i64,
) -> Result<Vec<NormalizedMessage>, NormalizeError> {
    let p = inst.price_scale;

    // index_price defaults to "" when absent; 0 is the sentinel for unavailable.
    let index_price = if mp.index_price.is_empty() {
        0
    } else {
        parse_scaled(&mp.index_price, p)?
    };

    let mark = MarkPrice {
        header: make_header(
            MessageType::MarkPrice,
            inst,
            ctx,
            take_seq(seq),
            ms_to_ns(mp.event_time_ms),
            recv_ts,
        ),
        symbol: inst.symbol.clone(),
        price_scale: p as u8,
        mark_price: parse_scaled(&mp.mark_price, p)?,
        index_price,
    };

    let mut out = vec![NormalizedMessage::MarkPrice(mark)];

    // FundingRate is only emitted for perpetual contracts where the rate is
    // present. Delivery contracts have an empty funding_rate field.
    if !mp.funding_rate.is_empty() {
        let rate = FundingRate {
            header: make_header(
                MessageType::FundingRate,
                inst,
                ctx,
                take_seq(seq),
                ms_to_ns(mp.event_time_ms),
                recv_ts,
            ),
            symbol: inst.symbol.clone(),
            funding_rate_scale: 9,
            // FundingRate is stored as rate × 10^9.
            funding_rate: parse_scaled(&mp.funding_rate, 9)?,
            next_funding_time: ms_to_ns(mp.next_funding_time_ms),
        };
        out.push(NormalizedMessage::FundingRate(rate));
    }

    Ok(out)
}

fn normalize_force_order(
    fo: &FuturesForceOrder,
    inst: &InstrumentDefinition,
    ctx: &NormalizeCtx,
    seq: u64,
    recv_ts: i64,
) -> Result<Liquidation, NormalizeError> {
    let ord = &fo.order;
    let side = match ord.side.as_str() {
        "BUY" => AggressorSide::Buy,
        "SELL" => AggressorSide::Sell,
        other => return Err(NormalizeError::UnknownSide(other.to_string())),
    };
    let p = inst.price_scale;
    let q = inst.qty_scale;
    Ok(Liquidation {
        header: make_header(
            MessageType::Liquidation,
            inst,
            ctx,
            seq,
            ms_to_ns(fo.event_time_ms),
            recv_ts,
        ),
        symbol: inst.symbol.clone(),
        price_scale: p as u8,
        qty_scale: q as u8,
        side,
        price: parse_scaled(&ord.price, p)?,
        qty: parse_scaled(&ord.qty, q)?,
        avg_price: parse_scaled(&ord.avg_price, p)?,
        last_filled_qty: parse_scaled(&ord.last_filled_qty, q)?,
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

/// Convert a Binance decimal string to a scaled integer with no floating-point.
///
/// `"96500.50"` with `scale=2` → `9_650_050`
/// `""` with any scale → `0` (maps to "field absent" sentinel)
/// Fractional digits beyond `scale` are truncated (not rounded).
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
    use protocol_json::parse_futures_message;

    fn test_ctx() -> NormalizeCtx {
        NormalizeCtx {
            venue_id: VenueId::BinanceFutures,
            market_type: MarketType::UsdmFutures,
            instance_id: 0,
            connection_id: 1,
        }
    }

    fn test_inst(instrument_id: u32, price_scale: u32, qty_scale: u32) -> InstrumentDefinition {
        InstrumentDefinition {
            header: MessageHeader {
                schema_version: SCHEMA_VERSION,
                message_type: MessageType::InstrumentDefinition,
                venue_id: VenueId::BinanceFutures,
                market_type: MarketType::UsdmFutures,
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
            step_size: 10,
            min_qty: 10,
            min_notional: 10_000_000_000,
            contract_size: 0,
            is_trading: true,
        }
    }

    // --- parse_scaled ---

    #[test]
    fn parse_scaled_full_decimal() {
        assert_eq!(
            parse_scaled("96500.50000000", 8).unwrap(),
            9_650_050_000_000
        );
    }

    #[test]
    fn parse_scaled_empty_string_returns_zero() {
        assert_eq!(parse_scaled("", 8).unwrap(), 0);
    }

    #[test]
    fn parse_scaled_funding_rate() {
        // "0.00010000" × 10^9 = 100_000
        assert_eq!(parse_scaled("0.00010000", 9).unwrap(), 100_000);
    }

    #[test]
    fn parse_scaled_integer_only() {
        assert_eq!(parse_scaled("9910", 2).unwrap(), 991_000);
    }

    #[test]
    fn parse_scaled_short_frac_is_right_padded() {
        assert_eq!(parse_scaled("1.5", 4).unwrap(), 15_000);
    }

    // --- BestBidOffer from bookTicker ---

    #[test]
    fn normalize_book_ticker_fields() {
        let raw = br#"{
            "stream":"btcusdt@bookTicker",
            "data":{"u":400900217,"s":"BTCUSDT","b":"96500.00000000","B":"1.23000000","a":"96501.00000000","A":"0.50000000"}
        }"#;
        let event = parse_futures_message(raw).unwrap();
        let inst = test_inst(12345, 8, 8);
        let ctx = test_ctx();
        let mut seq = 0u64;

        let msgs = normalize_futures_event(&event, &inst, &ctx, &mut seq, 999_000_000).unwrap();
        assert_eq!(msgs.len(), 1);
        let NormalizedMessage::BestBidOffer(bbo) = &msgs[0] else {
            panic!("wrong variant")
        };

        assert_eq!(bbo.header.instrument_id, 12345);
        assert_eq!(bbo.header.sequence_number, 0);
        assert_eq!(bbo.header.exchange_event_ts, TS_NONE);
        assert_eq!(bbo.header.local_recv_ts, 999_000_000);
        assert_eq!(bbo.symbol, "BTCUSDT");
        assert_eq!(bbo.bid_price, 9_650_000_000_000);
        assert_eq!(bbo.bid_qty, 123_000_000);
        assert_eq!(bbo.ask_price, 9_650_100_000_000);
        assert_eq!(bbo.ask_qty, 50_000_000);
        assert_eq!(bbo.update_id, 400_900_217);
        assert_eq!(seq, 1);
    }

    // --- BookDelta from depth update (with pu) ---

    #[test]
    fn normalize_depth_update_fills_prev_update_id_from_pu() {
        let raw = br#"{
            "stream":"btcusdt@depth@100ms",
            "data":{
                "e":"depthUpdate","E":1748000000000,"T":1748000000001,
                "s":"BTCUSDT","U":50000001,"u":50000005,"pu":50000000,
                "b":[["96500.00000000","2.50000000"]],"a":[]
            }
        }"#;
        let event = parse_futures_message(raw).unwrap();
        let inst = test_inst(99, 8, 8);
        let ctx = test_ctx();
        let mut seq = 10u64;

        let msgs = normalize_futures_event(&event, &inst, &ctx, &mut seq, 42).unwrap();
        assert_eq!(msgs.len(), 1);
        let NormalizedMessage::BookDelta(bd) = &msgs[0] else {
            panic!("wrong variant")
        };

        assert_eq!(bd.header.sequence_number, 10);
        assert_eq!(
            bd.header.exchange_event_ts,
            1_748_000_000_000_i64 * 1_000_000
        );
        assert_eq!(bd.first_update_id, 50_000_001);
        assert_eq!(bd.final_update_id, 50_000_005);
        // Key difference vs spot: prev_update_id comes from pu, not UPDATE_ID_NONE.
        assert_eq!(bd.prev_update_id, 50_000_000);
        assert_eq!(seq, 11);
    }

    #[test]
    fn depth_update_price_levels_correct() {
        let raw = br#"{
            "stream":"btcusdt@depth@100ms",
            "data":{
                "e":"depthUpdate","E":1748000000000,"T":1748000000001,
                "s":"BTCUSDT","U":1,"u":2,"pu":0,
                "b":[["96500.00","2.50"],["96499.00","0.00"]],"a":[["96501.00","1.00"]]
            }
        }"#;
        let event = parse_futures_message(raw).unwrap();
        let inst = test_inst(1, 2, 2);
        let ctx = test_ctx();
        let mut seq = 0u64;

        let msgs = normalize_futures_event(&event, &inst, &ctx, &mut seq, 0).unwrap();
        let NormalizedMessage::BookDelta(bd) = &msgs[0] else {
            panic!()
        };

        assert_eq!(bd.bids.len(), 2);
        assert_eq!(bd.bids[0].price, 9_650_000); // 96500.00 * 10^2
        assert_eq!(bd.bids[0].qty, 250); // 2.50 * 10^2
        assert_eq!(bd.bids[1].qty, 0); // removal marker
        assert_eq!(bd.asks.len(), 1);
        assert_eq!(bd.asks[0].price, 9_650_100);
    }

    // --- Trade from aggTrade ---

    #[test]
    fn normalize_agg_trade_seller_aggressor() {
        let raw = br#"{
            "stream":"btcusdt@aggTrade",
            "data":{
                "e":"aggTrade","E":1748000000000,"s":"BTCUSDT",
                "a":26129,"p":"96500.50000000","q":"0.01500000",
                "f":100,"l":105,"T":1748000000000,"m":false
            }
        }"#;
        let event = parse_futures_message(raw).unwrap();
        let inst = test_inst(7, 8, 8);
        let ctx = test_ctx();
        let mut seq = 5u64;

        let msgs = normalize_futures_event(&event, &inst, &ctx, &mut seq, 1).unwrap();
        assert_eq!(msgs.len(), 1);
        let NormalizedMessage::Trade(tr) = &msgs[0] else {
            panic!()
        };

        assert_eq!(tr.header.sequence_number, 5);
        assert_eq!(tr.trade_id, 26_129); // agg_trade_id used as trade_id
        assert_eq!(tr.price, 9_650_050_000_000);
        assert_eq!(tr.qty, 1_500_000);
        assert_eq!(tr.trade_ts, 1_748_000_000_000_i64 * 1_000_000);
        assert!(!tr.is_buyer_maker);
        assert_eq!(tr.aggressor_side, AggressorSide::Buy);
        assert_eq!(seq, 6);
    }

    #[test]
    fn normalize_agg_trade_buyer_maker() {
        let raw = br#"{
            "stream":"ethusdt@aggTrade",
            "data":{"e":"aggTrade","E":1748000000002,"s":"ETHUSDT","a":9999,
                    "p":"3500.00000000","q":"0.50000000","f":200,"l":200,"T":1748000000001,"m":true}
        }"#;
        let event = parse_futures_message(raw).unwrap();
        let inst = test_inst(88, 8, 8);
        let ctx = test_ctx();
        let mut seq = 0u64;

        let msgs = normalize_futures_event(&event, &inst, &ctx, &mut seq, 1).unwrap();
        let NormalizedMessage::Trade(tr) = &msgs[0] else {
            panic!()
        };
        assert!(tr.is_buyer_maker);
        assert_eq!(tr.aggressor_side, AggressorSide::Sell);
    }

    // --- MarkPrice + FundingRate ---

    #[test]
    fn normalize_mark_price_produces_two_messages() {
        let raw = br#"{
            "stream":"btcusdt@markPrice",
            "data":{
                "e":"markPriceUpdate","E":1748000000000,"s":"BTCUSDT",
                "p":"96500.50000000","i":"96501.00000000","P":"96498.00000000",
                "r":"0.00010000","T":1749600000000
            }
        }"#;
        let event = parse_futures_message(raw).unwrap();
        let inst = test_inst(3, 8, 8);
        let ctx = test_ctx();
        let mut seq = 0u64;

        let msgs = normalize_futures_event(&event, &inst, &ctx, &mut seq, 5).unwrap();
        assert_eq!(
            msgs.len(),
            2,
            "markPrice must produce MarkPrice + FundingRate"
        );

        let NormalizedMessage::MarkPrice(mp) = &msgs[0] else {
            panic!("expected MarkPrice")
        };
        assert_eq!(mp.header.sequence_number, 0);
        assert_eq!(mp.symbol, "BTCUSDT");
        assert_eq!(mp.mark_price, 9_650_050_000_000); // 96500.50 * 10^8
        assert_eq!(mp.index_price, 9_650_100_000_000); // 96501.00 * 10^8
        assert_eq!(
            mp.header.exchange_event_ts,
            1_748_000_000_000_i64 * 1_000_000
        );

        let NormalizedMessage::FundingRate(fr) = &msgs[1] else {
            panic!("expected FundingRate")
        };
        assert_eq!(fr.header.sequence_number, 1);
        assert_eq!(fr.funding_rate, 100_000); // 0.00010000 × 10^9
        assert_eq!(fr.next_funding_time, 1_749_600_000_000_i64 * 1_000_000);
        assert_eq!(seq, 2);
    }

    #[test]
    fn normalize_mark_price_missing_funding_rate_produces_one_message() {
        let raw = br#"{
            "stream":"btcusdt@markPrice",
            "data":{
                "e":"markPriceUpdate","E":1748000000000,"s":"BTCUSDT",
                "p":"96500.50000000","i":"96501.00000000","T":1749600000000
            }
        }"#;
        let event = parse_futures_message(raw).unwrap();
        let inst = test_inst(3, 8, 8);
        let ctx = test_ctx();
        let mut seq = 0u64;

        let msgs = normalize_futures_event(&event, &inst, &ctx, &mut seq, 0).unwrap();
        assert_eq!(msgs.len(), 1);
        assert!(matches!(msgs[0], NormalizedMessage::MarkPrice(_)));
        assert_eq!(seq, 1);
    }

    #[test]
    fn normalize_mark_price_missing_index_price_is_zero() {
        let raw = br#"{
            "stream":"btcusdt@markPrice",
            "data":{
                "e":"markPriceUpdate","E":1748000000000,"s":"BTCUSDT",
                "p":"96500.50","r":"0.00010000","T":1749600000000
            }
        }"#;
        let event = parse_futures_message(raw).unwrap();
        let inst = test_inst(3, 2, 8);
        let ctx = test_ctx();
        let mut seq = 0u64;

        let msgs = normalize_futures_event(&event, &inst, &ctx, &mut seq, 0).unwrap();
        let NormalizedMessage::MarkPrice(mp) = &msgs[0] else {
            panic!()
        };
        assert_eq!(mp.index_price, 0);
    }

    // --- Liquidation from forceOrder ---

    #[test]
    fn normalize_force_order_sell_side() {
        let raw = br#"{
            "stream":"btcusdt@forceOrder",
            "data":{
                "e":"forceOrder","E":1748000000000,
                "o":{"s":"BTCUSDT","S":"SELL","o":"LIMIT","f":"IOC",
                     "q":"0.014","p":"9910","ap":"9910","X":"FILLED",
                     "l":"0.014","z":"0.014","T":1748000000000}
            }
        }"#;
        let event = parse_futures_message(raw).unwrap();
        let inst = test_inst(5, 2, 3);
        let ctx = test_ctx();
        let mut seq = 0u64;

        let msgs = normalize_futures_event(&event, &inst, &ctx, &mut seq, 9).unwrap();
        assert_eq!(msgs.len(), 1);
        let NormalizedMessage::Liquidation(liq) = &msgs[0] else {
            panic!()
        };

        assert_eq!(liq.header.sequence_number, 0);
        assert_eq!(
            liq.header.exchange_event_ts,
            1_748_000_000_000_i64 * 1_000_000
        );
        assert_eq!(liq.symbol, "BTCUSDT");
        assert_eq!(liq.side, AggressorSide::Sell);
        assert_eq!(liq.price, 991_000); // "9910" * 10^2
        assert_eq!(liq.avg_price, 991_000);
        assert_eq!(liq.qty, 14); // "0.014" * 10^3
        assert_eq!(liq.last_filled_qty, 14);
        assert_eq!(seq, 1);
    }

    #[test]
    fn normalize_force_order_buy_side() {
        let raw = br#"{
            "stream":"ethusdt@forceOrder",
            "data":{
                "e":"forceOrder","E":1748000000001,
                "o":{"s":"ETHUSDT","S":"BUY","o":"LIMIT","f":"IOC",
                     "q":"1.00","p":"3500.00","ap":"3502.00","X":"FILLED",
                     "l":"1.00","z":"1.00","T":1748000000001}
            }
        }"#;
        let event = parse_futures_message(raw).unwrap();
        let inst = test_inst(8, 2, 2);
        let ctx = test_ctx();
        let mut seq = 0u64;

        let msgs = normalize_futures_event(&event, &inst, &ctx, &mut seq, 0).unwrap();
        let NormalizedMessage::Liquidation(liq) = &msgs[0] else {
            panic!()
        };
        assert_eq!(liq.side, AggressorSide::Buy);
    }

    #[test]
    fn normalize_force_order_unknown_side_returns_error() {
        let raw = br#"{
            "stream":"btcusdt@forceOrder",
            "data":{
                "e":"forceOrder","E":1748000000000,
                "o":{"s":"BTCUSDT","S":"LONG","o":"LIMIT","f":"IOC",
                     "q":"0.1","p":"9910","ap":"9910","X":"FILLED",
                     "l":"0.1","z":"0.1","T":1748000000000}
            }
        }"#;
        let event = parse_futures_message(raw).unwrap();
        let inst = test_inst(1, 2, 2);
        let ctx = test_ctx();
        let mut seq = 0u64;

        assert!(normalize_futures_event(&event, &inst, &ctx, &mut seq, 0).is_err());
    }

    // --- Unknown / sequence ---

    #[test]
    fn unknown_event_returns_empty_and_does_not_advance_seq() {
        let event = FuturesEvent::Unknown("btcusdt@kline_1m".to_string());
        let inst = test_inst(1, 8, 8);
        let ctx = test_ctx();
        let mut seq = 7u64;

        let msgs = normalize_futures_event(&event, &inst, &ctx, &mut seq, 0).unwrap();
        assert!(msgs.is_empty());
        assert_eq!(seq, 7);
    }

    #[test]
    fn sequence_increments_per_produced_message() {
        let bbo_raw = br#"{"stream":"btcusdt@bookTicker","data":{"u":1,"s":"BTCUSDT","b":"100.0","B":"1.0","a":"101.0","A":"1.0"}}"#;
        let inst = test_inst(1, 2, 2);
        let ctx = test_ctx();
        let mut seq = 0u64;

        for expected_seq in 0..5u64 {
            let event = parse_futures_message(bbo_raw).unwrap();
            let msgs = normalize_futures_event(&event, &inst, &ctx, &mut seq, 0).unwrap();
            assert_eq!(msgs[0].header().sequence_number, expected_seq);
        }
        assert_eq!(seq, 5);
    }
}
