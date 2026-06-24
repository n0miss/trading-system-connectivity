use connector_core::{
    AggressorSide as CoreAggressorSide, BestBidOffer, BookDelta, InstrumentDefinition,
    MessageHeader, MessageType, NormalizedMessage, PriceLevel, Trade, SCHEMA_VERSION, TS_NONE,
    UPDATE_ID_NONE,
};
use protocol_sbe::{
    AggressorSide as SbeAggressorSide, BboEvent, DepthDiffEvent, SbeMessage, TradeEvent,
};

use crate::normalizer::NormalizeCtx;

// ---------------------------------------------------------------------------
// Entry point
// ---------------------------------------------------------------------------

/// Convert a decoded SBE message into a normalized pipeline message.
///
/// Returns `None` for `SbeMessage::DepthSnapshot` (partial book; not an
/// incremental delta and not currently consumed by the diff-based pipeline).
///
/// The `seq` counter is advanced exactly once per returned `Some` value, the
/// same contract as [`crate::normalizer::normalize_spot_event`].
pub fn normalize_sbe_message(
    msg: SbeMessage,
    inst: &InstrumentDefinition,
    ctx: &NormalizeCtx,
    seq: &mut u64,
    recv_ts: i64,
) -> Option<NormalizedMessage> {
    match msg {
        SbeMessage::Trade(ev) => Some(normalize_trade(ev, inst, ctx, take_seq(seq), recv_ts)),
        SbeMessage::Bbo(ev) => Some(normalize_bbo(ev, inst, ctx, take_seq(seq), recv_ts)),
        SbeMessage::DepthDiff(ev) => {
            Some(normalize_depth_diff(ev, inst, ctx, take_seq(seq), recv_ts))
        }
        SbeMessage::DepthSnapshot(_) => None,
    }
}

// ---------------------------------------------------------------------------
// Per-message conversion
// ---------------------------------------------------------------------------

fn normalize_trade(
    ev: TradeEvent,
    inst: &InstrumentDefinition,
    ctx: &NormalizeCtx,
    seq: u64,
    recv_ts: i64,
) -> NormalizedMessage {
    let aggressor_side = sbe_aggressor_to_core(ev.aggressor_side);

    NormalizedMessage::Trade(Trade {
        header: make_header(
            MessageType::Trade,
            inst,
            ctx,
            seq,
            us_to_ns(ev.event_time),
            recv_ts,
        ),
        symbol: inst.symbol.clone(),
        price_scale: inst.price_scale as u8,
        qty_scale: inst.qty_scale as u8,
        trade_id: ev.trade_id as u64,
        price: ev.price.to_scaled(inst.price_scale),
        qty: ev.quantity.to_scaled(inst.qty_scale),
        trade_ts: us_to_ns(ev.transact_time),
        is_buyer_maker: ev.is_buyer_market_maker,
        aggressor_side,
    })
}

fn normalize_bbo(
    ev: BboEvent,
    inst: &InstrumentDefinition,
    ctx: &NormalizeCtx,
    seq: u64,
    recv_ts: i64,
) -> NormalizedMessage {
    NormalizedMessage::BestBidOffer(BestBidOffer {
        header: make_header(
            MessageType::BestBidOffer,
            inst,
            ctx,
            seq,
            us_to_ns(ev.event_time),
            recv_ts,
        ),
        symbol: inst.symbol.clone(),
        price_scale: inst.price_scale as u8,
        qty_scale: inst.qty_scale as u8,
        bid_price: ev.best_bid_price.to_scaled(inst.price_scale),
        bid_qty: ev.best_bid_qty.to_scaled(inst.qty_scale),
        ask_price: ev.best_ask_price.to_scaled(inst.price_scale),
        ask_qty: ev.best_ask_qty.to_scaled(inst.qty_scale),
        // SBE BBO carries timestamps (event_time, transact_time) instead of
        // an update_id; use the sentinel so downstream code that relies on
        // update_id ordering knows this field is absent.
        update_id: UPDATE_ID_NONE,
    })
}

fn normalize_depth_diff(
    ev: DepthDiffEvent,
    inst: &InstrumentDefinition,
    ctx: &NormalizeCtx,
    seq: u64,
    recv_ts: i64,
) -> NormalizedMessage {
    let p = inst.price_scale;
    let q = inst.qty_scale;

    let bids = ev
        .bids
        .into_iter()
        .map(|lvl| PriceLevel {
            price: lvl.price.to_scaled(p),
            qty: lvl.quantity.to_scaled(q),
        })
        .collect();

    let asks = ev
        .asks
        .into_iter()
        .map(|lvl| PriceLevel {
            price: lvl.price.to_scaled(p),
            qty: lvl.quantity.to_scaled(q),
        })
        .collect();

    NormalizedMessage::BookDelta(BookDelta {
        header: make_header(
            MessageType::BookDelta,
            inst,
            ctx,
            seq,
            us_to_ns(ev.event_time),
            recv_ts,
        ),
        symbol: inst.symbol.clone(),
        price_scale: p as u8,
        qty_scale: q as u8,
        first_update_id: ev.first_update_id as u64,
        final_update_id: ev.final_update_id as u64,
        // SBE DepthDiff carries prevFinalUpdateId; expose it so the Futures-
        // compatible sequence validator can use it for Spot-over-SBE as well.
        prev_update_id: ev.prev_final_update_id as u64,
        bids,
        asks,
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

/// Convert SBE microsecond timestamp to nanoseconds.
///
/// The SBE stream uses microseconds (unlike the JSON streams which use ms).
fn us_to_ns(us: i64) -> i64 {
    us.saturating_mul(1_000)
}

fn sbe_aggressor_to_core(side: SbeAggressorSide) -> CoreAggressorSide {
    match side {
        SbeAggressorSide::Buy => CoreAggressorSide::Buy,
        SbeAggressorSide::Sell => CoreAggressorSide::Sell,
        SbeAggressorSide::Unknown => CoreAggressorSide::Unknown,
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use connector_core::{MarketType, MessageType, VenueId, TS_NONE};
    use protocol_sbe::{
        AggressorSide as SbeAggressorSide, BboEvent, Decimal64, DepthDiffEvent, DepthLevel,
        DepthSnapshotEvent, SbeMessage, TradeEvent,
    };

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

    fn d64(mantissa: i64) -> Decimal64 {
        Decimal64 { mantissa }
    }

    // -----------------------------------------------------------------------
    // TradeEvent → NormalizedMessage::Trade
    // -----------------------------------------------------------------------

    fn btcusdt_trade_event() -> TradeEvent {
        TradeEvent {
            event_time: 1_699_000_000_000,
            transact_time: 1_699_000_001_000,
            trade_id: 12_345_678,
            price: d64(5_000_050_000_000), // 50000.50 × 10^8
            quantity: d64(100_000),        // 0.001 × 10^8
            buyer_order_id: 111_111,
            seller_order_id: 222_222,
            aggressor_side: SbeAggressorSide::Buy,
            is_buyer_market_maker: false,
            symbol: "BTCUSDT".to_string(),
        }
    }

    #[test]
    fn trade_header_fields() {
        let inst = test_inst(42, 2, 3);
        let ctx = test_ctx();
        let mut seq = 7u64;
        let msg = normalize_sbe_message(
            SbeMessage::Trade(btcusdt_trade_event()),
            &inst,
            &ctx,
            &mut seq,
            999,
        )
        .unwrap();
        let NormalizedMessage::Trade(tr) = msg else {
            panic!("wrong variant")
        };

        assert_eq!(tr.header.instrument_id, 42);
        assert_eq!(tr.header.sequence_number, 7);
        assert_eq!(tr.header.venue_id, VenueId::BinanceSpot);
        assert_eq!(tr.header.exchange_event_ts, 1_699_000_000_000_i64 * 1_000);
        assert_eq!(tr.header.exchange_tx_ts, TS_NONE);
        assert_eq!(tr.header.local_recv_ts, 999);
        assert_eq!(seq, 8, "seq must advance by 1");
    }

    #[test]
    fn trade_price_scaled_at_2() {
        let inst = test_inst(1, 2, 3);
        let ctx = test_ctx();
        let mut seq = 0u64;
        let msg = normalize_sbe_message(
            SbeMessage::Trade(btcusdt_trade_event()),
            &inst,
            &ctx,
            &mut seq,
            0,
        )
        .unwrap();
        let NormalizedMessage::Trade(tr) = msg else {
            panic!()
        };
        // 50000.50 at scale=2 → 5_000_050
        assert_eq!(tr.price, 5_000_050);
    }

    #[test]
    fn trade_qty_scaled_at_3() {
        let inst = test_inst(1, 2, 3);
        let ctx = test_ctx();
        let mut seq = 0u64;
        let msg = normalize_sbe_message(
            SbeMessage::Trade(btcusdt_trade_event()),
            &inst,
            &ctx,
            &mut seq,
            0,
        )
        .unwrap();
        let NormalizedMessage::Trade(tr) = msg else {
            panic!()
        };
        // 0.001 at scale=3 → 1
        assert_eq!(tr.qty, 1);
    }

    #[test]
    fn trade_id_and_timestamps() {
        let inst = test_inst(1, 2, 3);
        let ctx = test_ctx();
        let mut seq = 0u64;
        let msg = normalize_sbe_message(
            SbeMessage::Trade(btcusdt_trade_event()),
            &inst,
            &ctx,
            &mut seq,
            0,
        )
        .unwrap();
        let NormalizedMessage::Trade(tr) = msg else {
            panic!()
        };
        assert_eq!(tr.trade_id, 12_345_678u64);
        assert_eq!(tr.trade_ts, 1_699_000_001_000_i64 * 1_000);
        assert!(!tr.is_buyer_maker);
        assert_eq!(tr.aggressor_side, CoreAggressorSide::Buy);
    }

    #[test]
    fn trade_seller_aggresses() {
        let mut ev = btcusdt_trade_event();
        ev.aggressor_side = SbeAggressorSide::Sell;
        ev.is_buyer_market_maker = true;

        let inst = test_inst(1, 2, 3);
        let ctx = test_ctx();
        let mut seq = 0u64;
        let msg = normalize_sbe_message(SbeMessage::Trade(ev), &inst, &ctx, &mut seq, 0).unwrap();
        let NormalizedMessage::Trade(tr) = msg else {
            panic!()
        };
        assert_eq!(tr.aggressor_side, CoreAggressorSide::Sell);
        assert!(tr.is_buyer_maker);
    }

    #[test]
    fn trade_unknown_aggressor_maps_to_core_unknown() {
        let mut ev = btcusdt_trade_event();
        ev.aggressor_side = SbeAggressorSide::Unknown;

        let inst = test_inst(1, 2, 3);
        let ctx = test_ctx();
        let mut seq = 0u64;
        let msg = normalize_sbe_message(SbeMessage::Trade(ev), &inst, &ctx, &mut seq, 0).unwrap();
        let NormalizedMessage::Trade(tr) = msg else {
            panic!()
        };
        assert_eq!(tr.aggressor_side, CoreAggressorSide::Unknown);
    }

    // -----------------------------------------------------------------------
    // BboEvent → NormalizedMessage::BestBidOffer
    // -----------------------------------------------------------------------

    fn btcusdt_bbo_event() -> BboEvent {
        BboEvent {
            event_time: 1_699_000_000_000,
            transact_time: 1_699_000_000_500,
            best_bid_price: d64(9_650_000_000_000), // 96500.00
            best_bid_qty: d64(1_230_000_000_00),    // 1.23 × 10^8 = 123_000_000
            best_ask_price: d64(9_650_100_000_000), // 96501.00
            best_ask_qty: d64(50_000_000),          // 0.50 × 10^8
            symbol: "BTCUSDT".to_string(),
        }
    }

    #[test]
    fn bbo_header_and_timestamps() {
        let inst = test_inst(99, 2, 5);
        let ctx = test_ctx();
        let mut seq = 5u64;
        let msg = normalize_sbe_message(
            SbeMessage::Bbo(btcusdt_bbo_event()),
            &inst,
            &ctx,
            &mut seq,
            42,
        )
        .unwrap();
        let NormalizedMessage::BestBidOffer(bbo) = msg else {
            panic!()
        };

        assert_eq!(bbo.header.instrument_id, 99);
        assert_eq!(bbo.header.sequence_number, 5);
        assert_eq!(bbo.header.exchange_event_ts, 1_699_000_000_000_i64 * 1_000);
        assert_eq!(bbo.header.exchange_tx_ts, TS_NONE);
        assert_eq!(bbo.header.local_recv_ts, 42);
        assert_eq!(bbo.update_id, UPDATE_ID_NONE);
        assert_eq!(seq, 6);
    }

    #[test]
    fn bbo_prices_scaled_at_2() {
        let inst = test_inst(1, 2, 5);
        let ctx = test_ctx();
        let mut seq = 0u64;
        let msg = normalize_sbe_message(
            SbeMessage::Bbo(btcusdt_bbo_event()),
            &inst,
            &ctx,
            &mut seq,
            0,
        )
        .unwrap();
        let NormalizedMessage::BestBidOffer(bbo) = msg else {
            panic!()
        };
        assert_eq!(bbo.bid_price, 9_650_000); // 96500.00 at scale=2
        assert_eq!(bbo.ask_price, 9_650_100); // 96501.00 at scale=2
    }

    // -----------------------------------------------------------------------
    // DepthDiffEvent → NormalizedMessage::BookDelta
    // -----------------------------------------------------------------------

    fn btcusdt_diff_event() -> DepthDiffEvent {
        DepthDiffEvent {
            event_time: 1_699_000_000_000,
            transact_time: 1_699_000_000_200,
            first_update_id: 50_000_001,
            final_update_id: 50_000_005,
            prev_final_update_id: 50_000_000,
            symbol: "BTCUSDT".to_string(),
            bids: vec![
                DepthLevel {
                    price: d64(9_650_000_000_000),
                    quantity: d64(2_500_000_000),
                },
                DepthLevel {
                    price: d64(9_649_900_000_000),
                    quantity: d64(0),
                },
            ],
            asks: vec![DepthLevel {
                price: d64(9_650_100_000_000),
                quantity: d64(1_000_000_000),
            }],
        }
    }

    #[test]
    fn depth_diff_header_and_update_ids() {
        let inst = test_inst(7, 2, 3);
        let ctx = test_ctx();
        let mut seq = 10u64;
        let msg = normalize_sbe_message(
            SbeMessage::DepthDiff(btcusdt_diff_event()),
            &inst,
            &ctx,
            &mut seq,
            1,
        )
        .unwrap();
        let NormalizedMessage::BookDelta(bd) = msg else {
            panic!()
        };

        assert_eq!(bd.header.sequence_number, 10);
        assert_eq!(bd.header.exchange_event_ts, 1_699_000_000_000_i64 * 1_000);
        assert_eq!(bd.first_update_id, 50_000_001u64);
        assert_eq!(bd.final_update_id, 50_000_005u64);
        assert_eq!(bd.prev_update_id, 50_000_000u64);
        assert_eq!(seq, 11);
    }

    #[test]
    fn depth_diff_bid_levels_scaled() {
        let inst = test_inst(1, 2, 3);
        let ctx = test_ctx();
        let mut seq = 0u64;
        let msg = normalize_sbe_message(
            SbeMessage::DepthDiff(btcusdt_diff_event()),
            &inst,
            &ctx,
            &mut seq,
            0,
        )
        .unwrap();
        let NormalizedMessage::BookDelta(bd) = msg else {
            panic!()
        };
        assert_eq!(bd.bids.len(), 2);
        assert_eq!(bd.bids[0].price, 9_650_000); // 96500.00 at scale=2
        assert_eq!(bd.bids[0].qty, 25_000); // 25.0 at scale=3
        assert_eq!(bd.bids[1].qty, 0); // removal marker
    }

    // -----------------------------------------------------------------------
    // DepthSnapshot → None
    // -----------------------------------------------------------------------

    #[test]
    fn depth_snapshot_returns_none() {
        let ev = DepthSnapshotEvent {
            event_time: 0,
            last_update_id: 1,
            symbol: "BTCUSDT".to_string(),
            bids: vec![],
            asks: vec![],
        };
        let inst = test_inst(1, 2, 3);
        let ctx = test_ctx();
        let mut seq = 99u64;
        let result = normalize_sbe_message(SbeMessage::DepthSnapshot(ev), &inst, &ctx, &mut seq, 0);
        assert!(result.is_none());
        assert_eq!(seq, 99, "seq must not advance for DepthSnapshot");
    }

    // -----------------------------------------------------------------------
    // Sequence invariant
    // -----------------------------------------------------------------------

    #[test]
    fn seq_advances_once_per_produced_message() {
        let inst = test_inst(1, 2, 3);
        let ctx = test_ctx();
        let mut seq = 0u64;

        let events = vec![
            SbeMessage::Trade(btcusdt_trade_event()),
            SbeMessage::Bbo(btcusdt_bbo_event()),
            SbeMessage::DepthDiff(btcusdt_diff_event()),
        ];
        for (i, ev) in events.into_iter().enumerate() {
            let msg = normalize_sbe_message(ev, &inst, &ctx, &mut seq, 0).unwrap();
            assert_eq!(msg.header().sequence_number, i as u64);
        }
        assert_eq!(seq, 3);
    }
}
