//! Golden-file tests for all message types.
//!
//! Each test encodes a sample struct, writes the bytes to `tests/fixtures/<name>.bin`
//! on first run, and compares against the stored file on every subsequent run.
//! To re-bless a fixture after an intentional wire-format change, delete the file
//! and re-run the tests.

use std::path::PathBuf;

use connector_core::{
    AccountUpdate, AggressorSide, BestBidOffer, BookDelta, BookRecovered, BookSnapshot, BookStale,
    BookStaleReason, FeedState, FeedStatus, FundingRate, GapDetected, Heartbeat,
    InstrumentDefinition, Liquidation, MarkPrice, MarketType, MessageHeader, MessageType,
    NormalizedMessage, OpenInterest, OrderUpdate, PriceLevel, Trade, TradingStatus, VenueId,
    HEADER_SIZE, SCHEMA_VERSION, TS_NONE, UPDATE_ID_NONE,
};

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn fixture_path(name: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures")
        .join(format!("{name}.bin"))
}

fn check_golden(name: &str, actual: &[u8]) {
    let path = fixture_path(name);
    if !path.exists() {
        std::fs::write(&path, actual)
            .unwrap_or_else(|e| panic!("could not write fixture {name}: {e}"));
        println!("blessed: {}", path.display());
        return;
    }
    let expected =
        std::fs::read(&path).unwrap_or_else(|e| panic!("could not read fixture {name}: {e}"));
    assert_eq!(
        actual,
        expected.as_slice(),
        "Golden mismatch for `{name}`. If this is intentional, delete the fixture and re-run."
    );
}

fn base_header(message_type: MessageType) -> MessageHeader {
    MessageHeader {
        schema_version: SCHEMA_VERSION,
        message_type,
        venue_id: VenueId::BinanceSpot,
        market_type: MarketType::Spot,
        instrument_id: 1,
        connection_id: 0,
        instance_id: 1,
        sequence_number: 1,
        exchange_event_ts: 1_700_000_000_000_000_000,
        exchange_tx_ts: TS_NONE,
        local_recv_ts: 1_700_000_000_000_010_000,
        local_publish_ts: 1_700_000_000_000_020_000,
    }
}

fn encode<F: FnOnce(&mut [u8]) -> Result<usize, connector_core::Error>>(f: F) -> Vec<u8> {
    let mut buf = vec![0u8; 4096];
    let n = f(&mut buf).expect("encode failed");
    buf.truncate(n);
    buf
}

// ---------------------------------------------------------------------------
// Tests — one per message type
// ---------------------------------------------------------------------------

#[test]
fn golden_heartbeat() {
    let msg = Heartbeat {
        header: base_header(MessageType::Heartbeat),
    };
    let bytes = encode(|b| msg.encode_into(b));
    assert_eq!(bytes.len(), HEADER_SIZE);
    check_golden("heartbeat", &bytes);
    let rt = Heartbeat::decode(&bytes).unwrap();
    assert_eq!(rt, msg);
}

#[test]
fn golden_feed_status() {
    let msg = FeedStatus {
        header: base_header(MessageType::FeedStatus),
        state: FeedState::Live,
    };
    let bytes = encode(|b| msg.encode_into(b));
    assert_eq!(bytes.len(), HEADER_SIZE + 1);
    check_golden("feed_status", &bytes);
    let rt = FeedStatus::decode(&bytes).unwrap();
    assert_eq!(rt, msg);
}

#[test]
fn golden_bbo() {
    let msg = BestBidOffer {
        header: base_header(MessageType::BestBidOffer),
        symbol: "BTCUSDT".to_string(),
        bid_price: 4_300_000_000_000, // 43_000.00000000 at scale 8
        bid_qty: 100_000_000,         // 1.00000000
        ask_price: 4_300_100_000_000,
        ask_qty: 50_000_000,
        update_id: 123_456_789,
    };
    let bytes = encode(|b| msg.encode_into(b));
    check_golden("bbo", &bytes);
    let rt = NormalizedMessage::from_bytes(&bytes).unwrap();
    assert_eq!(rt, NormalizedMessage::BestBidOffer(msg));
}

#[test]
fn golden_trade() {
    let msg = Trade {
        header: base_header(MessageType::Trade),
        symbol: "BTCUSDT".to_string(),
        trade_id: 987_654_321,
        price: 4_300_000_000_000,
        qty: 25_000_000,
        trade_ts: 1_700_000_000_000_000_000,
        is_buyer_maker: false,
        aggressor_side: AggressorSide::Buy,
    };
    let bytes = encode(|b| msg.encode_into(b));
    check_golden("trade", &bytes);
    let rt = Trade::decode(&bytes).unwrap();
    assert_eq!(rt, msg);
}

#[test]
fn golden_book_delta() {
    let msg = BookDelta {
        header: base_header(MessageType::BookDelta),
        symbol: "BTCUSDT".to_string(),
        first_update_id: 1_000,
        final_update_id: 1_001,
        prev_update_id: 999,
        bids: vec![
            PriceLevel {
                price: 4_299_900_000_000,
                qty: 200_000_000,
            },
            PriceLevel {
                price: 4_299_800_000_000,
                qty: 0,
            }, // removal
        ],
        asks: vec![PriceLevel {
            price: 4_300_100_000_000,
            qty: 150_000_000,
        }],
    };
    let bytes = encode(|b| msg.encode_into(b));
    check_golden("book_delta", &bytes);
    let rt = BookDelta::decode(&bytes).unwrap();
    assert_eq!(rt, msg);
}

#[test]
fn golden_book_snapshot() {
    let msg = BookSnapshot {
        header: base_header(MessageType::BookSnapshot),
        symbol: "ETHUSDT".to_string(),
        update_id: 5_000,
        bids: vec![PriceLevel {
            price: 230_000_000_000,
            qty: 500_000_000,
        }],
        asks: vec![PriceLevel {
            price: 230_010_000_000,
            qty: 300_000_000,
        }],
    };
    let bytes = encode(|b| msg.encode_into(b));
    check_golden("book_snapshot", &bytes);
    let rt = BookSnapshot::decode(&bytes).unwrap();
    assert_eq!(rt, msg);
}

#[test]
fn golden_instrument_definition() {
    let msg = InstrumentDefinition {
        header: base_header(MessageType::InstrumentDefinition),
        symbol: "BTCUSDT".to_string(),
        base_asset: "BTC".to_string(),
        quote_asset: "USDT".to_string(),
        price_scale: 8,
        qty_scale: 8,
        tick_size: 100, // 0.00000100 at scale 8
        step_size: 1_000,
        min_qty: 1_000,
        min_notional: 1_000_000_000_000, // 10 USDT at scale 8
        contract_size: 0,
        is_trading: true,
    };
    let bytes = encode(|b| msg.encode_into(b));
    check_golden("instrument_definition", &bytes);
    let rt = InstrumentDefinition::decode(&bytes).unwrap();
    assert_eq!(rt, msg);
}

#[test]
fn golden_trading_status() {
    let msg = TradingStatus {
        header: base_header(MessageType::TradingStatus),
        symbol: "BTCUSDT".to_string(),
        is_trading: true,
    };
    let bytes = encode(|b| msg.encode_into(b));
    check_golden("trading_status", &bytes);
    let rt = TradingStatus::decode(&bytes).unwrap();
    assert_eq!(rt, msg);
}

#[test]
fn golden_mark_price() {
    let msg = MarkPrice {
        header: base_header(MessageType::MarkPrice),
        symbol: "BTCUSDT".to_string(),
        mark_price: 4_300_050_000_000,
        index_price: 4_300_000_000_000,
    };
    let bytes = encode(|b| msg.encode_into(b));
    check_golden("mark_price", &bytes);
    let rt = MarkPrice::decode(&bytes).unwrap();
    assert_eq!(rt, msg);
}

#[test]
fn golden_funding_rate() {
    let msg = FundingRate {
        header: base_header(MessageType::FundingRate),
        symbol: "BTCUSDT".to_string(),
        funding_rate: 100_000, // 0.0001 at scale 1e9 = 0.01%
        next_funding_time: 1_700_008_000_000_000_000,
    };
    let bytes = encode(|b| msg.encode_into(b));
    check_golden("funding_rate", &bytes);
    let rt = FundingRate::decode(&bytes).unwrap();
    assert_eq!(rt, msg);
}

#[test]
fn golden_liquidation() {
    let msg = Liquidation {
        header: base_header(MessageType::Liquidation),
        symbol: "BTCUSDT".to_string(),
        side: AggressorSide::Sell,
        price: 4_280_000_000_000,
        qty: 10_000_000,
        avg_price: 4_279_500_000_000,
        last_filled_qty: 10_000_000,
    };
    let bytes = encode(|b| msg.encode_into(b));
    check_golden("liquidation", &bytes);
    let rt = Liquidation::decode(&bytes).unwrap();
    assert_eq!(rt, msg);
}

#[test]
fn golden_open_interest() {
    let msg = OpenInterest {
        header: base_header(MessageType::OpenInterest),
        symbol: "BTCUSDT".to_string(),
        open_interest: 12_345_678_900_000_000,
    };
    let bytes = encode(|b| msg.encode_into(b));
    check_golden("open_interest", &bytes);
    let rt = OpenInterest::decode(&bytes).unwrap();
    assert_eq!(rt, msg);
}

#[test]
fn golden_gap_detected() {
    let msg = GapDetected {
        header: base_header(MessageType::GapDetected),
        symbol: "BTCUSDT".to_string(),
        expected_update_id: 1_001,
        received_update_id: 1_005,
    };
    let bytes = encode(|b| msg.encode_into(b));
    check_golden("gap_detected", &bytes);
    let rt = GapDetected::decode(&bytes).unwrap();
    assert_eq!(rt, msg);
}

#[test]
fn golden_book_stale() {
    let msg = BookStale {
        header: base_header(MessageType::BookStale),
        symbol: "BTCUSDT".to_string(),
        reason: BookStaleReason::SequenceGap,
    };
    let bytes = encode(|b| msg.encode_into(b));
    check_golden("book_stale", &bytes);
    let rt = BookStale::decode(&bytes).unwrap();
    assert_eq!(rt, msg);
}

#[test]
fn golden_book_recovered() {
    let msg = BookRecovered {
        header: base_header(MessageType::BookRecovered),
        symbol: "BTCUSDT".to_string(),
        snapshot_update_id: 1_002,
    };
    let bytes = encode(|b| msg.encode_into(b));
    check_golden("book_recovered", &bytes);
    let rt = BookRecovered::decode(&bytes).unwrap();
    assert_eq!(rt, msg);
}

// ---------------------------------------------------------------------------
// Additional property-style tests
// ---------------------------------------------------------------------------

#[test]
fn book_delta_empty_sides_round_trip() {
    let msg = BookDelta {
        header: base_header(MessageType::BookDelta),
        symbol: "XRPUSDT".to_string(),
        first_update_id: 1,
        final_update_id: 1,
        prev_update_id: UPDATE_ID_NONE,
        bids: vec![],
        asks: vec![],
    };
    let bytes = encode(|b| msg.encode_into(b));
    assert_eq!(BookDelta::decode(&bytes).unwrap(), msg);
}

#[test]
fn book_snapshot_many_levels_round_trip() {
    let levels: Vec<PriceLevel> = (0..100)
        .map(|i| PriceLevel {
            price: 100_000 + i,
            qty: 1_000 * (i + 1),
        })
        .collect();
    let msg = BookSnapshot {
        header: base_header(MessageType::BookSnapshot),
        symbol: "SOLUSDT".to_string(),
        update_id: 99_999,
        bids: levels.clone(),
        asks: levels,
    };
    let bytes = encode(|b| msg.encode_into(b));
    assert_eq!(BookSnapshot::decode(&bytes).unwrap(), msg);
}

#[test]
fn normalized_message_round_trip_all_types() {
    let messages: Vec<NormalizedMessage> = vec![
        NormalizedMessage::Heartbeat(Heartbeat {
            header: base_header(MessageType::Heartbeat),
        }),
        NormalizedMessage::FeedStatus(FeedStatus {
            header: base_header(MessageType::FeedStatus),
            state: FeedState::Recovering,
        }),
        NormalizedMessage::BestBidOffer(BestBidOffer {
            header: base_header(MessageType::BestBidOffer),
            symbol: "BTCUSDT".into(),
            bid_price: 1,
            bid_qty: 1,
            ask_price: 2,
            ask_qty: 1,
            update_id: UPDATE_ID_NONE,
        }),
        NormalizedMessage::BookDelta(BookDelta {
            header: base_header(MessageType::BookDelta),
            symbol: "BTCUSDT".into(),
            first_update_id: 1,
            final_update_id: 1,
            prev_update_id: UPDATE_ID_NONE,
            bids: vec![PriceLevel { price: 1, qty: 1 }],
            asks: vec![],
        }),
        NormalizedMessage::Trade(Trade {
            header: base_header(MessageType::Trade),
            symbol: "BTCUSDT".into(),
            trade_id: 1,
            price: 1,
            qty: 1,
            trade_ts: TS_NONE,
            is_buyer_maker: true,
            aggressor_side: AggressorSide::Unknown,
        }),
        NormalizedMessage::MarkPrice(MarkPrice {
            header: base_header(MessageType::MarkPrice),
            symbol: "BTCUSDT".into(),
            mark_price: 1,
            index_price: 1,
        }),
        NormalizedMessage::FundingRate(FundingRate {
            header: base_header(MessageType::FundingRate),
            symbol: "BTCUSDT".into(),
            funding_rate: 1,
            next_funding_time: 1,
        }),
        NormalizedMessage::Liquidation(Liquidation {
            header: base_header(MessageType::Liquidation),
            symbol: "BTCUSDT".into(),
            side: AggressorSide::Buy,
            price: 1,
            qty: 1,
            avg_price: 1,
            last_filled_qty: 1,
        }),
        NormalizedMessage::OpenInterest(OpenInterest {
            header: base_header(MessageType::OpenInterest),
            symbol: "BTCUSDT".into(),
            open_interest: 1,
        }),
        NormalizedMessage::BookSnapshot(BookSnapshot {
            header: base_header(MessageType::BookSnapshot),
            symbol: "BTCUSDT".into(),
            update_id: 1,
            bids: vec![],
            asks: vec![],
        }),
        NormalizedMessage::InstrumentDefinition(InstrumentDefinition {
            header: base_header(MessageType::InstrumentDefinition),
            symbol: "BTCUSDT".into(),
            base_asset: "BTC".into(),
            quote_asset: "USDT".into(),
            price_scale: 8,
            qty_scale: 8,
            tick_size: 1,
            step_size: 1,
            min_qty: 1,
            min_notional: 1,
            contract_size: 0,
            is_trading: true,
        }),
        NormalizedMessage::TradingStatus(TradingStatus {
            header: base_header(MessageType::TradingStatus),
            symbol: "BTCUSDT".into(),
            is_trading: false,
        }),
        NormalizedMessage::GapDetected(GapDetected {
            header: base_header(MessageType::GapDetected),
            symbol: "BTCUSDT".into(),
            expected_update_id: 1,
            received_update_id: 3,
        }),
        NormalizedMessage::BookStale(BookStale {
            header: base_header(MessageType::BookStale),
            symbol: "BTCUSDT".into(),
            reason: BookStaleReason::StaleTimeout,
        }),
        NormalizedMessage::BookRecovered(BookRecovered {
            header: base_header(MessageType::BookRecovered),
            symbol: "BTCUSDT".into(),
            snapshot_update_id: 1,
        }),
        NormalizedMessage::AccountUpdate(AccountUpdate {
            header: base_header(MessageType::AccountUpdate),
        }),
        NormalizedMessage::OrderUpdate(OrderUpdate {
            header: base_header(MessageType::OrderUpdate),
        }),
    ];

    for msg in messages {
        let bytes = encode(|b| msg.encode_into(b));
        let decoded = NormalizedMessage::from_bytes(&bytes)
            .unwrap_or_else(|e| panic!("decode failed for {:?}: {e}", msg.header().message_type));
        assert_eq!(
            decoded,
            msg,
            "round-trip failed for {:?}",
            msg.header().message_type
        );
    }
}

#[test]
fn wrong_message_type_is_rejected() {
    let hb = Heartbeat {
        header: base_header(MessageType::Heartbeat),
    };
    let bytes = encode(|b| hb.encode_into(b));
    // Heartbeat bytes fed to BestBidOffer::decode must fail
    let err = BestBidOffer::decode(&bytes).unwrap_err();
    assert!(matches!(
        err,
        connector_core::Error::MessageTypeMismatch { .. }
    ));
}
