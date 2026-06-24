use protocol_json::{parse_spot_message, JsonError, SpotEvent};
use protocol_sbe::{decode_message, SbeError, SbeMessage};
use thiserror::Error;

use crate::connection_manager::RawFrame;

// ---------------------------------------------------------------------------
// Public types
// ---------------------------------------------------------------------------

/// Which wire protocol produced a decoded frame.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FeedKind {
    /// Binary SBE frame (Binance SBE WebSocket endpoint).
    Sbe,
    /// Text JSON frame (Binance standard JSON WebSocket endpoint).
    Json,
}

/// A successfully decoded WebSocket frame ready for normalization.
#[derive(Debug)]
pub enum DecodedFrame {
    Sbe(SbeMessage),
    Json(SpotEvent),
}

impl DecodedFrame {
    /// Which protocol produced this frame.
    pub fn feed_kind(&self) -> FeedKind {
        match self {
            Self::Sbe(_) => FeedKind::Sbe,
            Self::Json(_) => FeedKind::Json,
        }
    }
}

/// Error decoding a raw WebSocket frame.
#[derive(Debug, Error)]
pub enum FeedError {
    /// Binary frame failed SBE schema validation and JSON fallback also failed.
    #[error("SBE decode failed ({sbe}); JSON fallback also failed ({json})")]
    BothFailed { sbe: SbeError, json: JsonError },

    /// Text frame failed JSON parsing.
    #[error("JSON parse error: {0}")]
    Json(#[from] JsonError),
}

// ---------------------------------------------------------------------------
// Decode entry point
// ---------------------------------------------------------------------------

/// Decode one raw WebSocket frame, preferring SBE for binary frames.
///
/// # Dispatch rules
///
/// | Frame type         | Primary | Fallback                            |
/// |--------------------|---------|-------------------------------------|
/// | Binary (`is_binary=true`)  | SBE   | JSON (if SBE header check fails)  |
/// | Text   (`is_binary=false`) | JSON  | — (binary SBE is never text)      |
///
/// The JSON fallback on binary frames handles the case where a connection
/// temporarily delivers JSON-wrapped bytes (e.g., during a schema upgrade
/// rollout at the exchange or an endpoint mis-routing).
///
/// Text frames are never attempted as SBE; the SBE header cannot appear in
/// valid UTF-8 JSON.
pub fn decode_raw_frame(frame: &RawFrame) -> Result<DecodedFrame, FeedError> {
    if frame.is_binary {
        decode_binary_frame(&frame.payload)
    } else {
        parse_spot_message(&frame.payload)
            .map(DecodedFrame::Json)
            .map_err(FeedError::Json)
    }
}

fn decode_binary_frame(payload: &[u8]) -> Result<DecodedFrame, FeedError> {
    match decode_message(payload) {
        Ok(msg) => Ok(DecodedFrame::Sbe(msg)),
        Err(sbe_err) => {
            // JSON fallback: a binary-capable transport might carry JSON
            // while SBE is being negotiated or re-negotiated.
            match parse_spot_message(payload) {
                Ok(event) => Ok(DecodedFrame::Json(event)),
                Err(json_err) => Err(FeedError::BothFailed {
                    sbe: sbe_err,
                    json: json_err,
                }),
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use protocol_sbe::SPOT_SCHEMA_ID;

    // -----------------------------------------------------------------------
    // Frame builders
    // -----------------------------------------------------------------------

    fn make_sbe_trade_frame() -> Vec<u8> {
        let mut buf = Vec::new();

        let write_u16 = |buf: &mut Vec<u8>, v: u16| buf.extend_from_slice(&v.to_le_bytes());
        let write_i64 = |buf: &mut Vec<u8>, v: i64| buf.extend_from_slice(&v.to_le_bytes());
        let write_u8 = |buf: &mut Vec<u8>, v: u8| buf.push(v);

        // SBE header: blockLength=58, templateId=0, schemaId=3, version=0
        write_u16(&mut buf, 58);
        write_u16(&mut buf, 0);
        write_u16(&mut buf, SPOT_SCHEMA_ID);
        write_u16(&mut buf, 0);
        // Root block (58 bytes):
        write_i64(&mut buf, 1_699_000_000_000); // eventTime
        write_i64(&mut buf, 1_699_000_001_000); // transactTime
        write_i64(&mut buf, 12_345_678); // tradeId
        write_i64(&mut buf, 5_000_050_000_000); // price mantissa
        write_i64(&mut buf, 100_000); // qty mantissa
        write_i64(&mut buf, 111_111); // buyerOrderId
        write_i64(&mut buf, 222_222); // sellerOrderId
        write_u8(&mut buf, 1); // aggressorSide=BUY
        write_u8(&mut buf, 0); // isBuyerMarketMaker=false
                               // varString symbol
        write_u16(&mut buf, 7);
        buf.extend_from_slice(b"BTCUSDT");
        buf
    }

    fn make_json_bbo_frame() -> Vec<u8> {
        br#"{"stream":"btcusdt@bookTicker","data":{"u":400900217,"s":"BTCUSDT","b":"96500.00000000","B":"1.23000000","a":"96501.00000000","A":"0.50000000"}}"#.to_vec()
    }

    fn make_json_depth_frame() -> Vec<u8> {
        br#"{"stream":"btcusdt@depth@100ms","data":{"e":"depthUpdate","E":1748000000000,"s":"BTCUSDT","U":50000001,"u":50000005,"b":[["96500.00000000","2.50000000"]],"a":[["96501.00000000","1.00000000"]]}}"#.to_vec()
    }

    fn binary_frame(payload: Vec<u8>) -> RawFrame {
        RawFrame {
            recv_ts: 0,
            payload,
            is_binary: true,
        }
    }

    fn text_frame(payload: Vec<u8>) -> RawFrame {
        RawFrame {
            recv_ts: 0,
            payload,
            is_binary: false,
        }
    }

    // -----------------------------------------------------------------------
    // Binary SBE frames → DecodedFrame::Sbe
    // -----------------------------------------------------------------------

    #[test]
    fn binary_sbe_frame_decodes_as_sbe() {
        let frame = binary_frame(make_sbe_trade_frame());
        let decoded = decode_raw_frame(&frame).unwrap();
        assert!(matches!(decoded, DecodedFrame::Sbe(_)));
        assert_eq!(decoded.feed_kind(), FeedKind::Sbe);
    }

    #[test]
    fn binary_sbe_trade_has_correct_trade_id() {
        let frame = binary_frame(make_sbe_trade_frame());
        let DecodedFrame::Sbe(SbeMessage::Trade(tr)) = decode_raw_frame(&frame).unwrap() else {
            panic!("expected SBE Trade")
        };
        assert_eq!(tr.trade_id, 12_345_678);
    }

    // -----------------------------------------------------------------------
    // Text JSON frames → DecodedFrame::Json
    // -----------------------------------------------------------------------

    #[test]
    fn text_bbo_frame_decodes_as_json() {
        let frame = text_frame(make_json_bbo_frame());
        let decoded = decode_raw_frame(&frame).unwrap();
        assert!(matches!(
            decoded,
            DecodedFrame::Json(SpotEvent::BookTicker(_))
        ));
        assert_eq!(decoded.feed_kind(), FeedKind::Json);
    }

    #[test]
    fn text_depth_frame_decodes_as_json() {
        let frame = text_frame(make_json_depth_frame());
        let decoded = decode_raw_frame(&frame).unwrap();
        assert!(matches!(
            decoded,
            DecodedFrame::Json(SpotEvent::DepthUpdate(_))
        ));
    }

    // -----------------------------------------------------------------------
    // JSON fallback for binary frames with bad SBE header
    // -----------------------------------------------------------------------

    #[test]
    fn binary_json_payload_falls_back_to_json() {
        // A binary-framed JSON payload: SBE validation will fail (wrong schema_id),
        // then the JSON fallback should succeed.
        let frame = binary_frame(make_json_bbo_frame());
        let decoded = decode_raw_frame(&frame).unwrap();
        assert!(
            matches!(decoded, DecodedFrame::Json(_)),
            "expected JSON fallback, got {:?}",
            decoded.feed_kind(),
        );
    }

    // -----------------------------------------------------------------------
    // Error cases
    // -----------------------------------------------------------------------

    #[test]
    fn text_frame_with_invalid_json_returns_json_error() {
        let frame = text_frame(b"not json at all".to_vec());
        let err = decode_raw_frame(&frame).unwrap_err();
        assert!(matches!(err, FeedError::Json(_)));
    }

    #[test]
    fn binary_frame_with_garbage_returns_both_failed() {
        // Random bytes that are neither valid SBE nor valid JSON.
        let frame = binary_frame(vec![0xFF, 0xFE, 0x00, 0x01, 0xAB, 0xCD, 0xEF, 0x42, 0x00]);
        let err = decode_raw_frame(&frame).unwrap_err();
        assert!(matches!(err, FeedError::BothFailed { .. }));
    }

    #[test]
    fn empty_binary_frame_returns_both_failed() {
        let frame = binary_frame(vec![]);
        let err = decode_raw_frame(&frame).unwrap_err();
        assert!(matches!(err, FeedError::BothFailed { .. }));
    }

    // -----------------------------------------------------------------------
    // FeedKind helpers
    // -----------------------------------------------------------------------

    #[test]
    fn sbe_decoded_frame_reports_sbe_kind() {
        let frame = binary_frame(make_sbe_trade_frame());
        let decoded = decode_raw_frame(&frame).unwrap();
        assert_eq!(decoded.feed_kind(), FeedKind::Sbe);
    }

    #[test]
    fn json_decoded_frame_reports_json_kind() {
        let frame = text_frame(make_json_bbo_frame());
        let decoded = decode_raw_frame(&frame).unwrap();
        assert_eq!(decoded.feed_kind(), FeedKind::Json);
    }
}
