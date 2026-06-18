use crate::aggressorside::AggressorSide;
use crate::decimal::Decimal64;
use crate::error::SbeError;
use crate::header::{SbeHeader, SBE_HEADER_SIZE};
use crate::template::TemplateId;

// ---------------------------------------------------------------------------
// Decoded message types
// ---------------------------------------------------------------------------

/// Decoded `TradesStreamEvent` (templateId = 0).
///
/// Corresponds to the Binance Spot `{symbol}@trade` WebSocket stream.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TradeEvent {
    /// Event creation time (exchange clock, epoch ms).
    pub event_time:            i64,
    /// Trade execution time (epoch ms).
    pub transact_time:         i64,
    pub trade_id:              i64,
    pub price:                 Decimal64,
    pub quantity:              Decimal64,
    pub buyer_order_id:        i64,
    pub seller_order_id:       i64,
    pub aggressor_side:        AggressorSide,
    /// `true` when the buyer is the passive (market-maker) side.
    pub is_buyer_market_maker: bool,
    pub symbol:                String,
}

/// Decoded `BestBidAskStreamEvent` (templateId = 1).
///
/// Corresponds to the Binance Spot `{symbol}@bookTicker` stream.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BboEvent {
    pub event_time:     i64,
    pub transact_time:  i64,
    pub best_bid_price: Decimal64,
    pub best_bid_qty:   Decimal64,
    pub best_ask_price: Decimal64,
    pub best_ask_qty:   Decimal64,
    pub symbol:         String,
}

/// One price level in a depth message.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DepthLevel {
    pub price:    Decimal64,
    pub quantity: Decimal64,
}

/// Decoded `DepthSnapshotStreamEvent` (templateId = 2).
///
/// Corresponds to the Binance Spot `{symbol}@depth<N>` partial book stream.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DepthSnapshotEvent {
    pub event_time:     i64,
    pub last_update_id: i64,
    pub symbol:         String,
    pub bids:           Vec<DepthLevel>,
    pub asks:           Vec<DepthLevel>,
}

/// Decoded `DepthDiffStreamEvent` (templateId = 3).
///
/// Corresponds to the Binance Spot `{symbol}@depth@<interval>ms` stream.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DepthDiffEvent {
    /// Event creation time (epoch ms).
    pub event_time:           i64,
    pub transact_time:        i64,
    /// First update ID in this batch (`U` in JSON).
    pub first_update_id:      i64,
    /// Final update ID in this batch (`u` in JSON).
    pub final_update_id:      i64,
    /// Previous final update ID (`pu` in JSON; 0 for Spot diff streams).
    pub prev_final_update_id: i64,
    pub symbol:               String,
    pub bids:                 Vec<DepthLevel>,
    pub asks:                 Vec<DepthLevel>,
}

/// Result of [`decode_message`]: one decoded SBE message.
#[derive(Debug)]
pub enum SbeMessage {
    Trade(TradeEvent),
    Bbo(BboEvent),
    DepthSnapshot(DepthSnapshotEvent),
    DepthDiff(DepthDiffEvent),
}

// ---------------------------------------------------------------------------
// Internal cursor
// ---------------------------------------------------------------------------

struct Cursor<'a> {
    buf: &'a [u8],
    pos: usize,
}

impl<'a> Cursor<'a> {
    fn new(buf: &'a [u8]) -> Self {
        Self { buf, pos: 0 }
    }

    fn need(&self, n: usize) -> Result<(), SbeError> {
        let end = self.pos + n;
        if self.buf.len() < end {
            Err(SbeError::BufferTooShort { needed: end, have: self.buf.len() })
        } else {
            Ok(())
        }
    }

    fn read_u8(&mut self) -> Result<u8, SbeError> {
        self.need(1)?;
        let v = self.buf[self.pos];
        self.pos += 1;
        Ok(v)
    }

    fn read_u16_le(&mut self) -> Result<u16, SbeError> {
        self.need(2)?;
        let v = u16::from_le_bytes([self.buf[self.pos], self.buf[self.pos + 1]]);
        self.pos += 2;
        Ok(v)
    }

    fn read_i64_le(&mut self) -> Result<i64, SbeError> {
        self.need(8)?;
        let bytes: [u8; 8] = self.buf[self.pos..self.pos + 8].try_into().unwrap();
        self.pos += 8;
        Ok(i64::from_le_bytes(bytes))
    }

    fn read_decimal64(&mut self) -> Result<Decimal64, SbeError> {
        Ok(Decimal64 { mantissa: self.read_i64_le()? })
    }

    /// Advance the cursor to `target`, skipping any bytes in between.
    ///
    /// Used for forward-compatibility: when a newer schema version adds fields
    /// to the root block or a group entry, `skip_to` ensures we land at the
    /// correct position before reading variable-length data.
    fn skip_to(&mut self, target: usize) -> Result<(), SbeError> {
        if target <= self.pos {
            return Ok(()); // already past target (smaller schema than expected)
        }
        self.need(target - self.pos)?;
        self.pos = target;
        Ok(())
    }

    /// Read a `groupSizeEncoding` header: `(entry_block_len, num_in_group)`.
    fn read_group_header(&mut self) -> Result<(u16, u8), SbeError> {
        let block_len = self.read_u16_le()?;
        let count     = self.read_u8()?;
        Ok((block_len, count))
    }

    /// Read a variable-length UTF-8 string (2-byte length prefix).
    fn read_var_string(&mut self) -> Result<String, SbeError> {
        let len = self.read_u16_le()? as usize;
        self.need(len)?;
        let s = std::str::from_utf8(&self.buf[self.pos..self.pos + len])
            .map_err(|_| SbeError::InvalidUtf8)?
            .to_owned();
        self.pos += len;
        Ok(s)
    }

    /// Read `count` depth level entries, each `entry_block_len` bytes long.
    fn read_depth_levels(&mut self, entry_block_len: u16, count: u8) -> Result<Vec<DepthLevel>, SbeError> {
        let mut levels = Vec::with_capacity(count as usize);
        for _ in 0..count {
            let entry_start = self.pos;
            let price    = self.read_decimal64()?;
            let quantity = self.read_decimal64()?;
            levels.push(DepthLevel { price, quantity });
            // Skip unknown trailing fields in this entry for forward-compat.
            let entry_end = entry_start + entry_block_len as usize;
            self.skip_to(entry_end)?;
        }
        Ok(levels)
    }
}

// ---------------------------------------------------------------------------
// Public decode functions
// ---------------------------------------------------------------------------

/// Decode and dispatch a complete SBE frame, validating schema and version.
///
/// This is the primary entry point for the SBE pipeline (§7.29).  After an
/// initial [`check_spot_schema`] at connection startup (§7.28), callers
/// typically call this function for every subsequent frame.
///
/// [`check_spot_schema`]: crate::check_spot_schema
pub fn decode_message(buf: &[u8]) -> Result<SbeMessage, SbeError> {
    let hdr = SbeHeader::decode(buf)?;
    hdr.validate_schema()?;
    match TemplateId::from_u16(hdr.template_id)? {
        TemplateId::TradesStream  => decode_trade_body(buf, hdr).map(SbeMessage::Trade),
        TemplateId::BestBidAsk   => decode_bbo_body(buf, hdr).map(SbeMessage::Bbo),
        TemplateId::DepthSnapshot => decode_depth_snapshot_body(buf, hdr).map(SbeMessage::DepthSnapshot),
        TemplateId::DepthDiff    => decode_depth_diff_body(buf, hdr).map(SbeMessage::DepthDiff),
    }
}

/// Decode a `TradesStreamEvent` frame (templateId = 0).
///
/// Validates schema and version; does not check the template ID so that
/// callers may call this directly when already certain of the message type.
pub fn decode_trade(buf: &[u8]) -> Result<TradeEvent, SbeError> {
    let hdr = SbeHeader::decode(buf)?;
    hdr.validate_schema()?;
    decode_trade_body(buf, hdr)
}

/// Decode a `BestBidAskStreamEvent` frame (templateId = 1).
pub fn decode_bbo(buf: &[u8]) -> Result<BboEvent, SbeError> {
    let hdr = SbeHeader::decode(buf)?;
    hdr.validate_schema()?;
    decode_bbo_body(buf, hdr)
}

/// Decode a `DepthSnapshotStreamEvent` frame (templateId = 2).
pub fn decode_depth_snapshot(buf: &[u8]) -> Result<DepthSnapshotEvent, SbeError> {
    let hdr = SbeHeader::decode(buf)?;
    hdr.validate_schema()?;
    decode_depth_snapshot_body(buf, hdr)
}

/// Decode a `DepthDiffStreamEvent` frame (templateId = 3).
pub fn decode_depth_diff(buf: &[u8]) -> Result<DepthDiffEvent, SbeError> {
    let hdr = SbeHeader::decode(buf)?;
    hdr.validate_schema()?;
    decode_depth_diff_body(buf, hdr)
}

// ---------------------------------------------------------------------------
// Private body decoders
// ---------------------------------------------------------------------------

fn decode_trade_body(buf: &[u8], hdr: SbeHeader) -> Result<TradeEvent, SbeError> {
    let mut cur = Cursor::new(buf);
    cur.pos = SBE_HEADER_SIZE;

    let event_time            = cur.read_i64_le()?;
    let transact_time         = cur.read_i64_le()?;
    let trade_id              = cur.read_i64_le()?;
    let price                 = cur.read_decimal64()?;
    let quantity              = cur.read_decimal64()?;
    let buyer_order_id        = cur.read_i64_le()?;
    let seller_order_id       = cur.read_i64_le()?;
    let aggressor_side        = AggressorSide::from_u8(cur.read_u8()?);
    let is_buyer_market_maker = cur.read_u8()? != 0;

    // Skip any extra fields added in newer schema versions.
    cur.skip_to(SBE_HEADER_SIZE + hdr.block_length as usize)?;

    let symbol = cur.read_var_string()?;

    Ok(TradeEvent {
        event_time, transact_time, trade_id, price, quantity,
        buyer_order_id, seller_order_id, aggressor_side,
        is_buyer_market_maker, symbol,
    })
}

fn decode_bbo_body(buf: &[u8], hdr: SbeHeader) -> Result<BboEvent, SbeError> {
    let mut cur = Cursor::new(buf);
    cur.pos = SBE_HEADER_SIZE;

    let event_time     = cur.read_i64_le()?;
    let transact_time  = cur.read_i64_le()?;
    let best_bid_price = cur.read_decimal64()?;
    let best_bid_qty   = cur.read_decimal64()?;
    let best_ask_price = cur.read_decimal64()?;
    let best_ask_qty   = cur.read_decimal64()?;

    cur.skip_to(SBE_HEADER_SIZE + hdr.block_length as usize)?;

    let symbol = cur.read_var_string()?;

    Ok(BboEvent {
        event_time, transact_time,
        best_bid_price, best_bid_qty,
        best_ask_price, best_ask_qty,
        symbol,
    })
}

fn decode_depth_snapshot_body(buf: &[u8], hdr: SbeHeader) -> Result<DepthSnapshotEvent, SbeError> {
    let mut cur = Cursor::new(buf);
    cur.pos = SBE_HEADER_SIZE;

    let event_time     = cur.read_i64_le()?;
    let last_update_id = cur.read_i64_le()?;

    cur.skip_to(SBE_HEADER_SIZE + hdr.block_length as usize)?;

    let (bid_entry_len, bid_count) = cur.read_group_header()?;
    let bids = cur.read_depth_levels(bid_entry_len, bid_count)?;

    let (ask_entry_len, ask_count) = cur.read_group_header()?;
    let asks = cur.read_depth_levels(ask_entry_len, ask_count)?;

    let symbol = cur.read_var_string()?;

    Ok(DepthSnapshotEvent { event_time, last_update_id, symbol, bids, asks })
}

fn decode_depth_diff_body(buf: &[u8], hdr: SbeHeader) -> Result<DepthDiffEvent, SbeError> {
    let mut cur = Cursor::new(buf);
    cur.pos = SBE_HEADER_SIZE;

    let event_time           = cur.read_i64_le()?;
    let transact_time        = cur.read_i64_le()?;
    let first_update_id      = cur.read_i64_le()?;
    let final_update_id      = cur.read_i64_le()?;
    let prev_final_update_id = cur.read_i64_le()?;

    cur.skip_to(SBE_HEADER_SIZE + hdr.block_length as usize)?;

    let (bid_entry_len, bid_count) = cur.read_group_header()?;
    let bids = cur.read_depth_levels(bid_entry_len, bid_count)?;

    let (ask_entry_len, ask_count) = cur.read_group_header()?;
    let asks = cur.read_depth_levels(ask_entry_len, ask_count)?;

    let symbol = cur.read_var_string()?;

    Ok(DepthDiffEvent {
        event_time, transact_time,
        first_update_id, final_update_id, prev_final_update_id,
        symbol, bids, asks,
    })
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::header::SPOT_SCHEMA_ID;

    // -----------------------------------------------------------------------
    // Golden frame builders
    // -----------------------------------------------------------------------

    fn write_u16_le(buf: &mut Vec<u8>, v: u16) { buf.extend_from_slice(&v.to_le_bytes()); }
    fn write_i64_le(buf: &mut Vec<u8>, v: i64) { buf.extend_from_slice(&v.to_le_bytes()); }
    fn write_u8(buf: &mut Vec<u8>, v: u8)      { buf.push(v); }

    fn write_sbe_header(buf: &mut Vec<u8>, block_length: u16, template_id: u16) {
        write_u16_le(buf, block_length);
        write_u16_le(buf, template_id);
        write_u16_le(buf, SPOT_SCHEMA_ID);
        write_u16_le(buf, 0); // version = 0
    }

    fn write_var_string(buf: &mut Vec<u8>, s: &str) {
        let b = s.as_bytes();
        write_u16_le(buf, b.len() as u16);
        buf.extend_from_slice(b);
    }

    fn write_group_header(buf: &mut Vec<u8>, entry_block_len: u16, count: u8) {
        write_u16_le(buf, entry_block_len);
        write_u8(buf, count);
    }

    /// Build a golden `TradesStreamEvent` frame.
    #[allow(clippy::too_many_arguments)]
    fn build_trade_frame(
        event_time: i64, transact_time: i64, trade_id: i64,
        price_mantissa: i64, qty_mantissa: i64,
        buyer_order_id: i64, seller_order_id: i64,
        aggressor_side: u8, is_bmm: u8,
        symbol: &str,
    ) -> Vec<u8> {
        let mut buf = Vec::new();
        write_sbe_header(&mut buf, 58, 0);
        write_i64_le(&mut buf, event_time);
        write_i64_le(&mut buf, transact_time);
        write_i64_le(&mut buf, trade_id);
        write_i64_le(&mut buf, price_mantissa);
        write_i64_le(&mut buf, qty_mantissa);
        write_i64_le(&mut buf, buyer_order_id);
        write_i64_le(&mut buf, seller_order_id);
        write_u8(&mut buf, aggressor_side);
        write_u8(&mut buf, is_bmm);
        write_var_string(&mut buf, symbol);
        buf
    }

    /// Build a golden `BestBidAskStreamEvent` frame.
    fn build_bbo_frame(
        event_time: i64, transact_time: i64,
        bid_price: i64, bid_qty: i64,
        ask_price: i64, ask_qty: i64,
        symbol: &str,
    ) -> Vec<u8> {
        let mut buf = Vec::new();
        write_sbe_header(&mut buf, 48, 1);
        write_i64_le(&mut buf, event_time);
        write_i64_le(&mut buf, transact_time);
        write_i64_le(&mut buf, bid_price);
        write_i64_le(&mut buf, bid_qty);
        write_i64_le(&mut buf, ask_price);
        write_i64_le(&mut buf, ask_qty);
        write_var_string(&mut buf, symbol);
        buf
    }

    /// Build a golden `DepthSnapshotStreamEvent` frame.
    fn build_depth_snapshot_frame(
        event_time: i64, last_update_id: i64,
        bids: &[(i64, i64)], asks: &[(i64, i64)],
        symbol: &str,
    ) -> Vec<u8> {
        let mut buf = Vec::new();
        write_sbe_header(&mut buf, 16, 2);
        write_i64_le(&mut buf, event_time);
        write_i64_le(&mut buf, last_update_id);
        // bids group
        write_group_header(&mut buf, 16, bids.len() as u8);
        for (p, q) in bids { write_i64_le(&mut buf, *p); write_i64_le(&mut buf, *q); }
        // asks group
        write_group_header(&mut buf, 16, asks.len() as u8);
        for (p, q) in asks { write_i64_le(&mut buf, *p); write_i64_le(&mut buf, *q); }
        write_var_string(&mut buf, symbol);
        buf
    }

    /// Build a golden `DepthDiffStreamEvent` frame.
    fn build_depth_diff_frame(
        event_time: i64, transact_time: i64,
        first_id: i64, final_id: i64, prev_id: i64,
        bids: &[(i64, i64)], asks: &[(i64, i64)],
        symbol: &str,
    ) -> Vec<u8> {
        let mut buf = Vec::new();
        write_sbe_header(&mut buf, 40, 3);
        write_i64_le(&mut buf, event_time);
        write_i64_le(&mut buf, transact_time);
        write_i64_le(&mut buf, first_id);
        write_i64_le(&mut buf, final_id);
        write_i64_le(&mut buf, prev_id);
        // bids group
        write_group_header(&mut buf, 16, bids.len() as u8);
        for (p, q) in bids { write_i64_le(&mut buf, *p); write_i64_le(&mut buf, *q); }
        // asks group
        write_group_header(&mut buf, 16, asks.len() as u8);
        for (p, q) in asks { write_i64_le(&mut buf, *p); write_i64_le(&mut buf, *q); }
        write_var_string(&mut buf, symbol);
        buf
    }

    // Helper: compare SBE-scaled value to what parse_scaled("string", scale) would return.
    // Inlined to avoid importing connector-refdata in tests.
    fn parse_scaled_str(s: &str, scale: u32) -> i64 {
        let (int_s, frac_s) = match s.find('.') {
            Some(pos) => (&s[..pos], &s[pos + 1..]),
            None      => (s, ""),
        };
        let mut frac = frac_s.to_string();
        frac.truncate(scale as usize);
        while frac.len() < scale as usize { frac.push('0'); }
        let int_v:  i64 = int_s.parse().unwrap_or(0);
        let frac_v: i64 = if frac.is_empty() { 0 } else { frac.parse().unwrap_or(0) };
        int_v * 10_i64.pow(scale) + frac_v
    }

    // -----------------------------------------------------------------------
    // TradeEvent golden fixture
    // -----------------------------------------------------------------------

    fn btcusdt_trade_frame() -> Vec<u8> {
        build_trade_frame(
            1_699_000_000_000,  // event_time
            1_699_000_001_000,  // transact_time
            12_345_678,         // trade_id
            5_000_050_000_000,  // price 50000.50 × 10^8
            100_000,            // qty   0.001   × 10^8
            111_111,            // buyer_order_id
            222_222,            // seller_order_id
            1,                  // aggressorSide = BUY
            0,                  // isBuyerMarketMaker = false
            "BTCUSDT",
        )
    }

    #[test]
    fn trade_event_fixed_fields() {
        let frame = btcusdt_trade_frame();
        let ev = decode_trade(&frame).unwrap();
        assert_eq!(ev.event_time,            1_699_000_000_000);
        assert_eq!(ev.transact_time,         1_699_000_001_000);
        assert_eq!(ev.trade_id,              12_345_678);
        assert_eq!(ev.buyer_order_id,        111_111);
        assert_eq!(ev.seller_order_id,       222_222);
        assert_eq!(ev.aggressor_side,        AggressorSide::Buy);
        assert!(!ev.is_buyer_market_maker);
        assert_eq!(ev.symbol, "BTCUSDT");
    }

    #[test]
    fn trade_event_price_at_scale_2() {
        let ev = decode_trade(&btcusdt_trade_frame()).unwrap();
        // 50000.50 at scale=2 → 5_000_050
        assert_eq!(ev.price.to_scaled(2), 5_000_050);
    }

    #[test]
    fn trade_event_qty_at_scale_3() {
        let ev = decode_trade(&btcusdt_trade_frame()).unwrap();
        // 0.001 at scale=3 → 1
        assert_eq!(ev.quantity.to_scaled(3), 1);
    }

    #[test]
    fn trade_event_sell_aggressor() {
        let frame = build_trade_frame(
            0, 0, 0,
            1_000_000_000, 1_000_000_000,
            0, 0,
            0,   // aggressorSide = SELL
            1,   // isBuyerMarketMaker = true
            "ETHUSDT",
        );
        let ev = decode_trade(&frame).unwrap();
        assert_eq!(ev.aggressor_side, AggressorSide::Sell);
        assert!(ev.is_buyer_market_maker);
    }

    // -----------------------------------------------------------------------
    // BboEvent golden fixture
    // -----------------------------------------------------------------------

    fn btcusdt_bbo_frame() -> Vec<u8> {
        build_bbo_frame(
            1_699_000_000_000,
            1_699_000_000_500,
            9_650_000_000_000,  // bid 96500.00 × 10^8
            1_230_000_000,      // bid qty 12.30 × 10^8 → 12.30
            9_650_100_000_000,  // ask 96501.00 × 10^8
            5_000_000_00,       // ask qty 0.50 × 10^8 → 0.50
            "BTCUSDT",
        )
    }

    #[test]
    fn bbo_event_fixed_fields() {
        let ev = decode_bbo(&btcusdt_bbo_frame()).unwrap();
        assert_eq!(ev.event_time,   1_699_000_000_000);
        assert_eq!(ev.transact_time,1_699_000_000_500);
        assert_eq!(ev.symbol,       "BTCUSDT");
    }

    #[test]
    fn bbo_event_bid_price_at_scale_2() {
        let ev = decode_bbo(&btcusdt_bbo_frame()).unwrap();
        // 96500.00 at scale=2 → 9_650_000
        assert_eq!(ev.best_bid_price.to_scaled(2), 9_650_000);
    }

    #[test]
    fn bbo_event_ask_price_at_scale_2() {
        let ev = decode_bbo(&btcusdt_bbo_frame()).unwrap();
        // 96501.00 at scale=2 → 9_650_100
        assert_eq!(ev.best_ask_price.to_scaled(2), 9_650_100);
    }

    // -----------------------------------------------------------------------
    // DepthSnapshotEvent golden fixture
    // -----------------------------------------------------------------------

    fn btcusdt_snapshot_frame() -> Vec<u8> {
        build_depth_snapshot_frame(
            1_699_000_000_000,
            987_654_321,
            &[
                (9_650_000_000_000, 2_500_000_000),  // bid 96500.00, qty 25.0
                (9_649_900_000_000, 1_000_000_000),  // bid 96499.00, qty 10.0
            ],
            &[
                (9_650_100_000_000, 1_000_000_000),  // ask 96501.00, qty 10.0
            ],
            "BTCUSDT",
        )
    }

    #[test]
    fn depth_snapshot_fixed_fields() {
        let ev = decode_depth_snapshot(&btcusdt_snapshot_frame()).unwrap();
        assert_eq!(ev.event_time,     1_699_000_000_000);
        assert_eq!(ev.last_update_id, 987_654_321);
        assert_eq!(ev.symbol,         "BTCUSDT");
    }

    #[test]
    fn depth_snapshot_bid_count_and_prices() {
        let ev = decode_depth_snapshot(&btcusdt_snapshot_frame()).unwrap();
        assert_eq!(ev.bids.len(), 2);
        assert_eq!(ev.bids[0].price.to_scaled(2), 9_650_000); // 96500.00
        assert_eq!(ev.bids[1].price.to_scaled(2), 9_649_900); // 96499.00
    }

    #[test]
    fn depth_snapshot_ask_count_and_prices() {
        let ev = decode_depth_snapshot(&btcusdt_snapshot_frame()).unwrap();
        assert_eq!(ev.asks.len(), 1);
        assert_eq!(ev.asks[0].price.to_scaled(2), 9_650_100); // 96501.00
    }

    #[test]
    fn depth_snapshot_empty_groups() {
        let frame = build_depth_snapshot_frame(0, 1, &[], &[], "ETHUSDT");
        let ev = decode_depth_snapshot(&frame).unwrap();
        assert!(ev.bids.is_empty());
        assert!(ev.asks.is_empty());
    }

    // -----------------------------------------------------------------------
    // DepthDiffEvent golden fixture
    // -----------------------------------------------------------------------

    fn btcusdt_diff_frame() -> Vec<u8> {
        build_depth_diff_frame(
            1_699_000_000_000,
            1_699_000_000_200,
            50_000_001,          // firstUpdateId
            50_000_005,          // finalUpdateId
            50_000_000,          // prevFinalUpdateId
            &[
                (9_650_000_000_000, 2_500_000_000),
                (9_649_900_000_000, 0),               // remove level (qty=0)
            ],
            &[
                (9_650_100_000_000, 1_000_000_000),
            ],
            "BTCUSDT",
        )
    }

    #[test]
    fn depth_diff_fixed_fields() {
        let ev = decode_depth_diff(&btcusdt_diff_frame()).unwrap();
        assert_eq!(ev.event_time,           1_699_000_000_000);
        assert_eq!(ev.transact_time,        1_699_000_000_200);
        assert_eq!(ev.first_update_id,      50_000_001);
        assert_eq!(ev.final_update_id,      50_000_005);
        assert_eq!(ev.prev_final_update_id, 50_000_000);
        assert_eq!(ev.symbol, "BTCUSDT");
    }

    #[test]
    fn depth_diff_level_removal_has_zero_qty() {
        let ev = decode_depth_diff(&btcusdt_diff_frame()).unwrap();
        assert_eq!(ev.bids.len(), 2);
        assert!(ev.bids[1].quantity.is_zero()); // qty=0 → remove
    }

    #[test]
    fn depth_diff_empty_groups() {
        let frame = build_depth_diff_frame(0, 0, 1, 1, 0, &[], &[], "ETHUSDT");
        let ev = decode_depth_diff(&frame).unwrap();
        assert!(ev.bids.is_empty());
        assert!(ev.asks.is_empty());
    }

    // -----------------------------------------------------------------------
    // decode_message dispatch
    // -----------------------------------------------------------------------

    #[test]
    fn dispatch_trade_template() {
        let frame = btcusdt_trade_frame();
        let msg = decode_message(&frame).unwrap();
        assert!(matches!(msg, SbeMessage::Trade(_)));
    }

    #[test]
    fn dispatch_bbo_template() {
        let frame = btcusdt_bbo_frame();
        let msg = decode_message(&frame).unwrap();
        assert!(matches!(msg, SbeMessage::Bbo(_)));
    }

    #[test]
    fn dispatch_depth_snapshot_template() {
        let frame = btcusdt_snapshot_frame();
        let msg = decode_message(&frame).unwrap();
        assert!(matches!(msg, SbeMessage::DepthSnapshot(_)));
    }

    #[test]
    fn dispatch_depth_diff_template() {
        let frame = btcusdt_diff_frame();
        let msg = decode_message(&frame).unwrap();
        assert!(matches!(msg, SbeMessage::DepthDiff(_)));
    }

    // -----------------------------------------------------------------------
    // Error cases
    // -----------------------------------------------------------------------

    #[test]
    fn empty_buffer_errors() {
        assert!(decode_message(&[]).is_err());
    }

    #[test]
    fn truncated_trade_body_errors() {
        let frame = btcusdt_trade_frame();
        let truncated = &frame[..frame.len() - 4];
        assert!(decode_trade(truncated).is_err());
    }

    #[test]
    fn wrong_schema_id_errors() {
        let mut frame = btcusdt_trade_frame();
        // Overwrite schema_id bytes (offset 4-5) with 0x01 0x00
        frame[4] = 1;
        frame[5] = 0;
        assert!(matches!(decode_message(&frame), Err(SbeError::SchemaMismatch { .. })));
    }

    // -----------------------------------------------------------------------
    // Cross-checks: SBE decode vs JSON parse_scaled values
    //
    // For every price/qty field we verify that:
    //   sbe_value.to_scaled(N) == parse_scaled_str(json_string, N)
    //
    // This proves the two pipelines produce identical internal representations.
    // -----------------------------------------------------------------------

    #[test]
    fn trade_price_matches_json_parse_scaled() {
        let ev = decode_trade(&btcusdt_trade_frame()).unwrap();
        // JSON equivalent: "50000.50000000"  at price_scale=2
        assert_eq!(ev.price.to_scaled(2),    parse_scaled_str("50000.50000000", 2));
        // JSON equivalent: "0.00100000"       at qty_scale=3
        assert_eq!(ev.quantity.to_scaled(3), parse_scaled_str("0.00100000",    3));
    }

    #[test]
    fn bbo_prices_match_json_parse_scaled() {
        let ev = decode_bbo(&btcusdt_bbo_frame()).unwrap();
        // bid 96500.00 / ask 96501.00 at scale=2
        assert_eq!(ev.best_bid_price.to_scaled(2), parse_scaled_str("96500.00000000", 2));
        assert_eq!(ev.best_ask_price.to_scaled(2), parse_scaled_str("96501.00000000", 2));
    }

    #[test]
    fn depth_snapshot_levels_match_json_parse_scaled() {
        let ev = decode_depth_snapshot(&btcusdt_snapshot_frame()).unwrap();
        // bid[0] price 96500.00 / qty 25.0
        assert_eq!(ev.bids[0].price.to_scaled(2),    parse_scaled_str("96500.00000000", 2));
        assert_eq!(ev.bids[0].quantity.to_scaled(3), parse_scaled_str("25.00000000", 3));
    }

    #[test]
    fn depth_diff_ids_match_json_values() {
        let ev = decode_depth_diff(&btcusdt_diff_frame()).unwrap();
        // JSON: "U":50000001, "u":50000005, "U"-1=pu=50000000
        assert_eq!(ev.first_update_id  as u64, 50_000_001u64);
        assert_eq!(ev.final_update_id  as u64, 50_000_005u64);
        assert_eq!(ev.prev_final_update_id as u64, 50_000_000u64);
    }

    // Cross-check using the actual protocol-json decoder on an equivalent JSON frame.
    #[cfg(test)]
    mod json_crosscheck {
        use super::*;
        use protocol_json::{parse_spot_message, SpotEvent};

        #[test]
        fn trade_event_matches_json_decoder() {
            // Build an SBE frame encoding a known trade.
            let frame = build_trade_frame(
                1_699_000_000_000, 1_699_000_001_000, 12_345_678,
                5_000_050_000_000,  // price  50000.50
                100_000,            // qty    0.001
                111_111, 222_222, 1, 0, "BTCUSDT",
            );
            let sbe = decode_trade(&frame).unwrap();

            // Equivalent JSON combined-stream frame.
            let json = br#"{"stream":"btcusdt@trade","data":{"e":"trade","E":1699000000000,"s":"BTCUSDT","t":12345678,"p":"50000.50000000","q":"0.00100000","T":1699000001000,"m":false,"M":true}}"#;
            let json_event = parse_spot_message(json).unwrap();
            let trade = match json_event {
                SpotEvent::Trade(t) => t,
                _ => panic!("expected Trade"),
            };

            assert_eq!(sbe.event_time,  trade.event_time_ms);
            assert_eq!(sbe.transact_time, trade.trade_time_ms);
            assert_eq!(sbe.trade_id,    trade.trade_id as i64);
            // price at scale=2
            assert_eq!(sbe.price.to_scaled(2),    parse_scaled_str(&trade.price, 2));
            // qty at scale=3
            assert_eq!(sbe.quantity.to_scaled(3), parse_scaled_str(&trade.qty, 3));
            // aggressor: JSON m=false → buyer aggressed → SBE aggressorSide=BUY
            assert_eq!(sbe.aggressor_side,        AggressorSide::Buy);
            assert!(!sbe.is_buyer_market_maker);
        }

        #[test]
        fn bbo_event_matches_json_decoder() {
            let frame = build_bbo_frame(
                1_699_000_000_000, 1_699_000_000_500,
                9_650_000_000_000,  // bid 96500.00
                1_230_000_000_00,   // bid qty 1.23
                9_650_100_000_000,  // ask 96501.00
                5_000_000_000,      // ask qty 0.05
                "BTCUSDT",
            );
            let sbe = decode_bbo(&frame).unwrap();

            let json = br#"{"stream":"btcusdt@bookTicker","data":{"u":400900217,"s":"BTCUSDT","b":"96500.00000000","B":"1.23000000","a":"96501.00000000","A":"0.05000000"}}"#;
            let json_event = parse_spot_message(json).unwrap();
            let bbo = match json_event {
                SpotEvent::BookTicker(b) => b,
                _ => panic!("expected BookTicker"),
            };

            assert_eq!(sbe.best_bid_price.to_scaled(2), parse_scaled_str(&bbo.bid_price, 2));
            assert_eq!(sbe.best_ask_price.to_scaled(2), parse_scaled_str(&bbo.ask_price, 2));
        }

        #[test]
        fn depth_diff_event_matches_json_decoder() {
            let frame = build_depth_diff_frame(
                1_748_000_000_000, 1_748_000_000_100,
                50_000_001, 50_000_005, 50_000_000,
                &[
                    (9_650_000_000_000, 2_500_000_000),  // 96500.00, 25.0
                    (9_649_900_000_000, 0),               // 96499.00, 0.0 (removal)
                ],
                &[
                    (9_650_100_000_000, 1_000_000_000),  // 96501.00, 10.0
                ],
                "BTCUSDT",
            );
            let sbe = decode_depth_diff(&frame).unwrap();

            let json = br#"{"stream":"btcusdt@depth","data":{"e":"depthUpdate","E":1748000000000,"s":"BTCUSDT","U":50000001,"u":50000005,"b":[["96500.00000000","25.00000000"],["96499.00000000","0.00000000"]],"a":[["96501.00000000","10.00000000"]]}}"#;
            let json_event = parse_spot_message(json).unwrap();
            let depth = match json_event {
                SpotEvent::DepthUpdate(d) => d,
                _ => panic!("expected DepthUpdate"),
            };

            assert_eq!(sbe.event_time,      depth.event_time_ms);
            assert_eq!(sbe.first_update_id, depth.first_update_id as i64);
            assert_eq!(sbe.final_update_id, depth.last_update_id  as i64);

            // bid[0] price and qty
            assert_eq!(
                sbe.bids[0].price.to_scaled(2),
                parse_scaled_str(&depth.bids[0][0], 2),
            );
            assert_eq!(
                sbe.bids[0].quantity.to_scaled(3),
                parse_scaled_str(&depth.bids[0][1], 3),
            );

            // bid[1] is a removal (qty == 0)
            assert!(sbe.bids[1].quantity.is_zero());
            assert_eq!(parse_scaled_str(&depth.bids[1][1], 3), 0);

            // ask[0]
            assert_eq!(
                sbe.asks[0].price.to_scaled(2),
                parse_scaled_str(&depth.asks[0][0], 2),
            );
        }
    }
}
