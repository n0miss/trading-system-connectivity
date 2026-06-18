//! Shadow-mode comparison library.
//!
//! Reads `BestBidOffer` frames from two `mpsc::Receiver<Vec<u8>>` streams —
//! one for the active connector generation, one for the shadow — decodes them
//! using `connector_core`, and reports price divergences per symbol.
//!
//! # Aeron wiring (production)
//!
//! In production, each stream is backed by an Aeron subscription.  Active
//! shards publish on stream IDs `1..=N`; shadow shards publish on
//! `SHADOW_STREAM_ID_OFFSET + 1..=SHADOW_STREAM_ID_OFFSET + N`.  When real
//! Aeron subscriptions land, replace the `Receiver<Vec<u8>>` constructor
//! parameter with an Aeron image handler that forwards frames to the same
//! mpsc channel.
//!
//! # In-process / test use
//!
//! Supply `mpsc::sync_channel` pairs directly.  `ChannelPublication` from
//! `aeron-publisher` produces frames in the same binary format.

use std::collections::{HashMap, HashSet};
use std::sync::mpsc::{Receiver, TryRecvError};

use connector_core::NormalizedMessage;

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

/// Shadow generation uses stream IDs offset by this value from the active ones.
///
/// Active shard `k` → Aeron stream `k + 1`.
/// Shadow shard `k` → Aeron stream `k + 1 + SHADOW_STREAM_ID_OFFSET`.
pub const SHADOW_STREAM_ID_OFFSET: i32 = 1_000;

// ---------------------------------------------------------------------------
// Config
// ---------------------------------------------------------------------------

/// Thresholds for declaring the shadow generation stable.
#[derive(Debug, Clone)]
pub struct CompareConfig {
    /// Maximum allowed price difference in basis points (1 bps = 0.01%).
    /// Differences at or below this are accepted.  Default: 1 bps.
    pub tolerance_bps: i64,
    /// Minimum number of per-symbol comparison samples before a verdict can
    /// be `Stable`.  Default: 60 (≈1 minute at 1 comparison/s/symbol).
    pub min_samples: u64,
    /// Maximum allowed divergence rate per symbol as a percentage.
    /// Default: 0.0 — no divergence above tolerance is permitted.
    pub max_divergence_pct: f64,
}

impl Default for CompareConfig {
    fn default() -> Self {
        Self { tolerance_bps: 1, min_samples: 60, max_divergence_pct: 0.0 }
    }
}

// ---------------------------------------------------------------------------
// SymbolStats
// ---------------------------------------------------------------------------

/// Cumulative comparison statistics for one symbol.
#[derive(Debug, Clone, Default)]
pub struct SymbolStats {
    pub symbol:           String,
    pub samples:          u64,
    pub divergences:      u64,
    pub max_bid_diff_bps: i64,
    pub max_ask_diff_bps: i64,
}

impl SymbolStats {
    /// Percentage of samples that exceeded the tolerance.
    pub fn divergence_pct(&self) -> f64 {
        if self.samples == 0 { 0.0 } else { self.divergences as f64 / self.samples as f64 * 100.0 }
    }

    pub fn is_acceptable(&self, cfg: &CompareConfig) -> bool {
        self.divergence_pct() <= cfg.max_divergence_pct
    }
}

// ---------------------------------------------------------------------------
// Verdict
// ---------------------------------------------------------------------------

/// Overall stability verdict emitted by [`Comparator::verdict`].
#[derive(Debug, Clone, PartialEq)]
pub enum Verdict {
    /// Not enough samples yet to decide.
    InsufficientData { samples: u64, required: u64 },
    /// Shadow matches active within tolerance across all symbols.
    Stable,
    /// At least one symbol exceeds the divergence threshold.
    Diverging { worst_symbol: String, worst_diff_bps: i64 },
}

impl Verdict {
    pub fn is_stable(&self) -> bool { matches!(self, Self::Stable) }

    /// Process exit code: 0 = stable, 1 = diverging, 2 = insufficient data.
    pub fn exit_code(&self) -> i32 {
        match self {
            Self::Stable             => 0,
            Self::Diverging { .. }  => 1,
            Self::InsufficientData { .. } => 2,
        }
    }
}

// ---------------------------------------------------------------------------
// Comparator
// ---------------------------------------------------------------------------

/// Compares BBO frames from two connector output streams (active vs shadow).
pub struct Comparator {
    config:     CompareConfig,
    active_rx:  Receiver<Vec<u8>>,
    shadow_rx:  Receiver<Vec<u8>>,
    active_bbo: HashMap<String, (i64, i64)>,
    shadow_bbo: HashMap<String, (i64, i64)>,
    /// Symbols updated this tick (either side); cleared after compare_pending.
    pending:    HashSet<String>,
    stats:      HashMap<String, SymbolStats>,
}

impl Comparator {
    pub fn new(
        config:    CompareConfig,
        active_rx: Receiver<Vec<u8>>,
        shadow_rx: Receiver<Vec<u8>>,
    ) -> Self {
        Self {
            config,
            active_rx,
            shadow_rx,
            active_bbo: HashMap::new(),
            shadow_bbo: HashMap::new(),
            pending:    HashSet::new(),
            stats:      HashMap::new(),
        }
    }

    /// Drain both streams and record any price comparisons for symbols updated
    /// since the last call.  Call this in a loop (e.g. every 100 ms).
    pub fn tick(&mut self) {
        self.drain_active();
        self.drain_shadow();
        self.compare_pending();
    }

    fn drain_active(&mut self) {
        loop {
            match self.active_rx.try_recv() {
                Ok(buf) => {
                    if let Ok(NormalizedMessage::BestBidOffer(bbo)) =
                        NormalizedMessage::from_bytes(&buf)
                    {
                        self.active_bbo.insert(bbo.symbol.clone(), (bbo.bid_price, bbo.ask_price));
                        self.pending.insert(bbo.symbol);
                    }
                }
                Err(TryRecvError::Empty | TryRecvError::Disconnected) => break,
            }
        }
    }

    fn drain_shadow(&mut self) {
        loop {
            match self.shadow_rx.try_recv() {
                Ok(buf) => {
                    if let Ok(NormalizedMessage::BestBidOffer(bbo)) =
                        NormalizedMessage::from_bytes(&buf)
                    {
                        self.shadow_bbo.insert(bbo.symbol.clone(), (bbo.bid_price, bbo.ask_price));
                        self.pending.insert(bbo.symbol);
                    }
                }
                Err(TryRecvError::Empty | TryRecvError::Disconnected) => break,
            }
        }
    }

    fn compare_pending(&mut self) {
        let symbols: Vec<String> = self
            .pending
            .iter()
            .filter(|s| self.active_bbo.contains_key(*s) && self.shadow_bbo.contains_key(*s))
            .cloned()
            .collect();
        self.pending.clear();

        for symbol in symbols {
            let (ab, aa) = self.active_bbo[&symbol];
            let (sb, sa) = self.shadow_bbo[&symbol];

            let bid_diff_bps = if ab != 0 { ((ab - sb).abs() * 10_000) / ab } else { 0 };
            let ask_diff_bps = if aa != 0 { ((aa - sa).abs() * 10_000) / aa } else { 0 };

            let stats = self.stats.entry(symbol.clone()).or_insert_with(|| SymbolStats {
                symbol: symbol.clone(),
                ..Default::default()
            });
            stats.samples += 1;
            if bid_diff_bps > self.config.tolerance_bps
                || ask_diff_bps > self.config.tolerance_bps
            {
                stats.divergences += 1;
            }
            stats.max_bid_diff_bps = stats.max_bid_diff_bps.max(bid_diff_bps);
            stats.max_ask_diff_bps = stats.max_ask_diff_bps.max(ask_diff_bps);
        }
    }

    /// Compute the current verdict.
    pub fn verdict(&self) -> Verdict {
        let total: u64 = self.stats.values().map(|s| s.samples).sum();
        if total < self.config.min_samples {
            return Verdict::InsufficientData { samples: total, required: self.config.min_samples };
        }

        let mut worst_symbol  = String::new();
        let mut worst_diff_bps = 0i64;

        for stats in self.stats.values() {
            if !stats.is_acceptable(&self.config) {
                let diff = stats.max_bid_diff_bps.max(stats.max_ask_diff_bps);
                if diff > worst_diff_bps {
                    worst_diff_bps = diff;
                    worst_symbol   = stats.symbol.clone();
                }
            }
        }

        if !worst_symbol.is_empty() {
            Verdict::Diverging { worst_symbol, worst_diff_bps }
        } else {
            Verdict::Stable
        }
    }

    pub fn stats(&self) -> &HashMap<String, SymbolStats> {
        &self.stats
    }

    /// Total comparison samples across all symbols.
    pub fn total_samples(&self) -> u64 {
        self.stats.values().map(|s| s.samples).sum()
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use std::sync::mpsc;

    use connector_core::{
        BestBidOffer, MessageHeader, MessageType, VenueId, MarketType, SCHEMA_VERSION, TS_NONE,
    };

    use super::*;

    // ── helpers ──────────────────────────────────────────────────────────────

    fn bbo_header() -> MessageHeader {
        MessageHeader {
            schema_version:    SCHEMA_VERSION,
            message_type:      MessageType::BestBidOffer,
            venue_id:          VenueId::BinanceSpot,
            market_type:       MarketType::Spot,
            instrument_id:     1,
            connection_id:     0,
            instance_id:       0,
            sequence_number:   1,
            exchange_event_ts: 0,
            exchange_tx_ts:    TS_NONE,
            local_recv_ts:     0,
            local_publish_ts:  0,
        }
    }

    fn encode_bbo(symbol: &str, bid: i64, ask: i64) -> Vec<u8> {
        let msg = BestBidOffer {
            header:    bbo_header(),
            symbol:    symbol.to_string(),
            bid_price: bid,
            bid_qty:   1_000_000,
            ask_price: ask,
            ask_qty:   1_000_000,
            update_id: 0,
        };
        let mut buf = vec![0u8; 512];
        let len = msg.encode_into(&mut buf).unwrap();
        buf.truncate(len);
        buf
    }

    fn make_comparator(cfg: CompareConfig) -> (
        Comparator,
        mpsc::SyncSender<Vec<u8>>,
        mpsc::SyncSender<Vec<u8>>,
    ) {
        let (active_tx, active_rx) = mpsc::sync_channel(1024);
        let (shadow_tx, shadow_rx) = mpsc::sync_channel(1024);
        let cmp = Comparator::new(cfg, active_rx, shadow_rx);
        (cmp, active_tx, shadow_tx)
    }

    // ── tests ─────────────────────────────────────────────────────────────

    #[test]
    fn insufficient_data_before_min_samples() {
        let (mut cmp, atx, stx) = make_comparator(CompareConfig { min_samples: 10, ..Default::default() });
        atx.send(encode_bbo("BTCUSDT", 5_000_000, 5_000_100)).unwrap();
        stx.send(encode_bbo("BTCUSDT", 5_000_000, 5_000_100)).unwrap();
        cmp.tick();
        assert!(matches!(cmp.verdict(), Verdict::InsufficientData { samples: 1, required: 10 }));
    }

    #[test]
    fn stable_after_min_samples_with_matching_prices() {
        let cfg = CompareConfig { min_samples: 5, tolerance_bps: 1, ..Default::default() };
        let (mut cmp, atx, stx) = make_comparator(cfg);
        for _ in 0..5 {
            atx.send(encode_bbo("BTCUSDT", 5_000_000, 5_000_100)).unwrap();
            stx.send(encode_bbo("BTCUSDT", 5_000_000, 5_000_100)).unwrap();
            cmp.tick();
        }
        assert_eq!(cmp.verdict(), Verdict::Stable);
        assert_eq!(cmp.verdict().exit_code(), 0);
    }

    #[test]
    fn diverging_when_price_exceeds_tolerance() {
        let cfg = CompareConfig { min_samples: 1, tolerance_bps: 1, max_divergence_pct: 0.0 };
        let (mut cmp, atx, stx) = make_comparator(cfg);
        // Active: bid 5_000_000 — Shadow: bid 5_001_000 → 2 bps difference
        atx.send(encode_bbo("BTCUSDT", 5_000_000, 5_000_100)).unwrap();
        stx.send(encode_bbo("BTCUSDT", 5_001_000, 5_001_100)).unwrap();
        cmp.tick();
        assert!(matches!(cmp.verdict(), Verdict::Diverging { .. }));
        assert_eq!(cmp.verdict().exit_code(), 1);
    }

    #[test]
    fn within_tolerance_is_not_a_divergence() {
        // 1 bps tolerance; difference = 0 bps
        let cfg = CompareConfig { min_samples: 1, tolerance_bps: 5, max_divergence_pct: 0.0 };
        let (mut cmp, atx, stx) = make_comparator(cfg);
        atx.send(encode_bbo("ETHUSDT", 1_000_000, 1_001_000)).unwrap();
        stx.send(encode_bbo("ETHUSDT", 1_000_000, 1_001_000)).unwrap();
        cmp.tick();
        assert_eq!(cmp.verdict(), Verdict::Stable);
    }

    #[test]
    fn one_divergence_within_allowed_pct_is_stable() {
        // Allow up to 20% of samples to diverge
        let cfg = CompareConfig { min_samples: 5, tolerance_bps: 1, max_divergence_pct: 20.0 };
        let (mut cmp, atx, stx) = make_comparator(cfg);

        // 4 matching ticks
        for _ in 0..4 {
            atx.send(encode_bbo("BNBUSDT", 600_000, 600_100)).unwrap();
            stx.send(encode_bbo("BNBUSDT", 600_000, 600_100)).unwrap();
            cmp.tick();
        }
        // 1 diverging tick (1/5 = 20% → within 20% limit)
        atx.send(encode_bbo("BNBUSDT", 600_000, 600_100)).unwrap();
        stx.send(encode_bbo("BNBUSDT", 605_000, 605_100)).unwrap();
        cmp.tick();

        assert_eq!(cmp.verdict(), Verdict::Stable);
    }

    #[test]
    fn divergence_rate_above_limit_triggers_diverging() {
        // Only 10% allowed; we inject 50%
        let cfg = CompareConfig { min_samples: 2, tolerance_bps: 1, max_divergence_pct: 10.0 };
        let (mut cmp, atx, stx) = make_comparator(cfg);

        atx.send(encode_bbo("SOLUSDT", 70_000, 70_100)).unwrap();
        stx.send(encode_bbo("SOLUSDT", 70_000, 70_100)).unwrap();
        cmp.tick();

        atx.send(encode_bbo("SOLUSDT", 70_000, 70_100)).unwrap();
        stx.send(encode_bbo("SOLUSDT", 72_000, 72_100)).unwrap(); // diverge
        cmp.tick();

        assert!(matches!(cmp.verdict(), Verdict::Diverging { .. }));
    }

    #[test]
    fn no_comparison_until_both_sides_have_seen_the_symbol() {
        let cfg = CompareConfig { min_samples: 1, ..Default::default() };
        let (mut cmp, atx, _stx) = make_comparator(cfg);
        atx.send(encode_bbo("BTCUSDT", 5_000_000, 5_000_100)).unwrap();
        cmp.tick();
        // shadow hasn't sent anything → total_samples should still be 0
        assert_eq!(cmp.total_samples(), 0);
        assert!(matches!(cmp.verdict(), Verdict::InsufficientData { samples: 0, .. }));
    }

    #[test]
    fn multiple_symbols_tracked_independently() {
        let cfg = CompareConfig { min_samples: 2, tolerance_bps: 1, max_divergence_pct: 0.0 };
        let (mut cmp, atx, stx) = make_comparator(cfg);

        for _ in 0..2 {
            atx.send(encode_bbo("BTCUSDT", 5_000_000, 5_000_100)).unwrap();
            atx.send(encode_bbo("ETHUSDT", 1_000_000, 1_000_100)).unwrap();
            stx.send(encode_bbo("BTCUSDT", 5_000_000, 5_000_100)).unwrap();
            stx.send(encode_bbo("ETHUSDT", 1_000_000, 1_000_100)).unwrap();
            cmp.tick();
        }
        assert_eq!(cmp.stats().len(), 2);
        assert_eq!(cmp.verdict(), Verdict::Stable);
    }

    #[test]
    fn only_diverging_symbol_is_named_in_verdict() {
        let cfg = CompareConfig { min_samples: 1, tolerance_bps: 1, max_divergence_pct: 0.0 };
        let (mut cmp, atx, stx) = make_comparator(cfg);

        atx.send(encode_bbo("BTCUSDT", 5_000_000, 5_000_100)).unwrap();
        stx.send(encode_bbo("BTCUSDT", 5_000_000, 5_000_100)).unwrap(); // matches
        atx.send(encode_bbo("ETHUSDT", 1_000_000, 1_000_100)).unwrap();
        stx.send(encode_bbo("ETHUSDT", 1_200_000, 1_200_100)).unwrap(); // diverges (20%)
        cmp.tick();

        match cmp.verdict() {
            Verdict::Diverging { worst_symbol, .. } => {
                assert_eq!(worst_symbol, "ETHUSDT");
            }
            other => panic!("expected Diverging, got {other:?}"),
        }
    }

    #[test]
    fn max_diff_bps_accumulated_in_stats() {
        let cfg = CompareConfig { min_samples: 2, tolerance_bps: 100, max_divergence_pct: 100.0 };
        let (mut cmp, atx, stx) = make_comparator(cfg);

        // First tick: 10 bps diff
        atx.send(encode_bbo("BTCUSDT", 10_000_000, 10_001_000)).unwrap();
        stx.send(encode_bbo("BTCUSDT", 10_010_000, 10_011_000)).unwrap(); // 10 bps on bid
        cmp.tick();

        // Second tick: 5 bps diff
        atx.send(encode_bbo("BTCUSDT", 10_000_000, 10_001_000)).unwrap();
        stx.send(encode_bbo("BTCUSDT", 10_005_000, 10_006_000)).unwrap(); // 5 bps on bid
        cmp.tick();

        let stats = &cmp.stats()["BTCUSDT"];
        assert_eq!(stats.samples, 2);
        assert_eq!(stats.max_bid_diff_bps, 10);
    }

    #[test]
    fn disconnected_channel_drains_remaining_and_stops() {
        let cfg = CompareConfig { min_samples: 1, ..Default::default() };
        let (mut cmp, atx, stx) = make_comparator(cfg);

        atx.send(encode_bbo("BTCUSDT", 5_000_000, 5_000_100)).unwrap();
        stx.send(encode_bbo("BTCUSDT", 5_000_000, 5_000_100)).unwrap();
        drop(atx);
        drop(stx);

        cmp.tick(); // should drain buffered messages and not panic
        assert_eq!(cmp.total_samples(), 1);
    }

    #[test]
    fn shadow_stream_id_offset_is_1000() {
        assert_eq!(SHADOW_STREAM_ID_OFFSET, 1_000);
    }

    #[test]
    fn verdict_exit_codes() {
        assert_eq!(Verdict::Stable.exit_code(), 0);
        assert_eq!(Verdict::Diverging { worst_symbol: "X".into(), worst_diff_bps: 10 }.exit_code(), 1);
        assert_eq!(Verdict::InsufficientData { samples: 0, required: 60 }.exit_code(), 2);
    }
}
