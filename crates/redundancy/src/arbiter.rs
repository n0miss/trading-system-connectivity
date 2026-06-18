/// Cross-instance checksum comparator and failover trigger (§9.35).
///
/// # Architecture
///
/// Both active and passive instances publish [`BookChecksum`] messages to the
/// **status stream** after every book update (§9.34).  The `ChecksumArbiter`
/// sits outside both instances, ingests those messages, and compares the
/// checksums for the same `(symbol, update_id)` pair.
///
/// # Sliding window
///
/// Checksums from the two instances will typically arrive a few microseconds
/// apart.  The arbiter maintains a per-symbol window of the last
/// [`DEFAULT_WINDOW`] update-ids, so messages that arrive slightly out of
/// order (network jitter) are still matched.  Messages older than the current
/// window tail are silently classified as [`ArbiterVerdict::Stale`].
///
/// # Phase-1 scope
///
/// Failover action is intentionally decoupled from detection.  The arbiter
/// returns a verdict; the caller passes a [`FailoverTrigger`] that decides
/// what to do.  The provided [`LogOnlyTrigger`] logs the event but takes no
/// corrective action — a Phase-1-appropriate stub of the full STONITH /
/// process-restart / Aeron handover flow described in §10.

use std::collections::{HashMap, VecDeque};

use tracing::error;

use connector_core::{BookChecksum, InstanceRole};

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

/// Number of distinct `update_id` values kept per symbol while waiting for
/// the other instance's checksum to arrive.  64 is enough to absorb typical
/// network jitter without excessive memory use.
pub const DEFAULT_WINDOW: usize = 64;

// ---------------------------------------------------------------------------
// ArbiterVerdict
// ---------------------------------------------------------------------------

/// Outcome of ingesting a single [`BookChecksum`] message.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ArbiterVerdict {
    /// Both instances reported the same checksum for `update_id`.
    /// The system is healthy.
    Matched {
        symbol:    String,
        update_id: u64,
        /// The agreed checksum value.
        checksum:  u64,
    },
    /// The two instances reported **different** checksums for the same
    /// `update_id`.  The caller should trigger a failover.
    Diverged {
        symbol:           String,
        update_id:        u64,
        active_checksum:  u64,
        passive_checksum: u64,
    },
    /// Only one instance has reported a checksum for this `update_id` yet.
    /// Ingest the other side's message to resolve.
    Pending,
    /// The `update_id` has already been evicted from the sliding window —
    /// the message arrived too late to be matched.
    Stale,
}

impl ArbiterVerdict {
    pub fn is_matched(&self)  -> bool { matches!(self, Self::Matched  { .. }) }
    pub fn is_diverged(&self) -> bool { matches!(self, Self::Diverged { .. }) }
    pub fn is_pending(&self)  -> bool { matches!(self, Self::Pending)          }
    pub fn is_stale(&self)    -> bool { matches!(self, Self::Stale)            }

    /// Returns `true` when this verdict indicates that a failover is needed.
    pub fn needs_failover(&self) -> bool { self.is_diverged() }
}

// ---------------------------------------------------------------------------
// Internal entry
// ---------------------------------------------------------------------------

struct Entry {
    update_id: u64,
    active:    Option<u64>,
    passive:   Option<u64>,
}

// ---------------------------------------------------------------------------
// ChecksumArbiter
// ---------------------------------------------------------------------------

/// Ingests [`BookChecksum`] messages from active and passive instances and
/// produces [`ArbiterVerdict`]s by comparing their checksums at the same
/// `update_id`.
///
/// # Usage
///
/// ```rust
/// use connector_core::{
///     BookChecksum, MarketType, MessageHeader, MessageType, VenueId, SCHEMA_VERSION, TS_NONE,
/// };
/// use connector_redundancy::arbiter::{ArbiterVerdict, ChecksumArbiter};
///
/// let mut arbiter = ChecksumArbiter::default();
///
/// let make_header = |instance_id: u32| MessageHeader {
///     schema_version:    SCHEMA_VERSION,
///     message_type:      MessageType::BookChecksum,
///     venue_id:          VenueId::BinanceSpot,
///     market_type:       MarketType::Spot,
///     instrument_id:     1,
///     connection_id:     0,
///     instance_id,
///     sequence_number:   0,
///     exchange_event_ts: TS_NONE,
///     exchange_tx_ts:    TS_NONE,
///     local_recv_ts:     TS_NONE,
///     local_publish_ts:  TS_NONE,
/// };
///
/// let active_msg = BookChecksum {
///     header:    make_header(0),   // instance 0 → Active
///     symbol:    "BTCUSDT".into(),
///     update_id: 1,
///     bid_depth: 10,
///     ask_depth: 10,
///     checksum:  0xDEAD_BEEF,
/// };
/// let passive_msg = BookChecksum {
///     header:    make_header(1),   // instance 1 → Passive
///     symbol:    "BTCUSDT".into(),
///     update_id: 1,
///     bid_depth: 10,
///     ask_depth: 10,
///     checksum:  0xDEAD_BEEF,   // same → Matched
/// };
///
/// assert!(arbiter.ingest(&active_msg).is_pending());   // only one side
/// assert!(arbiter.ingest(&passive_msg).is_matched());  // both sides agree
/// ```
pub struct ChecksumArbiter {
    window:  usize,
    symbols: HashMap<String, VecDeque<Entry>>,
}

impl ChecksumArbiter {
    pub fn new() -> Self { Self::with_window(DEFAULT_WINDOW) }

    pub fn with_window(window: usize) -> Self {
        assert!(window > 0, "window must be at least 1");
        Self { window, symbols: HashMap::new() }
    }

    /// Ingest a [`BookChecksum`] message from either instance.
    ///
    /// The instance role is derived from `msg.header.instance_id`:
    /// `0` → Active, any other value → Passive.
    ///
    /// Returns:
    /// * [`ArbiterVerdict::Pending`]   — only one side seen so far.
    /// * [`ArbiterVerdict::Matched`]   — both sides agree.
    /// * [`ArbiterVerdict::Diverged`]  — both sides disagree; trigger failover.
    /// * [`ArbiterVerdict::Stale`]     — `update_id` already evicted from window.
    pub fn ingest(&mut self, msg: &BookChecksum) -> ArbiterVerdict {
        let role      = InstanceRole::from_instance_id(msg.header.instance_id);
        let symbol    = &msg.symbol;
        let update_id = msg.update_id;
        let checksum  = msg.checksum;

        let queue = self.symbols.entry(symbol.clone()).or_default();

        // Reject messages older than the current window tail.
        if let Some(front) = queue.front() {
            if update_id < front.update_id {
                return ArbiterVerdict::Stale;
            }
        }

        // Find existing entry or push a new one.
        let pos = queue.iter().position(|e| e.update_id == update_id);
        if let Some(i) = pos {
            match role {
                InstanceRole::Active  => queue[i].active  = Some(checksum),
                InstanceRole::Passive => queue[i].passive = Some(checksum),
            }
        } else {
            let mut entry = Entry { update_id, active: None, passive: None };
            match role {
                InstanceRole::Active  => entry.active  = Some(checksum),
                InstanceRole::Passive => entry.passive = Some(checksum),
            }
            queue.push_back(entry);
            if queue.len() > self.window {
                queue.pop_front();
            }
        }

        // Build verdict from the (possibly just-updated) entry.
        let pos = queue.iter().position(|e| e.update_id == update_id).unwrap();
        let e   = &queue[pos];
        match (e.active, e.passive) {
            (Some(a), Some(p)) if a == p => ArbiterVerdict::Matched {
                symbol:    symbol.clone(),
                update_id,
                checksum:  a,
            },
            (Some(a), Some(p)) => ArbiterVerdict::Diverged {
                symbol:           symbol.clone(),
                update_id,
                active_checksum:  a,
                passive_checksum: p,
            },
            _ => ArbiterVerdict::Pending,
        }
    }

    /// Clear all tracked state for `symbol`, e.g. after failover completes.
    pub fn reset_symbol(&mut self, symbol: &str) {
        self.symbols.remove(symbol);
    }

    /// Clear all tracked state for all symbols.
    pub fn reset(&mut self) {
        self.symbols.clear();
    }

    /// Number of symbols currently being tracked.
    pub fn symbol_count(&self) -> usize {
        self.symbols.len()
    }

    /// Number of entries for `symbol` where only one instance has reported
    /// (i.e., still waiting for the other side).
    pub fn pending_count(&self, symbol: &str) -> usize {
        self.symbols
            .get(symbol)
            .map(|q| q.iter().filter(|e| e.active.is_none() || e.passive.is_none()).count())
            .unwrap_or(0)
    }

    /// Total number of tracked entries across all symbols (bounded by
    /// `symbol_count() * window`).
    pub fn entry_count(&self) -> usize {
        self.symbols.values().map(|q| q.len()).sum()
    }
}

impl Default for ChecksumArbiter {
    fn default() -> Self { Self::new() }
}

// ---------------------------------------------------------------------------
// FailoverTrigger
// ---------------------------------------------------------------------------

/// Action taken when [`ChecksumArbiter`] detects a checksum divergence.
///
/// The arbiter returns [`ArbiterVerdict::Diverged`] synchronously; the caller
/// then calls [`FailoverTrigger::on_divergence`] to execute the appropriate
/// response.
///
/// # Phase-1 scope
///
/// Only [`LogOnlyTrigger`] is provided.  Phase 2+ implementations will:
/// * STONITH the lagging instance via a management control plane.
/// * Promote the passive instance to active (update `InstanceConfig`).
/// * Signal Aeron subscribers to switch to the new active's stream.
pub trait FailoverTrigger {
    fn on_divergence(&self, symbol: &str, update_id: u64, active_cs: u64, passive_cs: u64);
}

/// Logs the divergence event at `ERROR` level but takes no corrective action.
///
/// This is the Phase-1-appropriate stub of the full failover system (§9.35,
/// §10).  Replace with a real trigger before operating in production.
pub struct LogOnlyTrigger;

impl FailoverTrigger for LogOnlyTrigger {
    fn on_divergence(&self, symbol: &str, update_id: u64, active_cs: u64, passive_cs: u64) {
        error!(
            %symbol,
            update_id,
            active_checksum  = active_cs,
            passive_checksum = passive_cs,
            "book checksum divergence — active and passive disagree; failover required (§9.35)"
        );
    }
}

// ---------------------------------------------------------------------------
// process() convenience combinator
// ---------------------------------------------------------------------------

/// Ingest `msg` into `arbiter`, invoke `trigger` if the verdict is
/// [`ArbiterVerdict::Diverged`], and return the verdict.
///
/// This is the main hot-path entry point for the arbiter loop.
///
/// ```rust
/// use connector_core::{
///     BookChecksum, MarketType, MessageHeader, MessageType, VenueId, SCHEMA_VERSION, TS_NONE,
/// };
/// use connector_redundancy::arbiter::{
///     process, ArbiterVerdict, ChecksumArbiter, LogOnlyTrigger,
/// };
///
/// let mut arbiter = ChecksumArbiter::default();
/// let trigger     = LogOnlyTrigger;
///
/// let make = |id: u32, cs: u64| BookChecksum {
///     header:    MessageHeader {
///         schema_version:    SCHEMA_VERSION,
///         message_type:      MessageType::BookChecksum,
///         venue_id:          VenueId::BinanceSpot,
///         market_type:       MarketType::Spot,
///         instrument_id:     1,
///         connection_id:     0,
///         instance_id:       id,
///         sequence_number:   0,
///         exchange_event_ts: TS_NONE,
///         exchange_tx_ts:    TS_NONE,
///         local_recv_ts:     TS_NONE,
///         local_publish_ts:  TS_NONE,
///     },
///     symbol:    "BTCUSDT".into(),
///     update_id: 1,
///     bid_depth: 0,
///     ask_depth: 0,
///     checksum:  cs,
/// };
///
/// let v1 = process(&mut arbiter, &make(0, 0xAA), &trigger);
/// assert!(v1.is_pending());
///
/// let v2 = process(&mut arbiter, &make(1, 0xAA), &trigger); // same checksum
/// assert!(v2.is_matched());
/// ```
pub fn process<T: FailoverTrigger>(
    arbiter: &mut ChecksumArbiter,
    msg: &BookChecksum,
    trigger: &T,
) -> ArbiterVerdict {
    let verdict = arbiter.ingest(msg);
    if let ArbiterVerdict::Diverged { ref symbol, update_id, active_checksum, passive_checksum } = verdict {
        trigger.on_divergence(symbol, update_id, active_checksum, passive_checksum);
    }
    verdict
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use std::cell::RefCell;
    use connector_core::{MarketType, MessageHeader, MessageType, VenueId, SCHEMA_VERSION, TS_NONE};

    // -----------------------------------------------------------------------
    // Helpers
    // -----------------------------------------------------------------------

    fn make_header(instance_id: u32) -> MessageHeader {
        MessageHeader {
            schema_version:    SCHEMA_VERSION,
            message_type:      MessageType::BookChecksum,
            venue_id:          VenueId::BinanceSpot,
            market_type:       MarketType::Spot,
            instrument_id:     1,
            connection_id:     0,
            instance_id,
            sequence_number:   0,
            exchange_event_ts: TS_NONE,
            exchange_tx_ts:    TS_NONE,
            local_recv_ts:     TS_NONE,
            local_publish_ts:  TS_NONE,
        }
    }

    fn msg(instance_id: u32, symbol: &str, update_id: u64, checksum: u64) -> BookChecksum {
        BookChecksum {
            header:    make_header(instance_id),
            symbol:    symbol.into(),
            update_id,
            bid_depth: 0,
            ask_depth: 0,
            checksum,
        }
    }

    fn active(symbol: &str, update_id: u64, checksum: u64)  -> BookChecksum { msg(0, symbol, update_id, checksum) }
    fn passive(symbol: &str, update_id: u64, checksum: u64) -> BookChecksum { msg(1, symbol, update_id, checksum) }

    // A trigger that records calls for assertion.
    struct RecordingTrigger {
        calls: RefCell<Vec<(String, u64, u64, u64)>>,
    }
    impl RecordingTrigger {
        fn new() -> Self { Self { calls: RefCell::new(vec![]) } }
        fn call_count(&self) -> usize { self.calls.borrow().len() }
    }
    impl FailoverTrigger for RecordingTrigger {
        fn on_divergence(&self, symbol: &str, update_id: u64, a: u64, p: u64) {
            self.calls.borrow_mut().push((symbol.into(), update_id, a, p));
        }
    }

    // -----------------------------------------------------------------------
    // Pending → Matched
    // -----------------------------------------------------------------------

    #[test]
    fn single_message_is_pending() {
        let mut arb = ChecksumArbiter::default();
        let v = arb.ingest(&active("BTCUSDT", 1, 0xAA));
        assert!(v.is_pending());
    }

    #[test]
    fn active_then_passive_same_checksum_is_matched() {
        let mut arb = ChecksumArbiter::default();
        arb.ingest(&active("BTCUSDT", 1, 0xAA));
        let v = arb.ingest(&passive("BTCUSDT", 1, 0xAA));
        assert!(v.is_matched());
        match v {
            ArbiterVerdict::Matched { symbol, update_id, checksum } => {
                assert_eq!(symbol,    "BTCUSDT");
                assert_eq!(update_id, 1);
                assert_eq!(checksum,  0xAA);
            }
            _ => panic!("expected Matched"),
        }
    }

    #[test]
    fn passive_then_active_same_checksum_is_matched() {
        let mut arb = ChecksumArbiter::default();
        arb.ingest(&passive("BTCUSDT", 1, 0xBB));
        let v = arb.ingest(&active("BTCUSDT", 1, 0xBB));
        assert!(v.is_matched());
    }

    // -----------------------------------------------------------------------
    // Divergence
    // -----------------------------------------------------------------------

    #[test]
    fn different_checksums_produce_diverged() {
        let mut arb = ChecksumArbiter::default();
        arb.ingest(&active("BTCUSDT", 1, 0xAA));
        let v = arb.ingest(&passive("BTCUSDT", 1, 0xBB));
        assert!(v.is_diverged());
        match v {
            ArbiterVerdict::Diverged { symbol, update_id, active_checksum, passive_checksum } => {
                assert_eq!(symbol,           "BTCUSDT");
                assert_eq!(update_id,        1);
                assert_eq!(active_checksum,  0xAA);
                assert_eq!(passive_checksum, 0xBB);
            }
            _ => panic!("expected Diverged"),
        }
    }

    #[test]
    fn diverged_needs_failover() {
        let mut arb = ChecksumArbiter::default();
        arb.ingest(&active("BTCUSDT", 1, 0xAA));
        let v = arb.ingest(&passive("BTCUSDT", 1, 0xCC));
        assert!(v.needs_failover());
    }

    #[test]
    fn matched_does_not_need_failover() {
        let mut arb = ChecksumArbiter::default();
        arb.ingest(&active("BTCUSDT", 1, 0xAA));
        let v = arb.ingest(&passive("BTCUSDT", 1, 0xAA));
        assert!(!v.needs_failover());
    }

    // -----------------------------------------------------------------------
    // Multiple update_ids
    // -----------------------------------------------------------------------

    #[test]
    fn independent_update_ids_resolved_separately() {
        let mut arb = ChecksumArbiter::default();

        // update_id 1 — match
        arb.ingest(&active("BTCUSDT", 1, 0xAA));
        arb.ingest(&passive("BTCUSDT", 1, 0xAA));

        // update_id 2 — diverge
        arb.ingest(&active("BTCUSDT", 2, 0xAA));
        let v2 = arb.ingest(&passive("BTCUSDT", 2, 0xBB));

        assert!(v2.is_diverged());
    }

    #[test]
    fn interleaved_symbols_tracked_independently() {
        let mut arb = ChecksumArbiter::default();

        arb.ingest(&active("BTCUSDT", 1, 0xAA));
        arb.ingest(&active("ETHUSDT", 1, 0x11));

        let v_btc = arb.ingest(&passive("BTCUSDT", 1, 0xAA)); // match
        let v_eth = arb.ingest(&passive("ETHUSDT", 1, 0x22)); // diverge

        assert!(v_btc.is_matched());
        assert!(v_eth.is_diverged());
        assert_eq!(arb.symbol_count(), 2);
    }

    // -----------------------------------------------------------------------
    // Window eviction and Stale
    // -----------------------------------------------------------------------

    #[test]
    fn stale_update_id_is_rejected() {
        let mut arb = ChecksumArbiter::with_window(2);
        // Fill the window with update_ids 1 and 2.
        arb.ingest(&active("BTCUSDT", 1, 0xAA));
        arb.ingest(&active("BTCUSDT", 2, 0xBB));
        // Push update_id 3 to evict update_id 1.
        arb.ingest(&active("BTCUSDT", 3, 0xCC));

        // Now update_id 1 is evicted; a late passive message for it is Stale.
        let v = arb.ingest(&passive("BTCUSDT", 1, 0xAA));
        assert!(v.is_stale());
    }

    #[test]
    fn window_entry_count_bounded() {
        let window = 8;
        let mut arb = ChecksumArbiter::with_window(window);
        for i in 0..20u64 {
            arb.ingest(&active("BTCUSDT", i, i));
        }
        assert!(arb.entry_count() <= window);
    }

    #[test]
    fn first_message_for_empty_symbol_is_never_stale() {
        let mut arb = ChecksumArbiter::default();
        let v = arb.ingest(&passive("BTCUSDT", 999, 0xAA));
        assert!(v.is_pending());
    }

    // -----------------------------------------------------------------------
    // Reset
    // -----------------------------------------------------------------------

    #[test]
    fn reset_symbol_clears_state() {
        let mut arb = ChecksumArbiter::default();
        arb.ingest(&active("BTCUSDT", 1, 0xAA));
        assert_eq!(arb.pending_count("BTCUSDT"), 1);

        arb.reset_symbol("BTCUSDT");
        assert_eq!(arb.symbol_count(),         0);
        assert_eq!(arb.pending_count("BTCUSDT"), 0);
    }

    #[test]
    fn reset_clears_all_symbols() {
        let mut arb = ChecksumArbiter::default();
        arb.ingest(&active("BTCUSDT", 1, 0xAA));
        arb.ingest(&active("ETHUSDT", 1, 0xBB));
        arb.reset();
        assert_eq!(arb.symbol_count(), 0);
        assert_eq!(arb.entry_count(), 0);
    }

    #[test]
    fn after_reset_symbol_can_be_reused() {
        let mut arb = ChecksumArbiter::default();
        arb.ingest(&active("BTCUSDT", 1, 0xAA));
        arb.reset_symbol("BTCUSDT");

        // Start fresh — same update_id should not appear Stale.
        let v = arb.ingest(&active("BTCUSDT", 1, 0xAA));
        assert!(v.is_pending());
    }

    // -----------------------------------------------------------------------
    // pending_count
    // -----------------------------------------------------------------------

    #[test]
    fn pending_count_tracks_unpaired_entries() {
        let mut arb = ChecksumArbiter::default();
        arb.ingest(&active("BTCUSDT", 1, 0xAA));
        arb.ingest(&active("BTCUSDT", 2, 0xBB));
        assert_eq!(arb.pending_count("BTCUSDT"), 2);

        arb.ingest(&passive("BTCUSDT", 1, 0xAA));
        assert_eq!(arb.pending_count("BTCUSDT"), 1);
    }

    #[test]
    fn pending_count_zero_for_unknown_symbol() {
        let arb = ChecksumArbiter::default();
        assert_eq!(arb.pending_count("UNKNOWN"), 0);
    }

    // -----------------------------------------------------------------------
    // FailoverTrigger / process()
    // -----------------------------------------------------------------------

    #[test]
    fn process_does_not_call_trigger_on_pending() {
        let mut arb     = ChecksumArbiter::default();
        let trigger     = RecordingTrigger::new();
        process(&mut arb, &active("BTCUSDT", 1, 0xAA), &trigger);
        assert_eq!(trigger.call_count(), 0);
    }

    #[test]
    fn process_does_not_call_trigger_on_matched() {
        let mut arb     = ChecksumArbiter::default();
        let trigger     = RecordingTrigger::new();
        process(&mut arb, &active("BTCUSDT", 1, 0xAA), &trigger);
        process(&mut arb, &passive("BTCUSDT", 1, 0xAA), &trigger);
        assert_eq!(trigger.call_count(), 0);
    }

    #[test]
    fn process_calls_trigger_on_diverged() {
        let mut arb = ChecksumArbiter::default();
        let trigger = RecordingTrigger::new();

        process(&mut arb, &active("BTCUSDT", 1, 0xAA), &trigger);
        process(&mut arb, &passive("BTCUSDT", 1, 0xBB), &trigger);

        assert_eq!(trigger.call_count(), 1);
        let (sym, uid, a, p) = &trigger.calls.borrow()[0];
        assert_eq!(sym, "BTCUSDT");
        assert_eq!(*uid, 1);
        assert_eq!(*a, 0xAA);
        assert_eq!(*p, 0xBB);
    }

    #[test]
    fn process_calls_trigger_once_per_divergence_not_on_repeat_ingestion() {
        let mut arb = ChecksumArbiter::default();
        let trigger = RecordingTrigger::new();

        // Same update_id diverges on second ingest.
        process(&mut arb, &active("BTCUSDT", 1, 0xAA), &trigger);
        process(&mut arb, &passive("BTCUSDT", 1, 0xBB), &trigger);
        assert_eq!(trigger.call_count(), 1);

        // A new update_id that diverges should trigger again.
        process(&mut arb, &active("BTCUSDT", 2, 0xCC), &trigger);
        process(&mut arb, &passive("BTCUSDT", 2, 0xDD), &trigger);
        assert_eq!(trigger.call_count(), 2);
    }

    #[test]
    fn log_only_trigger_does_not_panic() {
        let t = LogOnlyTrigger;
        // Just verify it doesn't panic (tracing is a no-op without a subscriber).
        t.on_divergence("BTCUSDT", 42, 0xAA, 0xBB);
    }

    // -----------------------------------------------------------------------
    // ArbiterVerdict predicate methods
    // -----------------------------------------------------------------------

    #[test]
    fn verdict_predicates() {
        let matched = ArbiterVerdict::Matched { symbol: "S".into(), update_id: 1, checksum: 0 };
        let diverged = ArbiterVerdict::Diverged {
            symbol: "S".into(), update_id: 1,
            active_checksum: 1, passive_checksum: 2,
        };
        assert!( matched.is_matched());
        assert!(!matched.is_diverged());
        assert!(!matched.is_pending());
        assert!(!matched.is_stale());
        assert!(!matched.needs_failover());

        assert!(!diverged.is_matched());
        assert!( diverged.is_diverged());
        assert!( diverged.needs_failover());

        assert!(ArbiterVerdict::Pending.is_pending());
        assert!(ArbiterVerdict::Stale.is_stale());
    }
}
