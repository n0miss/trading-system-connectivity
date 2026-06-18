/// The core replayer: drives a sequence of [`RecordedFrame`]s according to a
/// [`ReplayMode`] and returns [`PollResult`]s to the caller.

use std::time::{SystemTime, UNIX_EPOCH};

use crate::{
    frame::{RecordedFrame, SourceKind},
    mode::{Prng, ReplayMode},
};

// ---------------------------------------------------------------------------
// now_nanos
// ---------------------------------------------------------------------------

pub(crate) fn now_nanos() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos() as i64
}

// ---------------------------------------------------------------------------
// ReplayStats
// ---------------------------------------------------------------------------

/// Aggregate counters produced during a replay session.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ReplayStats {
    /// Frames delivered to the caller (originals, not counting duplicates).
    pub frames_delivered: u64,
    /// Frames silently dropped by the fault injector.
    pub frames_dropped: u64,
    /// Frames whose payload was byte-flipped by the fault injector.
    pub frames_corrupted: u64,
    /// Extra duplicate frames emitted by the fault injector.
    pub frames_duplicated: u64,
    /// Cumulative nanoseconds spent waiting across all `NotYet` polls.
    /// Populated by callers that sleep between polls (not by the replayer itself).
    pub total_wait_ns: i64,
}

// ---------------------------------------------------------------------------
// ReplayEvent
// ---------------------------------------------------------------------------

/// A frame that has passed the timing gate, ready for the caller to consume.
#[derive(Debug, Clone)]
pub struct ReplayEvent {
    /// How the payload should be decoded.
    pub source_kind: SourceKind,
    /// The payload bytes — may differ from the original if `was_corrupted`.
    pub payload: Vec<u8>,
    /// Replay-local timestamp in nanoseconds.
    ///
    /// * For `AsFastAsPossible` / `OriginalTiming` / `Scaled`: equals
    ///   `RecordedFrame::captured_at_ns`.
    /// * For `Deterministic`: a virtual clock that advances by the recorded
    ///   inter-frame delta, starting from the first frame's `captured_at_ns`.
    pub virtual_ts_ns: i64,
    /// `true` if the fault injector flipped one byte in `payload`.
    pub was_corrupted: bool,
    /// `true` if this is an injected duplicate of a previously delivered frame.
    pub is_duplicate: bool,
}

// ---------------------------------------------------------------------------
// PollResult
// ---------------------------------------------------------------------------

/// Returned by [`Replayer::next_frame_at`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PollResult {
    /// A frame is ready for consumption.
    Ready(ReplayEvent),
    /// The next frame's scheduled time has not arrived yet.
    /// The caller should retry after at least `wait_ns` nanoseconds.
    NotYet { wait_ns: i64 },
    /// All frames have been replayed.
    Done,
}

impl PollResult {
    pub fn is_ready(&self)   -> bool { matches!(self, Self::Ready(_))    }
    pub fn is_not_yet(&self) -> bool { matches!(self, Self::NotYet { .. }) }
    pub fn is_done(&self)    -> bool { matches!(self, Self::Done)         }

    pub fn unwrap_event(self) -> ReplayEvent {
        match self {
            Self::Ready(e) => e,
            other => panic!("called unwrap_event on {:?}", other),
        }
    }
}

impl PartialEq for ReplayEvent {
    fn eq(&self, other: &Self) -> bool {
        self.source_kind    == other.source_kind
            && self.payload      == other.payload
            && self.virtual_ts_ns == other.virtual_ts_ns
            && self.was_corrupted == other.was_corrupted
            && self.is_duplicate  == other.is_duplicate
    }
}
impl Eq for ReplayEvent {}

// ---------------------------------------------------------------------------
// Replayer
// ---------------------------------------------------------------------------

/// Drives a [`Vec<RecordedFrame>`] according to a [`ReplayMode`].
///
/// # Injected-clock API
///
/// The primary entry point is [`next_frame_at(now_ns)`][Replayer::next_frame_at],
/// which takes an externally supplied timestamp.  This makes the replayer
/// fully deterministic in tests without any wall-clock dependency.  The
/// convenience wrapper [`next_frame()`][Replayer::next_frame] reads the
/// system clock and calls `next_frame_at`.
///
/// # Example
///
/// ```rust
/// use connector_replay::{
///     frame::RecordedFrame,
///     mode::ReplayMode,
///     replayer::{PollResult, Replayer},
/// };
///
/// let frames = vec![
///     RecordedFrame::raw_ws(1_000_000_000, b"msg-0".to_vec()),
///     RecordedFrame::raw_ws(1_000_001_000, b"msg-1".to_vec()),
/// ];
///
/// let mut r = Replayer::new(frames, ReplayMode::AsFastAsPossible);
/// assert!(r.next_frame_at(0).is_ready());
/// assert!(r.next_frame_at(0).is_ready());
/// assert!(r.next_frame_at(0).is_done());
/// assert_eq!(r.stats().frames_delivered, 2);
/// ```
pub struct Replayer {
    frames:           Vec<RecordedFrame>,
    cursor:           usize,
    mode:             ReplayMode,
    /// Real time at which the first frame was emitted (for OriginalTiming/Scaled).
    start_real_ns:    Option<i64>,
    /// `captured_at_ns` of the first frame — the virtual-time origin.
    start_virtual_ns: i64,
    /// Virtual clock for Deterministic mode.
    virtual_clock_ns: i64,
    /// Buffered duplicate waiting to be returned on the next poll.
    pending_duplicate: Option<ReplayEvent>,
    /// PRNG for fault injection (seeded from `FaultConfig::seed`).
    prng:             Prng,
    pub stats:        ReplayStats,
}

impl Replayer {
    /// Create a new replayer.
    ///
    /// `frames` must be ordered by `captured_at_ns` for `OriginalTiming` and
    /// `Scaled` modes.  `AsFastAsPossible` and `Deterministic` are order-agnostic.
    pub fn new(frames: Vec<RecordedFrame>, mode: ReplayMode) -> Self {
        let seed             = mode.fault_config().map(|fc| fc.seed).unwrap_or(0);
        let start_virtual_ns = frames.first().map(|f| f.captured_at_ns).unwrap_or(0);
        Self {
            virtual_clock_ns: start_virtual_ns,
            start_virtual_ns,
            cursor: 0,
            start_real_ns: None,
            pending_duplicate: None,
            prng: Prng::new(seed),
            stats: ReplayStats::default(),
            frames,
            mode,
        }
    }

    // ------------------------------------------------------------------
    // Public API
    // ------------------------------------------------------------------

    /// Poll for the next frame, using `now_ns` as the current wall-clock time.
    ///
    /// In `Deterministic` mode `now_ns` is ignored.
    pub fn next_frame_at(&mut self, now_ns: i64) -> PollResult {
        // Return a buffered duplicate before advancing the cursor.
        if let Some(dup) = self.pending_duplicate.take() {
            self.stats.frames_duplicated += 1;
            return PollResult::Ready(dup);
        }

        loop {
            if self.cursor >= self.frames.len() {
                return PollResult::Done;
            }

            let captured_at = self.frames[self.cursor].captured_at_ns;

            // --- Timing gate ---
            if let Some(wait) = self.timing_wait_ns(now_ns, captured_at) {
                return PollResult::NotYet { wait_ns: wait };
            }

            // --- Drop ---
            let (drop_pct, corrupt_pct, dup_pct) = self.fault_percents();
            if self.prng.percent_chance(drop_pct) {
                self.stats.frames_dropped += 1;
                self.cursor += 1;
                continue; // try next frame (same now_ns — AsFastAsPossible semantics for drops)
            }

            // --- Build payload (copy so fault injection can mutate) ---
            let mut payload = self.frames[self.cursor].payload.clone();
            let mut was_corrupted = false;

            if self.prng.percent_chance(corrupt_pct) && !payload.is_empty() {
                let byte_idx = self.prng.next() as usize % payload.len();
                payload[byte_idx] ^= 0xFF;
                was_corrupted = true;
                self.stats.frames_corrupted += 1;
            }

            // --- Virtual timestamp ---
            let virtual_ts_ns = self.advance_virtual_clock(captured_at);

            let source_kind = self.frames[self.cursor].source_kind;
            self.cursor += 1;

            let event = ReplayEvent {
                source_kind,
                payload,
                virtual_ts_ns,
                was_corrupted,
                is_duplicate: false,
            };

            // --- Duplicate ---
            if self.prng.percent_chance(dup_pct) {
                self.pending_duplicate = Some(ReplayEvent { is_duplicate: true, ..event.clone() });
            }

            self.stats.frames_delivered += 1;
            return PollResult::Ready(event);
        }
    }

    /// Poll for the next frame using the real wall clock.
    pub fn next_frame(&mut self) -> PollResult {
        self.next_frame_at(now_nanos())
    }

    /// `true` when all frames have been consumed and no duplicate is pending.
    pub fn is_done(&self) -> bool {
        self.pending_duplicate.is_none() && self.cursor >= self.frames.len()
    }

    /// Reset to the beginning.  Stats are cleared and timing state is reset.
    pub fn reset(&mut self) {
        self.cursor            = 0;
        self.start_real_ns     = None;
        self.virtual_clock_ns  = self.start_virtual_ns;
        self.pending_duplicate = None;
        self.stats             = ReplayStats::default();
        let seed = self.mode.fault_config().map(|fc| fc.seed).unwrap_or(0);
        self.prng = Prng::new(seed);
    }

    /// Total number of frames in the recording.
    pub fn frame_count(&self) -> usize {
        self.frames.len()
    }

    /// Current read position (number of original frames consumed so far).
    pub fn cursor(&self) -> usize {
        self.cursor
    }

    pub fn stats(&self) -> &ReplayStats {
        &self.stats
    }

    // ------------------------------------------------------------------
    // Internal helpers
    // ------------------------------------------------------------------

    /// Returns `Some(wait_ns)` if the frame is not yet due, or `None` if ready.
    fn timing_wait_ns(&mut self, now_ns: i64, frame_captured_at: i64) -> Option<i64> {
        // Extract timing variant without holding a borrow across the mutation below.
        enum T { None, Original, Scaled(u32, u32) }
        let t = match self.mode.base_timing() {
            ReplayMode::AsFastAsPossible => T::None,
            ReplayMode::Deterministic    => T::None,
            ReplayMode::OriginalTiming   => T::Original,
            ReplayMode::Scaled { num, den } => T::Scaled(*num, *den),
            ReplayMode::FaultInjection { .. } => unreachable!("base_timing strips FaultInjection"),
        };

        match t {
            T::None => None,
            T::Original => {
                let start = *self.start_real_ns.get_or_insert(now_ns);
                let offset = frame_captured_at.saturating_sub(self.start_virtual_ns);
                let emit_at = start.saturating_add(offset);
                if now_ns >= emit_at { None } else { Some(emit_at - now_ns) }
            }
            T::Scaled(num, den) => {
                let start = *self.start_real_ns.get_or_insert(now_ns);
                let offset = frame_captured_at.saturating_sub(self.start_virtual_ns);
                let den    = den.max(1) as i64;
                let scaled = offset.saturating_mul(num as i64) / den;
                let emit_at = start.saturating_add(scaled);
                if now_ns >= emit_at { None } else { Some(emit_at - now_ns) }
            }
        }
    }

    /// Advance and return the virtual clock for this frame.
    ///
    /// In `Deterministic` mode the clock advances by the recorded inter-frame
    /// delta.  In all other modes it equals `captured_at_ns` from the frame.
    fn advance_virtual_clock(&mut self, captured_at: i64) -> i64 {
        let is_deterministic = matches!(self.mode.base_timing(), ReplayMode::Deterministic);
        if is_deterministic {
            if self.cursor > 0 {
                let prev = self.frames[self.cursor - 1].captured_at_ns;
                let delta = captured_at.saturating_sub(prev).max(0);
                self.virtual_clock_ns = self.virtual_clock_ns.saturating_add(delta);
            }
            self.virtual_clock_ns
        } else {
            captured_at
        }
    }

    /// Extract fault percentages without holding a borrow.
    fn fault_percents(&self) -> (u8, u8, u8) {
        match self.mode.fault_config() {
            Some(fc) => (fc.drop_percent, fc.corrupt_percent, fc.duplicate_percent),
            None     => (0, 0, 0),
        }
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        frame::{RecordedFrame, SourceKind},
        mode::{FaultConfig, ReplayMode},
    };

    // -----------------------------------------------------------------------
    // Helpers
    // -----------------------------------------------------------------------

    fn frames(n: usize) -> Vec<RecordedFrame> {
        (0..n)
            .map(|i| RecordedFrame::raw_ws(1_000 * i as i64, format!("msg-{i}").into_bytes()))
            .collect()
    }

    fn drain_all(r: &mut Replayer) -> Vec<ReplayEvent> {
        let mut out = vec![];
        loop {
            match r.next_frame_at(i64::MAX) {
                PollResult::Ready(e) => out.push(e),
                PollResult::NotYet { .. } => panic!("unexpected NotYet"),
                PollResult::Done => break,
            }
        }
        out
    }

    // -----------------------------------------------------------------------
    // AsFastAsPossible
    // -----------------------------------------------------------------------

    #[test]
    fn empty_frames_returns_done_immediately() {
        let mut r = Replayer::new(vec![], ReplayMode::AsFastAsPossible);
        assert!(r.next_frame_at(0).is_done());
    }

    #[test]
    fn single_frame_ready_then_done() {
        let mut r = Replayer::new(frames(1), ReplayMode::AsFastAsPossible);
        assert!(r.next_frame_at(0).is_ready());
        assert!(r.next_frame_at(0).is_done());
    }

    #[test]
    fn all_frames_ready_in_order() {
        let mut r = Replayer::new(frames(5), ReplayMode::AsFastAsPossible);
        let events = drain_all(&mut r);
        assert_eq!(events.len(), 5);
        for (i, e) in events.iter().enumerate() {
            assert_eq!(e.payload, format!("msg-{i}").as_bytes());
        }
    }

    #[test]
    fn cursor_advances() {
        let mut r = Replayer::new(frames(3), ReplayMode::AsFastAsPossible);
        assert_eq!(r.cursor(), 0);
        r.next_frame_at(0);
        assert_eq!(r.cursor(), 1);
        r.next_frame_at(0);
        assert_eq!(r.cursor(), 2);
    }

    #[test]
    fn source_kind_preserved() {
        let f = vec![RecordedFrame::normalized(0, b"bytes".to_vec())];
        let mut r = Replayer::new(f, ReplayMode::AsFastAsPossible);
        let e = r.next_frame_at(0).unwrap_event();
        assert_eq!(e.source_kind, SourceKind::NormalizedMessage);
    }

    #[test]
    fn stats_delivered_counts_originals() {
        let mut r = Replayer::new(frames(4), ReplayMode::AsFastAsPossible);
        drain_all(&mut r);
        assert_eq!(r.stats().frames_delivered, 4);
    }

    // -----------------------------------------------------------------------
    // OriginalTiming
    // -----------------------------------------------------------------------

    #[test]
    fn first_frame_always_ready_regardless_of_time() {
        let mut r = Replayer::new(frames(3), ReplayMode::OriginalTiming);
        // First call anchors the start time.
        assert!(r.next_frame_at(0).is_ready());
    }

    #[test]
    fn second_frame_not_yet_before_its_scheduled_time() {
        // frames[0].captured_at=0, frames[1].captured_at=1000
        let mut r = Replayer::new(frames(3), ReplayMode::OriginalTiming);
        r.next_frame_at(0); // anchors start at t=0; emits frame 0
        // Frame 1 is scheduled at offset 1000 ns from start (t=0).
        // now_ns = 500 → not yet.
        let v = r.next_frame_at(500);
        assert!(v.is_not_yet(), "expected NotYet, got {v:?}");
        if let PollResult::NotYet { wait_ns } = v {
            assert_eq!(wait_ns, 500);
        }
    }

    #[test]
    fn second_frame_ready_after_enough_time() {
        let mut r = Replayer::new(frames(3), ReplayMode::OriginalTiming);
        r.next_frame_at(0); // anchor at t=0
        // Frame 1 needs offset 1000; supply now_ns = 1000
        assert!(r.next_frame_at(1000).is_ready());
    }

    #[test]
    fn original_timing_virtual_ts_equals_captured_at() {
        let mut r = Replayer::new(frames(2), ReplayMode::OriginalTiming);
        r.next_frame_at(0);
        let e = r.next_frame_at(i64::MAX).unwrap_event();
        assert_eq!(e.virtual_ts_ns, 1_000); // frames[1].captured_at_ns
    }

    // -----------------------------------------------------------------------
    // Scaled
    // -----------------------------------------------------------------------

    #[test]
    fn scaled_2x_slower_doubles_wait() {
        // Frame 0 at t=0, frame 1 at t=1000.  Scaled 2/1 → effective offset = 2000.
        let mode = ReplayMode::Scaled { num: 2, den: 1 };
        let mut r = Replayer::new(frames(2), mode);
        r.next_frame_at(0); // anchor at 0; emits frame 0

        // At now=1000, frame 1 is not yet due (scaled offset = 2000).
        let v = r.next_frame_at(1000);
        assert!(v.is_not_yet(), "expected NotYet at 1000 with 2x scale");
        if let PollResult::NotYet { wait_ns } = v {
            assert_eq!(wait_ns, 1000);
        }

        // At now=2000, it's due.
        assert!(r.next_frame_at(2000).is_ready());
    }

    #[test]
    fn scaled_2x_faster_halves_wait() {
        let mode = ReplayMode::Scaled { num: 1, den: 2 };
        let mut r = Replayer::new(frames(2), mode);
        r.next_frame_at(0); // anchor at 0

        // Frame 1 captured_at=1000, scaled offset = 1000*1/2 = 500.
        // At now=300: not yet.
        assert!(r.next_frame_at(300).is_not_yet());
        // At now=500: ready.
        assert!(r.next_frame_at(500).is_ready());
    }

    #[test]
    fn scaled_den_zero_does_not_panic() {
        let mode = ReplayMode::Scaled { num: 1, den: 0 };
        let mut r = Replayer::new(frames(2), mode);
        r.next_frame_at(0);
        // den clamped to 1; behaves like 1x scale
        let v = r.next_frame_at(500);
        // offset = 1000, so 500 < 1000 → NotYet
        assert!(v.is_not_yet());
    }

    // -----------------------------------------------------------------------
    // Deterministic
    // -----------------------------------------------------------------------

    #[test]
    fn deterministic_always_ready_regardless_of_now() {
        let mut r = Replayer::new(frames(5), ReplayMode::Deterministic);
        for _ in 0..5 {
            assert!(r.next_frame_at(0).is_ready(), "all frames should be immediately ready");
        }
        assert!(r.next_frame_at(0).is_done());
    }

    #[test]
    fn deterministic_virtual_clock_advances_by_deltas() {
        // frames: captured_at = 0, 1000, 3000, 6000
        let f = vec![
            RecordedFrame::raw_ws(0,    b"a".to_vec()),
            RecordedFrame::raw_ws(1000, b"b".to_vec()),
            RecordedFrame::raw_ws(3000, b"c".to_vec()),
            RecordedFrame::raw_ws(6000, b"d".to_vec()),
        ];
        let mut r = Replayer::new(f, ReplayMode::Deterministic);
        let e0 = r.next_frame_at(0).unwrap_event();
        let e1 = r.next_frame_at(0).unwrap_event();
        let e2 = r.next_frame_at(0).unwrap_event();
        let e3 = r.next_frame_at(0).unwrap_event();
        assert_eq!(e0.virtual_ts_ns, 0);
        assert_eq!(e1.virtual_ts_ns, 1000); // +1000
        assert_eq!(e2.virtual_ts_ns, 3000); // +2000
        assert_eq!(e3.virtual_ts_ns, 6000); // +3000
    }

    #[test]
    fn deterministic_two_replays_with_same_frames_produce_same_virtual_clocks() {
        let f = frames(5);
        let mut r1 = Replayer::new(f.clone(), ReplayMode::Deterministic);
        let mut r2 = Replayer::new(f,         ReplayMode::Deterministic);
        let ts1: Vec<_> = drain_all(&mut r1).into_iter().map(|e| e.virtual_ts_ns).collect();
        let ts2: Vec<_> = drain_all(&mut r2).into_iter().map(|e| e.virtual_ts_ns).collect();
        assert_eq!(ts1, ts2);
    }

    // -----------------------------------------------------------------------
    // FaultInjection — drop
    // -----------------------------------------------------------------------

    #[test]
    fn drop_100_percent_skips_all_frames() {
        let fc = FaultConfig { drop_percent: 100, seed: 1, ..Default::default() };
        let mode = ReplayMode::FaultInjection {
            inner:  Box::new(ReplayMode::AsFastAsPossible),
            faults: fc,
        };
        let mut r = Replayer::new(frames(5), mode);
        assert!(r.next_frame_at(0).is_done());
        assert_eq!(r.stats().frames_dropped, 5);
        assert_eq!(r.stats().frames_delivered, 0);
    }

    #[test]
    fn drop_0_percent_keeps_all_frames() {
        let fc = FaultConfig { drop_percent: 0, seed: 1, ..Default::default() };
        let mode = ReplayMode::FaultInjection {
            inner:  Box::new(ReplayMode::AsFastAsPossible),
            faults: fc,
        };
        let mut r = Replayer::new(frames(5), mode);
        let events = drain_all(&mut r);
        assert_eq!(events.len(), 5);
        assert_eq!(r.stats().frames_dropped, 0);
    }

    #[test]
    fn dropped_frames_are_not_in_output() {
        // With seed=1 and drop_percent=100, we should only get Done.
        let fc = FaultConfig { drop_percent: 100, seed: 1, ..Default::default() };
        let mode = ReplayMode::FaultInjection {
            inner:  Box::new(ReplayMode::AsFastAsPossible),
            faults: fc,
        };
        let mut r = Replayer::new(frames(10), mode);
        let mut count = 0;
        loop {
            match r.next_frame_at(0) {
                PollResult::Ready(_) => count += 1,
                PollResult::Done     => break,
                PollResult::NotYet { .. } => panic!("unexpected NotYet"),
            }
        }
        assert_eq!(count, 0);
    }

    // -----------------------------------------------------------------------
    // FaultInjection — corrupt
    // -----------------------------------------------------------------------

    #[test]
    fn corrupt_100_percent_flips_bytes_in_all_frames() {
        let fc = FaultConfig { corrupt_percent: 100, seed: 2, ..Default::default() };
        let mode = ReplayMode::FaultInjection {
            inner:  Box::new(ReplayMode::AsFastAsPossible),
            faults: fc,
        };
        let mut r = Replayer::new(frames(5), mode);
        let events = drain_all(&mut r);
        assert_eq!(events.len(), 5);
        assert!(events.iter().all(|e| e.was_corrupted));
        assert_eq!(r.stats().frames_corrupted, 5);
    }

    #[test]
    fn corrupted_payload_differs_from_original() {
        let original = b"original payload".to_vec();
        let f = vec![RecordedFrame::raw_ws(0, original.clone())];
        let fc = FaultConfig { corrupt_percent: 100, seed: 99, ..Default::default() };
        let mode = ReplayMode::FaultInjection {
            inner:  Box::new(ReplayMode::AsFastAsPossible),
            faults: fc,
        };
        let mut r = Replayer::new(f, mode);
        let e = r.next_frame_at(0).unwrap_event();
        assert!(e.was_corrupted);
        assert_ne!(e.payload, original, "corrupted payload must differ from original");
    }

    #[test]
    fn corrupt_zero_percent_leaves_payloads_unchanged() {
        let fc = FaultConfig { corrupt_percent: 0, seed: 1, ..Default::default() };
        let mode = ReplayMode::FaultInjection {
            inner:  Box::new(ReplayMode::AsFastAsPossible),
            faults: fc,
        };
        let mut r = Replayer::new(frames(5), mode);
        let events = drain_all(&mut r);
        assert!(events.iter().all(|e| !e.was_corrupted));
    }

    // -----------------------------------------------------------------------
    // FaultInjection — duplicate
    // -----------------------------------------------------------------------

    #[test]
    fn duplicate_100_percent_emits_every_frame_twice() {
        let fc = FaultConfig { duplicate_percent: 100, seed: 3, ..Default::default() };
        let mode = ReplayMode::FaultInjection {
            inner:  Box::new(ReplayMode::AsFastAsPossible),
            faults: fc,
        };
        let mut r = Replayer::new(frames(3), mode);
        let events = drain_all(&mut r);
        // 3 originals + 3 duplicates = 6
        assert_eq!(events.len(), 6);
        assert_eq!(r.stats().frames_delivered,  3);
        assert_eq!(r.stats().frames_duplicated, 3);
    }

    #[test]
    fn duplicate_flag_set_on_duplicate_frames() {
        let fc = FaultConfig { duplicate_percent: 100, seed: 3, ..Default::default() };
        let mode = ReplayMode::FaultInjection {
            inner:  Box::new(ReplayMode::AsFastAsPossible),
            faults: fc,
        };
        let mut r = Replayer::new(frames(2), mode);
        let events = drain_all(&mut r);
        // Originals at indices 0, 2; duplicates at 1, 3.
        assert!(!events[0].is_duplicate);
        assert!( events[1].is_duplicate);
        assert!(!events[2].is_duplicate);
        assert!( events[3].is_duplicate);
    }

    #[test]
    fn duplicate_payload_matches_original() {
        let fc = FaultConfig { duplicate_percent: 100, seed: 5, ..Default::default() };
        let mode = ReplayMode::FaultInjection {
            inner:  Box::new(ReplayMode::AsFastAsPossible),
            faults: fc,
        };
        let mut r = Replayer::new(frames(1), mode);
        let orig = r.next_frame_at(0).unwrap_event();
        let dup  = r.next_frame_at(0).unwrap_event();
        assert_eq!(orig.payload, dup.payload);
    }

    // -----------------------------------------------------------------------
    // FaultInjection — composition with timing
    // -----------------------------------------------------------------------

    #[test]
    fn fault_injection_over_original_timing_respects_timing() {
        let fc = FaultConfig::default(); // no faults, just wrapping
        let mode = ReplayMode::FaultInjection {
            inner:  Box::new(ReplayMode::OriginalTiming),
            faults: fc,
        };
        let mut r = Replayer::new(frames(2), mode);
        r.next_frame_at(0); // anchor + emit frame 0
        // Frame 1 at offset 1000 — not ready at now=500
        assert!(r.next_frame_at(500).is_not_yet());
        assert!(r.next_frame_at(1000).is_ready());
    }

    #[test]
    fn fault_injection_over_deterministic_always_ready() {
        let fc = FaultConfig { drop_percent: 0, ..Default::default() };
        let mode = ReplayMode::FaultInjection {
            inner:  Box::new(ReplayMode::Deterministic),
            faults: fc,
        };
        let mut r = Replayer::new(frames(5), mode);
        for _ in 0..5 {
            assert!(r.next_frame_at(0).is_ready());
        }
    }

    // -----------------------------------------------------------------------
    // reset()
    // -----------------------------------------------------------------------

    #[test]
    fn reset_restores_cursor_to_zero() {
        let mut r = Replayer::new(frames(5), ReplayMode::AsFastAsPossible);
        drain_all(&mut r);
        assert_eq!(r.cursor(), 5);
        r.reset();
        assert_eq!(r.cursor(), 0);
    }

    #[test]
    fn reset_clears_stats() {
        let mut r = Replayer::new(frames(5), ReplayMode::AsFastAsPossible);
        drain_all(&mut r);
        assert_eq!(r.stats().frames_delivered, 5);
        r.reset();
        assert_eq!(r.stats().frames_delivered, 0);
    }

    #[test]
    fn reset_replays_frames_from_beginning() {
        let mut r = Replayer::new(frames(3), ReplayMode::AsFastAsPossible);
        let first_run  = drain_all(&mut r);
        r.reset();
        let second_run = drain_all(&mut r);
        assert_eq!(first_run.len(),  3);
        assert_eq!(second_run.len(), 3);
        for (a, b) in first_run.iter().zip(second_run.iter()) {
            assert_eq!(a.payload, b.payload);
        }
    }

    #[test]
    fn reset_produces_same_fault_sequence_with_same_seed() {
        let fc = FaultConfig { drop_percent: 30, seed: 42, ..Default::default() };
        let mode = ReplayMode::FaultInjection {
            inner:  Box::new(ReplayMode::AsFastAsPossible),
            faults: fc,
        };
        let mut r = Replayer::new(frames(20), mode);
        let first_run: Vec<_> = drain_all(&mut r).into_iter().map(|e| e.payload.clone()).collect();
        r.reset();
        let second_run: Vec<_> = drain_all(&mut r).into_iter().map(|e| e.payload.clone()).collect();
        assert_eq!(first_run, second_run, "fault injection with same seed must be fully reproducible");
    }

    #[test]
    fn reset_resets_original_timing_anchor() {
        let mut r = Replayer::new(frames(2), ReplayMode::OriginalTiming);
        r.next_frame_at(0); // anchor at t=0
        r.reset();
        // After reset the anchor is gone; re-anchors at new now_ns.
        r.next_frame_at(5000); // anchor at t=5000
        // Frame 1 offset = 1000; due at 5000+1000 = 6000.
        assert!(r.next_frame_at(5500).is_not_yet());
        assert!(r.next_frame_at(6000).is_ready());
    }

    // -----------------------------------------------------------------------
    // is_done()
    // -----------------------------------------------------------------------

    #[test]
    fn is_done_false_while_frames_remain() {
        let mut r = Replayer::new(frames(2), ReplayMode::AsFastAsPossible);
        assert!(!r.is_done());
        r.next_frame_at(0);
        assert!(!r.is_done());
        r.next_frame_at(0);
        assert!(r.is_done());
    }

    #[test]
    fn is_done_false_while_duplicate_pending() {
        let fc = FaultConfig { duplicate_percent: 100, seed: 1, ..Default::default() };
        let mode = ReplayMode::FaultInjection {
            inner:  Box::new(ReplayMode::AsFastAsPossible),
            faults: fc,
        };
        let mut r = Replayer::new(frames(1), mode);
        r.next_frame_at(0); // emits original + queues duplicate
        // cursor == 1 == len, but duplicate is pending
        assert!(!r.is_done());
        r.next_frame_at(0); // drains duplicate
        assert!(r.is_done());
    }

    // -----------------------------------------------------------------------
    // PollResult predicates
    // -----------------------------------------------------------------------

    #[test]
    fn poll_result_predicates() {
        let ready = PollResult::Ready(ReplayEvent {
            source_kind: SourceKind::RawWsPayload,
            payload: vec![],
            virtual_ts_ns: 0,
            was_corrupted: false,
            is_duplicate: false,
        });
        assert!( ready.is_ready());
        assert!(!ready.is_not_yet());
        assert!(!ready.is_done());

        let not_yet = PollResult::NotYet { wait_ns: 100 };
        assert!(!not_yet.is_ready());
        assert!( not_yet.is_not_yet());
        assert!(!not_yet.is_done());

        assert!(PollResult::Done.is_done());
    }
}
