/// Spot depth recovery procedure (§3.14).
///
/// When the sequence validator detects a gap it transitions to `Stale` and
/// the recovery buffer starts accumulating incoming deltas.  This module
/// provides [`run_spot_recovery`], which:
///
/// 1. Fetches a fresh REST depth snapshot.
/// 2. Applies it to the order book.
/// 3. Re-initialises the sequence validator with the snapshot's `lastUpdateId`.
/// 4. Drains the recovery buffer, discarding events that predate the snapshot.
/// 5. Replays each surviving event through the validator onto the book.
/// 6. On success, marks the book recovered.
///
/// If a gap is found in the replay buffer the function returns
/// [`RecoveryError::GapInReplay`] — the caller should handle this per
/// Stage 3.15 (circuit breaker / DEGRADED).

use connector_core::InstrumentDefinition;
use connector_order_book::OrderBook;
use connector_refdata::{RefDataError, RestClient};
use thiserror::Error;

use crate::recovery_buffer::RecoveryBuffer;
use crate::sequence::{SequenceValidator, ValidateResult};

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

#[derive(Debug, Error)]
pub enum RecoveryError {
    #[error("REST depth snapshot fetch failed: {0}")]
    SnapshotFetch(#[from] RefDataError),

    #[error("gap in replay buffer: expected first_update_id {expected}, received {actual}")]
    GapInReplay { expected: u64, actual: u64 },
}

/// Summary returned by a successful [`run_spot_recovery`] call.
#[derive(Debug, Clone, Copy)]
pub struct RecoveryOutcome {
    /// The `lastUpdateId` from the REST snapshot that was applied to the book.
    pub snapshot_id: u64,
    /// Number of buffered deltas replayed and applied onto the book.
    pub replayed:    usize,
    /// Number of buffered deltas discarded (validator said Discard).
    pub discarded:   usize,
}

// ---------------------------------------------------------------------------
// Recovery procedure
// ---------------------------------------------------------------------------

/// Execute the Spot depth recovery procedure.
///
/// # Arguments
///
/// * `rest`         — REST client pointed at the Binance base URL.
/// * `inst`         — Instrument definition (symbol, scales, IDs).
/// * `recv_ts`      — Nanosecond timestamp to stamp the snapshot header.
/// * `book`         — Mutable reference to the order book (currently stale).
/// * `validator`    — Mutable reference to the sequence validator (currently Stale).
/// * `recovery_buf` — Mutable reference to the recovery buffer.
///
/// # Returns
///
/// `Ok(RecoveryOutcome)` when the book is fully recovered.
/// `Err(RecoveryError)` when the snapshot fetch fails or a gap is found in
/// the replay buffer — the caller must handle the DEGRADED path (Stage 3.15).
pub async fn run_spot_recovery(
    rest:         &RestClient,
    inst:         &InstrumentDefinition,
    recv_ts:      i64,
    book:         &mut OrderBook,
    validator:    &mut SequenceValidator,
    recovery_buf: &mut RecoveryBuffer,
) -> Result<RecoveryOutcome, RecoveryError> {
    // 1. Fetch the REST depth snapshot.
    let snapshot    = rest.fetch_spot_depth_snapshot(inst, recv_ts).await?;
    let snapshot_id = snapshot.update_id;

    // 2. Apply snapshot to the order book and reinitialise the validator.
    //    The book now contains valid level data regardless of what happens next.
    book.apply_snapshot(&snapshot);
    validator.on_snapshot(snapshot_id);

    // 3. Drain the recovery buffer, discarding events that predate the snapshot.
    //    drain_after returns only events where final_update_id > snapshot_id.
    let candidates = recovery_buf.drain_after(snapshot_id);

    // 4. Replay surviving events through the sequence validator.
    let mut replayed = 0_usize;
    let mut discarded = 0_usize;

    for buffered in candidates {
        let first  = buffered.delta.first_update_id;
        let final_ = buffered.delta.final_update_id;

        match validator.validate(first, final_) {
            ValidateResult::Apply => {
                book.apply_delta(&buffered.delta);
                replayed += 1;
            }
            ValidateResult::Discard => {
                discarded += 1;
            }
            ValidateResult::Gap { expected, actual, .. } => {
                // A gap in the replay buffer means events were lost even while
                // buffering.  The caller must transition to DEGRADED (Stage 3.15).
                return Err(RecoveryError::GapInReplay { expected, actual });
            }
            ValidateResult::Buffering => {
                // Validator is in Bridging/Active/Stale after on_snapshot; Buffering
                // can only occur from Stale — this indicates a logic error.
                // Treat as discard and continue; the gap path would have fired instead.
                discarded += 1;
            }
        }
    }

    // 5. Book is now in sync: clear the stale flag.
    book.mark_recovered();

    Ok(RecoveryOutcome { snapshot_id, replayed, discarded })
}
