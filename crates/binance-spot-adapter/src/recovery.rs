/// Spot depth recovery procedure (§3.14 / §4.19).
///
/// When the sequence validator detects a gap it transitions to `Stale` and
/// the recovery buffer starts accumulating incoming deltas.  This module
/// provides two entry points:
///
/// * [`run_spot_recovery`] — convenience async wrapper that fetches a REST
///   snapshot and immediately applies it (single-symbol path).
/// * [`apply_spot_snapshot`] — sync half only; the caller fetches the
///   snapshot itself and holds no borrows across the `await`, which is
///   required for Send safety in per-shard tasks (§4.19).

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

/// Summary returned by a successful recovery call.
#[derive(Debug, Clone, Copy)]
pub struct RecoveryOutcome {
    /// The `lastUpdateId` from the REST snapshot applied to the book.
    pub snapshot_id: u64,
    /// Number of buffered deltas replayed and applied onto the book.
    pub replayed:    usize,
    /// Number of buffered deltas discarded (validator said Discard).
    pub discarded:   usize,
}

// ---------------------------------------------------------------------------
// Sync half — no async, safe to call while holding &mut SymbolState
// ---------------------------------------------------------------------------

/// Apply a pre-fetched REST depth snapshot to `book`, reinitialise `validator`,
/// and replay any buffered deltas whose `final_update_id > snapshot.update_id`.
///
/// Call this after `rest.fetch_spot_depth_snapshot(...).await` completes and
/// before re-acquiring any mutable borrows that were released for the await.
pub fn apply_spot_snapshot(
    snapshot:     &connector_core::BookSnapshot,
    book:         &mut OrderBook,
    validator:    &mut SequenceValidator,
    recovery_buf: &mut RecoveryBuffer,
) -> Result<RecoveryOutcome, RecoveryError> {
    let snapshot_id = snapshot.update_id;

    book.apply_snapshot(snapshot);
    validator.on_snapshot(snapshot_id);

    let candidates = recovery_buf.drain_after(snapshot_id);

    let mut replayed  = 0_usize;
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
                return Err(RecoveryError::GapInReplay { expected, actual });
            }
            ValidateResult::Buffering => {
                // Validator is Active/Bridging after on_snapshot; Buffering here
                // indicates a logic error — treat as discard.
                discarded += 1;
            }
        }
    }

    book.mark_recovered();

    Ok(RecoveryOutcome { snapshot_id, replayed, discarded })
}

// ---------------------------------------------------------------------------
// Async convenience wrapper (single-symbol path)
// ---------------------------------------------------------------------------

/// Execute the full spot depth recovery procedure: fetch snapshot then apply.
///
/// Equivalent to calling `rest.fetch_spot_depth_snapshot` followed by
/// [`apply_spot_snapshot`].  Convenient when there is no Send constraint
/// (i.e., all borrows are local to a single-threaded context).
pub async fn run_spot_recovery(
    rest:         &RestClient,
    inst:         &InstrumentDefinition,
    recv_ts:      i64,
    book:         &mut OrderBook,
    validator:    &mut SequenceValidator,
    recovery_buf: &mut RecoveryBuffer,
) -> Result<RecoveryOutcome, RecoveryError> {
    let snapshot = rest.fetch_spot_depth_snapshot(inst, recv_ts).await?;
    apply_spot_snapshot(&snapshot, book, validator, recovery_buf)
}
