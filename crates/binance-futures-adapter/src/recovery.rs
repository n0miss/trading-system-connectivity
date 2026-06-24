/// Futures depth recovery procedure (§5.25).
///
/// When the sequence validator detects a gap it transitions to `Stale` and the
/// recovery buffer starts accumulating incoming deltas.  This module provides:
///
/// * [`apply_futures_snapshot`] — sync half; apply a pre-fetched snapshot and
///   replay buffered deltas.  Safe to call while holding `&mut FuturesSymbolState`
///   borrows because the async fetch happens outside this function.
/// * [`run_futures_recovery`] — convenience async wrapper for single-symbol use.
///
/// # Replay difference vs Spot
///
/// The Spot replay calls `validator.validate(first, final_)`.
/// Futures replay calls `validator.validate(first, final_, prev_final)` where
/// `prev_final` is `delta.prev_update_id` — set to the `pu` field by the
/// normalizer (§5.22).  In Bridging state `prev_final` is ignored; it only
/// matters once the validator transitions to Active.
use connector_core::InstrumentDefinition;
use connector_order_book::OrderBook;
use connector_refdata::{RefDataError, RestClient};
use thiserror::Error;

use crate::recovery_buffer::RecoveryBuffer;
use crate::sequence::{FuturesSequenceValidator, ValidateResult};

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

#[derive(Debug, Error)]
pub enum RecoveryError {
    #[error("REST depth snapshot fetch failed: {0}")]
    SnapshotFetch(#[from] RefDataError),

    #[error("gap in replay buffer: expected pu {expected_pu}, received {actual_pu}")]
    GapInReplay { expected_pu: u64, actual_pu: u64 },
}

#[derive(Debug, Clone, Copy)]
pub struct RecoveryOutcome {
    pub snapshot_id: u64,
    pub replayed: usize,
    pub discarded: usize,
}

// ---------------------------------------------------------------------------
// Sync half
// ---------------------------------------------------------------------------

/// Apply a pre-fetched REST depth snapshot to `book`, reinitialise `validator`,
/// and replay any buffered deltas whose `final_update_id > snapshot.update_id`.
pub fn apply_futures_snapshot(
    snapshot: &connector_core::BookSnapshot,
    book: &mut OrderBook,
    validator: &mut FuturesSequenceValidator,
    recovery_buf: &mut RecoveryBuffer,
) -> Result<RecoveryOutcome, RecoveryError> {
    let snapshot_id = snapshot.update_id;

    book.apply_snapshot(snapshot);
    validator.on_snapshot(snapshot_id);

    let candidates = recovery_buf.drain_after(snapshot_id);

    let mut replayed = 0_usize;
    let mut discarded = 0_usize;

    for buffered in candidates {
        let first = buffered.delta.first_update_id;
        let final_ = buffered.delta.final_update_id;
        let prev_final = buffered.delta.prev_update_id; // = pu, set by normalizer

        match validator.validate(first, final_, prev_final) {
            ValidateResult::Apply => {
                book.apply_delta(&buffered.delta);
                replayed += 1;
            }
            ValidateResult::Discard => {
                discarded += 1;
            }
            ValidateResult::Gap {
                expected_pu,
                actual_pu,
                ..
            } => {
                return Err(RecoveryError::GapInReplay {
                    expected_pu,
                    actual_pu,
                });
            }
            ValidateResult::Buffering => {
                discarded += 1;
            }
        }
    }

    book.mark_recovered();

    Ok(RecoveryOutcome {
        snapshot_id,
        replayed,
        discarded,
    })
}

// ---------------------------------------------------------------------------
// Async convenience wrapper
// ---------------------------------------------------------------------------

pub async fn run_futures_recovery(
    rest: &RestClient,
    inst: &InstrumentDefinition,
    recv_ts: i64,
    book: &mut OrderBook,
    validator: &mut FuturesSequenceValidator,
    recovery_buf: &mut RecoveryBuffer,
) -> Result<RecoveryOutcome, RecoveryError> {
    let snapshot = rest.fetch_futures_depth_snapshot(inst, recv_ts).await?;
    apply_futures_snapshot(&snapshot, book, validator, recovery_buf)
}
