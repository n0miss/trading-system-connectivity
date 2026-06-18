/// Market-data replayer — raw WS payloads, normalized messages, Aeron Archive.
///
/// # Overview
///
/// The replay crate drives a recorded sequence of [`frame::RecordedFrame`]s
/// back through the pipeline.  The caller chooses a
/// [`mode::ReplayMode`] that controls *when* each frame is emitted and
/// *whether* faults are injected.
///
/// ## Source kinds
///
/// | [`frame::SourceKind`]          | Decoder                                  |
/// |--------------------------------|------------------------------------------|
/// | `RawWsPayload`                 | Exchange JSON parser → normalizer         |
/// | `NormalizedMessage`            | `connector_core::NormalizedMessage::from_bytes` |
/// | `AeronArchive`                 | Aeron framing → any of the above         |
///
/// ## Timing modes
///
/// | [`mode::ReplayMode`] variant   | Use-case                                 |
/// |--------------------------------|------------------------------------------|
/// | `AsFastAsPossible`             | Throughput benchmarks, bulk unit tests   |
/// | `OriginalTiming`               | Realistic latency simulation             |
/// | `Scaled { num, den }`          | Accelerated soak / slow-motion debug     |
/// | `Deterministic`                | Property tests, fully reproducible runs  |
/// | `FaultInjection { inner, .. }` | Chaos tests (drop / corrupt / duplicate) |
///
/// ## Injected-clock API
///
/// [`replayer::Replayer::next_frame_at(now_ns)`] accepts an externally
/// supplied wall-clock value, making all timing decisions deterministic in
/// tests without sleeping.  [`replayer::Replayer::next_frame`] is the thin
/// wrapper that reads the real clock.

pub mod frame;
pub mod mode;
pub mod replayer;

// Convenience re-exports so callers don't need to navigate sub-modules.
pub use frame::{RecordedFrame, SourceKind};
pub use mode::{FaultConfig, ReplayMode};
pub use replayer::{PollResult, ReplayEvent, ReplayStats, Replayer};
