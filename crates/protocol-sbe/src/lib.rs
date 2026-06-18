/// Binance Spot SBE decoder.
///
/// # Schema
///
/// The official Binance Spot SBE XML schema is vendored at
/// `schemas/spot_prod.xml` (schema ID 3, initial version 0).  This module
/// implements hand-written Rust decoders that faithfully reflect the schema
/// without requiring the SBE code-generation tool-chain at build time.
///
/// # Startup validation
///
/// Call [`check_spot_schema`] on the first frame received from a new Binance
/// Spot SBE WebSocket connection.  It decodes the 8-byte SBE header, verifies
/// `schema_id == 3` and `version <= SPOT_SCHEMA_VERSION_MAX`, and returns the
/// [`TemplateId`] so the caller can immediately route to the correct decoder.
///
/// # Message decoders (Stage 7.29)
///
/// Per-message decoders for `TradesStreamEvent`, `BestBidAskStreamEvent`,
/// `DepthSnapshotStreamEvent`, and `DepthDiffStreamEvent` are added in §7.29.
pub mod decimal;
pub mod error;
pub mod header;
pub mod template;

pub use decimal::Decimal64;
pub use error::SbeError;
pub use header::{SbeHeader, SBE_HEADER_SIZE, SPOT_SCHEMA_ID, SPOT_SCHEMA_VERSION_MAX};
pub use template::TemplateId;

// ---------------------------------------------------------------------------
// Top-level entry point
// ---------------------------------------------------------------------------

/// Decode the SBE message header, validate schema identity and version, and
/// return the [`TemplateId`] for routing.
///
/// This is the function to call **once per connection** at startup (§7.28).
/// Callers may also call it on every message during development to detect
/// unexpected schema changes, at the cost of a redundant schema check per
/// frame.
///
/// # Errors
///
/// - [`SbeError::BufferTooShort`]     — fewer than 8 bytes in `buf`
/// - [`SbeError::SchemaMismatch`]     — `schema_id` ≠ 3
/// - [`SbeError::VersionTooNew`]      — `version` > [`SPOT_SCHEMA_VERSION_MAX`]
/// - [`SbeError::UnknownTemplateId`]  — `template_id` not in {0,1,2,3}
pub fn check_spot_schema(buf: &[u8]) -> Result<TemplateId, SbeError> {
    let hdr = SbeHeader::decode(buf)?;
    hdr.validate_schema()?;
    TemplateId::from_u16(hdr.template_id)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn make_frame(block_length: u16, template_id: u16, schema_id: u16, version: u16) -> [u8; 16] {
        let mut buf = [0xAAu8; 16]; // fill with garbage after header
        buf[0..2].copy_from_slice(&block_length.to_le_bytes());
        buf[2..4].copy_from_slice(&template_id.to_le_bytes());
        buf[4..6].copy_from_slice(&schema_id.to_le_bytes());
        buf[6..8].copy_from_slice(&version.to_le_bytes());
        buf
    }

    #[test]
    fn check_spot_schema_valid_trades_event() {
        let buf = make_frame(58, 0, SPOT_SCHEMA_ID, 0);
        let tid = check_spot_schema(&buf).unwrap();
        assert_eq!(tid, TemplateId::TradesStream);
    }

    #[test]
    fn check_spot_schema_valid_bbo_event() {
        let buf = make_frame(48, 1, SPOT_SCHEMA_ID, 1);
        let tid = check_spot_schema(&buf).unwrap();
        assert_eq!(tid, TemplateId::BestBidAsk);
    }

    #[test]
    fn check_spot_schema_valid_depth_snapshot() {
        let buf = make_frame(16, 2, SPOT_SCHEMA_ID, 0);
        let tid = check_spot_schema(&buf).unwrap();
        assert_eq!(tid, TemplateId::DepthSnapshot);
    }

    #[test]
    fn check_spot_schema_valid_depth_diff() {
        let buf = make_frame(40, 3, SPOT_SCHEMA_ID, 0);
        let tid = check_spot_schema(&buf).unwrap();
        assert_eq!(tid, TemplateId::DepthDiff);
    }

    #[test]
    fn check_spot_schema_wrong_schema_id_errors() {
        let buf = make_frame(58, 0, 1, 0); // schema_id=1 (not Spot)
        let err = check_spot_schema(&buf).unwrap_err();
        assert!(matches!(err, SbeError::SchemaMismatch { expected: 3, actual: 1 }));
    }

    #[test]
    fn check_spot_schema_version_too_new_errors() {
        let buf = make_frame(58, 0, SPOT_SCHEMA_ID, SPOT_SCHEMA_VERSION_MAX + 1);
        let err = check_spot_schema(&buf).unwrap_err();
        assert!(matches!(err, SbeError::VersionTooNew { .. }));
    }

    #[test]
    fn check_spot_schema_unknown_template_errors() {
        let buf = make_frame(0, 99, SPOT_SCHEMA_ID, 0);
        let err = check_spot_schema(&buf).unwrap_err();
        assert_eq!(err, SbeError::UnknownTemplateId(99));
    }

    #[test]
    fn check_spot_schema_buffer_too_short_errors() {
        let err = check_spot_schema(&[0u8; 7]).unwrap_err();
        assert!(matches!(err, SbeError::BufferTooShort { needed: 8, .. }));
    }
}
