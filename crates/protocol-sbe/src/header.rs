use crate::error::SbeError;

/// Schema ID of the Binance Spot SBE schema (field `id` in the XML).
pub const SPOT_SCHEMA_ID: u16 = 3;

/// Maximum schema version this decoder supports.
///
/// Binance increments `version` when backward-compatible fields are added.
/// Messages with a higher version number may contain fields we do not
/// understand; reject them rather than silently ignore unknown data.
pub const SPOT_SCHEMA_VERSION_MAX: u16 = 2;

/// Byte size of the SBE message header on the wire.
pub const SBE_HEADER_SIZE: usize = 8;

// ---------------------------------------------------------------------------
// SbeHeader
// ---------------------------------------------------------------------------

/// Standard SBE message header (8 bytes, little-endian).
///
/// Every SBE message starts with this 8-byte composite.  The decoder reads
/// `block_length` to know how many bytes the root block occupies, dispatches
/// on `template_id` to select the right message decoder, and validates
/// `schema_id` / `version` at connection startup (§7.28).
///
/// Wire layout:
/// ```text
/// offset 0 : blockLength  uint16
/// offset 2 : templateId   uint16
/// offset 4 : schemaId     uint16
/// offset 6 : version      uint16
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SbeHeader {
    /// Byte count of the root (fixed-size) block that follows the header.
    pub block_length: u16,
    /// Identifies which message type this frame carries.
    pub template_id: u16,
    /// Must equal [`SPOT_SCHEMA_ID`] for Binance Spot messages.
    pub schema_id: u16,
    /// Schema revision; must be ≤ [`SPOT_SCHEMA_VERSION_MAX`].
    pub version: u16,
}

impl SbeHeader {
    /// Decode the 8-byte SBE header from the start of `buf`.
    ///
    /// Returns `Err(BufferTooShort)` when fewer than 8 bytes are available.
    pub fn decode(buf: &[u8]) -> Result<Self, SbeError> {
        if buf.len() < SBE_HEADER_SIZE {
            return Err(SbeError::BufferTooShort {
                needed: SBE_HEADER_SIZE,
                have: buf.len(),
            });
        }
        Ok(Self {
            block_length: u16::from_le_bytes([buf[0], buf[1]]),
            template_id: u16::from_le_bytes([buf[2], buf[3]]),
            schema_id: u16::from_le_bytes([buf[4], buf[5]]),
            version: u16::from_le_bytes([buf[6], buf[7]]),
        })
    }

    /// Validate that this header carries a Binance Spot SBE message of a
    /// supported schema version.
    ///
    /// Called once per connection at startup (§7.28) and optionally on every
    /// message during development.
    ///
    /// # Errors
    ///
    /// - [`SbeError::SchemaMismatch`]  — `schema_id` ≠ [`SPOT_SCHEMA_ID`]
    /// - [`SbeError::VersionTooNew`]   — `version` > [`SPOT_SCHEMA_VERSION_MAX`]
    pub fn validate_schema(&self) -> Result<(), SbeError> {
        if self.schema_id != SPOT_SCHEMA_ID {
            return Err(SbeError::SchemaMismatch {
                expected: SPOT_SCHEMA_ID,
                actual: self.schema_id,
            });
        }
        if self.version > SPOT_SCHEMA_VERSION_MAX {
            return Err(SbeError::VersionTooNew {
                max: SPOT_SCHEMA_VERSION_MAX,
                actual: self.version,
            });
        }
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    /// Build a valid 8-byte header buffer.
    fn make_header_bytes(
        block_length: u16,
        template_id: u16,
        schema_id: u16,
        version: u16,
    ) -> [u8; 8] {
        let mut buf = [0u8; 8];
        buf[0..2].copy_from_slice(&block_length.to_le_bytes());
        buf[2..4].copy_from_slice(&template_id.to_le_bytes());
        buf[4..6].copy_from_slice(&schema_id.to_le_bytes());
        buf[6..8].copy_from_slice(&version.to_le_bytes());
        buf
    }

    fn valid_header_bytes() -> [u8; 8] {
        make_header_bytes(58, 0, SPOT_SCHEMA_ID, 0)
    }

    // --- decode ---

    #[test]
    fn decode_correct_fields() {
        let buf = make_header_bytes(58, 1, SPOT_SCHEMA_ID, 0);
        let hdr = SbeHeader::decode(&buf).unwrap();
        assert_eq!(hdr.block_length, 58);
        assert_eq!(hdr.template_id, 1);
        assert_eq!(hdr.schema_id, SPOT_SCHEMA_ID);
        assert_eq!(hdr.version, 0);
    }

    #[test]
    fn decode_little_endian_multi_byte_values() {
        // block_length = 0x0200 = 512 in little-endian: bytes [0x00, 0x02]
        let buf = make_header_bytes(512, 3, SPOT_SCHEMA_ID, 1);
        let hdr = SbeHeader::decode(&buf).unwrap();
        assert_eq!(hdr.block_length, 512);
        assert_eq!(hdr.template_id, 3);
        assert_eq!(hdr.version, 1);
    }

    #[test]
    fn decode_buffer_too_short_returns_error() {
        let buf = [0u8; 7]; // one byte short
        let err = SbeHeader::decode(&buf).unwrap_err();
        assert_eq!(err, SbeError::BufferTooShort { needed: 8, have: 7 });
    }

    #[test]
    fn decode_empty_buffer_returns_error() {
        let err = SbeHeader::decode(&[]).unwrap_err();
        assert_eq!(err, SbeError::BufferTooShort { needed: 8, have: 0 });
    }

    #[test]
    fn decode_ignores_bytes_after_header() {
        let mut buf = [0xFFu8; 64];
        buf[0..8].copy_from_slice(&valid_header_bytes());
        let hdr = SbeHeader::decode(&buf).unwrap();
        assert_eq!(hdr.schema_id, SPOT_SCHEMA_ID);
    }

    // --- validate_schema ---

    #[test]
    fn validate_schema_correct_schema_and_version_ok() {
        let hdr = SbeHeader {
            block_length: 58,
            template_id: 0,
            schema_id: SPOT_SCHEMA_ID,
            version: 0,
        };
        assert!(hdr.validate_schema().is_ok());
    }

    #[test]
    fn validate_schema_max_supported_version_ok() {
        let hdr = SbeHeader {
            block_length: 0,
            template_id: 0,
            schema_id: SPOT_SCHEMA_ID,
            version: SPOT_SCHEMA_VERSION_MAX,
        };
        assert!(hdr.validate_schema().is_ok());
    }

    #[test]
    fn validate_schema_wrong_schema_id_errors() {
        let hdr = SbeHeader {
            block_length: 0,
            template_id: 0,
            schema_id: 99,
            version: 0,
        };
        let err = hdr.validate_schema().unwrap_err();
        assert_eq!(
            err,
            SbeError::SchemaMismatch {
                expected: SPOT_SCHEMA_ID,
                actual: 99
            }
        );
    }

    #[test]
    fn validate_schema_version_too_new_errors() {
        let hdr = SbeHeader {
            block_length: 0,
            template_id: 0,
            schema_id: SPOT_SCHEMA_ID,
            version: SPOT_SCHEMA_VERSION_MAX + 1,
        };
        let err = hdr.validate_schema().unwrap_err();
        assert_eq!(
            err,
            SbeError::VersionTooNew {
                max: SPOT_SCHEMA_VERSION_MAX,
                actual: SPOT_SCHEMA_VERSION_MAX + 1,
            }
        );
    }

    #[test]
    fn validate_schema_schema_id_zero_errors() {
        let hdr = SbeHeader {
            block_length: 0,
            template_id: 0,
            schema_id: 0,
            version: 0,
        };
        assert!(matches!(
            hdr.validate_schema(),
            Err(SbeError::SchemaMismatch { .. })
        ));
    }
}
