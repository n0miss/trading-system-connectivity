use crate::error::SbeError;

/// SBE template IDs for the Binance Spot schema (schema ID = 3).
///
/// Each ID maps to one message type defined in `schemas/spot_prod.xml`.
/// The decoder dispatches on this value after validating the schema header.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u16)]
pub enum TemplateId {
    /// Individual trade event (JSON: `aggTrade` / `trade` streams).
    TradesStream    = 0,
    /// Best bid and ask update (JSON: `bookTicker` stream).
    BestBidAsk      = 1,
    /// Partial order book snapshot (JSON: `<symbol>@depth<N>` stream).
    DepthSnapshot   = 2,
    /// Incremental depth diff update (JSON: `<symbol>@depth@<ms>` stream).
    DepthDiff       = 3,
}

impl TemplateId {
    /// Parse a raw `uint16` template ID from an [`SbeHeader`].
    ///
    /// Returns [`SbeError::UnknownTemplateId`] for any ID not defined in the
    /// current schema.  Unknown IDs must be treated as a fatal error — the
    /// caller cannot safely skip an unknown frame without knowing its length.
    ///
    /// [`SbeHeader`]: crate::header::SbeHeader
    pub fn from_u16(id: u16) -> Result<Self, SbeError> {
        match id {
            0 => Ok(Self::TradesStream),
            1 => Ok(Self::BestBidAsk),
            2 => Ok(Self::DepthSnapshot),
            3 => Ok(Self::DepthDiff),
            _ => Err(SbeError::UnknownTemplateId(id)),
        }
    }

    /// The numeric template ID as it appears on the wire.
    pub fn as_u16(self) -> u16 {
        self as u16
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn all_known_ids_parse_correctly() {
        assert_eq!(TemplateId::from_u16(0).unwrap(), TemplateId::TradesStream);
        assert_eq!(TemplateId::from_u16(1).unwrap(), TemplateId::BestBidAsk);
        assert_eq!(TemplateId::from_u16(2).unwrap(), TemplateId::DepthSnapshot);
        assert_eq!(TemplateId::from_u16(3).unwrap(), TemplateId::DepthDiff);
    }

    #[test]
    fn unknown_id_returns_error() {
        let err = TemplateId::from_u16(4).unwrap_err();
        assert_eq!(err, SbeError::UnknownTemplateId(4));
    }

    #[test]
    fn large_unknown_id_returns_error() {
        assert!(TemplateId::from_u16(u16::MAX).is_err());
    }

    #[test]
    fn as_u16_round_trips() {
        for (raw, variant) in [
            (0u16, TemplateId::TradesStream),
            (1,    TemplateId::BestBidAsk),
            (2,    TemplateId::DepthSnapshot),
            (3,    TemplateId::DepthDiff),
        ] {
            assert_eq!(variant.as_u16(), raw);
            assert_eq!(TemplateId::from_u16(raw).unwrap(), variant);
        }
    }
}
