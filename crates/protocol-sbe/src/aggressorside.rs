/// Aggressor side in a trade as encoded by the Binance Spot SBE schema
/// (`aggressorSide` enum, `encodingType="uint8"`).
///
/// Wire values (from `schemas/spot_prod.xml`):
///   0 → SELL, 1 → BUY, 255 → UNKNOWN
///
/// Values outside {0, 1, 255} are normalised to [`Unknown`] without
/// returning an error; forward-compatible handling lets schema additions
/// that define new values remain decodable.
///
/// [`Unknown`]: AggressorSide::Unknown
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AggressorSide {
    /// The seller is the aggressor (buyer is market maker).
    Sell,
    /// The buyer is the aggressor (seller is market maker).
    Buy,
    /// Value not recognised by this decoder version.
    Unknown,
}

impl AggressorSide {
    /// Decode from the one-byte wire value.
    pub fn from_u8(v: u8) -> Self {
        match v {
            0 => Self::Sell,
            1 => Self::Buy,
            _ => Self::Unknown,
        }
    }

    /// Wire value corresponding to this variant.
    pub fn as_u8(self) -> u8 {
        match self {
            Self::Sell    => 0,
            Self::Buy     => 1,
            Self::Unknown => 255,
        }
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sell_decodes_from_zero() {
        assert_eq!(AggressorSide::from_u8(0), AggressorSide::Sell);
    }

    #[test]
    fn buy_decodes_from_one() {
        assert_eq!(AggressorSide::from_u8(1), AggressorSide::Buy);
    }

    #[test]
    fn unknown_decodes_from_255() {
        assert_eq!(AggressorSide::from_u8(255), AggressorSide::Unknown);
    }

    #[test]
    fn unrecognised_value_becomes_unknown() {
        assert_eq!(AggressorSide::from_u8(42), AggressorSide::Unknown);
    }

    #[test]
    fn as_u8_round_trips() {
        for side in [AggressorSide::Sell, AggressorSide::Buy, AggressorSide::Unknown] {
            assert_eq!(AggressorSide::from_u8(side.as_u8()), side);
        }
    }
}
