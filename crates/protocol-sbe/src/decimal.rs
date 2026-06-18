use crate::error::SbeError;

/// Constant exponent used by all Binance Spot SBE price and quantity fields.
///
/// Actual value = mantissa × 10^(`EXPONENT`) = mantissa × 10^(−8).
pub const EXPONENT: i8  = -8;
/// Absolute value of [`EXPONENT`]; the number of decimal places.
pub const SCALE: u32    = 8;
/// Wire size of one `decimal64` field (mantissa only; exponent is constant).
pub const DECIMAL64_SIZE: usize = 8;

/// A Binance SBE fixed-point number with a constant exponent of −8.
///
/// All prices and quantities in the Binance Spot SBE schema use this type.
/// The actual value is `mantissa × 10^(−8)`.
///
/// # Conversion
///
/// To integrate with the rest of the pipeline (which stores prices as
/// `i64` scaled by `price_scale` or `qty_scale` from `InstrumentDefinition`)
/// use [`to_scaled`].
///
/// [`to_scaled`]: Decimal64::to_scaled
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Decimal64 {
    pub mantissa: i64,
}

impl Decimal64 {
    /// Decode one `decimal64` field from `buf` starting at `offset`.
    ///
    /// Reads 8 bytes as a little-endian `int64`.  The constant exponent is
    /// NOT present on the wire; it is fixed at [`EXPONENT`] = −8.
    pub fn decode(buf: &[u8], offset: usize) -> Result<Self, SbeError> {
        let end = offset + DECIMAL64_SIZE;
        if buf.len() < end {
            return Err(SbeError::BufferTooShort { needed: end, have: buf.len() });
        }
        let bytes = buf[offset..end].try_into().unwrap();
        Ok(Self { mantissa: i64::from_le_bytes(bytes) })
    }

    /// Convert to a scaled `i64` as used by the rest of the pipeline.
    ///
    /// The SBE value is `mantissa × 10^(-8)`.  The caller's representation is
    /// `result × 10^(-target_scale)`.  Solving for `result`:
    ///
    /// ```text
    /// result = mantissa × 10^(target_scale − 8)
    ///        = mantissa / 10^(8 − target_scale)   when target_scale ≤ 8
    ///        = mantissa × 10^(target_scale − 8)   when target_scale > 8
    /// ```
    ///
    /// Truncates (does not round) when downscaling.  This is correct for
    /// order-book prices where tick size ≥ 10^(−target_scale) by construction.
    ///
    /// # Examples
    ///
    /// ```
    /// use protocol_sbe::decimal::Decimal64;
    ///
    /// // BTCUSDT price 50 000.00, price_scale = 2
    /// let d = Decimal64 { mantissa: 5_000_000_000_000 }; // 50000 × 10^8
    /// assert_eq!(d.to_scaled(2), 5_000_000);              // 50000.00 as i64(scale=2)
    ///
    /// // ETHUSDT qty 0.001, qty_scale = 3
    /// let d = Decimal64 { mantissa: 100_000 };            // 0.001 × 10^8
    /// assert_eq!(d.to_scaled(3), 1);                      // 0.001 as i64(scale=3)
    /// ```
    pub fn to_scaled(self, target_scale: u32) -> i64 {
        match target_scale.cmp(&SCALE) {
            std::cmp::Ordering::Equal   => self.mantissa,
            std::cmp::Ordering::Less    => {
                let divisor = 10_i64.pow(SCALE - target_scale);
                self.mantissa / divisor
            }
            std::cmp::Ordering::Greater => {
                let multiplier = 10_i64.pow(target_scale - SCALE);
                self.mantissa * multiplier
            }
        }
    }

    /// Returns `true` when the mantissa is zero (null / empty quantity).
    pub fn is_zero(self) -> bool {
        self.mantissa == 0
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn make_buf(mantissa: i64) -> Vec<u8> {
        mantissa.to_le_bytes().to_vec()
    }

    // --- decode ---

    #[test]
    fn decode_positive_mantissa() {
        let buf = make_buf(5_000_000_000_000);
        let d = Decimal64::decode(&buf, 0).unwrap();
        assert_eq!(d.mantissa, 5_000_000_000_000);
    }

    #[test]
    fn decode_negative_mantissa() {
        let buf = make_buf(-1_000_000);
        let d = Decimal64::decode(&buf, 0).unwrap();
        assert_eq!(d.mantissa, -1_000_000);
    }

    #[test]
    fn decode_at_offset() {
        let mut buf = vec![0xFFu8; 8]; // garbage prefix
        buf.extend_from_slice(&1_000_000_i64.to_le_bytes());
        let d = Decimal64::decode(&buf, 8).unwrap();
        assert_eq!(d.mantissa, 1_000_000);
    }

    #[test]
    fn decode_buffer_too_short_errors() {
        let buf = [0u8; 7];
        let err = Decimal64::decode(&buf, 0).unwrap_err();
        assert_eq!(err, SbeError::BufferTooShort { needed: 8, have: 7 });
    }

    // --- to_scaled ---

    #[test]
    fn to_scaled_same_scale_identity() {
        let d = Decimal64 { mantissa: 1_234_567_890 };
        assert_eq!(d.to_scaled(8), 1_234_567_890);
    }

    #[test]
    fn to_scaled_downscale_btcusdt_price() {
        // 50000.00 with price_scale=2
        let d = Decimal64 { mantissa: 5_000_000_000_000 };
        assert_eq!(d.to_scaled(2), 5_000_000);
    }

    #[test]
    fn to_scaled_downscale_ethusdt_qty() {
        // 0.001 with qty_scale=3
        let d = Decimal64 { mantissa: 100_000 };
        assert_eq!(d.to_scaled(3), 1);
    }

    #[test]
    fn to_scaled_upscale() {
        // mantissa=1 (= 10^(-8)) upscaled to scale=9 → 10
        let d = Decimal64 { mantissa: 1 };
        assert_eq!(d.to_scaled(9), 10);
    }

    #[test]
    fn to_scaled_zero() {
        let d = Decimal64 { mantissa: 0 };
        assert_eq!(d.to_scaled(2), 0);
        assert!(d.is_zero());
    }

    #[test]
    fn is_zero_nonzero_returns_false() {
        let d = Decimal64 { mantissa: 1 };
        assert!(!d.is_zero());
    }
}
