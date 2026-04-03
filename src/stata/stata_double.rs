/// A value from a Stata "double" variable (8-byte IEEE 754 double).
///
/// In DTA format 113+, a double is stored as eight bytes (after endianness
/// correction). Stata reserves specific NaN bit patterns as missing-value
/// sentinels. If the bit pattern (as `u64`) is at least
/// `0x7FE0_0000_0000_0000`, the value encodes a missing value; otherwise it
/// is present data.
///
/// # Examples
///
/// ```
/// use dta::stata::stata_double::StataDouble;
/// use dta::stata::missing_value::MissingValue;
///
/// let present = StataDouble::try_from(3.14_f64).unwrap();
/// assert_eq!(present, StataDouble::Present(3.14));
///
/// let missing = StataDouble::try_from(f64::from_bits(0x7FE0_0000_0000_0000)).unwrap();
/// assert_eq!(missing, StataDouble::Missing(MissingValue::System));
/// ```
use super::missing_value::MissingValue;
use super::stata_error::{Result, StataError};

/// Bit pattern at or above which an `f64` encodes a Stata missing value.
const MISSING_DOUBLE_SYSTEM: u64 = 0x7FE0_0000_0000_0000;

/// Bit pattern encoding tagged missing `.a` for `f64`.
const MISSING_DOUBLE_A: u64 = 0x7FE0_0100_0000_0000;

/// Stride between consecutive tagged missing `f64` bit patterns.
const MISSING_DOUBLE_STRIDE: u64 = 0x0100_0000_0000;

/// A Stata double: either a present `f64` value or a [`MissingValue`].
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum StataDouble {
    /// A present data value.
    Present(f64),
    /// A missing value (`.`, `.a`–`.z`).
    Missing(MissingValue),
}

/// Interpret an `f64` read from a DTA file as a Stata double.
///
/// If the bit pattern is at or above `0x7FE0_0000_0000_0000`, the value is
/// classified as missing. A NaN whose bit pattern falls in the missing range
/// but does not match one of Stata's 27 specific patterns results in an error.
impl TryFrom<f64> for StataDouble {
    type Error = StataError;

    fn try_from(value: f64) -> Result<Self> {
        let bits = value.to_bits();
        // Stata missing values are positive NaNs with sign bit 0.
        // Negative values have the sign bit set (bit 63), making their
        // unsigned bit pattern > 0x7FE0_0000_0000_0000, so we must exclude them.
        if bits & 0x8000_0000_0000_0000 == 0 && bits >= MISSING_DOUBLE_SYSTEM {
            Ok(Self::Missing(MissingValue::try_from(value)?))
        } else {
            Ok(Self::Present(value))
        }
    }
}

/// Convert a [`StataDouble`] back to its raw `f64` DTA representation.
///
/// Present values are returned as-is. Missing values are encoded as their
/// specific NaN bit patterns.
impl From<StataDouble> for f64 {
    fn from(value: StataDouble) -> Self {
        match value {
            StataDouble::Present(v) => v,
            StataDouble::Missing(mv) => {
                let offset = u64::from(mv.code());
                let bits = if offset == 0 {
                    MISSING_DOUBLE_SYSTEM
                } else {
                    MISSING_DOUBLE_A + (offset - 1) * MISSING_DOUBLE_STRIDE
                };
                f64::from_bits(bits)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // -----------------------------------------------------------------------
    // Present values
    // -----------------------------------------------------------------------

    #[test]
    fn present_zero() {
        assert_eq!(
            StataDouble::try_from(0.0_f64).unwrap(),
            StataDouble::Present(0.0)
        );
    }

    #[test]
    fn present_negative_zero() {
        assert_eq!(
            StataDouble::try_from(-0.0_f64).unwrap(),
            StataDouble::Present(-0.0)
        );
    }

    #[test]
    fn present_one() {
        assert_eq!(
            StataDouble::try_from(1.0_f64).unwrap(),
            StataDouble::Present(1.0)
        );
    }

    #[test]
    fn present_negative() {
        assert_eq!(
            StataDouble::try_from(-1.5_f64).unwrap(),
            StataDouble::Present(-1.5)
        );
    }

    #[test]
    fn present_large() {
        // Largest f64 below the missing threshold
        let val = f64::from_bits(MISSING_DOUBLE_SYSTEM - 1);
        assert_eq!(
            StataDouble::try_from(val).unwrap(),
            StataDouble::Present(val)
        );
    }

    #[test]
    fn present_negative_infinity() {
        assert_eq!(
            StataDouble::try_from(f64::NEG_INFINITY).unwrap(),
            StataDouble::Present(f64::NEG_INFINITY),
        );
    }

    // -----------------------------------------------------------------------
    // Error: unrecognised NaN
    // -----------------------------------------------------------------------

    #[test]
    fn error_non_stata_nan() {
        let val = f64::from_bits(0x7FE0_0000_0000_0001);
        assert_eq!(StataDouble::try_from(val), Err(StataError::NotMissingValue));
    }

    #[test]
    fn error_positive_infinity() {
        // +Inf has bits 0x7FF0000000000000 which is >= MISSING_DOUBLE_SYSTEM but not a Stata pattern
        assert_eq!(
            StataDouble::try_from(f64::INFINITY),
            Err(StataError::NotMissingValue)
        );
    }

    // -----------------------------------------------------------------------
    // Missing values
    // -----------------------------------------------------------------------

    #[test]
    fn missing_system() {
        assert_eq!(
            StataDouble::try_from(f64::from_bits(0x7FE0_0000_0000_0000)).unwrap(),
            StataDouble::Missing(MissingValue::System),
        );
    }

    #[test]
    fn missing_a() {
        assert_eq!(
            StataDouble::try_from(f64::from_bits(0x7FE0_0100_0000_0000)).unwrap(),
            StataDouble::Missing(MissingValue::A),
        );
    }

    #[test]
    fn missing_b() {
        assert_eq!(
            StataDouble::try_from(f64::from_bits(0x7FE0_0200_0000_0000)).unwrap(),
            StataDouble::Missing(MissingValue::B),
        );
    }

    #[test]
    fn missing_c() {
        assert_eq!(
            StataDouble::try_from(f64::from_bits(0x7FE0_0300_0000_0000)).unwrap(),
            StataDouble::Missing(MissingValue::C),
        );
    }

    #[test]
    fn missing_d() {
        assert_eq!(
            StataDouble::try_from(f64::from_bits(0x7FE0_0400_0000_0000)).unwrap(),
            StataDouble::Missing(MissingValue::D),
        );
    }

    #[test]
    fn missing_e() {
        assert_eq!(
            StataDouble::try_from(f64::from_bits(0x7FE0_0500_0000_0000)).unwrap(),
            StataDouble::Missing(MissingValue::E),
        );
    }

    #[test]
    fn missing_f() {
        assert_eq!(
            StataDouble::try_from(f64::from_bits(0x7FE0_0600_0000_0000)).unwrap(),
            StataDouble::Missing(MissingValue::F),
        );
    }

    #[test]
    fn missing_g() {
        assert_eq!(
            StataDouble::try_from(f64::from_bits(0x7FE0_0700_0000_0000)).unwrap(),
            StataDouble::Missing(MissingValue::G),
        );
    }

    #[test]
    fn missing_h() {
        assert_eq!(
            StataDouble::try_from(f64::from_bits(0x7FE0_0800_0000_0000)).unwrap(),
            StataDouble::Missing(MissingValue::H),
        );
    }

    #[test]
    fn missing_i() {
        assert_eq!(
            StataDouble::try_from(f64::from_bits(0x7FE0_0900_0000_0000)).unwrap(),
            StataDouble::Missing(MissingValue::I),
        );
    }

    #[test]
    fn missing_j() {
        assert_eq!(
            StataDouble::try_from(f64::from_bits(0x7FE0_0A00_0000_0000)).unwrap(),
            StataDouble::Missing(MissingValue::J),
        );
    }

    #[test]
    fn missing_k() {
        assert_eq!(
            StataDouble::try_from(f64::from_bits(0x7FE0_0B00_0000_0000)).unwrap(),
            StataDouble::Missing(MissingValue::K),
        );
    }

    #[test]
    fn missing_l() {
        assert_eq!(
            StataDouble::try_from(f64::from_bits(0x7FE0_0C00_0000_0000)).unwrap(),
            StataDouble::Missing(MissingValue::L),
        );
    }

    #[test]
    fn missing_m() {
        assert_eq!(
            StataDouble::try_from(f64::from_bits(0x7FE0_0D00_0000_0000)).unwrap(),
            StataDouble::Missing(MissingValue::M),
        );
    }

    #[test]
    fn missing_n() {
        assert_eq!(
            StataDouble::try_from(f64::from_bits(0x7FE0_0E00_0000_0000)).unwrap(),
            StataDouble::Missing(MissingValue::N),
        );
    }

    #[test]
    fn missing_o() {
        assert_eq!(
            StataDouble::try_from(f64::from_bits(0x7FE0_0F00_0000_0000)).unwrap(),
            StataDouble::Missing(MissingValue::O),
        );
    }

    #[test]
    fn missing_p() {
        assert_eq!(
            StataDouble::try_from(f64::from_bits(0x7FE0_1000_0000_0000)).unwrap(),
            StataDouble::Missing(MissingValue::P),
        );
    }

    #[test]
    fn missing_q() {
        assert_eq!(
            StataDouble::try_from(f64::from_bits(0x7FE0_1100_0000_0000)).unwrap(),
            StataDouble::Missing(MissingValue::Q),
        );
    }

    #[test]
    fn missing_r() {
        assert_eq!(
            StataDouble::try_from(f64::from_bits(0x7FE0_1200_0000_0000)).unwrap(),
            StataDouble::Missing(MissingValue::R),
        );
    }

    #[test]
    fn missing_s() {
        assert_eq!(
            StataDouble::try_from(f64::from_bits(0x7FE0_1300_0000_0000)).unwrap(),
            StataDouble::Missing(MissingValue::S),
        );
    }

    #[test]
    fn missing_t() {
        assert_eq!(
            StataDouble::try_from(f64::from_bits(0x7FE0_1400_0000_0000)).unwrap(),
            StataDouble::Missing(MissingValue::T),
        );
    }

    #[test]
    fn missing_u() {
        assert_eq!(
            StataDouble::try_from(f64::from_bits(0x7FE0_1500_0000_0000)).unwrap(),
            StataDouble::Missing(MissingValue::U),
        );
    }

    #[test]
    fn missing_v() {
        assert_eq!(
            StataDouble::try_from(f64::from_bits(0x7FE0_1600_0000_0000)).unwrap(),
            StataDouble::Missing(MissingValue::V),
        );
    }

    #[test]
    fn missing_w() {
        assert_eq!(
            StataDouble::try_from(f64::from_bits(0x7FE0_1700_0000_0000)).unwrap(),
            StataDouble::Missing(MissingValue::W),
        );
    }

    #[test]
    fn missing_x() {
        assert_eq!(
            StataDouble::try_from(f64::from_bits(0x7FE0_1800_0000_0000)).unwrap(),
            StataDouble::Missing(MissingValue::X),
        );
    }

    #[test]
    fn missing_y() {
        assert_eq!(
            StataDouble::try_from(f64::from_bits(0x7FE0_1900_0000_0000)).unwrap(),
            StataDouble::Missing(MissingValue::Y),
        );
    }

    #[test]
    fn missing_z() {
        assert_eq!(
            StataDouble::try_from(f64::from_bits(0x7FE0_1A00_0000_0000)).unwrap(),
            StataDouble::Missing(MissingValue::Z),
        );
    }

    // -----------------------------------------------------------------------
    // From<StataDouble> for f64 — round-trip present values
    // -----------------------------------------------------------------------

    #[test]
    fn roundtrip_present_zero() {
        assert_eq!(f64::from(StataDouble::Present(0.0)), 0.0);
    }

    #[test]
    fn roundtrip_present_positive() {
        assert_eq!(f64::from(StataDouble::Present(3.14)), 3.14);
    }

    #[test]
    fn roundtrip_present_negative() {
        assert_eq!(f64::from(StataDouble::Present(-1.5)), -1.5);
    }

    // -----------------------------------------------------------------------
    // From<StataDouble> for f64 — round-trip missing values
    // -----------------------------------------------------------------------

    #[test]
    fn roundtrip_missing_system() {
        assert_eq!(
            f64::from(StataDouble::Missing(MissingValue::System)).to_bits(),
            0x7FE0_0000_0000_0000
        );
    }

    #[test]
    fn roundtrip_missing_a() {
        assert_eq!(
            f64::from(StataDouble::Missing(MissingValue::A)).to_bits(),
            0x7FE0_0100_0000_0000
        );
    }

    #[test]
    fn roundtrip_missing_z() {
        assert_eq!(
            f64::from(StataDouble::Missing(MissingValue::Z)).to_bits(),
            0x7FE0_1A00_0000_0000
        );
    }
}
