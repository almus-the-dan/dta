/// A value from a Stata "float" variable (4-byte IEEE 754 float).
///
/// In DTA format 113+, a float is stored as four bytes (after endianness
/// correction). Stata reserves specific NaN bit patterns as missing-value
/// sentinels. If the bit pattern (as `u32`) is at least `0x7F00_0000`, the
/// value encodes a missing value; otherwise it is present data.
///
/// # Examples
///
/// ```
/// use dta::stata::stata_float::StataFloat;
/// use dta::stata::missing_value::MissingValue;
///
/// let present = StataFloat::try_from(3.14_f32).unwrap();
/// assert_eq!(present, StataFloat::Present(3.14));
///
/// let missing = StataFloat::try_from(f32::from_bits(0x7F00_0000)).unwrap();
/// assert_eq!(missing, StataFloat::Missing(MissingValue::System));
/// ```
use super::missing_value::MissingValue;
use super::stata_error::{Result, StataError};

/// Bit pattern at or above which an `f32` encodes a Stata missing value.
const MISSING_FLOAT_SYSTEM: u32 = 0x7F00_0000;

/// Bit pattern encoding tagged missing `.a` for `f32`.
const MISSING_FLOAT_A: u32 = 0x7F00_0800;

/// Stride between consecutive tagged missing `f32` bit patterns.
const MISSING_FLOAT_STRIDE: u32 = 0x0800;

/// A Stata float: either a present `f32` value or a [`MissingValue`].
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum StataFloat {
    /// A present data value.
    Present(f32),
    /// A missing value (`.`, `.a`–`.z`).
    Missing(MissingValue),
}

/// Interpret an `f32` read from a DTA file as a Stata float.
///
/// If the bit pattern is at or above `0x7F00_0000`, the value is classified
/// as missing. A NaN whose bit pattern falls in the missing range but does
/// not match one of Stata's 27 specific patterns results in an error.
impl TryFrom<f32> for StataFloat {
    type Error = StataError;

    fn try_from(value: f32) -> Result<Self> {
        let bits = value.to_bits();
        // Stata missing values are positive NaNs with sign bit 0.
        // Negative values have the sign bit set (bit 31), making their
        // unsigned bit pattern > 0x7F00_0000, so we must exclude them.
        if bits & 0x8000_0000 == 0 && bits >= MISSING_FLOAT_SYSTEM {
            Ok(Self::Missing(MissingValue::try_from(value)?))
        } else {
            Ok(Self::Present(value))
        }
    }
}

/// Convert a [`StataFloat`] back to its raw `f32` DTA representation.
///
/// Present values are returned as-is. Missing values are encoded as their
/// specific NaN bit patterns.
impl From<StataFloat> for f32 {
    fn from(value: StataFloat) -> Self {
        match value {
            StataFloat::Present(v) => v,
            StataFloat::Missing(mv) => {
                let offset = u32::from(mv.code());
                let bits = if offset == 0 {
                    MISSING_FLOAT_SYSTEM
                } else {
                    MISSING_FLOAT_A + (offset - 1) * MISSING_FLOAT_STRIDE
                };
                f32::from_bits(bits)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use float_cmp::assert_approx_eq;

    // -----------------------------------------------------------------------
    // Present values
    // -----------------------------------------------------------------------

    #[test]
    fn present_zero() {
        assert_eq!(
            StataFloat::try_from(0.0_f32).unwrap(),
            StataFloat::Present(0.0)
        );
    }

    #[test]
    fn present_negative_zero() {
        assert_eq!(
            StataFloat::try_from(-0.0_f32).unwrap(),
            StataFloat::Present(-0.0)
        );
    }

    #[test]
    fn present_one() {
        assert_eq!(
            StataFloat::try_from(1.0_f32).unwrap(),
            StataFloat::Present(1.0)
        );
    }

    #[test]
    fn present_negative() {
        assert_eq!(
            StataFloat::try_from(-1.5_f32).unwrap(),
            StataFloat::Present(-1.5)
        );
    }

    #[test]
    fn present_large() {
        // Largest f32 below the missing threshold
        let val = f32::from_bits(MISSING_FLOAT_SYSTEM - 1);
        assert_eq!(StataFloat::try_from(val).unwrap(), StataFloat::Present(val));
    }

    #[test]
    fn present_negative_infinity() {
        assert_eq!(
            StataFloat::try_from(f32::NEG_INFINITY).unwrap(),
            StataFloat::Present(f32::NEG_INFINITY),
        );
    }

    // -----------------------------------------------------------------------
    // Error: unrecognised NaN
    // -----------------------------------------------------------------------

    #[test]
    fn error_non_stata_nan() {
        // A NaN in the missing range but not matching any of Stata's 27 patterns
        let val = f32::from_bits(0x7F00_0001);
        assert_eq!(StataFloat::try_from(val), Err(StataError::NotMissingValue));
    }

    #[test]
    fn error_positive_infinity() {
        // +Inf has bits 0x7F800000 which is >= MISSING_FLOAT_SYSTEM but not a Stata pattern
        assert_eq!(
            StataFloat::try_from(f32::INFINITY),
            Err(StataError::NotMissingValue)
        );
    }

    // -----------------------------------------------------------------------
    // Missing values
    // -----------------------------------------------------------------------

    #[test]
    fn missing_system() {
        assert_eq!(
            StataFloat::try_from(f32::from_bits(0x7F00_0000)).unwrap(),
            StataFloat::Missing(MissingValue::System),
        );
    }

    #[test]
    fn missing_a() {
        assert_eq!(
            StataFloat::try_from(f32::from_bits(0x7F00_0800)).unwrap(),
            StataFloat::Missing(MissingValue::A),
        );
    }

    #[test]
    fn missing_b() {
        assert_eq!(
            StataFloat::try_from(f32::from_bits(0x7F00_1000)).unwrap(),
            StataFloat::Missing(MissingValue::B),
        );
    }

    #[test]
    fn missing_c() {
        assert_eq!(
            StataFloat::try_from(f32::from_bits(0x7F00_1800)).unwrap(),
            StataFloat::Missing(MissingValue::C),
        );
    }

    #[test]
    fn missing_d() {
        assert_eq!(
            StataFloat::try_from(f32::from_bits(0x7F00_2000)).unwrap(),
            StataFloat::Missing(MissingValue::D),
        );
    }

    #[test]
    fn missing_e() {
        assert_eq!(
            StataFloat::try_from(f32::from_bits(0x7F00_2800)).unwrap(),
            StataFloat::Missing(MissingValue::E),
        );
    }

    #[test]
    fn missing_f() {
        assert_eq!(
            StataFloat::try_from(f32::from_bits(0x7F00_3000)).unwrap(),
            StataFloat::Missing(MissingValue::F),
        );
    }

    #[test]
    fn missing_g() {
        assert_eq!(
            StataFloat::try_from(f32::from_bits(0x7F00_3800)).unwrap(),
            StataFloat::Missing(MissingValue::G),
        );
    }

    #[test]
    fn missing_h() {
        assert_eq!(
            StataFloat::try_from(f32::from_bits(0x7F00_4000)).unwrap(),
            StataFloat::Missing(MissingValue::H),
        );
    }

    #[test]
    fn missing_i() {
        assert_eq!(
            StataFloat::try_from(f32::from_bits(0x7F00_4800)).unwrap(),
            StataFloat::Missing(MissingValue::I),
        );
    }

    #[test]
    fn missing_j() {
        assert_eq!(
            StataFloat::try_from(f32::from_bits(0x7F00_5000)).unwrap(),
            StataFloat::Missing(MissingValue::J),
        );
    }

    #[test]
    fn missing_k() {
        assert_eq!(
            StataFloat::try_from(f32::from_bits(0x7F00_5800)).unwrap(),
            StataFloat::Missing(MissingValue::K),
        );
    }

    #[test]
    fn missing_l() {
        assert_eq!(
            StataFloat::try_from(f32::from_bits(0x7F00_6000)).unwrap(),
            StataFloat::Missing(MissingValue::L),
        );
    }

    #[test]
    fn missing_m() {
        assert_eq!(
            StataFloat::try_from(f32::from_bits(0x7F00_6800)).unwrap(),
            StataFloat::Missing(MissingValue::M),
        );
    }

    #[test]
    fn missing_n() {
        assert_eq!(
            StataFloat::try_from(f32::from_bits(0x7F00_7000)).unwrap(),
            StataFloat::Missing(MissingValue::N),
        );
    }

    #[test]
    fn missing_o() {
        assert_eq!(
            StataFloat::try_from(f32::from_bits(0x7F00_7800)).unwrap(),
            StataFloat::Missing(MissingValue::O),
        );
    }

    #[test]
    fn missing_p() {
        assert_eq!(
            StataFloat::try_from(f32::from_bits(0x7F00_8000)).unwrap(),
            StataFloat::Missing(MissingValue::P),
        );
    }

    #[test]
    fn missing_q() {
        assert_eq!(
            StataFloat::try_from(f32::from_bits(0x7F00_8800)).unwrap(),
            StataFloat::Missing(MissingValue::Q),
        );
    }

    #[test]
    fn missing_r() {
        assert_eq!(
            StataFloat::try_from(f32::from_bits(0x7F00_9000)).unwrap(),
            StataFloat::Missing(MissingValue::R),
        );
    }

    #[test]
    fn missing_s() {
        assert_eq!(
            StataFloat::try_from(f32::from_bits(0x7F00_9800)).unwrap(),
            StataFloat::Missing(MissingValue::S),
        );
    }

    #[test]
    fn missing_t() {
        assert_eq!(
            StataFloat::try_from(f32::from_bits(0x7F00_A000)).unwrap(),
            StataFloat::Missing(MissingValue::T),
        );
    }

    #[test]
    fn missing_u() {
        assert_eq!(
            StataFloat::try_from(f32::from_bits(0x7F00_A800)).unwrap(),
            StataFloat::Missing(MissingValue::U),
        );
    }

    #[test]
    fn missing_v() {
        assert_eq!(
            StataFloat::try_from(f32::from_bits(0x7F00_B000)).unwrap(),
            StataFloat::Missing(MissingValue::V),
        );
    }

    #[test]
    fn missing_w() {
        assert_eq!(
            StataFloat::try_from(f32::from_bits(0x7F00_B800)).unwrap(),
            StataFloat::Missing(MissingValue::W),
        );
    }

    #[test]
    fn missing_x() {
        assert_eq!(
            StataFloat::try_from(f32::from_bits(0x7F00_C000)).unwrap(),
            StataFloat::Missing(MissingValue::X),
        );
    }

    #[test]
    fn missing_y() {
        assert_eq!(
            StataFloat::try_from(f32::from_bits(0x7F00_C800)).unwrap(),
            StataFloat::Missing(MissingValue::Y),
        );
    }

    #[test]
    fn missing_z() {
        assert_eq!(
            StataFloat::try_from(f32::from_bits(0x7F00_D000)).unwrap(),
            StataFloat::Missing(MissingValue::Z),
        );
    }

    // -----------------------------------------------------------------------
    // From<StataFloat> for f32 — round-trip present values
    // -----------------------------------------------------------------------

    #[test]
    fn roundtrip_present_zero() {
        assert_approx_eq!(f32, f32::from(StataFloat::Present(0.0)), 0.0);
    }

    #[test]
    fn roundtrip_present_positive() {
        assert_approx_eq!(f32, f32::from(StataFloat::Present(1.5)), 1.5);
    }

    #[test]
    fn roundtrip_present_negative() {
        assert_approx_eq!(f32, f32::from(StataFloat::Present(-1.5)), -1.5);
    }

    // -----------------------------------------------------------------------
    // From<StataFloat> for f32 — round-trip missing values
    // -----------------------------------------------------------------------

    #[test]
    fn roundtrip_missing_system() {
        assert_eq!(
            f32::from(StataFloat::Missing(MissingValue::System)).to_bits(),
            0x7F00_0000
        );
    }

    #[test]
    fn roundtrip_missing_a() {
        assert_eq!(
            f32::from(StataFloat::Missing(MissingValue::A)).to_bits(),
            0x7F00_0800
        );
    }

    #[test]
    fn roundtrip_missing_z() {
        assert_eq!(
            f32::from(StataFloat::Missing(MissingValue::Z)).to_bits(),
            0x7F00_D000
        );
    }
}
