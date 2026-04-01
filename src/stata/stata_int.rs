/// A value from a Stata "int" variable (2-byte signed integer).
///
/// In DTA format 113+, an int is stored as two bytes (after endianness
/// correction). Values whose signed interpretation is at most 32,740 represent
/// data; values 0x7FE5–0x7FFF (32,741–32,767 signed) encode missing values.
///
/// The valid data range in Stata is −32,767 to 32,740.
///
/// # Examples
///
/// ```
/// use dta::stata::stata_int::StataInt;
/// use dta::stata::missing_value::MissingValue;
///
/// let present = StataInt::try_from(1000_u16).unwrap();
/// assert_eq!(present, StataInt::Present(1000));
///
/// let missing = StataInt::try_from(0x7FE5_u16).unwrap();
/// assert_eq!(missing, StataInt::Missing(MissingValue::System));
/// ```
use super::missing_value::MissingValue;
use super::not_missing_value_error::NotMissingValueError;

/// Maximum valid (non-missing) Stata int value when interpreted as signed.
const DTA_113_MAX_INT16: i16 = 32_740;

/// Raw u16 value encoding system missing (`.`).
const MISSING_INT_SYSTEM: u16 = 0x7FE5;

/// A Stata int: either a present `i16` value or a [`MissingValue`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum StataInt {
    /// A present data value.
    Present(i16),
    /// A missing value (`.`, `.a`–`.z`).
    Missing(MissingValue),
}

/// Interpret a raw `u16` read from a DTA file as a Stata int.
///
/// The value is reinterpreted as a signed `i16`. If the signed value exceeds
/// the maximum valid value (32,740), it is classified as missing.
impl TryFrom<u16> for StataInt {
    type Error = NotMissingValueError;

    fn try_from(value: u16) -> Result<Self, Self::Error> {
        let signed = value.cast_signed();
        if signed > DTA_113_MAX_INT16 {
            Ok(Self::Missing(MissingValue::try_from(value)?))
        } else {
            Ok(Self::Present(signed))
        }
    }
}

/// Convert a [`StataInt`] back to its raw `u16` DTA representation.
///
/// Present values are reinterpreted from signed `i16` to unsigned `u16`.
/// Missing values are encoded as `0x7FE5` (`.`) through `0x7FFF` (`.z`).
impl From<StataInt> for u16 {
    fn from(value: StataInt) -> Self {
        match value {
            StataInt::Present(v) => v.cast_unsigned(),
            StataInt::Missing(mv) => MISSING_INT_SYSTEM + u16::from(mv.code()),
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
        assert_eq!(StataInt::try_from(0_u16).unwrap(), StataInt::Present(0));
    }

    #[test]
    fn present_one() {
        assert_eq!(StataInt::try_from(1_u16).unwrap(), StataInt::Present(1));
    }

    #[test]
    fn present_max() {
        assert_eq!(
            StataInt::try_from(0x7FE4_u16).unwrap(),
            StataInt::Present(32_740)
        );
    }

    #[test]
    fn present_min() {
        // 0x8001 as i16 = -32767
        assert_eq!(
            StataInt::try_from(0x8001_u16).unwrap(),
            StataInt::Present(-32_767)
        );
    }

    #[test]
    fn present_negative_one() {
        assert_eq!(
            StataInt::try_from(0xFFFF_u16).unwrap(),
            StataInt::Present(-1)
        );
    }

    #[test]
    fn present_negative_32768() {
        // 0x8000 as i16 = -32768; outside Stata's documented range but treated as present
        assert_eq!(
            StataInt::try_from(0x8000_u16).unwrap(),
            StataInt::Present(-32_768)
        );
    }

    // -----------------------------------------------------------------------
    // Missing values
    // -----------------------------------------------------------------------

    #[test]
    fn missing_system() {
        assert_eq!(
            StataInt::try_from(0x7FE5_u16).unwrap(),
            StataInt::Missing(MissingValue::System),
        );
    }

    #[test]
    fn missing_a() {
        assert_eq!(
            StataInt::try_from(0x7FE6_u16).unwrap(),
            StataInt::Missing(MissingValue::A),
        );
    }

    #[test]
    fn missing_b() {
        assert_eq!(
            StataInt::try_from(0x7FE7_u16).unwrap(),
            StataInt::Missing(MissingValue::B),
        );
    }

    #[test]
    fn missing_c() {
        assert_eq!(
            StataInt::try_from(0x7FE8_u16).unwrap(),
            StataInt::Missing(MissingValue::C),
        );
    }

    #[test]
    fn missing_d() {
        assert_eq!(
            StataInt::try_from(0x7FE9_u16).unwrap(),
            StataInt::Missing(MissingValue::D),
        );
    }

    #[test]
    fn missing_e() {
        assert_eq!(
            StataInt::try_from(0x7FEA_u16).unwrap(),
            StataInt::Missing(MissingValue::E),
        );
    }

    #[test]
    fn missing_f() {
        assert_eq!(
            StataInt::try_from(0x7FEB_u16).unwrap(),
            StataInt::Missing(MissingValue::F),
        );
    }

    #[test]
    fn missing_g() {
        assert_eq!(
            StataInt::try_from(0x7FEC_u16).unwrap(),
            StataInt::Missing(MissingValue::G),
        );
    }

    #[test]
    fn missing_h() {
        assert_eq!(
            StataInt::try_from(0x7FED_u16).unwrap(),
            StataInt::Missing(MissingValue::H),
        );
    }

    #[test]
    fn missing_i() {
        assert_eq!(
            StataInt::try_from(0x7FEE_u16).unwrap(),
            StataInt::Missing(MissingValue::I),
        );
    }

    #[test]
    fn missing_j() {
        assert_eq!(
            StataInt::try_from(0x7FEF_u16).unwrap(),
            StataInt::Missing(MissingValue::J),
        );
    }

    #[test]
    fn missing_k() {
        assert_eq!(
            StataInt::try_from(0x7FF0_u16).unwrap(),
            StataInt::Missing(MissingValue::K),
        );
    }

    #[test]
    fn missing_l() {
        assert_eq!(
            StataInt::try_from(0x7FF1_u16).unwrap(),
            StataInt::Missing(MissingValue::L),
        );
    }

    #[test]
    fn missing_m() {
        assert_eq!(
            StataInt::try_from(0x7FF2_u16).unwrap(),
            StataInt::Missing(MissingValue::M),
        );
    }

    #[test]
    fn missing_n() {
        assert_eq!(
            StataInt::try_from(0x7FF3_u16).unwrap(),
            StataInt::Missing(MissingValue::N),
        );
    }

    #[test]
    fn missing_o() {
        assert_eq!(
            StataInt::try_from(0x7FF4_u16).unwrap(),
            StataInt::Missing(MissingValue::O),
        );
    }

    #[test]
    fn missing_p() {
        assert_eq!(
            StataInt::try_from(0x7FF5_u16).unwrap(),
            StataInt::Missing(MissingValue::P),
        );
    }

    #[test]
    fn missing_q() {
        assert_eq!(
            StataInt::try_from(0x7FF6_u16).unwrap(),
            StataInt::Missing(MissingValue::Q),
        );
    }

    #[test]
    fn missing_r() {
        assert_eq!(
            StataInt::try_from(0x7FF7_u16).unwrap(),
            StataInt::Missing(MissingValue::R),
        );
    }

    #[test]
    fn missing_s() {
        assert_eq!(
            StataInt::try_from(0x7FF8_u16).unwrap(),
            StataInt::Missing(MissingValue::S),
        );
    }

    #[test]
    fn missing_t() {
        assert_eq!(
            StataInt::try_from(0x7FF9_u16).unwrap(),
            StataInt::Missing(MissingValue::T),
        );
    }

    #[test]
    fn missing_u() {
        assert_eq!(
            StataInt::try_from(0x7FFA_u16).unwrap(),
            StataInt::Missing(MissingValue::U),
        );
    }

    #[test]
    fn missing_v() {
        assert_eq!(
            StataInt::try_from(0x7FFB_u16).unwrap(),
            StataInt::Missing(MissingValue::V),
        );
    }

    #[test]
    fn missing_w() {
        assert_eq!(
            StataInt::try_from(0x7FFC_u16).unwrap(),
            StataInt::Missing(MissingValue::W),
        );
    }

    #[test]
    fn missing_x() {
        assert_eq!(
            StataInt::try_from(0x7FFD_u16).unwrap(),
            StataInt::Missing(MissingValue::X),
        );
    }

    #[test]
    fn missing_y() {
        assert_eq!(
            StataInt::try_from(0x7FFE_u16).unwrap(),
            StataInt::Missing(MissingValue::Y),
        );
    }

    #[test]
    fn missing_z() {
        assert_eq!(
            StataInt::try_from(0x7FFF_u16).unwrap(),
            StataInt::Missing(MissingValue::Z),
        );
    }

    // -----------------------------------------------------------------------
    // From<StataInt> for u16 — round-trip present values
    // -----------------------------------------------------------------------

    #[test]
    fn roundtrip_present_zero() {
        assert_eq!(u16::from(StataInt::Present(0)), 0);
    }

    #[test]
    fn roundtrip_present_max() {
        assert_eq!(u16::from(StataInt::Present(32_740)), 0x7FE4);
    }

    #[test]
    fn roundtrip_present_min() {
        assert_eq!(u16::from(StataInt::Present(-32_767)), 0x8001);
    }

    #[test]
    fn roundtrip_present_negative_one() {
        assert_eq!(u16::from(StataInt::Present(-1)), 0xFFFF);
    }

    // -----------------------------------------------------------------------
    // From<StataInt> for u16 — round-trip missing values
    // -----------------------------------------------------------------------

    #[test]
    fn roundtrip_missing_system() {
        assert_eq!(u16::from(StataInt::Missing(MissingValue::System)), 0x7FE5);
    }

    #[test]
    fn roundtrip_missing_a() {
        assert_eq!(u16::from(StataInt::Missing(MissingValue::A)), 0x7FE6);
    }

    #[test]
    fn roundtrip_missing_z() {
        assert_eq!(u16::from(StataInt::Missing(MissingValue::Z)), 0x7FFF);
    }
}
