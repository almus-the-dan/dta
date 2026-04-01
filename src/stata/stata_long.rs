/// A value from a Stata "long" variable (4-byte signed integer).
///
/// In DTA format 113+, a long is stored as four bytes (after endianness
/// correction). Values whose signed interpretation is at most 2,147,483,620
/// represent data; values `0x7FFF_FFE5`–`0x7FFF_FFFF` encode missing values.
///
/// The valid data range in Stata is −2,147,483,647 to 2,147,483,620.
///
/// # Examples
///
/// ```
/// use dta::stata::stata_long::StataLong;
/// use dta::stata::missing_value::MissingValue;
///
/// let present = StataLong::try_from(100_000_u32).unwrap();
/// assert_eq!(present, StataLong::Present(100_000));
///
/// let missing = StataLong::try_from(0x7FFF_FFE5_u32).unwrap();
/// assert_eq!(missing, StataLong::Missing(MissingValue::System));
/// ```
use super::missing_value::MissingValue;
use super::not_missing_value_error::NotMissingValueError;

/// Maximum valid (non-missing) Stata long value when interpreted as signed.
const DTA_113_MAX_INT32: i32 = 2_147_483_620;

/// Raw u32 value encoding system missing (`.`).
const MISSING_LONG_SYSTEM: u32 = 0x7FFF_FFE5;

/// A Stata long: either a present `i32` value or a [`MissingValue`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum StataLong {
    /// A present data value.
    Present(i32),
    /// A missing value (`.`, `.a`–`.z`).
    Missing(MissingValue),
}

/// Interpret a raw `u32` read from a DTA file as a Stata long.
///
/// The value is reinterpreted as a signed `i32`. If the signed value exceeds
/// the maximum valid value (2,147,483,620), it is classified as missing.
impl TryFrom<u32> for StataLong {
    type Error = NotMissingValueError;

    fn try_from(value: u32) -> Result<Self, Self::Error> {
        let signed = value.cast_signed();
        if signed > DTA_113_MAX_INT32 {
            Ok(Self::Missing(MissingValue::try_from(value)?))
        } else {
            Ok(Self::Present(signed))
        }
    }
}

/// Convert a [`StataLong`] back to its raw `u32` DTA representation.
///
/// Present values are reinterpreted from signed `i32` to unsigned `u32`.
/// Missing values are encoded as `0x7FFF_FFE5` (`.`) through `0x7FFF_FFFF` (`.z`).
impl From<StataLong> for u32 {
    fn from(value: StataLong) -> Self {
        match value {
            StataLong::Present(v) => v.cast_unsigned(),
            StataLong::Missing(mv) => MISSING_LONG_SYSTEM + u32::from(mv.code()),
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
        assert_eq!(StataLong::try_from(0_u32).unwrap(), StataLong::Present(0));
    }

    #[test]
    fn present_one() {
        assert_eq!(StataLong::try_from(1_u32).unwrap(), StataLong::Present(1));
    }

    #[test]
    fn present_max() {
        assert_eq!(
            StataLong::try_from(0x7FFF_FFE4_u32).unwrap(),
            StataLong::Present(2_147_483_620),
        );
    }

    #[test]
    fn present_min() {
        // 0x80000001 as i32 = -2_147_483_647
        assert_eq!(
            StataLong::try_from(0x8000_0001_u32).unwrap(),
            StataLong::Present(-2_147_483_647),
        );
    }

    #[test]
    fn present_negative_one() {
        assert_eq!(
            StataLong::try_from(0xFFFF_FFFF_u32).unwrap(),
            StataLong::Present(-1),
        );
    }

    #[test]
    fn present_negative_2147483648() {
        // 0x80000000 as i32 = -2_147_483_648; outside Stata's documented range but treated as present
        assert_eq!(
            StataLong::try_from(0x8000_0000_u32).unwrap(),
            StataLong::Present(-2_147_483_648),
        );
    }

    // -----------------------------------------------------------------------
    // Missing values
    // -----------------------------------------------------------------------

    #[test]
    fn missing_system() {
        assert_eq!(
            StataLong::try_from(0x7FFF_FFE5_u32).unwrap(),
            StataLong::Missing(MissingValue::System),
        );
    }

    #[test]
    fn missing_a() {
        assert_eq!(
            StataLong::try_from(0x7FFF_FFE6_u32).unwrap(),
            StataLong::Missing(MissingValue::A),
        );
    }

    #[test]
    fn missing_b() {
        assert_eq!(
            StataLong::try_from(0x7FFF_FFE7_u32).unwrap(),
            StataLong::Missing(MissingValue::B),
        );
    }

    #[test]
    fn missing_c() {
        assert_eq!(
            StataLong::try_from(0x7FFF_FFE8_u32).unwrap(),
            StataLong::Missing(MissingValue::C),
        );
    }

    #[test]
    fn missing_d() {
        assert_eq!(
            StataLong::try_from(0x7FFF_FFE9_u32).unwrap(),
            StataLong::Missing(MissingValue::D),
        );
    }

    #[test]
    fn missing_e() {
        assert_eq!(
            StataLong::try_from(0x7FFF_FFEA_u32).unwrap(),
            StataLong::Missing(MissingValue::E),
        );
    }

    #[test]
    fn missing_f() {
        assert_eq!(
            StataLong::try_from(0x7FFF_FFEB_u32).unwrap(),
            StataLong::Missing(MissingValue::F),
        );
    }

    #[test]
    fn missing_g() {
        assert_eq!(
            StataLong::try_from(0x7FFF_FFEC_u32).unwrap(),
            StataLong::Missing(MissingValue::G),
        );
    }

    #[test]
    fn missing_h() {
        assert_eq!(
            StataLong::try_from(0x7FFF_FFED_u32).unwrap(),
            StataLong::Missing(MissingValue::H),
        );
    }

    #[test]
    fn missing_i() {
        assert_eq!(
            StataLong::try_from(0x7FFF_FFEE_u32).unwrap(),
            StataLong::Missing(MissingValue::I),
        );
    }

    #[test]
    fn missing_j() {
        assert_eq!(
            StataLong::try_from(0x7FFF_FFEF_u32).unwrap(),
            StataLong::Missing(MissingValue::J),
        );
    }

    #[test]
    fn missing_k() {
        assert_eq!(
            StataLong::try_from(0x7FFF_FFF0_u32).unwrap(),
            StataLong::Missing(MissingValue::K),
        );
    }

    #[test]
    fn missing_l() {
        assert_eq!(
            StataLong::try_from(0x7FFF_FFF1_u32).unwrap(),
            StataLong::Missing(MissingValue::L),
        );
    }

    #[test]
    fn missing_m() {
        assert_eq!(
            StataLong::try_from(0x7FFF_FFF2_u32).unwrap(),
            StataLong::Missing(MissingValue::M),
        );
    }

    #[test]
    fn missing_n() {
        assert_eq!(
            StataLong::try_from(0x7FFF_FFF3_u32).unwrap(),
            StataLong::Missing(MissingValue::N),
        );
    }

    #[test]
    fn missing_o() {
        assert_eq!(
            StataLong::try_from(0x7FFF_FFF4_u32).unwrap(),
            StataLong::Missing(MissingValue::O),
        );
    }

    #[test]
    fn missing_p() {
        assert_eq!(
            StataLong::try_from(0x7FFF_FFF5_u32).unwrap(),
            StataLong::Missing(MissingValue::P),
        );
    }

    #[test]
    fn missing_q() {
        assert_eq!(
            StataLong::try_from(0x7FFF_FFF6_u32).unwrap(),
            StataLong::Missing(MissingValue::Q),
        );
    }

    #[test]
    fn missing_r() {
        assert_eq!(
            StataLong::try_from(0x7FFF_FFF7_u32).unwrap(),
            StataLong::Missing(MissingValue::R),
        );
    }

    #[test]
    fn missing_s() {
        assert_eq!(
            StataLong::try_from(0x7FFF_FFF8_u32).unwrap(),
            StataLong::Missing(MissingValue::S),
        );
    }

    #[test]
    fn missing_t() {
        assert_eq!(
            StataLong::try_from(0x7FFF_FFF9_u32).unwrap(),
            StataLong::Missing(MissingValue::T),
        );
    }

    #[test]
    fn missing_u() {
        assert_eq!(
            StataLong::try_from(0x7FFF_FFFA_u32).unwrap(),
            StataLong::Missing(MissingValue::U),
        );
    }

    #[test]
    fn missing_v() {
        assert_eq!(
            StataLong::try_from(0x7FFF_FFFB_u32).unwrap(),
            StataLong::Missing(MissingValue::V),
        );
    }

    #[test]
    fn missing_w() {
        assert_eq!(
            StataLong::try_from(0x7FFF_FFFC_u32).unwrap(),
            StataLong::Missing(MissingValue::W),
        );
    }

    #[test]
    fn missing_x() {
        assert_eq!(
            StataLong::try_from(0x7FFF_FFFD_u32).unwrap(),
            StataLong::Missing(MissingValue::X),
        );
    }

    #[test]
    fn missing_y() {
        assert_eq!(
            StataLong::try_from(0x7FFF_FFFE_u32).unwrap(),
            StataLong::Missing(MissingValue::Y),
        );
    }

    #[test]
    fn missing_z() {
        assert_eq!(
            StataLong::try_from(0x7FFF_FFFF_u32).unwrap(),
            StataLong::Missing(MissingValue::Z),
        );
    }

    // -----------------------------------------------------------------------
    // From<StataLong> for u32 — round-trip present values
    // -----------------------------------------------------------------------

    #[test]
    fn roundtrip_present_zero() {
        assert_eq!(u32::from(StataLong::Present(0)), 0);
    }

    #[test]
    fn roundtrip_present_max() {
        assert_eq!(u32::from(StataLong::Present(2_147_483_620)), 0x7FFF_FFE4);
    }

    #[test]
    fn roundtrip_present_min() {
        assert_eq!(u32::from(StataLong::Present(-2_147_483_647)), 0x8000_0001);
    }

    #[test]
    fn roundtrip_present_negative_one() {
        assert_eq!(u32::from(StataLong::Present(-1)), 0xFFFF_FFFF);
    }

    // -----------------------------------------------------------------------
    // From<StataLong> for u32 — round-trip missing values
    // -----------------------------------------------------------------------

    #[test]
    fn roundtrip_missing_system() {
        assert_eq!(
            u32::from(StataLong::Missing(MissingValue::System)),
            0x7FFF_FFE5
        );
    }

    #[test]
    fn roundtrip_missing_a() {
        assert_eq!(u32::from(StataLong::Missing(MissingValue::A)), 0x7FFF_FFE6);
    }

    #[test]
    fn roundtrip_missing_z() {
        assert_eq!(u32::from(StataLong::Missing(MissingValue::Z)), 0x7FFF_FFFF);
    }
}
