/// A value from a Stata "byte" variable (1-byte signed integer).
///
/// In DTA format 113+, a byte is stored as a single unsigned byte. Values
/// 0x00–0x64 (0–100 signed) and 0x80–0xFF (−128 to −1 signed) represent
/// data; values 0x65–0x7F (101–127 signed) encode missing values.
///
/// The valid data range in Stata is −127 to 100. The value −128 is outside
/// Stata's documented range but is classified as present.
///
/// # Examples
///
/// ```
/// use dta::stata::stata_byte::StataByte;
/// use dta::stata::missing_value::MissingValue;
///
/// let present = StataByte::try_from(42_u8).unwrap();
/// assert_eq!(present, StataByte::Present(42));
///
/// let missing = StataByte::try_from(0x65_u8).unwrap();
/// assert_eq!(missing, StataByte::Missing(MissingValue::System));
/// ```
use super::missing_value::MissingValue;
use super::not_missing_value_error::NotMissingValueError;

/// Maximum valid (non-missing) Stata byte value when interpreted as signed.
const DTA_113_MAX_INT8: i8 = 100;

/// Raw byte value encoding system missing (`.`).
const MISSING_BYTE_SYSTEM: u8 = 0x65;

/// A Stata byte: either a present `i8` value or a [`MissingValue`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum StataByte {
    /// A present data value.
    Present(i8),
    /// A missing value (`.`, `.a`–`.z`).
    Missing(MissingValue),
}

/// Interpret a raw `u8` read from a DTA file as a Stata byte.
///
/// The byte is reinterpreted as a signed `i8`. If the signed value exceeds
/// the maximum valid value (100), it is classified as missing.
impl TryFrom<u8> for StataByte {
    type Error = NotMissingValueError;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        let signed = value.cast_signed();
        if signed > DTA_113_MAX_INT8 {
            Ok(Self::Missing(MissingValue::try_from(value)?))
        } else {
            Ok(Self::Present(signed))
        }
    }
}

/// Convert a [`StataByte`] back to its raw `u8` DTA representation.
///
/// Present values are reinterpreted from signed `i8` to unsigned `u8`.
/// Missing values are encoded as `0x65` (`.`) through `0x7F` (`.z`).
impl From<StataByte> for u8 {
    fn from(value: StataByte) -> Self {
        match value {
            StataByte::Present(v) => v.cast_unsigned(),
            StataByte::Missing(mv) => MISSING_BYTE_SYSTEM + mv.code(),
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
        assert_eq!(StataByte::try_from(0_u8).unwrap(), StataByte::Present(0));
    }

    #[test]
    fn present_one() {
        assert_eq!(StataByte::try_from(1_u8).unwrap(), StataByte::Present(1));
    }

    #[test]
    fn present_max() {
        assert_eq!(
            StataByte::try_from(100_u8).unwrap(),
            StataByte::Present(100)
        );
    }

    #[test]
    fn present_min() {
        // 0x81 as i8 = -127, Stata's minimum valid byte
        assert_eq!(
            StataByte::try_from(0x81_u8).unwrap(),
            StataByte::Present(-127)
        );
    }

    #[test]
    fn present_negative_one() {
        // 0xFF as i8 = -1
        assert_eq!(
            StataByte::try_from(0xFF_u8).unwrap(),
            StataByte::Present(-1)
        );
    }

    #[test]
    fn present_negative_128() {
        // 0x80 as i8 = -128; outside Stata's documented range but treated as present
        assert_eq!(
            StataByte::try_from(0x80_u8).unwrap(),
            StataByte::Present(-128)
        );
    }

    // -----------------------------------------------------------------------
    // Missing values
    // -----------------------------------------------------------------------

    #[test]
    fn missing_system() {
        assert_eq!(
            StataByte::try_from(0x65_u8).unwrap(),
            StataByte::Missing(MissingValue::System),
        );
    }

    #[test]
    fn missing_a() {
        assert_eq!(
            StataByte::try_from(0x66_u8).unwrap(),
            StataByte::Missing(MissingValue::A),
        );
    }

    #[test]
    fn missing_b() {
        assert_eq!(
            StataByte::try_from(0x67_u8).unwrap(),
            StataByte::Missing(MissingValue::B),
        );
    }

    #[test]
    fn missing_c() {
        assert_eq!(
            StataByte::try_from(0x68_u8).unwrap(),
            StataByte::Missing(MissingValue::C),
        );
    }

    #[test]
    fn missing_d() {
        assert_eq!(
            StataByte::try_from(0x69_u8).unwrap(),
            StataByte::Missing(MissingValue::D),
        );
    }

    #[test]
    fn missing_e() {
        assert_eq!(
            StataByte::try_from(0x6A_u8).unwrap(),
            StataByte::Missing(MissingValue::E),
        );
    }

    #[test]
    fn missing_f() {
        assert_eq!(
            StataByte::try_from(0x6B_u8).unwrap(),
            StataByte::Missing(MissingValue::F),
        );
    }

    #[test]
    fn missing_g() {
        assert_eq!(
            StataByte::try_from(0x6C_u8).unwrap(),
            StataByte::Missing(MissingValue::G),
        );
    }

    #[test]
    fn missing_h() {
        assert_eq!(
            StataByte::try_from(0x6D_u8).unwrap(),
            StataByte::Missing(MissingValue::H),
        );
    }

    #[test]
    fn missing_i() {
        assert_eq!(
            StataByte::try_from(0x6E_u8).unwrap(),
            StataByte::Missing(MissingValue::I),
        );
    }

    #[test]
    fn missing_j() {
        assert_eq!(
            StataByte::try_from(0x6F_u8).unwrap(),
            StataByte::Missing(MissingValue::J),
        );
    }

    #[test]
    fn missing_k() {
        assert_eq!(
            StataByte::try_from(0x70_u8).unwrap(),
            StataByte::Missing(MissingValue::K),
        );
    }

    #[test]
    fn missing_l() {
        assert_eq!(
            StataByte::try_from(0x71_u8).unwrap(),
            StataByte::Missing(MissingValue::L),
        );
    }

    #[test]
    fn missing_m() {
        assert_eq!(
            StataByte::try_from(0x72_u8).unwrap(),
            StataByte::Missing(MissingValue::M),
        );
    }

    #[test]
    fn missing_n() {
        assert_eq!(
            StataByte::try_from(0x73_u8).unwrap(),
            StataByte::Missing(MissingValue::N),
        );
    }

    #[test]
    fn missing_o() {
        assert_eq!(
            StataByte::try_from(0x74_u8).unwrap(),
            StataByte::Missing(MissingValue::O),
        );
    }

    #[test]
    fn missing_p() {
        assert_eq!(
            StataByte::try_from(0x75_u8).unwrap(),
            StataByte::Missing(MissingValue::P),
        );
    }

    #[test]
    fn missing_q() {
        assert_eq!(
            StataByte::try_from(0x76_u8).unwrap(),
            StataByte::Missing(MissingValue::Q),
        );
    }

    #[test]
    fn missing_r() {
        assert_eq!(
            StataByte::try_from(0x77_u8).unwrap(),
            StataByte::Missing(MissingValue::R),
        );
    }

    #[test]
    fn missing_s() {
        assert_eq!(
            StataByte::try_from(0x78_u8).unwrap(),
            StataByte::Missing(MissingValue::S),
        );
    }

    #[test]
    fn missing_t() {
        assert_eq!(
            StataByte::try_from(0x79_u8).unwrap(),
            StataByte::Missing(MissingValue::T),
        );
    }

    #[test]
    fn missing_u() {
        assert_eq!(
            StataByte::try_from(0x7A_u8).unwrap(),
            StataByte::Missing(MissingValue::U),
        );
    }

    #[test]
    fn missing_v() {
        assert_eq!(
            StataByte::try_from(0x7B_u8).unwrap(),
            StataByte::Missing(MissingValue::V),
        );
    }

    #[test]
    fn missing_w() {
        assert_eq!(
            StataByte::try_from(0x7C_u8).unwrap(),
            StataByte::Missing(MissingValue::W),
        );
    }

    #[test]
    fn missing_x() {
        assert_eq!(
            StataByte::try_from(0x7D_u8).unwrap(),
            StataByte::Missing(MissingValue::X),
        );
    }

    #[test]
    fn missing_y() {
        assert_eq!(
            StataByte::try_from(0x7E_u8).unwrap(),
            StataByte::Missing(MissingValue::Y),
        );
    }

    #[test]
    fn missing_z() {
        assert_eq!(
            StataByte::try_from(0x7F_u8).unwrap(),
            StataByte::Missing(MissingValue::Z),
        );
    }

    // -----------------------------------------------------------------------
    // From<StataByte> for u8 — round-trip present values
    // -----------------------------------------------------------------------

    #[test]
    fn roundtrip_present_zero() {
        assert_eq!(u8::from(StataByte::Present(0)), 0);
    }

    #[test]
    fn roundtrip_present_max() {
        assert_eq!(u8::from(StataByte::Present(100)), 100);
    }

    #[test]
    fn roundtrip_present_min() {
        assert_eq!(u8::from(StataByte::Present(-127)), 0x81);
    }

    #[test]
    fn roundtrip_present_negative_one() {
        assert_eq!(u8::from(StataByte::Present(-1)), 0xFF);
    }

    // -----------------------------------------------------------------------
    // From<StataByte> for u8 — round-trip missing values
    // -----------------------------------------------------------------------

    #[test]
    fn roundtrip_missing_system() {
        assert_eq!(u8::from(StataByte::Missing(MissingValue::System)), 0x65);
    }

    #[test]
    fn roundtrip_missing_a() {
        assert_eq!(u8::from(StataByte::Missing(MissingValue::A)), 0x66);
    }

    #[test]
    fn roundtrip_missing_z() {
        assert_eq!(u8::from(StataByte::Missing(MissingValue::Z)), 0x7F);
    }
}
