/// A value from a Stata "byte" variable (1-byte signed integer).
///
/// In DTA format 113+, a byte is stored as a single unsigned byte. Values
/// 0x00–0x64 (0–100 signed) and 0x80–0xFF (−128 to −1 signed) represent
/// data; values 0x65–0x7F (101–127 signed) encode missing values
/// (`.`, `.a`–`.z`).
///
/// In pre-113 formats, only the single value 0x7F (127 signed) encodes
/// system missing; values 0x65–0x7E (101–126) are valid data. Tagged
/// missing values (`.a`–`.z`) are unrepresentable in those formats.
///
/// # Examples
///
/// ```
/// use dta::stata::dta::release::Release;
/// use dta::stata::missing_value::MissingValue;
/// use dta::stata::stata_byte::StataByte;
///
/// let present = StataByte::from_raw(42_u8, Release::V117).unwrap();
/// assert_eq!(present, StataByte::Present(42));
///
/// let missing = StataByte::from_raw(0x65_u8, Release::V117).unwrap();
/// assert_eq!(missing, StataByte::Missing(MissingValue::System));
/// ```
use super::dta::release::Release;
use super::missing_value::MissingValue;
use super::stata_error::{Result, StataError};

/// Maximum valid (non-missing) Stata byte value for DTA 113+.
const DTA_113_MAX_INT8: i8 = 100;

/// Raw byte value encoding system missing (`.`) in DTA 113+.
const MISSING_BYTE_SYSTEM_113: u8 = 0x65;

/// Raw byte value encoding system missing (`.`) in pre-113 formats.
const MISSING_BYTE_SYSTEM_PRE_113: u8 = 0x7F;

/// A Stata byte: either a present `i8` value or a [`MissingValue`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum StataByte {
    /// A present data value.
    Present(i8),
    /// A missing value (`.` in any release; `.a`–`.z` in DTA 113+ only).
    Missing(MissingValue),
}

impl StataByte {
    /// Decode a raw `u8` read from a DTA file as a Stata byte.
    ///
    /// The decoding rules depend on `release`:
    ///
    /// - **DTA 113+**: the signed byte range −127..=100 is data; 101..=127
    ///   encode `.`, `.a`, …, `.z` respectively. −128 is outside Stata's
    ///   documented range but is treated as present.
    /// - **Pre-DTA 113**: the signed byte range −128..=126 is data; 127
    ///   encodes system missing (`.`). Tagged missing values do not exist.
    ///
    /// # Errors
    ///
    /// Returns [`StataError::NotMissingValue`] if the raw value is inside
    /// the DTA 113+ missing range but does not match any of the 27
    /// sentinel values. This can only happen for `release >= V113` —
    /// the pre-113 decoder treats every byte as valid.
    pub fn from_raw(raw: u8, release: Release) -> Result<Self> {
        let signed = raw.cast_signed();
        if release.supports_tagged_missing() {
            if signed > DTA_113_MAX_INT8 {
                Ok(Self::Missing(MissingValue::try_from(raw)?))
            } else {
                Ok(Self::Present(signed))
            }
        } else if raw == MISSING_BYTE_SYSTEM_PRE_113 {
            Ok(Self::Missing(MissingValue::System))
        } else {
            Ok(Self::Present(signed))
        }
    }

    /// Encode this value as a raw `u8` for a DTA file.
    ///
    /// # Errors
    ///
    /// Returns [`StataError::TaggedMissingUnsupported`] if `self` is a
    /// tagged missing (`.a`–`.z`) and `release` is pre-113. Pre-113
    /// formats have no way to encode tagged missing values.
    pub fn to_raw(self, release: Release) -> Result<u8> {
        match self {
            Self::Present(v) => Ok(v.cast_unsigned()),
            Self::Missing(mv) => {
                if release.supports_tagged_missing() {
                    Ok(MISSING_BYTE_SYSTEM_113 + mv.code())
                } else if mv == MissingValue::System {
                    Ok(MISSING_BYTE_SYSTEM_PRE_113)
                } else {
                    Err(StataError::TaggedMissingUnsupported)
                }
            }
        }
    }

    /// Returns the underlying `i8` when this value is
    /// [`Present`](Self::Present), or `None` when it is
    /// [`Missing`](Self::Missing).
    #[must_use]
    #[inline]
    pub fn present(self) -> Option<i8> {
        match self {
            Self::Present(v) => Some(v),
            Self::Missing(_) => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // -----------------------------------------------------------------------
    // DTA 113+ — Present values
    // -----------------------------------------------------------------------

    #[test]
    fn v113_present_zero() {
        assert_eq!(
            StataByte::from_raw(0_u8, Release::V113).unwrap(),
            StataByte::Present(0)
        );
    }

    #[test]
    fn v113_present_one() {
        assert_eq!(
            StataByte::from_raw(1_u8, Release::V113).unwrap(),
            StataByte::Present(1)
        );
    }

    #[test]
    fn v113_present_max() {
        assert_eq!(
            StataByte::from_raw(100_u8, Release::V113).unwrap(),
            StataByte::Present(100)
        );
    }

    #[test]
    fn v113_present_min() {
        // 0x81 as i8 = -127, Stata's minimum valid byte
        assert_eq!(
            StataByte::from_raw(0x81_u8, Release::V113).unwrap(),
            StataByte::Present(-127)
        );
    }

    #[test]
    fn v113_present_negative_one() {
        assert_eq!(
            StataByte::from_raw(0xFF_u8, Release::V113).unwrap(),
            StataByte::Present(-1)
        );
    }

    #[test]
    fn v113_present_negative_128() {
        assert_eq!(
            StataByte::from_raw(0x80_u8, Release::V113).unwrap(),
            StataByte::Present(-128)
        );
    }

    // -----------------------------------------------------------------------
    // DTA 113+ — Missing values
    // -----------------------------------------------------------------------

    #[test]
    fn v113_missing_system() {
        assert_eq!(
            StataByte::from_raw(0x65_u8, Release::V113).unwrap(),
            StataByte::Missing(MissingValue::System),
        );
    }

    #[test]
    fn v113_missing_a() {
        assert_eq!(
            StataByte::from_raw(0x66_u8, Release::V113).unwrap(),
            StataByte::Missing(MissingValue::A),
        );
    }

    #[test]
    fn v113_missing_z() {
        assert_eq!(
            StataByte::from_raw(0x7F_u8, Release::V113).unwrap(),
            StataByte::Missing(MissingValue::Z),
        );
    }

    // -----------------------------------------------------------------------
    // Pre-113 — Present values
    // -----------------------------------------------------------------------

    #[test]
    fn v104_present_zero() {
        assert_eq!(
            StataByte::from_raw(0_u8, Release::V104).unwrap(),
            StataByte::Present(0)
        );
    }

    #[test]
    fn v104_present_101_is_data() {
        // In pre-113 byte, 101 is valid data (not a missing sentinel).
        assert_eq!(
            StataByte::from_raw(0x65_u8, Release::V104).unwrap(),
            StataByte::Present(101),
        );
    }

    #[test]
    fn v104_present_126_is_data() {
        assert_eq!(
            StataByte::from_raw(0x7E_u8, Release::V104).unwrap(),
            StataByte::Present(126),
        );
    }

    #[test]
    fn v104_present_negative_128() {
        assert_eq!(
            StataByte::from_raw(0x80_u8, Release::V104).unwrap(),
            StataByte::Present(-128),
        );
    }

    // -----------------------------------------------------------------------
    // Pre-113 — Missing values (only System; tagged do not exist)
    // -----------------------------------------------------------------------

    #[test]
    fn v104_missing_system() {
        assert_eq!(
            StataByte::from_raw(0x7F_u8, Release::V104).unwrap(),
            StataByte::Missing(MissingValue::System),
        );
    }

    #[test]
    fn v105_missing_system() {
        assert_eq!(
            StataByte::from_raw(0x7F_u8, Release::V105).unwrap(),
            StataByte::Missing(MissingValue::System),
        );
    }

    #[test]
    fn v112_missing_system() {
        // V112 is the last pre-tagged-missing format.
        assert_eq!(
            StataByte::from_raw(0x7F_u8, Release::V112).unwrap(),
            StataByte::Missing(MissingValue::System),
        );
    }

    // -----------------------------------------------------------------------
    // to_raw — Present round-trip
    // -----------------------------------------------------------------------

    #[test]
    fn v113_to_raw_present_zero() {
        assert_eq!(StataByte::Present(0).to_raw(Release::V113).unwrap(), 0);
    }

    #[test]
    fn v113_to_raw_present_max() {
        assert_eq!(StataByte::Present(100).to_raw(Release::V113).unwrap(), 100);
    }

    #[test]
    fn v113_to_raw_present_min() {
        assert_eq!(
            StataByte::Present(-127).to_raw(Release::V113).unwrap(),
            0x81
        );
    }

    #[test]
    fn v104_to_raw_present_101() {
        // Pre-113 can encode 101 as data.
        assert_eq!(StataByte::Present(101).to_raw(Release::V104).unwrap(), 101);
    }

    // -----------------------------------------------------------------------
    // to_raw — System missing
    // -----------------------------------------------------------------------

    #[test]
    fn v113_to_raw_missing_system() {
        assert_eq!(
            StataByte::Missing(MissingValue::System)
                .to_raw(Release::V113)
                .unwrap(),
            0x65,
        );
    }

    #[test]
    fn v104_to_raw_missing_system() {
        assert_eq!(
            StataByte::Missing(MissingValue::System)
                .to_raw(Release::V104)
                .unwrap(),
            0x7F,
        );
    }

    // -----------------------------------------------------------------------
    // to_raw — Tagged missings: only representable in 113+
    // -----------------------------------------------------------------------

    #[test]
    fn v113_to_raw_missing_a() {
        assert_eq!(
            StataByte::Missing(MissingValue::A)
                .to_raw(Release::V113)
                .unwrap(),
            0x66,
        );
    }

    #[test]
    fn v113_to_raw_missing_z() {
        assert_eq!(
            StataByte::Missing(MissingValue::Z)
                .to_raw(Release::V113)
                .unwrap(),
            0x7F,
        );
    }

    #[test]
    fn v104_to_raw_missing_tagged_errors() {
        assert_eq!(
            StataByte::Missing(MissingValue::A).to_raw(Release::V104),
            Err(StataError::TaggedMissingUnsupported),
        );
    }

    #[test]
    fn v112_to_raw_missing_tagged_errors() {
        assert_eq!(
            StataByte::Missing(MissingValue::Z).to_raw(Release::V112),
            Err(StataError::TaggedMissingUnsupported),
        );
    }

    // -----------------------------------------------------------------------
    // present()
    // -----------------------------------------------------------------------

    #[test]
    fn present_returns_inner_for_present() {
        assert_eq!(StataByte::Present(42).present(), Some(42));
    }

    #[test]
    fn present_returns_none_for_missing() {
        assert_eq!(StataByte::Missing(MissingValue::System).present(), None);
        assert_eq!(StataByte::Missing(MissingValue::A).present(), None);
    }
}
