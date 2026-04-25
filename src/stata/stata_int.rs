/// A value from a Stata "int" variable (2-byte signed integer).
///
/// In DTA format 113+, an int is stored as two bytes (after endianness
/// correction). Values whose signed interpretation is at most 32,740 represent
/// data; values 0x7FE5–0x7FFF (32,741–32,767 signed) encode missing values
/// (`.`, `.a`–`.z`).
///
/// In pre-113 formats, only the single value 0x7FFF (32,767 signed) encodes
/// system missing; values 0x7FE5–0x7FFE are valid data. Tagged missing values
/// (`.a`–`.z`) are unrepresentable in those formats.
///
/// # Examples
///
/// ```
/// use dta::stata::dta::release::Release;
/// use dta::stata::missing_value::MissingValue;
/// use dta::stata::stata_int::StataInt;
///
/// let present = StataInt::from_raw(1000_u16, Release::V117).unwrap();
/// assert_eq!(present, StataInt::Present(1000));
///
/// let missing = StataInt::from_raw(0x7FE5_u16, Release::V117).unwrap();
/// assert_eq!(missing, StataInt::Missing(MissingValue::System));
/// ```
use super::dta::release::Release;
use super::missing_value::MissingValue;
use super::stata_byte::StataByte;
use super::stata_error::{Result, StataError};
use super::stata_long::StataLong;

/// Maximum valid (non-missing) Stata int value for DTA 113+.
const DTA_113_MAX_INT16: i16 = 32_740;

/// Raw u16 value encoding system missing (`.`) in DTA 113+.
const MISSING_INT_SYSTEM_113: u16 = 0x7FE5;

/// Raw u16 value encoding system missing (`.`) in pre-113 formats.
const MISSING_INT_SYSTEM_PRE_113: u16 = 0x7FFF;

/// A Stata int: either a present `i16` value or a [`MissingValue`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum StataInt {
    /// A present data value.
    Present(i16),
    /// A missing value (`.` in any release; `.a`–`.z` in DTA 113+ only).
    Missing(MissingValue),
}

impl StataInt {
    /// Decode a raw `u16` read from a DTA file as a Stata int.
    ///
    /// The decoding rules depend on `release`:
    ///
    /// - **DTA 113+**: values in `−32,767..=32,740` are data; `32,741..=32,767`
    ///   encode `.`, `.a`, …, `.z` respectively. `−32,768` is outside Stata's
    ///   documented range but is treated as present.
    /// - **Pre-DTA 113**: values in `−32,768..=32,766` are data; `32,767`
    ///   encodes system missing (`.`). Tagged missings do not exist.
    ///
    /// # Errors
    ///
    /// Returns [`StataError::NotMissingValue`] if the raw value is inside
    /// the DTA 113+ missing range but does not match any of the 27
    /// sentinel values (pre-113 files never produce this error).
    pub fn from_raw(raw: u16, release: Release) -> Result<Self> {
        let signed = raw.cast_signed();
        if release.supports_tagged_missing() {
            if signed > DTA_113_MAX_INT16 {
                Ok(Self::Missing(MissingValue::try_from(raw)?))
            } else {
                Ok(Self::Present(signed))
            }
        } else if raw == MISSING_INT_SYSTEM_PRE_113 {
            Ok(Self::Missing(MissingValue::System))
        } else {
            Ok(Self::Present(signed))
        }
    }

    /// Encode this value as a raw `u16` for a DTA file.
    ///
    /// # Errors
    ///
    /// Returns [`StataError::TaggedMissingUnsupported`] if `self` is a
    /// tagged missing (`.a`–`.z`) and `release` is pre-113.
    pub fn to_raw(self, release: Release) -> Result<u16> {
        match self {
            Self::Present(v) => Ok(v.cast_unsigned()),
            Self::Missing(mv) => {
                if release.supports_tagged_missing() {
                    Ok(MISSING_INT_SYSTEM_113 + u16::from(mv.code()))
                } else if mv == MissingValue::System {
                    Ok(MISSING_INT_SYSTEM_PRE_113)
                } else {
                    Err(StataError::TaggedMissingUnsupported)
                }
            }
        }
    }

    /// Returns the underlying `i16` when this value is
    /// [`Present`](Self::Present), or `None` when it is
    /// [`Missing`](Self::Missing).
    #[must_use]
    #[inline]
    pub fn present(self) -> Option<i16> {
        match self {
            Self::Present(v) => Some(v),
            Self::Missing(_) => None,
        }
    }
}

// ---------------------------------------------------------------------------
// Widening conversions (From) and narrowing conversions (TryFrom)
// ---------------------------------------------------------------------------
//
// Mirror Rust's primitive `From<i8> for i16` and
// `TryFrom<i32> for i16`. Missing values translate directly because
// `MissingValue` is shared across every Stata numeric width.

impl From<StataByte> for StataInt {
    fn from(value: StataByte) -> Self {
        match value {
            StataByte::Present(v) => Self::Present(i16::from(v)),
            StataByte::Missing(mv) => Self::Missing(mv),
        }
    }
}

impl TryFrom<StataLong> for StataInt {
    type Error = std::num::TryFromIntError;

    fn try_from(value: StataLong) -> std::result::Result<Self, Self::Error> {
        match value {
            StataLong::Present(v) => Ok(Self::Present(i16::try_from(v)?)),
            StataLong::Missing(mv) => Ok(Self::Missing(mv)),
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
            StataInt::from_raw(0_u16, Release::V113).unwrap(),
            StataInt::Present(0)
        );
    }

    #[test]
    fn v113_present_max() {
        assert_eq!(
            StataInt::from_raw(0x7FE4_u16, Release::V113).unwrap(),
            StataInt::Present(32_740),
        );
    }

    #[test]
    fn v113_present_min() {
        assert_eq!(
            StataInt::from_raw(0x8001_u16, Release::V113).unwrap(),
            StataInt::Present(-32_767),
        );
    }

    #[test]
    fn v113_present_negative_one() {
        assert_eq!(
            StataInt::from_raw(0xFFFF_u16, Release::V113).unwrap(),
            StataInt::Present(-1),
        );
    }

    #[test]
    fn v113_present_negative_32768() {
        assert_eq!(
            StataInt::from_raw(0x8000_u16, Release::V113).unwrap(),
            StataInt::Present(-32_768),
        );
    }

    // -----------------------------------------------------------------------
    // DTA 113+ — Missing values
    // -----------------------------------------------------------------------

    #[test]
    fn v113_missing_system() {
        assert_eq!(
            StataInt::from_raw(0x7FE5_u16, Release::V113).unwrap(),
            StataInt::Missing(MissingValue::System),
        );
    }

    #[test]
    fn v113_missing_a() {
        assert_eq!(
            StataInt::from_raw(0x7FE6_u16, Release::V113).unwrap(),
            StataInt::Missing(MissingValue::A),
        );
    }

    #[test]
    fn v113_missing_z() {
        assert_eq!(
            StataInt::from_raw(0x7FFF_u16, Release::V113).unwrap(),
            StataInt::Missing(MissingValue::Z),
        );
    }

    // -----------------------------------------------------------------------
    // Pre-113 — Present values
    // -----------------------------------------------------------------------

    #[test]
    fn v104_present_32741_is_data() {
        // In pre-113 int, 32,741 is valid data (not a missing sentinel).
        assert_eq!(
            StataInt::from_raw(0x7FE5_u16, Release::V104).unwrap(),
            StataInt::Present(32_741),
        );
    }

    #[test]
    fn v104_present_32766_is_data() {
        assert_eq!(
            StataInt::from_raw(0x7FFE_u16, Release::V104).unwrap(),
            StataInt::Present(32_766),
        );
    }

    #[test]
    fn v104_present_negative_32768() {
        assert_eq!(
            StataInt::from_raw(0x8000_u16, Release::V104).unwrap(),
            StataInt::Present(-32_768),
        );
    }

    // -----------------------------------------------------------------------
    // Pre-113 — Missing values
    // -----------------------------------------------------------------------

    #[test]
    fn v104_missing_system() {
        assert_eq!(
            StataInt::from_raw(0x7FFF_u16, Release::V104).unwrap(),
            StataInt::Missing(MissingValue::System),
        );
    }

    #[test]
    fn v112_missing_system() {
        assert_eq!(
            StataInt::from_raw(0x7FFF_u16, Release::V112).unwrap(),
            StataInt::Missing(MissingValue::System),
        );
    }

    // -----------------------------------------------------------------------
    // to_raw — Present round-trip
    // -----------------------------------------------------------------------

    #[test]
    fn v113_to_raw_present_zero() {
        assert_eq!(StataInt::Present(0).to_raw(Release::V113).unwrap(), 0);
    }

    #[test]
    fn v113_to_raw_present_max() {
        assert_eq!(
            StataInt::Present(32_740).to_raw(Release::V113).unwrap(),
            0x7FE4,
        );
    }

    #[test]
    fn v113_to_raw_present_min() {
        assert_eq!(
            StataInt::Present(-32_767).to_raw(Release::V113).unwrap(),
            0x8001,
        );
    }

    // -----------------------------------------------------------------------
    // to_raw — System missing
    // -----------------------------------------------------------------------

    #[test]
    fn v113_to_raw_missing_system() {
        assert_eq!(
            StataInt::Missing(MissingValue::System)
                .to_raw(Release::V113)
                .unwrap(),
            0x7FE5,
        );
    }

    #[test]
    fn v104_to_raw_missing_system() {
        assert_eq!(
            StataInt::Missing(MissingValue::System)
                .to_raw(Release::V104)
                .unwrap(),
            0x7FFF,
        );
    }

    // -----------------------------------------------------------------------
    // to_raw — Tagged missings
    // -----------------------------------------------------------------------

    #[test]
    fn v113_to_raw_missing_a() {
        assert_eq!(
            StataInt::Missing(MissingValue::A)
                .to_raw(Release::V113)
                .unwrap(),
            0x7FE6,
        );
    }

    #[test]
    fn v113_to_raw_missing_z() {
        assert_eq!(
            StataInt::Missing(MissingValue::Z)
                .to_raw(Release::V113)
                .unwrap(),
            0x7FFF,
        );
    }

    #[test]
    fn v104_to_raw_missing_tagged_errors() {
        assert_eq!(
            StataInt::Missing(MissingValue::A).to_raw(Release::V104),
            Err(StataError::TaggedMissingUnsupported),
        );
    }

    #[test]
    fn v112_to_raw_missing_tagged_errors() {
        assert_eq!(
            StataInt::Missing(MissingValue::Z).to_raw(Release::V112),
            Err(StataError::TaggedMissingUnsupported),
        );
    }

    // -----------------------------------------------------------------------
    // present()
    // -----------------------------------------------------------------------

    #[test]
    fn present_returns_inner_for_present() {
        assert_eq!(StataInt::Present(1234).present(), Some(1234));
    }

    #[test]
    fn present_returns_none_for_missing() {
        assert_eq!(StataInt::Missing(MissingValue::System).present(), None);
        assert_eq!(StataInt::Missing(MissingValue::A).present(), None);
    }

    // -----------------------------------------------------------------------
    // From<StataByte>
    // -----------------------------------------------------------------------

    #[test]
    fn from_byte_present_widens() {
        assert_eq!(
            StataInt::from(StataByte::Present(42)),
            StataInt::Present(42),
        );
    }

    #[test]
    fn from_byte_present_negative_widens() {
        assert_eq!(
            StataInt::from(StataByte::Present(-128)),
            StataInt::Present(-128),
        );
    }

    #[test]
    fn from_byte_missing_translates_directly() {
        assert_eq!(
            StataInt::from(StataByte::Missing(MissingValue::System)),
            StataInt::Missing(MissingValue::System),
        );
        assert_eq!(
            StataInt::from(StataByte::Missing(MissingValue::Z)),
            StataInt::Missing(MissingValue::Z),
        );
    }

    // -----------------------------------------------------------------------
    // TryFrom<StataLong>
    // -----------------------------------------------------------------------

    #[test]
    fn try_from_long_in_range() {
        assert_eq!(
            StataInt::try_from(StataLong::Present(1234)).unwrap(),
            StataInt::Present(1234),
        );
    }

    #[test]
    fn try_from_long_at_i16_max() {
        assert_eq!(
            StataInt::try_from(StataLong::Present(32_767)).unwrap(),
            StataInt::Present(32_767),
        );
    }

    #[test]
    fn try_from_long_above_i16_max_errors() {
        assert!(StataInt::try_from(StataLong::Present(32_768)).is_err());
    }

    #[test]
    fn try_from_long_below_i16_min_errors() {
        assert!(StataInt::try_from(StataLong::Present(-32_769)).is_err());
    }

    #[test]
    fn try_from_long_missing_translates_directly() {
        assert_eq!(
            StataInt::try_from(StataLong::Missing(MissingValue::A)).unwrap(),
            StataInt::Missing(MissingValue::A),
        );
    }
}
