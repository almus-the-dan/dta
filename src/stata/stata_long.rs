/// A value from a Stata "long" variable (4-byte signed integer).
///
/// In DTA format 113+, a long is stored as four bytes (after endianness
/// correction). Values whose signed interpretation is at most 2,147,483,620
/// represent data; values `0x7FFF_FFE5`–`0x7FFF_FFFF` encode missing values
/// (`.`, `.a`–`.z`).
///
/// In pre-113 formats, only the single value `0x7FFF_FFFF` (2,147,483,647
/// signed) encodes system missing; values `0x7FFF_FFE5`–`0x7FFF_FFFE` are
/// valid data. Tagged missing values (`.a`–`.z`) are unrepresentable in those
/// formats.
///
/// # Examples
///
/// ```
/// use dta::stata::dta::release::Release;
/// use dta::stata::missing_value::MissingValue;
/// use dta::stata::stata_long::StataLong;
///
/// let present = StataLong::from_raw(100_000_u32, Release::V117).unwrap();
/// assert_eq!(present, StataLong::Present(100_000));
///
/// let missing = StataLong::from_raw(0x7FFF_FFE5_u32, Release::V117).unwrap();
/// assert_eq!(missing, StataLong::Missing(MissingValue::System));
/// ```
use super::dta::release::Release;
use super::missing_value::MissingValue;
use super::stata_byte::StataByte;
use super::stata_error::{Result, StataError};
use super::stata_int::StataInt;

/// Maximum valid (non-missing) Stata long value for DTA 113+.
const DTA_113_MAX_INT32: i32 = 2_147_483_620;

/// Raw u32 value encoding system missing (`.`) in DTA 113+.
const MISSING_LONG_SYSTEM_113: u32 = 0x7FFF_FFE5;

/// Raw u32 value encoding system missing (`.`) in pre-113 formats.
const MISSING_LONG_SYSTEM_PRE_113: u32 = 0x7FFF_FFFF;

/// A Stata long: either a present `i32` value or a [`MissingValue`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum StataLong {
    /// A present data value.
    Present(i32),
    /// A missing value (`.` in any release; `.a`–`.z` in DTA 113+ only).
    Missing(MissingValue),
}

impl StataLong {
    /// Decode a raw `u32` read from a DTA file as a Stata long.
    ///
    /// The decoding rules depend on `release`:
    ///
    /// - **DTA 113+**: values in `−2,147,483,647..=2,147,483,620` are data;
    ///   `2,147,483,621..=2,147,483,647` encode `.`, `.a`, …, `.z`.
    ///   `−2,147,483,648` is outside Stata's documented range but is
    ///   treated as present.
    /// - **Pre-DTA 113**: values in `−2,147,483,648..=2,147,483,646` are
    ///   data; `2,147,483,647` encodes system missing (`.`).
    ///
    /// # Errors
    ///
    /// Returns [`StataError::NotMissingValue`] if the raw value is inside
    /// the DTA 113+ missing range but does not match any of the 27
    /// sentinel values (pre-113 files never produce this error).
    pub fn from_raw(raw: u32, release: Release) -> Result<Self> {
        let signed = raw.cast_signed();
        if release.supports_tagged_missing() {
            if signed > DTA_113_MAX_INT32 {
                Ok(Self::Missing(MissingValue::try_from(raw)?))
            } else {
                Ok(Self::Present(signed))
            }
        } else if raw == MISSING_LONG_SYSTEM_PRE_113 {
            Ok(Self::Missing(MissingValue::System))
        } else {
            Ok(Self::Present(signed))
        }
    }

    /// Encode this value as a raw `u32` for a DTA file.
    ///
    /// # Errors
    ///
    /// Returns [`StataError::TaggedMissingUnsupported`] if `self` is a
    /// tagged missing (`.a`–`.z`) and `release` is pre-113.
    pub fn to_raw(self, release: Release) -> Result<u32> {
        match self {
            Self::Present(v) => Ok(v.cast_unsigned()),
            Self::Missing(mv) => {
                if release.supports_tagged_missing() {
                    Ok(MISSING_LONG_SYSTEM_113 + u32::from(mv.code()))
                } else if mv == MissingValue::System {
                    Ok(MISSING_LONG_SYSTEM_PRE_113)
                } else {
                    Err(StataError::TaggedMissingUnsupported)
                }
            }
        }
    }

    /// Returns the underlying `i32` when this value is
    /// [`Present`](Self::Present), or `None` when it is
    /// [`Missing`](Self::Missing).
    #[must_use]
    #[inline]
    pub fn present(self) -> Option<i32> {
        match self {
            Self::Present(v) => Some(v),
            Self::Missing(_) => None,
        }
    }
}

// ---------------------------------------------------------------------------
// Widening conversions (From)
// ---------------------------------------------------------------------------
//
// Mirror Rust's primitive `From<i8> for i32` and
// `From<i16> for i32`. Missing values translate directly because
// `MissingValue` is shared across every Stata numeric width.

impl From<StataByte> for StataLong {
    fn from(value: StataByte) -> Self {
        match value {
            StataByte::Present(v) => Self::Present(i32::from(v)),
            StataByte::Missing(mv) => Self::Missing(mv),
        }
    }
}

impl From<StataInt> for StataLong {
    fn from(value: StataInt) -> Self {
        match value {
            StataInt::Present(v) => Self::Present(i32::from(v)),
            StataInt::Missing(mv) => Self::Missing(mv),
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
            StataLong::from_raw(0_u32, Release::V113).unwrap(),
            StataLong::Present(0)
        );
    }

    #[test]
    fn v113_present_max() {
        assert_eq!(
            StataLong::from_raw(0x7FFF_FFE4_u32, Release::V113).unwrap(),
            StataLong::Present(2_147_483_620),
        );
    }

    #[test]
    fn v113_present_min() {
        assert_eq!(
            StataLong::from_raw(0x8000_0001_u32, Release::V113).unwrap(),
            StataLong::Present(-2_147_483_647),
        );
    }

    #[test]
    fn v113_present_negative_one() {
        assert_eq!(
            StataLong::from_raw(0xFFFF_FFFF_u32, Release::V113).unwrap(),
            StataLong::Present(-1),
        );
    }

    #[test]
    fn v113_present_i32_min() {
        assert_eq!(
            StataLong::from_raw(0x8000_0000_u32, Release::V113).unwrap(),
            StataLong::Present(i32::MIN),
        );
    }

    // -----------------------------------------------------------------------
    // DTA 113+ — Missing values
    // -----------------------------------------------------------------------

    #[test]
    fn v113_missing_system() {
        assert_eq!(
            StataLong::from_raw(0x7FFF_FFE5_u32, Release::V113).unwrap(),
            StataLong::Missing(MissingValue::System),
        );
    }

    #[test]
    fn v113_missing_a() {
        assert_eq!(
            StataLong::from_raw(0x7FFF_FFE6_u32, Release::V113).unwrap(),
            StataLong::Missing(MissingValue::A),
        );
    }

    #[test]
    fn v113_missing_z() {
        assert_eq!(
            StataLong::from_raw(0x7FFF_FFFF_u32, Release::V113).unwrap(),
            StataLong::Missing(MissingValue::Z),
        );
    }

    // -----------------------------------------------------------------------
    // Pre-113 — Present values
    // -----------------------------------------------------------------------

    #[test]
    fn v104_present_lower_sentinel_is_data() {
        assert_eq!(
            StataLong::from_raw(0x7FFF_FFE5_u32, Release::V104).unwrap(),
            StataLong::Present(2_147_483_621),
        );
    }

    #[test]
    fn v104_present_2147483646_is_data() {
        assert_eq!(
            StataLong::from_raw(0x7FFF_FFFE_u32, Release::V104).unwrap(),
            StataLong::Present(2_147_483_646),
        );
    }

    #[test]
    fn v104_present_i32_min() {
        assert_eq!(
            StataLong::from_raw(0x8000_0000_u32, Release::V104).unwrap(),
            StataLong::Present(i32::MIN),
        );
    }

    // -----------------------------------------------------------------------
    // Pre-113 — Missing values
    // -----------------------------------------------------------------------

    #[test]
    fn v104_missing_system() {
        assert_eq!(
            StataLong::from_raw(0x7FFF_FFFF_u32, Release::V104).unwrap(),
            StataLong::Missing(MissingValue::System),
        );
    }

    #[test]
    fn v112_missing_system() {
        assert_eq!(
            StataLong::from_raw(0x7FFF_FFFF_u32, Release::V112).unwrap(),
            StataLong::Missing(MissingValue::System),
        );
    }

    // -----------------------------------------------------------------------
    // to_raw — Present round-trip
    // -----------------------------------------------------------------------

    #[test]
    fn v113_to_raw_present_zero() {
        assert_eq!(StataLong::Present(0).to_raw(Release::V113).unwrap(), 0);
    }

    #[test]
    fn v113_to_raw_present_max() {
        assert_eq!(
            StataLong::Present(2_147_483_620)
                .to_raw(Release::V113)
                .unwrap(),
            0x7FFF_FFE4,
        );
    }

    #[test]
    fn v113_to_raw_present_min() {
        assert_eq!(
            StataLong::Present(-2_147_483_647)
                .to_raw(Release::V113)
                .unwrap(),
            0x8000_0001,
        );
    }

    // -----------------------------------------------------------------------
    // to_raw — System missing
    // -----------------------------------------------------------------------

    #[test]
    fn v113_to_raw_missing_system() {
        assert_eq!(
            StataLong::Missing(MissingValue::System)
                .to_raw(Release::V113)
                .unwrap(),
            0x7FFF_FFE5,
        );
    }

    #[test]
    fn v104_to_raw_missing_system() {
        assert_eq!(
            StataLong::Missing(MissingValue::System)
                .to_raw(Release::V104)
                .unwrap(),
            0x7FFF_FFFF,
        );
    }

    // -----------------------------------------------------------------------
    // to_raw — Tagged missings
    // -----------------------------------------------------------------------

    #[test]
    fn v113_to_raw_missing_a() {
        assert_eq!(
            StataLong::Missing(MissingValue::A)
                .to_raw(Release::V113)
                .unwrap(),
            0x7FFF_FFE6,
        );
    }

    #[test]
    fn v113_to_raw_missing_z() {
        assert_eq!(
            StataLong::Missing(MissingValue::Z)
                .to_raw(Release::V113)
                .unwrap(),
            0x7FFF_FFFF,
        );
    }

    #[test]
    fn v104_to_raw_missing_tagged_errors() {
        assert_eq!(
            StataLong::Missing(MissingValue::A).to_raw(Release::V104),
            Err(StataError::TaggedMissingUnsupported),
        );
    }

    #[test]
    fn v112_to_raw_missing_tagged_errors() {
        assert_eq!(
            StataLong::Missing(MissingValue::Z).to_raw(Release::V112),
            Err(StataError::TaggedMissingUnsupported),
        );
    }

    // -----------------------------------------------------------------------
    // present()
    // -----------------------------------------------------------------------

    #[test]
    fn present_returns_inner_for_present() {
        assert_eq!(StataLong::Present(100_000).present(), Some(100_000));
    }

    #[test]
    fn present_returns_none_for_missing() {
        assert_eq!(StataLong::Missing(MissingValue::System).present(), None);
        assert_eq!(StataLong::Missing(MissingValue::A).present(), None);
    }

    // -----------------------------------------------------------------------
    // From<StataByte> / From<StataInt>
    // -----------------------------------------------------------------------

    #[test]
    fn from_byte_present_widens() {
        assert_eq!(
            StataLong::from(StataByte::Present(42)),
            StataLong::Present(42),
        );
    }

    #[test]
    fn from_byte_missing_translates_directly() {
        assert_eq!(
            StataLong::from(StataByte::Missing(MissingValue::Z)),
            StataLong::Missing(MissingValue::Z),
        );
    }

    #[test]
    fn from_int_present_widens() {
        assert_eq!(
            StataLong::from(StataInt::Present(-32_768)),
            StataLong::Present(-32_768),
        );
    }

    #[test]
    fn from_int_missing_translates_directly() {
        assert_eq!(
            StataLong::from(StataInt::Missing(MissingValue::System)),
            StataLong::Missing(MissingValue::System),
        );
    }
}
