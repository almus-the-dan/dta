/// A value from a Stata "float" variable (4-byte IEEE 754 float).
///
/// In DTA format 113+, Stata reserves specific NaN bit patterns at and above
/// `0x7F00_0000` as missing-value sentinels (`.`, `.a`–`.z`). Values below
/// that bit pattern are data.
///
/// In pre-113 formats, only bit patterns above `+MAX_VALID_FLOAT` (roughly
/// `1.7014e38`) encode system missing (`.`); tagged missing values are
/// unrepresentable.
///
/// # Examples
///
/// ```
/// use dta::stata::dta::release::Release;
/// use dta::stata::missing_value::MissingValue;
/// use dta::stata::stata_float::StataFloat;
///
/// let present = StataFloat::from_raw(3.14_f32, Release::V117).unwrap();
/// assert_eq!(present, StataFloat::Present(3.14));
///
/// let missing = StataFloat::from_raw(f32::from_bits(0x7F00_0000), Release::V117).unwrap();
/// assert_eq!(missing, StataFloat::Missing(MissingValue::System));
/// ```
use super::dta::release::Release;
use super::missing_value::MissingValue;
use super::stata_error::{Result, StataError};

/// Bit pattern at or above which an `f32` encodes a Stata missing value in
/// DTA 113+.
const MISSING_FLOAT_SYSTEM_113: u32 = 0x7F00_0000;

/// Bit pattern encoding tagged missing `.a` for `f32` in DTA 113+.
const MISSING_FLOAT_A_113: u32 = 0x7F00_0800;

/// Stride between consecutive tagged missing `f32` bit patterns in DTA 113+.
const MISSING_FLOAT_STRIDE: u32 = 0x0800;

/// Bit pattern of the largest positive IEEE 754 single-precision value
/// considered valid data in pre-113 formats. Anything greater is treated
/// as system missing on read. Matches pandas' `OLD_VALID_RANGE` max for
/// float32 (roughly `1.7014117e38`).
const PRE_113_FLOAT_MAX_VALID_BITS: u32 = 0x7EFF_FFFF;

/// Bit pattern emitted as system missing when writing a pre-113 file.
/// Decodes to `1.7014118e38`, which the reader here and pandas both treat
/// as missing via the range check above.
const MISSING_FLOAT_SYSTEM_PRE_113: u32 = 0x7F00_0000;

/// A Stata float: either a present `f32` value or a [`MissingValue`].
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum StataFloat {
    /// A present data value.
    Present(f32),
    /// A missing value (`.` in any release; `.a`–`.z` in DTA 113+ only).
    Missing(MissingValue),
}

impl StataFloat {
    /// Decode an `f32` read from a DTA file as a Stata float.
    ///
    /// The decoding rules depend on `release`:
    ///
    /// - **DTA 113+**: bit patterns at or above `0x7F00_0000` (sign bit
    ///   clear) are missing. Exact matches of the 27 sentinel patterns
    ///   map to `.`, `.a`, …, `.z`; other patterns in that range error.
    /// - **Pre-DTA 113**: any positive value greater than the old
    ///   valid-range maximum (~`1.7014117e38`) is treated as system
    ///   missing (`.`). This matches pandas' `OLD_VALID_RANGE` check.
    ///
    /// # Errors
    ///
    /// Returns [`StataError::NotMissingValue`] if a DTA 113+ value's bit
    /// pattern falls in the missing range but does not match any of the
    /// 27 sentinels. Pre-113 decoding never returns this error.
    pub fn from_raw(raw: f32, release: Release) -> Result<Self> {
        let bits = raw.to_bits();
        let is_positive = bits & 0x8000_0000 == 0;
        if release.supports_tagged_missing() {
            if is_positive && bits >= MISSING_FLOAT_SYSTEM_113 {
                Ok(Self::Missing(MissingValue::try_from(raw)?))
            } else {
                Ok(Self::Present(raw))
            }
        } else if is_positive && bits > PRE_113_FLOAT_MAX_VALID_BITS {
            Ok(Self::Missing(MissingValue::System))
        } else {
            Ok(Self::Present(raw))
        }
    }

    /// Encode this value as a raw `f32` for a DTA file.
    ///
    /// # Errors
    ///
    /// Returns [`StataError::TaggedMissingUnsupported`] if `self` is a
    /// tagged missing (`.a`–`.z`) and `release` is pre-113.
    pub fn to_raw(self, release: Release) -> Result<f32> {
        match self {
            Self::Present(v) => Ok(v),
            Self::Missing(mv) => {
                if release.supports_tagged_missing() {
                    let offset = u32::from(mv.code());
                    let bits = if offset == 0 {
                        MISSING_FLOAT_SYSTEM_113
                    } else {
                        MISSING_FLOAT_A_113 + (offset - 1) * MISSING_FLOAT_STRIDE
                    };
                    Ok(f32::from_bits(bits))
                } else if mv == MissingValue::System {
                    Ok(f32::from_bits(MISSING_FLOAT_SYSTEM_PRE_113))
                } else {
                    Err(StataError::TaggedMissingUnsupported)
                }
            }
        }
    }

    /// Returns the underlying `f32` when this value is
    /// [`Present`](Self::Present), or `None` when it is
    /// [`Missing`](Self::Missing).
    #[must_use]
    #[inline]
    pub fn present(self) -> Option<f32> {
        match self {
            Self::Present(v) => Some(v),
            Self::Missing(_) => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use float_cmp::assert_approx_eq;

    // -----------------------------------------------------------------------
    // DTA 113+ — Present values
    // -----------------------------------------------------------------------

    #[test]
    fn v113_present_zero() {
        assert_eq!(
            StataFloat::from_raw(0.0_f32, Release::V113).unwrap(),
            StataFloat::Present(0.0),
        );
    }

    #[test]
    fn v113_present_negative_zero() {
        assert_eq!(
            StataFloat::from_raw(-0.0_f32, Release::V113).unwrap(),
            StataFloat::Present(-0.0),
        );
    }

    #[test]
    fn v113_present_one() {
        assert_eq!(
            StataFloat::from_raw(1.0_f32, Release::V113).unwrap(),
            StataFloat::Present(1.0),
        );
    }

    #[test]
    fn v113_present_negative() {
        assert_eq!(
            StataFloat::from_raw(-1.5_f32, Release::V113).unwrap(),
            StataFloat::Present(-1.5),
        );
    }

    #[test]
    fn v113_present_large_just_below_missing_range() {
        let val = f32::from_bits(MISSING_FLOAT_SYSTEM_113 - 1);
        assert_eq!(
            StataFloat::from_raw(val, Release::V113).unwrap(),
            StataFloat::Present(val),
        );
    }

    #[test]
    fn v113_present_negative_infinity() {
        assert_eq!(
            StataFloat::from_raw(f32::NEG_INFINITY, Release::V113).unwrap(),
            StataFloat::Present(f32::NEG_INFINITY),
        );
    }

    // -----------------------------------------------------------------------
    // DTA 113+ — Errors on unrecognized NaN bit patterns
    // -----------------------------------------------------------------------

    #[test]
    fn v113_error_non_stata_nan() {
        let val = f32::from_bits(0x7F00_0001);
        assert_eq!(
            StataFloat::from_raw(val, Release::V113),
            Err(StataError::NotMissingValue),
        );
    }

    #[test]
    fn v113_error_positive_infinity() {
        assert_eq!(
            StataFloat::from_raw(f32::INFINITY, Release::V113),
            Err(StataError::NotMissingValue),
        );
    }

    // -----------------------------------------------------------------------
    // DTA 113+ — Missing values
    // -----------------------------------------------------------------------

    #[test]
    fn v113_missing_system() {
        assert_eq!(
            StataFloat::from_raw(f32::from_bits(0x7F00_0000), Release::V113).unwrap(),
            StataFloat::Missing(MissingValue::System),
        );
    }

    #[test]
    fn v113_missing_a() {
        assert_eq!(
            StataFloat::from_raw(f32::from_bits(0x7F00_0800), Release::V113).unwrap(),
            StataFloat::Missing(MissingValue::A),
        );
    }

    #[test]
    fn v113_missing_z() {
        assert_eq!(
            StataFloat::from_raw(f32::from_bits(0x7F00_D000), Release::V113).unwrap(),
            StataFloat::Missing(MissingValue::Z),
        );
    }

    // -----------------------------------------------------------------------
    // Pre-113 — Present values
    // -----------------------------------------------------------------------

    #[test]
    fn v104_present_zero() {
        assert_eq!(
            StataFloat::from_raw(0.0_f32, Release::V104).unwrap(),
            StataFloat::Present(0.0),
        );
    }

    #[test]
    fn v104_present_normal() {
        assert_eq!(
            StataFloat::from_raw(42.0_f32, Release::V104).unwrap(),
            StataFloat::Present(42.0),
        );
    }

    #[test]
    fn v104_present_negative() {
        assert_eq!(
            StataFloat::from_raw(-1.5_f32, Release::V104).unwrap(),
            StataFloat::Present(-1.5),
        );
    }

    #[test]
    fn v104_present_negative_infinity() {
        // Negative infinity has sign bit set — never flagged as missing
        // by the positive-only range check.
        assert_eq!(
            StataFloat::from_raw(f32::NEG_INFINITY, Release::V104).unwrap(),
            StataFloat::Present(f32::NEG_INFINITY),
        );
    }

    #[test]
    fn v104_present_max_valid() {
        let val = f32::from_bits(PRE_113_FLOAT_MAX_VALID_BITS);
        assert_eq!(
            StataFloat::from_raw(val, Release::V104).unwrap(),
            StataFloat::Present(val),
        );
    }

    // -----------------------------------------------------------------------
    // Pre-113 — Missing values (range check)
    // -----------------------------------------------------------------------

    #[test]
    fn v104_missing_system_canonical() {
        // The actual bit pattern found in real v104 files.
        assert_eq!(
            StataFloat::from_raw(f32::from_bits(0x7F00_0000), Release::V104).unwrap(),
            StataFloat::Missing(MissingValue::System),
        );
    }

    #[test]
    fn v104_missing_system_above_max() {
        // Any positive value above valid-max is treated as missing.
        assert_eq!(
            StataFloat::from_raw(f32::from_bits(0x7F00_0001), Release::V104).unwrap(),
            StataFloat::Missing(MissingValue::System),
        );
    }

    #[test]
    fn v104_missing_positive_infinity() {
        assert_eq!(
            StataFloat::from_raw(f32::INFINITY, Release::V104).unwrap(),
            StataFloat::Missing(MissingValue::System),
        );
    }

    #[test]
    fn v112_missing_system() {
        assert_eq!(
            StataFloat::from_raw(f32::from_bits(0x7F00_0000), Release::V112).unwrap(),
            StataFloat::Missing(MissingValue::System),
        );
    }

    // -----------------------------------------------------------------------
    // to_raw — Present round-trip
    // -----------------------------------------------------------------------

    #[test]
    fn v113_to_raw_present_zero() {
        assert_approx_eq!(
            f32,
            StataFloat::Present(0.0).to_raw(Release::V113).unwrap(),
            0.0
        );
    }

    #[test]
    fn v113_to_raw_present_normal() {
        assert_approx_eq!(
            f32,
            StataFloat::Present(1.5).to_raw(Release::V113).unwrap(),
            1.5
        );
    }

    #[test]
    fn v113_to_raw_present_negative() {
        assert_approx_eq!(
            f32,
            StataFloat::Present(-1.5).to_raw(Release::V113).unwrap(),
            -1.5
        );
    }

    // -----------------------------------------------------------------------
    // to_raw — Missing values
    // -----------------------------------------------------------------------

    #[test]
    fn v113_to_raw_missing_system() {
        let got = StataFloat::Missing(MissingValue::System)
            .to_raw(Release::V113)
            .unwrap();
        assert_eq!(got.to_bits(), 0x7F00_0000);
    }

    #[test]
    fn v113_to_raw_missing_a() {
        let got = StataFloat::Missing(MissingValue::A)
            .to_raw(Release::V113)
            .unwrap();
        assert_eq!(got.to_bits(), 0x7F00_0800);
    }

    #[test]
    fn v113_to_raw_missing_z() {
        let got = StataFloat::Missing(MissingValue::Z)
            .to_raw(Release::V113)
            .unwrap();
        assert_eq!(got.to_bits(), 0x7F00_D000);
    }

    #[test]
    fn v104_to_raw_missing_system() {
        let got = StataFloat::Missing(MissingValue::System)
            .to_raw(Release::V104)
            .unwrap();
        assert_eq!(got.to_bits(), 0x7F00_0000);
    }

    #[test]
    fn v104_to_raw_missing_tagged_errors() {
        assert_eq!(
            StataFloat::Missing(MissingValue::A).to_raw(Release::V104),
            Err(StataError::TaggedMissingUnsupported),
        );
    }

    #[test]
    fn v112_to_raw_missing_tagged_errors() {
        assert_eq!(
            StataFloat::Missing(MissingValue::Z).to_raw(Release::V112),
            Err(StataError::TaggedMissingUnsupported),
        );
    }

    // -----------------------------------------------------------------------
    // present()
    // -----------------------------------------------------------------------

    #[test]
    fn present_returns_inner_for_present() {
        assert_eq!(StataFloat::Present(2.5).present(), Some(2.5));
    }

    #[test]
    fn present_returns_none_for_missing() {
        assert_eq!(StataFloat::Missing(MissingValue::System).present(), None);
        assert_eq!(StataFloat::Missing(MissingValue::A).present(), None);
    }
}
