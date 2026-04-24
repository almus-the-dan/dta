/// A value from a Stata "double" variable (8-byte IEEE 754 double).
///
/// In DTA format 113+, Stata reserves NaN bit patterns at and above
/// `0x7FE0_0000_0000_0000` as missing-value sentinels (`.`, `.a`–`.z`).
/// Values below that bit pattern are data.
///
/// Pre-113 formats have *two* different double-missing schemes:
///
/// - **V104 and V105 only**: the system missing value is the specific
///   bit pattern `0x54C0_0000_0000_0000` (= `2^333` ≈ `1.7472e100`).
///   This number falls well inside the normal valid IEEE range, so a
///   simple range check cannot catch it — the exact bit pattern must
///   match.
/// - **V106 through V112**: any positive value above `+8.988e307`
///   (pandas' `OLD_VALID_RANGE` maximum) is treated as system missing.
///
/// Tagged missings (`.a`–`.z`) are unrepresentable in any pre-113 format.
///
/// # Examples
///
/// ```
/// use dta::stata::dta::release::Release;
/// use dta::stata::missing_value::MissingValue;
/// use dta::stata::stata_double::StataDouble;
///
/// let present = StataDouble::from_raw(3.14_f64, Release::V117).unwrap();
/// assert_eq!(present, StataDouble::Present(3.14));
///
/// let missing = StataDouble::from_raw(f64::from_bits(0x7FE0_0000_0000_0000), Release::V117).unwrap();
/// assert_eq!(missing, StataDouble::Missing(MissingValue::System));
/// ```
use super::dta::release::Release;
use super::missing_value::MissingValue;
use super::stata_error::{Result, StataError};

/// Bit pattern at or above which an `f64` encodes a Stata missing value
/// in DTA 113+.
const MISSING_DOUBLE_SYSTEM_113: u64 = 0x7FE0_0000_0000_0000;

/// Bit pattern encoding tagged missing `.a` for `f64` in DTA 113+.
const MISSING_DOUBLE_A_113: u64 = 0x7FE0_0100_0000_0000;

/// Stride between consecutive tagged missing `f64` bit patterns in DTA 113+.
const MISSING_DOUBLE_STRIDE: u64 = 0x0100_0000_0000;

/// Legacy V104/V105 system missing: `2^333` (≈ `1.7472e100`).
const MISSING_DOUBLE_SYSTEM_V104: u64 = 0x54C0_0000_0000_0000;

/// Bit pattern of the largest positive IEEE 754 double-precision value
/// considered valid data in pre-113 formats (V106–V112). Matches pandas'
/// `OLD_VALID_RANGE` max (≈ `8.988e307`).
const PRE_113_DOUBLE_MAX_VALID_BITS: u64 = 0x7FDF_FFFF_FFFF_FFFF;

/// Bit pattern emitted as system missing when writing a V106–V112 file.
/// `0x7FEF_FFFF_FFFF_FFFF` is `+MAX_DOUBLE` (≈ `1.7977e308`) — well above
/// the old valid-range maximum so that pandas and our own reader both
/// recognize it as missing.
const MISSING_DOUBLE_SYSTEM_V106_V112: u64 = 0x7FEF_FFFF_FFFF_FFFF;

/// A Stata double: either a present `f64` value or a [`MissingValue`].
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum StataDouble {
    /// A present data value.
    Present(f64),
    /// A missing value (`.` in any release; `.a`–`.z` in DTA 113+ only).
    Missing(MissingValue),
}

impl StataDouble {
    /// Decode an `f64` read from a DTA file as a Stata double.
    ///
    /// See the module-level docs for the per-release sentinel rules.
    ///
    /// # Errors
    ///
    /// Returns [`StataError::NotMissingValue`] if a DTA 113+ bit pattern
    /// falls in the missing range but does not match any of the 27
    /// sentinels. Pre-113 decoding never returns this error.
    pub fn from_raw(raw: f64, release: Release) -> Result<Self> {
        let bits = raw.to_bits();
        let is_positive = bits & 0x8000_0000_0000_0000 == 0;

        if release.supports_tagged_missing() {
            if is_positive && bits >= MISSING_DOUBLE_SYSTEM_113 {
                return Ok(Self::Missing(MissingValue::try_from(raw)?));
            }
            return Ok(Self::Present(raw));
        }

        // V104/V105: the magic 2^333 sentinel lives inside the valid
        // IEEE range, so it must be matched exactly. Fall through to the
        // range check afterward so a V104 file carrying an out-of-range
        // positive value (e.g., a modern sentinel a loose writer emitted)
        // is also recognized.
        if release.uses_magic_double_missing() && bits == MISSING_DOUBLE_SYSTEM_V104 {
            return Ok(Self::Missing(MissingValue::System));
        }

        if is_positive && bits > PRE_113_DOUBLE_MAX_VALID_BITS {
            Ok(Self::Missing(MissingValue::System))
        } else {
            Ok(Self::Present(raw))
        }
    }

    /// Encode this value as a raw `f64` for a DTA file.
    ///
    /// # Errors
    ///
    /// Returns [`StataError::TaggedMissingUnsupported`] if `self` is a
    /// tagged missing (`.a`–`.z`) and `release` is pre-113.
    pub fn to_raw(self, release: Release) -> Result<f64> {
        match self {
            Self::Present(v) => Ok(v),
            Self::Missing(mv) => {
                if release.supports_tagged_missing() {
                    let offset = u64::from(mv.code());
                    let bits = if offset == 0 {
                        MISSING_DOUBLE_SYSTEM_113
                    } else {
                        MISSING_DOUBLE_A_113 + (offset - 1) * MISSING_DOUBLE_STRIDE
                    };
                    Ok(f64::from_bits(bits))
                } else if mv == MissingValue::System {
                    let bits = if release.uses_magic_double_missing() {
                        MISSING_DOUBLE_SYSTEM_V104
                    } else {
                        MISSING_DOUBLE_SYSTEM_V106_V112
                    };
                    Ok(f64::from_bits(bits))
                } else {
                    Err(StataError::TaggedMissingUnsupported)
                }
            }
        }
    }

    /// Returns the underlying `f64` when this value is
    /// [`Present`](Self::Present), or `None` when it is
    /// [`Missing`](Self::Missing).
    #[must_use]
    #[inline]
    pub fn present(self) -> Option<f64> {
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
            StataDouble::from_raw(0.0_f64, Release::V113).unwrap(),
            StataDouble::Present(0.0),
        );
    }

    #[test]
    fn v113_present_negative_zero() {
        assert_eq!(
            StataDouble::from_raw(-0.0_f64, Release::V113).unwrap(),
            StataDouble::Present(-0.0),
        );
    }

    #[test]
    fn v113_present_one() {
        assert_eq!(
            StataDouble::from_raw(1.0_f64, Release::V113).unwrap(),
            StataDouble::Present(1.0),
        );
    }

    #[test]
    fn v113_present_negative() {
        assert_eq!(
            StataDouble::from_raw(-1.5_f64, Release::V113).unwrap(),
            StataDouble::Present(-1.5),
        );
    }

    #[test]
    fn v113_present_large_just_below_missing_range() {
        let val = f64::from_bits(MISSING_DOUBLE_SYSTEM_113 - 1);
        assert_eq!(
            StataDouble::from_raw(val, Release::V113).unwrap(),
            StataDouble::Present(val),
        );
    }

    #[test]
    fn v113_present_negative_infinity() {
        assert_eq!(
            StataDouble::from_raw(f64::NEG_INFINITY, Release::V113).unwrap(),
            StataDouble::Present(f64::NEG_INFINITY),
        );
    }

    // -----------------------------------------------------------------------
    // DTA 113+ — Errors on unrecognized NaN
    // -----------------------------------------------------------------------

    #[test]
    fn v113_error_non_stata_nan() {
        let val = f64::from_bits(0x7FE0_0000_0000_0001);
        assert_eq!(
            StataDouble::from_raw(val, Release::V113),
            Err(StataError::NotMissingValue),
        );
    }

    #[test]
    fn v113_error_positive_infinity() {
        assert_eq!(
            StataDouble::from_raw(f64::INFINITY, Release::V113),
            Err(StataError::NotMissingValue),
        );
    }

    // -----------------------------------------------------------------------
    // DTA 113+ — Missing values
    // -----------------------------------------------------------------------

    #[test]
    fn v113_missing_system() {
        assert_eq!(
            StataDouble::from_raw(f64::from_bits(0x7FE0_0000_0000_0000), Release::V113).unwrap(),
            StataDouble::Missing(MissingValue::System),
        );
    }

    #[test]
    fn v113_missing_a() {
        assert_eq!(
            StataDouble::from_raw(f64::from_bits(0x7FE0_0100_0000_0000), Release::V113).unwrap(),
            StataDouble::Missing(MissingValue::A),
        );
    }

    #[test]
    fn v113_missing_z() {
        assert_eq!(
            StataDouble::from_raw(f64::from_bits(0x7FE0_1A00_0000_0000), Release::V113).unwrap(),
            StataDouble::Missing(MissingValue::Z),
        );
    }

    // -----------------------------------------------------------------------
    // V104/V105 — Magic 2^333 sentinel
    // -----------------------------------------------------------------------

    #[test]
    fn v104_missing_system_magic() {
        // 2^333 ≈ 1.7472e100 — the exact bit pattern found in real v104 files.
        assert_eq!(
            StataDouble::from_raw(f64::from_bits(0x54C0_0000_0000_0000), Release::V104).unwrap(),
            StataDouble::Missing(MissingValue::System),
        );
    }

    #[test]
    fn v105_missing_system_magic() {
        assert_eq!(
            StataDouble::from_raw(f64::from_bits(0x54C0_0000_0000_0000), Release::V105).unwrap(),
            StataDouble::Missing(MissingValue::System),
        );
    }

    #[test]
    fn v104_present_value_just_below_magic() {
        // Values near-but-not-equal to the magic sentinel are data.
        let val = f64::from_bits(0x54C0_0000_0000_0001);
        assert_eq!(
            StataDouble::from_raw(val, Release::V104).unwrap(),
            StataDouble::Present(val),
        );
    }

    #[test]
    fn v104_present_normal() {
        assert_eq!(
            StataDouble::from_raw(2.5_f64, Release::V104).unwrap(),
            StataDouble::Present(2.5),
        );
    }

    // -----------------------------------------------------------------------
    // V106–V112 — Range check
    // -----------------------------------------------------------------------

    #[test]
    fn v106_present_normal() {
        assert_eq!(
            StataDouble::from_raw(42.0_f64, Release::V106).unwrap(),
            StataDouble::Present(42.0),
        );
    }

    #[test]
    fn v106_present_max_valid() {
        let val = f64::from_bits(PRE_113_DOUBLE_MAX_VALID_BITS);
        assert_eq!(
            StataDouble::from_raw(val, Release::V106).unwrap(),
            StataDouble::Present(val),
        );
    }

    #[test]
    fn v106_missing_system_above_max() {
        let val = f64::from_bits(PRE_113_DOUBLE_MAX_VALID_BITS + 1);
        assert_eq!(
            StataDouble::from_raw(val, Release::V106).unwrap(),
            StataDouble::Missing(MissingValue::System),
        );
    }

    #[test]
    fn v106_missing_system_at_max_double() {
        // +MAX_DOUBLE — the value our writer emits for V106–V112 missings.
        assert_eq!(
            StataDouble::from_raw(
                f64::from_bits(MISSING_DOUBLE_SYSTEM_V106_V112),
                Release::V106
            )
            .unwrap(),
            StataDouble::Missing(MissingValue::System),
        );
    }

    #[test]
    fn v106_present_magic_v104_value_is_data() {
        // V106+ doesn't have the magic sentinel — 2^333 is just a number.
        let val = f64::from_bits(MISSING_DOUBLE_SYSTEM_V104);
        assert_eq!(
            StataDouble::from_raw(val, Release::V106).unwrap(),
            StataDouble::Present(val),
        );
    }

    #[test]
    fn v112_missing_system_positive_infinity() {
        assert_eq!(
            StataDouble::from_raw(f64::INFINITY, Release::V112).unwrap(),
            StataDouble::Missing(MissingValue::System),
        );
    }

    #[test]
    fn v112_present_negative_infinity() {
        // Negative infinity has sign bit set → not flagged by positive range.
        assert_eq!(
            StataDouble::from_raw(f64::NEG_INFINITY, Release::V112).unwrap(),
            StataDouble::Present(f64::NEG_INFINITY),
        );
    }

    // -----------------------------------------------------------------------
    // to_raw — Present round-trip
    // -----------------------------------------------------------------------

    #[test]
    fn v113_to_raw_present_zero() {
        assert_approx_eq!(
            f64,
            StataDouble::Present(0.0).to_raw(Release::V113).unwrap(),
            0.0
        );
    }

    #[test]
    fn v113_to_raw_present_positive() {
        assert_approx_eq!(
            f64,
            StataDouble::Present(1.5).to_raw(Release::V113).unwrap(),
            1.5
        );
    }

    #[test]
    fn v113_to_raw_present_negative() {
        assert_approx_eq!(
            f64,
            StataDouble::Present(-1.5).to_raw(Release::V113).unwrap(),
            -1.5
        );
    }

    // -----------------------------------------------------------------------
    // to_raw — Missing values: DTA 113+
    // -----------------------------------------------------------------------

    #[test]
    fn v113_to_raw_missing_system() {
        let got = StataDouble::Missing(MissingValue::System)
            .to_raw(Release::V113)
            .unwrap();
        assert_eq!(got.to_bits(), 0x7FE0_0000_0000_0000);
    }

    #[test]
    fn v113_to_raw_missing_a() {
        let got = StataDouble::Missing(MissingValue::A)
            .to_raw(Release::V113)
            .unwrap();
        assert_eq!(got.to_bits(), 0x7FE0_0100_0000_0000);
    }

    #[test]
    fn v113_to_raw_missing_z() {
        let got = StataDouble::Missing(MissingValue::Z)
            .to_raw(Release::V113)
            .unwrap();
        assert_eq!(got.to_bits(), 0x7FE0_1A00_0000_0000);
    }

    // -----------------------------------------------------------------------
    // to_raw — Missing values: pre-113
    // -----------------------------------------------------------------------

    #[test]
    fn v104_to_raw_missing_system_emits_magic() {
        let got = StataDouble::Missing(MissingValue::System)
            .to_raw(Release::V104)
            .unwrap();
        assert_eq!(got.to_bits(), MISSING_DOUBLE_SYSTEM_V104);
    }

    #[test]
    fn v105_to_raw_missing_system_emits_magic() {
        let got = StataDouble::Missing(MissingValue::System)
            .to_raw(Release::V105)
            .unwrap();
        assert_eq!(got.to_bits(), MISSING_DOUBLE_SYSTEM_V104);
    }

    #[test]
    fn v106_to_raw_missing_system_emits_max_double() {
        let got = StataDouble::Missing(MissingValue::System)
            .to_raw(Release::V106)
            .unwrap();
        assert_eq!(got.to_bits(), MISSING_DOUBLE_SYSTEM_V106_V112);
    }

    #[test]
    fn v112_to_raw_missing_system_emits_max_double() {
        let got = StataDouble::Missing(MissingValue::System)
            .to_raw(Release::V112)
            .unwrap();
        assert_eq!(got.to_bits(), MISSING_DOUBLE_SYSTEM_V106_V112);
    }

    #[test]
    fn v104_to_raw_missing_tagged_errors() {
        assert_eq!(
            StataDouble::Missing(MissingValue::A).to_raw(Release::V104),
            Err(StataError::TaggedMissingUnsupported),
        );
    }

    #[test]
    fn v112_to_raw_missing_tagged_errors() {
        assert_eq!(
            StataDouble::Missing(MissingValue::Z).to_raw(Release::V112),
            Err(StataError::TaggedMissingUnsupported),
        );
    }

    // -----------------------------------------------------------------------
    // present()
    // -----------------------------------------------------------------------

    #[test]
    fn present_returns_inner_for_present() {
        assert_eq!(StataDouble::Present(2.5).present(), Some(2.5));
    }

    #[test]
    fn present_returns_none_for_missing() {
        assert_eq!(StataDouble::Missing(MissingValue::System).present(), None);
        assert_eq!(StataDouble::Missing(MissingValue::A).present(), None);
    }
}
