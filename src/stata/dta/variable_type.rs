use core::fmt;

use super::release::Release;

/// A variable type in a DTA file.
///
/// Each variant corresponds to a Stata storage type and determines
/// how many bytes the variable occupies in each data row.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum VariableType {
    /// 1-byte signed integer (`byte` in Stata).
    Byte,
    /// 2-byte signed integer (`int` in Stata).
    Int,
    /// 4-byte signed integer (`long` in Stata).
    Long,
    /// 4-byte IEEE 754 float (`float` in Stata).
    Float,
    /// 8-byte IEEE 754 double (`double` in Stata).
    Double,
    /// Fixed-length string (`str1`–`str2045` in Stata).
    ///
    /// The value is the maximum byte length of the string in a data
    /// row.
    FixedString(u16),
    /// Long string reference (`strL` in Stata, format 117+).
    ///
    /// Occupies 8 bytes in the data row as a (variable, observation)
    /// reference pair that resolves to a string in the strL section.
    LongString,
}

impl VariableType {
    /// Number of bytes this type occupies in a data row.
    #[must_use]
    #[inline]
    pub(crate) fn width(self) -> usize {
        match self {
            Self::Byte => 1,
            Self::Int => 2,
            Self::Long | Self::Float => 4,
            Self::Double | Self::LongString => 8,
            Self::FixedString(len) => usize::from(len),
        }
    }

    /// Encodes this type as the raw type code stored in a DTA file's
    /// type list for the given `release`.
    ///
    /// Returns `None` when the type cannot be represented (e.g.,
    /// [`LongString`](Self::LongString) for pre-117 formats, or a
    /// [`FixedString`](Self::FixedString) whose length exceeds
    /// [`Release::max_fixed_string_len`]).
    ///
    /// Mirrors the `parse_type_code` reader logic:
    ///
    /// | Formats   | Numeric codes     | String codes            |
    /// |-----------|-------------------|-------------------------|
    /// | 104–110   | ASCII `b/i/l/f/d` | `0x80 + len`            |
    /// | 111–116   | `0xFB`–`0xFF`     | code = byte length      |
    /// | 117+      | `0xFFF6`–`0xFFFA` | code = byte length      |
    /// |           | `0x8000` = strL   |                         |
    #[must_use]
    pub(crate) fn try_to_u16(self, release: Release) -> Option<u16> {
        if release >= Release::V117 {
            self.try_to_u16_v117_plus(release)
        } else if release >= Release::V111 {
            self.try_to_u16_v111_v116(release)
        } else {
            self.try_to_u16_v104_v110(release)
        }
    }

    /// Encoding for formats 117+: 2-byte codes `0xFFF6`–`0xFFFA`
    /// cover the numerics, `0x8000` is `strL`, and fixed strings
    /// map directly to their byte length.
    fn try_to_u16_v117_plus(self, release: Release) -> Option<u16> {
        match self {
            Self::Byte => Some(0xFFFA),
            Self::Int => Some(0xFFF9),
            Self::Long => Some(0xFFF8),
            Self::Float => Some(0xFFF7),
            Self::Double => Some(0xFFF6),
            Self::LongString => Some(0x8000),
            Self::FixedString(len) if (1..=release.max_fixed_string_len()).contains(&len) => {
                Some(len)
            }
            Self::FixedString(_) => None,
        }
    }

    /// Encoding for formats 111–116: 1-byte codes `0xFB`–`0xFF`
    /// cover the numerics and fixed strings map to their byte
    /// length. `strL` is not representable.
    fn try_to_u16_v111_v116(self, release: Release) -> Option<u16> {
        match self {
            Self::Byte => Some(0xFB),
            Self::Int => Some(0xFC),
            Self::Long => Some(0xFD),
            Self::Float => Some(0xFE),
            Self::Double => Some(0xFF),
            Self::FixedString(len) if (1..=release.max_fixed_string_len()).contains(&len) => {
                Some(len)
            }
            Self::LongString | Self::FixedString(_) => None,
        }
    }

    /// Encoding for formats 104–110: ASCII letters `b/i/l/f/d`
    /// for numerics, fixed strings as `0x80 + (len - 1)` (i.e.
    /// `len + 0x7F`). `strL` is not representable.
    fn try_to_u16_v104_v110(self, release: Release) -> Option<u16> {
        match self {
            Self::Byte => Some(u16::from(b'b')),
            Self::Int => Some(u16::from(b'i')),
            Self::Long => Some(u16::from(b'l')),
            Self::Float => Some(u16::from(b'f')),
            Self::Double => Some(u16::from(b'd')),
            Self::FixedString(len) if (1..=release.max_fixed_string_len()).contains(&len) => {
                Some(len + 0x7F)
            }
            Self::LongString | Self::FixedString(_) => None,
        }
    }
}

impl fmt::Display for VariableType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Byte => f.write_str("byte"),
            Self::Int => f.write_str("int"),
            Self::Long => f.write_str("long"),
            Self::Float => f.write_str("float"),
            Self::Double => f.write_str("double"),
            Self::FixedString(len) => write!(f, "str{len}"),
            Self::LongString => f.write_str("strL"),
        }
    }
}
