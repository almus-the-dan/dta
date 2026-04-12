use core::fmt;

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
