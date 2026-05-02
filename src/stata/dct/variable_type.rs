use core::fmt;

/// Storage type declared for a column in a `.dct` dictionary.
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
    /// A string column. Width — fixed or free — is described by the
    /// column's [`ReadFormat`](super::input_format::InputFormat).
    String,
}

impl fmt::Display for VariableType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Byte => "byte",
            Self::Int => "int",
            Self::Long => "long",
            Self::Float => "float",
            Self::Double => "double",
            Self::String => "str",
        })
    }
}
