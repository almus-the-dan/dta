use core::fmt;

/// Unified error type for Stata value parsing.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum StataError {
    /// A raw value does not encode a Stata missing value.
    NotMissingValue,
    /// A tagged missing value (`.a`–`.z`) was encountered in a
    /// context that only supports the system missing value (`.`) —
    /// pre-V113 DTA formats do not carry tagged missings.
    TaggedMissingUnsupported,
    /// A timestamp string does not match the expected DTA format
    /// `"dd Mon yyyy hh:mm"`.
    InvalidTimestamp,
}

impl fmt::Display for StataError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::NotMissingValue => "value is not a Stata missing value",
            Self::TaggedMissingUnsupported => {
                "tagged missing values (.a–.z) are not supported by this DTA format"
            }
            Self::InvalidTimestamp => "invalid DTA timestamp",
        })
    }
}

impl std::error::Error for StataError {}

/// Convenience alias for results using [`StataError`].
pub type Result<T> = std::result::Result<T, StataError>;
