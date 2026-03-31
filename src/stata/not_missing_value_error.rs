/// Error returned when a raw value does not represent a Stata missing value.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NotMissingValueError;

impl core::fmt::Display for NotMissingValueError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.write_str("value is not a Stata missing value")
    }
}

impl std::error::Error for NotMissingValueError {}
