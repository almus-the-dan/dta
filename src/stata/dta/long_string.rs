use std::borrow::Cow;

/// A decoded long string (strL) entry from the DTA file.
///
/// Each long string is associated with a specific variable and
/// observation, matching the [`LongStringRef`](super::long_string_ref::LongStringRef)
/// encountered in the data section.
///
/// The string value borrows from the reader's buffer when possible
/// (e.g., when the source is already valid UTF-8) and allocates only
/// when encoding conversion is required.
#[derive(Debug, Clone)]
pub struct LongString<'a> {
    variable: u32,
    observation: u64,
    value: Cow<'a, str>,
}

impl LongString<'_> {
    /// One-based variable index.
    #[must_use]
    pub fn variable(&self) -> u32 {
        self.variable
    }

    /// One-based observation index.
    #[must_use]
    pub fn observation(&self) -> u64 {
        self.observation
    }

    /// The decoded string value.
    #[must_use]
    pub fn value(&self) -> &str {
        &self.value
    }
}
