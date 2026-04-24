use super::long_string::LongString;

/// A reference to a long string stored in the strL section of the file.
///
/// In the data section, strL variables are encoded as a
/// `(variable, observation)` pair that points into the long string
/// section. These references are unresolved until the long string
/// section is read.
///
/// The `observation` component is the one-based index of the first
/// observation where a given string value appeared. Together with
/// `variable`, it forms a unique key for looking up the string
/// content in the strL section — identical strings that first appear
/// in the same cell share the same `(variable, observation)` pair.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct LongStringRef {
    variable: u32,
    observation: u64,
}

impl LongStringRef {
    #[must_use]
    pub(crate) fn new(variable: u32, observation: u64) -> Self {
        Self {
            variable,
            observation,
        }
    }

    /// One-based variable index.
    #[must_use]
    #[inline]
    pub fn variable(&self) -> u32 {
        self.variable
    }

    /// One-based index of the first observation where this string content
    /// appeared. Acts as part of the `(variable, observation)` lookup key
    /// into the strL section, not necessarily the current row.
    #[must_use]
    #[inline]
    pub fn observation(&self) -> u64 {
        self.observation
    }
}

impl From<&LongString<'_>> for LongStringRef {
    #[inline]
    fn from(long_string: &LongString<'_>) -> Self {
        Self::new(long_string.variable(), long_string.observation())
    }
}

#[cfg(test)]
mod tests {
    use std::borrow::Cow;

    use super::*;

    #[test]
    fn from_long_string_copies_variable_and_observation() {
        let long_string = LongString::new(7, 42, false, Cow::Borrowed(b"hello"));
        let long_string_ref = LongStringRef::from(&long_string);
        assert_eq!(long_string_ref.variable(), 7);
        assert_eq!(long_string_ref.observation(), 42);
    }

    #[test]
    fn from_long_string_is_non_consuming() {
        let long_string = LongString::new(1, 2, false, Cow::Borrowed(b"data"));
        let _ref_a = LongStringRef::from(&long_string);
        // Still usable because `From<&LongString<'_>>` borrows.
        let _ref_b = LongStringRef::from(&long_string);
        assert_eq!(long_string.data(), b"data");
    }
}
