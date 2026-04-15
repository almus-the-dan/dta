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
