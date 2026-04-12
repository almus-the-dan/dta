/// A reference to a long string stored in the strL section of the file.
///
/// In the data section, strL variables are encoded as a
/// `(variable, observation)` pair that points into the long string
/// section. These references are unresolved until the long string
/// section is read.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct LongStringRef {
    variable: u32,
    observation: u64,
}

impl LongStringRef {
    /// One-based variable index.
    #[must_use]
    #[inline]
    pub fn variable(&self) -> u32 {
        self.variable
    }

    /// One-based observation index.
    #[must_use]
    #[inline]
    pub fn observation(&self) -> u64 {
        self.observation
    }
}
