use super::value::Value;

/// A single observation (row) from the data section of a DTA file.
///
/// Contains one [`Value`] per variable in the schema, parsed eagerly
/// when the record is read. Borrows string data from the reader's
/// internal buffer.
#[derive(Debug, Clone)]
pub struct Record<'a> {
    values: Vec<Value<'a>>,
}

impl<'a> Record<'a> {
    #[must_use]
    pub(crate) fn new(values: Vec<Value<'a>>) -> Self {
        Self { values }
    }

    /// The parsed values, one per variable in the schema.
    #[must_use]
    #[inline]
    pub fn values(&self) -> &[Value<'a>] {
        &self.values
    }
}
