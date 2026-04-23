use super::value::Value;

/// A single observation (row) from the data section of a DTA file.
///
/// Contains one [`Value`] per variable in the schema, parsed eagerly
/// when the record is read. Borrows both the value slice and any
/// contained string data from the reader's internal buffers, so the
/// record must be dropped before the next call to `read_record`.
#[derive(Debug, Clone, Copy)]
pub struct Record<'a> {
    values: &'a [Value<'a>],
}

impl<'a> Record<'a> {
    #[must_use]
    pub(crate) fn new(values: &'a [Value<'a>]) -> Self {
        Self { values }
    }

    /// The parsed values, one per variable in the schema.
    #[must_use]
    #[inline]
    pub fn values(&self) -> &[Value<'a>] {
        self.values
    }
}
