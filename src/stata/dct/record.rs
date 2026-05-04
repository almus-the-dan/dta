use super::value::Value;

/// A single observation (row) parsed from a DCT-described data file.
///
/// String fields borrow from the reader's internal line buffer, so a
/// `DctRecord` must be dropped before the next call to
/// [`DctDataReader::read_record`](super::dct_reader::DctReader::read_record).
#[derive(Debug, Clone)]
pub struct Record<'a> {
    values: Vec<Value<'a>>,
}

impl<'a> Record<'a> {
    #[must_use]
    pub(crate) fn new(values: Vec<Value<'a>>) -> Self {
        Self { values }
    }

    /// The parsed values, one per column in the schema.
    #[must_use]
    #[inline]
    pub fn values(&self) -> &[Value<'a>] {
        &self.values
    }
}
