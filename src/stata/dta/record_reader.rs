use std::io::{BufRead, Seek};

use super::dta_error::Result;
use super::header::Header;
use super::reader_state::ReaderState;
use super::schema::Schema;
use super::value_label_reader::ValueLabelReader;

/// Reads observation records from the data section of a DTA file.
///
/// Owns the parsed [`Header`] and [`Schema`] from previous
/// phases. Yields rows of [`Value`](super::value::Value) via
/// iteration, then transitions to value-label reading.
#[derive(Debug)]
pub struct RecordReader<R> {
    state: ReaderState<R>,
    header: Header,
    schema: Schema,
}

impl<R> RecordReader<R> {
    pub(crate) fn new(state: ReaderState<R>, header: Header, schema: Schema) -> Self {
        Self {
            state,
            header,
            schema,
        }
    }

    /// The parsed file header.
    #[must_use]
    #[inline]
    pub fn header(&self) -> &Header {
        &self.header
    }

    /// The parsed variable definitions.
    #[must_use]
    #[inline]
    pub fn schema(&self) -> &Schema {
        &self.schema
    }
}

impl<R: BufRead> RecordReader<R> {
    // TODO: iteration over rows yielding Value<'_> slices

    /// Consumes all remaining data records without processing them,
    /// then transitions to value-label reading.
    ///
    /// This is required before calling [`read_value_labels`](Self::read_value_labels)
    /// on a non-seekable reader. It is an error to advance to the
    /// next section without first consuming or skipping all records.
    pub fn read_to_end(&mut self) -> Result<()> {
        todo!()
    }

    /// Consumes remaining records and transitions to value-label reading.
    pub fn read_value_labels(mut self) -> Result<ValueLabelReader<R>> {
        todo!()
    }
}

impl<R: BufRead + Seek> RecordReader<R> {
    /// Seeks past remaining data records and transitions to
    /// value-label reading.
    pub fn seek_value_labels(mut self) -> Result<ValueLabelReader<R>> {
        todo!()
    }
}
