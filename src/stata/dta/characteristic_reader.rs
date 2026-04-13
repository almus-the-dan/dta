use std::io::{BufRead, Seek};

use super::dta_error::Result;
use super::header::Header;
use super::long_string_reader::LongStringReader;
use super::reader_state::ReaderState;
use super::record_reader::RecordReader;
use super::schema::Schema;
use super::value_label_reader::ValueLabelReader;

/// Reads characteristics from a DTA file.
///
/// For XML formats (117+), it reads the `<characteristics>` section.
/// For binary formats (104–116), reads expansion fields.
///
/// Owns the parsed [`Header`] and [`Schema`] from previous phases.
/// Call [`read_records`](Self::read_records) to consume
/// characteristics and advance to data reading.
#[derive(Debug)]
pub struct CharacteristicReader<R> {
    state: ReaderState<R>,
    header: Header,
    schema: Schema,
}

impl<R> CharacteristicReader<R> {
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

impl<R: BufRead> CharacteristicReader<R> {
    // TODO: iteration over characteristic entries

    /// Consumes all remaining characteristic entries without
    /// processing them.
    pub fn read_to_end(&mut self) -> Result<()> {
        todo!()
    }

    /// Consumes characteristics and transitions to record reading.
    pub fn read_records(mut self) -> Result<RecordReader<R>> {
        todo!()
    }
}

impl<R: BufRead + Seek> CharacteristicReader<R> {
    /// Seeks past characteristics and transitions to record reading.
    pub fn seek_records(mut self) -> Result<RecordReader<R>> {
        todo!()
    }

    /// Seeks to the value-label section.
    pub fn seek_value_labels(mut self) -> Result<ValueLabelReader<R>> {
        todo!()
    }

    /// Seeks to the long-string section.
    ///
    /// Returns `None` if the format version does not support long
    /// strings (pre-118).
    pub fn seek_long_strings(mut self) -> Result<Option<LongStringReader<R>>> {
        todo!()
    }
}
