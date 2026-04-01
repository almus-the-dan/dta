use std::io::{BufRead, Seek};

use super::header::Header;
use super::long_string_reader::LongStringReader;
use super::reader_state::ReaderState;
use super::record_reader::RecordReader;
use super::schema::Schema;

/// Reads value-label tables from a DTA file.
///
/// Owns the parsed [`Header`] and [`Schema`] from previous
/// phases. Yields [`ValueLabelTable`](super::value_label::ValueLabelTable)
/// entries via iteration, then optionally transitions to long-string
/// reading.
#[derive(Debug)]
pub struct ValueLabelReader<R> {
    state: ReaderState<R>,
    header: Header,
    schema: Schema,
}

impl<R> ValueLabelReader<R> {
    /// The parsed file header.
    #[must_use]
    pub fn header(&self) -> &Header {
        &self.header
    }

    /// The parsed variable definitions.
    #[must_use]
    pub fn schema(&self) -> &Schema {
        &self.schema
    }
}

impl<R: BufRead> ValueLabelReader<R> {
    // TODO: iteration over ValueLabelTable entries

    /// Consumes all remaining value-label entries without processing
    /// them, then transitions to long-string reading.
    ///
    /// Returns `None` if the format version does not support long
    /// strings (pre-118).
    pub fn read_to_end(&mut self) -> Result<(), std::io::Error> {
        todo!()
    }

    /// Consumes all remaining value-label entries without processing
    /// them, then transitions to long-string reading.
    ///
    /// Returns `None` if the format version does not support long
    /// strings (pre-118).
    pub fn read_long_strings(mut self) -> Result<Option<LongStringReader<R>>, std::io::Error> {
        todo!()
    }
}

impl<R: BufRead + Seek> ValueLabelReader<R> {
    /// Seeks to the long-string section.
    ///
    /// Returns `None` if the format version does not support long
    /// strings (pre-118).
    pub fn seek_long_strings(mut self) -> Result<Option<LongStringReader<R>>, std::io::Error> {
        todo!()
    }

    /// Seeks back to the data section.
    pub fn seek_data(mut self) -> Result<RecordReader<R>, std::io::Error> {
        todo!()
    }
}
