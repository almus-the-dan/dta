use std::io::{BufRead, Seek};

use super::header::Header;
use super::reader_state::ReaderState;
use super::record_reader::RecordReader;
use super::schema::Schema;
use super::value_label_reader::ValueLabelReader;

/// Reads long string (strL) entries from a DTA file.
///
/// Only present for format 118+. Owns the parsed [`Header`] and
/// [`Schema`] from previous phases. Yields
/// [`LongString`](super::long_string::LongString) entries via iteration.
#[derive(Debug)]
pub struct LongStringReader<R> {
    state: ReaderState<R>,
    header: Header,
    schema: Schema,
}

impl<R> LongStringReader<R> {
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

impl<R: BufRead> LongStringReader<R> {
    // TODO: iteration over LongString<'_> entries

    /// Consumes all remaining long-string entries without processing
    /// them.
    pub fn read_to_end(&mut self) -> Result<(), std::io::Error> {
        todo!()
    }
}

impl<R: BufRead + Seek> LongStringReader<R> {
    /// Seeks back to the data section.
    pub fn seek_records(self) -> Result<RecordReader<R>, std::io::Error> {
        todo!()
    }

    /// Seeks back to the value-label section.
    pub fn seek_value_labels(self) -> Result<ValueLabelReader<R>, std::io::Error> {
        todo!()
    }
}
