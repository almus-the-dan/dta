use std::io::{BufRead, Seek};

use super::header::Header;
use super::reader_state::ReaderState;
use super::record_reader::RecordReader;

/// Reads variable definitions from a DTA file.
///
/// Owns the parsed [`Header`] from the previous phase. Call
/// [`read_schema`](Self::read_schema) to parse variable definitions
/// and advance to data reading.
#[derive(Debug)]
pub struct SchemaReader<R> {
    state: ReaderState<R>,
    header: Header,
}

impl<R> SchemaReader<R> {
    /// The parsed file header.
    #[must_use]
    pub fn header(&self) -> &Header {
        &self.header
    }
}

impl<R: BufRead + Seek> SchemaReader<R> {
    /// Parses variable definitions and transitions to data reading.
    pub fn read_schema(mut self) -> Result<RecordReader<R>, std::io::Error> {
        todo!()
    }
}
