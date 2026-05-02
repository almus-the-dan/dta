use std::io::BufRead;

use super::dct_error::Result;
use super::record::Record;
use super::schema::Schema;

/// Reads logical observations from a data file described by a
/// [`Schema`].
///
/// The line buffer is reused across calls to minimize allocations,
/// which is why [`Record`] borrows from `&mut self` rather than
/// owning its string data.
#[derive(Debug)]
pub struct DctReader<R: BufRead> {
    inner: R,
    schema: Schema,
    line_buf: Vec<u8>,
    next_observation: usize,
    completed: bool,
}

impl<R: BufRead> DctReader<R> {
    /// Constructs a reader from a parsed schema and a data source.
    ///
    /// Use this when [`parse_dct`](super::parser::parse_dct) returned
    /// [`DctSource::External`](super::dct_source::DctSource::External) and
    /// you have separately opened the data file declared in the
    /// dictionary's `using` clause.
    #[must_use]
    pub fn new(schema: Schema, inner: R) -> Self {
        Self {
            inner,
            schema,
            line_buf: Vec::new(),
            next_observation: 1,
            completed: false,
        }
    }

    /// The schema this reader was constructed from.
    #[must_use]
    #[inline]
    pub fn schema(&self) -> &Schema {
        &self.schema
    }

    /// Reads the next observation from the data file.
    ///
    /// Returns `None` once the data file has been fully consumed.
    /// The returned [`Record`] borrows string data from this
    /// reader's internal line buffer, so it must be dropped before
    /// the next call.
    ///
    /// # Errors
    ///
    /// Returns [`DctError`](super::dct_error::DctError) on I/O failure
    /// or when a data field cannot be parsed against the column's
    /// declared type and read format.
    pub fn read_record(&mut self) -> Result<Option<Record<'_>>> {
        let _ = (
            &mut self.inner,
            &mut self.line_buf,
            &mut self.next_observation,
        );
        if self.completed {
            return Ok(None);
        }
        todo!("DctDataReader::read_record not yet implemented")
    }
}
