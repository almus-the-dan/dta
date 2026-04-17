use std::io::{Seek, Write};

use super::dta_error::Result;
use super::header::Header;
use super::long_string::LongString;
use super::schema::Schema;
use super::value_label_writer::ValueLabelWriter;
use super::writer_state::WriterState;

/// Writes long string (strL / GSO) entries to a DTA file.
///
/// Only XML formats (117+) support strLs. For earlier releases,
/// [`write_long_string`](Self::write_long_string) returns an error
/// and [`into_value_label_writer`](Self::into_value_label_writer)
/// transitions without emitting any strL content.
#[derive(Debug)]
pub struct LongStringWriter<W> {
    state: WriterState<W>,
    header: Header,
    schema: Schema,
}

impl<W> LongStringWriter<W> {
    #[must_use]
    pub(crate) fn new(state: WriterState<W>, header: Header, schema: Schema) -> Self {
        Self {
            state,
            header,
            schema,
        }
    }

    /// The header emitted during the header phase.
    #[must_use]
    #[inline]
    pub fn header(&self) -> &Header {
        todo!()
    }

    /// The schema emitted during the schema phase.
    #[must_use]
    #[inline]
    pub fn schema(&self) -> &Schema {
        todo!()
    }

    /// The file's active encoding, captured by
    /// [`LongStringTable::iter`](super::long_string_table::LongStringTable::iter)
    /// so the yielded [`LongString`]s carry the right decoder.
    #[must_use]
    #[inline]
    pub(crate) fn encoding(&self) -> &'static encoding_rs::Encoding {
        self.state.encoding()
    }
}

impl<W: Write + Seek> LongStringWriter<W> {
    /// Writes a single long-string (strL) entry as a GSO block.
    ///
    /// # Errors
    ///
    /// Returns [`DtaError::Format`](super::dta_error::DtaError::Format)
    /// when the header's release does not support strLs, and
    /// [`DtaError::Io`](super::dta_error::DtaError::Io) on sink
    /// failures.
    pub fn write_long_string(&mut self, _long_string: &LongString<'_>) -> Result<()> {
        todo!()
    }

    /// Closes the strL section (XML formats only), patches the
    /// value-labels offset in the map, and transitions to
    /// value-label writing.
    ///
    /// # Errors
    ///
    /// Returns [`DtaError::Io`](super::dta_error::DtaError::Io) on
    /// sink failures.
    pub fn into_value_label_writer(self) -> Result<ValueLabelWriter<W>> {
        todo!()
    }
}
