use std::io::{Seek, Write};

use super::dta_error::Result;
use super::header::Header;
use super::long_string_writer::LongStringWriter;
use super::schema::Schema;
use super::value::Value;
use super::writer_state::WriterState;

/// Writes observation records (data rows) to a DTA file.
///
/// Call [`write_record`](Self::write_record) once per observation,
/// passing a slice of [`Value`]s whose length and types match the
/// schema. Transition via
/// [`into_long_string_writer`](Self::into_long_string_writer) once
/// all rows have been written.
#[derive(Debug)]
pub struct RecordWriter<W> {
    state: WriterState<W>,
    header: Header,
    schema: Schema,
}

impl<W> RecordWriter<W> {
    #[must_use]
    pub(crate) fn new(_state: WriterState<W>, _header: Header, _schema: Schema) -> Self {
        todo!()
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
}

impl<W: Write + Seek> RecordWriter<W> {
    /// Writes a single observation row.
    ///
    /// The slice length must match `schema.variables().len()`, and
    /// each [`Value`] variant must match the corresponding
    /// [`VariableType`](super::variable_type::VariableType).
    ///
    /// # Errors
    ///
    /// Returns [`DtaError::Io`](super::dta_error::DtaError::Io) on
    /// sink failures and [`DtaError::Format`](super::dta_error::DtaError::Format)
    /// if the values do not match the schema.
    pub fn write_record(&mut self, _values: &[Value<'_>]) -> Result<()> {
        todo!()
    }

    /// Closes the data section, patches the long-strings offset in
    /// the map, and transitions to long-string writing.
    ///
    /// # Errors
    ///
    /// Returns [`DtaError::Io`](super::dta_error::DtaError::Io) on
    /// sink failures.
    pub fn into_long_string_writer(self) -> Result<LongStringWriter<W>> {
        todo!()
    }
}
