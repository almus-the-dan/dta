use std::io::{Seek, Write};

use super::characteristic_writer::CharacteristicWriter;
use super::dta_error::Result;
use super::header::Header;
use super::schema::Schema;
use super::writer_state::WriterState;

/// Writes variable definitions to a DTA file.
///
/// Owns the [`Header`] emitted by the previous phase. Call
/// [`write_schema`](Self::write_schema) to emit the variable
/// descriptors (type codes, names, sort order, display formats,
/// value-label associations, and variable labels) and advance to
/// characteristic writing.
///
/// For XML formats (117+), [`write_schema`](Self::write_schema) also
/// emits the `<map>` section with placeholder offsets, which later
/// writers patch as each section is completed.
#[derive(Debug)]
pub struct SchemaWriter<W> {
    state: WriterState<W>,
    header: Header,
}

impl<W> SchemaWriter<W> {
    #[must_use]
    pub(crate) fn new(state: WriterState<W>, header: Header) -> Self {
        Self { state, header }
    }

    /// The header emitted by the previous phase.
    #[must_use]
    #[inline]
    pub fn header(&self) -> &Header {
        &self.header
    }

    /// Consumes the writer and returns the underlying state, used by
    /// tests that want to recover the sink before `write_schema` is
    /// implemented.
    #[cfg(test)]
    pub(crate) fn into_state(self) -> WriterState<W> {
        self.state
    }
}

impl<W: Write + Seek> SchemaWriter<W> {
    /// Writes the `<map>` (XML only) and variable descriptor
    /// subsections, then transitions to characteristic writing.
    ///
    /// # Errors
    ///
    /// Returns [`DtaError::Io`](super::dta_error::DtaError::Io) on
    /// sink failures and [`DtaError::Format`](super::dta_error::DtaError::Format)
    /// if the schema cannot be represented in the header's release
    /// (e.g., `strL` columns in a pre-117 format, or variable names
    /// that exceed the fixed-field width).
    pub fn write_schema(self, _schema: Schema) -> Result<CharacteristicWriter<W>> {
        todo!()
    }
}
