use std::io::{Seek, Write};

use super::characteristic::Characteristic;
use super::dta_error::Result;
use super::header::Header;
use super::record_writer::RecordWriter;
use super::schema::Schema;
use super::writer_state::WriterState;

/// Writes characteristic (expansion-field) entries to a DTA file.
///
/// Unlike the header and schema phases, characteristic writing
/// accepts any number of entries via
/// [`write_characteristic`](Self::write_characteristic) before
/// transitioning via [`into_record_writer`](Self::into_record_writer).
///
/// The writer handles both binary and XML encodings internally:
/// binary formats emit `(data_type, length, payload)` triples
/// terminated by a zero-length entry; XML formats emit
/// `<characteristics>` / `<ch>` tags.
#[derive(Debug)]
pub struct CharacteristicWriter<W> {
    state: WriterState<W>,
    header: Header,
    schema: Schema,
}

impl<W> CharacteristicWriter<W> {
    #[must_use]
    pub(crate) fn new(state: WriterState<W>, header: Header, schema: Schema) -> Self {
        Self {
            state,
            header,
            schema,
        }
    }

    /// The header emitted by the previous phase.
    #[must_use]
    #[inline]
    pub fn header(&self) -> &Header {
        &self.header
    }

    /// The schema emitted by the previous phase.
    #[must_use]
    #[inline]
    pub fn schema(&self) -> &Schema {
        &self.schema
    }

    /// Consumes the writer and returns the underlying state. Used by
    /// schema-writer round-trip tests that need to recover the sink
    /// before the remaining writer phases are implemented.
    #[cfg(test)]
    pub(crate) fn into_state(self) -> WriterState<W> {
        self.state
    }
}

impl<W: Write + Seek> CharacteristicWriter<W> {
    /// Writes a single characteristic entry.
    ///
    /// Can be called any number of times (including zero).
    ///
    /// # Errors
    ///
    /// Returns [`DtaError::Io`](super::dta_error::DtaError::Io) on
    /// sink failures and [`DtaError::Format`](super::dta_error::DtaError::Format)
    /// if the entry cannot be represented (e.g., a name or value
    /// exceeding the field width, or a variable target that is not
    /// in the schema).
    pub fn write_characteristic(&mut self, _characteristic: &Characteristic) -> Result<()> {
        todo!()
    }

    /// Closes the characteristics section, patches the data offset
    /// in the map, and transitions to record writing.
    ///
    /// # Errors
    ///
    /// Returns [`DtaError::Io`](super::dta_error::DtaError::Io) on
    /// sink failures.
    pub fn into_record_writer(self) -> Result<RecordWriter<W>> {
        todo!()
    }
}
