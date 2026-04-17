use std::io::{Seek, Write};

use super::dta_error::Result;
use super::header::Header;
use super::schema::Schema;
use super::value_label::ValueLabelTable;
use super::writer_state::WriterState;

/// Writes value-label tables — the last section of a DTA file.
///
/// Call [`write_value_label_table`](Self::write_value_label_table)
/// once per table, then [`finish`](Self::finish) to emit the closing
/// tag (XML formats only), flush the sink, and recover the
/// underlying writer.
#[derive(Debug)]
pub struct ValueLabelWriter<W> {
    state: WriterState<W>,
    header: Header,
    schema: Schema,
}

impl<W> ValueLabelWriter<W> {
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

impl<W: Write + Seek> ValueLabelWriter<W> {
    /// Writes a single value-label table.
    ///
    /// Can be called any number of times (including zero).
    ///
    /// # Errors
    ///
    /// Returns [`DtaError::Io`](super::dta_error::DtaError::Io) on
    /// sink failures and [`DtaError::Format`](super::dta_error::DtaError::Format)
    /// if the table cannot be represented (e.g., a name exceeding the
    /// field width, or a label that does not fit the table's byte
    /// budget).
    pub fn write_value_label_table(&mut self, _table: &ValueLabelTable) -> Result<()> {
        todo!()
    }

    /// Emits the closing `</stata_dta>` tag (XML formats only),
    /// flushes the sink, and returns it.
    ///
    /// The returned sink is finalized — the DTA file is complete.
    /// Writing more bytes to it would corrupt the file.
    ///
    /// # Errors
    ///
    /// Returns [`DtaError::Io`](super::dta_error::DtaError::Io) on
    /// sink failures while writing the closing tag or flushing.
    pub fn finish(self) -> Result<W> {
        todo!()
    }
}
