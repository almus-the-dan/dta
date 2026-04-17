use std::io::{Seek, Write};

use encoding_rs::Encoding;

use super::dta_error::Result;
use super::header::Header;
use super::schema_writer::SchemaWriter;
use super::writer_state::WriterState;

/// Entry point for writing a DTA file.
///
/// Created via [`DtaWriter::to_writer`](super::dta_writer::DtaWriter::to_writer)
/// or [`DtaWriter::to_file`](super::dta_writer::DtaWriter::to_file),
/// then call [`write_header`](Self::write_header) to emit the file
/// header and advance to schema writing.
#[derive(Debug)]
pub struct HeaderWriter<W> {
    state: WriterState<W>,
    encoding_override: Option<&'static Encoding>,
}

impl<W> HeaderWriter<W> {
    /// Creates a header writer. The encoding override, if provided,
    /// will be used regardless of the header's release; otherwise the
    /// encoding is determined from the release number at writing time.
    #[must_use]
    pub(crate) fn new(_writer: W, _encoding: Option<&'static Encoding>) -> Self {
        todo!()
    }
}

impl<W: Write + Seek> HeaderWriter<W> {
    /// Writes the file header and transitions to schema writing.
    ///
    /// For binary formats (104–116) this emits the fixed 10-byte
    /// preamble followed by the dataset label and timestamp fields.
    /// For XML formats (117+) this emits the `<stata_dta><header>`
    /// opening tags and the `<release>`, `<byteorder>`, `<K>`, `<N>`,
    /// `<label>`, and `<timestamp>` fields.
    ///
    /// # Errors
    ///
    /// Returns [`DtaError::Io`](super::dta_error::DtaError::Io) on
    /// sink failures and [`DtaError::Format`](super::dta_error::DtaError::Format)
    /// if the [`Header`] contains values the target format cannot
    /// represent (e.g., `variable_count > u16::MAX` for a release
    /// that only stores a 16-bit count).
    pub fn write_header(self, _header: Header) -> Result<SchemaWriter<W>> {
        todo!()
    }
}
