use std::fs::File;
use std::io::{BufWriter, Seek, Write};
use std::path::Path;

use encoding_rs::Encoding;

use crate::stata::dta::dta_error::Result;
use crate::stata::dta::header_writer::HeaderWriter;

/// Builder for configuring and opening a DTA file writer.
///
/// Set options with chained methods, then call a terminal method
/// ([`from_path`](Self::from_path), [`from_file`](Self::from_file),
/// or [`from_writer`](Self::from_writer)) to begin writing.
///
/// # Seekability
///
/// The writer chain requires `Write + Seek` so that XML `<map>`
/// offsets can be patched in place once each section's real offset
/// is known. Writers that do not support seeking can be wrapped in
/// [`BufferedSeek`](super::buffered_seek::BufferedSeek) at the cost
/// of buffering the entire file in memory.
///
/// # Examples
///
/// ```no_run
/// use dta::stata::dta::dta_writer::DtaWriter;
///
/// let header_writer = DtaWriter::default()
///     .from_path("data.dta")
///     .unwrap();
/// ```
#[derive(Debug, Clone)]
pub struct DtaWriter {
    encoding: Option<&'static Encoding>,
}

impl DtaWriter {
    /// Creates a new builder with default values.
    #[must_use]
    #[inline]
    pub fn new() -> Self {
        Self { encoding: None }
    }

    /// Sets an explicit encoding override used regardless of format
    /// version.
    #[must_use]
    #[inline]
    pub fn encoding(mut self, encoding: &'static Encoding) -> Self {
        self.encoding = Some(encoding);
        self
    }

    /// Creates the file at `path` and begins writing a DTA file,
    /// wrapping it in a [`BufWriter`] automatically.
    ///
    /// # Errors
    ///
    /// Returns [`DtaError::Io`](super::dta_error::DtaError::Io) if
    /// the file cannot be created.
    //noinspection RsSelfConvention
    #[inline]
    pub fn from_path(self, _path: impl AsRef<Path>) -> Result<HeaderWriter<BufWriter<File>>> {
        todo!()
    }

    /// Begins writing a DTA file to a [`File`], wrapping it in a
    /// [`BufWriter`] automatically.
    //noinspection RsSelfConvention
    #[must_use]
    #[inline]
    pub fn from_file(self, _file: File) -> HeaderWriter<BufWriter<File>> {
        todo!()
    }

    /// Begins writing a DTA file to any `Write + Seek` sink,
    /// returning a [`HeaderWriter`] for the first phase of writing.
    //noinspection RsSelfConvention
    #[must_use]
    #[inline]
    pub fn from_writer<W: Write + Seek>(self, _writer: W) -> HeaderWriter<W> {
        todo!()
    }
}

impl Default for DtaWriter {
    #[inline]
    fn default() -> Self {
        Self::new()
    }
}
