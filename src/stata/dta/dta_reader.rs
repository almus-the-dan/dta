use encoding_rs::Encoding;

use super::header_reader::HeaderReader;

/// Options for configuring a [`DtaReader`].
#[derive(Debug, Clone)]
pub struct DtaReaderOptions {
    encoding: Option<&'static Encoding>,
}

impl DtaReaderOptions {
    /// Creates default options (encoding detected from format version).
    #[must_use]
    pub fn new() -> Self {
        Self { encoding: None }
    }

    /// Sets an explicit encoding override, used regardless of format
    /// version.
    #[must_use]
    pub fn with_encoding(mut self, encoding: &'static Encoding) -> Self {
        self.encoding = Some(encoding);
        self
    }

    /// The encoding override, if set.
    #[must_use]
    pub fn encoding(&self) -> Option<&'static Encoding> {
        self.encoding
    }
}

impl Default for DtaReaderOptions {
    fn default() -> Self {
        Self::new()
    }
}

/// Top-level entry point for reading a DTA file.
#[derive(Debug)]
pub struct DtaReader;

impl DtaReader {
    /// Begins reading a DTA file, returning a [`HeaderReader`] for
    /// the first phase of parsing.
    pub fn new<R>(reader: R, options: DtaReaderOptions) -> HeaderReader<R> {
        HeaderReader::new(reader, options.encoding)
    }
}
