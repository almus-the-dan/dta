use encoding_rs::Encoding;

/// Immutable options for configuring a [`DtaReader`].
#[derive(Debug, Clone)]
pub struct DtaReaderOptions {
    encoding: Option<&'static Encoding>,
}

impl DtaReaderOptions {
    /// Returns a new [`DtaReaderOptionsBuilder`].
    #[must_use]
    pub fn builder() -> DtaReaderOptionsBuilder {
        DtaReaderOptionsBuilder::new()
    }

    /// The encoding override, if set.
    #[must_use]
    pub fn encoding(&self) -> Option<&'static Encoding> {
        self.encoding
    }
}

impl Default for DtaReaderOptions {
    fn default() -> Self {
        DtaReaderOptionsBuilder::new().build()
    }
}

/// Builder for [`DtaReaderOptions`].
#[derive(Debug, Clone)]
pub struct DtaReaderOptionsBuilder {
    encoding: Option<&'static Encoding>,
}

impl DtaReaderOptionsBuilder {
    /// Creates a new builder with default values.
    #[must_use]
    pub fn new() -> Self {
        Self { encoding: None }
    }

    /// Sets an explicit encoding override, used regardless of format
    /// version.
    #[must_use]
    pub fn encoding(mut self, encoding: &'static Encoding) -> Self {
        self.encoding = Some(encoding);
        self
    }

    /// Builds the [`DtaReaderOptions`].
    #[must_use]
    pub fn build(self) -> DtaReaderOptions {
        DtaReaderOptions {
            encoding: self.encoding,
        }
    }
}

impl Default for DtaReaderOptionsBuilder {
    fn default() -> Self {
        Self::new()
    }
}
