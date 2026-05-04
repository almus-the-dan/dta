use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::Path;

use super::dct_error::Result;
use super::dct_reader::DctReader;
use super::schema::Schema;

#[cfg(feature = "tokio")]
use super::async_dct_reader::AsyncDctReader;

/// Configuration for constructing a [`DctReader`] from a parsed
/// [`Schema`].
///
/// Created via [`DctReader::options`]. The schema is required at
/// construction; future configuration knobs will be added as chained
/// setters before a terminal method that produces the reader.
///
/// `DctReader::new` is intentionally crate-private so new options
/// can land later without breaking callers — always go through this
/// builder.
#[derive(Debug)]
pub struct DctReaderOptions {
    schema: Schema,
    record_warnings: bool,
}

impl DctReaderOptions {
    #[must_use]
    pub(super) fn new(schema: Schema) -> Self {
        Self {
            schema,
            record_warnings: true,
        }
    }

    /// Controls whether per-record warnings are accumulated.
    ///
    /// Defaults to `true`. Set to `false` when the caller doesn't
    /// inspect [`DctReader::warnings`](DctReader::warnings) — the
    /// reader skips warning construction entirely, leaving the
    /// internal buffer empty across reads.
    #[must_use]
    #[inline]
    pub fn record_warnings(mut self, enabled: bool) -> Self {
        self.record_warnings = enabled;
        self
    }

    /// Creates a [`DctReader`] wrapping the given buffered source.
    ///
    /// For best performance, wrap the source in a [`BufReader`]
    /// before passing it here.
    //noinspection RsSelfConvention
    #[must_use]
    #[inline]
    pub fn from_reader<R: BufRead>(self, reader: R) -> DctReader<R> {
        DctReader::new(self.schema, reader, self.record_warnings)
    }

    /// Creates a [`DctReader`] wrapping the file in a [`BufReader`].
    //noinspection RsSelfConvention
    #[must_use]
    #[inline]
    pub fn from_file(self, file: File) -> DctReader<BufReader<File>> {
        let reader = BufReader::new(file);
        self.from_reader(reader)
    }

    /// Opens the file at `path` and wraps it in a [`BufReader`].
    ///
    /// # Errors
    ///
    /// Returns an I/O error if the file cannot be opened.
    //noinspection RsSelfConvention
    pub fn from_path(self, path: impl AsRef<Path>) -> Result<DctReader<BufReader<File>>> {
        let file = File::open(path)?;
        let reader = self.from_file(file);
        Ok(reader)
    }

    /// Creates an [`AsyncDctReader`] wrapping the given async
    /// buffered source.
    ///
    /// For best performance, wrap the source in a
    /// [`tokio::io::BufReader`] before passing it here.
    //noinspection RsSelfConvention
    #[cfg(feature = "tokio")]
    #[must_use]
    #[inline]
    pub fn from_tokio_reader<R: tokio::io::AsyncBufRead + Unpin>(
        self,
        reader: R,
    ) -> AsyncDctReader<R> {
        AsyncDctReader::new(self.schema, reader, self.record_warnings)
    }

    /// Creates an [`AsyncDctReader`] wrapping the async file in a
    /// [`tokio::io::BufReader`].
    //noinspection RsSelfConvention
    #[cfg(feature = "tokio")]
    #[must_use]
    #[inline]
    pub fn from_tokio_file(
        self,
        file: tokio::fs::File,
    ) -> AsyncDctReader<tokio::io::BufReader<tokio::fs::File>> {
        let reader = tokio::io::BufReader::new(file);
        self.from_tokio_reader(reader)
    }

    /// Opens the file at `path` asynchronously and wraps it in a
    /// [`tokio::io::BufReader`].
    ///
    /// # Errors
    ///
    /// Returns an I/O error if the file cannot be opened.
    //noinspection RsSelfConvention
    #[cfg(feature = "tokio")]
    pub async fn from_tokio_path(
        self,
        path: impl AsRef<Path>,
    ) -> Result<AsyncDctReader<tokio::io::BufReader<tokio::fs::File>>> {
        let file = tokio::fs::File::open(path).await?;
        let reader = self.from_tokio_file(file);
        Ok(reader)
    }
}
