use std::fs::File;
use std::io::BufReader;
use std::path::Path;

use encoding_rs::Encoding;

use crate::stata::dta::dta_error::{DtaError, Result, Section};
use crate::stata::dta::header_reader::HeaderReader;

#[cfg(feature = "tokio")]
use crate::stata::dta::async_header_reader::AsyncHeaderReader;
#[cfg(feature = "tokio")]
use tokio::io::{AsyncRead, BufReader as TokioBufReader};

/// Builder for configuring and opening a DTA file reader.
///
/// Set options with chained methods, then call a terminal method
/// ([`from_path`](Self::from_path), [`from_file`](Self::from_file),
/// or [`from_reader`](Self::from_reader)) to begin reading.
///
/// # Examples
///
/// ```no_run
/// use dta::stata::dta::dta_reader::DtaReader;
///
/// let header_reader = DtaReader::default()
///     .from_path("data.dta")
///     .unwrap();
/// ```
#[derive(Debug, Clone)]
pub struct DtaReader {
    encoding: Option<&'static Encoding>,
}

impl DtaReader {
    /// Creates a new builder with default values.
    #[must_use]
    #[inline]
    pub fn new() -> Self {
        Self { encoding: None }
    }

    /// Sets an explicit encoding override, used regardless of format
    /// version.
    #[must_use]
    #[inline]
    pub fn encoding(mut self, encoding: &'static Encoding) -> Self {
        self.encoding = Some(encoding);
        self
    }

    /// Opens the file at `path` and begins reading it as a DTA file,
    /// wrapping it in a [`BufReader`] automatically.
    ///
    /// # Errors
    ///
    /// Returns [`DtaError::Io`] if the file cannot be opened.
    //noinspection RsSelfConvention
    #[inline]
    pub fn from_path(self, path: impl AsRef<Path>) -> Result<HeaderReader<BufReader<File>>> {
        let file = File::open(path).map_err(|e| DtaError::io(Section::Header, e))?;
        Ok(self.from_file(file))
    }

    /// Begins reading a DTA file from a [`File`], wrapping it in a
    /// [`BufReader`] automatically.
    //noinspection RsSelfConvention
    #[must_use]
    #[inline]
    pub fn from_file(self, file: File) -> HeaderReader<BufReader<File>> {
        self.from_reader(BufReader::new(file))
    }

    /// Begins reading a DTA file from any reader, returning a
    /// [`HeaderReader`] for the first phase of parsing.
    //noinspection RsSelfConvention
    #[must_use]
    #[inline]
    pub fn from_reader<R>(self, reader: R) -> HeaderReader<R> {
        HeaderReader::new(reader, self.encoding)
    }
}

impl Default for DtaReader {
    #[inline]
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(feature = "tokio")]
impl DtaReader {
    /// Opens the file at `path` asynchronously and begins reading it
    /// as a DTA file, wrapping it in a [`tokio::io::BufReader`]
    /// automatically.
    ///
    /// # Errors
    ///
    /// Returns [`DtaError::Io`] if the file cannot be opened.
    //noinspection RsSelfConvention
    #[inline]
    pub async fn from_tokio_path(
        self,
        path: impl AsRef<Path>,
    ) -> Result<AsyncHeaderReader<TokioBufReader<tokio::fs::File>>> {
        let file = tokio::fs::File::open(path)
            .await
            .map_err(|e| DtaError::io(Section::Header, e))?;
        let reader = self.from_tokio_file(file);
        Ok(reader)
    }

    /// Begins reading a DTA file from a [`tokio::fs::File`], wrapping
    /// it in a [`tokio::io::BufReader`] automatically.
    //noinspection RsSelfConvention
    #[must_use]
    #[inline]
    pub fn from_tokio_file(
        self,
        file: tokio::fs::File,
    ) -> AsyncHeaderReader<TokioBufReader<tokio::fs::File>> {
        let reader = TokioBufReader::new(file);
        self.from_tokio_reader(reader)
    }

    /// Begins reading a DTA file from any async reader, returning an
    /// [`AsyncHeaderReader`] for the first phase of parsing.
    //noinspection RsSelfConvention
    #[must_use]
    #[inline]
    pub fn from_tokio_reader<R: AsyncRead + Unpin>(self, reader: R) -> AsyncHeaderReader<R> {
        AsyncHeaderReader::new(reader, self.encoding)
    }
}
