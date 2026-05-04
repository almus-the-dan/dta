use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::Path;

use super::dct_error::Result;
use super::dct_source::DctSource;
use super::parser::parse_dct;

/// Configuration for parsing a `.dct` dictionary into a [`DctSource`].
///
/// Created via [`DctSource::options`]. Today there are no
/// configurable knobs — the type exists so future options (e.g.,
/// label-text encoding override, strict-vs-lenient parsing) can land
/// without breaking the construction surface.
#[derive(Debug)]
pub struct DctSourceOptions {
    _private: (),
}

impl DctSourceOptions {
    #[must_use]
    pub(super) fn new() -> Self {
        Self { _private: () }
    }

    /// Parses a `.dct` dictionary from a buffered reader.
    ///
    /// On success the returned [`DctSource`] indicates whether the
    /// associated data file is embedded after the closing `}` or
    /// lives in a separate file.
    ///
    /// # Errors
    ///
    /// Returns [`DctError`](super::dct_error::DctError) on I/O
    /// failure, when the dictionary ends before its closing `}`,
    /// when the opening `dictionary {` is malformed, or when any
    /// directive fails to parse.
    //noinspection RsSelfConvention
    #[inline]
    pub fn from_reader<R: BufRead>(self, reader: R) -> Result<DctSource<R>> {
        parse_dct(reader)
    }

    /// Parses a `.dct` dictionary from an open file, wrapping it in a
    /// [`BufReader`] first.
    ///
    /// # Errors
    ///
    /// Returns [`DctError`](super::dct_error::DctError) if the
    /// dictionary fails to parse.
    //noinspection RsSelfConvention
    #[inline]
    pub fn from_file(self, file: File) -> Result<DctSource<BufReader<File>>> {
        let reader = BufReader::new(file);
        self.from_reader(reader)
    }

    /// Opens the file at `path` and parses it as a `.dct` dictionary.
    ///
    /// # Errors
    ///
    /// Returns [`DctError`](super::dct_error::DctError) if the file
    /// cannot be opened or its contents fail to parse.
    //noinspection RsSelfConvention
    pub fn from_path(self, path: impl AsRef<Path>) -> Result<DctSource<BufReader<File>>> {
        let file = File::open(path)?;
        self.from_file(file)
    }

    /// Parses a `.dct` dictionary from an async buffered reader.
    ///
    /// For the best performance, wrap the source in a
    /// [`tokio::io::BufReader`] before passing it here.
    ///
    /// # Errors
    ///
    /// Returns [`DctError`](super::dct_error::DctError) on the same
    /// conditions as [`from_reader`](Self::from_reader).
    //noinspection RsSelfConvention
    #[cfg(feature = "tokio")]
    #[inline]
    pub async fn from_tokio_reader<R: tokio::io::AsyncBufRead + Unpin>(
        self,
        reader: R,
    ) -> Result<DctSource<R>> {
        super::async_parser::parse_dct(reader).await
    }

    /// Parses a `.dct` dictionary from an open async file, wrapping
    /// it in a [`tokio::io::BufReader`] first.
    ///
    /// # Errors
    ///
    /// Returns [`DctError`](super::dct_error::DctError) if the
    /// dictionary fails to parse.
    //noinspection RsSelfConvention
    #[cfg(feature = "tokio")]
    #[inline]
    pub async fn from_tokio_file(
        self,
        file: tokio::fs::File,
    ) -> Result<DctSource<tokio::io::BufReader<tokio::fs::File>>> {
        let reader = tokio::io::BufReader::new(file);
        self.from_tokio_reader(reader).await
    }

    /// Opens the file at `path` asynchronously and parses it as a
    /// `.dct` dictionary.
    ///
    /// # Errors
    ///
    /// Returns [`DctError`](super::dct_error::DctError) if the file
    /// cannot be opened or its contents fail to parse.
    //noinspection RsSelfConvention
    #[cfg(feature = "tokio")]
    pub async fn from_tokio_path(
        self,
        path: impl AsRef<Path>,
    ) -> Result<DctSource<tokio::io::BufReader<tokio::fs::File>>> {
        let file = tokio::fs::File::open(path).await?;
        self.from_tokio_file(file).await
    }
}
