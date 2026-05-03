use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::Path;

use super::dct_error::Result;
use super::dct_reader::DctReader;
use super::schema::Schema;

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
}

impl DctReaderOptions {
    #[must_use]
    pub(super) fn new(schema: Schema) -> Self {
        Self { schema }
    }

    /// Creates a [`DctReader`] wrapping the given buffered source.
    ///
    /// For best performance, wrap the source in a [`BufReader`]
    /// before passing it here.
    //noinspection RsSelfConvention
    #[must_use]
    #[inline]
    pub fn from_reader<R: BufRead>(self, reader: R) -> DctReader<R> {
        DctReader::new(self.schema, reader)
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
}
