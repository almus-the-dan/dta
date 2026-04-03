use std::fs::File;
use std::io::BufReader;

use super::header_reader::HeaderReader;
use crate::stata::dta::dta_reader_options::DtaReaderOptions;

/// Top-level entry point for reading a DTA file.
#[derive(Debug)]
pub struct DtaReader;

impl DtaReader {
    /// Begins reading a DTA file from a [`File`], wrapping it in a
    /// [`BufReader`] automatically.
    #[must_use]
    #[inline]
    pub fn from_file(file: File, options: &DtaReaderOptions) -> HeaderReader<BufReader<File>> {
        Self::from_reader(BufReader::new(file), options)
    }

    /// Begins reading a DTA file, returning a [`HeaderReader`] for
    /// the first phase of parsing.
    #[must_use]
    #[inline]
    pub fn from_reader<R>(reader: R, options: &DtaReaderOptions) -> HeaderReader<R> {
        HeaderReader::new(reader, options.encoding())
    }
}
