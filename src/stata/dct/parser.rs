use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::Path;

use super::dct_error::Result;
use super::dct_source::DctSource;

/// Parses a `.dct` dictionary from a buffered reader.
///
/// On success the returned [`DctSource`] indicates whether the
/// associated data file is embedded in the same source (data follows
/// the closing `}`) or external (referenced by the dictionary's
/// `using` clause, or supplied separately by the caller).
///
/// # Errors
///
/// Returns [`DctError`](super::dct_error::DctError) when an I/O error
/// occurs, the dictionary ends before its closing `}`, or any
/// directive fails to parse.
pub fn parse_dct<R: BufRead>(reader: R) -> Result<DctSource<R>> {
    let _ = reader;
    todo!("parse_dct not yet implemented")
}

/// Opens the file at `path` and parses it as a `.dct` dictionary.
///
/// # Errors
///
/// Returns [`DctError`](super::dct_error::DctError) if the file cannot be
/// opened or its contents fail to parse.
pub fn open_dct<P: AsRef<Path>>(path: P) -> Result<DctSource<BufReader<File>>> {
    let file = File::open(path)?;
    parse_dct(BufReader::new(file))
}
