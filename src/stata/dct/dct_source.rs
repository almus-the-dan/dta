use std::io::BufRead;

use super::dct_reader::DctReader;
use super::schema::Schema;

/// The result of parsing a `.dct` dictionary.
///
/// Distinguishes the two ways a dictionary can be paired with its
/// data: an external file referenced by the `using` clause (or no
/// file at all), vs. data embedded in the same file immediately
/// after the dictionary's closing `}`.
#[derive(Debug)]
pub enum DctSource<R: BufRead> {
    /// The dictionary references an external data file or none at
    /// all. The reader passed to
    /// [`parse_dct`](super::parser::parse_dct) has been read up to
    /// the closing `}` and is not retained — supply your own data
    /// reader to construct a [`DctReader`].
    External(Schema),
    /// Data immediately follows the closing `}` in the dictionary
    /// file. The contained reader is positioned at the first byte of
    /// the data section.
    Embedded(DctReader<R>),
}

impl<R: BufRead> DctSource<R> {
    /// Returns the schema regardless of variant.
    #[must_use]
    pub fn schema(&self) -> &Schema {
        match self {
            Self::External(schema) => schema,
            Self::Embedded(reader) => reader.schema(),
        }
    }
}
