use super::dct_reader::DctReader;
use super::dct_source_options::DctSourceOptions;
use super::schema::Schema;

/// The result of parsing a `.dct` dictionary.
///
/// Distinguishes the two ways a dictionary can be paired with its
/// data: an external file referenced by the `using` clause (or no
/// file at all), vs. data embedded in the same file immediately
/// after the dictionary's closing `}`.
#[derive(Debug)]
pub enum DctSource<R> {
    /// The dictionary references an external data file or none at
    /// all. Supply the data reader yourself via
    /// [`DctReader::options`](DctReader::options) to read records.
    External(Schema),
    /// Data immediately follows the closing `}` in the dictionary
    /// file. The contained reader is positioned at the first byte of
    /// the data section.
    Embedded(DctReader<R>),
}

impl DctSource<()> {
    /// Creates a [`DctSourceOptions`] builder for parsing a `.dct`
    /// dictionary.
    ///
    /// Call one of `from_reader` / `from_file` / `from_path` on the
    /// returned builder to perform the parse.
    #[must_use]
    #[inline]
    pub fn options() -> DctSourceOptions {
        DctSourceOptions::new()
    }
}

impl<R> DctSource<R> {
    /// Returns the schema regardless of variant.
    #[must_use]
    pub fn schema(&self) -> &Schema {
        match self {
            Self::External(schema) => schema,
            Self::Embedded(reader) => reader.schema(),
        }
    }
}
