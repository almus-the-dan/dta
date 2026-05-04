use super::dct_source_options::DctSourceOptions;
use super::schema::Schema;

/// The result of parsing a `.dct` dictionary.
///
/// Distinguishes the two ways a dictionary can be paired with its
/// data: an external file referenced by the `using` clause (or no
/// file at all), vs. data embedded in the same file immediately
/// after the dictionary's closing `}`.
///
/// Both variants surface only the [`Schema`] (and, for embedded
/// data, the buffered reader positioned at the first byte of the
/// data section) — the caller pairs them with
/// [`DctReader::options`](super::dct_reader::DctReader::options) so
/// reader-side knobs like `record_warnings` can be configured
/// uniformly across both paths.
#[derive(Debug)]
pub enum DctSource<R> {
    /// The dictionary references an external data file or none at
    /// all. Pair the schema with your own data reader via
    /// [`DctReader::options`](super::dct_reader::DctReader::options).
    External(Schema),
    /// Data immediately follows the closing `}` in the dictionary
    /// file. `reader` is positioned at the first byte of the data
    /// section; pair it with `schema` via
    /// [`DctReader::options(schema).from_reader(reader)`](super::dct_reader_options::DctReaderOptions::from_reader)
    /// (or the async equivalent) to begin reading records.
    Embedded {
        /// Schema parsed from the dictionary block.
        schema: Schema,
        /// Buffered reader positioned at the first byte of the
        /// data section. Pair with `schema` through
        /// [`DctReader::options`](super::dct_reader::DctReader::options).
        reader: R,
    },
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
            Self::External(schema) | Self::Embedded { schema, .. } => schema,
        }
    }
}
