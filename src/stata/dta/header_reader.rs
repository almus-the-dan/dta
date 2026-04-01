use std::io::{BufRead, Read, Seek};

use encoding_rs::Encoding;

use super::reader_state::ReaderState;
use super::schema_reader::SchemaReader;

/// Entry point for reading a DTA file.
///
/// Created via [`DtaReader::new`](super::dta_reader::DtaReader::new),
/// then call [`read_header`](Self::read_header) to parse the file header
/// and advance to schema reading.
#[derive(Debug)]
pub struct HeaderReader<R> {
    reader: R,
    encoding_override: Option<&'static Encoding>,
}

impl<R> HeaderReader<R> {
    /// Creates a header reader. The encoding override, if provided,
    /// will be used regardless of format version; otherwise the
    /// encoding is determined from the release number.
    pub(crate) fn new(reader: R, encoding_override: Option<&'static Encoding>) -> Self {
        Self {
            reader,
            encoding_override,
        }
    }
}

impl<R: BufRead + Seek> HeaderReader<R> {
    /// Parses the file header, determines the encoding, and
    /// transitions to schema reading.
    pub fn read_header(mut self) -> Result<SchemaReader<R>, std::io::Error> {
        // TODO: parse header, determine release, then:
        // let encoding = self.encoding_override.unwrap_or_else(|| {
        //     if release < 118 { encoding_rs::WINDOWS_1252 } else { encoding_rs::UTF_8 }
        // });
        // let state = ReaderState::new(self.reader, encoding);
        todo!()
    }
}
