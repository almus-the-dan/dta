use std::io::{ErrorKind, Read, Seek, SeekFrom};

use encoding_rs::Encoding;

use super::byte_order::ByteOrder;
use super::dta_error::{DtaError, Field, FormatErrorKind, Result, Section};
use super::section_offsets::SectionOffsets;

#[derive(Debug)]
pub(crate) struct ReaderState<R> {
    reader: R,
    encoding: &'static Encoding,
    buffer: Vec<u8>,
    position: u64,
    section_offsets: Option<SectionOffsets>,
}

// -- Construction and accessors ----------------------------------------------

impl<R> ReaderState<R> {
    #[must_use]
    pub fn new(reader: R, encoding: &'static Encoding) -> Self {
        Self {
            reader,
            encoding,
            buffer: Vec::new(),
            position: 0,
            section_offsets: None,
        }
    }

    /// Returns a new state with the given encoding, preserving the
    /// reader, buffer allocation, and position.
    #[must_use]
    pub fn with_encoding(self, encoding: &'static Encoding) -> Self {
        Self { encoding, ..self }
    }

    /// Byte offset in the file.
    #[must_use]
    pub fn position(&self) -> u64 {
        self.position
    }

    /// The active character encoding.
    #[must_use]
    pub fn encoding(&self) -> &'static Encoding {
        self.encoding
    }

    /// The internal buffer, filled by the most recent
    /// [`read_exact`](Self::read_exact) call.
    #[must_use]
    pub fn buffer(&self) -> &[u8] {
        &self.buffer
    }

    /// Byte offsets for each post-schema section.
    ///
    /// Returns `None` before [`SchemaReader::read_schema`] has
    /// completed.
    #[must_use]
    pub fn section_offsets(&self) -> Option<&SectionOffsets> {
        self.section_offsets.as_ref()
    }

    /// Mutable access to section offsets.
    ///
    /// Used by the characteristic reader to fill in data and
    /// value-label offsets for binary formats. Returns `None` before
    /// schema reading.
    pub fn section_offsets_mut(&mut self) -> Option<&mut SectionOffsets> {
        self.section_offsets.as_mut()
    }

    /// Stores the section offsets. Called by the schema reader after
    /// parsing the map (XML) or computing positions (binary).
    pub fn set_section_offsets(&mut self, offsets: SectionOffsets) {
        self.section_offsets = Some(offsets);
    }
}

// -- Primitive readers -------------------------------------------------------

impl<R: Read> ReaderState<R> {
    /// Resizes the internal buffer to `len`, reads exactly `len` bytes
    /// into it, and returns the filled slice. The same allocation is
    /// reused across calls.
    pub fn read_exact(&mut self, len: usize, section: Section) -> Result<&[u8]> {
        self.buffer.resize(len, 0);
        self.reader
            .read_exact(&mut self.buffer)
            .map_err(|e| DtaError::io(section, e))?;
        self.position += u64::try_from(len).expect("buffer length exceeds u64");
        Ok(&self.buffer)
    }

    pub fn skip(&mut self, amount: usize, section: Section) -> Result<()> {
        self.read_exact(amount, section)?;
        Ok(())
    }

    pub fn read_u8(&mut self, section: Section) -> Result<u8> {
        let buffer = self.read_exact(1, section)?;
        let byte = buffer[0];
        Ok(byte)
    }

    pub fn read_u16(&mut self, byte_order: ByteOrder, section: Section) -> Result<u16> {
        let buffer = self.read_exact(2, section)?;
        Ok(byte_order.read_u16([buffer[0], buffer[1]]))
    }

    pub fn read_u32(&mut self, byte_order: ByteOrder, section: Section) -> Result<u32> {
        let buffer = self.read_exact(4, section)?;
        Ok(byte_order.read_u32([buffer[0], buffer[1], buffer[2], buffer[3]]))
    }

    pub fn read_u64(&mut self, byte_order: ByteOrder, section: Section) -> Result<u64> {
        let buffer = self.read_exact(8, section)?;
        Ok(byte_order.read_u64([
            buffer[0], buffer[1], buffer[2], buffer[3], buffer[4], buffer[5], buffer[6], buffer[7],
        ]))
    }

    /// Like [`read_exact`](Self::read_exact), but returns `None` on
    /// clean EOF (zero bytes read) instead of an error. A partial
    /// read still returns an error.
    pub fn try_read_exact(&mut self, len: usize, section: Section) -> Result<Option<&[u8]>> {
        self.buffer.resize(len, 0);
        match self.reader.read_exact(&mut self.buffer) {
            Ok(()) => {
                self.position += u64::try_from(len).expect("buffer length exceeds u64");
                Ok(Some(&self.buffer))
            }
            Err(e) if e.kind() == ErrorKind::UnexpectedEof => Ok(None),
            Err(e) => Err(DtaError::io(section, e)),
        }
    }

    /// Reads a `u16`, returning `None` on clean EOF.
    pub fn try_read_u16(&mut self, byte_order: ByteOrder, section: Section) -> Result<Option<u16>> {
        Ok(self
            .try_read_exact(2, section)?
            .map(|b| byte_order.read_u16([b[0], b[1]])))
    }

    /// Reads a `u32`, returning `None` on clean EOF.
    pub fn try_read_u32(&mut self, byte_order: ByteOrder, section: Section) -> Result<Option<u32>> {
        Ok(self
            .try_read_exact(4, section)?
            .map(|b| byte_order.read_u32([b[0], b[1], b[2], b[3]])))
    }

    /// Reads and validates an exact byte sequence. Returns the given
    /// `on_mismatch` error kind if the bytes do not match.
    pub fn expect_bytes(
        &mut self,
        expected: &[u8],
        section: Section,
        on_mismatch: FormatErrorKind,
    ) -> Result<()> {
        let position = self.position;
        let actual = self.read_exact(expected.len(), section)?;
        if actual != expected {
            return Err(DtaError::format(section, position, on_mismatch));
        }
        Ok(())
    }
}

// -- Seeking ------------------------------------------------------------------

impl<R: Seek> ReaderState<R> {
    /// Seeks to an absolute byte position in the underlying reader.
    pub fn seek_to(&mut self, position: u64, section: Section) -> Result<()> {
        self.reader
            .seek(SeekFrom::Start(position))
            .map_err(|e| DtaError::io(section, e))?;
        self.position = position;
        Ok(())
    }
}

// -- String reading -----------------------------------------------------------

impl<R: Read> ReaderState<R> {
    /// Reads a fixed-length byte field and decodes it as a
    /// null-terminated string. Returns an empty string when `len` is 0.
    ///
    /// Returns [`FormatErrorKind::InvalidEncoding`] if the bytes are
    /// not valid in the file's declared encoding.
    pub fn read_fixed_string(
        &mut self,
        len: usize,
        encoding: &'static Encoding,
        section: Section,
        field: Field,
    ) -> Result<String> {
        if len == 0 {
            return Ok(String::new());
        }
        let position = self.position;
        let buffer = self.read_exact(len, section)?;
        let end = buffer.iter().position(|&b| b == 0).unwrap_or(buffer.len());
        let decoded = encoding
            .decode_without_bom_handling_and_without_replacement(&buffer[..end])
            .ok_or_else(|| {
                DtaError::format(
                    section,
                    position,
                    FormatErrorKind::InvalidEncoding { field },
                )
            })?;
        Ok(decoded.into_owned())
    }
}
