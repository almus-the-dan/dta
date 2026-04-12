use std::io::Read;

use encoding_rs::Encoding;

use super::byte_order::ByteOrder;
use super::dta_error::{DtaError, Field, FormatErrorKind, Result, Section};

#[derive(Debug)]
pub(crate) struct ReaderState<R> {
    reader: R,
    encoding: &'static Encoding,
    buffer: Vec<u8>,
    position: u64,
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
        Ok(self.read_exact(1, section)?[0])
    }

    pub fn read_u16(&mut self, byte_order: ByteOrder, section: Section) -> Result<u16> {
        let buffer = self.read_exact(2, section)?;
        let bytes = [buffer[0], buffer[1]];
        Ok(match byte_order {
            ByteOrder::BigEndian => u16::from_be_bytes(bytes),
            ByteOrder::LittleEndian => u16::from_le_bytes(bytes),
        })
    }

    pub fn read_u32(&mut self, byte_order: ByteOrder, section: Section) -> Result<u32> {
        let buffer = self.read_exact(4, section)?;
        let bytes = [buffer[0], buffer[1], buffer[2], buffer[3]];
        Ok(match byte_order {
            ByteOrder::BigEndian => u32::from_be_bytes(bytes),
            ByteOrder::LittleEndian => u32::from_le_bytes(bytes),
        })
    }

    pub fn read_u64(&mut self, byte_order: ByteOrder, section: Section) -> Result<u64> {
        let buffer = self.read_exact(8, section)?;
        let bytes = [
            buffer[0], buffer[1], buffer[2], buffer[3], buffer[4], buffer[5], buffer[6], buffer[7],
        ];
        Ok(match byte_order {
            ByteOrder::BigEndian => u64::from_be_bytes(bytes),
            ByteOrder::LittleEndian => u64::from_le_bytes(bytes),
        })
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
        let (decoded, had_errors) = encoding.decode_without_bom_handling(&buffer[..end]);
        if had_errors {
            return Err(DtaError::format(
                section,
                position,
                FormatErrorKind::InvalidEncoding { field },
            ));
        }
        Ok(decoded.into_owned())
    }
}
