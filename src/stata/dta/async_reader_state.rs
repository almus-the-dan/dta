use encoding_rs::Encoding;
use tokio::io::{AsyncRead, AsyncReadExt};

use super::byte_order::ByteOrder;
use super::dta_error::{DtaError, Field, FormatErrorKind, Result, Section};
use super::section_offsets::SectionOffsets;
use super::string_decoding::decode_fixed_string;

/// Shared state carried across the async reader typestate chain.
///
/// Owns the underlying `AsyncRead` sink, tracks the current byte
/// position for offset bookkeeping, caches a scratch buffer reused
/// across reads, and remembers the active character encoding. Each
/// `read_*` primitive returns a future; the caller `.await`s it,
/// receives a slice into the scratch buffer (or a decoded value),
/// and the position counter advances.
#[derive(Debug)]
#[allow(dead_code)] // fields/methods picked up as the async chain grows past the header phase
pub(crate) struct AsyncReaderState<R> {
    reader: R,
    encoding: &'static Encoding,
    buffer: Vec<u8>,
    position: u64,
    section_offsets: Option<SectionOffsets>,
}

// -- Construction and accessors ----------------------------------------------

#[allow(dead_code)] // methods light up as the async chain grows past the header phase
impl<R> AsyncReaderState<R> {
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
    /// reader, buffer allocation, position, and section offsets.
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

    /// Byte offsets for each post-schema section. Returns `None`
    /// before the schema has been read.
    #[must_use]
    pub fn section_offsets(&self) -> Option<&SectionOffsets> {
        self.section_offsets.as_ref()
    }

    /// Mutable access to section offsets. Returns `None` before
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

impl<R: AsyncRead + Unpin> AsyncReaderState<R> {
    /// Resizes the internal buffer to `len`, reads exactly `len` bytes
    /// into it, and returns the filled slice. The same allocation is
    /// reused across calls.
    pub async fn read_exact(&mut self, len: usize, section: Section) -> Result<&[u8]> {
        self.buffer.resize(len, 0);
        self.reader
            .read_exact(&mut self.buffer)
            .await
            .map_err(|e| DtaError::io(section, e))?;
        self.position += u64::try_from(len).expect("buffer length exceeds u64");
        Ok(&self.buffer)
    }

    pub async fn skip(&mut self, amount: usize, section: Section) -> Result<()> {
        self.read_exact(amount, section).await?;
        Ok(())
    }

    pub async fn read_u8(&mut self, section: Section) -> Result<u8> {
        let buffer = self.read_exact(1, section).await?;
        Ok(buffer[0])
    }

    pub async fn read_u16(&mut self, byte_order: ByteOrder, section: Section) -> Result<u16> {
        let buffer = self.read_exact(2, section).await?;
        Ok(byte_order.read_u16([buffer[0], buffer[1]]))
    }

    pub async fn read_u32(&mut self, byte_order: ByteOrder, section: Section) -> Result<u32> {
        let buffer = self.read_exact(4, section).await?;
        Ok(byte_order.read_u32([buffer[0], buffer[1], buffer[2], buffer[3]]))
    }

    pub async fn read_u64(&mut self, byte_order: ByteOrder, section: Section) -> Result<u64> {
        let buffer = self.read_exact(8, section).await?;
        Ok(byte_order.read_u64([
            buffer[0], buffer[1], buffer[2], buffer[3], buffer[4], buffer[5], buffer[6], buffer[7],
        ]))
    }

    /// Reads and validates an exact byte sequence. Returns the given
    /// `on_mismatch` error kind if the bytes do not match.
    pub async fn expect_bytes(
        &mut self,
        expected: &[u8],
        section: Section,
        on_mismatch: FormatErrorKind,
    ) -> Result<()> {
        let position = self.position;
        let actual = self.read_exact(expected.len(), section).await?;
        if actual != expected {
            return Err(DtaError::format(section, position, on_mismatch));
        }
        Ok(())
    }

    /// Reads a fixed-length byte field and decodes it as a
    /// null-terminated string. Returns an empty string when `len` is 0.
    pub async fn read_fixed_string(
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
        let buffer = self.read_exact(len, section).await?;
        decode_fixed_string(buffer, encoding, section, field, position)
    }
}
