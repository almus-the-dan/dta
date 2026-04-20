use encoding_rs::Encoding;
use tokio::io::{AsyncWrite, AsyncWriteExt};

use super::byte_order::ByteOrder;
use super::dta_error::{DtaError, Field, FormatErrorKind, Result, Section};
use super::string_encoding::encode_value;

/// Shared state carried across the async writer typestate chain.
///
/// Owns the underlying `AsyncWrite` sink, tracks the current byte
/// position for offset bookkeeping, caches a scratch buffer reused
/// across encoding operations, and holds the offset slots that later
/// stages need for K/N patching once those stages exist.
///
/// Each `write_*` primitive returns a future; the caller `.await`s
/// it, the bytes are written, and the position counter advances.
/// Seek-based patching (map offsets, K/N backfill) is not needed for
/// the header-only POC and will land alongside the async schema and
/// record writers.
#[derive(Debug)]
#[allow(dead_code)] // fields/methods light up as the async chain grows past the header phase
pub(crate) struct AsyncWriterState<W> {
    writer: W,
    encoding: &'static Encoding,
    buffer: Vec<u8>,
    position: u64,
    header_variable_count_offset: Option<u64>,
    header_observation_count_offset: Option<u64>,
}

// -- Construction and accessors ----------------------------------------------

#[allow(dead_code)] // methods light up as the async chain grows past the header phase
impl<W> AsyncWriterState<W> {
    #[must_use]
    pub fn new(writer: W, encoding: &'static Encoding) -> Self {
        Self {
            writer,
            encoding,
            buffer: Vec::new(),
            position: 0,
            header_variable_count_offset: None,
            header_observation_count_offset: None,
        }
    }

    /// Current byte offset in the output sink.
    #[must_use]
    pub fn position(&self) -> u64 {
        self.position
    }

    /// The active character encoding used for string fields.
    #[must_use]
    pub fn encoding(&self) -> &'static Encoding {
        self.encoding
    }

    /// Returns a new state with the given encoding, preserving the
    /// writer, buffer allocation, position, and offset slots.
    #[must_use]
    pub fn with_encoding(self, encoding: &'static Encoding) -> Self {
        Self { encoding, ..self }
    }

    /// Records where the header K field was written. Called by the
    /// async header writer just before emitting the K placeholder.
    pub fn set_header_variable_count_offset(&mut self, offset: u64) {
        self.header_variable_count_offset = Some(offset);
    }

    /// Records where the header N field was written. Called by the
    /// async header writer just before emitting the N placeholder.
    pub fn set_header_observation_count_offset(&mut self, offset: u64) {
        self.header_observation_count_offset = Some(offset);
    }

    /// Consumes the state and returns the inner writer.
    pub fn into_inner(self) -> W {
        self.writer
    }
}

// -- Primitive writers --------------------------------------------------------

impl<W: AsyncWrite + Unpin> AsyncWriterState<W> {
    /// Writes an exact byte slice, advancing the tracked position.
    pub async fn write_exact(&mut self, bytes: &[u8], section: Section) -> Result<()> {
        self.writer
            .write_all(bytes)
            .await
            .map_err(|e| DtaError::io(section, e))?;
        self.position += u64::try_from(bytes.len()).expect("buffer length exceeds u64");
        Ok(())
    }

    pub async fn write_u8(&mut self, value: u8, section: Section) -> Result<()> {
        self.write_exact(&[value], section).await
    }

    pub async fn write_u16(
        &mut self,
        value: u16,
        byte_order: ByteOrder,
        section: Section,
    ) -> Result<()> {
        self.write_exact(&byte_order.write_u16(value), section)
            .await
    }

    pub async fn write_u32(
        &mut self,
        value: u32,
        byte_order: ByteOrder,
        section: Section,
    ) -> Result<()> {
        self.write_exact(&byte_order.write_u32(value), section)
            .await
    }

    pub async fn write_u64(
        &mut self,
        value: u64,
        byte_order: ByteOrder,
        section: Section,
    ) -> Result<()> {
        self.write_exact(&byte_order.write_u64(value), section)
            .await
    }

    /// Encodes `value` with the active encoding and writes it as a
    /// fixed-length, null-padded field of exactly `len` bytes.
    pub async fn write_fixed_string(
        &mut self,
        value: &str,
        len: usize,
        section: Section,
        field: Field,
    ) -> Result<()> {
        if len == 0 {
            return Ok(());
        }
        let position = self.position;
        let encoded = encode_value(value, self.encoding, section, field, position)?;
        if encoded.len() > len {
            return Err(DtaError::format(
                section,
                position,
                FormatErrorKind::FieldTooLarge {
                    field,
                    max: u64::try_from(len).expect("field length exceeds u64"),
                    actual: u64::try_from(encoded.len()).expect("encoded length exceeds u64"),
                },
            ));
        }
        self.buffer.clear();
        self.buffer.extend_from_slice(&encoded);
        self.buffer.resize(len, 0);
        self.writer
            .write_all(&self.buffer)
            .await
            .map_err(|e| DtaError::io(section, e))?;
        self.position += u64::try_from(len).expect("field length exceeds u64");
        Ok(())
    }
}
