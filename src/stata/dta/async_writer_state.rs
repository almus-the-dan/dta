use std::io::SeekFrom;

use encoding_rs::Encoding;
use tokio::io::{AsyncSeek, AsyncSeekExt, AsyncWrite, AsyncWriteExt};

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
    map_offset_base: Option<u64>,
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
            map_offset_base: None,
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

    /// Records where the 14 × `u64` payload of the XML `<map>`
    /// starts. Called by the async schema writer immediately before
    /// it emits the 14 placeholder `u64`s.
    pub fn set_map_offset_base(&mut self, offset: u64) {
        self.map_offset_base = Some(offset);
    }

    /// Byte offset of the variable count (K) field inside the header,
    /// captured by the header writer so the schema writer can patch
    /// it with `schema.variables().len()` once the schema is known.
    #[must_use]
    pub fn header_variable_count_offset(&self) -> Option<u64> {
        self.header_variable_count_offset
    }

    /// Records where the header K field was written. Called by the
    /// async header writer just before emitting the K placeholder.
    pub fn set_header_variable_count_offset(&mut self, offset: u64) {
        self.header_variable_count_offset = Some(offset);
    }

    /// Byte offset of the observation count (N) field inside the
    /// header, captured by the header writer so the record writer
    /// can patch it with the accumulated row count at its transition.
    #[must_use]
    pub fn header_observation_count_offset(&self) -> Option<u64> {
        self.header_observation_count_offset
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

    /// Narrows a `u64` value to `u16`, producing a `FieldTooLarge`
    /// format error on overflow.
    pub fn narrow_to_u16(&self, value: u64, section: Section, field: Field) -> Result<u16> {
        u16::try_from(value).map_err(|_| {
            DtaError::format(
                section,
                self.position,
                FormatErrorKind::FieldTooLarge {
                    field,
                    max: u64::from(u16::MAX),
                    actual: value,
                },
            )
        })
    }

    /// Narrows a `u64` value to `u32`, producing a `FieldTooLarge`
    /// format error on overflow.
    pub fn narrow_to_u32(&self, value: u64, section: Section, field: Field) -> Result<u32> {
        u32::try_from(value).map_err(|_| {
            DtaError::format(
                section,
                self.position,
                FormatErrorKind::FieldTooLarge {
                    field,
                    max: u64::from(u32::MAX),
                    actual: value,
                },
            )
        })
    }
}

// -- Primitive writers --------------------------------------------------------

#[allow(dead_code)] // methods light up as the async chain grows past the header phase
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

    pub async fn write_i32(
        &mut self,
        value: i32,
        byte_order: ByteOrder,
        section: Section,
    ) -> Result<()> {
        self.write_exact(&byte_order.write_i32(value), section)
            .await
    }

    /// Writes `bytes` padded with trailing zeros out to `width` bytes
    /// total, reusing the internal scratch buffer. Caller is
    /// responsible for validating `bytes.len() <= width`.
    pub async fn write_padded_bytes(
        &mut self,
        bytes: &[u8],
        width: usize,
        section: Section,
    ) -> Result<()> {
        debug_assert!(
            bytes.len() <= width,
            "write_padded_bytes: {} > {}",
            bytes.len(),
            width,
        );
        self.buffer.clear();
        self.buffer.extend_from_slice(bytes);
        self.buffer.resize(width, 0);
        self.writer
            .write_all(&self.buffer)
            .await
            .map_err(|e| DtaError::io(section, e))?;
        self.position += u64::try_from(width).expect("field width exceeds u64");
        Ok(())
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

// -- Seek-back patching -------------------------------------------------------

#[allow(dead_code)] // methods light up as the async chain grows past the header phase
impl<W: AsyncWrite + AsyncSeek + Unpin> AsyncWriterState<W> {
    /// Writes an exact byte slice at an earlier absolute byte offset,
    /// then seeks back to the end.
    pub async fn patch_bytes_at(
        &mut self,
        offset: u64,
        bytes: &[u8],
        section: Section,
    ) -> Result<()> {
        let end_position = self.position;
        self.writer
            .seek(SeekFrom::Start(offset))
            .await
            .map_err(|e| DtaError::io(section, e))?;
        self.writer
            .write_all(bytes)
            .await
            .map_err(|e| DtaError::io(section, e))?;
        self.writer
            .seek(SeekFrom::Start(end_position))
            .await
            .map_err(|e| DtaError::io(section, e))?;
        Ok(())
    }

    /// Patches a `u16` at an earlier absolute byte offset.
    pub async fn patch_u16_at(
        &mut self,
        offset: u64,
        value: u16,
        byte_order: ByteOrder,
        section: Section,
    ) -> Result<()> {
        self.patch_bytes_at(offset, &byte_order.write_u16(value), section)
            .await
    }

    /// Patches a `u32` at an earlier absolute byte offset.
    pub async fn patch_u32_at(
        &mut self,
        offset: u64,
        value: u32,
        byte_order: ByteOrder,
        section: Section,
    ) -> Result<()> {
        self.patch_bytes_at(offset, &byte_order.write_u32(value), section)
            .await
    }

    /// Patches a `u64` at an earlier absolute byte offset.
    pub async fn patch_u64_at(
        &mut self,
        offset: u64,
        value: u64,
        byte_order: ByteOrder,
        section: Section,
    ) -> Result<()> {
        self.patch_bytes_at(offset, &byte_order.write_u64(value), section)
            .await
    }

    /// Patches a single `u64` slot in the XML `<map>` payload.
    ///
    /// `index` is the 0-based slot index (valid range: 0..14).
    /// Requires [`set_map_offset_base`](Self::set_map_offset_base) to
    /// have been called — returns
    /// [`DtaError::missing_section_offsets`](super::dta_error::DtaError::missing_section_offsets)
    /// otherwise.
    pub async fn patch_map_entry(
        &mut self,
        index: usize,
        value: u64,
        byte_order: ByteOrder,
        section: Section,
    ) -> Result<()> {
        let base = self
            .map_offset_base
            .ok_or_else(|| DtaError::missing_section_offsets(section))?;
        let slot_offset = index
            .checked_mul(8)
            .and_then(|bytes| u64::try_from(bytes).ok())
            .ok_or_else(|| map_slot_overflow_error(section, self.position, index))?;
        self.patch_u64_at(base + slot_offset, value, byte_order, section)
            .await
    }
}

/// Produces a [`FormatErrorKind::FieldTooLarge`] for a map-slot byte
/// offset (`index * 8`) that overflows `u64`.
fn map_slot_overflow_error(section: Section, position: u64, index: usize) -> DtaError {
    DtaError::format(
        section,
        position,
        FormatErrorKind::FieldTooLarge {
            field: Field::VariableCount,
            max: u64::MAX,
            actual: u64::try_from(index).unwrap_or(u64::MAX).saturating_mul(8),
        },
    )
}
