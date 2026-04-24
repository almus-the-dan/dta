use std::io::{Seek, SeekFrom, Write};

use encoding_rs::Encoding;

use super::byte_order::ByteOrder;
use super::dta_error::{DtaError, Field, FormatErrorKind, Result, Section};
use super::string_encoding::encode_value;

/// Shared state carried across the writer typestate chain.
///
/// Owns the underlying sink, tracks the current byte position for
/// offset bookkeeping, caches a scratch buffer reused across encoding
/// operations, and remembers the section offsets written into the
/// XML `<map>` so later writers can patch them with the seek-back
/// primitive.
///
/// Parallels [`ReaderState`](super::reader_state::ReaderState) on
/// the read side.
#[derive(Debug)]
pub(crate) struct WriterState<W> {
    writer: W,
    encoding: &'static Encoding,
    buffer: Vec<u8>,
    position: u64,
    map_offset_base: Option<u64>,
    header_variable_count_offset: Option<u64>,
    header_observation_count_offset: Option<u64>,
}

// -- Construction and accessors ----------------------------------------------

impl<W> WriterState<W> {
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
    /// writer, buffer allocation, position, and section offsets.
    #[must_use]
    pub fn with_encoding(self, encoding: &'static Encoding) -> Self {
        Self { encoding, ..self }
    }

    /// Absolute byte offset where the 14 × `u64` payload of the XML
    /// Records where the XML `<map>` payload starts. Called by the
    /// schema writer immediately before it writes the 14 placeholder
    /// `u64`s. The recorded position is later consumed by
    /// [`patch_map_entry`](Self::patch_map_entry) via direct field
    /// access — there is no public getter.
    pub fn set_map_offset_base(&mut self, offset: u64) {
        self.map_offset_base = Some(offset);
    }

    /// Byte offset of the variable count (K) field inside the header,
    /// captured by the header writer so the schema writer can patch
    /// it with `schema.variables().len()` once the schema is known.
    /// The field width varies by release — the patcher decides
    /// whether to write `u16` (pre-V119) or `u32` (V119+).
    #[must_use]
    pub fn header_variable_count_offset(&self) -> Option<u64> {
        self.header_variable_count_offset
    }

    /// Records where the header K field was written. Called by the
    /// header writer just before emitting the K placeholder.
    pub fn set_header_variable_count_offset(&mut self, offset: u64) {
        self.header_variable_count_offset = Some(offset);
    }

    /// Byte offset of the observation count (N) field inside the
    /// header, captured by the header writer so the record writer
    /// can patch it with the accumulated row count at its transition.
    /// The field width varies by release — the patcher decides
    /// whether to write `u32` (pre-V118) or `u64` (V118+).
    #[must_use]
    pub fn header_observation_count_offset(&self) -> Option<u64> {
        self.header_observation_count_offset
    }

    /// Records where the header N field was written. Called by the
    /// header writer just before emitting the N placeholder.
    pub fn set_header_observation_count_offset(&mut self, offset: u64) {
        self.header_observation_count_offset = Some(offset);
    }

    /// Consumes the state and returns the inner writer.
    ///
    /// Called by [`ValueLabelWriter::finish`](super::value_label_writer::ValueLabelWriter::finish)
    /// after the closing tag has been emitted.
    pub fn into_inner(self) -> W {
        self.writer
    }

    /// Narrows a `u64` value to `u16`, producing a `FieldTooLarge`
    /// format error (tagged with the current [`position`](Self::position))
    /// on overflow.
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
    /// format error (tagged with the current [`position`](Self::position))
    /// on overflow.
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

impl<W: Write> WriterState<W> {
    /// Writes an exact byte slice, advancing the tracked position.
    pub fn write_exact(&mut self, bytes: &[u8], section: Section) -> Result<()> {
        self.writer
            .write_all(bytes)
            .map_err(|e| DtaError::io(section, e))?;
        self.position += u64::try_from(bytes.len()).expect("buffer length exceeds u64");
        Ok(())
    }

    pub fn write_u8(&mut self, value: u8, section: Section) -> Result<()> {
        self.write_exact(&[value], section)
    }

    pub fn write_u16(&mut self, value: u16, byte_order: ByteOrder, section: Section) -> Result<()> {
        self.write_exact(&byte_order.write_u16(value), section)
    }

    pub fn write_u32(&mut self, value: u32, byte_order: ByteOrder, section: Section) -> Result<()> {
        self.write_exact(&byte_order.write_u32(value), section)
    }

    pub fn write_u64(&mut self, value: u64, byte_order: ByteOrder, section: Section) -> Result<()> {
        self.write_exact(&byte_order.write_u64(value), section)
    }

    pub fn write_i32(&mut self, value: i32, byte_order: ByteOrder, section: Section) -> Result<()> {
        self.write_exact(&byte_order.write_i32(value), section)
    }

    /// Writes `bytes` padded with trailing zeros out to `width`
    /// bytes total, reusing the internal scratch buffer — no
    /// allocation per call once the buffer's capacity settles at
    /// the largest string the caller writes.
    ///
    /// Caller is responsible for validating `bytes.len() <= width`;
    /// this primitive trusts its input so the error-shape concern
    /// stays with the caller (which knows what field / identifier
    /// to report). Violating the precondition is caught by the
    /// `debug_assert!`.
    pub fn write_padded_bytes(
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
            .map_err(|e| DtaError::io(section, e))?;
        self.position += u64::try_from(width).expect("field width exceeds u64");
        Ok(())
    }
}

// -- Seek-back patching -------------------------------------------------------

impl<W: Write + Seek> WriterState<W> {
    /// Writes an exact byte slice at an earlier absolute byte offset,
    /// then seeks back to the end. The underlying primitive behind
    /// the typed `patch_u16_at` / `patch_u32_at` / `patch_u64_at`
    /// helpers.
    pub fn patch_bytes_at(&mut self, offset: u64, bytes: &[u8], section: Section) -> Result<()> {
        let end_position = self.position;
        self.writer
            .seek(SeekFrom::Start(offset))
            .map_err(|e| DtaError::io(section, e))?;
        self.writer
            .write_all(bytes)
            .map_err(|e| DtaError::io(section, e))?;
        self.writer
            .seek(SeekFrom::Start(end_position))
            .map_err(|e| DtaError::io(section, e))?;
        Ok(())
    }

    /// Patches a `u16` at an earlier absolute byte offset. Used to
    /// backfill header K (pre-V119) once the schema is known.
    pub fn patch_u16_at(
        &mut self,
        offset: u64,
        value: u16,
        byte_order: ByteOrder,
        section: Section,
    ) -> Result<()> {
        self.patch_bytes_at(offset, &byte_order.write_u16(value), section)
    }

    /// Patches a `u32` at an earlier absolute byte offset. Used to
    /// backfill header K (V119) or header N (pre-V118) once the
    /// accumulated count is known.
    pub fn patch_u32_at(
        &mut self,
        offset: u64,
        value: u32,
        byte_order: ByteOrder,
        section: Section,
    ) -> Result<()> {
        self.patch_bytes_at(offset, &byte_order.write_u32(value), section)
    }

    /// Patches a `u64` at an earlier absolute byte offset. Used to
    /// patch `<map>` payload slots and header N (V118+).
    pub fn patch_u64_at(
        &mut self,
        offset: u64,
        value: u64,
        byte_order: ByteOrder,
        section: Section,
    ) -> Result<()> {
        self.patch_bytes_at(offset, &byte_order.write_u64(value), section)
    }

    /// Patches a single `u64` slot in the XML `<map>` payload.
    ///
    /// `index` is the 0-based slot index (valid range: 0..14).
    /// Requires [`set_map_offset_base`](Self::set_map_offset_base) to
    /// have been called — returns
    /// [`missing_section_offsets`](DtaError::missing_section_offsets)
    /// otherwise.
    pub fn patch_map_entry(
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

// -- String writing -----------------------------------------------------------

impl<W: Write> WriterState<W> {
    /// Encodes `value` with the active encoding and writes it as a
    /// fixed-length, null-padded field of exactly `len` bytes.
    ///
    /// # Errors
    ///
    /// Returns [`FormatErrorKind::InvalidEncoding`] if `value`
    /// contains characters the active encoding cannot represent, and
    /// [`FormatErrorKind::FieldTooLarge`] if the encoded length
    /// exceeds `len`.
    pub fn write_fixed_string(
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
            let error = DtaError::format(
                section,
                position,
                FormatErrorKind::FieldTooLarge {
                    field,
                    max: u64::try_from(len).expect("field length exceeds u64"),
                    actual: u64::try_from(encoded.len()).expect("encoded length exceeds u64"),
                },
            );
            return Err(error);
        }
        self.buffer.clear();
        self.buffer.extend_from_slice(&encoded);
        self.buffer.resize(len, 0);
        self.writer
            .write_all(&self.buffer)
            .map_err(|e| DtaError::io(section, e))?;
        self.position += u64::try_from(len).expect("field length exceeds u64");
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;

    use super::*;

    fn new_state() -> WriterState<Cursor<Vec<u8>>> {
        WriterState::new(Cursor::new(Vec::new()), encoding_rs::UTF_8)
    }

    // -- Narrowing helpers ---------------------------------------------------

    #[test]
    fn narrow_to_u16_succeeds_at_max() {
        let state = new_state();
        let result = state
            .narrow_to_u16(u64::from(u16::MAX), Section::Header, Field::VariableCount)
            .unwrap();
        assert_eq!(result, u16::MAX);
    }

    #[test]
    fn narrow_to_u16_errors_above_max() {
        let state = new_state();
        let error = state
            .narrow_to_u16(
                u64::from(u16::MAX) + 1,
                Section::Header,
                Field::VariableCount,
            )
            .unwrap_err();
        assert!(matches!(
            error,
            DtaError::Format(ref e) if matches!(
                e.kind(),
                FormatErrorKind::FieldTooLarge {
                    field: Field::VariableCount,
                    max,
                    actual,
                } if max == u64::from(u16::MAX) && actual == u64::from(u16::MAX) + 1,
            )
        ));
    }

    #[test]
    fn narrow_to_u32_succeeds_at_max() {
        let state = new_state();
        let result = state
            .narrow_to_u32(
                u64::from(u32::MAX),
                Section::Header,
                Field::ObservationCount,
            )
            .unwrap();
        assert_eq!(result, u32::MAX);
    }

    #[test]
    fn narrow_to_u32_errors_above_max() {
        let state = new_state();
        let error = state
            .narrow_to_u32(
                u64::from(u32::MAX) + 1,
                Section::Header,
                Field::ObservationCount,
            )
            .unwrap_err();
        assert!(matches!(
            error,
            DtaError::Format(ref e) if matches!(
                e.kind(),
                FormatErrorKind::FieldTooLarge {
                    field: Field::ObservationCount,
                    max,
                    actual,
                } if max == u64::from(u32::MAX) && actual == u64::from(u32::MAX) + 1,
            )
        ));
    }

    // -- patch_u16_at / patch_u32_at round-trip ------------------------------

    #[test]
    fn patch_u16_at_overwrites_placeholder() {
        let mut state = new_state();
        state
            .write_u16(0, ByteOrder::LittleEndian, Section::Header)
            .unwrap();
        state
            .write_u16(0xDEAD, ByteOrder::LittleEndian, Section::Header)
            .unwrap();
        state
            .patch_u16_at(0, 0xBEEF, ByteOrder::LittleEndian, Section::Header)
            .unwrap();
        let bytes = state.into_inner().into_inner();
        assert_eq!(bytes, vec![0xEF, 0xBE, 0xAD, 0xDE]);
    }

    #[test]
    fn patch_u32_at_overwrites_placeholder() {
        let mut state = new_state();
        state
            .write_u32(0, ByteOrder::BigEndian, Section::Header)
            .unwrap();
        state
            .patch_u32_at(0, 0xCAFE_BABE, ByteOrder::BigEndian, Section::Header)
            .unwrap();
        let bytes = state.into_inner().into_inner();
        assert_eq!(bytes, vec![0xCA, 0xFE, 0xBA, 0xBE]);
    }
}
