use std::io::{Seek, SeekFrom, Write};

use encoding_rs::Encoding;

use super::byte_order::ByteOrder;
use super::dta_error::{DtaError, Field, FormatErrorKind, Result, Section};
use super::section_offsets::SectionOffsets;

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
    section_offsets: Option<SectionOffsets>,
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
            section_offsets: None,
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

    /// Section offsets written into the `<map>`, if known.
    ///
    /// Populated by [`SchemaWriter`](super::schema_writer::SchemaWriter)
    /// (with placeholder zeros for XML formats) and patched by later
    /// writers as each section's real offset is determined.
    #[must_use]
    pub fn section_offsets(&self) -> Option<&SectionOffsets> {
        self.section_offsets.as_ref()
    }

    /// Mutable access to section offsets, for writers that need to
    /// record a newly determined section offset before patching the
    /// map.
    pub fn section_offsets_mut(&mut self) -> Option<&mut SectionOffsets> {
        self.section_offsets.as_mut()
    }

    /// Stores the section offsets. Called by the schema writer after
    /// emitting the `<map>` placeholder.
    pub fn set_section_offsets(&mut self, offsets: SectionOffsets) {
        self.section_offsets = Some(offsets);
    }

    /// Consumes the state and returns the inner writer.
    ///
    /// Called by [`ValueLabelWriter::finish`](super::value_label_writer::ValueLabelWriter::finish)
    /// after the closing tag has been emitted.
    pub fn into_inner(self) -> W {
        self.writer
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
}

// -- Seek-back patching -------------------------------------------------------

impl<W: Write + Seek> WriterState<W> {
    /// Writes a `u64` at an earlier absolute byte offset, then seeks
    /// back to the end. Used to patch the XML `<map>` placeholders
    /// once a section's actual offset is known.
    pub fn patch_u64_at(
        &mut self,
        offset: u64,
        value: u64,
        byte_order: ByteOrder,
        section: Section,
    ) -> Result<()> {
        let end_position = self.position;
        self.writer
            .seek(SeekFrom::Start(offset))
            .map_err(|e| DtaError::io(section, e))?;
        self.writer
            .write_all(&byte_order.write_u64(value))
            .map_err(|e| DtaError::io(section, e))?;
        self.writer
            .seek(SeekFrom::Start(end_position))
            .map_err(|e| DtaError::io(section, e))?;
        Ok(())
    }
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
        let (encoded, _, had_unmappable) = self.encoding.encode(value);
        if had_unmappable {
            return Err(DtaError::format(
                section,
                position,
                FormatErrorKind::InvalidEncoding { field },
            ));
        }
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
            .map_err(|e| DtaError::io(section, e))?;
        self.position += u64::try_from(len).expect("field length exceeds u64");
        Ok(())
    }
}
