use std::io::{Seek, Write};

use encoding_rs::Encoding;

use super::byte_order::ByteOrder;
use super::dta_error::{DtaError, Field, FormatErrorKind, Result, Section};
use super::header::Header;
use super::release::Release;
use super::schema_writer::SchemaWriter;
use super::writer_state::WriterState;
use crate::stata::stata_timestamp::StataTimestamp;

/// Entry point for writing a DTA file.
///
/// Created via [`DtaWriter::from_writer`](super::dta_writer::DtaWriter::from_writer)
/// or [`DtaWriter::from_file`](super::dta_writer::DtaWriter::from_file),
/// then call [`write_header`](Self::write_header) to emit the file
/// header and advance to schema writing.
#[derive(Debug)]
pub struct HeaderWriter<W> {
    state: WriterState<W>,
    encoding_override: Option<&'static Encoding>,
}

impl<W> HeaderWriter<W> {
    /// Creates a header writer. The encoding override, if provided,
    /// will be used regardless of the header's release; otherwise the
    /// encoding is determined from the release number at writing time.
    #[must_use]
    pub(crate) fn new(writer: W, encoding: Option<&'static Encoding>) -> Self {
        // The initial encoding is a placeholder — it is replaced once
        // the header's release is known (or kept if an override was given).
        let initial_encoding = encoding.unwrap_or(encoding_rs::UTF_8);
        Self {
            state: WriterState::new(writer, initial_encoding),
            encoding_override: encoding,
        }
    }
}

impl<W: Write + Seek> HeaderWriter<W> {
    /// Writes the file header and transitions to schema writing.
    ///
    /// For binary formats (104–116) this emits the fixed 10-byte
    /// preamble followed by the dataset label and timestamp fields.
    /// For XML formats (117+) this emits the `<stata_dta><header>`
    /// opening tags and the `<release>`, `<byteorder>`, `<K>`, `<N>`,
    /// `<label>`, and `<timestamp>` fields.
    ///
    /// # Errors
    ///
    /// Returns [`DtaError::Io`](super::dta_error::DtaError::Io) on
    /// sink failures and [`DtaError::Format`](super::dta_error::DtaError::Format)
    /// if the [`Header`] contains values the target format cannot
    /// represent (e.g., `variable_count > u16::MAX` for a release
    /// that only stores a 16-bit count).
    pub fn write_header(mut self, header: Header) -> Result<SchemaWriter<W>> {
        let release = header.release();
        let encoding = self
            .encoding_override
            .unwrap_or_else(|| release.default_encoding());
        self.state = self.state.with_encoding(encoding);

        if release.is_xml_like() {
            self.write_xml_header(&header)?;
        } else {
            self.write_binary_header(&header)?;
        }

        Ok(SchemaWriter::new(self.state, header))
    }
}

// ---------------------------------------------------------------------------
// Binary format (104–116)
// ---------------------------------------------------------------------------

/// Fixed filetype byte in the binary preamble. Always `0x01`.
const BINARY_FILETYPE: u8 = 0x01;

/// Reserved padding byte following the filetype. Always `0x00`.
const BINARY_RESERVED_PADDING: u8 = 0x00;

impl<W: Write> HeaderWriter<W> {
    fn write_binary_header(&mut self, header: &Header) -> Result<()> {
        let release = header.release();
        let byte_order = header.byte_order();

        self.state.write_u8(release.to_byte(), Section::Header)?;
        self.state.write_u8(byte_order.to_byte(), Section::Header)?;
        self.state.write_u8(BINARY_FILETYPE, Section::Header)?;
        self.state
            .write_u8(BINARY_RESERVED_PADDING, Section::Header)?;

        // Binary formats (104–116) always use u16 K and u32 N. Emit
        // zero placeholders — the schema writer patches K and the
        // record writer patches N once counts are known.
        self.state
            .set_header_variable_count_offset(self.state.position());
        self.state.write_u16(0, byte_order, Section::Header)?;
        self.state
            .set_header_observation_count_offset(self.state.position());
        self.state.write_u32(0, byte_order, Section::Header)?;

        self.state.write_fixed_string(
            header.dataset_label(),
            release.dataset_label_len(),
            Section::Header,
            Field::DatasetLabel,
        )?;

        self.write_fixed_timestamp(header.timestamp(), release.timestamp_len())?;
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// XML format (117+)
// ---------------------------------------------------------------------------

impl<W: Write> HeaderWriter<W> {
    fn write_xml_header(&mut self, header: &Header) -> Result<()> {
        let release = header.release();
        let byte_order = header.byte_order();

        self.state
            .write_exact(b"<stata_dta><header>", Section::Header)?;

        // <release>NNN</release>
        self.state.write_exact(b"<release>", Section::Header)?;
        let release_digits = format!("{:03}", release.to_byte());
        self.state
            .write_exact(release_digits.as_bytes(), Section::Header)?;
        self.state.write_exact(b"</release>", Section::Header)?;

        // <byteorder>MSF|LSF</byteorder>
        self.state.write_exact(b"<byteorder>", Section::Header)?;
        self.state
            .write_exact(byte_order.to_string().as_bytes(), Section::Header)?;
        self.state.write_exact(b"</byteorder>", Section::Header)?;

        // <K>nvar</K> — emit a zero placeholder; the schema writer
        // patches this field once schema.variables().len() is known.
        self.state.write_exact(b"<K>", Section::Header)?;
        self.state
            .set_header_variable_count_offset(self.state.position());
        if release.supports_extended_variable_count() {
            self.state.write_u32(0, byte_order, Section::Header)?;
        } else {
            self.state.write_u16(0, byte_order, Section::Header)?;
        }
        self.state.write_exact(b"</K>", Section::Header)?;

        // <N>nobs</N> — emit a zero placeholder; the record writer
        // patches this field once the accumulated row count is known.
        self.state.write_exact(b"<N>", Section::Header)?;
        self.state
            .set_header_observation_count_offset(self.state.position());
        if release.supports_extended_observation_count() {
            self.state.write_u64(0, byte_order, Section::Header)?;
        } else {
            self.state.write_u32(0, byte_order, Section::Header)?;
        }
        self.state.write_exact(b"</N>", Section::Header)?;

        // <label> [len] [bytes] </label>
        self.state.write_exact(b"<label>", Section::Header)?;
        self.write_xml_label(header.dataset_label(), release, byte_order)?;
        self.state.write_exact(b"</label>", Section::Header)?;

        // <timestamp> [u8 len] [bytes] </timestamp>
        self.state.write_exact(b"<timestamp>", Section::Header)?;
        self.write_xml_timestamp(header.timestamp())?;
        self.state.write_exact(b"</timestamp>", Section::Header)?;

        self.state.write_exact(b"</header>", Section::Header)?;
        Ok(())
    }

    fn write_xml_label(
        &mut self,
        label: &str,
        release: Release,
        byte_order: ByteOrder,
    ) -> Result<()> {
        let (encoded, _, had_unmappable) = self.state.encoding().encode(label);
        if had_unmappable {
            return Err(DtaError::format(
                Section::Header,
                self.state.position(),
                FormatErrorKind::InvalidEncoding {
                    field: Field::DatasetLabel,
                },
            ));
        }
        if release.supports_extended_dataset_label() {
            let length = u16::try_from(encoded.len()).map_err(|_| {
                DtaError::format(
                    Section::Header,
                    self.state.position(),
                    FormatErrorKind::FieldTooLarge {
                        field: Field::DatasetLabel,
                        max: u64::from(u16::MAX),
                        actual: u64::try_from(encoded.len()).expect("label length exceeds u64"),
                    },
                )
            })?;
            self.state.write_u16(length, byte_order, Section::Header)?;
        } else {
            let length = u8::try_from(encoded.len()).map_err(|_| {
                DtaError::format(
                    Section::Header,
                    self.state.position(),
                    FormatErrorKind::FieldTooLarge {
                        field: Field::DatasetLabel,
                        max: u64::from(u8::MAX),
                        actual: u64::try_from(encoded.len()).expect("label length exceeds u64"),
                    },
                )
            })?;
            self.state.write_u8(length, Section::Header)?;
        }
        self.state.write_exact(&encoded, Section::Header)?;
        Ok(())
    }

    fn write_xml_timestamp(&mut self, timestamp: Option<&StataTimestamp>) -> Result<()> {
        let formatted = timestamp.map(ToString::to_string);
        let formatted = formatted.as_deref().unwrap_or("");
        let bytes = formatted.as_bytes();
        let length = u8::try_from(bytes.len()).map_err(|_| {
            DtaError::format(
                Section::Header,
                self.state.position(),
                FormatErrorKind::FieldTooLarge {
                    field: Field::Timestamp,
                    max: u64::from(u8::MAX),
                    actual: u64::try_from(bytes.len()).expect("timestamp length exceeds u64"),
                },
            )
        })?;
        self.state.write_u8(length, Section::Header)?;
        self.state.write_exact(bytes, Section::Header)?;
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Header-specific helpers
// ---------------------------------------------------------------------------

impl<W: Write> HeaderWriter<W> {
    /// Writes a fixed-length timestamp field. Absent timestamps emit
    /// an all-zero field; `len == 0` skips the field entirely.
    fn write_fixed_timestamp(
        &mut self,
        timestamp: Option<&StataTimestamp>,
        len: usize,
    ) -> Result<()> {
        if len == 0 {
            return Ok(());
        }
        let formatted = timestamp.map(ToString::to_string);
        let formatted = formatted.as_deref().unwrap_or("");
        self.state
            .write_fixed_string(formatted, len, Section::Header, Field::Timestamp)
    }
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;

    use super::*;
    use crate::stata::dta::dta_reader::DtaReader;
    use crate::stata::dta::dta_writer::DtaWriter;
    use crate::stata::dta::schema::Schema;

    /// Writes `header` through the full writer pipeline with an
    /// empty schema + empty data + empty value labels, then reads
    /// the file back with the real reader. Returns the round-tripped
    /// header so each test can assert on the header-only fields it
    /// cares about (release, byte order, label, timestamp).
    fn round_trip(header: &Header) -> Header {
        let bytes = DtaWriter::new()
            .from_writer(Cursor::new(Vec::<u8>::new()))
            .write_header(header.clone())
            .unwrap()
            .write_schema(Schema::builder().build().unwrap())
            .unwrap()
            .into_record_writer()
            .unwrap()
            .into_long_string_writer()
            .unwrap()
            .into_value_label_writer()
            .unwrap()
            .finish()
            .unwrap()
            .into_inner();
        DtaReader::new()
            .from_reader(Cursor::new(bytes))
            .read_header()
            .unwrap()
            .header()
            .clone()
    }

    // -- Binary header round-trips (formats 104–116) -------------------------

    #[test]
    fn binary_v114_little_endian_round_trip() {
        let timestamp = StataTimestamp::parse("01 Jan 2024 13:45").unwrap();
        let original = Header::builder(Release::V114, ByteOrder::LittleEndian)
            .dataset_label("My Dataset")
            .timestamp(Some(timestamp))
            .build();
        let parsed = round_trip(&original);
        assert_eq!(parsed.release(), Release::V114);
        assert_eq!(parsed.byte_order(), ByteOrder::LittleEndian);
        assert_eq!(parsed.dataset_label(), "My Dataset");
        assert_eq!(parsed.timestamp(), Some(&timestamp));
    }

    #[test]
    fn binary_v114_big_endian_round_trip() {
        let original = Header::builder(Release::V114, ByteOrder::BigEndian)
            .dataset_label("BE test")
            .build();
        let parsed = round_trip(&original);
        assert_eq!(parsed.byte_order(), ByteOrder::BigEndian);
        assert_eq!(parsed.dataset_label(), "BE test");
    }

    #[test]
    fn binary_v104_no_timestamp_round_trip() {
        let original = Header::builder(Release::V104, ByteOrder::LittleEndian)
            .dataset_label("Old format")
            .build();
        let parsed = round_trip(&original);
        assert_eq!(parsed.release(), Release::V104);
        assert_eq!(parsed.dataset_label(), "Old format");
        assert!(parsed.timestamp().is_none());
    }

    #[test]
    fn binary_empty_label_round_trip() {
        let original = Header::builder(Release::V114, ByteOrder::LittleEndian).build();
        let parsed = round_trip(&original);
        assert_eq!(parsed.dataset_label(), "");
        assert!(parsed.timestamp().is_none());
    }

    // -- XML header round-trips (formats 117–119) ----------------------------

    #[test]
    fn xml_v117_little_endian_round_trip() {
        let timestamp = StataTimestamp::parse("01 Jan 2024 13:45").unwrap();
        let original = Header::builder(Release::V117, ByteOrder::LittleEndian)
            .dataset_label("XML test")
            .timestamp(Some(timestamp))
            .build();
        let parsed = round_trip(&original);
        assert_eq!(parsed.release(), Release::V117);
        assert_eq!(parsed.dataset_label(), "XML test");
        assert_eq!(parsed.timestamp(), Some(&timestamp));
    }

    #[test]
    fn xml_empty_label_round_trip() {
        let original = Header::builder(Release::V117, ByteOrder::LittleEndian).build();
        let parsed = round_trip(&original);
        assert_eq!(parsed.dataset_label(), "");
        assert!(parsed.timestamp().is_none());
    }

    // The header writer now emits zero placeholders for the K (variable
    // count) and N (observation count) fields — the schema writer and
    // record writer patch them once the real counts are known. Count
    // round-trip tests for the wide-field paths (V119 u32 K, V118 u64 N)
    // live with the writer that owns the patch: see
    // `schema_writer::tests::v119_u32_variable_count_round_trip` and
    // the future record writer tests for N overflow.

    // -- Error cases ---------------------------------------------------------

    #[test]
    fn binary_label_too_long_errors() {
        let long_label = "x".repeat(200);
        let header = Header::builder(Release::V114, ByteOrder::LittleEndian)
            .dataset_label(long_label)
            .build();
        let error = DtaWriter::new()
            .from_writer(Cursor::new(Vec::<u8>::new()))
            .write_header(header)
            .unwrap_err();
        assert!(matches!(
            error,
            DtaError::Format(ref e) if matches!(
                e.kind(),
                FormatErrorKind::FieldTooLarge { field: Field::DatasetLabel, .. }
            )
        ));
    }

    #[test]
    fn unrepresentable_label_encoding_errors() {
        // Windows-1252 cannot represent Japanese characters.
        let header = Header::builder(Release::V114, ByteOrder::LittleEndian)
            .dataset_label("日本語")
            .build();
        let error = DtaWriter::new()
            .from_writer(Cursor::new(Vec::<u8>::new()))
            .write_header(header)
            .unwrap_err();
        assert!(matches!(
            error,
            DtaError::Format(ref e) if matches!(
                e.kind(),
                FormatErrorKind::InvalidEncoding { field: Field::DatasetLabel }
            )
        ));
    }

    #[test]
    fn utf8_encoding_override_allows_non_latin_label() {
        let header = Header::builder(Release::V114, ByteOrder::LittleEndian)
            .dataset_label("日本語")
            .build();
        let bytes = DtaWriter::new()
            .encoding(encoding_rs::UTF_8)
            .from_writer(Cursor::new(Vec::<u8>::new()))
            .write_header(header)
            .unwrap()
            .write_schema(Schema::builder().build().unwrap())
            .unwrap()
            .into_record_writer()
            .unwrap()
            .into_long_string_writer()
            .unwrap()
            .into_value_label_writer()
            .unwrap()
            .finish()
            .unwrap()
            .into_inner();
        let parsed = DtaReader::new()
            .encoding(encoding_rs::UTF_8)
            .from_reader(Cursor::new(bytes))
            .read_header()
            .unwrap()
            .header()
            .clone();
        assert_eq!(parsed.dataset_label(), "日本語");
    }
}
