use std::io::{BufRead, Read, Seek};

use encoding_rs::Encoding;

use super::byte_order::ByteOrder;
use super::dta_error::{Field, FormatErrorKind, Result, Section};
use super::header::Header;
use super::header_parse::{
    parse_binary_byte_order, parse_binary_release, parse_fixed_timestamp, parse_xml_byte_order,
    parse_xml_release,
};
use super::reader_state::ReaderState;
use super::release::Release;
use super::schema_reader::SchemaReader;
use crate::stata::stata_timestamp::StataTimestamp;

/// Entry point for reading a DTA file.
///
/// Created via [`DtaReader::from_reader`](super::dta_reader::DtaReader::from_reader)
/// or [`DtaReader::from_file`](super::dta_reader::DtaReader::from_file),
/// then call [`read_header`](Self::read_header) to parse the file header
/// and advance to schema reading.
#[derive(Debug)]
pub struct HeaderReader<R> {
    state: ReaderState<R>,
    encoding_override: Option<&'static Encoding>,
}

impl<R> HeaderReader<R> {
    /// Creates a header reader. The encoding override, if provided,
    /// will be used regardless of format version; otherwise the
    /// encoding is determined from the release number.
    #[must_use]
    pub(crate) fn new(reader: R, encoding: Option<&'static Encoding>) -> Self {
        // The initial encoding is a placeholder — it is replaced once
        // the release number is known (or kept if an override was given).
        let initial_encoding = encoding.unwrap_or(encoding_rs::UTF_8);
        let state = ReaderState::new(reader, initial_encoding);
        Self {
            state,
            encoding_override: encoding,
        }
    }

    /// The encoding override passed to
    /// [`DtaReader::encoding`](super::dta_reader::DtaReader::encoding),
    /// if any. When `None`, the reader picks the release-appropriate
    /// default (Windows-1252 for pre-V118, UTF-8 for V118+) once
    /// [`read_header`](Self::read_header) determines the format
    /// version. The resolved encoding is available on every downstream
    /// reader via `encoding()`.
    #[must_use]
    #[inline]
    pub fn encoding_override(&self) -> Option<&'static Encoding> {
        self.encoding_override
    }
}

impl<R: BufRead + Seek> HeaderReader<R> {
    /// Parses the file header, determines the encoding, and
    /// transitions to schema reading.
    ///
    /// # Errors
    ///
    /// Returns [`DtaError::Io`] on read failures and
    /// [`DtaError::Format`] when the header bytes do not match any
    /// supported DTA format (104–119).
    pub fn read_header(mut self) -> Result<SchemaReader<R>> {
        let first_byte = self.state.read_u8(Section::Header)?;

        let (release, byte_order, variable_count, observation_count) = if first_byte == b'<' {
            self.state.expect_bytes(
                b"stata_dta>",
                Section::Header,
                FormatErrorKind::InvalidMagic,
            )?;
            self.read_xml_preamble()?
        } else {
            self.read_binary_preamble(first_byte)?
        };

        let encoding = self
            .encoding_override
            .unwrap_or_else(|| release.default_encoding());

        let (dataset_label, timestamp) = if release.is_xml_like() {
            self.read_xml_label_and_timestamp(release, byte_order, encoding)?
        } else {
            self.read_binary_label_and_timestamp(release, encoding)?
        };

        let header = Header::builder(release, byte_order)
            .variable_count(variable_count)
            .observation_count(observation_count)
            .dataset_label(dataset_label)
            .timestamp(timestamp)
            .build();
        let state = self.state.with_encoding(encoding);
        let reader = SchemaReader::new(state, header);
        Ok(reader)
    }
}

// ---------------------------------------------------------------------------
// Binary format (104–116)
// ---------------------------------------------------------------------------

impl<R: Read> HeaderReader<R> {
    /// Parses the binary header struct fields (release already consumed).
    ///
    /// Binary layout (10 bytes total):
    /// ```text
    /// [0]  ds_format    (already read by caller)
    /// [1]  byteorder    0x01 = big-endian, 0x02 = little-endian
    /// [2]  filetype     always 0x01
    /// [3]  unused       padding
    /// [4-5]  nvar       u16
    /// [6-9]  nobs       u32
    /// ```
    fn read_binary_preamble(&mut self, release_byte: u8) -> Result<(Release, ByteOrder, u32, u64)> {
        let release = parse_binary_release(release_byte)?;
        let byte_order_byte = self.state.read_u8(Section::Header)?;
        let byte_order = parse_binary_byte_order(byte_order_byte, release)?;

        // filetype (0x01) + unused padding — skip both
        self.state.skip(2, Section::Header)?;

        let variable_count = self.state.read_u16(byte_order, Section::Header)?;
        let variable_count = u32::from(variable_count);
        // V102 stores N as `u16`; V103–V117 use `u32`. The XML path
        // (V118+) handles its own widths via `<N>`.
        let observation_count = if release.supports_extended_binary_observation_count() {
            u64::from(self.state.read_u32(byte_order, Section::Header)?)
        } else {
            u64::from(self.state.read_u16(byte_order, Section::Header)?)
        };

        Ok((release, byte_order, variable_count, observation_count))
    }

    /// Reads the dataset label and timestamp from a binary-format file.
    fn read_binary_label_and_timestamp(
        &mut self,
        release: Release,
        encoding: &'static Encoding,
    ) -> Result<(String, Option<StataTimestamp>)> {
        let dataset_label = self.state.read_fixed_string(
            release.dataset_label_len(),
            encoding,
            Section::Header,
            Field::DatasetLabel,
        )?;
        let timestamp = if let Some(len) = release.timestamp_len() {
            self.read_fixed_timestamp(len)?
        } else {
            None
        };
        Ok((dataset_label, timestamp))
    }
}

// ---------------------------------------------------------------------------
// XML format (117+)
// ---------------------------------------------------------------------------

impl<R: Read> HeaderReader<R> {
    /// Parses the XML header fields (`<stata_dta><header>` already consumed).
    ///
    /// Reads `<release>`, `<byteorder>`, `<K>`, and `<N>`.
    fn read_xml_preamble(&mut self) -> Result<(Release, ByteOrder, u32, u64)> {
        // <header>
        self.state
            .expect_bytes(b"<header>", Section::Header, FormatErrorKind::InvalidMagic)?;

        // <release>NNN</release>
        self.state
            .expect_bytes(b"<release>", Section::Header, FormatErrorKind::InvalidMagic)?;
        let release = self.read_xml_release()?;
        self.state.expect_bytes(
            b"</release>",
            Section::Header,
            FormatErrorKind::InvalidMagic,
        )?;

        // <byteorder>MSF|LSF</byteorder>
        self.state.expect_bytes(
            b"<byteorder>",
            Section::Header,
            FormatErrorKind::InvalidMagic,
        )?;
        let byte_order = self.read_xml_byte_order()?;
        self.state.expect_bytes(
            b"</byteorder>",
            Section::Header,
            FormatErrorKind::InvalidMagic,
        )?;

        // <K>nvar</K>
        self.state
            .expect_bytes(b"<K>", Section::Header, FormatErrorKind::InvalidMagic)?;
        let variable_count = self.read_xml_variable_count(release, byte_order)?;
        self.state
            .expect_bytes(b"</K>", Section::Header, FormatErrorKind::InvalidMagic)?;

        // <N>nobs</N>
        self.state
            .expect_bytes(b"<N>", Section::Header, FormatErrorKind::InvalidMagic)?;
        let observation_count = self.read_xml_observation_count(release, byte_order)?;
        self.state
            .expect_bytes(b"</N>", Section::Header, FormatErrorKind::InvalidMagic)?;

        Ok((release, byte_order, variable_count, observation_count))
    }

    /// Parses a 3-character ASCII release number (e.g. `"117"`).
    fn read_xml_release(&mut self) -> Result<Release> {
        let position = self.state.position();
        let buffer = self.state.read_exact(3, Section::Header)?;
        parse_xml_release(buffer, position)
    }

    /// Parses a 3-character byte-order tag (`"MSF"` or `"LSF"`).
    fn read_xml_byte_order(&mut self) -> Result<ByteOrder> {
        let position = self.state.position();
        let buffer = self.state.read_exact(3, Section::Header)?;
        parse_xml_byte_order(buffer, position)
    }

    /// Reads the variable count: `u16` for 117–118, `u32` for 119.
    fn read_xml_variable_count(&mut self, release: Release, byte_order: ByteOrder) -> Result<u32> {
        if release.supports_extended_variable_count() {
            self.state.read_u32(byte_order, Section::Header)
        } else {
            self.state
                .read_u16(byte_order, Section::Header)
                .map(u32::from)
        }
    }

    /// Reads the observation count: `u32` for 117, `u64` for 118+.
    fn read_xml_observation_count(
        &mut self,
        release: Release,
        byte_order: ByteOrder,
    ) -> Result<u64> {
        if release.supports_extended_observation_count() {
            self.state.read_u64(byte_order, Section::Header)
        } else {
            self.state
                .read_u32(byte_order, Section::Header)
                .map(u64::from)
        }
    }

    /// Reads the dataset label and timestamp from an XML-format file.
    ///
    /// Layout inside `<header>`:
    /// ```text
    /// <label>  [len_prefix] [label_bytes]  </label>
    /// <timestamp>  [u8 len] [timestamp_bytes]  </timestamp>
    /// </header>
    /// ```
    fn read_xml_label_and_timestamp(
        &mut self,
        release: Release,
        byte_order: ByteOrder,
        encoding: &'static Encoding,
    ) -> Result<(String, Option<StataTimestamp>)> {
        // <label> ... </label>
        self.state
            .expect_bytes(b"<label>", Section::Header, FormatErrorKind::InvalidMagic)?;
        let label_len = self.read_xml_label_len(release, byte_order)?;
        let dataset_label = self.state.read_fixed_string(
            label_len,
            encoding,
            Section::Header,
            Field::DatasetLabel,
        )?;
        self.state
            .expect_bytes(b"</label>", Section::Header, FormatErrorKind::InvalidMagic)?;

        // <timestamp> ... </timestamp>
        self.state.expect_bytes(
            b"<timestamp>",
            Section::Header,
            FormatErrorKind::InvalidMagic,
        )?;
        let timestamp_len = usize::from(self.state.read_u8(Section::Header)?);
        let timestamp = if timestamp_len == 0 {
            None
        } else {
            self.read_fixed_timestamp(timestamp_len)?
        };
        self.state.expect_bytes(
            b"</timestamp>",
            Section::Header,
            FormatErrorKind::InvalidMagic,
        )?;

        // </header>
        self.state
            .expect_bytes(b"</header>", Section::Header, FormatErrorKind::InvalidMagic)?;

        Ok((dataset_label, timestamp))
    }

    /// Reads the label-length prefix: `u8` for 117, `u16` for 118+.
    fn read_xml_label_len(&mut self, release: Release, byte_order: ByteOrder) -> Result<usize> {
        if release.supports_extended_dataset_label() {
            self.state
                .read_u16(byte_order, Section::Header)
                .map(usize::from)
        } else {
            self.state.read_u8(Section::Header).map(usize::from)
        }
    }
}

// ---------------------------------------------------------------------------
// Header-specific helpers
// ---------------------------------------------------------------------------

impl<R: Read> HeaderReader<R> {
    /// Reads a `len`-byte fixed-width timestamp field and parses it.
    /// Returns `None` when the bytes contain no parseable timestamp
    /// (empty or zero-filled). Callers that know the field is absent
    /// (`V104` binary, or an XML `<timestamp>` with a `0` length
    /// prefix) should not call this.
    fn read_fixed_timestamp(&mut self, len: usize) -> Result<Option<StataTimestamp>> {
        let buffer = self.state.read_exact(len, Section::Header)?;
        let timestamp = parse_fixed_timestamp(buffer);
        Ok(timestamp)
    }
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;

    use super::*;
    use crate::stata::dta::dta_error::DtaError;
    use crate::stata::dta::dta_reader::DtaReader;
    use crate::stata::dta::dta_writer::DtaWriter;
    use crate::stata::dta::release::Release;

    // -- Test serialization helpers ------------------------------------------

    /// Writes `header` through the full writer pipeline with a
    /// matching schema of `k` Byte variables and `n` rows of
    /// default-valued Bytes, producing a complete DTA file. Tests
    /// that assert `header.variable_count() == k` / `observation_count() == n`
    /// rely on the writer patching those fields from the schema and
    /// record stream.
    fn serialize_through_writer(header: &Header, k: u32, n: u64) -> Vec<u8> {
        use crate::stata::dta::schema::Schema;
        use crate::stata::dta::value::Value;
        use crate::stata::dta::variable::Variable;
        use crate::stata::dta::variable_type::VariableType;
        use crate::stata::stata_byte::StataByte;

        let k_usize = usize::try_from(k).unwrap();
        let variables: Vec<_> = (0..k_usize)
            .map(|i| Variable::builder(VariableType::Byte, format!("v{i}")).format("%8.0g"))
            .collect();
        let schema = Schema::builder().variables(variables).build().unwrap();

        let characteristic_writer = DtaWriter::new()
            .from_writer(Cursor::new(Vec::<u8>::new()))
            .write_header(header.clone())
            .unwrap()
            .write_schema(schema)
            .unwrap();
        let mut record_writer = characteristic_writer.into_record_writer().unwrap();
        let row: Vec<Value<'_>> = (0..k_usize)
            .map(|_| Value::Byte(StataByte::Present(0)))
            .collect();
        for _ in 0..n {
            record_writer.write_record(&row).unwrap();
        }
        record_writer
            .into_long_string_writer()
            .unwrap()
            .into_value_label_writer()
            .unwrap()
            .finish()
            .unwrap()
            .into_inner()
    }

    /// Serializes a [`Header`] into raw XML DTA bytes (formats 117–119).
    fn serialize_xml(header: &Header) -> Vec<u8> {
        let release = header.release();
        let byte_order = header.byte_order();

        let mut buffer = Vec::new();
        buffer.extend_from_slice(b"<stata_dta><header>");

        // <release>
        buffer.extend_from_slice(b"<release>");
        buffer.extend_from_slice(format!("{:03}", release.to_byte()).as_bytes());
        buffer.extend_from_slice(b"</release>");

        // <byteorder>
        buffer.extend_from_slice(b"<byteorder>");
        buffer.extend_from_slice(byte_order.to_string().as_bytes());
        buffer.extend_from_slice(b"</byteorder>");

        // <K> — variable count
        buffer.extend_from_slice(b"<K>");
        if release.supports_extended_variable_count() {
            buffer.extend_from_slice(&byte_order.write_u32(header.variable_count()));
        } else {
            let variable_count = u16::try_from(header.variable_count()).unwrap();
            buffer.extend_from_slice(&byte_order.write_u16(variable_count));
        }
        buffer.extend_from_slice(b"</K>");

        // <N> — observation count
        buffer.extend_from_slice(b"<N>");
        if release.supports_extended_observation_count() {
            buffer.extend_from_slice(&byte_order.write_u64(header.observation_count()));
        } else {
            let observation_count = u32::try_from(header.observation_count()).unwrap();
            buffer.extend_from_slice(&byte_order.write_u32(observation_count));
        }
        buffer.extend_from_slice(b"</N>");

        // <label>
        let label_bytes = header.dataset_label().as_bytes();
        buffer.extend_from_slice(b"<label>");
        if release.supports_extended_dataset_label() {
            let len = u16::try_from(label_bytes.len()).unwrap();
            buffer.extend_from_slice(&byte_order.write_u16(len));
        } else {
            buffer.push(u8::try_from(label_bytes.len()).unwrap());
        }
        buffer.extend_from_slice(label_bytes);
        buffer.extend_from_slice(b"</label>");

        // <timestamp>
        let timestamp = header.timestamp().map(ToString::to_string);
        let timestamp_bytes = timestamp.as_deref().unwrap_or("").as_bytes();
        buffer.extend_from_slice(b"<timestamp>");
        buffer.push(u8::try_from(timestamp_bytes.len()).unwrap());
        buffer.extend_from_slice(timestamp_bytes);
        buffer.extend_from_slice(b"</timestamp>");

        buffer.extend_from_slice(b"</header>");
        buffer
    }

    /// Parses a header from serialized bytes using default options.
    fn read_back(data: Vec<u8>) -> Header {
        DtaReader::default()
            .from_reader(Cursor::new(data))
            .read_header()
            .unwrap()
            .header()
            .clone()
    }

    // -- Binary header parsing (formats 104–116) -----------------------------

    #[test]
    fn binary_v114_little_endian() {
        let timestamp = StataTimestamp::parse("01 Jan 2024 13:45").unwrap();
        let expected = Header::builder(Release::V114, ByteOrder::LittleEndian)
            .dataset_label("My Dataset")
            .timestamp(Some(timestamp))
            .build();
        let header = read_back(serialize_through_writer(&expected, 5, 100));
        assert_eq!(header.release(), Release::V114);
        assert_eq!(header.byte_order(), ByteOrder::LittleEndian);
        assert_eq!(header.variable_count(), 5);
        assert_eq!(header.observation_count(), 100);
        assert_eq!(header.dataset_label(), "My Dataset");
        let actual_timestamp = header.timestamp().unwrap();
        assert_eq!(actual_timestamp.day(), 1);
        assert_eq!(actual_timestamp.month(), 1);
        assert_eq!(actual_timestamp.year(), 2024);
        assert_eq!(actual_timestamp.hour(), 13);
        assert_eq!(actual_timestamp.minute(), 45);
    }

    #[test]
    fn binary_v114_big_endian() {
        let expected = Header::builder(Release::V114, ByteOrder::BigEndian)
            .dataset_label("BE test")
            .timestamp(Some(StataTimestamp::parse("15 Mar 2020 09:30").unwrap()))
            .build();
        let header = read_back(serialize_through_writer(&expected, 3, 50));
        assert_eq!(header.release(), Release::V114);
        assert_eq!(header.byte_order(), ByteOrder::BigEndian);
        assert_eq!(header.variable_count(), 3);
        assert_eq!(header.observation_count(), 50);
        assert_eq!(header.dataset_label(), "BE test");
    }

    #[test]
    fn binary_v104_no_timestamp() {
        let expected = Header::builder(Release::V104, ByteOrder::LittleEndian)
            .dataset_label("Old format")
            .build();
        let header = read_back(serialize_through_writer(&expected, 2, 10));
        assert_eq!(header.release(), Release::V104);
        assert_eq!(header.variable_count(), 2);
        assert_eq!(header.observation_count(), 10);
        assert_eq!(header.dataset_label(), "Old format");
        assert!(header.timestamp().is_none());
    }

    #[test]
    fn binary_v102_round_trip() {
        // V102 doesn't support the `byte` storage type, so the
        // generic helper (which uses `Byte`) doesn't apply — build a
        // minimal `Int`-only record stream here instead.
        use crate::stata::dta::schema::Schema;
        use crate::stata::dta::value::Value;
        use crate::stata::dta::variable::Variable;
        use crate::stata::dta::variable_type::VariableType;
        use crate::stata::stata_int::StataInt;

        let expected = Header::builder(Release::V102, ByteOrder::LittleEndian)
            .dataset_label("v102 round-trip")
            .build();
        let schema = Schema::builder()
            .add_variable(Variable::builder(VariableType::Int, "x").format("%8.0g"))
            .build()
            .unwrap();
        let mut record_writer = DtaWriter::new()
            .from_writer(Cursor::new(Vec::<u8>::new()))
            .write_header(expected)
            .unwrap()
            .write_schema(schema)
            .unwrap()
            .into_record_writer()
            .unwrap();
        let row = [Value::Int(StataInt::Present(7))];
        for _ in 0..3 {
            record_writer.write_record(&row).unwrap();
        }
        let bytes = record_writer
            .into_long_string_writer()
            .unwrap()
            .into_value_label_writer()
            .unwrap()
            .finish()
            .unwrap()
            .into_inner();

        // Verify the on-disk byteorder byte is 0x00 for V102.
        assert_eq!(bytes[1], 0x00, "V102 should write byteorder 0x00");

        let header = read_back(bytes);
        assert_eq!(header.release(), Release::V102);
        assert_eq!(header.byte_order(), ByteOrder::LittleEndian);
        assert_eq!(header.variable_count(), 1);
        assert_eq!(header.observation_count(), 3);
        assert_eq!(header.dataset_label(), "v102 round-trip");
        assert!(header.timestamp().is_none());
    }

    #[test]
    fn binary_v103_round_trip() {
        // V103 adds `byte` and the standard 0x01/0x02 byteorder byte.
        let expected = Header::builder(Release::V103, ByteOrder::BigEndian)
            .dataset_label("v103 BE")
            .build();
        let bytes = serialize_through_writer(&expected, 2, 5);
        assert_eq!(bytes[1], 0x01, "V103 BE should write byteorder 0x01");

        let header = read_back(bytes);
        assert_eq!(header.release(), Release::V103);
        assert_eq!(header.byte_order(), ByteOrder::BigEndian);
        assert_eq!(header.variable_count(), 2);
        assert_eq!(header.observation_count(), 5);
        assert_eq!(header.dataset_label(), "v103 BE");
    }

    #[test]
    fn binary_v102_rejects_big_endian_at_write_time() {
        let header = Header::builder(Release::V102, ByteOrder::BigEndian).build();
        let error = DtaWriter::new()
            .from_writer(Cursor::new(Vec::<u8>::new()))
            .write_header(header)
            .unwrap_err();
        assert!(matches!(
            error,
            DtaError::Format(ref e) if e.kind() == FormatErrorKind::BigEndianUnsupported {
                release: Release::V102,
            }
        ));
    }

    #[test]
    fn binary_v107_short_label() {
        let expected = Header::builder(Release::V107, ByteOrder::LittleEndian)
            .dataset_label("short")
            .timestamp(Some(StataTimestamp::parse("12 Feb 2019 00:00").unwrap()))
            .build();
        let header = read_back(serialize_through_writer(&expected, 1, 1));
        assert_eq!(header.release(), Release::V107);
        assert_eq!(header.dataset_label(), "short");
    }

    #[test]
    fn binary_empty_label_and_timestamp() {
        let expected = Header::builder(Release::V114, ByteOrder::LittleEndian).build();
        let header = read_back(serialize_through_writer(&expected, 1, 0));
        assert_eq!(header.dataset_label(), "");
        assert!(header.timestamp().is_none());
    }

    #[test]
    fn binary_unsupported_release() {
        // Hand-craft bytes with an unsupported release number (101 is
        // below our supported range of 102–119).
        let data = vec![101, 0x02, 0x01, 0x00, 0, 1, 0, 0, 0, 1];
        let error = DtaReader::default()
            .from_reader(Cursor::new(data))
            .read_header()
            .unwrap_err();
        assert!(matches!(
            error,
            DtaError::Format(ref e) if e.kind() == FormatErrorKind::UnsupportedRelease { release: 101 }
        ));
    }

    #[test]
    fn binary_invalid_byte_order() {
        // Hand-craft bytes with an invalid byte-order code
        let data = vec![114, 0x00, 0x01, 0x00, 0, 1, 0, 0, 0, 1];
        let error = DtaReader::default()
            .from_reader(Cursor::new(data))
            .read_header()
            .unwrap_err();
        assert!(matches!(
            error,
            DtaError::Format(ref e) if e.kind() == FormatErrorKind::InvalidByteOrder { byte: 0x00 }
        ));
    }

    #[test]
    fn binary_encoding_override() {
        let expected = Header::builder(Release::V114, ByteOrder::LittleEndian)
            .dataset_label("test")
            .build();
        let schema = DtaReader::new()
            .encoding(encoding_rs::UTF_8)
            .from_reader(Cursor::new(serialize_through_writer(&expected, 1, 0)))
            .read_header()
            .unwrap();
        assert_eq!(schema.header().dataset_label(), "test");
    }

    // -- XML header parsing (formats 117–119) --------------------------------

    #[test]
    fn xml_v117_little_endian() {
        let expected = Header::builder(Release::V117, ByteOrder::LittleEndian)
            .dataset_label("XML test")
            .timestamp(Some(StataTimestamp::parse("01 Jan 2024 13:45").unwrap()))
            .build();
        let header = read_back(serialize_through_writer(&expected, 5, 100));
        assert_eq!(header.release(), Release::V117);
        assert_eq!(header.byte_order(), ByteOrder::LittleEndian);
        assert_eq!(header.variable_count(), 5);
        assert_eq!(header.observation_count(), 100);
        assert_eq!(header.dataset_label(), "XML test");
        let timestamp = header.timestamp().unwrap();
        assert_eq!(timestamp.day(), 1);
        assert_eq!(timestamp.year(), 2024);
    }

    #[test]
    fn xml_v117_big_endian() {
        let expected = Header::builder(Release::V117, ByteOrder::BigEndian)
            .dataset_label("BE XML")
            .timestamp(Some(StataTimestamp::parse("15 Mar 2020 09:30").unwrap()))
            .build();
        let header = read_back(serialize_through_writer(&expected, 3, 50));
        assert_eq!(header.release(), Release::V117);
        assert_eq!(header.byte_order(), ByteOrder::BigEndian);
        assert_eq!(header.variable_count(), 3);
        assert_eq!(header.observation_count(), 50);
        assert_eq!(header.dataset_label(), "BE XML");
    }

    #[test]
    fn xml_v118_u16_label_len_u64_nobs() {
        let expected = Header::builder(Release::V118, ByteOrder::LittleEndian)
            .variable_count(10)
            .observation_count(1_000_000)
            .dataset_label("v118 label")
            .build();
        let header = read_back(serialize_xml(&expected));
        assert_eq!(header.release(), Release::V118);
        assert_eq!(header.variable_count(), 10);
        assert_eq!(header.observation_count(), 1_000_000);
        assert_eq!(header.dataset_label(), "v118 label");
        assert!(header.timestamp().is_none());
    }

    #[test]
    fn xml_v119_u32_variable_count() {
        let expected = Header::builder(Release::V119, ByteOrder::LittleEndian)
            .variable_count(70_000)
            .observation_count(500)
            .dataset_label("wide")
            .timestamp(Some(StataTimestamp::parse("01 Jun 2025 12:00").unwrap()))
            .build();
        let header = read_back(serialize_xml(&expected));
        assert_eq!(header.release(), Release::V119);
        assert_eq!(header.variable_count(), 70_000);
        assert_eq!(header.observation_count(), 500);
        assert_eq!(header.dataset_label(), "wide");
    }

    #[test]
    fn xml_empty_label_and_timestamp() {
        let expected = Header::builder(Release::V117, ByteOrder::LittleEndian).build();
        let header = read_back(serialize_through_writer(&expected, 1, 0));
        assert_eq!(header.dataset_label(), "");
        assert!(header.timestamp().is_none());
    }

    // -- Error cases ---------------------------------------------------------

    #[test]
    fn xml_invalid_magic() {
        let error = DtaReader::default()
            .from_reader(Cursor::new(b"<not_dta>garbage".to_vec()))
            .read_header()
            .unwrap_err();
        assert!(matches!(
            error,
            DtaError::Format(ref e) if e.kind() == FormatErrorKind::InvalidMagic
        ));
    }

    #[test]
    fn xml_pre_117_release_is_invalid_magic() {
        // Pre-117 releases are binary-format only; finding one inside
        // XML tags is malformed and must reject up front, not drift
        // into the binary label path once `is_xml_like()` returns false.
        let mut data = Vec::new();
        data.extend_from_slice(b"<stata_dta><header>");
        data.extend_from_slice(b"<release>114</release>");
        let error = DtaReader::default()
            .from_reader(Cursor::new(data))
            .read_header()
            .unwrap_err();
        assert!(matches!(
            error,
            DtaError::Format(ref e) if e.kind() == FormatErrorKind::InvalidMagic
        ));
    }

    #[test]
    fn xml_bad_byte_order_tag() {
        let mut data = Vec::new();
        data.extend_from_slice(b"<stata_dta><header>");
        data.extend_from_slice(b"<release>117</release>");
        data.extend_from_slice(b"<byteorder>XYZ</byteorder>");
        let error = DtaReader::default()
            .from_reader(Cursor::new(data))
            .read_header()
            .unwrap_err();
        assert!(matches!(
            error,
            DtaError::Format(ref e) if e.kind() == FormatErrorKind::InvalidByteOrderTag
        ));
    }

    #[test]
    fn truncated_binary_header() {
        // Only 3 bytes — not enough for even the basic header struct
        let error = DtaReader::default()
            .from_reader(Cursor::new(vec![114, 0x02, 0x01]))
            .read_header()
            .unwrap_err();
        assert!(matches!(error, DtaError::Io { .. }));
    }
}
