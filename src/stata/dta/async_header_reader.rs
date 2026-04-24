use encoding_rs::Encoding;
use tokio::io::AsyncRead;

use super::async_reader_state::AsyncReaderState;
use super::async_schema_reader::AsyncSchemaReader;
use super::byte_order::ByteOrder;
use super::dta_error::{Field, FormatErrorKind, Result, Section};
use super::header::Header;
use super::header_parse::{
    parse_binary_byte_order, parse_binary_release, parse_fixed_timestamp, parse_xml_byte_order,
    parse_xml_release,
};
use super::release::Release;
use crate::stata::stata_timestamp::StataTimestamp;

/// Entry point for reading a DTA file asynchronously.
///
/// Created via [`DtaReader::from_tokio_reader`](super::dta_reader::DtaReader::from_tokio_reader),
/// [`from_tokio_file`](super::dta_reader::DtaReader::from_tokio_file),
/// or [`from_tokio_path`](super::dta_reader::DtaReader::from_tokio_path),
/// then call [`read_header`](Self::read_header) to parse the file
/// header.
#[derive(Debug)]
pub struct AsyncHeaderReader<R> {
    state: AsyncReaderState<R>,
    encoding_override: Option<&'static Encoding>,
}

impl<R> AsyncHeaderReader<R> {
    /// Creates a header reader. The encoding override, if provided,
    /// will be used regardless of format version; otherwise the
    /// encoding is determined from the release number.
    #[must_use]
    pub(crate) fn new(reader: R, encoding: Option<&'static Encoding>) -> Self {
        let initial_encoding = encoding.unwrap_or(encoding_rs::UTF_8);
        Self {
            state: AsyncReaderState::new(reader, initial_encoding),
            encoding_override: encoding,
        }
    }
}

impl<R: AsyncRead + Unpin> AsyncHeaderReader<R> {
    /// Parses the file header and transitions to schema reading.
    ///
    /// Auto-detects the format by reading the first byte: `b'<'`
    /// selects the XML header parser (formats 117+), otherwise the
    /// binary parser (formats 104–116). Determines the character
    /// encoding from the release number (Windows-1252 for pre-118,
    /// UTF-8 for 118+), unless an explicit encoding override was set
    /// on the `DtaReader` builder.
    ///
    /// # Errors
    ///
    /// Returns [`DtaError::Io`](super::dta_error::DtaError::Io) on
    /// read failures and [`DtaError::Format`](super::dta_error::DtaError::Format)
    /// when the header bytes do not match any supported DTA format
    /// (104–119).
    pub async fn read_header(mut self) -> Result<AsyncSchemaReader<R>> {
        let first_byte = self.state.read_u8(Section::Header).await?;

        let (release, byte_order, variable_count, observation_count) = if first_byte == b'<' {
            self.state
                .expect_bytes(
                    b"stata_dta>",
                    Section::Header,
                    FormatErrorKind::InvalidMagic,
                )
                .await?;
            self.read_xml_preamble().await?
        } else {
            self.read_binary_preamble(first_byte).await?
        };

        let encoding = self
            .encoding_override
            .unwrap_or_else(|| release.default_encoding());

        let (dataset_label, timestamp) = if release.is_xml_like() {
            self.read_xml_label_and_timestamp(release, byte_order, encoding)
                .await?
        } else {
            self.read_binary_label_and_timestamp(release, encoding)
                .await?
        };

        let header = Header::builder(release, byte_order)
            .variable_count(variable_count)
            .observation_count(observation_count)
            .dataset_label(dataset_label)
            .timestamp(timestamp)
            .build();
        let state = self.state.with_encoding(encoding);
        let reader = AsyncSchemaReader::new(state, header);
        Ok(reader)
    }
}

// ---------------------------------------------------------------------------
// Binary format (104–116)
// ---------------------------------------------------------------------------

impl<R: AsyncRead + Unpin> AsyncHeaderReader<R> {
    async fn read_binary_preamble(
        &mut self,
        release_byte: u8,
    ) -> Result<(Release, ByteOrder, u32, u64)> {
        let release = parse_binary_release(release_byte)?;
        let byte_order_byte = self.state.read_u8(Section::Header).await?;
        let byte_order = parse_binary_byte_order(byte_order_byte, release)?;

        // filetype (0x01) + unused padding — skip both
        self.state.skip(2, Section::Header).await?;

        let variable_count = self.state.read_u16(byte_order, Section::Header).await?;
        let variable_count = u32::from(variable_count);
        // V102 stores N as `u16`; V103–V117 use `u32`. The XML path
        // (V118+) handles its own widths via `<N>`.
        let observation_count = if release.supports_extended_binary_observation_count() {
            u64::from(self.state.read_u32(byte_order, Section::Header).await?)
        } else {
            u64::from(self.state.read_u16(byte_order, Section::Header).await?)
        };

        Ok((release, byte_order, variable_count, observation_count))
    }

    async fn read_binary_label_and_timestamp(
        &mut self,
        release: Release,
        encoding: &'static Encoding,
    ) -> Result<(String, Option<StataTimestamp>)> {
        let dataset_label = self
            .state
            .read_fixed_string(
                release.dataset_label_len(),
                encoding,
                Section::Header,
                Field::DatasetLabel,
            )
            .await?;
        let timestamp = if let Some(len) = release.timestamp_len() {
            self.read_fixed_timestamp(len).await?
        } else {
            None
        };
        Ok((dataset_label, timestamp))
    }
}

// ---------------------------------------------------------------------------
// XML format (117+)
// ---------------------------------------------------------------------------

impl<R: AsyncRead + Unpin> AsyncHeaderReader<R> {
    async fn read_xml_preamble(&mut self) -> Result<(Release, ByteOrder, u32, u64)> {
        self.state
            .expect_bytes(b"<header>", Section::Header, FormatErrorKind::InvalidMagic)
            .await?;

        self.state
            .expect_bytes(b"<release>", Section::Header, FormatErrorKind::InvalidMagic)
            .await?;
        let release = self.read_xml_release().await?;
        self.state
            .expect_bytes(
                b"</release>",
                Section::Header,
                FormatErrorKind::InvalidMagic,
            )
            .await?;

        self.state
            .expect_bytes(
                b"<byteorder>",
                Section::Header,
                FormatErrorKind::InvalidMagic,
            )
            .await?;
        let byte_order = self.read_xml_byte_order().await?;
        self.state
            .expect_bytes(
                b"</byteorder>",
                Section::Header,
                FormatErrorKind::InvalidMagic,
            )
            .await?;

        self.state
            .expect_bytes(b"<K>", Section::Header, FormatErrorKind::InvalidMagic)
            .await?;
        let variable_count = self.read_xml_variable_count(release, byte_order).await?;
        self.state
            .expect_bytes(b"</K>", Section::Header, FormatErrorKind::InvalidMagic)
            .await?;

        self.state
            .expect_bytes(b"<N>", Section::Header, FormatErrorKind::InvalidMagic)
            .await?;
        let observation_count = self.read_xml_observation_count(release, byte_order).await?;
        self.state
            .expect_bytes(b"</N>", Section::Header, FormatErrorKind::InvalidMagic)
            .await?;

        Ok((release, byte_order, variable_count, observation_count))
    }

    async fn read_xml_release(&mut self) -> Result<Release> {
        let position = self.state.position();
        let buffer = self.state.read_exact(3, Section::Header).await?;
        parse_xml_release(buffer, position)
    }

    async fn read_xml_byte_order(&mut self) -> Result<ByteOrder> {
        let position = self.state.position();
        let buffer = self.state.read_exact(3, Section::Header).await?;
        parse_xml_byte_order(buffer, position)
    }

    async fn read_xml_variable_count(
        &mut self,
        release: Release,
        byte_order: ByteOrder,
    ) -> Result<u32> {
        if release.supports_extended_variable_count() {
            self.state.read_u32(byte_order, Section::Header).await
        } else {
            self.state
                .read_u16(byte_order, Section::Header)
                .await
                .map(u32::from)
        }
    }

    async fn read_xml_observation_count(
        &mut self,
        release: Release,
        byte_order: ByteOrder,
    ) -> Result<u64> {
        if release.supports_extended_observation_count() {
            self.state.read_u64(byte_order, Section::Header).await
        } else {
            self.state
                .read_u32(byte_order, Section::Header)
                .await
                .map(u64::from)
        }
    }

    async fn read_xml_label_and_timestamp(
        &mut self,
        release: Release,
        byte_order: ByteOrder,
        encoding: &'static Encoding,
    ) -> Result<(String, Option<StataTimestamp>)> {
        self.state
            .expect_bytes(b"<label>", Section::Header, FormatErrorKind::InvalidMagic)
            .await?;
        let label_len = self.read_xml_label_len(release, byte_order).await?;
        let dataset_label = self
            .state
            .read_fixed_string(label_len, encoding, Section::Header, Field::DatasetLabel)
            .await?;
        self.state
            .expect_bytes(b"</label>", Section::Header, FormatErrorKind::InvalidMagic)
            .await?;

        self.state
            .expect_bytes(
                b"<timestamp>",
                Section::Header,
                FormatErrorKind::InvalidMagic,
            )
            .await?;
        let timestamp_len = usize::from(self.state.read_u8(Section::Header).await?);
        let timestamp = if timestamp_len == 0 {
            None
        } else {
            self.read_fixed_timestamp(timestamp_len).await?
        };
        self.state
            .expect_bytes(
                b"</timestamp>",
                Section::Header,
                FormatErrorKind::InvalidMagic,
            )
            .await?;

        self.state
            .expect_bytes(b"</header>", Section::Header, FormatErrorKind::InvalidMagic)
            .await?;

        Ok((dataset_label, timestamp))
    }

    async fn read_xml_label_len(
        &mut self,
        release: Release,
        byte_order: ByteOrder,
    ) -> Result<usize> {
        if release.supports_extended_dataset_label() {
            self.state
                .read_u16(byte_order, Section::Header)
                .await
                .map(usize::from)
        } else {
            self.state.read_u8(Section::Header).await.map(usize::from)
        }
    }
}

// ---------------------------------------------------------------------------
// Header-specific helpers
// ---------------------------------------------------------------------------

impl<R: AsyncRead + Unpin> AsyncHeaderReader<R> {
    /// Reads a `len`-byte fixed-width timestamp field and parses it.
    /// Returns `None` when the bytes contain no parseable timestamp
    /// (empty or zero-filled). Callers that know the field is absent
    /// (V104 binary, or an XML `<timestamp>` with a `0` length
    /// prefix) should not call this.
    async fn read_fixed_timestamp(&mut self, len: usize) -> Result<Option<StataTimestamp>> {
        let buffer = self.state.read_exact(len, Section::Header).await?;
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
    use crate::stata::dta::schema::Schema;

    /// Writes `header` through the async header + schema writers
    /// (empty schema, terminal for the POC) and reads the header
    /// back. Because the async schema writer patches K=0 for the
    /// empty schema, tests that need to assert non-zero K/N use
    /// `serialize_xml` below instead.
    async fn round_trip(header: &Header) -> Header {
        let cursor: Cursor<Vec<u8>> = DtaWriter::new()
            .from_tokio_writer(Cursor::new(Vec::<u8>::new()))
            .write_header(header.clone())
            .await
            .unwrap()
            .write_schema(Schema::builder().build().unwrap())
            .await
            .unwrap()
            .into_record_writer()
            .await
            .unwrap()
            .into_long_string_writer()
            .await
            .unwrap()
            .into_value_label_writer()
            .await
            .unwrap()
            .finish()
            .await
            .unwrap();
        let bytes = cursor.into_inner();
        DtaReader::default()
            .from_tokio_reader(bytes.as_slice())
            .read_header()
            .await
            .unwrap()
            .header()
            .clone()
    }

    /// Serializes a [`Header`] into raw XML DTA bytes (formats
    /// 117–119). Hand-crafted so tests can pin specific K/N values in
    /// the `<K>` / `<N>` slots — the async writer cannot emit
    /// non-zero K/N until the async schema and record writers land.
    fn serialize_xml(header: &Header) -> Vec<u8> {
        let release = header.release();
        let byte_order = header.byte_order();

        let mut buffer = Vec::new();
        buffer.extend_from_slice(b"<stata_dta><header>");

        buffer.extend_from_slice(b"<release>");
        buffer.extend_from_slice(format!("{:03}", release.to_byte()).as_bytes());
        buffer.extend_from_slice(b"</release>");

        buffer.extend_from_slice(b"<byteorder>");
        buffer.extend_from_slice(byte_order.to_string().as_bytes());
        buffer.extend_from_slice(b"</byteorder>");

        buffer.extend_from_slice(b"<K>");
        if release.supports_extended_variable_count() {
            buffer.extend_from_slice(&byte_order.write_u32(header.variable_count()));
        } else {
            let variable_count = u16::try_from(header.variable_count()).unwrap();
            buffer.extend_from_slice(&byte_order.write_u16(variable_count));
        }
        buffer.extend_from_slice(b"</K>");

        buffer.extend_from_slice(b"<N>");
        if release.supports_extended_observation_count() {
            buffer.extend_from_slice(&byte_order.write_u64(header.observation_count()));
        } else {
            let observation_count = u32::try_from(header.observation_count()).unwrap();
            buffer.extend_from_slice(&byte_order.write_u32(observation_count));
        }
        buffer.extend_from_slice(b"</N>");

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

        let timestamp = header.timestamp().map(ToString::to_string);
        let timestamp_bytes = timestamp.as_deref().unwrap_or("").as_bytes();
        buffer.extend_from_slice(b"<timestamp>");
        buffer.push(u8::try_from(timestamp_bytes.len()).unwrap());
        buffer.extend_from_slice(timestamp_bytes);
        buffer.extend_from_slice(b"</timestamp>");

        buffer.extend_from_slice(b"</header>");
        buffer
    }

    async fn read_back(data: Vec<u8>) -> Header {
        DtaReader::default()
            .from_tokio_reader(data.as_slice())
            .read_header()
            .await
            .unwrap()
            .header()
            .clone()
    }

    // -- Binary header parsing (formats 104–116) -----------------------------

    #[tokio::test]
    async fn binary_v114_little_endian() {
        let timestamp = StataTimestamp::parse("01 Jan 2024 13:45").unwrap();
        let expected = Header::builder(Release::V114, ByteOrder::LittleEndian)
            .dataset_label("My Dataset")
            .timestamp(Some(timestamp))
            .build();
        let header = round_trip(&expected).await;
        assert_eq!(header.release(), Release::V114);
        assert_eq!(header.byte_order(), ByteOrder::LittleEndian);
        assert_eq!(header.dataset_label(), "My Dataset");
        let actual_timestamp = header.timestamp().unwrap();
        assert_eq!(actual_timestamp.day(), 1);
        assert_eq!(actual_timestamp.month(), 1);
        assert_eq!(actual_timestamp.year(), 2024);
        assert_eq!(actual_timestamp.hour(), 13);
        assert_eq!(actual_timestamp.minute(), 45);
    }

    #[tokio::test]
    async fn binary_v114_big_endian() {
        let expected = Header::builder(Release::V114, ByteOrder::BigEndian)
            .dataset_label("BE test")
            .timestamp(Some(StataTimestamp::parse("15 Mar 2020 09:30").unwrap()))
            .build();
        let header = round_trip(&expected).await;
        assert_eq!(header.release(), Release::V114);
        assert_eq!(header.byte_order(), ByteOrder::BigEndian);
        assert_eq!(header.dataset_label(), "BE test");
    }

    #[tokio::test]
    async fn binary_v104_no_timestamp() {
        let expected = Header::builder(Release::V104, ByteOrder::LittleEndian)
            .dataset_label("Old format")
            .build();
        let header = round_trip(&expected).await;
        assert_eq!(header.release(), Release::V104);
        assert_eq!(header.dataset_label(), "Old format");
        assert!(header.timestamp().is_none());
    }

    #[tokio::test]
    async fn binary_v107_short_label() {
        let expected = Header::builder(Release::V107, ByteOrder::LittleEndian)
            .dataset_label("short")
            .timestamp(Some(StataTimestamp::parse("12 Feb 2019 00:00").unwrap()))
            .build();
        let header = round_trip(&expected).await;
        assert_eq!(header.release(), Release::V107);
        assert_eq!(header.dataset_label(), "short");
    }

    #[tokio::test]
    async fn binary_empty_label_and_timestamp() {
        let expected = Header::builder(Release::V114, ByteOrder::LittleEndian).build();
        let header = round_trip(&expected).await;
        assert_eq!(header.dataset_label(), "");
        assert!(header.timestamp().is_none());
    }

    #[tokio::test]
    async fn binary_unsupported_release() {
        // 101 is below our supported range of 102–119.
        let data = vec![101, 0x02, 0x01, 0x00, 0, 1, 0, 0, 0, 1];
        let error = DtaReader::default()
            .from_tokio_reader(data.as_slice())
            .read_header()
            .await
            .unwrap_err();
        assert!(matches!(
            error,
            DtaError::Format(ref e) if e.kind() == FormatErrorKind::UnsupportedRelease { release: 101 }
        ));
    }

    #[tokio::test]
    async fn binary_invalid_byte_order() {
        let data = vec![114, 0x00, 0x01, 0x00, 0, 1, 0, 0, 0, 1];
        let error = DtaReader::default()
            .from_tokio_reader(data.as_slice())
            .read_header()
            .await
            .unwrap_err();
        assert!(matches!(
            error,
            DtaError::Format(ref e) if e.kind() == FormatErrorKind::InvalidByteOrder { byte: 0x00 }
        ));
    }

    #[tokio::test]
    async fn binary_encoding_override() {
        let expected = Header::builder(Release::V114, ByteOrder::LittleEndian)
            .dataset_label("test")
            .build();
        let cursor: Cursor<Vec<u8>> = DtaWriter::new()
            .from_tokio_writer(Cursor::new(Vec::<u8>::new()))
            .write_header(expected)
            .await
            .unwrap()
            .write_schema(Schema::builder().build().unwrap())
            .await
            .unwrap()
            .into_record_writer()
            .await
            .unwrap()
            .into_long_string_writer()
            .await
            .unwrap()
            .into_value_label_writer()
            .await
            .unwrap()
            .finish()
            .await
            .unwrap();
        let bytes = cursor.into_inner();
        let schema_reader = DtaReader::new()
            .encoding(encoding_rs::UTF_8)
            .from_tokio_reader(bytes.as_slice())
            .read_header()
            .await
            .unwrap();
        assert_eq!(schema_reader.header().dataset_label(), "test");
    }

    // -- XML header parsing (formats 117–119) --------------------------------

    #[tokio::test]
    async fn xml_v117_little_endian() {
        let expected = Header::builder(Release::V117, ByteOrder::LittleEndian)
            .dataset_label("XML test")
            .timestamp(Some(StataTimestamp::parse("01 Jan 2024 13:45").unwrap()))
            .build();
        let header = round_trip(&expected).await;
        assert_eq!(header.release(), Release::V117);
        assert_eq!(header.byte_order(), ByteOrder::LittleEndian);
        assert_eq!(header.dataset_label(), "XML test");
        let timestamp = header.timestamp().unwrap();
        assert_eq!(timestamp.day(), 1);
        assert_eq!(timestamp.year(), 2024);
    }

    #[tokio::test]
    async fn xml_v117_big_endian() {
        let expected = Header::builder(Release::V117, ByteOrder::BigEndian)
            .dataset_label("BE XML")
            .timestamp(Some(StataTimestamp::parse("15 Mar 2020 09:30").unwrap()))
            .build();
        let header = round_trip(&expected).await;
        assert_eq!(header.release(), Release::V117);
        assert_eq!(header.byte_order(), ByteOrder::BigEndian);
        assert_eq!(header.dataset_label(), "BE XML");
    }

    #[tokio::test]
    async fn xml_v118_u16_label_len_u64_nobs() {
        let expected = Header::builder(Release::V118, ByteOrder::LittleEndian)
            .variable_count(10)
            .observation_count(1_000_000)
            .dataset_label("v118 label")
            .build();
        let header = read_back(serialize_xml(&expected)).await;
        assert_eq!(header.release(), Release::V118);
        assert_eq!(header.variable_count(), 10);
        assert_eq!(header.observation_count(), 1_000_000);
        assert_eq!(header.dataset_label(), "v118 label");
        assert!(header.timestamp().is_none());
    }

    #[tokio::test]
    async fn xml_v119_u32_variable_count() {
        let expected = Header::builder(Release::V119, ByteOrder::LittleEndian)
            .variable_count(70_000)
            .observation_count(500)
            .dataset_label("wide")
            .timestamp(Some(StataTimestamp::parse("01 Jun 2025 12:00").unwrap()))
            .build();
        let header = read_back(serialize_xml(&expected)).await;
        assert_eq!(header.release(), Release::V119);
        assert_eq!(header.variable_count(), 70_000);
        assert_eq!(header.observation_count(), 500);
        assert_eq!(header.dataset_label(), "wide");
    }

    #[tokio::test]
    async fn xml_empty_label_and_timestamp() {
        let expected = Header::builder(Release::V117, ByteOrder::LittleEndian).build();
        let header = round_trip(&expected).await;
        assert_eq!(header.dataset_label(), "");
        assert!(header.timestamp().is_none());
    }

    // -- Error cases ---------------------------------------------------------

    #[tokio::test]
    async fn xml_invalid_magic() {
        let error = DtaReader::default()
            .from_tokio_reader(&b"<not_dta>garbage"[..])
            .read_header()
            .await
            .unwrap_err();
        assert!(matches!(
            error,
            DtaError::Format(ref e) if e.kind() == FormatErrorKind::InvalidMagic
        ));
    }

    #[tokio::test]
    async fn xml_pre_117_release_is_invalid_magic() {
        let mut data = Vec::new();
        data.extend_from_slice(b"<stata_dta><header>");
        data.extend_from_slice(b"<release>114</release>");
        let error = DtaReader::default()
            .from_tokio_reader(data.as_slice())
            .read_header()
            .await
            .unwrap_err();
        assert!(matches!(
            error,
            DtaError::Format(ref e) if e.kind() == FormatErrorKind::InvalidMagic
        ));
    }

    #[tokio::test]
    async fn xml_bad_byte_order_tag() {
        let mut data = Vec::new();
        data.extend_from_slice(b"<stata_dta><header>");
        data.extend_from_slice(b"<release>117</release>");
        data.extend_from_slice(b"<byteorder>XYZ</byteorder>");
        let error = DtaReader::default()
            .from_tokio_reader(data.as_slice())
            .read_header()
            .await
            .unwrap_err();
        assert!(matches!(
            error,
            DtaError::Format(ref e) if e.kind() == FormatErrorKind::InvalidByteOrderTag
        ));
    }

    #[tokio::test]
    async fn truncated_binary_header() {
        let error = DtaReader::default()
            .from_tokio_reader(&[114u8, 0x02, 0x01][..])
            .read_header()
            .await
            .unwrap_err();
        assert!(matches!(error, DtaError::Io { .. }));
    }
}
