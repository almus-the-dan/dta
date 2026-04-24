use encoding_rs::Encoding;
use tokio::io::{AsyncSeek, AsyncWrite};

use super::async_schema_writer::AsyncSchemaWriter;
use super::async_writer_state::AsyncWriterState;
use super::byte_order::ByteOrder;
use super::dta_error::{DtaError, Field, Result, Section};
use super::header::Header;
use super::header_format::{BINARY_FILETYPE, BINARY_RESERVED_PADDING, format_timestamp};
use super::release::Release;
use super::string_encoding::{encode_value, narrow_len_to_u8, narrow_len_to_u16};
use crate::stata::stata_timestamp::StataTimestamp;

/// Entry point for writing a DTA file asynchronously.
///
/// Created via [`DtaWriter::from_tokio_writer`](super::dta_writer::DtaWriter::from_tokio_writer),
/// [`from_tokio_file`](super::dta_writer::DtaWriter::from_tokio_file),
/// or [`from_tokio_path`](super::dta_writer::DtaWriter::from_tokio_path),
/// then call [`write_header`](Self::write_header) to emit the file
/// header.
#[derive(Debug)]
pub struct AsyncHeaderWriter<W> {
    state: AsyncWriterState<W>,
    encoding_override: Option<&'static Encoding>,
}

impl<W> AsyncHeaderWriter<W> {
    /// Creates a header writer. The encoding override, if provided,
    /// will be used regardless of the header's release; otherwise the
    /// encoding is determined from the release number at writing time.
    #[must_use]
    pub(crate) fn new(writer: W, encoding: Option<&'static Encoding>) -> Self {
        let initial_encoding = encoding.unwrap_or(encoding_rs::UTF_8);
        Self {
            state: AsyncWriterState::new(writer, initial_encoding),
            encoding_override: encoding,
        }
    }

    /// The encoding override passed to
    /// [`DtaWriter::encoding`](super::dta_writer::DtaWriter::encoding),
    /// if any. When `None`, the writer picks the release-appropriate
    /// default (Windows-1252 for pre-V118, UTF-8 for V118+) once
    /// [`write_header`](Self::write_header) resolves the format
    /// version. The resolved encoding is available on every
    /// downstream writer via `encoding()`.
    #[must_use]
    #[inline]
    pub fn encoding_override(&self) -> Option<&'static Encoding> {
        self.encoding_override
    }
}

impl<W: AsyncWrite + AsyncSeek + Unpin> AsyncHeaderWriter<W> {
    /// Writes the file header and transitions to schema writing.
    ///
    /// For binary formats (104–116) this emits the fixed 10-byte
    /// preamble followed by the dataset label and timestamp fields.
    /// For XML formats (117+) this emits the `<stata_dta><header>`
    /// opening tags and the `<release>`, `<byteorder>`, `<K>`, `<N>`,
    /// `<label>`, and `<timestamp>` fields with zero placeholders for
    /// K and N. K is patched by the schema writer; N is patched by
    /// the record writer (once that stage exists on the async side).
    ///
    /// # Errors
    ///
    /// Returns [`DtaError::Io`] on
    /// sink failures and [`DtaError::Format`]
    /// if the [`Header`] contains values the target format cannot
    /// represent.
    pub async fn write_header(mut self, header: Header) -> Result<AsyncSchemaWriter<W>> {
        let release = header.release();
        let encoding = self
            .encoding_override
            .unwrap_or_else(|| release.default_encoding());
        self.state = self.state.with_encoding(encoding);

        if release.is_xml_like() {
            self.write_xml_header(&header).await?;
        } else {
            self.write_binary_header(&header).await?;
        }

        let writer = AsyncSchemaWriter::new(self.state, header);
        Ok(writer)
    }
}

// ---------------------------------------------------------------------------
// Binary format (104–116)
// ---------------------------------------------------------------------------

impl<W: AsyncWrite + Unpin> AsyncHeaderWriter<W> {
    async fn write_binary_header(&mut self, header: &Header) -> Result<()> {
        let release = header.release();
        let byte_order = header.byte_order();

        let byte_order_byte = byte_order
            .to_header_byte(release)
            .map_err(|kind| DtaError::format(Section::Header, self.state.position(), kind))?;

        self.state
            .write_u8(release.to_byte(), Section::Header)
            .await?;
        self.state
            .write_u8(byte_order_byte, Section::Header)
            .await?;
        self.state
            .write_u8(BINARY_FILETYPE, Section::Header)
            .await?;
        self.state
            .write_u8(BINARY_RESERVED_PADDING, Section::Header)
            .await?;

        // Zero placeholders for K and N; later async stages will
        // patch once they exist. K is u16. N is u16 for V102 and u32
        // for V103–V117.
        self.state
            .set_header_variable_count_offset(self.state.position());
        self.state.write_u16(0, byte_order, Section::Header).await?;
        self.state
            .set_header_observation_count_offset(self.state.position());
        if release.supports_extended_binary_observation_count() {
            self.state.write_u32(0, byte_order, Section::Header).await?;
        } else {
            self.state.write_u16(0, byte_order, Section::Header).await?;
        }

        self.state
            .write_fixed_string(
                header.dataset_label(),
                release.dataset_label_len(),
                Section::Header,
                Field::DatasetLabel,
            )
            .await?;

        if let Some(len) = release.timestamp_len() {
            self.write_fixed_timestamp(header.timestamp(), len).await?;
        }
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// XML format (117+)
// ---------------------------------------------------------------------------

impl<W: AsyncWrite + Unpin> AsyncHeaderWriter<W> {
    async fn write_xml_header(&mut self, header: &Header) -> Result<()> {
        let release = header.release();
        let byte_order = header.byte_order();

        self.state
            .write_exact(b"<stata_dta><header>", Section::Header)
            .await?;

        self.state
            .write_exact(b"<release>", Section::Header)
            .await?;
        let release_digits = format!("{:03}", release.to_byte());
        self.state
            .write_exact(release_digits.as_bytes(), Section::Header)
            .await?;
        self.state
            .write_exact(b"</release>", Section::Header)
            .await?;

        self.state
            .write_exact(b"<byteorder>", Section::Header)
            .await?;
        self.state
            .write_exact(byte_order.to_string().as_bytes(), Section::Header)
            .await?;
        self.state
            .write_exact(b"</byteorder>", Section::Header)
            .await?;

        self.state.write_exact(b"<K>", Section::Header).await?;
        self.state
            .set_header_variable_count_offset(self.state.position());
        if release.supports_extended_variable_count() {
            self.state.write_u32(0, byte_order, Section::Header).await?;
        } else {
            self.state.write_u16(0, byte_order, Section::Header).await?;
        }
        self.state.write_exact(b"</K>", Section::Header).await?;

        self.state.write_exact(b"<N>", Section::Header).await?;
        self.state
            .set_header_observation_count_offset(self.state.position());
        if release.supports_extended_observation_count() {
            self.state.write_u64(0, byte_order, Section::Header).await?;
        } else {
            self.state.write_u32(0, byte_order, Section::Header).await?;
        }
        self.state.write_exact(b"</N>", Section::Header).await?;

        self.state.write_exact(b"<label>", Section::Header).await?;
        self.write_xml_label(header.dataset_label(), release, byte_order)
            .await?;
        self.state.write_exact(b"</label>", Section::Header).await?;

        self.state
            .write_exact(b"<timestamp>", Section::Header)
            .await?;
        self.write_xml_timestamp(header.timestamp()).await?;
        self.state
            .write_exact(b"</timestamp>", Section::Header)
            .await?;

        self.state
            .write_exact(b"</header>", Section::Header)
            .await?;
        Ok(())
    }

    async fn write_xml_label(
        &mut self,
        label: &str,
        release: Release,
        byte_order: ByteOrder,
    ) -> Result<()> {
        let position = self.state.position();
        let encoded = encode_value(
            label,
            self.state.encoding(),
            Section::Header,
            Field::DatasetLabel,
            position,
        )?;
        if release.supports_extended_dataset_label() {
            let length = narrow_len_to_u16(
                encoded.len(),
                Section::Header,
                Field::DatasetLabel,
                position,
            )?;
            self.state
                .write_u16(length, byte_order, Section::Header)
                .await?;
        } else {
            let length = narrow_len_to_u8(
                encoded.len(),
                Section::Header,
                Field::DatasetLabel,
                position,
            )?;
            self.state.write_u8(length, Section::Header).await?;
        }
        self.state.write_exact(&encoded, Section::Header).await?;
        Ok(())
    }

    async fn write_xml_timestamp(&mut self, timestamp: Option<&StataTimestamp>) -> Result<()> {
        let formatted = format_timestamp(timestamp);
        let bytes = formatted.as_bytes();
        let length = narrow_len_to_u8(
            bytes.len(),
            Section::Header,
            Field::Timestamp,
            self.state.position(),
        )?;
        self.state.write_u8(length, Section::Header).await?;
        self.state.write_exact(bytes, Section::Header).await?;
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Header-specific helpers
// ---------------------------------------------------------------------------

impl<W: AsyncWrite + Unpin> AsyncHeaderWriter<W> {
    async fn write_fixed_timestamp(
        &mut self,
        timestamp: Option<&StataTimestamp>,
        len: usize,
    ) -> Result<()> {
        let formatted = format_timestamp(timestamp);
        self.state
            .write_fixed_string(&formatted, len, Section::Header, Field::Timestamp)
            .await
    }
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;

    use super::*;
    use crate::stata::dta::dta_error::{DtaError, FormatErrorKind};
    use crate::stata::dta::dta_reader::DtaReader;
    use crate::stata::dta::dta_writer::DtaWriter;
    use crate::stata::dta::schema::Schema;

    /// Writes `header` through the async header + schema writers
    /// (with an empty schema, terminal for the POC) and reads the
    /// header back via the async header reader, returning the
    /// round-tripped header. The empty schema gets K=0 patched into
    /// the header, matching the zero the reader sees.
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
        DtaReader::new()
            .from_tokio_reader(bytes.as_slice())
            .read_header()
            .await
            .unwrap()
            .header()
            .clone()
    }

    // -- Binary header round-trips (formats 104–116) -------------------------

    #[tokio::test]
    async fn binary_v114_little_endian_round_trip() {
        let timestamp = StataTimestamp::parse("01 Jan 2024 13:45").unwrap();
        let original = Header::builder(Release::V114, ByteOrder::LittleEndian)
            .dataset_label("My Dataset")
            .timestamp(Some(timestamp))
            .build();
        let parsed = round_trip(&original).await;
        assert_eq!(parsed.release(), Release::V114);
        assert_eq!(parsed.byte_order(), ByteOrder::LittleEndian);
        assert_eq!(parsed.dataset_label(), "My Dataset");
        assert_eq!(parsed.timestamp(), Some(&timestamp));
    }

    #[tokio::test]
    async fn binary_v114_big_endian_round_trip() {
        let original = Header::builder(Release::V114, ByteOrder::BigEndian)
            .dataset_label("BE test")
            .build();
        let parsed = round_trip(&original).await;
        assert_eq!(parsed.byte_order(), ByteOrder::BigEndian);
        assert_eq!(parsed.dataset_label(), "BE test");
    }

    #[tokio::test]
    async fn binary_v104_no_timestamp_round_trip() {
        let original = Header::builder(Release::V104, ByteOrder::LittleEndian)
            .dataset_label("Old format")
            .build();
        let parsed = round_trip(&original).await;
        assert_eq!(parsed.release(), Release::V104);
        assert_eq!(parsed.dataset_label(), "Old format");
        assert!(parsed.timestamp().is_none());
    }

    #[tokio::test]
    async fn binary_empty_label_round_trip() {
        let original = Header::builder(Release::V114, ByteOrder::LittleEndian).build();
        let parsed = round_trip(&original).await;
        assert_eq!(parsed.dataset_label(), "");
        assert!(parsed.timestamp().is_none());
    }

    // -- XML header round-trips (formats 117–119) ----------------------------

    #[tokio::test]
    async fn xml_v117_little_endian_round_trip() {
        let timestamp = StataTimestamp::parse("01 Jan 2024 13:45").unwrap();
        let original = Header::builder(Release::V117, ByteOrder::LittleEndian)
            .dataset_label("XML test")
            .timestamp(Some(timestamp))
            .build();
        let parsed = round_trip(&original).await;
        assert_eq!(parsed.release(), Release::V117);
        assert_eq!(parsed.dataset_label(), "XML test");
        assert_eq!(parsed.timestamp(), Some(&timestamp));
    }

    #[tokio::test]
    async fn xml_empty_label_round_trip() {
        let original = Header::builder(Release::V117, ByteOrder::LittleEndian).build();
        let parsed = round_trip(&original).await;
        assert_eq!(parsed.dataset_label(), "");
        assert!(parsed.timestamp().is_none());
    }

    // -- Error cases ---------------------------------------------------------

    #[tokio::test]
    async fn binary_label_too_long_errors() {
        let long_label = "x".repeat(200);
        let header = Header::builder(Release::V114, ByteOrder::LittleEndian)
            .dataset_label(long_label)
            .build();
        let error = DtaWriter::new()
            .from_tokio_writer(Cursor::new(Vec::<u8>::new()))
            .write_header(header)
            .await
            .unwrap_err();
        assert!(matches!(
            error,
            DtaError::Format(ref e) if matches!(
                e.kind(),
                FormatErrorKind::FieldTooLarge { field: Field::DatasetLabel, .. }
            )
        ));
    }

    #[tokio::test]
    async fn unrepresentable_label_encoding_errors() {
        let header = Header::builder(Release::V114, ByteOrder::LittleEndian)
            .dataset_label("日本語")
            .build();
        let error = DtaWriter::new()
            .from_tokio_writer(Cursor::new(Vec::<u8>::new()))
            .write_header(header)
            .await
            .unwrap_err();
        assert!(matches!(
            error,
            DtaError::Format(ref e) if matches!(
                e.kind(),
                FormatErrorKind::InvalidEncoding { field: Field::DatasetLabel }
            )
        ));
    }

    #[tokio::test]
    async fn utf8_encoding_override_allows_non_latin_label() {
        let header = Header::builder(Release::V114, ByteOrder::LittleEndian)
            .dataset_label("日本語")
            .build();
        let cursor: Cursor<Vec<u8>> = DtaWriter::new()
            .encoding(encoding_rs::UTF_8)
            .from_tokio_writer(Cursor::new(Vec::<u8>::new()))
            .write_header(header)
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
        assert_eq!(schema_reader.header().dataset_label(), "日本語");
    }
}
