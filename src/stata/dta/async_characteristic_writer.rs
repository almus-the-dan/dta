use tokio::io::{AsyncSeek, AsyncWrite};

use super::async_writer_state::AsyncWriterState;
use super::byte_order::ByteOrder;
use super::characteristic::{Characteristic, ExpansionFieldType};
use super::characteristic_format::{encode_characteristic_value, payload_length};
use super::dta_error::{DtaError, Field, FormatErrorKind, Result, Section};
use super::header::Header;
use super::release::Release;
use super::schema::Schema;

/// Writes characteristic (expansion-field) entries to a DTA file
/// asynchronously.
///
/// Unlike the header and schema phases, characteristic writing
/// accepts any number of entries via
/// [`write_characteristic`](Self::write_characteristic) before
/// transitioning via [`finish`](Self::finish). Handles both binary
/// and XML encodings internally: binary emits
/// `(data_type, length, payload)` triples terminated by a zero-length
/// entry; XML emits `<characteristics>` / `<ch>` tags.
#[derive(Debug)]
pub struct AsyncCharacteristicWriter<W> {
    state: AsyncWriterState<W>,
    header: Header,
    schema: Schema,
    /// Tracks whether the XML `<characteristics>` opening tag has
    /// been emitted. Unused (but harmless) for binary formats, which
    /// have no section tag.
    opened: bool,
}

impl<W> AsyncCharacteristicWriter<W> {
    #[must_use]
    pub(crate) fn new(state: AsyncWriterState<W>, header: Header, schema: Schema) -> Self {
        Self {
            state,
            header,
            schema,
            opened: false,
        }
    }

    /// The header emitted by the previous phase.
    #[must_use]
    #[inline]
    pub fn header(&self) -> &Header {
        &self.header
    }

    /// The schema emitted by the previous phase.
    #[must_use]
    #[inline]
    pub fn schema(&self) -> &Schema {
        &self.schema
    }

    /// Consumes the writer and returns the underlying sink. Used
    /// exclusively by `async_characteristic_reader` tests that need a
    /// partially written file — they drive the writer up through
    /// `write_schema` and then append hand-crafted expansion-field
    /// bytes (e.g., unknown `data_type` values the writer would never
    /// emit on its own) to exercise the reader's forward-compat
    /// skipping logic.
    #[cfg(test)]
    pub(crate) fn into_inner(self) -> W {
        self.state.into_inner()
    }
}

impl<W: AsyncWrite + AsyncSeek + Unpin> AsyncCharacteristicWriter<W> {
    /// Writes a single characteristic entry.
    ///
    /// Can be called any number of times (including zero). The first
    /// call also emits the XML `<characteristics>` opening tag for
    /// XML formats.
    ///
    /// # Errors
    ///
    /// Returns [`DtaError::Format`](DtaError::Format)
    /// with [`CharacteristicsUnsupported`](FormatErrorKind::CharacteristicsUnsupported)
    /// if the header's release is V104 (which has no expansion-field
    /// section), [`InvalidEncoding`](FormatErrorKind::InvalidEncoding)
    /// if the name or value contains bytes the active encoding cannot
    /// represent, and [`FieldTooLarge`](FormatErrorKind::FieldTooLarge)
    /// if the entry exceeds the format's length ceiling.
    pub async fn write_characteristic(&mut self, characteristic: &Characteristic) -> Result<()> {
        let release = self.header.release();
        let Some(is_extended) = release.supports_extended_expansion() else {
            return Err(DtaError::format(
                Section::Characteristics,
                self.state.position(),
                FormatErrorKind::CharacteristicsUnsupported { release },
            ));
        };
        if release.is_xml_like() {
            self.open_section_if_needed().await?;
            self.write_xml_entry(characteristic).await
        } else {
            self.write_binary_entry(characteristic, is_extended).await
        }
    }

    /// Closes the characteristics section, patches the data offset
    /// in the map (XML only), and returns the underlying writer.
    ///
    /// For XML the closing `</characteristics>` tag is emitted even
    /// if no entries were written (the opening tag is lazy-emitted
    /// here in that case). For pre-117 binary formats (V105–V116) a
    /// zero-length terminator entry is written. V104 has no
    /// expansion-field section at all, so nothing is written.
    ///
    /// POC-shaped terminal: once the async record writer exists this
    /// will return `AsyncRecordWriter<W>` and advance the typestate
    /// chain.
    ///
    /// # Errors
    ///
    /// Returns [`DtaError::Io`](DtaError::Io) on sink failures.
    pub async fn finish(mut self) -> Result<W> {
        let release = self.header.release();
        let byte_order = self.header.byte_order();

        if let Some(is_extended) = release.supports_extended_expansion() {
            self.write_terminator(release, is_extended, byte_order)
                .await?;
        }

        if release.is_xml_like() {
            let records_offset = self.state.position();
            self.state
                .patch_map_entry(9, records_offset, byte_order, Section::Characteristics)
                .await?;
        }

        Ok(self.state.into_inner())
    }

    async fn write_terminator(
        &mut self,
        release: Release,
        is_extended: bool,
        byte_order: ByteOrder,
    ) -> Result<()> {
        if release.is_xml_like() {
            self.open_section_if_needed().await?;
            self.state
                .write_exact(b"</characteristics>", Section::Characteristics)
                .await?;
        } else {
            self.state
                .write_u8(
                    ExpansionFieldType::Terminator.to_byte(),
                    Section::Characteristics,
                )
                .await?;
            if is_extended {
                self.state
                    .write_u32(0, byte_order, Section::Characteristics)
                    .await?;
            } else {
                self.state
                    .write_u16(0, byte_order, Section::Characteristics)
                    .await?;
            }
        }
        Ok(())
    }

    async fn open_section_if_needed(&mut self) -> Result<()> {
        debug_assert!(self.header.release().is_xml_like());
        if !self.opened {
            self.state
                .write_exact(b"<characteristics>", Section::Characteristics)
                .await?;
            self.opened = true;
        }
        Ok(())
    }
}

impl<W: AsyncWrite + Unpin> AsyncCharacteristicWriter<W> {
    /// Emits one `<ch>` entry: `<ch>` | `u32 payload_len` | payload |
    /// `</ch>`.
    async fn write_xml_entry(&mut self, characteristic: &Characteristic) -> Result<()> {
        let byte_order = self.header.byte_order();
        let encoded_value = encode_characteristic_value(
            characteristic.value(),
            self.state.encoding(),
            self.state.position(),
        )?;
        let payload_len = payload_length(
            self.header.release().variable_name_len(),
            encoded_value.len(),
            self.state.position(),
        )?;

        self.state
            .write_exact(b"<ch>", Section::Characteristics)
            .await?;
        self.state
            .write_u32(payload_len, byte_order, Section::Characteristics)
            .await?;
        self.write_payload(characteristic, &encoded_value).await?;
        self.state
            .write_exact(b"</ch>", Section::Characteristics)
            .await?;
        Ok(())
    }

    /// Emits one binary expansion-field triple: `u8 data_type=1` |
    /// `u16/u32 payload_len` | payload.
    async fn write_binary_entry(
        &mut self,
        characteristic: &Characteristic,
        is_extended: bool,
    ) -> Result<()> {
        let byte_order = self.header.byte_order();
        let encoded_value = encode_characteristic_value(
            characteristic.value(),
            self.state.encoding(),
            self.state.position(),
        )?;
        let payload_len = payload_length(
            self.header.release().variable_name_len(),
            encoded_value.len(),
            self.state.position(),
        )?;

        self.state
            .write_u8(
                ExpansionFieldType::Characteristic.to_byte(),
                Section::Characteristics,
            )
            .await?;
        if is_extended {
            self.state
                .write_u32(payload_len, byte_order, Section::Characteristics)
                .await?;
        } else {
            let narrow = self.state.narrow_to_u16(
                payload_len,
                Section::Characteristics,
                Field::CharacteristicValue,
            )?;
            self.state
                .write_u16(narrow, byte_order, Section::Characteristics)
                .await?;
        }
        self.write_payload(characteristic, &encoded_value).await?;
        Ok(())
    }

    /// Emits the three-part payload: target variable name
    /// (fixed-width, null-padded), characteristic name (same), value
    /// (variable-width, exact encoded bytes).
    async fn write_payload(
        &mut self,
        characteristic: &Characteristic,
        encoded_value: &[u8],
    ) -> Result<()> {
        let variable_name_len = self.header.release().variable_name_len();
        self.state
            .write_fixed_string(
                characteristic.target().as_variable_name(),
                variable_name_len,
                Section::Characteristics,
                Field::VariableName,
            )
            .await?;
        self.state
            .write_fixed_string(
                characteristic.name(),
                variable_name_len,
                Section::Characteristics,
                Field::CharacteristicName,
            )
            .await?;
        self.state
            .write_exact(encoded_value, Section::Characteristics)
            .await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;

    use super::*;
    use crate::stata::dta::characteristic::CharacteristicTarget;
    use crate::stata::dta::dta_reader::DtaReader;
    use crate::stata::dta::dta_writer::DtaWriter;
    use crate::stata::dta::variable::Variable;
    use crate::stata::dta::variable_type::VariableType;

    fn make_schema() -> Schema {
        Schema::builder()
            .add_variable(Variable::builder(VariableType::Byte, "mpg").format("%8.0g"))
            .build()
            .unwrap()
    }

    fn make_header(release: Release, byte_order: ByteOrder, schema: &Schema) -> Header {
        Header::builder(release, byte_order)
            .variable_count(u32::try_from(schema.variables().len()).unwrap())
            .build()
    }

    /// Writes `characteristics` through the async writer chain, then
    /// reads them back through the async reader chain.
    async fn round_trip(
        release: Release,
        byte_order: ByteOrder,
        characteristics: &[Characteristic],
    ) -> Vec<Characteristic> {
        let schema = make_schema();
        let header = make_header(release, byte_order, &schema);

        let mut writer = DtaWriter::new()
            .from_tokio_writer(Cursor::new(Vec::<u8>::new()))
            .write_header(header)
            .await
            .unwrap()
            .write_schema(schema)
            .await
            .unwrap();
        for entry in characteristics {
            writer.write_characteristic(entry).await.unwrap();
        }
        let cursor: Cursor<Vec<u8>> = writer.finish().await.unwrap();
        let bytes = cursor.into_inner();

        let mut reader = DtaReader::new()
            .from_tokio_reader(bytes.as_slice())
            .read_header()
            .await
            .unwrap()
            .read_schema()
            .await
            .unwrap();
        let mut parsed = Vec::new();
        while let Some(entry) = reader.read_characteristic().await.unwrap() {
            parsed.push(entry);
        }
        parsed
    }

    // -- Binary round-trips (formats 105–116) --------------------------------

    #[tokio::test]
    async fn binary_v114_dataset_characteristic_round_trip() {
        let entry = Characteristic::new(
            CharacteristicTarget::Dataset,
            "note1".to_owned(),
            "created for regression".to_owned(),
        );
        let parsed = round_trip(Release::V114, ByteOrder::LittleEndian, &[entry]).await;
        assert_eq!(parsed.len(), 1);
        assert_eq!(parsed[0].target(), &CharacteristicTarget::Dataset);
        assert_eq!(parsed[0].name(), "note1");
        assert_eq!(parsed[0].value(), "created for regression");
    }

    #[tokio::test]
    async fn binary_v114_variable_characteristic_round_trip() {
        let entry = Characteristic::new(
            CharacteristicTarget::Variable("mpg".to_owned()),
            "format_hint".to_owned(),
            "miles/gallon".to_owned(),
        );
        let parsed = round_trip(Release::V114, ByteOrder::LittleEndian, &[entry]).await;
        assert_eq!(parsed.len(), 1);
        assert_eq!(
            parsed[0].target(),
            &CharacteristicTarget::Variable("mpg".to_owned()),
        );
        assert_eq!(parsed[0].value(), "miles/gallon");
    }

    #[tokio::test]
    async fn binary_v114_multiple_characteristics_round_trip() {
        let entries = vec![
            Characteristic::new(
                CharacteristicTarget::Dataset,
                "first".to_owned(),
                "one".to_owned(),
            ),
            Characteristic::new(
                CharacteristicTarget::Variable("mpg".to_owned()),
                "second".to_owned(),
                "two two two".to_owned(),
            ),
            Characteristic::new(
                CharacteristicTarget::Dataset,
                "third".to_owned(),
                String::new(),
            ),
        ];
        let parsed = round_trip(Release::V114, ByteOrder::LittleEndian, &entries).await;
        assert_eq!(parsed.len(), 3);
        assert_eq!(parsed[0].name(), "first");
        assert_eq!(parsed[1].name(), "second");
        assert_eq!(parsed[2].value(), "");
    }

    #[tokio::test]
    async fn binary_v106_u16_length_round_trip() {
        let entry = Characteristic::new(
            CharacteristicTarget::Dataset,
            "note".to_owned(),
            "x".repeat(50),
        );
        let parsed = round_trip(Release::V106, ByteOrder::LittleEndian, &[entry]).await;
        assert_eq!(parsed.len(), 1);
        assert_eq!(parsed[0].value().len(), 50);
    }

    #[tokio::test]
    async fn binary_v114_big_endian_round_trip() {
        let entry = Characteristic::new(
            CharacteristicTarget::Dataset,
            "be".to_owned(),
            "big-endian value".to_owned(),
        );
        let parsed = round_trip(Release::V114, ByteOrder::BigEndian, &[entry]).await;
        assert_eq!(parsed.len(), 1);
        assert_eq!(parsed[0].value(), "big-endian value");
    }

    #[tokio::test]
    async fn binary_v114_zero_characteristics_round_trip() {
        let parsed = round_trip(Release::V114, ByteOrder::LittleEndian, &[]).await;
        assert!(parsed.is_empty());
    }

    // -- XML round-trips (formats 117–119) -----------------------------------

    #[tokio::test]
    async fn xml_v117_round_trip() {
        let entries = vec![
            Characteristic::new(
                CharacteristicTarget::Dataset,
                "note1".to_owned(),
                "hello".to_owned(),
            ),
            Characteristic::new(
                CharacteristicTarget::Variable("mpg".to_owned()),
                "tag".to_owned(),
                "world".to_owned(),
            ),
        ];
        let parsed = round_trip(Release::V117, ByteOrder::LittleEndian, &entries).await;
        assert_eq!(parsed.len(), 2);
        assert_eq!(parsed[0].target(), &CharacteristicTarget::Dataset);
        assert_eq!(parsed[0].value(), "hello");
        assert_eq!(
            parsed[1].target(),
            &CharacteristicTarget::Variable("mpg".to_owned()),
        );
    }

    #[tokio::test]
    async fn xml_v117_big_endian_round_trip() {
        let entry = Characteristic::new(
            CharacteristicTarget::Dataset,
            "msf".to_owned(),
            "big endian XML".to_owned(),
        );
        let parsed = round_trip(Release::V117, ByteOrder::BigEndian, &[entry]).await;
        assert_eq!(parsed.len(), 1);
        assert_eq!(parsed[0].value(), "big endian XML");
    }

    #[tokio::test]
    async fn xml_v117_zero_characteristics_round_trip() {
        let parsed = round_trip(Release::V117, ByteOrder::LittleEndian, &[]).await;
        assert!(parsed.is_empty());
    }

    #[tokio::test]
    async fn xml_v118_utf8_value_round_trip() {
        let entry = Characteristic::new(
            CharacteristicTarget::Dataset,
            "note".to_owned(),
            "日本語".to_owned(),
        );
        let parsed = round_trip(Release::V118, ByteOrder::LittleEndian, &[entry]).await;
        assert_eq!(parsed[0].value(), "日本語");
    }

    // -- Error cases ---------------------------------------------------------

    #[tokio::test]
    async fn v104_rejects_characteristic() {
        let schema = make_schema();
        let header = make_header(Release::V104, ByteOrder::LittleEndian, &schema);
        let mut writer = DtaWriter::new()
            .from_tokio_writer(Cursor::new(Vec::<u8>::new()))
            .write_header(header)
            .await
            .unwrap()
            .write_schema(schema)
            .await
            .unwrap();
        let entry = Characteristic::new(
            CharacteristicTarget::Dataset,
            "n".to_owned(),
            "v".to_owned(),
        );
        let error = writer.write_characteristic(&entry).await.unwrap_err();
        assert!(matches!(
            error,
            DtaError::Format(ref e) if matches!(
                e.kind(),
                FormatErrorKind::CharacteristicsUnsupported { release: Release::V104 }
            )
        ));
    }

    #[tokio::test]
    async fn v104_zero_characteristics_transitions_cleanly() {
        let schema = make_schema();
        let header = make_header(Release::V104, ByteOrder::LittleEndian, &schema);
        let writer = DtaWriter::new()
            .from_tokio_writer(Cursor::new(Vec::<u8>::new()))
            .write_header(header)
            .await
            .unwrap()
            .write_schema(schema)
            .await
            .unwrap();
        // Should not panic or error.
        let _cursor: Cursor<Vec<u8>> = writer.finish().await.unwrap();
    }

    #[tokio::test]
    async fn binary_v106_oversize_value_errors() {
        let schema = make_schema();
        let header = make_header(Release::V106, ByteOrder::LittleEndian, &schema);
        let mut writer = DtaWriter::new()
            .from_tokio_writer(Cursor::new(Vec::<u8>::new()))
            .write_header(header)
            .await
            .unwrap()
            .write_schema(schema)
            .await
            .unwrap();
        let huge_value = "x".repeat(usize::from(u16::MAX));
        let entry = Characteristic::new(CharacteristicTarget::Dataset, "n".to_owned(), huge_value);
        let error = writer.write_characteristic(&entry).await.unwrap_err();
        assert!(matches!(
            error,
            DtaError::Format(ref e) if matches!(
                e.kind(),
                FormatErrorKind::FieldTooLarge { field: Field::CharacteristicValue, .. }
            )
        ));
    }

    #[tokio::test]
    async fn non_latin_value_in_windows_1252_errors() {
        let schema = make_schema();
        let header = make_header(Release::V114, ByteOrder::LittleEndian, &schema);
        let mut writer = DtaWriter::new()
            .from_tokio_writer(Cursor::new(Vec::<u8>::new()))
            .write_header(header)
            .await
            .unwrap()
            .write_schema(schema)
            .await
            .unwrap();
        let entry = Characteristic::new(
            CharacteristicTarget::Dataset,
            "n".to_owned(),
            "日本語".to_owned(),
        );
        let error = writer.write_characteristic(&entry).await.unwrap_err();
        assert!(matches!(
            error,
            DtaError::Format(ref e) if matches!(
                e.kind(),
                FormatErrorKind::InvalidEncoding { field: Field::CharacteristicValue }
            )
        ));
    }

    #[tokio::test]
    async fn characteristic_name_too_long_errors() {
        let schema = make_schema();
        let header = make_header(Release::V114, ByteOrder::LittleEndian, &schema);
        let mut writer = DtaWriter::new()
            .from_tokio_writer(Cursor::new(Vec::<u8>::new()))
            .write_header(header)
            .await
            .unwrap()
            .write_schema(schema)
            .await
            .unwrap();
        let entry = Characteristic::new(
            CharacteristicTarget::Dataset,
            "n".repeat(50),
            "v".to_owned(),
        );
        let error = writer.write_characteristic(&entry).await.unwrap_err();
        assert!(matches!(
            error,
            DtaError::Format(ref e) if matches!(
                e.kind(),
                FormatErrorKind::FieldTooLarge { field: Field::CharacteristicName, .. }
            )
        ));
    }
}
