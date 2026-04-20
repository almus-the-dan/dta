use tokio::io::AsyncRead;

use super::async_reader_state::AsyncReaderState;
use super::characteristic::{Characteristic, CharacteristicTarget, ExpansionFieldType};
use super::characteristic_parse::{
    XML_SECTION_CLOSE_REST, XML_SECTION_OPEN_REST, XmlCharacteristicTag, characteristic_value_len,
    classify_xml_tag_head, expansion_length_to_usize,
};
use super::dta_error::{DtaError, Field, FormatErrorKind, Result, Section};
use super::header::Header;
use super::schema::Schema;

/// Reads characteristics from a DTA file asynchronously.
///
/// For XML formats (117+), reads the `<characteristics>` section.
/// For binary formats (104–116), reads expansion fields.
///
/// Owns the parsed [`Header`] and [`Schema`] from previous phases.
/// Call [`read_characteristic`](Self::read_characteristic) repeatedly
/// until it returns `None`, or [`skip_to_end`](Self::skip_to_end) to
/// advance past the section without processing.
#[derive(Debug)]
pub struct AsyncCharacteristicReader<R> {
    state: AsyncReaderState<R>,
    header: Header,
    schema: Schema,
    completed: bool,
}

impl<R> AsyncCharacteristicReader<R> {
    #[must_use]
    pub(crate) fn new(state: AsyncReaderState<R>, header: Header, schema: Schema) -> Self {
        Self {
            state,
            header,
            schema,
            completed: false,
        }
    }

    /// The parsed file header.
    #[must_use]
    #[inline]
    pub fn header(&self) -> &Header {
        &self.header
    }

    /// The parsed variable definitions.
    #[must_use]
    #[inline]
    pub fn schema(&self) -> &Schema {
        &self.schema
    }
}

impl<R: AsyncRead + Unpin> AsyncCharacteristicReader<R> {
    /// Reads the next characteristic entry.
    ///
    /// Returns `None` when all entries have been consumed. For XML
    /// formats each entry is a `<ch>` element containing a
    /// length-prefixed record with variable name, characteristic name,
    /// and contents. For binary formats each entry is an expansion
    /// field with a type byte and length-prefixed payload.
    ///
    /// # Errors
    ///
    /// Returns [`DtaError::Io`] on read failures and
    /// [`DtaError::Format`] when the entry bytes violate the DTA
    /// format specification.
    pub async fn read_characteristic(&mut self) -> Result<Option<Characteristic>> {
        if self.completed {
            return Ok(None);
        }
        if self.header.release().is_xml_like() {
            self.read_xml_characteristic().await
        } else {
            self.read_binary_characteristic().await
        }
    }

    /// Skips all remaining characteristic entries without processing
    /// them.
    ///
    /// # Errors
    ///
    /// Returns [`DtaError::Io`] on read failures and
    /// [`DtaError::Format`] when the section structure violates the
    /// DTA format specification.
    pub async fn skip_to_end(&mut self) -> Result<()> {
        if self.completed {
            return Ok(());
        }
        if self.header.release().is_xml_like() {
            self.skip_xml_characteristics().await
        } else {
            self.skip_binary_characteristics().await
        }
    }

    /// Consumes any remaining characteristic entries and returns the
    /// underlying reader, closing out this phase.
    ///
    /// For binary formats, this also computes and stores the
    /// data-section and value-label offsets in the state's section
    /// offsets — those positions are not known until expansion fields
    /// have been fully consumed.
    ///
    /// POC-shaped terminal: once the async record reader exists this
    /// will return `AsyncRecordReader<R>` and advance the typestate
    /// chain.
    ///
    /// # Errors
    ///
    /// Returns [`DtaError::Io`] on read failures or if the state's
    /// section offsets have not been initialized.
    pub async fn finish(mut self) -> Result<()> {
        self.skip_to_end().await?;
        if !self.header.release().is_xml_like() {
            self.compute_binary_section_offsets()?;
        }
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// XML internals (format 117+)
// ---------------------------------------------------------------------------

impl<R: AsyncRead + Unpin> AsyncCharacteristicReader<R> {
    /// Reads the next XML-level tag in the characteristics section.
    async fn read_xml_tag(&mut self) -> Result<XmlCharacteristicTag> {
        let position = self.state.position();
        let head = self.state.read_exact(4, Section::Characteristics).await?;
        let tag = classify_xml_tag_head(head).ok_or_else(|| {
            DtaError::format(
                Section::Characteristics,
                position,
                FormatErrorKind::InvalidMagic,
            )
        })?;
        match tag {
            XmlCharacteristicTag::SectionOpen => {
                self.state
                    .expect_bytes(
                        XML_SECTION_OPEN_REST,
                        Section::Characteristics,
                        FormatErrorKind::InvalidMagic,
                    )
                    .await?;
            }
            XmlCharacteristicTag::SectionClose => {
                self.state
                    .expect_bytes(
                        XML_SECTION_CLOSE_REST,
                        Section::Characteristics,
                        FormatErrorKind::InvalidMagic,
                    )
                    .await?;
            }
            XmlCharacteristicTag::EntryOpen => {}
        }
        Ok(tag)
    }

    /// Reads and parses one XML characteristic entry.
    async fn read_xml_characteristic(&mut self) -> Result<Option<Characteristic>> {
        loop {
            match self.read_xml_tag().await? {
                XmlCharacteristicTag::SectionOpen => {}
                XmlCharacteristicTag::SectionClose => {
                    self.completed = true;
                    return Ok(None);
                }
                XmlCharacteristicTag::EntryOpen => {
                    let byte_order = self.header.byte_order();
                    let length = self
                        .state
                        .read_u32(byte_order, Section::Characteristics)
                        .await?;
                    let characteristic = self.parse_characteristic_payload(length).await?;
                    self.state
                        .expect_bytes(
                            b"</ch>",
                            Section::Characteristics,
                            FormatErrorKind::InvalidMagic,
                        )
                        .await?;
                    return Ok(Some(characteristic));
                }
            }
        }
    }

    /// Skips all remaining XML characteristic entries.
    async fn skip_xml_characteristics(&mut self) -> Result<()> {
        let byte_order = self.header.byte_order();
        loop {
            match self.read_xml_tag().await? {
                XmlCharacteristicTag::SectionOpen => {}
                XmlCharacteristicTag::SectionClose => {
                    self.completed = true;
                    return Ok(());
                }
                XmlCharacteristicTag::EntryOpen => {
                    let length = self
                        .state
                        .read_u32(byte_order, Section::Characteristics)
                        .await?;
                    let length = expansion_length_to_usize(length)?;
                    self.state.skip(length, Section::Characteristics).await?;
                    self.state
                        .expect_bytes(
                            b"</ch>",
                            Section::Characteristics,
                            FormatErrorKind::InvalidMagic,
                        )
                        .await?;
                }
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Binary internals (format 104–116)
// ---------------------------------------------------------------------------

impl<R: AsyncRead + Unpin> AsyncCharacteristicReader<R> {
    async fn read_binary_characteristic(&mut self) -> Result<Option<Characteristic>> {
        let Some(is_extended) = self.header.release().supports_extended_expansion() else {
            self.completed = true;
            return Ok(None);
        };
        loop {
            let (data_type, length) = self.read_binary_entry_header(is_extended).await?;
            match ExpansionFieldType::from_byte(data_type) {
                Some(ExpansionFieldType::Terminator) => {
                    self.completed = true;
                    return Ok(None);
                }
                Some(ExpansionFieldType::Characteristic) => {
                    return Ok(Some(self.parse_characteristic_payload(length).await?));
                }
                None => {
                    self.skip_expansion_payload(length).await?;
                }
            }
        }
    }

    /// Reads one binary expansion-field header (type byte + length).
    /// Returns `(data_type, length)`. A terminator is signaled by
    /// `(0, 0)`.
    async fn read_binary_entry_header(&mut self, is_extended: bool) -> Result<(u8, u32)> {
        let byte_order = self.header.byte_order();
        let data_type = self.state.read_u8(Section::Characteristics).await?;
        let length = if is_extended {
            self.state
                .read_u32(byte_order, Section::Characteristics)
                .await?
        } else {
            u32::from(
                self.state
                    .read_u16(byte_order, Section::Characteristics)
                    .await?,
            )
        };
        Ok((data_type, length))
    }

    async fn skip_binary_characteristics(&mut self) -> Result<()> {
        let Some(is_extended) = self.header.release().supports_extended_expansion() else {
            self.completed = true;
            return Ok(());
        };
        loop {
            let (data_type, length) = self.read_binary_entry_header(is_extended).await?;
            match ExpansionFieldType::from_byte(data_type) {
                Some(ExpansionFieldType::Terminator) => {
                    self.completed = true;
                    return Ok(());
                }
                Some(ExpansionFieldType::Characteristic) | None => {
                    self.skip_expansion_payload(length).await?;
                }
            }
        }
    }

    async fn skip_expansion_payload(&mut self, length: u32) -> Result<()> {
        let length_usize = expansion_length_to_usize(length)?;
        self.state
            .skip(length_usize, Section::Characteristics)
            .await
    }

    /// Computes and stores the data-section and value-label offsets
    /// for binary formats, where these positions are not known until
    /// all expansion fields have been consumed.
    fn compute_binary_section_offsets(&mut self) -> Result<()> {
        let records_offset = self.state.position();
        let row_len = u64::try_from(self.schema.row_len()).map_err(|_| {
            DtaError::io(
                Section::Records,
                std::io::Error::other("row length exceeds u64"),
            )
        })?;
        let data_section_size = self
            .header
            .observation_count()
            .checked_mul(row_len)
            .ok_or_else(|| {
                DtaError::io(
                    Section::Records,
                    std::io::Error::other("data section size overflow"),
                )
            })?;
        let value_labels_offset =
            records_offset
                .checked_add(data_section_size)
                .ok_or_else(|| {
                    DtaError::io(
                        Section::ValueLabels,
                        std::io::Error::other("value labels offset overflow"),
                    )
                })?;

        let offsets = self
            .state
            .section_offsets_mut()
            .ok_or_else(|| DtaError::missing_section_offsets(Section::Records))?;
        offsets.set_records(records_offset);
        offsets.set_value_labels(value_labels_offset);
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Shared payload parsing
// ---------------------------------------------------------------------------

impl<R: AsyncRead + Unpin> AsyncCharacteristicReader<R> {
    /// Parses the payload of a characteristic entry: variable name,
    /// characteristic name, and value (all null-terminated, fixed-width
    /// name fields).
    async fn parse_characteristic_payload(&mut self, total_length: u32) -> Result<Characteristic> {
        let encoding = self.state.encoding();
        let variable_name_len = self.header.release().variable_name_len();
        let entry_position = self.state.position();

        let variable_name = self
            .state
            .read_fixed_string(
                variable_name_len,
                encoding,
                Section::Characteristics,
                Field::VariableName,
            )
            .await?;
        let characteristic_name = self
            .state
            .read_fixed_string(
                variable_name_len,
                encoding,
                Section::Characteristics,
                Field::CharacteristicName,
            )
            .await?;

        let value_len = characteristic_value_len(total_length, variable_name_len, entry_position)?;
        let value = self
            .state
            .read_fixed_string(
                value_len,
                encoding,
                Section::Characteristics,
                Field::CharacteristicValue,
            )
            .await?;

        let target = CharacteristicTarget::from_variable_name(variable_name);
        Ok(Characteristic::new(target, characteristic_name, value))
    }
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;

    use super::*;
    use crate::stata::dta::byte_order::ByteOrder;
    use crate::stata::dta::dta_reader::DtaReader;
    use crate::stata::dta::dta_writer::DtaWriter;
    use crate::stata::dta::release::Release;
    use crate::stata::dta::schema::Schema;

    /// Writes a V114 binary DTA header + empty schema through the
    /// async writer, then appends `expansion_fields` verbatim as the
    /// characteristics section. Lets us hand-craft expansion-field
    /// sequences the writer would never emit on its own (e.g., unknown
    /// `data_type` values reserved by the DTA spec).
    async fn synthetic_v114_file(expansion_fields: &[u8]) -> Vec<u8> {
        let schema = Schema::builder().build().unwrap();
        let header = Header::builder(Release::V114, ByteOrder::LittleEndian).build();
        let characteristic_writer = DtaWriter::new()
            .from_tokio_writer(Cursor::new(Vec::<u8>::new()))
            .write_header(header)
            .await
            .unwrap()
            .write_schema(schema)
            .await
            .unwrap();
        let cursor: Cursor<Vec<u8>> = characteristic_writer.into_inner();
        let mut bytes = cursor.into_inner();
        bytes.extend_from_slice(expansion_fields);
        bytes
    }

    /// Builds a real characteristic payload: `_dta` / variable-name
    /// field + characteristic name field + value bytes (null-padded
    /// for the two names, exact for the value).
    fn characteristic_payload(target: &str, name: &str, value: &[u8]) -> Vec<u8> {
        let variable_name_len = Release::V114.variable_name_len();
        let mut payload = Vec::with_capacity(2 * variable_name_len + value.len());
        let mut target_field = target.as_bytes().to_vec();
        target_field.resize(variable_name_len, 0);
        payload.extend_from_slice(&target_field);
        let mut name_field = name.as_bytes().to_vec();
        name_field.resize(variable_name_len, 0);
        payload.extend_from_slice(&name_field);
        payload.extend_from_slice(value);
        payload
    }

    #[tokio::test]
    async fn unknown_expansion_field_type_is_skipped() {
        let mut expansion_fields = Vec::new();
        expansion_fields.push(2u8);
        expansion_fields.extend_from_slice(&7u32.to_le_bytes());
        expansion_fields.extend_from_slice(&[0xAA; 7]);

        let payload = characteristic_payload("_dta", "note1", b"hi");
        expansion_fields.push(1u8);
        expansion_fields.extend_from_slice(&u32::try_from(payload.len()).unwrap().to_le_bytes());
        expansion_fields.extend_from_slice(&payload);

        expansion_fields.push(0u8);
        expansion_fields.extend_from_slice(&0u32.to_le_bytes());

        let bytes = synthetic_v114_file(&expansion_fields).await;
        let mut reader = DtaReader::new()
            .from_tokio_reader(bytes.as_slice())
            .read_header()
            .await
            .unwrap()
            .read_schema()
            .await
            .unwrap();

        let first = reader.read_characteristic().await.unwrap().unwrap();
        assert_eq!(first.target(), &CharacteristicTarget::Dataset);
        assert_eq!(first.name(), "note1");
        assert_eq!(first.value(), "hi");

        assert!(reader.read_characteristic().await.unwrap().is_none());
    }

    #[tokio::test]
    async fn multiple_unknown_expansion_fields_between_real_entries() {
        let mut expansion_fields = Vec::new();

        expansion_fields.push(2u8);
        expansion_fields.extend_from_slice(&3u32.to_le_bytes());
        expansion_fields.extend_from_slice(&[0xBB; 3]);

        let payload = characteristic_payload("_dta", "middle", b"real");
        expansion_fields.push(1u8);
        expansion_fields.extend_from_slice(&u32::try_from(payload.len()).unwrap().to_le_bytes());
        expansion_fields.extend_from_slice(&payload);

        expansion_fields.push(42u8);
        expansion_fields.extend_from_slice(&9u32.to_le_bytes());
        expansion_fields.extend_from_slice(&[0xCC; 9]);

        expansion_fields.push(0u8);
        expansion_fields.extend_from_slice(&0u32.to_le_bytes());

        let bytes = synthetic_v114_file(&expansion_fields).await;
        let mut reader = DtaReader::new()
            .from_tokio_reader(bytes.as_slice())
            .read_header()
            .await
            .unwrap()
            .read_schema()
            .await
            .unwrap();

        let first = reader.read_characteristic().await.unwrap().unwrap();
        assert_eq!(first.name(), "middle");
        assert_eq!(first.value(), "real");
        assert!(reader.read_characteristic().await.unwrap().is_none());
    }

    #[tokio::test]
    async fn skip_to_end_tolerates_unknown_expansion_field_type() {
        let mut expansion_fields = Vec::new();
        expansion_fields.push(3u8);
        expansion_fields.extend_from_slice(&4u32.to_le_bytes());
        expansion_fields.extend_from_slice(&[0xDD; 4]);
        expansion_fields.push(0u8);
        expansion_fields.extend_from_slice(&0u32.to_le_bytes());

        let bytes = synthetic_v114_file(&expansion_fields).await;
        let mut reader = DtaReader::new()
            .from_tokio_reader(bytes.as_slice())
            .read_header()
            .await
            .unwrap()
            .read_schema()
            .await
            .unwrap();
        reader.skip_to_end().await.unwrap();
    }
}
