use std::io::{BufRead, Seek};

use super::characteristic::{Characteristic, CharacteristicTarget, ExpansionFieldType};
use super::characteristic_parse::{
    XML_SECTION_CLOSE_REST, XML_SECTION_OPEN_REST, XmlCharacteristicTag, characteristic_value_len,
    classify_xml_tag_head, expansion_length_to_usize,
};
use super::dta_error::{DtaError, Field, FormatErrorKind, Result, Section};
use super::header::Header;
use super::long_string_reader::LongStringReader;
use super::reader_state::ReaderState;
use super::record_parse::data_section_overflow_error;
use super::record_reader::RecordReader;
use super::schema::Schema;
use super::value_label_reader::ValueLabelReader;

/// Reads characteristics from a DTA file.
///
/// For XML formats (117+), it reads the `<characteristics>` section.
/// For binary formats (104–116), reads expansion fields.
///
/// Owns the parsed [`Header`] and [`Schema`] from previous phases.
/// Call [`into_record_reader`](Self::into_record_reader) after
/// consuming all entries to advance to data reading.
#[derive(Debug)]
pub struct CharacteristicReader<R> {
    state: ReaderState<R>,
    header: Header,
    schema: Schema,
    completed: bool,
}

impl<R> CharacteristicReader<R> {
    #[must_use]
    pub(crate) fn new(state: ReaderState<R>, header: Header, schema: Schema) -> Self {
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

    /// The encoding this reader uses to decode string fields.
    ///
    /// Defaults to Windows-1252 for pre-V118 releases and UTF-8 for
    /// V118+, overridable via
    /// [`DtaReader::encoding`](super::dta_reader::DtaReader::encoding).
    #[must_use]
    #[inline]
    pub fn encoding(&self) -> &'static encoding_rs::Encoding {
        self.state.encoding()
    }
}

// ---------------------------------------------------------------------------
// Sequential reading (BufRead)
// ---------------------------------------------------------------------------

impl<R: BufRead> CharacteristicReader<R> {
    /// Reads the next characteristic entry.
    ///
    /// Returns `None` when all entries have been consumed. For XML
    /// formats, each entry is a `<ch>` element containing a
    /// length-prefixed record with variable name, characteristic name,
    /// and contents. For binary formats, each entry is an expansion
    /// field with a type byte and length-prefixed payload.
    ///
    /// # Errors
    ///
    /// Returns [`DtaError::Io`] on read failures and
    /// [`DtaError::Format`] when the entry bytes violate the DTA
    /// format specification.
    pub fn read_characteristic(&mut self) -> Result<Option<Characteristic>> {
        if self.completed {
            return Ok(None);
        }
        if self.header.release().is_xml_like() {
            self.read_xml_characteristic()
        } else {
            self.read_binary_characteristic()
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
    pub fn skip_to_end(&mut self) -> Result<()> {
        if self.completed {
            return Ok(());
        }
        if self.header.release().is_xml_like() {
            self.skip_xml_characteristics()
        } else {
            self.skip_binary_characteristics()
        }
    }

    /// Transitions to record reading.
    ///
    /// All characteristic entries must have been consumed (via
    /// [`read_characteristic`](Self::read_characteristic) or
    /// [`skip_to_end`](Self::skip_to_end)) before calling this
    /// method.
    ///
    /// For binary formats, this method computes and stores the
    /// data-section and value-label offsets in the section offsets,
    /// since those are not known until expansion fields have been
    /// fully consumed.
    ///
    /// # Errors
    ///
    /// Returns [`DtaError::Io`] if characteristics have not been
    /// fully consumed or if section offsets are missing, and
    /// [`DtaError::Format`] with
    /// [`FieldTooLarge`](FormatErrorKind::FieldTooLarge) tagged
    /// `Field::ObservationCount` if the computed data-section size
    /// or value-labels offset (binary formats only) overflows `u64`.
    pub fn into_record_reader(mut self) -> Result<RecordReader<R>> {
        if !self.completed {
            return Err(DtaError::io(
                Section::Characteristics,
                std::io::Error::other(
                    "characteristics section must be fully consumed \
                     before transitioning to record reading",
                ),
            ));
        }

        if !self.header.release().is_xml_like() {
            self.compute_binary_section_offsets()?;
        }

        let reader = RecordReader::new(self.state, self.header, self.schema);
        Ok(reader)
    }
}

// ---------------------------------------------------------------------------
// XML internals (format 117+)
// ---------------------------------------------------------------------------

impl<R: BufRead> CharacteristicReader<R> {
    /// Reads the next XML-level tag in the characteristics section.
    fn read_xml_tag(&mut self) -> Result<XmlCharacteristicTag> {
        let position = self.state.position();
        let head = self.state.read_exact(4, Section::Characteristics)?;
        let tag = classify_xml_tag_head(head).ok_or_else(|| {
            DtaError::format(
                Section::Characteristics,
                position,
                FormatErrorKind::InvalidMagic,
            )
        })?;
        match tag {
            XmlCharacteristicTag::SectionOpen => {
                self.state.expect_bytes(
                    XML_SECTION_OPEN_REST,
                    Section::Characteristics,
                    FormatErrorKind::InvalidMagic,
                )?;
            }
            XmlCharacteristicTag::SectionClose => {
                self.state.expect_bytes(
                    XML_SECTION_CLOSE_REST,
                    Section::Characteristics,
                    FormatErrorKind::InvalidMagic,
                )?;
            }
            XmlCharacteristicTag::EntryOpen => {}
        }
        Ok(tag)
    }

    /// Reads and parses one XML characteristic entry.
    fn read_xml_characteristic(&mut self) -> Result<Option<Characteristic>> {
        loop {
            match self.read_xml_tag()? {
                XmlCharacteristicTag::SectionOpen => {}
                XmlCharacteristicTag::SectionClose => {
                    self.completed = true;
                    return Ok(None);
                }
                XmlCharacteristicTag::EntryOpen => {
                    let byte_order = self.header.byte_order();
                    let length = self.state.read_u32(byte_order, Section::Characteristics)?;
                    let characteristic = self.parse_characteristic_payload(length)?;
                    self.state.expect_bytes(
                        b"</ch>",
                        Section::Characteristics,
                        FormatErrorKind::InvalidMagic,
                    )?;
                    return Ok(Some(characteristic));
                }
            }
        }
    }

    /// Skips all remaining XML characteristic entries.
    fn skip_xml_characteristics(&mut self) -> Result<()> {
        loop {
            match self.read_xml_tag()? {
                XmlCharacteristicTag::SectionOpen => {}
                XmlCharacteristicTag::SectionClose => {
                    self.completed = true;
                    return Ok(());
                }
                XmlCharacteristicTag::EntryOpen => {
                    let byte_order = self.header.byte_order();
                    let length = self.state.read_u32(byte_order, Section::Characteristics)?;
                    let length = expansion_length_to_usize(length)?;
                    self.state.skip(length, Section::Characteristics)?;
                    self.state.expect_bytes(
                        b"</ch>",
                        Section::Characteristics,
                        FormatErrorKind::InvalidMagic,
                    )?;
                }
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Binary internals (format 104–116)
// ---------------------------------------------------------------------------

impl<R: BufRead> CharacteristicReader<R> {
    /// Reads one binary expansion-field header (type byte + length).
    ///
    /// The `is_extended` flag selects whether the length field is 4
    /// bytes (`true`, formats 110–116) or 2 bytes (`false`, formats
    /// 105–109). Callers handle the V104 case (no expansion fields)
    /// before calling this method.
    ///
    /// Returns `(data_type, length)`. A terminator is signaled by
    /// `(0, 0)`.
    fn read_binary_entry_header(&mut self, is_extended: bool) -> Result<(u8, u32)> {
        let byte_order = self.header.byte_order();
        let data_type = self.state.read_u8(Section::Characteristics)?;
        let length = if is_extended {
            self.state.read_u32(byte_order, Section::Characteristics)?
        } else {
            u32::from(self.state.read_u16(byte_order, Section::Characteristics)?)
        };
        Ok((data_type, length))
    }

    /// Reads and parses one binary characteristic entry.
    ///
    /// Skips past any expansion-field entries whose `data_type` byte
    /// is neither `0` (terminator) nor `1` (characteristic) —
    /// per the DTA spec's forward-compat rule, "unknown expansion
    /// types can simply be skipped". Returns `None` once the
    /// terminator is reached.
    fn read_binary_characteristic(&mut self) -> Result<Option<Characteristic>> {
        let Some(is_extended) = self.header.release().supports_extended_expansion() else {
            // V104: no expansion fields.
            self.completed = true;
            return Ok(None);
        };
        loop {
            let (data_type, length) = self.read_binary_entry_header(is_extended)?;
            match ExpansionFieldType::from_byte(data_type) {
                Some(ExpansionFieldType::Terminator) => {
                    self.completed = true;
                    return Ok(None);
                }
                Some(ExpansionFieldType::Characteristic) => {
                    return Ok(Some(self.parse_characteristic_payload(length)?));
                }
                None => {
                    self.skip_expansion_payload(length)?;
                }
            }
        }
    }

    /// Skips all remaining binary expansion-field entries.
    fn skip_binary_characteristics(&mut self) -> Result<()> {
        let Some(is_extended) = self.header.release().supports_extended_expansion() else {
            // V104: no expansion fields.
            self.completed = true;
            return Ok(());
        };
        loop {
            let (data_type, length) = self.read_binary_entry_header(is_extended)?;
            match ExpansionFieldType::from_byte(data_type) {
                Some(ExpansionFieldType::Terminator) => {
                    self.completed = true;
                    return Ok(());
                }
                Some(ExpansionFieldType::Characteristic) | None => {
                    self.skip_expansion_payload(length)?;
                }
            }
        }
    }

    /// Skips `length` payload bytes inside the characteristics
    /// section, translating the length to `usize` with a clean I/O
    /// error on overflow.
    fn skip_expansion_payload(&mut self, length: u32) -> Result<()> {
        let length_usize = expansion_length_to_usize(length)?;
        self.state.skip(length_usize, Section::Characteristics)
    }

    /// Computes and stores the data-section and value-label offsets
    /// for binary formats, where these positions are not known until
    /// all expansion fields have been consumed.
    fn compute_binary_section_offsets(&mut self) -> Result<()> {
        let records_offset = self.state.position();
        let row_len = u64::try_from(self.schema.row_len())
            .map_err(|_| data_section_overflow_error(records_offset))?;
        let data_section_size = self
            .header
            .observation_count()
            .checked_mul(row_len)
            .ok_or_else(|| data_section_overflow_error(records_offset))?;
        let value_labels_offset = records_offset
            .checked_add(data_section_size)
            .ok_or_else(|| data_section_overflow_error(records_offset))?;

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

impl<R: BufRead> CharacteristicReader<R> {
    /// Parses the payload of a characteristic entry: variable name,
    /// characteristic name, and value (all null-terminated, fixed-width
    /// name fields).
    fn parse_characteristic_payload(&mut self, total_length: u32) -> Result<Characteristic> {
        let encoding = self.state.encoding();
        let variable_name_len = self.header.release().variable_name_len();
        let entry_position = self.state.position();

        let variable_name = self.state.read_fixed_string(
            variable_name_len,
            encoding,
            Section::Characteristics,
            Field::VariableName,
        )?;
        let characteristic_name = self.state.read_fixed_string(
            variable_name_len,
            encoding,
            Section::Characteristics,
            Field::CharacteristicName,
        )?;

        let value_len = characteristic_value_len(total_length, variable_name_len, entry_position)?;
        let value = self.state.read_fixed_string(
            value_len,
            encoding,
            Section::Characteristics,
            Field::CharacteristicValue,
        )?;

        let target = CharacteristicTarget::from_variable_name(variable_name);
        let characteristic = Characteristic::new(target, characteristic_name, value);
        Ok(characteristic)
    }
}

// ---------------------------------------------------------------------------
// Seek-based navigation (BufRead + Seek)
// ---------------------------------------------------------------------------

impl<R: BufRead + Seek> CharacteristicReader<R> {
    /// Seeks to the start of the characteristics section.
    ///
    /// # Errors
    ///
    /// Returns [`DtaError::Io`] if the section offsets have not been
    /// initialized or if the seek fails.
    pub fn seek_characteristics(mut self) -> Result<Self> {
        let offset = self
            .state
            .section_offsets()
            .ok_or_else(|| DtaError::missing_section_offsets(Section::Characteristics))?
            .characteristics();
        self.state.seek_to(offset, Section::Characteristics)?;
        let reader = Self::new(self.state, self.header, self.schema);
        Ok(reader)
    }

    /// Seeks past characteristics and transitions to record reading.
    ///
    /// For binary formats, the data-section offset is computed on
    /// demand by skipping to the end of the characteristics section
    /// if it hasn't already been determined.
    ///
    /// # Errors
    ///
    /// Returns [`DtaError::Io`] if the section offsets have not been
    /// initialized or if the seek fails.
    pub fn seek_records(mut self) -> Result<RecordReader<R>> {
        self.ensure_post_characteristics_offsets()?;
        let offset = self
            .state
            .section_offsets()
            .ok_or_else(|| DtaError::missing_section_offsets(Section::Records))?
            .records();
        self.state.seek_to(offset, Section::Records)?;
        let reader = RecordReader::new(self.state, self.header, self.schema);
        Ok(reader)
    }

    /// Seeks to the value-label section.
    ///
    /// For binary formats, the value-label offset is computed on
    /// demand by skipping to the end of the characteristics section
    /// if it hasn't already been determined.
    ///
    /// # Errors
    ///
    /// Returns [`DtaError::Io`] if the section offsets have not been
    /// initialized or if the seek fails.
    pub fn seek_value_labels(mut self) -> Result<ValueLabelReader<R>> {
        self.ensure_post_characteristics_offsets()?;
        let offset = self
            .state
            .section_offsets()
            .ok_or_else(|| DtaError::missing_section_offsets(Section::ValueLabels))?
            .value_labels();
        self.state.seek_to(offset, Section::ValueLabels)?;
        let reader = ValueLabelReader::new(self.state, self.header, self.schema);
        Ok(reader)
    }

    /// Seeks to the long-string section.
    ///
    /// For formats that do not support long strings (pre-117),
    /// the returned reader immediately yields `None` from
    /// [`read_long_string`](LongStringReader::read_long_string).
    /// The data-section and value-label offsets are still computed
    /// so that the returned reader's `seek_records` /
    /// `seek_value_labels` work for binary formats.
    ///
    /// # Errors
    ///
    /// Returns [`DtaError::Io`] if the section offsets have not been
    /// initialized or if the seek fails.
    pub fn seek_long_strings(mut self) -> Result<LongStringReader<R>> {
        self.ensure_post_characteristics_offsets()?;
        let long_strings_offset = self
            .state
            .section_offsets()
            .ok_or_else(|| DtaError::missing_section_offsets(Section::LongStrings))?
            .long_strings();
        if let Some(offset) = long_strings_offset {
            self.state.seek_to(offset, Section::LongStrings)?;
        }
        let reader = LongStringReader::new(self.state, self.header, self.schema);
        Ok(reader)
    }

    /// Populates the data-section and value-label offsets for binary
    /// formats by consuming any unread characteristics, so subsequent
    /// seek-based navigation has correct positions. A no-op for XML
    /// formats (which learn all offsets from the `<map>` section at
    /// schema-read time) and for already-computed binary readers.
    fn ensure_post_characteristics_offsets(&mut self) -> Result<()> {
        if self.header.release().is_xml_like() {
            return Ok(());
        }
        // `records()` is 0 exactly when the offsets haven't been
        // computed yet — header/schema always occupy more than 0
        // bytes, so 0 is never a valid records offset.
        let offsets = self
            .state
            .section_offsets()
            .ok_or_else(|| DtaError::missing_section_offsets(Section::Records))?;
        if offsets.records() != 0 {
            return Ok(());
        }
        if !self.completed {
            self.skip_to_end()?;
        }
        self.compute_binary_section_offsets()
    }
}

// ===========================================================================
// Tests — forward-compat skipping of unknown expansion-field data_type bytes
// ===========================================================================

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
    /// real writer, then appends `expansion_fields` verbatim as the
    /// characteristics section. Lets us hand-craft expansion-field
    /// sequences the writer would never emit on its own (e.g.
    /// unknown `data_type` values reserved by the DTA spec).
    fn synthetic_v114_file(expansion_fields: &[u8]) -> Vec<u8> {
        let schema = Schema::builder().build().unwrap();
        let header = Header::builder(Release::V114, ByteOrder::LittleEndian).build();
        let characteristic_writer = DtaWriter::new()
            .from_writer(Cursor::new(Vec::<u8>::new()))
            .write_header(header)
            .unwrap()
            .write_schema(schema)
            .unwrap();
        let mut bytes = characteristic_writer.into_state().into_inner().into_inner();
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

    #[test]
    fn unknown_expansion_field_type_is_skipped() {
        // Layout: unknown `data_type=2` entry, then a real
        // characteristic, then the terminator. The unknown entry's
        // 7-byte payload should be skipped wholesale.
        let mut expansion_fields = Vec::new();
        expansion_fields.push(2u8);
        expansion_fields.extend_from_slice(&7u32.to_le_bytes());
        expansion_fields.extend_from_slice(&[0xAA; 7]);

        let payload = characteristic_payload("_dta", "note1", b"hi");
        expansion_fields.push(1u8);
        expansion_fields.extend_from_slice(&u32::try_from(payload.len()).unwrap().to_le_bytes());
        expansion_fields.extend_from_slice(&payload);

        // Terminator.
        expansion_fields.push(0u8);
        expansion_fields.extend_from_slice(&0u32.to_le_bytes());

        let bytes = synthetic_v114_file(&expansion_fields);
        let mut reader = DtaReader::new()
            .from_reader(Cursor::new(bytes))
            .read_header()
            .unwrap()
            .read_schema()
            .unwrap();

        let first = reader.read_characteristic().unwrap().unwrap();
        assert_eq!(first.target(), &CharacteristicTarget::Dataset);
        assert_eq!(first.name(), "note1");
        assert_eq!(first.value(), "hi");

        assert!(reader.read_characteristic().unwrap().is_none());
    }

    #[test]
    fn multiple_unknown_expansion_fields_between_real_entries() {
        // Two unknowns framing one real characteristic — exercises
        // the read-loop across more than a single skip.
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

        let bytes = synthetic_v114_file(&expansion_fields);
        let mut reader = DtaReader::new()
            .from_reader(Cursor::new(bytes))
            .read_header()
            .unwrap()
            .read_schema()
            .unwrap();

        let first = reader.read_characteristic().unwrap().unwrap();
        assert_eq!(first.name(), "middle");
        assert_eq!(first.value(), "real");
        assert!(reader.read_characteristic().unwrap().is_none());
    }

    #[test]
    fn skip_to_end_tolerates_unknown_expansion_field_type() {
        let mut expansion_fields = Vec::new();
        expansion_fields.push(3u8);
        expansion_fields.extend_from_slice(&4u32.to_le_bytes());
        expansion_fields.extend_from_slice(&[0xDD; 4]);
        expansion_fields.push(0u8);
        expansion_fields.extend_from_slice(&0u32.to_le_bytes());

        let bytes = synthetic_v114_file(&expansion_fields);
        let mut reader = DtaReader::new()
            .from_reader(Cursor::new(bytes))
            .read_header()
            .unwrap()
            .read_schema()
            .unwrap();
        reader.skip_to_end().unwrap();
    }
}
