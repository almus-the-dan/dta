use std::io::{BufRead, Seek};

use super::characteristic::{Characteristic, CharacteristicTarget};
use super::dta_error::{DtaError, Field, FormatErrorKind, Result, Section};
use super::header::Header;
use super::long_string_reader::LongStringReader;
use super::reader_state::ReaderState;
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
    /// fully consumed or if section offsets are missing.
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

        Ok(RecordReader::new(self.state, self.header, self.schema))
    }
}

// ---------------------------------------------------------------------------
// XML internals (format 117+)
// ---------------------------------------------------------------------------

/// Disambiguated XML tag at the entry-start position within the
/// characteristics section.
enum XmlCharacteristicTag {
    /// Opening `<characteristics>` section tag.
    SectionOpen,
    /// Closing `</characteristics>` section tag.
    SectionClose,
    /// Opening `<ch>` entry tag.
    EntryOpen,
}

impl<R: BufRead> CharacteristicReader<R> {
    /// Reads the next XML-level tag in the characteristics section.
    ///
    /// The first four bytes distinguish all three possibilities:
    /// - `<cha` → `<characteristics>` (a section open, consumes rest)
    /// - `</ch` → `</characteristics>` (a section close, consumes rest)
    /// - `<ch>` → `<ch>` (an entry open, complete in 4 bytes)
    fn read_xml_tag(&mut self) -> Result<XmlCharacteristicTag> {
        let position = self.state.position();
        let tag_bytes = self.state.read_exact(4, Section::Characteristics)?;

        match tag_bytes {
            b"<cha" => {
                // <characteristics> — consume remaining "racteristics>"
                self.state.expect_bytes(
                    b"racteristics>",
                    Section::Characteristics,
                    FormatErrorKind::InvalidMagic,
                )?;
                Ok(XmlCharacteristicTag::SectionOpen)
            }
            b"</ch" => {
                // </characteristics> — consume remaining "aracteristics>"
                self.state.expect_bytes(
                    b"aracteristics>",
                    Section::Characteristics,
                    FormatErrorKind::InvalidMagic,
                )?;
                Ok(XmlCharacteristicTag::SectionClose)
            }
            b"<ch>" => Ok(XmlCharacteristicTag::EntryOpen),
            _ => Err(DtaError::format(
                Section::Characteristics,
                position,
                FormatErrorKind::InvalidMagic,
            )),
        }
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
        let byte_order = self.header.byte_order();
        loop {
            match self.read_xml_tag()? {
                XmlCharacteristicTag::SectionOpen => {}
                XmlCharacteristicTag::SectionClose => {
                    self.completed = true;
                    return Ok(());
                }
                XmlCharacteristicTag::EntryOpen => {
                    let length = self.state.read_u32(byte_order, Section::Characteristics)?;
                    let length = usize::try_from(length).map_err(|_| {
                        DtaError::io(
                            Section::Characteristics,
                            std::io::Error::other("characteristic length exceeds usize"),
                        )
                    })?;
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
    fn read_binary_characteristic(&mut self) -> Result<Option<Characteristic>> {
        let expansion_len_width = self.header.release().expansion_len_width();
        if expansion_len_width == 0 {
            // V104: no expansion fields.
            self.completed = true;
            return Ok(None);
        }
        let is_extended = expansion_len_width == 4;
        let (data_type, length) = self.read_binary_entry_header(is_extended)?;
        if data_type == 0 && length == 0 {
            self.completed = true;
            return Ok(None);
        }
        let characteristic = self.parse_characteristic_payload(length)?;
        Ok(Some(characteristic))
    }

    /// Skips all remaining binary expansion-field entries.
    fn skip_binary_characteristics(&mut self) -> Result<()> {
        let expansion_len_width = self.header.release().expansion_len_width();
        if expansion_len_width == 0 {
            // V104: no expansion fields.
            self.completed = true;
            return Ok(());
        }
        let is_extended = expansion_len_width == 4;
        loop {
            let (data_type, length) = self.read_binary_entry_header(is_extended)?;
            if data_type == 0 && length == 0 {
                self.completed = true;
                return Ok(());
            }
            let length_usize = usize::try_from(length).map_err(|_| {
                DtaError::io(
                    Section::Characteristics,
                    std::io::Error::other("characteristic length exceeds usize"),
                )
            })?;
            self.state.skip(length_usize, Section::Characteristics)?;
        }
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

        let total_length_usize = usize::try_from(total_length).map_err(|_| {
            DtaError::io(
                Section::Characteristics,
                std::io::Error::other("characteristic length exceeds usize"),
            )
        })?;
        let two_names_len = 2 * variable_name_len;
        let value_len = total_length_usize
            .checked_sub(two_names_len)
            .ok_or_else(|| {
                DtaError::format(
                    Section::Characteristics,
                    entry_position,
                    FormatErrorKind::Truncated {
                        expected: u64::try_from(two_names_len).unwrap_or(u64::MAX),
                        actual: u64::from(total_length),
                    },
                )
            })?;

        let value = self.state.read_fixed_string(
            value_len,
            encoding,
            Section::Characteristics,
            Field::CharacteristicValue,
        )?;

        let target = CharacteristicTarget::from_variable_name(variable_name);
        Ok(Characteristic::new(target, characteristic_name, value))
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
        Ok(Self::new(self.state, self.header, self.schema))
    }

    /// Seeks past characteristics and transitions to record reading.
    ///
    /// For binary formats (where [`Release::is_xml_like`](super::release::Release::is_xml_like) returns
    /// `false`), the data-section offset is not known until
    /// characteristics have been read. Calling this before reading
    /// characteristics for a binary format will seek to an incorrect
    /// position.
    ///
    /// # Errors
    ///
    /// Returns [`DtaError::Io`] if the section offsets have not been
    /// initialized or if the seek fails.
    pub fn seek_records(mut self) -> Result<RecordReader<R>> {
        let offset = self
            .state
            .section_offsets()
            .ok_or_else(|| DtaError::missing_section_offsets(Section::Records))?
            .records();
        self.state.seek_to(offset, Section::Records)?;
        Ok(RecordReader::new(self.state, self.header, self.schema))
    }

    /// Seeks to the value-label section.
    ///
    /// For binary formats (where [`Release::is_xml_like`](super::release::Release::is_xml_like) returns
    /// `false`), the value-label offset is not known until
    /// characteristics have been read. Calling this before reading
    /// characteristics for a binary format will seek to an incorrect
    /// position.
    ///
    /// # Errors
    ///
    /// Returns [`DtaError::Io`] if the section offsets have not been
    /// initialized or if the seek fails.
    pub fn seek_value_labels(mut self) -> Result<ValueLabelReader<R>> {
        let offset = self
            .state
            .section_offsets()
            .ok_or_else(|| DtaError::missing_section_offsets(Section::ValueLabels))?
            .value_labels();
        self.state.seek_to(offset, Section::ValueLabels)?;
        Ok(ValueLabelReader::new(self.state, self.header, self.schema))
    }

    /// Seeks to the long-string section.
    ///
    /// Returns `None` if the format does not have a long-string
    /// section. Because this method consumes `self`, check
    /// [`Release::supports_long_strings`](super::release::Release::supports_long_strings) beforehand to avoid losing
    /// access to the reader.
    ///
    /// For binary formats (where [`Release::is_xml_like`](super::release::Release::is_xml_like) returns
    /// `false`), the long-strings section does not exist, so this
    /// always returns `Ok(None)`.
    ///
    /// # Errors
    ///
    /// Returns [`DtaError::Io`] if the section offsets have not been
    /// initialized or if the seek fails.
    pub fn seek_long_strings(mut self) -> Result<Option<LongStringReader<R>>> {
        let long_strings_offset = self
            .state
            .section_offsets()
            .ok_or_else(|| DtaError::missing_section_offsets(Section::LongStrings))?
            .long_strings();
        match long_strings_offset {
            Some(offset) => {
                self.state.seek_to(offset, Section::LongStrings)?;
                Ok(Some(LongStringReader::new(
                    self.state,
                    self.header,
                    self.schema,
                )))
            }
            None => Ok(None),
        }
    }
}
