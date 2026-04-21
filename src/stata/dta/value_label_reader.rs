use std::io::{BufRead, Seek};

use super::characteristic_reader::CharacteristicReader;
use super::dta_error::{DtaError, Field, FormatErrorKind, Result, Section};
use super::header::Header;
use super::long_string_reader::LongStringReader;
use super::reader_state::ReaderState;
use super::record_reader::RecordReader;
use super::schema::Schema;
use super::value_label::{ValueLabelEntry, ValueLabelTable};
use super::value_label_parse::{
    VALUE_LABELS_CLOSE_REST, XmlLabelTag, classify_xml_label_tag, decode_label, entry_index_to_i32,
    overflow_error, parse_modern_payload,
};

/// Reads value-label tables from a DTA file.
///
/// Owns the parsed [`Header`] and [`Schema`] from previous
/// phases. Yields [`ValueLabelTable`] entries via iteration, then
/// optionally transitions to long-string reading.
#[derive(Debug)]
pub struct ValueLabelReader<R> {
    state: ReaderState<R>,
    header: Header,
    schema: Schema,
    opened: bool,
    completed: bool,
}

impl<R> ValueLabelReader<R> {
    #[must_use]
    pub(crate) fn new(state: ReaderState<R>, header: Header, schema: Schema) -> Self {
        Self {
            state,
            header,
            schema,
            opened: false,
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

impl<R: BufRead> ValueLabelReader<R> {
    /// Reads the next value-label table.
    ///
    /// Returns `None` when all tables have been consumed. Each table
    /// contains a name and a set of integer-to-string mappings.
    ///
    /// # Errors
    ///
    /// Returns [`DtaError::Io`] on read failures and
    /// [`DtaError::Format`] when the table bytes violate the DTA
    /// format specification.
    pub fn read_value_label_table(&mut self) -> Result<Option<ValueLabelTable>> {
        if self.completed {
            return Ok(None);
        }
        if self.header.release().has_old_value_labels() {
            self.read_old_table()
        } else {
            self.read_modern_table()
        }
    }

    /// Skips all remaining value-label entries without processing
    /// them.
    ///
    /// # Errors
    ///
    /// Returns [`DtaError::Io`] on read failures and
    /// [`DtaError::Format`] if section tags (XML formats) are
    /// missing or malformed.
    pub fn skip_to_end(&mut self) -> Result<()> {
        if self.completed {
            return Ok(());
        }
        if self.header.release().has_old_value_labels() {
            while self.skip_old_table()? {}
        } else {
            while self.skip_modern_table()? {}
        }
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Shared helpers
// ---------------------------------------------------------------------------

impl<R: BufRead> ValueLabelReader<R> {
    /// Reads the table name and skips the trailing padding bytes.
    fn read_table_name(&mut self) -> Result<String> {
        let release = self.header.release();
        let encoding = self.state.encoding();
        let name = self.state.read_fixed_string(
            release.value_label_name_len(),
            encoding,
            Section::ValueLabels,
            Field::ValueLabelName,
        )?;
        self.state.skip(
            release.value_label_table_padding_len(),
            Section::ValueLabels,
        )?;
        Ok(name)
    }

    /// Skips the table name and trailing padding bytes without
    /// decoding.
    fn skip_table_name(&mut self) -> Result<()> {
        let release = self.header.release();
        let skip_len = release.value_label_name_len() + release.value_label_table_padding_len();
        self.state.skip(skip_len, Section::ValueLabels)
    }
}

// ---------------------------------------------------------------------------
// Old value labels (format 104)
// ---------------------------------------------------------------------------

impl<R: BufRead> ValueLabelReader<R> {
    /// Reads and parses one table in the old (pre-105) layout.
    fn read_old_table(&mut self) -> Result<Option<ValueLabelTable>> {
        let Some(table_len) = self.read_old_table_header()? else {
            return Ok(None);
        };

        let name = self.read_table_name()?;
        let encoding = self.state.encoding();

        let entry_count = table_len / 8;
        let payload = self.state.read_exact(table_len, Section::ValueLabels)?;

        let mut entries = Vec::with_capacity(entry_count);
        for entry_index in 0..entry_count {
            let label_bytes = &payload[8 * entry_index..8 * entry_index + 8];
            if label_bytes[0] == 0 {
                continue;
            }
            let label = decode_label(label_bytes, 8, encoding)?;
            let value = entry_index_to_i32(entry_index)?;
            let entry = ValueLabelEntry::new(value, label);
            entries.push(entry);
        }

        let table = ValueLabelTable::new(name, entries);
        Ok(Some(table))
    }

    /// Reads the old-format table header (table length, name, padding).
    /// Returns the payload size in bytes, or `None` at EOF.
    fn read_old_table_header(&mut self) -> Result<Option<usize>> {
        let byte_order = self.header.byte_order();
        let Some(table_len) = self.state.try_read_u16(byte_order, Section::ValueLabels)? else {
            self.completed = true;
            return Ok(None);
        };
        let table_len = usize::from(table_len);
        Ok(Some(table_len))
    }

    /// Skips one old-format table. Returns `false` at EOF.
    fn skip_old_table(&mut self) -> Result<bool> {
        let Some(table_len) = self.read_old_table_header()? else {
            return Ok(false);
        };
        self.skip_table_name()?;
        self.state.skip(table_len, Section::ValueLabels)?;
        Ok(true)
    }
}

// ---------------------------------------------------------------------------
// Modern value labels (format 105+)
// ---------------------------------------------------------------------------

impl<R: BufRead> ValueLabelReader<R> {
    /// Reads and parses one table in the modern (105+) layout.
    fn read_modern_table(&mut self) -> Result<Option<ValueLabelTable>> {
        let Some(table_len) = self.read_modern_table_header()? else {
            return Ok(None);
        };

        let name = self.read_table_name()?;
        let byte_order = self.header.byte_order();
        let encoding = self.state.encoding();

        let payload = self.state.read_exact(table_len, Section::ValueLabels)?;
        let table = parse_modern_payload(payload, byte_order, encoding, &name)?;

        self.read_modern_table_footer()?;
        Ok(Some(table))
    }

    /// Reads the modern-format table header (XML tags, table length,
    /// name, padding). Returns the payload size in bytes, or `None`
    /// when the section is exhausted.
    fn read_modern_table_header(&mut self) -> Result<Option<usize>> {
        let byte_order = self.header.byte_order();
        let is_xml = self.header.release().is_xml_like();

        if !self.opened && is_xml {
            self.state.expect_bytes(
                b"<value_labels>",
                Section::ValueLabels,
                FormatErrorKind::InvalidMagic,
            )?;
            self.opened = true;
        }

        if is_xml && let XmlLabelTag::SectionClose = self.read_xml_label_or_close()? {
            self.completed = true;
            return Ok(None);
        }

        let Some(table_len) = self.state.try_read_u32(byte_order, Section::ValueLabels)? else {
            self.completed = true;
            return Ok(None);
        };
        let table_len = usize::try_from(table_len).map_err(|_| overflow_error())?;
        Ok(Some(table_len))
    }

    /// Reads the closing `</lbl>` tag if this is an XML format.
    fn read_modern_table_footer(&mut self) -> Result<()> {
        if self.header.release().is_xml_like() {
            self.state.expect_bytes(
                b"</lbl>",
                Section::ValueLabels,
                FormatErrorKind::InvalidMagic,
            )?;
        }
        Ok(())
    }

    /// Skips one modern-format table. Returns `false` when the section
    /// is exhausted.
    fn skip_modern_table(&mut self) -> Result<bool> {
        let Some(table_len) = self.read_modern_table_header()? else {
            return Ok(false);
        };
        self.skip_table_name()?;
        self.state.skip(table_len, Section::ValueLabels)?;
        self.read_modern_table_footer()?;
        Ok(true)
    }

    /// Reads the next XML tag in the value-labels section,
    /// distinguishing `<lbl>` from `</value_labels>`.
    fn read_xml_label_or_close(&mut self) -> Result<XmlLabelTag> {
        let position = self.state.position();
        let head = self.state.read_exact(5, Section::ValueLabels)?;
        let tag = classify_xml_label_tag(head).ok_or_else(|| {
            DtaError::format(
                Section::ValueLabels,
                position,
                FormatErrorKind::InvalidMagic,
            )
        })?;
        if let XmlLabelTag::SectionClose = tag {
            self.state.expect_bytes(
                VALUE_LABELS_CLOSE_REST,
                Section::ValueLabels,
                FormatErrorKind::InvalidMagic,
            )?;
        }
        Ok(tag)
    }
}

// ---------------------------------------------------------------------------
// Seek-based navigation (BufRead + Seek)
// ---------------------------------------------------------------------------

impl<R: BufRead + Seek> ValueLabelReader<R> {
    /// Seeks to the characteristics section.
    ///
    /// # Errors
    ///
    /// Returns [`DtaError::Io`] if the section offsets have not been
    /// initialized or if the seek fails.
    pub fn seek_characteristics(mut self) -> Result<CharacteristicReader<R>> {
        let offset = self
            .state
            .section_offsets()
            .ok_or_else(|| DtaError::missing_section_offsets(Section::Characteristics))?
            .characteristics();
        self.state.seek_to(offset, Section::Characteristics)?;
        let reader = CharacteristicReader::new(self.state, self.header, self.schema);
        Ok(reader)
    }

    /// Seeks to the data section.
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
        let reader = RecordReader::new(self.state, self.header, self.schema);
        Ok(reader)
    }

    /// Seeks to the start of the value-labels section.
    ///
    /// # Errors
    ///
    /// Returns [`DtaError::Io`] if the section offsets have not been
    /// initialized or if the seek fails.
    pub fn seek_value_labels(mut self) -> Result<Self> {
        let offset = self
            .state
            .section_offsets()
            .ok_or_else(|| DtaError::missing_section_offsets(Section::ValueLabels))?
            .value_labels();
        self.state.seek_to(offset, Section::ValueLabels)?;
        let reader = Self::new(self.state, self.header, self.schema);
        Ok(reader)
    }

    /// Seeks to the long-string section.
    ///
    /// For formats that do not support long strings (pre-117),
    /// the returned reader immediately yields `None` from
    /// [`read_long_string`](LongStringReader::read_long_string).
    ///
    /// # Errors
    ///
    /// Returns [`DtaError::Io`] if the section offsets have not been
    /// initialized or if the seek fails.
    pub fn seek_long_strings(mut self) -> Result<LongStringReader<R>> {
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
}
