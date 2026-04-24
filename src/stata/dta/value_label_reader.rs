use std::io::{BufRead, Seek};

use super::characteristic_reader::CharacteristicReader;
use super::dta_error::{DtaError, Field, FormatErrorKind, Result, Section};
use super::header::Header;
use super::long_string_reader::LongStringReader;
use super::reader_state::ReaderState;
use super::record_reader::RecordReader;
use super::schema::Schema;
use super::value_label::ValueLabelSet;
use super::value_label_parse::{
    OLD_VALUE_LABEL_SIZE, VALUE_LABELS_CLOSE_REST, XmlLabelTag, classify_xml_label_tag,
    overflow_error, parse_modern_payload, parse_old_payload,
};
use super::value_label_table::ValueLabelTable;

/// Reads value-label sets from a DTA file.
///
/// Owns the parsed [`Header`] and [`Schema`] from previous
/// phases. Yields [`ValueLabelSet`] entries via iteration, then
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
    /// Reads the next value-label set.
    ///
    /// Returns `None` when all sets have been consumed. Each set
    /// contains a name and integer-to-string label mappings.
    ///
    /// # Errors
    ///
    /// Returns [`DtaError::Io`] on read failures and
    /// [`DtaError::Format`] when the set bytes violate the DTA
    /// format specification.
    pub fn read_value_label_set(&mut self) -> Result<Option<ValueLabelSet>> {
        if self.completed {
            return Ok(None);
        }
        if self.header.release().has_old_value_labels() {
            self.read_old_set()
        } else {
            self.read_modern_set()
        }
    }

    /// Reads all remaining value-label sets into `table`, keyed by
    /// set name.
    ///
    /// Sets are inserted with first-wins semantics: if `table` already contains
    /// a set for a given name, it is left untouched and the duplicate
    /// from the file is discarded.
    ///
    /// This method drains the reader to completion — after it
    /// returns, `self` is ready for section navigation or to be
    /// dropped.
    ///
    /// Pairs naturally with [`ValueLabelTable::label_for`] for looking
    /// up labels from record values.
    ///
    /// # Errors
    ///
    /// Returns [`DtaError::Io`] on read failures and
    /// [`DtaError::Format`] when the set bytes violate the DTA format
    /// specification.
    pub fn read_remaining_into(&mut self, table: &mut ValueLabelTable) -> Result<()> {
        while let Some(set) = self.read_value_label_set()? {
            if table.get(set.name()).is_none() {
                table.insert(set);
            }
        }
        Ok(())
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
            while self.skip_old_set()? {}
        } else {
            while self.skip_modern_set()? {}
        }
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Shared helpers
// ---------------------------------------------------------------------------

impl<R: BufRead> ValueLabelReader<R> {
    /// Reads the set name and skips the trailing padding bytes.
    fn read_set_name(&mut self) -> Result<String> {
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

    /// Skips the set name and trailing padding bytes without
    /// decoding.
    fn skip_set_name(&mut self) -> Result<()> {
        let release = self.header.release();
        let skip_len = release.value_label_name_len() + release.value_label_table_padding_len();
        self.state.skip(skip_len, Section::ValueLabels)
    }
}

// ---------------------------------------------------------------------------
// Old value labels (format 104-107)
// ---------------------------------------------------------------------------
//
// Pre-V108 sets have the layout:
//   u16 n          — entry count
//   char[9] name
//   byte pad
//   u16[n] values  — little-/big-endian per the file's byte order
//   char[8][n]     — fixed-width, null-padded labels
//
// Values round-trip through `i16` so negative codings survive.

impl<R: BufRead> ValueLabelReader<R> {
    /// Reads and parses one set in the old (V104-V107) layout.
    fn read_old_set(&mut self) -> Result<Option<ValueLabelSet>> {
        let Some(entry_count) = self.read_old_entry_count()? else {
            return Ok(None);
        };

        let name = self.read_set_name()?;
        let byte_order = self.header.byte_order();
        let encoding = self.state.encoding();

        let payload_len = old_payload_len(entry_count)?;
        let payload = self.state.read_exact(payload_len, Section::ValueLabels)?;
        let set = parse_old_payload(payload, byte_order, encoding, &name)?;
        Ok(Some(set))
    }

    /// Reads the leading `u16` entry count, or returns `None` at a
    /// clean EOF.
    fn read_old_entry_count(&mut self) -> Result<Option<usize>> {
        let byte_order = self.header.byte_order();
        let Some(entry_count) = self.state.try_read_u16(byte_order, Section::ValueLabels)? else {
            self.completed = true;
            return Ok(None);
        };
        Ok(Some(usize::from(entry_count)))
    }

    /// Skips one old-format set. Returns `false` at EOF.
    fn skip_old_set(&mut self) -> Result<bool> {
        let Some(entry_count) = self.read_old_entry_count()? else {
            return Ok(false);
        };
        self.skip_set_name()?;
        let payload_len = old_payload_len(entry_count)?;
        self.state.skip(payload_len, Section::ValueLabels)?;
        Ok(true)
    }
}

/// Computes the byte length of a pre-V108 payload: `entry_count × (2
/// value bytes + 8 label bytes)`. Overflow escalates to the shared
/// value-label overflow error — unreachable on 64-bit platforms but
/// real on 16-bit targets.
fn old_payload_len(entry_count: usize) -> Result<usize> {
    entry_count
        .checked_mul(2 + OLD_VALUE_LABEL_SIZE)
        .ok_or_else(overflow_error)
}

// ---------------------------------------------------------------------------
// Modern value labels (format 105+)
// ---------------------------------------------------------------------------

impl<R: BufRead> ValueLabelReader<R> {
    /// Reads and parses one set in the modern (105+) layout.
    fn read_modern_set(&mut self) -> Result<Option<ValueLabelSet>> {
        let Some(set_len) = self.read_modern_set_header()? else {
            return Ok(None);
        };

        let name = self.read_set_name()?;
        let byte_order = self.header.byte_order();
        let encoding = self.state.encoding();

        let payload = self.state.read_exact(set_len, Section::ValueLabels)?;
        let set = parse_modern_payload(payload, byte_order, encoding, &name)?;

        self.read_modern_set_footer()?;
        Ok(Some(set))
    }

    /// Reads the modern-format table header (XML tags, table length,
    /// name, padding). Returns the payload size in bytes, or `None`
    /// when the section is exhausted.
    fn read_modern_set_header(&mut self) -> Result<Option<usize>> {
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

        let Some(set_len) = self.state.try_read_u32(byte_order, Section::ValueLabels)? else {
            self.completed = true;
            return Ok(None);
        };
        let set_len = usize::try_from(set_len).map_err(|_| overflow_error())?;
        Ok(Some(set_len))
    }

    /// Reads the closing `</lbl>` tag if this is an XML format.
    fn read_modern_set_footer(&mut self) -> Result<()> {
        if self.header.release().is_xml_like() {
            self.state.expect_bytes(
                b"</lbl>",
                Section::ValueLabels,
                FormatErrorKind::InvalidMagic,
            )?;
        }
        Ok(())
    }

    /// Skips one modern-format set. Returns `false` when the section
    /// is exhausted.
    fn skip_modern_set(&mut self) -> Result<bool> {
        let Some(set_len) = self.read_modern_set_header()? else {
            return Ok(false);
        };
        self.skip_set_name()?;
        self.state.skip(set_len, Section::ValueLabels)?;
        self.read_modern_set_footer()?;
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

#[cfg(test)]
mod tests {
    use std::io::Cursor;

    use super::*;
    use crate::stata::dta::byte_order::ByteOrder;
    use crate::stata::dta::dta_reader::DtaReader;
    use crate::stata::dta::dta_writer::DtaWriter;
    use crate::stata::dta::release::Release;
    use crate::stata::dta::value_label::ValueLabelEntry;
    use crate::stata::dta::variable::Variable;
    use crate::stata::dta::variable_type::VariableType;

    fn entries(pairs: &[(i32, &str)]) -> Vec<ValueLabelEntry> {
        pairs
            .iter()
            .map(|&(v, l)| ValueLabelEntry::new(v, l.to_owned()))
            .collect()
    }

    fn build_file_with_sets(release: Release, sets: &[ValueLabelSet]) -> Vec<u8> {
        let schema = Schema::builder()
            .add_variable(Variable::builder(VariableType::Byte, "x").format("%8.0g"))
            .build()
            .unwrap();
        let header = Header::builder(release, ByteOrder::LittleEndian).build();
        let mut value_label_writer = DtaWriter::new()
            .from_writer(Cursor::new(Vec::<u8>::new()))
            .write_header(header)
            .unwrap()
            .write_schema(schema)
            .unwrap()
            .into_record_writer()
            .unwrap()
            .into_long_string_writer()
            .unwrap()
            .into_value_label_writer()
            .unwrap();
        for set in sets {
            value_label_writer.write_value_label_set(set).unwrap();
        }
        value_label_writer.finish().unwrap().into_inner()
    }

    fn value_label_reader_for(bytes: Vec<u8>) -> ValueLabelReader<Cursor<Vec<u8>>> {
        let mut characteristic_reader = DtaReader::new()
            .from_reader(Cursor::new(bytes))
            .read_header()
            .unwrap()
            .read_schema()
            .unwrap();
        characteristic_reader.skip_to_end().unwrap();
        let mut record_reader = characteristic_reader.into_record_reader().unwrap();
        record_reader.skip_to_end().unwrap();
        let mut long_string_reader = record_reader.into_long_string_reader().unwrap();
        long_string_reader.skip_to_end().unwrap();
        long_string_reader.into_value_label_reader().unwrap()
    }

    #[test]
    fn read_remaining_into_populates_table() {
        let bytes = build_file_with_sets(
            Release::V117,
            &[
                ValueLabelSet::new("a".to_owned(), entries(&[(0, "zero"), (1, "one")])),
                ValueLabelSet::new("b".to_owned(), entries(&[(-1, "neg")])),
            ],
        );
        let mut reader = value_label_reader_for(bytes);

        let mut table = ValueLabelTable::new();
        reader.read_remaining_into(&mut table).unwrap();

        assert_eq!(table.len(), 2);
        assert_eq!(table.get("a").unwrap().label_for(0), Some("zero"));
        assert_eq!(table.get("a").unwrap().label_for(1), Some("one"));
        assert_eq!(table.get("b").unwrap().label_for(-1), Some("neg"));
    }

    #[test]
    fn read_remaining_into_works_on_old_format() {
        let bytes = build_file_with_sets(
            Release::V104,
            &[ValueLabelSet::new(
                "old".to_owned(),
                entries(&[(0, "a"), (1, "b")]),
            )],
        );
        let mut reader = value_label_reader_for(bytes);

        let mut table = ValueLabelTable::new();
        reader.read_remaining_into(&mut table).unwrap();

        assert_eq!(table.len(), 1);
        assert_eq!(table.get("old").unwrap().label_for(0), Some("a"));
    }

    #[test]
    fn read_remaining_into_is_noop_when_no_sets() {
        let bytes = build_file_with_sets(Release::V117, &[]);
        let mut reader = value_label_reader_for(bytes);

        let mut table = ValueLabelTable::new();
        reader.read_remaining_into(&mut table).unwrap();
        assert!(table.is_empty());
    }

    #[test]
    fn read_remaining_into_first_wins_over_pre_existing_entries() {
        let bytes = build_file_with_sets(
            Release::V117,
            &[ValueLabelSet::new(
                "shared".to_owned(),
                entries(&[(1, "from file")]),
            )],
        );
        let mut reader = value_label_reader_for(bytes);

        let mut table = ValueLabelTable::new();
        table.insert(ValueLabelSet::new(
            "shared".to_owned(),
            entries(&[(1, "pre-existing")]),
        ));
        reader.read_remaining_into(&mut table).unwrap();

        assert_eq!(table.len(), 1);
        assert_eq!(
            table.get("shared").unwrap().label_for(1),
            Some("pre-existing")
        );
    }

    #[test]
    fn read_remaining_into_after_partial_consumption() {
        let bytes = build_file_with_sets(
            Release::V117,
            &[
                ValueLabelSet::new("a".to_owned(), entries(&[(0, "first")])),
                ValueLabelSet::new("b".to_owned(), entries(&[(0, "second")])),
            ],
        );
        let mut reader = value_label_reader_for(bytes);

        let first = reader.read_value_label_set().unwrap().unwrap();
        assert_eq!(first.name(), "a");

        let mut table = ValueLabelTable::new();
        reader.read_remaining_into(&mut table).unwrap();
        assert_eq!(table.len(), 1);
        assert_eq!(table.get("b").unwrap().label_for(0), Some("second"));
    }
}
