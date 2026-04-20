use std::io::{Seek, Write};

use super::dta_error::{DtaError, Field, FormatErrorKind, Result, Section};
use super::header::Header;
use super::schema::Schema;
use super::value_label::ValueLabelTable;
use super::value_label_format::{build_modern_text_payload, build_old_slot_table};
use super::writer_state::WriterState;

/// Writes value-label tables — the last section of a DTA file.
///
/// Call [`write_value_label_table`](Self::write_value_label_table)
/// once per table, then [`finish`](Self::finish) to close the
/// section (XML formats only), flush the sink, and recover the
/// underlying writer.
#[derive(Debug)]
pub struct ValueLabelWriter<W> {
    state: WriterState<W>,
    header: Header,
    schema: Schema,
    /// Tracks whether the XML `<value_labels>` opening tag has been
    /// emitted. Unused (but harmless) for pre-117 formats, which
    /// have no section tag.
    opened: bool,
}

impl<W> ValueLabelWriter<W> {
    #[must_use]
    pub(crate) fn new(state: WriterState<W>, header: Header, schema: Schema) -> Self {
        Self {
            state,
            header,
            schema,
            opened: false,
        }
    }

    /// The header emitted during the header phase.
    #[must_use]
    #[inline]
    pub fn header(&self) -> &Header {
        &self.header
    }

    /// The schema emitted during the schema phase.
    #[must_use]
    #[inline]
    pub fn schema(&self) -> &Schema {
        &self.schema
    }
}

impl<W: Write + Seek> ValueLabelWriter<W> {
    /// Writes a single value-label table.
    ///
    /// Can be called any number of times (including zero).
    ///
    /// # Errors
    ///
    /// Returns [`DtaError::Format`](DtaError::Format) if the table
    /// cannot be represented — a name or label exceeding its field
    /// width, a value outside the range supported by the release's
    /// layout, or text that cannot be encoded in the active
    /// encoding. Returns [`DtaError::Io`](DtaError::Io) on sink
    /// failures.
    pub fn write_value_label_table(&mut self, table: &ValueLabelTable) -> Result<()> {
        if self.header.release().is_xml_like() {
            self.open_section_if_needed()?;
        }
        if self.header.release().has_old_value_labels() {
            self.write_old_table(table)
        } else {
            self.write_modern_table(table)
        }
    }

    /// Closes the value-labels section (XML only), emits the final
    /// `</stata_dta>` tag (XML only), patches the end-of-file map
    /// slots (XML only), flushes the sink, and returns it.
    ///
    /// The returned writer is finalized — the DTA file is complete.
    /// Writing more bytes to it would corrupt the file.
    ///
    /// # Errors
    ///
    /// Returns [`DtaError::Io`](DtaError::Io) on sink failures while
    /// writing the closing tags or flushing.
    pub fn finish(mut self) -> Result<W> {
        let release = self.header.release();
        let byte_order = self.header.byte_order();

        if release.is_xml_like() {
            self.open_section_if_needed()?;
            self.state
                .write_exact(b"</value_labels>", Section::ValueLabels)?;

            let stata_dta_close_offset = self.state.position();
            self.state
                .write_exact(b"</stata_dta>", Section::ValueLabels)?;
            let eof_offset = self.state.position();

            self.state.patch_map_entry(
                12,
                stata_dta_close_offset,
                byte_order,
                Section::ValueLabels,
            )?;
            self.state
                .patch_map_entry(13, eof_offset, byte_order, Section::ValueLabels)?;
        }

        let mut writer = self.state.into_inner();
        writer
            .flush()
            .map_err(|e| DtaError::io(Section::ValueLabels, e))?;
        Ok(writer)
    }

    /// Emits the XML `<value_labels>` tag on first use. No-op for
    /// binary formats (which have no section tag) and on later
    /// calls.
    fn open_section_if_needed(&mut self) -> Result<()> {
        if !self.opened {
            self.state
                .write_exact(b"<value_labels>", Section::ValueLabels)?;
            self.opened = true;
        }
        Ok(())
    }
}

impl<W: Write + Seek> ValueLabelWriter<W> {
    /// Writes one table in the V104 legacy layout:
    ///
    /// - `table_len`: `u16` = `slot_count * 8`
    /// - `name`: fixed-width + 2-byte padding
    /// - `payload`: `slot_count × 8` bytes, one slot per index.
    ///
    /// Each slot holds a null-padded label (8 bytes max). Empty slots
    /// indicate no entry for that index. The caller's values must
    /// therefore be non-negative and ≤ `u16::MAX / 8` (= 8190);
    /// duplicate values are rejected.
    fn write_old_table(&mut self, table: &ValueLabelTable) -> Result<()> {
        let release = self.header.release();
        let byte_order = self.header.byte_order();
        let position_before = self.state.position();

        let slots = build_old_slot_table(table, self.state.encoding(), position_before)?;
        let slot_count = slots.len();
        let table_len_u16 = slot_count
            .checked_mul(8)
            .and_then(|n| u16::try_from(n).ok())
            .ok_or_else(|| {
                // `actual` is only for error display — saturate at u64::MAX
                // so we report a useful number even if `slot_count * 8`
                // overflows `usize` on a 16-bit target.
                let actual = u64::try_from(slot_count)
                    .unwrap_or(u64::MAX)
                    .saturating_mul(8);
                DtaError::format(
                    Section::ValueLabels,
                    position_before,
                    FormatErrorKind::FieldTooLarge {
                        field: Field::ValueLabelEntry,
                        max: u64::from(u16::MAX),
                        actual,
                    },
                )
            })?;

        self.state
            .write_u16(table_len_u16, byte_order, Section::ValueLabels)?;
        self.state.write_fixed_string(
            table.name(),
            release.value_label_name_len(),
            Section::ValueLabels,
            Field::ValueLabelName,
        )?;
        // V104 padding is 2 bytes of zeros.
        self.state.write_padded_bytes(
            &[],
            release.value_label_table_padding_len(),
            Section::ValueLabels,
        )?;

        for slot in &slots {
            let bytes = slot.as_deref().unwrap_or_default();
            self.state
                .write_padded_bytes(bytes, 8, Section::ValueLabels)?;
        }
        Ok(())
    }

    /// Writes one table in the modern (V105+) layout:
    ///
    /// - (XML only) `<lbl>`
    /// - `table_len`: `u32` — byte size of the payload that follows
    ///   the name + padding.
    /// - `name`: fixed-width + 3-byte padding
    /// - payload:
    ///   - `n`: `u32` — entry count
    ///   - `text_len`: `u32`
    ///   - `offsets`: `[u32; n]` — byte offsets into text
    ///   - `values`: `[i32; n]`
    ///   - `text`: `text_len` bytes of null-terminated labels
    /// - (XML only) `</lbl>`
    fn write_modern_table(&mut self, table: &ValueLabelTable) -> Result<()> {
        let release = self.header.release();
        let byte_order = self.header.byte_order();
        let position_before = self.state.position();
        let entries = table.entries();

        // Encode labels up front so we can validate and compute
        // offsets without emitting any bytes yet. `encoded_labels`
        // keeps each label as a `Cow` — borrowed directly from the
        // caller's table on the pass-through encoding path.
        let (encoded_labels, offsets, text_len) =
            build_modern_text_payload(entries, self.state.encoding(), position_before)?;
        let entry_count = u32::try_from(entries.len()).map_err(|_| {
            DtaError::format(
                Section::ValueLabels,
                position_before,
                FormatErrorKind::FieldTooLarge {
                    field: Field::ValueLabelEntry,
                    max: u64::from(u32::MAX),
                    actual: u64::try_from(entries.len()).unwrap_or(u64::MAX),
                },
            )
        })?;

        // Payload bytes = 8 (n + text_len) + 4*n (offsets) + 4*n (values) + text_len.
        let payload_bytes = u64::from(entry_count)
            .checked_mul(8)
            .and_then(|n| n.checked_add(8))
            .and_then(|n| n.checked_add(u64::from(text_len)))
            .ok_or_else(|| {
                DtaError::format(
                    Section::ValueLabels,
                    position_before,
                    FormatErrorKind::FieldTooLarge {
                        field: Field::ValueLabelEntry,
                        max: u64::from(u32::MAX),
                        actual: u64::MAX,
                    },
                )
            })?;
        let table_len = u32::try_from(payload_bytes).map_err(|_| {
            DtaError::format(
                Section::ValueLabels,
                position_before,
                FormatErrorKind::FieldTooLarge {
                    field: Field::ValueLabelEntry,
                    max: u64::from(u32::MAX),
                    actual: payload_bytes,
                },
            )
        })?;

        if release.is_xml_like() {
            self.state.write_exact(b"<lbl>", Section::ValueLabels)?;
        }

        self.state
            .write_u32(table_len, byte_order, Section::ValueLabels)?;
        self.state.write_fixed_string(
            table.name(),
            release.value_label_name_len(),
            Section::ValueLabels,
            Field::ValueLabelName,
        )?;
        // Modern padding (V105+) is 3 bytes of zeros.
        self.state.write_padded_bytes(
            &[],
            release.value_label_table_padding_len(),
            Section::ValueLabels,
        )?;

        // Payload header.
        self.state
            .write_u32(entry_count, byte_order, Section::ValueLabels)?;
        self.state
            .write_u32(text_len, byte_order, Section::ValueLabels)?;

        // Offsets.
        for offset in &offsets {
            self.state
                .write_u32(*offset, byte_order, Section::ValueLabels)?;
        }

        // Values (signed i32 — negative codings like "refused" or
        // "don't know" are common in survey data).
        for entry in entries {
            self.state
                .write_i32(entry.value(), byte_order, Section::ValueLabels)?;
        }

        // Text area — each label followed by a null terminator.
        // Emitting per-label avoids concatenating every label into a
        // single buffer up front, so the borrowed branch of each
        // `Cow` stays a pointer into the caller's `ValueLabelTable`.
        for label in &encoded_labels {
            self.state.write_exact(label, Section::ValueLabels)?;
            self.state.write_u8(0, Section::ValueLabels)?;
        }

        if release.is_xml_like() {
            self.state.write_exact(b"</lbl>", Section::ValueLabels)?;
        }

        Ok(())
    }
}

// ===========================================================================
// Tests — chain through the full writer pipeline via `finish()`
// ===========================================================================

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

    // -- Helpers -------------------------------------------------------------

    /// Runs the full writer pipeline end-to-end and returns the
    /// finalized file bytes. The caller emits zero or more value-label
    /// tables; everything else uses a minimal schema with no rows.
    fn round_trip<F>(release: Release, byte_order: ByteOrder, write_fn: F) -> Vec<u8>
    where
        F: FnOnce(&mut ValueLabelWriter<Cursor<Vec<u8>>>) -> Result<()>,
    {
        let schema = Schema::builder()
            .add_variable(Variable::builder(VariableType::Byte, "x").format("%8.0g"))
            .build()
            .unwrap();
        let header = Header::builder(release, byte_order).build();
        let characteristic_writer = DtaWriter::new()
            .from_writer(Cursor::new(Vec::<u8>::new()))
            .write_header(header)
            .unwrap()
            .write_schema(schema)
            .unwrap();
        let record_writer = characteristic_writer.into_record_writer().unwrap();
        let long_string_writer = record_writer.into_long_string_writer().unwrap();
        let mut value_label_writer = long_string_writer.into_value_label_writer().unwrap();
        write_fn(&mut value_label_writer).unwrap();
        value_label_writer.finish().unwrap().into_inner()
    }

    /// Reads back the full chain and collects every value-label table.
    fn read_back(bytes: Vec<u8>) -> Vec<ValueLabelTable> {
        let mut characteristic_reader = DtaReader::new()
            .from_reader(Cursor::new(bytes))
            .read_header()
            .unwrap()
            .read_schema()
            .unwrap();
        while characteristic_reader
            .read_characteristic()
            .unwrap()
            .is_some()
        {}
        let mut record_reader = characteristic_reader.into_record_reader().unwrap();
        while record_reader.read_record().unwrap().is_some() {}
        let mut long_string_reader = record_reader.into_long_string_reader().unwrap();
        while long_string_reader.read_long_string().unwrap().is_some() {}
        let mut value_label_reader = long_string_reader.into_value_label_reader().unwrap();
        let mut tables = Vec::new();
        while let Some(table) = value_label_reader.read_value_label_table().unwrap() {
            tables.push(table);
        }
        tables
    }

    fn entries(pairs: &[(i32, &str)]) -> Vec<ValueLabelEntry> {
        pairs
            .iter()
            .map(|&(v, l)| ValueLabelEntry::new(v, l.to_owned()))
            .collect()
    }

    // -- Modern-layout round-trips (V105+) ----------------------------------

    #[test]
    fn binary_v114_single_table_round_trip() {
        let table = ValueLabelTable::new(
            "pricelbl".to_owned(),
            entries(&[(0, "cheap"), (1, "pricey")]),
        );
        let bytes = round_trip(Release::V114, ByteOrder::LittleEndian, |writer| {
            writer.write_value_label_table(&table)
        });
        let tables = read_back(bytes);
        assert_eq!(tables.len(), 1);
        assert_eq!(tables[0].name(), "pricelbl");
        assert_eq!(tables[0].entries().len(), 2);
        assert_eq!(tables[0].entries()[0].value(), 0);
        assert_eq!(tables[0].entries()[0].label(), "cheap");
        assert_eq!(tables[0].entries()[1].value(), 1);
        assert_eq!(tables[0].entries()[1].label(), "pricey");
    }

    #[test]
    fn binary_v114_multiple_tables_round_trip() {
        let t1 = ValueLabelTable::new("a".to_owned(), entries(&[(0, "zero"), (1, "one")]));
        let t2 = ValueLabelTable::new("b".to_owned(), entries(&[(-1, "neg")]));
        let bytes = round_trip(Release::V114, ByteOrder::LittleEndian, |writer| {
            writer.write_value_label_table(&t1)?;
            writer.write_value_label_table(&t2)
        });
        let tables = read_back(bytes);
        assert_eq!(tables.len(), 2);
        assert_eq!(tables[0].name(), "a");
        assert_eq!(tables[1].name(), "b");
        assert_eq!(tables[1].entries()[0].value(), -1);
        assert_eq!(tables[1].entries()[0].label(), "neg");
    }

    #[test]
    fn binary_v114_empty_table_round_trip() {
        let table = ValueLabelTable::new("empty".to_owned(), Vec::new());
        let bytes = round_trip(Release::V114, ByteOrder::LittleEndian, |writer| {
            writer.write_value_label_table(&table)
        });
        let tables = read_back(bytes);
        assert_eq!(tables.len(), 1);
        assert_eq!(tables[0].name(), "empty");
        assert!(tables[0].entries().is_empty());
    }

    #[test]
    fn binary_v114_big_endian_round_trip() {
        let table = ValueLabelTable::new("be".to_owned(), entries(&[(10, "ten"), (20, "twenty")]));
        let bytes = round_trip(Release::V114, ByteOrder::BigEndian, |writer| {
            writer.write_value_label_table(&table)
        });
        let tables = read_back(bytes);
        assert_eq!(tables[0].entries()[0].value(), 10);
        assert_eq!(tables[0].entries()[1].value(), 20);
    }

    #[test]
    fn binary_v114_no_tables_round_trip() {
        let bytes = round_trip(Release::V114, ByteOrder::LittleEndian, |_| Ok(()));
        let tables = read_back(bytes);
        assert!(tables.is_empty());
    }

    // -- XML round-trips ----------------------------------------------------

    #[test]
    fn xml_v117_round_trip() {
        let t1 = ValueLabelTable::new("pricelbl".to_owned(), entries(&[(1, "low"), (5, "high")]));
        let t2 = ValueLabelTable::new("empty".to_owned(), Vec::new());
        let bytes = round_trip(Release::V117, ByteOrder::LittleEndian, |writer| {
            writer.write_value_label_table(&t1)?;
            writer.write_value_label_table(&t2)
        });
        let tables = read_back(bytes);
        assert_eq!(tables.len(), 2);
        assert_eq!(tables[0].entries()[0].value(), 1);
        assert_eq!(tables[0].entries()[1].value(), 5);
        assert_eq!(tables[0].entries()[1].label(), "high");
        assert_eq!(tables[1].name(), "empty");
        assert!(tables[1].entries().is_empty());
    }

    #[test]
    fn xml_v117_no_tables_round_trip() {
        // Zero tables must still produce `<value_labels></value_labels>`
        // so the reader's expect_bytes succeeds.
        let bytes = round_trip(Release::V117, ByteOrder::LittleEndian, |_| Ok(()));
        let tables = read_back(bytes);
        assert!(tables.is_empty());
    }

    #[test]
    fn xml_v118_utf8_label_round_trip() {
        let table =
            ValueLabelTable::new("lang".to_owned(), entries(&[(1, "日本語"), (2, "español")]));
        let bytes = round_trip(Release::V118, ByteOrder::LittleEndian, |writer| {
            writer.write_value_label_table(&table)
        });
        let tables = read_back(bytes);
        assert_eq!(tables[0].entries()[0].label(), "日本語");
        assert_eq!(tables[0].entries()[1].label(), "español");
    }

    #[test]
    fn xml_v117_big_endian_round_trip() {
        let table = ValueLabelTable::new("be".to_owned(), entries(&[(100, "hundred")]));
        let bytes = round_trip(Release::V117, ByteOrder::BigEndian, |writer| {
            writer.write_value_label_table(&table)
        });
        let tables = read_back(bytes);
        assert_eq!(tables[0].entries()[0].value(), 100);
        assert_eq!(tables[0].entries()[0].label(), "hundred");
    }

    // -- V104 legacy layout -------------------------------------------------

    #[test]
    fn v104_single_table_round_trip() {
        // V104 uses entry index as value. Values must be 0..8190.
        let table = ValueLabelTable::new("old".to_owned(), entries(&[(0, "zero"), (2, "two")]));
        let bytes = round_trip(Release::V104, ByteOrder::LittleEndian, |writer| {
            writer.write_value_label_table(&table)
        });
        let tables = read_back(bytes);
        assert_eq!(tables.len(), 1);
        // Slot 1 was empty in the input — reader skips empty slots,
        // so entries come back with only values 0 and 2.
        assert_eq!(tables[0].entries().len(), 2);
        assert_eq!(tables[0].entries()[0].value(), 0);
        assert_eq!(tables[0].entries()[0].label(), "zero");
        assert_eq!(tables[0].entries()[1].value(), 2);
        assert_eq!(tables[0].entries()[1].label(), "two");
    }

    #[test]
    fn v104_rejects_negative_value() {
        let table = ValueLabelTable::new("neg".to_owned(), entries(&[(-1, "nope")]));
        // Use a one-shot writer; don't round_trip because finish()
        // shouldn't be reached when write_value_label_table errors.
        let schema = Schema::builder()
            .add_variable(Variable::builder(VariableType::Byte, "x").format("%8.0g"))
            .build()
            .unwrap();
        let header = Header::builder(Release::V104, ByteOrder::LittleEndian).build();
        let mut writer = DtaWriter::new()
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
        let error = writer.write_value_label_table(&table).unwrap_err();
        assert!(matches!(
            error,
            DtaError::Format(ref e) if matches!(
                e.kind(),
                FormatErrorKind::OldValueLabelValueOutOfRange { value: -1 }
            )
        ));
    }

    #[test]
    fn v104_rejects_duplicate_value() {
        let table = ValueLabelTable::new("dup".to_owned(), entries(&[(1, "a"), (1, "b")]));
        let schema = Schema::builder()
            .add_variable(Variable::builder(VariableType::Byte, "x").format("%8.0g"))
            .build()
            .unwrap();
        let header = Header::builder(Release::V104, ByteOrder::LittleEndian).build();
        let mut writer = DtaWriter::new()
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
        let error = writer.write_value_label_table(&table).unwrap_err();
        assert!(matches!(
            error,
            DtaError::Format(ref e) if matches!(
                e.kind(),
                FormatErrorKind::OldValueLabelValueOutOfRange { value: 1 }
            )
        ));
    }

    // -- Error cases --------------------------------------------------------

    #[test]
    fn label_too_long_in_v104_errors() {
        // V104 labels are 8-byte slots; anything longer should error.
        let table = ValueLabelTable::new("long".to_owned(), entries(&[(0, "nine char")]));
        let schema = Schema::builder()
            .add_variable(Variable::builder(VariableType::Byte, "x").format("%8.0g"))
            .build()
            .unwrap();
        let header = Header::builder(Release::V104, ByteOrder::LittleEndian).build();
        let mut writer = DtaWriter::new()
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
        let error = writer.write_value_label_table(&table).unwrap_err();
        assert!(matches!(
            error,
            DtaError::Format(ref e) if matches!(
                e.kind(),
                FormatErrorKind::FieldTooLarge { field: Field::ValueLabelEntry, .. }
            )
        ));
    }

    #[test]
    fn table_name_too_long_errors() {
        // V114's value_label_name_len is 33 bytes.
        let long_name = "n".repeat(50);
        let table = ValueLabelTable::new(long_name, Vec::new());
        let schema = Schema::builder()
            .add_variable(Variable::builder(VariableType::Byte, "x").format("%8.0g"))
            .build()
            .unwrap();
        let header = Header::builder(Release::V114, ByteOrder::LittleEndian).build();
        let mut writer = DtaWriter::new()
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
        let error = writer.write_value_label_table(&table).unwrap_err();
        assert!(matches!(
            error,
            DtaError::Format(ref e) if matches!(
                e.kind(),
                FormatErrorKind::FieldTooLarge { field: Field::ValueLabelName, .. }
            )
        ));
    }

    #[test]
    fn non_latin_label_in_windows_1252_errors() {
        // V114 default encoding is Windows-1252 — Japanese characters
        // aren't representable.
        let table = ValueLabelTable::new("lang".to_owned(), entries(&[(1, "日")]));
        let schema = Schema::builder()
            .add_variable(Variable::builder(VariableType::Byte, "x").format("%8.0g"))
            .build()
            .unwrap();
        let header = Header::builder(Release::V114, ByteOrder::LittleEndian).build();
        let mut writer = DtaWriter::new()
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
        let error = writer.write_value_label_table(&table).unwrap_err();
        assert!(matches!(
            error,
            DtaError::Format(ref e) if matches!(
                e.kind(),
                FormatErrorKind::InvalidEncoding { field: Field::ValueLabelEntry }
            )
        ));
    }

    // -- XML <map> offset validation ---------------------------------------

    /// Writes a fully populated XML DTA file: two-variable schema
    /// (one `Byte`, one `LongString`), one dataset-level
    /// characteristic, one record, one `strL` payload, one
    /// value-label table. Shared by the seek-navigation and direct
    /// map-inspection tests.
    fn build_populated_xml_file(release: Release, byte_order: ByteOrder) -> Vec<u8> {
        use crate::stata::dta::characteristic::{Characteristic, CharacteristicTarget};
        use crate::stata::dta::long_string_table::LongStringTable;
        use crate::stata::dta::value::Value;
        use crate::stata::stata_byte::StataByte;

        let schema = Schema::builder()
            .add_variable(Variable::builder(VariableType::Byte, "x").format("%8.0g"))
            .add_variable(Variable::builder(VariableType::LongString, "note").format("%9s"))
            .build()
            .unwrap();
        let header = Header::builder(release, byte_order).build();

        let mut long_strings = LongStringTable::new();
        let ls_ref = long_strings.get_or_insert(2, 1, b"hello strL", false);

        let value_label_table =
            ValueLabelTable::new("lbl".to_owned(), entries(&[(0, "zero"), (1, "one")]));

        let characteristic = Characteristic::new(
            CharacteristicTarget::Dataset,
            "note1".to_owned(),
            "map-offset test".to_owned(),
        );

        let mut characteristic_writer = DtaWriter::new()
            .from_writer(Cursor::new(Vec::<u8>::new()))
            .write_header(header)
            .unwrap()
            .write_schema(schema)
            .unwrap();
        characteristic_writer
            .write_characteristic(&characteristic)
            .unwrap();
        let mut record_writer = characteristic_writer.into_record_writer().unwrap();
        record_writer
            .write_record(&[
                Value::Byte(StataByte::Present(1)),
                Value::LongStringRef(ls_ref),
            ])
            .unwrap();
        let mut long_string_writer = record_writer.into_long_string_writer().unwrap();
        long_string_writer
            .write_long_string_table(&long_strings)
            .unwrap();
        let mut value_label_writer = long_string_writer.into_value_label_writer().unwrap();
        value_label_writer
            .write_value_label_table(&value_label_table)
            .unwrap();
        value_label_writer.finish().unwrap().into_inner()
    }

    /// Drives the reader's `seek_*` methods over a populated file.
    /// Each seek consumes a map slot; a wrong slot would make the
    /// subsequent `read_*` land on garbage and fail.
    fn assert_seek_navigation_exercises_map(bytes: Vec<u8>) {
        let characteristic_reader = DtaReader::new()
            .from_reader(Cursor::new(bytes))
            .read_header()
            .unwrap()
            .read_schema()
            .unwrap();

        // Slot 9: <data>.
        let mut record_reader = characteristic_reader.seek_records().unwrap();
        let first_record = record_reader.read_record().unwrap().unwrap();
        assert_eq!(first_record.values().len(), 2);

        // Slot 10: <strls>.
        let mut long_string_reader = record_reader.seek_long_strings().unwrap();
        let parsed_ls = long_string_reader.read_long_string().unwrap().unwrap();
        assert_eq!(parsed_ls.data(), b"hello strL");

        // Slot 11: <value_labels>.
        let mut value_label_reader = long_string_reader.seek_value_labels().unwrap();
        let parsed_table = value_label_reader
            .read_value_label_table()
            .unwrap()
            .unwrap();
        assert_eq!(parsed_table.name(), "lbl");
        assert_eq!(parsed_table.entries().len(), 2);

        // Slot 8: <characteristics>. Seek back from the value-label
        // reader — this is the only place that consumes slot 8
        // (sequential reading reaches characteristics naturally from
        // the schema's current position).
        let mut characteristic_reader = value_label_reader.seek_characteristics().unwrap();
        let parsed_char = characteristic_reader
            .read_characteristic()
            .unwrap()
            .unwrap();
        assert_eq!(parsed_char.name(), "note1");
        assert_eq!(parsed_char.value(), "map-offset test");
    }

    #[test]
    fn xml_v117_map_offsets_via_seek_navigation() {
        let bytes = build_populated_xml_file(Release::V117, ByteOrder::LittleEndian);
        assert_seek_navigation_exercises_map(bytes);
    }

    #[test]
    fn xml_v118_map_offsets_via_seek_navigation() {
        // V118 uses u16+u48 for data-section strL refs, u32+u64 for
        // GSO observation, u64 for the header N field, and UTF-8 for
        // strings. Different wire layouts than V117 — seek navigation
        // exercises the same four map slots against the different
        // encoding to prove the writer and reader agree for V118.
        let bytes = build_populated_xml_file(Release::V118, ByteOrder::LittleEndian);
        assert_seek_navigation_exercises_map(bytes);
    }

    #[test]
    fn xml_v119_map_offsets_via_seek_navigation_big_endian() {
        // V119 + big endian — widest variable-count field (u32 K)
        // combined with the reverse byte order. Nothing special
        // happens structurally, but an extra pass through the
        // endian-swap code paths is inexpensive insurance.
        let bytes = build_populated_xml_file(Release::V119, ByteOrder::BigEndian);
        assert_seek_navigation_exercises_map(bytes);
    }

    /// Scans a byte buffer for a literal needle and returns its
    /// starting position. Used by the direct-inspection test to
    /// locate the `<map>` tag without depending on the writer's
    /// exact header byte layout.
    fn find_tag(bytes: &[u8], needle: &[u8]) -> usize {
        bytes
            .windows(needle.len())
            .position(|window| window == needle)
            .unwrap_or_else(|| panic!("{:?} not found in file", std::str::from_utf8(needle)))
    }

    /// Reads the 14 `u64` map slots starting at `payload_start`.
    fn read_map_slots(bytes: &[u8], payload_start: usize, byte_order: ByteOrder) -> [u64; 14] {
        let mut slots = [0u64; 14];
        for (index, slot) in slots.iter_mut().enumerate() {
            let start = payload_start + index * 8;
            *slot = byte_order.read_u64([
                bytes[start],
                bytes[start + 1],
                bytes[start + 2],
                bytes[start + 3],
                bytes[start + 4],
                bytes[start + 5],
                bytes[start + 6],
                bytes[start + 7],
            ]);
        }
        slots
    }

    /// Asserts that the bytes at `offset` start with `expected_tag`.
    fn assert_tag_at(bytes: &[u8], offset: u64, expected_tag: &[u8], slot: usize) {
        let offset_usize = usize::try_from(offset)
            .unwrap_or_else(|_| panic!("map slot {slot} offset {offset} exceeds usize"));
        assert!(
            bytes[offset_usize..].starts_with(expected_tag),
            "map slot {slot} at offset {offset} should start with {:?}, got {:?}",
            std::str::from_utf8(expected_tag).unwrap(),
            std::str::from_utf8(
                &bytes[offset_usize..(offset_usize + expected_tag.len()).min(bytes.len())]
            )
            .unwrap_or("<non-utf8>"),
        );
    }

    /// Belt-and-suspenders: parse the `<map>` payload directly from
    /// the finished file and verify every one of the 14 slots
    /// points at the expected tag (or, for slot 13, at end-of-file).
    /// `seek_*` navigation already covers slots 8–11 in the other
    /// tests; this one also covers slots 0–7 and 12–13, which no
    /// reader API consumes today.
    #[test]
    fn xml_v117_map_slots_point_at_expected_tags() {
        let bytes = build_populated_xml_file(Release::V117, ByteOrder::LittleEndian);
        let byte_order = ByteOrder::LittleEndian;

        // Locate <map> (don't trust the writer's exact header length).
        let map_tag_start = find_tag(&bytes, b"<map>");
        let payload_start = map_tag_start + b"<map>".len();
        let slots = read_map_slots(&bytes, payload_start, byte_order);

        // Slot 0: <stata_dta> at byte 0.
        assert_eq!(slots[0], 0, "slot 0 should be the <stata_dta> offset (0)");
        assert_tag_at(&bytes, slots[0], b"<stata_dta>", 0);

        // Slot 1: <map> position we already located.
        assert_eq!(
            slots[1],
            u64::try_from(map_tag_start).unwrap(),
            "slot 1 should match the scanned <map> position",
        );
        assert_tag_at(&bytes, slots[1], b"<map>", 1);

        // Slots 2–11: descriptor sub-sections + post-schema sections.
        let expected: [(usize, &[u8]); 10] = [
            (2, b"<variable_types>"),
            (3, b"<varnames>"),
            (4, b"<sortlist>"),
            (5, b"<formats>"),
            (6, b"<value_label_names>"),
            (7, b"<variable_labels>"),
            (8, b"<characteristics>"),
            (9, b"<data>"),
            (10, b"<strls>"),
            (11, b"<value_labels>"),
        ];
        for (slot, tag) in expected {
            assert_tag_at(&bytes, slots[slot], tag, slot);
        }

        // Slot 12: </stata_dta> close tag.
        assert_tag_at(&bytes, slots[12], b"</stata_dta>", 12);

        // Slot 13: EOF. The writer sets this to the position right
        // after </stata_dta> — which for the current layout equals
        // `bytes.len()` since nothing follows.
        assert_eq!(
            slots[13],
            u64::try_from(bytes.len()).unwrap(),
            "slot 13 should mark EOF",
        );
    }
}
