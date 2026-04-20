use std::borrow::Cow;
use std::io::{Seek, Write};

use super::dta_error::{DtaError, Field, FormatErrorKind, Result, Section};
use super::header::Header;
use super::schema::Schema;
use super::value_label::{ValueLabelEntry, ValueLabelTable};
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

    /// Consumes the writer and returns the underlying state. Used by
    /// long-string-writer round-trip tests that need to recover the
    /// sink before `finish` is implemented.
    #[cfg(test)]
    pub(crate) fn into_state(self) -> WriterState<W> {
        self.state
    }
}

/// Maximum value that fits in the V104 legacy layout. Each slot is
/// 8 bytes and `table_len` is a `u16`, so `slot_count * 8 ≤ u16::MAX`
/// gives `slot_count ≤ 8191`. Values are 0-indexed, so the largest
/// representable value is `8191 - 1 = 8190`.
const OLD_VALUE_LABEL_MAX_VALUE: i32 = 8190;

/// Output shape of
/// [`ValueLabelWriter::build_modern_text_payload`] — encoded labels
/// (borrowed when possible), per-entry byte offsets into the logical
/// text area, and the total text length.
type ModernTextPayload<'a> = (Vec<Cow<'a, [u8]>>, Vec<u32>, u32);

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

        let slots = self.build_old_slot_table(table)?;
        let slot_count = slots.len();
        let table_len_u16 = u16::try_from(slot_count.saturating_mul(8)).map_err(|_| {
            DtaError::format(
                Section::ValueLabels,
                position_before,
                FormatErrorKind::FieldTooLarge {
                    field: Field::ValueLabelEntry,
                    max: u64::from(u16::MAX),
                    actual: u64::try_from(slot_count * 8).unwrap_or(u64::MAX),
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

    /// Validates every entry in `table` and packs encoded labels into
    /// a slot-indexed vector for the V104 legacy layout. The returned
    /// `Vec`'s length is the slot count (= max value + 1, or 0 when
    /// empty); empty slots are `None` and represent "no entry for
    /// this value".
    ///
    /// Each slot holds a `Cow<[u8]>` — borrowed directly from the
    /// caller's `ValueLabelTable` on the UTF-8 → UTF-8 pass-through
    /// path, owned only when the active encoding applied.
    ///
    /// Errors upfront — before any bytes are written — on negative or
    /// out-of-range values (`OldValueLabelValueOutOfRange`), duplicate
    /// values (same variant), labels that exceed the 8-byte slot
    /// (`FieldTooLarge`), and labels the active encoding can't
    /// represent (`InvalidEncoding`).
    fn build_old_slot_table<'a>(
        &self,
        table: &'a ValueLabelTable,
    ) -> Result<Vec<Option<Cow<'a, [u8]>>>> {
        let encoding = self.state.encoding();
        let position_before = self.state.position();

        let mut slots: Vec<Option<Cow<'a, [u8]>>> = Vec::new();
        for entry in table.entries() {
            let value = entry.value();
            if !(0..=OLD_VALUE_LABEL_MAX_VALUE).contains(&value) {
                return Err(DtaError::format(
                    Section::ValueLabels,
                    position_before,
                    FormatErrorKind::OldValueLabelValueOutOfRange { value },
                ));
            }
            let slot = usize::try_from(value).expect("0..=OLD_VALUE_LABEL_MAX_VALUE fits usize");

            if slot >= slots.len() {
                slots.resize(slot + 1, None);
            }
            if slots[slot].is_some() {
                return Err(DtaError::format(
                    Section::ValueLabels,
                    position_before,
                    FormatErrorKind::OldValueLabelValueOutOfRange { value },
                ));
            }

            let (encoded, _, had_unmappable) = encoding.encode(entry.label());
            if had_unmappable {
                return Err(DtaError::format(
                    Section::ValueLabels,
                    position_before,
                    FormatErrorKind::InvalidEncoding {
                        field: Field::ValueLabelEntry,
                    },
                ));
            }
            if encoded.len() > 8 {
                return Err(DtaError::format(
                    Section::ValueLabels,
                    position_before,
                    FormatErrorKind::FieldTooLarge {
                        field: Field::ValueLabelEntry,
                        max: 8,
                        actual: u64::try_from(encoded.len()).unwrap_or(u64::MAX),
                    },
                ));
            }
            slots[slot] = Some(encoded);
        }
        Ok(slots)
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
        let (encoded_labels, offsets, text_len) = self.build_modern_text_payload(entries)?;
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
        let payload_bytes = 8u64 + u64::from(entry_count).saturating_mul(8) + u64::from(text_len);
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

    /// Encodes every label into an owned or borrowed `Cow` (no
    /// concatenation) and records each label's byte offset into the
    /// logical null-terminated text area the DTA layout expects.
    /// Returns `(encoded_labels, offsets, text_len)` where
    /// `offsets[i]` is the byte position of the `i`-th label in the
    /// text area and `text_len` is the total text-area byte count
    /// (including the per-label null terminators).
    ///
    /// Errors upfront — before any bytes are written — on labels the
    /// active encoding can't represent (`InvalidEncoding`) and on
    /// cumulative text length exceeding `u32::MAX`.
    fn build_modern_text_payload<'a>(
        &self,
        entries: &'a [ValueLabelEntry],
    ) -> Result<ModernTextPayload<'a>> {
        let encoding = self.state.encoding();
        let position_before = self.state.position();
        let mut encoded_labels: Vec<Cow<'a, [u8]>> = Vec::with_capacity(entries.len());
        let mut offsets: Vec<u32> = Vec::with_capacity(entries.len());
        let mut running_len: usize = 0;
        for entry in entries {
            let (encoded, _, had_unmappable) = encoding.encode(entry.label());
            if had_unmappable {
                return Err(DtaError::format(
                    Section::ValueLabels,
                    position_before,
                    FormatErrorKind::InvalidEncoding {
                        field: Field::ValueLabelEntry,
                    },
                ));
            }
            let offset = u32::try_from(running_len)
                .map_err(|_| text_overflow(position_before, running_len))?;
            offsets.push(offset);
            // Each label contributes its own bytes plus one
            // null-terminator byte to the logical text area.
            running_len = running_len
                .checked_add(encoded.len())
                .and_then(|n| n.checked_add(1))
                .ok_or_else(|| text_overflow(position_before, usize::MAX))?;
            encoded_labels.push(encoded);
        }
        let text_len =
            u32::try_from(running_len).map_err(|_| text_overflow(position_before, running_len))?;
        Ok((encoded_labels, offsets, text_len))
    }
}

fn text_overflow(position: u64, actual: usize) -> DtaError {
    DtaError::format(
        Section::ValueLabels,
        position,
        FormatErrorKind::FieldTooLarge {
            field: Field::ValueLabelEntry,
            max: u64::from(u32::MAX),
            actual: u64::try_from(actual).unwrap_or(u64::MAX),
        },
    )
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
}
