use std::io::{Seek, Write};

use super::dta_error::{DtaError, Field, FormatErrorKind, Result, Section};
use super::header::Header;
use super::long_string::{GsoType, LongString};
use super::long_string_format::{GSO_MAGIC, narrow_long_string_data_len};
use super::long_string_table::LongStringTable;
use super::schema::Schema;
use super::value_label_writer::ValueLabelWriter;
use super::writer_state::WriterState;

/// Writes long string (strL / GSO) entries to a DTA file.
///
/// Only XML formats (117+) support strLs. For earlier releases,
/// [`write_long_string`](Self::write_long_string) returns an error
/// and [`into_value_label_writer`](Self::into_value_label_writer)
/// transitions without emitting any strL content.
#[derive(Debug)]
pub struct LongStringWriter<W> {
    state: WriterState<W>,
    header: Header,
    schema: Schema,
    /// Tracks whether the XML `<strls>` opening tag has been emitted.
    /// Unused (but harmless) for pre-117 formats, which have no
    /// section at all.
    opened: bool,
}

impl<W> LongStringWriter<W> {
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

    /// The encoding this writer uses to encode long-string payloads.
    ///
    /// Defaults to Windows-1252 for pre-V118 releases and UTF-8 for
    /// V118+, overridable via
    /// [`DtaWriter::encoding`](super::dta_writer::DtaWriter::encoding).
    #[must_use]
    #[inline]
    pub fn encoding(&self) -> &'static encoding_rs::Encoding {
        self.state.encoding()
    }
}

impl<W: Write + Seek> LongStringWriter<W> {
    /// Writes a single long-string (strL) entry as a GSO block.
    ///
    /// The first call also emits the `<strls>` opening tag. GSO
    /// block layout:
    ///
    /// - `"GSO"` magic (3 bytes)
    /// - `variable`: `u32` (4 bytes)
    /// - `observation`: `u32` (V117) or `u64` (V118+)
    /// - `gso_type`: `u8` — `0x81` for binary, `0x82` for text
    /// - `data_len`: `u32`
    /// - `data`: `data_len` bytes
    ///
    /// # Errors
    ///
    /// Returns [`DtaError::Format`] with
    /// [`LongStringsUnsupported`](FormatErrorKind::LongStringsUnsupported)
    /// if the header's release is pre-V117 (no `<strls>` section),
    /// [`FieldTooLarge`](FormatErrorKind::FieldTooLarge) if the
    /// payload exceeds `u32::MAX` bytes or the observation index
    /// exceeds `u32::MAX` on a V117 file. Returns
    /// [`DtaError::Io`] on sink failures.
    pub fn write_long_string(&mut self, long_string: &LongString<'_>) -> Result<()> {
        let release = self.header.release();
        if !release.supports_long_strings() {
            let error = DtaError::format(
                Section::LongStrings,
                self.state.position(),
                FormatErrorKind::LongStringsUnsupported { release },
            );
            return Err(error);
        }
        if release.is_xml_like() {
            self.open_section_if_needed()?;
        }
        self.write_gso_block(long_string)
    }

    /// Writes every entry from `table` via
    /// [`write_long_string`](Self::write_long_string), in the
    /// `(variable, observation)` order the DTA spec requires.
    ///
    /// An empty table is a no-op — including on pre-117 releases,
    /// where writing any entry would error but writing none is
    /// benign.
    ///
    /// # Errors
    ///
    /// Surfaces the first error from
    /// [`write_long_string`](Self::write_long_string). For pre-117
    /// releases with a non-empty table, that's
    /// [`LongStringsUnsupported`](FormatErrorKind::LongStringsUnsupported)
    /// on the very first entry.
    pub fn write_long_string_table(&mut self, table: &LongStringTable) -> Result<()> {
        // `LongStringTable::iter` borrows only the table, so we're
        // free to re-borrow `self` as `&mut` inside the loop body.
        for long_string in table.iter() {
            self.write_long_string(&long_string)?;
        }
        Ok(())
    }

    /// Closes the long-strings section, patches map slot 11
    /// (value-labels offset) for XML releases, and transitions to
    /// value-label writing.
    ///
    /// For XML (V117+) the closing `</strls>` tag is emitted even
    /// when no entries were written (the opening tag is lazy-emitted
    /// here in that case). For pre-V117 no section exists at all —
    /// nothing is written.
    ///
    /// # Errors
    ///
    /// Returns [`DtaError::Io`] on sink failures.
    pub fn into_value_label_writer(mut self) -> Result<ValueLabelWriter<W>> {
        let release = self.header.release();
        let byte_order = self.header.byte_order();

        if release.supports_long_strings() {
            if self.header.release().is_xml_like() {
                self.open_section_if_needed()?;
                self.state.write_exact(b"</strls>", Section::LongStrings)?;
            }
            let value_labels_offset = self.state.position();
            self.state.patch_map_entry(
                11,
                value_labels_offset,
                byte_order,
                Section::LongStrings,
            )?;
        }
        // Pre-117: no `<strls>` section, no map — nothing to do.

        let writer = ValueLabelWriter::new(self.state, self.header, self.schema);
        Ok(writer)
    }

    /// Emits the XML `<strls>` tag on first use. Only called on
    /// paths that have already verified the release supports
    /// long strings.
    fn open_section_if_needed(&mut self) -> Result<()> {
        if !self.opened {
            self.state.write_exact(b"<strls>", Section::LongStrings)?;
            self.opened = true;
        }
        Ok(())
    }
}

impl<W: Write> LongStringWriter<W> {
    /// Emits one GSO block. Assumes the release has been validated
    /// to support long strings.
    fn write_gso_block(&mut self, long_string: &LongString<'_>) -> Result<()> {
        let byte_order = self.header.byte_order();
        let release = self.header.release();

        self.state.write_exact(GSO_MAGIC, Section::LongStrings)?;

        self.state
            .write_u32(long_string.variable(), byte_order, Section::LongStrings)?;

        if release.supports_extended_observation_count() {
            self.state
                .write_u64(long_string.observation(), byte_order, Section::LongStrings)?;
        } else {
            let observation = self.state.narrow_to_u32(
                long_string.observation(),
                Section::LongStrings,
                Field::ObservationCount,
            )?;
            self.state
                .write_u32(observation, byte_order, Section::LongStrings)?;
        }

        let gso_type = if long_string.is_binary() {
            GsoType::Binary
        } else {
            GsoType::Text
        };
        self.state
            .write_u8(gso_type.to_byte(), Section::LongStrings)?;

        let data = long_string.data();
        let data_len = narrow_long_string_data_len(data.len(), self.state.position())?;
        self.state
            .write_u32(data_len, byte_order, Section::LongStrings)?;
        self.state.write_exact(data, Section::LongStrings)?;
        Ok(())
    }
}

// ===========================================================================
// Tests
// ===========================================================================

#[cfg(test)]
mod tests {
    use std::borrow::Cow;
    use std::io::Cursor;

    use super::*;
    use crate::stata::dta::byte_order::ByteOrder;
    use crate::stata::dta::dta_reader::DtaReader;
    use crate::stata::dta::dta_writer::DtaWriter;
    use crate::stata::dta::long_string::LongStringContent;
    use crate::stata::dta::release::Release;
    use crate::stata::dta::variable::Variable;
    use crate::stata::dta::variable_type::VariableType;

    // -- Helpers -------------------------------------------------------------

    /// Runs the writer pipeline up through `LongStringWriter`,
    /// letting the caller emit long-string entries, then transitions
    /// to `ValueLabelWriter` and returns the raw sink bytes.
    fn round_trip<F>(release: Release, byte_order: ByteOrder, write_fn: F) -> Vec<u8>
    where
        F: FnOnce(&mut LongStringWriter<Cursor<Vec<u8>>>) -> Result<()>,
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
        let mut long_string_writer = record_writer.into_long_string_writer().unwrap();
        write_fn(&mut long_string_writer).unwrap();
        long_string_writer
            .into_value_label_writer()
            .unwrap()
            .finish()
            .unwrap()
            .into_inner()
    }

    /// Reads a file produced by [`round_trip`] back through the
    /// reader chain and collects every `<strls>` entry as owned data.
    fn read_back(bytes: Vec<u8>) -> Vec<OwnedLongString> {
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
        let mut entries = Vec::new();
        while let Some(ls) = long_string_reader.read_long_string().unwrap() {
            entries.push(OwnedLongString::from(&ls));
        }
        entries
    }

    /// Owned echo of [`LongString`] for test assertions.
    #[derive(Debug, PartialEq, Eq)]
    struct OwnedLongString {
        variable: u32,
        observation: u64,
        binary: bool,
        data: Vec<u8>,
    }

    impl From<&LongString<'_>> for OwnedLongString {
        fn from(ls: &LongString<'_>) -> Self {
            Self {
                variable: ls.variable(),
                observation: ls.observation(),
                binary: ls.is_binary(),
                data: ls.data().to_vec(),
            }
        }
    }

    fn text(variable: u32, observation: u64, data: &'static str) -> LongString<'static> {
        LongString::new(
            variable,
            observation,
            LongStringContent::Text(Cow::Borrowed(data.as_bytes())),
        )
    }

    fn binary(variable: u32, observation: u64, data: &'static [u8]) -> LongString<'static> {
        LongString::new(
            variable,
            observation,
            LongStringContent::Binary(Cow::Borrowed(data)),
        )
    }

    // -- V117 round-trips ---------------------------------------------------

    #[test]
    fn xml_v117_single_long_string_round_trip() {
        let bytes = round_trip(Release::V117, ByteOrder::LittleEndian, |writer| {
            writer.write_long_string(&text(1, 1, "hello"))
        });
        let entries = read_back(bytes);
        assert_eq!(
            entries,
            vec![OwnedLongString {
                variable: 1,
                observation: 1,
                binary: false,
                data: b"hello".to_vec(),
            }],
        );
    }

    #[test]
    fn xml_v117_big_endian_round_trip() {
        let bytes = round_trip(Release::V117, ByteOrder::BigEndian, |writer| {
            writer.write_long_string(&text(3, 7, "endian test"))
        });
        let entries = read_back(bytes);
        assert_eq!(entries[0].variable, 3);
        assert_eq!(entries[0].observation, 7);
        assert_eq!(entries[0].data, b"endian test");
    }

    #[test]
    fn xml_v117_binary_payload_round_trip() {
        let payload: &[u8] = b"\x00\x01\x02\x80\xFF";
        let bytes = round_trip(Release::V117, ByteOrder::LittleEndian, |writer| {
            writer.write_long_string(&binary(1, 1, payload))
        });
        let entries = read_back(bytes);
        assert!(entries[0].binary);
        assert_eq!(entries[0].data, payload);
    }

    #[test]
    fn xml_v117_multiple_entries_round_trip() {
        let bytes = round_trip(Release::V117, ByteOrder::LittleEndian, |writer| {
            writer.write_long_string(&text(1, 1, "first"))?;
            writer.write_long_string(&text(1, 2, "second"))?;
            writer.write_long_string(&text(2, 1, "third"))
        });
        let entries = read_back(bytes);
        assert_eq!(entries.len(), 3);
        assert_eq!(entries[0].data, b"first");
        assert_eq!(entries[1].data, b"second");
        assert_eq!(entries[2].data, b"third");
    }

    #[test]
    fn xml_v117_empty_section_still_emits_tags() {
        // No writes at all — `<strls></strls>` must still land on
        // disk so the reader's `expect_bytes(b"<strls>")` succeeds.
        let bytes = round_trip(Release::V117, ByteOrder::LittleEndian, |_| Ok(()));
        let entries = read_back(bytes);
        assert!(entries.is_empty());
    }

    // -- V118 round-trips (wider observation field) -------------------------

    #[test]
    fn xml_v118_u64_observation_round_trip() {
        // V118 stores the GSO observation as u64; give it a value
        // that wouldn't fit u32 to exercise the wide path.
        let bytes = round_trip(Release::V118, ByteOrder::LittleEndian, |writer| {
            writer.write_long_string(&text(1, 5_000_000_000, "wide obs"))
        });
        let entries = read_back(bytes);
        assert_eq!(entries[0].observation, 5_000_000_000);
        assert_eq!(entries[0].data, b"wide obs");
    }

    // -- write_long_string_table --------------------------------------------

    #[test]
    fn write_long_string_table_round_trip() {
        // Passes `&str` directly — `From<&str> for LongStringContent`
        // wraps it as a `Text` variant.
        let mut table = LongStringTable::for_writing();
        table.get_or_insert(1, 1, "apple");
        table.get_or_insert(1, 2, "banana");
        table.get_or_insert(2, 1, "carrot");
        // Duplicate payload must not produce a second entry.
        let duplicate_ref = table.get_or_insert(99, 99, "apple");
        assert_eq!(duplicate_ref.variable(), 1);
        assert_eq!(duplicate_ref.observation(), 1);
        assert_eq!(table.len(), 3);

        let bytes = round_trip(Release::V117, ByteOrder::LittleEndian, |writer| {
            writer.write_long_string_table(&table)
        });
        let entries = read_back(bytes);
        // Entries come back in (variable, observation) order.
        assert_eq!(entries.len(), 3);
        assert_eq!(entries[0].data, b"apple");
        assert_eq!(entries[0].variable, 1);
        assert_eq!(entries[0].observation, 1);
        assert_eq!(entries[1].data, b"banana");
        assert_eq!(entries[2].data, b"carrot");
        assert_eq!(entries[2].variable, 2);
    }

    #[test]
    fn write_long_string_table_empty_on_v117_round_trip() {
        let table = LongStringTable::for_writing();
        let bytes = round_trip(Release::V117, ByteOrder::LittleEndian, |writer| {
            writer.write_long_string_table(&table)
        });
        let entries = read_back(bytes);
        assert!(entries.is_empty());
    }

    // -- Pre-117 rejection --------------------------------------------------

    fn v114_long_string_writer() -> LongStringWriter<Cursor<Vec<u8>>> {
        let schema = Schema::builder()
            .add_variable(Variable::builder(VariableType::Byte, "x").format("%8.0g"))
            .build()
            .unwrap();
        let header = Header::builder(Release::V114, ByteOrder::LittleEndian).build();
        let characteristic_writer = DtaWriter::new()
            .from_writer(Cursor::new(Vec::<u8>::new()))
            .write_header(header)
            .unwrap()
            .write_schema(schema)
            .unwrap();
        let record_writer = characteristic_writer.into_record_writer().unwrap();
        record_writer.into_long_string_writer().unwrap()
    }

    #[test]
    fn pre_v117_rejects_write_long_string() {
        let mut writer = v114_long_string_writer();
        let error = writer
            .write_long_string(&text(1, 1, "anything"))
            .unwrap_err();
        assert!(matches!(
            error,
            DtaError::Format(ref e) if matches!(
                e.kind(),
                FormatErrorKind::LongStringsUnsupported { release: Release::V114 }
            )
        ));
    }

    #[test]
    fn pre_v117_tolerates_empty_long_string_table() {
        let mut writer = v114_long_string_writer();
        let empty = LongStringTable::for_writing();
        // Empty table is a no-op — no iterations, so no error.
        writer.write_long_string_table(&empty).unwrap();
        // Transition should still succeed without emitting any
        // `<strls>` bytes.
        let _value_label_writer = writer.into_value_label_writer().unwrap();
    }

    #[test]
    fn pre_v117_rejects_write_long_string_table_with_entries() {
        let mut writer = v114_long_string_writer();
        let mut table = LongStringTable::for_writing();
        table.get_or_insert(1, 1, LongStringContent::Text(Cow::Borrowed(b"x")));
        let error = writer.write_long_string_table(&table).unwrap_err();
        assert!(matches!(
            error,
            DtaError::Format(ref e) if matches!(
                e.kind(),
                FormatErrorKind::LongStringsUnsupported { release: Release::V114 }
            )
        ));
    }

    // -- V117 observation overflow ------------------------------------------

    #[test]
    fn v117_observation_exceeds_u32_errors() {
        let mut writer = round_trip_writer(Release::V117, ByteOrder::LittleEndian);
        let big_observation = u64::from(u32::MAX) + 1;
        let error = writer
            .write_long_string(&text(1, big_observation, "oops"))
            .unwrap_err();
        assert!(matches!(
            error,
            DtaError::Format(ref e) if matches!(
                e.kind(),
                FormatErrorKind::FieldTooLarge { field: Field::ObservationCount, .. }
            )
        ));
    }

    fn round_trip_writer(
        release: Release,
        byte_order: ByteOrder,
    ) -> LongStringWriter<Cursor<Vec<u8>>> {
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
        record_writer.into_long_string_writer().unwrap()
    }
}
