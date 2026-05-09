use std::io::{Read, Seek};

use super::characteristic_reader::CharacteristicReader;
use super::dta_error::{DtaError, FormatErrorKind, Result, Section};
use super::header::Header;
use super::lazy_record::LazyRecord;
use super::long_string_reader::LongStringReader;
use super::reader_state::ReaderState;
use super::record::Record;
use super::record_parse::{CLOSING_TAG, OPENING_TAG, compute_record_seek_target, parse_row};
use super::schema::Schema;
use super::value_label_reader::ValueLabelReader;

/// Reads observation records from the data section of a DTA file.
///
/// Owns the parsed [`Header`] and [`Schema`] from previous
/// phases. Yields rows of [`Value`](super::value::Value) via
/// iteration, then transitions to value-label reading.
#[derive(Debug)]
pub struct RecordReader<R> {
    state: ReaderState<R>,
    header: Header,
    schema: Schema,
    remaining_observations: u64,
    opened: bool,
    completed: bool,
}

impl<R> RecordReader<R> {
    #[must_use]
    pub(crate) fn new(state: ReaderState<R>, header: Header, schema: Schema) -> Self {
        let remaining_observations = header.observation_count();
        Self {
            state,
            header,
            schema,
            remaining_observations,
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
// Sequential reading (Read)
// ---------------------------------------------------------------------------

impl<R: Read> RecordReader<R> {
    /// Reads the next observation, eagerly parsing all values.
    ///
    /// Returns `None` when all observations have been consumed.
    /// The returned [`Record`] borrows string data from the
    /// reader's internal buffer, so it must be dropped before the
    /// next call.
    ///
    /// # Errors
    ///
    /// Returns [`DtaError::Io`] on read failures and
    /// [`DtaError::Format`] when the row bytes violate the DTA
    /// format specification.
    pub fn read_record(&mut self) -> Result<Option<Record<'_>>> {
        if !self.read_next_row()? {
            return Ok(None);
        }

        let byte_order = self.header.byte_order();
        let release = self.header.release();
        let encoding = self.state.encoding();
        let row_bytes = self.state.buffer();
        let values = parse_row(row_bytes, &self.schema, byte_order, release, encoding)?;
        let record = Record::new(values);
        Ok(Some(record))
    }

    /// Reads the next observation without parsing individual values.
    ///
    /// Returns `None` when all observations have been consumed.
    /// The returned [`LazyRecord`] holds the raw row bytes and
    /// decodes values on demand via
    /// [`LazyRecord::value`]. This avoids parsing columns that
    /// are never accessed.
    ///
    /// # Errors
    ///
    /// Returns [`DtaError::Io`] on read failures.
    pub fn read_lazy_record(&mut self) -> Result<Option<LazyRecord<'_>>> {
        if !self.read_next_row()? {
            return Ok(None);
        }

        let record = LazyRecord::new(
            self.state.buffer(),
            self.schema.variables(),
            self.header.release(),
            self.header.byte_order(),
            self.state.encoding(),
        );
        Ok(Some(record))
    }

    /// Skips all remaining data records without processing them.
    ///
    /// This is required before calling
    /// [`into_long_string_reader`](Self::into_long_string_reader) on
    /// a non-seekable reader. All records must be consumed or skipped
    /// before transitioning to the next section.
    ///
    /// # Errors
    ///
    /// Returns [`DtaError::Io`] on read failures and
    /// [`DtaError::Format`] if the closing `</data>` tag (XML
    /// formats) is missing or malformed.
    pub fn skip_to_end(&mut self) -> Result<()> {
        if self.completed {
            return Ok(());
        }

        self.read_opening_tag()?;

        let row_len = self.schema.row_len();
        while self.remaining_observations > 0 {
            self.state.skip(row_len, Section::Records)?;
            self.remaining_observations -= 1;
        }

        self.read_closing_tag()?;
        self.completed = true;
        Ok(())
    }

    /// Transitions to long-string reading.
    ///
    /// For formats that do not support long strings (pre-117),
    /// the returned reader immediately yields `None` from
    /// [`read_long_string`](LongStringReader::read_long_string).
    /// All data records must have been consumed or skipped (via
    /// [`skip_to_end`](Self::skip_to_end)) before calling this
    /// method.
    ///
    /// # Errors
    ///
    /// Returns [`DtaError::Io`] if the data section has not been
    /// fully consumed.
    pub fn into_long_string_reader(self) -> Result<LongStringReader<R>> {
        if !self.completed {
            let error = DtaError::io(
                Section::Records,
                std::io::Error::other(
                    "data section must be fully consumed \
                     before transitioning to long-string reading",
                ),
            );
            return Err(error);
        }
        let reader = LongStringReader::new(self.state, self.header, self.schema);
        Ok(reader)
    }
}

// ---------------------------------------------------------------------------
// Internal helpers
// ---------------------------------------------------------------------------

impl<R: Read> RecordReader<R> {
    /// Reads the next row's bytes into the internal buffer.
    ///
    /// Returns `true` if a row was read, `false` if all observations
    /// have been consumed (also handles closing tag and sets
    /// `completed`).
    fn read_next_row(&mut self) -> Result<bool> {
        if self.completed {
            return Ok(false);
        }

        self.read_opening_tag()?;

        if self.remaining_observations == 0 {
            self.read_closing_tag()?;
            self.completed = true;
            return Ok(false);
        }

        let row_len = self.schema.row_len();
        self.state.read_exact(row_len, Section::Records)?;
        self.remaining_observations -= 1;

        Ok(true)
    }

    /// Reads the `<data>` opening tag for XML formats on first access.
    fn read_opening_tag(&mut self) -> Result<()> {
        if self.opened {
            return Ok(());
        }
        self.opened = true;
        if self.header.release().is_xml_like() {
            self.state.expect_bytes(
                OPENING_TAG,
                Section::Records,
                FormatErrorKind::InvalidMagic,
            )?;
        }
        Ok(())
    }

    /// Reads the `</data>` closing tag for XML formats.
    fn read_closing_tag(&mut self) -> Result<()> {
        if self.header.release().is_xml_like() {
            self.state.expect_bytes(
                CLOSING_TAG,
                Section::Records,
                FormatErrorKind::InvalidMagic,
            )?;
        }
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Seek-based navigation (Read + Seek)
// ---------------------------------------------------------------------------

impl<R: Read + Seek> RecordReader<R> {
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

    /// Seeks to the start of the data section.
    ///
    /// # Errors
    ///
    /// Returns [`DtaError::Io`] if the section offsets have not been
    /// initialized or if the seek fails.
    pub fn seek_records(mut self) -> Result<Self> {
        let offset = self
            .state
            .section_offsets()
            .ok_or_else(|| DtaError::missing_section_offsets(Section::Records))?
            .records();
        self.state.seek_to(offset, Section::Records)?;
        let reader = Self::new(self.state, self.header, self.schema);
        Ok(reader)
    }

    /// Jumps to a specific observation by 0-based index.
    ///
    /// Records have a fixed byte width, so the target offset is
    /// computed as `data_start + index * row_len` and reached with a
    /// single seek. Subsequent calls to
    /// [`read_record`](Self::read_record) /
    /// [`read_lazy_record`](Self::read_lazy_record) resume from that
    /// observation.
    ///
    /// `index == observation_count` is valid and leaves the reader in
    /// the same state as if every record had been consumed
    /// sequentially: for XML formats (117+) the `</data>` closing tag
    /// is read and validated, and the reader is marked completed so
    /// [`into_long_string_reader`](Self::into_long_string_reader) can
    /// be called immediately. Seeking backward from that completed
    /// state revives the reader.
    ///
    /// # Errors
    ///
    /// Returns [`DtaError::Io`] with
    /// [`InvalidInput`](std::io::ErrorKind::InvalidInput) if
    /// `index > observation_count`, and [`DtaError::Io`] if section
    /// offsets are missing or the seek fails. Returns
    /// [`DtaError::Format`] with
    /// [`FieldTooLarge`](FormatErrorKind::FieldTooLarge) if the
    /// target byte offset overflows `u64`, and (for XML formats)
    /// [`DtaError::Format`] with [`FormatErrorKind::InvalidMagic`]
    /// when `index == observation_count` and the bytes at the closing
    /// position are not `</data>`.
    pub fn seek_to_record(&mut self, index: u64) -> Result<()> {
        let observation_count = self.header.observation_count();
        let records_offset = self
            .state
            .section_offsets()
            .ok_or_else(|| DtaError::missing_section_offsets(Section::Records))?
            .records();
        let seek_target = compute_record_seek_target(
            index,
            observation_count,
            records_offset,
            self.schema.row_len(),
            self.header.release().is_xml_like(),
        )?;

        self.state.seek_to(seek_target.target, Section::Records)?;
        self.opened = true;
        self.remaining_observations = observation_count - index;

        if seek_target.at_end_of_data {
            self.read_closing_tag()?;
            self.completed = true;
        } else {
            self.completed = false;
        }

        Ok(())
    }

    /// Seeks past remaining data records and transitions to
    /// value-label reading.
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
        let reader = ValueLabelReader::new(self.state, self.header, self.schema);
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
        let offsets = self
            .state
            .section_offsets()
            .ok_or_else(|| DtaError::missing_section_offsets(Section::LongStrings))?;
        if let Some(offset) = offsets.long_strings() {
            self.state.seek_to(offset, Section::LongStrings)?;
        } else {
            // Pre-117 has no strL section. Park the reader at the
            // start of value-labels so the immediately completed
            // LongStringReader transitions cleanly via
            // `into_value_label_reader`.
            let offset = offsets.value_labels();
            self.state.seek_to(offset, Section::ValueLabels)?;
        }
        let reader = LongStringReader::new(self.state, self.header, self.schema);
        Ok(reader)
    }
}

#[cfg(test)]
mod tests {
    use std::io::{Cursor, ErrorKind};

    use super::*;
    use crate::stata::dta::byte_order::ByteOrder;
    use crate::stata::dta::dta_reader::DtaReader;
    use crate::stata::dta::dta_writer::DtaWriter;
    use crate::stata::dta::release::Release;
    use crate::stata::dta::value::Value;
    use crate::stata::dta::variable::Variable;
    use crate::stata::dta::variable_type::VariableType;
    use crate::stata::stata_byte::StataByte;

    /// Writes a DTA file with `row_count` byte rows where row `i` holds
    /// value `i as i8`. The schema is a single `Byte` column.
    fn build_file_with_byte_rows(release: Release, row_count: u8) -> Vec<u8> {
        let schema = Schema::builder()
            .add_variable(Variable::builder(VariableType::Byte, "x").format("%8.0g"))
            .build()
            .unwrap();
        let header = Header::builder(release, ByteOrder::LittleEndian).build();
        let mut record_writer = DtaWriter::new()
            .from_writer(Cursor::new(Vec::<u8>::new()))
            .write_header(header)
            .unwrap()
            .write_schema(schema)
            .unwrap()
            .into_record_writer()
            .unwrap();
        for byte in 0..row_count {
            let signed = i8::try_from(byte).unwrap();
            record_writer
                .write_record(&[Value::Byte(StataByte::Present(signed))])
                .unwrap();
        }
        record_writer
            .into_long_string_writer()
            .unwrap()
            .into_value_label_writer()
            .unwrap()
            .finish()
            .unwrap()
            .into_inner()
    }

    /// Opens a [`RecordReader`] over the given bytes, positioned at the
    /// start of the data section with `opened = false` /
    /// `completed = false`.
    fn record_reader_for(bytes: Vec<u8>) -> RecordReader<Cursor<Vec<u8>>> {
        let mut characteristic_reader = DtaReader::new()
            .from_reader(Cursor::new(bytes))
            .read_header()
            .unwrap()
            .read_schema()
            .unwrap();
        characteristic_reader.skip_to_end().unwrap();
        characteristic_reader.into_record_reader().unwrap()
    }

    fn read_byte(reader: &mut RecordReader<Cursor<Vec<u8>>>) -> Option<i8> {
        let record = reader.read_record().unwrap()?;
        match &record.values()[0] {
            Value::Byte(StataByte::Present(byte)) => Some(*byte),
            other => panic!("expected present byte, got {other:?}"),
        }
    }

    #[test]
    fn seek_to_first_record_xml() {
        let bytes = build_file_with_byte_rows(Release::V117, 5);
        let mut reader = record_reader_for(bytes);
        reader.seek_to_record(0).unwrap();
        assert_eq!(read_byte(&mut reader), Some(0));
        assert_eq!(read_byte(&mut reader), Some(1));
    }

    #[test]
    fn seek_to_middle_record_xml() {
        let bytes = build_file_with_byte_rows(Release::V117, 5);
        let mut reader = record_reader_for(bytes);
        reader.seek_to_record(3).unwrap();
        assert_eq!(read_byte(&mut reader), Some(3));
        assert_eq!(read_byte(&mut reader), Some(4));
        assert_eq!(read_byte(&mut reader), None);
    }

    #[test]
    fn seek_to_middle_record_binary() {
        let bytes = build_file_with_byte_rows(Release::V114, 5);
        let mut reader = record_reader_for(bytes);
        reader.seek_to_record(2).unwrap();
        assert_eq!(read_byte(&mut reader), Some(2));
        assert_eq!(read_byte(&mut reader), Some(3));
        assert_eq!(read_byte(&mut reader), Some(4));
        assert_eq!(read_byte(&mut reader), None);
    }

    #[test]
    fn seek_before_any_read_does_not_require_opening_tag_consumption() {
        // The reader is fresh: `opened = false`. The seek should still
        // land us at the right record without separately consuming
        // `<data>`.
        let bytes = build_file_with_byte_rows(Release::V118, 4);
        let mut reader = record_reader_for(bytes);
        reader.seek_to_record(2).unwrap();
        assert_eq!(read_byte(&mut reader), Some(2));
    }

    #[test]
    fn seek_to_observation_count_marks_completed_xml() {
        let bytes = build_file_with_byte_rows(Release::V117, 3);
        let mut reader = record_reader_for(bytes);
        reader.seek_to_record(3).unwrap();
        assert!(reader.read_record().unwrap().is_none());
        // Transition through the rest of the pipeline must succeed
        // because the reader is in the same state as full sequential
        // consumption.
        let _long_string_reader = reader.into_long_string_reader().unwrap();
    }

    #[test]
    fn seek_to_observation_count_marks_completed_binary() {
        let bytes = build_file_with_byte_rows(Release::V114, 3);
        let mut reader = record_reader_for(bytes);
        reader.seek_to_record(3).unwrap();
        assert!(reader.read_record().unwrap().is_none());
        let _long_string_reader = reader.into_long_string_reader().unwrap();
    }

    #[test]
    fn seek_to_observation_count_on_empty_file() {
        let bytes = build_file_with_byte_rows(Release::V117, 0);
        let mut reader = record_reader_for(bytes);
        reader.seek_to_record(0).unwrap();
        assert!(reader.read_record().unwrap().is_none());
        let _long_string_reader = reader.into_long_string_reader().unwrap();
    }

    #[test]
    fn seek_backward_from_completed_revives_reader() {
        let bytes = build_file_with_byte_rows(Release::V117, 4);
        let mut reader = record_reader_for(bytes);
        // Drain sequentially so completed = true.
        while reader.read_record().unwrap().is_some() {}
        // Jump back to record 1.
        reader.seek_to_record(1).unwrap();
        assert_eq!(read_byte(&mut reader), Some(1));
        assert_eq!(read_byte(&mut reader), Some(2));
        assert_eq!(read_byte(&mut reader), Some(3));
        assert_eq!(read_byte(&mut reader), None);
    }

    #[test]
    fn seek_backward_after_partial_read_xml() {
        let bytes = build_file_with_byte_rows(Release::V117, 4);
        let mut reader = record_reader_for(bytes);
        assert_eq!(read_byte(&mut reader), Some(0));
        assert_eq!(read_byte(&mut reader), Some(1));
        reader.seek_to_record(0).unwrap();
        assert_eq!(read_byte(&mut reader), Some(0));
    }

    #[test]
    fn seek_past_end_returns_invalid_input() {
        let bytes = build_file_with_byte_rows(Release::V117, 3);
        let mut reader = record_reader_for(bytes);
        let error = reader.seek_to_record(4).unwrap_err();
        match error {
            DtaError::Io { section, source } => {
                assert_eq!(section, Section::Records);
                assert_eq!(source.kind(), ErrorKind::InvalidInput);
            }
            other => panic!("expected DtaError::Io, got {other:?}"),
        }
        // Reader must still be usable for a valid index after the
        // rejected seek.
        reader.seek_to_record(1).unwrap();
        assert_eq!(read_byte(&mut reader), Some(1));
    }

    #[test]
    fn skip_to_end_after_seek_xml() {
        let bytes = build_file_with_byte_rows(Release::V117, 4);
        let mut reader = record_reader_for(bytes);
        reader.seek_to_record(1).unwrap();
        assert_eq!(read_byte(&mut reader), Some(1));
        reader.skip_to_end().unwrap();
        // After skip_to_end the reader is completed and can transition.
        let _long_string_reader = reader.into_long_string_reader().unwrap();
    }

    #[test]
    fn seek_through_bufreader_invalidates_buffer_correctly() {
        // Wrap in std::io::BufReader to exercise the path where seeking
        // must invalidate an outer read-ahead buffer in addition to the
        // ReaderState's row buffer.
        let bytes = build_file_with_byte_rows(Release::V117, 6);
        let cursor = Cursor::new(bytes);
        let buffered = std::io::BufReader::new(cursor);
        let mut characteristic_reader = DtaReader::new()
            .from_reader(buffered)
            .read_header()
            .unwrap()
            .read_schema()
            .unwrap();
        characteristic_reader.skip_to_end().unwrap();
        let mut reader = characteristic_reader.into_record_reader().unwrap();

        // Touch row 0 so the BufReader has prefetched ahead, then jump
        // to row 5. If the buffer were not invalidated correctly we'd
        // see row 1 instead.
        let record = reader.read_record().unwrap().unwrap();
        match &record.values()[0] {
            Value::Byte(StataByte::Present(0)) => {}
            other => panic!("expected byte 0, got {other:?}"),
        }
        drop(record);

        reader.seek_to_record(5).unwrap();
        let record = reader.read_record().unwrap().unwrap();
        match &record.values()[0] {
            Value::Byte(StataByte::Present(5)) => {}
            other => panic!("expected byte 5, got {other:?}"),
        }
    }
}
