use std::borrow::Cow;
use std::io::{BufRead, Seek};

use super::byte_order::ByteOrder;
use super::characteristic_reader::CharacteristicReader;
use super::dta_error::{DtaError, FormatErrorKind, Result, Section};
use super::header::Header;
use super::long_string::LongString;
use super::long_string_parse::{
    GSO_SECTION_CLOSE_REST, GsoHeader, GsoTag, classify_gso_tag, long_string_data_len_to_usize,
};
use super::long_string_table::LongStringTable;
use super::reader_state::ReaderState;
use super::record_reader::RecordReader;
use super::schema::Schema;
use super::value_label_reader::ValueLabelReader;

/// Reads long string (strL) entries from a DTA file.
///
/// Only present for format 117+. Owns the parsed [`Header`] and
/// [`Schema`] from previous phases. Yields [`LongString`] entries
/// via iteration.
#[derive(Debug)]
pub struct LongStringReader<R> {
    state: ReaderState<R>,
    header: Header,
    schema: Schema,
    opened: bool,
    completed: bool,
}

impl<R> LongStringReader<R> {
    #[must_use]
    pub(crate) fn new(state: ReaderState<R>, header: Header, schema: Schema) -> Self {
        let completed = !header.release().supports_long_strings();
        Self {
            state,
            header,
            schema,
            opened: false,
            completed,
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

    /// The encoding this reader uses to decode long-string payloads.
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

impl<R: BufRead> LongStringReader<R> {
    /// Reads the next long string (strL / GSO) entry.
    ///
    /// Returns `None` when all entries have been consumed. Each entry
    /// contains the `(variable, observation)` key and the raw data
    /// bytes. Use [`LongString::data_str`] to decode the bytes as a
    /// string, or [`LongString::data`] for raw access.
    ///
    /// # Errors
    ///
    /// Returns [`DtaError::Io`] on read failures and
    /// [`DtaError::Format`] when the entry bytes violate the DTA
    /// format specification.
    pub fn read_long_string(&mut self) -> Result<Option<LongString<'_>>> {
        let Some(gso_header) = self.read_gso_header()? else {
            return Ok(None);
        };

        let data = self
            .state
            .read_exact(gso_header.data_len, Section::LongStrings)?;

        let long_string = LongString::new(
            gso_header.variable,
            gso_header.observation,
            gso_header.is_binary(),
            Cow::Borrowed(data),
        );
        Ok(Some(long_string))
    }

    /// Reads all remaining long-string entries into `table`, keyed by
    /// their on-disk `(variable, observation)` pairs.
    ///
    /// `table` must have been created with
    /// [`LongStringTable::for_reading`] so that
    /// [`get_or_insert`](LongStringTable::get_or_insert) preserves the
    /// file's keys. [`LongStringRef`](super::long_string_ref::LongStringRef)s
    /// from the data section then resolve via
    /// [`LongStringTable::get`]. The reader's internal buffer is
    /// copied into the table, so callers are free to drop the reader
    /// afterward.
    ///
    /// This method drains the reader to completion — after it
    /// returns, `self` is ready for
    /// [`into_value_label_reader`](Self::into_value_label_reader).
    ///
    /// For pre-117 files, which have no strL section, the call is a
    /// no-op.
    ///
    /// # Errors
    ///
    /// Returns [`DtaError::Io`] on read failures and
    /// [`DtaError::Format`] when the entry bytes violate the DTA
    /// format specification.
    pub fn read_remaining_into(&mut self, table: &mut LongStringTable) -> Result<()> {
        while let Some(long_string) = self.read_long_string()? {
            table.get_or_insert(
                long_string.variable(),
                long_string.observation(),
                long_string.data(),
                long_string.is_binary(),
            );
        }
        Ok(())
    }

    /// Skips all remaining long-string entries without processing
    /// them.
    ///
    /// # Errors
    ///
    /// Returns [`DtaError::Io`] on read failures and
    /// [`DtaError::Format`] if section tags are missing or malformed.
    pub fn skip_to_end(&mut self) -> Result<()> {
        if self.completed {
            return Ok(());
        }
        loop {
            let Some(gso_header) = self.read_gso_header()? else {
                return Ok(());
            };
            self.state.skip(gso_header.data_len, Section::LongStrings)?;
        }
    }

    /// Transitions to value-label reading.
    ///
    /// All long-string entries must have been consumed or skipped
    /// (via [`skip_to_end`](Self::skip_to_end)) before calling this
    /// method.
    ///
    /// # Errors
    ///
    /// Returns [`DtaError::Io`] if the long-strings section has not
    /// been fully consumed.
    pub fn into_value_label_reader(self) -> Result<ValueLabelReader<R>> {
        if !self.completed {
            let error = DtaError::io(
                Section::LongStrings,
                std::io::Error::other(
                    "long-strings section must be fully consumed \
                     before transitioning to value-label reading",
                ),
            );
            return Err(error);
        }
        let reader = ValueLabelReader::new(self.state, self.header, self.schema);
        Ok(reader)
    }
}

// ---------------------------------------------------------------------------
// Internal helpers
// ---------------------------------------------------------------------------

impl<R: BufRead> LongStringReader<R> {
    /// Reads the next GSO entry header, or returns `None` at the
    /// `</strls>` closing tag.
    ///
    /// Dispatches between the 117 and 118+ header layouts based on
    /// the file's format version.
    fn read_gso_header(&mut self) -> Result<Option<GsoHeader>> {
        if self.completed {
            return Ok(None);
        }

        self.read_opening_tag()?;

        let position = self.state.position();
        let head = self.state.read_exact(3, Section::LongStrings)?;
        let tag = classify_gso_tag(head).ok_or_else(|| {
            DtaError::format(
                Section::LongStrings,
                position,
                FormatErrorKind::InvalidLongStringEntry,
            )
        })?;
        if let GsoTag::SectionClose = tag {
            self.state.expect_bytes(
                GSO_SECTION_CLOSE_REST,
                Section::LongStrings,
                FormatErrorKind::InvalidMagic,
            )?;
            self.completed = true;
            return Ok(None);
        }

        let byte_order = self.header.byte_order();
        let (variable, observation) = self.read_variable_observation(byte_order)?;
        let gso_type = self.state.read_u8(Section::LongStrings)?;
        let data_len = self.state.read_u32(byte_order, Section::LongStrings)?;
        let data_len = long_string_data_len_to_usize(data_len)?;

        let header = GsoHeader {
            variable,
            observation,
            gso_type,
            data_len,
        };
        Ok(Some(header))
    }

    /// Reads the `(variable, observation)` index pair at the start of
    /// a GSO entry. The variable is always `u32`; the observation
    /// widens to `u64` on V118+ and stays `u32` on V117.
    fn read_variable_observation(&mut self, byte_order: ByteOrder) -> Result<(u32, u64)> {
        let variable = self.state.read_u32(byte_order, Section::LongStrings)?;
        let observation = if self.header.release().supports_extended_observation_count() {
            self.state.read_u64(byte_order, Section::LongStrings)?
        } else {
            u64::from(self.state.read_u32(byte_order, Section::LongStrings)?)
        };
        Ok((variable, observation))
    }

    /// Reads the `<strls>` opening tag on first access.
    fn read_opening_tag(&mut self) -> Result<()> {
        if self.opened {
            return Ok(());
        }
        self.opened = true;
        self.state.expect_bytes(
            b"<strls>",
            Section::LongStrings,
            FormatErrorKind::InvalidMagic,
        )?;
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Seek-based navigation (BufRead + Seek)
// ---------------------------------------------------------------------------

impl<R: BufRead + Seek> LongStringReader<R> {
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

    /// Seeks to the value-label section.
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

    /// Seeks to the start of the long-strings section.
    ///
    /// For formats that do not support long strings (pre-117),
    /// no seek is performed and the reader remains immediately
    /// completed.
    ///
    /// # Errors
    ///
    /// Returns [`DtaError::Io`] if the section offsets have not been
    /// initialized or if the seek fails.
    pub fn seek_long_strings(mut self) -> Result<Self> {
        let long_strings_offset = self
            .state
            .section_offsets()
            .ok_or_else(|| DtaError::missing_section_offsets(Section::LongStrings))?
            .long_strings();
        if let Some(offset) = long_strings_offset {
            self.state.seek_to(offset, Section::LongStrings)?;
        }
        let reader = Self::new(self.state, self.header, self.schema);
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
    use crate::stata::dta::long_string_ref::LongStringRef;
    use crate::stata::dta::release::Release;
    use crate::stata::dta::variable::Variable;
    use crate::stata::dta::variable_type::VariableType;

    fn text(variable: u32, observation: u64, data: &'static str) -> LongString<'static> {
        LongString::new(variable, observation, false, Cow::Borrowed(data.as_bytes()))
    }

    fn build_file_with_long_strings(release: Release, entries: &[LongString<'_>]) -> Vec<u8> {
        let schema = Schema::builder()
            .add_variable(Variable::builder(VariableType::Byte, "x").format("%8.0g"))
            .build()
            .unwrap();
        let header = Header::builder(release, ByteOrder::LittleEndian).build();
        let mut long_string_writer = DtaWriter::new()
            .from_writer(Cursor::new(Vec::<u8>::new()))
            .write_header(header)
            .unwrap()
            .write_schema(schema)
            .unwrap()
            .into_record_writer()
            .unwrap()
            .into_long_string_writer()
            .unwrap();
        for entry in entries {
            long_string_writer.write_long_string(entry).unwrap();
        }
        long_string_writer
            .into_value_label_writer()
            .unwrap()
            .finish()
            .unwrap()
            .into_inner()
    }

    fn long_string_reader_for(bytes: Vec<u8>) -> LongStringReader<Cursor<Vec<u8>>> {
        let mut characteristic_reader = DtaReader::new()
            .from_reader(Cursor::new(bytes))
            .read_header()
            .unwrap()
            .read_schema()
            .unwrap();
        characteristic_reader.skip_to_end().unwrap();
        let mut record_reader = characteristic_reader.into_record_reader().unwrap();
        record_reader.skip_to_end().unwrap();
        record_reader.into_long_string_reader().unwrap()
    }

    #[test]
    fn read_remaining_into_populates_table() {
        let bytes = build_file_with_long_strings(
            Release::V117,
            &[text(1, 1, "alpha"), text(1, 2, "beta"), text(2, 1, "gamma")],
        );
        let mut reader = long_string_reader_for(bytes);

        let mut table = LongStringTable::for_reading();
        reader.read_remaining_into(&mut table).unwrap();

        assert_eq!(table.len(), 3);
        assert_eq!(
            table.get(&LongStringRef::new(1, 1)).unwrap().data(),
            b"alpha"
        );
        assert_eq!(
            table.get(&LongStringRef::new(1, 2)).unwrap().data(),
            b"beta"
        );
        assert_eq!(
            table.get(&LongStringRef::new(2, 1)).unwrap().data(),
            b"gamma"
        );
    }

    #[test]
    fn read_remaining_into_preserves_wide_v118_observations() {
        let bytes =
            build_file_with_long_strings(Release::V118, &[text(1, 5_000_000_000, "wide obs")]);
        let mut reader = long_string_reader_for(bytes);

        let mut table = LongStringTable::for_reading();
        reader.read_remaining_into(&mut table).unwrap();

        let reference = LongStringRef::new(1, 5_000_000_000);
        assert_eq!(table.get(&reference).unwrap().data(), b"wide obs");
    }

    #[test]
    fn read_remaining_into_is_noop_on_pre_117_file() {
        let bytes = build_file_with_long_strings(Release::V114, &[]);
        let mut reader = long_string_reader_for(bytes);

        let mut table = LongStringTable::for_reading();
        reader.read_remaining_into(&mut table).unwrap();
        assert!(table.is_empty());
    }

    #[test]
    fn read_remaining_into_after_partial_consumption() {
        let bytes = build_file_with_long_strings(
            Release::V117,
            &[text(1, 1, "alpha"), text(1, 2, "beta"), text(2, 1, "gamma")],
        );
        let mut reader = long_string_reader_for(bytes);

        // Consume the first entry manually to prove read_remaining_into
        // picks up from wherever the reader currently is.
        let first = reader.read_long_string().unwrap().unwrap();
        assert_eq!(first.data(), b"alpha");
        drop(first);

        let mut table = LongStringTable::for_reading();
        reader.read_remaining_into(&mut table).unwrap();
        assert_eq!(table.len(), 2);
        assert!(table.get(&LongStringRef::new(1, 1)).is_none());
        assert_eq!(
            table.get(&LongStringRef::new(1, 2)).unwrap().data(),
            b"beta"
        );
        assert_eq!(
            table.get(&LongStringRef::new(2, 1)).unwrap().data(),
            b"gamma"
        );
    }

    #[test]
    fn read_remaining_into_allows_transition_to_value_label_reader() {
        let bytes = build_file_with_long_strings(Release::V117, &[text(1, 1, "one")]);
        let mut reader = long_string_reader_for(bytes);

        let mut table = LongStringTable::for_reading();
        reader.read_remaining_into(&mut table).unwrap();
        // Drained reader must be able to transition to the next phase.
        let _value_label_reader = reader.into_value_label_reader().unwrap();
    }

    #[test]
    fn read_remaining_into_appends_to_non_empty_table() {
        let bytes = build_file_with_long_strings(Release::V117, &[text(2, 2, "from file")]);
        let mut reader = long_string_reader_for(bytes);

        let mut table = LongStringTable::for_reading();
        table.get_or_insert(1, 1, b"pre-existing", false);

        reader.read_remaining_into(&mut table).unwrap();
        assert_eq!(table.len(), 2);
        assert_eq!(
            table.get(&LongStringRef::new(1, 1)).unwrap().data(),
            b"pre-existing"
        );
        assert_eq!(
            table.get(&LongStringRef::new(2, 2)).unwrap().data(),
            b"from file"
        );
    }
}
