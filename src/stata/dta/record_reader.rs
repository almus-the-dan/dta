use std::io::{BufRead, Seek};

use super::characteristic_reader::CharacteristicReader;
use super::dta_error::{DtaError, Result, Section};
use super::header::Header;
use super::lazy_record::LazyRecord;
use super::long_string_reader::LongStringReader;
use super::reader_state::ReaderState;
use super::record::Record;
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
}

impl<R> RecordReader<R> {
    pub(crate) fn new(state: ReaderState<R>, header: Header, schema: Schema) -> Self {
        Self {
            state,
            header,
            schema,
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

impl<R: BufRead> RecordReader<R> {
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
        todo!()
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
        todo!()
    }

    /// Skips all remaining data records without processing them.
    ///
    /// This is required before calling
    /// [`into_value_label_reader`](Self::into_value_label_reader) on
    /// a non-seekable reader. All records must be consumed or skipped
    /// before transitioning to the next section.
    pub fn skip_to_end(&mut self) -> Result<()> {
        todo!()
    }

    /// Transitions to value-label reading.
    ///
    /// All data records must have been consumed or skipped (via
    /// [`skip_to_end`](Self::skip_to_end)) before calling this
    /// method.
    pub fn into_value_label_reader(self) -> Result<ValueLabelReader<R>> {
        todo!()
    }
}

impl<R: BufRead + Seek> RecordReader<R> {
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
        Ok(CharacteristicReader::new(
            self.state,
            self.header,
            self.schema,
        ))
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
        Ok(Self::new(self.state, self.header, self.schema))
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
        Ok(ValueLabelReader::new(self.state, self.header, self.schema))
    }

    /// Seeks to the long-string section.
    ///
    /// Returns `None` if the format does not have a long-string
    /// section. Because this method consumes `self`, check
    /// [`Release::supports_long_strings`](super::release::Release::supports_long_strings) beforehand to avoid losing
    /// access to the reader.
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
