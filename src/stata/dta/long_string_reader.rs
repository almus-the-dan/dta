use std::io::{BufRead, Seek};

use super::characteristic_reader::CharacteristicReader;
use super::dta_error::{DtaError, Result, Section};
use super::header::Header;
use super::reader_state::ReaderState;
use super::record_reader::RecordReader;
use super::schema::Schema;
use super::value_label_reader::ValueLabelReader;

/// Reads long string (strL) entries from a DTA file.
///
/// Only present for format 118+. Owns the parsed [`Header`] and
/// [`Schema`] from previous phases. Yields
/// [`LongString`](super::long_string::LongString) entries via iteration.
#[derive(Debug)]
pub struct LongStringReader<R> {
    state: ReaderState<R>,
    header: Header,
    schema: Schema,
}

impl<R> LongStringReader<R> {
    #[must_use]
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

impl<R: BufRead> LongStringReader<R> {
    // TODO: iteration over LongString<'_> entries

    /// Consumes all remaining long-string entries without processing
    /// them.
    pub fn read_to_end(&mut self) -> Result<()> {
        todo!()
    }
}

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
        Ok(CharacteristicReader::new(
            self.state,
            self.header,
            self.schema,
        ))
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
        Ok(RecordReader::new(self.state, self.header, self.schema))
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
        Ok(ValueLabelReader::new(self.state, self.header, self.schema))
    }

    /// Seeks to the start of the long-strings section.
    ///
    /// # Errors
    ///
    /// Returns [`DtaError::Io`] if the section offsets have not been
    /// initialized or if the seek fails.
    pub fn seek_long_strings(mut self) -> Result<Self> {
        let offset = self
            .state
            .section_offsets()
            .ok_or_else(|| DtaError::missing_section_offsets(Section::LongStrings))?
            .long_strings()
            .ok_or_else(|| DtaError::missing_section_offsets(Section::LongStrings))?;
        self.state.seek_to(offset, Section::LongStrings)?;
        Ok(Self::new(self.state, self.header, self.schema))
    }
}
