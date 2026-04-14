use std::io::{BufRead, Seek};

use super::characteristic::Characteristic;
use super::dta_error::{DtaError, Result, Section};
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
}

impl<R> CharacteristicReader<R> {
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
        todo!()
    }

    /// Skips all remaining characteristic entries without processing
    /// them.
    pub fn skip_to_end(&mut self) -> Result<()> {
        todo!()
    }

    /// Transitions to record reading.
    ///
    /// All characteristic entries must have been consumed (via
    /// [`read_characteristic`](Self::read_characteristic) or
    /// [`skip_to_end`](Self::skip_to_end)) before calling this
    /// method.
    pub fn into_record_reader(self) -> Result<RecordReader<R>> {
        todo!()
    }
}

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
