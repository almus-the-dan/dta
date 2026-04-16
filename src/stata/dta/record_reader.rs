use std::io::{BufRead, Seek};

use super::characteristic_reader::CharacteristicReader;
use super::dta_error::{DtaError, FormatErrorKind, Result, Section};
use super::header::Header;
use super::lazy_record::LazyRecord;
use super::long_string_reader::LongStringReader;
use super::reader_state::ReaderState;
use super::record::Record;
use super::schema::Schema;
use super::value::Value;
use super::value_label_reader::ValueLabelReader;

/// Reads observation records from the data section of a DTA file.
///
/// Owns the parsed [`Header`] and [`Schema`] from previous
/// phases. Yields rows of [`Value`](Value) via
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
}

// ---------------------------------------------------------------------------
// Sequential reading (BufRead)
// ---------------------------------------------------------------------------

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
        if !self.read_next_row()? {
            return Ok(None);
        }

        let byte_order = self.header.byte_order();
        let release = self.header.release();
        let encoding = self.state.encoding();
        let row_bytes = self.state.buffer();
        let variables = self.schema.variables();

        let mut values = Vec::with_capacity(variables.len());
        for variable in variables {
            let offset = variable.offset();
            let width = variable.variable_type().width();
            let column_bytes = &row_bytes[offset..offset + width];
            let value = Value::from_column_bytes(
                column_bytes,
                variable.variable_type(),
                byte_order,
                release,
                encoding,
            )?;
            values.push(value);
        }

        Ok(Some(Record::new(values)))
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

        Ok(Some(LazyRecord::new(
            self.state.buffer(),
            self.schema.variables(),
            self.header.release(),
            self.header.byte_order(),
            self.state.encoding(),
        )))
    }

    /// Skips all remaining data records without processing them.
    ///
    /// This is required before calling
    /// [`into_value_label_reader`](Self::into_value_label_reader) on
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
            return Err(DtaError::io(
                Section::Records,
                std::io::Error::other(
                    "data section must be fully consumed \
                     before transitioning to long-string reading",
                ),
            ));
        }
        Ok(LongStringReader::new(self.state, self.header, self.schema))
    }
}

// ---------------------------------------------------------------------------
// Internal helpers
// ---------------------------------------------------------------------------

impl<R: BufRead> RecordReader<R> {
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
            self.state
                .expect_bytes(b"<data>", Section::Records, FormatErrorKind::InvalidMagic)?;
        }
        Ok(())
    }

    /// Reads the `</data>` closing tag for XML formats.
    fn read_closing_tag(&mut self) -> Result<()> {
        if self.header.release().is_xml_like() {
            self.state
                .expect_bytes(b"</data>", Section::Records, FormatErrorKind::InvalidMagic)?;
        }
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Seek-based navigation (BufRead + Seek)
// ---------------------------------------------------------------------------

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
