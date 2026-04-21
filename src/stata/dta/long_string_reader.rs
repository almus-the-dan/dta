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

        let encoding = self.state.encoding();
        let data = self
            .state
            .read_exact(gso_header.data_len, Section::LongStrings)?;

        let long_string = LongString::new(
            gso_header.variable,
            gso_header.observation,
            gso_header.is_binary(),
            Cow::Borrowed(data),
            encoding,
        );
        Ok(Some(long_string))
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
