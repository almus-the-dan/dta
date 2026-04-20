use std::borrow::Cow;
use std::io::{BufRead, Seek};

use super::characteristic_reader::CharacteristicReader;
use super::dta_error::{DtaError, FormatErrorKind, Result, Section};
use super::header::Header;
use super::long_string::{GsoType, LongString};
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
            .read_exact(gso_header.data_len(), Section::LongStrings)?;

        Ok(Some(LongString::new(
            gso_header.variable(),
            gso_header.observation(),
            gso_header.is_binary(),
            Cow::Borrowed(data),
            encoding,
        )))
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
            self.state
                .skip(gso_header.data_len(), Section::LongStrings)?;
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
            return Err(DtaError::io(
                Section::LongStrings,
                std::io::Error::other(
                    "long-strings section must be fully consumed \
                     before transitioning to value-label reading",
                ),
            ));
        }
        Ok(ValueLabelReader::new(self.state, self.header, self.schema))
    }
}

// ---------------------------------------------------------------------------
// Internal helpers
// ---------------------------------------------------------------------------

/// Parsed GSO entry header (after the "GSO" magic).
struct GsoHeader {
    variable: u32,
    observation: u64,
    gso_type: u8,
    data_len: usize,
}

impl GsoHeader {
    fn is_binary(&self) -> bool {
        // Non-`Binary`/`Text` bytes are treated as text, matching the
        // previous lenient behavior. Switching to strict rejection of
        // unknown type bytes is an option if real-world files show
        // them.
        GsoType::from_byte(self.gso_type) == Some(GsoType::Binary)
    }

    fn variable(&self) -> u32 {
        self.variable
    }

    fn observation(&self) -> u64 {
        self.observation
    }

    fn data_len(&self) -> usize {
        self.data_len
    }
}

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
        let tag = self.state.read_exact(3, Section::LongStrings)?;

        match tag {
            b"GSO" => {}
            b"</s" => {
                self.state.expect_bytes(
                    b"trls>",
                    Section::LongStrings,
                    FormatErrorKind::InvalidMagic,
                )?;
                self.completed = true;
                return Ok(None);
            }
            _ => {
                return Err(DtaError::format(
                    Section::LongStrings,
                    position,
                    FormatErrorKind::InvalidLongStringEntry,
                ));
            }
        }

        let byte_order = self.header.byte_order();
        let extended = self.header.release().supports_extended_observation_count();

        let (variable, observation) = if extended {
            // Format 118+: v = u32 (4 bytes), o = u64 (8 bytes)
            let v = self.state.read_u32(byte_order, Section::LongStrings)?;
            let o = self.state.read_u64(byte_order, Section::LongStrings)?;
            (v, o)
        } else {
            // Format 117: v = u32 (4 bytes), o = u32 (4 bytes)
            let v = self.state.read_u32(byte_order, Section::LongStrings)?;
            let o = self.state.read_u32(byte_order, Section::LongStrings)?;
            (v, u64::from(o))
        };

        let gso_type = self.state.read_u8(Section::LongStrings)?;
        let data_len = self.state.read_u32(byte_order, Section::LongStrings)?;
        let data_len = usize::try_from(data_len).map_err(|_| {
            DtaError::io(
                Section::LongStrings,
                std::io::Error::other("long string data length exceeds usize"),
            )
        })?;

        Ok(Some(GsoHeader {
            variable,
            observation,
            gso_type,
            data_len,
        }))
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
        Ok(Self::new(self.state, self.header, self.schema))
    }
}
