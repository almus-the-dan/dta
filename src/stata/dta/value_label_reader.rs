use std::io::{BufRead, Seek};

use super::byte_order::ByteOrder;
use super::characteristic_reader::CharacteristicReader;
use super::dta_error::{DtaError, Field, FormatErrorKind, Result, Section};
use super::header::Header;
use super::long_string_reader::LongStringReader;
use super::reader_state::ReaderState;
use super::record_reader::RecordReader;
use super::schema::Schema;
use super::value_label::{ValueLabelEntry, ValueLabelTable};

/// Reads value-label tables from a DTA file.
///
/// Owns the parsed [`Header`] and [`Schema`] from previous
/// phases. Yields [`ValueLabelTable`] entries via iteration, then
/// optionally transitions to long-string reading.
#[derive(Debug)]
pub struct ValueLabelReader<R> {
    state: ReaderState<R>,
    header: Header,
    schema: Schema,
    opened: bool,
    completed: bool,
}

impl<R> ValueLabelReader<R> {
    #[must_use]
    pub(crate) fn new(state: ReaderState<R>, header: Header, schema: Schema) -> Self {
        Self {
            state,
            header,
            schema,
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

impl<R: BufRead> ValueLabelReader<R> {
    /// Reads the next value-label table.
    ///
    /// Returns `None` when all tables have been consumed. Each table
    /// contains a name and a set of integer-to-string mappings.
    ///
    /// # Errors
    ///
    /// Returns [`DtaError::Io`] on read failures and
    /// [`DtaError::Format`] when the table bytes violate the DTA
    /// format specification.
    pub fn read_value_label_table(&mut self) -> Result<Option<ValueLabelTable>> {
        if self.completed {
            return Ok(None);
        }
        if self.header.release().has_old_value_labels() {
            self.read_old_table()
        } else {
            self.read_modern_table()
        }
    }

    /// Skips all remaining value-label entries without processing
    /// them.
    ///
    /// # Errors
    ///
    /// Returns [`DtaError::Io`] on read failures and
    /// [`DtaError::Format`] if section tags (XML formats) are
    /// missing or malformed.
    pub fn skip_to_end(&mut self) -> Result<()> {
        if self.completed {
            return Ok(());
        }
        if self.header.release().has_old_value_labels() {
            while self.skip_old_table()? {}
        } else {
            while self.skip_modern_table()? {}
        }
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Shared helpers
// ---------------------------------------------------------------------------

impl<R: BufRead> ValueLabelReader<R> {
    /// Reads the table name and skips the trailing padding bytes.
    fn read_table_name(&mut self) -> Result<String> {
        let release = self.header.release();
        let encoding = self.state.encoding();
        let name = self.state.read_fixed_string(
            release.value_label_name_len(),
            encoding,
            Section::ValueLabels,
            Field::ValueLabelName,
        )?;
        self.state.skip(
            release.value_label_table_padding_len(),
            Section::ValueLabels,
        )?;
        Ok(name)
    }

    /// Skips the table name and trailing padding bytes without
    /// decoding.
    fn skip_table_name(&mut self) -> Result<()> {
        let release = self.header.release();
        let skip_len = release.value_label_name_len() + release.value_label_table_padding_len();
        self.state.skip(skip_len, Section::ValueLabels)
    }
}

/// Decodes a null-terminated label from raw bytes using the given
/// encoding. `max_len` caps the search for the null terminator.
fn decode_label(
    bytes: &[u8],
    max_len: usize,
    encoding: &'static encoding_rs::Encoding,
) -> Result<String> {
    let bounded = &bytes[..bytes.len().min(max_len)];
    let end = bounded
        .iter()
        .position(|&b| b == 0)
        .unwrap_or(bounded.len());
    encoding
        .decode_without_bom_handling_and_without_replacement(&bounded[..end])
        .map(std::borrow::Cow::into_owned)
        .ok_or_else(|| {
            DtaError::io(
                Section::ValueLabels,
                std::io::Error::other("invalid string encoding in value label"),
            )
        })
}

fn overflow_error() -> DtaError {
    DtaError::io(
        Section::ValueLabels,
        std::io::Error::other("value label table size overflow"),
    )
}

// ---------------------------------------------------------------------------
// Old value labels (format 104)
// ---------------------------------------------------------------------------

impl<R: BufRead> ValueLabelReader<R> {
    /// Reads and parses one table in the old (pre-105) layout.
    fn read_old_table(&mut self) -> Result<Option<ValueLabelTable>> {
        let Some(table_len) = self.read_old_table_header()? else {
            return Ok(None);
        };

        let name = self.read_table_name()?;
        let encoding = self.state.encoding();

        let entry_count = table_len / 8;
        let payload = self.state.read_exact(table_len, Section::ValueLabels)?;

        let mut entries = Vec::with_capacity(entry_count);
        for entry_index in 0..entry_count {
            let label_bytes = &payload[8 * entry_index..8 * entry_index + 8];
            if label_bytes[0] == 0 {
                continue;
            }
            let label = decode_label(label_bytes, 8, encoding)?;
            let value = i32::try_from(entry_index).map_err(|_| {
                DtaError::io(
                    Section::ValueLabels,
                    std::io::Error::other("value label index exceeds i32"),
                )
            })?;
            entries.push(ValueLabelEntry::new(value, label));
        }

        Ok(Some(ValueLabelTable::new(name, entries)))
    }

    /// Reads the old-format table header (table length, name, padding).
    /// Returns the payload size in bytes, or `None` at EOF.
    fn read_old_table_header(&mut self) -> Result<Option<usize>> {
        let byte_order = self.header.byte_order();
        let Some(table_len) = self.state.try_read_u16(byte_order, Section::ValueLabels)? else {
            self.completed = true;
            return Ok(None);
        };
        Ok(Some(usize::from(table_len)))
    }

    /// Skips one old-format table. Returns `false` at EOF.
    fn skip_old_table(&mut self) -> Result<bool> {
        let Some(table_len) = self.read_old_table_header()? else {
            return Ok(false);
        };
        self.skip_table_name()?;
        self.state.skip(table_len, Section::ValueLabels)?;
        Ok(true)
    }
}

// ---------------------------------------------------------------------------
// Modern value labels (format 105+)
// ---------------------------------------------------------------------------

impl<R: BufRead> ValueLabelReader<R> {
    /// Reads and parses one table in the modern (105+) layout.
    fn read_modern_table(&mut self) -> Result<Option<ValueLabelTable>> {
        let Some(table_len) = self.read_modern_table_header()? else {
            return Ok(None);
        };

        let name = self.read_table_name()?;
        let byte_order = self.header.byte_order();
        let encoding = self.state.encoding();

        let payload = self.state.read_exact(table_len, Section::ValueLabels)?;
        let table = parse_modern_payload(payload, byte_order, encoding, &name)?;

        self.read_modern_table_footer()?;
        Ok(Some(table))
    }

    /// Reads the modern-format table header (XML tags, table length,
    /// name, padding). Returns the payload size in bytes, or `None`
    /// when the section is exhausted.
    fn read_modern_table_header(&mut self) -> Result<Option<usize>> {
        let byte_order = self.header.byte_order();
        let is_xml = self.header.release().is_xml_like();

        if !self.opened && is_xml {
            self.state.expect_bytes(
                b"<value_labels>",
                Section::ValueLabels,
                FormatErrorKind::InvalidMagic,
            )?;
            self.opened = true;
        }

        if is_xml {
            match self.read_xml_label_or_close()? {
                XmlLabelTag::EntryOpen => {}
                XmlLabelTag::SectionClose => {
                    self.completed = true;
                    return Ok(None);
                }
            }
        }

        let Some(table_len) = self.state.try_read_u32(byte_order, Section::ValueLabels)? else {
            self.completed = true;
            return Ok(None);
        };
        let table_len = usize::try_from(table_len).map_err(|_| overflow_error())?;
        Ok(Some(table_len))
    }

    /// Reads the closing `</lbl>` tag if this is an XML format.
    fn read_modern_table_footer(&mut self) -> Result<()> {
        if self.header.release().is_xml_like() {
            self.state.expect_bytes(
                b"</lbl>",
                Section::ValueLabels,
                FormatErrorKind::InvalidMagic,
            )?;
        }
        Ok(())
    }

    /// Skips one modern-format table. Returns `false` when the section
    /// is exhausted.
    fn skip_modern_table(&mut self) -> Result<bool> {
        let Some(table_len) = self.read_modern_table_header()? else {
            return Ok(false);
        };
        self.skip_table_name()?;
        self.state.skip(table_len, Section::ValueLabels)?;
        self.read_modern_table_footer()?;
        Ok(true)
    }

    /// Reads the next XML tag in the value-labels section,
    /// distinguishing `<lbl>` from `</value_labels>`.
    fn read_xml_label_or_close(&mut self) -> Result<XmlLabelTag> {
        let position = self.state.position();
        let tag_bytes = self.state.read_exact(5, Section::ValueLabels)?;

        match tag_bytes {
            b"<lbl>" => Ok(XmlLabelTag::EntryOpen),
            b"</val" => {
                self.state.expect_bytes(
                    b"ue_labels>",
                    Section::ValueLabels,
                    FormatErrorKind::InvalidMagic,
                )?;
                Ok(XmlLabelTag::SectionClose)
            }
            _ => Err(DtaError::format(
                Section::ValueLabels,
                position,
                FormatErrorKind::InvalidMagic,
            )),
        }
    }
}

enum XmlLabelTag {
    EntryOpen,
    SectionClose,
}

/// Parses the modern value-label payload:
/// `n` (u32), `txtlen` (u32), `off[n]` (u32 each), `val[n]` (i32 each),
/// `txt[txtlen]`.
fn parse_modern_payload(
    payload: &[u8],
    byte_order: ByteOrder,
    encoding: &'static encoding_rs::Encoding,
    table_name: &str,
) -> Result<ValueLabelTable> {
    if payload.len() < 8 {
        return Err(DtaError::format(
            Section::ValueLabels,
            0,
            FormatErrorKind::Truncated {
                expected: 8,
                actual: u64::try_from(payload.len()).unwrap_or(u64::MAX),
            },
        ));
    }

    let entry_count = byte_order.read_u32([payload[0], payload[1], payload[2], payload[3]]);
    let text_len = byte_order.read_u32([payload[4], payload[5], payload[6], payload[7]]);

    let entry_count_usize = usize::try_from(entry_count).map_err(|_| overflow_error())?;
    let text_len_usize = usize::try_from(text_len).map_err(|_| overflow_error())?;

    // Validate payload length: 8 (header) + 4*n (offsets) + 4*n (values) + txt length
    let expected_len = 8usize
        .checked_add(
            entry_count_usize
                .checked_mul(8)
                .ok_or_else(overflow_error)?,
        )
        .and_then(|v| v.checked_add(text_len_usize))
        .ok_or_else(overflow_error)?;

    if payload.len() < expected_len {
        return Err(DtaError::format(
            Section::ValueLabels,
            0,
            FormatErrorKind::Truncated {
                expected: u64::try_from(expected_len).unwrap_or(u64::MAX),
                actual: u64::try_from(payload.len()).unwrap_or(u64::MAX),
            },
        ));
    }

    let offsets_start = 8;
    let values_start = offsets_start + 4 * entry_count_usize;
    let text_start = values_start + 4 * entry_count_usize;

    let mut entries = Vec::with_capacity(entry_count_usize);
    for entry_index in 0..entry_count_usize {
        let offset_position = offsets_start + 4 * entry_index;
        let text_offset = byte_order.read_u32([
            payload[offset_position],
            payload[offset_position + 1],
            payload[offset_position + 2],
            payload[offset_position + 3],
        ]);
        let text_offset_usize = usize::try_from(text_offset).map_err(|_| overflow_error())?;

        if text_offset_usize >= text_len_usize {
            return Err(DtaError::format(
                Section::ValueLabels,
                0,
                FormatErrorKind::Truncated {
                    expected: u64::from(text_offset) + 1,
                    actual: u64::from(text_len),
                },
            ));
        }

        let value_position = values_start + 4 * entry_index;
        let raw_value = byte_order.read_u32([
            payload[value_position],
            payload[value_position + 1],
            payload[value_position + 2],
            payload[value_position + 3],
        ]);
        let value = i32::from_ne_bytes(raw_value.to_ne_bytes());

        let label_bytes = &payload[text_start + text_offset_usize..];
        let label = decode_label(label_bytes, text_len_usize - text_offset_usize, encoding)?;

        entries.push(ValueLabelEntry::new(value, label));
    }

    Ok(ValueLabelTable::new(table_name.to_owned(), entries))
}

// ---------------------------------------------------------------------------
// Seek-based navigation (BufRead + Seek)
// ---------------------------------------------------------------------------

impl<R: BufRead + Seek> ValueLabelReader<R> {
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

    /// Seeks to the start of the value-labels section.
    ///
    /// # Errors
    ///
    /// Returns [`DtaError::Io`] if the section offsets have not been
    /// initialized or if the seek fails.
    pub fn seek_value_labels(mut self) -> Result<Self> {
        let offset = self
            .state
            .section_offsets()
            .ok_or_else(|| DtaError::missing_section_offsets(Section::ValueLabels))?
            .value_labels();
        self.state.seek_to(offset, Section::ValueLabels)?;
        Ok(Self::new(self.state, self.header, self.schema))
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
        let long_strings_offset = self
            .state
            .section_offsets()
            .ok_or_else(|| DtaError::missing_section_offsets(Section::LongStrings))?
            .long_strings();
        if let Some(offset) = long_strings_offset {
            self.state.seek_to(offset, Section::LongStrings)?;
        }
        Ok(LongStringReader::new(self.state, self.header, self.schema))
    }
}
