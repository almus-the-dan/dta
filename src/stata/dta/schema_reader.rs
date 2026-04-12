use std::io::{BufRead, Read, Seek};

use super::byte_order::ByteOrder;
use super::dta_error::{DtaError, Field, FormatErrorKind, Result, Section};
use super::header::Header;
use super::reader_state::ReaderState;
use super::record_reader::RecordReader;
use super::release::Release;
use super::schema::Schema;
use super::variable::Variable;
use super::variable_type::VariableType;

/// Reads variable definitions from a DTA file.
///
/// Owns the parsed [`Header`] from the previous phase. Call
/// [`read_schema`](Self::read_schema) to parse variable definitions
/// and advance to data reading.
#[derive(Debug)]
pub struct SchemaReader<R> {
    state: ReaderState<R>,
    header: Header,
}

impl<R> SchemaReader<R> {
    pub(crate) fn new(state: ReaderState<R>, header: Header) -> Self {
        Self { state, header }
    }

    /// The parsed file header.
    #[must_use]
    #[inline]
    pub fn header(&self) -> &Header {
        &self.header
    }
}

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

impl<R: BufRead + Seek> SchemaReader<R> {
    /// Parses variable definitions and transitions to data reading.
    ///
    /// Reads type codes, variable names, sort order, display formats,
    /// value-label associations, and variable labels. For XML formats,
    /// reads the section map and seeks past characteristics. For binary
    /// formats, reads through expansion fields sequentially.
    ///
    /// # Errors
    ///
    /// Returns [`DtaError::Io`] on read failures and
    /// [`DtaError::Format`] when the schema bytes violate the DTA
    /// format specification.
    pub fn read_schema(mut self) -> Result<RecordReader<R>> {
        let release = self.header.release();
        let byte_order = self.header.byte_order();
        let variable_count = usize::try_from(self.header.variable_count())
            .map_err(|_| DtaError::io(Section::Schema, unsupported_variable_count()))?;

        // XML formats store section offsets in a map before the
        // descriptors; binary formats have no map.
        let data_offset = if release.is_xml_like() {
            let offset = self.read_map(byte_order)?;
            Some(offset)
        } else {
            None
        };

        let variable_types = self.read_variable_types(variable_count, release, byte_order)?;
        let variable_names = self.read_variable_names(variable_count, release)?;
        let sort_order = self.read_sort_order(variable_count, release, byte_order)?;
        let formats = self.read_formats(variable_count, release)?;
        let value_label_names = self.read_value_label_names(variable_count, release)?;
        let variable_labels = self.read_variable_labels(variable_count, release)?;

        // Position the stream at the start of the data section.
        if let Some(offset) = data_offset {
            // XML: the map told us where <data> lives — seek there,
            // skipping the (potentially large) characteristics section.
            self.state.seek_to(offset, Section::Schema)?;
        } else {
            // Binary: expansion fields sit between the descriptors and
            // data.  Read through them to advance to data.
            self.skip_binary_expansion_fields(release, byte_order)?;
        }

        let variables = assemble_variables(
            variable_types,
            variable_names,
            formats,
            value_label_names,
            variable_labels,
        );

        let schema = Schema::builder()
            .variables(variables)
            .sort_order(sort_order)
            .build()?;
        Ok(RecordReader::new(self.state, self.header, schema))
    }
}

// ---------------------------------------------------------------------------
// XML tag handling
// ---------------------------------------------------------------------------

impl<R: Read> SchemaReader<R> {
    /// Validates an expected XML section tag.  No-op for binary formats.
    fn expect_tag(&mut self, tag: &[u8]) -> Result<()> {
        if self.header.release().is_xml_like() {
            self.state
                .expect_bytes(tag, Section::Schema, FormatErrorKind::InvalidMagic)?;
        }
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Map section (XML only, 14 × u64 offsets)
// ---------------------------------------------------------------------------

impl<R: Read> SchemaReader<R> {
    /// Reads the `<map>` section and returns the data-section offset.
    fn read_map(&mut self, byte_order: ByteOrder) -> Result<u64> {
        self.expect_tag(b"<map>")?;

        let buffer = self.state.read_exact(14 * 8, Section::Schema)?;
        let data_offset = read_u64_at(buffer, 9, byte_order);

        self.expect_tag(b"</map>")?;
        Ok(data_offset)
    }
}

// ---------------------------------------------------------------------------
// Descriptor sub-sections
// ---------------------------------------------------------------------------

impl<R: Read> SchemaReader<R> {
    /// Reads the typlist — one type code per variable.
    fn read_variable_types(
        &mut self,
        variable_count: usize,
        release: Release,
        byte_order: ByteOrder,
    ) -> Result<Vec<VariableType>> {
        self.expect_tag(b"<variable_types>")?;

        let entry_len = release.type_list_entry_len();
        let section_start = self.state.position();
        let buffer = self
            .state
            .read_exact(variable_count * entry_len, Section::Schema)?;

        let mut types = Vec::with_capacity(variable_count);
        for i in 0..variable_count {
            let code = if entry_len == 1 {
                u16::from(buffer[i])
            } else {
                let offset = i * 2;
                let bytes = [buffer[offset], buffer[offset + 1]];
                match byte_order {
                    ByteOrder::BigEndian => u16::from_be_bytes(bytes),
                    ByteOrder::LittleEndian => u16::from_le_bytes(bytes),
                }
            };
            let position = offset_position(section_start, i * entry_len)?;
            types.push(parse_type_code(code, release, position)?);
        }

        self.expect_tag(b"</variable_types>")?;
        Ok(types)
    }

    /// Reads the varlist — one fixed-length name per variable.
    fn read_variable_names(
        &mut self,
        variable_count: usize,
        release: Release,
    ) -> Result<Vec<String>> {
        self.read_fixed_string_array(
            variable_count,
            release.variable_name_len(),
            b"<varnames>",
            b"</varnames>",
            Field::VariableName,
        )
    }

    /// Reads the sort-list — (`variable_count` + 1) entries of 1-based
    /// indices terminated by zero.
    fn read_sort_order(
        &mut self,
        variable_count: usize,
        release: Release,
        byte_order: ByteOrder,
    ) -> Result<Vec<u32>> {
        self.expect_tag(b"<sortlist>")?;

        let entry_len = release.sort_entry_len();
        let entry_count = variable_count + 1;
        let buffer = self
            .state
            .read_exact(entry_count * entry_len, Section::Schema)?;

        let mut sort_order = Vec::new();
        for i in 0..entry_count {
            let offset = i * entry_len;
            let index = if entry_len == 2 {
                let bytes = [buffer[offset], buffer[offset + 1]];
                u32::from(match byte_order {
                    ByteOrder::BigEndian => u16::from_be_bytes(bytes),
                    ByteOrder::LittleEndian => u16::from_le_bytes(bytes),
                })
            } else {
                let bytes = [
                    buffer[offset],
                    buffer[offset + 1],
                    buffer[offset + 2],
                    buffer[offset + 3],
                ];
                match byte_order {
                    ByteOrder::BigEndian => u32::from_be_bytes(bytes),
                    ByteOrder::LittleEndian => u32::from_le_bytes(bytes),
                }
            };

            if index == 0 {
                break;
            }
            // File stores 1-based indices; convert to 0-based.
            sort_order.push(index - 1);
        }

        self.expect_tag(b"</sortlist>")?;
        Ok(sort_order)
    }

    /// Reads the fmtlist — one display format per variable.
    fn read_formats(&mut self, variable_count: usize, release: Release) -> Result<Vec<String>> {
        self.read_fixed_string_array(
            variable_count,
            release.format_entry_len(),
            b"<formats>",
            b"</formats>",
            Field::VariableFormat,
        )
    }

    /// Reads the lbllist — one value-label table name per variable.
    fn read_value_label_names(
        &mut self,
        variable_count: usize,
        release: Release,
    ) -> Result<Vec<String>> {
        self.read_fixed_string_array(
            variable_count,
            release.value_label_name_len(),
            b"<value_label_names>",
            b"</value_label_names>",
            Field::ValueLabelName,
        )
    }

    /// Reads one descriptive label per variable.
    fn read_variable_labels(
        &mut self,
        variable_count: usize,
        release: Release,
    ) -> Result<Vec<String>> {
        self.read_fixed_string_array(
            variable_count,
            release.variable_label_len(),
            b"<variable_labels>",
            b"</variable_labels>",
            Field::VariableLabel,
        )
    }
}

// ---------------------------------------------------------------------------
// Fixed-length string array helper
// ---------------------------------------------------------------------------

impl<R: Read> SchemaReader<R> {
    /// Reads `count` null-terminated fixed-length strings from the
    /// stream, optionally wrapped in XML tags.
    fn read_fixed_string_array(
        &mut self,
        count: usize,
        entry_len: usize,
        open_tag: &[u8],
        close_tag: &[u8],
        field: Field,
    ) -> Result<Vec<String>> {
        self.expect_tag(open_tag)?;

        let encoding = self.state.encoding();
        let section_start = self.state.position();
        let buffer = self.state.read_exact(count * entry_len, Section::Schema)?;

        let mut result = Vec::with_capacity(count);
        for i in 0..count {
            let start = i * entry_len;
            let position = offset_position(section_start, start)?;
            let raw = &buffer[start..start + entry_len];
            let null_end = raw.iter().position(|&b| b == 0).unwrap_or(raw.len());
            let decoded = encoding
                .decode_without_bom_handling_and_without_replacement(&raw[..null_end])
                .ok_or_else(|| {
                    DtaError::format(
                        Section::Schema,
                        position,
                        FormatErrorKind::InvalidEncoding { field },
                    )
                })?;
            result.push(decoded.into_owned());
        }

        self.expect_tag(close_tag)?;
        Ok(result)
    }
}

// ---------------------------------------------------------------------------
// Binary expansion fields (pre-117)
// ---------------------------------------------------------------------------

impl<R: Read> SchemaReader<R> {
    /// Reads and discards binary expansion-field entries.
    ///
    /// Each entry is: `[u8 data_type] [u16|u32 length] [length bytes]`.
    /// The section ends when both `data_type` and `length` are zero.
    /// Format 104 has no expansion fields at all.
    fn skip_binary_expansion_fields(
        &mut self,
        release: Release,
        byte_order: ByteOrder,
    ) -> Result<()> {
        let len_width = release.expansion_len_width();
        if len_width == 0 {
            return Ok(());
        }

        loop {
            let data_type = self.state.read_u8(Section::Schema)?;
            let length = if len_width == 2 {
                let length = self.state.read_u16(byte_order, Section::Schema)?;
                u64::from(length)
            } else {
                let length = self.state.read_u32(byte_order, Section::Schema)?;
                u64::from(length)
            };

            if data_type == 0 && length == 0 {
                break;
            }

            let skip_len = usize::try_from(length).map_err(|_| {
                DtaError::format(
                    Section::Schema,
                    self.state.position(),
                    FormatErrorKind::Truncated {
                        expected: length,
                        actual: 0,
                    },
                )
            })?;
            self.state.skip(skip_len, Section::Schema)?;
        }

        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Type code parsing
// ---------------------------------------------------------------------------

/// Converts a raw type code to a [`VariableType`].
///
/// The interpretation depends on the format version:
///
/// | Formats   | Numeric codes          | String codes            |
/// |-----------|------------------------|-------------------------|
/// | 104–110   | ASCII `b/i/l/f/d`      | `≥ 0x7F` → len − 0x7F  |
/// | 111–116   | `0xFB`–`0xFF`          | code = byte length      |
/// | 117+      | `0xFFF6`–`0xFFFA`      | code = byte length      |
/// |           | `0x8000` = strL        |                         |
fn parse_type_code(code: u16, release: Release, position: u64) -> Result<VariableType> {
    if release >= Release::V117 {
        // 2-byte codes (format 117+)
        match code {
            0xFFFA => Ok(VariableType::Byte),
            0xFFF9 => Ok(VariableType::Int),
            0xFFF8 => Ok(VariableType::Long),
            0xFFF7 => Ok(VariableType::Float),
            0xFFF6 => Ok(VariableType::Double),
            0x8000 => Ok(VariableType::LongString),
            _ => Ok(VariableType::FixedString(code)),
        }
    } else if release >= Release::V111 {
        // 1-byte codes, high byte (format 111–116)
        match code {
            0xFB => Ok(VariableType::Byte),
            0xFC => Ok(VariableType::Int),
            0xFD => Ok(VariableType::Long),
            0xFE => Ok(VariableType::Float),
            0xFF => Ok(VariableType::Double),
            _ => Ok(VariableType::FixedString(code)),
        }
    } else {
        // ASCII codes (format 104–110)
        match code {
            0x62 => Ok(VariableType::Byte),   // 'b'
            0x69 => Ok(VariableType::Int),    // 'i'
            0x6C => Ok(VariableType::Long),   // 'l'
            0x66 => Ok(VariableType::Float),  // 'f'
            0x64 => Ok(VariableType::Double), // 'd'
            c if c >= 0x7F => Ok(VariableType::FixedString(c - 0x7F)),
            _ => Err(DtaError::format(
                Section::Schema,
                position,
                FormatErrorKind::InvalidVariableType { code },
            )),
        }
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Adds a `usize` byte offset to a `u64` base position.
///
/// Returns an I/O error if the offset exceeds `u64`.
fn offset_position(base: u64, offset: usize) -> Result<u64> {
    let offset = u64::try_from(offset).map_err(|_| {
        DtaError::io(
            Section::Schema,
            std::io::Error::new(
                std::io::ErrorKind::Unsupported,
                "section offset exceeds u64",
            ),
        )
    })?;
    Ok(base + offset)
}

/// Creates an I/O error for a variable count that exceeds the
/// platform's addressable range.
fn unsupported_variable_count() -> std::io::Error {
    std::io::Error::new(
        std::io::ErrorKind::Unsupported,
        "variable count exceeds platform address space",
    )
}

/// Reads a little-endian or big-endian `u64` at the given index within
/// a buffer of packed `u64` values.
fn read_u64_at(buffer: &[u8], index: usize, byte_order: ByteOrder) -> u64 {
    let offset = index * 8;
    let bytes = [
        buffer[offset],
        buffer[offset + 1],
        buffer[offset + 2],
        buffer[offset + 3],
        buffer[offset + 4],
        buffer[offset + 5],
        buffer[offset + 6],
        buffer[offset + 7],
    ];
    match byte_order {
        ByteOrder::BigEndian => u64::from_be_bytes(bytes),
        ByteOrder::LittleEndian => u64::from_le_bytes(bytes),
    }
}

/// Zips the per-variable arrays into a single [`Variable`] vec.
fn assemble_variables(
    types: Vec<VariableType>,
    names: Vec<String>,
    formats: Vec<String>,
    value_label_names: Vec<String>,
    labels: Vec<String>,
) -> Vec<Variable> {
    types
        .into_iter()
        .zip(names)
        .zip(formats)
        .zip(value_label_names)
        .zip(labels)
        .map(|((((vt, name), fmt), vln), lbl)| {
            Variable::builder(vt, name)
                .format(fmt)
                .value_label_name(vln)
                .label(lbl)
                .build()
        })
        .collect()
}

// ===========================================================================
// Tests
// ===========================================================================

#[cfg(test)]
mod tests {
    use std::io::Cursor;

    use super::*;
    use crate::stata::dta::byte_order::ByteOrder;
    use crate::stata::dta::dta_error::DtaError;
    use crate::stata::dta::dta_reader::DtaReader;
    use crate::stata::dta::dta_reader_options::DtaReaderOptions;
    use crate::stata::dta::release::Release;

    // -- parse_type_code unit tests ------------------------------------------

    #[test]
    fn type_code_v117_numerics() {
        let r = Release::V117;
        assert_eq!(parse_type_code(0xFFFA, r, 0).unwrap(), VariableType::Byte);
        assert_eq!(parse_type_code(0xFFF9, r, 0).unwrap(), VariableType::Int);
        assert_eq!(parse_type_code(0xFFF8, r, 0).unwrap(), VariableType::Long);
        assert_eq!(parse_type_code(0xFFF7, r, 0).unwrap(), VariableType::Float);
        assert_eq!(parse_type_code(0xFFF6, r, 0).unwrap(), VariableType::Double);
    }

    #[test]
    fn type_code_v117_strl() {
        assert_eq!(
            parse_type_code(0x8000, Release::V117, 0).unwrap(),
            VariableType::LongString,
        );
    }

    #[test]
    fn type_code_v117_fixed_string() {
        assert_eq!(
            parse_type_code(20, Release::V117, 0).unwrap(),
            VariableType::FixedString(20),
        );
        assert_eq!(
            parse_type_code(2045, Release::V117, 0).unwrap(),
            VariableType::FixedString(2045),
        );
    }

    #[test]
    fn type_code_v111_numerics() {
        let r = Release::V114;
        assert_eq!(parse_type_code(0xFB, r, 0).unwrap(), VariableType::Byte);
        assert_eq!(parse_type_code(0xFC, r, 0).unwrap(), VariableType::Int);
        assert_eq!(parse_type_code(0xFD, r, 0).unwrap(), VariableType::Long);
        assert_eq!(parse_type_code(0xFE, r, 0).unwrap(), VariableType::Float);
        assert_eq!(parse_type_code(0xFF, r, 0).unwrap(), VariableType::Double);
    }

    #[test]
    fn type_code_v111_fixed_string() {
        assert_eq!(
            parse_type_code(10, Release::V114, 0).unwrap(),
            VariableType::FixedString(10),
        );
        assert_eq!(
            parse_type_code(244, Release::V114, 0).unwrap(),
            VariableType::FixedString(244),
        );
    }

    #[test]
    fn type_code_old_numerics() {
        let r = Release::V104;
        assert_eq!(parse_type_code(0x62, r, 0).unwrap(), VariableType::Byte);
        assert_eq!(parse_type_code(0x69, r, 0).unwrap(), VariableType::Int);
        assert_eq!(parse_type_code(0x6C, r, 0).unwrap(), VariableType::Long);
        assert_eq!(parse_type_code(0x66, r, 0).unwrap(), VariableType::Float);
        assert_eq!(parse_type_code(0x64, r, 0).unwrap(), VariableType::Double);
    }

    #[test]
    fn type_code_old_fixed_string() {
        // code 0x89 = 137, length = 137 - 127 = 10
        assert_eq!(
            parse_type_code(0x89, Release::V104, 0).unwrap(),
            VariableType::FixedString(10),
        );
        // code 0x7F = 127, length = 0
        assert_eq!(
            parse_type_code(0x7F, Release::V104, 0).unwrap(),
            VariableType::FixedString(0),
        );
    }

    #[test]
    fn type_code_old_invalid() {
        let err = parse_type_code(0x00, Release::V104, 42).unwrap_err();
        assert!(matches!(
            err,
            DtaError::Format(ref e)
                if e.kind() == FormatErrorKind::InvalidVariableType { code: 0 }
                && e.position() == 42
        ));
    }

    // -- VariableType::width -------------------------------------------------

    #[test]
    fn variable_type_widths() {
        assert_eq!(VariableType::Byte.width(), 1);
        assert_eq!(VariableType::Int.width(), 2);
        assert_eq!(VariableType::Long.width(), 4);
        assert_eq!(VariableType::Float.width(), 4);
        assert_eq!(VariableType::Double.width(), 8);
        assert_eq!(VariableType::FixedString(20).width(), 20);
        assert_eq!(VariableType::LongString.width(), 8);
    }

    // -- Test file serialization helpers -------------------------------------

    /// Writes a null-padded fixed-length string field.
    fn write_fixed(buf: &mut Vec<u8>, s: &str, len: usize) {
        let bytes = s.as_bytes();
        let mut field = vec![0u8; len];
        field[..bytes.len()].copy_from_slice(bytes);
        buf.extend_from_slice(&field);
    }

    /// Converts a [`VariableType`] to a raw type code for the given
    /// release, for test serialization.
    fn type_to_code(vt: VariableType, release: Release) -> u16 {
        if release >= Release::V117 {
            match vt {
                VariableType::Byte => 0xFFFA,
                VariableType::Int => 0xFFF9,
                VariableType::Long => 0xFFF8,
                VariableType::Float => 0xFFF7,
                VariableType::Double => 0xFFF6,
                VariableType::LongString => 0x8000,
                VariableType::FixedString(len) => len,
            }
        } else if release >= Release::V111 {
            match vt {
                VariableType::Byte => 0xFB,
                VariableType::Int => 0xFC,
                VariableType::Long => 0xFD,
                VariableType::Float => 0xFE,
                VariableType::Double => 0xFF,
                VariableType::FixedString(len) => len,
                VariableType::LongString => panic!("strL unavailable before v117"),
            }
        } else {
            match vt {
                VariableType::Byte => u16::from(b'b'),
                VariableType::Int => u16::from(b'i'),
                VariableType::Long => u16::from(b'l'),
                VariableType::Float => u16::from(b'f'),
                VariableType::Double => u16::from(b'd'),
                VariableType::FixedString(len) => len + 0x7F,
                VariableType::LongString => panic!("strL unavailable before v117"),
            }
        }
    }

    /// Describes a test variable for serialization.
    struct TestVar {
        variable_type: VariableType,
        name: &'static str,
        format: &'static str,
        value_label_name: &'static str,
        label: &'static str,
    }

    /// Serializes a complete binary DTA file (formats 104–116)
    /// consisting of a header, schema descriptors, and expansion
    /// terminator.
    fn serialize_binary_file(
        release: Release,
        byte_order: ByteOrder,
        variables: &[TestVar],
        sort_order: &[u32],
    ) -> Vec<u8> {
        let nvar = u16::try_from(variables.len()).unwrap();
        let mut buf = Vec::new();

        // -- Header --
        buf.push(release.number());
        buf.push(byte_order.to_byte());
        buf.push(0x01); // filetype
        buf.push(0x00); // padding
        match byte_order {
            ByteOrder::BigEndian => buf.extend_from_slice(&nvar.to_be_bytes()),
            ByteOrder::LittleEndian => buf.extend_from_slice(&nvar.to_le_bytes()),
        }
        match byte_order {
            ByteOrder::BigEndian => buf.extend_from_slice(&0u32.to_be_bytes()),
            ByteOrder::LittleEndian => buf.extend_from_slice(&0u32.to_le_bytes()),
        }
        write_fixed(&mut buf, "", release.dataset_label_len());
        if release.has_timestamp() {
            write_fixed(&mut buf, "", release.timestamp_len());
        }

        // -- Typlist --
        for v in variables {
            let code = type_to_code(v.variable_type, release);
            buf.push(u8::try_from(code).unwrap());
        }

        // -- Varlist --
        for v in variables {
            write_fixed(&mut buf, v.name, release.variable_name_len());
        }

        // -- Sortlist --
        let sort_entry_len = release.sort_entry_len();
        for i in 0..=usize::from(nvar) {
            let index: u32 = sort_order.get(i).map(|idx| idx + 1).unwrap_or(0);
            let index_u16 = u16::try_from(index).unwrap();
            match byte_order {
                ByteOrder::BigEndian => buf.extend_from_slice(&index_u16.to_be_bytes()),
                ByteOrder::LittleEndian => buf.extend_from_slice(&index_u16.to_le_bytes()),
            }
            // Pad to entry_len if it's 4 bytes (only v119, which is XML)
            if sort_entry_len == 4 {
                buf.extend_from_slice(&[0, 0]);
            }
        }

        // -- Fmtlist --
        for v in variables {
            write_fixed(&mut buf, v.format, release.format_entry_len());
        }

        // -- Lbllist --
        for v in variables {
            write_fixed(&mut buf, v.value_label_name, release.value_label_name_len());
        }

        // -- Variable labels --
        for v in variables {
            write_fixed(&mut buf, v.label, release.variable_label_len());
        }

        // -- Expansion field terminator --
        let len_width = release.expansion_len_width();
        if len_width > 0 {
            buf.push(0); // data_type = 0
            buf.extend_from_slice(&vec![0u8; len_width]); // length = 0
        }

        buf
    }

    /// Serializes a complete XML DTA file (formats 117–119) consisting
    /// of a header, section map, schema descriptors, and an empty
    /// characteristics section.
    fn serialize_xml_file(
        release: Release,
        byte_order: ByteOrder,
        variables: &[TestVar],
        sort_order: &[u32],
    ) -> Vec<u8> {
        let nvar = variables.len();
        let mut buf = Vec::new();

        // -- Header --
        buf.extend_from_slice(b"<stata_dta><header>");
        buf.extend_from_slice(b"<release>");
        buf.extend_from_slice(format!("{:03}", release.number()).as_bytes());
        buf.extend_from_slice(b"</release>");
        buf.extend_from_slice(b"<byteorder>");
        buf.extend_from_slice(byte_order.to_string().as_bytes());
        buf.extend_from_slice(b"</byteorder>");

        buf.extend_from_slice(b"<K>");
        if release.supports_extended_variable_count() {
            let nvar_u32 = u32::try_from(nvar).unwrap();
            match byte_order {
                ByteOrder::BigEndian => buf.extend_from_slice(&nvar_u32.to_be_bytes()),
                ByteOrder::LittleEndian => buf.extend_from_slice(&nvar_u32.to_le_bytes()),
            }
        } else {
            let nvar_u16 = u16::try_from(nvar).unwrap();
            match byte_order {
                ByteOrder::BigEndian => buf.extend_from_slice(&nvar_u16.to_be_bytes()),
                ByteOrder::LittleEndian => buf.extend_from_slice(&nvar_u16.to_le_bytes()),
            }
        }
        buf.extend_from_slice(b"</K>");

        buf.extend_from_slice(b"<N>");
        if release.supports_extended_observation_count() {
            match byte_order {
                ByteOrder::BigEndian => buf.extend_from_slice(&0u64.to_be_bytes()),
                ByteOrder::LittleEndian => buf.extend_from_slice(&0u64.to_le_bytes()),
            }
        } else {
            match byte_order {
                ByteOrder::BigEndian => buf.extend_from_slice(&0u32.to_be_bytes()),
                ByteOrder::LittleEndian => buf.extend_from_slice(&0u32.to_le_bytes()),
            }
        }
        buf.extend_from_slice(b"</N>");

        buf.extend_from_slice(b"<label>");
        match release.data_label_len_width() {
            2 => match byte_order {
                ByteOrder::BigEndian => buf.extend_from_slice(&0u16.to_be_bytes()),
                ByteOrder::LittleEndian => buf.extend_from_slice(&0u16.to_le_bytes()),
            },
            1 => buf.push(0),
            _ => {}
        }
        buf.extend_from_slice(b"</label>");
        buf.extend_from_slice(b"<timestamp>");
        buf.push(0);
        buf.extend_from_slice(b"</timestamp>");
        buf.extend_from_slice(b"</header>");

        // -- Map (placeholder for data_offset at index 9) --
        buf.extend_from_slice(b"<map>");
        let map_data_start = buf.len();
        buf.extend_from_slice(&[0u8; 14 * 8]);
        buf.extend_from_slice(b"</map>");

        // -- Variable types --
        buf.extend_from_slice(b"<variable_types>");
        for v in variables {
            let code = type_to_code(v.variable_type, release);
            match byte_order {
                ByteOrder::BigEndian => buf.extend_from_slice(&code.to_be_bytes()),
                ByteOrder::LittleEndian => buf.extend_from_slice(&code.to_le_bytes()),
            }
        }
        buf.extend_from_slice(b"</variable_types>");

        // -- Variable names --
        buf.extend_from_slice(b"<varnames>");
        for v in variables {
            write_fixed(&mut buf, v.name, release.variable_name_len());
        }
        buf.extend_from_slice(b"</varnames>");

        // -- Sort list --
        buf.extend_from_slice(b"<sortlist>");
        let sort_entry_len = release.sort_entry_len();
        for i in 0..=nvar {
            let index: u32 = sort_order.get(i).map(|idx| idx + 1).unwrap_or(0);
            if sort_entry_len == 2 {
                let index_u16 = u16::try_from(index).unwrap();
                match byte_order {
                    ByteOrder::BigEndian => buf.extend_from_slice(&index_u16.to_be_bytes()),
                    ByteOrder::LittleEndian => buf.extend_from_slice(&index_u16.to_le_bytes()),
                }
            } else {
                match byte_order {
                    ByteOrder::BigEndian => buf.extend_from_slice(&index.to_be_bytes()),
                    ByteOrder::LittleEndian => buf.extend_from_slice(&index.to_le_bytes()),
                }
            }
        }
        buf.extend_from_slice(b"</sortlist>");

        // -- Formats --
        buf.extend_from_slice(b"<formats>");
        for v in variables {
            write_fixed(&mut buf, v.format, release.format_entry_len());
        }
        buf.extend_from_slice(b"</formats>");

        // -- Value label names --
        buf.extend_from_slice(b"<value_label_names>");
        for v in variables {
            write_fixed(&mut buf, v.value_label_name, release.value_label_name_len());
        }
        buf.extend_from_slice(b"</value_label_names>");

        // -- Variable labels --
        buf.extend_from_slice(b"<variable_labels>");
        for v in variables {
            write_fixed(&mut buf, v.label, release.variable_label_len());
        }
        buf.extend_from_slice(b"</variable_labels>");

        // -- Empty characteristics --
        buf.extend_from_slice(b"<characteristics>");
        buf.extend_from_slice(b"</characteristics>");

        // Patch data_offset at map index 9
        let data_offset = u64::try_from(buf.len()).unwrap();
        let offset_bytes = match byte_order {
            ByteOrder::BigEndian => data_offset.to_be_bytes(),
            ByteOrder::LittleEndian => data_offset.to_le_bytes(),
        };
        buf[map_data_start + 9 * 8..map_data_start + 10 * 8].copy_from_slice(&offset_bytes);

        buf
    }

    /// Parses a schema from serialized bytes using default options.
    fn read_schema(data: Vec<u8>) -> Schema {
        let cursor = Cursor::new(data);
        let options = DtaReaderOptions::default();
        DtaReader::from_reader(cursor, &options)
            .read_header()
            .unwrap()
            .read_schema()
            .unwrap()
            .schema()
            .clone()
    }

    // -- Binary round-trip tests (formats 104–116) ---------------------------

    #[test]
    fn binary_v114_mixed_types() {
        let vars = [
            TestVar {
                variable_type: VariableType::Byte,
                name: "x",
                format: "%9.0g",
                value_label_name: "",
                label: "The X var",
            },
            TestVar {
                variable_type: VariableType::FixedString(10),
                name: "city",
                format: "%10s",
                value_label_name: "",
                label: "City name",
            },
            TestVar {
                variable_type: VariableType::Double,
                name: "price",
                format: "%10.2f",
                value_label_name: "pricelbl",
                label: "Price in USD",
            },
        ];
        let data = serialize_binary_file(Release::V114, ByteOrder::LittleEndian, &vars, &[]);
        let schema = read_schema(data);

        assert_eq!(schema.variables().len(), 3);
        assert_eq!(schema.row_len(), 1 + 10 + 8);

        let v0 = &schema.variables()[0];
        assert_eq!(v0.variable_type(), VariableType::Byte);
        assert_eq!(v0.name(), "x");
        assert_eq!(v0.format(), "%9.0g");
        assert_eq!(v0.value_label_name(), "");
        assert_eq!(v0.label(), "The X var");

        let v1 = &schema.variables()[1];
        assert_eq!(v1.variable_type(), VariableType::FixedString(10));
        assert_eq!(v1.name(), "city");
        assert_eq!(v1.format(), "%10s");
        assert_eq!(v1.label(), "City name");

        let v2 = &schema.variables()[2];
        assert_eq!(v2.variable_type(), VariableType::Double);
        assert_eq!(v2.name(), "price");
        assert_eq!(v2.value_label_name(), "pricelbl");
        assert_eq!(v2.label(), "Price in USD");
    }

    #[test]
    fn binary_v114_big_endian() {
        let vars = [TestVar {
            variable_type: VariableType::Long,
            name: "id",
            format: "%12.0g",
            value_label_name: "",
            label: "",
        }];
        let data = serialize_binary_file(Release::V114, ByteOrder::BigEndian, &vars, &[]);
        let schema = read_schema(data);

        assert_eq!(schema.variables().len(), 1);
        assert_eq!(schema.variables()[0].variable_type(), VariableType::Long);
        assert_eq!(schema.variables()[0].name(), "id");
        assert_eq!(schema.row_len(), 4);
    }

    #[test]
    fn binary_v104_old_type_codes() {
        let vars = [
            TestVar {
                variable_type: VariableType::Int,
                name: "a",
                format: "%8.0g",
                value_label_name: "",
                label: "A",
            },
            TestVar {
                variable_type: VariableType::FixedString(10),
                name: "b",
                format: "%10s",
                value_label_name: "",
                label: "B",
            },
        ];
        let data = serialize_binary_file(Release::V104, ByteOrder::LittleEndian, &vars, &[]);
        let schema = read_schema(data);

        assert_eq!(schema.variables().len(), 2);
        assert_eq!(schema.variables()[0].variable_type(), VariableType::Int);
        assert_eq!(
            schema.variables()[1].variable_type(),
            VariableType::FixedString(10)
        );
        assert_eq!(schema.row_len(), 2 + 10);
    }

    #[test]
    fn binary_v114_with_sort_order() {
        let vars = [
            TestVar {
                variable_type: VariableType::Byte,
                name: "a",
                format: "%9.0g",
                value_label_name: "",
                label: "",
            },
            TestVar {
                variable_type: VariableType::Byte,
                name: "b",
                format: "%9.0g",
                value_label_name: "",
                label: "",
            },
            TestVar {
                variable_type: VariableType::Byte,
                name: "c",
                format: "%9.0g",
                value_label_name: "",
                label: "",
            },
        ];
        // Sort by c (index 2) then a (index 0)
        let data = serialize_binary_file(Release::V114, ByteOrder::LittleEndian, &vars, &[2, 0]);
        let schema = read_schema(data);

        assert_eq!(schema.sort_order(), &[2, 0]);
    }

    #[test]
    fn binary_v114_empty_sort_order() {
        let vars = [TestVar {
            variable_type: VariableType::Double,
            name: "y",
            format: "%10.0g",
            value_label_name: "",
            label: "",
        }];
        let data = serialize_binary_file(Release::V114, ByteOrder::LittleEndian, &vars, &[]);
        let schema = read_schema(data);
        assert!(schema.sort_order().is_empty());
    }

    #[test]
    fn binary_v114_zero_variables() {
        let data = serialize_binary_file(Release::V114, ByteOrder::LittleEndian, &[], &[]);
        let schema = read_schema(data);

        assert!(schema.variables().is_empty());
        assert!(schema.sort_order().is_empty());
        assert_eq!(schema.row_len(), 0);
    }

    #[test]
    fn binary_v114_all_numeric_types() {
        let vars = [
            TestVar {
                variable_type: VariableType::Byte,
                name: "a",
                format: "%8.0g",
                value_label_name: "",
                label: "",
            },
            TestVar {
                variable_type: VariableType::Int,
                name: "b",
                format: "%8.0g",
                value_label_name: "",
                label: "",
            },
            TestVar {
                variable_type: VariableType::Long,
                name: "c",
                format: "%12.0g",
                value_label_name: "",
                label: "",
            },
            TestVar {
                variable_type: VariableType::Float,
                name: "d",
                format: "%9.0g",
                value_label_name: "",
                label: "",
            },
            TestVar {
                variable_type: VariableType::Double,
                name: "e",
                format: "%10.0g",
                value_label_name: "",
                label: "",
            },
        ];
        let data = serialize_binary_file(Release::V114, ByteOrder::LittleEndian, &vars, &[]);
        let schema = read_schema(data);

        assert_eq!(schema.row_len(), 1 + 2 + 4 + 4 + 8);
        for (i, expected) in [
            VariableType::Byte,
            VariableType::Int,
            VariableType::Long,
            VariableType::Float,
            VariableType::Double,
        ]
        .iter()
        .enumerate()
        {
            assert_eq!(schema.variables()[i].variable_type(), *expected);
        }
    }

    #[test]
    fn binary_v105_short_fields() {
        // v105: 12-byte formats, 9-byte names, 2-byte expansion len
        let vars = [TestVar {
            variable_type: VariableType::Float,
            name: "temp",
            format: "%9.0g",
            value_label_name: "",
            label: "Temperature",
        }];
        let data = serialize_binary_file(Release::V105, ByteOrder::LittleEndian, &vars, &[]);
        let schema = read_schema(data);

        assert_eq!(schema.variables().len(), 1);
        assert_eq!(schema.variables()[0].variable_type(), VariableType::Float);
        assert_eq!(schema.variables()[0].name(), "temp");
        assert_eq!(schema.variables()[0].label(), "Temperature");
    }

    // -- XML round-trip tests (formats 117–119) ------------------------------

    #[test]
    fn xml_v117_mixed_types() {
        let vars = [
            TestVar {
                variable_type: VariableType::Int,
                name: "count",
                format: "%8.0g",
                value_label_name: "cntlbl",
                label: "Count",
            },
            TestVar {
                variable_type: VariableType::LongString,
                name: "notes",
                format: "%9s",
                value_label_name: "",
                label: "Notes field",
            },
        ];
        let data = serialize_xml_file(Release::V117, ByteOrder::LittleEndian, &vars, &[]);
        let schema = read_schema(data);

        assert_eq!(schema.variables().len(), 2);
        assert_eq!(schema.row_len(), 2 + 8);

        let v0 = &schema.variables()[0];
        assert_eq!(v0.variable_type(), VariableType::Int);
        assert_eq!(v0.name(), "count");
        assert_eq!(v0.value_label_name(), "cntlbl");

        let v1 = &schema.variables()[1];
        assert_eq!(v1.variable_type(), VariableType::LongString);
        assert_eq!(v1.name(), "notes");
    }

    #[test]
    fn xml_v118_all_numeric_types() {
        let vars = [
            TestVar {
                variable_type: VariableType::Byte,
                name: "a",
                format: "%8.0g",
                value_label_name: "",
                label: "",
            },
            TestVar {
                variable_type: VariableType::Int,
                name: "b",
                format: "%8.0g",
                value_label_name: "",
                label: "",
            },
            TestVar {
                variable_type: VariableType::Long,
                name: "c",
                format: "%12.0g",
                value_label_name: "",
                label: "",
            },
            TestVar {
                variable_type: VariableType::Float,
                name: "d",
                format: "%9.0g",
                value_label_name: "",
                label: "",
            },
            TestVar {
                variable_type: VariableType::Double,
                name: "e",
                format: "%10.0g",
                value_label_name: "",
                label: "",
            },
        ];
        let data = serialize_xml_file(Release::V118, ByteOrder::LittleEndian, &vars, &[]);
        let schema = read_schema(data);

        assert_eq!(schema.row_len(), 1 + 2 + 4 + 4 + 8);
        assert_eq!(schema.variables()[0].name(), "a");
        assert_eq!(schema.variables()[4].name(), "e");
    }

    #[test]
    fn xml_v117_big_endian() {
        let vars = [TestVar {
            variable_type: VariableType::Double,
            name: "val",
            format: "%10.0g",
            value_label_name: "",
            label: "Value",
        }];
        let data = serialize_xml_file(Release::V117, ByteOrder::BigEndian, &vars, &[]);
        let schema = read_schema(data);

        assert_eq!(schema.variables().len(), 1);
        assert_eq!(schema.variables()[0].variable_type(), VariableType::Double);
        assert_eq!(schema.variables()[0].name(), "val");
    }

    #[test]
    fn xml_v117_with_sort_order() {
        let vars = [
            TestVar {
                variable_type: VariableType::Byte,
                name: "a",
                format: "%9.0g",
                value_label_name: "",
                label: "",
            },
            TestVar {
                variable_type: VariableType::Byte,
                name: "b",
                format: "%9.0g",
                value_label_name: "",
                label: "",
            },
        ];
        let data = serialize_xml_file(Release::V117, ByteOrder::LittleEndian, &vars, &[1, 0]);
        let schema = read_schema(data);
        assert_eq!(schema.sort_order(), &[1, 0]);
    }

    #[test]
    fn xml_v117_zero_variables() {
        let data = serialize_xml_file(Release::V117, ByteOrder::LittleEndian, &[], &[]);
        let schema = read_schema(data);

        assert!(schema.variables().is_empty());
        assert_eq!(schema.row_len(), 0);
    }

    #[test]
    fn xml_v117_fixed_string() {
        let vars = [TestVar {
            variable_type: VariableType::FixedString(20),
            name: "city",
            format: "%20s",
            value_label_name: "",
            label: "City",
        }];
        let data = serialize_xml_file(Release::V117, ByteOrder::LittleEndian, &vars, &[]);
        let schema = read_schema(data);

        assert_eq!(
            schema.variables()[0].variable_type(),
            VariableType::FixedString(20)
        );
        assert_eq!(schema.row_len(), 20);
    }

    // -- Binary expansion field tests ----------------------------------------

    #[test]
    fn binary_v110_expansion_fields_skipped() {
        // v110 uses 4-byte expansion lengths.  Insert a non-trivial
        // expansion entry (data_type=1, length=6, 6 junk bytes) before
        // the terminator.
        let vars = [TestVar {
            variable_type: VariableType::Byte,
            name: "x",
            format: "%9.0g",
            value_label_name: "",
            label: "",
        }];
        let mut data = serialize_binary_file(Release::V110, ByteOrder::LittleEndian, &vars, &[]);

        // The last 5 bytes are the terminator (type=0, len=0 as u32).
        // Insert an expansion entry before it.
        let terminator_start = data.len() - 5;
        let mut with_entry = data[..terminator_start].to_vec();
        with_entry.push(1); // data_type = 1
        with_entry.extend_from_slice(&6u32.to_le_bytes()); // length = 6
        with_entry.extend_from_slice(&[0xAA; 6]); // 6 junk bytes
        with_entry.extend_from_slice(&data[terminator_start..]); // terminator

        let schema = read_schema(with_entry);
        assert_eq!(schema.variables().len(), 1);
        assert_eq!(schema.variables()[0].name(), "x");
    }

    #[test]
    fn binary_v106_expansion_fields_u16_length() {
        // v106: expansion_len_width = 2
        let vars = [TestVar {
            variable_type: VariableType::Double,
            name: "y",
            format: "%9.0g",
            value_label_name: "",
            label: "",
        }];
        let mut data = serialize_binary_file(Release::V106, ByteOrder::LittleEndian, &vars, &[]);

        // Insert an expansion entry before the terminator (last 3 bytes).
        let terminator_start = data.len() - 3;
        let mut with_entry = data[..terminator_start].to_vec();
        with_entry.push(1); // data_type = 1
        with_entry.extend_from_slice(&4u16.to_le_bytes()); // length = 4
        with_entry.extend_from_slice(&[0xBB; 4]); // 4 junk bytes
        with_entry.extend_from_slice(&data[terminator_start..]); // terminator

        let schema = read_schema(with_entry);
        assert_eq!(schema.variables().len(), 1);
        assert_eq!(schema.variables()[0].variable_type(), VariableType::Double);
    }

    // -- Schema builder validation tests -------------------------------------

    #[test]
    fn schema_builder_valid_sort_order() {
        use crate::stata::dta::schema::Schema;

        let schema = Schema::builder()
            .add_variable(Variable::builder(VariableType::Byte, "a").build())
            .add_variable(Variable::builder(VariableType::Int, "b").build())
            .add_variable(Variable::builder(VariableType::Double, "c").build())
            .sort_order(vec![2, 0])
            .build()
            .unwrap();

        assert_eq!(schema.variables().len(), 3);
        assert_eq!(schema.sort_order(), &[2, 0]);
        assert_eq!(schema.row_len(), 1 + 2 + 8);
    }

    #[test]
    fn schema_builder_sort_order_out_of_bounds() {
        use crate::stata::dta::schema::Schema;

        let err = Schema::builder()
            .add_variable(Variable::builder(VariableType::Byte, "a").build())
            .add_variable(Variable::builder(VariableType::Byte, "b").build())
            .sort_order(vec![0, 5])
            .build()
            .unwrap_err();

        assert!(matches!(
            err,
            DtaError::SortOrderOutOfBounds {
                index: 5,
                variable_count: 2,
            }
        ));
    }

    #[test]
    fn schema_builder_empty_sort_order_always_valid() {
        use crate::stata::dta::schema::Schema;

        let schema = Schema::builder()
            .add_variable(Variable::builder(VariableType::Byte, "x").build())
            .build()
            .unwrap();

        assert!(schema.sort_order().is_empty());
    }

    #[test]
    fn schema_builder_no_variables() {
        use crate::stata::dta::schema::Schema;

        let schema = Schema::builder().build().unwrap();

        assert!(schema.variables().is_empty());
        assert_eq!(schema.row_len(), 0);
    }

    #[test]
    fn schema_builder_sort_order_oob_on_empty_variables() {
        use crate::stata::dta::schema::Schema;

        let err = Schema::builder().sort_order(vec![0]).build().unwrap_err();

        assert!(matches!(
            err,
            DtaError::SortOrderOutOfBounds {
                index: 0,
                variable_count: 0,
            }
        ));
    }

    #[test]
    fn binary_sort_order_out_of_bounds_returns_error() {
        let vars = [TestVar {
            variable_type: VariableType::Byte,
            name: "a",
            format: "%9.0g",
            value_label_name: "",
            label: "",
        }];
        // Sort order index 5 is out of bounds for 1 variable
        let data = serialize_binary_file(Release::V114, ByteOrder::LittleEndian, &vars, &[5]);
        let cursor = Cursor::new(data);
        let options = DtaReaderOptions::default();
        let err = DtaReader::from_reader(cursor, &options)
            .read_header()
            .unwrap()
            .read_schema()
            .unwrap_err();

        assert!(matches!(
            err,
            DtaError::SortOrderOutOfBounds {
                index: 5,
                variable_count: 1,
            }
        ));
    }
}
