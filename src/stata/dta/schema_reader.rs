use std::io::{BufRead, Read};

use super::byte_order::ByteOrder;
use super::characteristic_reader::CharacteristicReader;
use super::dta_error::{Field, FormatErrorKind, Result, Section};
use super::header::Header;
use super::reader_state::ReaderState;
use super::release::Release;
use super::schema::Schema;
use super::schema_parse::{
    assemble_variables, buffer_size, narrow_variable_count_to_usize, offset_position,
    parse_type_code, read_u64_at, sort_entry_count,
};
use super::section_offsets::SectionOffsets;
use super::string_decoding::decode_fixed_string;
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
    #[must_use]
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

impl<R: BufRead> SchemaReader<R> {
    /// Parses variable definitions and transitions to characteristic
    /// reading.
    ///
    /// Reads type codes, variable names, sort order, display formats,
    /// value-label associations, and variable labels. For XML formats,
    /// also reads the section map. The stream is left positioned at
    /// the start of the characteristics section.
    ///
    /// # Errors
    ///
    /// Returns [`DtaError::Io`] on read failures and
    /// [`DtaError::Format`] when the schema bytes violate the DTA
    /// format specification.
    pub fn read_schema(mut self) -> Result<CharacteristicReader<R>> {
        let release = self.header.release();
        let byte_order = self.header.byte_order();
        let header_position = self.state.position();
        let variable_count =
            narrow_variable_count_to_usize(self.header.variable_count(), header_position)?;

        // XML formats store section offsets in a map before the
        // descriptors; binary formats have no map.
        if release.is_xml_like() {
            let offsets = self.read_map(byte_order)?;
            self.state.set_section_offsets(offsets);
        }

        let variable_types = self.read_variable_types(variable_count, release, byte_order)?;
        let variable_names = self.read_variable_names(variable_count, release)?;
        let sort_order = self.read_sort_order(variable_count, release, byte_order)?;
        let formats = self.read_formats(variable_count, release)?;
        let value_label_names = self.read_value_label_names(variable_count, release)?;
        let variable_labels = self.read_variable_labels(variable_count, release)?;

        // For binary formats, characteristics start at the current
        // position (right after the last descriptor subsection).
        // Data and value-label offsets are not yet known — the
        // characteristic reader computes them after consuming the
        // expansion fields.
        if !release.is_xml_like() {
            let characteristics_offset = self.state.position();
            self.state
                .set_section_offsets(SectionOffsets::new(characteristics_offset, 0, 0, None));
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
        let reader = CharacteristicReader::new(self.state, self.header, schema);
        Ok(reader)
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
    /// Reads the `<map>` section and returns offsets for each
    /// post-schema section.
    ///
    /// The map contains 14 `u64` values. The indices used are:
    ///
    /// | Index | Section              |
    /// |-------|----------------------|
    /// |   8   | `<characteristics>`  |
    /// |   9   | `<data>`             |
    /// |  10   | `<strls>`            |
    /// |  11   | `<value_labels>`     |
    fn read_map(&mut self, byte_order: ByteOrder) -> Result<SectionOffsets> {
        self.expect_tag(b"<map>")?;

        let buffer = self.state.read_exact(14 * 8, Section::Schema)?;
        let characteristics_offset = read_u64_at(buffer, 8, byte_order);
        let data_offset = read_u64_at(buffer, 9, byte_order);
        let long_strings_offset = read_u64_at(buffer, 10, byte_order);
        let value_labels_offset = read_u64_at(buffer, 11, byte_order);

        self.expect_tag(b"</map>")?;
        let offsets = SectionOffsets::new(
            characteristics_offset,
            data_offset,
            value_labels_offset,
            Some(long_strings_offset),
        );
        Ok(offsets)
    }
}

// ---------------------------------------------------------------------------
// Descriptor sub-sections
// ---------------------------------------------------------------------------

impl<R: Read> SchemaReader<R> {
    /// Reads the type list — one type code per variable.
    fn read_variable_types(
        &mut self,
        variable_count: usize,
        release: Release,
        byte_order: ByteOrder,
    ) -> Result<Vec<VariableType>> {
        self.expect_tag(b"<variable_types>")?;

        let extended = release.supports_extended_type_list_entry();
        let entry_len = release.type_list_entry_len();
        let section_start = self.state.position();
        let total_bytes = buffer_size(variable_count, entry_len, section_start)?;
        let buffer = self.state.read_exact(total_bytes, Section::Schema)?;

        let mut types = Vec::with_capacity(variable_count);
        let mut offset = 0;
        for index in 0..variable_count {
            let code = if extended {
                let bytes = [buffer[offset], buffer[offset + 1]];
                byte_order.read_u16(bytes)
            } else {
                u16::from(buffer[index])
            };
            offset += entry_len;
            let position = offset_position(section_start, offset)?;
            types.push(parse_type_code(code, release, position)?);
        }

        self.expect_tag(b"</variable_types>")?;
        Ok(types)
    }

    /// Reads the variable list — one fixed-length name per variable.
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

        let extended = release.supports_extended_sort_entry();
        let entry_len = release.sort_entry_len();
        let position = self.state.position();
        let entry_count = sort_entry_count(variable_count, position)?;
        let total_bytes = buffer_size(entry_count, entry_len, position)?;
        let buffer = self.state.read_exact(total_bytes, Section::Schema)?;

        let mut sort_order = Vec::new();
        let mut offset = 0;
        for _ in 0..entry_count {
            let index = if extended {
                let bytes = [
                    buffer[offset],
                    buffer[offset + 1],
                    buffer[offset + 2],
                    buffer[offset + 3],
                ];
                byte_order.read_u32(bytes)
            } else {
                let bytes = [buffer[offset], buffer[offset + 1]];
                u32::from(byte_order.read_u16(bytes))
            };
            offset += entry_len;

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

    /// Reads the lbllist — one value-label set name per variable.
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
        let total_bytes = buffer_size(count, entry_len, section_start)?;
        let buffer = self.state.read_exact(total_bytes, Section::Schema)?;

        let mut result = Vec::with_capacity(count);
        let mut start = 0;
        for _ in 0..count {
            let position = offset_position(section_start, start)?;
            let raw = &buffer[start..start + entry_len];
            let value = decode_fixed_string(raw, encoding, Section::Schema, field, position)?;
            result.push(value);
            start += entry_len;
        }

        self.expect_tag(close_tag)?;
        Ok(result)
    }
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
    use crate::stata::dta::dta_writer::DtaWriter;
    use crate::stata::dta::release::Release;
    use crate::stata::dta::variable::Variable;

    // -- parse_type_code unit tests ------------------------------------------

    #[test]
    fn type_code_v117_numerics() {
        let release = Release::V117;
        assert_eq!(
            parse_type_code(0xFFFA, release, 0).unwrap(),
            VariableType::Byte
        );
        assert_eq!(
            parse_type_code(0xFFF9, release, 0).unwrap(),
            VariableType::Int
        );
        assert_eq!(
            parse_type_code(0xFFF8, release, 0).unwrap(),
            VariableType::Long
        );
        assert_eq!(
            parse_type_code(0xFFF7, release, 0).unwrap(),
            VariableType::Float
        );
        assert_eq!(
            parse_type_code(0xFFF6, release, 0).unwrap(),
            VariableType::Double
        );
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
            parse_type_code(1, Release::V117, 0).unwrap(),
            VariableType::FixedString(1),
        );
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
    fn type_code_v117_invalid() {
        // Zero is invalid.
        assert!(parse_type_code(0, Release::V117, 0).is_err());
        // 2046 is beyond the str2045 maximum.
        assert!(parse_type_code(2046, Release::V117, 0).is_err());
        // Codes in the gap between valid strings and reserved codes.
        assert!(parse_type_code(0x8001, Release::V117, 0).is_err());
    }

    #[test]
    fn type_code_v111_numerics() {
        let release = Release::V114;
        assert_eq!(
            parse_type_code(0xFB, release, 0).unwrap(),
            VariableType::Byte
        );
        assert_eq!(
            parse_type_code(0xFC, release, 0).unwrap(),
            VariableType::Int
        );
        assert_eq!(
            parse_type_code(0xFD, release, 0).unwrap(),
            VariableType::Long
        );
        assert_eq!(
            parse_type_code(0xFE, release, 0).unwrap(),
            VariableType::Float
        );
        assert_eq!(
            parse_type_code(0xFF, release, 0).unwrap(),
            VariableType::Double
        );
    }

    #[test]
    fn type_code_v111_fixed_string() {
        assert_eq!(
            parse_type_code(1, Release::V114, 0).unwrap(),
            VariableType::FixedString(1),
        );
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
    fn type_code_v111_invalid() {
        // Zero is invalid.
        assert!(parse_type_code(0, Release::V114, 0).is_err());
        // 245–250 fall between valid strings and numeric codes.
        assert!(parse_type_code(245, Release::V114, 0).is_err());
        assert!(parse_type_code(250, Release::V114, 0).is_err());
    }

    #[test]
    fn type_code_old_numerics() {
        let release = Release::V104;
        assert_eq!(
            parse_type_code(0x62, release, 0).unwrap(),
            VariableType::Byte
        );
        assert_eq!(
            parse_type_code(0x69, release, 0).unwrap(),
            VariableType::Int
        );
        assert_eq!(
            parse_type_code(0x6C, release, 0).unwrap(),
            VariableType::Long
        );
        assert_eq!(
            parse_type_code(0x66, release, 0).unwrap(),
            VariableType::Float
        );
        assert_eq!(
            parse_type_code(0x64, release, 0).unwrap(),
            VariableType::Double
        );
    }

    #[test]
    fn type_code_old_fixed_string() {
        // code 0x89 = 137, length = 137 - 127 = 10
        assert_eq!(
            parse_type_code(0x89, Release::V104, 0).unwrap(),
            VariableType::FixedString(10),
        );
        // code 0x80 = 128, length = 1 (str1, minimum valid)
        assert_eq!(
            parse_type_code(0x80, Release::V104, 0).unwrap(),
            VariableType::FixedString(1),
        );
        // code 0xCF = 207, length = 80 (str80, maximum valid)
        assert_eq!(
            parse_type_code(0xCF, Release::V104, 0).unwrap(),
            VariableType::FixedString(80),
        );
    }

    #[test]
    fn type_code_old_invalid() {
        // Zero is invalid.
        let error = parse_type_code(0x00, Release::V104, 42).unwrap_err();
        assert!(matches!(
            error,
            DtaError::Format(ref e)
                if e.kind() == FormatErrorKind::InvalidVariableType { code: 0 }
                && e.position() == 42
        ));
        // 0x7F (str0) is below the valid string range.
        assert!(parse_type_code(0x7F, Release::V104, 0).is_err());
        // 0xD0 (str81) exceeds the str80 maximum.
        assert!(parse_type_code(0xD0, Release::V104, 0).is_err());
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

    /// Describes a test variable for serialization.
    struct TestVariable {
        variable_type: VariableType,
        name: &'static str,
        format: &'static str,
        value_label_name: &'static str,
        label: &'static str,
    }

    /// Serializes a complete DTA file (binary or XML) by running the
    /// given variables + sort order through the full writer pipeline.
    /// The file ends with the characteristics-terminator (binary) or
    /// the `</stata_dta>` close tag (XML) — no records, no strLs, no
    /// value labels.
    ///
    /// Used by every round-trip test in this module. The two
    /// expansion-field tests splice raw bytes in before the
    /// terminator and rely on the writer's output ending with the
    /// same `u8 data_type + u16/u32 length` terminator that the hand-
    /// crafted helper used to emit.
    fn serialize_file(
        release: Release,
        byte_order: ByteOrder,
        variables: &[TestVariable],
        sort_order: &[u32],
    ) -> Vec<u8> {
        let header = Header::builder(release, byte_order).build();
        let variable_builders: Vec<_> = variables
            .iter()
            .map(|v| {
                Variable::builder(v.variable_type, v.name)
                    .format(v.format)
                    .value_label_name(v.value_label_name)
                    .label(v.label)
            })
            .collect();
        let schema = Schema::builder()
            .variables(variable_builders)
            .sort_order(sort_order.to_vec())
            .build()
            .unwrap();
        DtaWriter::new()
            .from_writer(Cursor::new(Vec::<u8>::new()))
            .write_header(header)
            .unwrap()
            .write_schema(schema)
            .unwrap()
            .into_record_writer()
            .unwrap()
            .into_long_string_writer()
            .unwrap()
            .into_value_label_writer()
            .unwrap()
            .finish()
            .unwrap()
            .into_inner()
    }

    /// Parses a schema from serialized bytes using default options.
    fn read_schema(data: Vec<u8>) -> Schema {
        DtaReader::default()
            .from_reader(Cursor::new(data))
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
        let variables = [
            TestVariable {
                variable_type: VariableType::Byte,
                name: "x",
                format: "%9.0g",
                value_label_name: "",
                label: "The X var",
            },
            TestVariable {
                variable_type: VariableType::FixedString(10),
                name: "city",
                format: "%10s",
                value_label_name: "",
                label: "City name",
            },
            TestVariable {
                variable_type: VariableType::Double,
                name: "price",
                format: "%10.2f",
                value_label_name: "pricelbl",
                label: "Price in USD",
            },
        ];
        let data = serialize_file(Release::V114, ByteOrder::LittleEndian, &variables, &[]);
        let schema = read_schema(data);

        assert_eq!(schema.variables().len(), 3);
        assert_eq!(schema.row_len(), 1 + 10 + 8);

        let variable_0 = &schema.variables()[0];
        assert_eq!(variable_0.variable_type(), VariableType::Byte);
        assert_eq!(variable_0.name(), "x");
        assert_eq!(variable_0.format(), "%9.0g");
        assert_eq!(variable_0.value_label_name(), "");
        assert_eq!(variable_0.label(), "The X var");

        let variable_1 = &schema.variables()[1];
        assert_eq!(variable_1.variable_type(), VariableType::FixedString(10));
        assert_eq!(variable_1.name(), "city");
        assert_eq!(variable_1.format(), "%10s");
        assert_eq!(variable_1.label(), "City name");

        let variable_2 = &schema.variables()[2];
        assert_eq!(variable_2.variable_type(), VariableType::Double);
        assert_eq!(variable_2.name(), "price");
        assert_eq!(variable_2.value_label_name(), "pricelbl");
        assert_eq!(variable_2.label(), "Price in USD");
    }

    #[test]
    fn binary_v114_big_endian() {
        let id_variable = TestVariable {
            variable_type: VariableType::Long,
            name: "id",
            format: "%12.0g",
            value_label_name: "",
            label: "",
        };
        let variables = [id_variable];
        let data = serialize_file(Release::V114, ByteOrder::BigEndian, &variables, &[]);
        let schema = read_schema(data);

        assert_eq!(schema.variables().len(), 1);
        assert_eq!(schema.variables()[0].variable_type(), VariableType::Long);
        assert_eq!(schema.variables()[0].name(), "id");
        assert_eq!(schema.row_len(), 4);
    }

    #[test]
    fn binary_v104_old_type_codes() {
        let variable_a = TestVariable {
            variable_type: VariableType::Int,
            name: "a",
            format: "%8.0g",
            value_label_name: "",
            label: "A",
        };
        let variable_b = TestVariable {
            variable_type: VariableType::FixedString(10),
            name: "b",
            format: "%10s",
            value_label_name: "",
            label: "B",
        };
        let expected_variables = [variable_a, variable_b];
        let data = serialize_file(
            Release::V104,
            ByteOrder::LittleEndian,
            &expected_variables,
            &[],
        );
        let schema = read_schema(data);

        let actual_variables = schema.variables();
        assert_eq!(actual_variables.len(), 2);
        let first_variable = &actual_variables[0];
        assert_eq!(first_variable.variable_type(), VariableType::Int);
        let second_variable = &actual_variables[1];
        assert_eq!(
            second_variable.variable_type(),
            VariableType::FixedString(10)
        );
        assert_eq!(schema.row_len(), 2 + 10);
    }

    #[test]
    fn binary_v114_with_sort_order() {
        let variable_a = TestVariable {
            variable_type: VariableType::Byte,
            name: "a",
            format: "%9.0g",
            value_label_name: "",
            label: "",
        };
        let variable_b = TestVariable {
            variable_type: VariableType::Byte,
            name: "b",
            format: "%9.0g",
            value_label_name: "",
            label: "",
        };
        let variable_c = TestVariable {
            variable_type: VariableType::Byte,
            name: "c",
            format: "%9.0g",
            value_label_name: "",
            label: "",
        };
        let variables = [variable_a, variable_b, variable_c];
        // Sort by c (index 2) then a (index 0)
        let data = serialize_file(Release::V114, ByteOrder::LittleEndian, &variables, &[2, 0]);
        let schema = read_schema(data);

        assert_eq!(schema.sort_order(), &[2, 0]);
    }

    #[test]
    fn binary_v114_empty_sort_order() {
        let variable = TestVariable {
            variable_type: VariableType::Double,
            name: "y",
            format: "%10.0g",
            value_label_name: "",
            label: "",
        };
        let variables = [variable];
        let data = serialize_file(Release::V114, ByteOrder::LittleEndian, &variables, &[]);
        let schema = read_schema(data);
        assert!(schema.sort_order().is_empty());
    }

    #[test]
    fn binary_v114_zero_variables() {
        let data = serialize_file(Release::V114, ByteOrder::LittleEndian, &[], &[]);
        let schema = read_schema(data);

        assert!(schema.variables().is_empty());
        assert!(schema.sort_order().is_empty());
        assert_eq!(schema.row_len(), 0);
    }

    #[test]
    fn binary_v114_all_numeric_types() {
        let variable_a = TestVariable {
            variable_type: VariableType::Byte,
            name: "a",
            format: "%8.0g",
            value_label_name: "",
            label: "",
        };
        let variable_b = TestVariable {
            variable_type: VariableType::Int,
            name: "b",
            format: "%8.0g",
            value_label_name: "",
            label: "",
        };
        let variable_c = TestVariable {
            variable_type: VariableType::Long,
            name: "c",
            format: "%12.0g",
            value_label_name: "",
            label: "",
        };
        let variable_d = TestVariable {
            variable_type: VariableType::Float,
            name: "d",
            format: "%9.0g",
            value_label_name: "",
            label: "",
        };
        let variable_e = TestVariable {
            variable_type: VariableType::Double,
            name: "e",
            format: "%10.0g",
            value_label_name: "",
            label: "",
        };
        let variables = [variable_a, variable_b, variable_c, variable_d, variable_e];
        let data = serialize_file(Release::V114, ByteOrder::LittleEndian, &variables, &[]);
        let schema = read_schema(data);

        let actual_variables = schema.variables();
        assert_eq!(VariableType::Byte, actual_variables[0].variable_type());
        assert_eq!(VariableType::Int, actual_variables[1].variable_type());
        assert_eq!(VariableType::Long, actual_variables[2].variable_type());
        assert_eq!(VariableType::Float, actual_variables[3].variable_type());
        assert_eq!(VariableType::Double, actual_variables[4].variable_type());
        let expected_length = VariableType::Byte.width()
            + VariableType::Int.width()
            + VariableType::Long.width()
            + VariableType::Float.width()
            + VariableType::Double.width();
        assert_eq!(schema.row_len(), expected_length);
    }

    #[test]
    fn binary_v105_short_fields() {
        // v105: 12-byte formats, 9-byte names, 2-byte expansion len
        let variable = TestVariable {
            variable_type: VariableType::Float,
            name: "temp",
            format: "%9.0g",
            value_label_name: "",
            label: "Temperature",
        };
        let variables = [variable];
        let data = serialize_file(Release::V105, ByteOrder::LittleEndian, &variables, &[]);
        let schema = read_schema(data);

        let actual_variables = schema.variables();
        assert_eq!(actual_variables.len(), 1);
        let actual_variable = &actual_variables[0];
        assert_eq!(actual_variable.variable_type(), VariableType::Float);
        assert_eq!(actual_variable.name(), "temp");
        assert_eq!(actual_variable.label(), "Temperature");
    }

    // -- XML round-trip tests (formats 117–119) ------------------------------

    #[test]
    fn xml_v117_mixed_types() {
        let count = TestVariable {
            variable_type: VariableType::Int,
            name: "count",
            format: "%8.0g",
            value_label_name: "cntlbl",
            label: "Count",
        };
        let notes = TestVariable {
            variable_type: VariableType::LongString,
            name: "notes",
            format: "%9s",
            value_label_name: "",
            label: "Notes field",
        };
        let variables = [count, notes];
        let data = serialize_file(Release::V117, ByteOrder::LittleEndian, &variables, &[]);
        let schema = read_schema(data);

        let actual_variables = schema.variables();
        assert_eq!(actual_variables.len(), 2);
        assert_eq!(schema.row_len(), 2 + 8);

        let actual_count = &actual_variables[0];
        assert_eq!(actual_count.variable_type(), VariableType::Int);
        assert_eq!(actual_count.name(), "count");
        assert_eq!(actual_count.value_label_name(), "cntlbl");

        let actual_notes = &actual_variables[1];
        assert_eq!(actual_notes.variable_type(), VariableType::LongString);
        assert_eq!(actual_notes.name(), "notes");
    }

    #[test]
    fn xml_v118_all_numeric_types() {
        let variable_a = TestVariable {
            variable_type: VariableType::Byte,
            name: "a",
            format: "%8.0g",
            value_label_name: "",
            label: "",
        };
        let variable_b = TestVariable {
            variable_type: VariableType::Int,
            name: "b",
            format: "%8.0g",
            value_label_name: "",
            label: "",
        };
        let variable_c = TestVariable {
            variable_type: VariableType::Long,
            name: "c",
            format: "%12.0g",
            value_label_name: "",
            label: "",
        };
        let variable_d = TestVariable {
            variable_type: VariableType::Float,
            name: "d",
            format: "%9.0g",
            value_label_name: "",
            label: "",
        };
        let variable_e = TestVariable {
            variable_type: VariableType::Double,
            name: "e",
            format: "%10.0g",
            value_label_name: "",
            label: "",
        };
        let variables = [variable_a, variable_b, variable_c, variable_d, variable_e];
        let data = serialize_file(Release::V118, ByteOrder::LittleEndian, &variables, &[]);
        let schema = read_schema(data);

        assert_eq!(schema.row_len(), 1 + 2 + 4 + 4 + 8);
        let actual_variables = schema.variables();
        assert_eq!(actual_variables[0].name(), "a");
        assert_eq!(actual_variables[1].name(), "b");
        assert_eq!(actual_variables[2].name(), "c");
        assert_eq!(actual_variables[3].name(), "d");
        assert_eq!(actual_variables[4].name(), "e");
    }

    #[test]
    fn xml_v117_big_endian() {
        let variable = TestVariable {
            variable_type: VariableType::Double,
            name: "val",
            format: "%10.0g",
            value_label_name: "",
            label: "Value",
        };
        let variables = [variable];
        let data = serialize_file(Release::V117, ByteOrder::BigEndian, &variables, &[]);
        let schema = read_schema(data);

        let actual_variables = schema.variables();
        assert_eq!(actual_variables.len(), 1);
        let actual_variable = &actual_variables[0];
        assert_eq!(actual_variable.variable_type(), VariableType::Double);
        assert_eq!(actual_variable.name(), "val");
    }

    #[test]
    fn xml_v117_with_sort_order() {
        let variable_a = TestVariable {
            variable_type: VariableType::Byte,
            name: "a",
            format: "%9.0g",
            value_label_name: "",
            label: "",
        };
        let variable_b = TestVariable {
            variable_type: VariableType::Byte,
            name: "b",
            format: "%9.0g",
            value_label_name: "",
            label: "",
        };
        let variables = [variable_a, variable_b];
        let data = serialize_file(Release::V117, ByteOrder::LittleEndian, &variables, &[1, 0]);
        let schema = read_schema(data);
        assert_eq!(schema.sort_order(), &[1, 0]);
    }

    #[test]
    fn xml_v117_zero_variables() {
        let data = serialize_file(Release::V117, ByteOrder::LittleEndian, &[], &[]);
        let schema = read_schema(data);

        assert!(schema.variables().is_empty());
        assert_eq!(schema.row_len(), 0);
    }

    #[test]
    fn xml_v117_fixed_string() {
        let city = TestVariable {
            variable_type: VariableType::FixedString(20),
            name: "city",
            format: "%20s",
            value_label_name: "",
            label: "City",
        };
        let variables = [city];
        let data = serialize_file(Release::V117, ByteOrder::LittleEndian, &variables, &[]);
        let schema = read_schema(data);

        let actual_variables = schema.variables();
        let actual_city = &actual_variables[0];
        assert_eq!(actual_city.variable_type(), VariableType::FixedString(20));
        assert_eq!(schema.row_len(), 20);
    }

    // -- Binary expansion field tests ----------------------------------------

    #[test]
    fn binary_v110_expansion_fields_skipped() {
        // v110 uses 4-byte expansion lengths.  Insert a non-trivial
        // expansion entry (data_type=1, length=6, 6 junk bytes) before
        // the terminator.
        let variable = TestVariable {
            variable_type: VariableType::Byte,
            name: "x",
            format: "%9.0g",
            value_label_name: "",
            label: "",
        };
        let variables = [variable];
        let data = serialize_file(Release::V110, ByteOrder::LittleEndian, &variables, &[]);

        // The last 5 bytes are the terminator (type=0, len=0 as u32).
        // Insert an expansion entry before it.
        let terminator_start = data.len() - 5;
        let mut with_entry = data[..terminator_start].to_vec();
        with_entry.push(1); // data_type = 1
        with_entry.extend_from_slice(&6u32.to_le_bytes()); // length = 6
        with_entry.extend_from_slice(&[0xAA; 6]); // 6 junk bytes
        with_entry.extend_from_slice(&data[terminator_start..]); // terminator

        let schema = read_schema(with_entry);
        let actual_variables = schema.variables();
        assert_eq!(actual_variables.len(), 1);
        assert_eq!(actual_variables[0].name(), "x");
    }

    #[test]
    fn binary_v106_expansion_fields_u16_length() {
        // v106: expansion_len_width = 2
        let variable = TestVariable {
            variable_type: VariableType::Double,
            name: "y",
            format: "%9.0g",
            value_label_name: "",
            label: "",
        };
        let variables = [variable];
        let data = serialize_file(Release::V106, ByteOrder::LittleEndian, &variables, &[]);

        // Insert an expansion entry before the terminator (last 3 bytes).
        let terminator_start = data.len() - 3;
        let mut with_entry = data[..terminator_start].to_vec();
        with_entry.push(1); // data_type = 1
        with_entry.extend_from_slice(&4u16.to_le_bytes()); // length = 4
        with_entry.extend_from_slice(&[0xBB; 4]); // 4 junk bytes
        with_entry.extend_from_slice(&data[terminator_start..]); // terminator

        let schema = read_schema(with_entry);
        let actual_variables = schema.variables();
        assert_eq!(actual_variables.len(), 1);
        assert_eq!(actual_variables[0].variable_type(), VariableType::Double);
    }

    // -- Schema builder validation tests -------------------------------------

    #[test]
    fn schema_builder_valid_sort_order() {
        use crate::stata::dta::schema::Schema;

        let schema = Schema::builder()
            .add_variable(Variable::builder(VariableType::Byte, "a"))
            .add_variable(Variable::builder(VariableType::Int, "b"))
            .add_variable(Variable::builder(VariableType::Double, "c"))
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

        let error = Schema::builder()
            .add_variable(Variable::builder(VariableType::Byte, "a"))
            .add_variable(Variable::builder(VariableType::Byte, "b"))
            .sort_order(vec![0, 5])
            .build()
            .unwrap_err();

        assert!(matches!(
            error,
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
            .add_variable(Variable::builder(VariableType::Byte, "x"))
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

        let error = Schema::builder().sort_order(vec![0]).build().unwrap_err();

        assert!(matches!(
            error,
            DtaError::SortOrderOutOfBounds {
                index: 0,
                variable_count: 0,
            }
        ));
    }

    #[test]
    fn binary_sort_order_out_of_bounds_returns_error() {
        // `Schema::builder` rejects an out-of-bounds sort entry at
        // build time, so the writer can't produce this file. Hand-craft
        // the bytes to verify the reader catches the same invariant
        // when parsing raw input.
        let release = Release::V114;
        let byte_order = ByteOrder::LittleEndian;
        let mut data = vec![
            release.to_byte(),
            byte_order.to_header_byte(release).unwrap(),
            0x01, // filetype
            0x00, // padding
        ];
        // Header: K=1, N=0, 81-byte label, 18-byte timestamp.
        data.extend_from_slice(&byte_order.write_u16(1));
        data.extend_from_slice(&byte_order.write_u32(0));
        data.extend_from_slice(&[0u8; 81]);
        data.extend_from_slice(&[0u8; 18]);
        // Type list: single Byte variable (0xFB for V111+).
        data.push(0xFB);
        // Variable name list: "a" in 33 null-padded bytes.
        let mut name = [0u8; 33];
        name[0] = b'a';
        data.extend_from_slice(&name);
        // Sort list: (variable_count + 1) = 2 entries. Deliberately
        // out-of-bounds: 6 (1-based) = 5 (0-based), then terminator 0.
        data.extend_from_slice(&byte_order.write_u16(6));
        data.extend_from_slice(&byte_order.write_u16(0));
        // Format list: "%9.0g" in 49 null-padded bytes.
        let mut format = [0u8; 49];
        format[..5].copy_from_slice(b"%9.0g");
        data.extend_from_slice(&format);
        // Value-label names list (1 × 33 empty).
        data.extend_from_slice(&[0u8; 33]);
        // Variable labels (1 × 81 empty).
        data.extend_from_slice(&[0u8; 81]);
        // Expansion-field terminator: data_type=0 + u32 length=0.
        data.extend_from_slice(&[0u8; 5]);

        let error = DtaReader::default()
            .from_reader(Cursor::new(data))
            .read_header()
            .unwrap()
            .read_schema()
            .unwrap_err();

        assert!(matches!(
            error,
            DtaError::SortOrderOutOfBounds {
                index: 5,
                variable_count: 1,
            }
        ));
    }
}
