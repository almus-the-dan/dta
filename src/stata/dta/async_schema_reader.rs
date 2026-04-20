use tokio::io::AsyncRead;

use super::async_characteristic_reader::AsyncCharacteristicReader;
use super::async_reader_state::AsyncReaderState;
use super::byte_order::ByteOrder;
use super::dta_error::{DtaError, Field, FormatErrorKind, Result, Section};
use super::header::Header;
use super::release::Release;
use super::schema::Schema;
use super::schema_parse::{
    assemble_variables, offset_position, parse_type_code, read_u64_at, unsupported_variable_count,
};
use super::section_offsets::SectionOffsets;
use super::string_decoding::decode_fixed_string;
use super::variable_type::VariableType;

/// Reads variable definitions from a DTA file asynchronously.
///
/// Owns the parsed [`Header`] from the previous phase. Call
/// [`read_schema`](Self::read_schema) to parse variable definitions.
#[derive(Debug)]
pub struct AsyncSchemaReader<R> {
    state: AsyncReaderState<R>,
    header: Header,
}

impl<R> AsyncSchemaReader<R> {
    #[must_use]
    pub(crate) fn new(state: AsyncReaderState<R>, header: Header) -> Self {
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

impl<R: AsyncRead + Unpin> AsyncSchemaReader<R> {
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
    pub async fn read_schema(mut self) -> Result<AsyncCharacteristicReader<R>> {
        let release = self.header.release();
        let byte_order = self.header.byte_order();
        let variable_count = usize::try_from(self.header.variable_count())
            .map_err(|_| DtaError::io(Section::Schema, unsupported_variable_count()))?;

        if release.is_xml_like() {
            let offsets = self.read_map(byte_order).await?;
            self.state.set_section_offsets(offsets);
        }

        let variable_types = self
            .read_variable_types(variable_count, release, byte_order)
            .await?;
        let variable_names = self.read_variable_names(variable_count, release).await?;
        let sort_order = self
            .read_sort_order(variable_count, release, byte_order)
            .await?;
        let formats = self.read_formats(variable_count, release).await?;
        let value_label_names = self.read_value_label_names(variable_count, release).await?;
        let variable_labels = self.read_variable_labels(variable_count, release).await?;

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
        Ok(AsyncCharacteristicReader::new(
            self.state,
            self.header,
            schema,
        ))
    }
}

// ---------------------------------------------------------------------------
// XML tag handling
// ---------------------------------------------------------------------------

impl<R: AsyncRead + Unpin> AsyncSchemaReader<R> {
    /// Validates an expected XML section tag. No-op for binary formats.
    async fn expect_tag(&mut self, tag: &[u8]) -> Result<()> {
        if self.header.release().is_xml_like() {
            self.state
                .expect_bytes(tag, Section::Schema, FormatErrorKind::InvalidMagic)
                .await?;
        }
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Map section (XML only, 14 × u64 offsets)
// ---------------------------------------------------------------------------

impl<R: AsyncRead + Unpin> AsyncSchemaReader<R> {
    /// Reads the `<map>` section and returns offsets for each
    /// post-schema section.
    async fn read_map(&mut self, byte_order: ByteOrder) -> Result<SectionOffsets> {
        self.expect_tag(b"<map>").await?;

        let buffer = self.state.read_exact(14 * 8, Section::Schema).await?;
        let characteristics_offset = read_u64_at(buffer, 8, byte_order);
        let data_offset = read_u64_at(buffer, 9, byte_order);
        let long_strings_offset = read_u64_at(buffer, 10, byte_order);
        let value_labels_offset = read_u64_at(buffer, 11, byte_order);

        self.expect_tag(b"</map>").await?;
        Ok(SectionOffsets::new(
            characteristics_offset,
            data_offset,
            value_labels_offset,
            Some(long_strings_offset),
        ))
    }
}

// ---------------------------------------------------------------------------
// Descriptor sub-sections
// ---------------------------------------------------------------------------

impl<R: AsyncRead + Unpin> AsyncSchemaReader<R> {
    async fn read_variable_types(
        &mut self,
        variable_count: usize,
        release: Release,
        byte_order: ByteOrder,
    ) -> Result<Vec<VariableType>> {
        self.expect_tag(b"<variable_types>").await?;

        let entry_len = release.type_list_entry_len();
        let section_start = self.state.position();
        let buffer = self
            .state
            .read_exact(variable_count * entry_len, Section::Schema)
            .await?;

        let mut types = Vec::with_capacity(variable_count);
        for index in 0..variable_count {
            let code = if entry_len == 1 {
                u16::from(buffer[index])
            } else {
                let offset = index * 2;
                let bytes = [buffer[offset], buffer[offset + 1]];
                byte_order.read_u16(bytes)
            };
            let position = offset_position(section_start, index * entry_len)?;
            types.push(parse_type_code(code, release, position)?);
        }

        self.expect_tag(b"</variable_types>").await?;
        Ok(types)
    }

    async fn read_variable_names(
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
        .await
    }

    async fn read_sort_order(
        &mut self,
        variable_count: usize,
        release: Release,
        byte_order: ByteOrder,
    ) -> Result<Vec<u32>> {
        self.expect_tag(b"<sortlist>").await?;

        let extended = release.supports_extended_sort_entry();
        let entry_len = if extended { 4 } else { 2 };
        let entry_count = variable_count + 1;
        let buffer = self
            .state
            .read_exact(entry_count * entry_len, Section::Schema)
            .await?;

        let mut sort_order = Vec::new();
        for index in 0..entry_count {
            let offset = index * entry_len;
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

            if index == 0 {
                break;
            }
            sort_order.push(index - 1);
        }

        self.expect_tag(b"</sortlist>").await?;
        Ok(sort_order)
    }

    async fn read_formats(
        &mut self,
        variable_count: usize,
        release: Release,
    ) -> Result<Vec<String>> {
        self.read_fixed_string_array(
            variable_count,
            release.format_entry_len(),
            b"<formats>",
            b"</formats>",
            Field::VariableFormat,
        )
        .await
    }

    async fn read_value_label_names(
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
        .await
    }

    async fn read_variable_labels(
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
        .await
    }
}

// ---------------------------------------------------------------------------
// Fixed-length string array helper
// ---------------------------------------------------------------------------

impl<R: AsyncRead + Unpin> AsyncSchemaReader<R> {
    async fn read_fixed_string_array(
        &mut self,
        count: usize,
        entry_len: usize,
        open_tag: &[u8],
        close_tag: &[u8],
        field: Field,
    ) -> Result<Vec<String>> {
        self.expect_tag(open_tag).await?;

        let encoding = self.state.encoding();
        let section_start = self.state.position();
        let buffer = self
            .state
            .read_exact(count * entry_len, Section::Schema)
            .await?;

        let mut result = Vec::with_capacity(count);
        for index in 0..count {
            let start = index * entry_len;
            let position = offset_position(section_start, start)?;
            let raw = &buffer[start..start + entry_len];
            let value = decode_fixed_string(raw, encoding, Section::Schema, field, position)?;
            result.push(value);
        }

        self.expect_tag(close_tag).await?;
        Ok(result)
    }
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;

    use super::*;
    use crate::stata::dta::byte_order::ByteOrder;
    use crate::stata::dta::dta_reader::DtaReader;
    use crate::stata::dta::dta_writer::DtaWriter;
    use crate::stata::dta::variable::Variable;
    use crate::stata::dta::variable_type::VariableType;

    /// Writes `header` + `schema` through the async writer pipeline,
    /// reads the header + schema back through the async reader
    /// pipeline, returning the parsed schema.
    async fn read_back(header: Header, schema: Schema) -> Schema {
        let cursor: Cursor<Vec<u8>> = DtaWriter::new()
            .from_tokio_writer(Cursor::new(Vec::<u8>::new()))
            .write_header(header)
            .await
            .unwrap()
            .write_schema(schema)
            .await
            .unwrap()
            .into_record_writer()
            .await
            .unwrap()
            .into_long_string_writer()
            .await
            .unwrap()
            .finish()
            .await
            .unwrap();
        let bytes = cursor.into_inner();
        DtaReader::new()
            .from_tokio_reader(bytes.as_slice())
            .read_header()
            .await
            .unwrap()
            .read_schema()
            .await
            .unwrap()
            .schema()
            .clone()
    }

    fn make_header(release: Release, byte_order: ByteOrder, schema: &Schema) -> Header {
        Header::builder(release, byte_order)
            .variable_count(u32::try_from(schema.variables().len()).unwrap())
            .build()
    }

    // -- Binary reader happy-paths (formats 104–116) -------------------------

    #[tokio::test]
    async fn binary_v114_mixed_types() {
        let schema = Schema::builder()
            .add_variable(
                Variable::builder(VariableType::Byte, "x")
                    .format("%9.0g")
                    .label("X var"),
            )
            .add_variable(
                Variable::builder(VariableType::FixedString(10), "city")
                    .format("%10s")
                    .label("City"),
            )
            .build()
            .unwrap();
        let header = make_header(Release::V114, ByteOrder::LittleEndian, &schema);
        let parsed = read_back(header, schema).await;
        assert_eq!(parsed.variables().len(), 2);
        assert_eq!(
            parsed.variables()[1].variable_type(),
            VariableType::FixedString(10)
        );
    }

    #[tokio::test]
    async fn binary_v104_old_type_codes() {
        let schema = Schema::builder()
            .add_variable(Variable::builder(VariableType::Int, "a").format("%8.0g"))
            .add_variable(Variable::builder(VariableType::FixedString(5), "b").format("%5s"))
            .build()
            .unwrap();
        let header = make_header(Release::V104, ByteOrder::LittleEndian, &schema);
        let parsed = read_back(header, schema).await;
        assert_eq!(parsed.variables()[0].variable_type(), VariableType::Int);
        assert_eq!(
            parsed.variables()[1].variable_type(),
            VariableType::FixedString(5)
        );
    }

    #[tokio::test]
    async fn binary_v114_with_sort_order() {
        let schema = Schema::builder()
            .add_variable(Variable::builder(VariableType::Byte, "a").format("%9.0g"))
            .add_variable(Variable::builder(VariableType::Byte, "b").format("%9.0g"))
            .add_variable(Variable::builder(VariableType::Byte, "c").format("%9.0g"))
            .sort_order(vec![2, 0])
            .build()
            .unwrap();
        let header = make_header(Release::V114, ByteOrder::BigEndian, &schema);
        let parsed = read_back(header, schema).await;
        assert_eq!(parsed.sort_order(), &[2, 0]);
    }

    // -- XML reader happy-paths (formats 117–119) ----------------------------

    #[tokio::test]
    async fn xml_v117_mixed_types() {
        let schema = Schema::builder()
            .add_variable(
                Variable::builder(VariableType::Int, "count")
                    .format("%8.0g")
                    .value_label_name("cntlbl"),
            )
            .add_variable(Variable::builder(VariableType::LongString, "notes").format("%9s"))
            .build()
            .unwrap();
        let header = make_header(Release::V117, ByteOrder::LittleEndian, &schema);
        let parsed = read_back(header, schema).await;
        assert_eq!(parsed.variables().len(), 2);
        assert_eq!(parsed.variables()[0].value_label_name(), "cntlbl");
        assert_eq!(
            parsed.variables()[1].variable_type(),
            VariableType::LongString
        );
    }

    #[tokio::test]
    async fn xml_v118_numeric_types() {
        let schema = Schema::builder()
            .add_variable(Variable::builder(VariableType::Byte, "a").format("%8.0g"))
            .add_variable(Variable::builder(VariableType::Double, "b").format("%10.0g"))
            .build()
            .unwrap();
        let header = make_header(Release::V118, ByteOrder::LittleEndian, &schema);
        let parsed = read_back(header, schema).await;
        assert_eq!(parsed.variables()[0].variable_type(), VariableType::Byte);
        assert_eq!(parsed.variables()[1].variable_type(), VariableType::Double);
    }

    #[tokio::test]
    async fn xml_v117_zero_variables() {
        let schema = Schema::builder().build().unwrap();
        let header = make_header(Release::V117, ByteOrder::LittleEndian, &schema);
        let parsed = read_back(header, schema).await;
        assert!(parsed.variables().is_empty());
    }
}
