use std::io::{Seek, Write};

use super::byte_order::ByteOrder;
use super::characteristic_writer::CharacteristicWriter;
use super::dta_error::{DtaError, Field, FormatErrorKind, Result, Section};
use super::header::Header;
use super::release::Release;
use super::schema::Schema;
use super::variable::Variable;
use super::writer_state::WriterState;

/// Writes variable definitions to a DTA file.
///
/// Owns the [`Header`] emitted by the previous phase. Call
/// [`write_schema`](Self::write_schema) to emit the variable
/// descriptors (type codes, names, sort order, display formats,
/// value-label associations, and variable labels) and advance to
/// characteristic writing.
///
/// For XML formats (117+), [`write_schema`](Self::write_schema) also
/// emits the `<map>` section with placeholder offsets, which later
/// writers patch as each section is completed.
#[derive(Debug)]
pub struct SchemaWriter<W> {
    state: WriterState<W>,
    header: Header,
}

impl<W> SchemaWriter<W> {
    #[must_use]
    pub(crate) fn new(state: WriterState<W>, header: Header) -> Self {
        Self { state, header }
    }

    /// The header emitted by the previous phase.
    #[must_use]
    #[inline]
    pub fn header(&self) -> &Header {
        &self.header
    }

    /// Consumes the writer and returns the underlying state, used by
    /// tests that want to recover the sink before `write_schema` is
    /// implemented.
    #[cfg(test)]
    pub(crate) fn into_state(self) -> WriterState<W> {
        self.state
    }
}

impl<W: Write + Seek> SchemaWriter<W> {
    /// Writes the `<map>` (XML only) and variable descriptor
    /// subsections, then transitions to characteristic writing.
    ///
    /// # Errors
    ///
    /// Returns [`DtaError::Io`](DtaError::Io) on
    /// sink failures and [`DtaError::Format`](DtaError::Format)
    /// if the schema cannot be represented in the header's release
    /// (e.g., `strL` columns in a pre-117 format, or variable names
    /// that exceed the fixed-field width).
    pub fn write_schema(mut self, schema: Schema) -> Result<CharacteristicWriter<W>> {
        let release = self.header.release();
        let byte_order = self.header.byte_order();
        let is_xml = release.is_xml_like();

        self.validate_variable_types(&schema)?;
        self.patch_header_variable_count(&schema)?;

        // For XML formats, capture the absolute byte offset of each
        // descriptor (sub)section as we write it, then patch the map
        // placeholders at the end. `descriptor_offsets[i]` mirrors
        // the `<map>` payload layout: indices 0–8 are filled here,
        // 9–11 are filled by downstream writers via
        // `WriterState::patch_map_entry`.
        //
        // Index 0 is the offset of the opening `<stata_dta>` tag,
        // which is always 0 — so the zero-initialized slot is already
        // correct, and we never assign to it explicitly.
        let mut descriptor_offsets = [0u64; 14];

        if is_xml {
            // <map>
            descriptor_offsets[1] = self.state.position();
            self.state.write_exact(b"<map>", Section::Schema)?;
            self.state.set_map_offset_base(self.state.position());
            for _ in 0..14 {
                self.state.write_u64(0, byte_order, Section::Schema)?;
            }
            self.state.write_exact(b"</map>", Section::Schema)?;
        }

        // <variable_types>
        descriptor_offsets[2] = self.state.position();
        self.write_variable_types(&schema, release, byte_order, is_xml)?;

        // <varnames>
        descriptor_offsets[3] = self.state.position();
        self.write_fixed_string_array(
            &schema,
            release.variable_name_len(),
            xml_tags(is_xml, b"<varnames>", b"</varnames>"),
            Field::VariableName,
            Variable::name,
        )?;

        // <sortlist>
        descriptor_offsets[4] = self.state.position();
        self.write_sort_order(&schema, release, byte_order, is_xml)?;

        // <formats>
        descriptor_offsets[5] = self.state.position();
        self.write_fixed_string_array(
            &schema,
            release.format_entry_len(),
            xml_tags(is_xml, b"<formats>", b"</formats>"),
            Field::VariableFormat,
            Variable::format,
        )?;

        // <value_label_names>
        descriptor_offsets[6] = self.state.position();
        self.write_fixed_string_array(
            &schema,
            release.value_label_name_len(),
            xml_tags(is_xml, b"<value_label_names>", b"</value_label_names>"),
            Field::ValueLabelName,
            Variable::value_label_name,
        )?;

        // <variable_labels>
        descriptor_offsets[7] = self.state.position();
        self.write_fixed_string_array(
            &schema,
            release.variable_label_len(),
            xml_tags(is_xml, b"<variable_labels>", b"</variable_labels>"),
            Field::VariableLabel,
            Variable::label,
        )?;

        // <characteristics>
        descriptor_offsets[8] = self.state.position();

        self.finalize_schema_section(&descriptor_offsets)?;

        Ok(CharacteristicWriter::new(self.state, self.header, schema))
    }

    /// Seek-patches the header's K (variable count) field with
    /// `schema.variables().len()`, narrowing to the field's on-disk
    /// width (`u16` for pre-V119, `u32` for V119+). Called before
    /// any schema bytes are written so overflow surfaces before the
    /// file gets polluted.
    fn patch_header_variable_count(&mut self, schema: &Schema) -> Result<()> {
        let release = self.header.release();
        let byte_order = self.header.byte_order();
        let offset = self
            .state
            .header_variable_count_offset()
            .expect("header writer set K offset before SchemaWriter was constructed");
        let count = self.state.narrow_to_u32(
            u64::try_from(schema.variables().len()).expect("variable count exceeds u64"),
            Section::Header,
            Field::VariableCount,
        )?;
        if release.supports_extended_variable_count() {
            self.state
                .patch_u32_at(offset, count, byte_order, Section::Header)?;
        } else {
            let narrowed =
                self.state
                    .narrow_to_u16(count, Section::Header, Field::VariableCount)?;
            self.state
                .patch_u16_at(offset, narrowed, byte_order, Section::Header)?;
        }
        Ok(())
    }

    /// Validates that every variable's type is representable in the
    /// header's release — flags `strL` in pre-117 formats and fixed
    /// strings wider than [`Release::max_fixed_string_len`].
    fn validate_variable_types(&self, schema: &Schema) -> Result<()> {
        let release = self.header.release();
        let position = self.state.position();
        for variable in schema.variables() {
            if variable.variable_type().try_to_u16(release).is_none() {
                return Err(DtaError::format(
                    Section::Schema,
                    position,
                    FormatErrorKind::UnsupportedVariableType {
                        variable_type: variable.variable_type(),
                        release,
                    },
                ));
            }
        }
        Ok(())
    }

    /// Patches the XML `<map>` slots the schema writer now knows
    /// (indices 0–8).
    ///
    /// Downstream writers fill indices 9-11 (`data`, `strls`, `value_labels`)
    /// and 12-13 (end-of-file markers) via [`WriterState::patch_map_entry`].
    /// Binary formats have no map, so this is a no-op for them.
    fn finalize_schema_section(&mut self, descriptor_offsets: &[u64; 14]) -> Result<()> {
        if !self.header.release().is_xml_like() {
            return Ok(());
        }
        let byte_order = self.header.byte_order();
        for (index, &offset) in descriptor_offsets.iter().enumerate().take(9) {
            self.state
                .patch_map_entry(index, offset, byte_order, Section::Schema)?;
        }
        Ok(())
    }
}

impl<W: Write> SchemaWriter<W> {
    /// Writes the type list: one 1-byte code per variable for pre-117
    /// formats, one 2-byte code for 117+. Assumes
    /// [`validate_variable_types`](Self::validate_variable_types)
    /// has already been called.
    fn write_variable_types(
        &mut self,
        schema: &Schema,
        release: Release,
        byte_order: ByteOrder,
        is_xml: bool,
    ) -> Result<()> {
        if is_xml {
            self.state
                .write_exact(b"<variable_types>", Section::Schema)?;
        }
        let entry_len = release.type_list_entry_len();
        for variable in schema.variables() {
            let code = variable
                .variable_type()
                .try_to_u16(release)
                .expect("variable type validated up front");
            if entry_len == 1 {
                let narrow = u8::try_from(code).expect("pre-117 type code fits u8");
                self.state.write_u8(narrow, Section::Schema)?;
            } else {
                self.state.write_u16(code, byte_order, Section::Schema)?;
            }
        }
        if is_xml {
            self.state
                .write_exact(b"</variable_types>", Section::Schema)?;
        }
        Ok(())
    }

    /// Writes the sort list: `variable_count + 1` entries of 1-based
    /// variable indices followed by zero padding. The user-facing
    /// [`Schema::sort_order`] stores 0-based indices; this method
    /// adds 1 on the way out.
    fn write_sort_order(
        &mut self,
        schema: &Schema,
        release: Release,
        byte_order: ByteOrder,
        is_xml: bool,
    ) -> Result<()> {
        if is_xml {
            self.state.write_exact(b"<sortlist>", Section::Schema)?;
        }
        let slot_count = schema.variables().len() + 1;
        let entry_len = release.sort_entry_len();
        for index in 0..slot_count {
            let on_disk = schema
                .sort_order()
                .get(index)
                .map_or(0, |&stored| stored + 1);
            if entry_len == 2 {
                let narrow =
                    self.state
                        .narrow_to_u16(on_disk, Section::Schema, Field::SortOrder)?;
                self.state.write_u16(narrow, byte_order, Section::Schema)?;
            } else {
                self.state.write_u32(on_disk, byte_order, Section::Schema)?;
            }
        }
        if is_xml {
            self.state.write_exact(b"</sortlist>", Section::Schema)?;
        }
        Ok(())
    }

    /// Writes a per-variable array of fixed-length, null-padded
    /// strings optionally wrapped in XML open/close tags. `selector`
    /// picks which field to pull from each [`Variable`].
    fn write_fixed_string_array(
        &mut self,
        schema: &Schema,
        entry_len: usize,
        xml_tags: Option<(&[u8], &[u8])>,
        field: Field,
        selector: fn(&Variable) -> &str,
    ) -> Result<()> {
        if let Some((open, _)) = xml_tags {
            self.state.write_exact(open, Section::Schema)?;
        }
        for variable in schema.variables() {
            self.state
                .write_fixed_string(selector(variable), entry_len, Section::Schema, field)?;
        }
        if let Some((_, close)) = xml_tags {
            self.state.write_exact(close, Section::Schema)?;
        }
        Ok(())
    }
}

/// Returns `Some((open, close))` when `is_xml` is true, `None`
/// otherwise — short-circuits the call-site noise at
/// [`SchemaWriter::write_fixed_string_array`].
fn xml_tags<'a>(is_xml: bool, open: &'a [u8], close: &'a [u8]) -> Option<(&'a [u8], &'a [u8])> {
    if is_xml { Some((open, close)) } else { None }
}

// ===========================================================================
// Tests
// ===========================================================================

#[cfg(test)]
mod tests {
    use std::io::Cursor;

    use super::*;
    use crate::stata::dta::byte_order::ByteOrder;
    use crate::stata::dta::dta_reader::DtaReader;
    use crate::stata::dta::dta_writer::DtaWriter;
    use crate::stata::dta::variable::Variable;
    use crate::stata::dta::variable_type::VariableType;

    // -- Helpers -------------------------------------------------------------

    /// Writes `header` + `schema` using the real writer, then reads
    /// the resulting bytes back using the real reader. Stops after
    /// the schema read since downstream writers aren't implemented
    /// yet — so the test only round-trips the schema, not the full
    /// file.
    fn round_trip(header: Header, schema: Schema) -> (Header, Schema) {
        let buffer = Cursor::new(Vec::<u8>::new());
        let characteristic_writer = DtaWriter::new()
            .from_writer(buffer)
            .write_header(header)
            .unwrap()
            .write_schema(schema)
            .unwrap();
        let bytes = characteristic_writer.into_state().into_inner().into_inner();

        let header_reader = DtaReader::new()
            .from_reader(Cursor::new(bytes))
            .read_header()
            .unwrap();
        let schema_reader = header_reader.read_schema().unwrap();
        (
            schema_reader.header().clone(),
            schema_reader.schema().clone(),
        )
    }

    /// Creates a minimal header that matches `schema.variables().len()`.
    fn make_header(release: Release, byte_order: ByteOrder, schema: &Schema) -> Header {
        Header::builder(release, byte_order)
            .variable_count(u32::try_from(schema.variables().len()).unwrap())
            .build()
    }

    // -- Binary round-trips (formats 104–116) --------------------------------

    #[test]
    fn binary_v114_mixed_types() {
        let schema = Schema::builder()
            .add_variable(
                Variable::builder(VariableType::Byte, "x")
                    .format("%9.0g")
                    .label("The X var"),
            )
            .add_variable(
                Variable::builder(VariableType::FixedString(10), "city")
                    .format("%10s")
                    .label("City name"),
            )
            .add_variable(
                Variable::builder(VariableType::Double, "price")
                    .format("%10.2f")
                    .value_label_name("pricelbl")
                    .label("Price in USD"),
            )
            .build()
            .unwrap();
        let header = make_header(Release::V114, ByteOrder::LittleEndian, &schema);
        let (_, parsed) = round_trip(header, schema);

        assert_eq!(parsed.variables().len(), 3);
        assert_eq!(parsed.variables()[0].name(), "x");
        assert_eq!(parsed.variables()[0].variable_type(), VariableType::Byte);
        assert_eq!(parsed.variables()[0].format(), "%9.0g");
        assert_eq!(parsed.variables()[0].label(), "The X var");
        assert_eq!(
            parsed.variables()[1].variable_type(),
            VariableType::FixedString(10)
        );
        assert_eq!(parsed.variables()[2].value_label_name(), "pricelbl");
        assert_eq!(parsed.variables()[2].label(), "Price in USD");
    }

    #[test]
    fn binary_v114_big_endian() {
        let schema = Schema::builder()
            .add_variable(Variable::builder(VariableType::Long, "id").format("%12.0g"))
            .build()
            .unwrap();
        let header = make_header(Release::V114, ByteOrder::BigEndian, &schema);
        let (parsed_header, parsed_schema) = round_trip(header, schema);
        assert_eq!(parsed_header.byte_order(), ByteOrder::BigEndian);
        assert_eq!(
            parsed_schema.variables()[0].variable_type(),
            VariableType::Long
        );
    }

    #[test]
    fn binary_v104_old_type_codes() {
        let schema = Schema::builder()
            .add_variable(Variable::builder(VariableType::Int, "a").format("%8.0g"))
            .add_variable(Variable::builder(VariableType::FixedString(10), "b").format("%10s"))
            .build()
            .unwrap();
        let header = make_header(Release::V104, ByteOrder::LittleEndian, &schema);
        let (_, parsed) = round_trip(header, schema);
        assert_eq!(parsed.variables()[0].variable_type(), VariableType::Int);
        assert_eq!(
            parsed.variables()[1].variable_type(),
            VariableType::FixedString(10)
        );
    }

    #[test]
    fn binary_v110_all_numeric_types() {
        let schema = Schema::builder()
            .add_variable(Variable::builder(VariableType::Byte, "a").format("%8.0g"))
            .add_variable(Variable::builder(VariableType::Int, "b").format("%8.0g"))
            .add_variable(Variable::builder(VariableType::Long, "c").format("%12.0g"))
            .add_variable(Variable::builder(VariableType::Float, "d").format("%9.0g"))
            .add_variable(Variable::builder(VariableType::Double, "e").format("%10.0g"))
            .build()
            .unwrap();
        let header = make_header(Release::V110, ByteOrder::LittleEndian, &schema);
        let (_, parsed) = round_trip(header, schema);
        assert_eq!(parsed.variables().len(), 5);
        assert_eq!(parsed.variables()[0].variable_type(), VariableType::Byte);
        assert_eq!(parsed.variables()[4].variable_type(), VariableType::Double);
    }

    #[test]
    fn binary_v114_sort_order_round_trips() {
        let schema = Schema::builder()
            .add_variable(Variable::builder(VariableType::Byte, "a").format("%9.0g"))
            .add_variable(Variable::builder(VariableType::Byte, "b").format("%9.0g"))
            .add_variable(Variable::builder(VariableType::Byte, "c").format("%9.0g"))
            .sort_order(vec![2, 0])
            .build()
            .unwrap();
        let header = make_header(Release::V114, ByteOrder::LittleEndian, &schema);
        let (_, parsed) = round_trip(header, schema);
        assert_eq!(parsed.sort_order(), &[2, 0]);
    }

    #[test]
    fn binary_v114_empty_sort_order() {
        let schema = Schema::builder()
            .add_variable(Variable::builder(VariableType::Double, "y").format("%10.0g"))
            .build()
            .unwrap();
        let header = make_header(Release::V114, ByteOrder::LittleEndian, &schema);
        let (_, parsed) = round_trip(header, schema);
        assert!(parsed.sort_order().is_empty());
    }

    #[test]
    fn binary_v114_zero_variables() {
        let schema = Schema::builder().build().unwrap();
        let header = make_header(Release::V114, ByteOrder::LittleEndian, &schema);
        let (_, parsed) = round_trip(header, schema);
        assert!(parsed.variables().is_empty());
        assert!(parsed.sort_order().is_empty());
    }

    // -- XML round-trips (formats 117–119) ----------------------------------

    #[test]
    fn xml_v117_mixed_types() {
        let schema = Schema::builder()
            .add_variable(
                Variable::builder(VariableType::Int, "count")
                    .format("%8.0g")
                    .value_label_name("cntlbl")
                    .label("Count"),
            )
            .add_variable(
                Variable::builder(VariableType::LongString, "notes")
                    .format("%9s")
                    .label("Notes field"),
            )
            .build()
            .unwrap();
        let header = make_header(Release::V117, ByteOrder::LittleEndian, &schema);
        let (_, parsed) = round_trip(header, schema);
        assert_eq!(parsed.variables().len(), 2);
        assert_eq!(parsed.variables()[0].value_label_name(), "cntlbl");
        assert_eq!(
            parsed.variables()[1].variable_type(),
            VariableType::LongString
        );
    }

    #[test]
    fn xml_v118_all_numeric_types() {
        let schema = Schema::builder()
            .add_variable(Variable::builder(VariableType::Byte, "a").format("%8.0g"))
            .add_variable(Variable::builder(VariableType::Int, "b").format("%8.0g"))
            .add_variable(Variable::builder(VariableType::Long, "c").format("%12.0g"))
            .add_variable(Variable::builder(VariableType::Float, "d").format("%9.0g"))
            .add_variable(Variable::builder(VariableType::Double, "e").format("%10.0g"))
            .build()
            .unwrap();
        let header = make_header(Release::V118, ByteOrder::LittleEndian, &schema);
        let (_, parsed) = round_trip(header, schema);
        assert_eq!(parsed.variables().len(), 5);
        assert_eq!(parsed.variables()[4].variable_type(), VariableType::Double);
    }

    #[test]
    fn xml_v119_big_endian_with_sort_order() {
        let schema = Schema::builder()
            .add_variable(Variable::builder(VariableType::Byte, "a").format("%9.0g"))
            .add_variable(Variable::builder(VariableType::Byte, "b").format("%9.0g"))
            .sort_order(vec![1, 0])
            .build()
            .unwrap();
        let header = make_header(Release::V119, ByteOrder::BigEndian, &schema);
        let (parsed_header, parsed_schema) = round_trip(header, schema);
        assert_eq!(parsed_header.byte_order(), ByteOrder::BigEndian);
        assert_eq!(parsed_schema.sort_order(), &[1, 0]);
    }

    #[test]
    fn xml_v117_fixed_string() {
        let schema = Schema::builder()
            .add_variable(
                Variable::builder(VariableType::FixedString(20), "city")
                    .format("%20s")
                    .label("City"),
            )
            .build()
            .unwrap();
        let header = make_header(Release::V117, ByteOrder::LittleEndian, &schema);
        let (_, parsed) = round_trip(header, schema);
        assert_eq!(
            parsed.variables()[0].variable_type(),
            VariableType::FixedString(20)
        );
        assert_eq!(parsed.variables()[0].format(), "%20s");
        assert_eq!(parsed.variables()[0].label(), "City");
    }

    #[test]
    fn xml_v117_zero_variables() {
        let schema = Schema::builder().build().unwrap();
        let header = make_header(Release::V117, ByteOrder::LittleEndian, &schema);
        let (_, parsed) = round_trip(header, schema);
        assert!(parsed.variables().is_empty());
    }

    // -- Header K field patching --------------------------------------------

    #[test]
    fn k_field_patched_with_schema_variable_count() {
        // The header writer emits 0 for K; the schema writer must
        // patch it with schema.variables().len() so the reader sees
        // the correct count.
        let schema = Schema::builder()
            .add_variable(Variable::builder(VariableType::Byte, "a").format("%8.0g"))
            .add_variable(Variable::builder(VariableType::Byte, "b").format("%8.0g"))
            .add_variable(Variable::builder(VariableType::Byte, "c").format("%8.0g"))
            .build()
            .unwrap();
        let header = make_header(Release::V114, ByteOrder::LittleEndian, &schema);
        let (parsed_header, _) = round_trip(header, schema);
        assert_eq!(parsed_header.variable_count(), 3);
    }

    #[test]
    fn v119_u32_variable_count_round_trip() {
        // V119 uses a u32 K field; exercise it with > u16::MAX
        // variables to verify the extended-width patch goes through
        // the `patch_u32_at` path and reads back correctly.
        let variables: Vec<_> = (0..70_000)
            .map(|_| Variable::builder(VariableType::Byte, "v").format("%8.0g"))
            .collect();
        let schema = Schema::builder().variables(variables).build().unwrap();
        let header = make_header(Release::V119, ByteOrder::LittleEndian, &schema);
        let (parsed_header, parsed_schema) = round_trip(header, schema);
        assert_eq!(parsed_header.variable_count(), 70_000);
        assert_eq!(parsed_schema.variables().len(), 70_000);
    }

    #[test]
    fn pre_v119_variable_count_overflow_errors() {
        // V114's K field is u16; 70_000 variables should fail the
        // narrow-to-u16 patch step with FieldTooLarge.
        let variables: Vec<_> = (0..70_000)
            .map(|_| Variable::builder(VariableType::Byte, "v").format("%8.0g"))
            .collect();
        let schema = Schema::builder().variables(variables).build().unwrap();
        let header = make_header(Release::V114, ByteOrder::LittleEndian, &schema);
        let error = DtaWriter::new()
            .from_writer(Cursor::new(Vec::<u8>::new()))
            .write_header(header)
            .unwrap()
            .write_schema(schema)
            .unwrap_err();
        assert!(matches!(
            error,
            DtaError::Format(ref e) if matches!(
                e.kind(),
                FormatErrorKind::FieldTooLarge { field: Field::VariableCount, .. }
            )
        ));
    }

    // -- Error cases ---------------------------------------------------------

    #[test]
    fn strl_in_pre_117_errors() {
        let schema = Schema::builder()
            .add_variable(Variable::builder(VariableType::LongString, "notes").format("%9s"))
            .build()
            .unwrap();
        let header = make_header(Release::V114, ByteOrder::LittleEndian, &schema);
        let error = DtaWriter::new()
            .from_writer(Cursor::new(Vec::<u8>::new()))
            .write_header(header)
            .unwrap()
            .write_schema(schema)
            .unwrap_err();
        assert!(matches!(
            error,
            DtaError::Format(ref e) if matches!(
                e.kind(),
                FormatErrorKind::UnsupportedVariableType {
                    variable_type: VariableType::LongString,
                    release: Release::V114,
                }
            )
        ));
    }

    #[test]
    fn oversized_fixed_string_pre_117_errors() {
        // str500 requires format 117+ (max 244 in 111–116, 80 in pre-111).
        let schema = Schema::builder()
            .add_variable(Variable::builder(VariableType::FixedString(500), "x").format("%500s"))
            .build()
            .unwrap();
        let header = make_header(Release::V114, ByteOrder::LittleEndian, &schema);
        let error = DtaWriter::new()
            .from_writer(Cursor::new(Vec::<u8>::new()))
            .write_header(header)
            .unwrap()
            .write_schema(schema)
            .unwrap_err();
        assert!(matches!(
            error,
            DtaError::Format(ref e) if matches!(
                e.kind(),
                FormatErrorKind::UnsupportedVariableType {
                    variable_type: VariableType::FixedString(500),
                    release: Release::V114,
                }
            )
        ));
    }

    #[test]
    fn variable_name_too_long_errors() {
        // v114 limits variable names to 33 bytes; use 40 to overflow.
        let long_name = "v".repeat(40);
        let schema = Schema::builder()
            .add_variable(Variable::builder(VariableType::Byte, long_name).format("%8.0g"))
            .build()
            .unwrap();
        let header = make_header(Release::V114, ByteOrder::LittleEndian, &schema);
        let error = DtaWriter::new()
            .from_writer(Cursor::new(Vec::<u8>::new()))
            .write_header(header)
            .unwrap()
            .write_schema(schema)
            .unwrap_err();
        assert!(matches!(
            error,
            DtaError::Format(ref e) if matches!(
                e.kind(),
                FormatErrorKind::FieldTooLarge { field: Field::VariableName, .. }
            )
        ));
    }

    #[test]
    fn variable_label_too_long_errors() {
        // v117 limits variable labels to 81 bytes; use 100 to overflow.
        let long_label = "x".repeat(100);
        let schema = Schema::builder()
            .add_variable(
                Variable::builder(VariableType::Byte, "a")
                    .format("%8.0g")
                    .label(long_label),
            )
            .build()
            .unwrap();
        let header = make_header(Release::V117, ByteOrder::LittleEndian, &schema);
        let error = DtaWriter::new()
            .from_writer(Cursor::new(Vec::<u8>::new()))
            .write_header(header)
            .unwrap()
            .write_schema(schema)
            .unwrap_err();
        assert!(matches!(
            error,
            DtaError::Format(ref e) if matches!(
                e.kind(),
                FormatErrorKind::FieldTooLarge { field: Field::VariableLabel, .. }
            )
        ));
    }

    #[test]
    fn non_latin_name_in_windows_1252_errors() {
        let schema = Schema::builder()
            .add_variable(Variable::builder(VariableType::Byte, "日本語").format("%8.0g"))
            .build()
            .unwrap();
        let header = make_header(Release::V114, ByteOrder::LittleEndian, &schema);
        let error = DtaWriter::new()
            .from_writer(Cursor::new(Vec::<u8>::new()))
            .write_header(header)
            .unwrap()
            .write_schema(schema)
            .unwrap_err();
        assert!(matches!(
            error,
            DtaError::Format(ref e) if matches!(
                e.kind(),
                FormatErrorKind::InvalidEncoding { field: Field::VariableName }
            )
        ));
    }
}
