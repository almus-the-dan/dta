use std::io::{Seek, Write};

use super::byte_order::ByteOrder;
use super::dta_error::{Field, Result, Section};
use super::header::Header;
use super::long_string_ref::LongStringRef;
use super::long_string_writer::LongStringWriter;
use super::record_format::{
    encode_numeric, encode_record_string, encode_u24, encode_u40, encode_u48,
    narrow_variable_index, observation_count_overflow_error, validate_record_arity,
    validate_record_value_types,
};
use super::release::Release;
use super::schema::Schema;
use super::value::Value;
use super::variable_type::VariableType;
use super::writer_state::WriterState;

/// Writes observation records (data rows) to a DTA file.
///
/// Call [`write_record`](Self::write_record) once per observation,
/// passing a slice of [`Value`]s whose length and types match the
/// schema. Transition via
/// [`into_long_string_writer`](Self::into_long_string_writer) once
/// all rows have been written — that step patches the header's N
/// (observation count) field with the accumulated row count.
#[derive(Debug)]
pub struct RecordWriter<W> {
    state: WriterState<W>,
    header: Header,
    schema: Schema,
    observation_count: u64,
    /// Tracks whether the XML `<data>` opening tag has been emitted.
    /// Unused (but harmless) for binary formats, which have no
    /// section tag.
    opened: bool,
}

impl<W> RecordWriter<W> {
    #[must_use]
    pub(crate) fn new(state: WriterState<W>, header: Header, schema: Schema) -> Self {
        Self {
            state,
            header,
            schema,
            observation_count: 0,
            opened: false,
        }
    }

    /// The header emitted during the header phase.
    #[must_use]
    #[inline]
    pub fn header(&self) -> &Header {
        &self.header
    }

    /// The schema emitted during the schema phase.
    #[must_use]
    #[inline]
    pub fn schema(&self) -> &Schema {
        &self.schema
    }

    /// The encoding this writer uses to encode string fields.
    ///
    /// Defaults to Windows-1252 for pre-V118 releases and UTF-8 for
    /// V118+, overridable via
    /// [`DtaWriter::encoding`](super::dta_writer::DtaWriter::encoding).
    #[must_use]
    #[inline]
    pub fn encoding(&self) -> &'static encoding_rs::Encoding {
        self.state.encoding()
    }
}

impl<W: Write + Seek> RecordWriter<W> {
    /// Writes a single observation row.
    ///
    /// `values` must have exactly one entry per variable in the
    /// schema, in schema order. The first call also emits the XML
    /// `<data>` opening tag for XML formats.
    ///
    /// # Errors
    ///
    /// Returns [`DtaError::Format`](super::dta_error::DtaError::Format) with:
    /// - [`RecordArityMismatch`](super::dta_error::FormatErrorKind::RecordArityMismatch)
    ///   if `values.len() != schema.variables().len()`.
    /// - [`RecordValueTypeMismatch`](super::dta_error::FormatErrorKind::RecordValueTypeMismatch)
    ///   if any value's variant does not match its variable's type.
    /// - [`RecordStringTooLong`](super::dta_error::FormatErrorKind::RecordStringTooLong)
    ///   if a string value exceeds its variable's fixed-width slot.
    /// - [`InvalidEncoding`](super::dta_error::FormatErrorKind::InvalidEncoding) if a
    ///   string cannot be represented in the active encoding.
    /// - [`FieldTooLarge`](super::dta_error::FormatErrorKind::FieldTooLarge) if a
    ///   [`LongStringRef`] component (variable or observation)
    ///   exceeds the on-disk field width.
    pub fn write_record(&mut self, values: &[Value<'_>]) -> Result<()> {
        // Validate upfront so we never write partial row bytes that
        // would corrupt the output if the row turns out invalid
        // further down the line.
        let position = self.state.position();
        validate_record_arity(values.len(), self.schema.variables().len(), position)?;
        validate_record_value_types(values, self.schema.variables(), position)?;

        self.open_section_if_needed()?;

        for (index, value) in values.iter().enumerate() {
            // Read the variable type out eagerly (it's `Copy`) so
            // the borrow of `self.schema` ends before we call the
            // `&mut self` helper below.
            let variables = self.schema.variables();
            let variable = &variables[index];
            let variable_type = variable.variable_type();
            let variable_index = narrow_variable_index(index, self.state.position())?;
            self.write_value(variable_index, variable_type, value)?;
        }

        self.observation_count = self
            .observation_count
            .checked_add(1)
            .ok_or_else(|| observation_count_overflow_error(self.state.position()))?;
        Ok(())
    }

    /// Closes the data section, patches map slot 10 (XML only),
    /// seek-patches the header N field with the accumulated
    /// observation count, and transitions to long-string writing.
    ///
    /// For XML the closing `</data>` tag is emitted even if no rows
    /// were written (the opening tag is lazy-emitted here in that
    /// case). For binary formats the data section has no tags —
    /// nothing is written before the long-string transition.
    ///
    /// # Errors
    ///
    /// Returns [`DtaError::Io`](super::dta_error::DtaError::Io) on
    /// sink failures and
    /// [`DtaError::Format`](super::dta_error::DtaError::Format) with
    /// [`FieldTooLarge`](super::dta_error::FormatErrorKind::FieldTooLarge)
    /// if the observation count exceeds `u32::MAX` on a pre-V118 release.
    ///
    /// # Panics
    ///
    /// Panics if the header writer did not capture the N offset —
    /// an internal invariant of the writer chain.
    pub fn into_long_string_writer(mut self) -> Result<LongStringWriter<W>> {
        self.open_section_if_needed()?;

        let release = self.header.release();
        let byte_order = self.header.byte_order();
        if release.is_xml_like() {
            self.state.write_exact(b"</data>", Section::Records)?;
            let long_strings_offset = self.state.position();
            self.state
                .patch_map_entry(10, long_strings_offset, byte_order, Section::Records)?;
        }

        self.patch_header_observation_count()?;

        let writer = LongStringWriter::new(self.state, self.header, self.schema);
        Ok(writer)
    }

    /// Emits the XML `<data>` tag on first use. No-op for binary
    /// formats (which have no section tag) and on subsequent calls.
    fn open_section_if_needed(&mut self) -> Result<()> {
        if !self.header.release().is_xml_like() {
            return Ok(());
        }
        if !self.opened {
            self.state.write_exact(b"<data>", Section::Records)?;
            self.opened = true;
        }
        Ok(())
    }

    /// Seek-patches the header N field with `self.observation_count`.
    ///
    /// Narrowed to match the release's on-disk width:
    /// `u16` for V102, `u32` for V103–V117, `u64` for V118+.
    fn patch_header_observation_count(&mut self) -> Result<()> {
        let release = self.header.release();
        let byte_order = self.header.byte_order();
        let offset = self
            .state
            .header_observation_count_offset()
            .expect("header writer set N offset before RecordWriter was constructed");
        match release.binary_observation_count_width() {
            8 => {
                self.state.patch_u64_at(
                    offset,
                    self.observation_count,
                    byte_order,
                    Section::Header,
                )?;
            }
            4 => {
                let narrowed = self.state.narrow_to_u32(
                    self.observation_count,
                    Section::Header,
                    Field::ObservationCount,
                )?;
                self.state
                    .patch_u32_at(offset, narrowed, byte_order, Section::Header)?;
            }
            2 => {
                let narrowed = self.state.narrow_to_u16(
                    self.observation_count,
                    Section::Header,
                    Field::ObservationCount,
                )?;
                self.state
                    .patch_u16_at(offset, narrowed, byte_order, Section::Header)?;
            }
            other => unreachable!("unexpected observation count width {}", other),
        }
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Per-variable value serialization
// ---------------------------------------------------------------------------

impl<W: Write> RecordWriter<W> {
    /// Dispatches on the variable type and serializes `value` into
    /// the current row position. The variable's on-disk offset is
    /// implicit in the order of calls — we iterate variables in
    /// schema order, which matches the monotonically increasing
    /// offsets computed by `Schema::builder()::build()`.
    fn write_value(
        &mut self,
        variable_index: u32,
        variable_type: VariableType,
        value: &Value<'_>,
    ) -> Result<()> {
        let byte_order = self.header.byte_order();
        let release = self.header.release();
        let position = self.state.position();
        // `validate_value_types` ran before any writes, so the
        // `value` variant is known to match `variable_type` — we
        // only need to dispatch on the value. `variable_type` is
        // kept as a parameter only because the `FixedString` width
        // lives on it.
        match value {
            &Value::Byte(stata_value) => {
                let raw = encode_numeric(
                    stata_value.to_raw(release),
                    release,
                    variable_index,
                    position,
                )?;
                self.state.write_u8(raw, Section::Records)
            }
            &Value::Int(stata_value) => {
                let raw = encode_numeric(
                    stata_value.to_raw(release),
                    release,
                    variable_index,
                    position,
                )?;
                self.state.write_u16(raw, byte_order, Section::Records)
            }
            &Value::Long(stata_value) => {
                let raw = encode_numeric(
                    stata_value.to_raw(release),
                    release,
                    variable_index,
                    position,
                )?;
                self.state.write_u32(raw, byte_order, Section::Records)
            }
            &Value::Float(stata_value) => {
                let raw = encode_numeric(
                    stata_value.to_raw(release),
                    release,
                    variable_index,
                    position,
                )?;
                self.state
                    .write_u32(raw.to_bits(), byte_order, Section::Records)
            }
            &Value::Double(stata_value) => {
                let raw = encode_numeric(
                    stata_value.to_raw(release),
                    release,
                    variable_index,
                    position,
                )?;
                self.state
                    .write_u64(raw.to_bits(), byte_order, Section::Records)
            }
            Value::String(text) => {
                let VariableType::FixedString(width) = variable_type else {
                    unreachable!(
                        "Value::String paired with non-FixedString variable — \
                         validation should have caught this"
                    );
                };
                self.write_record_string(variable_index, text.as_ref(), width)
            }
            &Value::LongStringRef(long_string_ref) => {
                self.write_long_string_ref(long_string_ref, byte_order, release)
            }
        }
    }

    /// Emits a `FixedString` value: encoded bytes, then zero padding
    /// to the declared slot width.
    fn write_record_string(&mut self, variable_index: u32, text: &str, width: u16) -> Result<()> {
        let encoded = encode_record_string(
            text,
            self.state.encoding(),
            variable_index,
            width,
            self.state.position(),
        )?;
        self.state
            .write_padded_bytes(&encoded, usize::from(width), Section::Records)
    }

    /// Emits an 8-byte strL reference. Layout depends on release:
    ///
    /// - V117:  `v` as `u32` (4 bytes) + `o` as `u32` (4 bytes).
    /// - V118:  `v` as `u16` (2 bytes) + `o` as `u48` (6 bytes).
    /// - V119+: `v` as `u24` (3 bytes) + `o` as `u40` (5 bytes).
    ///
    /// Narrows `variable` / `observation` to the on-disk width,
    /// returning `FieldTooLarge` on overflow.
    fn write_long_string_ref(
        &mut self,
        long_string_ref: LongStringRef,
        byte_order: ByteOrder,
        release: Release,
    ) -> Result<()> {
        let variable = long_string_ref.variable();
        let observation = long_string_ref.observation();
        if release.supports_extended_variable_count() {
            // V119+: u24 variable + u40 observation.
            self.write_u24(variable, byte_order)?;
            self.write_u40(observation, byte_order)?;
        } else if release.supports_extended_observation_count() {
            // V118: u16 variable + u48 observation.
            let narrowed_variable = self.state.narrow_to_u16(
                u64::from(variable),
                Section::Records,
                Field::VariableCount,
            )?;
            self.state
                .write_u16(narrowed_variable, byte_order, Section::Records)?;
            self.write_u48(observation, byte_order)?;
        } else {
            // V117: u32 variable + u32 observation.
            self.state
                .write_u32(variable, byte_order, Section::Records)?;
            let narrowed_observation =
                self.state
                    .narrow_to_u32(observation, Section::Records, Field::ObservationCount)?;
            self.state
                .write_u32(narrowed_observation, byte_order, Section::Records)?;
        }
        Ok(())
    }

    /// Writes `value` as a 48-bit unsigned integer in the given byte
    /// order. Errors if `value >= 2^48`.
    fn write_u48(&mut self, value: u64, byte_order: ByteOrder) -> Result<()> {
        let bytes = encode_u48(value, byte_order, self.state.position())?;
        self.state.write_exact(&bytes, Section::Records)
    }

    /// Writes `value` as a 24-bit unsigned integer in the given byte
    /// order. Errors if `value >= 2^24`.
    fn write_u24(&mut self, value: u32, byte_order: ByteOrder) -> Result<()> {
        let bytes = encode_u24(value, byte_order, self.state.position())?;
        self.state.write_exact(&bytes, Section::Records)
    }

    /// Writes `value` as a 40-bit unsigned integer in the given byte
    /// order. Errors if `value >= 2^40`.
    fn write_u40(&mut self, value: u64, byte_order: ByteOrder) -> Result<()> {
        let bytes = encode_u40(value, byte_order, self.state.position())?;
        self.state.write_exact(&bytes, Section::Records)
    }
}

// ===========================================================================
// Tests
// ===========================================================================

#[cfg(test)]
mod tests {
    use std::borrow::Cow;
    use std::io::Cursor;

    use float_cmp::assert_approx_eq;

    use super::*;
    use crate::stata::dta::byte_order::ByteOrder;
    use crate::stata::dta::dta_error::{DtaError, FormatErrorKind};
    use crate::stata::dta::dta_reader::DtaReader;
    use crate::stata::dta::dta_writer::DtaWriter;
    use crate::stata::dta::long_string::LongStringContent;
    use crate::stata::dta::long_string_table::LongStringTable;
    use crate::stata::dta::release::Release;
    use crate::stata::dta::variable::Variable;
    use crate::stata::dta::variable_type::VariableType;
    use crate::stata::stata_byte::StataByte;
    use crate::stata::stata_double::StataDouble;
    use crate::stata::stata_float::StataFloat;
    use crate::stata::stata_int::StataInt;
    use crate::stata::stata_long::StataLong;

    // -- Helpers -------------------------------------------------------------

    /// Writes a schema + records through the full pipeline to
    /// [`ValueLabelWriter::finish`], then reads the resulting bytes
    /// back up through the record reader.
    fn round_trip<F>(
        release: Release,
        byte_order: ByteOrder,
        schema: Schema,
        write_records: F,
    ) -> Vec<u8>
    where
        F: FnOnce(&mut RecordWriter<Cursor<Vec<u8>>>),
    {
        let header = Header::builder(release, byte_order).build();
        let characteristic_writer = DtaWriter::new()
            .from_writer(Cursor::new(Vec::<u8>::new()))
            .write_header(header)
            .unwrap()
            .write_schema(schema)
            .unwrap();
        let mut record_writer = characteristic_writer.into_record_writer().unwrap();
        write_records(&mut record_writer);
        record_writer
            .into_long_string_writer()
            .unwrap()
            .into_value_label_writer()
            .unwrap()
            .finish()
            .unwrap()
            .into_inner()
    }

    /// Reads back a file produced by [`round_trip`] and collects all
    /// records. Values are converted to an `OwnedValue` so the
    /// returned vec doesn't borrow from the reader's buffer.
    fn read_back(bytes: Vec<u8>) -> Vec<Vec<OwnedValue>> {
        let mut characteristic_reader = DtaReader::new()
            .from_reader(Cursor::new(bytes))
            .read_header()
            .unwrap()
            .read_schema()
            .unwrap();
        let header_n = characteristic_reader.header().observation_count();
        let header_k = characteristic_reader.header().variable_count();
        let schema_variable_count = characteristic_reader.schema().variables().len();
        while characteristic_reader
            .read_characteristic()
            .unwrap()
            .is_some()
        {}
        let mut record_reader = characteristic_reader.into_record_reader().unwrap();
        let mut records = Vec::new();
        while let Some(record) = record_reader.read_record().unwrap() {
            let owned: Vec<OwnedValue> = record
                .values()
                .iter()
                .cloned()
                .map(OwnedValue::from)
                .collect();
            records.push(owned);
        }
        // The record writer patches the header's N field from the
        // accumulated row count; the schema writer patches K from
        // `schema.variables().len()`. Assert both round-trip — every
        // record_writer test inherits these checks.
        assert_eq!(
            u64::try_from(records.len()).expect("record count fits u64"),
            header_n,
            "header N field must match the number of rows in the file",
        );
        assert_eq!(
            usize::try_from(header_k).expect("variable count fits usize"),
            schema_variable_count,
            "header K field must match schema variable count",
        );
        records
    }

    /// Owned echo of [`Value`] for test assertions — the reader's
    /// `Value::String` borrows from its internal buffer, so we copy
    /// to an owned `String` at read time.
    #[derive(Debug, PartialEq)]
    enum OwnedValue {
        Byte(StataByte),
        Int(StataInt),
        Long(StataLong),
        Float(StataFloat),
        Double(StataDouble),
        String(String),
        LongStringRef(LongStringRef),
    }

    impl From<Value<'_>> for OwnedValue {
        fn from(value: Value<'_>) -> Self {
            match value {
                Value::Byte(v) => Self::Byte(v),
                Value::Int(v) => Self::Int(v),
                Value::Long(v) => Self::Long(v),
                Value::Float(v) => Self::Float(v),
                Value::Double(v) => Self::Double(v),
                Value::String(s) => Self::String(s.into_owned()),
                Value::LongStringRef(r) => Self::LongStringRef(r),
            }
        }
    }

    // -- Binary round-trips (formats 104–116) --------------------------------

    #[test]
    fn binary_v114_all_numeric_types_round_trip() {
        let schema = Schema::builder()
            .add_variable(Variable::builder(VariableType::Byte, "b").format("%8.0g"))
            .add_variable(Variable::builder(VariableType::Int, "i").format("%8.0g"))
            .add_variable(Variable::builder(VariableType::Long, "l").format("%12.0g"))
            .add_variable(Variable::builder(VariableType::Float, "f").format("%9.0g"))
            .add_variable(Variable::builder(VariableType::Double, "d").format("%10.0g"))
            .build()
            .unwrap();
        let bytes = round_trip(Release::V114, ByteOrder::LittleEndian, schema, |writer| {
            writer
                .write_record(&[
                    Value::Byte(StataByte::Present(42)),
                    Value::Int(StataInt::Present(-500)),
                    Value::Long(StataLong::Present(1_000_000)),
                    Value::Float(StataFloat::Present(3.5)),
                    Value::Double(StataDouble::Present(1.25)),
                ])
                .unwrap();
        });
        let records = read_back(bytes);
        assert_eq!(records.len(), 1);
        assert_eq!(records[0][0], OwnedValue::Byte(StataByte::Present(42)));
        assert_eq!(records[0][1], OwnedValue::Int(StataInt::Present(-500)));
        assert_eq!(
            records[0][2],
            OwnedValue::Long(StataLong::Present(1_000_000)),
        );
        let OwnedValue::Float(StataFloat::Present(f)) = records[0][3] else {
            panic!("expected float");
        };
        assert_approx_eq!(f32, f, 3.5);
        let OwnedValue::Double(StataDouble::Present(d)) = records[0][4] else {
            panic!("expected double");
        };
        assert_approx_eq!(f64, d, 1.25);
    }

    #[test]
    fn binary_v114_multiple_rows_round_trip() {
        let schema = Schema::builder()
            .add_variable(Variable::builder(VariableType::Byte, "x").format("%8.0g"))
            .build()
            .unwrap();
        let bytes = round_trip(Release::V114, ByteOrder::LittleEndian, schema, |writer| {
            for i in 0..5 {
                writer
                    .write_record(&[Value::Byte(StataByte::Present(i))])
                    .unwrap();
            }
        });
        let records = read_back(bytes);
        assert_eq!(records.len(), 5);
        for (i, record) in records.iter().enumerate() {
            let expected = i8::try_from(i).unwrap();
            assert_eq!(record[0], OwnedValue::Byte(StataByte::Present(expected)));
        }
    }

    #[test]
    fn binary_v114_fixed_string_round_trip() {
        let schema = Schema::builder()
            .add_variable(Variable::builder(VariableType::FixedString(10), "city").format("%10s"))
            .build()
            .unwrap();
        let bytes = round_trip(Release::V114, ByteOrder::LittleEndian, schema, |writer| {
            writer.write_record(&[Value::string("Portland")]).unwrap();
            writer.write_record(&[Value::string("NYC")]).unwrap();
        });
        let records = read_back(bytes);
        assert_eq!(records.len(), 2);
        assert_eq!(records[0][0], OwnedValue::String("Portland".to_owned()));
        assert_eq!(records[1][0], OwnedValue::String("NYC".to_owned()));
    }

    #[test]
    fn binary_v114_big_endian_round_trip() {
        let schema = Schema::builder()
            .add_variable(Variable::builder(VariableType::Long, "x").format("%12.0g"))
            .build()
            .unwrap();
        let bytes = round_trip(Release::V114, ByteOrder::BigEndian, schema, |writer| {
            writer
                .write_record(&[Value::Long(StataLong::Present(-123_456_789))])
                .unwrap();
        });
        let records = read_back(bytes);
        assert_eq!(
            records[0][0],
            OwnedValue::Long(StataLong::Present(-123_456_789)),
        );
    }

    #[test]
    fn binary_v114_zero_rows_round_trip() {
        let schema = Schema::builder()
            .add_variable(Variable::builder(VariableType::Byte, "x").format("%8.0g"))
            .build()
            .unwrap();
        let bytes = round_trip(Release::V114, ByteOrder::LittleEndian, schema, |_| {});
        let records = read_back(bytes);
        assert!(records.is_empty());
    }

    #[test]
    fn binary_v114_missing_value_round_trip() {
        use crate::stata::missing_value::MissingValue;
        let schema = Schema::builder()
            .add_variable(Variable::builder(VariableType::Byte, "x").format("%8.0g"))
            .build()
            .unwrap();
        let bytes = round_trip(Release::V114, ByteOrder::LittleEndian, schema, |writer| {
            writer
                .write_record(&[Value::Byte(StataByte::Missing(MissingValue::A))])
                .unwrap();
        });
        let records = read_back(bytes);
        assert_eq!(
            records[0][0],
            OwnedValue::Byte(StataByte::Missing(MissingValue::A)),
        );
    }

    // -- XML round-trips (formats 117–119) -----------------------------------

    #[test]
    fn xml_v117_round_trip() {
        let schema = Schema::builder()
            .add_variable(Variable::builder(VariableType::Int, "count").format("%8.0g"))
            .add_variable(Variable::builder(VariableType::FixedString(5), "label").format("%5s"))
            .build()
            .unwrap();
        let bytes = round_trip(Release::V117, ByteOrder::LittleEndian, schema, |writer| {
            writer
                .write_record(&[Value::Int(StataInt::Present(7)), Value::string("hi")])
                .unwrap();
        });
        let records = read_back(bytes);
        assert_eq!(records[0][0], OwnedValue::Int(StataInt::Present(7)));
        assert_eq!(records[0][1], OwnedValue::String("hi".to_owned()));
    }

    #[test]
    fn xml_v117_zero_rows_round_trip() {
        // XML <data></data> must be emitted even for zero rows.
        let schema = Schema::builder()
            .add_variable(Variable::builder(VariableType::Byte, "x").format("%8.0g"))
            .build()
            .unwrap();
        let bytes = round_trip(Release::V117, ByteOrder::LittleEndian, schema, |_| {});
        let records = read_back(bytes);
        assert!(records.is_empty());
    }

    #[test]
    fn xml_v118_utf8_string_round_trip() {
        let schema = Schema::builder()
            .add_variable(Variable::builder(VariableType::FixedString(16), "label").format("%16s"))
            .build()
            .unwrap();
        let bytes = round_trip(Release::V118, ByteOrder::LittleEndian, schema, |writer| {
            writer.write_record(&[Value::string("日本語")]).unwrap();
        });
        let records = read_back(bytes);
        assert_eq!(records[0][0], OwnedValue::String("日本語".to_owned()));
    }

    // -- strL references via LongStringTable --------------------------------

    #[test]
    fn xml_v117_long_string_ref_round_trip() {
        let schema = Schema::builder()
            .add_variable(Variable::builder(VariableType::LongString, "note").format("%9s"))
            .build()
            .unwrap();
        let mut table = LongStringTable::for_writing();
        let ref1 = table.get_or_insert(1, 1, LongStringContent::Text(Cow::Borrowed(b"hello")));
        let ref2 = table.get_or_insert(1, 2, LongStringContent::Text(Cow::Borrowed(b"world")));

        let bytes = round_trip(Release::V117, ByteOrder::LittleEndian, schema, |writer| {
            writer.write_record(&[Value::LongStringRef(ref1)]).unwrap();
            writer.write_record(&[Value::LongStringRef(ref2)]).unwrap();
        });
        let records = read_back(bytes);
        assert_eq!(records.len(), 2);
        assert_eq!(records[0][0], OwnedValue::LongStringRef(ref1));
        assert_eq!(records[1][0], OwnedValue::LongStringRef(ref2));
    }

    #[test]
    fn xml_v117_long_string_ref_big_endian_round_trip() {
        // V117 uses u32 variable + u32 observation in the data
        // section; verify the BE byte order also round-trips.
        let schema = Schema::builder()
            .add_variable(Variable::builder(VariableType::LongString, "note").format("%9s"))
            .build()
            .unwrap();
        let mut table = LongStringTable::for_writing();
        let reference =
            table.get_or_insert(3, 5, LongStringContent::Text(Cow::Borrowed(b"payload")));

        let bytes = round_trip(Release::V117, ByteOrder::BigEndian, schema, |writer| {
            writer
                .write_record(&[Value::LongStringRef(reference)])
                .unwrap();
        });
        let records = read_back(bytes);
        assert_eq!(records[0][0], OwnedValue::LongStringRef(reference));
    }

    #[test]
    fn xml_v118_long_string_ref_round_trip() {
        // V118 uses u16 variable + u48 observation in the data
        // section — exercises the different on-disk layout.
        let schema = Schema::builder()
            .add_variable(Variable::builder(VariableType::LongString, "note").format("%9s"))
            .build()
            .unwrap();
        let mut table = LongStringTable::for_writing();
        let reference =
            table.get_or_insert(1, 42, LongStringContent::Text(Cow::Borrowed(b"hello")));

        let bytes = round_trip(Release::V118, ByteOrder::LittleEndian, schema, |writer| {
            writer
                .write_record(&[Value::LongStringRef(reference)])
                .unwrap();
        });
        let records = read_back(bytes);
        assert_eq!(records[0][0], OwnedValue::LongStringRef(reference));
    }

    #[test]
    fn xml_v118_long_string_ref_big_endian_round_trip() {
        let schema = Schema::builder()
            .add_variable(Variable::builder(VariableType::LongString, "note").format("%9s"))
            .build()
            .unwrap();
        let mut table = LongStringTable::for_writing();
        let reference =
            table.get_or_insert(3, 5, LongStringContent::Text(Cow::Borrowed(b"payload")));

        let bytes = round_trip(Release::V118, ByteOrder::BigEndian, schema, |writer| {
            writer
                .write_record(&[Value::LongStringRef(reference)])
                .unwrap();
        });
        let records = read_back(bytes);
        assert_eq!(records[0][0], OwnedValue::LongStringRef(reference));
    }

    #[test]
    fn xml_v119_long_string_ref_round_trip() {
        // V119 uses u24 variable + u40 observation in the data
        // section — narrower variable than V117 (u32) and narrower
        // observation than V118 (u48).
        let schema = Schema::builder()
            .add_variable(Variable::builder(VariableType::LongString, "note").format("%9s"))
            .build()
            .unwrap();
        let mut table = LongStringTable::for_writing();
        let reference =
            table.get_or_insert(7, 42, LongStringContent::Text(Cow::Borrowed(b"hello")));

        let bytes = round_trip(Release::V119, ByteOrder::LittleEndian, schema, |writer| {
            writer
                .write_record(&[Value::LongStringRef(reference)])
                .unwrap();
        });
        let records = read_back(bytes);
        assert_eq!(records[0][0], OwnedValue::LongStringRef(reference));
    }

    #[test]
    fn xml_v119_long_string_ref_big_endian_round_trip() {
        let schema = Schema::builder()
            .add_variable(Variable::builder(VariableType::LongString, "note").format("%9s"))
            .build()
            .unwrap();
        let mut table = LongStringTable::for_writing();
        let reference =
            table.get_or_insert(3, 5, LongStringContent::Text(Cow::Borrowed(b"payload")));

        let bytes = round_trip(Release::V119, ByteOrder::BigEndian, schema, |writer| {
            writer
                .write_record(&[Value::LongStringRef(reference)])
                .unwrap();
        });
        let records = read_back(bytes);
        assert_eq!(records[0][0], OwnedValue::LongStringRef(reference));
    }

    #[test]
    fn xml_v119_long_string_ref_wide_observation_round_trip() {
        // V119 supports observations up to 2^40 - 1; verify a value
        // above the V118 u48 zero region but inside u40 round-trips.
        let schema = Schema::builder()
            .add_variable(Variable::builder(VariableType::LongString, "note").format("%9s"))
            .build()
            .unwrap();
        let mut table = LongStringTable::for_writing();
        let wide_observation: u64 = 5_000_000_000;
        let reference = table.get_or_insert(
            1,
            wide_observation,
            LongStringContent::Text(Cow::Borrowed(b"wide")),
        );

        let bytes = round_trip(Release::V119, ByteOrder::LittleEndian, schema, |writer| {
            writer
                .write_record(&[Value::LongStringRef(reference)])
                .unwrap();
        });
        let records = read_back(bytes);
        assert_eq!(records[0][0], OwnedValue::LongStringRef(reference));
    }

    // -- Observation-count patching -----------------------------------------

    #[test]
    fn header_n_field_patched_after_record_writer_transition() {
        // After `into_long_string_writer`, the header's N field must
        // reflect `observation_count`.
        let schema = Schema::builder()
            .add_variable(Variable::builder(VariableType::Byte, "x").format("%8.0g"))
            .build()
            .unwrap();
        let bytes = round_trip(Release::V114, ByteOrder::LittleEndian, schema, |writer| {
            for i in 0..7 {
                writer
                    .write_record(&[Value::Byte(StataByte::Present(i))])
                    .unwrap();
            }
        });
        let header = DtaReader::new()
            .from_reader(Cursor::new(bytes))
            .read_header()
            .unwrap()
            .header()
            .clone();
        assert_eq!(header.observation_count(), 7);
    }

    #[test]
    fn xml_v118_n_field_patched_with_u64_width() {
        // V118 uses a u64 N field. Make sure the wide patch path is
        // exercised end-to-end, even with a small count.
        let schema = Schema::builder()
            .add_variable(Variable::builder(VariableType::Byte, "x").format("%8.0g"))
            .build()
            .unwrap();
        let bytes = round_trip(Release::V118, ByteOrder::LittleEndian, schema, |writer| {
            writer
                .write_record(&[Value::Byte(StataByte::Present(1))])
                .unwrap();
        });
        let header = DtaReader::new()
            .from_reader(Cursor::new(bytes))
            .read_header()
            .unwrap()
            .header()
            .clone();
        assert_eq!(header.observation_count(), 1);
    }

    // -- Error cases ---------------------------------------------------------

    /// Builds a minimal 1-variable schema + header + characteristic
    /// writer pair, transitions to `RecordWriter`, and returns it so
    /// tests can exercise error paths without re-doing the setup.
    fn scalar_record_writer(
        variable_type: VariableType,
        release: Release,
    ) -> RecordWriter<Cursor<Vec<u8>>> {
        let schema = Schema::builder()
            .add_variable(Variable::builder(variable_type, "x").format("%8.0g"))
            .build()
            .unwrap();
        let header = Header::builder(release, ByteOrder::LittleEndian).build();
        DtaWriter::new()
            .from_writer(Cursor::new(Vec::<u8>::new()))
            .write_header(header)
            .unwrap()
            .write_schema(schema)
            .unwrap()
            .into_record_writer()
            .unwrap()
    }

    #[test]
    fn arity_mismatch_errors() {
        let mut writer = scalar_record_writer(VariableType::Byte, Release::V114);
        let error = writer.write_record(&[]).unwrap_err();
        assert!(matches!(
            error,
            DtaError::Format(ref e) if matches!(
                e.kind(),
                FormatErrorKind::RecordArityMismatch { expected: 1, actual: 0 }
            )
        ));
    }

    #[test]
    fn value_type_mismatch_errors() {
        let mut writer = scalar_record_writer(VariableType::Byte, Release::V114);
        let error = writer
            .write_record(&[Value::Int(StataInt::Present(0))])
            .unwrap_err();
        assert!(matches!(
            error,
            DtaError::Format(ref e) if matches!(
                e.kind(),
                FormatErrorKind::RecordValueTypeMismatch {
                    variable_index: 0,
                    expected: VariableType::Byte,
                }
            )
        ));
    }

    #[test]
    fn mid_record_type_mismatch_does_not_corrupt_earlier_rows() {
        // If the type-check fired mid-row we'd leave partial bytes
        // from the first column behind. Upfront validation should
        // catch a mismatch at column index 2 before any of this
        // record's bytes hit the sink.
        let schema = Schema::builder()
            .add_variable(Variable::builder(VariableType::Byte, "a").format("%8.0g"))
            .add_variable(Variable::builder(VariableType::Byte, "b").format("%8.0g"))
            .add_variable(Variable::builder(VariableType::Byte, "c").format("%8.0g"))
            .build()
            .unwrap();
        let header = Header::builder(Release::V114, ByteOrder::LittleEndian).build();
        let mut writer = DtaWriter::new()
            .from_writer(Cursor::new(Vec::<u8>::new()))
            .write_header(header)
            .unwrap()
            .write_schema(schema)
            .unwrap()
            .into_record_writer()
            .unwrap();

        // One good row first.
        writer
            .write_record(&[
                Value::Byte(StataByte::Present(1)),
                Value::Byte(StataByte::Present(2)),
                Value::Byte(StataByte::Present(3)),
            ])
            .unwrap();

        // Now a record that's valid at index 0–1 but mismatches at
        // index 2. Pre-validation must catch this before any row
        // bytes are written, so the file should still contain
        // exactly one row (3 bytes of row data) after the error.
        let position_before = writer.state.position();
        let error = writer
            .write_record(&[
                Value::Byte(StataByte::Present(4)),
                Value::Byte(StataByte::Present(5)),
                Value::Int(StataInt::Present(6)),
            ])
            .unwrap_err();
        let position_after = writer.state.position();

        assert!(matches!(
            error,
            DtaError::Format(ref e) if matches!(
                e.kind(),
                FormatErrorKind::RecordValueTypeMismatch {
                    variable_index: 2,
                    expected: VariableType::Byte,
                }
            )
        ));
        assert_eq!(
            position_before, position_after,
            "failed write_record must not have written any row bytes",
        );

        // And the file must still round-trip with just the one good row.
        let bytes = writer
            .into_long_string_writer()
            .unwrap()
            .into_value_label_writer()
            .unwrap()
            .finish()
            .unwrap()
            .into_inner();
        let records = read_back(bytes);
        assert_eq!(records.len(), 1);
    }

    #[test]
    fn fixed_string_too_long_errors() {
        let mut writer = scalar_record_writer(VariableType::FixedString(3), Release::V114);
        let error = writer.write_record(&[Value::string("four")]).unwrap_err();
        assert!(matches!(
            error,
            DtaError::Format(ref e) if matches!(
                e.kind(),
                FormatErrorKind::RecordStringTooLong {
                    variable_index: 0,
                    max: 3,
                    actual: 4,
                }
            )
        ));
    }

    #[test]
    fn non_latin_string_in_windows_1252_errors() {
        let mut writer = scalar_record_writer(VariableType::FixedString(10), Release::V114);
        let error = writer.write_record(&[Value::string("日本語")]).unwrap_err();
        assert!(matches!(
            error,
            DtaError::Format(ref e) if matches!(
                e.kind(),
                FormatErrorKind::InvalidEncoding { field: Field::VariableValue }
            )
        ));
    }
}
