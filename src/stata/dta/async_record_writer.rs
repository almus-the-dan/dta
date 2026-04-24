use tokio::io::{AsyncSeek, AsyncWrite};

use super::async_long_string_writer::AsyncLongStringWriter;
use super::async_writer_state::AsyncWriterState;
use super::byte_order::ByteOrder;
use super::dta_error::{Field, Result, Section};
use super::header::Header;
use super::long_string_ref::LongStringRef;
use super::record_format::{
    encode_numeric, encode_record_string, encode_u48, narrow_variable_index,
    observation_count_overflow_error, validate_record_arity, validate_record_value_types,
};
use super::release::Release;
use super::schema::Schema;
use super::value::Value;
use super::variable_type::VariableType;

/// Writes observation records (data rows) to a DTA file
/// asynchronously.
///
/// Call [`write_record`](Self::write_record) once per observation,
/// passing a slice of [`Value`]s whose length and types match the
/// schema. Transition via [`finish`](Self::finish) once all rows have
/// been written — that step patches the header's N (observation
/// count) field with the accumulated row count.
#[derive(Debug)]
pub struct AsyncRecordWriter<W> {
    state: AsyncWriterState<W>,
    header: Header,
    schema: Schema,
    observation_count: u64,
    /// Tracks whether the XML `<data>` opening tag has been emitted.
    /// Unused (but harmless) for binary formats, which have no
    /// section tag.
    opened: bool,
}

impl<W> AsyncRecordWriter<W> {
    #[must_use]
    pub(crate) fn new(state: AsyncWriterState<W>, header: Header, schema: Schema) -> Self {
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
}

impl<W: AsyncWrite + AsyncSeek + Unpin> AsyncRecordWriter<W> {
    /// Writes a single observation row.
    ///
    /// `values` must have exactly one entry per variable in the
    /// schema, in schema order. The first call also emits the XML
    /// `<data>` opening tag for XML formats.
    ///
    /// # Errors
    ///
    /// Returns [`DtaError::Format`] with:
    /// - [`RecordArityMismatch`](super::dta_error::FormatErrorKind::RecordArityMismatch)
    ///   if `values.len() != schema.variables().len()`.
    /// - [`RecordValueTypeMismatch`](super::dta_error::FormatErrorKind::RecordValueTypeMismatch)
    ///   if any value's variant does not match its variable's type.
    /// - [`RecordStringTooLong`](super::dta_error::FormatErrorKind::RecordStringTooLong)
    ///   if a string value exceeds its variable's fixed-width slot.
    /// - [`InvalidEncoding`](super::dta_error::FormatErrorKind::InvalidEncoding)
    ///   if a string cannot be represented in the active encoding.
    /// - [`FieldTooLarge`](super::dta_error::FormatErrorKind::FieldTooLarge)
    ///   if a [`LongStringRef`] component (variable or observation)
    ///   exceeds the on-disk field width.
    pub async fn write_record(&mut self, values: &[Value<'_>]) -> Result<()> {
        let position = self.state.position();
        validate_record_arity(values.len(), self.schema.variables().len(), position)?;
        validate_record_value_types(values, self.schema.variables(), position)?;

        self.open_section_if_needed().await?;

        for (index, value) in values.iter().enumerate() {
            let variables = self.schema.variables();
            let variable = &variables[index];
            let variable_type = variable.variable_type();
            let variable_index = narrow_variable_index(index, self.state.position())?;
            self.write_value(variable_index, variable_type, value)
                .await?;
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
    /// # Errors
    ///
    /// Returns [`DtaError::Io`] on sink failures and
    /// [`DtaError::Format`] with
    /// [`FieldTooLarge`](super::dta_error::FormatErrorKind::FieldTooLarge)
    /// if the observation count exceeds `u32::MAX` on a pre-V118 release.
    ///
    /// # Panics
    ///
    /// Panics if the header writer did not capture the N offset — an
    /// internal invariant of the writer chain.
    pub async fn into_long_string_writer(mut self) -> Result<AsyncLongStringWriter<W>> {
        self.open_section_if_needed().await?;

        let release = self.header.release();
        let byte_order = self.header.byte_order();
        if release.is_xml_like() {
            self.state.write_exact(b"</data>", Section::Records).await?;
            let long_strings_offset = self.state.position();
            self.state
                .patch_map_entry(10, long_strings_offset, byte_order, Section::Records)
                .await?;
        }

        self.patch_header_observation_count().await?;

        let writer = AsyncLongStringWriter::new(self.state, self.header, self.schema);
        Ok(writer)
    }

    async fn open_section_if_needed(&mut self) -> Result<()> {
        if !self.header.release().is_xml_like() {
            return Ok(());
        }
        if !self.opened {
            self.state.write_exact(b"<data>", Section::Records).await?;
            self.opened = true;
        }
        Ok(())
    }

    /// Seek-patches the header N field with `self.observation_count`,
    /// narrowing to `u32` for pre-V118 releases.
    async fn patch_header_observation_count(&mut self) -> Result<()> {
        let release = self.header.release();
        let byte_order = self.header.byte_order();
        let offset = self
            .state
            .header_observation_count_offset()
            .expect("header writer set N offset before AsyncRecordWriter was constructed");
        if release.supports_extended_observation_count() {
            self.state
                .patch_u64_at(offset, self.observation_count, byte_order, Section::Header)
                .await?;
        } else {
            let narrowed = self.state.narrow_to_u32(
                self.observation_count,
                Section::Header,
                Field::ObservationCount,
            )?;
            self.state
                .patch_u32_at(offset, narrowed, byte_order, Section::Header)
                .await?;
        }
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Per-variable value serialization
// ---------------------------------------------------------------------------

impl<W: AsyncWrite + Unpin> AsyncRecordWriter<W> {
    async fn write_value(
        &mut self,
        variable_index: u32,
        variable_type: VariableType,
        value: &Value<'_>,
    ) -> Result<()> {
        let byte_order = self.header.byte_order();
        let release = self.header.release();
        let position = self.state.position();
        match *value {
            Value::Byte(stata_value) => {
                let raw = encode_numeric(
                    stata_value.to_raw(release),
                    release,
                    variable_index,
                    position,
                )?;
                self.state.write_u8(raw, Section::Records).await
            }
            Value::Int(stata_value) => {
                let raw = encode_numeric(
                    stata_value.to_raw(release),
                    release,
                    variable_index,
                    position,
                )?;
                self.state
                    .write_u16(raw, byte_order, Section::Records)
                    .await
            }
            Value::Long(stata_value) => {
                let raw = encode_numeric(
                    stata_value.to_raw(release),
                    release,
                    variable_index,
                    position,
                )?;
                self.state
                    .write_u32(raw, byte_order, Section::Records)
                    .await
            }
            Value::Float(stata_value) => {
                let raw = encode_numeric(
                    stata_value.to_raw(release),
                    release,
                    variable_index,
                    position,
                )?;
                self.state
                    .write_u32(raw.to_bits(), byte_order, Section::Records)
                    .await
            }
            Value::Double(stata_value) => {
                let raw = encode_numeric(
                    stata_value.to_raw(release),
                    release,
                    variable_index,
                    position,
                )?;
                self.state
                    .write_u64(raw.to_bits(), byte_order, Section::Records)
                    .await
            }
            Value::String(text) => {
                let VariableType::FixedString(width) = variable_type else {
                    unreachable!(
                        "Value::String paired with non-FixedString variable — \
                         validation should have caught this"
                    );
                };
                self.write_record_string(variable_index, text, width).await
            }
            Value::LongStringRef(long_string_ref) => {
                self.write_long_string_ref(long_string_ref, byte_order, release)
                    .await
            }
        }
    }

    async fn write_record_string(
        &mut self,
        variable_index: u32,
        text: &str,
        width: u16,
    ) -> Result<()> {
        let encoded = encode_record_string(
            text,
            self.state.encoding(),
            variable_index,
            width,
            self.state.position(),
        )?;
        self.state
            .write_padded_bytes(&encoded, usize::from(width), Section::Records)
            .await
    }

    /// Emits an 8-byte strL reference. Layout depends on release:
    ///
    /// - V117: `v` as `u32` (4 bytes) + `o` as `u32` (4 bytes).
    /// - V118+: `v` as `u16` (2 bytes) + `o` as `u48` (6 bytes).
    async fn write_long_string_ref(
        &mut self,
        long_string_ref: LongStringRef,
        byte_order: ByteOrder,
        release: Release,
    ) -> Result<()> {
        let variable = long_string_ref.variable();
        let observation = long_string_ref.observation();
        if release.supports_extended_observation_count() {
            let narrowed_variable =
                self.state
                    .narrow_to_u16(variable, Section::Records, Field::VariableCount)?;
            self.state
                .write_u16(narrowed_variable, byte_order, Section::Records)
                .await?;
            let bytes = encode_u48(observation, byte_order, self.state.position())?;
            self.state.write_exact(&bytes, Section::Records).await?;
        } else {
            self.state
                .write_u32(variable, byte_order, Section::Records)
                .await?;
            let narrowed_observation =
                self.state
                    .narrow_to_u32(observation, Section::Records, Field::ObservationCount)?;
            self.state
                .write_u32(narrowed_observation, byte_order, Section::Records)
                .await?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;

    use float_cmp::assert_approx_eq;

    use super::*;
    use crate::stata::dta::byte_order::ByteOrder;
    use crate::stata::dta::dta_error::{DtaError, FormatErrorKind};
    use crate::stata::dta::dta_reader::DtaReader;
    use crate::stata::dta::dta_writer::DtaWriter;
    use crate::stata::dta::long_string_ref::LongStringRef;
    use crate::stata::dta::long_string_table::LongStringTable;
    use crate::stata::dta::release::Release;
    use crate::stata::dta::variable::Variable;
    use crate::stata::stata_byte::StataByte;
    use crate::stata::stata_double::StataDouble;
    use crate::stata::stata_float::StataFloat;
    use crate::stata::stata_int::StataInt;
    use crate::stata::stata_long::StataLong;

    /// Owned echo of [`Value`] for test assertions — the reader's
    /// `Value::String` borrows from its internal buffer, so we copy to
    /// an owned `String` at read time.
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
                Value::String(s) => Self::String(s.to_owned()),
                Value::LongStringRef(r) => Self::LongStringRef(r),
            }
        }
    }

    /// Writes a schema + records through the async pipeline, then
    /// reads them back through the async reader pipeline.
    async fn round_trip(
        release: Release,
        byte_order: ByteOrder,
        schema: Schema,
        records: Vec<Vec<Value<'_>>>,
    ) -> Vec<Vec<OwnedValue>> {
        let header = Header::builder(release, byte_order).build();
        let mut record_writer = DtaWriter::new()
            .from_tokio_writer(Cursor::new(Vec::<u8>::new()))
            .write_header(header)
            .await
            .unwrap()
            .write_schema(schema)
            .await
            .unwrap()
            .into_record_writer()
            .await
            .unwrap();
        for values in &records {
            record_writer.write_record(values).await.unwrap();
        }
        let cursor: Cursor<Vec<u8>> = record_writer
            .into_long_string_writer()
            .await
            .unwrap()
            .into_value_label_writer()
            .await
            .unwrap()
            .finish()
            .await
            .unwrap();
        let bytes = cursor.into_inner();

        let characteristic_reader = DtaReader::new()
            .from_tokio_reader(bytes.as_slice())
            .read_header()
            .await
            .unwrap()
            .read_schema()
            .await
            .unwrap();
        let header_n = characteristic_reader.header().observation_count();
        let header_k = characteristic_reader.header().variable_count();
        let schema_variable_count = characteristic_reader.schema().variables().len();
        let mut record_reader = characteristic_reader.into_record_reader().await.unwrap();
        let mut parsed = Vec::new();
        while let Some(record) = record_reader.read_record().await.unwrap() {
            let owned: Vec<OwnedValue> = record
                .values()
                .iter()
                .copied()
                .map(OwnedValue::from)
                .collect();
            parsed.push(owned);
        }
        assert_eq!(
            u64::try_from(parsed.len()).expect("record count fits u64"),
            header_n,
            "header N field must match the number of rows in the file",
        );
        assert_eq!(
            usize::try_from(header_k).expect("variable count fits usize"),
            schema_variable_count,
            "header K field must match schema variable count",
        );
        parsed
    }

    // -- Binary round-trips (formats 104–116) --------------------------------

    #[tokio::test]
    async fn binary_v114_all_numeric_types_round_trip() {
        let schema = Schema::builder()
            .add_variable(Variable::builder(VariableType::Byte, "b").format("%8.0g"))
            .add_variable(Variable::builder(VariableType::Int, "i").format("%8.0g"))
            .add_variable(Variable::builder(VariableType::Long, "l").format("%12.0g"))
            .add_variable(Variable::builder(VariableType::Float, "f").format("%9.0g"))
            .add_variable(Variable::builder(VariableType::Double, "d").format("%10.0g"))
            .build()
            .unwrap();
        let records = vec![vec![
            Value::Byte(StataByte::Present(42)),
            Value::Int(StataInt::Present(-500)),
            Value::Long(StataLong::Present(1_000_000)),
            Value::Float(StataFloat::Present(3.5)),
            Value::Double(StataDouble::Present(1.25)),
        ]];
        let parsed = round_trip(Release::V114, ByteOrder::LittleEndian, schema, records).await;
        assert_eq!(parsed.len(), 1);
        assert_eq!(parsed[0][0], OwnedValue::Byte(StataByte::Present(42)));
        assert_eq!(parsed[0][1], OwnedValue::Int(StataInt::Present(-500)));
        assert_eq!(
            parsed[0][2],
            OwnedValue::Long(StataLong::Present(1_000_000)),
        );
        let OwnedValue::Float(StataFloat::Present(f)) = parsed[0][3] else {
            panic!("expected float");
        };
        assert_approx_eq!(f32, f, 3.5);
        let OwnedValue::Double(StataDouble::Present(d)) = parsed[0][4] else {
            panic!("expected double");
        };
        assert_approx_eq!(f64, d, 1.25);
    }

    #[tokio::test]
    async fn binary_v114_multiple_rows_round_trip() {
        let schema = Schema::builder()
            .add_variable(Variable::builder(VariableType::Byte, "x").format("%8.0g"))
            .build()
            .unwrap();
        let records: Vec<Vec<Value<'_>>> = (0..5)
            .map(|i| vec![Value::Byte(StataByte::Present(i))])
            .collect();
        let parsed = round_trip(Release::V114, ByteOrder::LittleEndian, schema, records).await;
        assert_eq!(parsed.len(), 5);
        for (i, record) in parsed.iter().enumerate() {
            let expected = i8::try_from(i).unwrap();
            assert_eq!(record[0], OwnedValue::Byte(StataByte::Present(expected)));
        }
    }

    #[tokio::test]
    async fn binary_v114_fixed_string_round_trip() {
        let schema = Schema::builder()
            .add_variable(Variable::builder(VariableType::FixedString(10), "city").format("%10s"))
            .build()
            .unwrap();
        let records = vec![vec![Value::String("Portland")], vec![Value::String("NYC")]];
        let parsed = round_trip(Release::V114, ByteOrder::LittleEndian, schema, records).await;
        assert_eq!(parsed.len(), 2);
        assert_eq!(parsed[0][0], OwnedValue::String("Portland".to_owned()));
        assert_eq!(parsed[1][0], OwnedValue::String("NYC".to_owned()));
    }

    #[tokio::test]
    async fn binary_v114_big_endian_round_trip() {
        let schema = Schema::builder()
            .add_variable(Variable::builder(VariableType::Long, "x").format("%12.0g"))
            .build()
            .unwrap();
        let records = vec![vec![Value::Long(StataLong::Present(-123_456_789))]];
        let parsed = round_trip(Release::V114, ByteOrder::BigEndian, schema, records).await;
        assert_eq!(
            parsed[0][0],
            OwnedValue::Long(StataLong::Present(-123_456_789)),
        );
    }

    #[tokio::test]
    async fn binary_v114_zero_rows_round_trip() {
        let schema = Schema::builder()
            .add_variable(Variable::builder(VariableType::Byte, "x").format("%8.0g"))
            .build()
            .unwrap();
        let parsed = round_trip(Release::V114, ByteOrder::LittleEndian, schema, vec![]).await;
        assert!(parsed.is_empty());
    }

    #[tokio::test]
    async fn binary_v114_missing_value_round_trip() {
        use crate::stata::missing_value::MissingValue;
        let schema = Schema::builder()
            .add_variable(Variable::builder(VariableType::Byte, "x").format("%8.0g"))
            .build()
            .unwrap();
        let records = vec![vec![Value::Byte(StataByte::Missing(MissingValue::A))]];
        let parsed = round_trip(Release::V114, ByteOrder::LittleEndian, schema, records).await;
        assert_eq!(
            parsed[0][0],
            OwnedValue::Byte(StataByte::Missing(MissingValue::A)),
        );
    }

    // -- XML round-trips (formats 117–119) -----------------------------------

    #[tokio::test]
    async fn xml_v117_round_trip() {
        let schema = Schema::builder()
            .add_variable(Variable::builder(VariableType::Int, "count").format("%8.0g"))
            .add_variable(Variable::builder(VariableType::FixedString(5), "label").format("%5s"))
            .build()
            .unwrap();
        let records = vec![vec![Value::Int(StataInt::Present(7)), Value::String("hi")]];
        let parsed = round_trip(Release::V117, ByteOrder::LittleEndian, schema, records).await;
        assert_eq!(parsed[0][0], OwnedValue::Int(StataInt::Present(7)));
        assert_eq!(parsed[0][1], OwnedValue::String("hi".to_owned()));
    }

    #[tokio::test]
    async fn xml_v117_zero_rows_round_trip() {
        let schema = Schema::builder()
            .add_variable(Variable::builder(VariableType::Byte, "x").format("%8.0g"))
            .build()
            .unwrap();
        let parsed = round_trip(Release::V117, ByteOrder::LittleEndian, schema, vec![]).await;
        assert!(parsed.is_empty());
    }

    #[tokio::test]
    async fn xml_v118_utf8_string_round_trip() {
        let schema = Schema::builder()
            .add_variable(Variable::builder(VariableType::FixedString(16), "label").format("%16s"))
            .build()
            .unwrap();
        let records = vec![vec![Value::String("日本語")]];
        let parsed = round_trip(Release::V118, ByteOrder::LittleEndian, schema, records).await;
        assert_eq!(parsed[0][0], OwnedValue::String("日本語".to_owned()));
    }

    // -- strL references via LongStringTable --------------------------------

    #[tokio::test]
    async fn xml_v117_long_string_ref_round_trip() {
        let schema = Schema::builder()
            .add_variable(Variable::builder(VariableType::LongString, "note").format("%9s"))
            .build()
            .unwrap();
        let mut table = LongStringTable::new();
        let ref1 = table.get_or_insert_by_content(1, 1, b"hello", false);
        let ref2 = table.get_or_insert_by_content(1, 2, b"world", false);
        let records = vec![
            vec![Value::LongStringRef(ref1)],
            vec![Value::LongStringRef(ref2)],
        ];
        let parsed = round_trip(Release::V117, ByteOrder::LittleEndian, schema, records).await;
        assert_eq!(parsed.len(), 2);
        assert_eq!(parsed[0][0], OwnedValue::LongStringRef(ref1));
        assert_eq!(parsed[1][0], OwnedValue::LongStringRef(ref2));
    }

    #[tokio::test]
    async fn xml_v118_long_string_ref_round_trip() {
        let schema = Schema::builder()
            .add_variable(Variable::builder(VariableType::LongString, "note").format("%9s"))
            .build()
            .unwrap();
        let mut table = LongStringTable::new();
        let reference = table.get_or_insert_by_content(1, 42, b"hello", false);
        let records = vec![vec![Value::LongStringRef(reference)]];
        let parsed = round_trip(Release::V118, ByteOrder::LittleEndian, schema, records).await;
        assert_eq!(parsed[0][0], OwnedValue::LongStringRef(reference));
    }

    #[tokio::test]
    async fn xml_v118_long_string_ref_big_endian_round_trip() {
        let schema = Schema::builder()
            .add_variable(Variable::builder(VariableType::LongString, "note").format("%9s"))
            .build()
            .unwrap();
        let mut table = LongStringTable::new();
        let reference = table.get_or_insert_by_content(3, 5, b"payload", false);
        let records = vec![vec![Value::LongStringRef(reference)]];
        let parsed = round_trip(Release::V118, ByteOrder::BigEndian, schema, records).await;
        assert_eq!(parsed[0][0], OwnedValue::LongStringRef(reference));
    }

    // -- Observation-count patching -----------------------------------------

    #[tokio::test]
    async fn header_n_field_patched_after_record_writer_transition() {
        let schema = Schema::builder()
            .add_variable(Variable::builder(VariableType::Byte, "x").format("%8.0g"))
            .build()
            .unwrap();
        let records: Vec<Vec<Value<'_>>> = (0..7)
            .map(|i| vec![Value::Byte(StataByte::Present(i))])
            .collect();
        let header = Header::builder(Release::V114, ByteOrder::LittleEndian).build();
        let mut writer = DtaWriter::new()
            .from_tokio_writer(Cursor::new(Vec::<u8>::new()))
            .write_header(header)
            .await
            .unwrap()
            .write_schema(schema)
            .await
            .unwrap()
            .into_record_writer()
            .await
            .unwrap();
        for values in &records {
            writer.write_record(values).await.unwrap();
        }
        let cursor: Cursor<Vec<u8>> = writer
            .into_long_string_writer()
            .await
            .unwrap()
            .into_value_label_writer()
            .await
            .unwrap()
            .finish()
            .await
            .unwrap();
        let bytes = cursor.into_inner();
        let header = DtaReader::new()
            .from_tokio_reader(bytes.as_slice())
            .read_header()
            .await
            .unwrap()
            .header()
            .clone();
        assert_eq!(header.observation_count(), 7);
    }

    #[tokio::test]
    async fn xml_v118_n_field_patched_with_u64_width() {
        let schema = Schema::builder()
            .add_variable(Variable::builder(VariableType::Byte, "x").format("%8.0g"))
            .build()
            .unwrap();
        let records = vec![vec![Value::Byte(StataByte::Present(1))]];
        let _ = round_trip(Release::V118, ByteOrder::LittleEndian, schema, records).await;
        // The `round_trip` helper asserts header_n == rows, so a
        // passing run already proves the u64 patch path works.
    }

    // -- Error cases ---------------------------------------------------------

    async fn scalar_record_writer(
        variable_type: VariableType,
        release: Release,
    ) -> AsyncRecordWriter<Cursor<Vec<u8>>> {
        let schema = Schema::builder()
            .add_variable(Variable::builder(variable_type, "x").format("%8.0g"))
            .build()
            .unwrap();
        let header = Header::builder(release, ByteOrder::LittleEndian).build();
        DtaWriter::new()
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
    }

    #[tokio::test]
    async fn arity_mismatch_errors() {
        let mut writer = scalar_record_writer(VariableType::Byte, Release::V114).await;
        let error = writer.write_record(&[]).await.unwrap_err();
        assert!(matches!(
            error,
            DtaError::Format(ref e) if matches!(
                e.kind(),
                FormatErrorKind::RecordArityMismatch { expected: 1, actual: 0 }
            )
        ));
    }

    #[tokio::test]
    async fn value_type_mismatch_errors() {
        let mut writer = scalar_record_writer(VariableType::Byte, Release::V114).await;
        let error = writer
            .write_record(&[Value::Int(StataInt::Present(0))])
            .await
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

    #[tokio::test]
    async fn fixed_string_too_long_errors() {
        let mut writer = scalar_record_writer(VariableType::FixedString(3), Release::V114).await;
        let error = writer
            .write_record(&[Value::String("four")])
            .await
            .unwrap_err();
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

    #[tokio::test]
    async fn non_latin_string_in_windows_1252_errors() {
        let mut writer = scalar_record_writer(VariableType::FixedString(10), Release::V114).await;
        let error = writer
            .write_record(&[Value::String("日本語")])
            .await
            .unwrap_err();
        assert!(matches!(
            error,
            DtaError::Format(ref e) if matches!(
                e.kind(),
                FormatErrorKind::InvalidEncoding { field: Field::VariableValue }
            )
        ));
    }
}
