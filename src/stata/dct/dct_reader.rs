use std::io::BufRead;

use super::dct_error::Result;
use super::dct_reader_options::DctReaderOptions;
use super::dct_reader_state::{DctReaderState, LineOutcome};
use super::dct_warning::DctWarning;
use super::lazy_record::LazyRecord;
use super::record::Record;
use super::schema::Schema;

/// Reads logical observations from a data file described by a
/// [`Schema`].
///
/// One physical line per `lines_per_observation` is read per
/// [`read_record`](Self::read_record) call; their contents stay in an
/// internal buffer reused across calls. That's why [`Record`] borrows
/// from `&mut self` — string fields point into the buffer rather than
/// allocating.
///
/// Non-fatal issues encountered while parsing a record are recorded
/// on the reader and accessible via [`warnings`](Self::warnings).
/// The buffer is cleared at the start of each `read_record` call, so
/// `warnings()` always reflects only the most recent observation —
/// safe to call inside a streaming loop without unbounded memory
/// growth on large files.
#[derive(Debug)]
pub struct DctReader<R> {
    inner: R,
    state: DctReaderState,
}

impl DctReader<()> {
    /// Creates a [`DctReaderOptions`] for the given schema.
    ///
    /// This is the entry point for constructing a `DctReader`:
    /// configure any reader options on the returned builder, then
    /// finish with one of `from_reader` / `from_file` / `from_path`.
    /// `DctReader::new` is intentionally not public — going through
    /// the options builder lets new configuration knobs ship without
    /// breaking callers.
    #[must_use]
    #[inline]
    pub fn options(schema: Schema) -> DctReaderOptions {
        DctReaderOptions::new(schema)
    }
}

impl<R> DctReader<R> {
    /// Constructs a reader from a parsed schema and a data source.
    ///
    /// Crate-private. External callers go through
    /// [`DctReader::options`] / [`DctReaderOptions`] so future
    /// configuration knobs can be added without breaking the
    /// construction surface.
    pub(super) fn new(schema: Schema, inner: R, record_warnings: bool) -> Self {
        Self {
            inner,
            state: DctReaderState::new(schema, record_warnings),
        }
    }

    /// The schema this reader was constructed from.
    #[must_use]
    #[inline]
    pub fn schema(&self) -> &Schema {
        self.state.schema()
    }

    /// Warnings produced while reading the most recent observation.
    ///
    /// Cleared at the start of every [`read_record`](Self::read_record)
    /// call. After `read_record` returns `Ok(Some(_))`, this slice
    /// contains zero or more warnings about that record (e.g., blank
    /// short-line fields treated as missing, integer values that were
    /// promoted to a wider storage type).
    #[must_use]
    #[inline]
    pub fn warnings(&self) -> &[DctWarning] {
        self.state.warnings()
    }

    /// Consumes the reader and returns the underlying data source.
    ///
    /// Useful when callers need to release a file handle eagerly,
    /// rewind a seekable source to read the data again, or process
    /// any bytes that follow the data section.
    #[must_use]
    pub fn into_inner(self) -> R {
        self.inner
    }
}

impl<R: BufRead> DctReader<R> {
    /// Reads the next observation from the data file.
    ///
    /// Returns `None` once the data file has been fully consumed.
    /// The returned [`Record`] borrows string data from this
    /// reader's internal line buffers, so it must be dropped before
    /// the next call.
    ///
    /// # Errors
    ///
    /// Returns [`DctError`](super::dct_error::DctError) on I/O
    /// failure, when the data file ends in the middle of an
    /// observation, or when a field cannot be parsed against the
    /// column's declared type and read format.
    pub fn read_record(&mut self) -> Result<Option<Record<'_>>> {
        if !self.advance_to_next_observation()? {
            return Ok(None);
        }
        let record = self.state.build_record()?;
        Ok(Some(record))
    }

    /// Reads the next observation as a [`LazyRecord`].
    ///
    /// The line buffers are loaded eagerly (the I/O has to happen),
    /// but value decoding is deferred until
    /// [`LazyRecord::value`](LazyRecord::value)
    /// is called. Use this when you only need a subset of columns
    /// per record and want to skip the parse work for the rest.
    ///
    /// `LazyRecord::value` discards warnings; this method clears the
    /// reader's warning buffer to keep
    /// [`warnings`](Self::warnings) reflecting only what the most
    /// recent eager read produced.
    ///
    /// # Errors
    ///
    /// Returns [`DctError`](super::dct_error::DctError) on I/O
    /// failure or when the data file ends mid-observation.
    pub fn read_lazy_record(&mut self) -> Result<Option<LazyRecord<'_>>> {
        if !self.advance_to_next_observation()? {
            return Ok(None);
        }
        Ok(Some(self.state.build_lazy_record()))
    }

    /// Shared sync read loop: drives [`DctReaderState`] through one
    /// observation's worth of lines. Returns `Ok(false)` on a clean
    /// end-of-data, `Ok(true)` when an observation has been staged
    /// and is ready to be parsed.
    fn advance_to_next_observation(&mut self) -> Result<bool> {
        if self.state.is_completed() {
            return Ok(false);
        }
        let lines_per_observation = self.state.begin_observation();
        for line_index in 0..lines_per_observation {
            let buffer = self.state.line_buffer_mut(line_index);
            let bytes_read = self.inner.read_line(buffer)?;
            match self.state.finalize_line(line_index, bytes_read) {
                LineOutcome::Read => {}
                LineOutcome::CleanEof => return Ok(false),
                LineOutcome::PartialObservation => {
                    return Err(self.state.unexpected_eof_error());
                }
            }
        }
        self.state.advance_observation();
        Ok(true)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::stata::dct::dct_error::DctError;
    use crate::stata::dct::dct_source::DctSource;
    use crate::stata::dct::value::Value;
    use crate::stata::dct::variable_type::VariableType;
    use crate::stata::missing_value::MissingValue;
    use crate::stata::stata_byte::StataByte;
    use crate::stata::stata_double::StataDouble;
    use crate::stata::stata_float::StataFloat;
    use crate::stata::stata_int::StataInt;
    use std::io::Cursor;

    fn parse_with_data(input: &[u8]) -> DctReader<Cursor<&[u8]>> {
        let source = DctSource::options()
            .from_reader(Cursor::new(input))
            .unwrap();
        let DctSource::Embedded { schema, reader } = source else {
            panic!("expected embedded data")
        };
        DctReader::options(schema).from_reader(reader)
    }

    fn external_with_data<'a>(dict: &[u8], data: &'a [u8]) -> DctReader<Cursor<&'a [u8]>> {
        let source = DctSource::options().from_reader(Cursor::new(dict)).unwrap();
        let DctSource::External(schema) = source else {
            panic!("expected external schema");
        };
        DctReader::options(schema).from_reader(Cursor::new(data))
    }

    #[test]
    fn reads_single_fixed_width_record() {
        let input = b"dictionary {\n\
            _column(1) byte b1 %3.0f\n\
            _column(4) int i1 %5.0f\n\
            _column(9) str s1 %5s\n\
            }\n\
            04212345hello\n";
        let mut reader = parse_with_data(input);
        let record = reader.read_record().unwrap().unwrap();
        let values = record.values();
        assert_eq!(values.len(), 3);
        assert!(matches!(values[0], Value::Byte(StataByte::Present(42))));
        assert!(matches!(values[1], Value::Int(StataInt::Present(12345))));
        match &values[2] {
            Value::String(s) => assert_eq!(s.as_ref(), "hello"),
            _ => panic!("expected string"),
        }
    }

    #[test]
    fn applies_implicit_decimals() {
        let input = b"dictionary {\n\
            _column(1) float f1 %5.2f\n\
            }\n\
            12345\n";
        let mut reader = parse_with_data(input);
        let record = reader.read_record().unwrap().unwrap();
        match record.values()[0] {
            Value::Float(StataFloat::Present(v)) => {
                assert!((v - 123.45_f32).abs() < 0.001);
            }
            _ => panic!("expected float"),
        }
    }

    #[test]
    fn detects_dot_as_system_missing() {
        let input = b"dictionary {\n\
            _column(1) byte b1 %3.0f\n\
            }\n\
              .\n";
        let mut reader = parse_with_data(input);
        let record = reader.read_record().unwrap().unwrap();
        assert!(matches!(
            record.values()[0],
            Value::Byte(StataByte::Missing(MissingValue::System))
        ));
        assert!(reader.warnings().is_empty());
    }

    #[test]
    fn legitimate_blank_field_is_missing_without_warning() {
        // Field is blank but the line is the right length — no warning.
        // (Use external_with_data so `\` line continuations in the
        // dict literal can't accidentally swallow the data's spaces.)
        let dict = b"dictionary {\n_column(1) byte b1 %3.0f\n}\n";
        let data = b"   \n";
        let mut reader = external_with_data(dict, data);
        {
            let record = reader.read_record().unwrap().unwrap();
            assert!(matches!(
                record.values()[0],
                Value::Byte(StataByte::Missing(MissingValue::System))
            ));
        }
        assert!(reader.warnings().is_empty());
    }

    #[test]
    fn short_line_field_warns_blank_treated_as_missing() {
        // Schema declares a 3-byte field at offset 0, but the data line
        // is only 1 byte long — short line.
        let input = b"dictionary {\n\
            _column(1) byte b1 %3.0f\n\
            }\n\
            \n";
        let mut reader = parse_with_data(input);
        let record = reader.read_record().unwrap().unwrap();
        assert!(matches!(
            record.values()[0],
            Value::Byte(StataByte::Missing(MissingValue::System))
        ));
        assert!(
            reader
                .warnings()
                .iter()
                .any(|w| matches!(w, DctWarning::BlankFieldTreatedAsMissing { .. }))
        );
    }

    #[test]
    fn warnings_are_cleared_between_records() {
        let input = b"dictionary {\n\
            _column(1) byte b1 %3.0f\n\
            }\n\
            \n\
            042\n";
        let mut reader = parse_with_data(input);
        // First record: short line → warning emitted. Scope the
        // borrow so we can read `warnings()` afterwards.
        {
            let _r1 = reader.read_record().unwrap().unwrap();
        }
        assert_eq!(reader.warnings().len(), 1);
        // Second record: regular field → warnings cleared at the
        // start of `read_record`.
        {
            let _r2 = reader.read_record().unwrap().unwrap();
        }
        assert!(reader.warnings().is_empty());
    }

    #[test]
    fn integer_value_promotes_byte_to_int() {
        // 200 doesn't fit in i8 but fits in i16 → promote.
        let input = b"dictionary {\n\
            _column(1) byte b1 %3.0f\n\
            }\n\
            200\n";
        let mut reader = parse_with_data(input);
        let record = reader.read_record().unwrap().unwrap();
        assert!(matches!(
            record.values()[0],
            Value::Int(StataInt::Present(200))
        ));
        assert!(reader.warnings().iter().any(|w| matches!(
            w,
            DctWarning::IntegerPromotion {
                from: VariableType::Byte,
                to: VariableType::Int,
                ..
            }
        )));
    }

    #[test]
    fn integer_value_promotes_byte_through_long_to_double() {
        // ~5e9 doesn't fit in i8/i16/i32 but is a finite f64 → Double.
        let input = b"dictionary {\n\
            _column(1) byte b1 %15.0f\n\
            }\n\
            5000000000     \n";
        let mut reader = parse_with_data(input);
        {
            let record = reader.read_record().unwrap().unwrap();
            match &record.values()[0] {
                Value::Double(StataDouble::Present(v)) => {
                    assert!((v - 5_000_000_000.0).abs() < 1.0);
                }
                other => panic!("expected promoted Double, got {other:?}"),
            }
        }
        assert!(reader.warnings().iter().any(|w| matches!(
            w,
            DctWarning::IntegerPromotion {
                from: VariableType::Byte,
                to: VariableType::Double,
                ..
            }
        )));
    }

    #[test]
    fn byte_value_at_missing_marker_boundary_promotes() {
        // 101 fits in i8 but is reserved as `.a` in V113+ DTA. Must
        // promote so the value isn't silently misinterpreted as a
        // tagged missing marker downstream.
        let dict = b"dictionary {\n_column(1) byte b1 %3.0f\n}\n";
        let data = b"101\n";
        let mut reader = external_with_data(dict, data);
        {
            let record = reader.read_record().unwrap().unwrap();
            assert!(matches!(
                record.values()[0],
                Value::Int(StataInt::Present(101))
            ));
        }
        assert!(reader.warnings().iter().any(|w| matches!(
            w,
            DctWarning::IntegerPromotion {
                from: VariableType::Byte,
                to: VariableType::Int,
                ..
            }
        )));
    }

    #[test]
    fn byte_value_at_max_present_does_not_promote() {
        // 100 is exactly the V113+ Byte max present — should fit
        // without promotion.
        let dict = b"dictionary {\n_column(1) byte b1 %3.0f\n}\n";
        let data = b"100\n";
        let mut reader = external_with_data(dict, data);
        {
            let record = reader.read_record().unwrap().unwrap();
            assert!(matches!(
                record.values()[0],
                Value::Byte(StataByte::Present(100))
            ));
        }
        assert!(reader.warnings().is_empty());
    }

    #[test]
    fn integer_value_in_range_does_not_promote() {
        let input = b"dictionary {\n\
            _column(1) byte b1 %3.0f\n\
            }\n\
            042\n";
        let mut reader = parse_with_data(input);
        let record = reader.read_record().unwrap().unwrap();
        assert!(matches!(
            record.values()[0],
            Value::Byte(StataByte::Present(42))
        ));
        assert!(
            !reader
                .warnings()
                .iter()
                .any(|w| matches!(w, DctWarning::IntegerPromotion { .. }))
        );
    }

    #[test]
    fn reads_multiple_records_until_eof() {
        let input = b"dictionary {\n\
            _column(1) byte b1 %3.0f\n\
            }\n\
            001\n\
            002\n\
            003\n";
        let mut reader = parse_with_data(input);
        let mut count = 0;
        while reader.read_record().unwrap().is_some() {
            count += 1;
        }
        assert_eq!(count, 3);
    }

    #[test]
    fn returns_none_at_clean_eof() {
        let dict = b"dictionary {\n\
            _column(1) byte b1 %3.0f\n\
            }\n";
        let mut reader = external_with_data(dict, b"");
        assert!(reader.read_record().unwrap().is_none());
    }

    #[test]
    fn reads_multi_line_observation() {
        let input = b"dictionary {\n\
            _column(1) byte a %3.0f\n\
            _newline\n\
            _column(1) byte b %3.0f\n\
            _column(5) byte c %3.0f\n\
            }\n\
            010\n\
            020 030\n\
            040\n\
            050 060\n";
        let mut reader = parse_with_data(input);
        let r1 = reader.read_record().unwrap().unwrap();
        let v1 = r1.values();
        assert!(matches!(v1[0], Value::Byte(StataByte::Present(10))));
        assert!(matches!(v1[1], Value::Byte(StataByte::Present(20))));
        assert!(matches!(v1[2], Value::Byte(StataByte::Present(30))));
        drop(r1);
        let r2 = reader.read_record().unwrap().unwrap();
        let v2 = r2.values();
        assert!(matches!(v2[0], Value::Byte(StataByte::Present(40))));
        assert!(matches!(v2[1], Value::Byte(StataByte::Present(50))));
        assert!(matches!(v2[2], Value::Byte(StataByte::Present(60))));
    }

    #[test]
    fn errors_on_invalid_numeric_field() {
        let input = b"dictionary {\n\
            _column(1) byte a %3.0f\n\
            }\n\
            abc\n";
        let mut reader = parse_with_data(input);
        let result = reader.read_record();
        assert!(matches!(result, Err(DctError::InvalidNumericValue { .. })));
    }

    #[test]
    fn disabled_warnings_leave_buffer_empty() {
        // Same scenario as the integer-promotion test, but with
        // warnings disabled at the options builder. The promotion
        // still happens (it has to — the value doesn't fit the
        // declared type) but no warning is recorded.
        let dict = b"dictionary {\n_column(1) byte b1 %3.0f\n}\n";
        let data = b"200\n";
        let source = DctSource::options()
            .from_reader(Cursor::new(&dict[..]))
            .unwrap();
        let DctSource::External(schema) = source else {
            panic!("expected external schema");
        };
        let mut reader = DctReader::options(schema)
            .record_warnings(false)
            .from_reader(Cursor::new(&data[..]));
        {
            let record = reader.read_record().unwrap().unwrap();
            assert!(matches!(
                record.values()[0],
                Value::Int(StataInt::Present(200))
            ));
        }
        assert!(reader.warnings().is_empty());
    }

    #[test]
    fn errors_on_mid_observation_eof() {
        let input = b"dictionary {\n\
            _column(1) byte a %3.0f\n\
            _newline\n\
            _column(1) byte b %3.0f\n\
            }\n\
            042\n";
        let mut reader = parse_with_data(input);
        let result = reader.read_record();
        assert!(matches!(result, Err(DctError::UnexpectedEofInData { .. })));
    }
}
