use crate::stata::dct::line_ending::strip_terminator;
use crate::stata::missing_value::MissingValue;
use crate::stata::stata_byte::{DTA_113_MAX_INT8, StataByte};
use crate::stata::stata_double::StataDouble;
use crate::stata::stata_float::StataFloat;
use crate::stata::stata_int::{DTA_113_MAX_INT16, StataInt};
use crate::stata::stata_long::{DTA_113_MAX_INT32, StataLong};
use std::borrow::Cow;
use std::io::BufRead;

use super::column::Column;
use super::dct_error::{DctError, Result};
use super::dct_warning::DctWarning;
use super::input_format::InputFormat;
use super::lazy_record::LazyRecord;
use super::record::Record;
use super::schema::Schema;
use super::value::Value;
use super::variable_type::VariableType;

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
pub struct DctReader<R: BufRead> {
    inner: R,
    schema: Schema,
    line_buffers: Vec<String>,
    observation_number: usize,
    completed: bool,
    warnings: Vec<DctWarning>,
}

impl<R: BufRead> DctReader<R> {
    /// Constructs a reader from a parsed schema and a data source.
    ///
    /// Use this when [`parse_dct`](super::parser::parse_dct) returned
    /// [`DctSource::External`](super::dct_source::DctSource::External) and
    /// you have separately opened the data file declared in the
    /// dictionary's `using` clause.
    #[must_use]
    pub fn new(schema: Schema, inner: R) -> Self {
        Self {
            inner,
            schema,
            line_buffers: Vec::new(),
            observation_number: 0,
            completed: false,
            warnings: Vec::new(),
        }
    }

    /// The schema this reader was constructed from.
    #[must_use]
    #[inline]
    pub fn schema(&self) -> &Schema {
        &self.schema
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
        &self.warnings
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

    /// Reads the next observation from the data file.
    ///
    /// Returns `None` once the data file has been fully consumed.
    /// The returned [`Record`] borrows string data from this
    /// reader's internal line buffers, so it must be dropped before
    /// the next call.
    ///
    /// # Errors
    ///
    /// Returns [`DctError`](DctError) on I/O
    /// failure, when the data file ends in the middle of an
    /// observation, or when a field cannot be parsed against the
    /// column's declared type and read format.
    pub fn read_record(&mut self) -> Result<Option<Record<'_>>> {
        if !self.advance_to_next_observation()? {
            return Ok(None);
        }

        let record = self.build_record()?;
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
    /// Returns [`DctError`] on I/O failure or when the data file
    /// ends mid-observation.
    pub fn read_lazy_record(&mut self) -> Result<Option<LazyRecord<'_>>> {
        if !self.advance_to_next_observation()? {
            return Ok(None);
        }

        let lazy = LazyRecord::new(
            &self.line_buffers,
            self.schema.columns(),
            self.observation_number,
        );
        Ok(Some(lazy))
    }

    /// Shared setup for `read_record` and `read_lazy_record`. Clears
    /// per-record state, loads the next observation's lines into the
    /// internal buffers, and bumps `observation_number`. Returns
    /// `Ok(false)` on a clean end-of-data, `Ok(true)` when an
    /// observation is staged and ready to be parsed.
    fn advance_to_next_observation(&mut self) -> Result<bool> {
        if self.completed {
            return Ok(false);
        }

        self.warnings.clear();

        let lines_per_observation = self.schema.lines_per_observation();
        self.line_buffers
            .resize_with(lines_per_observation, String::new);
        for buffer in &mut self.line_buffers {
            buffer.clear();
        }

        if !self.read_lines()? {
            return Ok(false);
        }

        self.observation_number += 1;
        Ok(true)
    }

    fn read_lines(&mut self) -> Result<bool> {
        let lines_per_observation = self.schema.lines_per_observation();
        for line_index in 0..lines_per_observation {
            let read_count = self.inner.read_line(&mut self.line_buffers[line_index])?;
            if read_count == 0 {
                self.completed = true;
                if line_index == 0 {
                    return Ok(false);
                }
                let error = DctError::UnexpectedEofInData {
                    observation: self.observation_number + 1,
                    variables_read: 0,
                };
                return Err(error);
            }
            strip_terminator(&mut self.line_buffers[line_index]);
        }
        Ok(true)
    }

    fn build_record(&mut self) -> Result<Record<'_>> {
        let mut values = Vec::with_capacity(self.schema.columns().len());
        for column in self.schema.columns() {
            let line = &self.line_buffers[column.line_offset()];
            let value = parse_field(
                line,
                column,
                self.observation_number,
                Some(&mut self.warnings),
            )?;
            values.push(value);
        }

        let record = Record::new(values);
        Ok(record)
    }
}

pub(super) fn parse_field<'a>(
    line: &'a str,
    column: &Column,
    observation: usize,
    warnings: Option<&mut Vec<DctWarning>>,
) -> Result<Value<'a>> {
    match column.input_format() {
        InputFormat::FixedNumeric {
            width, decimals, ..
        } => parse_fixed_numeric(line, column, width, decimals, observation, warnings),
        InputFormat::FixedString { width } => {
            parse_fixed_string(line, column, width, observation, warnings)
        }
        InputFormat::FreeNumeric => parse_free_numeric(line, column, observation, warnings),
        InputFormat::FreeString => Ok(parse_free_string(line, column.offset())),
    }
}

fn parse_fixed_numeric<'a>(
    line: &str,
    column: &Column,
    width: usize,
    decimals: u8,
    observation: usize,
    warnings: Option<&mut Vec<DctWarning>>,
) -> Result<Value<'a>> {
    let offset = column.offset();
    let end = offset
        .checked_add(width)
        .ok_or_else(|| record_offset_overflow(column, observation))?;

    let line_len = line.len();
    let truncated = end > line_len;
    let raw_field = &line[offset.min(line_len)..end.min(line_len)];
    let trimmed = raw_field.trim_ascii();

    if trimmed.is_empty() {
        if truncated && let Some(warnings) = warnings {
            let variable = column.name().to_string();
            let warning = DctWarning::BlankFieldTreatedAsMissing {
                variable,
                observation,
            };
            warnings.push(warning);
        }
        let value = missing_value_for(column.storage_type());
        return Ok(value);
    }
    if trimmed == "." {
        let value = missing_value_for(column.storage_type());
        return Ok(value);
    }

    let raw: f64 = trimmed
        .parse()
        .map_err(|_| invalid_numeric(column, observation, trimmed))?;
    let shifted = if decimals == 0 {
        raw
    } else {
        raw / 10f64.powi(i32::from(decimals))
    };

    coerce_numeric(shifted, column, observation, warnings)
}

fn parse_free_numeric<'a>(
    line: &str,
    column: &Column,
    observation: usize,
    warnings: Option<&mut Vec<DctWarning>>,
) -> Result<Value<'a>> {
    let token = take_free_token(line, column.offset());

    if token.is_empty() || token == "." {
        let value = missing_value_for(column.storage_type());
        return Ok(value);
    }

    let raw: f64 = token
        .parse()
        .map_err(|_| invalid_numeric(column, observation, token))?;
    coerce_numeric(raw, column, observation, warnings)
}

fn parse_fixed_string<'a>(
    line: &'a str,
    column: &Column,
    width: usize,
    observation: usize,
    warnings: Option<&mut Vec<DctWarning>>,
) -> Result<Value<'a>> {
    let offset = column.offset();
    let end = offset
        .checked_add(width)
        .ok_or_else(|| record_offset_overflow(column, observation))?;

    let line_len = line.len();
    let truncated = end > line_len;
    let raw = &line[offset.min(line_len)..end.min(line_len)];
    // Stata convention: trailing spaces are padding, not data. Leading
    // spaces are typically meaningful (and trim_ascii would also trim
    // them, which we don't want), so trim only the end.
    let trimmed = raw.trim_ascii_end();

    if truncated
        && trimmed.is_empty()
        && let Some(warnings) = warnings
    {
        let variable = column.name().to_string();
        let warning = DctWarning::BlankFieldTreatedAsMissing {
            variable,
            observation,
        };
        warnings.push(warning);
    }

    Ok(Value::String(Cow::Borrowed(trimmed)))
}

fn parse_free_string(line: &str, offset: usize) -> Value<'_> {
    let from = offset.min(line.len());
    let after = line[from..].trim_ascii_start();

    if let Some(body) = after.strip_prefix('"') {
        let close = body.find('"').unwrap_or(body.len());
        let slice = &body[..close];
        return Value::String(Cow::Borrowed(slice));
    }

    let end = after
        .find(|c: char| c.is_ascii_whitespace())
        .unwrap_or(after.len());
    let slice = &after[..end];
    Value::String(Cow::Borrowed(slice))
}

/// Returns the next whitespace-delimited token at or after `offset`,
/// skipping leading whitespace. Returns an empty string if no token
/// is available before end of line.
fn take_free_token(line: &str, offset: usize) -> &str {
    let from = offset.min(line.len());
    let after = line[from..].trim_ascii_start();
    let end = after
        .find(|c: char| c.is_ascii_whitespace())
        .unwrap_or(after.len());
    &after[..end]
}

fn missing_value_for(storage_type: VariableType) -> Value<'static> {
    match storage_type {
        VariableType::Byte => Value::Byte(StataByte::Missing(MissingValue::System)),
        VariableType::Int => Value::Int(StataInt::Missing(MissingValue::System)),
        VariableType::Long => Value::Long(StataLong::Missing(MissingValue::System)),
        VariableType::Float => Value::Float(StataFloat::Missing(MissingValue::System)),
        VariableType::Double => Value::Double(StataDouble::Missing(MissingValue::System)),
        VariableType::String => Value::String(Cow::Borrowed("")),
    }
}

fn coerce_numeric<'a>(
    value: f64,
    column: &Column,
    observation: usize,
    warnings: Option<&mut Vec<DctWarning>>,
) -> Result<Value<'a>> {
    if !value.is_finite() {
        return Err(invalid_numeric(column, observation, &value.to_string()));
    }
    match column.storage_type() {
        VariableType::Byte | VariableType::Int | VariableType::Long => {
            promote_integer(value, column, observation, warnings)
        }
        VariableType::Float => Ok(Value::Float(StataFloat::Present(f64_to_f32(value)))),
        VariableType::Double => Ok(Value::Double(StataDouble::Present(value))),
        VariableType::String => Err(invalid_numeric(column, observation, &value.to_string())),
    }
}

/// Tries to fit `value` into the declared integer storage type;
/// promotes to the next wider type if it doesn't fit, all the way up
/// to `Double`. Emits a [`DctWarning::IntegerPromotion`] warning when
/// promotion happens. Matches Stata's permissive-import behavior:
/// a too-narrow declared type is a hint, not a hard guarantee.
fn promote_integer<'a>(
    value: f64,
    column: &Column,
    observation: usize,
    warnings: Option<&mut Vec<DctWarning>>,
) -> Result<Value<'a>> {
    let declared = column.storage_type();
    let rounded = value.round();
    let chain = promotion_chain(declared);

    for &candidate in chain {
        if let Some(fitted) = fit_integer(rounded, candidate) {
            if candidate != declared
                && let Some(warnings) = warnings
            {
                let variable = column.name().to_string();
                let warning = DctWarning::IntegerPromotion {
                    variable,
                    observation,
                    from: declared,
                    to: candidate,
                };
                warnings.push(warning);
            }
            return Ok(fitted);
        }
    }

    Err(invalid_numeric(column, observation, &value.to_string()))
}

fn promotion_chain(declared: VariableType) -> &'static [VariableType] {
    match declared {
        VariableType::Byte => &[
            VariableType::Byte,
            VariableType::Int,
            VariableType::Long,
            VariableType::Double,
        ],
        VariableType::Int => &[VariableType::Int, VariableType::Long, VariableType::Double],
        VariableType::Long => &[VariableType::Long, VariableType::Double],
        _ => &[],
    }
}

/// Constructs a `Value` of `target` if `rounded` is in range. The
/// caller is responsible for having already rounded the value.
fn fit_integer<'a>(rounded: f64, target: VariableType) -> Option<Value<'a>> {
    match target {
        VariableType::Byte => fit_i8(rounded).map(|n| Value::Byte(StataByte::Present(n))),
        VariableType::Int => fit_i16(rounded).map(|n| Value::Int(StataInt::Present(n))),
        VariableType::Long => fit_i32(rounded).map(|n| Value::Long(StataLong::Present(n))),
        VariableType::Double => Some(Value::Double(StataDouble::Present(rounded))),
        _ => None,
    }
}

// Stata's typed integer storages reserve the top of their numeric
// range for missing-value markers. The `DTA_113_MAX_INT*` constants
// imported above give the V113+ ceilings — V113+ is the strictest
// layout (27 values reserved per type for system missing plus
// `.a`–`.z`), so anything that fits the V113+ present range also
// fits every older format. Negative values aren't reserved in any
// release, so the floor stays at `iN::MIN`.

#[allow(clippy::cast_possible_truncation)]
fn fit_i8(value: f64) -> Option<i8> {
    if (f64::from(i8::MIN)..=f64::from(DTA_113_MAX_INT8)).contains(&value) {
        Some(value as i8)
    } else {
        None
    }
}

#[allow(clippy::cast_possible_truncation)]
fn fit_i16(value: f64) -> Option<i16> {
    if (f64::from(i16::MIN)..=f64::from(DTA_113_MAX_INT16)).contains(&value) {
        Some(value as i16)
    } else {
        None
    }
}

#[allow(clippy::cast_possible_truncation)]
fn fit_i32(value: f64) -> Option<i32> {
    if (f64::from(i32::MIN)..=f64::from(DTA_113_MAX_INT32)).contains(&value) {
        Some(value as i32)
    } else {
        None
    }
}

#[allow(clippy::cast_possible_truncation)]
fn f64_to_f32(value: f64) -> f32 {
    value as f32
}

fn invalid_numeric(column: &Column, observation: usize, content: &str) -> DctError {
    DctError::InvalidNumericValue {
        observation,
        variable: column.name().to_string(),
        content: content.to_string(),
    }
}

fn record_offset_overflow(column: &Column, observation: usize) -> DctError {
    DctError::RecordOffsetOverflow {
        observation,
        variable: column.name().to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::stata::dct::parser::parse_dct;
    use std::io::Cursor;

    fn parse_with_data(input: &[u8]) -> DctReader<Cursor<&[u8]>> {
        let source = parse_dct(Cursor::new(input)).unwrap();
        match source {
            crate::stata::dct::dct_source::DctSource::Embedded(reader) => reader,
            crate::stata::dct::dct_source::DctSource::External(_) => {
                panic!("expected embedded data")
            }
        }
    }

    fn external_with_data<'a>(dict: &[u8], data: &'a [u8]) -> DctReader<Cursor<&'a [u8]>> {
        let source = parse_dct(Cursor::new(dict)).unwrap();
        let crate::stata::dct::dct_source::DctSource::External(schema) = source else {
            panic!("expected external schema");
        };
        DctReader::new(schema, Cursor::new(data))
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
