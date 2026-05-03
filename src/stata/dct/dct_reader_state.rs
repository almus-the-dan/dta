//! Pure parse state for DCT data records.
//!
//! Owns the schema, line buffers, observation counter, and warning
//! channel. The sync and async readers wrap this state with their
//! respective I/O loops; both share the same parse logic via
//! [`build_record`](DctReaderState::build_record) and
//! [`build_lazy_record`](DctReaderState::build_lazy_record).

use std::borrow::Cow;

use crate::stata::missing_value::MissingValue;
use crate::stata::stata_byte::{DTA_113_MAX_INT8, StataByte};
use crate::stata::stata_double::StataDouble;
use crate::stata::stata_float::StataFloat;
use crate::stata::stata_int::{DTA_113_MAX_INT16, StataInt};
use crate::stata::stata_long::{DTA_113_MAX_INT32, StataLong};

use super::column::Column;
use super::dct_error::{DctError, Result};
use super::dct_warning::DctWarning;
use super::input_format::InputFormat;
use super::lazy_record::LazyRecord;
use super::line_ending::strip_terminator;
use super::record::Record;
use super::schema::Schema;
use super::value::Value;
use super::variable_type::VariableType;

/// Result of an attempted line read for the current observation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum LineOutcome {
    /// A line was read successfully into the requested buffer.
    Read,
    /// EOF was reached cleanly before any line of this observation
    /// was read. The reader should stop.
    CleanEof,
    /// EOF was reached partway through reading an observation. The
    /// reader should produce an `UnexpectedEofInData` error.
    PartialObservation,
}

/// Mutable parse state for one streaming pass through a DCT data
/// file's records.
///
/// State methods are pure (no I/O); the sync or async wrapper does
/// `read_line` into the buffer slot returned by
/// [`line_buffer_mut`](Self::line_buffer_mut), then notifies the
/// state via [`finalize_line`](Self::finalize_line).
#[derive(Debug)]
pub(super) struct DctReaderState {
    schema: Schema,
    line_buffers: Vec<String>,
    observation_number: usize,
    completed: bool,
    warnings: Vec<DctWarning>,
}

impl DctReaderState {
    #[must_use]
    pub(super) fn new(schema: Schema) -> Self {
        Self {
            schema,
            line_buffers: Vec::new(),
            observation_number: 0,
            completed: false,
            warnings: Vec::new(),
        }
    }

    #[must_use]
    pub(super) fn schema(&self) -> &Schema {
        &self.schema
    }

    #[must_use]
    pub(super) fn warnings(&self) -> &[DctWarning] {
        &self.warnings
    }

    #[must_use]
    pub(super) fn is_completed(&self) -> bool {
        self.completed
    }

    /// Resets per-observation state and returns the number of
    /// physical lines the I/O layer must read for the next record.
    pub(super) fn begin_observation(&mut self) -> usize {
        self.warnings.clear();
        let lines_per_observation = self.schema.lines_per_observation();
        self.line_buffers
            .resize_with(lines_per_observation, String::new);
        for buffer in &mut self.line_buffers {
            buffer.clear();
        }
        lines_per_observation
    }

    /// Returns `&mut` to the line buffer for `line_index`. The I/O
    /// layer reads one physical line into it.
    pub(super) fn line_buffer_mut(&mut self, line_index: usize) -> &mut String {
        &mut self.line_buffers[line_index]
    }

    /// Notifies the state that the read at `line_index` returned
    /// `bytes_read` bytes. Strips line endings on success and flags
    /// completion on EOF.
    pub(super) fn finalize_line(&mut self, line_index: usize, bytes_read: usize) -> LineOutcome {
        if bytes_read == 0 {
            self.completed = true;
            if line_index == 0 {
                LineOutcome::CleanEof
            } else {
                LineOutcome::PartialObservation
            }
        } else {
            strip_terminator(&mut self.line_buffers[line_index]);
            LineOutcome::Read
        }
    }

    /// Bumps the observation counter once all lines for an
    /// observation have been successfully read.
    pub(super) fn advance_observation(&mut self) {
        self.observation_number += 1;
    }

    /// Builds the error returned when the data file ends partway
    /// through an observation.
    pub(super) fn unexpected_eof_error(&self) -> DctError {
        DctError::UnexpectedEofInData {
            observation: self.observation_number + 1,
            variables_read: 0,
        }
    }

    /// Builds an eager [`Record`] from the current line buffers.
    pub(super) fn build_record(&mut self) -> Result<Record<'_>> {
        let schema = &self.schema;
        let line_buffers = &self.line_buffers;
        let observation_number = self.observation_number;
        let warnings = &mut self.warnings;

        let mut values = Vec::with_capacity(schema.columns().len());
        for column in schema.columns() {
            let line = &line_buffers[column.line_offset()];
            let value = parse_field(line, column, observation_number, Some(warnings))?;
            values.push(value);
        }

        Ok(Record::new(values))
    }

    /// Builds a [`LazyRecord`] borrowing from the current line buffers.
    pub(super) fn build_lazy_record(&self) -> LazyRecord<'_> {
        LazyRecord::new(
            &self.line_buffers,
            self.schema.columns(),
            self.observation_number,
        )
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
