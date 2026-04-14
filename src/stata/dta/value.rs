use std::borrow::Cow;

use encoding_rs::Encoding;

use crate::stata::stata_byte::StataByte;
use crate::stata::stata_double::StataDouble;
use crate::stata::stata_float::StataFloat;
use crate::stata::stata_int::StataInt;
use crate::stata::stata_long::StataLong;

use super::byte_order::ByteOrder;
use super::dta_error::{DtaError, Result, Section};
use super::long_string_ref::LongStringRef;
use super::release::Release;
use super::variable_type::VariableType;

/// A single cell value from the data section of a DTA file.
///
/// Numeric variants use the typed Stata representations that
/// distinguish present values from missing values. String variants
/// borrow from the reader's internal buffer for zero-copy access.
///
/// `LongStringRef` values are unresolved pointers into the strL
/// section; use the [`LongStringReader`] to retrieve the actual text.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Value<'a> {
    /// A 1-byte signed integer or missing value.
    Byte(StataByte),
    /// A 2-byte signed integer or missing value.
    Int(StataInt),
    /// A 4-byte signed integer or missing value.
    Long(StataLong),
    /// A 4-byte IEEE 754 float or missing value.
    Float(StataFloat),
    /// An 8-byte IEEE 754 double or missing value.
    Double(StataDouble),
    /// A fixed-length string, decoded and trimmed of null padding.
    String(&'a str),
    /// A reference to a long string in the strL section.
    LongStringRef(LongStringRef),
}

impl<'a> Value<'a> {
    /// Parses a single value from raw column bytes in a data row.
    ///
    /// The caller is responsible for slicing `column_bytes` to the
    /// correct width for the given `variable_type`. Numeric values
    /// are decoded using `byte_order`; strings are decoded using
    /// `encoding`. The `release` is needed for strL reference
    /// layout, which differs between format 117 and 118+.
    ///
    /// # String encoding limitation
    ///
    /// `Value::String` borrows from `column_bytes`. This is
    /// zero-copy for UTF-8 and ASCII, but non-UTF-8 encodings that
    /// require transcoding (e.g., Windows-1252 with non-ASCII
    /// characters) produce owned data that cannot be returned as a
    /// reference. In that case this method returns an error.
    ///
    /// # Errors
    ///
    /// Returns an error if a numeric value has an unrecognized
    /// missing-value bit pattern, if a string cannot be decoded, or
    /// if a non-UTF-8 string requires an owned allocation.
    pub(crate) fn from_column_bytes(
        column_bytes: &'a [u8],
        variable_type: VariableType,
        byte_order: ByteOrder,
        release: Release,
        encoding: &'static Encoding,
    ) -> Result<Self> {
        match variable_type {
            VariableType::Byte => parse_byte(column_bytes),
            VariableType::Int => parse_int(column_bytes, byte_order),
            VariableType::Long => parse_long(column_bytes, byte_order),
            VariableType::Float => parse_float(column_bytes, byte_order),
            VariableType::Double => parse_double(column_bytes, byte_order),
            VariableType::FixedString(_) => parse_fixed_string(column_bytes, encoding),
            VariableType::LongString => {
                Ok(parse_long_string_ref(column_bytes, byte_order, release))
            }
        }
    }
}

fn parse_byte(column_bytes: &[u8]) -> Result<Value<'_>> {
    let stata_value = StataByte::try_from(column_bytes[0]).map_err(|_| unrecognized_value())?;
    Ok(Value::Byte(stata_value))
}

fn parse_int(column_bytes: &[u8], byte_order: ByteOrder) -> Result<Value<'_>> {
    let raw = byte_order.read_u16([column_bytes[0], column_bytes[1]]);
    let stata_value = StataInt::try_from(raw).map_err(|_| unrecognized_value())?;
    Ok(Value::Int(stata_value))
}

fn parse_long(column_bytes: &[u8], byte_order: ByteOrder) -> Result<Value<'_>> {
    let raw = byte_order.read_u32([
        column_bytes[0],
        column_bytes[1],
        column_bytes[2],
        column_bytes[3],
    ]);
    let stata_value = StataLong::try_from(raw).map_err(|_| unrecognized_value())?;
    Ok(Value::Long(stata_value))
}

fn parse_float(column_bytes: &[u8], byte_order: ByteOrder) -> Result<Value<'_>> {
    let raw = byte_order.read_f32([
        column_bytes[0],
        column_bytes[1],
        column_bytes[2],
        column_bytes[3],
    ]);
    let stata_value = StataFloat::try_from(raw).map_err(|_| unrecognized_value())?;
    Ok(Value::Float(stata_value))
}

fn parse_double(column_bytes: &[u8], byte_order: ByteOrder) -> Result<Value<'_>> {
    let raw = byte_order.read_f64([
        column_bytes[0],
        column_bytes[1],
        column_bytes[2],
        column_bytes[3],
        column_bytes[4],
        column_bytes[5],
        column_bytes[6],
        column_bytes[7],
    ]);
    let stata_value = StataDouble::try_from(raw).map_err(|_| unrecognized_value())?;
    Ok(Value::Double(stata_value))
}

fn parse_fixed_string<'a>(
    column_bytes: &'a [u8],
    encoding: &'static Encoding,
) -> Result<Value<'a>> {
    let end = column_bytes
        .iter()
        .position(|&b| b == 0)
        .unwrap_or(column_bytes.len());
    let decoded = encoding
        .decode_without_bom_handling_and_without_replacement(&column_bytes[..end])
        .ok_or_else(|| {
            DtaError::io(
                Section::Records,
                std::io::Error::other("invalid string encoding in record"),
            )
        })?;
    match decoded {
        Cow::Borrowed(s) => Ok(Value::String(s)),
        Cow::Owned(_) => Err(DtaError::io(
            Section::Records,
            std::io::Error::other(
                "cannot return non-UTF-8 decoded string as a \
                 reference; use read_record() for non-UTF-8 files \
                 with non-ASCII strings",
            ),
        )),
    }
}

/// Parses a strL reference from 8 raw bytes.
///
/// The layout differs by format version:
///   - 117:  `v` = u32 (4 bytes), `o` = u32 (4 bytes)
///   - 118+: `v` = u16 (2 bytes), `o` = u48 (6 bytes)
fn parse_long_string_ref(
    column_bytes: &[u8],
    byte_order: ByteOrder,
    release: Release,
) -> Value<'_> {
    let (variable, observation) = if release.supports_extended_observation_count() {
        parse_extended_long_string_ref(column_bytes, byte_order)
    } else {
        parse_classic_long_string_ref(column_bytes, byte_order)
    };
    Value::LongStringRef(LongStringRef::new(variable, observation))
}

/// Format 118+: `v` = u16 (2 bytes), `o` = u48 (6 bytes).
fn parse_extended_long_string_ref(column_bytes: &[u8], byte_order: ByteOrder) -> (u32, u64) {
    let variable_index = byte_order.read_u16([column_bytes[0], column_bytes[1]]);
    let observation_index = byte_order.read_u64([
        0,
        0,
        column_bytes[2],
        column_bytes[3],
        column_bytes[4],
        column_bytes[5],
        column_bytes[6],
        column_bytes[7],
    ]);
    (u32::from(variable_index), observation_index)
}

/// Format 117: `v` = u32 (4 bytes), `o` = u32 (4 bytes).
fn parse_classic_long_string_ref(column_bytes: &[u8], byte_order: ByteOrder) -> (u32, u64) {
    let variable_index = byte_order.read_u32([
        column_bytes[0],
        column_bytes[1],
        column_bytes[2],
        column_bytes[3],
    ]);
    let observation_index = byte_order.read_u32([
        column_bytes[4],
        column_bytes[5],
        column_bytes[6],
        column_bytes[7],
    ]);
    (variable_index, u64::from(observation_index))
}

fn unrecognized_value() -> DtaError {
    DtaError::io(
        Section::Records,
        std::io::Error::other("unrecognized Stata missing value bit pattern"),
    )
}
