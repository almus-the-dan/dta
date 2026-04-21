//! Pure formatting helpers shared by the sync and async record
//! writers. I/O stays in the caller.

use std::borrow::Cow;

use encoding_rs::Encoding;

use super::byte_order::ByteOrder;
use super::dta_error::{DtaError, Field, FormatErrorKind, Result, Section};
use super::value::Value;
use super::variable::Variable;
use super::variable_type::VariableType;

/// Returns `true` when `value`'s variant matches the on-disk
/// [`VariableType`]. `FixedString` widths are not checked here —
/// those errors surface at write time.
#[must_use]
pub(super) fn value_matches(variable_type: VariableType, value: &Value<'_>) -> bool {
    matches!(
        (variable_type, value),
        (VariableType::Byte, Value::Byte(_))
            | (VariableType::Int, Value::Int(_))
            | (VariableType::Long, Value::Long(_))
            | (VariableType::Float, Value::Float(_))
            | (VariableType::Double, Value::Double(_))
            | (VariableType::FixedString(_), Value::String(_))
            | (VariableType::LongString, Value::LongStringRef(_))
    )
}

/// Rejects a record whose length doesn't match the schema's variable
/// count.
pub(super) fn validate_record_arity(
    actual_len: usize,
    expected_len: usize,
    position: u64,
) -> Result<()> {
    if actual_len != expected_len {
        let error = DtaError::format(
            Section::Records,
            position,
            FormatErrorKind::RecordArityMismatch {
                expected: u64::try_from(expected_len).unwrap_or(u64::MAX),
                actual: u64::try_from(actual_len).unwrap_or(u64::MAX),
            },
        );
        return Err(error);
    }
    Ok(())
}

/// Rejects a record whose per-column value variants don't match the
/// schema's variable types. Assumes arity has already been validated
/// (indexes into `variables` with `values` positions).
pub(super) fn validate_record_value_types(
    values: &[Value<'_>],
    variables: &[Variable],
    position: u64,
) -> Result<()> {
    for (index, value) in values.iter().enumerate() {
        let variable = &variables[index];
        let expected = variable.variable_type();
        if !value_matches(expected, value) {
            let variable_index = narrow_variable_index(index, position)?;
            let error = DtaError::format(
                Section::Records,
                position,
                FormatErrorKind::RecordValueTypeMismatch {
                    variable_index,
                    expected,
                },
            );
            return Err(error);
        }
    }
    Ok(())
}

/// Narrows a `usize` variable index to the `u32` on-disk width used
/// throughout the record section, producing a
/// [`FormatErrorKind::FieldTooLarge`] tagged with `Field::VariableCount`
/// if the index exceeds `u32::MAX`. Shared by the sync and async
/// record writers.
pub(super) fn narrow_variable_index(index: usize, position: u64) -> Result<u32> {
    u32::try_from(index).map_err(|_| {
        DtaError::format(
            Section::Records,
            position,
            FormatErrorKind::FieldTooLarge {
                field: Field::VariableCount,
                max: u64::from(u32::MAX),
                actual: u64::try_from(index).unwrap_or(u64::MAX),
            },
        )
    })
}

/// Signals that the in-memory observation count has exceeded `u64`.
/// Practically unreachable (would require 2^64 `write_record` calls),
/// but keeps the error variant aligned with every other size/overflow
/// condition in the writers rather than leaking a free-form I/O error.
pub(super) fn observation_count_overflow_error(position: u64) -> DtaError {
    DtaError::format(
        Section::Records,
        position,
        FormatErrorKind::FieldTooLarge {
            field: Field::ObservationCount,
            max: u64::MAX,
            actual: u64::MAX,
        },
    )
}

/// Encodes the 6 bytes of a `u48` value in the given byte order for
/// emission into the data section (V118+ `strL` observation field).
/// Errors with [`FormatErrorKind::FieldTooLarge`] if `value >= 2^48`.
///
/// For big-endian the 6 data-carrying bytes are the upper ones of the
/// equivalent `u64` layout (`bytes8[2..8]`); for little-endian they
/// are the lower ones (`bytes8[0..6]`).
pub(super) fn encode_u48(value: u64, byte_order: ByteOrder, position: u64) -> Result<[u8; 6]> {
    const MAX_U48: u64 = (1u64 << 48) - 1;
    if value > MAX_U48 {
        let error = DtaError::format(
            Section::Records,
            position,
            FormatErrorKind::FieldTooLarge {
                field: Field::ObservationCount,
                max: MAX_U48,
                actual: value,
            },
        );
        return Err(error);
    }
    let bytes8 = byte_order.write_u64(value);
    let slice = match byte_order {
        ByteOrder::BigEndian => &bytes8[2..8],
        ByteOrder::LittleEndian => &bytes8[0..6],
    };
    let mut bytes = [0u8; 6];
    bytes.copy_from_slice(slice);
    Ok(bytes)
}

/// Encodes a `FixedString` column value, surfacing encoding or width
/// violations as record-shaped format errors.
///
/// [`FormatErrorKind::InvalidEncoding`] tagged with
/// `Field::VariableValue` for unmappable characters;
/// [`FormatErrorKind::RecordStringTooLong`] tagged with the variable
/// index + declared width if the encoded bytes exceed the slot.
pub(super) fn encode_record_string<'a>(
    text: &'a str,
    encoding: &'static Encoding,
    variable_index: u32,
    width: u16,
    position: u64,
) -> Result<Cow<'a, [u8]>> {
    let (encoded, _, had_unmappable) = encoding.encode(text);
    if had_unmappable {
        let error = DtaError::format(
            Section::Records,
            position,
            FormatErrorKind::InvalidEncoding {
                field: Field::VariableValue,
            },
        );
        return Err(error);
    }
    if encoded.len() > usize::from(width) {
        let error = DtaError::format(
            Section::Records,
            position,
            FormatErrorKind::RecordStringTooLong {
                variable_index,
                max: width,
                actual: u32::try_from(encoded.len()).unwrap_or(u32::MAX),
            },
        );
        return Err(error);
    }
    Ok(encoded)
}
