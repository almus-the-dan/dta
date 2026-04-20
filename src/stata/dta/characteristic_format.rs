//! Pure formatting helpers shared by the sync and async characteristic
//! writers. I/O stays in the caller.

use std::borrow::Cow;

use encoding_rs::Encoding;

use super::dta_error::{DtaError, Field, FormatErrorKind, Result, Section};
use super::string_encoding::encode_value;

/// Encodes a characteristic's value string via `encoding`, surfacing
/// any unmappable characters as a
/// [`FormatErrorKind::InvalidEncoding`] with `Field::CharacteristicValue`.
///
/// Thin shortcut around [`encode_value`] that pins the section/field
/// pair both writer flavors use.
pub(super) fn encode_characteristic_value<'a>(
    value: &'a str,
    encoding: &'static Encoding,
    position: u64,
) -> Result<Cow<'a, [u8]>> {
    encode_value(
        value,
        encoding,
        Section::Characteristics,
        Field::CharacteristicValue,
        position,
    )
}

/// Total characteristic-entry payload length: two fixed-width name
/// fields plus the encoded value. Returns a
/// [`FormatErrorKind::FieldTooLarge`] if the total doesn't fit
/// `u32` (the on-disk length field is at most 4 bytes in every
/// release).
pub(super) fn payload_length(
    variable_name_len: usize,
    encoded_value_len: usize,
    position: u64,
) -> Result<u32> {
    let overflow = || payload_overflow_error(encoded_value_len, position);
    let total = variable_name_len
        .checked_mul(2)
        .and_then(|names_len| names_len.checked_add(encoded_value_len))
        .ok_or_else(overflow)?;
    u32::try_from(total).map_err(|_| overflow())
}

fn payload_overflow_error(encoded_value_len: usize, position: u64) -> DtaError {
    DtaError::format(
        Section::Characteristics,
        position,
        FormatErrorKind::FieldTooLarge {
            field: Field::CharacteristicValue,
            max: u64::from(u32::MAX),
            actual: u64::try_from(encoded_value_len).unwrap_or(u64::MAX),
        },
    )
}
