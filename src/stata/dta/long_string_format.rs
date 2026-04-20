//! Pure formatting helpers and constants shared by the sync and
//! async long-string writers.

use super::dta_error::{DtaError, Field, FormatErrorKind, Result, Section};

/// GSO block magic bytes — exactly these three ASCII bytes open every
/// long-string entry.
pub(super) const GSO_MAGIC: &[u8; 3] = b"GSO";

/// Narrows a long-string payload length from `usize` to `u32`,
/// producing a [`FormatErrorKind::FieldTooLarge`] tagged with
/// `Field::LongStringData` on overflow.
pub(super) fn narrow_long_string_data_len(data_len: usize, position: u64) -> Result<u32> {
    u32::try_from(data_len).map_err(|_| {
        DtaError::format(
            Section::LongStrings,
            position,
            FormatErrorKind::FieldTooLarge {
                field: Field::LongStringData,
                max: u64::from(u32::MAX),
                actual: u64::try_from(data_len).unwrap_or(u64::MAX),
            },
        )
    })
}
