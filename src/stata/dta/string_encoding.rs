//! Shared byte-level string encoding and length-narrowing helpers
//! used by the sync and async writer state machinery. The sync and
//! async flavors handle their own I/O and reuse this module for the
//! representation-level bits that are identical between them.

use std::borrow::Cow;

use encoding_rs::Encoding;

use super::dta_error::{DtaError, Field, FormatErrorKind, Result, Section};

/// Encodes `value` with `encoding`, returning the encoded bytes or a
/// [`FormatErrorKind::InvalidEncoding`] error if any characters are
/// unmappable.
pub(super) fn encode_value<'a>(
    value: &'a str,
    encoding: &'static Encoding,
    section: Section,
    field: Field,
    position: u64,
) -> Result<Cow<'a, [u8]>> {
    let (encoded, _, had_unmappable) = encoding.encode(value);
    if had_unmappable {
        return Err(DtaError::format(
            section,
            position,
            FormatErrorKind::InvalidEncoding { field },
        ));
    }
    Ok(encoded)
}

/// Narrows `bytes_len` to `u8`, producing a `FieldTooLarge` format
/// error at the given `position` on overflow.
pub(super) fn narrow_len_to_u8(
    bytes_len: usize,
    section: Section,
    field: Field,
    position: u64,
) -> Result<u8> {
    u8::try_from(bytes_len).map_err(|_| {
        DtaError::format(
            section,
            position,
            FormatErrorKind::FieldTooLarge {
                field,
                max: u64::from(u8::MAX),
                actual: u64::try_from(bytes_len).expect("length exceeds u64"),
            },
        )
    })
}

/// Narrows `bytes_len` to `u16`, producing a `FieldTooLarge` format
/// error at the given `position` on overflow.
pub(super) fn narrow_len_to_u16(
    bytes_len: usize,
    section: Section,
    field: Field,
    position: u64,
) -> Result<u16> {
    u16::try_from(bytes_len).map_err(|_| {
        DtaError::format(
            section,
            position,
            FormatErrorKind::FieldTooLarge {
                field,
                max: u64::from(u16::MAX),
                actual: u64::try_from(bytes_len).expect("length exceeds u64"),
            },
        )
    })
}
