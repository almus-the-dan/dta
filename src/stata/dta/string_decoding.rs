//! Shared byte-level string decoding used by the sync and async
//! reader state machinery. Kept here (rather than on the state types
//! themselves) so both flavors can decode fixed-width null-terminated
//! fields with identical error reporting without duplicating the
//! decode logic.

use encoding_rs::Encoding;

use super::dta_error::{DtaError, Field, FormatErrorKind, Result, Section};

/// Decodes a null-terminated fixed-width string buffer using the
/// given encoding.
///
/// Both the sync `ReaderState::read_fixed_string` and its async
/// equivalent handle the I/O and hand the filled buffer (plus the
/// byte position it was read at, for error reporting) to this helper.
pub(super) fn decode_fixed_string(
    buffer: &[u8],
    encoding: &'static Encoding,
    section: Section,
    field: Field,
    position: u64,
) -> Result<String> {
    let end = buffer.iter().position(|&b| b == 0).unwrap_or(buffer.len());
    let decoded = encoding
        .decode_without_bom_handling_and_without_replacement(&buffer[..end])
        .ok_or_else(|| {
            DtaError::format(
                section,
                position,
                FormatErrorKind::InvalidEncoding { field },
            )
        })?;
    Ok(decoded.into_owned())
}
