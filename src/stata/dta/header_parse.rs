//! Pure parse helpers shared by the sync and async header readers.
//!
//! Each function takes already-read bytes (plus the byte offset at
//! which they were read, for error reporting) and returns the parsed
//! value or a [`DtaError`]. The I/O itself stays in the caller —
//! that's what lets both the sync and async readers use the same
//! logic without duplicating parsing alongside two flavors of read
//! machinery.

use super::byte_order::ByteOrder;
use super::dta_error::{DtaError, FormatErrorKind, Result, Section};
use super::release::Release;
use crate::stata::stata_timestamp::StataTimestamp;

/// Validates a binary-format release byte. Binary formats only accept
/// 104–116; 117+ use XML and trip `InvalidMagic` here.
pub(super) fn parse_binary_release(byte: u8) -> Result<Release> {
    let release =
        Release::try_from(byte).map_err(|kind| DtaError::format(Section::Header, 0, kind))?;
    if release.is_xml_like() {
        return Err(DtaError::format(
            Section::Header,
            0,
            FormatErrorKind::InvalidMagic,
        ));
    }
    Ok(release)
}

/// Validates a binary-format byte-order byte (`0x01` or `0x02`).
/// The position is fixed at byte 1 of the file since this byte always
/// follows the release byte at offset 0.
pub(super) fn parse_binary_byte_order(byte: u8) -> Result<ByteOrder> {
    ByteOrder::from_byte(byte).map_err(|kind| DtaError::format(Section::Header, 1, kind))
}

/// Parses a 3-byte ASCII release number from the XML `<release>` tag.
/// Rejects pre-117 releases — those are binary-format only and
/// appearing inside XML tags is a malformed file.
pub(super) fn parse_xml_release(buffer: &[u8], position: u64) -> Result<Release> {
    debug_assert_eq!(buffer.len(), 3, "XML release buffer must be 3 bytes");
    let release = ascii_digits_to_u8(buffer[0], buffer[1], buffer[2]).ok_or(DtaError::format(
        Section::Header,
        position,
        FormatErrorKind::InvalidMagic,
    ))?;
    let release = Release::try_from(release)
        .map_err(|kind| DtaError::format(Section::Header, position, kind))?;
    if !release.is_xml_like() {
        return Err(DtaError::format(
            Section::Header,
            position,
            FormatErrorKind::InvalidMagic,
        ));
    }
    Ok(release)
}

/// Parses a 3-byte XML byte-order tag (`"MSF"` or `"LSF"`).
pub(super) fn parse_xml_byte_order(buffer: &[u8], position: u64) -> Result<ByteOrder> {
    debug_assert_eq!(buffer.len(), 3, "XML byte-order buffer must be 3 bytes");
    let tag = core::str::from_utf8(buffer).map_err(|_| {
        DtaError::format(
            Section::Header,
            position,
            FormatErrorKind::InvalidByteOrderTag,
        )
    })?;
    ByteOrder::from_tag(tag).map_err(|kind| DtaError::format(Section::Header, position, kind))
}

/// Extracts a Stata timestamp from a fixed-width null-terminated
/// buffer. Returns `None` if the buffer is empty, non-UTF-8, or fails
/// to parse as a Stata timestamp.
pub(super) fn parse_fixed_timestamp(buffer: &[u8]) -> Option<StataTimestamp> {
    let end = buffer.iter().position(|&b| b == 0).unwrap_or(buffer.len());
    std::str::from_utf8(&buffer[..end])
        .ok()
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .and_then(|s| StataTimestamp::parse(s).ok())
}

/// Converts three ASCII digit bytes to a `u8`, e.g. `b"117"` → `117`.
fn ascii_digits_to_u8(hundreds: u8, tens: u8, ones: u8) -> Option<u8> {
    let hundreds = hundreds.checked_sub(b'0')?;
    let hundreds = u16::from(hundreds);
    if hundreds > 9 {
        return None;
    }
    let tens = tens.checked_sub(b'0')?;
    let tens = u16::from(tens);
    if tens > 9 {
        return None;
    }
    let ones = ones.checked_sub(b'0')?;
    let ones = u16::from(ones);
    if ones > 9 {
        return None;
    }
    u8::try_from(hundreds * 100 + tens * 10 + ones).ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ascii_digits_valid() {
        assert_eq!(ascii_digits_to_u8(b'1', b'0', b'4'), Some(104));
        assert_eq!(ascii_digits_to_u8(b'1', b'1', b'9'), Some(119));
        assert_eq!(ascii_digits_to_u8(b'0', b'0', b'0'), Some(0));
        assert_eq!(ascii_digits_to_u8(b'2', b'5', b'5'), Some(255));
    }

    #[test]
    fn ascii_digits_invalid() {
        assert_eq!(ascii_digits_to_u8(b'a', b'b', b'c'), None);
        assert_eq!(ascii_digits_to_u8(b'1', b'a', b'2'), None);
    }

    #[test]
    fn ascii_digits_overflow() {
        assert_eq!(ascii_digits_to_u8(b'2', b'5', b'6'), None);
    }
}
