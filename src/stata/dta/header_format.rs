//! Pure formatting helpers and constants shared by the sync and
//! async header writers.
//!
//! I/O stays in the caller; this module only holds the
//! representation-level pieces (fixed preamble bytes, timestamp
//! string rendering) both writer flavors need.

use crate::stata::stata_timestamp::StataTimestamp;

/// Fixed filetype byte in the binary preamble. Always `0x01`.
pub(super) const BINARY_FILETYPE: u8 = 0x01;

/// Reserved padding byte following the filetype. Always `0x00`.
pub(super) const BINARY_RESERVED_PADDING: u8 = 0x00;

/// Formats a timestamp for header emission. Absent timestamps render
/// as the empty string.
pub(super) fn format_timestamp(timestamp: Option<&StataTimestamp>) -> String {
    timestamp.map(ToString::to_string).unwrap_or_default()
}
