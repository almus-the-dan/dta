//! Pure parse helpers shared by the sync and async long-string
//! readers. I/O stays in the caller.

use super::dta_error::{DtaError, Result, Section};
use super::long_string::GsoType;

/// Parsed GSO entry header (the fields after the `"GSO"` magic).
///
/// The reader fills each field by pulling the corresponding bytes
/// off the stream — this struct is pure data, so the sync and async
/// readers can hand their parsed header to the same shared
/// [`read_exact`]/[`Section::LongStrings`] downstream code.
pub(super) struct GsoHeader {
    pub variable: u32,
    pub observation: u64,
    pub gso_type: u8,
    pub data_len: usize,
}

impl GsoHeader {
    /// Whether the entry's payload is binary. Non-binary type bytes
    /// (including unknown values) are treated as text — matching the
    /// lenient decoding behavior both readers need.
    pub(super) fn is_binary(&self) -> bool {
        GsoType::from_byte(self.gso_type) == Some(GsoType::Binary)
    }
}

/// Disambiguated 3-byte tag at the entry-start position within the
/// long-strings section.
pub(super) enum GsoTag {
    /// GSO block magic (`"GSO"`) opening a new long-string entry.
    EntryStart,
    /// Start of the `</strls>` section close tag (`"</s"`).
    SectionClose,
}

/// Bytes remaining after the 3-byte head of `</strls>`.
pub(super) const GSO_SECTION_CLOSE_REST: &[u8] = b"trls>";

/// Classifies the first three bytes of a long-string entry or section
/// terminator. Returns `None` when the head matches neither — the
/// caller should raise
/// [`InvalidLongStringEntry`](super::dta_error::FormatErrorKind::InvalidLongStringEntry)
/// at the tag's start position.
pub(super) fn classify_gso_tag(head: &[u8]) -> Option<GsoTag> {
    debug_assert_eq!(head.len(), 3, "GSO tag head must be 3 bytes");
    match head {
        b"GSO" => Some(GsoTag::EntryStart),
        b"</s" => Some(GsoTag::SectionClose),
        _ => None,
    }
}

/// Converts a long-string data length from `u32` to `usize`, producing
/// a clean I/O error on overflow.
pub(super) fn long_string_data_len_to_usize(length: u32) -> Result<usize> {
    usize::try_from(length).map_err(|_| {
        DtaError::io(
            Section::LongStrings,
            std::io::Error::other("long string data length exceeds usize"),
        )
    })
}
