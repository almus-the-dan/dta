//! Pure parse helpers shared by the sync and async characteristic
//! readers. I/O stays in the caller; this module covers the
//! representation-level bits (XML tag dispatch, length arithmetic,
//! error shaping) both reader flavors reuse.

use super::dta_error::{DtaError, Field, FormatErrorKind, Result, Section};

/// Disambiguated XML tag at the entry-start position within the
/// characteristics section.
pub(super) enum XmlCharacteristicTag {
    /// Opening `<characteristics>` section tag.
    SectionOpen,
    /// Closing `</characteristics>` section tag.
    SectionClose,
    /// Opening `<ch>` entry tag.
    EntryOpen,
}

/// Bytes remaining after the 4-byte head of `<characteristics>`.
pub(super) const XML_SECTION_OPEN_REST: &[u8] = b"racteristics>";

/// Bytes remaining after the 4-byte head of `</characteristics>`.
pub(super) const XML_SECTION_CLOSE_REST: &[u8] = b"aracteristics>";

/// Classifies the first four bytes of an XML characteristic tag.
/// Returns `None` if the head matches no known tag — the caller
/// should raise an [`InvalidMagic`](FormatErrorKind::InvalidMagic)
/// error at the tag's start position.
///
/// The tag's head is chosen, so all three possibilities are
/// distinguishable in four bytes:
/// - `<cha` → `<characteristics>` (section open, 13 more bytes follow)
/// - `</ch` → `</characteristics>` (section close, 14 more bytes follow)
/// - `<ch>` → `<ch>` (entry open, complete in 4 bytes)
pub(super) fn classify_xml_tag_head(head: &[u8]) -> Option<XmlCharacteristicTag> {
    debug_assert_eq!(head.len(), 4, "XML tag head must be 4 bytes");
    match head {
        b"<cha" => Some(XmlCharacteristicTag::SectionOpen),
        b"</ch" => Some(XmlCharacteristicTag::SectionClose),
        b"<ch>" => Some(XmlCharacteristicTag::EntryOpen),
        _ => None,
    }
}

/// Converts an expansion-field length from `u32` to `usize`, producing
/// a clean I/O error on overflow. Used by the characteristic reader
/// wherever a `u32` length needs to drive a `skip` / `read_exact`
/// call.
pub(super) fn expansion_length_to_usize(length: u32) -> Result<usize> {
    usize::try_from(length).map_err(|_| {
        DtaError::io(
            Section::Characteristics,
            std::io::Error::other("characteristic length exceeds usize"),
        )
    })
}

/// Returns the byte count of a characteristic's value (the payload
/// remaining after the two fixed-width name fields). Produces a
/// [`FormatErrorKind::Truncated`] if the declared `total_length` is
/// shorter than the two names.
pub(super) fn characteristic_value_len(
    total_length: u32,
    variable_name_len: usize,
    entry_position: u64,
) -> Result<usize> {
    let total_length_usize = expansion_length_to_usize(total_length)?;
    let two_names_len = variable_name_len.checked_mul(2).ok_or_else(|| {
        DtaError::format(
            Section::Characteristics,
            entry_position,
            FormatErrorKind::FieldTooLarge {
                field: Field::CharacteristicName,
                max: u64::from(u32::MAX),
                actual: u64::try_from(variable_name_len)
                    .unwrap_or(u64::MAX)
                    .saturating_mul(2),
            },
        )
    })?;
    total_length_usize
        .checked_sub(two_names_len)
        .ok_or_else(|| {
            DtaError::format(
                Section::Characteristics,
                entry_position,
                FormatErrorKind::Truncated {
                    expected: u64::try_from(two_names_len).unwrap_or(u64::MAX),
                    actual: u64::from(total_length),
                },
            )
        })
}
