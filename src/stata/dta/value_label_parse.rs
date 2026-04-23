//! Pure parse helpers shared by the sync and async value-label
//! readers. I/O stays in the caller.

use super::byte_order::ByteOrder;
use super::dta_error::{DtaError, Field, FormatErrorKind, Result, Section};
use super::value_label::{ValueLabelEntry, ValueLabelSet};

/// Disambiguated 5-byte tag at the entry-start position within the
/// modern value-labels section.
pub(super) enum XmlLabelTag {
    /// Opening `<lbl>` tag for a single value-label set.
    EntryOpen,
    /// Start of the `</value_labels>` section close tag (`"</val"`).
    SectionClose,
}

/// Bytes remaining after the 5-byte head of `</value_labels>`.
pub(super) const VALUE_LABELS_CLOSE_REST: &[u8] = b"ue_labels>";

/// Classifies the first five bytes of a value-label entry or section
/// terminator. Returns `None` when the head matches neither.
pub(super) fn classify_xml_label_tag(head: &[u8]) -> Option<XmlLabelTag> {
    debug_assert_eq!(head.len(), 5, "XML value-label tag head must be 5 bytes");
    match head {
        b"<lbl>" => Some(XmlLabelTag::EntryOpen),
        b"</val" => Some(XmlLabelTag::SectionClose),
        _ => None,
    }
}

/// Decodes a null-terminated label from raw bytes using the given
/// encoding. `max_len` caps the search for the null terminator.
pub(super) fn decode_label(
    bytes: &[u8],
    max_len: usize,
    encoding: &'static encoding_rs::Encoding,
) -> Result<String> {
    let bounded = &bytes[..bytes.len().min(max_len)];
    super::string_decoding::decode_null_terminated(bounded, encoding)
        .map(std::borrow::Cow::into_owned)
        .ok_or_else(|| {
            DtaError::io(
                Section::ValueLabels,
                std::io::Error::other("invalid string encoding in value label"),
            )
        })
}

/// Returns the shared "value-label set size overflow" format error.
/// Fires when a declared table field (entry count, text length, or a
/// byte offset derived from them) overflows the platform's address
/// space during parsing.
pub(super) fn overflow_error() -> DtaError {
    DtaError::format(
        Section::ValueLabels,
        0,
        FormatErrorKind::FieldTooLarge {
            field: Field::ValueLabelEntry,
            max: u64::try_from(usize::MAX).unwrap_or(u64::MAX),
            actual: u64::MAX,
        },
    )
}

/// Narrows a V104 entry index (`usize`) to `i32` — the legacy layout
/// uses the entry's position as its value. Produces a
/// [`FormatErrorKind::FieldTooLarge`] on overflow.
pub(super) fn entry_index_to_i32(entry_index: usize) -> Result<i32> {
    i32::try_from(entry_index).map_err(|_| {
        DtaError::format(
            Section::ValueLabels,
            0,
            FormatErrorKind::FieldTooLarge {
                field: Field::ValueLabelEntry,
                max: u64::try_from(i32::MAX).unwrap_or(u64::MAX),
                actual: u64::try_from(entry_index).unwrap_or(u64::MAX),
            },
        )
    })
}

/// Parses the modern value-label payload:
/// `n` (u32), `txtlen` (u32), `off[n]` (u32 each), `val[n]` (i32 each),
/// `txt[txtlen]`.
pub(super) fn parse_modern_payload(
    payload: &[u8],
    byte_order: ByteOrder,
    encoding: &'static encoding_rs::Encoding,
    set_name: &str,
) -> Result<ValueLabelSet> {
    if payload.len() < 8 {
        let error = DtaError::format(
            Section::ValueLabels,
            0,
            FormatErrorKind::Truncated {
                expected: 8,
                actual: u64::try_from(payload.len()).unwrap_or(u64::MAX),
            },
        );
        return Err(error);
    }

    let entry_count = byte_order.read_u32([payload[0], payload[1], payload[2], payload[3]]);
    let text_len = byte_order.read_u32([payload[4], payload[5], payload[6], payload[7]]);

    let entry_count_usize = usize::try_from(entry_count).map_err(|_| overflow_error())?;
    let text_len_usize = usize::try_from(text_len).map_err(|_| overflow_error())?;

    // Validate payload length: 8 (header) + 4*n (offsets) + 4*n (values) + txt length
    let expected_len = 8usize
        .checked_add(
            entry_count_usize
                .checked_mul(8)
                .ok_or_else(overflow_error)?,
        )
        .and_then(|v| v.checked_add(text_len_usize))
        .ok_or_else(overflow_error)?;

    if payload.len() < expected_len {
        let error = DtaError::format(
            Section::ValueLabels,
            0,
            FormatErrorKind::Truncated {
                expected: u64::try_from(expected_len).unwrap_or(u64::MAX),
                actual: u64::try_from(payload.len()).unwrap_or(u64::MAX),
            },
        );
        return Err(error);
    }

    let offsets_start = 8;
    let values_start = offsets_start + 4 * entry_count_usize;
    let text_start = values_start + 4 * entry_count_usize;

    let mut entries = Vec::with_capacity(entry_count_usize);
    for entry_index in 0..entry_count_usize {
        let offset_position = offsets_start + 4 * entry_index;
        let text_offset = byte_order.read_u32([
            payload[offset_position],
            payload[offset_position + 1],
            payload[offset_position + 2],
            payload[offset_position + 3],
        ]);
        let text_offset_usize = usize::try_from(text_offset).map_err(|_| overflow_error())?;

        if text_offset_usize >= text_len_usize {
            let error = DtaError::format(
                Section::ValueLabels,
                0,
                FormatErrorKind::Truncated {
                    expected: u64::from(text_offset) + 1,
                    actual: u64::from(text_len),
                },
            );
            return Err(error);
        }

        let value_position = values_start + 4 * entry_index;
        let value = byte_order.read_i32([
            payload[value_position],
            payload[value_position + 1],
            payload[value_position + 2],
            payload[value_position + 3],
        ]);

        let label_bytes = &payload[text_start + text_offset_usize..];
        let label = decode_label(label_bytes, text_len_usize - text_offset_usize, encoding)?;

        let entry = ValueLabelEntry::new(value, label);
        entries.push(entry);
    }

    let set = ValueLabelSet::new(set_name.to_owned(), entries);
    Ok(set)
}
