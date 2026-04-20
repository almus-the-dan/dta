//! Pure parse helpers shared by the sync and async value-label
//! readers. I/O stays in the caller.

use super::byte_order::ByteOrder;
use super::dta_error::{DtaError, Field, FormatErrorKind, Result, Section};
use super::value_label::{ValueLabelEntry, ValueLabelTable};

/// Disambiguated 5-byte tag at the entry-start position within the
/// modern value-labels section.
pub(super) enum XmlLabelTag {
    /// Opening `<lbl>` tag for a single value-label table.
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
    let end = bounded
        .iter()
        .position(|&b| b == 0)
        .unwrap_or(bounded.len());
    encoding
        .decode_without_bom_handling_and_without_replacement(&bounded[..end])
        .map(std::borrow::Cow::into_owned)
        .ok_or_else(|| {
            DtaError::io(
                Section::ValueLabels,
                std::io::Error::other("invalid string encoding in value label"),
            )
        })
}

/// Returns the shared "value-label table size overflow" format error.
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

/// Parses the modern value-label payload:
/// `n` (u32), `txtlen` (u32), `off[n]` (u32 each), `val[n]` (i32 each),
/// `txt[txtlen]`.
pub(super) fn parse_modern_payload(
    payload: &[u8],
    byte_order: ByteOrder,
    encoding: &'static encoding_rs::Encoding,
    table_name: &str,
) -> Result<ValueLabelTable> {
    if payload.len() < 8 {
        return Err(DtaError::format(
            Section::ValueLabels,
            0,
            FormatErrorKind::Truncated {
                expected: 8,
                actual: u64::try_from(payload.len()).unwrap_or(u64::MAX),
            },
        ));
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
        return Err(DtaError::format(
            Section::ValueLabels,
            0,
            FormatErrorKind::Truncated {
                expected: u64::try_from(expected_len).unwrap_or(u64::MAX),
                actual: u64::try_from(payload.len()).unwrap_or(u64::MAX),
            },
        ));
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
            return Err(DtaError::format(
                Section::ValueLabels,
                0,
                FormatErrorKind::Truncated {
                    expected: u64::from(text_offset) + 1,
                    actual: u64::from(text_len),
                },
            ));
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

        entries.push(ValueLabelEntry::new(value, label));
    }

    Ok(ValueLabelTable::new(table_name.to_owned(), entries))
}
