//! Pure formatting helpers shared by the sync and async value-label
//! writers. I/O stays in the caller.

use std::borrow::Cow;

use encoding_rs::Encoding;

use super::dta_error::{DtaError, Field, FormatErrorKind, Result, Section};
use super::value_label::{ValueLabelEntry, ValueLabelSet};

/// Maximum value that fits in the V104 legacy layout. Each slot is
/// 8 bytes and `table_len` is a `u16`, so `slot_count * 8 ≤ u16::MAX`
/// gives `slot_count ≤ 8191`. Values are 0-indexed, so the largest
/// representable value is `8191 - 1 = 8190`.
pub(super) const OLD_VALUE_LABEL_MAX_VALUE: i32 = 8190;

/// Output shape of [`build_modern_text_payload`] — encoded labels
/// (borrowed when possible), per-entry byte offsets into the logical
/// text area, and the total text length.
pub(super) type ModernTextPayload<'a> = (Vec<Cow<'a, [u8]>>, Vec<u32>, u32);

/// Validates every entry in `table` and packs encoded labels into a
/// slot-indexed vector for the V104 legacy layout.
///
/// The returned `Vec`'s length is the slot count (= max value + 1,
/// or 0 when empty); empty slots are `None` ("no entry for this
/// value"). Each slot holds a `Cow<[u8]>` — borrowed directly from
/// the caller's `ValueLabelSet` on the UTF-8 → UTF-8 pass-through
/// path, owned only when the active encoding had to transcode.
///
/// Errors upfront on:
/// - negative or out-of-range values (`OldValueLabelValueOutOfRange`)
/// - duplicate values (same variant)
/// - labels exceeding the 8-byte slot (`FieldTooLarge`)
/// - labels the active encoding can't represent (`InvalidEncoding`)
pub(super) fn build_old_slot_table<'a>(
    table: &'a ValueLabelSet,
    encoding: &'static Encoding,
    position: u64,
) -> Result<Vec<Option<Cow<'a, [u8]>>>> {
    let mut slots: Vec<Option<Cow<'a, [u8]>>> = Vec::new();
    for entry in table.entries() {
        let value = entry.value();
        if !(0..=OLD_VALUE_LABEL_MAX_VALUE).contains(&value) {
            let error = DtaError::format(
                Section::ValueLabels,
                position,
                FormatErrorKind::OldValueLabelValueOutOfRange { value },
            );
            return Err(error);
        }
        // Verified above
        let slot = usize::try_from(value).expect("0..=OLD_VALUE_LABEL_MAX_VALUE fits usize");

        if slot >= slots.len() {
            slots.resize(slot + 1, None);
        }
        if slots[slot].is_some() {
            let error = DtaError::format(
                Section::ValueLabels,
                position,
                FormatErrorKind::OldValueLabelValueOutOfRange { value },
            );
            return Err(error);
        }

        let (encoded, _, had_unmappable) = encoding.encode(entry.label());
        if had_unmappable {
            let error = DtaError::format(
                Section::ValueLabels,
                position,
                FormatErrorKind::InvalidEncoding {
                    field: Field::ValueLabelEntry,
                },
            );
            return Err(error);
        }
        if encoded.len() > 8 {
            let error = DtaError::format(
                Section::ValueLabels,
                position,
                FormatErrorKind::FieldTooLarge {
                    field: Field::ValueLabelEntry,
                    max: 8,
                    actual: u64::try_from(encoded.len()).unwrap_or(u64::MAX),
                },
            );
            return Err(error);
        }
        slots[slot] = Some(encoded);
    }
    Ok(slots)
}

/// Encodes every label into an owned or borrowed `Cow` (no
/// concatenation) and records each label's byte offset into the
/// logical null-terminated text area the DTA layout expects.
///
/// Returns `(encoded_labels, offsets, text_len)` where `offsets[i]`
/// is the byte position of the `i`-th label in the text area and
/// `text_len` is the total text-area byte count (including the
/// per-label null terminators).
///
/// Errors upfront on labels the active encoding can't represent
/// (`InvalidEncoding`) and on cumulative text length exceeding
/// `u32::MAX`.
pub(super) fn build_modern_text_payload<'a>(
    entries: &'a [ValueLabelEntry],
    encoding: &'static Encoding,
    position: u64,
) -> Result<ModernTextPayload<'a>> {
    let mut encoded_labels = Vec::with_capacity(entries.len());
    let mut offsets = Vec::with_capacity(entries.len());
    let mut running_len = 0;
    for entry in entries {
        let (encoded, _, had_unmappable) = encoding.encode(entry.label());
        if had_unmappable {
            return Err(DtaError::format(
                Section::ValueLabels,
                position,
                FormatErrorKind::InvalidEncoding {
                    field: Field::ValueLabelEntry,
                },
            ));
        }
        let offset =
            u32::try_from(running_len).map_err(|_| text_overflow(position, running_len))?;
        offsets.push(offset);
        // Each label contributes its own bytes plus one null-terminator
        // byte to the logical text area.
        running_len = running_len
            .checked_add(encoded.len())
            .and_then(|n| n.checked_add(1))
            .ok_or_else(|| text_overflow(position, usize::MAX))?;
        encoded_labels.push(encoded);
    }
    let text_len = u32::try_from(running_len).map_err(|_| text_overflow(position, running_len))?;
    Ok((encoded_labels, offsets, text_len))
}

/// Shared constructor for the "text area too large for u32" format
/// error — the label payload's length field is `u32` on every
/// release.
pub(super) fn text_overflow(position: u64, actual: usize) -> DtaError {
    DtaError::format(
        Section::ValueLabels,
        position,
        FormatErrorKind::FieldTooLarge {
            field: Field::ValueLabelEntry,
            max: u64::from(u32::MAX),
            actual: u64::try_from(actual).unwrap_or(u64::MAX),
        },
    )
}

/// Narrows the V104 table size (`slot_count * 8`) to `u16`, producing
/// a [`FormatErrorKind::FieldTooLarge`] at `position` on overflow.
pub(super) fn narrow_slot_count_to_table_len(slot_count: usize, position: u64) -> Result<u16> {
    slot_count
        .checked_mul(8)
        .and_then(|n| u16::try_from(n).ok())
        .ok_or_else(|| {
            let actual = u64::try_from(slot_count)
                .unwrap_or(u64::MAX)
                .saturating_mul(8);
            DtaError::format(
                Section::ValueLabels,
                position,
                FormatErrorKind::FieldTooLarge {
                    field: Field::ValueLabelEntry,
                    max: u64::from(u16::MAX),
                    actual,
                },
            )
        })
}

/// Narrows the modern-layout entry count (`entries.len()`) to `u32`,
/// producing a [`FormatErrorKind::FieldTooLarge`] at `position` on
/// overflow.
pub(super) fn narrow_entry_count_to_u32(entries_len: usize, position: u64) -> Result<u32> {
    u32::try_from(entries_len).map_err(|_| {
        DtaError::format(
            Section::ValueLabels,
            position,
            FormatErrorKind::FieldTooLarge {
                field: Field::ValueLabelEntry,
                max: u64::from(u32::MAX),
                actual: u64::try_from(entries_len).unwrap_or(u64::MAX),
            },
        )
    })
}

/// Computes the modern value-label payload's `table_len` field as a
/// `u32`. Layout: `8 (header) + 8*n (offsets + values) + text_len`.
/// Returns a [`FormatErrorKind::FieldTooLarge`] at `position` if any
/// step overflows.
pub(super) fn modern_payload_bytes(entry_count: u32, text_len: u32, position: u64) -> Result<u32> {
    let payload_bytes = u64::from(entry_count)
        .checked_mul(8)
        .and_then(|n| n.checked_add(8))
        .and_then(|n| n.checked_add(u64::from(text_len)))
        .ok_or_else(|| {
            DtaError::format(
                Section::ValueLabels,
                position,
                FormatErrorKind::FieldTooLarge {
                    field: Field::ValueLabelEntry,
                    max: u64::from(u32::MAX),
                    actual: u64::MAX,
                },
            )
        })?;
    u32::try_from(payload_bytes).map_err(|_| {
        DtaError::format(
            Section::ValueLabels,
            position,
            FormatErrorKind::FieldTooLarge {
                field: Field::ValueLabelEntry,
                max: u64::from(u32::MAX),
                actual: payload_bytes,
            },
        )
    })
}
