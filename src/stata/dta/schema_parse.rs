//! Pure parse helpers shared by the sync and async schema readers.
//!
//! Each function takes already-read bytes (plus the byte offset at
//! which they were read, for error reporting) and returns the parsed
//! value or a [`DtaError`]. The I/O itself stays in the caller so
//! both reader flavors can reuse the same parsing logic.

use super::byte_order::ByteOrder;
use super::dta_error::{DtaError, Field, FormatErrorKind, Result, Section};
use super::release::Release;
use super::variable::{Variable, VariableBuilder};
use super::variable_type::VariableType;

/// Converts a raw type code to a [`VariableType`].
///
/// The interpretation depends on the format version:
///
/// | Formats   | Numeric codes          | String codes            |
/// |-----------|------------------------|-------------------------|
/// | 104–110   | ASCII `b/i/l/f/d`      | `≥ 0x7F` → len − 0x7F  |
/// | 111–116   | `0xFB`–`0xFF`          | code = byte length      |
/// | 117+      | `0xFFF6`–`0xFFFA`      | code = byte length      |
/// |           | `0x8000` = strL        |                         |
///
/// String codes are validated against the maximum fixed-string length
/// for the format version (80 for 104–110, 244 for 111–116, 2045 for
/// 117+). Codes outside the valid range produce an
/// [`InvalidVariableType`](FormatErrorKind::InvalidVariableType) error.
pub(super) fn parse_type_code(code: u16, release: Release, position: u64) -> Result<VariableType> {
    let invalid = || {
        let error = DtaError::format(
            Section::Schema,
            position,
            FormatErrorKind::InvalidVariableType { code },
        );
        Err(error)
    };

    if release >= Release::V117 {
        match code {
            0xFFFA => Ok(VariableType::Byte),
            0xFFF9 => Ok(VariableType::Int),
            0xFFF8 => Ok(VariableType::Long),
            0xFFF7 => Ok(VariableType::Float),
            0xFFF6 => Ok(VariableType::Double),
            0x8000 => Ok(VariableType::LongString),
            1..=2045 => Ok(VariableType::FixedString(code)),
            _ => invalid(),
        }
    } else if release >= Release::V111 {
        match code {
            0xFB => Ok(VariableType::Byte),
            0xFC => Ok(VariableType::Int),
            0xFD => Ok(VariableType::Long),
            0xFE => Ok(VariableType::Float),
            0xFF => Ok(VariableType::Double),
            1..=244 => Ok(VariableType::FixedString(code)),
            _ => invalid(),
        }
    } else {
        match code {
            0x62 => Ok(VariableType::Byte),   // 'b'
            0x69 => Ok(VariableType::Int),    // 'i'
            0x6C => Ok(VariableType::Long),   // 'l'
            0x66 => Ok(VariableType::Float),  // 'f'
            0x64 => Ok(VariableType::Double), // 'd'
            0x80..=0xCF => Ok(VariableType::FixedString(code - 0x7F)),
            _ => invalid(),
        }
    }
}

/// Adds a `usize` byte offset to a `u64` base position.
///
/// Returns a [`FormatErrorKind::FieldTooLarge`] tagged with
/// `Field::VariableCount` if the offset doesn't fit in `u64` or if
/// the sum overflows `u64` — both are realistically unreachable, but
/// this keeps the math defensively checked instead of silently
/// wrapping in release builds.
pub(super) fn offset_position(base: u64, offset: usize) -> Result<u64> {
    let offset_u64 = u64::try_from(offset).map_err(|_| section_size_overflow_error(base))?;
    base.checked_add(offset_u64)
        .ok_or_else(|| section_size_overflow_error(base))
}

/// Computes `count * entry_len` for sizing a schema section's read
/// buffer. Returns a [`FormatErrorKind::FieldTooLarge`] tagged with
/// `Field::VariableCount` if the multiplication overflows `usize` —
/// which happens when the schema's `variable_count` paired with the
/// release's per-variable field width exceeds the target's address
/// space (a real concern on 16-bit platforms).
pub(super) fn buffer_size(count: usize, entry_len: usize, position: u64) -> Result<usize> {
    count
        .checked_mul(entry_len)
        .ok_or_else(|| section_size_overflow_error(position))
}

/// Narrows the header's `u32` variable count to `usize`. Fires when
/// the declared count exceeds the platform's address space (e.g., a
/// 16-bit target facing a V119 file with > 65k variables). Returns
/// a [`FormatErrorKind::FieldTooLarge`] tagged with
/// `Field::VariableCount`.
pub(super) fn narrow_variable_count_to_usize(declared: u32, position: u64) -> Result<usize> {
    usize::try_from(declared).map_err(|_| {
        DtaError::format(
            Section::Schema,
            position,
            FormatErrorKind::FieldTooLarge {
                field: Field::VariableCount,
                max: u64::try_from(usize::MAX).unwrap_or(u64::MAX),
                actual: u64::from(declared),
            },
        )
    })
}

/// Computes the sort-list entry count (`variable_count + 1`, since
/// the on-disk list is zero-terminated). Returns a
/// [`FormatErrorKind::FieldTooLarge`] at `position` on overflow.
pub(super) fn sort_entry_count(variable_count: usize, position: u64) -> Result<usize> {
    variable_count.checked_add(1).ok_or_else(|| {
        DtaError::format(
            Section::Schema,
            position,
            FormatErrorKind::FieldTooLarge {
                field: Field::VariableCount,
                max: u64::from(u32::MAX),
                actual: u64::try_from(variable_count)
                    .unwrap_or(u64::MAX)
                    .saturating_add(1),
            },
        )
    })
}

/// Shared constructor for the "schema section size overflowed the
/// platform's addressable range" format error. The only meaningful
/// upper bound we can communicate is the format's `K` field width
/// (u32) — the actual overflow may be driven by any of the
/// per-variable byte multiplications.
fn section_size_overflow_error(position: u64) -> DtaError {
    DtaError::format(
        Section::Schema,
        position,
        FormatErrorKind::FieldTooLarge {
            field: Field::VariableCount,
            max: u64::from(u32::MAX),
            actual: u64::MAX,
        },
    )
}

/// Reads a little-endian or big-endian `u64` at the given index within
/// a buffer of packed `u64` values.
pub(super) fn read_u64_at(buffer: &[u8], index: usize, byte_order: ByteOrder) -> u64 {
    let offset = index * 8;
    let bytes = [
        buffer[offset],
        buffer[offset + 1],
        buffer[offset + 2],
        buffer[offset + 3],
        buffer[offset + 4],
        buffer[offset + 5],
        buffer[offset + 6],
        buffer[offset + 7],
    ];
    byte_order.read_u64(bytes)
}

/// Zips the per-variable arrays read out of the schema section into a
/// single vector of [`VariableBuilder`]s. All input vectors must have
/// the same length (the caller reads each from the same
/// `variable_count`) — mismatches panic on internal invariant.
pub(super) fn assemble_variables(
    types: Vec<VariableType>,
    names: Vec<String>,
    formats: Vec<String>,
    value_label_names: Vec<String>,
    labels: Vec<String>,
) -> Vec<VariableBuilder> {
    let count = types.len();
    let mut types = types.into_iter();
    let mut names = names.into_iter();
    let mut formats = formats.into_iter();
    let mut value_label_names = value_label_names.into_iter();
    let mut labels = labels.into_iter();

    let mut variables = Vec::with_capacity(count);
    for _ in 0..count {
        let variable_type = types.next().expect("types length mismatch");
        let variable_name = names.next().expect("names length mismatch");
        let format = formats.next().expect("formats length mismatch");
        let label_name = value_label_names
            .next()
            .expect("value_label_names length mismatch");
        let label_value = labels.next().expect("labels length mismatch");
        let variable = Variable::builder(variable_type, variable_name)
            .format(format)
            .value_label_name(label_name)
            .label(label_value);
        variables.push(variable);
    }
    variables
}
