//! Pure parse helpers shared by the sync and async record readers.
//! I/O stays in the caller; this module turns a filled row buffer
//! into a vector of [`Value`]s.

use encoding_rs::Encoding;

use super::byte_order::ByteOrder;
use super::dta_error::{DtaError, Field, FormatErrorKind, Result, Section};
use super::release::Release;
use super::schema::Schema;
use super::value::Value;

/// XML-format opening tag for the data section (format 117+).
pub(super) const OPENING_TAG: &[u8] = b"<data>";

/// XML-format closing tag for the data section (format 117+).
pub(super) const CLOSING_TAG: &[u8] = b"</data>";

/// Decodes a filled row buffer into per-variable [`Value`]s using
/// each variable's on-disk offset + width. The returned values
/// borrow string bytes from `row_bytes`, so the buffer must outlive
/// the returned vector (the reader's scratch buffer is reused on
/// the next `read_record` call).
pub(super) fn parse_row<'a>(
    row_bytes: &'a [u8],
    schema: &Schema,
    byte_order: ByteOrder,
    release: Release,
    encoding: &'static Encoding,
) -> Result<Vec<Value<'a>>> {
    let variables = schema.variables();
    let mut values = Vec::with_capacity(variables.len());
    for variable in variables {
        let offset = variable.offset();
        let variable_type = variable.variable_type();
        let width = variable_type.width();
        let column_bytes = &row_bytes[offset..offset + width];
        let value =
            Value::from_column_bytes(column_bytes, variable_type, byte_order, release, encoding)?;
        values.push(value);
    }
    Ok(values)
}

/// Returns the shared "data section byte offset/size overflows `u64`"
/// format error. The binary characteristic-to-record transition
/// computes `records_offset + observation_count * row_len` to locate
/// the start of the value-labels section; any of those arithmetic
/// steps failing represents the same underlying concern.
pub(super) fn data_section_overflow_error(position: u64) -> DtaError {
    DtaError::format(
        Section::Records,
        position,
        FormatErrorKind::FieldTooLarge {
            field: Field::ObservationCount,
            max: u64::MAX,
            actual: u64::MAX,
        },
    )
}

/// Result of [`compute_record_seek_target`]: the absolute byte offset
/// to seek to, and whether the target lands at the end of the data
/// section.
pub(super) struct RecordSeekTarget {
    /// Absolute byte offset to seek the underlying reader to.
    pub target: u64,
    /// `true` when `index == observation_count`. The caller should
    /// read the closing `</data>` tag (XML formats only) and mark the
    /// reader completed after seeking.
    pub at_end_of_data: bool,
}

/// Computes the target byte offset for a record-index seek.
///
/// The data section is fixed-width: row `i` lives at
/// `records_offset + opening_tag_len + i * row_len`, where
/// `opening_tag_len` is [`OPENING_TAG`]'s width for XML formats
/// (117+) and 0 for binary formats.
///
/// `index == observation_count` is valid and signals the end-of-data
/// position (right before `</data>` for XML, right at the value-labels
/// boundary for binary).
///
/// Shared by the sync and async record readers; the caller performs
/// the actual seek and any closing-tag read.
///
/// # Errors
///
/// Returns [`DtaError::Io`] with
/// [`InvalidInput`](std::io::ErrorKind::InvalidInput) if
/// `index > observation_count`, and [`DtaError::Format`] with
/// [`FieldTooLarge`](FormatErrorKind::FieldTooLarge) if any of the
/// offset arithmetic overflows `u64`.
pub(super) fn compute_record_seek_target(
    index: u64,
    observation_count: u64,
    records_offset: u64,
    row_len: usize,
    is_xml_like: bool,
) -> Result<RecordSeekTarget> {
    if index > observation_count {
        let message =
            format!("record index {index} is out of bounds for {observation_count} observations",);
        let error = std::io::Error::new(std::io::ErrorKind::InvalidInput, message);
        return Err(DtaError::io(Section::Records, error));
    }

    let opening_tag_len: u64 = if is_xml_like {
        u64::try_from(OPENING_TAG.len()).map_err(|_| data_section_overflow_error(records_offset))?
    } else {
        0
    };
    let row_len =
        u64::try_from(row_len).map_err(|_| data_section_overflow_error(records_offset))?;
    let data_start = records_offset
        .checked_add(opening_tag_len)
        .ok_or_else(|| data_section_overflow_error(records_offset))?;
    let index_offset = index
        .checked_mul(row_len)
        .ok_or_else(|| data_section_overflow_error(records_offset))?;
    let target = data_start
        .checked_add(index_offset)
        .ok_or_else(|| data_section_overflow_error(records_offset))?;

    Ok(RecordSeekTarget {
        target,
        at_end_of_data: index == observation_count,
    })
}
