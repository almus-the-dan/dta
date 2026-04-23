//! Pure parse helpers shared by the sync and async record readers.
//! I/O stays in the caller; this module turns a filled row buffer
//! into a vector of [`Value`]s.

use encoding_rs::Encoding;

use super::byte_order::ByteOrder;
use super::dta_error::{DtaError, Field, FormatErrorKind, Result, Section};
use super::release::Release;
use super::schema::Schema;
use super::value::Value;

/// Decodes a filled row buffer into per-variable [`Value`]s using
/// each variable's on-disk offset + width, pushing the parsed values
/// into the caller-supplied `scratch` vec.
///
/// Values borrow string bytes from `row_bytes`, so the buffer must
/// outlive the scratch vec. Callers clear `scratch` before the call
/// and never expose stale entries; see [`scratch_with_lifetime`].
pub(super) fn parse_row_into<'a>(
    row_bytes: &'a [u8],
    schema: &Schema,
    byte_order: ByteOrder,
    release: Release,
    encoding: &'static Encoding,
    scratch: &mut Vec<Value<'a>>,
) -> Result<()> {
    let variables = schema.variables();
    scratch.reserve(variables.len());
    for variable in variables {
        let offset = variable.offset();
        let variable_type = variable.variable_type();
        let width = variable_type.width();
        let column_bytes = &row_bytes[offset..offset + width];
        let value =
            Value::from_column_bytes(column_bytes, variable_type, byte_order, release, encoding)?;
        scratch.push(value);
    }
    Ok(())
}

/// Relabels the element lifetime of a reader-owned scratch vec to
/// match the caller's row-buffer lifetime `'a`, and clears any stale
/// entries.
///
/// The declared type `Vec<Value<'static>>` is a placeholder — entries
/// actually borrow from the reader's row buffer, which has a shorter
/// lifetime. Rust can't express "lifetime varies per call" on a struct
/// field, so the reader stores the vec with the longest lifetime and
/// uses this helper to narrow it at each call site.
///
/// # Safety
///
/// Sound because:
///
/// 1. The helper clears the vec before returning, dropping any
///    entries whose lifetime has since ended.
/// 2. The returned `&'a mut Vec<Value<'a>>` ties any newly pushed
///    entries to the caller's `'a`.
/// 3. The caller's `read_record` returns `Record<'a>` borrowing from
///    the vec, which reborrows `&'a mut self` through to return —
///    so the next call to this helper (which takes `&mut self` too)
///    is blocked until the returned record is dropped.
pub(super) unsafe fn scratch_with_lifetime<'a>(
    scratch: &'a mut Vec<Value<'static>>,
) -> &'a mut Vec<Value<'a>> {
    // SAFETY: `Vec<T>` layout does not depend on `T`'s lifetime;
    // only on its size/alignment, which are identical across
    // `Value<'static>` and `Value<'a>`. The clear() below enforces
    // the soundness invariant described on this function.
    let scratch: &'a mut Vec<Value<'a>> =
        unsafe { &mut *(core::ptr::from_mut(scratch).cast::<Vec<Value<'a>>>()) };
    scratch.clear();
    scratch
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
