//! Pure formatting helpers shared by the sync and async schema
//! writers. I/O stays in the caller; this module holds validation and
//! small representation-level helpers both writer flavours reuse.

use super::dta_error::{DtaError, FormatErrorKind, Result, Section};
use super::release::Release;
use super::schema::Schema;

/// Validates that every variable's type is representable in the
/// header's release — flags `strL` in pre-117 formats and fixed
/// strings wider than [`Release::max_fixed_string_len`]. `position`
/// should be the byte offset at which schema writing is about to
/// begin, for error reporting.
pub(super) fn validate_variable_types(
    schema: &Schema,
    release: Release,
    position: u64,
) -> Result<()> {
    for variable in schema.variables() {
        if variable.variable_type().try_to_u16(release).is_none() {
            let error = DtaError::format(
                Section::Schema,
                position,
                FormatErrorKind::UnsupportedVariableType {
                    variable_type: variable.variable_type(),
                    release,
                },
            );
            return Err(error);
        }
    }
    Ok(())
}

/// Returns `Some((open, close))` when `is_xml` is true, `None`
/// otherwise. Lets writer methods write
/// `if let Some((open, close)) = xml_tags(...)` instead of branching
/// on `is_xml` twice.
pub(super) fn xml_tags<'a>(
    is_xml: bool,
    open: &'a [u8],
    close: &'a [u8],
) -> Option<(&'a [u8], &'a [u8])> {
    if is_xml { Some((open, close)) } else { None }
}
