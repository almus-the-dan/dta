//! Pure parse helpers shared by the sync and async record readers.
//! I/O stays in the caller; this module turns a filled row buffer
//! into a vector of [`Value`]s.

use encoding_rs::Encoding;

use super::byte_order::ByteOrder;
use super::dta_error::Result;
use super::release::Release;
use super::schema::Schema;
use super::value::Value;

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
        let width = variable.variable_type().width();
        let column_bytes = &row_bytes[offset..offset + width];
        let value = Value::from_column_bytes(
            column_bytes,
            variable.variable_type(),
            byte_order,
            release,
            encoding,
        )?;
        values.push(value);
    }
    Ok(values)
}
