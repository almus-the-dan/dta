use encoding_rs::Encoding;

use super::byte_order::ByteOrder;
use super::dta_error::{DtaError, Result, Section};
use super::release::Release;
use super::value::Value;
use super::variable::Variable;

/// A single observation (row) from the data section of a DTA file
/// that parses values on demand.
///
/// Unlike [`Record`](super::record::Record), a `LazyRecord` does not
/// eagerly parse all values into a `Vec`. Instead, it holds the raw
/// row bytes and decodes individual [`Value`]s only when requested
/// via [`value`](Self::value). This avoids allocation and parsing
/// overhead when only a subset of columns is needed.
///
/// String values require the decoded bytes to borrow directly from
/// the row buffer, which is only possible when the file encoding is
/// UTF-8 (or the string contains only ASCII). For non-UTF-8 files
/// with non-ASCII strings, use [`Record`](super::record::Record)
/// via [`RecordReader::read_record`](super::record_reader::RecordReader::read_record)
/// instead.
#[derive(Debug)]
pub struct LazyRecord<'a> {
    row_bytes: &'a [u8],
    variables: &'a [Variable],
    release: Release,
    byte_order: ByteOrder,
    encoding: &'static Encoding,
}

impl<'a> LazyRecord<'a> {
    #[must_use]
    pub(crate) fn new(
        row_bytes: &'a [u8],
        variables: &'a [Variable],
        release: Release,
        byte_order: ByteOrder,
        encoding: &'static Encoding,
    ) -> Self {
        Self {
            row_bytes,
            variables,
            release,
            byte_order,
            encoding,
        }
    }

    /// Parses and returns the value at the given column index.
    ///
    /// The column index corresponds to the position in the schema's
    /// variable list. Values are decoded from the raw row bytes on
    /// each call — no caching is performed.
    ///
    /// # Errors
    ///
    /// Returns an error if `index` is out of bounds, if a numeric
    /// value has an unrecognized missing-value bit pattern, or if a
    /// string requires non-UTF-8 decoding that would produce an
    /// owned allocation.
    pub fn value(&self, index: usize) -> Result<Value<'a>> {
        let variable = self.variables.get(index).ok_or_else(|| {
            DtaError::io(
                Section::Records,
                std::io::Error::other("column index out of bounds"),
            )
        })?;

        let offset = variable.offset();
        let width = variable.variable_type().width();
        let column_bytes = self.row_bytes.get(offset..offset + width).ok_or_else(|| {
            DtaError::io(
                Section::Records,
                std::io::Error::other("row data too short for variable layout"),
            )
        })?;

        Value::from_column_bytes(
            column_bytes,
            variable.variable_type(),
            self.byte_order,
            self.release,
            self.encoding,
        )
    }

    /// The number of columns (variables) in this record.
    #[must_use]
    #[inline]
    pub fn variable_count(&self) -> usize {
        self.variables.len()
    }
}
