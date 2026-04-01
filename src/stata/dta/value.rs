use crate::stata::stata_byte::StataByte;
use crate::stata::stata_double::StataDouble;
use crate::stata::stata_float::StataFloat;
use crate::stata::stata_int::StataInt;
use crate::stata::stata_long::StataLong;

use super::long_string_ref::LongStringRef;

/// A single cell value from the data section of a DTA file.
///
/// Numeric variants use the typed Stata representations that
/// distinguish present values from missing values. String variants
/// borrow from the reader's internal buffer for zero-copy access.
///
/// `LongStringRef` values are unresolved pointers into the strL
/// section; use the [`LongStringReader`] to retrieve the actual text.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Value<'a> {
    /// A 1-byte signed integer or missing value.
    Byte(StataByte),
    /// A 2-byte signed integer or missing value.
    Int(StataInt),
    /// A 4-byte signed integer or missing value.
    Long(StataLong),
    /// A 4-byte IEEE 754 float or missing value.
    Float(StataFloat),
    /// An 8-byte IEEE 754 double or missing value.
    Double(StataDouble),
    /// A fixed-length string, decoded and trimmed of null padding.
    String(&'a str),
    /// A reference to a long string in the strL section.
    LongStringRef(LongStringRef),
}
