use std::borrow::Cow;

use crate::stata::stata_byte::StataByte;
use crate::stata::stata_double::StataDouble;
use crate::stata::stata_float::StataFloat;
use crate::stata::stata_int::StataInt;
use crate::stata::stata_long::StataLong;

/// A single cell value parsed from a DCT-described data file.
///
/// Numeric variants reuse the typed Stata representations from
/// [`crate::stata`] so that the system missing value (a lone `.` or
/// blank fixed-width field in raw text) is preserved alongside
/// present values.
///
/// String values borrow from the reader's line buffer when possible
/// (the data is already valid UTF-8) and own when transcoding or
/// trimming required allocation.
#[derive(Debug, Clone, PartialEq)]
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
    /// A string field, decoded and trimmed of trailing padding.
    String(Cow<'a, str>),
}

impl<'a> Value<'a> {
    /// Convenience constructor for [`Value::String`] from a borrowed
    /// `&str`. Equivalent to `DctValue::String(Cow::Borrowed(s))`.
    #[must_use]
    #[inline]
    pub fn string(s: &'a str) -> Self {
        Self::String(Cow::Borrowed(s))
    }
}
