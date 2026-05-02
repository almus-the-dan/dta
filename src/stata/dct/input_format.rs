use crate::stata::dct::numeric_style::NumericStyle;

/// How bytes are consumed from a data record for a single variable.
///
/// Derived from the `%infmt` token on a dictionary line, with
/// [`Self::FreeNumeric`] as the Stata default when no token is
/// present.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InputFormat {
    /// Fixed-width numeric: consume exactly `width` bytes from the
    /// record, interpret as a number, and shift the decimal point
    /// left by `decimals` digits.
    FixedNumeric {
        /// Field width in bytes.
        width: usize,
        /// Implicit decimal places to apply after parsing.
        decimals: u8,
        /// Whether the field is a fixed-point, general, or scientific
        /// format.
        style: NumericStyle,
    },
    /// Fixed-width string: consume exactly `width` bytes as raw text.
    FixedString {
        /// Field width in bytes.
        width: usize,
    },
    /// Free-format numeric: skip leading whitespace, then read until
    /// the next whitespace or end of record.
    FreeNumeric,
    /// Free-format string: skip leading whitespace, then read until
    /// the next whitespace or, when the token starts with a quote,
    /// the matching closing quote.
    FreeString,
}
