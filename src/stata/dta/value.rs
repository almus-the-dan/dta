use std::borrow::Cow;

use encoding_rs::Encoding;

use crate::stata::stata_byte::StataByte;
use crate::stata::stata_double::StataDouble;
use crate::stata::stata_float::StataFloat;
use crate::stata::stata_int::StataInt;
use crate::stata::stata_long::StataLong;

use super::byte_order::ByteOrder;
use super::dta_error::{DtaError, Result, Section};
use super::long_string_ref::LongStringRef;
use super::release::Release;
use super::string_decoding::decode_null_terminated;
use super::variable_type::VariableType;

/// A single cell value from the data section of a DTA file.
///
/// Numeric variants use the typed Stata representations that
/// distinguish present values from missing values. The
/// [`String`](Self::String) variant carries a [`Cow<'a, str>`] —
/// borrowed directly from the reader's row buffer on the zero-copy
/// path (UTF-8 or ASCII content), and owned when the declared
/// encoding required transcoding (e.g. Windows-1252 with accented
/// characters).
///
/// `LongStringRef` values are unresolved pointers into the strL
/// section; use the
/// [`LongStringReader`](super::long_string_reader::LongStringReader)
/// to retrieve the actual text.
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
    /// A fixed-length string, decoded and trimmed of null padding.
    /// Borrowed from the row buffer when the bytes are already valid
    /// UTF-8 (including pure ASCII); owned when the declared encoding
    /// had to transcode.
    String(Cow<'a, str>),
    /// A reference to a long string in the strL section.
    LongStringRef(LongStringRef),
}

impl<'a> Value<'a> {
    /// Convenience constructor for `Value::String` from a borrowed
    /// `&str`. Equivalent to `Value::String(Cow::Borrowed(s))` and
    /// useful at call sites that hand the writer a string literal.
    #[must_use]
    #[inline]
    pub fn string(s: &'a str) -> Self {
        Self::String(Cow::Borrowed(s))
    }

    /// Parses a single value from raw column bytes in a data row.
    ///
    /// The caller is responsible for slicing `column_bytes` to the
    /// correct width for the given `variable_type`. Numeric values
    /// are decoded using `byte_order`; strings are decoded using
    /// `encoding`. The `release` is needed for strL reference
    /// layout, which differs between format 117 and 118+.
    ///
    /// # String encoding
    ///
    /// String decoding uses the declared encoding. ASCII and native
    /// UTF-8 content stays in the row buffer (`Cow::Borrowed`);
    /// content that needs transcoding (e.g., Windows-1252 with
    /// accented characters) is returned as `Cow::Owned`. Bytes that
    /// don't decode at all in the declared encoding produce a
    /// [`FormatErrorKind::InvalidEncoding`](super::dta_error::FormatErrorKind::InvalidEncoding).
    ///
    /// # Errors
    ///
    /// Returns an error if a numeric value has an unrecognized
    /// missing-value bit pattern, or if a string's bytes are not
    /// valid in the declared encoding.
    pub(crate) fn from_column_bytes(
        column_bytes: &'a [u8],
        variable_type: VariableType,
        byte_order: ByteOrder,
        release: Release,
        encoding: &'static Encoding,
    ) -> Result<Self> {
        match variable_type {
            VariableType::Byte => parse_byte(column_bytes, release),
            VariableType::Int => parse_int(column_bytes, byte_order, release),
            VariableType::Long => parse_long(column_bytes, byte_order, release),
            VariableType::Float => parse_float(column_bytes, byte_order, release),
            VariableType::Double => parse_double(column_bytes, byte_order, release),
            VariableType::FixedString(_) => parse_fixed_string(column_bytes, encoding),
            VariableType::LongString => {
                Ok(parse_long_string_ref(column_bytes, byte_order, release))
            }
        }
    }
}

fn parse_byte(column_bytes: &[u8], release: Release) -> Result<Value<'_>> {
    let stata_value =
        StataByte::from_raw(column_bytes[0], release).map_err(|_| unrecognized_value())?;
    let value = Value::Byte(stata_value);
    Ok(value)
}

fn parse_int(column_bytes: &[u8], byte_order: ByteOrder, release: Release) -> Result<Value<'_>> {
    let raw = byte_order.read_u16([column_bytes[0], column_bytes[1]]);
    let stata_value = StataInt::from_raw(raw, release).map_err(|_| unrecognized_value())?;
    let value = Value::Int(stata_value);
    Ok(value)
}

fn parse_long(column_bytes: &[u8], byte_order: ByteOrder, release: Release) -> Result<Value<'_>> {
    let raw = byte_order.read_u32([
        column_bytes[0],
        column_bytes[1],
        column_bytes[2],
        column_bytes[3],
    ]);
    let stata_value = StataLong::from_raw(raw, release).map_err(|_| unrecognized_value())?;
    let value = Value::Long(stata_value);
    Ok(value)
}

fn parse_float(column_bytes: &[u8], byte_order: ByteOrder, release: Release) -> Result<Value<'_>> {
    let raw = byte_order.read_f32([
        column_bytes[0],
        column_bytes[1],
        column_bytes[2],
        column_bytes[3],
    ]);
    let stata_value = StataFloat::from_raw(raw, release).map_err(|_| unrecognized_value())?;
    let value = Value::Float(stata_value);
    Ok(value)
}

fn parse_double(column_bytes: &[u8], byte_order: ByteOrder, release: Release) -> Result<Value<'_>> {
    let raw = byte_order.read_f64([
        column_bytes[0],
        column_bytes[1],
        column_bytes[2],
        column_bytes[3],
        column_bytes[4],
        column_bytes[5],
        column_bytes[6],
        column_bytes[7],
    ]);
    let stata_value = StataDouble::from_raw(raw, release).map_err(|_| unrecognized_value())?;
    let value = Value::Double(stata_value);
    Ok(value)
}

fn parse_fixed_string<'a>(
    column_bytes: &'a [u8],
    encoding: &'static Encoding,
) -> Result<Value<'a>> {
    decode_null_terminated(column_bytes, encoding)
        .map(Value::String)
        .ok_or_else(|| {
            DtaError::io(
                Section::Records,
                std::io::Error::other("invalid string encoding in record"),
            )
        })
}

/// Parses a strL reference from 8 raw bytes.
///
/// The layout differs by format version:
///   - 117:  `v` = u32 (4 bytes), `o` = u32 (4 bytes)
///   - 118:  `v` = u16 (2 bytes), `o` = u48 (6 bytes)
///   - 119+: `v` = u24 (3 bytes), `o` = u40 (5 bytes)
fn parse_long_string_ref(
    column_bytes: &[u8],
    byte_order: ByteOrder,
    release: Release,
) -> Value<'_> {
    let (variable, observation) = if release.supports_extended_variable_count() {
        parse_v119_long_string_ref(column_bytes, byte_order)
    } else if release.supports_extended_observation_count() {
        parse_v118_long_string_ref(column_bytes, byte_order)
    } else {
        parse_classic_long_string_ref(column_bytes, byte_order)
    };
    let long_string_ref = LongStringRef::new(variable, observation);
    Value::LongStringRef(long_string_ref)
}

/// Format 118: `v` = u16 (2 bytes), `o` = u48 (6 bytes).
///
/// The 48-bit observation is stored at `column_bytes[2..8]` in the
/// file's byte order. Widening it to `u64` means padding with two
/// zero bytes — but *where* the padding lands depends on the byte
/// order, because LE and BE place their most-significant byte at
/// opposite ends of the 8-byte window.
///
/// - BE: most-significant byte is at index 0 → pad the **high** end,
///   i.e. put `[0, 0, cb[2..8]]`. `cb[2]` is the u48's `MSByte`.
/// - LE: most-significant byte is at index 7 → pad the **high** end
///   too, but the high end is now at indices 6–7 → put
///   `[cb[2..8], 0, 0]`. `cb[2]` is the u48's `LSByte`.
///
/// Matches `ReadStat`'s `read_data_row` strL handling (see
/// `src/stata/readstat_dta_read.c` for the LE branch that reads
/// `cb[2]` as the `LSByte` of `o`).
fn parse_v118_long_string_ref(column_bytes: &[u8], byte_order: ByteOrder) -> (u32, u64) {
    let variable_index = byte_order.read_u16([column_bytes[0], column_bytes[1]]);
    let widened = match byte_order {
        ByteOrder::BigEndian => [
            0,
            0,
            column_bytes[2],
            column_bytes[3],
            column_bytes[4],
            column_bytes[5],
            column_bytes[6],
            column_bytes[7],
        ],
        ByteOrder::LittleEndian => [
            column_bytes[2],
            column_bytes[3],
            column_bytes[4],
            column_bytes[5],
            column_bytes[6],
            column_bytes[7],
            0,
            0,
        ],
    };
    let observation_index = byte_order.read_u64(widened);
    (u32::from(variable_index), observation_index)
}

/// Format 119+: `v` = u24 (3 bytes), `o` = u40 (5 bytes).
///
/// V119 widened the variable count to `u32` and rebalanced the
/// 8-byte strL ref accordingly: 3 bytes for `v` (up to ~16M
/// variables) and 5 bytes for `o` (up to ~1T observations). Each is
/// decoded in the file's byte order, with the high-end zero padding
/// placed symmetric to the V118 path — high end of the equivalent
/// `u32` / `u64`, which lands at different array indices for LE and
/// BE.
fn parse_v119_long_string_ref(column_bytes: &[u8], byte_order: ByteOrder) -> (u32, u64) {
    let variable_widened = match byte_order {
        ByteOrder::BigEndian => [0, column_bytes[0], column_bytes[1], column_bytes[2]],
        ByteOrder::LittleEndian => [column_bytes[0], column_bytes[1], column_bytes[2], 0],
    };
    let variable_index = byte_order.read_u32(variable_widened);
    let observation_widened = match byte_order {
        ByteOrder::BigEndian => [
            0,
            0,
            0,
            column_bytes[3],
            column_bytes[4],
            column_bytes[5],
            column_bytes[6],
            column_bytes[7],
        ],
        ByteOrder::LittleEndian => [
            column_bytes[3],
            column_bytes[4],
            column_bytes[5],
            column_bytes[6],
            column_bytes[7],
            0,
            0,
            0,
        ],
    };
    let observation_index = byte_order.read_u64(observation_widened);
    (variable_index, observation_index)
}

/// Format 117: `v` = u32 (4 bytes), `o` = u32 (4 bytes).
fn parse_classic_long_string_ref(column_bytes: &[u8], byte_order: ByteOrder) -> (u32, u64) {
    let variable_index = byte_order.read_u32([
        column_bytes[0],
        column_bytes[1],
        column_bytes[2],
        column_bytes[3],
    ]);
    let observation_index = byte_order.read_u32([
        column_bytes[4],
        column_bytes[5],
        column_bytes[6],
        column_bytes[7],
    ]);
    (variable_index, u64::from(observation_index))
}

fn unrecognized_value() -> DtaError {
    DtaError::io(
        Section::Records,
        std::io::Error::other("unrecognized Stata missing value bit pattern"),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    // -- parse_v118_long_string_ref (V118) -----------------------------------

    #[test]
    fn parse_v118_long_string_ref_le() {
        // LE u48 at cb[2..8] with cb[2] = LSByte.
        // cb[2..8] = [0x01, 0x02, 0x03, 0x04, 0x05, 0x00]
        //   → observation = 0x01 | 0x02<<8 | 0x03<<16 | 0x04<<24
        //                 | 0x05<<32 | 0x00<<40
        //                 = 0x0000_0005_0403_0201
        let bytes = [
            0x01, 0x02, // variable = 0x0201 (LE u16)
            0x01, 0x02, 0x03, 0x04, 0x05, 0x00,
        ];
        let (variable, observation) = parse_v118_long_string_ref(&bytes, ByteOrder::LittleEndian);
        assert_eq!(variable, 0x0201);
        assert_eq!(observation, 0x0000_0005_0403_0201);
    }

    #[test]
    fn parse_v118_long_string_ref_le_with_top_bits() {
        // LE u48: cb[2..8] = [0x01..0x06] → observation = 0x0000_0605_0403_0201.
        let bytes = [0x10, 0x20, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06];
        let (variable, observation) = parse_v118_long_string_ref(&bytes, ByteOrder::LittleEndian);
        assert_eq!(variable, 0x2010);
        assert_eq!(observation, 0x0000_0605_0403_0201);
    }

    #[test]
    fn parse_v118_long_string_ref_be() {
        // BE u48 at cb[2..8] with cb[2] = MSByte.
        // cb[2..8] = [0x03, 0x04, 0x05, 0x06, 0x07, 0x08]
        //   → observation = 0x03<<40 | 0x04<<32 | 0x05<<24
        //                 | 0x06<<16 | 0x07<<8 | 0x08
        //                 = 0x0000_0304_0506_0708
        let bytes = [0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08];
        let (variable, observation) = parse_v118_long_string_ref(&bytes, ByteOrder::BigEndian);
        assert_eq!(variable, 0x0102);
        assert_eq!(observation, 0x0000_0304_0506_0708);
    }

    #[test]
    fn parse_v118_long_string_ref_le_observation_one() {
        // Regression test for the old bug where observation came out
        // as (true_o << 16). For true_o = 1, the old reader produced
        // 0x1_0000.
        let bytes = [
            0x00, 0x00, // variable = 0
            0x01, 0x00, 0x00, 0x00, 0x00, 0x00, // observation = 1 (LE u48)
        ];
        let (_, observation) = parse_v118_long_string_ref(&bytes, ByteOrder::LittleEndian);
        assert_eq!(observation, 1);
    }

    // -- parse_classic_long_string_ref (V117) --------------------------------

    #[test]
    fn parse_classic_long_string_ref_le() {
        let bytes = [
            0x01, 0x02, 0x03, 0x04, // variable = 0x0403_0201 (LE u32)
            0x05, 0x06, 0x07, 0x08, // observation = 0x0807_0605 (LE u32)
        ];
        let (variable, observation) =
            parse_classic_long_string_ref(&bytes, ByteOrder::LittleEndian);
        assert_eq!(variable, 0x0403_0201);
        assert_eq!(observation, 0x0807_0605);
    }

    #[test]
    fn parse_classic_long_string_ref_be() {
        let bytes = [
            0x01, 0x02, 0x03, 0x04, // variable = 0x0102_0304 (BE u32)
            0x05, 0x06, 0x07, 0x08, // observation = 0x0506_0708 (BE u32)
        ];
        let (variable, observation) = parse_classic_long_string_ref(&bytes, ByteOrder::BigEndian);
        assert_eq!(variable, 0x0102_0304);
        assert_eq!(observation, 0x0506_0708);
    }

    // -- parse_v119_long_string_ref (V119+) ----------------------------------

    #[test]
    fn parse_v119_long_string_ref_le() {
        // Bytes captured from pandas's stata12_119.dta (V119 LE):
        // [03 00 00 01 00 00 00 00] should resolve to (variable=3, observation=1).
        let bytes = [0x03, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00];
        let (variable, observation) = parse_v119_long_string_ref(&bytes, ByteOrder::LittleEndian);
        assert_eq!(variable, 3);
        assert_eq!(observation, 1);
    }

    #[test]
    fn parse_v119_long_string_ref_be() {
        // Bytes captured from pandas's stata12_be_119.dta (V119 BE):
        // [00 00 03 00 00 00 00 01] should resolve to (variable=3, observation=1).
        let bytes = [0x00, 0x00, 0x03, 0x00, 0x00, 0x00, 0x00, 0x01];
        let (variable, observation) = parse_v119_long_string_ref(&bytes, ByteOrder::BigEndian);
        assert_eq!(variable, 3);
        assert_eq!(observation, 1);
    }

    #[test]
    fn parse_v119_long_string_ref_le_full_width() {
        // Exercises every payload byte to confirm u24 v + u40 o
        // boundary. cb[0..3] = LE u24, cb[3..8] = LE u40.
        let bytes = [0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08];
        let (variable, observation) = parse_v119_long_string_ref(&bytes, ByteOrder::LittleEndian);
        assert_eq!(variable, 0x0003_0201);
        assert_eq!(observation, 0x0000_0008_0706_0504);
    }

    #[test]
    fn parse_v119_long_string_ref_be_full_width() {
        // BE counterpart: cb[0..3] = BE u24, cb[3..8] = BE u40.
        let bytes = [0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08];
        let (variable, observation) = parse_v119_long_string_ref(&bytes, ByteOrder::BigEndian);
        assert_eq!(variable, 0x0001_0203);
        assert_eq!(observation, 0x0000_0004_0506_0708);
    }
}
