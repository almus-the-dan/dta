use core::fmt;

use super::dta_error::FormatErrorKind;

/// Byte order (endianness) of values in a DTA file.
///
/// Formats 113–117 encode this as a single byte (`0x01` = big-endian,
/// `0x02` = little-endian). Formats 118–119 use the string tags
/// `"MSF"` (the most significant first) and `"LSF"` (the least significant first).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ByteOrder {
    /// Most-significant byte first (big-endian).
    BigEndian,
    /// Least-significant byte first (little-endian).
    LittleEndian,
}

// ---------------------------------------------------------------------------
// Display — uses the v118+ string representation (MSF / LSF)
// ---------------------------------------------------------------------------

impl fmt::Display for ByteOrder {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::BigEndian => f.write_str("MSF"),
            Self::LittleEndian => f.write_str("LSF"),
        }
    }
}

// ---------------------------------------------------------------------------
// Parsing helpers
// ---------------------------------------------------------------------------

impl ByteOrder {
    /// Parses a v118+ string tag (`"MSF"` or `"LSF"`).
    pub(crate) fn from_tag(s: &str) -> Result<Self, FormatErrorKind> {
        match s {
            "MSF" => Ok(Self::BigEndian),
            "LSF" => Ok(Self::LittleEndian),
            _ => Err(FormatErrorKind::InvalidByteOrderTag),
        }
    }

    /// Returns the binary byte-order code (`0x01` or `0x02`).
    #[must_use]
    #[inline]
    pub(crate) fn to_byte(self) -> u8 {
        match self {
            Self::BigEndian => 0x01,
            Self::LittleEndian => 0x02,
        }
    }

    /// Decodes a `u16` from a 2-byte array.
    #[must_use]
    pub(crate) fn read_u16(self, bytes: [u8; 2]) -> u16 {
        match self {
            Self::BigEndian => u16::from_be_bytes(bytes),
            Self::LittleEndian => u16::from_le_bytes(bytes),
        }
    }

    /// Decodes a `u32` from a 4-byte array.
    #[must_use]
    pub(crate) fn read_u32(self, bytes: [u8; 4]) -> u32 {
        match self {
            Self::BigEndian => u32::from_be_bytes(bytes),
            Self::LittleEndian => u32::from_le_bytes(bytes),
        }
    }

    /// Decodes a `u64` from an 8-byte array.
    #[must_use]
    pub(crate) fn read_u64(self, bytes: [u8; 8]) -> u64 {
        match self {
            Self::BigEndian => u64::from_be_bytes(bytes),
            Self::LittleEndian => u64::from_le_bytes(bytes),
        }
    }

    /// Decodes an `i32` from a 4-byte array.
    #[must_use]
    pub(crate) fn read_i32(self, bytes: [u8; 4]) -> i32 {
        match self {
            Self::BigEndian => i32::from_be_bytes(bytes),
            Self::LittleEndian => i32::from_le_bytes(bytes),
        }
    }

    /// Encodes an `i32` into a 4-byte array.
    #[must_use]
    pub(crate) fn write_i32(self, value: i32) -> [u8; 4] {
        match self {
            Self::BigEndian => value.to_be_bytes(),
            Self::LittleEndian => value.to_le_bytes(),
        }
    }

    /// Decodes an `f32` from a 4-byte array.
    #[must_use]
    pub(crate) fn read_f32(self, bytes: [u8; 4]) -> f32 {
        match self {
            Self::BigEndian => f32::from_be_bytes(bytes),
            Self::LittleEndian => f32::from_le_bytes(bytes),
        }
    }

    /// Decodes an `f64` from an 8-byte array.
    #[must_use]
    pub(crate) fn read_f64(self, bytes: [u8; 8]) -> f64 {
        match self {
            Self::BigEndian => f64::from_be_bytes(bytes),
            Self::LittleEndian => f64::from_le_bytes(bytes),
        }
    }

    /// Encodes a `u16` into a 2-byte array.
    #[must_use]
    pub(crate) fn write_u16(self, value: u16) -> [u8; 2] {
        match self {
            Self::BigEndian => value.to_be_bytes(),
            Self::LittleEndian => value.to_le_bytes(),
        }
    }

    /// Encodes a `u32` into a 4-byte array.
    #[must_use]
    pub(crate) fn write_u32(self, value: u32) -> [u8; 4] {
        match self {
            Self::BigEndian => value.to_be_bytes(),
            Self::LittleEndian => value.to_le_bytes(),
        }
    }

    /// Encodes a `u64` into an 8-byte array.
    #[must_use]
    pub(crate) fn write_u64(self, value: u64) -> [u8; 8] {
        match self {
            Self::BigEndian => value.to_be_bytes(),
            Self::LittleEndian => value.to_le_bytes(),
        }
    }

    /// Parses a v113–117 binary byte-order code (`0x01` or `0x02`).
    pub(crate) fn from_byte(b: u8) -> Result<Self, FormatErrorKind> {
        match b {
            0x01 => Ok(Self::BigEndian),
            0x02 => Ok(Self::LittleEndian),
            _ => Err(FormatErrorKind::InvalidByteOrder { byte: b }),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn display_big_endian() {
        assert_eq!(ByteOrder::BigEndian.to_string(), "MSF");
    }

    #[test]
    fn display_little_endian() {
        assert_eq!(ByteOrder::LittleEndian.to_string(), "LSF");
    }

    #[test]
    fn from_tag_msf() {
        assert_eq!(ByteOrder::from_tag("MSF"), Ok(ByteOrder::BigEndian));
    }

    #[test]
    fn from_tag_lsf() {
        assert_eq!(ByteOrder::from_tag("LSF"), Ok(ByteOrder::LittleEndian));
    }

    #[test]
    fn from_tag_invalid() {
        assert_eq!(
            ByteOrder::from_tag("XYZ"),
            Err(FormatErrorKind::InvalidByteOrderTag),
        );
    }

    #[test]
    fn from_byte_big_endian() {
        assert_eq!(ByteOrder::from_byte(0x01), Ok(ByteOrder::BigEndian));
    }

    #[test]
    fn from_byte_little_endian() {
        assert_eq!(ByteOrder::from_byte(0x02), Ok(ByteOrder::LittleEndian));
    }

    #[test]
    fn from_byte_invalid() {
        assert_eq!(
            ByteOrder::from_byte(0x00),
            Err(FormatErrorKind::InvalidByteOrder { byte: 0x00 }),
        );
    }
}
