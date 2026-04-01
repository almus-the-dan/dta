use core::fmt;

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
// TryFrom<&str> — v118+ string tags
// ---------------------------------------------------------------------------

/// Error returned when a string is not a valid byte-order tag.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct InvalidByteOrderString;

impl fmt::Display for InvalidByteOrderString {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("expected \"MSF\" or \"LSF\"")
    }
}

impl std::error::Error for InvalidByteOrderString {}

impl TryFrom<&str> for ByteOrder {
    type Error = InvalidByteOrderString;

    fn try_from(s: &str) -> Result<Self, Self::Error> {
        match s {
            "MSF" => Ok(Self::BigEndian),
            "LSF" => Ok(Self::LittleEndian),
            _ => Err(InvalidByteOrderString),
        }
    }
}

// ---------------------------------------------------------------------------
// TryFrom<u8> — v113–117 binary representation
// ---------------------------------------------------------------------------

/// Error returned when a byte is not a valid byte-order code.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct InvalidByteOrderByte(pub u8);

impl fmt::Display for InvalidByteOrderByte {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "expected 0x01 or 0x02, got {:#04X}", self.0)
    }
}

impl std::error::Error for InvalidByteOrderByte {}

impl TryFrom<u8> for ByteOrder {
    type Error = InvalidByteOrderByte;

    fn try_from(b: u8) -> Result<Self, Self::Error> {
        match b {
            0x01 => Ok(Self::BigEndian),
            0x02 => Ok(Self::LittleEndian),
            _ => Err(InvalidByteOrderByte(b)),
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
    fn try_from_str_msf() {
        assert_eq!(ByteOrder::try_from("MSF"), Ok(ByteOrder::BigEndian));
    }

    #[test]
    fn try_from_str_lsf() {
        assert_eq!(ByteOrder::try_from("LSF"), Ok(ByteOrder::LittleEndian));
    }

    #[test]
    fn try_from_str_invalid() {
        assert!(ByteOrder::try_from("XYZ").is_err());
    }

    #[test]
    fn try_from_u8_hilo() {
        assert_eq!(ByteOrder::try_from(0x01_u8), Ok(ByteOrder::BigEndian));
    }

    #[test]
    fn try_from_u8_lohi() {
        assert_eq!(ByteOrder::try_from(0x02_u8), Ok(ByteOrder::LittleEndian));
    }

    #[test]
    fn try_from_u8_invalid() {
        assert_eq!(
            ByteOrder::try_from(0x00_u8),
            Err(InvalidByteOrderByte(0x00))
        );
    }
}
