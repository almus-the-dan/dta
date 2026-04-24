use std::borrow::Cow;

use encoding_rs::Encoding;

use super::string_decoding::decode_null_terminated;

/// Type byte stored in a GSO block header, classifying the payload
/// as binary bytes or as text that can be decoded using the file's
/// encoding.
///
/// The DTA spec defines exactly two values:
/// `0x81` (`Binary`) and `0x82` (`Text`). Other values are not
/// documented, and [`from_byte`](Self::from_byte) returns `None` for
/// them — callers decide whether to error or to default-classify.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub(crate) enum GsoType {
    /// Binary payload. Typically, this cannot be decoded as a string.
    Binary = 0x81,
    /// Text payload. Decoded with the file's active encoding.
    Text = 0x82,
}

impl GsoType {
    /// Raw byte written to the file.
    #[must_use]
    #[inline]
    pub(crate) fn to_byte(self) -> u8 {
        // SAFETY: `#[repr(u8)]` guarantees the discriminant fits in
        // a `u8`. Mirrors the same pattern used by
        // [`Release::to_byte`](super::release::Release) and
        // [`ExpansionFieldType::to_byte`](super::characteristic::ExpansionFieldType).
        self as u8
    }

    /// Classifies a raw type byte. Returns `None` for values outside
    /// the two defined by the DTA spec.
    #[must_use]
    pub(crate) fn from_byte(byte: u8) -> Option<Self> {
        match byte {
            0x81 => Some(Self::Binary),
            0x82 => Some(Self::Text),
            _ => None,
        }
    }
}

/// A long string (strL / GSO) entry from the DTA file.
///
/// Each entry is keyed by a `(variable, observation)` pair,
/// matching the [`LongStringRef`](super::long_string_ref::LongStringRef)
/// encountered in the data section. The `observation` component is the
/// one-based index of the first observation where the string content
/// appeared, serving as a deduplication key rather than a row address.
///
/// The raw bytes are stored as-is from the file. Use [`data`](Self::data)
/// for raw access or [`data_str`](Self::data_str) to decode using a
/// caller-supplied encoding — typically the one reported by the
/// reader that produced this entry (see the `encoding()` accessor on
/// each reader).
#[derive(Debug, Clone)]
pub struct LongString<'a> {
    variable: u32,
    observation: u64,
    binary: bool,
    data: Cow<'a, [u8]>,
}

impl<'a> LongString<'a> {
    #[must_use]
    pub(crate) fn new(variable: u32, observation: u64, binary: bool, data: Cow<'a, [u8]>) -> Self {
        Self {
            variable,
            observation,
            binary,
            data,
        }
    }

    /// One-based variable index.
    #[must_use]
    #[inline]
    pub fn variable(&self) -> u32 {
        self.variable
    }

    /// One-based index of the first observation where this string content
    /// appeared. Acts as part of the `(variable, observation)` lookup key,
    /// not necessarily the current row.
    #[must_use]
    #[inline]
    pub fn observation(&self) -> u64 {
        self.observation
    }

    /// Whether this entry was stored as binary (GSO type `0x81`)
    /// rather than ASCII text (`0x82`). Binary entries typically
    /// cannot be decoded as strings via [`data_str`](Self::data_str).
    #[must_use]
    #[inline]
    pub fn is_binary(&self) -> bool {
        self.binary
    }

    /// The raw bytes from the GSO entry, without any decoding or
    /// null-terminator stripping.
    #[must_use]
    #[inline]
    pub fn data(&self) -> &[u8] {
        &self.data
    }

    /// Decodes the entry as a string using the given encoding,
    /// stripping any trailing null terminator.
    ///
    /// Pass the encoding reported by the reader (or writer) that
    /// produced this entry — for example `reader.encoding()`.
    ///
    /// Returns `None` if the bytes are not valid in the given
    /// encoding.
    #[must_use]
    pub fn data_str(&self, encoding: &'static Encoding) -> Option<Cow<'_, str>> {
        decode_null_terminated(&self.data, encoding)
    }
}
