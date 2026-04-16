use std::borrow::Cow;

use encoding_rs::Encoding;

/// A long string (strL / GSO) entry from the DTA file.
///
/// Each entry is keyed by a `(variable, observation)` pair,
/// matching the [`LongStringRef`](super::long_string_ref::LongStringRef)
/// encountered in the data section. The `observation` component is the
/// one-based index of the first observation where the string content
/// appeared, serving as a deduplication key rather than a row address.
///
/// The raw bytes are stored as-is from the file. Use [`value_bytes`](Self::value_bytes)
/// for raw access or [`value_str`](Self::value_str) to decode using
/// the file's encoding.
#[derive(Debug, Clone)]
pub struct LongString<'a> {
    variable: u32,
    observation: u64,
    binary: bool,
    data: Cow<'a, [u8]>,
    encoding: &'static Encoding,
}

impl<'a> LongString<'a> {
    #[must_use]
    pub(crate) fn new(
        variable: u32,
        observation: u64,
        binary: bool,
        data: Cow<'a, [u8]>,
        encoding: &'static Encoding,
    ) -> Self {
        Self {
            variable,
            observation,
            binary,
            data,
            encoding,
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
    /// cannot be decoded as strings via [`value_str`](Self::value_str).
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

    /// Decodes the entry as a string using the file's encoding,
    /// stripping any trailing null terminator.
    ///
    /// Returns `None` if the bytes are not valid in the file's
    /// encoding.
    #[must_use]
    pub fn data_str(&self) -> Option<Cow<'_, str>> {
        let end = self
            .data
            .iter()
            .position(|&b| b == 0)
            .unwrap_or(self.data.len());
        self.encoding
            .decode_without_bom_handling_and_without_replacement(&self.data[..end])
    }
}
