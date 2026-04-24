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

/// Payload of a long-string entry — either a text blob (to be
/// decoded with the file's encoding) or raw binary bytes.
///
/// Both variants wrap `Cow<'a, [u8]>` so the data can be borrowed
/// directly from a reader's buffer on the happy path. Use the
/// [`From<&str>`](#impl-From<%26str>-for-LongStringContent<'a>) impl
/// for ergonomic text construction; binary payloads need the
/// explicit `Binary(...)` variant to avoid silently classifying
/// arbitrary byte slices as binary.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LongStringContent<'a> {
    /// Text payload (GSO type `0x82`). Decode with
    /// [`LongString::data_str`] and the file's encoding.
    Text(Cow<'a, [u8]>),
    /// Binary payload (GSO type `0x81`). Treat as opaque bytes.
    Binary(Cow<'a, [u8]>),
}

impl LongStringContent<'_> {
    /// Raw bytes, regardless of variant.
    #[must_use]
    #[inline]
    pub fn data(&self) -> &[u8] {
        match self {
            Self::Text(data) | Self::Binary(data) => data,
        }
    }

    /// `true` when this is a [`Text`](Self::Text) payload.
    #[must_use]
    #[inline]
    pub fn is_text(&self) -> bool {
        matches!(self, Self::Text(_))
    }

    /// `true` when this is a [`Binary`](Self::Binary) payload.
    #[must_use]
    #[inline]
    pub fn is_binary(&self) -> bool {
        matches!(self, Self::Binary(_))
    }
}

impl<'a> From<&'a str> for LongStringContent<'a> {
    /// Borrows the string's UTF-8 bytes as a [`Text`](Self::Text)
    /// payload. No `From<&[u8]>` is provided — binary payloads must
    /// be constructed explicitly so that arbitrary byte slices
    /// aren't silently misclassified.
    fn from(s: &'a str) -> Self {
        Self::Text(Cow::Borrowed(s.as_bytes()))
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
/// The payload is held as a [`LongStringContent`] so the
/// text-vs-binary distinction is encoded in the type. Use
/// [`data`](Self::data) for raw byte access regardless of variant,
/// [`content`](Self::content) to match on the variant, or
/// [`data_str`](Self::data_str) to decode text payloads with a
/// caller-supplied encoding.
#[derive(Debug, Clone)]
pub struct LongString<'a> {
    variable: u32,
    observation: u64,
    content: LongStringContent<'a>,
}

impl<'a> LongString<'a> {
    #[must_use]
    pub(crate) fn new(variable: u32, observation: u64, content: LongStringContent<'a>) -> Self {
        Self {
            variable,
            observation,
            content,
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

    /// The payload, as a [`LongStringContent`] enum exposing both
    /// the text/binary tag and the underlying bytes.
    #[must_use]
    #[inline]
    pub fn content(&self) -> &LongStringContent<'a> {
        &self.content
    }

    /// Consumes the entry and returns its [`LongStringContent`],
    /// avoiding a clone when the caller no longer needs the
    /// surrounding variable/observation metadata.
    #[must_use]
    #[inline]
    pub fn into_content(self) -> LongStringContent<'a> {
        self.content
    }

    /// `true` when this entry's payload is text (GSO type `0x82`).
    #[must_use]
    #[inline]
    pub fn is_text(&self) -> bool {
        self.content.is_text()
    }

    /// `true` when this entry's payload is binary (GSO type `0x81`).
    /// Binary entries typically cannot be decoded as strings via
    /// [`data_str`](Self::data_str).
    #[must_use]
    #[inline]
    pub fn is_binary(&self) -> bool {
        self.content.is_binary()
    }

    /// The raw bytes from the GSO entry, without any decoding or
    /// null-terminator stripping — identical to
    /// `self.content().data()`.
    #[must_use]
    #[inline]
    pub fn data(&self) -> &[u8] {
        self.content.data()
    }

    /// Decodes the entry as a string using the given encoding,
    /// stripping any trailing null terminator.
    ///
    /// Pass the encoding reported by the reader (or writer) that
    /// produced this entry — for example, `reader.encoding()`.
    ///
    /// Returns `None` if the bytes are not valid in the given
    /// encoding. Meaningful primarily for [`Text`](LongStringContent::Text)
    /// payloads; binary data will typically return `None` or garbage.
    #[must_use]
    pub fn data_str(&self, encoding: &'static Encoding) -> Option<Cow<'_, str>> {
        decode_null_terminated(self.content.data(), encoding)
    }
}
