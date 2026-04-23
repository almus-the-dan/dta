//! Shared byte-level string decoding used by the sync and async
//! reader state machinery. Kept here (rather than on the state types
//! themselves) so both flavors can decode fixed-width null-terminated
//! fields with identical error reporting without duplicating the
//! decode logic.

use std::borrow::Cow;

use encoding_rs::Encoding;

use super::dta_error::{DtaError, Field, FormatErrorKind, Result, Section};

/// Decodes a null-terminated byte buffer using the given encoding,
/// returning a borrowed `&str` when possible.
///
/// Callers shape the error type they need: this helper just answers
/// "here are the decoded characters up to the first null, or `None`
/// if the bytes are not valid in the encoding". Use [`find_null`] +
/// [`Cow::into_owned`] at the call site when an owned string is
/// required.
///
/// # UTF-8 fast path
///
/// When `encoding` is UTF-8 we bypass encoding_rs's dispatch and
/// validate directly with [`std::str::from_utf8`]. This is the
/// common modern case (V118+ files) and lets the callers avoid the
/// extra indirection through encoding_rs's generic decode path.
#[inline]
pub(super) fn decode_null_terminated<'a>(
    buffer: &'a [u8],
    encoding: &'static Encoding,
) -> Option<Cow<'a, str>> {
    let end = find_null(buffer);
    let bytes = &buffer[..end];
    if encoding == encoding_rs::UTF_8 {
        return std::str::from_utf8(bytes).ok().map(Cow::Borrowed);
    }
    encoding.decode_without_bom_handling_and_without_replacement(bytes)
}

/// Decodes a null-terminated fixed-width string buffer using the
/// given encoding.
///
/// Both the sync `ReaderState::read_fixed_string` and its async
/// equivalent handle the I/O and hand the filled buffer (plus the
/// byte position it was read at, for error reporting) to this helper.
pub(super) fn decode_fixed_string(
    buffer: &[u8],
    encoding: &'static Encoding,
    section: Section,
    field: Field,
    position: u64,
) -> Result<String> {
    decode_null_terminated(buffer, encoding)
        .map(Cow::into_owned)
        .ok_or_else(|| {
            DtaError::format(
                section,
                position,
                FormatErrorKind::InvalidEncoding { field },
            )
        })
}

/// Returns the index of the first zero byte in `data`, or `data.len()`
/// if none is present.
///
/// Scans the buffer in `usize`-wide chunks using the classic
/// "has-zero-byte" bit trick (`(x - 0x0101…01) & !x & 0x8080…80`),
/// falling back to byte-at-a-time only within the chunk that matches
/// and the final unaligned remainder. On the ASCII/UTF-8 happy path
/// — short string followed by a run of zero padding — this processes
/// eight bytes per cycle on a 64-bit target.
#[inline]
fn find_null(data: &[u8]) -> usize {
    const WORD_SIZE: usize = size_of::<usize>();
    // Byte-splat constants: 0x0101…01 and 0x8080…80 at `usize` width.
    const LO: usize = usize::MAX / 0xFF;
    const HI: usize = LO << 7;

    let (chunks, remainder) = data.as_chunks::<WORD_SIZE>();
    for (chunk_idx, chunk) in chunks.iter().enumerate() {
        let word = usize::from_ne_bytes(*chunk);
        if word.wrapping_sub(LO) & !word & HI != 0 {
            for (byte_idx, &b) in chunk.iter().enumerate() {
                if b == 0 {
                    return chunk_idx * WORD_SIZE + byte_idx;
                }
            }
            // The has-zero-byte test fires iff the chunk contains at
            // least one zero, so the inner loop above always returns.
            unreachable!("has-zero test guarantees a zero byte in this chunk");
        }
    }

    let offset = chunks.len() * WORD_SIZE;
    for (i, &b) in remainder.iter().enumerate() {
        if b == 0 {
            return offset + i;
        }
    }
    data.len()
}

#[cfg(test)]
mod tests {
    use super::{decode_null_terminated, find_null};

    #[test]
    fn find_null_empty_returns_zero() {
        assert_eq!(find_null(b""), 0);
    }

    #[test]
    fn find_null_all_nonzero_returns_len() {
        assert_eq!(find_null(b"abc"), 3);
        assert_eq!(find_null(&[0xFFu8; 33]), 33);
    }

    #[test]
    fn find_null_zero_at_start() {
        assert_eq!(find_null(b"\0abc"), 0);
    }

    #[test]
    fn find_null_zero_at_end_of_first_chunk() {
        // On a 64-bit target the first chunk is 8 bytes; put the zero
        // at index 7 to hit the chunked path with a zero at the last
        // byte of the chunk.
        assert_eq!(find_null(b"abcdefg\0"), 7);
    }

    #[test]
    fn find_null_zero_in_remainder() {
        // 9 bytes -> one chunk of 8 (all nonzero) plus 1-byte remainder.
        assert_eq!(find_null(b"abcdefgh\0"), 8);
    }

    #[test]
    fn find_null_zero_in_second_chunk() {
        // Two full 8-byte chunks; zero in the second one at offset 3.
        let mut buf = [b'x'; 16];
        buf[11] = 0;
        assert_eq!(find_null(&buf), 11);
    }

    #[test]
    fn find_null_zero_in_remainder_after_two_chunks() {
        let mut buf = [b'x'; 18];
        buf[17] = 0;
        assert_eq!(find_null(&buf), 17);
    }

    #[test]
    fn find_null_typical_fixed_string_slot() {
        // 32-byte slot with short string + zero padding.
        let mut buf = [0u8; 32];
        buf[..5].copy_from_slice(b"hello");
        assert_eq!(find_null(&buf), 5);
    }

    #[test]
    fn find_null_matches_naive_scan_exhaustively() {
        // Compare against the naive scan over a variety of lengths and
        // zero positions to catch any off-by-one bug in the chunking.
        for len in 0..40 {
            for zero_pos in 0..=len {
                let mut buf = vec![0xA5u8; len];
                if zero_pos < len {
                    buf[zero_pos] = 0;
                }
                let naive = buf.iter().position(|&b| b == 0).unwrap_or(buf.len());
                assert_eq!(find_null(&buf), naive, "len={len}, zero_pos={zero_pos}");
            }
        }
    }

    #[test]
    fn decode_null_terminated_utf8_borrows_ascii() {
        let mut buf = [0u8; 16];
        buf[..5].copy_from_slice(b"hello");
        let decoded = decode_null_terminated(&buf, encoding_rs::UTF_8).unwrap();
        assert_eq!(decoded, "hello");
        assert!(matches!(decoded, std::borrow::Cow::Borrowed(_)));
    }

    #[test]
    fn decode_null_terminated_utf8_borrows_multibyte() {
        let text = "日本語";
        let mut buf = vec![0u8; text.len() + 4];
        buf[..text.len()].copy_from_slice(text.as_bytes());
        let decoded = decode_null_terminated(&buf, encoding_rs::UTF_8).unwrap();
        assert_eq!(decoded, text);
        assert!(matches!(decoded, std::borrow::Cow::Borrowed(_)));
    }

    #[test]
    fn decode_null_terminated_utf8_rejects_invalid_bytes() {
        // 0xFF is not valid UTF-8.
        let buf = [0xFFu8, 0x00];
        assert!(decode_null_terminated(&buf, encoding_rs::UTF_8).is_none());
    }

    #[test]
    fn decode_null_terminated_windows1252_borrows_ascii() {
        let mut buf = [0u8; 16];
        buf[..5].copy_from_slice(b"hello");
        let decoded = decode_null_terminated(&buf, encoding_rs::WINDOWS_1252).unwrap();
        assert_eq!(decoded, "hello");
        assert!(matches!(decoded, std::borrow::Cow::Borrowed(_)));
    }

    #[test]
    fn decode_null_terminated_windows1252_owns_transcoded() {
        // 0xE9 is "é" in Windows-1252; the UTF-8 encoding requires two
        // bytes, so encoding_rs returns an owned buffer.
        let mut buf = [0u8; 8];
        buf[0] = 0xE9;
        let decoded = decode_null_terminated(&buf, encoding_rs::WINDOWS_1252).unwrap();
        assert_eq!(decoded, "é");
        assert!(matches!(decoded, std::borrow::Cow::Owned(_)));
    }
}
