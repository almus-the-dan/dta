use core::fmt;

use encoding_rs::Encoding;

use super::dta_error::FormatErrorKind;

/// DTA format version (release number).
///
/// Each variant corresponds to a `ds_format` byte value found in the
/// file header. The supported range is 104–119, matching `ReadStat`.
///
/// Version-specific field sizes and feature queries are exposed as
/// methods so that callers can dispatch on the release without
/// hard-coding version thresholds.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[repr(u8)]
pub enum Release {
    /// Stata format 104.
    V104 = 104,
    /// Stata format 105. Adds timestamps and expansion fields.
    V105 = 105,
    /// Stata format 106.
    V106 = 106,
    /// Stata format 107.
    V107 = 107,
    /// Stata format 108. Variable/dataset labels grow to 81 bytes.
    V108 = 108,
    /// Stata format 109.
    V109 = 109,
    /// Stata format 110. Variable names grow to 33 bytes.
    V110 = 110,
    /// Stata format 111. Type codes switch from ASCII to 0xFB–0xFF.
    V111 = 111,
    /// Stata format 112.
    V112 = 112,
    /// Stata format 113. Tagged missing values (.a–.z).
    V113 = 113,
    /// Stata format 114. Format list entry grows to 49 bytes.
    V114 = 114,
    /// Stata format 115.
    V115 = 115,
    /// Stata format 116.
    V116 = 116,
    /// Stata format 117. XML-tagged sections, strL support.
    V117 = 117,
    /// Stata format 118. UTF-8, 64-bit observation count, longer names.
    V118 = 118,
    /// Stata format 119. 32-bit variable count.
    V119 = 119,
}

impl TryFrom<u8> for Release {
    type Error = FormatErrorKind;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            104 => Ok(Self::V104),
            105 => Ok(Self::V105),
            106 => Ok(Self::V106),
            107 => Ok(Self::V107),
            108 => Ok(Self::V108),
            109 => Ok(Self::V109),
            110 => Ok(Self::V110),
            111 => Ok(Self::V111),
            112 => Ok(Self::V112),
            113 => Ok(Self::V113),
            114 => Ok(Self::V114),
            115 => Ok(Self::V115),
            116 => Ok(Self::V116),
            117 => Ok(Self::V117),
            118 => Ok(Self::V118),
            119 => Ok(Self::V119),
            _ => Err(FormatErrorKind::UnsupportedRelease { release: value }),
        }
    }
}

impl fmt::Display for Release {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.number())
    }
}

// ---------------------------------------------------------------------------
// Header layout queries
// ---------------------------------------------------------------------------

impl Release {
    /// The raw format version number (e.g., 117).
    #[must_use]
    pub(crate) fn number(self) -> u8 {
        // SAFETY: #[repr(u8)] guarantees the discriminant fits in u8.
        // This is the only place we use `as` for this enum.
        self as u8
    }

    /// Whether the file uses XML-style section tags (format 117+).
    #[must_use]
    pub(crate) fn is_xml_like(self) -> bool {
        self >= Self::V117
    }

    /// Default character encoding for this format version.
    ///
    /// Formats before 118 default to Windows-1252 (the most common
    /// system encoding where Stata ran). Formats 118+ are UTF-8.
    #[must_use]
    pub(crate) fn default_encoding(self) -> &'static Encoding {
        if self >= Self::V118 {
            encoding_rs::UTF_8
        } else {
            encoding_rs::WINDOWS_1252
        }
    }

    /// Fixed-length dataset label field size (binary formats only).
    ///
    /// For XML formats (117+), the label has a length prefix instead;
    /// see [`data_label_len_width`](Self::data_label_len_width).
    #[must_use]
    pub(crate) fn dataset_label_len(self) -> usize {
        if self < Self::V108 { 32 } else { 81 }
    }

    /// Width (in bytes) of the dataset-label length prefix for XML
    /// formats: 1 for format 117, 2 for 118+.
    ///
    /// Returns 0 for binary formats (no length prefix).
    #[must_use]
    pub(crate) fn data_label_len_width(self) -> usize {
        if self >= Self::V118 {
            2
        } else {
            usize::from(self >= Self::V117)
        }
    }

    /// Whether this format stores a timestamp in the header.
    #[must_use]
    pub(crate) fn has_timestamp(self) -> bool {
        self >= Self::V105
    }

    /// Fixed-length timestamp field size for binary formats.
    ///
    /// Returns 0 for format 104 (no timestamp) and 18 for 105–116.
    /// For XML formats, the timestamp has a 1-byte length prefix.
    #[must_use]
    pub(crate) fn timestamp_len(self) -> usize {
        if self < Self::V105 { 0 } else { 18 }
    }

    /// Whether the variable count is stored as `u32` (format 119).
    /// Earlier formats use `u16`.
    #[must_use]
    pub(crate) fn supports_extended_variable_count(self) -> bool {
        self >= Self::V119
    }

    /// Whether the observation count is stored as `u64` (format 118+).
    /// Earlier formats use `u32`.
    #[must_use]
    pub(crate) fn supports_extended_observation_count(self) -> bool {
        self >= Self::V118
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // -- TryFrom<u8> ---------------------------------------------------------

    #[test]
    fn try_from_valid_range() {
        for v in 104..=119 {
            let r = Release::try_from(v).unwrap();
            assert_eq!(r.number(), v);
        }
    }

    #[test]
    fn try_from_below_range() {
        assert_eq!(
            Release::try_from(103),
            Err(FormatErrorKind::UnsupportedRelease { release: 103 }),
        );
    }

    #[test]
    fn try_from_above_range() {
        assert_eq!(
            Release::try_from(120),
            Err(FormatErrorKind::UnsupportedRelease { release: 120 }),
        );
    }

    // -- Display -------------------------------------------------------------

    #[test]
    fn display() {
        assert_eq!(Release::V104.to_string(), "104");
        assert_eq!(Release::V119.to_string(), "119");
    }

    // -- Ordering ------------------------------------------------------------

    #[test]
    fn ordering() {
        assert!(Release::V104 < Release::V119);
        assert!(Release::V117 < Release::V118);
        assert_eq!(Release::V115, Release::V115);
    }

    // -- is_xml --------------------------------------------------------------

    #[test]
    fn is_xml_boundary() {
        assert!(!Release::V116.is_xml_like());
        assert!(Release::V117.is_xml_like());
        assert!(Release::V118.is_xml_like());
        assert!(Release::V119.is_xml_like());
    }

    // -- default_encoding ----------------------------------------------------

    #[test]
    fn default_encoding_pre_118() {
        assert_eq!(Release::V104.default_encoding(), encoding_rs::WINDOWS_1252);
        assert_eq!(Release::V117.default_encoding(), encoding_rs::WINDOWS_1252);
    }

    #[test]
    fn default_encoding_118_plus() {
        assert_eq!(Release::V118.default_encoding(), encoding_rs::UTF_8);
        assert_eq!(Release::V119.default_encoding(), encoding_rs::UTF_8);
    }

    // -- dataset_label_len ---------------------------------------------------

    #[test]
    fn dataset_label_len_boundary() {
        assert_eq!(Release::V107.dataset_label_len(), 32);
        assert_eq!(Release::V108.dataset_label_len(), 81);
        assert_eq!(Release::V116.dataset_label_len(), 81);
    }

    // -- data_label_len_width ------------------------------------------------

    #[test]
    fn data_label_len_width_binary() {
        assert_eq!(Release::V116.data_label_len_width(), 0);
    }

    #[test]
    fn data_label_len_width_117() {
        assert_eq!(Release::V117.data_label_len_width(), 1);
    }

    #[test]
    fn data_label_len_width_118_plus() {
        assert_eq!(Release::V118.data_label_len_width(), 2);
        assert_eq!(Release::V119.data_label_len_width(), 2);
    }

    // -- timestamp_len -------------------------------------------------------

    #[test]
    fn timestamp_len_v104() {
        assert_eq!(Release::V104.timestamp_len(), 0);
        assert!(!Release::V104.has_timestamp());
    }

    #[test]
    fn timestamp_len_v105_plus() {
        assert_eq!(Release::V105.timestamp_len(), 18);
        assert!(Release::V105.has_timestamp());
        assert_eq!(Release::V116.timestamp_len(), 18);
    }

    // -- supports_long_variable_count ----------------------------------------

    #[test]
    fn long_variable_count_pre_119() {
        assert!(!Release::V118.supports_extended_variable_count());
    }

    #[test]
    fn long_variable_count_119() {
        assert!(Release::V119.supports_extended_variable_count());
    }

    // -- supports_long_observation_count -------------------------------------

    #[test]
    fn long_observation_count_pre_118() {
        assert!(!Release::V117.supports_extended_observation_count());
    }

    #[test]
    fn long_observation_count_118_plus() {
        assert!(Release::V118.supports_extended_observation_count());
        assert!(Release::V119.supports_extended_observation_count());
    }
}
