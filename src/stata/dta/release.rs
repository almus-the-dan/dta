use core::fmt;

use encoding_rs::Encoding;

use super::dta_error::FormatErrorKind;

/// DTA format version (release number).
///
/// Each variant corresponds to a `ds_format` byte value found in the
/// file header. The supported range is 102–119, matching `ReadStat`.
///
/// Version-specific field sizes and feature queries are exposed as
/// methods so that callers can dispatch on the release without
/// hard-coding version thresholds.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[repr(u8)]
pub enum Release {
    /// Stata format 102. Stata 3 on DOS — no `byte` type, no
    /// big-endian support, 30-byte dataset label, byteorder header
    /// byte is always `0x00`.
    V102 = 102,
    /// Stata format 103. Adds the `byte` type, big-endian support,
    /// and a 32-byte dataset label (matching V104+).
    V103 = 103,
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
            102 => Ok(Self::V102),
            103 => Ok(Self::V103),
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
        write!(f, "{}", self.to_byte())
    }
}

// ---------------------------------------------------------------------------
// Header layout queries
// ---------------------------------------------------------------------------

impl Release {
    /// The raw format version number (e.g., 117).
    #[must_use]
    pub(crate) fn to_byte(self) -> u8 {
        // SAFETY: #[repr(u8)] guarantees the discriminant fits in u8.
        // This is the only place we use `as` for this enum.
        self as u8
    }

    /// Whether the file uses XML-style section tags (format 117+).
    ///
    /// XML formats store a section map with absolute byte offsets for
    /// every section, so all post-schema seek operations are available
    /// immediately after schema reading. Binary formats require
    /// sequential reading through expansion fields before the data and
    /// value-label offsets are known.
    #[must_use]
    #[inline]
    pub fn is_xml_like(self) -> bool {
        self >= Self::V117
    }

    /// Whether the format supports long strings (strL) and has a
    /// `<strls>` section (format 117+).
    #[must_use]
    #[inline]
    pub fn supports_long_strings(self) -> bool {
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
    /// see [`supports_extended_dataset_label`](Self::supports_extended_dataset_label).
    #[must_use]
    pub(crate) fn dataset_label_len(self) -> usize {
        if self < Self::V108 { 32 } else { 81 }
    }

    /// Whether the XML dataset-label length prefix is stored as a
    /// `u16` (format 118+). Earlier XML formats (117) use a `u8`.
    ///
    /// Only meaningful for XML formats; binary formats do not write
    /// a length prefix for the dataset label.
    #[must_use]
    #[inline]
    pub(crate) fn supports_extended_dataset_label(self) -> bool {
        self >= Self::V118
    }

    /// Fixed-length timestamp field size for binary formats.
    ///
    /// Returns `None` for format 104 (no timestamp at all) and
    /// `Some(18)` for 105–116. For XML formats, the timestamp has a
    /// 1-byte length prefix, and this method is not consulted.
    #[must_use]
    pub(crate) fn timestamp_len(self) -> Option<usize> {
        if self < Self::V105 { None } else { Some(18) }
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

    /// Whether the binary-format observation count is stored as `u32`
    /// (V103+). V102 predates multi-million-row datasets and uses
    /// `u16`.
    ///
    /// Only meaningful for the binary-format header path (V102–V117).
    /// XML formats (V118+) dispatch on
    /// [`supports_extended_observation_count`](Self::supports_extended_observation_count).
    #[must_use]
    #[inline]
    pub(crate) fn supports_extended_binary_observation_count(self) -> bool {
        self >= Self::V103
    }

    /// Byte width of the binary-format observation count field.
    ///
    /// | Release | Width | Type |
    /// |---------|-------|------|
    /// | V102    | 2     | u16  |
    /// | V103–V117 | 4   | u32  |
    /// | V118+   | 8     | u64  |
    ///
    /// Not consulted for XML formats — those read the count from
    /// the `<N>` tag as a binary integer whose width is governed by
    /// [`supports_extended_observation_count`](Self::supports_extended_observation_count).
    #[must_use]
    pub(crate) fn binary_observation_count_width(self) -> usize {
        if self >= Self::V118 {
            8
        } else if self.supports_extended_binary_observation_count() {
            4
        } else {
            2
        }
    }

    /// Whether the format supports the `byte` storage type.
    ///
    /// V102 (Stata 3) had only `int`, `long`, `float`, `double`, and
    /// fixed-width strings. V103 added `byte` and it has been present
    /// ever since.
    #[must_use]
    #[inline]
    pub(crate) fn supports_byte_type(self) -> bool {
        self >= Self::V103
    }

    /// Whether the format supports tagged missing values (`.a`–`.z`),
    /// introduced in format 113.
    ///
    /// Earlier formats encode a single system missing value (`.`) only.
    /// Attempting to write a tagged missing to a pre-113 file returns
    /// a format error; attempting to read one is treated as a file
    /// corruption signal by the integer decoders (pre-113 integer
    /// sentinels have only one valid value per type).
    #[must_use]
    #[inline]
    pub(crate) fn supports_tagged_missing(self) -> bool {
        self >= Self::V113
    }

    /// Whether the format uses the legacy "magic 2^333" double missing
    /// sentinel (formats 104 and 105 only).
    ///
    /// The bit pattern `0x54C0_0000_0000_0000` (= `2^333` ≈ 1.75e100)
    /// falls well inside the valid IEEE-754 double range, so a simple
    /// range check cannot catch it — it must be matched exactly.
    /// Formats 106–111 switched to an out-of-range sentinel near
    /// `+MAX_DOUBLE`, and 113+ uses NaN bit patterns.
    #[must_use]
    #[inline]
    pub(crate) fn uses_magic_double_missing(self) -> bool {
        self <= Self::V105
    }
}

// ---------------------------------------------------------------------------
// Schema layout queries
// ---------------------------------------------------------------------------

impl Release {
    /// Whether each type list entry is stored as `u16` (format 117+).
    /// Earlier formats use `u8`.
    #[must_use]
    pub(crate) fn supports_extended_type_list_entry(self) -> bool {
        self >= Self::V117
    }

    /// The width of each type-list entry in bytes.
    ///
    /// Formats before 117 use 1-byte type codes; 117+ use 2-byte codes
    /// (needed for strL and the wider numeric codes).
    #[must_use]
    pub(crate) fn type_list_entry_len(self) -> usize {
        if self.supports_extended_type_list_entry() {
            2
        } else {
            1
        }
    }

    /// Fixed-length variable name field size (includes null terminator).
    #[must_use]
    pub(crate) fn variable_name_len(self) -> usize {
        if self >= Self::V118 {
            129
        } else if self >= Self::V110 {
            33
        } else {
            9
        }
    }

    /// Fixed-length display format field size.
    #[must_use]
    pub(crate) fn format_entry_len(self) -> usize {
        if self >= Self::V118 {
            57
        } else if self >= Self::V114 {
            49
        } else if self >= Self::V105 {
            12
        } else {
            7
        }
    }

    /// Fixed-length value-label name field size.
    ///
    /// Matches [`variable_name_len`](Self::variable_name_len) for all
    /// format versions.
    #[must_use]
    pub(crate) fn value_label_name_len(self) -> usize {
        self.variable_name_len()
    }

    /// Fixed-length variable label field size.
    #[must_use]
    pub(crate) fn variable_label_len(self) -> usize {
        if self >= Self::V118 {
            321
        } else if self >= Self::V108 {
            81
        } else {
            32
        }
    }

    /// Whether each sort-list entry is stored as `u32` (format 119+).
    /// Earlier formats use `u16`.
    #[must_use]
    pub(crate) fn supports_extended_sort_entry(self) -> bool {
        self >= Self::V119
    }

    #[must_use]
    pub(crate) fn sort_entry_len(self) -> usize {
        if self.supports_extended_sort_entry() {
            4
        } else {
            2
        }
    }

    /// Maximum byte length of a fixed-length string variable.
    ///
    /// Formats 104–110 support str1–str80, 111–116 support str1–str244,
    /// and 117+ support str1–str2045.
    #[must_use]
    pub(crate) fn max_fixed_string_len(self) -> u16 {
        if self >= Self::V117 {
            2045
        } else if self >= Self::V111 {
            244
        } else {
            80
        }
    }

    /// Classifies the binary expansion-field length field for this
    /// release.
    ///
    /// | Release   | Return           | Meaning                          |
    /// |-----------|------------------|----------------------------------|
    /// | V104      | `None`           | no expansion-field section       |
    /// | V105–V109 | `Some(false)`    | `u16` length (narrow)            |
    /// | V110+     | `Some(true)`     | `u32` length (extended)          |
    ///
    /// Callers should treat `None` as "the file cannot hold
    /// characteristics at all", and only need to branch on the
    /// `bool` after confirming the section exists.
    #[must_use]
    pub(crate) fn supports_extended_expansion(self) -> Option<bool> {
        if self >= Self::V110 {
            Some(true)
        } else if self >= Self::V105 {
            Some(false)
        } else {
            None
        }
    }

    /// Whether the format uses the old (pre-108) value-label layout.
    ///
    /// The old layout (V104-V107) stores each set as a `u16` entry
    /// count, a 9-byte name, a 1-byte pad, `u16` values, and 8-byte
    /// fixed-width labels. Format 108+ switches to a richer layout
    /// with a `u32` payload length, `u32` values, and variable-width
    /// labels.
    #[must_use]
    pub(crate) fn has_old_value_labels(self) -> bool {
        self < Self::V108
    }

    /// Padding bytes after the label name field in a value-label
    /// table entry.
    ///
    /// Returns 1 for V104-V107 and 3 for V108+.
    #[must_use]
    pub(crate) fn value_label_table_padding_len(self) -> usize {
        if self.has_old_value_labels() { 1 } else { 3 }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // -- TryFrom<u8> ---------------------------------------------------------

    #[test]
    fn try_from_valid_range() {
        for v in 102..=119 {
            let r = Release::try_from(v).unwrap();
            assert_eq!(r.to_byte(), v);
        }
    }

    #[test]
    fn try_from_below_range() {
        assert_eq!(
            Release::try_from(101),
            Err(FormatErrorKind::UnsupportedRelease { release: 101 }),
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

    // -- supports_extended_dataset_label -------------------------------------

    #[test]
    fn supports_extended_dataset_label_pre_118() {
        assert!(!Release::V117.supports_extended_dataset_label());
    }

    #[test]
    fn supports_extended_dataset_label_118_plus() {
        assert!(Release::V118.supports_extended_dataset_label());
        assert!(Release::V119.supports_extended_dataset_label());
    }

    // -- timestamp_len -------------------------------------------------------

    #[test]
    fn timestamp_len_v104() {
        assert_eq!(Release::V104.timestamp_len(), None);
    }

    #[test]
    fn timestamp_len_v105_plus() {
        assert_eq!(Release::V105.timestamp_len(), Some(18));
        assert_eq!(Release::V116.timestamp_len(), Some(18));
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

    // -- supports_tagged_missing ---------------------------------------------

    #[test]
    fn supports_tagged_missing_pre_113() {
        assert!(!Release::V104.supports_tagged_missing());
        assert!(!Release::V105.supports_tagged_missing());
        assert!(!Release::V111.supports_tagged_missing());
        assert!(!Release::V112.supports_tagged_missing());
    }

    #[test]
    fn supports_tagged_missing_113_plus() {
        assert!(Release::V113.supports_tagged_missing());
        assert!(Release::V114.supports_tagged_missing());
        assert!(Release::V119.supports_tagged_missing());
    }

    // -- uses_magic_double_missing -------------------------------------------

    #[test]
    fn uses_magic_double_missing_104_105() {
        assert!(Release::V104.uses_magic_double_missing());
        assert!(Release::V105.uses_magic_double_missing());
    }

    #[test]
    fn uses_magic_double_missing_106_plus() {
        assert!(!Release::V106.uses_magic_double_missing());
        assert!(!Release::V111.uses_magic_double_missing());
        assert!(!Release::V113.uses_magic_double_missing());
        assert!(!Release::V119.uses_magic_double_missing());
    }

    // -- type_list_entry_len ---------------------------------------------------

    #[test]
    fn type_list_entry_len_pre_117() {
        assert_eq!(Release::V104.type_list_entry_len(), 1);
        assert_eq!(Release::V111.type_list_entry_len(), 1);
        assert_eq!(Release::V116.type_list_entry_len(), 1);
    }

    #[test]
    fn type_list_entry_len_117_plus() {
        assert_eq!(Release::V117.type_list_entry_len(), 2);
        assert_eq!(Release::V118.type_list_entry_len(), 2);
        assert_eq!(Release::V119.type_list_entry_len(), 2);
    }

    // -- variable_name_len ---------------------------------------------------

    #[test]
    fn variable_name_len_boundaries() {
        assert_eq!(Release::V104.variable_name_len(), 9);
        assert_eq!(Release::V109.variable_name_len(), 9);
        assert_eq!(Release::V110.variable_name_len(), 33);
        assert_eq!(Release::V117.variable_name_len(), 33);
        assert_eq!(Release::V118.variable_name_len(), 129);
        assert_eq!(Release::V119.variable_name_len(), 129);
    }

    // -- format_entry_len ----------------------------------------------------

    #[test]
    fn format_entry_len_boundaries() {
        assert_eq!(Release::V104.format_entry_len(), 7);
        assert_eq!(Release::V105.format_entry_len(), 12);
        assert_eq!(Release::V113.format_entry_len(), 12);
        assert_eq!(Release::V114.format_entry_len(), 49);
        assert_eq!(Release::V117.format_entry_len(), 49);
        assert_eq!(Release::V118.format_entry_len(), 57);
        assert_eq!(Release::V119.format_entry_len(), 57);
    }

    // -- value_label_name_len ------------------------------------------------

    #[test]
    fn value_label_name_len_matches_variable_name_len() {
        for v in 104..=119 {
            let r = Release::try_from(v).unwrap();
            assert_eq!(r.value_label_name_len(), r.variable_name_len());
        }
    }

    // -- variable_label_len --------------------------------------------------

    #[test]
    fn variable_label_len_boundaries() {
        assert_eq!(Release::V104.variable_label_len(), 32);
        assert_eq!(Release::V107.variable_label_len(), 32);
        assert_eq!(Release::V108.variable_label_len(), 81);
        assert_eq!(Release::V117.variable_label_len(), 81);
        assert_eq!(Release::V118.variable_label_len(), 321);
        assert_eq!(Release::V119.variable_label_len(), 321);
    }

    // -- supports_extended_sort_entry ----------------------------------------

    #[test]
    fn supports_extended_sort_entry_pre_119() {
        assert!(!Release::V104.supports_extended_sort_entry());
        assert!(!Release::V118.supports_extended_sort_entry());
    }

    #[test]
    fn supports_extended_sort_entry_119() {
        assert!(Release::V119.supports_extended_sort_entry());
    }

    // -- supports_extended_expansion -----------------------------------------

    #[test]
    fn supports_extended_expansion_boundaries() {
        assert_eq!(Release::V104.supports_extended_expansion(), None);
        assert_eq!(Release::V105.supports_extended_expansion(), Some(false));
        assert_eq!(Release::V109.supports_extended_expansion(), Some(false));
        assert_eq!(Release::V110.supports_extended_expansion(), Some(true));
        assert_eq!(Release::V119.supports_extended_expansion(), Some(true));
    }

    // -- max_fixed_string_len ------------------------------------------------

    #[test]
    fn max_fixed_string_len_boundaries() {
        assert_eq!(Release::V104.max_fixed_string_len(), 80);
        assert_eq!(Release::V110.max_fixed_string_len(), 80);
        assert_eq!(Release::V111.max_fixed_string_len(), 244);
        assert_eq!(Release::V116.max_fixed_string_len(), 244);
        assert_eq!(Release::V117.max_fixed_string_len(), 2045);
        assert_eq!(Release::V119.max_fixed_string_len(), 2045);
    }
}
