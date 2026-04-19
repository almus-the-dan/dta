use core::fmt;

/// Section of the DTA file where an error occurred.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Section {
    /// File header (release, byte order, counts, label, timestamp).
    Header,
    /// Variable definitions (names, types, formats, labels).
    Schema,
    /// Characteristics / expansion fields.
    Characteristics,
    /// Observation data rows.
    Records,
    /// Value-label mapping tables.
    ValueLabels,
    /// Long string (strL) entries (format 117+ only).
    LongStrings,
}

impl fmt::Display for Section {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Header => "header",
            Self::Schema => "schema",
            Self::Characteristics => "characteristics",
            Self::Records => "records",
            Self::ValueLabels => "value labels",
            Self::LongStrings => "long strings",
        })
    }
}

/// Identifies a specific field inside a section.
///
/// Used by [`FormatErrorKind::UnexpectedValue`] and
/// [`FormatErrorKind::InvalidEncoding`] to pinpoint which field
/// triggered the error.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Field {
    /// The release / format version number.
    ReleaseNumber,
    /// The byte-order indicator.
    ByteOrder,
    /// A variable type code.
    VariableType,
    /// The sort-order specification.
    SortOrder,
    /// The dataset label string.
    DatasetLabel,
    /// A variable name string.
    VariableName,
    /// A variable label string.
    VariableLabel,
    /// A variable display format string.
    VariableFormat,
    /// A value-label table name.
    ValueLabelName,
    /// An entry inside a value-label table.
    ValueLabelEntry,
    /// The type field of a long string (strL) entry.
    LongStringType,
    /// The name field of a characteristic entry.
    CharacteristicName,
    /// The value/contents field of a characteristic entry.
    CharacteristicValue,
    /// The timestamp field in the file header.
    Timestamp,
    /// The variable count (K) in the file header.
    VariableCount,
    /// The observation count (N) in the file header.
    ObservationCount,
}

impl fmt::Display for Field {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::ReleaseNumber => "release number",
            Self::ByteOrder => "byte order",
            Self::VariableType => "variable type",
            Self::SortOrder => "sort order",
            Self::DatasetLabel => "dataset label",
            Self::VariableName => "variable name",
            Self::VariableLabel => "variable label",
            Self::VariableFormat => "variable format",
            Self::ValueLabelName => "value-label name",
            Self::ValueLabelEntry => "value-label entry",
            Self::LongStringType => "long string type",
            Self::CharacteristicName => "characteristic name",
            Self::CharacteristicValue => "characteristic value",
            Self::Timestamp => "timestamp",
            Self::VariableCount => "variable count",
            Self::ObservationCount => "observation count",
        })
    }
}

/// Known XML-style section tags in format 117+ files.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Tag {
    /// `<header>`
    Header,
    /// `<map>`
    Map,
    /// `<variable_types>`
    VariableTypes,
    /// `<varnames>`
    VariableNames,
    /// `<sortlist>`
    SortList,
    /// `<formats>`
    Formats,
    /// `<value_label_names>`
    ValueLabelNames,
    /// `<variable_labels>`
    VariableLabels,
    /// `<characteristics>`
    Characteristics,
    /// `<data>`
    Data,
    /// `<strls>`
    LongStrings,
    /// `<value_labels>`
    ValueLabels,
    /// `</stata_dta>`
    EndOfFile,
    /// Tag bytes did not match any known tag.
    Unknown,
}

impl fmt::Display for Tag {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Header => "header",
            Self::Map => "map",
            Self::VariableTypes => "variable_types",
            Self::VariableNames => "varnames",
            Self::SortList => "sortlist",
            Self::Formats => "formats",
            Self::ValueLabelNames => "value_label_names",
            Self::VariableLabels => "variable_labels",
            Self::Characteristics => "characteristics",
            Self::Data => "data",
            Self::LongStrings => "strls",
            Self::ValueLabels => "value_labels",
            Self::EndOfFile => "/stata_dta",
            Self::Unknown => "unknown",
        })
    }
}

/// Specific kind of format violation.
///
/// Every variant is small and stack-only — no heap allocations.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FormatErrorKind {
    /// Magic bytes or section tag do not match any known format.
    InvalidMagic,
    /// Byte-order marker byte is not a recognized value.
    InvalidByteOrder {
        /// The unrecognized byte.
        byte: u8,
    },
    /// Byte-order string tag is not `"MSF"` or `"LSF"`.
    InvalidByteOrderTag,
    /// The release/version number is not supported.
    UnsupportedRelease {
        /// The unsupported release number.
        release: u8,
    },
    /// A field contained an unexpected byte value.
    UnexpectedValue {
        /// Which field held the unexpected value.
        field: Field,
        /// The first unexpected byte.
        value: u8,
    },
    /// A section or field ended before the expected number of bytes
    /// was present.
    Truncated {
        /// Number of bytes expected.
        expected: u64,
        /// Number of bytes actually available.
        actual: u64,
    },
    /// A variable type code is not recognized.
    InvalidVariableType {
        /// The unrecognized type code.
        code: u16,
    },
    /// A string field contains bytes that are not valid in the
    /// file's declared encoding.
    InvalidEncoding {
        /// Which field failed to decode.
        field: Field,
    },
    /// An XML-style section tag (format 118+) was not the expected
    /// tag.
    UnexpectedTag {
        /// The tag that was expected.
        expected: Tag,
        /// The tag that was found.
        actual: Tag,
    },
    /// A value-label table's internal offsets are inconsistent.
    InvalidValueLabelTable,
    /// A strL entry header is malformed.
    InvalidLongStringEntry,
    /// A value is too large for the field that would store it
    /// (e.g., a string longer than its fixed-width slot, or a
    /// variable count that exceeds the format's 16-bit ceiling).
    FieldTooLarge {
        /// The field being written.
        field: Field,
        /// The largest representable value for that field.
        max: u64,
        /// The actual value presented by the caller.
        actual: u64,
    },
}

impl fmt::Display for FormatErrorKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidMagic => f.write_str("invalid magic bytes"),
            Self::InvalidByteOrder { byte } => {
                write!(f, "invalid byte-order code {byte:#04X}")
            }
            Self::InvalidByteOrderTag => f.write_str("expected byte-order tag \"MSF\" or \"LSF\""),
            Self::UnsupportedRelease { release } => {
                write!(f, "unsupported format version {release}")
            }
            Self::UnexpectedValue { field, value } => {
                write!(f, "unexpected value {value:#04X} in {field}")
            }
            Self::Truncated { expected, actual } => {
                write!(f, "truncated: expected {expected} bytes, got {actual}")
            }
            Self::InvalidVariableType { code } => {
                write!(f, "invalid variable type code {code}")
            }
            Self::InvalidEncoding { field } => {
                write!(f, "invalid encoding in {field}")
            }
            Self::UnexpectedTag { expected, actual } => {
                write!(f, "expected <{expected}> tag, found <{actual}>")
            }
            Self::InvalidValueLabelTable => {
                f.write_str("value-label table has inconsistent offsets")
            }
            Self::InvalidLongStringEntry => f.write_str("malformed strL entry header"),
            Self::FieldTooLarge { field, max, actual } => write!(
                f,
                "{field} value {actual} exceeds maximum {max} for this format",
            ),
        }
    }
}

/// A format violation with file context.
#[derive(Debug)]
pub struct FormatError {
    section: Section,
    position: u64,
    kind: FormatErrorKind,
}

impl FormatError {
    /// Creates a new format error.
    pub(crate) const fn new(section: Section, position: u64, kind: FormatErrorKind) -> Self {
        Self {
            section,
            position,
            kind,
        }
    }

    /// The section of the file where the error occurred.
    #[must_use]
    #[inline]
    pub fn section(&self) -> Section {
        self.section
    }

    /// The byte offset in the file where the error was detected.
    #[must_use]
    #[inline]
    pub fn position(&self) -> u64 {
        self.position
    }

    /// The specific kind of format violation.
    #[must_use]
    #[inline]
    pub fn kind(&self) -> FormatErrorKind {
        self.kind
    }
}

impl fmt::Display for FormatError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "format error in {} section at byte {}: {}",
            self.section, self.position, self.kind,
        )
    }
}

impl std::error::Error for FormatError {}

/// Unified error type for the DTA reader.
#[derive(Debug)]
pub enum DtaError {
    /// An I/O error from the underlying reader.
    Io {
        /// The section being read when the I/O error occurred.
        section: Section,
        /// The underlying I/O error.
        source: std::io::Error,
    },
    /// The file contents violate the DTA format specification.
    Format(FormatError),
    /// A sort-order entry references a nonexistent variable.
    SortOrderOutOfBounds {
        /// The 0-based sort-order index that was out of range.
        index: u32,
        /// The number of variables in the schema.
        variable_count: usize,
    },
}

impl DtaError {
    /// Creates an I/O error tagged with a section.
    pub(crate) fn io(section: Section, source: std::io::Error) -> Self {
        Self::Io { section, source }
    }

    /// Creates an error for a seek attempted before section offsets
    /// have been initialized (i.e., before schema reading).
    pub(crate) fn missing_section_offsets(section: Section) -> Self {
        Self::io(
            section,
            std::io::Error::other("section offsets not available — schema must be read first"),
        )
    }

    /// Creates a format error. Shorthand for wrapping
    /// [`FormatError::new`].
    pub(crate) const fn format(section: Section, position: u64, kind: FormatErrorKind) -> Self {
        Self::Format(FormatError::new(section, position, kind))
    }

    /// The section of the file where the error occurred.
    #[must_use]
    #[inline]
    pub fn section(&self) -> Section {
        match self {
            Self::Io { section, .. } | Self::Format(FormatError { section, .. }) => *section,
            Self::SortOrderOutOfBounds { .. } => Section::Schema,
        }
    }
}

impl fmt::Display for DtaError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io { section, source } => {
                write!(f, "I/O error in {section} section: {source}")
            }
            Self::Format(err) => fmt::Display::fmt(err, f),
            Self::SortOrderOutOfBounds {
                index,
                variable_count,
            } => {
                write!(
                    f,
                    "sort-order index {index} is out of bounds for {variable_count} variables",
                )
            }
        }
    }
}

impl std::error::Error for DtaError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Io { source, .. } => Some(source),
            Self::Format(_) | Self::SortOrderOutOfBounds { .. } => None,
        }
    }
}

/// Convenience alias used throughout the `dta` module.
pub type Result<T> = std::result::Result<T, DtaError>;
