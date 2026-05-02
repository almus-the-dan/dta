use core::fmt;

/// Unified error type for the DCT reader.
#[derive(Debug)]
#[non_exhaustive]
pub enum DctError {
    /// An I/O error from the underlying reader.
    Io(std::io::Error),
    /// The dictionary file ended before its closing `}` was reached.
    UnexpectedEofInDictionary,
    /// A `_column(#)` directive could not be parsed.
    InvalidColumnDirective {
        /// 1-based line number within the dictionary file.
        line: usize,
        /// Verbatim line content for diagnostic display.
        content: String,
    },
    /// A `%infmt` read-format token was not recognized.
    InvalidReadFormat {
        /// 1-based line number within the dictionary file.
        line: usize,
        /// The unrecognized read-format token.
        format: String,
    },
    /// A storage-type token was not one of the known Stata types
    /// (`byte`, `int`, `long`, `float`, `double`, `str`, `str#`).
    UnknownStorageType {
        /// 1-based line number within the dictionary file.
        line: usize,
        /// The unrecognized token.
        token: String,
    },
    /// A data record exceeded the maximum permitted length
    /// (524,275 bytes).
    RecordTooLong {
        /// 1-based line number within the data file.
        line: usize,
        /// Actual length in bytes.
        length: usize,
    },
    /// The data file ended in the middle of an observation.
    UnexpectedEofInData {
        /// 1-based observation number being read when EOF occurred.
        observation: usize,
        /// Number of variables successfully parsed before EOF.
        variables_read: usize,
    },
    /// A data field could not be parsed as the declared numeric type.
    InvalidNumericValue {
        /// 1-based observation number containing the bad field.
        observation: usize,
        /// Name of the variable being parsed.
        variable: String,
        /// Verbatim field content.
        content: String,
    },
}

impl DctError {
    pub(crate) fn io(source: std::io::Error) -> Self {
        Self::Io(source)
    }
}

impl fmt::Display for DctError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(source) => write!(f, "I/O error: {source}"),
            Self::UnexpectedEofInDictionary => {
                f.write_str("dictionary ended before its closing '}'")
            }
            Self::InvalidColumnDirective { line, content } => {
                write!(f, "invalid _column(#) directive on line {line}: {content}",)
            }
            Self::InvalidReadFormat { line, format } => {
                write!(f, "invalid read format '{format}' on line {line}")
            }
            Self::UnknownStorageType { line, token } => {
                write!(f, "unknown storage type '{token}' on line {line}")
            }
            Self::RecordTooLong { line, length } => write!(
                f,
                "data record on line {line} is {length} bytes, exceeds the 524275-byte limit",
            ),
            Self::UnexpectedEofInData {
                observation,
                variables_read,
            } => write!(
                f,
                "data file ended mid-observation {observation} after \
                 reading {variables_read} variable(s)",
            ),
            Self::InvalidNumericValue {
                observation,
                variable,
                content,
            } => write!(
                f,
                "could not parse '{content}' as a numeric value for variable \
                 '{variable}' in observation {observation}",
            ),
        }
    }
}

impl std::error::Error for DctError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Io(source) => Some(source),
            _ => None,
        }
    }
}

impl From<std::io::Error> for DctError {
    fn from(source: std::io::Error) -> Self {
        Self::Io(source)
    }
}

/// Convenience alias used throughout the `dct` module.
pub type Result<T> = std::result::Result<T, DctError>;
