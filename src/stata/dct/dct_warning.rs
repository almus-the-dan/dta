use core::fmt;

use super::variable_type::VariableType;

/// A non-fatal issue encountered while parsing a `.dct` file or its
/// associated data.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub enum DctWarning {
    /// A line in the dictionary did not match any recognized
    /// directive and was skipped.
    UnrecognizedDirective {
        /// 1-based line number within the dictionary file.
        line: usize,
        /// Verbatim line content.
        content: String,
    },
    /// A free-format field consumed past the next declared `_column`
    /// position; the `_column` anchor was honored for the next
    /// variable.
    FreeFormatOverflow {
        /// Name of the variable that overflowed.
        variable: String,
        /// Column position the free-format read advanced to.
        consumed_to: usize,
        /// Column position declared for the next variable.
        next_column: usize,
    },
    /// An integer value exceeded the declared storage type's range
    /// and was promoted to the next wider numeric type for this value
    /// only. The schema's declared storage type is unchanged.
    IntegerPromotion {
        /// Name of the affected variable.
        variable: String,
        /// 1-based observation number.
        observation: usize,
        /// The variable's declared storage type.
        from: VariableType,
        /// The wider type the value was promoted to.
        to: VariableType,
    },
    /// A fixed-width field was entirely blank and was treated as
    /// system missing.
    BlankFieldTreatedAsMissing {
        /// Name of the affected variable.
        variable: String,
        /// 1-based observation number.
        observation: usize,
    },
    /// The dictionary declared a `using` path that the library did
    /// not act upon. The path is available in
    /// [`DctSchema::declared_data_path`](super::schema::Schema::declared_data_path).
    DeclaredPathIgnored {
        /// The path declared in the dictionary's `using` clause.
        path: String,
    },
}

impl fmt::Display for DctWarning {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnrecognizedDirective { line, content } => {
                write!(f, "unrecognized directive on line {line}: {content}")
            }
            Self::FreeFormatOverflow {
                variable,
                consumed_to,
                next_column,
            } => write!(
                f,
                "free-format read for '{variable}' advanced to column {consumed_to}, \
                 past the next anchor at column {next_column}",
            ),
            Self::IntegerPromotion {
                variable,
                observation,
                from,
                to,
            } => write!(
                f,
                "value for '{variable}' in observation {observation} was promoted \
                 from {from} to {to}",
            ),
            Self::BlankFieldTreatedAsMissing {
                variable,
                observation,
            } => write!(
                f,
                "blank field for '{variable}' in observation {observation} \
                 treated as system missing",
            ),
            Self::DeclaredPathIgnored { path } => {
                write!(f, "declared data path '{path}' was not acted upon")
            }
        }
    }
}
