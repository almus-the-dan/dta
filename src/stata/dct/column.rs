use super::input_format::InputFormat;
use super::variable_type::VariableType;

/// A single variable as declared in a `.dct` dictionary.
#[derive(Debug, Clone)]
pub struct Column {
    line_offset: usize,
    offset: usize,
    storage_type: VariableType,
    name: String,
    read_format: InputFormat,
    label: Option<String>,
}

impl Column {
    /// Builds a new column declaration.
    #[must_use]
    pub(crate) fn new(
        line_offset: usize,
        offset: usize,
        storage_type: VariableType,
        name: String,
        read_format: InputFormat,
        label: Option<String>,
    ) -> Self {
        Self {
            line_offset,
            offset,
            storage_type,
            name,
            read_format,
            label,
        }
    }

    /// 0-based index of the physical line within an observation that
    /// this column lives on.
    ///
    /// Single-line observations always report `0`. Multi-line
    /// observations are produced by `_newline` directives in the
    /// dictionary: the first variable sits on line `0`, each
    /// subsequent `_newline` advances the line index by one for the
    /// variables that follow.
    #[must_use]
    #[inline]
    pub fn line_offset(&self) -> usize {
        self.line_offset
    }

    /// 0-based byte offset within this column's physical line at
    /// which the variable's field begins.
    ///
    /// Derived from the `_column(#)` directive in the dictionary,
    /// which is 1-based; the parser subtracts one and validates that
    /// the declared value was at least 1. After a `_newline` the
    /// `_column(#)` reference restarts from 1, so this offset is
    /// relative to [`line_offset`](Self::line_offset), not cumulative
    /// across the observation.
    #[must_use]
    #[inline]
    pub fn offset(&self) -> usize {
        self.offset
    }

    /// The variable's declared storage type.
    #[must_use]
    #[inline]
    pub fn storage_type(&self) -> VariableType {
        self.storage_type
    }

    /// The variable's name.
    #[must_use]
    #[inline]
    pub fn name(&self) -> &str {
        &self.name
    }

    /// The read format that controls how bytes are consumed from
    /// each data record for this variable.
    #[must_use]
    #[inline]
    pub fn input_format(&self) -> InputFormat {
        self.read_format
    }

    /// The optional human-readable variable label.
    #[must_use]
    #[inline]
    pub fn label(&self) -> Option<&str> {
        self.label.as_deref()
    }
}
