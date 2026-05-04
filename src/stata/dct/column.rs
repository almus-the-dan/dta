use super::column_anchor::ColumnAnchor;
use super::input_format::InputFormat;
use super::variable_type::VariableType;

/// A single variable as declared in a `.dct` dictionary.
#[derive(Debug, Clone)]
pub struct Column {
    line_offset: usize,
    anchor: ColumnAnchor,
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
        anchor: ColumnAnchor,
        storage_type: VariableType,
        name: String,
        read_format: InputFormat,
        label: Option<String>,
    ) -> Self {
        Self {
            line_offset,
            anchor,
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

    /// Where this column's first byte sits within its physical line.
    ///
    /// Returns [`ColumnAnchor::Absolute`] for columns whose start
    /// position is statically resolvable from the dictionary alone
    /// (the common case — every fixed-width column, plus any column
    /// with an explicit `_column(#)`).
    ///
    /// Returns [`ColumnAnchor::RelativeToCursor`] when a free-format
    /// predecessor (`%f`, `%g`, `%e`, `%s` with no width) sits
    /// between this column and the most recent absolute anchor on the
    /// same line. Free-format reads consume input dynamically, so
    /// the start position has to be resolved at runtime against the
    /// actual line bytes. The DCT reader handles this transparently
    /// inside `read_record` / `read_lazy_record`.
    #[must_use]
    #[inline]
    pub fn anchor(&self) -> ColumnAnchor {
        self.anchor
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
