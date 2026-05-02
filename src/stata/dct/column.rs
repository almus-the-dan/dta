use super::input_format::InputFormat;
use super::variable_type::VariableType;

/// A single variable as declared in a `.dct` dictionary.
#[derive(Debug, Clone)]
pub struct Column {
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
        offset: usize,
        storage_type: VariableType,
        name: String,
        read_format: InputFormat,
        label: Option<String>,
    ) -> Self {
        Self {
            offset,
            storage_type,
            name,
            read_format,
            label,
        }
    }

    /// 1-based byte offset within each data record, set by the
    /// `_column(#)` directive.
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
