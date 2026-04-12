use super::variable_type::VariableType;

/// A single variable (column) definition from the schema.
#[derive(Debug, Clone)]
#[allow(clippy::struct_field_names)]
pub struct Variable {
    variable_type: VariableType,
    name: String,
    format: String,
    value_label_name: String,
    label: String,
}

impl Variable {
    /// Returns a new [`VariableBuilder`] with the given type and name.
    /// All other fields default to empty strings.
    #[must_use]
    #[inline]
    pub fn builder(variable_type: VariableType, name: impl Into<String>) -> VariableBuilder {
        VariableBuilder {
            variable_type,
            name: name.into(),
            format: String::new(),
            value_label_name: String::new(),
            label: String::new(),
        }
    }

    /// The storage type of this variable.
    #[must_use]
    #[inline]
    pub fn variable_type(&self) -> VariableType {
        self.variable_type
    }

    /// The variable name (e.g., `"mpg"`, `"price"`).
    #[must_use]
    #[inline]
    pub fn name(&self) -> &str {
        &self.name
    }

    /// The display format string (e.g., `"%9.0g"`, `"%20s"`).
    #[must_use]
    #[inline]
    pub fn format(&self) -> &str {
        &self.format
    }

    /// The name of the value-label table associated with this
    /// variable, or an empty string if none.
    #[must_use]
    #[inline]
    pub fn value_label_name(&self) -> &str {
        &self.value_label_name
    }

    /// The descriptive label for this variable, or an empty string
    /// if none.
    #[must_use]
    #[inline]
    pub fn label(&self) -> &str {
        &self.label
    }
}

/// Builder for [`Variable`].
///
/// Created via [`Variable::builder`]. Type and name are required; all
/// other fields default to empty strings.
#[derive(Debug, Clone)]
pub struct VariableBuilder {
    variable_type: VariableType,
    name: String,
    format: String,
    value_label_name: String,
    label: String,
}

impl VariableBuilder {
    /// Sets the display format string.
    #[must_use]
    #[inline]
    pub fn format(mut self, format: impl Into<String>) -> Self {
        self.format = format.into();
        self
    }

    /// Sets the value-label table name.
    #[must_use]
    #[inline]
    pub fn value_label_name(mut self, name: impl Into<String>) -> Self {
        self.value_label_name = name.into();
        self
    }

    /// Sets the descriptive label.
    #[must_use]
    #[inline]
    pub fn label(mut self, label: impl Into<String>) -> Self {
        self.label = label.into();
        self
    }

    /// Builds the [`Variable`].
    #[must_use]
    #[inline]
    pub fn build(self) -> Variable {
        Variable {
            variable_type: self.variable_type,
            name: self.name,
            format: self.format,
            value_label_name: self.value_label_name,
            label: self.label,
        }
    }
}
