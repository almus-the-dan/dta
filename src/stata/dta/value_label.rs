/// A named table that maps integer values to string labels.
///
/// Variables can reference a value-label table by name. When a
/// variable has an associated table, each integer value in the data
/// section can be resolved to a human-readable label.
#[derive(Debug, Clone)]
pub struct ValueLabelTable {
    name: String,
    entries: Vec<ValueLabelEntry>,
}

impl ValueLabelTable {
    /// The name of this value-label table.
    #[must_use]
    #[inline]
    pub fn name(&self) -> &str {
        &self.name
    }

    /// The label entries in this table.
    #[must_use]
    #[inline]
    pub fn entries(&self) -> &[ValueLabelEntry] {
        &self.entries
    }
}

/// A single mapping from an integer value to a string label.
#[derive(Debug, Clone)]
pub struct ValueLabelEntry {
    value: i32,
    label: String,
}

impl ValueLabelEntry {
    /// The integer value.
    #[must_use]
    #[inline]
    pub fn value(&self) -> i32 {
        self.value
    }

    /// The label text, decoded using the file's encoding.
    #[must_use]
    #[inline]
    pub fn label(&self) -> &str {
        &self.label
    }
}
