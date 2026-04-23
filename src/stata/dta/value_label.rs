/// A single mapping from an integer value to a string label.
#[derive(Debug, Clone)]
pub struct ValueLabelEntry {
    value: i32,
    label: String,
}

impl ValueLabelEntry {
    #[must_use]
    pub(crate) fn new(value: i32, label: String) -> Self {
        Self { value, label }
    }

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

/// A named set of integer-to-label mappings.
///
/// Variables reference a value-label set by name. When a variable has
/// an associated set, each integer value in the data section can be
/// resolved to a human-readable label.
///
/// A set lives inside a
/// [`ValueLabelTable`](crate::stata::dta::value_label_table::ValueLabelTable)
/// keyed by its name.
#[derive(Debug, Clone)]
pub struct ValueLabelSet {
    name: String,
    entries: Vec<ValueLabelEntry>,
}

impl ValueLabelSet {
    #[must_use]
    pub(crate) fn new(name: String, entries: Vec<ValueLabelEntry>) -> Self {
        Self { name, entries }
    }

    /// The name of this value-label set.
    #[must_use]
    #[inline]
    pub fn name(&self) -> &str {
        &self.name
    }

    /// The label entries in this set.
    #[must_use]
    #[inline]
    pub fn entries(&self) -> &[ValueLabelEntry] {
        &self.entries
    }

    /// Returns the label for `value`, or `None` if no entry matches.
    ///
    /// Value-label sets are typically small (a handful of entries),
    /// so this is a linear scan. For the full chain from a variable
    /// plus a record value to its label, use
    /// [`ValueLabelTable::label_for`](crate::stata::dta::value_label_table::ValueLabelTable::label_for).
    #[must_use]
    pub fn label_for(&self, value: i32) -> Option<&str> {
        self.entries
            .iter()
            .find(|entry| entry.value == value)
            .map(ValueLabelEntry::label)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_entries(pairs: &[(i32, &str)]) -> Vec<ValueLabelEntry> {
        pairs
            .iter()
            .map(|&(v, l)| ValueLabelEntry::new(v, l.to_owned()))
            .collect()
    }

    fn set_with(name: &str, pairs: &[(i32, &str)]) -> ValueLabelSet {
        ValueLabelSet::new(name.to_owned(), make_entries(pairs))
    }

    #[test]
    fn label_for_finds_existing_entry() {
        let set = set_with("pricelbl", &[(0, "cheap"), (1, "mid"), (2, "pricey")]);
        assert_eq!(set.label_for(0), Some("cheap"));
        assert_eq!(set.label_for(1), Some("mid"));
        assert_eq!(set.label_for(2), Some("pricey"));
    }

    #[test]
    fn label_for_returns_none_on_miss() {
        let set = set_with("t", &[(0, "zero")]);
        assert_eq!(set.label_for(1), None);
    }

    #[test]
    fn label_for_empty_returns_none() {
        let set = set_with("empty", &[]);
        assert_eq!(set.label_for(0), None);
    }

    #[test]
    fn label_for_returns_first_match_on_duplicate_values() {
        // The writer forbids this but the parser does not — guarantee
        // deterministic first-wins behavior for data the caller might
        // have constructed by hand.
        let set = set_with("dup", &[(1, "first"), (1, "second")]);
        assert_eq!(set.label_for(1), Some("first"));
    }
}
