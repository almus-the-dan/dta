use std::collections::HashMap;
use std::collections::hash_map::Entry;

use super::value_label::ValueLabelSet;
use super::variable::Variable;

/// A collection of [`ValueLabelSet`]s keyed by name.
///
/// Populated by draining a reader with
/// [`ValueLabelReader::read_remaining_into`](crate::stata::dta::value_label_reader::ValueLabelReader::read_remaining_into)
/// (or its async mirror), or built up by the caller. Use
/// [`label_for`](Self::label_for) to resolve a
/// [`Variable`](crate::stata::dta::variable::Variable) plus an integer
/// value to the label text.
///
/// Mirrors the shape of
/// [`LongStringTable`](crate::stata::dta::long_string_table::LongStringTable):
/// [`insert`](Self::insert) replaces; [`get_or_insert`](Self::get_or_insert)
/// is first-wins on name.
#[derive(Debug, Clone, Default)]
pub struct ValueLabelTable {
    sets: HashMap<String, ValueLabelSet>,
    labels: HashMap<(String, i32), String>,
}

impl ValueLabelTable {
    /// Creates an empty table.
    #[must_use]
    #[inline]
    pub fn new() -> Self {
        Self {
            sets: HashMap::new(),
            labels: HashMap::new(),
        }
    }

    /// Inserts `set`, replacing any existing set with the same name
    /// and returning it.
    ///
    /// Use [`get_or_insert`](Self::get_or_insert) if you want
    /// first-wins semantics.
    pub fn insert(&mut self, set: ValueLabelSet) -> Option<ValueLabelSet> {
        self.sets.insert(set.name().to_owned(), set)
    }

    /// Inserts `set` if no set with its name is already present and
    /// returns a reference to whichever set now occupies the slot.
    ///
    /// This is the read-side insertion path: it preserves any set the
    /// caller pre-populated or that a previous drain already inserted.
    pub fn get_or_insert(&mut self, set: ValueLabelSet) -> &ValueLabelSet {
        match self.sets.entry(set.name().to_owned()) {
            Entry::Occupied(slot) => slot.into_mut(),
            Entry::Vacant(slot) => slot.insert(set),
        }
    }

    /// Returns the set with the given name, if any.
    #[must_use]
    pub fn get(&self, name: &str) -> Option<&ValueLabelSet> {
        self.sets.get(name)
    }

    /// Removes and returns the set with the given name, if any.
    pub fn remove(&mut self, name: &str) -> Option<ValueLabelSet> {
        self.sets.remove(name)
    }

    /// Number of sets in the table.
    #[must_use]
    #[inline]
    pub fn len(&self) -> usize {
        self.sets.len()
    }

    /// `true` when the table holds no sets.
    #[must_use]
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.sets.is_empty()
    }

    /// Yields the stored sets.
    pub fn iter(&self) -> impl Iterator<Item = &ValueLabelSet> {
        self.sets.values()
    }

    /// Resolves the label for `value` on `variable`.
    ///
    /// Looks up the variable's
    /// [`value_label_name`](Variable::value_label_name), finds the
    /// matching [`ValueLabelSet`] in this table, and returns the label
    /// for `value`. Returns `None` when:
    ///
    /// - The variable has no associated value-label name.
    /// - No set with that name exists in this table.
    /// - The set has no entry for `value`.
    #[must_use]
    pub fn label_for(&self, variable: &Variable, value: i32) -> Option<&str> {
        let name = variable.value_label_name();
        if name.is_empty() {
            return None;
        }
        let set = self.sets.get(name)?;
        set.label_for(value)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::stata::dta::value_label::ValueLabelEntry;
    use crate::stata::dta::variable_type::VariableType;

    fn make_entries(pairs: &[(i32, &str)]) -> Vec<ValueLabelEntry> {
        pairs
            .iter()
            .map(|&(v, l)| ValueLabelEntry::new(v, l.to_owned()))
            .collect()
    }

    fn set_with(name: &str, pairs: &[(i32, &str)]) -> ValueLabelSet {
        ValueLabelSet::new(name.to_owned(), make_entries(pairs))
    }

    fn variable_with_label(name: &str, value_label_name: &str) -> Variable {
        Variable::builder(VariableType::Byte, name)
            .format("%8.0g")
            .value_label_name(value_label_name)
            .build()
    }

    // -- ValueLabelTable basics ---------------------------------------------

    #[test]
    fn new_is_empty() {
        let table = ValueLabelTable::new();
        assert_eq!(table.len(), 0);
        assert!(table.is_empty());
    }

    #[test]
    fn default_matches_new() {
        assert!(ValueLabelTable::default().is_empty());
    }

    #[test]
    fn insert_stores_set_keyed_by_name() {
        let mut table = ValueLabelTable::new();
        assert!(table.insert(set_with("a", &[(1, "one")])).is_none());
        assert_eq!(table.len(), 1);
        assert_eq!(table.get("a").unwrap().label_for(1), Some("one"));
    }

    #[test]
    fn insert_replaces_and_returns_previous() {
        let mut table = ValueLabelTable::new();
        table.insert(set_with("a", &[(1, "old")]));
        let previous = table.insert(set_with("a", &[(1, "new")])).unwrap();
        assert_eq!(previous.label_for(1), Some("old"));
        assert_eq!(table.len(), 1);
        assert_eq!(table.get("a").unwrap().label_for(1), Some("new"));
    }

    #[test]
    fn get_or_insert_is_first_wins() {
        let mut table = ValueLabelTable::new();
        table.insert(set_with("a", &[(1, "first")]));
        let stored = table.get_or_insert(set_with("a", &[(1, "second")]));
        assert_eq!(stored.label_for(1), Some("first"));
        assert_eq!(table.len(), 1);
    }

    #[test]
    fn get_or_insert_inserts_when_absent() {
        let mut table = ValueLabelTable::new();
        let stored = table.get_or_insert(set_with("a", &[(1, "new")]));
        assert_eq!(stored.label_for(1), Some("new"));
        assert_eq!(table.len(), 1);
    }

    #[test]
    fn remove_returns_existing_and_drops_it() {
        let mut table = ValueLabelTable::new();
        table.insert(set_with("a", &[(1, "one")]));
        let removed = table.remove("a").unwrap();
        assert_eq!(removed.label_for(1), Some("one"));
        assert!(table.is_empty());
        assert!(table.get("a").is_none());
    }

    #[test]
    fn remove_returns_none_for_missing_name() {
        let mut table = ValueLabelTable::new();
        assert!(table.remove("ghost").is_none());
    }

    #[test]
    fn iter_yields_all_stored_sets() {
        let mut table = ValueLabelTable::new();
        table.insert(set_with("a", &[(1, "a1")]));
        table.insert(set_with("b", &[(1, "b1")]));
        table.insert(set_with("c", &[(1, "c1")]));

        let mut names: Vec<&str> = table.iter().map(ValueLabelSet::name).collect();
        names.sort_unstable();
        assert_eq!(names, vec!["a", "b", "c"]);
    }

    // -- ValueLabelTable::label_for ----------------------------------------

    #[test]
    fn label_for_happy_path() {
        let variable = variable_with_label("rating", "ratinglbl");
        let mut table = ValueLabelTable::new();
        table.insert(set_with("ratinglbl", &[(1, "low"), (5, "high")]));
        assert_eq!(table.label_for(&variable, 1), Some("low"));
        assert_eq!(table.label_for(&variable, 5), Some("high"));
    }

    #[test]
    fn label_for_returns_none_when_variable_has_no_label_name() {
        let variable = variable_with_label("rating", "");
        let mut table = ValueLabelTable::new();
        // Even an entry keyed by empty string is ignored.
        table.insert(set_with("", &[(1, "ignored")]));
        assert_eq!(table.label_for(&variable, 1), None);
    }

    #[test]
    fn label_for_returns_none_when_set_missing() {
        let variable = variable_with_label("rating", "ratinglbl");
        let table = ValueLabelTable::new();
        assert_eq!(table.label_for(&variable, 1), None);
    }

    #[test]
    fn label_for_returns_none_when_value_missing_in_set() {
        let variable = variable_with_label("rating", "ratinglbl");
        let mut table = ValueLabelTable::new();
        table.insert(set_with("ratinglbl", &[(1, "low")]));
        assert_eq!(table.label_for(&variable, 99), None);
    }
}
