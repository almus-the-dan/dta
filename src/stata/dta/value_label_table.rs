use std::cell::OnceCell;
use std::collections::HashMap;
use std::collections::hash_map::Entry;
use std::rc::Rc;

use super::value_label::ValueLabelSet;
use super::variable::Variable;

/// Minimum set size at which a hash-indexed label lookup starts
/// beating a linear scan in practice. Sets below this stick with the
/// linear-scan path in [`ValueLabelSet::label_for`] — allocating the
/// index would cost more than it saves.
const INDEX_THRESHOLD: usize = 10;

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
///
/// # Caching
///
/// For sets with at least 10 entries, [`label_for`](Self::label_for)
/// builds and caches a `HashMap<i32, String>` the first time a
/// lookup hits the set. Subsequent lookups on the same set are O(1).
/// Smaller sets stay on the linear-scan path — the cost of building
/// the index outweighs the savings. The cache is invalidated when a
/// set is replaced or removed.
#[derive(Debug, Clone, Default)]
pub struct ValueLabelTable {
    // Name is keyed by `Rc<str>` so `sets` and `indexes` can share a
    // single heap allocation per set name. `Rc<str>: Borrow<str>`, so
    // external lookups still pass a plain `&str`.
    sets: HashMap<Rc<str>, ValueLabelSet>,
    // One lazy index per set name. Kept in lockstep with `sets`:
    // every `insert` / `get_or_insert` / `remove` that touches `sets`
    // does the matching touch on `indexes`, so `label_for` can always
    // assume `indexes.get(name)` returns `Some` when `sets.get(name)`
    // does.
    //
    // `OnceCell` (not `RefCell`) is load-bearing: `label_for` returns
    // `&str`, which must borrow directly from the stored `String`. A
    // `RefCell` borrow guard would drop at the end of the call and
    // invalidate the reference.
    indexes: HashMap<Rc<str>, OnceCell<HashMap<i32, String>>>,
}

impl ValueLabelTable {
    /// Creates an empty table.
    #[must_use]
    #[inline]
    pub fn new() -> Self {
        Self {
            sets: HashMap::new(),
            indexes: HashMap::new(),
        }
    }

    /// Inserts `set`, replacing any existing set with the same name
    /// and returning it.
    ///
    /// Replacing invalidates the cached index for that name (a fresh
    /// `OnceCell` is installed).
    ///
    /// Use [`get_or_insert`](Self::get_or_insert) if you want
    /// first-wins semantics.
    pub fn insert(&mut self, set: ValueLabelSet) -> Option<ValueLabelSet> {
        let name: Rc<str> = Rc::from(set.name());
        self.indexes.insert(Rc::clone(&name), OnceCell::new());
        self.sets.insert(name, set)
    }

    /// Inserts `set` if no set with its name is already present and
    /// returns a reference to whichever set now occupies the slot.
    ///
    /// This is the read-side insertion path: it preserves any set the
    /// caller pre-populated or that a previous drain already inserted.
    pub fn get_or_insert(&mut self, set: ValueLabelSet) -> &ValueLabelSet {
        let name: Rc<str> = Rc::from(set.name());
        match self.sets.entry(Rc::clone(&name)) {
            Entry::Occupied(slot) => slot.into_mut(),
            Entry::Vacant(slot) => {
                self.indexes.entry(name).or_default();
                slot.insert(set)
            }
        }
    }

    /// Returns the set with the given name, if any.
    #[must_use]
    pub fn get(&self, name: &str) -> Option<&ValueLabelSet> {
        self.sets.get(name)
    }

    /// Removes and returns the set with the given name, if any.
    pub fn remove(&mut self, name: &str) -> Option<ValueLabelSet> {
        self.indexes.remove(name);
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
    ///
    /// For sets with at least 10 entries the first call builds a
    /// cached `i32 → String` index; subsequent calls on the same set
    /// are O(1).
    #[must_use]
    pub fn label_for(&self, variable: &Variable, value: i32) -> Option<&str> {
        let name = variable.value_label_name();
        if name.is_empty() {
            return None;
        }
        let set = self.sets.get(name)?;
        if set.entries().len() < INDEX_THRESHOLD {
            return set.label_for(value);
        }
        // `indexes` is kept in lockstep with `sets` by insert /
        // get_or_insert / remove, so the `Some` branch always wins.
        // The `None` fallback is defensive: a missing cell would only
        // happen if that invariant regresses, and returning the
        // linear-scan result stays correct (just slower).
        let Some(cell) = self.indexes.get(name) else {
            return set.label_for(value);
        };
        let index = cell.get_or_init(|| build_index(set));
        index.get(&value).map(String::as_str)
    }
}

/// Builds a first-wins `i32 → String` index from a set's entries.
///
/// The first-wins approach matches the semantics of
/// [`ValueLabelSet::label_for`](ValueLabelSet::label_for)
/// — both paths resolve duplicate values the same way.
fn build_index(set: &ValueLabelSet) -> HashMap<i32, String> {
    let mut index = HashMap::with_capacity(set.entries().len());
    for entry in set.entries() {
        index
            .entry(entry.value())
            .or_insert_with(|| entry.label().to_owned());
    }
    index
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

    /// Builds a set with `count` entries, values `0..count`, labels
    /// `"lbl-{i}"`. Handy for exercising the cached branch of
    /// `label_for`.
    fn large_set(name: &str, count: i32) -> ValueLabelSet {
        let pairs: Vec<(i32, String)> = (0..count).map(|i| (i, format!("lbl-{i}"))).collect();
        let entries: Vec<ValueLabelEntry> = pairs
            .into_iter()
            .map(|(v, l)| ValueLabelEntry::new(v, l))
            .collect();
        ValueLabelSet::new(name.to_owned(), entries)
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

    // -- Cached branch (>= 10 entries) -------------------------------------

    #[test]
    fn label_for_uses_cache_for_large_sets() {
        let variable = variable_with_label("rating", "big");
        let mut table = ValueLabelTable::new();
        table.insert(large_set("big", 25));

        // Covers both endpoints and an interior value to make sure
        // the cached index actually indexes the full set.
        assert_eq!(table.label_for(&variable, 0), Some("lbl-0"));
        assert_eq!(table.label_for(&variable, 12), Some("lbl-12"));
        assert_eq!(table.label_for(&variable, 24), Some("lbl-24"));
    }

    #[test]
    fn label_for_cached_miss_returns_none() {
        let variable = variable_with_label("rating", "big");
        let mut table = ValueLabelTable::new();
        table.insert(large_set("big", 15));

        assert_eq!(table.label_for(&variable, -1), None);
        assert_eq!(table.label_for(&variable, 9999), None);
    }

    #[test]
    fn insert_replacement_invalidates_cache() {
        let variable = variable_with_label("rating", "big");
        let mut table = ValueLabelTable::new();
        table.insert(large_set("big", 12));

        // Prime the cache.
        assert_eq!(table.label_for(&variable, 3), Some("lbl-3"));

        // Replace with a different 12-entry set; the old cache must
        // not leak through.
        let replacement_pairs: Vec<(i32, String)> =
            (0..12).map(|i| (i, format!("new-{i}"))).collect();
        let replacement_entries = replacement_pairs
            .into_iter()
            .map(|(v, l)| ValueLabelEntry::new(v, l))
            .collect();
        table.insert(ValueLabelSet::new("big".to_owned(), replacement_entries));

        assert_eq!(table.label_for(&variable, 3), Some("new-3"));
        assert_eq!(table.label_for(&variable, 11), Some("new-11"));
    }

    #[test]
    fn cached_path_is_first_wins_on_duplicate_values() {
        // Parsed files shouldn't have duplicate values in a set, but
        // callers can hand-build one. Make sure the cache honors the
        // same first-wins rule `ValueLabelSet::label_for` uses.
        let mut entries: Vec<ValueLabelEntry> = (0..9)
            .map(|i| ValueLabelEntry::new(i, format!("lbl-{i}")))
            .collect();
        entries.push(ValueLabelEntry::new(3, "DUP".to_owned()));
        let set = ValueLabelSet::new("dup".to_owned(), entries);
        assert!(set.entries().len() >= INDEX_THRESHOLD);

        let variable = variable_with_label("rating", "dup");
        let mut table = ValueLabelTable::new();
        table.insert(set);
        assert_eq!(table.label_for(&variable, 3), Some("lbl-3"));
    }
}
