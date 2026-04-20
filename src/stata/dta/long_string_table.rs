use std::borrow::Cow;
use std::collections::{BTreeMap, HashMap};
use std::rc::Rc;

use super::long_string::LongString;
use super::long_string_ref::LongStringRef;

/// Deduplicating table of long string (strL / GSO) payloads, used
/// while preparing records for a DTA writer.
///
/// For each strL column in each observation, call
/// [`get_or_insert`](Self::get_or_insert) to obtain a
/// [`LongStringRef`] to include in the record (wrap it in
/// [`Value::LongStringRef`](super::value::Value::LongStringRef)).
/// The table dedupes on `(data, binary)` — identical payloads share
/// a single ref, so repeated strings occupy one strL entry on disk.
///
/// After writing all records and the chain has advanced to
/// [`LongStringWriter`], iterate the table with [`iter`](Self::iter)
/// and pass each yielded [`LongString`] to
/// [`LongStringWriter::write_long_string`].
///
/// Entries are yielded in `(variable, observation)` order, matching
/// the DTA file layout requirement for the strL section.
///
/// # Memory
///
/// Stored payloads are reference-counted via [`Rc`] and shared
/// between the content-indexed and location-indexed maps — a long
/// string is held in memory once, regardless of how many times it
/// was referenced from the data section.
#[derive(Debug, Default)]
pub struct LongStringTable {
    // Ordered storage: drives the iteration order required by the
    // strL section layout.
    position: BTreeMap<(u32, u64), StoredEntry>,
    // Dedup: text and binary payloads live in separate maps, so the
    // key can be a plain `Rc<[u8]>`. `Rc<T>: Borrow<T>` then lets
    // `get(&[u8])` look up without allocating a query key.
    content_text: HashMap<Rc<[u8]>, (u32, u64)>,
    content_binary: HashMap<Rc<[u8]>, (u32, u64)>,
}

/// Entry held in the location-indexed map. `data` is shared (via
/// `Rc`) with whichever content-indexed map corresponds to `binary`.
#[derive(Debug)]
struct StoredEntry {
    data: Rc<[u8]>,
    binary: bool,
}

impl LongStringTable {
    /// Creates an empty table.
    #[must_use]
    #[inline]
    pub fn new() -> Self {
        Self {
            position: BTreeMap::new(),
            content_text: HashMap::new(),
            content_binary: HashMap::new(),
        }
    }

    /// Returns the ref for the given `(data, binary)` payload.
    ///
    /// If the payload was already inserted, returns the ref assigned
    /// at its first insertion (the given `variable` and
    /// `observation` are ignored in that case). Otherwise, a new ref
    /// is assigned using the given `(variable, observation)` as its
    /// key.
    pub fn get_or_insert(
        &mut self,
        variable: u32,
        observation: u64,
        data: &[u8],
        binary: bool,
    ) -> LongStringRef {
        let content = if binary {
            &mut self.content_binary
        } else {
            &mut self.content_text
        };
        // `Rc<[u8]>: Borrow<[u8]>` lets `get(&[u8])` look up without
        // allocating. The payload is only copied (via `Rc::from`) on
        // a miss.
        if let Some(&(existing_variable, existing_observation)) = content.get(data) {
            return LongStringRef::new(existing_variable, existing_observation);
        }
        let shared: Rc<[u8]> = Rc::from(data);
        content.insert(Rc::clone(&shared), (variable, observation));
        let entry = StoredEntry {
            data: shared,
            binary,
        };
        self.position.insert((variable, observation), entry);
        LongStringRef::new(variable, observation)
    }

    /// Number of unique payloads stored.
    #[must_use]
    #[inline]
    pub fn len(&self) -> usize {
        self.position.len()
    }

    /// `true` when the table holds no entries.
    #[must_use]
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.position.is_empty()
    }

    /// Yields stored entries in `(variable, observation)` order,
    /// ready to be passed to
    /// [`LongStringWriter::write_long_string`].
    ///
    /// The `encoding` argument is the writer's active encoding — it
    /// decorates each yielded [`LongString`] so callers can round-trip
    /// payload bytes through [`LongString::data_str`] without plumbing
    /// the encoding separately. The returned iterator borrows `self`
    /// and nothing else, so callers can freely invoke
    /// `write_long_string` inside the loop.
    pub fn iter<'a>(
        &'a self,
        encoding: &'static encoding_rs::Encoding,
    ) -> impl Iterator<Item = LongString<'a>> + 'a {
        self.position
            .iter()
            .map(move |(&(variable, observation), entry)| {
                LongString::new(
                    variable,
                    observation,
                    entry.binary,
                    Cow::Borrowed(&*entry.data),
                    encoding,
                )
            })
    }
}

#[cfg(test)]
mod tests {
    use encoding_rs::{UTF_8, WINDOWS_1252};

    use super::*;

    #[test]
    fn new_is_empty() {
        let table = LongStringTable::new();
        assert_eq!(table.len(), 0);
        assert!(table.is_empty());
    }

    #[test]
    fn default_matches_new() {
        let table = LongStringTable::default();
        assert!(table.is_empty());
    }

    #[test]
    fn get_or_insert_new_returns_given_key() {
        let mut table = LongStringTable::new();
        let reference = table.get_or_insert(3, 5, b"hello", false);
        assert_eq!(reference.variable(), 3);
        assert_eq!(reference.observation(), 5);
        assert_eq!(table.len(), 1);
    }

    #[test]
    fn get_or_insert_duplicate_returns_original_ref() {
        let mut table = LongStringTable::new();
        let first = table.get_or_insert(3, 5, b"hello", false);
        let second = table.get_or_insert(7, 99, b"hello", false);
        assert_eq!(first, second);
        assert_eq!(table.len(), 1);
    }

    #[test]
    fn get_or_insert_different_binary_flag_is_distinct() {
        let mut table = LongStringTable::new();
        let text_ref = table.get_or_insert(1, 1, b"\x00\x01\x02", false);
        let binary_ref = table.get_or_insert(2, 2, b"\x00\x01\x02", true);
        assert_ne!(text_ref, binary_ref);
        assert_eq!(table.len(), 2);
    }

    #[test]
    fn get_or_insert_distinct_payloads_are_stored_separately() {
        let mut table = LongStringTable::new();
        table.get_or_insert(1, 1, b"a", false);
        table.get_or_insert(1, 2, b"b", false);
        table.get_or_insert(1, 3, b"c", false);
        assert_eq!(table.len(), 3);
    }

    #[test]
    fn is_empty_tracks_insertion() {
        let mut table = LongStringTable::new();
        assert!(table.is_empty());
        table.get_or_insert(1, 1, b"x", false);
        assert!(!table.is_empty());
    }

    #[test]
    fn iter_yields_in_variable_then_observation_order() {
        let mut table = LongStringTable::new();
        table.get_or_insert(3, 1, b"c1", false);
        table.get_or_insert(1, 2, b"a2", false);
        table.get_or_insert(2, 5, b"b5", false);
        table.get_or_insert(1, 1, b"a1", false);

        let ordered: Vec<(u32, u64)> = table
            .iter(UTF_8)
            .map(|ls| (ls.variable(), ls.observation()))
            .collect();
        assert_eq!(ordered, vec![(1, 1), (1, 2), (2, 5), (3, 1)]);
    }

    #[test]
    fn iter_preserves_data_and_binary_flag() {
        let mut table = LongStringTable::new();
        table.get_or_insert(1, 1, b"text", false);
        table.get_or_insert(2, 2, b"\x00\x01", true);

        let long_strings: Vec<_> = table.iter(UTF_8).collect();
        assert_eq!(long_strings[0].data(), b"text");
        assert!(!long_strings[0].is_binary());
        assert_eq!(long_strings[1].data(), b"\x00\x01");
        assert!(long_strings[1].is_binary());
    }

    #[test]
    fn iter_captures_caller_supplied_encoding() {
        // 0x80 is Euro sign in Windows-1252; invalid UTF-8.
        let mut table = LongStringTable::new();
        table.get_or_insert(1, 1, b"\x80", false);

        let decoded = table
            .iter(WINDOWS_1252)
            .next()
            .unwrap()
            .data_str()
            .unwrap()
            .into_owned();
        assert_eq!(decoded, "€");

        assert!(table.iter(UTF_8).next().unwrap().data_str().is_none());
    }
}
