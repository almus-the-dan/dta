use std::borrow::Cow;
use std::collections::{BTreeMap, HashMap};
use std::rc::Rc;

use super::long_string::LongString;
use super::long_string_ref::LongStringRef;

/// Deduplicating table of long string (strL / GSO) payloads, used
/// while preparing records for a DTA writer or to resolve
/// [`LongStringRef`]s read from a file.
///
/// Two insertion paths are provided depending on what the caller
/// controls:
///
/// - [`get_or_insert_by_content`](Self::get_or_insert_by_content)
///   dedupes on the payload bytes and assigns the caller's
///   `(variable, observation)` only on a first-time insertion. This
///   is the write-side flow — repeated payloads collapse into a
///   single strL entry on disk, and the caller embeds the returned
///   ref in the data section.
/// - [`get_or_insert_by_key`](Self::get_or_insert_by_key) keys on the
///   given `(variable, observation)`, inserting only when that key is
///   free. This is the read-side flow — the file has already assigned
///   canonical keys and they must be preserved so that
///   [`LongStringRef`]s from the data section resolve via
///   [`get`](Self::get).
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

    /// Returns the ref for the given `(data, binary)` payload,
    /// keyed by content.
    ///
    /// If the payload was already inserted, returns the ref assigned
    /// at its first insertion (the given `variable` and
    /// `observation` are ignored in that case). Otherwise, a new ref
    /// is assigned using the given `(variable, observation)` as its
    /// key.
    ///
    /// This is the write-side insertion path. Use
    /// [`get_or_insert_by_key`](Self::get_or_insert_by_key) when the
    /// caller is reading a file and must preserve the file's keys
    /// verbatim.
    pub fn get_or_insert_by_content(
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

    /// Inserts the given `(data, binary)` payload under the given
    /// `(variable, observation)` key if that key is free.
    ///
    /// Unlike [`get_or_insert_by_content`](Self::get_or_insert_by_content),
    /// this path never synthesizes a different key — the caller's
    /// `(variable, observation)` is treated as authoritative. This is
    /// the read-side insertion path: when populating the table from a
    /// DTA file's strL section, each GSO block's keys must survive
    /// intact so that [`LongStringRef`]s from the data section
    /// resolve via [`get`](Self::get).
    ///
    /// # Collision behavior
    ///
    /// - **Key collision**: if the `(variable, observation)` slot is
    ///   already occupied, the existing entry is kept (first in wins)
    ///   and the caller's `data`/`binary` are discarded.
    /// - **Content collision**: if the payload was already stored
    ///   under a different key, the existing payload is reused (the
    ///   `Rc<[u8]>` is shared) but the content map's recorded key is
    ///   *not* updated, so [`get_or_insert_by_content`](Self::get_or_insert_by_content)
    ///   continues to return the first inserter's key.
    ///
    /// Always returns `LongStringRef::new(variable, observation)`.
    pub fn get_or_insert_by_key(
        &mut self,
        variable: u32,
        observation: u64,
        data: &[u8],
        binary: bool,
    ) -> LongStringRef {
        let key = (variable, observation);
        if self.position.contains_key(&key) {
            return LongStringRef::new(variable, observation);
        }

        let content = if binary {
            &mut self.content_binary
        } else {
            &mut self.content_text
        };
        // Reuse the existing `Rc` when this payload is already stored
        // under a different key, so both position entries share one
        // allocation. The content map's recorded key is left as-is so
        // that `get_or_insert_by_content` continues to return the
        // original inserter.
        let shared: Rc<[u8]> = if let Some((existing_rc, _)) = content.get_key_value(data) {
            Rc::clone(existing_rc)
        } else {
            let new_rc: Rc<[u8]> = Rc::from(data);
            content.insert(Rc::clone(&new_rc), (variable, observation));
            new_rc
        };

        let entry = StoredEntry {
            data: shared,
            binary,
        };
        self.position.insert(key, entry);
        LongStringRef::new(variable, observation)
    }

    /// Removes the entry matching the given key, returning `true` if
    /// an entry was removed.
    ///
    /// When the removed entry was the canonical content-dedup target
    /// (i.e., [`get_or_insert_by_content`](Self::get_or_insert_by_content)
    /// would have returned this key for its payload), the content map
    /// entry is also cleared so that a subsequent `get_or_insert_by_content`
    /// call assigns a fresh canonical target. Other position entries
    /// that happen to share the same payload via a cloned `Rc` are
    /// untouched.
    pub fn remove_by_key(&mut self, reference: &LongStringRef) -> bool {
        let key = (reference.variable(), reference.observation());
        let Some(removed) = self.position.remove(&key) else {
            return false;
        };
        let content = if removed.binary {
            &mut self.content_binary
        } else {
            &mut self.content_text
        };
        if content.get(&*removed.data).copied() == Some(key) {
            content.remove(&*removed.data);
        }
        true
    }

    /// Returns the entry matching the `reference` if one exists.
    ///
    /// The returned [`LongString`] borrows payload bytes from the
    /// table, so the table must outlive it. `encoding` is used to
    /// decorate the returned entry so callers can round-trip bytes
    /// through [`LongString::data_str`] without plumbing the encoding
    /// separately — it does not affect lookup, which keys solely on
    /// `(variable, observation)`.
    #[must_use]
    pub fn get(
        &self,
        reference: &LongStringRef,
        encoding: &'static encoding_rs::Encoding,
    ) -> Option<LongString<'_>> {
        let key = (reference.variable(), reference.observation());
        let entry = self.position.get(&key)?;
        Some(LongString::new(
            reference.variable(),
            reference.observation(),
            entry.binary,
            Cow::Borrowed(&entry.data),
            encoding,
        ))
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
    fn get_or_insert_by_content_new_returns_given_key() {
        let mut table = LongStringTable::new();
        let reference = table.get_or_insert_by_content(3, 5, b"hello", false);
        assert_eq!(reference.variable(), 3);
        assert_eq!(reference.observation(), 5);
        assert_eq!(table.len(), 1);
    }

    #[test]
    fn get_or_insert_by_content_duplicate_returns_original_ref() {
        let mut table = LongStringTable::new();
        let first = table.get_or_insert_by_content(3, 5, b"hello", false);
        let second = table.get_or_insert_by_content(7, 99, b"hello", false);
        assert_eq!(first, second);
        assert_eq!(table.len(), 1);
    }

    #[test]
    fn get_or_insert_by_content_different_binary_flag_is_distinct() {
        let mut table = LongStringTable::new();
        let text_ref = table.get_or_insert_by_content(1, 1, b"\x00\x01\x02", false);
        let binary_ref = table.get_or_insert_by_content(2, 2, b"\x00\x01\x02", true);
        assert_ne!(text_ref, binary_ref);
        assert_eq!(table.len(), 2);
    }

    #[test]
    fn get_or_insert_by_content_distinct_payloads_are_stored_separately() {
        let mut table = LongStringTable::new();
        table.get_or_insert_by_content(1, 1, b"a", false);
        table.get_or_insert_by_content(1, 2, b"b", false);
        table.get_or_insert_by_content(1, 3, b"c", false);
        assert_eq!(table.len(), 3);
    }

    #[test]
    fn is_empty_tracks_insertion() {
        let mut table = LongStringTable::new();
        assert!(table.is_empty());
        table.get_or_insert_by_content(1, 1, b"x", false);
        assert!(!table.is_empty());
    }

    #[test]
    fn iter_yields_in_variable_then_observation_order() {
        let mut table = LongStringTable::new();
        table.get_or_insert_by_content(3, 1, b"c1", false);
        table.get_or_insert_by_content(1, 2, b"a2", false);
        table.get_or_insert_by_content(2, 5, b"b5", false);
        table.get_or_insert_by_content(1, 1, b"a1", false);

        let ordered: Vec<(u32, u64)> = table
            .iter(UTF_8)
            .map(|ls| (ls.variable(), ls.observation()))
            .collect();
        assert_eq!(ordered, vec![(1, 1), (1, 2), (2, 5), (3, 1)]);
    }

    #[test]
    fn iter_preserves_data_and_binary_flag() {
        let mut table = LongStringTable::new();
        table.get_or_insert_by_content(1, 1, b"text", false);
        table.get_or_insert_by_content(2, 2, b"\x00\x01", true);

        let long_strings: Vec<_> = table.iter(UTF_8).collect();
        assert_eq!(long_strings[0].data(), b"text");
        assert!(!long_strings[0].is_binary());
        assert_eq!(long_strings[1].data(), b"\x00\x01");
        assert!(long_strings[1].is_binary());
    }

    #[test]
    fn get_or_insert_by_key_inserts_new_entry() {
        let mut table = LongStringTable::new();
        let reference = table.get_or_insert_by_key(3, 5, b"hello", false);
        assert_eq!(reference.variable(), 3);
        assert_eq!(reference.observation(), 5);
        assert_eq!(table.len(), 1);
        let stored = table.get(&reference, UTF_8).unwrap();
        assert_eq!(stored.data(), b"hello");
    }

    #[test]
    fn get_or_insert_by_key_always_returns_passed_key() {
        let mut table = LongStringTable::new();
        // Even though the same payload could dedupe via `by_content`,
        // `by_key` must honor the caller's (variable, observation).
        let first = table.get_or_insert_by_key(3, 5, b"hello", false);
        let second = table.get_or_insert_by_key(7, 9, b"hello", false);
        assert_eq!(first.variable(), 3);
        assert_eq!(first.observation(), 5);
        assert_eq!(second.variable(), 7);
        assert_eq!(second.observation(), 9);
        assert_eq!(table.len(), 2);
    }

    #[test]
    fn get_or_insert_by_key_is_first_wins_on_key_collision() {
        let mut table = LongStringTable::new();
        table.get_or_insert_by_key(3, 5, b"first", false);
        // Second call on the same key with different data is a no-op.
        let reference = table.get_or_insert_by_key(3, 5, b"second", false);
        assert_eq!(reference.variable(), 3);
        assert_eq!(reference.observation(), 5);
        assert_eq!(table.len(), 1);
        let stored = table.get(&reference, UTF_8).unwrap();
        assert_eq!(stored.data(), b"first");
    }

    #[test]
    fn get_or_insert_by_key_shares_payload_across_duplicate_content() {
        let mut table = LongStringTable::new();
        table.get_or_insert_by_key(3, 5, b"hello", false);
        table.get_or_insert_by_key(7, 9, b"hello", false);

        // Both keys resolve to the same payload.
        let first = table.get(&LongStringRef::new(3, 5), UTF_8).unwrap();
        let second = table.get(&LongStringRef::new(7, 9), UTF_8).unwrap();
        assert_eq!(first.data(), b"hello");
        assert_eq!(second.data(), b"hello");
    }

    #[test]
    fn by_content_after_by_key_returns_first_key_inserter() {
        let mut table = LongStringTable::new();
        table.get_or_insert_by_key(3, 5, b"hello", false);
        table.get_or_insert_by_key(7, 9, b"hello", false);

        // Subsequent content-keyed insertion should dedupe against
        // the first by_key entry, not the second.
        let reference = table.get_or_insert_by_content(99, 99, b"hello", false);
        assert_eq!(reference.variable(), 3);
        assert_eq!(reference.observation(), 5);
        assert_eq!(table.len(), 2);
    }

    #[test]
    fn by_key_after_by_content_preserves_both_keys() {
        let mut table = LongStringTable::new();
        // Writer-style first.
        let content_ref = table.get_or_insert_by_content(1, 1, b"hello", false);
        // Reader-style adds a second key for the same payload.
        let key_ref = table.get_or_insert_by_key(2, 2, b"hello", false);
        assert_eq!(content_ref.variable(), 1);
        assert_eq!(key_ref.variable(), 2);
        assert_eq!(table.len(), 2);

        // A later `by_content` still returns the original inserter's key.
        let later = table.get_or_insert_by_content(99, 99, b"hello", false);
        assert_eq!(later, content_ref);
    }

    #[test]
    fn get_or_insert_by_key_separates_text_and_binary() {
        let mut table = LongStringTable::new();
        let text = table.get_or_insert_by_key(1, 1, b"\x00\x01\x02", false);
        let binary = table.get_or_insert_by_key(2, 2, b"\x00\x01\x02", true);
        assert_ne!(text, binary);
        assert_eq!(table.len(), 2);
        assert!(
            !table
                .get(&LongStringRef::new(1, 1), UTF_8)
                .unwrap()
                .is_binary()
        );
        assert!(
            table
                .get(&LongStringRef::new(2, 2), UTF_8)
                .unwrap()
                .is_binary()
        );
    }

    #[test]
    fn remove_by_key_removes_existing_entry() {
        let mut table = LongStringTable::new();
        table.get_or_insert_by_content(3, 5, b"hello", false);
        let reference = LongStringRef::new(3, 5);

        assert!(table.remove_by_key(&reference));
        assert_eq!(table.len(), 0);
        assert!(table.get(&reference, UTF_8).is_none());
    }

    #[test]
    fn remove_by_key_returns_false_for_missing_entry() {
        let mut table = LongStringTable::new();
        table.get_or_insert_by_content(1, 1, b"present", false);

        let missing = LongStringRef::new(99, 99);
        assert!(!table.remove_by_key(&missing));
        assert_eq!(table.len(), 1);
    }

    #[test]
    fn remove_by_key_clears_content_pointer_when_canonical() {
        let mut table = LongStringTable::new();
        let reference = table.get_or_insert_by_content(3, 5, b"hello", false);
        table.remove_by_key(&reference);

        // After removal, a fresh `by_content` call must be able to
        // assign its own key rather than point back at the gone entry.
        let fresh = table.get_or_insert_by_content(7, 9, b"hello", false);
        assert_eq!(fresh.variable(), 7);
        assert_eq!(fresh.observation(), 9);
    }

    #[test]
    fn remove_by_key_preserves_content_pointer_for_non_canonical_entry() {
        let mut table = LongStringTable::new();
        // (3, 5) is canonical; (7, 9) shares the payload but is not.
        let canonical = table.get_or_insert_by_content(3, 5, b"hello", false);
        table.get_or_insert_by_key(7, 9, b"hello", false);

        // Removing the non-canonical entry must not disturb the
        // content map's pointer to the canonical entry.
        assert!(table.remove_by_key(&LongStringRef::new(7, 9)));
        let later = table.get_or_insert_by_content(99, 99, b"hello", false);
        assert_eq!(later, canonical);
    }

    #[test]
    fn remove_by_key_text_and_binary_tables_are_independent() {
        let mut table = LongStringTable::new();
        table.get_or_insert_by_content(1, 1, b"data", false);
        table.get_or_insert_by_content(2, 2, b"data", true);

        assert!(table.remove_by_key(&LongStringRef::new(1, 1)));
        assert_eq!(table.len(), 1);
        // The binary entry with identical bytes is unaffected.
        let binary = table.get(&LongStringRef::new(2, 2), UTF_8).unwrap();
        assert!(binary.is_binary());
        assert_eq!(binary.data(), b"data");
    }

    #[test]
    fn get_returns_stored_entry_by_ref() {
        let mut table = LongStringTable::new();
        table.get_or_insert_by_content(3, 5, b"hello", false);

        let reference = LongStringRef::new(3, 5);
        let long_string = table.get(&reference, UTF_8).unwrap();
        assert_eq!(long_string.variable(), 3);
        assert_eq!(long_string.observation(), 5);
        assert_eq!(long_string.data(), b"hello");
        assert!(!long_string.is_binary());
    }

    #[test]
    fn get_returns_none_for_missing_ref() {
        let mut table = LongStringTable::new();
        table.get_or_insert_by_content(1, 1, b"only", false);

        let missing = LongStringRef::new(99, 99);
        assert!(table.get(&missing, UTF_8).is_none());
    }

    #[test]
    fn get_preserves_binary_flag() {
        let mut table = LongStringTable::new();
        table.get_or_insert_by_content(2, 2, b"\x00\x01\x02", true);

        let reference = LongStringRef::new(2, 2);
        let long_string = table.get(&reference, UTF_8).unwrap();
        assert!(long_string.is_binary());
        assert_eq!(long_string.data(), b"\x00\x01\x02");
    }

    #[test]
    fn get_uses_caller_supplied_encoding_for_decoding() {
        // 0x80 is Euro sign in Windows-1252; invalid UTF-8.
        let mut table = LongStringTable::new();
        table.get_or_insert_by_content(1, 1, b"\x80", false);

        let reference = LongStringRef::new(1, 1);
        let decoded = table
            .get(&reference, WINDOWS_1252)
            .unwrap()
            .data_str()
            .unwrap()
            .into_owned();
        assert_eq!(decoded, "€");

        assert!(table.get(&reference, UTF_8).unwrap().data_str().is_none());
    }

    #[test]
    fn iter_captures_caller_supplied_encoding() {
        // 0x80 is Euro sign in Windows-1252; invalid UTF-8.
        let mut table = LongStringTable::new();
        table.get_or_insert_by_content(1, 1, b"\x80", false);

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
