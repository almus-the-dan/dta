use super::long_string::{LongString, LongStringContent};
use super::long_string_ref::LongStringRef;
use std::borrow::Cow;
use std::collections::{BTreeMap, HashMap};
use std::rc::Rc;

/// Table of long string (strL / GSO) payloads, used while preparing
/// records for a DTA writer or to resolve [`LongStringRef`]s read from
/// a file.
///
/// The table is constructed in one of two modes, and the mode fixes
/// how [`get_or_insert`](Self::get_or_insert) behaves:
///
/// - [`for_writing`](Self::for_writing) dedupes by payload bytes.
///   Inserting a previously seen payload returns the ref assigned at
///   its first insertion and ignores the caller's `(variable,
///   observation)`. This is the write-side flow — repeated payloads
///   collapse into a single strL entry on disk, and the caller embeds
///   the returned ref in the data section.
/// - [`for_reading`](Self::for_reading) keys on the given `(variable,
///   observation)`. Inserting always returns that key; if the slot is
///   already occupied, the existing entry is kept (first-in wins).
///   This is the read-side flow — the file has already assigned
///   canonical keys, and they must be preserved so that
///   [`LongStringRef`]s from the data section resolve via
///   [`get`](Self::get), so duplicate content is preserved.
///
/// When writing, after writing all records and the chain has advanced to
/// [`LongStringWriter`], iterate the table with [`iter`](Self::iter)
/// and pass each yielded [`LongString`] to
/// [`LongStringWriter::write_long_string`].
///
/// When reading, skip to the long string section and read each long
/// string into the table using [`LongStringReader::read_remaining_into`].
/// Then use [`LongStringReader::seek_records`] to return the beginning
/// of the records section. As records are read, values of type
/// [`Value::LongStringRef`] will provide the observation/variable pairs
/// needed to look up the [`LongString`] using [`get`](Self::get).
///
/// Entries are yielded in `(variable, observation)` order, matching
/// the DTA file layout requirement for the strL section.
///
/// # Memory
///
/// Stored payloads are reference-counted via [`Rc`]. When a
/// reading-mode table is populated with duplicate payloads under
/// different keys, the bytes are held in memory once and shared
/// between position entries.
#[derive(Debug)]
pub struct LongStringTable {
    mode: Mode,
    // Ordered storage: drives the iteration order required by the
    // strL section layout.
    position: BTreeMap<(u32, u64), StoredEntry>,
    // Dedup: text and binary payloads live in separate maps, so the
    // key can be a plain `Rc<[u8]>`. `Rc<T>: Borrow<T>` then lets
    // `get(&[u8])` look up without allocating a query key.
    //
    // The recorded `(variable, observation)` value is meaningful only
    // in `Mode::Writing`, where it points at the canonical (first)
    // inserter for a payload. In `Mode::Reading` the value is still
    // populated but unused — the maps exist purely to share the
    // `Rc<[u8]>` across duplicate content.
    content_text: HashMap<Rc<[u8]>, (u32, u64)>,
    content_binary: HashMap<Rc<[u8]>, (u32, u64)>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Mode {
    Reading,
    Writing,
}

/// Entry held in the location-indexed map. `data` is shared (via
/// `Rc`) with whichever content-indexed map corresponds to `binary`.
#[derive(Debug)]
struct StoredEntry {
    data: Rc<[u8]>,
    binary: bool,
}

impl LongStringTable {
    /// Creates a table for populating from a DTA file's strL section.
    ///
    /// [`get_or_insert`](Self::get_or_insert) preserves the caller's
    /// `(variable, observation)` key. This is the mode to use when
    /// feeding a [`LongStringReader`](super::long_string_reader::LongStringReader)
    /// or its async counterpart into the table.
    #[must_use]
    #[inline]
    pub fn for_reading() -> Self {
        Self::with_mode(Mode::Reading)
    }

    /// Creates a table for collecting strL payloads to be written to a
    /// DTA file.
    ///
    /// [`get_or_insert`](Self::get_or_insert) dedupes by payload
    /// bytes. Repeat payloads collapse into the ref assigned during
    /// the first insertion.
    #[must_use]
    #[inline]
    pub fn for_writing() -> Self {
        Self::with_mode(Mode::Writing)
    }

    fn with_mode(mode: Mode) -> Self {
        Self {
            mode,
            position: BTreeMap::new(),
            content_text: HashMap::new(),
            content_binary: HashMap::new(),
        }
    }

    /// Inserts the given payload, returning a [`LongStringRef`].
    ///
    /// The `content` argument accepts anything convertible into
    /// [`LongStringContent`] — most commonly a `&str` (which borrows
    /// as a [`Text`](LongStringContent::Text) payload) or an explicit
    /// [`Binary`](LongStringContent::Binary) variant.
    ///
    /// Behavior depends on the table's mode:
    ///
    /// - **Writing** ([`for_writing`](Self::for_writing)): dedupes by
    ///   `(bytes, variant)`. If the payload was already inserted, the
    ///   returned ref is the one assigned at its first insertion and
    ///   the caller's `variable`/`observation` are ignored.
    /// - **Reading** ([`for_reading`](Self::for_reading)): keys on the
    ///   caller's `(variable, observation)`. If the slot is already
    ///   occupied, the existing entry is kept (first-in wins, the new
    ///   content is discarded), and the returned ref is always
    ///   `LongStringRef::new(variable, observation)`.
    pub fn get_or_insert<'a>(
        &mut self,
        variable: u32,
        observation: u64,
        content: impl Into<LongStringContent<'a>>,
    ) -> LongStringRef {
        let content = content.into();
        let binary = content.is_binary();
        let data = content.data();
        match self.mode {
            Mode::Writing => self.get_or_insert_by_content(variable, observation, data, binary),
            Mode::Reading => self.get_or_insert_by_key(variable, observation, data, binary),
        }
    }

    fn get_or_insert_by_content(
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

    fn get_or_insert_by_key(
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
        // allocation.
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
    /// In writing mode, when the removed entry was the canonical
    /// content-dedup target (i.e., [`get_or_insert`](Self::get_or_insert)
    /// would have returned this key for its payload), the content map
    /// entry is also cleared so that a subsequent `get_or_insert` call
    /// assigns a fresh canonical target. Other position entries that
    /// happen to share the same payload via a cloned `Rc` are
    /// untouched.
    pub fn remove(&mut self, reference: &LongStringRef) -> bool {
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
    /// table, so the table must outlive it. To decode the bytes as a
    /// string, call
    /// [`LongString::data_str`](super::long_string::LongString::data_str)
    /// with the encoding reported by the reader that produced the
    /// entry (see each reader's `encoding()` accessor).
    #[must_use]
    pub fn get(&self, reference: &LongStringRef) -> Option<LongString<'_>> {
        let key = (reference.variable(), reference.observation());
        let entry = self.position.get(&key)?;
        Some(LongString::new(
            reference.variable(),
            reference.observation(),
            make_content(entry.binary, Cow::Borrowed(&entry.data)),
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
    /// [`LongStringWriter::write_long_string`]. The returned iterator
    /// borrows `self` and nothing else, so callers can freely invoke
    /// `write_long_string` inside the loop.
    pub fn iter(&self) -> impl Iterator<Item = LongString<'_>> + '_ {
        self.position
            .iter()
            .map(move |(&(variable, observation), entry)| {
                LongString::new(
                    variable,
                    observation,
                    make_content(entry.binary, Cow::Borrowed(&*entry.data)),
                )
            })
    }
}

/// Reconstructs a [`LongStringContent`] from the table's internal
/// split representation (a `binary` bool plus payload bytes).
#[inline]
fn make_content(binary: bool, data: Cow<'_, [u8]>) -> LongStringContent<'_> {
    if binary {
        LongStringContent::Binary(data)
    } else {
        LongStringContent::Text(data)
    }
}

#[cfg(test)]
mod tests {
    use encoding_rs::{UTF_8, WINDOWS_1252};

    use super::*;

    #[test]
    fn for_reading_is_empty() {
        let table = LongStringTable::for_reading();
        assert_eq!(table.len(), 0);
        assert!(table.is_empty());
    }

    #[test]
    fn for_writing_is_empty() {
        let table = LongStringTable::for_writing();
        assert_eq!(table.len(), 0);
        assert!(table.is_empty());
    }

    // -- Writing mode -------------------------------------------------------

    #[test]
    fn writing_insert_new_returns_given_key() {
        let mut table = LongStringTable::for_writing();
        let reference = table.get_or_insert(3, 5, LongStringContent::Text(Cow::Borrowed(b"hello")));
        assert_eq!(reference.variable(), 3);
        assert_eq!(reference.observation(), 5);
        assert_eq!(table.len(), 1);
    }

    #[test]
    fn writing_insert_duplicate_returns_original_ref() {
        let mut table = LongStringTable::for_writing();
        let first = table.get_or_insert(3, 5, LongStringContent::Text(Cow::Borrowed(b"hello")));
        let second = table.get_or_insert(7, 99, LongStringContent::Text(Cow::Borrowed(b"hello")));
        assert_eq!(first, second);
        assert_eq!(table.len(), 1);
    }

    #[test]
    fn writing_different_binary_flag_is_distinct() {
        let mut table = LongStringTable::for_writing();
        let text_ref = table.get_or_insert(
            1,
            1,
            LongStringContent::Text(Cow::Borrowed(b"\x00\x01\x02")),
        );
        let binary_ref = table.get_or_insert(
            2,
            2,
            LongStringContent::Binary(Cow::Borrowed(b"\x00\x01\x02")),
        );
        assert_ne!(text_ref, binary_ref);
        assert_eq!(table.len(), 2);
    }

    #[test]
    fn writing_distinct_payloads_are_stored_separately() {
        let mut table = LongStringTable::for_writing();
        table.get_or_insert(1, 1, LongStringContent::Text(Cow::Borrowed(b"a")));
        table.get_or_insert(1, 2, LongStringContent::Text(Cow::Borrowed(b"b")));
        table.get_or_insert(1, 3, LongStringContent::Text(Cow::Borrowed(b"c")));
        assert_eq!(table.len(), 3);
    }

    #[test]
    fn writing_is_empty_tracks_insertion() {
        let mut table = LongStringTable::for_writing();
        assert!(table.is_empty());
        table.get_or_insert(1, 1, LongStringContent::Text(Cow::Borrowed(b"x")));
        assert!(!table.is_empty());
    }

    // -- Reading mode -------------------------------------------------------

    #[test]
    fn reading_insert_new_entry() {
        let mut table = LongStringTable::for_reading();
        let reference = table.get_or_insert(3, 5, LongStringContent::Text(Cow::Borrowed(b"hello")));
        assert_eq!(reference.variable(), 3);
        assert_eq!(reference.observation(), 5);
        assert_eq!(table.len(), 1);
        let stored = table.get(&reference).unwrap();
        assert_eq!(stored.data(), b"hello");
    }

    #[test]
    fn reading_always_returns_passed_key() {
        let mut table = LongStringTable::for_reading();
        // Even though the same payload could dedupe under a writing
        // table, reading mode must honor the caller's key.
        let first = table.get_or_insert(3, 5, LongStringContent::Text(Cow::Borrowed(b"hello")));
        let second = table.get_or_insert(7, 9, LongStringContent::Text(Cow::Borrowed(b"hello")));
        assert_eq!(first.variable(), 3);
        assert_eq!(first.observation(), 5);
        assert_eq!(second.variable(), 7);
        assert_eq!(second.observation(), 9);
        assert_eq!(table.len(), 2);
    }

    #[test]
    fn reading_is_first_wins_on_key_collision() {
        let mut table = LongStringTable::for_reading();
        table.get_or_insert(3, 5, LongStringContent::Text(Cow::Borrowed(b"first")));
        // Second call on the same key with different data is a no-op.
        let reference =
            table.get_or_insert(3, 5, LongStringContent::Text(Cow::Borrowed(b"second")));
        assert_eq!(reference.variable(), 3);
        assert_eq!(reference.observation(), 5);
        assert_eq!(table.len(), 1);
        let stored = table.get(&reference).unwrap();
        assert_eq!(stored.data(), b"first");
    }

    #[test]
    fn reading_shares_payload_across_duplicate_content() {
        let mut table = LongStringTable::for_reading();
        table.get_or_insert(3, 5, LongStringContent::Text(Cow::Borrowed(b"hello")));
        table.get_or_insert(7, 9, LongStringContent::Text(Cow::Borrowed(b"hello")));

        // Both keys resolve to the same payload.
        let first = table.get(&LongStringRef::new(3, 5)).unwrap();
        let second = table.get(&LongStringRef::new(7, 9)).unwrap();
        assert_eq!(first.data(), b"hello");
        assert_eq!(second.data(), b"hello");
    }

    #[test]
    fn reading_separates_text_and_binary() {
        let mut table = LongStringTable::for_reading();
        let text = table.get_or_insert(
            1,
            1,
            LongStringContent::Text(Cow::Borrowed(b"\x00\x01\x02")),
        );
        let binary = table.get_or_insert(
            2,
            2,
            LongStringContent::Binary(Cow::Borrowed(b"\x00\x01\x02")),
        );
        assert_ne!(text, binary);
        assert_eq!(table.len(), 2);
        assert!(!table.get(&LongStringRef::new(1, 1)).unwrap().is_binary());
        assert!(table.get(&LongStringRef::new(2, 2)).unwrap().is_binary());
    }

    // -- iter ---------------------------------------------------------------

    #[test]
    fn iter_yields_in_variable_then_observation_order() {
        let mut table = LongStringTable::for_writing();
        table.get_or_insert(3, 1, LongStringContent::Text(Cow::Borrowed(b"c1")));
        table.get_or_insert(1, 2, LongStringContent::Text(Cow::Borrowed(b"a2")));
        table.get_or_insert(2, 5, LongStringContent::Text(Cow::Borrowed(b"b5")));
        table.get_or_insert(1, 1, LongStringContent::Text(Cow::Borrowed(b"a1")));

        let ordered: Vec<(u32, u64)> = table
            .iter()
            .map(|ls| (ls.variable(), ls.observation()))
            .collect();
        assert_eq!(ordered, vec![(1, 1), (1, 2), (2, 5), (3, 1)]);
    }

    #[test]
    fn iter_preserves_data_and_binary_flag() {
        let mut table = LongStringTable::for_writing();
        table.get_or_insert(1, 1, LongStringContent::Text(Cow::Borrowed(b"text")));
        table.get_or_insert(2, 2, LongStringContent::Binary(Cow::Borrowed(b"\x00\x01")));

        let long_strings: Vec<_> = table.iter().collect();
        assert_eq!(long_strings[0].data(), b"text");
        assert!(!long_strings[0].is_binary());
        assert_eq!(long_strings[1].data(), b"\x00\x01");
        assert!(long_strings[1].is_binary());
    }

    // -- remove_by_key ------------------------------------------------------

    #[test]
    fn remove_removes_existing_entry() {
        let mut table = LongStringTable::for_writing();
        table.get_or_insert(3, 5, LongStringContent::Text(Cow::Borrowed(b"hello")));
        let reference = LongStringRef::new(3, 5);

        assert!(table.remove(&reference));
        assert_eq!(table.len(), 0);
        assert!(table.get(&reference).is_none());
    }

    #[test]
    fn remove_returns_false_for_missing_entry() {
        let mut table = LongStringTable::for_writing();
        table.get_or_insert(1, 1, LongStringContent::Text(Cow::Borrowed(b"present")));

        let missing = LongStringRef::new(99, 99);
        assert!(!table.remove(&missing));
        assert_eq!(table.len(), 1);
    }

    #[test]
    fn remove_clears_content_pointer_when_canonical() {
        let mut table = LongStringTable::for_writing();
        let reference = table.get_or_insert(3, 5, LongStringContent::Text(Cow::Borrowed(b"hello")));
        table.remove(&reference);

        // After removal, a fresh insert must be able to assign its
        // own key rather than point back at the gone entry.
        let fresh = table.get_or_insert(7, 9, LongStringContent::Text(Cow::Borrowed(b"hello")));
        assert_eq!(fresh.variable(), 7);
        assert_eq!(fresh.observation(), 9);
    }

    #[test]
    fn remove_text_and_binary_tables_are_independent() {
        let mut table = LongStringTable::for_writing();
        table.get_or_insert(1, 1, LongStringContent::Text(Cow::Borrowed(b"data")));
        table.get_or_insert(2, 2, LongStringContent::Binary(Cow::Borrowed(b"data")));

        assert!(table.remove(&LongStringRef::new(1, 1)));
        assert_eq!(table.len(), 1);
        // The binary entry with identical bytes is unaffected.
        let binary = table.get(&LongStringRef::new(2, 2)).unwrap();
        assert!(binary.is_binary());
        assert_eq!(binary.data(), b"data");
    }

    // -- get ----------------------------------------------------------------

    #[test]
    fn get_returns_stored_entry_by_ref() {
        let mut table = LongStringTable::for_writing();
        table.get_or_insert(3, 5, LongStringContent::Text(Cow::Borrowed(b"hello")));

        let reference = LongStringRef::new(3, 5);
        let long_string = table.get(&reference).unwrap();
        assert_eq!(long_string.variable(), 3);
        assert_eq!(long_string.observation(), 5);
        assert_eq!(long_string.data(), b"hello");
        assert!(!long_string.is_binary());
    }

    #[test]
    fn get_returns_none_for_missing_ref() {
        let mut table = LongStringTable::for_writing();
        table.get_or_insert(1, 1, LongStringContent::Text(Cow::Borrowed(b"only")));

        let missing = LongStringRef::new(99, 99);
        assert!(table.get(&missing).is_none());
    }

    #[test]
    fn get_preserves_binary_flag() {
        let mut table = LongStringTable::for_writing();
        table.get_or_insert(
            2,
            2,
            LongStringContent::Binary(Cow::Borrowed(b"\x00\x01\x02")),
        );

        let reference = LongStringRef::new(2, 2);
        let long_string = table.get(&reference).unwrap();
        assert!(long_string.is_binary());
        assert_eq!(long_string.data(), b"\x00\x01\x02");
    }

    #[test]
    fn data_str_uses_caller_supplied_encoding() {
        // 0x80 is Euro sign in Windows-1252; invalid UTF-8.
        let mut table = LongStringTable::for_writing();
        table.get_or_insert(1, 1, LongStringContent::Text(Cow::Borrowed(b"\x80")));

        let reference = LongStringRef::new(1, 1);
        let decoded = table
            .get(&reference)
            .unwrap()
            .data_str(WINDOWS_1252)
            .unwrap()
            .into_owned();
        assert_eq!(decoded, "€");

        assert!(table.get(&reference).unwrap().data_str(UTF_8).is_none());
    }

    #[test]
    fn iter_decoding_uses_caller_supplied_encoding() {
        // 0x80 is Euro sign in Windows-1252; invalid UTF-8.
        let mut table = LongStringTable::for_writing();
        table.get_or_insert(1, 1, LongStringContent::Text(Cow::Borrowed(b"\x80")));

        let decoded = table
            .iter()
            .next()
            .unwrap()
            .data_str(WINDOWS_1252)
            .unwrap()
            .into_owned();
        assert_eq!(decoded, "€");

        assert!(table.iter().next().unwrap().data_str(UTF_8).is_none());
    }
}
