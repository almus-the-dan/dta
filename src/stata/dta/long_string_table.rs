use std::collections::{BTreeMap, HashMap};
use std::rc::Rc;

use super::long_string::LongString;
use super::long_string_ref::LongStringRef;
use super::long_string_writer::LongStringWriter;

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
/// After all records are written and the chain has advanced to
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
    by_location: BTreeMap<(u32, u64), Rc<StoredEntry>>,
    by_content: HashMap<Rc<StoredEntry>, (u32, u64)>,
}

/// Interned `(data, binary)` payload shared between the location-
/// and content-indexed maps.
#[derive(Debug, Hash, Eq, PartialEq)]
struct StoredEntry {
    data: Vec<u8>,
    binary: bool,
}

impl LongStringTable {
    /// Creates an empty table.
    #[must_use]
    #[inline]
    pub fn new() -> Self {
        todo!()
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
        _variable: u32,
        _observation: u64,
        _data: &[u8],
        _binary: bool,
    ) -> LongStringRef {
        todo!()
    }

    /// Number of unique payloads stored.
    #[must_use]
    #[inline]
    pub fn len(&self) -> usize {
        todo!()
    }

    /// `true` when the table holds no entries.
    #[must_use]
    #[inline]
    pub fn is_empty(&self) -> bool {
        todo!()
    }

    /// Yields stored entries in `(variable, observation)` order,
    /// ready to be passed to
    /// [`LongStringWriter::write_long_string`].
    ///
    /// The `writer` argument is used once, synchronously, to capture
    /// the file's encoding for the resulting [`LongString`] values.
    /// The returned iterator borrows `self` but not the writer, so
    /// callers can freely invoke `write_long_string` inside the
    /// loop.
    pub fn iter<'a, W>(
        &'a self,
        _writer: &LongStringWriter<W>,
    ) -> impl Iterator<Item = LongString<'a>> + 'a {
        // Placeholder: `todo!()` doesn't unify with `impl Iterator`
        // in return position, so we return an empty iterator that
        // will be replaced during implementation.
        std::iter::empty()
    }
}
