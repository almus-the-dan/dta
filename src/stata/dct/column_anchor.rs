/// Where a column's first byte sits within its physical line.
///
/// Most DCT columns have an [`Absolute`](Self::Absolute) anchor —
/// either declared via `_column(#)` or accumulated through a chain
/// of fixed-width reads back to a `_column(#)` (or `_newline`)
/// anchor. Those slice directly: `&line[o..]`.
///
/// The [`RelativeToCursor`](Self::RelativeToCursor) variant only
/// arises when a free-format read (`%f`, `%g`, `%e`, `%s` with no
/// width) sits between this column and the previous absolute anchor
/// on the same physical line. A free-format read consumes input
/// dynamically — the parser can't statically know where it ends —
/// so this column's start position has to be resolved at runtime by
/// re-simulating the predecessor reads against the actual line bytes.
///
/// The reader handles that resolution transparently. Consumers that
/// just want to read records via `read_record` / `read_lazy_record`
/// don't need to do anything special; this enum surfaces the
/// distinction for callers who want to inspect the schema directly.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ColumnAnchor {
    /// Statically resolvable byte offset within the column's
    /// physical line. The reader can slice `line[offset..]` directly.
    Absolute(usize),
    /// Runtime resolution required: the column's start depends on
    /// where the previous variable's read landed. The reader walks
    /// from the most recent [`Absolute`](Self::Absolute) anchor on
    /// the same line, simulating each intermediate read against the
    /// actual data, then adds `skip` to the resulting cursor.
    RelativeToCursor {
        /// `_skip(N)` amount stacked on top of the resolved cursor.
        /// Zero when no `_skip` was specified.
        skip: usize,
    },
}

impl ColumnAnchor {
    /// Returns `Some(offset)` for [`Absolute`](Self::Absolute)
    /// anchors, `None` for runtime-resolved ones.
    #[must_use]
    pub fn static_offset(self) -> Option<usize> {
        match self {
            Self::Absolute(offset) => Some(offset),
            Self::RelativeToCursor { .. } => None,
        }
    }
}
