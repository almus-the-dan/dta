use super::column::Column;
use super::column_anchor::ColumnAnchor;
use super::dct_error::{DctError, Result};
use super::dct_reader_state::{RelativeOffsetCache, parse_field, resolve_runtime_offset};
use super::value::Value;

/// A single observation that decodes its values on demand.
///
/// Unlike [`Record`](super::record::Record), a `LazyRecord` holds the
/// raw line buffer(s) for the observation and decodes individual
/// [`Value`]s only when [`value`](Self::value) is called. Useful when
/// the caller only needs a subset of columns per row — skip the
/// parse work for everything else.
///
/// String values borrow from the reader's line buffers, so a
/// `LazyRecord` must be dropped before the next read.
///
/// # Free-format chains
///
/// Columns whose [`anchor`](Column::anchor) is
/// [`ColumnAnchor::Absolute`] decode in O(1): the byte offset is
/// statically known and the value is sliced directly. Columns
/// downstream of a free-format predecessor have
/// [`ColumnAnchor::RelativeToCursor`] and need a runtime walk against
/// the actual line bytes. `LazyRecord` caches the resolved runtime
/// offset for each such column so repeated `value(i)` calls don't
/// re-walk.
///
/// # Warnings
///
/// `LazyRecord::value` discards any warnings the underlying field
/// parser would have emitted (e.g.,
/// [`BlankFieldTreatedAsMissing`](super::dct_warning::DctWarning::BlankFieldTreatedAsMissing),
/// [`IntegerPromotion`](super::dct_warning::DctWarning::IntegerPromotion)).
/// `LazyRecord` is the fast path; if you need per-field diagnostics,
/// use the eager [`Record`](super::record::Record) path via
/// [`DctReader::read_record`](super::dct_reader::DctReader::read_record).
#[derive(Debug)]
pub struct LazyRecord<'a> {
    lines: &'a [String],
    columns: &'a [Column],
    observation: usize,
    /// Per-column cache of resolved runtime offsets, borrowed from
    /// the parent `DctReaderState` so the allocation is reused
    /// across observations. Populated lazily and only for columns
    /// whose anchor is [`ColumnAnchor::RelativeToCursor`]; absolute
    /// columns skip the cache and slice directly. `RefCell` lets
    /// `value(i)` stay `&self` while still mutating the cache.
    relative_offset_cache: &'a RelativeOffsetCache,
}

impl<'a> LazyRecord<'a> {
    #[must_use]
    pub(crate) fn new(
        lines: &'a [String],
        columns: &'a [Column],
        observation: usize,
        relative_offset_cache: &'a RelativeOffsetCache,
    ) -> Self {
        Self {
            lines,
            columns,
            observation,
            relative_offset_cache,
        }
    }

    /// Number of variables (columns) in this record.
    #[must_use]
    #[inline]
    pub fn len(&self) -> usize {
        self.columns.len()
    }

    /// Whether the record is empty (the schema declared no
    /// variables). Should not happen for any real DCT file.
    #[must_use]
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.columns.is_empty()
    }

    /// Parses and returns the value at `index`.
    ///
    /// The column index corresponds to the position in the schema's
    /// column list. Field decoding is not cached, so repeated
    /// `value(i)` calls re-parse the bytes — but the runtime offset
    /// (only required for columns downstream of a free-format read)
    /// is cached.
    ///
    /// # Errors
    ///
    /// Returns [`DctError`] when `index` is out of bounds, the
    /// computed offset overflows, or the field can't be parsed
    /// against its declared type and read format.
    pub fn value(&self, index: usize) -> Result<Value<'a>> {
        let column = self
            .columns
            .get(index)
            .ok_or_else(|| DctError::Io(std::io::Error::other("column index out of bounds")))?;
        let line = self.lines.get(column.line_offset()).ok_or_else(|| {
            DctError::Io(std::io::Error::other(
                "internal invariant violated: line_offset exceeds lines_per_observation",
            ))
        })?;

        let runtime_offset = match column.anchor() {
            ColumnAnchor::Absolute(offset) => offset,
            ColumnAnchor::RelativeToCursor { .. } => self.resolve_relative(index, line)?,
        };

        // Warnings for lazy parses are dropped on the floor — see the
        // type-level doc comment.
        parse_field(line, runtime_offset, column, self.observation, None)
    }

    fn resolve_relative(&self, index: usize, line: &str) -> Result<usize> {
        if let Some(cached) = self.relative_offset_cache.borrow()[index] {
            return Ok(cached);
        }
        let resolved = resolve_runtime_offset(line, self.columns, index, self.observation)?;
        self.relative_offset_cache.borrow_mut()[index] = Some(resolved);
        Ok(resolved)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::stata::dct::dct_source::DctSource;
    use crate::stata::stata_byte::StataByte;
    use crate::stata::stata_int::StataInt;
    use std::io::Cursor;

    fn parse_with_data(input: &[u8]) -> crate::stata::dct::dct_reader::DctReader<Cursor<&[u8]>> {
        let source = DctSource::options()
            .from_reader(Cursor::new(input))
            .unwrap();
        let DctSource::Embedded { schema, reader } = source else {
            panic!("expected embedded data")
        };
        crate::stata::dct::dct_reader::DctReader::options(schema).from_reader(reader)
    }

    #[test]
    fn lazy_record_decodes_individual_columns() {
        let input = b"dictionary {\n\
            _column(1) byte b1 %3.0f\n\
            _column(4) int i1 %5.0f\n\
            _column(9) str s1 %5s\n\
            }\n\
            04212345hello\n";
        let mut reader = parse_with_data(input);
        let record = reader.read_lazy_record().unwrap().unwrap();
        assert_eq!(record.len(), 3);

        // Decode out of order — only what the caller asks for.
        match record.value(2).unwrap() {
            Value::String(s) => assert_eq!(s.as_ref(), "hello"),
            other => panic!("expected string, got {other:?}"),
        }
        assert!(matches!(
            record.value(0).unwrap(),
            Value::Byte(StataByte::Present(42))
        ));
        assert!(matches!(
            record.value(1).unwrap(),
            Value::Int(StataInt::Present(12345))
        ));
    }

    #[test]
    fn lazy_record_index_out_of_bounds_errors() {
        let input = b"dictionary {\n\
            _column(1) byte b1 %3.0f\n\
            }\n\
            042\n";
        let mut reader = parse_with_data(input);
        let record = reader.read_lazy_record().unwrap().unwrap();
        assert!(record.value(99).is_err());
    }

    #[test]
    fn lazy_reader_iterates_to_eof() {
        let input = b"dictionary {\n\
            _column(1) byte b1 %3.0f\n\
            }\n\
            001\n\
            002\n\
            003\n";
        let mut reader = parse_with_data(input);
        let mut count = 0;
        while let Some(record) = reader.read_lazy_record().unwrap() {
            // Touch the value so we exercise the lazy path.
            assert!(matches!(record.value(0).unwrap(), Value::Byte(_)));
            count += 1;
        }
        assert_eq!(count, 3);
    }

    #[test]
    fn lazy_record_resolves_free_format_chain_at_runtime() {
        // Three free-format byte columns. Only the first has an
        // explicit `_column(#)`; the other two depend on where the
        // previous read landed.
        let input = b"dictionary {\n\
            _column(1) byte b1 %f\n\
            byte b2 %f\n\
            byte b3 %f\n\
            }\n\
            10 20 30\n";
        let mut reader = parse_with_data(input);
        let record = reader.read_lazy_record().unwrap().unwrap();
        assert!(matches!(
            record.value(0).unwrap(),
            Value::Byte(StataByte::Present(10))
        ));
        assert!(matches!(
            record.value(1).unwrap(),
            Value::Byte(StataByte::Present(20))
        ));
        assert!(matches!(
            record.value(2).unwrap(),
            Value::Byte(StataByte::Present(30))
        ));
    }

    #[test]
    fn lazy_record_resolves_skip_after_free_format_at_runtime() {
        // _skip(2) after a free-format predecessor: the skip is
        // statically known, the cursor it stacks on isn't. b1's
        // free-numeric read stops at the first whitespace (cursor
        // lands at byte 2); _skip(2) pushes b2's start to byte 4.
        let input = b"dictionary {\n\
            _column(1) byte b1 %f\n\
            _skip(2) byte b2 %f\n\
            }\n\
            10  20\n";
        let mut reader = parse_with_data(input);
        let record = reader.read_lazy_record().unwrap().unwrap();
        assert!(matches!(
            record.value(0).unwrap(),
            Value::Byte(StataByte::Present(10))
        ));
        assert!(matches!(
            record.value(1).unwrap(),
            Value::Byte(StataByte::Present(20))
        ));
    }
}
