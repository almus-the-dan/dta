use super::column::Column;
use super::dct_error::{DctError, Result};
use super::dct_reader::parse_field;
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
}

impl<'a> LazyRecord<'a> {
    #[must_use]
    pub(crate) fn new(lines: &'a [String], columns: &'a [Column], observation: usize) -> Self {
        Self {
            lines,
            columns,
            observation,
        }
    }

    /// The columns from the schema this record was read against.
    #[must_use]
    #[inline]
    pub fn columns(&self) -> &'a [Column] {
        self.columns
    }

    /// 1-based observation number this record corresponds to.
    #[must_use]
    #[inline]
    pub fn observation(&self) -> usize {
        self.observation
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
    /// column list. Values are decoded from the raw line buffers on
    /// every call — no caching is performed, so repeated `value(i)`
    /// calls re-parse.
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
        // Warnings for lazy parses are dropped on the floor — see the
        // type-level doc comment.
        let mut sink = Vec::new();
        parse_field(line, column, self.observation, &mut sink)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::stata::dct::dct_source::DctSource;
    use crate::stata::dct::parser::parse_dct;
    use crate::stata::stata_byte::StataByte;
    use crate::stata::stata_int::StataInt;
    use std::io::Cursor;

    fn parse_with_data(input: &[u8]) -> crate::stata::dct::dct_reader::DctReader<Cursor<&[u8]>> {
        let source = parse_dct(Cursor::new(input)).unwrap();
        match source {
            DctSource::Embedded(reader) => reader,
            DctSource::External(_) => panic!("expected embedded data"),
        }
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
    fn lazy_record_observation_number_matches_position() {
        let input = b"dictionary {\n\
            _column(1) byte b1 %3.0f\n\
            }\n\
            001\n\
            002\n";
        let mut reader = parse_with_data(input);
        {
            let r1 = reader.read_lazy_record().unwrap().unwrap();
            assert_eq!(r1.observation(), 1);
        }
        {
            let r2 = reader.read_lazy_record().unwrap().unwrap();
            assert_eq!(r2.observation(), 2);
        }
    }
}
