//! Sync I/O wrapper that drives [`DctSourceState`] from a
//! [`BufRead`] source.
//!
//! All parsing logic lives in
//! [`dct_source_state`](super::dct_source_state); this module only
//! handles the sync read loop. The async counterpart will live in
//! `async_parser.rs` (feature-gated) and share the same state.

use std::io::BufRead;

use super::dct_error::{DctError, Result};
use super::dct_reader::DctReader;
use super::dct_source::DctSource;
use super::dct_source_state::{DctSourceState, FeedOutcome};

/// Parses a `.dct` dictionary from a buffered reader.
///
/// Crate-private. Public callers go through
/// [`DctSource::options`](crate::stata::dct::dct_source::DctSource::options)
/// and the
/// [`DctSourceOptions`](crate::stata::dct::dct_source_options::DctSourceOptions)
/// builder so future configuration knobs can land without breaking
/// the construction surface.
pub(super) fn parse_dct<R: BufRead>(mut reader: R) -> Result<DctSource<R>> {
    let mut state = DctSourceState::new();
    loop {
        let read = reader.read_line(state.buffer_mut())?;
        if read == 0 {
            return Err(DctError::UnexpectedEofInDictionary);
        }
        let result = state.feed_buffered_line()?;
        if matches!(result, FeedOutcome::Done) {
            break;
        }
    }

    let schema = state.into_schema();
    let source = if has_more_data(&mut reader)? {
        let reader = DctReader::new(schema, reader, true);
        DctSource::Embedded(reader)
    } else {
        DctSource::External(schema)
    };
    Ok(source)
}

fn has_more_data<R: BufRead>(reader: &mut R) -> Result<bool> {
    let buffer = reader.fill_buf()?;
    Ok(!buffer.is_empty())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::stata::dct::column_anchor::ColumnAnchor;
    use crate::stata::dct::dct_warning::DctWarning;
    use crate::stata::dct::input_format::InputFormat;
    use crate::stata::dct::numeric_style::NumericStyle;
    use crate::stata::dct::variable_type::VariableType;
    use std::io::Cursor;

    fn parse(input: &[u8]) -> Result<DctSource<Cursor<&[u8]>>> {
        parse_dct(Cursor::new(input))
    }

    #[test]
    fn parses_minimal_dictionary() {
        let src = parse(b"dictionary {\n_column(1) myvar\n}\n").unwrap();
        let schema = src.schema();
        assert_eq!(schema.columns().len(), 1);
        assert_eq!(schema.columns()[0].name(), "myvar");
        assert_eq!(schema.columns()[0].anchor(), ColumnAnchor::Absolute(0));
        assert_eq!(schema.columns()[0].storage_type(), VariableType::Float);
        assert!(matches!(
            schema.columns()[0].input_format(),
            InputFormat::FreeNumeric
        ));
        assert_eq!(schema.declared_data_path(), None);
        assert!(matches!(src, DctSource::External(_)));
    }

    #[test]
    fn parses_using_clause_emits_path_warning() {
        let src = parse(b"dictionary using foo.raw {\n_column(1) v\n}\n").unwrap();
        assert_eq!(src.schema().declared_data_path(), Some("foo.raw"));
        assert!(
            src.schema().warnings().iter().any(
                |w| matches!(w, DctWarning::DeclaredPathIgnored { path } if path == "foo.raw")
            )
        );
    }

    #[test]
    fn parses_quoted_using_path() {
        let src = parse(b"dictionary using \"path with spaces.raw\" {\n_column(1) v\n}\n").unwrap();
        assert_eq!(
            src.schema().declared_data_path(),
            Some("path with spaces.raw")
        );
    }

    #[test]
    fn parses_infile_prefix() {
        let src = parse(b"infile dictionary using foo.raw {\n_column(1) v\n}\n").unwrap();
        assert_eq!(src.schema().declared_data_path(), Some("foo.raw"));
    }

    #[test]
    fn handles_brace_on_separate_line() {
        let src = parse(b"dictionary using foo.raw\n{\n_column(1) v\n}\n").unwrap();
        assert_eq!(src.schema().declared_data_path(), Some("foo.raw"));
        assert_eq!(src.schema().columns().len(), 1);
    }

    #[test]
    fn skips_comments_inside_and_around_dictionary() {
        let src = parse(
            b"* leading comment\n\
              dictionary {\n\
              * inline comment\n\
              _column(1) v\n\
              }\n",
        )
        .unwrap();
        assert_eq!(src.schema().columns().len(), 1);
    }

    #[test]
    fn parses_storage_types_and_formats() {
        let dict = b"dictionary {\n\
            _column(1) byte b1 %3.0f \"a byte\"\n\
            _column(4) int i1\n\
            _column(6) long l1 %10.0f\n\
            _column(16) float f1 %9.2f\n\
            _column(25) double d1\n\
            _column(33) str s1 %20s\n\
            _column(53) str10 s2\n\
            _column(63) str s3 \"free string\"\n\
            }\n";
        let src = parse(dict).unwrap();
        let cols = src.schema().columns();
        assert_eq!(cols.len(), 8);

        assert_eq!(cols[0].storage_type(), VariableType::Byte);
        assert_eq!(cols[0].label(), Some("a byte"));
        assert!(matches!(
            cols[0].input_format(),
            InputFormat::FixedNumeric {
                width: 3,
                decimals: 0,
                style: NumericStyle::Fixed,
            }
        ));

        assert_eq!(cols[1].storage_type(), VariableType::Int);
        assert!(matches!(cols[1].input_format(), InputFormat::FreeNumeric));

        assert_eq!(cols[2].storage_type(), VariableType::Long);
        assert_eq!(cols[3].storage_type(), VariableType::Float);
        assert_eq!(cols[4].storage_type(), VariableType::Double);

        assert_eq!(cols[5].storage_type(), VariableType::String);
        assert!(matches!(
            cols[5].input_format(),
            InputFormat::FixedString { width: 20 }
        ));

        // str10 (storage hint, no %infmt) → FixedString { 10 }
        assert_eq!(cols[6].storage_type(), VariableType::String);
        assert!(matches!(
            cols[6].input_format(),
            InputFormat::FixedString { width: 10 }
        ));

        // bare str (no width, no %infmt) → FreeString. Has label.
        assert_eq!(cols[7].storage_type(), VariableType::String);
        assert!(matches!(cols[7].input_format(), InputFormat::FreeString));
        assert_eq!(cols[7].label(), Some("free string"));
    }

    #[test]
    fn parses_lrecl() {
        let src = parse(b"dictionary {\nlrecl(80)\n_column(1) v\n}\n").unwrap();
        assert_eq!(src.schema().logical_record_length(), Some(80));
    }

    #[test]
    fn parses_first_line_of_file() {
        let src = parse(b"dictionary {\nfirstlineoffile(5)\n_column(1) v\n}\n").unwrap();
        assert_eq!(src.schema().first_line_of_file(), Some(5));
    }

    #[test]
    fn rejects_duplicate_lrecl() {
        let result = parse(b"dictionary {\nlrecl(80)\nlrecl(120)\n_column(1) v\n}\n");
        assert!(matches!(
            result,
            Err(DctError::DuplicateDirective { directive, .. }) if directive == "lrecl"
        ));
    }

    #[test]
    fn skip_modifier_advances_cursor_relatively() {
        // _skip(M) without _column anchors relative to the running
        // cursor, which is itself advanced by each fixed-width read.
        let dict = b"dictionary {\n\
            _column(1) byte b1 %3.0f\n\
            _skip(2) byte b2 %3.0f\n\
            }\n";
        let src = parse(dict).unwrap();
        let cols = src.schema().columns();
        assert_eq!(cols.len(), 2);
        assert_eq!(cols[0].anchor(), ColumnAnchor::Absolute(0));
        // b1 finishes at byte 3 (0 + width 3); _skip(2) → 5.
        assert_eq!(cols[1].anchor(), ColumnAnchor::Absolute(5));
    }

    #[test]
    fn column_then_skip_combines() {
        // _column(N) sets absolute, then _skip(M) advances from there.
        let dict = b"dictionary {\n\
            _column(10) _skip(3) byte b1 %3.0f\n\
            }\n";
        let src = parse(dict).unwrap();
        let cols = src.schema().columns();
        // _column(10) → 0-based 9, then _skip(3) → 12.
        assert_eq!(cols[0].anchor(), ColumnAnchor::Absolute(12));
    }

    #[test]
    fn bare_skip_advances_one_byte() {
        let dict = b"dictionary {\n\
            _column(1) byte b1 %3.0f\n\
            _skip byte b2 %3.0f\n\
            }\n";
        let src = parse(dict).unwrap();
        let cols = src.schema().columns();
        // b1 ends at 3; bare _skip → 4.
        assert_eq!(cols[1].anchor(), ColumnAnchor::Absolute(4));
    }

    #[test]
    fn variable_with_no_anchors_uses_running_cursor() {
        // Bare-storage continuation lines pick up where the previous
        // variable ended.
        let dict = b"dictionary {\n\
            _column(1) byte b1 %3.0f\n\
            byte b2 %3.0f\n\
            byte b3 %3.0f\n\
            }\n";
        let src = parse(dict).unwrap();
        let cols = src.schema().columns();
        assert_eq!(cols.len(), 3);
        assert_eq!(cols[0].anchor(), ColumnAnchor::Absolute(0));
        assert_eq!(cols[1].anchor(), ColumnAnchor::Absolute(3));
        assert_eq!(cols[2].anchor(), ColumnAnchor::Absolute(6));
    }

    #[test]
    fn newline_resets_cursor_to_line_start() {
        let dict = b"dictionary {\n\
            _column(1) byte b1 %3.0f\n\
            _skip(5) byte b2 %3.0f\n\
            _newline\n\
            _skip(2) int i1 %5.0f\n\
            }\n";
        let src = parse(dict).unwrap();
        let cols = src.schema().columns();
        assert_eq!(cols[0].line_offset(), 0);
        assert_eq!(cols[0].anchor(), ColumnAnchor::Absolute(0));
        // b1 ends at 3, _skip(5) → 8.
        assert_eq!(cols[1].line_offset(), 0);
        assert_eq!(cols[1].anchor(), ColumnAnchor::Absolute(8));
        // _newline resets cursor to 0 on a new physical line, then
        // _skip(2) → 2.
        assert_eq!(cols[2].line_offset(), 1);
        assert_eq!(cols[2].anchor(), ColumnAnchor::Absolute(2));
    }

    #[test]
    fn detects_embedded_data() {
        let src = parse(b"dictionary {\n_column(1) v\n}\n42\n").unwrap();
        assert!(matches!(src, DctSource::Embedded(_)));
    }

    #[test]
    fn detects_external_data() {
        let src = parse(b"dictionary {\n_column(1) v\n}\n").unwrap();
        assert!(matches!(src, DctSource::External(_)));
    }

    #[test]
    fn handles_crlf_line_endings() {
        let src = parse(b"dictionary {\r\n_column(1) v\r\n}\r\n").unwrap();
        assert_eq!(src.schema().columns().len(), 1);
    }

    #[test]
    fn errors_on_missing_closing_brace() {
        let result = parse(b"dictionary {\n_column(1) v\n");
        assert!(matches!(result, Err(DctError::UnexpectedEofInDictionary)));
    }

    #[test]
    fn errors_on_missing_dictionary_keyword() {
        let result = parse(b"random {\n_column(1) v\n}\n");
        assert!(matches!(
            result,
            Err(DctError::InvalidDictionaryHeader { .. })
        ));
    }

    #[test]
    fn errors_on_invalid_column_directive() {
        let result = parse(b"dictionary {\n_column(abc) v\n}\n");
        assert!(matches!(
            result,
            Err(DctError::InvalidColumnDirective { .. })
        ));
    }

    #[test]
    fn errors_on_zero_column_value() {
        let result = parse(b"dictionary {\n_column(0) v\n}\n");
        assert!(matches!(
            result,
            Err(DctError::InvalidColumnDirective { .. })
        ));
    }

    #[test]
    fn errors_on_invalid_input_format() {
        let result = parse(b"dictionary {\n_column(1) byte v %5.2x\n}\n");
        assert!(matches!(result, Err(DctError::InvalidReadFormat { .. })));
    }

    #[test]
    fn parses_multi_line_observation() {
        let dict = b"dictionary {\n\
            _column(1) byte b1\n\
            _newline\n\
            _column(1) int i1\n\
            _column(5) double d1\n\
            }\n";
        let src = parse(dict).unwrap();
        let schema = src.schema();
        assert_eq!(schema.lines_per_observation(), 2);
        let cols = schema.columns();
        assert_eq!(cols.len(), 3);
        assert_eq!(cols[0].line_offset(), 0);
        assert_eq!(cols[0].anchor(), ColumnAnchor::Absolute(0));
        assert_eq!(cols[1].line_offset(), 1);
        assert_eq!(cols[1].anchor(), ColumnAnchor::Absolute(0));
        assert_eq!(cols[2].line_offset(), 1);
        assert_eq!(cols[2].anchor(), ColumnAnchor::Absolute(4));
    }

    #[test]
    fn newline_with_no_following_columns_still_advances() {
        // Trailing _newline implies an extra physical line per observation
        // (e.g., data files with a footer line per record).
        let dict = b"dictionary {\n\
            _column(1) byte b1\n\
            _newline\n\
            _newline\n\
            _column(1) int i1\n\
            }\n";
        let src = parse(dict).unwrap();
        assert_eq!(src.schema().lines_per_observation(), 3);
        let cols = src.schema().columns();
        assert_eq!(cols[0].line_offset(), 0);
        assert_eq!(cols[1].line_offset(), 2);
    }

    #[test]
    fn single_line_observation_reports_one_line() {
        let src = parse(b"dictionary {\n_column(1) v\n}\n").unwrap();
        assert_eq!(src.schema().lines_per_observation(), 1);
        assert_eq!(src.schema().columns()[0].line_offset(), 0);
    }

    #[test]
    fn warns_on_unrecognized_directive() {
        let src = parse(b"dictionary {\n_column(1) v\nfoobar baz\n}\n").unwrap();
        assert!(
            src.schema()
                .warnings()
                .iter()
                .any(|w| matches!(w, DctWarning::UnrecognizedDirective { .. }))
        );
    }
}
