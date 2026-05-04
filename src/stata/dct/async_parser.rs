//! Async I/O wrapper that drives [`DctSourceState`] from a tokio
//! [`AsyncBufRead`] source.
//!
//! Mirrors [`parser`](super::parser) — same pure parsing state, same
//! semantics, just `.await`ed reads. Both paths converge on
//! [`DctSourceState::feed_buffered_line`] for the actual parse work.

use tokio::io::{AsyncBufRead, AsyncBufReadExt};

use super::dct_error::{DctError, Result};
use super::dct_reader::DctReader;
use super::dct_source::DctSource;
use super::dct_source_state::{DctSourceState, FeedOutcome};

/// Parses a `.dct` dictionary from an async buffered reader.
///
/// Crate-private. Public callers go through
/// [`DctSourceOptions::from_tokio_reader`](crate::stata::dct::dct_source_options::DctSourceOptions::from_tokio_reader)
/// (and its `from_tokio_file` / `from_tokio_path` siblings).
pub(super) async fn parse_dct<R: AsyncBufRead + Unpin>(mut reader: R) -> Result<DctSource<R>> {
    let mut state = DctSourceState::new();
    loop {
        let read = reader.read_line(state.buffer_mut()).await?;
        if read == 0 {
            return Err(DctError::UnexpectedEofInDictionary);
        }
        let result = state.feed_buffered_line()?;
        if matches!(result, FeedOutcome::Done) {
            break;
        }
    }

    let schema = state.into_schema();
    let source = if has_more_data(&mut reader).await? {
        let reader = DctReader::new(schema, reader, true);
        DctSource::Embedded(reader)
    } else {
        DctSource::External(schema)
    };
    Ok(source)
}

async fn has_more_data<R: AsyncBufRead + Unpin>(reader: &mut R) -> Result<bool> {
    let buffer = reader.fill_buf().await?;
    Ok(!buffer.is_empty())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::stata::dct::column_anchor::ColumnAnchor;
    use crate::stata::dct::dct_warning::DctWarning;
    use crate::stata::dct::input_format::InputFormat;
    use crate::stata::dct::variable_type::VariableType;

    async fn parse(input: &[u8]) -> Result<DctSource<&[u8]>> {
        parse_dct(input).await
    }

    #[tokio::test]
    async fn parses_minimal_dictionary() {
        let src = parse(b"dictionary {\n_column(1) myvar\n}\n").await.unwrap();
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

    #[tokio::test]
    async fn parses_using_clause_emits_path_warning() {
        let src = parse(b"dictionary using foo.raw {\n_column(1) v\n}\n")
            .await
            .unwrap();
        assert_eq!(src.schema().declared_data_path(), Some("foo.raw"));
        assert!(
            src.schema().warnings().iter().any(
                |w| matches!(w, DctWarning::DeclaredPathIgnored { path } if path == "foo.raw")
            )
        );
    }

    #[tokio::test]
    async fn parses_multi_line_observation() {
        let dict = b"dictionary {\n\
            _column(1) byte b1\n\
            _newline\n\
            _column(1) int i1\n\
            }\n";
        let src = parse(dict).await.unwrap();
        let schema = src.schema();
        assert_eq!(schema.lines_per_observation(), 2);
        assert_eq!(schema.columns().len(), 2);
        assert_eq!(schema.columns()[1].line_offset(), 1);
    }

    #[tokio::test]
    async fn detects_embedded_data() {
        let src = parse(b"dictionary {\n_column(1) v\n}\n42\n").await.unwrap();
        assert!(matches!(src, DctSource::Embedded(_)));
    }

    #[tokio::test]
    async fn detects_external_data() {
        let src = parse(b"dictionary {\n_column(1) v\n}\n").await.unwrap();
        assert!(matches!(src, DctSource::External(_)));
    }

    #[tokio::test]
    async fn handles_crlf_line_endings() {
        let src = parse(b"dictionary {\r\n_column(1) v\r\n}\r\n")
            .await
            .unwrap();
        assert_eq!(src.schema().columns().len(), 1);
    }

    #[tokio::test]
    async fn errors_on_missing_closing_brace() {
        let result = parse(b"dictionary {\n_column(1) v\n").await;
        assert!(matches!(result, Err(DctError::UnexpectedEofInDictionary)));
    }

    #[tokio::test]
    async fn errors_on_missing_dictionary_keyword() {
        let result = parse(b"random {\n_column(1) v\n}\n").await;
        assert!(matches!(
            result,
            Err(DctError::InvalidDictionaryHeader { .. })
        ));
    }

    #[tokio::test]
    async fn errors_on_invalid_column_directive() {
        let result = parse(b"dictionary {\n_column(abc) v\n}\n").await;
        assert!(matches!(
            result,
            Err(DctError::InvalidColumnDirective { .. })
        ));
    }
}
