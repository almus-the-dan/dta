//! Async data-record reader powered by tokio.
//!
//! Mirrors [`DctReader`](crate::stata::dct::DctReader) — same
//! `DctReaderState` drives the parse, just with `.await`ed reads.
//! Construct via
//! [`DctReaderOptions::from_tokio_reader`](crate::stata::dct::DctReaderOptions::from_tokio_reader)
//! / `from_tokio_file` / `from_tokio_path`.

use tokio::io::{AsyncBufRead, AsyncBufReadExt};

use super::dct_error::Result;
use super::dct_reader_state::{DctReaderState, LineOutcome};
use super::dct_warning::DctWarning;
use super::lazy_record::LazyRecord;
use super::record::Record;
use super::schema::Schema;

/// Async counterpart of [`DctReader`](super::dct_reader::DctReader).
///
/// One physical line per `lines_per_observation` is read per
/// [`read_record`](Self::read_record) call; their contents stay in
/// an internal buffer reused across calls. That's why [`Record`]
/// borrows from `&mut self`.
///
/// Per-observation warnings are surfaced via
/// [`warnings`](Self::warnings), cleared at the start of each read.
#[derive(Debug)]
pub struct AsyncDctReader<R> {
    inner: R,
    state: DctReaderState,
}

impl<R> AsyncDctReader<R> {
    /// Constructs an async reader from a parsed schema and a data
    /// source.
    ///
    /// Crate-private. External callers go through
    /// [`DctReaderOptions::from_tokio_reader`](super::dct_reader_options::DctReaderOptions::from_tokio_reader)
    /// (and its `from_tokio_file` / `from_tokio_path` siblings).
    pub(super) fn new(schema: Schema, inner: R) -> Self {
        Self {
            inner,
            state: DctReaderState::new(schema),
        }
    }

    /// The schema this reader was constructed from.
    #[must_use]
    pub fn schema(&self) -> &Schema {
        self.state.schema()
    }

    /// Warnings produced while reading the most recent observation.
    /// Cleared at the start of every [`read_record`](Self::read_record)
    /// call.
    #[must_use]
    pub fn warnings(&self) -> &[DctWarning] {
        self.state.warnings()
    }

    /// Consumes the reader and returns the underlying data source.
    #[must_use]
    pub fn into_inner(self) -> R {
        self.inner
    }
}

impl<R: AsyncBufRead + Unpin> AsyncDctReader<R> {
    /// Reads the next observation from the data file.
    ///
    /// # Errors
    ///
    /// Returns [`DctError`](super::dct_error::DctError) on I/O
    /// failure, when the data file ends in the middle of an
    /// observation, or when a field cannot be parsed against the
    /// column's declared type and read format.
    pub async fn read_record(&mut self) -> Result<Option<Record<'_>>> {
        if !self.advance_to_next_observation().await? {
            return Ok(None);
        }
        let record = self.state.build_record()?;
        Ok(Some(record))
    }

    /// Reads the next observation as a [`LazyRecord`].
    ///
    /// # Errors
    ///
    /// Returns [`DctError`](super::dct_error::DctError) on I/O
    /// failure or when the data file ends mid-observation.
    pub async fn read_lazy_record(&mut self) -> Result<Option<LazyRecord<'_>>> {
        if !self.advance_to_next_observation().await? {
            return Ok(None);
        }
        let record = self.state.build_lazy_record();
        Ok(Some(record))
    }

    /// Async counterpart of
    /// [`DctReader::advance_to_next_observation`](super::dct_reader::DctReader)
    /// — drives `DctReaderState` through one observation's worth of
    /// lines.
    async fn advance_to_next_observation(&mut self) -> Result<bool> {
        if self.state.is_completed() {
            return Ok(false);
        }
        let lines_per_observation = self.state.begin_observation();
        for line_index in 0..lines_per_observation {
            let buffer = self.state.line_buffer_mut(line_index);
            let bytes_read = self.inner.read_line(buffer).await?;
            match self.state.finalize_line(line_index, bytes_read) {
                LineOutcome::Read => {}
                LineOutcome::CleanEof => return Ok(false),
                LineOutcome::PartialObservation => {
                    return Err(self.state.unexpected_eof_error());
                }
            }
        }
        self.state.advance_observation();
        Ok(true)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::stata::dct::dct_error::DctError;
    use crate::stata::dct::dct_source::DctSource;
    use crate::stata::dct::dct_warning::DctWarning;
    use crate::stata::dct::value::Value;
    use crate::stata::stata_byte::StataByte;
    use crate::stata::stata_int::StataInt;

    async fn external_with_data<'a>(dict: &[u8], data: &'a [u8]) -> AsyncDctReader<&'a [u8]> {
        let source = DctSource::options().from_tokio_reader(dict).await.unwrap();
        let DctSource::External(schema) = source else {
            panic!("expected external schema");
        };
        crate::stata::dct::dct_reader::DctReader::options(schema).from_tokio_reader(data)
    }

    #[tokio::test]
    async fn reads_single_record_async() {
        let dict = b"dictionary {\n_column(1) byte b1 %3.0f\n}\n";
        let data = b"042\n";
        let mut reader = external_with_data(dict, data).await;
        let record = reader.read_record().await.unwrap().unwrap();
        assert!(matches!(
            record.values()[0],
            Value::Byte(StataByte::Present(42))
        ));
    }

    #[tokio::test]
    async fn reads_multiple_records_until_eof_async() {
        let dict = b"dictionary {\n_column(1) byte b1 %3.0f\n}\n";
        let data = b"001\n002\n003\n";
        let mut reader = external_with_data(dict, data).await;
        let mut count = 0;
        while reader.read_record().await.unwrap().is_some() {
            count += 1;
        }
        assert_eq!(count, 3);
    }

    #[tokio::test]
    async fn returns_none_at_clean_eof_async() {
        let dict = b"dictionary {\n_column(1) byte b1 %3.0f\n}\n";
        let mut reader = external_with_data(dict, b"").await;
        assert!(reader.read_record().await.unwrap().is_none());
    }

    #[tokio::test]
    async fn integer_promotion_warning_async() {
        let dict = b"dictionary {\n_column(1) byte b1 %3.0f\n}\n";
        let data = b"200\n";
        let mut reader = external_with_data(dict, data).await;
        {
            let record = reader.read_record().await.unwrap().unwrap();
            assert!(matches!(
                record.values()[0],
                Value::Int(StataInt::Present(200))
            ));
        }
        assert!(
            reader
                .warnings()
                .iter()
                .any(|w| matches!(w, DctWarning::IntegerPromotion { .. }))
        );
    }

    #[tokio::test]
    async fn lazy_record_decodes_individual_columns_async() {
        let dict = b"dictionary {\n\
            _column(1) byte b1 %3.0f\n\
            _column(4) int i1 %5.0f\n\
            }\n";
        let data = b"04212345\n";
        let mut reader = external_with_data(dict, data).await;
        let record = reader.read_lazy_record().await.unwrap().unwrap();
        assert!(matches!(
            record.value(0).unwrap(),
            Value::Byte(StataByte::Present(42))
        ));
        assert!(matches!(
            record.value(1).unwrap(),
            Value::Int(StataInt::Present(12345))
        ));
    }

    #[tokio::test]
    async fn errors_on_mid_observation_eof_async() {
        let dict = b"dictionary {\n\
            _column(1) byte a %3.0f\n\
            _newline\n\
            _column(1) byte b %3.0f\n\
            }\n";
        let data = b"042\n";
        let mut reader = external_with_data(dict, data).await;
        let result = reader.read_record().await;
        assert!(matches!(result, Err(DctError::UnexpectedEofInData { .. })));
    }

    #[tokio::test]
    async fn warnings_cleared_between_records_async() {
        let dict = b"dictionary {\n_column(1) byte b1 %3.0f\n}\n";
        let data = b"\n042\n";
        let mut reader = external_with_data(dict, data).await;
        {
            let _r1 = reader.read_record().await.unwrap().unwrap();
        }
        assert_eq!(reader.warnings().len(), 1);
        {
            let _r2 = reader.read_record().await.unwrap().unwrap();
        }
        assert!(reader.warnings().is_empty());
    }
}
