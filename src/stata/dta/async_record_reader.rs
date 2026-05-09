use tokio::io::{AsyncRead, AsyncSeek};

use super::async_long_string_reader::AsyncLongStringReader;
use super::async_reader_state::AsyncReaderState;
use super::dta_error::{DtaError, FormatErrorKind, Result, Section};
use super::header::Header;
use super::lazy_record::LazyRecord;
use super::record::Record;
use super::record_parse::{CLOSING_TAG, OPENING_TAG, compute_record_seek_target, parse_row};
use super::schema::Schema;

/// Reads observation records from the data section of a DTA file
/// asynchronously.
///
/// Owns the parsed [`Header`] and [`Schema`] from previous phases.
/// Yields rows of [`Value`](super::value::Value) via iteration.
#[derive(Debug)]
pub struct AsyncRecordReader<R> {
    state: AsyncReaderState<R>,
    header: Header,
    schema: Schema,
    remaining_observations: u64,
    opened: bool,
    completed: bool,
}

impl<R> AsyncRecordReader<R> {
    #[must_use]
    pub(crate) fn new(state: AsyncReaderState<R>, header: Header, schema: Schema) -> Self {
        let remaining_observations = header.observation_count();
        Self {
            state,
            header,
            schema,
            remaining_observations,
            opened: false,
            completed: false,
        }
    }

    /// The parsed file header.
    #[must_use]
    #[inline]
    pub fn header(&self) -> &Header {
        &self.header
    }

    /// The parsed variable definitions.
    #[must_use]
    #[inline]
    pub fn schema(&self) -> &Schema {
        &self.schema
    }

    /// The encoding this reader uses to decode string fields.
    ///
    /// Defaults to Windows-1252 for pre-V118 releases and UTF-8 for
    /// V118+, overridable via
    /// [`DtaReader::encoding`](super::dta_reader::DtaReader::encoding).
    #[must_use]
    #[inline]
    pub fn encoding(&self) -> &'static encoding_rs::Encoding {
        self.state.encoding()
    }
}

impl<R: AsyncRead + Unpin> AsyncRecordReader<R> {
    /// Reads the next observation, eagerly parsing all values.
    ///
    /// Returns `None` when all observations have been consumed.
    /// The returned [`Record`] borrows string data from the reader's
    /// internal buffer, so it must be dropped before the next call.
    ///
    /// # Errors
    ///
    /// Returns [`DtaError::Io`](super::dta_error::DtaError::Io) on
    /// read failures and
    /// [`DtaError::Format`](super::dta_error::DtaError::Format) when
    /// the row bytes violate the DTA format specification.
    pub async fn read_record(&mut self) -> Result<Option<Record<'_>>> {
        if !self.read_next_row().await? {
            return Ok(None);
        }

        let byte_order = self.header.byte_order();
        let release = self.header.release();
        let encoding = self.state.encoding();
        let row_bytes = self.state.buffer();
        let values = parse_row(row_bytes, &self.schema, byte_order, release, encoding)?;
        let record = Record::new(values);
        Ok(Some(record))
    }

    /// Reads the next observation without parsing individual values.
    ///
    /// Returns `None` when all observations have been consumed.
    /// The returned [`LazyRecord`] holds the raw row bytes and decodes
    /// values on demand via [`LazyRecord::value`]. This avoids parsing
    /// columns that are never accessed.
    ///
    /// # Errors
    ///
    /// Returns [`DtaError::Io`](super::dta_error::DtaError::Io) on
    /// read failures.
    pub async fn read_lazy_record(&mut self) -> Result<Option<LazyRecord<'_>>> {
        if !self.read_next_row().await? {
            return Ok(None);
        }

        let record = LazyRecord::new(
            self.state.buffer(),
            self.schema.variables(),
            self.header.release(),
            self.header.byte_order(),
            self.state.encoding(),
        );
        Ok(Some(record))
    }

    /// Skips all remaining data records without processing them.
    ///
    /// # Errors
    ///
    /// Returns [`DtaError::Io`](super::dta_error::DtaError::Io) on
    /// read failures and
    /// [`DtaError::Format`](super::dta_error::DtaError::Format) if the
    /// closing `</data>` tag (XML formats) is missing or malformed.
    pub async fn skip_to_end(&mut self) -> Result<()> {
        if self.completed {
            return Ok(());
        }

        self.read_opening_tag().await?;

        let row_len = self.schema.row_len();
        while self.remaining_observations > 0 {
            self.state.skip(row_len, Section::Records).await?;
            self.remaining_observations -= 1;
        }

        self.read_closing_tag().await?;
        self.completed = true;
        Ok(())
    }

    /// Consumes any remaining records and transitions to long-string
    /// reading.
    ///
    /// # Errors
    ///
    /// Returns [`DtaError::Io`](super::dta_error::DtaError::Io) on
    /// read failures.
    pub async fn into_long_string_reader(mut self) -> Result<AsyncLongStringReader<R>> {
        self.skip_to_end().await?;
        let reader = AsyncLongStringReader::new(self.state, self.header, self.schema);
        Ok(reader)
    }
}

// ---------------------------------------------------------------------------
// Internal helpers
// ---------------------------------------------------------------------------

impl<R: AsyncRead + Unpin> AsyncRecordReader<R> {
    /// Reads the next row's bytes into the internal buffer.
    ///
    /// Returns `true` if a row was read, `false` if all observations
    /// have been consumed (also handles closing tag and sets
    /// `completed`).
    async fn read_next_row(&mut self) -> Result<bool> {
        if self.completed {
            return Ok(false);
        }

        self.read_opening_tag().await?;

        if self.remaining_observations == 0 {
            self.read_closing_tag().await?;
            self.completed = true;
            return Ok(false);
        }

        let row_len = self.schema.row_len();
        self.state.read_exact(row_len, Section::Records).await?;
        self.remaining_observations -= 1;

        Ok(true)
    }

    async fn read_opening_tag(&mut self) -> Result<()> {
        if self.opened {
            return Ok(());
        }
        self.opened = true;
        if self.header.release().is_xml_like() {
            self.state
                .expect_bytes(OPENING_TAG, Section::Records, FormatErrorKind::InvalidMagic)
                .await?;
        }
        Ok(())
    }

    async fn read_closing_tag(&mut self) -> Result<()> {
        if self.header.release().is_xml_like() {
            self.state
                .expect_bytes(CLOSING_TAG, Section::Records, FormatErrorKind::InvalidMagic)
                .await?;
        }
        Ok(())
    }
}

impl<R: AsyncRead + AsyncSeek + Unpin> AsyncRecordReader<R> {
    /// Jumps to a specific observation by 0-based index.
    ///
    /// Records have a fixed byte width, so the target offset is
    /// computed as `data_start + index * row_len` and reached with a
    /// single seek. Subsequent calls to
    /// [`read_record`](Self::read_record) /
    /// [`read_lazy_record`](Self::read_lazy_record) resume from that
    /// observation.
    ///
    /// `index == observation_count` is valid and leaves the reader in
    /// the same state as if every record had been consumed
    /// sequentially: for XML formats (117+) the `</data>` closing tag
    /// is read and validated, and the reader is marked completed so
    /// [`into_long_string_reader`](Self::into_long_string_reader) can
    /// be called immediately. Seeking backward from that completed
    /// state revives the reader.
    ///
    /// # Errors
    ///
    /// Returns [`DtaError::Io`](super::dta_error::DtaError::Io) with
    /// [`InvalidInput`](std::io::ErrorKind::InvalidInput) if
    /// `index > observation_count`, and
    /// [`DtaError::Io`](super::dta_error::DtaError::Io) if section
    /// offsets are missing or the seek fails. Returns
    /// [`DtaError::Format`](super::dta_error::DtaError::Format) with
    /// [`FieldTooLarge`](FormatErrorKind::FieldTooLarge) if the
    /// target byte offset overflows `u64`, and (for XML formats)
    /// [`DtaError::Format`](super::dta_error::DtaError::Format) with
    /// [`FormatErrorKind::InvalidMagic`] when
    /// `index == observation_count` and the bytes at the closing
    /// position are not `</data>`.
    pub async fn seek_to_record(&mut self, index: u64) -> Result<()> {
        let observation_count = self.header.observation_count();
        let records_offset = self
            .state
            .section_offsets()
            .ok_or_else(|| DtaError::missing_section_offsets(Section::Records))?
            .records();
        let seek_target = compute_record_seek_target(
            index,
            observation_count,
            records_offset,
            self.schema.row_len(),
            self.header.release().is_xml_like(),
        )?;

        self.state
            .seek_to(seek_target.target, Section::Records)
            .await?;
        self.opened = true;
        self.remaining_observations = observation_count - index;

        if seek_target.at_end_of_data {
            self.read_closing_tag().await?;
            self.completed = true;
        } else {
            self.completed = false;
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;

    use float_cmp::assert_approx_eq;

    use super::*;
    use crate::stata::dta::byte_order::ByteOrder;
    use crate::stata::dta::dta_reader::DtaReader;
    use crate::stata::dta::dta_writer::DtaWriter;
    use crate::stata::dta::release::Release;
    use crate::stata::dta::schema::Schema;
    use crate::stata::dta::value::Value;
    use crate::stata::dta::variable::Variable;
    use crate::stata::dta::variable_type::VariableType;
    use crate::stata::stata_byte::StataByte;
    use crate::stata::stata_double::StataDouble;
    use crate::stata::stata_int::StataInt;

    /// Writes `values` as a single record, then reads it back through
    /// the async reader pipeline, and then calls `assert_fn` on the parsed
    /// record. The reader is kept alive across the call so
    /// `record.values()` can borrow the state's buffer.
    async fn read_one_record<F>(
        release: Release,
        byte_order: ByteOrder,
        schema: Schema,
        values: Vec<Value<'_>>,
        assert_fn: F,
    ) where
        F: FnOnce(&[Value<'_>]),
    {
        let header = Header::builder(release, byte_order).build();
        let mut record_writer = DtaWriter::new()
            .from_tokio_writer(Cursor::new(Vec::<u8>::new()))
            .write_header(header)
            .await
            .unwrap()
            .write_schema(schema)
            .await
            .unwrap()
            .into_record_writer()
            .await
            .unwrap();
        record_writer.write_record(&values).await.unwrap();
        let cursor: Cursor<Vec<u8>> = record_writer
            .into_long_string_writer()
            .await
            .unwrap()
            .into_value_label_writer()
            .await
            .unwrap()
            .finish()
            .await
            .unwrap();
        let bytes = cursor.into_inner();

        let mut reader = DtaReader::new()
            .from_tokio_reader(bytes.as_slice())
            .read_header()
            .await
            .unwrap()
            .read_schema()
            .await
            .unwrap()
            .into_record_reader()
            .await
            .unwrap();
        let record = reader.read_record().await.unwrap().unwrap();
        assert_fn(record.values());
    }

    #[tokio::test]
    async fn binary_v114_reads_int() {
        let schema = Schema::builder()
            .add_variable(Variable::builder(VariableType::Int, "i").format("%8.0g"))
            .build()
            .unwrap();
        read_one_record(
            Release::V114,
            ByteOrder::LittleEndian,
            schema,
            vec![Value::Int(StataInt::Present(-3))],
            |values| {
                assert!(matches!(values[0], Value::Int(StataInt::Present(-3))));
            },
        )
        .await;
    }

    #[tokio::test]
    async fn xml_v117_reads_fixed_string() {
        let schema = Schema::builder()
            .add_variable(Variable::builder(VariableType::FixedString(8), "s").format("%8s"))
            .build()
            .unwrap();
        read_one_record(
            Release::V117,
            ByteOrder::LittleEndian,
            schema,
            vec![Value::string("hi")],
            |values| {
                let Value::String(s) = &values[0] else {
                    panic!("expected string");
                };
                assert_eq!(s.as_ref(), "hi");
            },
        )
        .await;
    }

    #[tokio::test]
    async fn xml_v118_reads_double_be() {
        let schema = Schema::builder()
            .add_variable(Variable::builder(VariableType::Double, "d").format("%10.0g"))
            .build()
            .unwrap();
        read_one_record(
            Release::V118,
            ByteOrder::BigEndian,
            schema,
            vec![Value::Double(StataDouble::Present(1.25))],
            |values| {
                let Value::Double(StataDouble::Present(d)) = values[0] else {
                    panic!("expected double");
                };
                assert_approx_eq!(f64, d, 1.25);
            },
        )
        .await;
    }

    #[tokio::test]
    async fn binary_v114_reads_missing_byte() {
        use crate::stata::missing_value::MissingValue;
        let schema = Schema::builder()
            .add_variable(Variable::builder(VariableType::Byte, "x").format("%8.0g"))
            .build()
            .unwrap();
        read_one_record(
            Release::V114,
            ByteOrder::LittleEndian,
            schema,
            vec![Value::Byte(StataByte::Missing(MissingValue::A))],
            |values| {
                assert!(matches!(
                    values[0],
                    Value::Byte(StataByte::Missing(MissingValue::A)),
                ));
            },
        )
        .await;
    }

    #[tokio::test]
    async fn skip_to_end_on_empty_data_section_succeeds() {
        // Zero-row data section should close cleanly whether consumed
        // via read_record (returns None right away) or skip_to_end.
        let schema = Schema::builder()
            .add_variable(Variable::builder(VariableType::Byte, "x").format("%8.0g"))
            .build()
            .unwrap();
        let header = Header::builder(Release::V114, ByteOrder::LittleEndian).build();
        let cursor: Cursor<Vec<u8>> = DtaWriter::new()
            .from_tokio_writer(Cursor::new(Vec::<u8>::new()))
            .write_header(header)
            .await
            .unwrap()
            .write_schema(schema)
            .await
            .unwrap()
            .into_record_writer()
            .await
            .unwrap()
            .into_long_string_writer()
            .await
            .unwrap()
            .into_value_label_writer()
            .await
            .unwrap()
            .finish()
            .await
            .unwrap();
        let bytes = cursor.into_inner();
        let mut reader = DtaReader::new()
            .from_tokio_reader(bytes.as_slice())
            .read_header()
            .await
            .unwrap()
            .read_schema()
            .await
            .unwrap()
            .into_record_reader()
            .await
            .unwrap();
        reader.skip_to_end().await.unwrap();
        assert!(reader.read_record().await.unwrap().is_none());
    }

    /// Writes a DTA file with `row_count` byte rows where row `i` holds
    /// value `i as i8`. The schema is a single `Byte` column.
    async fn build_file_with_byte_rows(release: Release, row_count: u8) -> Vec<u8> {
        let schema = Schema::builder()
            .add_variable(Variable::builder(VariableType::Byte, "x").format("%8.0g"))
            .build()
            .unwrap();
        let header = Header::builder(release, ByteOrder::LittleEndian).build();
        let mut record_writer = DtaWriter::new()
            .from_tokio_writer(Cursor::new(Vec::<u8>::new()))
            .write_header(header)
            .await
            .unwrap()
            .write_schema(schema)
            .await
            .unwrap()
            .into_record_writer()
            .await
            .unwrap();
        for byte in 0..row_count {
            let signed = i8::try_from(byte).unwrap();
            record_writer
                .write_record(&[Value::Byte(StataByte::Present(signed))])
                .await
                .unwrap();
        }
        let cursor: Cursor<Vec<u8>> = record_writer
            .into_long_string_writer()
            .await
            .unwrap()
            .into_value_label_writer()
            .await
            .unwrap()
            .finish()
            .await
            .unwrap();
        cursor.into_inner()
    }

    /// Opens an `AsyncRecordReader` over the given bytes wrapped in a
    /// `Cursor` so that `AsyncSeek` is available.
    async fn async_record_reader_for(bytes: Vec<u8>) -> AsyncRecordReader<Cursor<Vec<u8>>> {
        DtaReader::new()
            .from_tokio_reader(Cursor::new(bytes))
            .read_header()
            .await
            .unwrap()
            .read_schema()
            .await
            .unwrap()
            .into_record_reader()
            .await
            .unwrap()
    }

    async fn read_byte(reader: &mut AsyncRecordReader<Cursor<Vec<u8>>>) -> Option<i8> {
        let record = reader.read_record().await.unwrap()?;
        match &record.values()[0] {
            Value::Byte(StataByte::Present(byte)) => Some(*byte),
            other => panic!("expected present byte, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn seek_to_middle_record_xml() {
        let bytes = build_file_with_byte_rows(Release::V117, 5).await;
        let mut reader = async_record_reader_for(bytes).await;
        reader.seek_to_record(3).await.unwrap();
        assert_eq!(read_byte(&mut reader).await, Some(3));
        assert_eq!(read_byte(&mut reader).await, Some(4));
        assert_eq!(read_byte(&mut reader).await, None);
    }

    #[tokio::test]
    async fn seek_to_middle_record_binary() {
        let bytes = build_file_with_byte_rows(Release::V114, 5).await;
        let mut reader = async_record_reader_for(bytes).await;
        reader.seek_to_record(2).await.unwrap();
        assert_eq!(read_byte(&mut reader).await, Some(2));
        assert_eq!(read_byte(&mut reader).await, Some(3));
        assert_eq!(read_byte(&mut reader).await, Some(4));
        assert_eq!(read_byte(&mut reader).await, None);
    }

    #[tokio::test]
    async fn seek_before_any_read_does_not_require_opening_tag_consumption() {
        let bytes = build_file_with_byte_rows(Release::V118, 4).await;
        let mut reader = async_record_reader_for(bytes).await;
        reader.seek_to_record(2).await.unwrap();
        assert_eq!(read_byte(&mut reader).await, Some(2));
    }

    #[tokio::test]
    async fn seek_to_observation_count_marks_completed_xml() {
        let bytes = build_file_with_byte_rows(Release::V117, 3).await;
        let mut reader = async_record_reader_for(bytes).await;
        reader.seek_to_record(3).await.unwrap();
        assert!(reader.read_record().await.unwrap().is_none());
        // The `into_long_string_reader` chain requires the data
        // section be fully consumed; this exercises the same
        // post-seek state as full sequential drain.
        let _long_string_reader = reader.into_long_string_reader().await.unwrap();
    }

    #[tokio::test]
    async fn seek_backward_from_completed_revives_reader() {
        let bytes = build_file_with_byte_rows(Release::V117, 4).await;
        let mut reader = async_record_reader_for(bytes).await;
        while reader.read_record().await.unwrap().is_some() {}
        reader.seek_to_record(1).await.unwrap();
        assert_eq!(read_byte(&mut reader).await, Some(1));
        assert_eq!(read_byte(&mut reader).await, Some(2));
        assert_eq!(read_byte(&mut reader).await, Some(3));
        assert_eq!(read_byte(&mut reader).await, None);
    }

    #[tokio::test]
    async fn seek_past_end_returns_invalid_input() {
        use std::io::ErrorKind;
        let bytes = build_file_with_byte_rows(Release::V117, 3).await;
        let mut reader = async_record_reader_for(bytes).await;
        let error = reader.seek_to_record(4).await.unwrap_err();
        match error {
            DtaError::Io { section, source } => {
                assert_eq!(section, Section::Records);
                assert_eq!(source.kind(), ErrorKind::InvalidInput);
            }
            other => panic!("expected DtaError::Io, got {other:?}"),
        }
        reader.seek_to_record(1).await.unwrap();
        assert_eq!(read_byte(&mut reader).await, Some(1));
    }
}
