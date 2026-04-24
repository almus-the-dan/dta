use tokio::io::AsyncRead;

use super::async_long_string_reader::AsyncLongStringReader;
use super::async_reader_state::AsyncReaderState;
use super::dta_error::{FormatErrorKind, Result, Section};
use super::header::Header;
use super::lazy_record::LazyRecord;
use super::record::Record;
use super::record_parse::parse_row;
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
    /// Returns [`DtaError::Io`] on read failures and
    /// [`DtaError::Format`] when the row bytes violate the DTA
    /// format specification.
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
    /// Returns [`DtaError::Io`] on read failures.
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
    /// Returns [`DtaError::Io`] on read failures and
    /// [`DtaError::Format`] if the closing `</data>` tag (XML formats)
    /// is missing or malformed.
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
    /// Returns [`DtaError::Io`] on read failures.
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
                .expect_bytes(b"<data>", Section::Records, FormatErrorKind::InvalidMagic)
                .await?;
        }
        Ok(())
    }

    async fn read_closing_tag(&mut self) -> Result<()> {
        if self.header.release().is_xml_like() {
            self.state
                .expect_bytes(b"</data>", Section::Records, FormatErrorKind::InvalidMagic)
                .await?;
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
}
