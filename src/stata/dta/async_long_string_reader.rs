use std::borrow::Cow;

use tokio::io::AsyncRead;

use super::async_reader_state::AsyncReaderState;
use super::async_value_label_reader::AsyncValueLabelReader;
use super::byte_order::ByteOrder;
use super::dta_error::{DtaError, FormatErrorKind, Result, Section};
use super::header::Header;
use super::long_string::LongString;
use super::long_string_parse::{
    GSO_SECTION_CLOSE_REST, GsoHeader, GsoTag, classify_gso_tag, long_string_data_len_to_usize,
};
use super::schema::Schema;

/// Reads long string (strL) entries from a DTA file asynchronously.
///
/// Only present for format 117+. Owns the parsed [`Header`] and
/// [`Schema`] from previous phases. Yields [`LongString`] entries
/// via iteration.
#[derive(Debug)]
pub struct AsyncLongStringReader<R> {
    state: AsyncReaderState<R>,
    header: Header,
    schema: Schema,
    opened: bool,
    completed: bool,
}

impl<R> AsyncLongStringReader<R> {
    #[must_use]
    pub(crate) fn new(state: AsyncReaderState<R>, header: Header, schema: Schema) -> Self {
        let completed = !header.release().supports_long_strings();
        Self {
            state,
            header,
            schema,
            opened: false,
            completed,
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

impl<R: AsyncRead + Unpin> AsyncLongStringReader<R> {
    /// Reads the next long string (strL / GSO) entry.
    ///
    /// Returns `None` when all entries have been consumed. Each entry
    /// contains the `(variable, observation)` key and the raw data
    /// bytes. Use [`LongString::data_str`] to decode the bytes as a
    /// string, or [`LongString::data`] for raw access.
    ///
    /// # Errors
    ///
    /// Returns [`DtaError::Io`] on read failures and
    /// [`DtaError::Format`] when the entry bytes violate the DTA
    /// format specification.
    pub async fn read_long_string(&mut self) -> Result<Option<LongString<'_>>> {
        let Some(gso_header) = self.read_gso_header().await? else {
            return Ok(None);
        };

        let encoding = self.state.encoding();
        let data = self
            .state
            .read_exact(gso_header.data_len, Section::LongStrings)
            .await?;

        Ok(Some(LongString::new(
            gso_header.variable,
            gso_header.observation,
            gso_header.is_binary(),
            Cow::Borrowed(data),
            encoding,
        )))
    }

    /// Skips all remaining long-string entries without processing
    /// them.
    ///
    /// # Errors
    ///
    /// Returns [`DtaError::Io`] on read failures and
    /// [`DtaError::Format`] if section tags are missing or malformed.
    pub async fn skip_to_end(&mut self) -> Result<()> {
        if self.completed {
            return Ok(());
        }
        loop {
            let Some(gso_header) = self.read_gso_header().await? else {
                return Ok(());
            };
            self.state
                .skip(gso_header.data_len, Section::LongStrings)
                .await?;
        }
    }

    /// Consumes any remaining entries and transitions to value-label
    /// reading.
    ///
    /// # Errors
    ///
    /// Returns [`DtaError::Io`] on read failures.
    pub async fn into_value_label_reader(mut self) -> Result<AsyncValueLabelReader<R>> {
        self.skip_to_end().await?;
        Ok(AsyncValueLabelReader::new(
            self.state,
            self.header,
            self.schema,
        ))
    }
}

// ---------------------------------------------------------------------------
// Internal helpers
// ---------------------------------------------------------------------------

impl<R: AsyncRead + Unpin> AsyncLongStringReader<R> {
    /// Reads the next GSO entry header, or returns `None` at the
    /// `</strls>` closing tag.
    async fn read_gso_header(&mut self) -> Result<Option<GsoHeader>> {
        if self.completed {
            return Ok(None);
        }

        self.read_opening_tag().await?;

        let position = self.state.position();
        let head = self.state.read_exact(3, Section::LongStrings).await?;
        let tag = classify_gso_tag(head).ok_or_else(|| {
            DtaError::format(
                Section::LongStrings,
                position,
                FormatErrorKind::InvalidLongStringEntry,
            )
        })?;
        if let GsoTag::SectionClose = tag {
            self.state
                .expect_bytes(
                    GSO_SECTION_CLOSE_REST,
                    Section::LongStrings,
                    FormatErrorKind::InvalidMagic,
                )
                .await?;
            self.completed = true;
            return Ok(None);
        }

        let byte_order = self.header.byte_order();
        let (variable, observation) = self.read_variable_observation(byte_order).await?;
        let gso_type = self.state.read_u8(Section::LongStrings).await?;
        let data_len = self
            .state
            .read_u32(byte_order, Section::LongStrings)
            .await?;
        let data_len = long_string_data_len_to_usize(data_len)?;

        Ok(Some(GsoHeader {
            variable,
            observation,
            gso_type,
            data_len,
        }))
    }

    /// Reads the `(variable, observation)` index pair at the start of
    /// a GSO entry. The variable is always `u32`; the observation
    /// widens to `u64` on V118+ and stays `u32` on V117.
    async fn read_variable_observation(&mut self, byte_order: ByteOrder) -> Result<(u32, u64)> {
        let variable = self
            .state
            .read_u32(byte_order, Section::LongStrings)
            .await?;
        let observation = if self.header.release().supports_extended_observation_count() {
            self.state
                .read_u64(byte_order, Section::LongStrings)
                .await?
        } else {
            let observation = self
                .state
                .read_u32(byte_order, Section::LongStrings)
                .await?;
            u64::from(observation)
        };
        Ok((variable, observation))
    }

    async fn read_opening_tag(&mut self) -> Result<()> {
        if self.opened {
            return Ok(());
        }
        self.opened = true;
        self.state
            .expect_bytes(
                b"<strls>",
                Section::LongStrings,
                FormatErrorKind::InvalidMagic,
            )
            .await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::borrow::Cow;
    use std::io::Cursor;

    use encoding_rs::UTF_8;

    use super::*;
    use crate::stata::dta::byte_order::ByteOrder;
    use crate::stata::dta::dta_reader::DtaReader;
    use crate::stata::dta::dta_writer::DtaWriter;
    use crate::stata::dta::release::Release;
    use crate::stata::dta::variable::Variable;
    use crate::stata::dta::variable_type::VariableType;

    fn text(variable: u32, observation: u64, data: &'static str) -> LongString<'static> {
        LongString::new(
            variable,
            observation,
            false,
            Cow::Borrowed(data.as_bytes()),
            UTF_8,
        )
    }

    async fn read_one(
        release: Release,
        byte_order: ByteOrder,
        entry: LongString<'_>,
    ) -> (u32, u64, bool, Vec<u8>) {
        let schema = Schema::builder()
            .add_variable(Variable::builder(VariableType::Byte, "x").format("%8.0g"))
            .build()
            .unwrap();
        let header = Header::builder(release, byte_order).build();
        let mut long_string_writer = DtaWriter::new()
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
            .unwrap();
        long_string_writer.write_long_string(&entry).await.unwrap();
        let cursor: Cursor<Vec<u8>> = long_string_writer
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
            .unwrap()
            .into_long_string_reader()
            .await
            .unwrap();
        let ls = reader.read_long_string().await.unwrap().unwrap();
        let result = (
            ls.variable(),
            ls.observation(),
            ls.is_binary(),
            ls.data().to_vec(),
        );
        assert!(reader.read_long_string().await.unwrap().is_none());
        result
    }

    #[tokio::test]
    async fn xml_v117_reads_entry() {
        let (variable, observation, binary, data) =
            read_one(Release::V117, ByteOrder::LittleEndian, text(1, 1, "hello")).await;
        assert_eq!(variable, 1);
        assert_eq!(observation, 1);
        assert!(!binary);
        assert_eq!(data, b"hello");
    }

    #[tokio::test]
    async fn xml_v118_reads_wide_observation() {
        let (_, observation, _, data) = read_one(
            Release::V118,
            ByteOrder::LittleEndian,
            text(1, 5_000_000_000, "wide"),
        )
        .await;
        assert_eq!(observation, 5_000_000_000);
        assert_eq!(data, b"wide");
    }

    #[tokio::test]
    async fn pre_v117_reader_yields_none_immediately() {
        // V114 has no `<strls>` section at all — reader is pre-set to
        // completed, so read_long_string should return None on the
        // first call and never try to parse anything.
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
            .unwrap()
            .into_long_string_reader()
            .await
            .unwrap();
        assert!(reader.read_long_string().await.unwrap().is_none());
    }
}
