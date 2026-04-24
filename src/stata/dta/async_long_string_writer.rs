use tokio::io::{AsyncSeek, AsyncWrite};

use super::async_value_label_writer::AsyncValueLabelWriter;
use super::async_writer_state::AsyncWriterState;
use super::dta_error::{DtaError, Field, FormatErrorKind, Result, Section};
use super::header::Header;
use super::long_string::{GsoType, LongString};
use super::long_string_format::{GSO_MAGIC, narrow_long_string_data_len};
use super::long_string_table::LongStringTable;
use super::schema::Schema;

/// Writes long string (strL / GSO) entries to a DTA file
/// asynchronously.
///
/// Only XML formats (117+) support strLs. For earlier releases,
/// [`write_long_string`](Self::write_long_string) returns an error
/// and [`into_value_label_writer`](Self::into_value_label_writer)
/// transitions without emitting any strL content.
#[derive(Debug)]
pub struct AsyncLongStringWriter<W> {
    state: AsyncWriterState<W>,
    header: Header,
    schema: Schema,
    /// Tracks whether the XML `<strls>` opening tag has been emitted.
    /// Unused (but harmless) for pre-117 formats, which have no
    /// section at all.
    opened: bool,
}

impl<W> AsyncLongStringWriter<W> {
    #[must_use]
    pub(crate) fn new(state: AsyncWriterState<W>, header: Header, schema: Schema) -> Self {
        Self {
            state,
            header,
            schema,
            opened: false,
        }
    }

    /// The header emitted during the header phase.
    #[must_use]
    #[inline]
    pub fn header(&self) -> &Header {
        &self.header
    }

    /// The schema emitted during the schema phase.
    #[must_use]
    #[inline]
    pub fn schema(&self) -> &Schema {
        &self.schema
    }

    /// The encoding this writer uses to encode long-string payloads.
    ///
    /// Defaults to Windows-1252 for pre-V118 releases and UTF-8 for
    /// V118+, overridable via
    /// [`DtaWriter::encoding`](super::dta_writer::DtaWriter::encoding).
    #[must_use]
    #[inline]
    pub fn encoding(&self) -> &'static encoding_rs::Encoding {
        self.state.encoding()
    }
}

impl<W: AsyncWrite + AsyncSeek + Unpin> AsyncLongStringWriter<W> {
    /// Writes a single long-string (strL) entry as a GSO block.
    ///
    /// The first call also emits the `<strls>` opening tag. GSO block
    /// layout:
    ///
    /// - `"GSO"` magic (3 bytes)
    /// - `variable`: `u32` (4 bytes)
    /// - `observation`: `u32` (V117) or `u64` (V118+)
    /// - `gso_type`: `u8` — `0x81` for binary, `0x82` for text
    /// - `data_len`: `u32`
    /// - `data`: `data_len` bytes
    ///
    /// # Errors
    ///
    /// Returns [`DtaError::Format`] with
    /// [`LongStringsUnsupported`](FormatErrorKind::LongStringsUnsupported)
    /// if the header's release is pre-V117 (no `<strls>` section),
    /// [`FieldTooLarge`](FormatErrorKind::FieldTooLarge) if the
    /// payload exceeds `u32::MAX` bytes or the observation index
    /// exceeds `u32::MAX` on a V117 file. Returns
    /// [`DtaError::Io`] on sink failures.
    pub async fn write_long_string(&mut self, long_string: &LongString<'_>) -> Result<()> {
        let release = self.header.release();
        if !release.supports_long_strings() {
            let error = DtaError::format(
                Section::LongStrings,
                self.state.position(),
                FormatErrorKind::LongStringsUnsupported { release },
            );
            return Err(error);
        }
        if release.is_xml_like() {
            self.open_section_if_needed().await?;
        }
        self.write_gso_block(long_string).await
    }

    /// Writes every entry from `table` via
    /// [`write_long_string`](Self::write_long_string), in the
    /// `(variable, observation)` order the DTA spec requires.
    ///
    /// An empty table is a no-op — including on pre-117 releases,
    /// where writing any entry would error but writing none is
    /// benign.
    ///
    /// # Errors
    ///
    /// Surfaces the first error from
    /// [`write_long_string`](Self::write_long_string). For pre-117
    /// releases with a non-empty table, that's
    /// [`LongStringsUnsupported`](FormatErrorKind::LongStringsUnsupported)
    /// on the very first entry.
    pub async fn write_long_string_table(&mut self, table: &LongStringTable) -> Result<()> {
        // `LongStringTable::iter` borrows only the table, so we're
        // free to re-borrow `self` as `&mut` inside the loop body.
        // The iterator yields by value, but holding a LongString
        // across an `.await` is fine since it borrows from the table.
        let entries: Vec<LongString<'_>> = table.iter().collect();
        for long_string in &entries {
            self.write_long_string(long_string).await?;
        }
        Ok(())
    }

    /// Closes the long-strings section, patches map slot 11
    /// (value-labels offset) for XML releases, and transitions to
    /// value-label writing.
    ///
    /// For XML (V117+) the closing `</strls>` tag is emitted even
    /// when no entries were written (the opening tag is lazy-emitted
    /// here in that case). For pre-V117 no section exists at all —
    /// nothing is written.
    ///
    /// # Errors
    ///
    /// Returns [`DtaError::Io`] on sink failures.
    pub async fn into_value_label_writer(mut self) -> Result<AsyncValueLabelWriter<W>> {
        let release = self.header.release();
        let byte_order = self.header.byte_order();

        if release.supports_long_strings() {
            if release.is_xml_like() {
                self.open_section_if_needed().await?;
                self.state
                    .write_exact(b"</strls>", Section::LongStrings)
                    .await?;
            }
            let value_labels_offset = self.state.position();
            self.state
                .patch_map_entry(11, value_labels_offset, byte_order, Section::LongStrings)
                .await?;
        }

        let writer = AsyncValueLabelWriter::new(self.state, self.header, self.schema);
        Ok(writer)
    }

    /// Emits the XML `<strls>` tag on first use. Only called on
    /// paths that have already verified the release supports long
    /// strings.
    async fn open_section_if_needed(&mut self) -> Result<()> {
        if !self.opened {
            self.state
                .write_exact(b"<strls>", Section::LongStrings)
                .await?;
            self.opened = true;
        }
        Ok(())
    }
}

impl<W: AsyncWrite + Unpin> AsyncLongStringWriter<W> {
    /// Emits one GSO block. Assumes the release has been validated
    /// to support long strings.
    async fn write_gso_block(&mut self, long_string: &LongString<'_>) -> Result<()> {
        let byte_order = self.header.byte_order();
        let release = self.header.release();

        self.state
            .write_exact(GSO_MAGIC, Section::LongStrings)
            .await?;

        self.state
            .write_u32(long_string.variable(), byte_order, Section::LongStrings)
            .await?;

        if release.supports_extended_observation_count() {
            self.state
                .write_u64(long_string.observation(), byte_order, Section::LongStrings)
                .await?;
        } else {
            let observation = self.state.narrow_to_u32(
                long_string.observation(),
                Section::LongStrings,
                Field::ObservationCount,
            )?;
            self.state
                .write_u32(observation, byte_order, Section::LongStrings)
                .await?;
        }

        let gso_type = if long_string.is_binary() {
            GsoType::Binary
        } else {
            GsoType::Text
        };
        self.state
            .write_u8(gso_type.to_byte(), Section::LongStrings)
            .await?;

        let data = long_string.data();
        let data_len = narrow_long_string_data_len(data.len(), self.state.position())?;
        self.state
            .write_u32(data_len, byte_order, Section::LongStrings)
            .await?;
        self.state.write_exact(data, Section::LongStrings).await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::borrow::Cow;
    use std::io::Cursor;

    use super::*;
    use crate::stata::dta::byte_order::ByteOrder;
    use crate::stata::dta::dta_reader::DtaReader;
    use crate::stata::dta::dta_writer::DtaWriter;
    use crate::stata::dta::long_string::LongStringContent;
    use crate::stata::dta::release::Release;
    use crate::stata::dta::variable::Variable;
    use crate::stata::dta::variable_type::VariableType;

    /// Owned echo of [`LongString`] for test assertions.
    #[derive(Debug, PartialEq, Eq)]
    struct OwnedLongString {
        variable: u32,
        observation: u64,
        binary: bool,
        data: Vec<u8>,
    }

    impl From<&LongString<'_>> for OwnedLongString {
        fn from(ls: &LongString<'_>) -> Self {
            Self {
                variable: ls.variable(),
                observation: ls.observation(),
                binary: ls.is_binary(),
                data: ls.data().to_vec(),
            }
        }
    }

    fn text(variable: u32, observation: u64, data: &'static str) -> LongString<'static> {
        LongString::new(
            variable,
            observation,
            LongStringContent::Text(Cow::Borrowed(data.as_bytes())),
        )
    }

    fn binary(variable: u32, observation: u64, data: &'static [u8]) -> LongString<'static> {
        LongString::new(
            variable,
            observation,
            LongStringContent::Binary(Cow::Borrowed(data)),
        )
    }

    async fn round_trip<F, Fut>(
        release: Release,
        byte_order: ByteOrder,
        write_fn: F,
    ) -> Vec<OwnedLongString>
    where
        F: FnOnce(AsyncLongStringWriter<Cursor<Vec<u8>>>) -> Fut,
        Fut: std::future::Future<Output = AsyncLongStringWriter<Cursor<Vec<u8>>>>,
    {
        let schema = Schema::builder()
            .add_variable(Variable::builder(VariableType::Byte, "x").format("%8.0g"))
            .build()
            .unwrap();
        let header = Header::builder(release, byte_order).build();
        let long_string_writer = DtaWriter::new()
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
        let long_string_writer = write_fn(long_string_writer).await;
        let cursor: Cursor<Vec<u8>> = long_string_writer
            .into_value_label_writer()
            .await
            .unwrap()
            .finish()
            .await
            .unwrap();
        let bytes = cursor.into_inner();

        let mut long_string_reader = DtaReader::new()
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
        let mut entries = Vec::new();
        while let Some(ls) = long_string_reader.read_long_string().await.unwrap() {
            entries.push(OwnedLongString::from(&ls));
        }
        entries
    }

    // -- V117 round-trips ---------------------------------------------------

    #[tokio::test]
    async fn xml_v117_single_long_string_round_trip() {
        let entries = round_trip(Release::V117, ByteOrder::LittleEndian, |mut w| async move {
            w.write_long_string(&text(1, 1, "hello")).await.unwrap();
            w
        })
        .await;
        assert_eq!(
            entries,
            vec![OwnedLongString {
                variable: 1,
                observation: 1,
                binary: false,
                data: b"hello".to_vec(),
            }],
        );
    }

    #[tokio::test]
    async fn xml_v117_big_endian_round_trip() {
        let entries = round_trip(Release::V117, ByteOrder::BigEndian, |mut w| async move {
            w.write_long_string(&text(3, 7, "endian test"))
                .await
                .unwrap();
            w
        })
        .await;
        assert_eq!(entries[0].variable, 3);
        assert_eq!(entries[0].observation, 7);
        assert_eq!(entries[0].data, b"endian test");
    }

    #[tokio::test]
    async fn xml_v117_binary_payload_round_trip() {
        let payload: &[u8] = b"\x00\x01\x02\x80\xFF";
        let entries = round_trip(Release::V117, ByteOrder::LittleEndian, |mut w| async move {
            w.write_long_string(&binary(1, 1, payload)).await.unwrap();
            w
        })
        .await;
        assert!(entries[0].binary);
        assert_eq!(entries[0].data, payload);
    }

    #[tokio::test]
    async fn xml_v117_multiple_entries_round_trip() {
        let entries = round_trip(Release::V117, ByteOrder::LittleEndian, |mut w| async move {
            w.write_long_string(&text(1, 1, "first")).await.unwrap();
            w.write_long_string(&text(1, 2, "second")).await.unwrap();
            w.write_long_string(&text(2, 1, "third")).await.unwrap();
            w
        })
        .await;
        assert_eq!(entries.len(), 3);
        assert_eq!(entries[0].data, b"first");
        assert_eq!(entries[1].data, b"second");
        assert_eq!(entries[2].data, b"third");
    }

    #[tokio::test]
    async fn xml_v117_empty_section_still_emits_tags() {
        let entries =
            round_trip(Release::V117, ByteOrder::LittleEndian, |w| async move { w }).await;
        assert!(entries.is_empty());
    }

    // -- V118 round-trips (wider observation field) -------------------------

    #[tokio::test]
    async fn xml_v118_u64_observation_round_trip() {
        let entries = round_trip(Release::V118, ByteOrder::LittleEndian, |mut w| async move {
            w.write_long_string(&text(1, 5_000_000_000, "wide obs"))
                .await
                .unwrap();
            w
        })
        .await;
        assert_eq!(entries[0].observation, 5_000_000_000);
        assert_eq!(entries[0].data, b"wide obs");
    }

    // -- write_long_string_table --------------------------------------------

    #[tokio::test]
    async fn write_long_string_table_round_trip() {
        let mut table = LongStringTable::for_writing();
        table.get_or_insert(1, 1, LongStringContent::Text(Cow::Borrowed(b"apple")));
        table.get_or_insert(1, 2, LongStringContent::Text(Cow::Borrowed(b"banana")));
        table.get_or_insert(2, 1, LongStringContent::Text(Cow::Borrowed(b"carrot")));
        let duplicate_ref =
            table.get_or_insert(99, 99, LongStringContent::Text(Cow::Borrowed(b"apple")));
        assert_eq!(duplicate_ref.variable(), 1);
        assert_eq!(duplicate_ref.observation(), 1);
        assert_eq!(table.len(), 3);

        let entries = round_trip(Release::V117, ByteOrder::LittleEndian, |mut w| async move {
            w.write_long_string_table(&table).await.unwrap();
            w
        })
        .await;
        assert_eq!(entries.len(), 3);
        assert_eq!(entries[0].data, b"apple");
        assert_eq!(entries[0].variable, 1);
        assert_eq!(entries[0].observation, 1);
        assert_eq!(entries[1].data, b"banana");
        assert_eq!(entries[2].data, b"carrot");
        assert_eq!(entries[2].variable, 2);
    }

    #[tokio::test]
    async fn write_long_string_table_empty_on_v117_round_trip() {
        let table = LongStringTable::for_writing();
        let entries = round_trip(Release::V117, ByteOrder::LittleEndian, |mut w| async move {
            w.write_long_string_table(&table).await.unwrap();
            w
        })
        .await;
        assert!(entries.is_empty());
    }

    // -- Pre-117 rejection --------------------------------------------------

    async fn v114_long_string_writer() -> AsyncLongStringWriter<Cursor<Vec<u8>>> {
        let schema = Schema::builder()
            .add_variable(Variable::builder(VariableType::Byte, "x").format("%8.0g"))
            .build()
            .unwrap();
        let header = Header::builder(Release::V114, ByteOrder::LittleEndian).build();
        DtaWriter::new()
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
    }

    #[tokio::test]
    async fn pre_v117_rejects_write_long_string() {
        let mut writer = v114_long_string_writer().await;
        let error = writer
            .write_long_string(&text(1, 1, "anything"))
            .await
            .unwrap_err();
        assert!(matches!(
            error,
            DtaError::Format(ref e) if matches!(
                e.kind(),
                FormatErrorKind::LongStringsUnsupported { release: Release::V114 }
            )
        ));
    }

    #[tokio::test]
    async fn pre_v117_tolerates_empty_long_string_table() {
        let mut writer = v114_long_string_writer().await;
        let empty = LongStringTable::for_writing();
        writer.write_long_string_table(&empty).await.unwrap();
        // Transition should still succeed without emitting any
        // `<strls>` bytes.
        let _: Cursor<Vec<u8>> = writer
            .into_value_label_writer()
            .await
            .unwrap()
            .finish()
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn pre_v117_rejects_write_long_string_table_with_entries() {
        let mut writer = v114_long_string_writer().await;
        let mut table = LongStringTable::for_writing();
        table.get_or_insert(1, 1, LongStringContent::Text(Cow::Borrowed(b"x")));
        let error = writer.write_long_string_table(&table).await.unwrap_err();
        assert!(matches!(
            error,
            DtaError::Format(ref e) if matches!(
                e.kind(),
                FormatErrorKind::LongStringsUnsupported { release: Release::V114 }
            )
        ));
    }

    // -- V117 observation overflow ------------------------------------------

    #[tokio::test]
    async fn v117_observation_exceeds_u32_errors() {
        let mut writer = v117_long_string_writer().await;
        let big_observation = u64::from(u32::MAX) + 1;
        let error = writer
            .write_long_string(&text(1, big_observation, "oops"))
            .await
            .unwrap_err();
        assert!(matches!(
            error,
            DtaError::Format(ref e) if matches!(
                e.kind(),
                FormatErrorKind::FieldTooLarge { field: Field::ObservationCount, .. }
            )
        ));
    }

    async fn v117_long_string_writer() -> AsyncLongStringWriter<Cursor<Vec<u8>>> {
        let schema = Schema::builder()
            .add_variable(Variable::builder(VariableType::Byte, "x").format("%8.0g"))
            .build()
            .unwrap();
        let header = Header::builder(Release::V117, ByteOrder::LittleEndian).build();
        DtaWriter::new()
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
    }
}
