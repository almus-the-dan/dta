use tokio::io::{AsyncSeek, AsyncWrite, AsyncWriteExt};

use super::async_writer_state::AsyncWriterState;
use super::dta_error::{DtaError, Field, FormatErrorKind, Result, Section};
use super::header::Header;
use super::schema::Schema;
use super::value_label::ValueLabelTable;
use super::value_label_format::{build_modern_text_payload, build_old_slot_table};

/// Writes value-label tables asynchronously — the last section of a
/// DTA file.
///
/// Call [`write_value_label_table`](Self::write_value_label_table)
/// once per table, then [`finish`](Self::finish) to close the section
/// (XML formats only), flush the sink, and recover the underlying
/// writer.
#[derive(Debug)]
pub struct AsyncValueLabelWriter<W> {
    state: AsyncWriterState<W>,
    header: Header,
    schema: Schema,
    /// Tracks whether the XML `<value_labels>` opening tag has been
    /// emitted. Unused (but harmless) for pre-117 formats, which have
    /// no section tag.
    opened: bool,
}

impl<W> AsyncValueLabelWriter<W> {
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
}

impl<W: AsyncWrite + AsyncSeek + Unpin> AsyncValueLabelWriter<W> {
    /// Writes a single value-label table.
    ///
    /// Can be called any number of times (including zero).
    ///
    /// # Errors
    ///
    /// Returns [`DtaError::Format`](DtaError::Format) if the table
    /// cannot be represented — a name or label exceeding its field
    /// width, a value outside the range supported by the release's
    /// layout, or text that cannot be encoded in the active encoding.
    /// Returns [`DtaError::Io`](DtaError::Io) on sink failures.
    pub async fn write_value_label_table(&mut self, table: &ValueLabelTable) -> Result<()> {
        if self.header.release().is_xml_like() {
            self.open_section_if_needed().await?;
        }
        if self.header.release().has_old_value_labels() {
            self.write_old_table(table).await
        } else {
            self.write_modern_table(table).await
        }
    }

    /// Closes the value-labels section (XML only), emits the final
    /// `</stata_dta>` tag (XML only), patches the end-of-file map
    /// slots (XML only), flushes the sink, and returns it.
    ///
    /// The returned writer is finalized — the DTA file is complete.
    /// Writing more bytes to it would corrupt the file.
    ///
    /// # Errors
    ///
    /// Returns [`DtaError::Io`](DtaError::Io) on sink failures while
    /// writing the closing tags or flushing.
    pub async fn finish(mut self) -> Result<W> {
        let release = self.header.release();
        let byte_order = self.header.byte_order();

        if release.is_xml_like() {
            self.open_section_if_needed().await?;
            self.state
                .write_exact(b"</value_labels>", Section::ValueLabels)
                .await?;

            let stata_dta_close_offset = self.state.position();
            self.state
                .write_exact(b"</stata_dta>", Section::ValueLabels)
                .await?;
            let eof_offset = self.state.position();

            self.state
                .patch_map_entry(12, stata_dta_close_offset, byte_order, Section::ValueLabels)
                .await?;
            self.state
                .patch_map_entry(13, eof_offset, byte_order, Section::ValueLabels)
                .await?;
        }

        let mut writer = self.state.into_inner();
        writer
            .flush()
            .await
            .map_err(|e| DtaError::io(Section::ValueLabels, e))?;
        Ok(writer)
    }

    async fn open_section_if_needed(&mut self) -> Result<()> {
        if !self.opened {
            self.state
                .write_exact(b"<value_labels>", Section::ValueLabels)
                .await?;
            self.opened = true;
        }
        Ok(())
    }
}

impl<W: AsyncWrite + AsyncSeek + Unpin> AsyncValueLabelWriter<W> {
    /// Writes one table in the V104 legacy layout.
    async fn write_old_table(&mut self, table: &ValueLabelTable) -> Result<()> {
        let release = self.header.release();
        let byte_order = self.header.byte_order();
        let position_before = self.state.position();

        let slots = build_old_slot_table(table, self.state.encoding(), position_before)?;
        let slot_count = slots.len();
        let table_len_u16 = slot_count
            .checked_mul(8)
            .and_then(|n| u16::try_from(n).ok())
            .ok_or_else(|| {
                // `actual` is only for error display — saturate at u64::MAX
                // so we report a useful number even if `slot_count * 8`
                // overflows `usize` on a 16-bit target.
                let actual = u64::try_from(slot_count)
                    .unwrap_or(u64::MAX)
                    .saturating_mul(8);
                DtaError::format(
                    Section::ValueLabels,
                    position_before,
                    FormatErrorKind::FieldTooLarge {
                        field: Field::ValueLabelEntry,
                        max: u64::from(u16::MAX),
                        actual,
                    },
                )
            })?;

        self.state
            .write_u16(table_len_u16, byte_order, Section::ValueLabels)
            .await?;
        self.state
            .write_fixed_string(
                table.name(),
                release.value_label_name_len(),
                Section::ValueLabels,
                Field::ValueLabelName,
            )
            .await?;
        // V104 padding is 2 bytes of zeros.
        self.state
            .write_padded_bytes(
                &[],
                release.value_label_table_padding_len(),
                Section::ValueLabels,
            )
            .await?;

        for slot in &slots {
            let bytes = slot.as_deref().unwrap_or_default();
            self.state
                .write_padded_bytes(bytes, 8, Section::ValueLabels)
                .await?;
        }
        Ok(())
    }

    /// Writes one table in the modern (V105+) layout.
    async fn write_modern_table(&mut self, table: &ValueLabelTable) -> Result<()> {
        let release = self.header.release();
        let byte_order = self.header.byte_order();
        let position_before = self.state.position();
        let entries = table.entries();

        let (encoded_labels, offsets, text_len) =
            build_modern_text_payload(entries, self.state.encoding(), position_before)?;
        let entry_count = u32::try_from(entries.len()).map_err(|_| {
            DtaError::format(
                Section::ValueLabels,
                position_before,
                FormatErrorKind::FieldTooLarge {
                    field: Field::ValueLabelEntry,
                    max: u64::from(u32::MAX),
                    actual: u64::try_from(entries.len()).unwrap_or(u64::MAX),
                },
            )
        })?;

        // Payload bytes = 8 (n + text_len) + 4*n (offsets) + 4*n (values) + text_len.
        let payload_bytes = u64::from(entry_count)
            .checked_mul(8)
            .and_then(|n| n.checked_add(8))
            .and_then(|n| n.checked_add(u64::from(text_len)))
            .ok_or_else(|| {
                DtaError::format(
                    Section::ValueLabels,
                    position_before,
                    FormatErrorKind::FieldTooLarge {
                        field: Field::ValueLabelEntry,
                        max: u64::from(u32::MAX),
                        actual: u64::MAX,
                    },
                )
            })?;
        let table_len = u32::try_from(payload_bytes).map_err(|_| {
            DtaError::format(
                Section::ValueLabels,
                position_before,
                FormatErrorKind::FieldTooLarge {
                    field: Field::ValueLabelEntry,
                    max: u64::from(u32::MAX),
                    actual: payload_bytes,
                },
            )
        })?;

        if release.is_xml_like() {
            self.state
                .write_exact(b"<lbl>", Section::ValueLabels)
                .await?;
        }

        self.state
            .write_u32(table_len, byte_order, Section::ValueLabels)
            .await?;
        self.state
            .write_fixed_string(
                table.name(),
                release.value_label_name_len(),
                Section::ValueLabels,
                Field::ValueLabelName,
            )
            .await?;
        // Modern padding (V105+) is 3 bytes of zeros.
        self.state
            .write_padded_bytes(
                &[],
                release.value_label_table_padding_len(),
                Section::ValueLabels,
            )
            .await?;

        // Payload header.
        self.state
            .write_u32(entry_count, byte_order, Section::ValueLabels)
            .await?;
        self.state
            .write_u32(text_len, byte_order, Section::ValueLabels)
            .await?;

        // Offsets.
        for offset in &offsets {
            self.state
                .write_u32(*offset, byte_order, Section::ValueLabels)
                .await?;
        }

        // Values.
        for entry in entries {
            self.state
                .write_i32(entry.value(), byte_order, Section::ValueLabels)
                .await?;
        }

        // Text area — each label followed by a null terminator.
        for label in &encoded_labels {
            self.state.write_exact(label, Section::ValueLabels).await?;
            self.state.write_u8(0, Section::ValueLabels).await?;
        }

        if release.is_xml_like() {
            self.state
                .write_exact(b"</lbl>", Section::ValueLabels)
                .await?;
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;

    use super::*;
    use crate::stata::dta::byte_order::ByteOrder;
    use crate::stata::dta::dta_reader::DtaReader;
    use crate::stata::dta::dta_writer::DtaWriter;
    use crate::stata::dta::release::Release;
    use crate::stata::dta::value_label::ValueLabelEntry;
    use crate::stata::dta::variable::Variable;
    use crate::stata::dta::variable_type::VariableType;

    fn entries(pairs: &[(i32, &str)]) -> Vec<ValueLabelEntry> {
        pairs
            .iter()
            .map(|&(v, l)| ValueLabelEntry::new(v, l.to_owned()))
            .collect()
    }

    async fn round_trip<F, Fut>(
        release: Release,
        byte_order: ByteOrder,
        write_fn: F,
    ) -> Vec<ValueLabelTable>
    where
        F: FnOnce(AsyncValueLabelWriter<Cursor<Vec<u8>>>) -> Fut,
        Fut: std::future::Future<Output = AsyncValueLabelWriter<Cursor<Vec<u8>>>>,
    {
        let schema = Schema::builder()
            .add_variable(Variable::builder(VariableType::Byte, "x").format("%8.0g"))
            .build()
            .unwrap();
        let header = Header::builder(release, byte_order).build();
        let value_label_writer = DtaWriter::new()
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
            .unwrap();
        let value_label_writer = write_fn(value_label_writer).await;
        let cursor: Cursor<Vec<u8>> = value_label_writer.finish().await.unwrap();
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
            .unwrap()
            .into_value_label_reader()
            .await
            .unwrap();
        let mut tables = Vec::new();
        while let Some(table) = reader.read_value_label_table().await.unwrap() {
            tables.push(table);
        }
        tables
    }

    // -- Modern-layout round-trips (V105+) ----------------------------------

    #[tokio::test]
    async fn binary_v114_single_table_round_trip() {
        let table = ValueLabelTable::new(
            "pricelbl".to_owned(),
            entries(&[(0, "cheap"), (1, "pricey")]),
        );
        let tables = round_trip(Release::V114, ByteOrder::LittleEndian, |mut w| async move {
            w.write_value_label_table(&table).await.unwrap();
            w
        })
        .await;
        assert_eq!(tables.len(), 1);
        assert_eq!(tables[0].name(), "pricelbl");
        assert_eq!(tables[0].entries().len(), 2);
        assert_eq!(tables[0].entries()[0].value(), 0);
        assert_eq!(tables[0].entries()[0].label(), "cheap");
        assert_eq!(tables[0].entries()[1].value(), 1);
        assert_eq!(tables[0].entries()[1].label(), "pricey");
    }

    #[tokio::test]
    async fn binary_v114_multiple_tables_round_trip() {
        let tables = round_trip(Release::V114, ByteOrder::LittleEndian, |mut w| async move {
            let t1 = ValueLabelTable::new("a".to_owned(), entries(&[(0, "zero"), (1, "one")]));
            let t2 = ValueLabelTable::new("b".to_owned(), entries(&[(-1, "neg")]));
            w.write_value_label_table(&t1).await.unwrap();
            w.write_value_label_table(&t2).await.unwrap();
            w
        })
        .await;
        assert_eq!(tables.len(), 2);
        assert_eq!(tables[0].name(), "a");
        assert_eq!(tables[1].name(), "b");
        assert_eq!(tables[1].entries()[0].value(), -1);
        assert_eq!(tables[1].entries()[0].label(), "neg");
    }

    #[tokio::test]
    async fn binary_v114_empty_table_round_trip() {
        let table = ValueLabelTable::new("empty".to_owned(), Vec::new());
        let tables = round_trip(Release::V114, ByteOrder::LittleEndian, |mut w| async move {
            w.write_value_label_table(&table).await.unwrap();
            w
        })
        .await;
        assert_eq!(tables.len(), 1);
        assert_eq!(tables[0].name(), "empty");
        assert!(tables[0].entries().is_empty());
    }

    #[tokio::test]
    async fn binary_v114_big_endian_round_trip() {
        let table = ValueLabelTable::new("be".to_owned(), entries(&[(10, "ten"), (20, "twenty")]));
        let tables = round_trip(Release::V114, ByteOrder::BigEndian, |mut w| async move {
            w.write_value_label_table(&table).await.unwrap();
            w
        })
        .await;
        assert_eq!(tables[0].entries()[0].value(), 10);
        assert_eq!(tables[0].entries()[1].value(), 20);
    }

    #[tokio::test]
    async fn binary_v114_no_tables_round_trip() {
        let tables = round_trip(Release::V114, ByteOrder::LittleEndian, |w| async move { w }).await;
        assert!(tables.is_empty());
    }

    // -- XML round-trips ----------------------------------------------------

    #[tokio::test]
    async fn xml_v117_round_trip() {
        let tables = round_trip(Release::V117, ByteOrder::LittleEndian, |mut w| async move {
            let t1 =
                ValueLabelTable::new("pricelbl".to_owned(), entries(&[(1, "low"), (5, "high")]));
            let t2 = ValueLabelTable::new("empty".to_owned(), Vec::new());
            w.write_value_label_table(&t1).await.unwrap();
            w.write_value_label_table(&t2).await.unwrap();
            w
        })
        .await;
        assert_eq!(tables.len(), 2);
        assert_eq!(tables[0].entries()[0].value(), 1);
        assert_eq!(tables[0].entries()[1].value(), 5);
        assert_eq!(tables[0].entries()[1].label(), "high");
        assert_eq!(tables[1].name(), "empty");
        assert!(tables[1].entries().is_empty());
    }

    #[tokio::test]
    async fn xml_v117_no_tables_round_trip() {
        let tables = round_trip(Release::V117, ByteOrder::LittleEndian, |w| async move { w }).await;
        assert!(tables.is_empty());
    }

    #[tokio::test]
    async fn xml_v118_utf8_label_round_trip() {
        let tables = round_trip(Release::V118, ByteOrder::LittleEndian, |mut w| async move {
            let table =
                ValueLabelTable::new("lang".to_owned(), entries(&[(1, "日本語"), (2, "español")]));
            w.write_value_label_table(&table).await.unwrap();
            w
        })
        .await;
        assert_eq!(tables[0].entries()[0].label(), "日本語");
        assert_eq!(tables[0].entries()[1].label(), "español");
    }

    // -- V104 legacy layout -------------------------------------------------

    #[tokio::test]
    async fn v104_single_table_round_trip() {
        let tables = round_trip(Release::V104, ByteOrder::LittleEndian, |mut w| async move {
            let table = ValueLabelTable::new("old".to_owned(), entries(&[(0, "zero"), (2, "two")]));
            w.write_value_label_table(&table).await.unwrap();
            w
        })
        .await;
        assert_eq!(tables.len(), 1);
        assert_eq!(tables[0].entries().len(), 2);
        assert_eq!(tables[0].entries()[0].value(), 0);
        assert_eq!(tables[0].entries()[0].label(), "zero");
        assert_eq!(tables[0].entries()[1].value(), 2);
        assert_eq!(tables[0].entries()[1].label(), "two");
    }

    async fn v104_value_label_writer() -> AsyncValueLabelWriter<Cursor<Vec<u8>>> {
        let schema = Schema::builder()
            .add_variable(Variable::builder(VariableType::Byte, "x").format("%8.0g"))
            .build()
            .unwrap();
        let header = Header::builder(Release::V104, ByteOrder::LittleEndian).build();
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
            .into_value_label_writer()
            .await
            .unwrap()
    }

    #[tokio::test]
    async fn v104_rejects_negative_value() {
        let table = ValueLabelTable::new("neg".to_owned(), entries(&[(-1, "nope")]));
        let mut writer = v104_value_label_writer().await;
        let error = writer.write_value_label_table(&table).await.unwrap_err();
        assert!(matches!(
            error,
            DtaError::Format(ref e) if matches!(
                e.kind(),
                FormatErrorKind::OldValueLabelValueOutOfRange { value: -1 }
            )
        ));
    }

    #[tokio::test]
    async fn v104_rejects_duplicate_value() {
        let table = ValueLabelTable::new("dup".to_owned(), entries(&[(1, "a"), (1, "b")]));
        let mut writer = v104_value_label_writer().await;
        let error = writer.write_value_label_table(&table).await.unwrap_err();
        assert!(matches!(
            error,
            DtaError::Format(ref e) if matches!(
                e.kind(),
                FormatErrorKind::OldValueLabelValueOutOfRange { value: 1 }
            )
        ));
    }

    #[tokio::test]
    async fn label_too_long_in_v104_errors() {
        let table = ValueLabelTable::new("long".to_owned(), entries(&[(0, "nine char")]));
        let mut writer = v104_value_label_writer().await;
        let error = writer.write_value_label_table(&table).await.unwrap_err();
        assert!(matches!(
            error,
            DtaError::Format(ref e) if matches!(
                e.kind(),
                FormatErrorKind::FieldTooLarge { field: Field::ValueLabelEntry, .. }
            )
        ));
    }

    async fn v114_value_label_writer() -> AsyncValueLabelWriter<Cursor<Vec<u8>>> {
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
            .into_value_label_writer()
            .await
            .unwrap()
    }

    #[tokio::test]
    async fn table_name_too_long_errors() {
        let long_name = "n".repeat(50);
        let table = ValueLabelTable::new(long_name, Vec::new());
        let mut writer = v114_value_label_writer().await;
        let error = writer.write_value_label_table(&table).await.unwrap_err();
        assert!(matches!(
            error,
            DtaError::Format(ref e) if matches!(
                e.kind(),
                FormatErrorKind::FieldTooLarge { field: Field::ValueLabelName, .. }
            )
        ));
    }

    #[tokio::test]
    async fn non_latin_label_in_windows_1252_errors() {
        let table = ValueLabelTable::new("lang".to_owned(), entries(&[(1, "日")]));
        let mut writer = v114_value_label_writer().await;
        let error = writer.write_value_label_table(&table).await.unwrap_err();
        assert!(matches!(
            error,
            DtaError::Format(ref e) if matches!(
                e.kind(),
                FormatErrorKind::InvalidEncoding { field: Field::ValueLabelEntry }
            )
        ));
    }
}
