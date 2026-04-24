use tokio::io::{AsyncSeek, AsyncWrite, AsyncWriteExt};

use super::async_writer_state::AsyncWriterState;
use super::dta_error::{DtaError, Field, Result, Section};
use super::header::Header;
use super::schema::Schema;
use super::value_label::ValueLabelSet;
use super::value_label_format::{
    build_modern_text_payload, encode_old_entries, modern_payload_bytes, narrow_entry_count_to_u32,
    narrow_old_entry_count_to_u16,
};
use super::value_label_parse::OLD_VALUE_LABEL_SIZE;
use super::value_label_table::ValueLabelTable;

/// Writes value-label sets asynchronously — the last section of a
/// DTA file.
///
/// Call [`write_value_label_set`](Self::write_value_label_set)
/// once per set, then [`finish`](Self::finish) to close the section
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
    /// Writes a single value-label set.
    ///
    /// Can be called any number of times (including zero).
    ///
    /// # Errors
    ///
    /// Returns [`DtaError::Format`](DtaError::Format) if the set
    /// cannot be represented — a name or label exceeding its field
    /// width, a value outside the range supported by the release's
    /// layout, or text that cannot be encoded in the active encoding.
    /// Returns [`DtaError::Io`](DtaError::Io) on sink failures.
    pub async fn write_value_label_set(&mut self, set: &ValueLabelSet) -> Result<()> {
        if self.header.release().is_xml_like() {
            self.open_section_if_needed().await?;
        }
        if self.header.release().has_old_value_labels() {
            self.write_old_set(set).await
        } else {
            self.write_modern_set(set).await
        }
    }

    /// Writes every set in `table` via
    /// [`write_value_label_set`](Self::write_value_label_set).
    ///
    /// An empty table is a no-op. Iteration order is not stable
    /// (the underlying map is a [`HashMap`](std::collections::HashMap)),
    /// but value-label sets in a DTA file are independent of each
    /// other, so order does not affect round-trips.
    ///
    /// # Errors
    ///
    /// Surfaces the first error from
    /// [`write_value_label_set`](Self::write_value_label_set).
    pub async fn write_value_label_table(&mut self, table: &ValueLabelTable) -> Result<()> {
        for set in table.iter() {
            self.write_value_label_set(set).await?;
        }
        Ok(())
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
    /// Writes one set in the V104-V107 legacy layout:
    ///
    /// - `n`: `u16` — entry count
    /// - `name`: fixed-width 9 bytes + 1-byte padding
    /// - `values`: `u16[n]` — the `i16` encoding of each entry's value
    /// - `labels`: `char[8][n]` — null-padded labels, 8 bytes each
    async fn write_old_set(&mut self, set: &ValueLabelSet) -> Result<()> {
        let release = self.header.release();
        let byte_order = self.header.byte_order();
        let position_before = self.state.position();

        let encoded_labels = encode_old_entries(set, self.state.encoding(), position_before)?;
        let entry_count = narrow_old_entry_count_to_u16(encoded_labels.len(), position_before)?;

        self.state
            .write_u16(entry_count, byte_order, Section::ValueLabels)
            .await?;
        self.state
            .write_fixed_string(
                set.name(),
                release.value_label_name_len(),
                Section::ValueLabels,
                Field::ValueLabelName,
            )
            .await?;
        // Pre-V108 padding after the name is 1 byte of zeros.
        self.state
            .write_padded_bytes(
                &[],
                release.value_label_table_padding_len(),
                Section::ValueLabels,
            )
            .await?;

        // Values as `u16` (round-tripped through `i16` so the negative
        // range survives). `encode_old_entries` already checked the
        // range, so the cast can't truncate.
        for entry in set.entries() {
            let signed = i16::try_from(entry.value())
                .expect("encode_old_entries verified the value fits in i16");
            self.state
                .write_u16(signed.cast_unsigned(), byte_order, Section::ValueLabels)
                .await?;
        }

        for label in &encoded_labels {
            self.state
                .write_padded_bytes(label, OLD_VALUE_LABEL_SIZE, Section::ValueLabels)
                .await?;
        }
        Ok(())
    }

    /// Writes one set in the modern (V105+) layout.
    async fn write_modern_set(&mut self, set: &ValueLabelSet) -> Result<()> {
        let release = self.header.release();
        let byte_order = self.header.byte_order();
        let position_before = self.state.position();
        let entries = set.entries();

        let (encoded_labels, offsets, text_len) =
            build_modern_text_payload(entries, self.state.encoding(), position_before)?;
        let entry_count = narrow_entry_count_to_u32(entries.len(), position_before)?;
        let table_len = modern_payload_bytes(entry_count, text_len, position_before)?;

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
                set.name(),
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
    use crate::stata::dta::dta_error::FormatErrorKind;
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
    ) -> Vec<ValueLabelSet>
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
        let mut sets = Vec::new();
        while let Some(set) = reader.read_value_label_set().await.unwrap() {
            sets.push(set);
        }
        sets
    }

    // -- Modern-layout round-trips (V105+) ----------------------------------

    #[tokio::test]
    async fn binary_v114_single_set_round_trip() {
        let set = ValueLabelSet::new(
            "pricelbl".to_owned(),
            entries(&[(0, "cheap"), (1, "pricey")]),
        );
        let sets = round_trip(Release::V114, ByteOrder::LittleEndian, |mut w| async move {
            w.write_value_label_set(&set).await.unwrap();
            w
        })
        .await;
        assert_eq!(sets.len(), 1);
        assert_eq!(sets[0].name(), "pricelbl");
        assert_eq!(sets[0].entries().len(), 2);
        assert_eq!(sets[0].entries()[0].value(), 0);
        assert_eq!(sets[0].entries()[0].label(), "cheap");
        assert_eq!(sets[0].entries()[1].value(), 1);
        assert_eq!(sets[0].entries()[1].label(), "pricey");
    }

    #[tokio::test]
    async fn binary_v114_multiple_sets_round_trip() {
        let sets = round_trip(Release::V114, ByteOrder::LittleEndian, |mut w| async move {
            let set1 = ValueLabelSet::new("a".to_owned(), entries(&[(0, "zero"), (1, "one")]));
            let set2 = ValueLabelSet::new("b".to_owned(), entries(&[(-1, "neg")]));
            w.write_value_label_set(&set1).await.unwrap();
            w.write_value_label_set(&set2).await.unwrap();
            w
        })
        .await;
        assert_eq!(sets.len(), 2);
        assert_eq!(sets[0].name(), "a");
        assert_eq!(sets[1].name(), "b");
        assert_eq!(sets[1].entries()[0].value(), -1);
        assert_eq!(sets[1].entries()[0].label(), "neg");
    }

    #[tokio::test]
    async fn binary_v114_empty_set_round_trip() {
        let set = ValueLabelSet::new("empty".to_owned(), Vec::new());
        let sets = round_trip(Release::V114, ByteOrder::LittleEndian, |mut w| async move {
            w.write_value_label_set(&set).await.unwrap();
            w
        })
        .await;
        assert_eq!(sets.len(), 1);
        assert_eq!(sets[0].name(), "empty");
        assert!(sets[0].entries().is_empty());
    }

    #[tokio::test]
    async fn binary_v114_big_endian_round_trip() {
        let set = ValueLabelSet::new("be".to_owned(), entries(&[(10, "ten"), (20, "twenty")]));
        let sets = round_trip(Release::V114, ByteOrder::BigEndian, |mut w| async move {
            w.write_value_label_set(&set).await.unwrap();
            w
        })
        .await;
        assert_eq!(sets[0].entries()[0].value(), 10);
        assert_eq!(sets[0].entries()[1].value(), 20);
    }

    #[tokio::test]
    async fn binary_v114_no_sets_round_trip() {
        let sets = round_trip(Release::V114, ByteOrder::LittleEndian, |w| async move { w }).await;
        assert!(sets.is_empty());
    }

    // -- XML round-trips ----------------------------------------------------

    #[tokio::test]
    async fn xml_v117_round_trip() {
        let sets = round_trip(Release::V117, ByteOrder::LittleEndian, |mut w| async move {
            let set1 =
                ValueLabelSet::new("pricelbl".to_owned(), entries(&[(1, "low"), (5, "high")]));
            let set2 = ValueLabelSet::new("empty".to_owned(), Vec::new());
            w.write_value_label_set(&set1).await.unwrap();
            w.write_value_label_set(&set2).await.unwrap();
            w
        })
        .await;
        assert_eq!(sets.len(), 2);
        assert_eq!(sets[0].entries()[0].value(), 1);
        assert_eq!(sets[0].entries()[1].value(), 5);
        assert_eq!(sets[0].entries()[1].label(), "high");
        assert_eq!(sets[1].name(), "empty");
        assert!(sets[1].entries().is_empty());
    }

    #[tokio::test]
    async fn xml_v117_no_sets_round_trip() {
        let sets = round_trip(Release::V117, ByteOrder::LittleEndian, |w| async move { w }).await;
        assert!(sets.is_empty());
    }

    // -- write_value_label_table --------------------------------------------

    #[tokio::test]
    async fn write_value_label_table_round_trip() {
        use crate::stata::dta::value_label_table::ValueLabelTable;

        let mut table = ValueLabelTable::new();
        table.insert(ValueLabelSet::new(
            "pricelbl".to_owned(),
            entries(&[(0, "cheap"), (1, "pricey")]),
        ));
        table.insert(ValueLabelSet::new(
            "ratinglbl".to_owned(),
            entries(&[(1, "low"), (5, "high")]),
        ));

        let mut sets = round_trip(Release::V117, ByteOrder::LittleEndian, |mut w| async move {
            w.write_value_label_table(&table).await.unwrap();
            w
        })
        .await;
        sets.sort_by(|a, b| a.name().cmp(b.name()));
        assert_eq!(sets.len(), 2);
        assert_eq!(sets[0].name(), "pricelbl");
        assert_eq!(sets[0].label_for(0), Some("cheap"));
        assert_eq!(sets[1].name(), "ratinglbl");
        assert_eq!(sets[1].label_for(5), Some("high"));
    }

    #[tokio::test]
    async fn write_value_label_table_empty_round_trip() {
        use crate::stata::dta::value_label_table::ValueLabelTable;

        let sets = round_trip(Release::V117, ByteOrder::LittleEndian, |mut w| async move {
            w.write_value_label_table(&ValueLabelTable::new())
                .await
                .unwrap();
            w
        })
        .await;
        assert!(sets.is_empty());
    }

    #[tokio::test]
    async fn xml_v118_utf8_label_round_trip() {
        let sets = round_trip(Release::V118, ByteOrder::LittleEndian, |mut w| async move {
            let set =
                ValueLabelSet::new("lang".to_owned(), entries(&[(1, "日本語"), (2, "español")]));
            w.write_value_label_set(&set).await.unwrap();
            w
        })
        .await;
        assert_eq!(sets[0].entries()[0].label(), "日本語");
        assert_eq!(sets[0].entries()[1].label(), "español");
    }

    // -- V104 legacy layout -------------------------------------------------

    #[tokio::test]
    async fn v104_single_set_round_trip() {
        let sets = round_trip(Release::V104, ByteOrder::LittleEndian, |mut w| async move {
            let set = ValueLabelSet::new("old".to_owned(), entries(&[(0, "zero"), (2, "two")]));
            w.write_value_label_set(&set).await.unwrap();
            w
        })
        .await;
        assert_eq!(sets.len(), 1);
        assert_eq!(sets[0].entries().len(), 2);
        assert_eq!(sets[0].entries()[0].value(), 0);
        assert_eq!(sets[0].entries()[0].label(), "zero");
        assert_eq!(sets[0].entries()[1].value(), 2);
        assert_eq!(sets[0].entries()[1].label(), "two");
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
        let set = ValueLabelSet::new("neg".to_owned(), entries(&[(-1, "nope")]));
        let mut writer = v104_value_label_writer().await;
        let error = writer.write_value_label_set(&set).await.unwrap_err();
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
        let set = ValueLabelSet::new("dup".to_owned(), entries(&[(1, "a"), (1, "b")]));
        let mut writer = v104_value_label_writer().await;
        let error = writer.write_value_label_set(&set).await.unwrap_err();
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
        let set = ValueLabelSet::new("long".to_owned(), entries(&[(0, "nine char")]));
        let mut writer = v104_value_label_writer().await;
        let error = writer.write_value_label_set(&set).await.unwrap_err();
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
    async fn set_name_too_long_errors() {
        let long_name = "n".repeat(50);
        let set = ValueLabelSet::new(long_name, Vec::new());
        let mut writer = v114_value_label_writer().await;
        let error = writer.write_value_label_set(&set).await.unwrap_err();
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
        let set = ValueLabelSet::new("lang".to_owned(), entries(&[(1, "日")]));
        let mut writer = v114_value_label_writer().await;
        let error = writer.write_value_label_set(&set).await.unwrap_err();
        assert!(matches!(
            error,
            DtaError::Format(ref e) if matches!(
                e.kind(),
                FormatErrorKind::InvalidEncoding { field: Field::ValueLabelEntry }
            )
        ));
    }
}
