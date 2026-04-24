use tokio::io::AsyncRead;

use super::async_reader_state::AsyncReaderState;
use super::dta_error::{DtaError, Field, FormatErrorKind, Result, Section};
use super::header::Header;
use super::schema::Schema;
use super::value_label::ValueLabelSet;
use super::value_label_parse::{
    OLD_VALUE_LABEL_SIZE, VALUE_LABELS_CLOSE_REST, XmlLabelTag, classify_xml_label_tag,
    overflow_error, parse_modern_payload, parse_old_payload,
};
use super::value_label_table::ValueLabelTable;

/// Reads value-label sets from a DTA file asynchronously.
///
/// Owns the parsed [`Header`] and [`Schema`] from previous phases.
/// Yields [`ValueLabelSet`] entries via iteration.
#[derive(Debug)]
pub struct AsyncValueLabelReader<R> {
    state: AsyncReaderState<R>,
    header: Header,
    schema: Schema,
    opened: bool,
    completed: bool,
}

impl<R> AsyncValueLabelReader<R> {
    #[must_use]
    pub(crate) fn new(state: AsyncReaderState<R>, header: Header, schema: Schema) -> Self {
        Self {
            state,
            header,
            schema,
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

// ---------------------------------------------------------------------------
// Sequential reading (AsyncRead)
// ---------------------------------------------------------------------------

impl<R: AsyncRead + Unpin> AsyncValueLabelReader<R> {
    /// Reads the next value-label set.
    ///
    /// Returns `None` when all sets have been consumed. Each set
    /// contains a name and integer-to-string label mappings.
    ///
    /// # Errors
    ///
    /// Returns [`DtaError::Io`] on read failures and
    /// [`DtaError::Format`] when the set bytes violate the DTA
    /// format specification.
    pub async fn read_value_label_set(&mut self) -> Result<Option<ValueLabelSet>> {
        if self.completed {
            return Ok(None);
        }
        if self.header.release().has_old_value_labels() {
            self.read_old_set().await
        } else {
            self.read_modern_set().await
        }
    }

    /// Reads all remaining value-label sets into `table`, keyed by
    /// set name.
    ///
    /// Sets are inserted with first-wins semantics: if `table` already contains
    /// a set for a given name, it is left untouched and the duplicate
    /// from the file is discarded.
    ///
    /// This method drains the reader to completion — after it
    /// returns, `self` is ready to be dropped.
    ///
    /// Pairs naturally with [`ValueLabelTable::label_for`] for looking
    /// up labels from record values.
    ///
    /// # Errors
    ///
    /// Returns [`DtaError::Io`] on read failures and
    /// [`DtaError::Format`] when the set bytes violate the DTA format
    /// specification.
    pub async fn read_remaining_into(&mut self, table: &mut ValueLabelTable) -> Result<()> {
        while let Some(set) = self.read_value_label_set().await? {
            if table.get(set.name()).is_none() {
                table.insert(set);
            }
        }
        Ok(())
    }

    /// Skips all remaining value-label entries without processing
    /// them.
    ///
    /// # Errors
    ///
    /// Returns [`DtaError::Io`] on read failures and
    /// [`DtaError::Format`] if section tags (XML formats) are missing
    /// or malformed.
    pub async fn skip_to_end(&mut self) -> Result<()> {
        if self.completed {
            return Ok(());
        }
        if self.header.release().has_old_value_labels() {
            while self.skip_old_set().await? {}
        } else {
            while self.skip_modern_set().await? {}
        }
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Shared helpers
// ---------------------------------------------------------------------------

impl<R: AsyncRead + Unpin> AsyncValueLabelReader<R> {
    /// Reads the set name and skips the trailing padding bytes.
    async fn read_set_name(&mut self) -> Result<String> {
        let release = self.header.release();
        let encoding = self.state.encoding();
        let name = self
            .state
            .read_fixed_string(
                release.value_label_name_len(),
                encoding,
                Section::ValueLabels,
                Field::ValueLabelName,
            )
            .await?;
        self.state
            .skip(
                release.value_label_table_padding_len(),
                Section::ValueLabels,
            )
            .await?;
        Ok(name)
    }

    /// Skips the set name and trailing padding bytes without
    /// decoding.
    async fn skip_set_name(&mut self) -> Result<()> {
        let release = self.header.release();
        let skip_len = release.value_label_name_len() + release.value_label_table_padding_len();
        self.state.skip(skip_len, Section::ValueLabels).await
    }
}

// ---------------------------------------------------------------------------
// Old value labels (format 104-107)
// ---------------------------------------------------------------------------
//
// Pre-V108 sets have the layout:
//   u16 n          — entry count
//   char[9] name
//   byte pad
//   u16[n] values  — little-/big-endian per the file's byte order
//   char[8][n]     — fixed-width, null-padded labels

impl<R: AsyncRead + Unpin> AsyncValueLabelReader<R> {
    async fn read_old_set(&mut self) -> Result<Option<ValueLabelSet>> {
        let Some(entry_count) = self.read_old_entry_count().await? else {
            return Ok(None);
        };

        let name = self.read_set_name().await?;
        let byte_order = self.header.byte_order();
        let encoding = self.state.encoding();

        let payload_len = old_payload_len(entry_count)?;
        let payload = self
            .state
            .read_exact(payload_len, Section::ValueLabels)
            .await?;
        let set = parse_old_payload(payload, byte_order, encoding, &name)?;
        Ok(Some(set))
    }

    async fn read_old_entry_count(&mut self) -> Result<Option<usize>> {
        let byte_order = self.header.byte_order();
        let entry_count = self
            .state
            .try_read_u16(byte_order, Section::ValueLabels)
            .await?;
        let Some(entry_count) = entry_count else {
            self.completed = true;
            return Ok(None);
        };
        Ok(Some(usize::from(entry_count)))
    }

    async fn skip_old_set(&mut self) -> Result<bool> {
        let Some(entry_count) = self.read_old_entry_count().await? else {
            return Ok(false);
        };
        self.skip_set_name().await?;
        let payload_len = old_payload_len(entry_count)?;
        self.state.skip(payload_len, Section::ValueLabels).await?;
        Ok(true)
    }
}

fn old_payload_len(entry_count: usize) -> Result<usize> {
    entry_count
        .checked_mul(2 + OLD_VALUE_LABEL_SIZE)
        .ok_or_else(overflow_error)
}

// ---------------------------------------------------------------------------
// Modern value labels (format 105+)
// ---------------------------------------------------------------------------

impl<R: AsyncRead + Unpin> AsyncValueLabelReader<R> {
    async fn read_modern_set(&mut self) -> Result<Option<ValueLabelSet>> {
        let Some(set_len) = self.read_modern_set_header().await? else {
            return Ok(None);
        };

        let name = self.read_set_name().await?;
        let byte_order = self.header.byte_order();
        let encoding = self.state.encoding();

        let payload = self.state.read_exact(set_len, Section::ValueLabels).await?;
        let set = parse_modern_payload(payload, byte_order, encoding, &name)?;

        self.read_modern_set_footer().await?;
        Ok(Some(set))
    }

    async fn read_modern_set_header(&mut self) -> Result<Option<usize>> {
        let byte_order = self.header.byte_order();
        let is_xml = self.header.release().is_xml_like();

        if is_xml {
            if !self.opened {
                self.state
                    .expect_bytes(
                        b"<value_labels>",
                        Section::ValueLabels,
                        FormatErrorKind::InvalidMagic,
                    )
                    .await?;
                self.opened = true;
            }

            if let XmlLabelTag::SectionClose = self.read_xml_label_or_close().await? {
                self.completed = true;
                return Ok(None);
            }
        }

        let set_len = self
            .state
            .try_read_u32(byte_order, Section::ValueLabels)
            .await?;
        let Some(set_len) = set_len else {
            self.completed = true;
            return Ok(None);
        };
        let set_len = usize::try_from(set_len).map_err(|_| overflow_error())?;
        Ok(Some(set_len))
    }

    async fn skip_modern_set(&mut self) -> Result<bool> {
        let Some(set_len) = self.read_modern_set_header().await? else {
            return Ok(false);
        };
        self.skip_set_name().await?;
        self.state.skip(set_len, Section::ValueLabels).await?;
        self.read_modern_set_footer().await?;
        Ok(true)
    }

    async fn read_modern_set_footer(&mut self) -> Result<()> {
        if self.header.release().is_xml_like() {
            self.state
                .expect_bytes(
                    b"</lbl>",
                    Section::ValueLabels,
                    FormatErrorKind::InvalidMagic,
                )
                .await?;
        }
        Ok(())
    }

    async fn read_xml_label_or_close(&mut self) -> Result<XmlLabelTag> {
        let position = self.state.position();
        let head = self.state.read_exact(5, Section::ValueLabels).await?;
        let tag = classify_xml_label_tag(head).ok_or_else(|| {
            DtaError::format(
                Section::ValueLabels,
                position,
                FormatErrorKind::InvalidMagic,
            )
        })?;
        if let XmlLabelTag::SectionClose = tag {
            self.state
                .expect_bytes(
                    VALUE_LABELS_CLOSE_REST,
                    Section::ValueLabels,
                    FormatErrorKind::InvalidMagic,
                )
                .await?;
        }
        Ok(tag)
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

    async fn build_file_with_sets(release: Release, sets: &[ValueLabelSet]) -> Vec<u8> {
        let schema = Schema::builder()
            .add_variable(Variable::builder(VariableType::Byte, "x").format("%8.0g"))
            .build()
            .unwrap();
        let header = Header::builder(release, ByteOrder::LittleEndian).build();
        let mut value_label_writer = DtaWriter::new()
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
        for set in sets {
            value_label_writer.write_value_label_set(set).await.unwrap();
        }
        value_label_writer.finish().await.unwrap().into_inner()
    }

    async fn value_label_reader_for(
        bytes: &[u8],
    ) -> AsyncValueLabelReader<impl AsyncRead + Unpin + '_> {
        DtaReader::new()
            .from_tokio_reader(bytes)
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
            .unwrap()
    }

    #[tokio::test]
    async fn read_remaining_into_populates_table() {
        let bytes = build_file_with_sets(
            Release::V117,
            &[
                ValueLabelSet::new("a".to_owned(), entries(&[(0, "zero"), (1, "one")])),
                ValueLabelSet::new("b".to_owned(), entries(&[(-1, "neg")])),
            ],
        )
        .await;
        let mut reader = value_label_reader_for(&bytes).await;

        let mut table = ValueLabelTable::new();
        reader.read_remaining_into(&mut table).await.unwrap();

        assert_eq!(table.len(), 2);
        assert_eq!(table.get("a").unwrap().label_for(0), Some("zero"));
        assert_eq!(table.get("a").unwrap().label_for(1), Some("one"));
        assert_eq!(table.get("b").unwrap().label_for(-1), Some("neg"));
    }

    #[tokio::test]
    async fn read_remaining_into_works_on_old_format() {
        let bytes = build_file_with_sets(
            Release::V104,
            &[ValueLabelSet::new(
                "old".to_owned(),
                entries(&[(0, "a"), (1, "b")]),
            )],
        )
        .await;
        let mut reader = value_label_reader_for(&bytes).await;

        let mut table = ValueLabelTable::new();
        reader.read_remaining_into(&mut table).await.unwrap();

        assert_eq!(table.len(), 1);
        assert_eq!(table.get("old").unwrap().label_for(0), Some("a"));
    }

    #[tokio::test]
    async fn read_remaining_into_first_wins_over_pre_existing_entries() {
        let bytes = build_file_with_sets(
            Release::V117,
            &[ValueLabelSet::new(
                "shared".to_owned(),
                entries(&[(1, "from file")]),
            )],
        )
        .await;
        let mut reader = value_label_reader_for(&bytes).await;

        let mut table = ValueLabelTable::new();
        table.insert(ValueLabelSet::new(
            "shared".to_owned(),
            entries(&[(1, "pre-existing")]),
        ));
        reader.read_remaining_into(&mut table).await.unwrap();

        assert_eq!(table.len(), 1);
        assert_eq!(
            table.get("shared").unwrap().label_for(1),
            Some("pre-existing")
        );
    }
}
