use tokio::io::AsyncRead;

use super::async_reader_state::AsyncReaderState;
use super::dta_error::{DtaError, Field, FormatErrorKind, Result, Section};
use super::header::Header;
use super::schema::Schema;
use super::value_label::{ValueLabelEntry, ValueLabelTable};
use super::value_label_parse::{
    VALUE_LABELS_CLOSE_REST, XmlLabelTag, classify_xml_label_tag, decode_label, entry_index_to_i32,
    overflow_error, parse_modern_payload,
};

/// Reads value-label tables from a DTA file asynchronously.
///
/// Owns the parsed [`Header`] and [`Schema`] from previous phases.
/// Yields [`ValueLabelTable`] entries via iteration.
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
    /// Reads the next value-label table.
    ///
    /// Returns `None` when all tables have been consumed. Each table
    /// contains a name and a set of integer-to-string mappings.
    ///
    /// # Errors
    ///
    /// Returns [`DtaError::Io`] on read failures and
    /// [`DtaError::Format`] when the table bytes violate the DTA
    /// format specification.
    pub async fn read_value_label_table(&mut self) -> Result<Option<ValueLabelTable>> {
        if self.completed {
            return Ok(None);
        }
        if self.header.release().has_old_value_labels() {
            self.read_old_table().await
        } else {
            self.read_modern_table().await
        }
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
            while self.skip_old_table().await? {}
        } else {
            while self.skip_modern_table().await? {}
        }
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Shared helpers
// ---------------------------------------------------------------------------

impl<R: AsyncRead + Unpin> AsyncValueLabelReader<R> {
    /// Reads the table name and skips the trailing padding bytes.
    async fn read_table_name(&mut self) -> Result<String> {
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

    /// Skips the table name and trailing padding bytes without
    /// decoding.
    async fn skip_table_name(&mut self) -> Result<()> {
        let release = self.header.release();
        let skip_len = release.value_label_name_len() + release.value_label_table_padding_len();
        self.state.skip(skip_len, Section::ValueLabels).await
    }
}

// ---------------------------------------------------------------------------
// Old value labels (format 104)
// ---------------------------------------------------------------------------

impl<R: AsyncRead + Unpin> AsyncValueLabelReader<R> {
    async fn read_old_table(&mut self) -> Result<Option<ValueLabelTable>> {
        let Some(table_len) = self.read_old_table_header().await? else {
            return Ok(None);
        };

        let name = self.read_table_name().await?;
        let encoding = self.state.encoding();

        let entry_count = table_len / 8;
        let payload = self
            .state
            .read_exact(table_len, Section::ValueLabels)
            .await?;

        let mut entries = Vec::with_capacity(entry_count);
        for entry_index in 0..entry_count {
            let label_bytes = &payload[8 * entry_index..8 * entry_index + 8];
            if label_bytes[0] == 0 {
                continue;
            }
            let label = decode_label(label_bytes, 8, encoding)?;
            let value = entry_index_to_i32(entry_index)?;
            let entry = ValueLabelEntry::new(value, label);
            entries.push(entry);
        }

        let table = ValueLabelTable::new(name, entries);
        Ok(Some(table))
    }

    async fn read_old_table_header(&mut self) -> Result<Option<usize>> {
        let byte_order = self.header.byte_order();
        let table_len = self
            .state
            .try_read_u16(byte_order, Section::ValueLabels)
            .await?;
        let Some(table_len) = table_len else {
            self.completed = true;
            return Ok(None);
        };
        let table_len_usize = usize::from(table_len);
        Ok(Some(table_len_usize))
    }

    async fn skip_old_table(&mut self) -> Result<bool> {
        let Some(table_len) = self.read_old_table_header().await? else {
            return Ok(false);
        };
        self.skip_table_name().await?;
        self.state.skip(table_len, Section::ValueLabels).await?;
        Ok(true)
    }
}

// ---------------------------------------------------------------------------
// Modern value labels (format 105+)
// ---------------------------------------------------------------------------

impl<R: AsyncRead + Unpin> AsyncValueLabelReader<R> {
    async fn read_modern_table(&mut self) -> Result<Option<ValueLabelTable>> {
        let Some(table_len) = self.read_modern_table_header().await? else {
            return Ok(None);
        };

        let name = self.read_table_name().await?;
        let byte_order = self.header.byte_order();
        let encoding = self.state.encoding();

        let payload = self
            .state
            .read_exact(table_len, Section::ValueLabels)
            .await?;
        let table = parse_modern_payload(payload, byte_order, encoding, &name)?;

        self.read_modern_table_footer().await?;
        Ok(Some(table))
    }

    async fn read_modern_table_header(&mut self) -> Result<Option<usize>> {
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

        let table_len = self
            .state
            .try_read_u32(byte_order, Section::ValueLabels)
            .await?;
        let Some(table_len) = table_len else {
            self.completed = true;
            return Ok(None);
        };
        let table_len = usize::try_from(table_len).map_err(|_| overflow_error())?;
        Ok(Some(table_len))
    }

    async fn read_modern_table_footer(&mut self) -> Result<()> {
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

    async fn skip_modern_table(&mut self) -> Result<bool> {
        let Some(table_len) = self.read_modern_table_header().await? else {
            return Ok(false);
        };
        self.skip_table_name().await?;
        self.state.skip(table_len, Section::ValueLabels).await?;
        self.read_modern_table_footer().await?;
        Ok(true)
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
