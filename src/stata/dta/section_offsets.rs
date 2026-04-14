/// Byte offsets for each post-schema section in a DTA file.
///
/// For XML formats (117+), all offsets are read from the `<map>`
/// section during schema parsing. For binary formats (104–116), the
/// variable characteristics offset is known after schema reading; the data and
/// value-label offsets are computed by the characteristic reader after
/// it consumes the expansion fields.
#[derive(Debug, Clone, Copy)]
pub(crate) struct SectionOffsets {
    characteristics: u64,
    records: u64,
    value_labels: u64,
    long_strings: Option<u64>,
}

impl SectionOffsets {
    /// Creates a new set of section offsets.
    ///
    /// Pass `None` for `long_strings` when the format does not have a
    /// strL section (binary formats 104–116).
    #[inline]
    pub fn new(
        characteristics: u64,
        data: u64,
        value_labels: u64,
        long_strings: Option<u64>,
    ) -> Self {
        Self {
            characteristics,
            records: data,
            value_labels,
            long_strings,
        }
    }

    /// Absolute byte offset of the characteristics section.
    #[must_use]
    #[inline]
    pub fn characteristics(&self) -> u64 {
        self.characteristics
    }

    /// Absolute byte offset of the data (records) section.
    #[must_use]
    #[inline]
    pub fn records(&self) -> u64 {
        self.records
    }

    /// Absolute byte offset of the value-labels section.
    #[must_use]
    #[inline]
    pub fn value_labels(&self) -> u64 {
        self.value_labels
    }

    /// Absolute byte offset of the long-strings (strL) section, or
    /// `None` if the format does not have one.
    #[must_use]
    #[inline]
    pub fn long_strings(&self) -> Option<u64> {
        self.long_strings
    }

    /// Sets the data-section offset.
    ///
    /// Used by the characteristic reader for binary formats, where the
    /// offset is not known until expansion fields have been consumed.
    #[inline]
    pub fn set_records(&mut self, offset: u64) {
        self.records = offset;
    }

    /// Sets the value-labels section offset.
    ///
    /// Used by the characteristic reader for binary formats, where the
    /// offset is not known until the data section size can be computed.
    #[inline]
    pub fn set_value_labels(&mut self, offset: u64) {
        self.value_labels = offset;
    }
}
