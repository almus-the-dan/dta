use super::byte_order::ByteOrder;
use super::release::Release;
use crate::stata::stata_timestamp::StataTimestamp;

/// Parsed header from a DTA file.
///
/// Contains format metadata needed to interpret the rest of the file:
/// the format version (release), byte order, variable and observation
/// counts, and optional dataset label and timestamp.
///
/// Construct via [`Header::builder`].
#[derive(Debug, Clone)]
pub struct Header {
    release: Release,
    byte_order: ByteOrder,
    variable_count: u32,
    observation_count: u64,
    dataset_label: String,
    timestamp: Option<StataTimestamp>,
}

impl Header {
    /// Returns a new [`HeaderBuilder`] with the given release and byte
    /// order. All other fields default to zero / empty / `None`.
    #[must_use]
    #[inline]
    pub fn builder(release: Release, byte_order: ByteOrder) -> HeaderBuilder {
        HeaderBuilder {
            release,
            byte_order,
            variable_count: 0,
            observation_count: 0,
            dataset_label: String::new(),
            timestamp: None,
        }
    }

    /// Format version.
    #[must_use]
    #[inline]
    pub fn release(&self) -> Release {
        self.release
    }

    /// Byte order used for multibyte values in the file.
    #[must_use]
    #[inline]
    pub fn byte_order(&self) -> ByteOrder {
        self.byte_order
    }

    /// Number of variables (columns). Widened to `u32` to accommodate
    /// format 119, which supports more than 65,535 variables.
    #[must_use]
    #[inline]
    pub fn variable_count(&self) -> u32 {
        self.variable_count
    }

    /// Number of observations (rows). Widened to `u64` to accommodate
    /// format 119, which supports more than 2^31 - 1 observations.
    #[must_use]
    #[inline]
    pub fn observation_count(&self) -> u64 {
        self.observation_count
    }

    /// Dataset label, decoded to a string using the file's encoding.
    #[must_use]
    #[inline]
    pub fn dataset_label(&self) -> &str {
        &self.dataset_label
    }

    /// Timestamp, if present in the file.
    #[must_use]
    #[inline]
    pub fn timestamp(&self) -> Option<&StataTimestamp> {
        self.timestamp.as_ref()
    }
}

/// Builder for [`Header`].
///
/// Created via [`Header::builder`]. Release and byte order are
/// required; all other fields have sensible defaults.
#[derive(Debug, Clone)]
pub struct HeaderBuilder {
    release: Release,
    byte_order: ByteOrder,
    variable_count: u32,
    observation_count: u64,
    dataset_label: String,
    timestamp: Option<StataTimestamp>,
}

impl HeaderBuilder {
    /// Sets the number of variables (columns).
    ///
    /// Crate-private: the writer derives this from the schema at
    /// writing time, so users do not set it directly. The reader still
    /// uses it to populate [`Header`] from parsed file bytes.
    #[must_use]
    #[inline]
    pub(crate) fn variable_count(mut self, count: u32) -> Self {
        self.variable_count = count;
        self
    }

    /// Sets the number of observations (rows).
    ///
    /// Crate-private: the writer derives this from the record stream
    /// at writing time, so users do not set it directly. The reader
    /// still uses it to populate [`Header`] from parsed file bytes.
    #[must_use]
    #[inline]
    pub(crate) fn observation_count(mut self, count: u64) -> Self {
        self.observation_count = count;
        self
    }

    /// Sets the dataset label.
    #[must_use]
    #[inline]
    pub fn dataset_label(mut self, label: impl Into<String>) -> Self {
        self.dataset_label = label.into();
        self
    }

    /// Sets the timestamp.
    #[must_use]
    #[inline]
    pub fn timestamp(mut self, timestamp: Option<StataTimestamp>) -> Self {
        self.timestamp = timestamp;
        self
    }

    /// Builds the [`Header`].
    #[must_use]
    #[inline]
    pub fn build(self) -> Header {
        Header {
            release: self.release,
            byte_order: self.byte_order,
            variable_count: self.variable_count,
            observation_count: self.observation_count,
            dataset_label: self.dataset_label,
            timestamp: self.timestamp,
        }
    }
}
