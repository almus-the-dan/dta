use super::byte_order::ByteOrder;
use crate::stata::stata_timestamp::StataTimestamp;

/// Parsed header from a DTA file.
///
/// Contains format metadata needed to interpret the rest of the file:
/// the format version (release), byte order, variable and observation
/// counts, and optional dataset label and timestamp.
#[derive(Debug, Clone)]
pub struct Header {
    release: u8,
    byte_order: ByteOrder,
    variable_count: u32,
    observation_count: u64,
    dataset_label: String,
    timestamp: Option<StataTimestamp>,
}

impl Header {
    /// Format version number (e.g., 113, 114, 115, 117, 118, 119).
    #[must_use]
    pub fn release(&self) -> u8 {
        self.release
    }

    /// Byte order used for multibyte values in the file.
    #[must_use]
    pub fn byte_order(&self) -> ByteOrder {
        self.byte_order
    }

    /// Number of variables (columns). Widened to `u32` to accommodate
    /// format 119, which supports more than 65,535 variables.
    #[must_use]
    pub fn variable_count(&self) -> u32 {
        self.variable_count
    }

    /// Number of observations (rows). Widened to `u64` to accommodate
    /// format 119, which supports more than 2^31 - 1 observations.
    #[must_use]
    pub fn observation_count(&self) -> u64 {
        self.observation_count
    }

    /// Dataset label, decoded to a string using the file's encoding.
    #[must_use]
    pub fn dataset_label(&self) -> &str {
        &self.dataset_label
    }

    /// Timestamp, if present in the file.
    #[must_use]
    pub fn timestamp(&self) -> Option<&StataTimestamp> {
        self.timestamp.as_ref()
    }
}
