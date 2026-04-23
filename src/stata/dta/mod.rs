/// Async reader for characteristics / expansion fields (tokio feature).
#[cfg(feature = "tokio")]
pub mod async_characteristic_reader;
/// Async writer for characteristics / expansion fields (tokio feature).
#[cfg(feature = "tokio")]
pub mod async_characteristic_writer;
/// Async entry point for reading a DTA file (tokio feature).
#[cfg(feature = "tokio")]
pub mod async_header_reader;
/// Async entry point for writing a DTA file (tokio feature).
#[cfg(feature = "tokio")]
pub mod async_header_writer;
/// Async reader for long-string (strL) entries (tokio feature).
#[cfg(feature = "tokio")]
pub mod async_long_string_reader;
/// Async writer for long-string (strL) entries (tokio feature).
#[cfg(feature = "tokio")]
pub mod async_long_string_writer;
#[cfg(feature = "tokio")]
mod async_reader_state;
/// Async reader for observation records (tokio feature).
#[cfg(feature = "tokio")]
pub mod async_record_reader;
/// Async writer for observation records (tokio feature).
#[cfg(feature = "tokio")]
pub mod async_record_writer;
/// Async reader for variable definitions (tokio feature).
#[cfg(feature = "tokio")]
pub mod async_schema_reader;
/// Async writer for variable definitions (tokio feature).
#[cfg(feature = "tokio")]
pub mod async_schema_writer;
/// Async reader for value-label sets (tokio feature).
#[cfg(feature = "tokio")]
pub mod async_value_label_reader;
/// Async writer for value-label sets (tokio feature).
#[cfg(feature = "tokio")]
pub mod async_value_label_writer;
#[cfg(feature = "tokio")]
mod async_writer_state;
/// Byte order (endianness) representation.
pub mod byte_order;
/// A single characteristic entry (key-value metadata).
pub mod characteristic;
mod characteristic_format;
mod characteristic_parse;
/// Reads characteristics (expansion fields) from a DTA file.
pub mod characteristic_reader;
/// Writes characteristics (expansion fields) to a DTA file.
pub mod characteristic_writer;
/// Unified error type for the DTA reader.
pub mod dta_error;
/// Entry point for configuring and opening a DTA file reader.
pub mod dta_reader;
/// Entry point for configuring and opening a DTA file writer.
pub mod dta_writer;
/// Parsed DTA file header.
pub mod header;
mod header_format;
mod header_parse;
/// Entry point for reading a DTA file.
pub mod header_reader;
/// Entry point for writing a DTA file.
pub mod header_writer;
/// A lazily parsed observation row.
pub mod lazy_record;
/// Decoded long string (strL) entry.
pub mod long_string;
mod long_string_format;
mod long_string_parse;
/// Reads long string (strL) entries (format 118+ only).
pub mod long_string_reader;
/// Unresolved reference to a long string in the strL section.
pub mod long_string_ref;
/// Deduplicating table of long string entries for writing.
pub mod long_string_table;
/// Writes long string (strL) entries to a DTA file.
pub mod long_string_writer;
mod reader_state;
/// An eagerly parsed observation row.
pub mod record;
mod record_format;
mod record_parse;
/// Reads observation records from the data section.
pub mod record_reader;
/// Writes observation records to the data section.
pub mod record_writer;
/// DTA format version (release number).
pub mod release;
/// Variable definitions and layout.
pub mod schema;
mod schema_format;
mod schema_parse;
/// Reads variable definitions from a DTA file.
pub mod schema_reader;
/// Writes variable definitions to a DTA file.
pub mod schema_writer;
/// Byte offsets for post-schema sections.
mod section_offsets;
mod string_decoding;
mod string_encoding;
/// Cell value from the data section.
pub mod value;
/// Named set mapping integer values to string labels.
pub mod value_label;
mod value_label_format;
mod value_label_parse;
/// Reads value-label sets from a DTA file.
pub mod value_label_reader;
/// Keyed collection of value-label sets with resolver helpers.
pub mod value_label_table;
/// Writes value-label sets to a DTA file.
pub mod value_label_writer;
/// Single variable (column) definition.
pub mod variable;
/// Variable storage type.
pub mod variable_type;
mod writer_state;
