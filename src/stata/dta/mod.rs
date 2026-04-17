/// In-memory [`Seek`](std::io::Seek) adapter for write-only sinks.
pub mod buffered_seek;
/// Byte order (endianness) representation.
pub mod byte_order;
/// A single characteristic entry (key-value metadata).
pub mod characteristic;
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
/// Entry point for reading a DTA file.
pub mod header_reader;
/// Entry point for writing a DTA file.
pub mod header_writer;
/// A lazily parsed observation row.
pub mod lazy_record;
/// Decoded long string (strL) entry.
pub mod long_string;
/// Reads long string (strL) entries (format 118+ only).
pub mod long_string_reader;
/// Unresolved reference to a long string in the strL section.
pub mod long_string_ref;
/// Writes long string (strL) entries to a DTA file.
pub mod long_string_writer;
mod reader_state;
/// An eagerly parsed observation row.
pub mod record;
/// Reads observation records from the data section.
pub mod record_reader;
/// Writes observation records to the data section.
pub mod record_writer;
/// DTA format version (release number).
pub mod release;
/// Variable definitions and layout.
pub mod schema;
/// Reads variable definitions from a DTA file.
pub mod schema_reader;
/// Writes variable definitions to a DTA file.
pub mod schema_writer;
/// Byte offsets for post-schema sections.
mod section_offsets;
/// Cell value from the data section.
pub mod value;
/// Named table mapping integer values to string labels.
pub mod value_label;
/// Reads value-label tables from a DTA file.
pub mod value_label_reader;
/// Writes value-label tables to a DTA file.
pub mod value_label_writer;
/// Single variable (column) definition.
pub mod variable;
/// Variable storage type.
pub mod variable_type;
mod writer_state;
