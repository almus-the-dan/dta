/// Byte order (endianness) representation.
pub mod byte_order;
/// Unified error type for the DTA reader.
pub mod dta_error;
/// Creates a header reader.
pub mod dta_reader;
/// Allows configuring a reader.
pub mod dta_reader_options;
/// Parsed DTA file header.
pub mod header;
/// Entry point for reading a DTA file.
pub mod header_reader;
/// Decoded long string (strL) entry.
pub mod long_string;
/// Reads long string (strL) entries (format 118+ only).
pub mod long_string_reader;
/// Unresolved reference to a long string in the strL section.
pub mod long_string_ref;
mod reader_state;
/// Reads observation records from the data section.
pub mod record_reader;
/// Variable definitions and layout.
pub mod schema;
/// Reads variable definitions from a DTA file.
pub mod schema_reader;
/// Cell value from the data section.
pub mod value;
/// Named table mapping integer values to string labels.
pub mod value_label;
/// Reads value-label tables from a DTA file.
pub mod value_label_reader;
