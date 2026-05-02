//! Reader for Stata dictionary (`.dct`) files.
//!
//! A `.dct` dictionary describes the schema of a fixed-width or
//! free-format plain-text data file. This module parses the
//! dictionary, then iterates observations from the associated data
//! file. Data may be embedded after the dictionary's closing `}` or
//! live in a separate file; see [`DctSource`].
//!
//! Unlike the binary DTA format, DCT files are plain ASCII and never
//! carry `strL` references, so the DCT-domain
//! [`Value`] / [`VariableType`] types deliberately diverge from
//! their DTA counterparts.

/// Per-variable column declaration parsed from the dictionary.
pub mod column;
/// Error type for DCT parsing and reading.
pub mod dct_error;
/// Data-row reader paired with a parsed schema.
pub mod dct_reader;
/// External vs. embedded data classification returned by the parser.
pub mod dct_source;
/// Non-fatal warning channel.
pub mod dct_warning;
/// Input format vocabulary derived from the `%infmt` token.
pub mod input_format;
/// Numeric sub-format (fixed-point, general, scientific) implied by a
/// fixed-width numeric `%infmt`.
pub mod numeric_style;
/// Dictionary parser entry points (`parse_dct`, `open_dct`).
pub mod parser;
/// A single parsed observation.
pub mod record;
/// The parsed dictionary, excluding data.
pub mod schema;
/// A single parsed cell value.
pub mod value;
/// Storage type vocabulary used by `DctColumn`.
pub mod variable_type;

pub use column::Column;
pub use dct_error::{DctError, Result};
pub use dct_reader::DctReader;
pub use dct_source::DctSource;
pub use dct_warning::DctWarning;
pub use input_format::InputFormat;
pub use numeric_style::NumericStyle;
pub use parser::{open_dct, parse_dct};
pub use record::Record;
pub use schema::Schema;
pub use value::Value;
pub use variable_type::VariableType;
