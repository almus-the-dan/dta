#![warn(missing_docs)]

//! A pure Rust reader and writer for Stata's DTA file format.
//!
//! DTA is the binary format [Stata](https://www.stata.com/) uses to
//! persist datasets. This crate covers every released version of the
//! format (104 through 119), including XML-framed releases (117+),
//! tagged missing values, value-label sets, and long-string (`strL`)
//! storage.
//!
//! The API is built around a typestate chain — you walk through the
//! sections of a file in order, and each phase hands the underlying
//! I/O handle to the next. See the [README] for the full tour.
//!
//! [README]: https://github.com/almus-the-dan/dta/#readme
//!
//! # Reading a DTA file
//!
//! ```no_run
//! use dta::stata::dta::dta_reader::DtaReader;
//! use dta::stata::dta::dta_error::Result;
//!
//! # fn demo() -> Result<()> {
//! let mut characteristic_reader = DtaReader::new()
//!     .from_path("example.dta")?
//!     .read_header()?
//!     .read_schema()?;
//!
//! // Characteristics are optional — skip them if you don't care.
//! characteristic_reader.skip_to_end()?;
//!
//! // Iterate observation rows.
//! let mut record_reader = characteristic_reader.into_record_reader()?;
//! let schema = record_reader.schema().clone();
//! while let Some(record) = record_reader.read_record()? {
//!     for (variable, value) in schema.variables().iter().zip(record.values()) {
//!         println!("{}: {:?}", variable.name(), value);
//!     }
//! }
//! # Ok(())
//! # }
//! ```
//!
//! # Writing a DTA file
//!
//! ```no_run
//! use dta::stata::dta::byte_order::ByteOrder;
//! use dta::stata::dta::dta_error::Result;
//! use dta::stata::dta::dta_writer::DtaWriter;
//! use dta::stata::dta::header::Header;
//! use dta::stata::dta::release::Release;
//! use dta::stata::dta::schema::Schema;
//! use dta::stata::dta::value::Value;
//! use dta::stata::dta::variable::Variable;
//! use dta::stata::dta::variable_type::VariableType;
//! use dta::stata::stata_long::StataLong;
//!
//! # fn demo() -> Result<()> {
//! let header = Header::builder(Release::V118, ByteOrder::LittleEndian).build();
//! let schema = Schema::builder()
//!     .add_variable(Variable::builder(VariableType::Long, "id").format("%12.0g"))
//!     .build()?;
//!
//! let mut record_writer = DtaWriter::new()
//!     .from_path("example.dta")?
//!     .write_header(header)?
//!     .write_schema(schema)?
//!     .into_record_writer()?;
//! record_writer.write_record(&[Value::Long(StataLong::Present(1))])?;
//!
//! record_writer
//!     .into_long_string_writer()?
//!     .into_value_label_writer()?
//!     .finish()?;
//! # Ok(())
//! # }
//! ```
//!
//! # Round-trip (runnable)
//!
//! Both sides together against an in-memory buffer, so this example
//! actually executes in the test harness:
//!
//! ```
//! use std::io::Cursor;
//! use dta::stata::dta::byte_order::ByteOrder;
//! use dta::stata::dta::dta_error::Result;
//! use dta::stata::dta::dta_reader::DtaReader;
//! use dta::stata::dta::dta_writer::DtaWriter;
//! use dta::stata::dta::header::Header;
//! use dta::stata::dta::release::Release;
//! use dta::stata::dta::schema::Schema;
//! use dta::stata::dta::value::Value;
//! use dta::stata::dta::variable::Variable;
//! use dta::stata::dta::variable_type::VariableType;
//! use dta::stata::stata_long::StataLong;
//!
//! # fn demo() -> Result<()> {
//! let header = Header::builder(Release::V118, ByteOrder::LittleEndian).build();
//! let schema = Schema::builder()
//!     .add_variable(Variable::builder(VariableType::Long, "id").format("%12.0g"))
//!     .build()?;
//!
//! let mut record_writer = DtaWriter::new()
//!     .from_writer(Cursor::new(Vec::<u8>::new()))
//!     .write_header(header)?
//!     .write_schema(schema)?
//!     .into_record_writer()?;
//! record_writer.write_record(&[Value::Long(StataLong::Present(42))])?;
//! let bytes = record_writer
//!     .into_long_string_writer()?
//!     .into_value_label_writer()?
//!     .finish()?
//!     .into_inner();
//!
//! let mut characteristic_reader = DtaReader::new()
//!     .from_reader(Cursor::new(bytes))
//!     .read_header()?
//!     .read_schema()?;
//! characteristic_reader.skip_to_end()?;
//! let mut record_reader = characteristic_reader.into_record_reader()?;
//! let record = record_reader.read_record()?.unwrap();
//! assert_eq!(record.values().len(), 1);
//! # Ok(())
//! # }
//! # demo().unwrap();
//! ```
//!
//! # Async
//!
//! Enable the `tokio` feature for async reader and writer mirrors —
//! same typestate chain, `.await` at each step. `DtaReader::from_tokio_*`
//! / `DtaWriter::from_tokio_*` are the entry points.

/// Stata file format types and utilities.
pub mod stata;
