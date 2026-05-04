#![warn(missing_docs)]

//! A pure Rust reader and writer for Stata data formats.
//!
//! Two related formats live in this crate:
//!
//! - **DTA** — Stata's binary dataset format ([`stata::dta`]). Every
//!   released version is supported (104 through 119), including
//!   XML-framed releases (117+), tagged missing values, value-label
//!   sets, and long-string (`strL`) storage. The API is built around
//!   a typestate chain — you walk through the sections of a file in
//!   order, and each phase hands the underlying I/O handle to the
//!   next.
//! - **DCT** — Stata's plain-text dictionary format ([`stata::dct`]).
//!   Describes the schema of a fixed-width or free-format data file.
//!   The reader is a two-step builder: parse the dictionary, then
//!   pair the resulting schema with a data source.
//!
//! Format-agnostic Stata-domain types — `MissingValue`,
//! `StataByte`/`Int`/`Long`/`Float`/`Double`, `StataTimestamp`, the
//! temporal helpers — live at [`stata`] and are shared between the
//! two formats.
//!
//! See the [README] for the full tour, including DCT examples.
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
//! # Reading a DCT dictionary + data file
//!
//! ```no_run
//! use dta::stata::dct::dct_reader::DctReader;
//! use dta::stata::dct::dct_source::DctSource;
//! use dta::stata::dct::dct_error::Result;
//!
//! # fn demo() -> Result<()> {
//! let source = DctSource::options().from_path("schema.dct")?;
//! let mut reader = match source {
//!     DctSource::External(schema) => {
//!         DctReader::options(schema).from_path("data.dat")?
//!     }
//!     DctSource::Embedded { schema, reader } => {
//!         DctReader::options(schema).from_reader(reader)
//!     }
//! };
//!
//! // Capture column names up front: the lending pattern means
//! // `record` borrows the reader exclusively, so `reader.schema()`
//! // can't be called inside the loop body.
//! let column_names: Vec<String> = reader
//!     .schema()
//!     .columns()
//!     .iter()
//!     .map(|c| c.name().to_string())
//!     .collect();
//!
//! while let Some(record) = reader.read_record()? {
//!     for (name, value) in column_names.iter().zip(record.values()) {
//!         println!("{}: {:?}", name, value);
//!     }
//! }
//! # Ok(())
//! # }
//! ```
//!
//! # Async
//!
//! Enable the `tokio` feature for async mirrors of every entry point.
//! Same typestate chain, `.await` at each step:
//!
//! - DTA: `DtaReader::from_tokio_*` / `DtaWriter::from_tokio_*`
//! - DCT: `DctSource::options().from_tokio_*` and
//!   `DctReader::options(schema).from_tokio_*`
//!
//! The async DCT paths share the same pure parsing state with the
//! sync paths — the only difference is `.await` on `read_line` and
//! `fill_buf`.

/// Stata file format types and utilities.
pub mod stata;
