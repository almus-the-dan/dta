//! Integration test for pre-v113 missing-value decoding.
//!
//! The bytes inlined below are a 363-byte DTA v104 file produced by
//! pandas' test suite (originally at `pandas/tests/io/data/stata/` in
//! the pandas-dev/pandas repo). The schema holds one observation with
//! five variables — `f_miss`, `d_miss`, `b_miss`, `i_miss`, `l_miss` —
//! each carrying the format's system-missing sentinel.
//!
//! The specific sentinel bit patterns are:
//!
//! - `b_miss` = `0x7F` (127)
//! - `i_miss` = `0x7FFF` (32767)
//! - `l_miss` = `0x7FFF_FFFF` (2147483647)
//! - `f_miss` = `0x7F00_0000` (≈ 1.7014e38, just above the valid max)
//! - `d_miss` = `0x54C0_0000_0000_0000` (= 2^333 ≈ 1.7472e100, the
//!   V104/V105 "magic double" sentinel that falls inside the normal
//!   IEEE-754 range)
//!
//! A bug in `ReadStat` (and anything built on it, including `haven` in R
//! and prior versions of this crate) misreads all five as present data
//! because it uses V113+ NaN-based sentinel rules unconditionally.

use dta::stata::dta::byte_order::ByteOrder;
use dta::stata::dta::dta_error::{DtaError, FormatErrorKind};
use dta::stata::dta::dta_reader::DtaReader;
use dta::stata::dta::dta_writer::DtaWriter;
use dta::stata::dta::header::Header;
use dta::stata::dta::release::Release;
use dta::stata::dta::schema::Schema;
use dta::stata::dta::value::Value;
use dta::stata::dta::variable::Variable;
use dta::stata::dta::variable_type::VariableType;
use dta::stata::missing_value::MissingValue;
use dta::stata::stata_byte::StataByte;
use dta::stata::stata_double::StataDouble;
use dta::stata::stata_float::StataFloat;
use dta::stata::stata_int::StataInt;
use dta::stata::stata_long::StataLong;

use std::io::Cursor;

/// Byte-for-byte copy of `stata1_104.dta` (pandas fixture). 363 bytes.
#[rustfmt::skip]
const STATA1_V104_BYTES: &[u8] = &[
    // Header
    0x68, 0x02, 0x01, 0x00,                         // ds_format=104, byteorder=LSF, filetype=1, unused
    0x05, 0x00,                                     // nvar=5
    0x01, 0x00, 0x00, 0x00,                         // nobs=1 (u32 in V104)
    // data_label (32 bytes of zeros)
    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
    // typlist (5 bytes)
    b'f', b'd', b'b', b'i', b'l',
    // varlist (5 × 9 = 45 bytes)
    b'f', b'_', b'm', b'i', b's', b's', 0, 0, 0,
    b'd', b'_', b'm', b'i', b's', b's', 0, 0, 0,
    b'b', b'_', b'm', b'i', b's', b's', 0, 0, 0,
    b'i', b'_', b'm', b'i', b's', b's', 0, 0, 0,
    b'l', b'_', b'm', b'i', b's', b's', 0, 0, 0,
    // srtlist ((5+1) × 2 = 12 bytes)
    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
    // fmtlist (5 × 7 = 35 bytes)
    b'%', b'9', b'.', b'0', b'g', 0, 0,
    b'%', b'1', b'0', b'.', b'0', b'g', 0,
    b'%', b'8', b'.', b'0', b'g', 0, 0,
    b'%', b'8', b'.', b'0', b'g', 0, 0,
    b'%', b'1', b'2', b'.', b'0', b'g', 0,
    // lbllist (5 × 9 = 45 bytes of zeros)
    0, 0, 0, 0, 0, 0, 0, 0, 0,
    0, 0, 0, 0, 0, 0, 0, 0, 0,
    0, 0, 0, 0, 0, 0, 0, 0, 0,
    0, 0, 0, 0, 0, 0, 0, 0, 0,
    0, 0, 0, 0, 0, 0, 0, 0, 0,
    // varlabels (5 × 32 = 160 bytes of zeros)
    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
    // Data row: 4 + 8 + 1 + 2 + 4 = 19 bytes
    0x00, 0x00, 0x00, 0x7F,                         // f_miss = 1.7014118e+38 (LE)
    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xC0, 0x54, // d_miss = 2^333 (LE)
    0x7F,                                           // b_miss = 127
    0xFF, 0x7F,                                     // i_miss = 32767 (LE)
    0xFF, 0xFF, 0xFF, 0x7F,                         // l_miss = 2147483647 (LE)
];

#[test]
fn v104_fixture_length_is_363_bytes() {
    assert_eq!(STATA1_V104_BYTES.len(), 363);
}

#[test]
fn v104_fixture_all_five_missings_decode_as_system() {
    let schema_reader = DtaReader::default()
        .from_reader(Cursor::new(STATA1_V104_BYTES))
        .read_header()
        .expect("header parses");

    assert_eq!(schema_reader.header().release(), Release::V104);
    assert_eq!(schema_reader.header().variable_count(), 5);
    assert_eq!(schema_reader.header().observation_count(), 1);

    let mut characteristic_reader = schema_reader.read_schema().expect("schema parses");
    while characteristic_reader
        .read_characteristic()
        .expect("characteristic iteration")
        .is_some()
    {}

    let mut record_reader = characteristic_reader
        .into_record_reader()
        .expect("transition to record reader");

    let record = record_reader
        .read_record()
        .expect("record reads")
        .expect("exactly one record");
    let values = record.values();
    assert_eq!(values.len(), 5);

    // f_miss — previously decoded as a literal 1.7014e38
    assert_eq!(
        values[0],
        Value::Float(StataFloat::Missing(MissingValue::System)),
        "f_miss should be system missing"
    );

    // d_miss — the smoking gun: previously decoded as literal 1.7472e100
    assert_eq!(
        values[1],
        Value::Double(StataDouble::Missing(MissingValue::System)),
        "d_miss should be system missing (2^333 magic sentinel)"
    );

    // b_miss — previously decoded as MissingValue::Z
    assert_eq!(
        values[2],
        Value::Byte(StataByte::Missing(MissingValue::System)),
        "b_miss should be system missing, not .z"
    );

    // i_miss — previously decoded as MissingValue::Z
    assert_eq!(
        values[3],
        Value::Int(StataInt::Missing(MissingValue::System)),
        "i_miss should be system missing, not .z"
    );

    // l_miss — previously decoded as MissingValue::Z
    assert_eq!(
        values[4],
        Value::Long(StataLong::Missing(MissingValue::System)),
        "l_miss should be system missing, not .z"
    );

    assert!(
        record_reader
            .read_record()
            .expect("second read is idempotent")
            .is_none(),
        "file has exactly one row"
    );
}

/// Builds a v104 schema with one column of each of the five numeric
/// types, using the same variable names as the pandas fixture.
fn v104_numeric_schema() -> Schema {
    Schema::builder()
        .add_variable(Variable::builder(VariableType::Float, "f_miss").format("%9.0g"))
        .add_variable(Variable::builder(VariableType::Double, "d_miss").format("%10.0g"))
        .add_variable(Variable::builder(VariableType::Byte, "b_miss").format("%8.0g"))
        .add_variable(Variable::builder(VariableType::Int, "i_miss").format("%8.0g"))
        .add_variable(Variable::builder(VariableType::Long, "l_miss").format("%12.0g"))
        .build()
        .expect("schema builds")
}

/// Drives the writer chain end-to-end for a v104 file with a single
/// record, returning the serialized bytes.
fn write_v104_with(values: &[Value<'_>]) -> Result<Vec<u8>, DtaError> {
    let header = Header::builder(Release::V104, ByteOrder::LittleEndian).build();
    let schema = v104_numeric_schema();
    let characteristic_writer = DtaWriter::new()
        .from_writer(Cursor::new(Vec::<u8>::new()))
        .write_header(header)?
        .write_schema(schema)?;
    let mut record_writer = characteristic_writer.into_record_writer()?;
    record_writer.write_record(values)?;
    let bytes = record_writer
        .into_long_string_writer()?
        .into_value_label_writer()?
        .finish()?
        .into_inner();
    Ok(bytes)
}

#[test]
fn v104_round_trip_system_missings() {
    let values = [
        Value::Float(StataFloat::Missing(MissingValue::System)),
        Value::Double(StataDouble::Missing(MissingValue::System)),
        Value::Byte(StataByte::Missing(MissingValue::System)),
        Value::Int(StataInt::Missing(MissingValue::System)),
        Value::Long(StataLong::Missing(MissingValue::System)),
    ];
    let bytes = write_v104_with(&values).expect("write succeeds");

    let schema_reader = DtaReader::default()
        .from_reader(Cursor::new(bytes))
        .read_header()
        .expect("header parses");
    assert_eq!(schema_reader.header().release(), Release::V104);

    let mut characteristic_reader = schema_reader.read_schema().expect("schema parses");
    while characteristic_reader
        .read_characteristic()
        .expect("characteristic iteration")
        .is_some()
    {}

    let mut record_reader = characteristic_reader
        .into_record_reader()
        .expect("transition to record reader");
    let record = record_reader
        .read_record()
        .expect("record reads")
        .expect("exactly one record");
    assert_eq!(record.values(), values.as_slice());
}

#[test]
fn v104_writing_tagged_missing_errors() {
    // .a for a float column in a v104 file — must surface as
    // TaggedMissingUnsupported with the variable index set correctly.
    let values = [
        Value::Float(StataFloat::Missing(MissingValue::A)),
        Value::Double(StataDouble::Missing(MissingValue::System)),
        Value::Byte(StataByte::Missing(MissingValue::System)),
        Value::Int(StataInt::Missing(MissingValue::System)),
        Value::Long(StataLong::Missing(MissingValue::System)),
    ];
    let error = write_v104_with(&values).expect_err("tagged missing must fail on v104");
    match error {
        DtaError::Format(format_error) => match format_error.kind() {
            FormatErrorKind::TaggedMissingUnsupported {
                release,
                variable_index,
            } => {
                assert_eq!(release, Release::V104);
                assert_eq!(variable_index, 0);
            }
            other => panic!("expected TaggedMissingUnsupported, got {other:?}"),
        },
        other => panic!("expected Format error, got {other:?}"),
    }
}

#[test]
fn v104_writing_tagged_missing_errors_reports_correct_variable_index() {
    // .z on the long column (index 4) — confirms variable_index plumbing.
    let values = [
        Value::Float(StataFloat::Missing(MissingValue::System)),
        Value::Double(StataDouble::Missing(MissingValue::System)),
        Value::Byte(StataByte::Missing(MissingValue::System)),
        Value::Int(StataInt::Missing(MissingValue::System)),
        Value::Long(StataLong::Missing(MissingValue::Z)),
    ];
    let error = write_v104_with(&values).expect_err("tagged missing must fail on v104");
    match error {
        DtaError::Format(format_error) => match format_error.kind() {
            FormatErrorKind::TaggedMissingUnsupported {
                release: _,
                variable_index,
            } => {
                assert_eq!(variable_index, 4);
            }
            other => panic!("expected TaggedMissingUnsupported, got {other:?}"),
        },
        other => panic!("expected Format error, got {other:?}"),
    }
}

#[test]
fn v117_tagged_missing_round_trips() {
    // Positive control: on a modern format, the same tagged-missing
    // values that error on v104 round-trip cleanly.
    let header = Header::builder(Release::V117, ByteOrder::LittleEndian).build();
    let schema = v104_numeric_schema();
    let values = [
        Value::Float(StataFloat::Missing(MissingValue::A)),
        Value::Double(StataDouble::Missing(MissingValue::System)),
        Value::Byte(StataByte::Missing(MissingValue::System)),
        Value::Int(StataInt::Missing(MissingValue::System)),
        Value::Long(StataLong::Missing(MissingValue::Z)),
    ];

    let characteristic_writer = DtaWriter::new()
        .from_writer(Cursor::new(Vec::<u8>::new()))
        .write_header(header)
        .expect("v117 header writes")
        .write_schema(schema)
        .expect("v117 schema writes");
    let mut record_writer = characteristic_writer
        .into_record_writer()
        .expect("v117 record writer");
    record_writer
        .write_record(&values)
        .expect("tagged missing allowed on v117");
    let bytes = record_writer
        .into_long_string_writer()
        .expect("long-string transition")
        .into_value_label_writer()
        .expect("value-label transition")
        .finish()
        .expect("finish")
        .into_inner();

    let schema_reader = DtaReader::default()
        .from_reader(Cursor::new(bytes))
        .read_header()
        .expect("v117 header parses");
    let mut characteristic_reader = schema_reader.read_schema().expect("v117 schema parses");
    while characteristic_reader
        .read_characteristic()
        .expect("characteristic iteration")
        .is_some()
    {}
    let mut record_reader = characteristic_reader
        .into_record_reader()
        .expect("transition to record reader");
    let record = record_reader
        .read_record()
        .expect("record reads")
        .expect("one record");
    assert_eq!(record.values(), values.as_slice());
}
