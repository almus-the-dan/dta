//! Round-trip integration test for the temporal helpers.
//!
//! Writes a small DTA file in memory containing one column per
//! recognized temporal format, reads it back through the standard
//! reader chain, and checks that
//! [`temporal_from_value`](dta::stata::temporal::chrono_adapter::temporal_from_value)
//! produces the expected [`StataTemporal`] values.
//!
//! The point is to exercise the dispatcher end-to-end against bytes
//! that actually went through the schema/format encoder, not just
//! synthetic `Value` enums in unit tests. If the writer ever drops
//! a format suffix, mangles a long-storage `%td`, or otherwise
//! deviates from what the dispatcher expects, this test catches it.
//!
//! Only compiled when the `chrono` feature is enabled; gated at the
//! file level with `#![cfg(feature = "chrono")]`.

#![cfg(feature = "chrono")]

use std::io::Cursor;

use chrono::NaiveDate;

use dta::stata::dta::byte_order::ByteOrder;
use dta::stata::dta::dta_reader::DtaReader;
use dta::stata::dta::dta_writer::DtaWriter;
use dta::stata::dta::header::Header;
use dta::stata::dta::release::Release;
use dta::stata::dta::schema::Schema;
use dta::stata::dta::value::Value;
use dta::stata::dta::variable::Variable;
use dta::stata::dta::variable_type::VariableType;
use dta::stata::stata_double::StataDouble;
use dta::stata::stata_int::StataInt;
use dta::stata::stata_long::StataLong;
use dta::stata::temporal::chrono_adapter::{StataTemporal, temporal_from_value};

/// Builds a one-record DTA in memory with `variables`, writes
/// `record`, then reads it back through the full reader chain and
/// returns the per-column `temporal_from_value` results.
fn round_trip_dispatched(
    variables: Vec<dta::stata::dta::variable::VariableBuilder>,
    record: &[Value<'_>],
) -> Vec<Option<StataTemporal>> {
    let header = Header::builder(Release::V118, ByteOrder::LittleEndian).build();
    let mut schema_builder = Schema::builder();
    for variable in variables {
        schema_builder = schema_builder.add_variable(variable);
    }
    let schema = schema_builder.build().expect("schema is valid");

    let mut record_writer = DtaWriter::new()
        .from_writer(Cursor::new(Vec::<u8>::new()))
        .write_header(header)
        .expect("header write")
        .write_schema(schema)
        .expect("schema write")
        .into_record_writer()
        .expect("record-writer transition");
    record_writer.write_record(record).expect("record write");
    let bytes = record_writer
        .into_long_string_writer()
        .expect("strL writer")
        .into_value_label_writer()
        .expect("value-label writer")
        .finish()
        .expect("finish")
        .into_inner();

    let mut characteristic_reader = DtaReader::new()
        .from_reader(Cursor::new(bytes))
        .read_header()
        .expect("header read")
        .read_schema()
        .expect("schema read");
    characteristic_reader.skip_to_end().expect("skip chars");
    let mut record_reader = characteristic_reader
        .into_record_reader()
        .expect("record-reader transition");
    let schema = record_reader.schema().clone();
    let read_back = record_reader
        .read_record()
        .expect("record read")
        .expect("at least one record");
    schema
        .variables()
        .iter()
        .zip(read_back.values().iter())
        .map(|(variable, value)| temporal_from_value(value, variable.format()))
        .collect()
}

#[test]
fn round_trip_each_temporal_format_through_writer_and_reader() {
    // Build a schema with one column per recognized temporal kind.
    // Column types match Stata's real-world choice for each format:
    // - %td: long (i32), wide enough for the chrono-representable range
    // - %tc: double (f64), required for ms-since-epoch
    // - %ty/%tm/%tq/%th/%tw: int (i16), comfortable for sane ranges
    let stata_days = i32::try_from(
        NaiveDate::from_ymd_opt(2026, 4, 24)
            .unwrap()
            .signed_duration_since(NaiveDate::from_ymd_opt(1960, 1, 1).unwrap())
            .num_days(),
    )
    .unwrap();
    // Datetime: 2026-04-24T12:34:56.789, expressed in ms since 1960-01-01.
    let seconds_into_day: i32 = 12 * 3600 + 34 * 60 + 56;
    let stata_millis =
        f64::from(stata_days) * 86_400_000.0 + f64::from(seconds_into_day) * 1000.0 + 789.0;

    let dispatched = round_trip_dispatched(
        vec![
            Variable::builder(VariableType::Long, "date_col").format("%td"),
            Variable::builder(VariableType::Double, "datetime_col").format("%tc"),
            Variable::builder(VariableType::Int, "year_col").format("%ty"),
            Variable::builder(VariableType::Int, "month_col").format("%tm"),
            Variable::builder(VariableType::Int, "quarter_col").format("%tq"),
            Variable::builder(VariableType::Int, "half_col").format("%th"),
            Variable::builder(VariableType::Int, "week_col").format("%tw"),
        ],
        &[
            Value::Long(StataLong::Present(stata_days)),
            Value::Double(StataDouble::Present(stata_millis)),
            Value::Int(StataInt::Present(2026)),
            // 2026-04 = (2026-1960)*12 + 3 = 795
            Value::Int(StataInt::Present(795)),
            // 2026-Q2 = (2026-1960)*4 + 1 = 265
            Value::Int(StataInt::Present(265)),
            // 2026-H1 = (2026-1960)*2 + 0 = 132
            Value::Int(StataInt::Present(132)),
            // 2026-W17 = (2026-1960)*52 + 16 = 3448
            Value::Int(StataInt::Present(66 * 52 + 16)),
        ],
    );

    assert_eq!(
        dispatched[0],
        Some(StataTemporal::Date(
            NaiveDate::from_ymd_opt(2026, 4, 24).unwrap()
        )),
        "%td round-trip",
    );
    assert_eq!(
        dispatched[1],
        Some(StataTemporal::DateTime(
            NaiveDate::from_ymd_opt(2026, 4, 24)
                .unwrap()
                .and_hms_milli_opt(12, 34, 56, 789)
                .unwrap()
        )),
        "%tc round-trip",
    );
    assert_eq!(
        dispatched[2],
        Some(StataTemporal::Year(2026)),
        "%ty round-trip",
    );
    assert_eq!(
        dispatched[3],
        Some(StataTemporal::YearMonth {
            year: 2026,
            month: 4
        }),
        "%tm round-trip",
    );
    assert_eq!(
        dispatched[4],
        Some(StataTemporal::YearQuarter {
            year: 2026,
            quarter: 2
        }),
        "%tq round-trip",
    );
    assert_eq!(
        dispatched[5],
        Some(StataTemporal::YearHalf {
            year: 2026,
            half: 1
        }),
        "%th round-trip",
    );
    assert_eq!(
        dispatched[6],
        Some(StataTemporal::YearWeek {
            year: 2026,
            week: 17
        }),
        "%tw round-trip",
    );
}

#[test]
fn missing_value_in_date_column_dispatches_to_none() {
    use dta::stata::missing_value::MissingValue;

    let header = Header::builder(Release::V118, ByteOrder::LittleEndian).build();
    let schema = Schema::builder()
        .add_variable(Variable::builder(VariableType::Long, "date_col").format("%td"))
        .build()
        .expect("schema is valid");

    let mut record_writer = DtaWriter::new()
        .from_writer(Cursor::new(Vec::<u8>::new()))
        .write_header(header)
        .expect("header write")
        .write_schema(schema)
        .expect("schema write")
        .into_record_writer()
        .expect("record-writer transition");

    record_writer
        .write_record(&[Value::Long(StataLong::Missing(MissingValue::System))])
        .expect("record write");
    record_writer
        .write_record(&[Value::Long(StataLong::Missing(MissingValue::A))])
        .expect("record write");

    let bytes = record_writer
        .into_long_string_writer()
        .expect("strL writer")
        .into_value_label_writer()
        .expect("value-label writer")
        .finish()
        .expect("finish")
        .into_inner();

    let mut characteristic_reader = DtaReader::new()
        .from_reader(Cursor::new(bytes))
        .read_header()
        .expect("header read")
        .read_schema()
        .expect("schema read");
    characteristic_reader.skip_to_end().expect("skip chars");
    let mut record_reader = characteristic_reader
        .into_record_reader()
        .expect("record-reader transition");
    let schema = record_reader.schema().clone();
    let format = schema.variables()[0].format();

    while let Some(record) = record_reader.read_record().expect("record read") {
        let value = &record.values()[0];
        assert_eq!(
            temporal_from_value(value, format),
            None,
            "missing values must dispatch to None regardless of sentinel kind",
        );
    }
}
