#![cfg(feature = "chrono")]

use dta::stata::dta::dta_reader::DtaReader;
use dta::stata::dta::long_string_table::LongStringTable;
use dta::stata::dta::value::Value;
use dta::stata::dta::value_label_table::ValueLabelTable;
use dta::stata::dta::variable::Variable;
use dta::stata::stata_byte::StataByte;
use dta::stata::stata_double::StataDouble;
use dta::stata::stata_float::StataFloat;
use dta::stata::stata_int::StataInt;
use dta::stata::stata_long::StataLong;
use dta::stata::temporal::chrono_adapter::{StataTemporal, temporal_from_value};
use encoding_rs::Encoding;
use std::fs::{self, File};
use std::path::{Path, PathBuf};

#[test]
#[ignore = "Using local files that require a license"]
fn read_auto_dta_section_counts() {
    let fixture_dir = Path::new("/mnt/c/Publish/pandas-stata-fixtures");
    let mut paths: Vec<PathBuf> = fs::read_dir(fixture_dir)
        .expect("failed to read fixture directory")
        .filter_map(Result::ok)
        .map(|entry| entry.path())
        .filter(|path| path.extension().and_then(|e| e.to_str()) == Some("dta"))
        .filter(|path| {
            // stata1_encoding.dta (V114/Windows-1252) reads fine, but
            // stata1_encoding_118.dta contains UTF-16-LE byte sequences
            // inside a file that declares itself UTF-8 — pandas bug or
            // not, the data genuinely isn't valid UTF-8 and the strict
            // V118 decoder rejects it. Separate concern from library
            // support.
            let name = path
                .file_name()
                .and_then(|n| n.to_str())
                .unwrap_or_default();
            name != "stata1_encoding_118.dta"
        })
        .collect();
    paths.sort();

    for path in &paths {
        read_dta_section_counts(path);
    }
}

#[test]
#[ignore = "Using local files that require a license"]
fn read_single_file() {
    let path = Path::new("/mnt/c/Publish/pandas-stata-fixtures/stata12_be_119.dta");
    read_dta_section_counts(path);
}

fn read_dta_section_counts(path: &Path) {
    eprintln!("File: {}", path.to_string_lossy());
    let file =
        File::open(path).unwrap_or_else(|e| panic!("failed to open {}: {e}", path.display()));
    let header_reader = DtaReader::default().from_file(file);

    // Header + Schema
    let schema_reader = header_reader.read_header().expect("failed to read header");
    let header = schema_reader.header();
    eprintln!("Variable count: {}", header.variable_count());
    eprintln!("Observation count: {}", header.observation_count());

    let mut characteristic_reader = schema_reader.read_schema().expect("failed to read schema");
    let schema = characteristic_reader.schema();
    eprintln!("Actual variable count: {}", schema.variables().len());
    eprintln!("Sort order count: {}", schema.sort_order().len());

    // Characteristics
    let mut characteristic_count = 0;
    while let Some(_characteristic) = characteristic_reader
        .read_characteristic()
        .expect("failed to read characteristic")
    {
        characteristic_count += 1;
    }
    eprintln!("Characteristic count: {characteristic_count}");

    // Long strings (strls come before value labels in the file).
    // Jump forward to the strL section, populate the resolve table,
    // then jump to records.
    let mut long_string_reader = characteristic_reader
        .seek_long_strings()
        .expect("failed to jump to long string reader");

    let mut long_string_table = LongStringTable::for_reading();
    long_string_reader
        .read_remaining_into(&mut long_string_table)
        .expect("Could not read long string table");

    let mut value_label_reader = long_string_reader
        .into_value_label_reader()
        .expect("failed to transition to value label reader");

    let mut value_label_table = ValueLabelTable::new();
    value_label_reader
        .read_remaining_into(&mut value_label_table)
        .expect("Could not read value label table");

    let mut record_reader = value_label_reader
        .seek_records()
        .expect("failed to jump to records");

    // Records
    let mut record_count = 0u64;
    let encoding = record_reader.encoding();
    let variables: Vec<_> = record_reader.schema().variables().to_vec();
    let variable_names: Vec<_> = variables.iter().map(Variable::name).collect();
    let variable_names_joined = variable_names.join(" | ");
    eprintln!("{variable_names_joined}");
    while let Some(record) = record_reader.read_record().expect("failed to read record") {
        if record_count < 10 {
            let mut value_strings = Vec::with_capacity(record.values().len());
            for (variable, value) in variables.iter().zip(record.values()) {
                let value_str = format_value(
                    &long_string_table,
                    &value_label_table,
                    encoding,
                    variable,
                    value,
                );
                value_strings.push(value_str);
            }
            let joined = value_strings.join("  |  ");
            eprintln!("{joined}");
        }
        record_count += 1;
    }
    eprintln!("Actual observation count: {record_count}");

    // Long strings (strls come before value labels in the file)
    let mut long_string_reader = record_reader
        .into_long_string_reader()
        .expect("failed to transition to long string reader");

    let mut long_string_count = 0;
    while let Some(_long_string) = long_string_reader
        .read_long_string()
        .expect("failed to read long string")
    {
        long_string_count += 1;
    }
    eprintln!("Long string count: {long_string_count}");

    // Value labels
    let mut value_label_reader = long_string_reader
        .into_value_label_reader()
        .expect("failed to transition to value label reader");

    let mut value_label_set_count = 0;
    while let Some(_value_label_set) = value_label_reader
        .read_value_label_set()
        .expect("failed to read value label set")
    {
        value_label_set_count += 1;
    }
    eprintln!("Value label count: {value_label_set_count}");
}

fn format_value(
    long_string_table: &LongStringTable,
    value_label_table: &ValueLabelTable,
    encoding: &'static Encoding,
    variable: &Variable,
    value: &Value<'_>,
) -> String {
    // Temporal columns (recognized via the variable's display
    // format) override the default rendering — including the
    // value-label enrichment on integer storage — because a `%td`
    // cell carries a date, not a labeled enum.
    if let Some(temporal) = temporal_from_value(value, variable.format()) {
        return format_temporal(temporal);
    }

    match value {
        Value::Byte(b) => match *b {
            StataByte::Present(v) => {
                enrich_with_value_label_i32(value_label_table, variable, i32::from(v))
            }
            StataByte::Missing(mv) => mv.to_string(),
        },
        Value::Int(i) => match *i {
            StataInt::Present(v) => {
                enrich_with_value_label_i32(value_label_table, variable, i32::from(v))
            }
            StataInt::Missing(mv) => mv.to_string(),
        },
        Value::Long(l) => match *l {
            StataLong::Present(v) => enrich_with_value_label_i32(value_label_table, variable, v),
            StataLong::Missing(mv) => mv.to_string(),
        },
        Value::Float(f) => match *f {
            StataFloat::Present(v) => {
                enrich_with_value_label_f64(value_label_table, variable, f64::from(v))
            }
            StataFloat::Missing(mv) => mv.to_string(),
        },
        Value::Double(d) => match *d {
            StataDouble::Present(v) => enrich_with_value_label_f64(value_label_table, variable, v),
            StataDouble::Missing(mv) => mv.to_string(),
        },
        Value::String(d) => d.to_string(),
        Value::LongStringRef(r) => long_string_table
            .get(r)
            .and_then(|s| s.data_str(encoding).map(|s| s.to_string()))
            .unwrap_or("NA".to_string()),
    }
}

fn format_temporal(temporal: StataTemporal) -> String {
    match temporal {
        StataTemporal::Date(date) => date.format("%Y-%m-%d").to_string(),
        StataTemporal::DateTime(datetime) => datetime.format("%Y-%m-%d %H:%M:%S").to_string(),
        StataTemporal::Year(year) => year.to_string(),
        StataTemporal::YearMonth { year, month } => format!("{year}-{month:02}"),
        StataTemporal::YearQuarter { year, quarter } => format!("{year}Q{quarter}"),
        StataTemporal::YearHalf { year, half } => format!("{year}H{half}"),
        StataTemporal::YearWeek { year, week } => format!("{year}W{week:02}"),
        // `StataTemporal` is `#[non_exhaustive]`; future variants
        // fall back to a placeholder rather than failing to compile.
        _ => "?".to_string(),
    }
}

fn enrich_with_value_label_i32(
    value_label_table: &ValueLabelTable,
    variable: &Variable,
    value: i32,
) -> String {
    enrich_or(value_label_table, variable, value).unwrap_or_else(|| value.to_string())
}

fn enrich_with_value_label_f64(
    value_label_table: &ValueLabelTable,
    variable: &Variable,
    value: f64,
) -> String {
    #[allow(clippy::cast_possible_truncation)]
    let i = value as i32;
    let f = f64::from(i);
    #[allow(clippy::float_cmp)]
    let b = value == f;
    b.then(|| enrich_or(value_label_table, variable, i))
        .flatten()
        .unwrap_or_else(|| format!("{value:0.4}"))
}

fn enrich_or(
    value_label_table: &ValueLabelTable,
    variable: &Variable,
    value: i32,
) -> Option<String> {
    if variable.value_label_name().is_empty() {
        return None;
    }
    value_label_table
        .label_for(variable, value)
        .map(|label| format!("{label} ({value})"))
}
