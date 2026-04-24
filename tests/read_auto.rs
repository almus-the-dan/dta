use std::fs::{self, File};
use std::path::{Path, PathBuf};

use dta::stata::dta::dta_reader::DtaReader;

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
            // Releases 102 and 103 are not supported.
            let name = path
                .file_name()
                .and_then(|n| n.to_str())
                .unwrap_or_default();
            !name.contains("102") && !name.contains("103")
        })
        .collect();
    paths.sort();

    for path in &paths {
        read_dta_section_counts(path);
    }
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

    let mut record_reader = characteristic_reader
        .into_record_reader()
        .expect("failed to transition to record reader");

    // Records
    let mut record_count = 0u64;
    while let Some(_record) = record_reader.read_record().expect("failed to read record") {
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
