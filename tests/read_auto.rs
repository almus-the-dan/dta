use std::fs::File;

use dta::stata::dta::dta_reader::DtaReader;

#[test]
#[ignore = "Using local file that requires a license"]
fn read_auto_dta_section_counts() {
    let file = File::open("/mnt/c/Publish/auto.dta").expect("failed to open auto.dta");
    let header_reader = DtaReader::default().from_file(file);

    // Header + Schema
    let schema_reader = header_reader.read_header().expect("failed to read header");
    assert_eq!(schema_reader.header().variable_count(), 12);
    assert_eq!(schema_reader.header().observation_count(), 74);

    let mut characteristic_reader = schema_reader.read_schema().expect("failed to read schema");

    // Characteristics
    let mut characteristic_count = 0;
    while characteristic_reader
        .read_characteristic()
        .expect("failed to read characteristic")
        .is_some()
    {
        characteristic_count += 1;
    }
    assert_eq!(characteristic_count, 4);

    let mut record_reader = characteristic_reader
        .into_record_reader()
        .expect("failed to transition to record reader");

    // Records
    let mut record_count = 0u64;
    while record_reader
        .read_record()
        .expect("failed to read record")
        .is_some()
    {
        record_count += 1;
    }
    assert_eq!(record_count, 74);

    // Long strings (strls come before value labels in the file)
    let mut long_string_reader = record_reader
        .into_long_string_reader()
        .expect("failed to transition to long string reader");

    let mut long_string_count = 0;
    while long_string_reader
        .read_long_string()
        .expect("failed to read long string")
        .is_some()
    {
        long_string_count += 1;
    }
    assert_eq!(long_string_count, 0);

    // Value labels
    let mut value_label_reader = long_string_reader
        .into_value_label_reader()
        .expect("failed to transition to value label reader");

    let mut value_label_table_count = 0;
    while value_label_reader
        .read_value_label_table()
        .expect("failed to read value label table")
        .is_some()
    {
        value_label_table_count += 1;
    }
    assert_eq!(value_label_table_count, 1);
}
