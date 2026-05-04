use std::path::Path;

use dta::stata::dct::dct_reader::DctReader;
use dta::stata::dct::dct_source::DctSource;

#[test]
#[ignore = "Using local files that require a license"]
fn read_2002_fem_preg_schema_and_records() {
    let directory = Path::new("/mnt/c/Publish/dct/2002FemPreg");
    let dictionary_path = directory.join("2002FemPreg.dct");
    let data_path = directory.join("2002FemPreg.dat");

    let source = DctSource::options()
        .from_path(&dictionary_path)
        .expect("dictionary should parse");
    let DctSource::External(schema) = source else {
        panic!("expected an external data file, but the dictionary embeds its data");
    };

    assert!(
        !schema.columns().is_empty(),
        "dictionary should declare at least one variable",
    );

    let mut reader = DctReader::options(schema)
        .from_path(&data_path)
        .expect("data file should open");

    let mut record_count: usize = 0;
    while let Some(record) = reader.read_record().expect("record should read") {
        assert_eq!(
            record.values().len(),
            reader.schema().columns().len(),
            "each record should have one value per declared column",
        );
        record_count += 1;
    }

    assert!(
        record_count > 0,
        "data file should contain at least one record"
    );
    println!(
        "read {record_count} records covering {} columns",
        reader.schema().columns().len(),
    );
}
