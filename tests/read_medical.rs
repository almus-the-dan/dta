use std::path::Path;

use dta::stata::dct::dct_reader::DctReader;
use dta::stata::dct::dct_source::DctSource;
use dta::stata::dct::parser::open_dct;
use dta::stata::dct::value::Value;

/// Reads the canonical Stata-docs `medical.data` example, where the
/// dictionary lives in one file and the data lives in another.
#[test]
#[ignore = "Reads local fixture files"]
fn read_medical_external() {
    let directory = Path::new("/mnt/c/Publish/medical-external");
    let dictionary_path = directory.join("medical.dct");
    let data_path = directory.join("medical.data");

    let source = open_dct(&dictionary_path).expect("dictionary should parse");
    let DctSource::External(schema) = source else {
        panic!("expected the dictionary to declare an external data file");
    };

    assert_eq!(schema.lines_per_observation(), 3);
    assert_eq!(schema.columns().len(), 4);
    assert_eq!(schema.declared_data_path(), Some("medical.data"));

    let mut reader = DctReader::options(schema)
        .from_path(&data_path)
        .expect("data file should open");

    let observations = collect_observations(&mut reader);
    assert_eq!(observations, expected_observations());
}

/// Reads a dictionary whose data is inlined after the closing `}`.
#[test]
#[ignore = "Reads local fixture files"]
fn read_medical_embedded() {
    let dictionary_path = Path::new("/mnt/c/Publish/medical-embedded/medical.dct");

    let source = open_dct(dictionary_path).expect("dictionary should parse");
    let DctSource::Embedded(mut reader) = source else {
        panic!("expected the dictionary to embed its data after the closing brace");
    };

    assert_eq!(reader.schema().lines_per_observation(), 3);
    assert_eq!(reader.schema().columns().len(), 4);
    assert_eq!(reader.schema().declared_data_path(), None);

    let observations = collect_observations(&mut reader);
    assert_eq!(observations, expected_observations());
}

fn expected_observations() -> Vec<(String, String, String, String)> {
    [
        ("John Smith", "A", "A", "555-123-4567"),
        ("Jane Doe", "O", "I", "555-987-6543"),
        ("Robert Wilson", "B", "A", "555-555-5555"),
    ]
    .into_iter()
    .map(|(n, b, s, p)| (n.to_string(), b.to_string(), s.to_string(), p.to_string()))
    .collect()
}

/// Drains a [`DctReader`] into a `Vec` of `(name, blood, status, phone)`
/// tuples, copying each field into an owned `String` so the result can
/// outlive the reader's internal buffer.
fn collect_observations<R: std::io::BufRead>(
    reader: &mut DctReader<R>,
) -> Vec<(String, String, String, String)> {
    let mut observations = Vec::new();
    while let Some(record) = reader.read_record().expect("record should read") {
        let values = record.values();
        observations.push((
            string_value(&values[0]),
            string_value(&values[1]),
            string_value(&values[2]),
            string_value(&values[3]),
        ));
    }
    observations
}

fn string_value(value: &Value<'_>) -> String {
    match value {
        Value::String(s) => s.as_ref().to_string(),
        other => panic!("expected a string value, got {other:?}"),
    }
}
