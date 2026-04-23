use std::fs::File;
use std::io::Cursor;
use std::path::Path;

use criterion::{Criterion, criterion_group, criterion_main};
use dta::stata::dta::byte_order::ByteOrder;
use dta::stata::dta::dta_reader::DtaReader;
use dta::stata::dta::dta_writer::DtaWriter;
use dta::stata::dta::header::Header;
use dta::stata::dta::release::Release;
use dta::stata::dta::schema::Schema;
use dta::stata::dta::value::Value;
use dta::stata::dta::variable::Variable;
use dta::stata::dta::variable_type::VariableType;
use dta::stata::stata_byte::StataByte;
use dta::stata::stata_double::StataDouble;
use dta::stata::stata_float::StataFloat;
use dta::stata::stata_int::StataInt;
use dta::stata::stata_long::StataLong;

// ---------------------------------------------------------------------------
// Synthetic DTA file generator
// ---------------------------------------------------------------------------

const BENCH_FILE: &str = "benches/data/bench_large.dta";
const RECORD_COUNT: usize = 100_000;
const RELEASE: Release = Release::V118;
const BYTE_ORDER: ByteOrder = ByteOrder::LittleEndian;

/// Panel/survey-style schema exercising the main variable types.
///
/// | Field   | Type          | Width |
/// |---------|---------------|-------|
/// | id      | Long          |     4 |
/// | age     | Byte          |     1 |
/// | region  | Int           |     2 |
/// | income  | Double        |     8 |
/// | score   | Float         |     4 |
/// | name    | FixedString32 |    32 |
/// | status  | FixedString8  |     8 |
/// | notes   | FixedString64 |    64 |
fn build_schema() -> Schema {
    Schema::builder()
        .add_variable(Variable::builder(VariableType::Long, "id").format("%12.0g"))
        .add_variable(Variable::builder(VariableType::Byte, "age").format("%8.0g"))
        .add_variable(Variable::builder(VariableType::Int, "region").format("%8.0g"))
        .add_variable(Variable::builder(VariableType::Double, "income").format("%10.2f"))
        .add_variable(Variable::builder(VariableType::Float, "score").format("%9.2f"))
        .add_variable(Variable::builder(VariableType::FixedString(32), "name").format("%32s"))
        .add_variable(Variable::builder(VariableType::FixedString(8), "status").format("%8s"))
        .add_variable(Variable::builder(VariableType::FixedString(64), "notes").format("%64s"))
        .build()
        .unwrap()
}

fn build_header() -> Header {
    Header::builder(RELEASE, BYTE_ORDER)
        .dataset_label("bench")
        .build()
}

fn build_template_values() -> Vec<Value<'static>> {
    vec![
        Value::Long(StataLong::Present(1_234_567)),
        Value::Byte(StataByte::Present(42)),
        Value::Int(StataInt::Present(17)),
        Value::Double(StataDouble::Present(98_765.43)),
        Value::Float(StataFloat::Present(4.25)),
        Value::String("John Q. Public"),
        Value::String("POSTED"),
        Value::String("Monthly payment; see TXN-20240115-01 for reference."),
    ]
}

/// Generate a synthetic DTA file at the given path.
fn generate_dta(path: &Path) {
    let file = File::create(path).expect("failed to create benchmark file");
    let values = build_template_values();

    let characteristic_writer = DtaWriter::new()
        .from_file(file)
        .write_header(build_header())
        .unwrap()
        .write_schema(build_schema())
        .unwrap();
    let mut record_writer = characteristic_writer.into_record_writer().unwrap();
    for _ in 0..RECORD_COUNT {
        record_writer.write_record(&values).unwrap();
    }
    record_writer
        .into_long_string_writer()
        .unwrap()
        .into_value_label_writer()
        .unwrap()
        .finish()
        .unwrap();
}

/// Ensure the synthetic file exists, regenerating if needed.
fn ensure_bench_file() -> std::path::PathBuf {
    let path = Path::new(BENCH_FILE);
    if !path.exists() {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).expect("failed to create benches/data directory");
        }
        eprintln!("Generating synthetic benchmark file ({RECORD_COUNT} records)...");
        generate_dta(path);
        eprintln!(
            "Generated: {} ({} bytes)",
            path.display(),
            std::fs::metadata(path).unwrap().len()
        );
    }
    path.to_path_buf()
}

// ---------------------------------------------------------------------------
// Benchmarks
// ---------------------------------------------------------------------------

/// Write throughput to an in-memory buffer (no filesystem I/O).
fn write_to_vec(c: &mut Criterion) {
    let values = build_template_values();

    c.bench_function("write_to_vec", |b| {
        b.iter(|| {
            let buf: Vec<u8> = Vec::with_capacity(RECORD_COUNT * 256);
            let cursor = Cursor::new(buf);
            let characteristic_writer = DtaWriter::new()
                .from_writer(cursor)
                .write_header(build_header())
                .unwrap()
                .write_schema(build_schema())
                .unwrap();
            let mut record_writer = characteristic_writer.into_record_writer().unwrap();
            for _ in 0..RECORD_COUNT {
                record_writer.write_record(&values).unwrap();
            }
            let finished = record_writer
                .into_long_string_writer()
                .unwrap()
                .into_value_label_writer()
                .unwrap()
                .finish()
                .unwrap();
            finished.into_inner().len()
        });
    });
}

/// Read throughput using `read_record()` from an in-memory buffer.
fn read_from_cursor(c: &mut Criterion) {
    let path = ensure_bench_file();
    let file_bytes = std::fs::read(&path).unwrap();

    c.bench_function("read_from_cursor", |b| {
        b.iter(|| {
            let cursor = Cursor::new(file_bytes.as_slice());
            let mut characteristic_reader = DtaReader::new()
                .from_reader(cursor)
                .read_header()
                .unwrap()
                .read_schema()
                .unwrap();
            characteristic_reader.skip_to_end().unwrap();
            let mut record_reader = characteristic_reader.into_record_reader().unwrap();
            let mut count = 0u64;
            while record_reader.read_record().unwrap().is_some() {
                count += 1;
            }
            assert_eq!(count, RECORD_COUNT as u64);
        });
    });
}

/// Read throughput using `read_lazy_record()` from an in-memory buffer.
///
/// Lazy records skip per-value parsing; the difference against
/// `read_from_cursor` isolates the cost of value decoding.
fn read_lazy_from_cursor(c: &mut Criterion) {
    let path = ensure_bench_file();
    let file_bytes = std::fs::read(&path).unwrap();

    c.bench_function("read_lazy_from_cursor", |b| {
        b.iter(|| {
            let cursor = Cursor::new(file_bytes.as_slice());
            let mut characteristic_reader = DtaReader::new()
                .from_reader(cursor)
                .read_header()
                .unwrap()
                .read_schema()
                .unwrap();
            characteristic_reader.skip_to_end().unwrap();
            let mut record_reader = characteristic_reader.into_record_reader().unwrap();
            let mut count = 0u64;
            while record_reader.read_lazy_record().unwrap().is_some() {
                count += 1;
            }
            assert_eq!(count, RECORD_COUNT as u64);
        });
    });
}

/// Read throughput using `read_record()` from a file.
fn read_from_file(c: &mut Criterion) {
    let path = ensure_bench_file();

    c.bench_function("read_from_file", |b| {
        b.iter(|| {
            let file = File::open(&path).unwrap();
            let mut characteristic_reader = DtaReader::new()
                .from_file(file)
                .read_header()
                .unwrap()
                .read_schema()
                .unwrap();
            characteristic_reader.skip_to_end().unwrap();
            let mut record_reader = characteristic_reader.into_record_reader().unwrap();
            let mut count = 0u64;
            while record_reader.read_record().unwrap().is_some() {
                count += 1;
            }
            assert_eq!(count, RECORD_COUNT as u64);
        });
    });
}

// ---------------------------------------------------------------------------
// Async benchmarks (tokio feature)
// ---------------------------------------------------------------------------

#[cfg(feature = "tokio")]
fn tokio_rt() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
}

/// Async write throughput to a temp file.
///
/// Async benches use the filesystem because the writer chain requires
/// `AsyncWrite + AsyncSeek` and tokio has no built-in in-memory
/// cursor with both traits.
#[cfg(feature = "tokio")]
fn async_write_to_file(c: &mut Criterion) {
    let rt = tokio_rt();
    let values = build_template_values();
    let tempdir = tempfile::tempdir().unwrap();
    let path = tempdir.path().join("async_bench_write.dta");

    c.bench_function("async_write_to_file", |b| {
        b.iter(|| {
            rt.block_on(async {
                let file = tokio::fs::File::create(&path).await.unwrap();
                let characteristic_writer = DtaWriter::new()
                    .from_tokio_file(file)
                    .write_header(build_header())
                    .await
                    .unwrap()
                    .write_schema(build_schema())
                    .await
                    .unwrap();
                let mut record_writer = characteristic_writer.into_record_writer().await.unwrap();
                for _ in 0..RECORD_COUNT {
                    record_writer.write_record(&values).await.unwrap();
                }
                record_writer
                    .into_long_string_writer()
                    .await
                    .unwrap()
                    .into_value_label_writer()
                    .await
                    .unwrap()
                    .finish()
                    .await
                    .unwrap();
            });
        });
    });
}

/// Async read throughput using `read_record()` from a file.
#[cfg(feature = "tokio")]
fn async_read_from_file(c: &mut Criterion) {
    let path = ensure_bench_file();
    let rt = tokio_rt();

    c.bench_function("async_read_from_file", |b| {
        b.iter(|| {
            rt.block_on(async {
                let file = tokio::fs::File::open(&path).await.unwrap();
                let mut characteristic_reader = DtaReader::new()
                    .from_tokio_file(file)
                    .read_header()
                    .await
                    .unwrap()
                    .read_schema()
                    .await
                    .unwrap();
                characteristic_reader.skip_to_end().await.unwrap();
                let mut record_reader = characteristic_reader.into_record_reader().await.unwrap();
                let mut count = 0u64;
                while record_reader.read_record().await.unwrap().is_some() {
                    count += 1;
                }
                assert_eq!(count, RECORD_COUNT as u64);
            });
        });
    });
}

// ---------------------------------------------------------------------------
// Criterion groups
// ---------------------------------------------------------------------------

#[cfg(not(feature = "tokio"))]
criterion_group! {
    name = benches;
    config = Criterion::default().measurement_time(std::time::Duration::from_secs(10));
    targets =
        write_to_vec,
        read_from_cursor,
        read_lazy_from_cursor,
        read_from_file
}

#[cfg(feature = "tokio")]
criterion_group! {
    name = benches;
    config = Criterion::default().measurement_time(std::time::Duration::from_secs(10));
    targets =
        write_to_vec,
        read_from_cursor,
        read_lazy_from_cursor,
        read_from_file,
        async_write_to_file,
        async_read_from_file
}

criterion_main!(benches);
