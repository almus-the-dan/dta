use std::fs::File;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use clap::{Parser, ValueEnum};
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
use tempfile::NamedTempFile;

const RELEASE: Release = Release::V118;
const BYTE_ORDER: ByteOrder = ByteOrder::LittleEndian;

#[derive(Parser)]
#[command(about = "Profile DTA read/write throughput")]
struct Cli {
    /// Which phase to run
    #[arg(long, short, default_value = "both")]
    phase: Phase,

    /// Number of records to generate
    #[arg(long, short, default_value_t = 1_000_000)]
    records: usize,

    /// Path to use for the data file. If omitted, a temporary file is used.
    #[arg(long, short)]
    file: Option<PathBuf>,
}

#[derive(Clone, PartialEq, ValueEnum)]
enum Phase {
    Write,
    Read,
    Both,
    #[cfg(feature = "tokio")]
    AsyncWrite,
    #[cfg(feature = "tokio")]
    AsyncRead,
    #[cfg(feature = "tokio")]
    AsyncBoth,
    #[cfg(feature = "tokio")]
    All,
}

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
        .dataset_label("profile")
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

// ---------------------------------------------------------------------------
// Synchronous phases
// ---------------------------------------------------------------------------

fn run_write(path: &Path, record_count: usize) -> (Duration, u64) {
    let values = build_template_values();
    let file = File::create(path).unwrap();

    let start = Instant::now();
    let characteristic_writer = DtaWriter::new()
        .from_file(file)
        .write_header(build_header())
        .unwrap()
        .write_schema(build_schema())
        .unwrap();
    let mut record_writer = characteristic_writer.into_record_writer().unwrap();
    for _ in 0..record_count {
        record_writer.write_record(&values).unwrap();
    }
    record_writer
        .into_long_string_writer()
        .unwrap()
        .into_value_label_writer()
        .unwrap()
        .finish()
        .unwrap();
    let elapsed = start.elapsed();

    let file_size = std::fs::metadata(path).unwrap().len();
    (elapsed, file_size)
}

fn run_read(path: &Path) -> (Duration, u64, u64) {
    let file_size = std::fs::metadata(path).unwrap().len();
    let file = File::open(path).unwrap();

    let start = Instant::now();
    let mut characteristic_reader = DtaReader::new()
        .from_file(file)
        .read_header()
        .unwrap()
        .read_schema()
        .unwrap();
    characteristic_reader.skip_to_end().unwrap();
    let mut record_reader = characteristic_reader.into_record_reader().unwrap();
    let mut record_count: u64 = 0;
    while record_reader.read_record().unwrap().is_some() {
        record_count += 1;
    }
    let elapsed = start.elapsed();

    (elapsed, record_count, file_size)
}

// ---------------------------------------------------------------------------
// Async phases
// ---------------------------------------------------------------------------

#[cfg(feature = "tokio")]
async fn run_async_write(path: &Path, record_count: usize) -> (Duration, u64) {
    let values = build_template_values();
    let file = tokio::fs::File::create(path).await.unwrap();

    let start = Instant::now();
    let characteristic_writer = DtaWriter::new()
        .from_tokio_file(file)
        .write_header(build_header())
        .await
        .unwrap()
        .write_schema(build_schema())
        .await
        .unwrap();
    let mut record_writer = characteristic_writer.into_record_writer().await.unwrap();
    for _ in 0..record_count {
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
    let elapsed = start.elapsed();

    let file_size = std::fs::metadata(path).unwrap().len();
    (elapsed, file_size)
}

#[cfg(feature = "tokio")]
async fn run_async_read(path: &Path) -> (Duration, u64, u64) {
    let file_size = std::fs::metadata(path).unwrap().len();
    let file = tokio::fs::File::open(path).await.unwrap();

    let start = Instant::now();
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
    let mut record_count: u64 = 0;
    while record_reader.read_record().await.unwrap().is_some() {
        record_count += 1;
    }
    let elapsed = start.elapsed();

    (elapsed, record_count, file_size)
}

// ---------------------------------------------------------------------------
// Reporting
// ---------------------------------------------------------------------------

#[allow(clippy::cast_precision_loss)]
fn print_row(phase: &str, records: u64, size_bytes: u64, elapsed: Duration) {
    let size_mb = size_bytes as f64 / (1024.0 * 1024.0);
    let secs = elapsed.as_secs_f64();
    let records_per_sec = records as f64 / secs;
    let mb_per_sec = size_mb / secs;
    println!(
        "{phase:<12} {records:>12} {size_mb:>10.1} {secs:>10.3} {records_per_sec:>12.0} {mb_per_sec:>10.1}",
    );
}

/// Returns true for the `All` variant (only exists with tokio).
fn cfg_all(phase: &Phase) -> bool {
    #[cfg(feature = "tokio")]
    if *phase == Phase::All {
        return true;
    }
    let _ = phase;
    false
}

/// Returns true if any read phase is selected that needs a pre-existing file.
fn needs_seed_file(phase: &Phase) -> bool {
    let sync_read = matches!(phase, Phase::Read | Phase::Both);
    #[cfg(feature = "tokio")]
    let async_read = matches!(phase, Phase::AsyncRead | Phase::AsyncBoth | Phase::All);
    #[cfg(not(feature = "tokio"))]
    let async_read = false;
    sync_read || async_read
}

fn main() {
    let cli = Cli::parse();

    println!("Records: {}", cli.records);
    println!();
    println!(
        "{:<12} {:>12} {:>10} {:>10} {:>12} {:>10}",
        "Phase", "Records", "Size (MB)", "Time (s)", "Records/s", "MB/s"
    );
    println!("{}", "-".repeat(70));

    let (_tmp, file_path) = if let Some(path) = cli.file.clone() {
        (None, path)
    } else {
        let tmp = NamedTempFile::new().unwrap();
        let path = tmp.path().to_path_buf();
        (Some(tmp), path)
    };

    let run_sync_write = matches!(cli.phase, Phase::Write | Phase::Both) || cfg_all(&cli.phase);
    let run_sync_read = matches!(cli.phase, Phase::Read | Phase::Both) || cfg_all(&cli.phase);

    if needs_seed_file(&cli.phase) && !run_sync_write && !file_path.exists() {
        run_write(&file_path, cli.records);
    }

    if run_sync_write {
        let (elapsed, file_size) = run_write(&file_path, cli.records);
        print_row("Write", cli.records as u64, file_size, elapsed);
    }

    if run_sync_read {
        let (elapsed, records_read, file_size) = run_read(&file_path);
        print_row("Read", records_read, file_size, elapsed);
    }

    #[cfg(feature = "tokio")]
    run_async_phases(&cli, &file_path, run_sync_write);
}

#[cfg(feature = "tokio")]
fn run_async_phases(cli: &Cli, file_path: &Path, sync_wrote: bool) {
    let should_write = matches!(cli.phase, Phase::AsyncWrite | Phase::AsyncBoth | Phase::All);
    let should_read = matches!(cli.phase, Phase::AsyncRead | Phase::AsyncBoth | Phase::All);

    if !should_write && !should_read {
        return;
    }

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();

    rt.block_on(async {
        if should_write {
            let (elapsed, file_size) = run_async_write(file_path, cli.records).await;
            print_row("AsyncWrite", cli.records as u64, file_size, elapsed);
        }

        if should_read {
            if !sync_wrote && !should_write && !file_path.exists() {
                run_write(file_path, cli.records);
            }
            let (elapsed, records_read, file_size) = run_async_read(file_path).await;
            print_row("AsyncRead", records_read, file_size, elapsed);
        }
    });
}
