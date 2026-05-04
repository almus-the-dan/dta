# dta

A pure Rust library for reading and writing Stata data formats.

This crate covers two related-but-distinct file formats:

- **DTA** — the binary format Stata uses to persist datasets. Every released version is supported (102 through 119), including XML-framed releases (117+), tagged missing values, value-label sets, and long-string (`strL`) storage.
- **DCT** — Stata's plain-text dictionary format, used to describe fixed-width or free-format `.dat`/`.raw` data files. Common when importing into Stata from external systems.

Both formats sit under [Stata](https://www.stata.com/), still the dominant interchange format in academic economics, public health, and social science research — often the only format that downstream collaborators can actually open.

The DTA API is built around a typestate chain, so you can't accidentally write a schema before a header, or read data bytes before the schema has been parsed. Each phase borrows the underlying I/O handle and hands it to the next phase, keeping the pipeline zero-copy where possible.

The DCT API is a two-step builder — parse the dictionary into a `Schema`, then pair the schema with a data source to read records.

## Usage — DTA

### Reading a file

The reader walks through the sections of a DTA file in order. Each phase returns the next phase when you're done with it.

```rust
use dta::stata::dta::dta_reader::DtaReader;
use dta::stata::dta::dta_error::Result;

fn read_stata(path: &str) -> Result<()> {
    // HeaderReader: parse the file header (release, byte order, dataset
    // label, timestamp, K, N).
    let header_reader = DtaReader::new().from_path(path)?;

    // SchemaReader: parse variable types, names, formats, labels, and
    // value-label associations.
    let schema_reader = header_reader.read_header()?;
    let header = schema_reader.header().clone();
    println!("Release {}, {} variables, {} observations",
        header.release(), header.variable_count(), header.observation_count());

    // CharacteristicReader: iterate (or skip) the characteristics /
    // expansion-fields section.
    let mut characteristic_reader = schema_reader.read_schema()?;
    characteristic_reader.skip_to_end()?;

    // RecordReader: iterate observation rows.
    let mut record_reader = characteristic_reader.into_record_reader()?;
    let schema = record_reader.schema().clone();
    while let Some(record) = record_reader.read_record()? {
        for (variable, value) in schema.variables().iter().zip(record.values()) {
            println!("{}: {:?}", variable.name(), value);
        }
    }

    // LongStringReader: read `strL` entries referenced from the data
    // section (117+ only — transitions silently on earlier releases).
    let mut long_string_reader = record_reader.into_long_string_reader()?;
    long_string_reader.skip_to_end()?;

    // ValueLabelReader: iterate value-label sets.
    let mut value_label_reader = long_string_reader.into_value_label_reader()?;
    while let Some(set) = value_label_reader.read_value_label_set()? {
        println!("label set: {}", set.name());
    }

    Ok(())
}
```

Each reader exposes `header()` and `schema()` accessors, so you can inspect metadata without threading them through yourself.

### Lazy records

`RecordReader::read_record()` eagerly decodes every cell in the row. If you only need a subset of columns — or you want to defer parsing until you know whether you care — use `read_lazy_record()` instead. `LazyRecord` hands back raw byte slices that you decode one cell at a time.

```rust
while let Some(lazy) = record_reader.read_lazy_record()? {
    // Decode only the columns you need.
    let id = lazy.value(0)?;
    let name = lazy.value(5)?;
}
```

### Writing a file

The writer is the mirror image of the reader: a typestate chain that locks in the ordering of the header, schema, characteristics, data, long strings, and value labels. The `<map>` offsets and the header's `K`/`N` placeholders are patched in place as each section closes, which is why the writer requires `Write + Seek`.

```rust
use dta::stata::dta::byte_order::ByteOrder;
use dta::stata::dta::dta_writer::DtaWriter;
use dta::stata::dta::header::Header;
use dta::stata::dta::release::Release;
use dta::stata::dta::schema::Schema;
use dta::stata::dta::value::Value;
use dta::stata::dta::variable::Variable;
use dta::stata::dta::variable_type::VariableType;
use dta::stata::stata_double::StataDouble;
use dta::stata::stata_long::StataLong;
use dta::stata::dta::dta_error::Result;

fn write_stata(path: &str) -> Result<()> {
    let header = Header::builder(Release::V118, ByteOrder::LittleEndian)
        .dataset_label("example dataset")
        .build();

    let schema = Schema::builder()
        .add_variable(Variable::builder(VariableType::Long, "id").format("%12.0g"))
        .add_variable(Variable::builder(VariableType::Double, "price").format("%10.2f"))
        .add_variable(Variable::builder(VariableType::FixedString(32), "name").format("%32s"))
        .build()?;

    let characteristic_writer = DtaWriter::new()
        .from_path(path)?
        .write_header(header)?
        .write_schema(schema)?;

    // Skip characteristics (they're optional) and begin writing rows.
    let mut record_writer = characteristic_writer.into_record_writer()?;
    record_writer.write_record(&[
        Value::Long(StataLong::Present(1)),
        Value::Double(StataDouble::Present(19.99)),
        Value::String("widget"),
    ])?;
    record_writer.write_record(&[
        Value::Long(StataLong::Present(2)),
        Value::Double(StataDouble::Missing),
        Value::String("gadget"),
    ])?;

    // Transition through the remaining sections (strLs and value labels
    // are optional) and close the file.
    record_writer
        .into_long_string_writer()?
        .into_value_label_writer()?
        .finish()?;

    Ok(())
}
```

`RecordWriter::write_record` validates arity and per-column types against the schema, so a mismatch is caught at the call site rather than producing a malformed file.

## Usage — DCT

A `.dct` dictionary describes the schema of a plain-text data file: where each variable starts, how wide it is, what type it is, what its name is, and (optionally) a label. The associated data may live in a separate `.dat`/`.raw` file (the dictionary's `using` clause) or inline after the closing `}`.

```rust
use dta::stata::dct::dct_reader::DctReader;
use dta::stata::dct::dct_source::DctSource;
use dta::stata::dct::dct_error::Result;

fn read_dct(dict_path: &str, data_path: &str) -> Result<()> {
    // Step 1: parse the dictionary. The result tells you whether the
    // data file is embedded after the closing `}` or external.
    let source = DctSource::options().from_path(dict_path)?;

    let mut reader = match source {
        DctSource::External(schema) => {
            // Step 2a: open the data file separately.
            DctReader::options(schema).from_path(data_path)?
        }
        DctSource::Embedded(reader) => {
            // Step 2b: data was inlined; the reader is positioned at it.
            reader
        }
    };

    // Capture column names up front: the lending pattern means
    // `record` borrows `reader` exclusively, so `reader.schema()`
    // can't be called inside the loop body.
    let column_names: Vec<String> = reader
        .schema()
        .columns()
        .iter()
        .map(|c| c.name().to_string())
        .collect();

    // Step 3: iterate records.
    while let Some(record) = reader.read_record()? {
        for (name, value) in column_names.iter().zip(record.values()) {
            println!("{}: {:?}", name, value);
        }
    }

    // Per-record warnings (blank short-line fields treated as
    // missing, integers auto-promoted to a wider type) accumulate
    // on the reader and are cleared at the start of each call.
    for warning in reader.warnings() {
        eprintln!("warning: {warning}");
    }

    Ok(())
}
```

### Lazy DCT records

If you only need a subset of columns per row, `read_lazy_record()` defers value decoding until you ask for it. Useful for skipping the parse work on the columns you don't care about.

```rust
while let Some(lazy) = reader.read_lazy_record()? {
    let id = lazy.value(0)?;
    let name = lazy.value(5)?;
    // ...other columns are never decoded
}
```

### What's supported

The DCT parser supports the directives you'll see in real-world dictionary files:

- `_column(#)` — absolute byte position (1-based; stored 0-based internally)
- `_skip(#)` and bare `_skip` — per-variable column-pointer modifier (relative to the running cursor; combines with `_column(#)` when both are present)
- `_newline` — multi-line observations; `_column(#)` references restart at 1 after each
- `lrecl(#)` and `firstlineoffile(#)` directives
- All Stata storage types (`byte`, `int`, `long`, `float`, `double`, `str`, `str#`)
- All read formats (`%w.df`, `%w.dg`, `%w.de`, `%wf`, `%f`, `%ws`, `%s`)
- Both single-line and multi-line records
- Both fixed-width and free-format reads
- The `using FILE` clause and the optional `infile` prefix
- Comments (`*` lines)

Per-record diagnostics surface as `DctWarning` values on the reader: blank short-line fields treated as system missing, integer values auto-promoted to a wider storage type when they fall in the missing-marker range, declared `using` paths the library doesn't act on, and unrecognized directives.

A note on real-world coverage: the rarer directives (`lrecl(#)`, `firstlineoffile(#)`, `_newline`, free-format reads) are implemented against Stata's documentation but I haven't found a `.dct` file in the wild that uses them — every public dictionary I sampled had been cleaned up to the common subset. If you hit a bug with one of these, please file an issue with a small representative `.dct` file and the matching data file so I can reproduce.

If you don't intend to inspect `warnings()` — for example, in a tight read loop where you only care about values — disable warning collection on the options builder so the reader can skip building the diagnostic structs entirely:

```rust
let mut reader = DctReader::options(schema)
    .record_warnings(false)
    .from_path(data_path)?;
```

## Feature Flags

### `chrono` — date/time helpers

Disabled by default. Stata stores dates and timestamps as plain numeric values whose meaning is encoded in the variable's display format (`%td`, `%tc`, `%tm`, …). Enabling `chrono` adds typed conversions to [chrono](https://docs.rs/chrono) types so you don't have to track the 1960 epoch, the milliseconds-vs-days distinction, or the legacy `%d` alias yourself.

```toml
[dependencies]
dta = { version = "0.5", features = ["chrono"] }
chrono = "0.4"
```

The module is layered. The lower layers ship without any time-crate dependency and are always available — only the typed adapters live behind the feature flag:

- `dta::stata::temporal::conversion` — pure numeric helpers (`td_days_to_unix_days`, `tc_millis_to_unix_millis`, plus `(year, sub-period)` decomposers for `%tw`/`%tm`/`%tq`/`%th`). Useful as-is for consumers using `jiff`, `time`, or raw Unix timestamps.
- `dta::stata::temporal::TemporalKind::from_format` — classify a Stata format string into `Date`, `DateTime`, `Year`, `Week`, etc. Recognizes the eight `%t*` prefixes plus the legacy `%d` alias, and ignores display suffixes (`%tdCCYY-NN-DD` classifies the same as bare `%td`).
- `dta::stata::temporal::chrono_adapter` (feature-gated) — `NaiveDate` / `NaiveDateTime` adapters, `Value`-aware helpers that handle storage-type widening and Stata missing-value sentinels, and a `temporal_from_value(&Value, &str) -> Option<StataTemporal>` dispatcher that picks the right path from a column's format string.

```rust
use dta::stata::dta::dta_reader::DtaReader;
use dta::stata::dta::dta_error::Result;
use dta::stata::temporal::chrono_adapter::{StataTemporal, temporal_from_value};

fn dump_temporal_columns(path: &str) -> Result<()> {
    let mut reader = DtaReader::new()
        .from_path(path)?
        .read_header()?
        .read_schema()?;
    reader.skip_to_end()?;
    let mut record_reader = reader.into_record_reader()?;
    let schema = record_reader.schema().clone();

    while let Some(record) = record_reader.read_record()? {
        for (variable, value) in schema.variables().iter().zip(record.values()) {
            match temporal_from_value(value, variable.format()) {
                Some(StataTemporal::Date(d)) => println!("{}: {}", variable.name(), d),
                Some(StataTemporal::DateTime(dt)) => println!("{}: {}", variable.name(), dt),
                Some(StataTemporal::Year(y)) => println!("{}: {}", variable.name(), y),
                Some(StataTemporal::YearMonth { year, month }) => {
                    println!("{}: {}-{:02}", variable.name(), year, month);
                }
                Some(_) | None => {} // quarter / half / week / non-temporal
            }
        }
    }
    Ok(())
}
```

`temporal_from_value` returns `None` for non-temporal columns, Stata missing-value sentinels (`.` and `.a`–`.z`), storage/format mismatches (e.g., a `%tc` cell stored as `Long`), and the leap-second-adjusted `%tC` format (chrono can't model leap seconds; drop to the Layer 1 `tc_millis_to_unix_millis` helper if you need to make a policy decision yourself).

The feature also enables `StataTimestamp::to_naive_date_time`, which converts the file-header timestamp directly to a `NaiveDateTime`.

### `tokio` — async I/O

Disabled by default. Enable it for async reading and writing with [tokio](https://docs.rs/tokio).

```toml
[dependencies]
dta = { version = "0.5", features = ["tokio"] }
tokio = { version = "1", features = ["fs", "io-util", "rt", "macros"] }
```

This unlocks async terminals on every options builder:

- **DTA**: `DtaReader::from_tokio_*` returns an `AsyncHeaderReader` that mirrors the sync chain with `.await` at each step; `DtaWriter::from_tokio_*` returns an `AsyncHeaderWriter`. The async writer requires `AsyncWrite + AsyncSeek + Unpin` so that the XML `<map>` and header placeholders can still be patched in place.
- **DCT**: `DctSource::options().from_tokio_*` returns a `DctSource<R>`; `DctReader::options(schema).from_tokio_*` returns an `AsyncDctReader<R>` whose `read_record` / `read_lazy_record` methods are `.await`ed.

```rust
let mut reader = DctReader::options(schema).from_tokio_path(path).await?;
while let Some(record) = reader.read_record().await? {
    // ...
}
```

The sync and async DCT paths share the same pure parsing state — only the I/O loop differs.

```rust
use dta::stata::dta::dta_reader::DtaReader;
use dta::stata::dta::dta_error::Result;

async fn read_async(path: &str) -> Result<()> {
    let mut characteristic_reader = DtaReader::new()
        .from_tokio_path(path)
        .await?
        .read_header()
        .await?
        .read_schema()
        .await?;
    characteristic_reader.skip_to_end().await?;

    let mut record_reader = characteristic_reader.into_record_reader().await?;
    while let Some(_record) = record_reader.read_record().await? {
        // ...
    }

    Ok(())
}
```

### Character encoding

[encoding_rs](https://docs.rs/encoding_rs) is a hard dependency, not a feature flag. The reader and writer pick an encoding from the release number by default: Windows-1252 for pre-118 files and UTF-8 for 118+. If you need to override that — for example, reading a pre-118 file produced by a locale that wasn't Windows-1252 — pass an explicit encoding:

```rust
use dta::stata::dta::dta_reader::DtaReader;

let reader = DtaReader::new()
    .encoding(encoding_rs::SHIFT_JIS)
    .from_path("legacy.dta")?;
```

## Benchmarks

The project ships [Criterion](https://docs.rs/criterion) benchmarks that exercise sync and async read/write throughput against a synthetic panel-style file:

```sh
# Sync benchmarks only (no optional features)
cargo bench

# Include the async (tokio) benchmarks
cargo bench --all-features
```

Results are saved to `target/criterion/` with HTML reports. To run a single benchmark by name:

```sh
cargo bench --all-features -- read_from_cursor
```

The first run generates `benches/data/bench_large.dta` (~100K rows) and caches it for subsequent runs.

## Profiling

The repository includes a profiling workflow built on [samply](https://github.com/mstange/samply). One-time setup:

```sh
# Install samply (Linux / macOS / Windows)
cargo install --locked samply

# On Linux, samply needs perf_event_open access:
echo 1 | sudo tee /proc/sys/kernel/perf_event_paranoid
```

Then run the profiling script:

```sh
# Default: 1M records, top 10 functions
./profile.sh

# Customize record count and report depth
./profile.sh --records 500000 --top 20
```

On Windows, use `.\profile.ps1` with the same options (`-Records`, `-Top`). samply uses ETW on Windows, so the script must run from an elevated PowerShell window.

The script builds optimized binaries with debug symbols, records separate samply profiles for the sync write, sync read, async write, and async read phases, and prints a table of the hottest functions from this crate by inclusive and self time.

## About

This is a passion project that I maintain on my own time. I care deeply about its quality and want it to be genuinely useful, but I also want to keep it fun and sustainable. To that end:

- **Bug reports** are always welcome. Please file issues for anything that isn't working correctly.
- **Feature requests** are best expressed as pull requests. I'm much more likely to engage with a well-crafted PR than a request for new work.
- **Timelines** are my own. I'll get to things when I can, and I may close issues or PRs that don't align with the project's direction – nothing personal.

If you find this library valuable, the best way to support it is to contribute or share it with others.
