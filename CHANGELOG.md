# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- New `chrono` Cargo feature (off by default) bringing typed date/time conversions for Stata temporal values. The crate sits below pandas / haven / ReadStat in the stack, so the Stata-domain knowledge — 1960 epoch, milliseconds-vs-days, the case-sensitive `%tc`/`%tC` distinction, the legacy `%d` alias, and per-format storage-type expectations — is encoded once here instead of being reinvented by each downstream library.
- New module `dta::stata::temporal`. Layered so the lower layers stay time-crate-agnostic and ship even without the feature flag:
  - `temporal::TemporalKind::from_format(&str) -> Option<TemporalKind>` classifies a Stata format string into one of `Date` / `DateTime` / `DateTimeLeap` / `Week` / `Month` / `Quarter` / `HalfYear` / `Year`. Recognizes the eight `%t*` prefixes plus the legacy `%d` alias, and ignores display suffixes (`%tdCCYY-NN-DD` classifies the same as bare `%td`). Available without the `chrono` feature.
  - `temporal::conversion` exposes `STATA_EPOCH_UNIX_DAYS`, `STATA_EPOCH_UNIX_MILLIS`, `td_days_to_unix_days`, `tc_millis_to_unix_millis`, and `(year, sub-period)` decomposers `tw_weeks_to_year_week` / `tm_months_to_year_month` / `tq_quarters_to_year_quarter` / `th_halves_to_year_half`. Available without the `chrono` feature.
  - `temporal::chrono_adapter` (behind the `chrono` feature) wraps the conversion layer with `naive_date_from_td_days`, `naive_date_time_from_tc_millis`, `Value`-aware `naive_date_from_value` / `naive_date_time_from_value` (handling Stata missing-value sentinels and storage-type widening), and a `temporal_from_value(&Value, &str) -> Option<StataTemporal>` dispatcher. The `StataTemporal` enum is `#[non_exhaustive]` and unifies `Date(NaiveDate)`, `DateTime(NaiveDateTime)`, `Year(i32)`, and `(year, sub-period)` variants for the period formats.
- `StataTimestamp::to_naive_date_time(&self) -> Option<chrono::NaiveDateTime>` (behind the `chrono` feature) for converting a parsed file-header timestamp directly to a chrono value. Returns `None` when the parsed components don't form a valid Gregorian date (e.g., `31 Feb`, `29 Feb 2023`).

### Fixed

- Reading a strL value from a V119 file no longer returns a `LongStringRef` whose `(variable, observation)` pair fails to resolve against the strL table. V119 redistributed the 8-byte data-section strL ref from V118's `u16` variable + `u48` observation into `u24` variable + `u40` observation; 0.3.0 ran V118 logic for V119 files, producing tuples that didn't match any GSO entry. Both reader and writer now dispatch on release across V117 (`u32`+`u32`) / V118 (`u16`+`u48`) / V119+ (`u24`+`u40`), with the V119 layout determined empirically from pandas's `stata12_119.dta` and `stata12_be_119.dta` fixtures and verified end-to-end on both byte orders.
- Reading a value-label section through the seek-based navigation chain on a pre-117 file no longer fails with `DtaError::Io { section: ValueLabels, source: UnexpectedEof }`. `seek_long_strings` for a format without a strL section (V102–V116) was leaving the cursor wherever the previous reader left it — at the start of the records section after `skip_to_end()` on characteristics — so the chained `into_value_label_reader()` then read records bytes as value-label headers. `seek_long_strings` now parks the immediately-completed `LongStringReader` at the value-labels offset for pre-117 files, so the chain `seek_long_strings → into_value_label_reader → read_remaining_into` works on every supported release.

### Notes

- `%tC` (capital C, leap-second-adjusted) is recognized by the format parser but the `chrono` adapter explicitly returns `None` for it. chrono does not model leap seconds, and silently treating `%tC` as `%tc` would produce timestamps that drift by seconds (and occasionally a minute) for values past the leap-second epoch. Consumers needing `%tC` can drop to `tc_millis_to_unix_millis` and apply their own policy.
- The `Value`-aware helpers refuse storage/format mismatches (e.g., a `%td` cell stored as `Double`, or a `%tc` cell stored as `Long`) instead of coercing them, on the grounds that well-formed Stata files never produce these combinations and silent coercion would mask upstream-pipeline bugs. Drop to the Layer 1 helpers with a manually extracted scalar if you genuinely need to handle malformed data.

## [0.3.0] - 2026-04-24

### Added

- `StataByte::present`, `StataInt::present`, `StataLong::present`, `StataFloat::present`, and `StataDouble::present` return `Some(T)` for a present value and `None` for any missing variant, letting callers elide the `match` when they only care about the underlying scalar.
- `Value::string(&'a str)` convenience constructor that wraps the argument in `Cow::Borrowed` — useful when building records for the writer.
- Support for Stata formats **V102** and **V103**. The reader handles the V102 quirks — `0x00` byteorder byte (DOS/Intel implied little-endian), `u16` observation count, and the absence of the `byte` storage type — and writers emit the same shape. The pandas fixtures at V102 and V103 now read end-to-end. New error variant `FormatErrorKind::BigEndianUnsupported { release }` fires when big-endian is requested for V102, which Stata 3 never supported. Attempting to write a V102 file with a `byte` variable errors as `UnsupportedVariableType` (same pattern as `strL` in pre-V117 files).

### Changed

- **Breaking:** `LongStringTable` now requires the caller to declare whether the table is being populated while reading a file or while preparing one for writing. Construct with `LongStringTable::for_reading()` or `LongStringTable::for_writing()` in place of `LongStringTable::new()` / `LongStringTable::default()`. The mode fixes how `get_or_insert` behaves: a writing table dedupes by payload bytes, while a reading table preserves the caller's `(variable, observation)` key.
- **Breaking:** `LongStringTable::get_or_insert_by_content` and `get_or_insert_by_key` are replaced by a single `get_or_insert(variable, observation, data, binary)` that dispatches on the table's mode.
- **Breaking:** `Release::supports_tagged_missing` and `Release::uses_magic_double_missing` are now crate-private. They were inadvertently published in 0.2.0; the release-gating they describe is an internal encoding concern.
- **Breaking:** The public error enums `DtaError`, `FormatErrorKind`, `Section`, `Field`, `Tag`, and `StataError` are now `#[non_exhaustive]`. Downstream `match` expressions on any of these must include a wildcard arm. Future releases can then add new variants (new sections, new format violations, new tagged missings) without breaking the public API.
- **Breaking:** Pre-V108 value-label sets now round-trip the real Stata layout — `u16 n` + 9-byte name + 1-byte pad + `u16` values + 8-byte fixed-width labels — across V104, V105, V106, and V107. V104 files written by other tools now round-trip; files written by `dta` 0.2.0 with a V104 header are *not* readable by 0.3.0, since 0.2.0 used a self-consistent slot-indexed format that no other tool produces.
- **Breaking:** `FormatErrorKind::OldValueLabelValueOutOfRange` now fires on values outside `i16` range (`-32768..=32767`). The old bound (`0..=8190`, the V104 slot-table maximum) no longer applies. Negative values and values up to 32767 are valid in pre-V108 sets, and duplicates are preserved in order.
- **Breaking:** `Value::String` now holds `Cow<'a, str>` instead of `&'a str`, and `Value` no longer derives `Copy` (it still derives `Clone`). On the happy path (UTF-8 or ASCII content), the reader still borrows directly from the row buffer — `Cow::Borrowed`. Content that needs transcoding (e.g. pre-V118 files with non-ASCII Windows-1252 characters) is returned as `Cow::Owned` instead of failing. Construct from a `&str` literal with the new `Value::string` helper or `Cow::Borrowed` explicitly. Most read-side code that just wants `&str` works unchanged because `Cow<str>: Deref<Target = str>` — use `s.as_ref()` if you need an explicit `&str`.
- **Breaking:** `LongString` no longer carries the encoding. `LongString::data_str` now takes the encoding as an argument instead of using a stored value; `LongStringTable::get` and `LongStringTable::iter` lose their encoding parameters in turn. Pass the encoding reported by the reader or writer that produced the entry (`reader.encoding()` / `writer.encoding()`) when you call `data_str`. Removes redundant state from every `LongString` instance and keeps the encoding explicit at the decode site, matching how `Value::from_column_bytes` and the value-label payload parsers already work.
- **Breaking:** Replaced the `is_binary: bool` + bytes pair on `LongString` with a new `LongStringContent<'a>` enum carrying `Text(Cow<'a, [u8]>)` / `Binary(Cow<'a, [u8]>)`. `LongString::new` now takes a `LongStringContent`; `LongStringTable::get_or_insert` takes `impl Into<LongStringContent<'_>>`. `From<&str>` is implemented for `LongStringContent` so text payloads stay ergonomic (`table.get_or_insert(var, obs, "hello")`); binary payloads must be constructed explicitly (`LongStringContent::Binary(Cow::Borrowed(&bytes))`). `LongString::is_binary()` and `is_text()` remain as convenience shortcuts; new `LongString::content()` returns the enum for callers who want to match on the variant.

### Fixed

- Reading a real Stata or pandas V105-V107 file no longer fails with `DtaError::Io { section: ValueLabels, source: UnexpectedEof }`. 0.2.0 routed V105-V107 through the V108+ "modern" value-label path, which reads a `u32` length where the file stores a `u16`; the first length read then asked for gigabytes of payload from an 84-byte section. Pre-V108 files now follow the correct `u16 n` layout (see the related entry under "Changed").
- Reading a record from a non-UTF-8 file no longer fails with `"cannot return non-UTF-8 decoded string as a reference; use read_record() for non-UTF-8 files with non-ASCII strings"` — that error was misleading (the suggested workaround routed through the same failing code path) and is now gone. Non-ASCII content in non-UTF-8 encodings is returned via the owned branch of `Cow<'a, str>` in `Value::String`.

### Removed

- `LongStringTable::new()` and its `Default` implementation — use `for_reading()` or `for_writing()` instead.
- `LongStringTable::get_or_insert_by_content` and `LongStringTable::get_or_insert_by_key` — use `get_or_insert` on a mode-specific table.

### Migration guide

#### Populating a table while reading

```rust
// 0.2
let mut table = LongStringTable::new();
reader.read_remaining_into(&mut table)?;

// 0.3
let mut table = LongStringTable::for_reading();
reader.read_remaining_into(&mut table)?;
```

#### Collecting strL payloads before writing

```rust
use std::borrow::Cow;
use dta::stata::dta::long_string::LongStringContent;

// 0.2
let mut table = LongStringTable::new();
let reference = table.get_or_insert_by_content(variable, observation, data, binary);

// 0.3 — text via the &str ergonomic path
let mut table = LongStringTable::for_writing();
let reference = table.get_or_insert(variable, observation, "payload");

// 0.3 — binary requires the explicit variant
let reference = table.get_or_insert(
    variable,
    observation,
    LongStringContent::Binary(Cow::Borrowed(&bytes)),
);
```

#### Decoding a long string

```rust
// 0.2
let text = table.get(&reference, encoding).and_then(|s| s.data_str());

// 0.3 — encoding moves from `get`/`LongString` to `data_str`
let text = table.get(&reference).and_then(|s| s.data_str(encoding));
```

## [0.2.0] - 2026-04-23

### Added

- `FormatErrorKind::TaggedMissingUnsupported` is returned by the record writers when a tagged missing value is written to a pre-113 file.
- `tests/read_v104_missings.rs` integration tests covering system-missing round-tripping on V104 fixtures and V117 tagged-missing round-trips.

### Changed

- **Breaking:** `StataByte`, `StataInt`, `StataLong`, `StataFloat`, and `StataDouble` replace their `TryFrom<raw>` / `From<StataX> for raw` conversions with `from_raw(raw, release)` and `to_raw(release)` methods. Decoding and encoding now depend on the DTA release, so that:
  - Pre-113 files encode system missing (`.`) using the single legacy sentinel per numeric type and treat tagged missings as unrepresentable.
  - Formats 104 and 105 recognize the legacy `2^333` double-missing bit pattern.
  - 113+ files continue to use the documented 27-value missing range.
- **Breaking:** `StataByte::to_raw` and the matching `to_raw` methods on the other numeric types return `Result` — writing a tagged missing (`.a`–`.z`) to a pre-113 file now errors instead of silently producing a garbage byte.
- `Value::parse` threads the active `Release` through to the per-type parsers so that numeric values decode correctly on every supported format.

### Migration guide

#### Decoding a raw byte / word

```rust
// 0.1
let byte = StataByte::try_from(raw)?;

// 0.2
let byte = StataByte::from_raw(raw, release)?;
```

The same shape applies to `StataInt::from_raw`, `StataLong::from_raw`, `StataFloat::from_raw`, and `StataDouble::from_raw`.

#### Encoding back to the DTA representation

```rust
// 0.1
let raw: u8 = byte.into();

// 0.2
let raw: u8 = byte.to_raw(release)?;
```

The encode methods now return `Result`; handle `StataError::TaggedMissingUnsupported` when targeting a pre-113 format.

## [0.1.0] - 2026-04-23

Initial release.
