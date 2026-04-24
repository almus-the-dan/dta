# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.3.0] - 2026-04-24

### Added

- `StataByte::present`, `StataInt::present`, `StataLong::present`, `StataFloat::present`, and `StataDouble::present` return `Some(T)` for a present value and `None` for any missing variant, letting callers elide the `match` when they only care about the underlying scalar.

### Changed

- **Breaking:** `LongStringTable` now requires the caller to declare whether the table is being populated while reading a file or while preparing one for writing. Construct with `LongStringTable::for_reading()` or `LongStringTable::for_writing()` in place of `LongStringTable::new()` / `LongStringTable::default()`. The mode fixes how `get_or_insert` behaves: a writing table dedupes by payload bytes, while a reading table preserves the caller's `(variable, observation)` key.
- **Breaking:** `LongStringTable::get_or_insert_by_content` and `get_or_insert_by_key` are replaced by a single `get_or_insert(variable, observation, data, binary)` that dispatches on the table's mode.
- **Breaking:** `Release::supports_tagged_missing` and `Release::uses_magic_double_missing` are now crate-private. They were inadvertently published in 0.2.0; the release-gating they describe is an internal encoding concern.
- **Breaking:** The public error enums `DtaError`, `FormatErrorKind`, `Section`, `Field`, `Tag`, and `StataError` are now `#[non_exhaustive]`. Downstream `match` expressions on any of these must include a wildcard arm. Future releases can then add new variants (new sections, new format violations, new tagged missings) without breaking the public API.

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
// 0.2
let mut table = LongStringTable::new();
let reference = table.get_or_insert_by_content(variable, observation, data, binary);

// 0.3
let mut table = LongStringTable::for_writing();
let reference = table.get_or_insert(variable, observation, data, binary);
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
