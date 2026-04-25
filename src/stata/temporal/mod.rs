//! Stata temporal value support.
//!
//! Stata represents dates and timestamps as plain numeric values
//! whose meaning is encoded in the variable's display format string
//! (e.g., `%td`, `%tc`, `%tm`). The format string is the *only* thing
//! that distinguishes "days since 1960-01-01" from "milliseconds since
//! 1960-01-01" from "the year 2026" — the underlying storage is just
//! an `i16` / `i32` / `f64`.
//!
//! This module owns the Stata-domain knowledge needed to interpret
//! those values: the 1960 epoch, the format-prefix taxonomy, and the
//! period-decomposition math for week/month/quarter/half-formats. It
//! is deliberately time-crate-agnostic — every public item here works
//! without `chrono`, `jiff`, or `time`.
//!
//! Typed adapters for specific time crates layer on top via Cargo
//! features (currently only `chrono`; others may follow on demand).
//!
//! # Layering
//!
//! - [`TemporalKind`] + [`TemporalKind::from_format`]: classify a
//!   Stata format string. Returns `None` for non-temporal formats
//!   (`%9.0g`, `%-12s`, …) and ignores any display suffix
//!   (`%tdCCYY-NN-DD`).
//! - [`conversion`]: pure numeric helpers — `td_days_to_unix_days`,
//!   `tc_millis_to_unix_millis`, and period decomposers. These
//!   encode the Stata epoch and period boundaries once so consumers
//!   don't redo the math.
//!
//! # Why this lives in the crate
//!
//! The crate sits below data consumer libraries. Pushing
//! "interpret a `%td` value" up to every consumer guarantees that
//! the 1960 offset, ms-vs-days distinction, case-sensitive
//! `%tc`/`%tC`, and legacy `%d` alias get reinvented (and
//! occasionally miscoded) in every downstream library. Centralizing
//! the knowledge here is the value-add of being at this layer.

pub mod conversion;
pub mod kind;

pub use kind::TemporalKind;
