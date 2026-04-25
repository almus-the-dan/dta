//! [`chrono`] adapters for Stata temporal values.
//!
//! Layer 2 of the temporal stack. Each function delegates to the
//! Layer 1 helper in [`super::conversion`] for the Stata-epoch math
//! and then maps the resulting Unix-relative count into the
//! corresponding [`chrono`] type.
//!
//! Available only when the crate is built with the `chrono` feature
//! enabled. The Layer 1 helpers remain available without it for
//! consumers that prefer a different time crate or want to consume
//! the raw Unix offsets directly.

use chrono::{DateTime, NaiveDate, NaiveDateTime, TimeDelta};

use super::conversion::{
    tc_millis_to_unix_millis, td_days_to_unix_days, th_halves_to_year_half,
    tm_months_to_year_month, tq_quarters_to_year_quarter, tw_weeks_to_year_week,
};
use super::kind::TemporalKind;
use crate::stata::dta::value::Value;

/// Converts a `%td` Stata day count (days since 1960-01-01) into a
/// [`NaiveDate`].
///
/// Returns `None` if the resulting date falls outside the
/// representable [`NaiveDate`] range (year ±262143). Stata-typical
/// inputs never trigger this.
///
/// # Examples
///
/// ```
/// use chrono::NaiveDate;
/// use dta::stata::temporal::chrono_adapter::naive_date_from_td_days;
///
/// // Stata day 0 is the Stata epoch.
/// assert_eq!(
///     naive_date_from_td_days(0),
///     NaiveDate::from_ymd_opt(1960, 1, 1),
/// );
/// // Stata day 3653 is the Unix epoch.
/// assert_eq!(
///     naive_date_from_td_days(3653),
///     NaiveDate::from_ymd_opt(1970, 1, 1),
/// );
/// ```
#[must_use]
pub fn naive_date_from_td_days(stata_days: i32) -> Option<NaiveDate> {
    let unix_days = td_days_to_unix_days(stata_days)?;
    // 1970-01-01 is always a valid Gregorian date; the `?` is a
    // belt-and-suspenders propagation that costs nothing at runtime
    // and avoids needing a `# Panics` doc section.
    let unix_epoch = NaiveDate::from_ymd_opt(1970, 1, 1)?;
    unix_epoch.checked_add_signed(TimeDelta::days(i64::from(unix_days)))
}

/// Converts a `%tc` Stata millisecond count (ms since
/// 1960-01-01T00:00:00, no leap-second adjustment) into a
/// [`NaiveDateTime`].
///
/// Returns `None` if `stata_millis` is non-finite, the result
/// overflows `i64` ms, or the resulting timestamp falls outside
/// chrono's representable range.
///
/// `%tC` (capital C, leap-second-adjusted) is not handled here —
/// use [`TemporalKind::DateTimeLeap`](super::TemporalKind::DateTimeLeap)
/// at the dispatcher layer to reject those rather than silently
/// treat them as `%tc`.
#[must_use]
pub fn naive_date_time_from_tc_millis(stata_millis: f64) -> Option<NaiveDateTime> {
    let unix_millis = tc_millis_to_unix_millis(stata_millis)?;
    DateTime::from_timestamp_millis(unix_millis).map(|dt| dt.naive_utc())
}

/// Converts a Stata [`Value`] into a [`NaiveDate`], treating it as a
/// `%td` (days since 1960-01-01) reading.
///
/// Accepted variants — widened losslessly to `i32`:
///
/// | Variant         | Behavior                              |
/// |-----------------|---------------------------------------|
/// | [`Value::Byte`] | Present `i8` → `i32`; missing → `None` |
/// | [`Value::Int`]  | Present `i16` → `i32`; missing → `None` |
/// | [`Value::Long`] | Present `i32` directly; missing → `None` |
///
/// All other variants — including [`Value::Float`] / [`Value::Double`]
/// — return `None`. Well-formed Stata files store `%td` columns as
/// integer types only; a date stored as a float is either an
/// upgrade-path artifact or malformed data, and silently coercing
/// it would mask bugs in the upstream pipeline. Drop to
/// [`naive_date_from_td_days`] with a manually extracted integer
/// if you need to handle such files.
///
/// Missing-value sentinels (`.` and the tagged `.a`–`.z` from
/// format 113+) all map to `None`. The dispatcher in
/// [`temporal_from_value`] preserves the same convention.
#[must_use]
pub fn naive_date_from_value(value: &Value<'_>) -> Option<NaiveDate> {
    naive_date_from_td_days(extract_stata_int32(value)?)
}

/// Extracts an `i32` from a Stata [`Value`], widening losslessly
/// from the integer storage variants and refusing the rest.
///
/// Used by every "this Stata column stores an integer" temporal
/// helper — `%td`, `%ty`, and the period formats (`%tw/%tm/%tq/%th`).
/// Returns `None` for missing values and for [`Value::Float`] /
/// [`Value::Double`] / [`Value::String`] / [`Value::LongStringRef`],
/// which are never used to store these formats in well-formed
/// files.
fn extract_stata_int32(value: &Value<'_>) -> Option<i32> {
    match value {
        Value::Byte(byte_value) => byte_value.present().map(i32::from),
        Value::Int(int_value) => int_value.present().map(i32::from),
        Value::Long(long_value) => long_value.present(),
        Value::Float(_) | Value::Double(_) | Value::String(_) | Value::LongStringRef(_) => None,
    }
}

/// Converts a Stata [`Value`] into a [`NaiveDateTime`], treating it
/// as a `%tc` (milliseconds since 1960-01-01T00:00:00) reading.
///
/// Accepted variants:
///
/// | Variant           | Behavior                                  |
/// |-------------------|-------------------------------------------|
/// | [`Value::Double`] | Present `f64` directly; missing → `None`  |
/// | [`Value::Float`]  | Present `f32` widened to `f64`; missing → `None` |
///
/// All other variants return `None`. `%tc` values exceed the `i32`
/// range past ~25 days from the Stata epoch, so well-formed Stata
/// files always use a floating-point storage type for `%tc`
/// columns. The integer variants are rejected to surface
/// upstream-pipeline bugs rather than coerce a 24-day-range value
/// silently.
///
/// Missing-value sentinels (`.` and the tagged `.a`–`.z` from
/// format 113+) map to `None`. Float widening to `f64` is
/// lossless. See [`naive_date_time_from_tc_millis`] for the
/// underlying conversion semantics, including non-finite handling.
#[must_use]
pub fn naive_date_time_from_value(value: &Value<'_>) -> Option<NaiveDateTime> {
    naive_date_time_from_tc_millis(extract_stata_float64(value)?)
}

/// Extracts an `f64` from a Stata [`Value`], widening losslessly
/// from [`Value::Float`] and refusing the integer storage variants.
///
/// Used by `%tc` conversion. Integer types are rejected because the
/// `%tc` value range exceeds `i32` after ~25 days from the Stata
/// epoch, so an integer-stored `%tc` column is malformed.
fn extract_stata_float64(value: &Value<'_>) -> Option<f64> {
    match value {
        Value::Double(double_value) => double_value.present(),
        Value::Float(float_value) => float_value.present().map(f64::from),
        Value::Byte(_)
        | Value::Int(_)
        | Value::Long(_)
        | Value::String(_)
        | Value::LongStringRef(_) => None,
    }
}

/// A Stata temporal value, classified by format and resolved into
/// the most useful representation per kind.
///
/// Returned by [`temporal_from_value`] when a Stata cell carries a
/// recognized temporal format. Date / datetime variants use
/// [`chrono`] types; period variants return `(year, sub-period)`
/// pairs since chrono has no native "year + week" or "year + half"
/// type, and synthesizing a `NaiveDate` (e.g., the first day of the
/// period) would commit consumers to a particular convention they
/// may not want.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum StataTemporal {
    /// `%td` (or legacy `%d`) — a calendar date.
    Date(NaiveDate),
    /// `%tc` — a datetime, no leap-second adjustment.
    DateTime(NaiveDateTime),
    /// `%ty` — a calendar year (e.g., `2026`). The Stata value *is*
    /// the year; no offset is applied.
    Year(i32),
    /// `%tm` — a year and 1-indexed month (`1..=12`).
    YearMonth {
        /// Calendar year.
        year: i32,
        /// Month, `1..=12`.
        month: u8,
    },
    /// `%tq` — a year and 1-indexed quarter (`1..=4`).
    YearQuarter {
        /// Calendar year.
        year: i32,
        /// Quarter, `1..=4`.
        quarter: u8,
    },
    /// `%th` — a year and 1-indexed half (`1..=2`).
    YearHalf {
        /// Calendar year.
        year: i32,
        /// Half, `1..=2`.
        half: u8,
    },
    /// `%tw` — a year and 1-indexed Stata week (`1..=52`).
    ///
    /// Note that Stata weeks are *not* ISO weeks: week 1 of any
    /// year always begins on January 1, and every year has exactly
    /// 52 weeks (the last absorbs the trailing days).
    YearWeek {
        /// Calendar year.
        year: i32,
        /// Stata week number, `1..=52`.
        week: u8,
    },
}

/// Classifies `format` and converts `value` accordingly.
///
/// This is the convenience entry point for "I have a cell, tell me
/// what it means as a date/time". The format string is parsed via
/// [`TemporalKind::from_format`], the value is interpreted using
/// the appropriate Layer 2 / Layer 1 helper, and the result is
/// wrapped in the matching [`StataTemporal`] variant.
///
/// Returns `None` in any of these cases:
///
/// - `format` is not a recognized temporal format (numeric, string,
///   empty, malformed).
/// - `format` is `%tC` (leap-second-adjusted datetime) — chrono
///   does not model leap seconds, and silently dropping them would
///   produce times an hour or seconds off in some ranges. Drop to
///   Layer 1 and decide your own policy if you need this.
/// - The cell holds a Stata missing-value sentinel (`.` or
///   `.a`–`.z`).
/// - The cell's storage type doesn't match what `format` expects
///   (e.g., a `%td` cell stored as [`Value::Double`], or a `%tc`
///   cell stored as [`Value::Long`]). Mismatches almost always
///   indicate upstream-pipeline bugs and are surfaced rather than
///   coerced.
/// - Conversion produces an out-of-range chrono value (only
///   triggered by extreme inputs far outside Stata-typical data).
///
/// # Examples
///
/// ```
/// use chrono::NaiveDate;
/// use dta::stata::dta::value::Value;
/// use dta::stata::stata_long::StataLong;
/// use dta::stata::temporal::chrono_adapter::{StataTemporal, temporal_from_value};
///
/// let cell = Value::Long(StataLong::Present(0));
/// assert_eq!(
///     temporal_from_value(&cell, "%td"),
///     Some(StataTemporal::Date(NaiveDate::from_ymd_opt(1960, 1, 1).unwrap())),
/// );
/// ```
#[must_use]
pub fn temporal_from_value(value: &Value<'_>, format: &str) -> Option<StataTemporal> {
    match TemporalKind::from_format(format)? {
        TemporalKind::Date => naive_date_from_value(value).map(StataTemporal::Date),
        TemporalKind::DateTime => naive_date_time_from_value(value).map(StataTemporal::DateTime),
        TemporalKind::DateTimeLeap => None,
        TemporalKind::Year => extract_stata_int32(value).map(StataTemporal::Year),
        TemporalKind::Month => {
            let (year, month) = tm_months_to_year_month(extract_stata_int32(value)?);
            Some(StataTemporal::YearMonth { year, month })
        }
        TemporalKind::Quarter => {
            let (year, quarter) = tq_quarters_to_year_quarter(extract_stata_int32(value)?);
            Some(StataTemporal::YearQuarter { year, quarter })
        }
        TemporalKind::HalfYear => {
            let (year, half) = th_halves_to_year_half(extract_stata_int32(value)?);
            Some(StataTemporal::YearHalf { year, half })
        }
        TemporalKind::Week => {
            let (year, week) = tw_weeks_to_year_week(extract_stata_int32(value)?);
            Some(StataTemporal::YearWeek { year, week })
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // -- naive_date_from_td_days --------------------------------------------

    #[test]
    fn td_zero_is_stata_epoch() {
        assert_eq!(
            naive_date_from_td_days(0),
            Some(NaiveDate::from_ymd_opt(1960, 1, 1).unwrap()),
        );
    }

    #[test]
    fn td_3653_is_unix_epoch() {
        assert_eq!(
            naive_date_from_td_days(3653),
            Some(NaiveDate::from_ymd_opt(1970, 1, 1).unwrap()),
        );
    }

    #[test]
    fn td_one_is_january_2_1960() {
        assert_eq!(
            naive_date_from_td_days(1),
            Some(NaiveDate::from_ymd_opt(1960, 1, 2).unwrap()),
        );
    }

    #[test]
    fn td_handles_1960_leap_day() {
        // 1960 was a leap year. Day 59 = 1960-02-29.
        assert_eq!(
            naive_date_from_td_days(59),
            Some(NaiveDate::from_ymd_opt(1960, 2, 29).unwrap()),
        );
    }

    #[test]
    fn td_negative_is_pre_epoch() {
        // 1959-12-31 is one day before the Stata epoch.
        assert_eq!(
            naive_date_from_td_days(-1),
            Some(NaiveDate::from_ymd_opt(1959, 12, 31).unwrap()),
        );
    }

    #[test]
    fn td_modern_date() {
        // 2026-04-24 — used to verify a plausible "today"-shaped date
        // independent of the simple round-trip cases.
        let expected = NaiveDate::from_ymd_opt(2026, 4, 24).unwrap();
        let stata_epoch = NaiveDate::from_ymd_opt(1960, 1, 1).unwrap();
        let stata_days =
            i32::try_from(expected.signed_duration_since(stata_epoch).num_days()).unwrap();
        assert_eq!(naive_date_from_td_days(stata_days), Some(expected));
    }

    #[test]
    fn td_overflow_at_layer_one_propagates() {
        // i32::MIN overflows in td_days_to_unix_days; the chrono
        // wrapper must surface that as None.
        assert_eq!(naive_date_from_td_days(i32::MIN), None);
    }

    #[test]
    fn td_extreme_positive_falls_outside_chrono_range() {
        // i32::MAX days past 1960 lands ~5.9 million years out, well
        // beyond chrono's NaiveDate max (year 262143).
        assert_eq!(naive_date_from_td_days(i32::MAX), None);
    }

    // -- naive_date_time_from_tc_millis -------------------------------------

    #[test]
    fn tc_zero_is_stata_epoch_midnight() {
        let expected = NaiveDate::from_ymd_opt(1960, 1, 1)
            .unwrap()
            .and_hms_opt(0, 0, 0)
            .unwrap();
        assert_eq!(naive_date_time_from_tc_millis(0.0), Some(expected));
    }

    #[test]
    fn tc_one_second_after_epoch() {
        let expected = NaiveDate::from_ymd_opt(1960, 1, 1)
            .unwrap()
            .and_hms_opt(0, 0, 1)
            .unwrap();
        assert_eq!(naive_date_time_from_tc_millis(1000.0), Some(expected));
    }

    #[test]
    fn tc_with_subsecond_milliseconds() {
        let expected = NaiveDate::from_ymd_opt(1960, 1, 1)
            .unwrap()
            .and_hms_milli_opt(0, 0, 0, 123)
            .unwrap();
        assert_eq!(naive_date_time_from_tc_millis(123.0), Some(expected));
    }

    #[test]
    fn tc_unix_epoch() {
        let unix_epoch = NaiveDate::from_ymd_opt(1970, 1, 1)
            .unwrap()
            .and_hms_opt(0, 0, 0)
            .unwrap();
        let stata_millis_at_unix_epoch = 3653.0_f64 * 86_400_000.0;
        assert_eq!(
            naive_date_time_from_tc_millis(stata_millis_at_unix_epoch),
            Some(unix_epoch),
        );
    }

    #[test]
    fn tc_pre_epoch() {
        let expected = NaiveDate::from_ymd_opt(1959, 12, 31)
            .unwrap()
            .and_hms_opt(23, 59, 59)
            .unwrap();
        assert_eq!(naive_date_time_from_tc_millis(-1000.0), Some(expected),);
    }

    #[test]
    fn tc_nan_is_none() {
        assert_eq!(naive_date_time_from_tc_millis(f64::NAN), None);
    }

    #[test]
    fn tc_positive_infinity_is_none() {
        assert_eq!(naive_date_time_from_tc_millis(f64::INFINITY), None);
    }

    #[test]
    fn tc_negative_infinity_is_none() {
        assert_eq!(naive_date_time_from_tc_millis(f64::NEG_INFINITY), None);
    }

    #[test]
    fn tc_above_i64_range_is_none() {
        // Value that overflows i64 ms after the epoch shift.
        assert_eq!(naive_date_time_from_tc_millis(9.3e18), None,);
    }

    #[test]
    fn tc_above_chrono_range_but_within_i64_is_none() {
        // chrono's NaiveDateTime maxes out around year 262143; an
        // i64 ms count well past that should still be rejected by
        // the chrono constructor.
        let beyond_chrono = 1.0e16_f64; // ~317000 years post-Stata-epoch
        assert_eq!(naive_date_time_from_tc_millis(beyond_chrono), None);
    }

    // -- naive_date_from_value -----------------------------------------------

    mod date_from_value {
        use super::super::*;
        use crate::stata::dta::long_string_ref::LongStringRef;
        use crate::stata::missing_value::MissingValue;
        use crate::stata::stata_byte::StataByte;
        use crate::stata::stata_double::StataDouble;
        use crate::stata::stata_float::StataFloat;
        use crate::stata::stata_int::StataInt;
        use crate::stata::stata_long::StataLong;

        fn epoch() -> NaiveDate {
            NaiveDate::from_ymd_opt(1960, 1, 1).unwrap()
        }

        #[test]
        fn long_present_zero_is_epoch() {
            let value = Value::Long(StataLong::Present(0));
            assert_eq!(naive_date_from_value(&value), Some(epoch()));
        }

        #[test]
        fn long_present_nonzero() {
            let value = Value::Long(StataLong::Present(366));
            assert_eq!(
                naive_date_from_value(&value),
                Some(NaiveDate::from_ymd_opt(1961, 1, 1).unwrap()),
            );
        }

        #[test]
        fn long_present_negative_pre_epoch() {
            let value = Value::Long(StataLong::Present(-1));
            assert_eq!(
                naive_date_from_value(&value),
                Some(NaiveDate::from_ymd_opt(1959, 12, 31).unwrap()),
            );
        }

        #[test]
        fn int_present_widens_to_i32() {
            let value = Value::Int(StataInt::Present(366));
            assert_eq!(
                naive_date_from_value(&value),
                Some(NaiveDate::from_ymd_opt(1961, 1, 1).unwrap()),
            );
        }

        #[test]
        fn byte_present_widens_to_i32() {
            let value = Value::Byte(StataByte::Present(31));
            assert_eq!(
                naive_date_from_value(&value),
                Some(NaiveDate::from_ymd_opt(1960, 2, 1).unwrap()),
            );
        }

        #[test]
        fn long_missing_system_is_none() {
            let value = Value::Long(StataLong::Missing(MissingValue::System));
            assert_eq!(naive_date_from_value(&value), None);
        }

        #[test]
        fn long_missing_tagged_is_none() {
            let value = Value::Long(StataLong::Missing(MissingValue::A));
            assert_eq!(naive_date_from_value(&value), None);
        }

        #[test]
        fn int_missing_is_none() {
            let value = Value::Int(StataInt::Missing(MissingValue::System));
            assert_eq!(naive_date_from_value(&value), None);
        }

        #[test]
        fn byte_missing_is_none() {
            let value = Value::Byte(StataByte::Missing(MissingValue::System));
            assert_eq!(naive_date_from_value(&value), None);
        }

        #[test]
        fn float_present_is_none() {
            // Well-formed Stata files don't store %td as Float; we
            // refuse rather than coerce.
            let value = Value::Float(StataFloat::Present(0.0));
            assert_eq!(naive_date_from_value(&value), None);
        }

        #[test]
        fn double_present_is_none() {
            let value = Value::Double(StataDouble::Present(0.0));
            assert_eq!(naive_date_from_value(&value), None);
        }

        #[test]
        fn string_is_none() {
            let value = Value::string("not a date");
            assert_eq!(naive_date_from_value(&value), None);
        }

        #[test]
        fn long_string_ref_is_none() {
            let value = Value::LongStringRef(LongStringRef::new(1, 1));
            assert_eq!(naive_date_from_value(&value), None);
        }

        #[test]
        fn long_present_extreme_returns_none_via_chrono_range() {
            let value = Value::Long(StataLong::Present(i32::MAX));
            assert_eq!(naive_date_from_value(&value), None);
        }
    }

    // -- naive_date_time_from_value ------------------------------------------

    mod date_time_from_value {
        use super::super::*;
        use crate::stata::dta::long_string_ref::LongStringRef;
        use crate::stata::missing_value::MissingValue;
        use crate::stata::stata_byte::StataByte;
        use crate::stata::stata_double::StataDouble;
        use crate::stata::stata_float::StataFloat;
        use crate::stata::stata_int::StataInt;
        use crate::stata::stata_long::StataLong;

        fn epoch_midnight() -> NaiveDateTime {
            NaiveDate::from_ymd_opt(1960, 1, 1)
                .unwrap()
                .and_hms_opt(0, 0, 0)
                .unwrap()
        }

        #[test]
        fn double_present_zero_is_epoch_midnight() {
            let value = Value::Double(StataDouble::Present(0.0));
            assert_eq!(naive_date_time_from_value(&value), Some(epoch_midnight()));
        }

        #[test]
        fn double_present_one_second() {
            let value = Value::Double(StataDouble::Present(1000.0));
            let expected = NaiveDate::from_ymd_opt(1960, 1, 1)
                .unwrap()
                .and_hms_opt(0, 0, 1)
                .unwrap();
            assert_eq!(naive_date_time_from_value(&value), Some(expected));
        }

        #[test]
        fn float_present_widens_losslessly() {
            // Float can exactly represent 0.0 and 1000.0 — pick
            // values within Float's integer-precision range
            // (~2^24 = 16777216) so the widening is exact.
            let value = Value::Float(StataFloat::Present(1000.0));
            let expected = NaiveDate::from_ymd_opt(1960, 1, 1)
                .unwrap()
                .and_hms_opt(0, 0, 1)
                .unwrap();
            assert_eq!(naive_date_time_from_value(&value), Some(expected));
        }

        #[test]
        fn double_missing_system_is_none() {
            let value = Value::Double(StataDouble::Missing(MissingValue::System));
            assert_eq!(naive_date_time_from_value(&value), None);
        }

        #[test]
        fn double_missing_tagged_is_none() {
            let value = Value::Double(StataDouble::Missing(MissingValue::Z));
            assert_eq!(naive_date_time_from_value(&value), None);
        }

        #[test]
        fn float_missing_is_none() {
            let value = Value::Float(StataFloat::Missing(MissingValue::System));
            assert_eq!(naive_date_time_from_value(&value), None);
        }

        #[test]
        fn double_nan_present_is_none() {
            // NaN here is a *present* f64 value (not a missing
            // sentinel) — the Layer 1 finite-check rejects it.
            let value = Value::Double(StataDouble::Present(f64::NAN));
            assert_eq!(naive_date_time_from_value(&value), None);
        }

        #[test]
        fn long_present_is_none() {
            // Integer storage for %tc is malformed (range is ~24
            // days). Refuse rather than coerce.
            let value = Value::Long(StataLong::Present(0));
            assert_eq!(naive_date_time_from_value(&value), None);
        }

        #[test]
        fn int_present_is_none() {
            let value = Value::Int(StataInt::Present(0));
            assert_eq!(naive_date_time_from_value(&value), None);
        }

        #[test]
        fn byte_present_is_none() {
            let value = Value::Byte(StataByte::Present(0));
            assert_eq!(naive_date_time_from_value(&value), None);
        }

        #[test]
        fn string_is_none() {
            let value = Value::string("not a datetime");
            assert_eq!(naive_date_time_from_value(&value), None);
        }

        #[test]
        fn long_string_ref_is_none() {
            let value = Value::LongStringRef(LongStringRef::new(1, 1));
            assert_eq!(naive_date_time_from_value(&value), None);
        }
    }

    // -- temporal_from_value -------------------------------------------------

    mod dispatcher {
        use super::super::*;
        use crate::stata::missing_value::MissingValue;
        use crate::stata::stata_double::StataDouble;
        use crate::stata::stata_int::StataInt;
        use crate::stata::stata_long::StataLong;

        #[test]
        fn td_dispatches_to_date() {
            let value = Value::Long(StataLong::Present(0));
            assert_eq!(
                temporal_from_value(&value, "%td"),
                Some(StataTemporal::Date(
                    NaiveDate::from_ymd_opt(1960, 1, 1).unwrap()
                )),
            );
        }

        #[test]
        fn td_with_display_suffix_still_dispatches() {
            let value = Value::Long(StataLong::Present(0));
            assert_eq!(
                temporal_from_value(&value, "%tdCCYY-NN-DD"),
                Some(StataTemporal::Date(
                    NaiveDate::from_ymd_opt(1960, 1, 1).unwrap()
                )),
            );
        }

        #[test]
        fn legacy_d_dispatches_to_date() {
            let value = Value::Long(StataLong::Present(0));
            assert_eq!(
                temporal_from_value(&value, "%d"),
                Some(StataTemporal::Date(
                    NaiveDate::from_ymd_opt(1960, 1, 1).unwrap()
                )),
            );
        }

        #[test]
        fn tc_dispatches_to_datetime() {
            let value = Value::Double(StataDouble::Present(0.0));
            let expected = NaiveDate::from_ymd_opt(1960, 1, 1)
                .unwrap()
                .and_hms_opt(0, 0, 0)
                .unwrap();
            assert_eq!(
                temporal_from_value(&value, "%tc"),
                Some(StataTemporal::DateTime(expected)),
            );
        }

        #[test]
        fn tc_leap_returns_none() {
            // Even with a perfectly valid Double, %tC is refused
            // because chrono can't model leap seconds.
            let value = Value::Double(StataDouble::Present(0.0));
            assert_eq!(temporal_from_value(&value, "%tC"), None);
        }

        #[test]
        fn ty_dispatches_to_year() {
            let value = Value::Int(StataInt::Present(2026));
            assert_eq!(
                temporal_from_value(&value, "%ty"),
                Some(StataTemporal::Year(2026)),
            );
        }

        #[test]
        fn tm_dispatches_to_year_month() {
            let value = Value::Int(StataInt::Present(0));
            assert_eq!(
                temporal_from_value(&value, "%tm"),
                Some(StataTemporal::YearMonth {
                    year: 1960,
                    month: 1
                }),
            );
        }

        #[test]
        fn tm_negative_offset_decomposes_correctly() {
            let value = Value::Int(StataInt::Present(-1));
            assert_eq!(
                temporal_from_value(&value, "%tm"),
                Some(StataTemporal::YearMonth {
                    year: 1959,
                    month: 12
                }),
            );
        }

        #[test]
        fn tq_dispatches_to_year_quarter() {
            let value = Value::Int(StataInt::Present(5));
            assert_eq!(
                temporal_from_value(&value, "%tq"),
                Some(StataTemporal::YearQuarter {
                    year: 1961,
                    quarter: 2
                }),
            );
        }

        #[test]
        fn th_dispatches_to_year_half() {
            let value = Value::Int(StataInt::Present(3));
            assert_eq!(
                temporal_from_value(&value, "%th"),
                Some(StataTemporal::YearHalf {
                    year: 1961,
                    half: 2
                }),
            );
        }

        #[test]
        fn tw_dispatches_to_year_week() {
            let value = Value::Int(StataInt::Present(52));
            assert_eq!(
                temporal_from_value(&value, "%tw"),
                Some(StataTemporal::YearWeek {
                    year: 1961,
                    week: 1
                }),
            );
        }

        // -- non-temporal formats --------------------------------------------

        #[test]
        fn numeric_format_returns_none() {
            let value = Value::Long(StataLong::Present(0));
            assert_eq!(temporal_from_value(&value, "%9.0g"), None);
        }

        #[test]
        fn string_format_returns_none() {
            let value = Value::Long(StataLong::Present(0));
            assert_eq!(temporal_from_value(&value, "%-12s"), None);
        }

        #[test]
        fn empty_format_returns_none() {
            let value = Value::Long(StataLong::Present(0));
            assert_eq!(temporal_from_value(&value, ""), None);
        }

        #[test]
        fn malformed_format_returns_none() {
            let value = Value::Long(StataLong::Present(0));
            assert_eq!(temporal_from_value(&value, "%t"), None);
            assert_eq!(temporal_from_value(&value, "%tx"), None);
        }

        // -- missing values --------------------------------------------------

        #[test]
        fn missing_long_with_td_returns_none() {
            let value = Value::Long(StataLong::Missing(MissingValue::System));
            assert_eq!(temporal_from_value(&value, "%td"), None);
        }

        #[test]
        fn missing_double_with_tc_returns_none() {
            let value = Value::Double(StataDouble::Missing(MissingValue::A));
            assert_eq!(temporal_from_value(&value, "%tc"), None);
        }

        #[test]
        fn missing_int_with_tm_returns_none() {
            let value = Value::Int(StataInt::Missing(MissingValue::System));
            assert_eq!(temporal_from_value(&value, "%tm"), None);
        }

        // -- mismatched value/format combinations ----------------------------

        #[test]
        fn double_with_td_returns_none() {
            // %td expects an integer storage type; a Double is
            // malformed and refused.
            let value = Value::Double(StataDouble::Present(0.0));
            assert_eq!(temporal_from_value(&value, "%td"), None);
        }

        #[test]
        fn long_with_tc_returns_none() {
            // %tc requires Double; a Long-stored %tc column is
            // malformed.
            let value = Value::Long(StataLong::Present(0));
            assert_eq!(temporal_from_value(&value, "%tc"), None);
        }

        #[test]
        fn double_with_tm_returns_none() {
            // Period formats use integer storage like %td.
            let value = Value::Double(StataDouble::Present(0.0));
            assert_eq!(temporal_from_value(&value, "%tm"), None);
        }
    }
}
