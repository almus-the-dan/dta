//! Numeric conversions for Stata temporal values.
//!
//! These helpers translate the raw integer or floating-point values
//! Stata stores for date / datetime / period columns into more
//! universal forms — Unix-relative day counts and millisecond
//! counts, plus `(year, sub-period)` decompositions for the period
//! formats.
//!
//! Everything here is time-crate-agnostic: no `chrono`, no `jiff`,
//! no `time`. Adapters for those crates are layered on top, under feature
//! flags. A consumer that wants to use a different time crate (or
//! none at all) can call these helpers directly.
//!
//! # Conventions
//!
//! - **Stata epoch** is 1960-01-01 (UTC, ignoring leap seconds for
//!   `%tc`).
//! - **Period formats** (`%tw`, `%tm`, `%tq`, `%th`) store an offset
//!   from the first period of 1960; the helpers decompose into
//!   `(year, sub-period)` where the sub-period is 1-indexed.
//! - **Stata weeks are not ISO weeks.** Each year has exactly 52
//!   weeks; week 1 starts on January 1; the last week absorbs the
//!   extra 1–2 days.
//! - All Layer 1 functions return `Option` for any input that would
//!   overflow or otherwise yield an undefined result. Stata-typical
//!   inputs (within ±10000 years of the epoch) never trigger `None`.

/// Days from the Unix epoch (1970-01-01) back to the Stata epoch
/// (1960-01-01).
///
/// Negative because Stata's epoch precedes Unix's. Computed as ten
/// 365-day years plus three intervening leap days (1960, 1964, 1968).
pub const STATA_EPOCH_UNIX_DAYS: i32 = -3653;

/// Milliseconds from the Unix epoch back to the Stata epoch.
///
/// Equal to [`STATA_EPOCH_UNIX_DAYS`] times the number of
/// milliseconds in a day, expressed as `i64` so it can serve
/// directly as an additive offset for `%tc` conversions.
pub const STATA_EPOCH_UNIX_MILLIS: i64 = (STATA_EPOCH_UNIX_DAYS as i64) * 86_400_000;

/// Converts a `%td` Stata day count (days since 1960-01-01) into a
/// Unix day count (days since 1970-01-01).
///
/// Returns `None` only when the addition would overflow `i32`,
/// which is well outside any plausible Stata data range (the
/// boundary is roughly year ±5.9 million).
///
/// # Examples
///
/// ```
/// use dta::stata::temporal::conversion::{td_days_to_unix_days, STATA_EPOCH_UNIX_DAYS};
///
/// // Stata day 0 (1960-01-01) is 3653 days before the Unix epoch.
/// assert_eq!(td_days_to_unix_days(0), Some(STATA_EPOCH_UNIX_DAYS));
/// // Stata day 3653 (1970-01-01) is the Unix epoch.
/// assert_eq!(td_days_to_unix_days(3653), Some(0));
/// ```
#[must_use]
#[inline]
pub fn td_days_to_unix_days(stata_days: i32) -> Option<i32> {
    stata_days.checked_add(STATA_EPOCH_UNIX_DAYS)
}

/// Converts a `%tc` Stata millisecond count into a Unix millisecond
/// count.
///
/// `%tc` values are stored as `f64` because the integer count
/// exceeds `i32` range almost immediately past the epoch. This
/// helper converts to `i64` ms — the standard form expected by most
/// time crates' `from_timestamp_millis` constructors.
///
/// Returns `None` if `stata_millis` is non-finite (NaN, ±∞), or if
/// the resulting value falls outside `i64` range. Note that "fits
/// in `i64`" is much wider than any time crate can represent — the
/// caller should still treat the returned `i64` as potentially
/// out-of-range for its target type.
///
/// Stata stores `%tc` as integral milliseconds; any fractional
/// component is rounded to the nearest integer (with halves toward
/// even).
#[must_use]
pub fn tc_millis_to_unix_millis(stata_millis: f64) -> Option<i64> {
    if !stata_millis.is_finite() {
        return None;
    }
    // f64 addition is safely lossless here only when the operands
    // fit in 53 bits. Far-future / far-past Stata values lose
    // precision — that's a property of the f64 storage choice, not
    // our arithmetic, so we accept the rounding silently.
    //
    // Computing the epoch offset from the i32 day constant via
    // `f64::from` keeps every step lossless: i32 → f64 is exact,
    // and -3653 × 86_400_000 = -3.156e11 is well within 2^52.
    let epoch_offset = f64::from(STATA_EPOCH_UNIX_DAYS) * 86_400_000.0;
    let unix = stata_millis + epoch_offset;
    let rounded = unix.round();
    // Range guard: rule out f64 values that would saturate-cast to
    // i64::MAX / i64::MIN. The upper bound is `2.0_f64.powi(63)`,
    // which is exactly representable and equals `i64::MAX + 1`.
    let i64_max_plus_one = 9_223_372_036_854_775_808.0_f64;
    if rounded < -i64_max_plus_one || rounded >= i64_max_plus_one {
        return None;
    }
    // f64 → i64 has no `TryFrom` in stable Rust. The range guard
    // above ensures `as` won't saturate or produce UB.
    #[allow(clippy::cast_possible_truncation)]
    Some(rounded as i64)
}

/// Decomposes a `%tm` Stata month offset (months since 1960m1) into
/// `(year, month)` with `month` in `1..=12`.
///
/// Negative offsets land before 1960 and are decomposed using floor
/// division (so month -1 is 1959-12, not 1960-(-1)).
///
/// # Examples
///
/// ```
/// use dta::stata::temporal::conversion::tm_months_to_year_month;
///
/// assert_eq!(tm_months_to_year_month(0), (1960, 1));
/// assert_eq!(tm_months_to_year_month(11), (1960, 12));
/// assert_eq!(tm_months_to_year_month(12), (1961, 1));
/// assert_eq!(tm_months_to_year_month(-1), (1959, 12));
/// assert_eq!(tm_months_to_year_month(-13), (1958, 12));
/// ```
#[must_use]
pub fn tm_months_to_year_month(months_since_1960: i32) -> (i32, u8) {
    decompose_period(months_since_1960, 12)
}

/// Decomposes a `%tq` Stata quarter offset (quarters since 1960q1)
/// into `(year, quarter)` with `quarter` in `1..=4`.
#[must_use]
pub fn tq_quarters_to_year_quarter(quarters_since_1960: i32) -> (i32, u8) {
    decompose_period(quarters_since_1960, 4)
}

/// Decomposes a `%th` Stata half-year offset (halves since 1960h1)
/// into `(year, half)` with `half` in `1..=2`.
#[must_use]
pub fn th_halves_to_year_half(halves_since_1960: i32) -> (i32, u8) {
    decompose_period(halves_since_1960, 2)
}

/// Decomposes a `%tw` Stata week offset (weeks since 1960w1) into
/// `(year, week)` with `week` in `1..=52`.
///
/// Stata defines exactly 52 weeks per year, with week 1 starting on
/// January 1; the last week absorbs the trailing 1–2 days. This is
/// not the ISO week scheme.
#[must_use]
pub fn tw_weeks_to_year_week(weeks_since_1960: i32) -> (i32, u8) {
    decompose_period(weeks_since_1960, 52)
}

/// Shared decomposition for fixed-period-per-year formats.
///
/// `periods_per_year` must be in `1..=255` (so the sub-period fits
/// in `u8`). The result year is the Stata epoch year (1960) plus
/// the floor-divided period count, and the sub-period is the
/// 1-indexed remainder.
///
/// Year overflow is impossible for any `i32` input at the
/// `periods_per_year` values used by Stata: the largest possible
/// year displacement is `i32::MAX / 1` ≈ 2.1 billion, plus 1960,
/// which still fits in `i32`.
fn decompose_period(periods_since_1960: i32, periods_per_year: i32) -> (i32, u8) {
    debug_assert!(
        (1..=255).contains(&periods_per_year),
        "periods_per_year must fit in u8 to support 1-indexed sub-period",
    );
    let year_offset = periods_since_1960.div_euclid(periods_per_year);
    let sub_period_zero_indexed = periods_since_1960.rem_euclid(periods_per_year);
    let year = year_offset.saturating_add(1960);
    // rem_euclid bounds the result to 0..periods_per_year, which
    // fits in u8 by the precondition above.
    let sub_period = u8::try_from(sub_period_zero_indexed)
        .expect("rem_euclid result fits in u8 for periods_per_year <= 255");
    (year, sub_period + 1)
}

#[cfg(test)]
mod tests {
    use super::*;

    // -- Constants -----------------------------------------------------------

    #[test]
    fn epoch_days_constant() {
        // Sanity check: 1960 → 1970 spans 10 years (3650 days) plus
        // three leap days (1960, 1964, 1968).
        assert_eq!(STATA_EPOCH_UNIX_DAYS, -3653);
    }

    #[test]
    fn epoch_millis_matches_days() {
        assert_eq!(
            STATA_EPOCH_UNIX_MILLIS,
            i64::from(STATA_EPOCH_UNIX_DAYS) * 86_400_000,
        );
    }

    // -- td_days_to_unix_days -----------------------------------------------

    #[test]
    fn td_zero_is_stata_epoch() {
        assert_eq!(td_days_to_unix_days(0), Some(STATA_EPOCH_UNIX_DAYS));
    }

    #[test]
    fn td_3653_is_unix_epoch() {
        assert_eq!(td_days_to_unix_days(3653), Some(0));
    }

    #[test]
    fn td_negative_pre_epoch() {
        // 1900-01-01 is 21915 days before 1970-01-01, so 21915 - 3653
        // = 18262 days before 1960-01-01.
        assert_eq!(td_days_to_unix_days(-18262), Some(-21915));
    }

    #[test]
    fn td_overflow_at_extreme_negative() {
        // i32::MIN + (-3653) overflows.
        assert_eq!(td_days_to_unix_days(i32::MIN), None);
        assert_eq!(td_days_to_unix_days(i32::MIN + 3652), None);
        assert_eq!(td_days_to_unix_days(i32::MIN + 3653), Some(i32::MIN));
    }

    #[test]
    fn td_no_overflow_at_extreme_positive() {
        // Adding a negative offset to i32::MAX never overflows.
        assert_eq!(
            td_days_to_unix_days(i32::MAX),
            Some(i32::MAX + STATA_EPOCH_UNIX_DAYS),
        );
    }

    // -- tc_millis_to_unix_millis -------------------------------------------

    #[test]
    fn tc_zero_is_stata_epoch_in_ms() {
        assert_eq!(tc_millis_to_unix_millis(0.0), Some(STATA_EPOCH_UNIX_MILLIS));
    }

    #[test]
    fn tc_one_day_after_epoch() {
        let one_day_ms = 86_400_000.0_f64;
        assert_eq!(
            tc_millis_to_unix_millis(one_day_ms),
            Some(STATA_EPOCH_UNIX_MILLIS + 86_400_000),
        );
    }

    #[test]
    fn tc_3653_days_is_unix_epoch() {
        let unix_epoch_in_stata_ms = 3653.0_f64 * 86_400_000.0;
        assert_eq!(tc_millis_to_unix_millis(unix_epoch_in_stata_ms), Some(0));
    }

    #[test]
    fn tc_rounds_fractional_to_nearest_even() {
        // f64::round rounds halves away from zero, which is what we
        // want for "nearest integer ms".
        assert_eq!(tc_millis_to_unix_millis(0.4), Some(STATA_EPOCH_UNIX_MILLIS));
        assert_eq!(
            tc_millis_to_unix_millis(0.6),
            Some(STATA_EPOCH_UNIX_MILLIS + 1),
        );
    }

    #[test]
    fn tc_nan_is_none() {
        assert_eq!(tc_millis_to_unix_millis(f64::NAN), None);
    }

    #[test]
    fn tc_positive_infinity_is_none() {
        assert_eq!(tc_millis_to_unix_millis(f64::INFINITY), None);
    }

    #[test]
    fn tc_negative_infinity_is_none() {
        assert_eq!(tc_millis_to_unix_millis(f64::NEG_INFINITY), None);
    }

    #[test]
    fn tc_above_i64_range_is_none() {
        // 2^63 ms is just above i64::MAX; anything that lands at or
        // beyond it after the epoch shift must return None.
        let too_big = 9_300_000_000_000_000_000.0_f64;
        assert_eq!(tc_millis_to_unix_millis(too_big), None);
    }

    #[test]
    fn tc_below_i64_range_is_none() {
        let too_small = -9_300_000_000_000_000_000.0_f64;
        assert_eq!(tc_millis_to_unix_millis(too_small), None);
    }

    #[test]
    fn tc_negative_pre_epoch() {
        // One day before the Stata epoch.
        let before_epoch = -86_400_000.0_f64;
        assert_eq!(
            tc_millis_to_unix_millis(before_epoch),
            Some(STATA_EPOCH_UNIX_MILLIS - 86_400_000),
        );
    }

    // -- tm_months_to_year_month --------------------------------------------

    #[test]
    fn tm_zero_is_january_1960() {
        assert_eq!(tm_months_to_year_month(0), (1960, 1));
    }

    #[test]
    fn tm_eleven_is_december_1960() {
        assert_eq!(tm_months_to_year_month(11), (1960, 12));
    }

    #[test]
    fn tm_twelve_is_january_1961() {
        assert_eq!(tm_months_to_year_month(12), (1961, 1));
    }

    #[test]
    fn tm_negative_one_is_december_1959() {
        assert_eq!(tm_months_to_year_month(-1), (1959, 12));
    }

    #[test]
    fn tm_negative_twelve_is_january_1959() {
        assert_eq!(tm_months_to_year_month(-12), (1959, 1));
    }

    #[test]
    fn tm_negative_thirteen_is_december_1958() {
        assert_eq!(tm_months_to_year_month(-13), (1958, 12));
    }

    #[test]
    fn tm_extreme_positive_no_overflow() {
        let (year, month) = tm_months_to_year_month(i32::MAX);
        assert!((1..=12).contains(&month));
        // Year stays within i32 (years per i32 max months ≈ 178M).
        assert!(year > 1960);
    }

    #[test]
    fn tm_extreme_negative_no_overflow() {
        let (year, month) = tm_months_to_year_month(i32::MIN);
        assert!((1..=12).contains(&month));
        assert!(year < 1960);
    }

    // -- tq_quarters_to_year_quarter ----------------------------------------

    #[test]
    fn tq_zero_is_q1_1960() {
        assert_eq!(tq_quarters_to_year_quarter(0), (1960, 1));
    }

    #[test]
    fn tq_three_is_q4_1960() {
        assert_eq!(tq_quarters_to_year_quarter(3), (1960, 4));
    }

    #[test]
    fn tq_four_is_q1_1961() {
        assert_eq!(tq_quarters_to_year_quarter(4), (1961, 1));
    }

    #[test]
    fn tq_negative_one_is_q4_1959() {
        assert_eq!(tq_quarters_to_year_quarter(-1), (1959, 4));
    }

    #[test]
    fn tq_negative_four_is_q1_1959() {
        assert_eq!(tq_quarters_to_year_quarter(-4), (1959, 1));
    }

    // -- th_halves_to_year_half ---------------------------------------------

    #[test]
    fn th_zero_is_h1_1960() {
        assert_eq!(th_halves_to_year_half(0), (1960, 1));
    }

    #[test]
    fn th_one_is_h2_1960() {
        assert_eq!(th_halves_to_year_half(1), (1960, 2));
    }

    #[test]
    fn th_two_is_h1_1961() {
        assert_eq!(th_halves_to_year_half(2), (1961, 1));
    }

    #[test]
    fn th_negative_one_is_h2_1959() {
        assert_eq!(th_halves_to_year_half(-1), (1959, 2));
    }

    // -- tw_weeks_to_year_week ----------------------------------------------

    #[test]
    fn tw_zero_is_week1_1960() {
        assert_eq!(tw_weeks_to_year_week(0), (1960, 1));
    }

    #[test]
    fn tw_fifty_one_is_week52_1960() {
        assert_eq!(tw_weeks_to_year_week(51), (1960, 52));
    }

    #[test]
    fn tw_fifty_two_is_week1_1961() {
        assert_eq!(tw_weeks_to_year_week(52), (1961, 1));
    }

    #[test]
    fn tw_negative_one_is_week52_1959() {
        assert_eq!(tw_weeks_to_year_week(-1), (1959, 52));
    }

    #[test]
    fn tw_negative_fifty_two_is_week1_1959() {
        assert_eq!(tw_weeks_to_year_week(-52), (1959, 1));
    }
}
