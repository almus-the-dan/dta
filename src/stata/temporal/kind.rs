//! Classification of a Stata display format string into a temporal
//! category (or `None` for non-temporal formats).

/// The semantic category of a Stata temporal display format.
///
/// Stata variables don't carry an explicit "this is a date" flag —
/// the temporal meaning is inferred entirely from the variable's
/// display format (the `%fmt` field). This enum captures the eight
/// recognized temporal prefixes.
///
/// Use [`TemporalKind::from_format`] to classify a format string
/// from a DTA file. Returns `None` for the much more common case of
/// non-temporal formats (numeric `%9.0g`, string `%-12s`, etc.).
///
/// # Display suffixes
///
/// Stata format strings often carry display suffixes after the
/// prefix (e.g., `%tdCCYY-NN-DD`, `%tcDDmonCCYY_HH:MM:SS`). Those
/// suffixes only affect rendering — they don't change the
/// underlying numeric meaning. The parser ignores everything after
/// the prefix.
///
/// # Case sensitivity
///
/// `%tc` and `%tC` are *different* formats: lowercase ignores leap
/// seconds, uppercase accounts for them. Matching is therefore
/// case-sensitive and the two map to distinct variants.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum TemporalKind {
    /// `%td` — date, stored as days since 1960-01-01.
    ///
    /// Also matched for the legacy pre-Stata-10 `%d` format, which
    /// is semantically identical.
    Date,
    /// `%tc` — datetime, stored as milliseconds since
    /// 1960-01-01T00:00:00, **without** leap-second adjustment.
    DateTime,
    /// `%tC` — datetime with leap-second adjustment.
    ///
    /// Recognized by the parser but explicitly distinguished from
    /// [`DateTime`](Self::DateTime); time-crate adapters that don't
    /// model leap seconds are expected to refuse this variant
    /// rather than silently treat it as `%tc`.
    DateTimeLeap,
    /// `%tw` — week, stored as weeks since 1960w1.
    ///
    /// Stata weeks are not ISO weeks: week 1 of any year always
    /// starts on January 1, and every year has exactly 52 weeks
    /// (the last week is 8 or 9 days long).
    Week,
    /// `%tm` — month, stored as months since 1960m1.
    Month,
    /// `%tq` — quarter, stored as quarters since 1960q1.
    Quarter,
    /// `%th` — half-year, stored as halves since 1960h1.
    HalfYear,
    /// `%ty` — calendar year (the value *is* the year, e.g. `2026`).
    ///
    /// Unlike the other temporal formats, this is not an offset from
    /// 1960; the underlying numeric value is read directly.
    Year,
}

impl TemporalKind {
    /// Classifies a Stata display format string.
    ///
    /// Returns `Some(kind)` if the string starts with `%` followed
    /// by a recognized temporal prefix, or `None` otherwise (which
    /// covers all non-temporal formats and malformed inputs).
    ///
    /// Recognized prefixes:
    ///
    /// | Prefix | Variant |
    /// |--------|---------|
    /// | `%td`  | [`Date`](Self::Date) |
    /// | `%d`   | [`Date`](Self::Date) (legacy alias) |
    /// | `%tc`  | [`DateTime`](Self::DateTime) |
    /// | `%tC`  | [`DateTimeLeap`](Self::DateTimeLeap) |
    /// | `%tw`  | [`Week`](Self::Week) |
    /// | `%tm`  | [`Month`](Self::Month) |
    /// | `%tq`  | [`Quarter`](Self::Quarter) |
    /// | `%th`  | [`HalfYear`](Self::HalfYear) |
    /// | `%ty`  | [`Year`](Self::Year) |
    ///
    /// Any characters following a recognized prefix are ignored, so
    /// `%tdCCYY-NN-DD` and `%tcHH:MM:SS` classify the same as the
    /// bare prefix.
    ///
    /// Matching is case-sensitive — `%tc` and `%tC` produce
    /// different variants, and `%TD` returns `None`.
    ///
    /// # Examples
    ///
    /// ```
    /// use dta::stata::temporal::TemporalKind;
    ///
    /// assert_eq!(TemporalKind::from_format("%td"), Some(TemporalKind::Date));
    /// assert_eq!(TemporalKind::from_format("%tdCCYY-NN-DD"), Some(TemporalKind::Date));
    /// assert_eq!(TemporalKind::from_format("%d"), Some(TemporalKind::Date));
    /// assert_eq!(TemporalKind::from_format("%tC"), Some(TemporalKind::DateTimeLeap));
    /// assert_eq!(TemporalKind::from_format("%9.0g"), None);
    /// assert_eq!(TemporalKind::from_format(""), None);
    /// ```
    #[must_use]
    pub fn from_format(format: &str) -> Option<Self> {
        let body = format.strip_prefix('%')?;
        let mut chars = body.chars();
        let first = chars.next()?;
        match first {
            'd' => Some(Self::Date),
            't' => match chars.next()? {
                'd' => Some(Self::Date),
                'c' => Some(Self::DateTime),
                'C' => Some(Self::DateTimeLeap),
                'w' => Some(Self::Week),
                'm' => Some(Self::Month),
                'q' => Some(Self::Quarter),
                'h' => Some(Self::HalfYear),
                'y' => Some(Self::Year),
                _ => None,
            },
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // -- Bare prefixes -------------------------------------------------------

    #[test]
    fn bare_td_is_date() {
        assert_eq!(TemporalKind::from_format("%td"), Some(TemporalKind::Date));
    }

    #[test]
    fn bare_tc_is_datetime() {
        assert_eq!(
            TemporalKind::from_format("%tc"),
            Some(TemporalKind::DateTime)
        );
    }

    #[test]
    fn bare_tc_uppercase_is_datetime_leap() {
        assert_eq!(
            TemporalKind::from_format("%tC"),
            Some(TemporalKind::DateTimeLeap)
        );
    }

    #[test]
    fn bare_tw_is_week() {
        assert_eq!(TemporalKind::from_format("%tw"), Some(TemporalKind::Week));
    }

    #[test]
    fn bare_tm_is_month() {
        assert_eq!(TemporalKind::from_format("%tm"), Some(TemporalKind::Month));
    }

    #[test]
    fn bare_tq_is_quarter() {
        assert_eq!(
            TemporalKind::from_format("%tq"),
            Some(TemporalKind::Quarter)
        );
    }

    #[test]
    fn bare_th_is_half_year() {
        assert_eq!(
            TemporalKind::from_format("%th"),
            Some(TemporalKind::HalfYear)
        );
    }

    #[test]
    fn bare_ty_is_year() {
        assert_eq!(TemporalKind::from_format("%ty"), Some(TemporalKind::Year));
    }

    // -- Legacy alias --------------------------------------------------------

    #[test]
    fn bare_d_is_legacy_date() {
        assert_eq!(TemporalKind::from_format("%d"), Some(TemporalKind::Date));
    }

    #[test]
    fn legacy_d_with_suffix() {
        assert_eq!(
            TemporalKind::from_format("%dCCYY-NN-DD"),
            Some(TemporalKind::Date)
        );
    }

    // -- Suffixes ignored ----------------------------------------------------

    #[test]
    fn td_with_iso_suffix() {
        assert_eq!(
            TemporalKind::from_format("%tdCCYY-NN-DD"),
            Some(TemporalKind::Date)
        );
    }

    #[test]
    fn td_with_us_suffix() {
        assert_eq!(
            TemporalKind::from_format("%tdNN/DD/CCYY"),
            Some(TemporalKind::Date)
        );
    }

    #[test]
    fn tc_with_time_suffix() {
        assert_eq!(
            TemporalKind::from_format("%tcDDmonCCYY_HH:MM:SS"),
            Some(TemporalKind::DateTime)
        );
    }

    #[test]
    fn tc_uppercase_with_suffix_stays_leap() {
        assert_eq!(
            TemporalKind::from_format("%tCHH:MM:SS"),
            Some(TemporalKind::DateTimeLeap)
        );
    }

    #[test]
    fn arbitrary_suffix_chars_ignored() {
        // The parser does not validate suffix syntax — anything goes.
        assert_eq!(
            TemporalKind::from_format("%tdgarbage!@#"),
            Some(TemporalKind::Date)
        );
    }

    // -- Non-temporal formats ------------------------------------------------

    #[test]
    fn numeric_format_is_none() {
        assert_eq!(TemporalKind::from_format("%9.0g"), None);
    }

    #[test]
    fn numeric_format_with_commas_is_none() {
        assert_eq!(TemporalKind::from_format("%9.0gc"), None);
    }

    #[test]
    fn fixed_format_is_none() {
        assert_eq!(TemporalKind::from_format("%8.2f"), None);
    }

    #[test]
    fn exponential_format_is_none() {
        assert_eq!(TemporalKind::from_format("%-12.4e"), None);
    }

    #[test]
    fn string_format_is_none() {
        assert_eq!(TemporalKind::from_format("%9s"), None);
    }

    #[test]
    fn left_aligned_string_format_is_none() {
        assert_eq!(TemporalKind::from_format("%-12s"), None);
    }

    // -- Malformed inputs ----------------------------------------------------

    #[test]
    fn empty_string_is_none() {
        assert_eq!(TemporalKind::from_format(""), None);
    }

    #[test]
    fn missing_percent_is_none() {
        assert_eq!(TemporalKind::from_format("td"), None);
    }

    #[test]
    fn just_percent_is_none() {
        assert_eq!(TemporalKind::from_format("%"), None);
    }

    #[test]
    fn just_percent_t_is_none() {
        assert_eq!(TemporalKind::from_format("%t"), None);
    }

    #[test]
    fn unknown_t_subprefix_is_none() {
        assert_eq!(TemporalKind::from_format("%tx"), None);
    }

    #[test]
    fn leading_whitespace_is_none() {
        // Format strings come from a fixed-width null-padded field
        // already trimmed by the reader; a leading space indicates
        // garbage, not a date.
        assert_eq!(TemporalKind::from_format(" %td"), None);
    }

    // -- Case sensitivity ----------------------------------------------------

    #[test]
    fn uppercase_outer_prefix_is_none() {
        // %TD is not a valid Stata format.
        assert_eq!(TemporalKind::from_format("%TD"), None);
    }

    #[test]
    fn uppercase_d_is_none() {
        // %D would be uppercase legacy-date; Stata doesn't define it.
        assert_eq!(TemporalKind::from_format("%D"), None);
    }

    #[test]
    fn tc_and_tc_leap_are_distinct() {
        assert_ne!(
            TemporalKind::from_format("%tc"),
            TemporalKind::from_format("%tC"),
        );
    }
}
