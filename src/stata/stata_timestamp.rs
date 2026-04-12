use core::fmt;

use super::stata_error::{Result, StataError};

/// A parsed timestamp from a DTA file header.
///
/// DTA files encode timestamps as fixed-length strings in the format
/// `"dd Mon yyyy hh:mm"` (e.g. `"01 Jan 2024 13:45"`). This struct
/// stores the parsed components and can reconstruct the string via
/// [`Display`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StataTimestamp {
    day: u8,
    month: u8,
    year: u16,
    hour: u8,
    minute: u8,
}

impl StataTimestamp {
    /// Parses a DTA timestamp string in the format `"dd Mon yyyy hh:mm"`.
    ///
    /// Accepts the same month abbreviations as `ReadStat`, including
    /// localized variants (e.g. `"Ene"` for January in Spanish).
    /// Leading/trailing whitespace and extra spaces between the year
    /// and time are tolerated.
    ///
    /// # Errors
    ///
    /// Returns [`StataError::InvalidTimestamp`] when the string does
    /// not match the expected format.
    pub fn parse(s: &str) -> Result<Self> {
        // Split on whitespace: ["dd", "Mon", "yyyy", "hh:mm"]
        let mut parts = s.split_whitespace();

        let day = parts.next().ok_or(StataError::InvalidTimestamp)?;
        let day: u8 = parse_day(day)?;

        let month = parts.next().ok_or(StataError::InvalidTimestamp)?;
        let month = parse_month(month)?;

        let year = parts.next().ok_or(StataError::InvalidTimestamp)?;
        let year: u16 = next_int(year)?;

        let time = parts.next().ok_or(StataError::InvalidTimestamp)?;
        let (hour, minute) = parse_time(time)?;

        // Ensure there's no trailing garbage
        if parts.next().is_some() {
            return Err(StataError::InvalidTimestamp);
        }

        Ok(Self {
            day,
            month,
            year,
            hour,
            minute,
        })
    }

    /// Day of the month (1–31).
    #[must_use]
    #[inline]
    pub fn day(&self) -> u8 {
        self.day
    }

    /// Month of the year (1–12).
    #[must_use]
    #[inline]
    pub fn month(&self) -> u8 {
        self.month
    }

    /// Four-digit year.
    #[must_use]
    #[inline]
    pub fn year(&self) -> u16 {
        self.year
    }

    /// Hour (0–23).
    #[must_use]
    #[inline]
    pub fn hour(&self) -> u8 {
        self.hour
    }

    /// Minute (0–59).
    #[must_use]
    #[inline]
    pub fn minute(&self) -> u8 {
        self.minute
    }
}

/// Parses a three-letter month abbreviation (case-insensitive).
///
/// Supports English and the localized variants that appear in
/// `ReadStat`'s Ragel grammar: Ene, Abr, Mai, Ago, Okt, Dez, Dic.
fn parse_month(s: &str) -> Result<u8> {
    let bytes = s.as_bytes();
    if bytes.len() != 3 {
        return Err(StataError::InvalidTimestamp);
    }
    let lower = [
        bytes[0].to_ascii_lowercase(),
        bytes[1].to_ascii_lowercase(),
        bytes[2].to_ascii_lowercase(),
    ];
    match &lower {
        b"jan" | b"ene" => Ok(1),
        b"feb" => Ok(2),
        b"mar" => Ok(3),
        b"apr" | b"abr" => Ok(4),
        b"may" | b"mai" => Ok(5),
        b"jun" => Ok(6),
        b"jul" => Ok(7),
        b"aug" | b"ago" => Ok(8),
        b"sep" => Ok(9),
        b"oct" | b"okt" => Ok(10),
        b"nov" => Ok(11),
        b"dec" | b"dez" | b"dic" => Ok(12),
        _ => Err(StataError::InvalidTimestamp),
    }
}

/// Parses `"hh:mm"` into (hour, minute).
fn parse_time(s: &str) -> Result<(u8, u8)> {
    let (h, m) = s.split_once(':').ok_or(StataError::InvalidTimestamp)?;
    let hour: u8 = h.parse().map_err(|_| StataError::InvalidTimestamp)?;
    let minute: u8 = m.parse().map_err(|_| StataError::InvalidTimestamp)?;
    if hour > 23 || minute > 59 {
        return Err(StataError::InvalidTimestamp);
    }
    Ok((hour, minute))
}

/// Parses "dd" into a day between 1 and 31. There is no protection
/// against invalid dates, like February 31st.
fn parse_day(value: &str) -> Result<u8> {
    let day: u8 = next_int(value)?;
    if !(1..=31).contains(&day) {
        return Err(StataError::InvalidTimestamp);
    }
    Ok(day)
}

/// Parses the next whitespace-delimited token as an integer.
fn next_int<T: core::str::FromStr>(value: &str) -> Result<T> {
    value.parse().map_err(|_| StataError::InvalidTimestamp)
}

impl fmt::Display for StataTimestamp {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        const MONTHS: [&str; 12] = [
            "Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec",
        ];
        let month_name = MONTHS
            .get((self.month.wrapping_sub(1)) as usize)
            .unwrap_or(&"???");
        write!(
            f,
            "{:02} {} {:04} {:02}:{:02}",
            self.day, month_name, self.year, self.hour, self.minute
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::stata::stata_error::StataError;

    #[test]
    fn parse_typical() {
        let ts = StataTimestamp::parse("01 Jan 2024 13:45").unwrap();
        assert_eq!(ts.day(), 1);
        assert_eq!(ts.month(), 1);
        assert_eq!(ts.year(), 2024);
        assert_eq!(ts.hour(), 13);
        assert_eq!(ts.minute(), 45);
    }

    #[test]
    fn parse_single_digit_day() {
        let ts = StataTimestamp::parse("5 Mar 2023 09:00").unwrap();
        assert_eq!(ts.day(), 5);
        assert_eq!(ts.month(), 3);
    }

    #[test]
    fn parse_leading_space() {
        let ts = StataTimestamp::parse(" 5 Jan 2024 14:30").unwrap();
        assert_eq!(ts.day(), 5);
        assert_eq!(ts.hour(), 14);
        assert_eq!(ts.minute(), 30);
    }

    #[test]
    fn parse_extra_spaces_before_time() {
        let ts = StataTimestamp::parse("12 Dec 2023  09:00").unwrap();
        assert_eq!(ts.day(), 12);
        assert_eq!(ts.month(), 12);
        assert_eq!(ts.hour(), 9);
    }

    #[test]
    fn parse_case_insensitive_month() {
        let ts = StataTimestamp::parse("15 JAN 2020 00:00").unwrap();
        assert_eq!(ts.month(), 1);
        let ts = StataTimestamp::parse("15 jan 2020 00:00").unwrap();
        assert_eq!(ts.month(), 1);
    }

    #[test]
    fn parse_localised_months() {
        // Spanish
        assert_eq!(
            StataTimestamp::parse("01 Ene 2020 00:00").unwrap().month(),
            1
        );
        assert_eq!(
            StataTimestamp::parse("01 Abr 2020 00:00").unwrap().month(),
            4
        );
        assert_eq!(
            StataTimestamp::parse("01 Ago 2020 00:00").unwrap().month(),
            8
        );
        assert_eq!(
            StataTimestamp::parse("01 Dic 2020 00:00").unwrap().month(),
            12
        );
        // German
        assert_eq!(
            StataTimestamp::parse("01 Okt 2020 00:00").unwrap().month(),
            10
        );
        assert_eq!(
            StataTimestamp::parse("01 Dez 2020 00:00").unwrap().month(),
            12
        );
        // French/Portuguese
        assert_eq!(
            StataTimestamp::parse("01 Mai 2020 00:00").unwrap().month(),
            5
        );
    }

    #[test]
    fn parse_roundtrip_through_display() {
        let ts = StataTimestamp::parse("07 Sep 2019 23:59").unwrap();
        let formatted = ts.to_string();
        let ts2 = StataTimestamp::parse(&formatted).unwrap();
        assert_eq!(ts, ts2);
    }

    #[test]
    fn parse_empty_string() {
        assert_eq!(StataTimestamp::parse(""), Err(StataError::InvalidTimestamp),);
    }

    #[test]
    fn parse_missing_time() {
        assert_eq!(
            StataTimestamp::parse("01 Jan 2024"),
            Err(StataError::InvalidTimestamp),
        );
    }

    #[test]
    fn parse_extra_token() {
        assert_eq!(
            StataTimestamp::parse("01 Jan 2024 13:45 extra"),
            Err(StataError::InvalidTimestamp),
        );
    }

    #[test]
    fn parse_invalid_month() {
        assert_eq!(
            StataTimestamp::parse("01 Xyz 2024 13:45"),
            Err(StataError::InvalidTimestamp),
        );
    }

    #[test]
    fn parse_day_zero() {
        assert_eq!(
            StataTimestamp::parse("00 Jan 2024 13:45"),
            Err(StataError::InvalidTimestamp),
        );
    }

    #[test]
    fn parse_hour_24() {
        assert_eq!(
            StataTimestamp::parse("01 Jan 2024 24:00"),
            Err(StataError::InvalidTimestamp),
        );
    }

    #[test]
    fn parse_minute_60() {
        assert_eq!(
            StataTimestamp::parse("01 Jan 2024 13:60"),
            Err(StataError::InvalidTimestamp),
        );
    }

    #[test]
    fn parse_bad_time_separator() {
        assert_eq!(
            StataTimestamp::parse("01 Jan 2024 13-45"),
            Err(StataError::InvalidTimestamp),
        );
    }
}
