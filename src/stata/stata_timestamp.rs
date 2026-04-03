use core::fmt;

use super::stata_error::Result;

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
    pub fn parse(_s: &str) -> Result<Self> {
        todo!()
    }

    /// Day of the month (1–31).
    #[must_use]
    pub fn day(&self) -> u8 {
        self.day
    }

    /// Month of the year (1–12).
    #[must_use]
    pub fn month(&self) -> u8 {
        self.month
    }

    /// Four-digit year.
    #[must_use]
    pub fn year(&self) -> u16 {
        self.year
    }

    /// Hour (0–23).
    #[must_use]
    pub fn hour(&self) -> u8 {
        self.hour
    }

    /// Minute (0–59).
    #[must_use]
    pub fn minute(&self) -> u8 {
        self.minute
    }
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
