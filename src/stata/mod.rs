/// DTA file format reader.
pub mod dta;
/// Stata missing value representation.
pub mod missing_value;
/// Error type for values that are not Stata missing values.
pub mod not_missing_value_error;
/// Stata byte value (1-byte signed integer or missing).
pub mod stata_byte;
/// Stata double value (8-byte IEEE 754 float or missing).
pub mod stata_double;
/// Stata float value (4-byte IEEE 754 float or missing).
pub mod stata_float;
/// Stata int value (2-byte signed integer or missing).
pub mod stata_int;
/// Stata long value (4-byte signed integer or missing).
pub mod stata_long;
