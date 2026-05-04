/// The numeric sub-format implied by a fixed-width numeric `%infmt`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NumericStyle {
    /// Fixed-point notation (`%w.df`).
    Fixed,
    /// General notation (`%w.dg`) — Stata's default for numerics.
    General,
    /// Scientific notation (`%w.de`).
    Scientific,
}
