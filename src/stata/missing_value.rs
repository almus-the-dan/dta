/// Represents a Stata missing value.
///
/// Stata supports 27 distinct missing values: the *system* missing value (`.`) and
/// 26 *extended* (or "tagged") missing values (`.a` through `.z`).
///
/// In DTA files these are encoded differently depending on the storage type:
///
/// | Stata type | Rust type | `.` value | `.a` value | stride |
/// |------------|-----------|-----------|------------|--------|
/// | byte       | `u8`      | `0x65`    | `0x66`     | 1      |
/// | int        | `u16`     | `0x7FE5`  | `0x7FE6`   | 1      |
/// | long       | `u32`     | `0x7FFF_FFE5` | `0x7FFF_FFE6` | 1 |
/// | float      | `f32`     | `0x7F00_0000`† | `0x7F00_0800`† | `0x0800` |
/// | double     | `f64`     | `0x7FE0_0000_0000_0000`† | `0x7FE0_0100_0000_0000`† | `0x0100_0000_0000` |
///
/// † = IEEE 754 bit pattern interpreted as an integer.
///
/// These encodings match the DTA format 113+ constants used by
/// [ReadStat](https://github.com/WizardMac/ReadStat).
///
/// # Examples
///
/// ```
/// use dta::stata::missing_value::MissingValue;
///
/// // Default is the system missing value
/// let mv = MissingValue::default();
/// assert_eq!(mv.to_string(), ".");
///
/// // Parse from a raw byte value
/// let mv = MissingValue::try_from(0x66_u8).unwrap();
/// assert_eq!(mv, MissingValue::A);
/// assert_eq!(mv.to_string(), ".a");
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum MissingValue {
    /// The system missing value, displayed as `.`.
    System,
    /// Extended missing `.a`.
    A,
    /// Extended missing `.b`.
    B,
    /// Extended missing `.c`.
    C,
    /// Extended missing `.d`.
    D,
    /// Extended missing `.e`.
    E,
    /// Extended missing `.f`.
    F,
    /// Extended missing `.g`.
    G,
    /// Extended missing `.h`.
    H,
    /// Extended missing `.i`.
    I,
    /// Extended missing `.j`.
    J,
    /// Extended missing `.k`.
    K,
    /// Extended missing `.l`.
    L,
    /// Extended missing `.m`.
    M,
    /// Extended missing `.n`.
    N,
    /// Extended missing `.o`.
    O,
    /// Extended missing `.p`.
    P,
    /// Extended missing `.q`.
    Q,
    /// Extended missing `.r`.
    R,
    /// Extended missing `.s`.
    S,
    /// Extended missing `.t`.
    T,
    /// Extended missing `.u`.
    U,
    /// Extended missing `.v`.
    V,
    /// Extended missing `.w`.
    W,
    /// Extended missing `.x`.
    X,
    /// Extended missing `.y`.
    Y,
    /// Extended missing `.z`.
    Z,
}

impl MissingValue {
    /// Returns the numeric offset of this variant: `System` = 0, `A` = 1, …, `Z` = 26.
    ///
    /// This is the enum discriminant and is used internally when encoding
    /// missing values back into their raw DTA representation.
    #[must_use]
    pub const fn code(self) -> u8 {
        self as u8
    }
}

impl Default for MissingValue {
    /// Returns [`MissingValue::System`].
    fn default() -> Self {
        Self::System
    }
}

impl core::fmt::Display for MissingValue {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::System => f.write_str("."),
            other => {
                let letter = b'a' + other.code() - 1;
                write!(f, ".{}", letter as char)
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Constants for DTA format 113+ missing-value encoding
// ---------------------------------------------------------------------------

// Byte (Stata "byte", stored as signed i8 but read as u8)
const MISSING_BYTE_SYSTEM: u8 = 0x65; // 101
const MISSING_BYTE_Z: u8 = 0x7F; // 127

// Int (Stata "int", stored as signed i16 but read as u16)
const MISSING_INT_SYSTEM: u16 = 0x7FE5; // 32_741
const MISSING_INT_Z: u16 = 0x7FFF; // 32_767

// Long (Stata "long", stored as signed i32 but read as u32)
const MISSING_LONG_SYSTEM: u32 = 0x7FFF_FFE5; // 2_147_483_621
const MISSING_LONG_Z: u32 = 0x7FFF_FFFF; // 2_147_483_647

// Float (Stata "float", IEEE 754 f32 — values expressed as bit patterns)
const MISSING_FLOAT_SYSTEM: u32 = 0x7F00_0000;
const MISSING_FLOAT_A: u32 = 0x7F00_0800;
const MISSING_FLOAT_STRIDE: u32 = 0x0800;

// Double (Stata "double", IEEE 754 f64 — values expressed as bit patterns)
const MISSING_DOUBLE_SYSTEM: u64 = 0x7FE0_0000_0000_0000;
const MISSING_DOUBLE_A: u64 = 0x7FE0_0100_0000_0000;
const MISSING_DOUBLE_STRIDE: u64 = 0x0100_0000_0000;

use super::stata_error::{Result, StataError};

// ---------------------------------------------------------------------------
// Helper: convert an offset (0 = System, 1 = A, …, 26 = Z) to a variant
// ---------------------------------------------------------------------------

/// Convert an offset from the system missing value to a [`MissingValue`].
///
/// Returns `Ok(MissingValue::System)` for `offset == 0`, `Ok(MissingValue::A)` for
/// `offset == 1`, and so on up to `Ok(MissingValue::Z)` for `offset == 26`.
/// Returns `Err(StataError::NotMissingValue)` for any other offset.
fn from_code(offset: u32) -> Result<MissingValue> {
    match offset {
        0 => Ok(MissingValue::System),
        1 => Ok(MissingValue::A),
        2 => Ok(MissingValue::B),
        3 => Ok(MissingValue::C),
        4 => Ok(MissingValue::D),
        5 => Ok(MissingValue::E),
        6 => Ok(MissingValue::F),
        7 => Ok(MissingValue::G),
        8 => Ok(MissingValue::H),
        9 => Ok(MissingValue::I),
        10 => Ok(MissingValue::J),
        11 => Ok(MissingValue::K),
        12 => Ok(MissingValue::L),
        13 => Ok(MissingValue::M),
        14 => Ok(MissingValue::N),
        15 => Ok(MissingValue::O),
        16 => Ok(MissingValue::P),
        17 => Ok(MissingValue::Q),
        18 => Ok(MissingValue::R),
        19 => Ok(MissingValue::S),
        20 => Ok(MissingValue::T),
        21 => Ok(MissingValue::U),
        22 => Ok(MissingValue::V),
        23 => Ok(MissingValue::W),
        24 => Ok(MissingValue::X),
        25 => Ok(MissingValue::Y),
        26 => Ok(MissingValue::Z),
        _ => Err(StataError::NotMissingValue),
    }
}

// ---------------------------------------------------------------------------
// TryFrom implementations
// ---------------------------------------------------------------------------

/// Interpret a raw byte as a Stata "byte" missing value (DTA 113+).
///
/// Valid missing byte values are `0x65` (`.`) through `0x7F` (`.z`).
impl TryFrom<u8> for MissingValue {
    type Error = StataError;

    fn try_from(value: u8) -> Result<Self> {
        if !(MISSING_BYTE_SYSTEM..=MISSING_BYTE_Z).contains(&value) {
            return Err(StataError::NotMissingValue);
        }
        let remainder = u32::from(value - MISSING_BYTE_SYSTEM);
        from_code(remainder)
    }
}

/// Interpret a raw 16-bit value as a Stata "int" missing value (DTA 113+).
///
/// Valid missing int values are `0x7FE5` (`.`) through `0x7FFF` (`.z`).
impl TryFrom<u16> for MissingValue {
    type Error = StataError;

    fn try_from(value: u16) -> Result<Self> {
        if !(MISSING_INT_SYSTEM..=MISSING_INT_Z).contains(&value) {
            return Err(StataError::NotMissingValue);
        }
        let remainder = u32::from(value - MISSING_INT_SYSTEM);
        from_code(remainder)
    }
}

/// Interpret a raw 32-bit value as a Stata "long" missing value (DTA 113+).
///
/// Valid missing long values are `0x7FFF_FFE5` (`.`) through `0x7FFF_FFFF` (`.z`).
impl TryFrom<u32> for MissingValue {
    type Error = StataError;

    fn try_from(value: u32) -> Result<Self> {
        if !(MISSING_LONG_SYSTEM..=MISSING_LONG_Z).contains(&value) {
            return Err(StataError::NotMissingValue);
        }
        let remainder = value - MISSING_LONG_SYSTEM;
        from_code(remainder)
    }
}

/// Interpret an `f32` as a Stata "float" missing value (DTA 113+).
///
/// Stata encodes float missing values as specific NaN bit patterns:
/// - `.`  → `0x7F00_0000`
/// - `.a` → `0x7F00_0800`
/// - Each subsequent letter adds a stride of `0x0800`
impl TryFrom<f32> for MissingValue {
    type Error = StataError;

    fn try_from(value: f32) -> Result<Self> {
        let bits = value.to_bits();
        if bits == MISSING_FLOAT_SYSTEM {
            return Ok(MissingValue::System);
        }
        if bits < MISSING_FLOAT_A {
            return Err(StataError::NotMissingValue);
        }
        let offset_raw = bits - MISSING_FLOAT_A;
        if !offset_raw.is_multiple_of(MISSING_FLOAT_STRIDE) {
            return Err(StataError::NotMissingValue);
        }
        let offset = offset_raw / MISSING_FLOAT_STRIDE;
        // offset 0 = .a, so add 1 for from_offset
        from_code(offset + 1)
    }
}

/// Interpret an `f64` as a Stata "double" missing value (DTA 113+).
///
/// Stata encodes double missing values as specific NaN bit patterns:
/// - `.`  → `0x7FE0_0000_0000_0000`
/// - `.a` → `0x7FE0_0100_0000_0000`
/// - Each subsequent letter adds a stride of `0x0100_0000_0000`
impl TryFrom<f64> for MissingValue {
    type Error = StataError;

    fn try_from(value: f64) -> Result<Self> {
        let bits = value.to_bits();
        if bits == MISSING_DOUBLE_SYSTEM {
            return Ok(MissingValue::System);
        }
        if bits < MISSING_DOUBLE_A {
            return Err(StataError::NotMissingValue);
        }
        let offset_raw = bits - MISSING_DOUBLE_A;
        if !offset_raw.is_multiple_of(MISSING_DOUBLE_STRIDE) {
            return Err(StataError::NotMissingValue);
        }
        let offset = offset_raw / MISSING_DOUBLE_STRIDE;
        // offset 0 = .a, so convert to u32 and add 1
        let offset = u32::try_from(offset).map_err(|_| StataError::NotMissingValue)?;
        from_code(offset + 1)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // -----------------------------------------------------------------------
    // Display
    // -----------------------------------------------------------------------

    #[test]
    fn display_system() {
        assert_eq!(MissingValue::System.to_string(), ".");
    }

    #[test]
    fn display_a() {
        assert_eq!(MissingValue::A.to_string(), ".a");
    }

    #[test]
    fn display_b() {
        assert_eq!(MissingValue::B.to_string(), ".b");
    }

    #[test]
    fn display_c() {
        assert_eq!(MissingValue::C.to_string(), ".c");
    }

    #[test]
    fn display_d() {
        assert_eq!(MissingValue::D.to_string(), ".d");
    }

    #[test]
    fn display_e() {
        assert_eq!(MissingValue::E.to_string(), ".e");
    }

    #[test]
    fn display_f() {
        assert_eq!(MissingValue::F.to_string(), ".f");
    }

    #[test]
    fn display_g() {
        assert_eq!(MissingValue::G.to_string(), ".g");
    }

    #[test]
    fn display_h() {
        assert_eq!(MissingValue::H.to_string(), ".h");
    }

    #[test]
    fn display_i() {
        assert_eq!(MissingValue::I.to_string(), ".i");
    }

    #[test]
    fn display_j() {
        assert_eq!(MissingValue::J.to_string(), ".j");
    }

    #[test]
    fn display_k() {
        assert_eq!(MissingValue::K.to_string(), ".k");
    }

    #[test]
    fn display_l() {
        assert_eq!(MissingValue::L.to_string(), ".l");
    }

    #[test]
    fn display_m() {
        assert_eq!(MissingValue::M.to_string(), ".m");
    }

    #[test]
    fn display_n() {
        assert_eq!(MissingValue::N.to_string(), ".n");
    }

    #[test]
    fn display_o() {
        assert_eq!(MissingValue::O.to_string(), ".o");
    }

    #[test]
    fn display_p() {
        assert_eq!(MissingValue::P.to_string(), ".p");
    }

    #[test]
    fn display_q() {
        assert_eq!(MissingValue::Q.to_string(), ".q");
    }

    #[test]
    fn display_r() {
        assert_eq!(MissingValue::R.to_string(), ".r");
    }

    #[test]
    fn display_s() {
        assert_eq!(MissingValue::S.to_string(), ".s");
    }

    #[test]
    fn display_t() {
        assert_eq!(MissingValue::T.to_string(), ".t");
    }

    #[test]
    fn display_u() {
        assert_eq!(MissingValue::U.to_string(), ".u");
    }

    #[test]
    fn display_v() {
        assert_eq!(MissingValue::V.to_string(), ".v");
    }

    #[test]
    fn display_w() {
        assert_eq!(MissingValue::W.to_string(), ".w");
    }

    #[test]
    fn display_x() {
        assert_eq!(MissingValue::X.to_string(), ".x");
    }

    #[test]
    fn display_y() {
        assert_eq!(MissingValue::Y.to_string(), ".y");
    }

    #[test]
    fn display_z() {
        assert_eq!(MissingValue::Z.to_string(), ".z");
    }

    // -----------------------------------------------------------------------
    // Default
    // -----------------------------------------------------------------------

    #[test]
    fn default_is_system() {
        assert_eq!(MissingValue::default(), MissingValue::System);
    }

    // -----------------------------------------------------------------------
    // TryFrom<u8> — Stata "byte"
    // -----------------------------------------------------------------------

    #[test]
    fn byte_system() {
        assert_eq!(
            MissingValue::try_from(0x65_u8).unwrap(),
            MissingValue::System
        );
    }

    #[test]
    fn byte_a() {
        assert_eq!(MissingValue::try_from(0x66_u8).unwrap(), MissingValue::A);
    }

    #[test]
    fn byte_b() {
        assert_eq!(MissingValue::try_from(0x67_u8).unwrap(), MissingValue::B);
    }

    #[test]
    fn byte_c() {
        assert_eq!(MissingValue::try_from(0x68_u8).unwrap(), MissingValue::C);
    }

    #[test]
    fn byte_d() {
        assert_eq!(MissingValue::try_from(0x69_u8).unwrap(), MissingValue::D);
    }

    #[test]
    fn byte_e() {
        assert_eq!(MissingValue::try_from(0x6A_u8).unwrap(), MissingValue::E);
    }

    #[test]
    fn byte_f() {
        assert_eq!(MissingValue::try_from(0x6B_u8).unwrap(), MissingValue::F);
    }

    #[test]
    fn byte_g() {
        assert_eq!(MissingValue::try_from(0x6C_u8).unwrap(), MissingValue::G);
    }

    #[test]
    fn byte_h() {
        assert_eq!(MissingValue::try_from(0x6D_u8).unwrap(), MissingValue::H);
    }

    #[test]
    fn byte_i() {
        assert_eq!(MissingValue::try_from(0x6E_u8).unwrap(), MissingValue::I);
    }

    #[test]
    fn byte_j() {
        assert_eq!(MissingValue::try_from(0x6F_u8).unwrap(), MissingValue::J);
    }

    #[test]
    fn byte_k() {
        assert_eq!(MissingValue::try_from(0x70_u8).unwrap(), MissingValue::K);
    }

    #[test]
    fn byte_l() {
        assert_eq!(MissingValue::try_from(0x71_u8).unwrap(), MissingValue::L);
    }

    #[test]
    fn byte_m() {
        assert_eq!(MissingValue::try_from(0x72_u8).unwrap(), MissingValue::M);
    }

    #[test]
    fn byte_n() {
        assert_eq!(MissingValue::try_from(0x73_u8).unwrap(), MissingValue::N);
    }

    #[test]
    fn byte_o() {
        assert_eq!(MissingValue::try_from(0x74_u8).unwrap(), MissingValue::O);
    }

    #[test]
    fn byte_p() {
        assert_eq!(MissingValue::try_from(0x75_u8).unwrap(), MissingValue::P);
    }

    #[test]
    fn byte_q() {
        assert_eq!(MissingValue::try_from(0x76_u8).unwrap(), MissingValue::Q);
    }

    #[test]
    fn byte_r() {
        assert_eq!(MissingValue::try_from(0x77_u8).unwrap(), MissingValue::R);
    }

    #[test]
    fn byte_s() {
        assert_eq!(MissingValue::try_from(0x78_u8).unwrap(), MissingValue::S);
    }

    #[test]
    fn byte_t() {
        assert_eq!(MissingValue::try_from(0x79_u8).unwrap(), MissingValue::T);
    }

    #[test]
    fn byte_u() {
        assert_eq!(MissingValue::try_from(0x7A_u8).unwrap(), MissingValue::U);
    }

    #[test]
    fn byte_v() {
        assert_eq!(MissingValue::try_from(0x7B_u8).unwrap(), MissingValue::V);
    }

    #[test]
    fn byte_w() {
        assert_eq!(MissingValue::try_from(0x7C_u8).unwrap(), MissingValue::W);
    }

    #[test]
    fn byte_x() {
        assert_eq!(MissingValue::try_from(0x7D_u8).unwrap(), MissingValue::X);
    }

    #[test]
    fn byte_y() {
        assert_eq!(MissingValue::try_from(0x7E_u8).unwrap(), MissingValue::Y);
    }

    #[test]
    fn byte_z() {
        assert_eq!(MissingValue::try_from(0x7F_u8).unwrap(), MissingValue::Z);
    }

    #[test]
    fn byte_below_range() {
        assert_eq!(
            MissingValue::try_from(0x64_u8),
            Err(StataError::NotMissingValue)
        );
    }

    #[test]
    fn byte_zero() {
        assert_eq!(
            MissingValue::try_from(0_u8),
            Err(StataError::NotMissingValue)
        );
    }

    #[test]
    fn byte_max_valid() {
        assert_eq!(
            MissingValue::try_from(100_u8),
            Err(StataError::NotMissingValue)
        );
    }

    // -----------------------------------------------------------------------
    // TryFrom<u16> — Stata "int"
    // -----------------------------------------------------------------------

    #[test]
    fn int_system() {
        assert_eq!(
            MissingValue::try_from(0x7FE5_u16).unwrap(),
            MissingValue::System
        );
    }

    #[test]
    fn int_a() {
        assert_eq!(MissingValue::try_from(0x7FE6_u16).unwrap(), MissingValue::A);
    }

    #[test]
    fn int_b() {
        assert_eq!(MissingValue::try_from(0x7FE7_u16).unwrap(), MissingValue::B);
    }

    #[test]
    fn int_c() {
        assert_eq!(MissingValue::try_from(0x7FE8_u16).unwrap(), MissingValue::C);
    }

    #[test]
    fn int_d() {
        assert_eq!(MissingValue::try_from(0x7FE9_u16).unwrap(), MissingValue::D);
    }

    #[test]
    fn int_e() {
        assert_eq!(MissingValue::try_from(0x7FEA_u16).unwrap(), MissingValue::E);
    }

    #[test]
    fn int_f() {
        assert_eq!(MissingValue::try_from(0x7FEB_u16).unwrap(), MissingValue::F);
    }

    #[test]
    fn int_g() {
        assert_eq!(MissingValue::try_from(0x7FEC_u16).unwrap(), MissingValue::G);
    }

    #[test]
    fn int_h() {
        assert_eq!(MissingValue::try_from(0x7FED_u16).unwrap(), MissingValue::H);
    }

    #[test]
    fn int_i() {
        assert_eq!(MissingValue::try_from(0x7FEE_u16).unwrap(), MissingValue::I);
    }

    #[test]
    fn int_j() {
        assert_eq!(MissingValue::try_from(0x7FEF_u16).unwrap(), MissingValue::J);
    }

    #[test]
    fn int_k() {
        assert_eq!(MissingValue::try_from(0x7FF0_u16).unwrap(), MissingValue::K);
    }

    #[test]
    fn int_l() {
        assert_eq!(MissingValue::try_from(0x7FF1_u16).unwrap(), MissingValue::L);
    }

    #[test]
    fn int_m() {
        assert_eq!(MissingValue::try_from(0x7FF2_u16).unwrap(), MissingValue::M);
    }

    #[test]
    fn int_n() {
        assert_eq!(MissingValue::try_from(0x7FF3_u16).unwrap(), MissingValue::N);
    }

    #[test]
    fn int_o() {
        assert_eq!(MissingValue::try_from(0x7FF4_u16).unwrap(), MissingValue::O);
    }

    #[test]
    fn int_p() {
        assert_eq!(MissingValue::try_from(0x7FF5_u16).unwrap(), MissingValue::P);
    }

    #[test]
    fn int_q() {
        assert_eq!(MissingValue::try_from(0x7FF6_u16).unwrap(), MissingValue::Q);
    }

    #[test]
    fn int_r() {
        assert_eq!(MissingValue::try_from(0x7FF7_u16).unwrap(), MissingValue::R);
    }

    #[test]
    fn int_s() {
        assert_eq!(MissingValue::try_from(0x7FF8_u16).unwrap(), MissingValue::S);
    }

    #[test]
    fn int_t() {
        assert_eq!(MissingValue::try_from(0x7FF9_u16).unwrap(), MissingValue::T);
    }

    #[test]
    fn int_u() {
        assert_eq!(MissingValue::try_from(0x7FFA_u16).unwrap(), MissingValue::U);
    }

    #[test]
    fn int_v() {
        assert_eq!(MissingValue::try_from(0x7FFB_u16).unwrap(), MissingValue::V);
    }

    #[test]
    fn int_w() {
        assert_eq!(MissingValue::try_from(0x7FFC_u16).unwrap(), MissingValue::W);
    }

    #[test]
    fn int_x() {
        assert_eq!(MissingValue::try_from(0x7FFD_u16).unwrap(), MissingValue::X);
    }

    #[test]
    fn int_y() {
        assert_eq!(MissingValue::try_from(0x7FFE_u16).unwrap(), MissingValue::Y);
    }

    #[test]
    fn int_z() {
        assert_eq!(MissingValue::try_from(0x7FFF_u16).unwrap(), MissingValue::Z);
    }

    #[test]
    fn int_below_range() {
        assert_eq!(
            MissingValue::try_from(0x7FE4_u16),
            Err(StataError::NotMissingValue)
        );
    }

    #[test]
    fn int_zero() {
        assert_eq!(
            MissingValue::try_from(0_u16),
            Err(StataError::NotMissingValue)
        );
    }

    // -----------------------------------------------------------------------
    // TryFrom<u32> — Stata "long"
    // -----------------------------------------------------------------------

    #[test]
    fn long_system() {
        assert_eq!(
            MissingValue::try_from(0x7FFF_FFE5_u32).unwrap(),
            MissingValue::System
        );
    }

    #[test]
    fn long_a() {
        assert_eq!(
            MissingValue::try_from(0x7FFF_FFE6_u32).unwrap(),
            MissingValue::A
        );
    }

    #[test]
    fn long_b() {
        assert_eq!(
            MissingValue::try_from(0x7FFF_FFE7_u32).unwrap(),
            MissingValue::B
        );
    }

    #[test]
    fn long_c() {
        assert_eq!(
            MissingValue::try_from(0x7FFF_FFE8_u32).unwrap(),
            MissingValue::C
        );
    }

    #[test]
    fn long_d() {
        assert_eq!(
            MissingValue::try_from(0x7FFF_FFE9_u32).unwrap(),
            MissingValue::D
        );
    }

    #[test]
    fn long_e() {
        assert_eq!(
            MissingValue::try_from(0x7FFF_FFEA_u32).unwrap(),
            MissingValue::E
        );
    }

    #[test]
    fn long_f() {
        assert_eq!(
            MissingValue::try_from(0x7FFF_FFEB_u32).unwrap(),
            MissingValue::F
        );
    }

    #[test]
    fn long_g() {
        assert_eq!(
            MissingValue::try_from(0x7FFF_FFEC_u32).unwrap(),
            MissingValue::G
        );
    }

    #[test]
    fn long_h() {
        assert_eq!(
            MissingValue::try_from(0x7FFF_FFED_u32).unwrap(),
            MissingValue::H
        );
    }

    #[test]
    fn long_i() {
        assert_eq!(
            MissingValue::try_from(0x7FFF_FFEE_u32).unwrap(),
            MissingValue::I
        );
    }

    #[test]
    fn long_j() {
        assert_eq!(
            MissingValue::try_from(0x7FFF_FFEF_u32).unwrap(),
            MissingValue::J
        );
    }

    #[test]
    fn long_k() {
        assert_eq!(
            MissingValue::try_from(0x7FFF_FFF0_u32).unwrap(),
            MissingValue::K
        );
    }

    #[test]
    fn long_l() {
        assert_eq!(
            MissingValue::try_from(0x7FFF_FFF1_u32).unwrap(),
            MissingValue::L
        );
    }

    #[test]
    fn long_m() {
        assert_eq!(
            MissingValue::try_from(0x7FFF_FFF2_u32).unwrap(),
            MissingValue::M
        );
    }

    #[test]
    fn long_n() {
        assert_eq!(
            MissingValue::try_from(0x7FFF_FFF3_u32).unwrap(),
            MissingValue::N
        );
    }

    #[test]
    fn long_o() {
        assert_eq!(
            MissingValue::try_from(0x7FFF_FFF4_u32).unwrap(),
            MissingValue::O
        );
    }

    #[test]
    fn long_p() {
        assert_eq!(
            MissingValue::try_from(0x7FFF_FFF5_u32).unwrap(),
            MissingValue::P
        );
    }

    #[test]
    fn long_q() {
        assert_eq!(
            MissingValue::try_from(0x7FFF_FFF6_u32).unwrap(),
            MissingValue::Q
        );
    }

    #[test]
    fn long_r() {
        assert_eq!(
            MissingValue::try_from(0x7FFF_FFF7_u32).unwrap(),
            MissingValue::R
        );
    }

    #[test]
    fn long_s() {
        assert_eq!(
            MissingValue::try_from(0x7FFF_FFF8_u32).unwrap(),
            MissingValue::S
        );
    }

    #[test]
    fn long_t() {
        assert_eq!(
            MissingValue::try_from(0x7FFF_FFF9_u32).unwrap(),
            MissingValue::T
        );
    }

    #[test]
    fn long_u() {
        assert_eq!(
            MissingValue::try_from(0x7FFF_FFFA_u32).unwrap(),
            MissingValue::U
        );
    }

    #[test]
    fn long_v() {
        assert_eq!(
            MissingValue::try_from(0x7FFF_FFFB_u32).unwrap(),
            MissingValue::V
        );
    }

    #[test]
    fn long_w() {
        assert_eq!(
            MissingValue::try_from(0x7FFF_FFFC_u32).unwrap(),
            MissingValue::W
        );
    }

    #[test]
    fn long_x() {
        assert_eq!(
            MissingValue::try_from(0x7FFF_FFFD_u32).unwrap(),
            MissingValue::X
        );
    }

    #[test]
    fn long_y() {
        assert_eq!(
            MissingValue::try_from(0x7FFF_FFFE_u32).unwrap(),
            MissingValue::Y
        );
    }

    #[test]
    fn long_z() {
        assert_eq!(
            MissingValue::try_from(0x7FFF_FFFF_u32).unwrap(),
            MissingValue::Z
        );
    }

    #[test]
    fn long_below_range() {
        assert_eq!(
            MissingValue::try_from(0x7FFF_FFE4_u32),
            Err(StataError::NotMissingValue)
        );
    }

    #[test]
    fn long_zero() {
        assert_eq!(
            MissingValue::try_from(0_u32),
            Err(StataError::NotMissingValue)
        );
    }

    // -----------------------------------------------------------------------
    // TryFrom<f32> — Stata "float"
    // -----------------------------------------------------------------------

    #[test]
    fn float_system() {
        assert_eq!(
            MissingValue::try_from(f32::from_bits(0x7F00_0000)).unwrap(),
            MissingValue::System
        );
    }

    #[test]
    fn float_a() {
        assert_eq!(
            MissingValue::try_from(f32::from_bits(0x7F00_0800)).unwrap(),
            MissingValue::A
        );
    }

    #[test]
    fn float_b() {
        assert_eq!(
            MissingValue::try_from(f32::from_bits(0x7F00_1000)).unwrap(),
            MissingValue::B
        );
    }

    #[test]
    fn float_c() {
        assert_eq!(
            MissingValue::try_from(f32::from_bits(0x7F00_1800)).unwrap(),
            MissingValue::C
        );
    }

    #[test]
    fn float_d() {
        assert_eq!(
            MissingValue::try_from(f32::from_bits(0x7F00_2000)).unwrap(),
            MissingValue::D
        );
    }

    #[test]
    fn float_e() {
        assert_eq!(
            MissingValue::try_from(f32::from_bits(0x7F00_2800)).unwrap(),
            MissingValue::E
        );
    }

    #[test]
    fn float_f() {
        assert_eq!(
            MissingValue::try_from(f32::from_bits(0x7F00_3000)).unwrap(),
            MissingValue::F
        );
    }

    #[test]
    fn float_g() {
        assert_eq!(
            MissingValue::try_from(f32::from_bits(0x7F00_3800)).unwrap(),
            MissingValue::G
        );
    }

    #[test]
    fn float_h() {
        assert_eq!(
            MissingValue::try_from(f32::from_bits(0x7F00_4000)).unwrap(),
            MissingValue::H
        );
    }

    #[test]
    fn float_i() {
        assert_eq!(
            MissingValue::try_from(f32::from_bits(0x7F00_4800)).unwrap(),
            MissingValue::I
        );
    }

    #[test]
    fn float_j() {
        assert_eq!(
            MissingValue::try_from(f32::from_bits(0x7F00_5000)).unwrap(),
            MissingValue::J
        );
    }

    #[test]
    fn float_k() {
        assert_eq!(
            MissingValue::try_from(f32::from_bits(0x7F00_5800)).unwrap(),
            MissingValue::K
        );
    }

    #[test]
    fn float_l() {
        assert_eq!(
            MissingValue::try_from(f32::from_bits(0x7F00_6000)).unwrap(),
            MissingValue::L
        );
    }

    #[test]
    fn float_m() {
        assert_eq!(
            MissingValue::try_from(f32::from_bits(0x7F00_6800)).unwrap(),
            MissingValue::M
        );
    }

    #[test]
    fn float_n() {
        assert_eq!(
            MissingValue::try_from(f32::from_bits(0x7F00_7000)).unwrap(),
            MissingValue::N
        );
    }

    #[test]
    fn float_o() {
        assert_eq!(
            MissingValue::try_from(f32::from_bits(0x7F00_7800)).unwrap(),
            MissingValue::O
        );
    }

    #[test]
    fn float_p() {
        assert_eq!(
            MissingValue::try_from(f32::from_bits(0x7F00_8000)).unwrap(),
            MissingValue::P
        );
    }

    #[test]
    fn float_q() {
        assert_eq!(
            MissingValue::try_from(f32::from_bits(0x7F00_8800)).unwrap(),
            MissingValue::Q
        );
    }

    #[test]
    fn float_r() {
        assert_eq!(
            MissingValue::try_from(f32::from_bits(0x7F00_9000)).unwrap(),
            MissingValue::R
        );
    }

    #[test]
    fn float_s() {
        assert_eq!(
            MissingValue::try_from(f32::from_bits(0x7F00_9800)).unwrap(),
            MissingValue::S
        );
    }

    #[test]
    fn float_t() {
        assert_eq!(
            MissingValue::try_from(f32::from_bits(0x7F00_A000)).unwrap(),
            MissingValue::T
        );
    }

    #[test]
    fn float_u() {
        assert_eq!(
            MissingValue::try_from(f32::from_bits(0x7F00_A800)).unwrap(),
            MissingValue::U
        );
    }

    #[test]
    fn float_v() {
        assert_eq!(
            MissingValue::try_from(f32::from_bits(0x7F00_B000)).unwrap(),
            MissingValue::V
        );
    }

    #[test]
    fn float_w() {
        assert_eq!(
            MissingValue::try_from(f32::from_bits(0x7F00_B800)).unwrap(),
            MissingValue::W
        );
    }

    #[test]
    fn float_x() {
        assert_eq!(
            MissingValue::try_from(f32::from_bits(0x7F00_C000)).unwrap(),
            MissingValue::X
        );
    }

    #[test]
    fn float_y() {
        assert_eq!(
            MissingValue::try_from(f32::from_bits(0x7F00_C800)).unwrap(),
            MissingValue::Y
        );
    }

    #[test]
    fn float_z() {
        assert_eq!(
            MissingValue::try_from(f32::from_bits(0x7F00_D000)).unwrap(),
            MissingValue::Z
        );
    }

    #[test]
    fn float_non_missing_nan() {
        assert_eq!(
            MissingValue::try_from(f32::from_bits(0x7F00_0001)),
            Err(StataError::NotMissingValue)
        );
    }

    #[test]
    fn float_normal_value() {
        assert_eq!(
            MissingValue::try_from(1.0_f32),
            Err(StataError::NotMissingValue)
        );
    }

    #[test]
    fn float_zero() {
        assert_eq!(
            MissingValue::try_from(0.0_f32),
            Err(StataError::NotMissingValue)
        );
    }

    #[test]
    fn float_negative() {
        assert_eq!(
            MissingValue::try_from(-1.0_f32),
            Err(StataError::NotMissingValue)
        );
    }

    // -----------------------------------------------------------------------
    // TryFrom<f64> — Stata "double"
    // -----------------------------------------------------------------------

    #[test]
    fn double_system() {
        assert_eq!(
            MissingValue::try_from(f64::from_bits(0x7FE0_0000_0000_0000)).unwrap(),
            MissingValue::System
        );
    }

    #[test]
    fn double_a() {
        assert_eq!(
            MissingValue::try_from(f64::from_bits(0x7FE0_0100_0000_0000)).unwrap(),
            MissingValue::A
        );
    }

    #[test]
    fn double_b() {
        assert_eq!(
            MissingValue::try_from(f64::from_bits(0x7FE0_0200_0000_0000)).unwrap(),
            MissingValue::B
        );
    }

    #[test]
    fn double_c() {
        assert_eq!(
            MissingValue::try_from(f64::from_bits(0x7FE0_0300_0000_0000)).unwrap(),
            MissingValue::C
        );
    }

    #[test]
    fn double_d() {
        assert_eq!(
            MissingValue::try_from(f64::from_bits(0x7FE0_0400_0000_0000)).unwrap(),
            MissingValue::D
        );
    }

    #[test]
    fn double_e() {
        assert_eq!(
            MissingValue::try_from(f64::from_bits(0x7FE0_0500_0000_0000)).unwrap(),
            MissingValue::E
        );
    }

    #[test]
    fn double_f() {
        assert_eq!(
            MissingValue::try_from(f64::from_bits(0x7FE0_0600_0000_0000)).unwrap(),
            MissingValue::F
        );
    }

    #[test]
    fn double_g() {
        assert_eq!(
            MissingValue::try_from(f64::from_bits(0x7FE0_0700_0000_0000)).unwrap(),
            MissingValue::G
        );
    }

    #[test]
    fn double_h() {
        assert_eq!(
            MissingValue::try_from(f64::from_bits(0x7FE0_0800_0000_0000)).unwrap(),
            MissingValue::H
        );
    }

    #[test]
    fn double_i() {
        assert_eq!(
            MissingValue::try_from(f64::from_bits(0x7FE0_0900_0000_0000)).unwrap(),
            MissingValue::I
        );
    }

    #[test]
    fn double_j() {
        assert_eq!(
            MissingValue::try_from(f64::from_bits(0x7FE0_0A00_0000_0000)).unwrap(),
            MissingValue::J
        );
    }

    #[test]
    fn double_k() {
        assert_eq!(
            MissingValue::try_from(f64::from_bits(0x7FE0_0B00_0000_0000)).unwrap(),
            MissingValue::K
        );
    }

    #[test]
    fn double_l() {
        assert_eq!(
            MissingValue::try_from(f64::from_bits(0x7FE0_0C00_0000_0000)).unwrap(),
            MissingValue::L
        );
    }

    #[test]
    fn double_m() {
        assert_eq!(
            MissingValue::try_from(f64::from_bits(0x7FE0_0D00_0000_0000)).unwrap(),
            MissingValue::M
        );
    }

    #[test]
    fn double_n() {
        assert_eq!(
            MissingValue::try_from(f64::from_bits(0x7FE0_0E00_0000_0000)).unwrap(),
            MissingValue::N
        );
    }

    #[test]
    fn double_o() {
        assert_eq!(
            MissingValue::try_from(f64::from_bits(0x7FE0_0F00_0000_0000)).unwrap(),
            MissingValue::O
        );
    }

    #[test]
    fn double_p() {
        assert_eq!(
            MissingValue::try_from(f64::from_bits(0x7FE0_1000_0000_0000)).unwrap(),
            MissingValue::P
        );
    }

    #[test]
    fn double_q() {
        assert_eq!(
            MissingValue::try_from(f64::from_bits(0x7FE0_1100_0000_0000)).unwrap(),
            MissingValue::Q
        );
    }

    #[test]
    fn double_r() {
        assert_eq!(
            MissingValue::try_from(f64::from_bits(0x7FE0_1200_0000_0000)).unwrap(),
            MissingValue::R
        );
    }

    #[test]
    fn double_s() {
        assert_eq!(
            MissingValue::try_from(f64::from_bits(0x7FE0_1300_0000_0000)).unwrap(),
            MissingValue::S
        );
    }

    #[test]
    fn double_t() {
        assert_eq!(
            MissingValue::try_from(f64::from_bits(0x7FE0_1400_0000_0000)).unwrap(),
            MissingValue::T
        );
    }

    #[test]
    fn double_u() {
        assert_eq!(
            MissingValue::try_from(f64::from_bits(0x7FE0_1500_0000_0000)).unwrap(),
            MissingValue::U
        );
    }

    #[test]
    fn double_v() {
        assert_eq!(
            MissingValue::try_from(f64::from_bits(0x7FE0_1600_0000_0000)).unwrap(),
            MissingValue::V
        );
    }

    #[test]
    fn double_w() {
        assert_eq!(
            MissingValue::try_from(f64::from_bits(0x7FE0_1700_0000_0000)).unwrap(),
            MissingValue::W
        );
    }

    #[test]
    fn double_x() {
        assert_eq!(
            MissingValue::try_from(f64::from_bits(0x7FE0_1800_0000_0000)).unwrap(),
            MissingValue::X
        );
    }

    #[test]
    fn double_y() {
        assert_eq!(
            MissingValue::try_from(f64::from_bits(0x7FE0_1900_0000_0000)).unwrap(),
            MissingValue::Y
        );
    }

    #[test]
    fn double_z() {
        assert_eq!(
            MissingValue::try_from(f64::from_bits(0x7FE0_1A00_0000_0000)).unwrap(),
            MissingValue::Z
        );
    }

    #[test]
    fn double_non_missing_nan() {
        assert_eq!(
            MissingValue::try_from(f64::from_bits(0x7FE0_0000_0000_0001)),
            Err(StataError::NotMissingValue)
        );
    }

    #[test]
    fn double_normal_value() {
        assert_eq!(
            MissingValue::try_from(1.0_f64),
            Err(StataError::NotMissingValue)
        );
    }

    #[test]
    fn double_zero() {
        assert_eq!(
            MissingValue::try_from(0.0_f64),
            Err(StataError::NotMissingValue)
        );
    }

    #[test]
    fn double_negative() {
        assert_eq!(
            MissingValue::try_from(-1.0_f64),
            Err(StataError::NotMissingValue)
        );
    }

    // -----------------------------------------------------------------------
    // Cross-type consistency: all five types agree on System, A, and Z
    // -----------------------------------------------------------------------

    #[test]
    fn all_types_agree_on_system() {
        assert_eq!(
            MissingValue::try_from(0x65_u8).unwrap(),
            MissingValue::System
        );
        assert_eq!(
            MissingValue::try_from(0x7FE5_u16).unwrap(),
            MissingValue::System
        );
        assert_eq!(
            MissingValue::try_from(0x7FFF_FFE5_u32).unwrap(),
            MissingValue::System
        );
        assert_eq!(
            MissingValue::try_from(f32::from_bits(0x7F00_0000)).unwrap(),
            MissingValue::System
        );
        assert_eq!(
            MissingValue::try_from(f64::from_bits(0x7FE0_0000_0000_0000)).unwrap(),
            MissingValue::System
        );
    }

    #[test]
    fn all_types_agree_on_a() {
        assert_eq!(MissingValue::try_from(0x66_u8).unwrap(), MissingValue::A);
        assert_eq!(MissingValue::try_from(0x7FE6_u16).unwrap(), MissingValue::A);
        assert_eq!(
            MissingValue::try_from(0x7FFF_FFE6_u32).unwrap(),
            MissingValue::A
        );
        assert_eq!(
            MissingValue::try_from(f32::from_bits(0x7F00_0800)).unwrap(),
            MissingValue::A
        );
        assert_eq!(
            MissingValue::try_from(f64::from_bits(0x7FE0_0100_0000_0000)).unwrap(),
            MissingValue::A
        );
    }

    #[test]
    fn all_types_agree_on_z() {
        assert_eq!(MissingValue::try_from(0x7F_u8).unwrap(), MissingValue::Z);
        assert_eq!(MissingValue::try_from(0x7FFF_u16).unwrap(), MissingValue::Z);
        assert_eq!(
            MissingValue::try_from(0x7FFF_FFFF_u32).unwrap(),
            MissingValue::Z
        );
        assert_eq!(
            MissingValue::try_from(f32::from_bits(0x7F00_D000)).unwrap(),
            MissingValue::Z
        );
        assert_eq!(
            MissingValue::try_from(f64::from_bits(0x7FE0_1A00_0000_0000)).unwrap(),
            MissingValue::Z
        );
    }
}
