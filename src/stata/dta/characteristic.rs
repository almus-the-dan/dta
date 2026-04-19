/// What a characteristic is attached to: the dataset or a specific
/// variable.
///
/// Stata represents dataset-level characteristics with the magic
/// variable name `"_dta"`. This enum makes the distinction explicit
/// so that writers cannot accidentally produce an invalid target.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum CharacteristicTarget {
    /// A dataset-level characteristic (Stata's `_dta[name]`).
    Dataset,
    /// A variable-level characteristic (Stata's `varname[name]`).
    Variable(String),
}

impl CharacteristicTarget {
    /// The raw variable-name string written to the DTA file.
    ///
    /// Returns `"_dta"` for [`Dataset`](Self::Dataset) and the
    /// variable name for [`Variable`](Self::Variable).
    #[must_use]
    #[inline]
    pub fn as_variable_name(&self) -> &str {
        match self {
            Self::Dataset => "_dta",
            Self::Variable(name) => name,
        }
    }

    /// Creates a target from a raw variable-name string read from a
    /// DTA file.
    #[must_use]
    pub(crate) fn from_variable_name(name: String) -> Self {
        if name == "_dta" {
            Self::Dataset
        } else {
            Self::Variable(name)
        }
    }
}

/// Kind of entry in a pre-117 binary expansion-field section.
///
/// The DTA spec (see Stata help page `dta_115`) defines only two
/// `data_type` byte values. All other values are reserved for
/// future use, and the spec instructs readers to skip unknown
/// entries rather than treat their payloads as characteristics.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub(crate) enum ExpansionFieldType {
    /// Section terminator; paired with `length == 0` to end the
    /// characteristics section.
    Terminator = 0,
    /// Variable or dataset characteristic entry.
    Characteristic = 1,
}

impl ExpansionFieldType {
    /// Raw byte value written to the file.
    #[must_use]
    #[inline]
    pub(crate) fn to_byte(self) -> u8 {
        // SAFETY: `#[repr(u8)]` guarantees the discriminant fits in
        // a `u8`. Mirrors `Release::to_byte` — the only other place
        // in the crate that uses `as` for this purpose.
        self as u8
    }

    /// Classifies a raw `data_type` byte read from the file. Returns
    /// `None` for values outside the two currently defined by the
    /// DTA spec — the reader's contract is to skip such entries per
    /// Stata's forward-compatibility rule.
    #[must_use]
    pub(crate) fn from_byte(byte: u8) -> Option<Self> {
        match byte {
            0 => Some(Self::Terminator),
            1 => Some(Self::Characteristic),
            _ => None,
        }
    }
}

/// A single characteristic entry from a DTA file.
///
/// Characteristics are arbitrary key-value metadata attached to
/// either the dataset as a whole or to individual variables. Stata
/// uses characteristics internally for notes, multilingual labels,
/// and other metadata.
#[derive(Debug, Clone)]
pub struct Characteristic {
    target: CharacteristicTarget,
    name: String,
    value: String,
}

impl Characteristic {
    #[must_use]
    pub(crate) fn new(target: CharacteristicTarget, name: String, value: String) -> Self {
        Self {
            target,
            name,
            value,
        }
    }

    /// What this characteristic is attached to.
    #[must_use]
    #[inline]
    pub fn target(&self) -> &CharacteristicTarget {
        &self.target
    }

    /// The name of the characteristic.
    #[must_use]
    #[inline]
    pub fn name(&self) -> &str {
        &self.name
    }

    /// The contents of the characteristic, decoded using the file's
    /// encoding.
    #[must_use]
    #[inline]
    pub fn value(&self) -> &str {
        &self.value
    }
}
