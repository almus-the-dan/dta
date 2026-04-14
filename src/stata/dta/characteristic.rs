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
