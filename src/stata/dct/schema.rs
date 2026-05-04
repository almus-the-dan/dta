use super::column::Column;
use super::dct_warning::DctWarning;

/// The parsed contents of a `.dct` dictionary, excluding any data.
#[derive(Debug, Clone)]
pub struct Schema {
    columns: Vec<Column>,
    logical_record_length: Option<usize>,
    first_line_of_file: Option<usize>,
    lines_per_observation: usize,
    declared_data_path: Option<String>,
    warnings: Vec<DctWarning>,
}

impl Schema {
    #[must_use]
    pub(crate) fn new(
        columns: Vec<Column>,
        logical_record_length: Option<usize>,
        first_line_of_file: Option<usize>,
        lines_per_observation: usize,
        declared_data_path: Option<String>,
        warnings: Vec<DctWarning>,
    ) -> Self {
        Self {
            columns,
            logical_record_length,
            first_line_of_file,
            lines_per_observation,
            declared_data_path,
            warnings,
        }
    }

    /// Columns in declaration order. Determines variable order in
    /// records yielded by the data reader.
    #[must_use]
    #[inline]
    pub fn columns(&self) -> &[Column] {
        &self.columns
    }

    /// Logical record length for fixed-block files with no newline
    /// delimiters. `None` means standard newline-terminated records.
    #[must_use]
    #[inline]
    pub fn logical_record_length(&self) -> Option<usize> {
        self.logical_record_length
    }

    /// 1-based line of the data file at which records begin. `None`
    /// means line 1 (or the line after `}` for embedded data).
    #[must_use]
    #[inline]
    pub fn first_line_of_file(&self) -> Option<usize> {
        self.first_line_of_file
    }

    /// Number of physical lines per logical observation.
    ///
    /// Always at least `1`. Single-line observations report `1`;
    /// multi-line observations report one more than the number of
    /// `_newline` directives encountered while parsing the
    /// dictionary body.
    #[must_use]
    #[inline]
    pub fn lines_per_observation(&self) -> usize {
        self.lines_per_observation
    }

    /// Path declared in the `using` clause of the dictionary, if
    /// present. Informational only — the library never opens this
    /// path on the caller's behalf; supply data sources explicitly.
    #[must_use]
    #[inline]
    pub fn declared_data_path(&self) -> Option<&str> {
        self.declared_data_path.as_deref()
    }

    /// Non-fatal issues encountered while parsing the dictionary.
    #[must_use]
    #[inline]
    pub fn warnings(&self) -> &[DctWarning] {
        &self.warnings
    }
}
