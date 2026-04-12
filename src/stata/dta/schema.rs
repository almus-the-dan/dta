use super::dta_error::{DtaError, Result};
use super::variable::Variable;

/// Variable definitions and layout information from a DTA file.
///
/// Contains variable names, types, display formats, labels, sort
/// order, and value-label table associations. This is everything
/// needed to interpret the data section.
///
/// Construct via [`Schema::builder`].
#[derive(Debug, Clone)]
pub struct Schema {
    variables: Vec<Variable>,
    sort_order: Vec<u32>,
    row_len: usize,
}

impl Schema {
    /// Returns a new empty [`SchemaBuilder`].
    #[must_use]
    #[inline]
    pub fn builder() -> SchemaBuilder {
        SchemaBuilder {
            variables: Vec::new(),
            sort_order: Vec::new(),
        }
    }

    /// The variable definitions, one per column.
    #[must_use]
    #[inline]
    pub fn variables(&self) -> &[Variable] {
        &self.variables
    }

    /// Indices of variables that the data is sorted by (0-based).
    ///
    /// Empty when the file has no declared sort order.
    #[must_use]
    #[inline]
    pub fn sort_order(&self) -> &[u32] {
        &self.sort_order
    }

    /// Total number of bytes per observation (row) in the data section.
    #[must_use]
    #[inline]
    pub(crate) fn row_len(&self) -> usize {
        self.row_len
    }
}

/// Builder for [`Schema`].
///
/// Created via [`Schema::builder`]. Add variables with
/// [`add_variable`](Self::add_variable) or [`variables`](Self::variables),
/// optionally set a [`sort_order`](Self::sort_order), then call
/// [`build`](Self::build).
#[derive(Debug, Clone)]
pub struct SchemaBuilder {
    variables: Vec<Variable>,
    sort_order: Vec<u32>,
}

impl SchemaBuilder {
    /// Appends a single variable.
    #[must_use]
    #[inline]
    pub fn add_variable(mut self, variable: Variable) -> Self {
        self.variables.push(variable);
        self
    }

    /// Replaces all variables.
    #[must_use]
    #[inline]
    pub fn variables(mut self, variables: Vec<Variable>) -> Self {
        self.variables = variables;
        self
    }

    /// Appends a single sort-order entry (0-based variable index).
    #[must_use]
    #[inline]
    pub fn add_sort_order(mut self, index: u32) -> Self {
        self.sort_order.push(index);
        self
    }

    /// Replaces the entire sort order (0-based variable indices).
    #[must_use]
    #[inline]
    pub fn sort_order(mut self, sort_order: Vec<u32>) -> Self {
        self.sort_order = sort_order;
        self
    }

    /// Builds the [`Schema`], computing the row length from variable
    /// types and validating sort-order indices.
    ///
    /// # Errors
    ///
    /// Returns [`DtaError::SortOrderOutOfBounds`] if any sort-order
    /// entry references a variable index >= the number of variables.
    pub fn build(self) -> Result<Schema> {
        let variable_count = self.variables.len();
        for &index in &self.sort_order {
            let index_usize =
                usize::try_from(index).map_err(|_| DtaError::SortOrderOutOfBounds {
                    index,
                    variable_count,
                })?;
            if index_usize >= variable_count {
                return Err(DtaError::SortOrderOutOfBounds {
                    index,
                    variable_count,
                });
            }
        }
        let row_len = self
            .variables
            .iter()
            .map(|v| v.variable_type().width())
            .sum();
        Ok(Schema {
            variables: self.variables,
            sort_order: self.sort_order,
            row_len,
        })
    }
}
