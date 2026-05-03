//! State machine that drives parsing of a `.dct` dictionary.
//!
//! The state owns its line buffer and all accumulated parse state.
//! The I/O layer (sync or async) reads bytes into [`buffer_mut`](DctSourceState::buffer_mut),
//! then calls [`feed_buffered_line`](DctSourceState::feed_buffered_line)
//! to advance the parse. When `feed_buffered_line` returns
//! [`FeedOutcome::Done`], the dictionary's closing `}` has been
//! consumed and [`into_schema`](DctSourceState::into_schema) builds
//! the final [`Schema`].
//!
//! All parsing helpers are pure functions in this module — no I/O —
//! so the state can be reused unchanged by both the sync and async
//! parsers.

use std::iter::Peekable;
use std::ops::Range;

use super::column::Column;
use super::dct_error::{DctError, Result};
use super::dct_warning::DctWarning;
use super::input_format::InputFormat;
use super::line_ending::strip_terminator;
use super::numeric_style::NumericStyle;
use super::schema::Schema;
use super::variable_type::VariableType;

/// Result of feeding one line to [`DctSourceState`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum FeedOutcome {
    /// More input is needed before the dictionary is complete.
    NeedMore,
    /// The closing `}` was consumed; no more lines should be fed.
    Done,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Stage {
    Header,
    Body,
    Done,
}

/// Mutable parse state for a `.dct` dictionary, fed one line at a
/// time by the sync or async I/O wrapper.
#[derive(Debug)]
pub(super) struct DctSourceState {
    buffer: String,
    line_number: usize,
    stage: Stage,
    header_tokens: Vec<String>,
    body_data: BodyData,
    cursor_offset: usize,
    scratch_ranges: Vec<Range<usize>>,
    declared_data_path: Option<String>,
}

impl DctSourceState {
    #[must_use]
    pub(super) fn new() -> Self {
        Self {
            buffer: String::new(),
            line_number: 0,
            stage: Stage::Header,
            header_tokens: Vec::new(),
            body_data: BodyData {
                columns: Vec::new(),
                logical_record_length: None,
                first_line_of_file: None,
                lines_per_observation: 0, // bumped to 1 inside `into_schema`
                warnings: Vec::new(),
            },
            cursor_offset: 0,
            scratch_ranges: Vec::new(),
            declared_data_path: None,
        }
    }

    /// Returns `&mut` to the internal line buffer, after clearing it.
    /// The I/O layer reads one line into the returned buffer.
    pub(super) fn buffer_mut(&mut self) -> &mut String {
        self.buffer.clear();
        &mut self.buffer
    }

    /// Processes the line currently in the internal buffer.
    ///
    /// The buffer is expected to contain bytes just read by
    /// `BufRead::read_line` (sync) or `AsyncBufReadExt::read_line`
    /// (async) — terminator bytes are stripped here.
    pub(super) fn feed_buffered_line(&mut self) -> Result<FeedOutcome> {
        strip_terminator(&mut self.buffer);
        self.line_number += 1;

        match self.stage {
            Stage::Header => self.feed_header_line(),
            Stage::Body => self.feed_body_line(),
            Stage::Done => Ok(FeedOutcome::Done),
        }
    }

    /// Consumes the state and produces the final [`Schema`].
    ///
    /// Should only be called after `feed_buffered_line` returned
    /// [`FeedOutcome::Done`]; calling earlier produces a schema with
    /// whatever partial state was accumulated.
    #[must_use]
    pub(super) fn into_schema(self) -> Schema {
        let DctSourceState {
            mut body_data,
            declared_data_path,
            ..
        } = self;

        // `lines_per_observation` accumulates as a count of `_newline`
        // directives during the body parse; one more line is implicit
        // (the first line of every observation).
        body_data.lines_per_observation += 1;

        let mut warnings = body_data.warnings;
        if let Some(path) = &declared_data_path {
            let warning = DctWarning::DeclaredPathIgnored { path: path.clone() };
            warnings.push(warning);
        }

        Schema::new(
            body_data.columns,
            body_data.logical_record_length,
            body_data.first_line_of_file,
            body_data.lines_per_observation,
            declared_data_path,
            warnings,
        )
    }

    fn feed_header_line(&mut self) -> Result<FeedOutcome> {
        let line = self.buffer.trim_ascii_start();
        if line.starts_with('*') {
            return Ok(FeedOutcome::NeedMore);
        }
        tokenize(line, &mut self.scratch_ranges);

        let mut found_brace = false;
        for range in &self.scratch_ranges {
            let token = token(line, range);
            if token == "{" {
                found_brace = true;
            }
            self.header_tokens.push(token.to_string());
        }

        if !found_brace {
            return Ok(FeedOutcome::NeedMore);
        }

        // The opening `{` has been seen — finalize the header and
        // transition to body parsing.
        let header_line = self.line_number;
        let header_tokens = &self.header_tokens;
        let invalid = || DctError::InvalidDictionaryHeader {
            line: header_line,
            content: header_tokens.join(" "),
        };

        let mut iter = self.header_tokens.iter().map(String::as_str).peekable();
        if matches!(iter.peek(), Some(&"infile")) {
            iter.next();
        }
        if !matches!(iter.next(), Some("dictionary")) {
            return Err(invalid());
        }
        let using_path = parse_using_path(&mut iter).map_err(|()| invalid())?;
        if !matches!(iter.next(), Some("{")) {
            return Err(invalid());
        }

        self.declared_data_path = using_path;
        self.stage = Stage::Body;
        Ok(FeedOutcome::NeedMore)
    }

    fn feed_body_line(&mut self) -> Result<FeedOutcome> {
        let trimmed = self.buffer.trim_ascii();
        if trimmed.is_empty() || trimmed.starts_with('*') {
            return Ok(FeedOutcome::NeedMore);
        }
        if trimmed == "}" {
            self.stage = Stage::Done;
            return Ok(FeedOutcome::Done);
        }

        tokenize(trimmed, &mut self.scratch_ranges);
        let Some(first_range) = self.scratch_ranges.first() else {
            return Ok(FeedOutcome::NeedMore);
        };
        let first = token(trimmed, first_range);
        let line_number = self.line_number;

        if process_directive(
            first,
            line_number,
            &mut self.body_data,
            &mut self.cursor_offset,
        )? {
            return Ok(FeedOutcome::NeedMore);
        }

        if looks_like_variable_line(first) {
            let column = parse_variable_line(
                &self.scratch_ranges,
                trimmed,
                line_number,
                self.body_data.lines_per_observation,
                &mut self.cursor_offset,
            )?;
            self.body_data.columns.push(column);
            return Ok(FeedOutcome::NeedMore);
        }

        let warning = DctWarning::UnrecognizedDirective {
            line: line_number,
            content: trimmed.to_string(),
        };
        self.body_data.warnings.push(warning);
        Ok(FeedOutcome::NeedMore)
    }
}

#[derive(Debug)]
struct BodyData {
    columns: Vec<Column>,
    logical_record_length: Option<usize>,
    first_line_of_file: Option<usize>,
    lines_per_observation: usize,
    warnings: Vec<DctWarning>,
}

/// Splits a line into whitespace-separated tokens, treating a
/// double-quoted run as a single token (quotes included). Each
/// token is recorded as a byte range within `line`.
///
/// `ranges` is cleared and refilled in place so the caller's
/// allocation can be reused across many lines. Byte ranges (rather
/// than `&str`) are used so the buffer's lifetime doesn't bleed into
/// `Vec`'s type and prevent reuse across iterations.
fn tokenize(line: &str, ranges: &mut Vec<Range<usize>>) {
    ranges.clear();
    let bytes = line.as_bytes();
    let mut index = 0;
    while index < bytes.len() {
        while index < bytes.len() && bytes[index].is_ascii_whitespace() {
            index += 1;
        }
        if index >= bytes.len() {
            break;
        }
        let start = index;
        if bytes[index] == b'"' {
            index += 1;
            while index < bytes.len() && bytes[index] != b'"' {
                index += 1;
            }
            if index < bytes.len() {
                index += 1; // include the closing quote
            }
        } else {
            while index < bytes.len() && !bytes[index].is_ascii_whitespace() {
                index += 1;
            }
        }
        ranges.push(start..index);
    }
}

/// Resolves a byte range produced by [`tokenize`] back to its
/// `&str` slice within `line`.
#[inline]
fn token<'a>(line: &'a str, range: &Range<usize>) -> &'a str {
    &line[range.start..range.end]
}

#[must_use]
fn unquote(token: &str) -> &str {
    if token.len() >= 2 && token.starts_with('"') && token.ends_with('"') {
        &token[1..token.len() - 1]
    } else {
        token
    }
}

fn parse_using_path<'a, I: Iterator<Item = &'a str>>(
    iter: &mut Peekable<I>,
) -> std::result::Result<Option<String>, ()> {
    if !matches!(iter.peek(), Some(&"using")) {
        return Ok(None);
    }
    iter.next();
    let path_token = iter.next().ok_or(())?;
    let path_token = unquote(path_token);
    let path_token = path_token.to_string();
    Ok(Some(path_token))
}

enum Directive {
    LogicalRecordLength(usize),
    FirstLineOfFile(usize),
    Newline,
}

fn process_directive(
    first: &str,
    line_number: usize,
    data: &mut BodyData,
    cursor_offset: &mut usize,
) -> Result<bool> {
    let Some(directive) = parse_directive(first) else {
        return Ok(false);
    };
    match directive {
        Directive::LogicalRecordLength(n) => {
            if data.logical_record_length.is_some() {
                let error = DctError::DuplicateDirective {
                    line: line_number,
                    directive: "lrecl".to_string(),
                };
                return Err(error);
            }
            data.logical_record_length = Some(n);
        }
        Directive::FirstLineOfFile(n) => {
            if data.first_line_of_file.is_some() {
                let error = DctError::DuplicateDirective {
                    line: line_number,
                    directive: "firstlineoffile".to_string(),
                };
                return Err(error);
            }
            data.first_line_of_file = Some(n);
        }
        Directive::Newline => {
            data.lines_per_observation += 1;
            // Per the spec: "after `_newline`, `_column`
            // references restart from 1." The same applies
            // implicitly to `_skip`, which is relative to the
            // current pointer.
            *cursor_offset = 0;
        }
    }
    Ok(true)
}

fn parse_directive(token: &str) -> Option<Directive> {
    if token == "_newline" {
        return Some(Directive::Newline);
    }
    if let Some(inner) = directive_argument(token, "lrecl") {
        return inner.parse().ok().map(Directive::LogicalRecordLength);
    }
    if let Some(inner) = directive_argument(token, "firstlineoffile") {
        return inner.parse().ok().map(Directive::FirstLineOfFile);
    }
    None
}

/// Whether the line at the given first token plausibly defines a
/// variable. Variable lines start with `_column(#)`, `_skip(#)`, a
/// bare `_skip`, or a storage type keyword (which lets fixed-width
/// continuation lines like `byte v2 %3.0f` work without re-anchoring).
fn looks_like_variable_line(first: &str) -> bool {
    first.starts_with("_column(")
        || first.starts_with("_skip(")
        || first == "_skip"
        || parse_storage_type(first).is_some()
}

/// Parses a `_skip` or `_skip(#)` token, returning the byte count to
/// advance the column pointer by. Bare `_skip` is `_skip(1)`.
fn parse_skip_modifier(token: &str) -> Option<usize> {
    if token == "_skip" {
        return Some(1);
    }
    directive_argument(token, "_skip")?.parse().ok()
}

/// For a token `name(value)`, returns the trimmed `value`. Returns
/// `None` if `token` doesn't have the expected shape.
fn directive_argument<'a>(token: &'a str, name: &str) -> Option<&'a str> {
    let after_name = token.strip_prefix(name)?;
    let after_open = after_name.strip_prefix('(')?;
    let inside = after_open.strip_suffix(')')?;
    Some(inside.trim_ascii())
}

fn parse_variable_line(
    ranges: &[Range<usize>],
    line: &str,
    line_number: usize,
    line_offset: usize,
    cursor_offset: &mut usize,
) -> Result<Column> {
    let invalid = || DctError::InvalidColumnDirective {
        line: line_number,
        content: line.trim_ascii().to_string(),
    };
    let overflow = || DctError::DictionaryOffsetOverflow {
        line: line_number,
        content: line.trim_ascii().to_string(),
    };

    let mut iterator = ranges.iter().map(|range| token(line, range)).peekable();
    // Optional `_column(#)` resets to an absolute byte position;
    // optional `_skip(#)` then advances relatively. Either, both, or
    // neither may appear — neither means "start at the running
    // column pointer".
    let offset =
        compute_variable_offset(&mut iterator, *cursor_offset).map_err(|fault| match fault {
            OffsetFault::Invalid => invalid(),
            OffsetFault::Overflow => overflow(),
        })?;
    // Optional storage type. Spec default is `float`.
    let (storage_type, storage_str_width) =
        try_parse_storage_type_width(&mut iterator).map_err(|()| invalid())?;
    // Required name. Names cannot start with `%` (read format),
    // `"` (label), or `_` (reserved for directives — catches a
    // misplaced `_column`/`_skip` token after the offset block).
    let name = try_parse_name(&mut iterator).map_err(|()| invalid())?;
    // Optional %infmt.
    let input_format = try_parse_input_format(&mut iterator, line_number)?;
    // Optional "label".
    let label = parse_label(&mut iterator);

    if iterator.next().is_some() {
        return Err(invalid());
    }

    let input_format = input_format.unwrap_or(select_fallback_input_format(
        storage_type,
        storage_str_width,
    ));

    *cursor_offset = advance_cursor(offset, input_format).ok_or_else(overflow)?;

    let column = Column::new(line_offset, offset, storage_type, name, input_format, label);
    Ok(column)
}

/// Failure mode for [`compute_variable_offset`]. Distinguishes a
/// malformed `_column(#)` directive (e.g., non-numeric or zero) from
/// an arithmetic overflow when combining `_column(#)` and `_skip(#)`.
enum OffsetFault {
    Invalid,
    Overflow,
}

/// Resolves the variable's starting byte offset. Consumes a leading
/// `_column(#)` token if present (absolute), then a leading
/// `_skip(#)` token if present (relative). When neither appears, the
/// offset is the running cursor.
fn compute_variable_offset<'a, I: Iterator<Item = &'a str>>(
    iterator: &mut Peekable<I>,
    cursor_offset: usize,
) -> std::result::Result<usize, OffsetFault> {
    let mut offset = cursor_offset;
    if let Some(&token) = iterator.peek()
        && token.starts_with("_column(")
    {
        iterator.next();
        let one_based = parse_column_directive(token).ok_or(OffsetFault::Invalid)?;
        if one_based < 1 {
            return Err(OffsetFault::Invalid);
        }
        offset = one_based - 1;
    }
    if let Some(&token) = iterator.peek()
        && let Some(skip) = parse_skip_modifier(token)
    {
        iterator.next();
        offset = offset.checked_add(skip).ok_or(OffsetFault::Overflow)?;
    }
    Ok(offset)
}

/// Returns the column pointer's position after a variable using
/// `input_format` is read from `offset`. Fixed-width formats consume
/// exactly `width` bytes; free-format reads consume input dynamically
/// at runtime, so the parser-time cursor stays where the variable
/// started. Downstream variables that need to follow a free-format
/// read should anchor explicitly with `_column(#)`.
///
/// Returns `None` when the offset+width sum overflows `usize`.
fn advance_cursor(offset: usize, input_format: InputFormat) -> Option<usize> {
    match input_format {
        InputFormat::FixedNumeric { width, .. } | InputFormat::FixedString { width } => {
            offset.checked_add(width)
        }
        InputFormat::FreeNumeric | InputFormat::FreeString => Some(offset),
    }
}

fn try_parse_storage_type_width<'a, I: Iterator<Item = &'a str>>(
    iterator: &mut Peekable<I>,
) -> std::result::Result<(VariableType, Option<usize>), ()> {
    let Some(&token) = iterator.peek() else {
        return Err(());
    };
    if let Some(parsed) = parse_storage_type(token) {
        iterator.next();
        Ok(parsed)
    } else {
        Ok((VariableType::Float, None))
    }
}

fn try_parse_name<'a, I: Iterator<Item = &'a str>>(
    iterator: &mut Peekable<I>,
) -> std::result::Result<String, ()> {
    let name_token = iterator.next().ok_or(())?;
    if name_token.starts_with('%') || name_token.starts_with('"') || name_token.starts_with('_') {
        return Err(());
    }
    let name = name_token.to_string();
    Ok(name)
}

fn try_parse_input_format<'a, I: Iterator<Item = &'a str>>(
    iterator: &mut Peekable<I>,
    line_number: usize,
) -> Result<Option<InputFormat>> {
    let Some(&token) = iterator.peek() else {
        return Ok(None);
    };
    if !token.starts_with('%') {
        return Ok(None);
    }
    let input_format = parse_input_format(token).ok_or_else(|| DctError::InvalidReadFormat {
        line: line_number,
        format: token.to_string(),
    })?;
    let input_format = Some(input_format);
    iterator.next();
    Ok(input_format)
}

fn parse_label<'a, I: Iterator<Item = &'a str>>(iterator: &mut Peekable<I>) -> Option<String> {
    let &token = iterator.peek()?;
    if !token.starts_with('"') {
        return None;
    }
    let label = unquote(token).to_string();
    let label = Some(label);
    iterator.next();
    label
}

fn select_fallback_input_format(
    storage_type: VariableType,
    storage_str_width: Option<usize>,
) -> InputFormat {
    match (storage_type, storage_str_width) {
        (VariableType::String, Some(width)) => InputFormat::FixedString { width },
        (VariableType::String, None) => InputFormat::FreeString,
        _ => InputFormat::FreeNumeric,
    }
}

fn parse_column_directive(token: &str) -> Option<usize> {
    directive_argument(token, "_column").and_then(|inner| inner.parse().ok())
}

fn parse_storage_type(token: &str) -> Option<(VariableType, Option<usize>)> {
    match token {
        "byte" => Some((VariableType::Byte, None)),
        "int" => Some((VariableType::Int, None)),
        "long" => Some((VariableType::Long, None)),
        "float" => Some((VariableType::Float, None)),
        "double" => Some((VariableType::Double, None)),
        "str" => Some((VariableType::String, None)),
        token if token.starts_with("str") => {
            let width: usize = token[3..].parse().ok()?;
            Some((VariableType::String, Some(width)))
        }
        _ => None,
    }
}

fn parse_input_format(token: &str) -> Option<InputFormat> {
    let body = token.strip_prefix('%')?;
    if body.is_empty() {
        return None;
    }

    let last = body.chars().last()?;
    if !matches!(last, 'f' | 'g' | 'e' | 's') {
        return None;
    }
    let prefix = &body[..body.len() - last.len_utf8()];

    if last == 's' {
        if prefix.is_empty() {
            return Some(InputFormat::FreeString);
        }
        let width: usize = prefix.parse().ok()?;
        return Some(InputFormat::FixedString { width });
    }

    let style = match last {
        'f' => NumericStyle::Fixed,
        'g' => NumericStyle::General,
        'e' => NumericStyle::Scientific,
        _ => unreachable!(),
    };

    if prefix.is_empty() {
        return Some(InputFormat::FreeNumeric);
    }

    let (width_str, decimals) = match prefix.find('.') {
        Some(idx) => {
            let dec: u8 = prefix[idx + 1..].parse().ok()?;
            (&prefix[..idx], dec)
        }
        None => (prefix, 0),
    };

    let width: usize = width_str.parse().ok()?;
    Some(InputFormat::FixedNumeric {
        width,
        decimals,
        style,
    })
}
