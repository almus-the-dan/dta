use std::fs::File;
use std::io::{BufRead, BufReader};
use std::iter::Peekable;
use std::ops::Range;
use std::path::Path;

use crate::stata::dct::column::Column;
use crate::stata::dct::dct_error::{DctError, Result};
use crate::stata::dct::dct_reader::DctReader;
use crate::stata::dct::dct_source::DctSource;
use crate::stata::dct::dct_warning::DctWarning;
use crate::stata::dct::input_format::InputFormat;
use crate::stata::dct::numeric_style::NumericStyle;
use crate::stata::dct::schema::Schema;
use crate::stata::dct::variable_type::VariableType;

/// Parses a `.dct` dictionary from a buffered reader.
///
/// On success the returned [`DctSource`] indicates whether the
/// associated data file is embedded in the same source (data follows
/// the closing `}`) or external (referenced by the dictionary's
/// `using` clause, or supplied separately by the caller).
///
/// # Errors
///
/// Returns [`DctError`] when an I/O error occurs, the dictionary
/// ends before its closing `}`, the opening `dictionary {` line is
/// malformed, or any directive fails to parse.
pub fn parse_dct<R: BufRead>(reader: R) -> Result<DctSource<R>> {
    let mut cursor = LineCursor::new(reader);
    let declared_data_path = parse_dictionary_header(&mut cursor)?;
    let body = parse_body(&mut cursor)?;

    let mut warnings = body.warnings;
    if let Some(path) = &declared_data_path {
        let warning = DctWarning::DeclaredPathIgnored { path: path.clone() };
        warnings.push(warning);
    }

    let schema = Schema::new(
        body.columns,
        body.logical_record_length,
        body.first_line_of_file,
        body.lines_per_observation,
        declared_data_path,
        warnings,
    );

    let mut inner = cursor.into_inner();
    let source = if has_more_data(&mut inner)? {
        DctSource::Embedded(DctReader::new(schema, inner))
    } else {
        DctSource::External(schema)
    };
    Ok(source)
}

/// Opens the file at `path` and parses it as a `.dct` dictionary.
///
/// # Errors
///
/// Returns [`DctError`] if the file cannot be opened or its contents
/// fail to parse.
pub fn open_dct<P: AsRef<Path>>(path: P) -> Result<DctSource<BufReader<File>>> {
    let file = File::open(path)?;
    let reader = BufReader::new(file);
    parse_dct(reader)
}

fn has_more_data<R: BufRead>(reader: &mut R) -> Result<bool> {
    let buffer = reader.fill_buf()?;
    Ok(!buffer.is_empty())
}

struct LineCursor<R: BufRead> {
    inner: R,
    line_number: usize,
    buffer: String,
}

impl<R: BufRead> LineCursor<R> {
    #[must_use]
    fn new(inner: R) -> Self {
        Self {
            inner,
            line_number: 0,
            buffer: String::new(),
        }
    }

    /// Reads the next line into the internal buffer, stripping a
    /// trailing `\r\n` or `\n`. Returns `Ok(true)` when a line was
    /// read, `Ok(false)` at the end of input.
    ///
    /// Classic Mac `\r`-only line endings are not handled as
    /// separators because [`BufRead::read_line`] only breaks on
    /// `\n` — a `\r`-delimited file would arrive as a single line.
    /// Stata never emitted DCT files with `\r`-only endings, so we
    /// accept the limitation rather than read byte-by-byte. A bare
    /// trailing `\r` at the end of an otherwise-newline-terminated
    /// line still gets stripped via the `ends_with` check.
    fn read_line(&mut self) -> Result<bool> {
        self.buffer.clear();
        let n = self.inner.read_line(&mut self.buffer)?;
        if n == 0 {
            return Ok(false);
        }
        self.line_number += 1;
        while self.buffer.ends_with(['\n', '\r']) {
            self.buffer.pop();
        }
        Ok(true)
    }

    fn current(&self) -> &str {
        &self.buffer
    }

    fn line_number(&self) -> usize {
        self.line_number
    }

    fn into_inner(self) -> R {
        self.inner
    }
}

fn tokenize_lines<R: BufRead>(cursor: &mut LineCursor<R>) -> Result<Vec<String>> {
    let mut accumulated: Vec<String> = Vec::new();
    let mut ranges: Vec<Range<usize>> = Vec::new();

    loop {
        if !cursor.read_line()? {
            return Err(DctError::UnexpectedEofInDictionary);
        }
        let line = cursor.current().trim_ascii_start();
        if line.starts_with('*') {
            continue;
        }
        tokenize(line, &mut ranges);

        let mut found_brace = false;
        for range in &ranges {
            let token = token(line, range);
            if token == "{" {
                found_brace = true;
            }
            accumulated.push(token.to_string());
        }

        if found_brace {
            break;
        }
    }
    Ok(accumulated)
}

/// Splits a line into whitespace-separated tokens, treating a
/// double-quoted run as a single token (quotes included). Each
/// token is recorded as a byte range within `line`.
///
/// The `ranges` variable is cleared and refilled in place so the caller's
/// allocation can be reused across many lines. We store byte ranges
/// rather than `&str` because a `Vec<&str>` would carry the line's
/// lifetime in its type, which prevents reusing the buffer once the
/// line is released and a new one is read.
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

/// Reads lines until the opening `{` is consumed. Returns the
/// `using` path if one was declared.
fn parse_dictionary_header<R: BufRead>(cursor: &mut LineCursor<R>) -> Result<Option<String>> {
    let accumulated = tokenize_lines(cursor)?;

    let header_line = cursor.line_number();
    let invalid = || DctError::InvalidDictionaryHeader {
        line: header_line,
        content: accumulated.join(" "),
    };

    let mut iter = accumulated.iter().map(String::as_str).peekable();

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

    Ok(using_path)
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

struct BodyData {
    columns: Vec<Column>,
    logical_record_length: Option<usize>,
    first_line_of_file: Option<usize>,
    lines_per_observation: usize,
    warnings: Vec<DctWarning>,
}

enum Directive {
    LogicalRecordLength(usize),
    FirstLineOfFile(usize),
    Newline,
}

fn parse_body<R: BufRead>(cursor: &mut LineCursor<R>) -> Result<BodyData> {
    let mut cursor_offset: usize = 0;
    let mut ranges: Vec<Range<usize>> = Vec::new();

    let mut data = BodyData {
        columns: Vec::new(),
        logical_record_length: None,
        first_line_of_file: None,
        lines_per_observation: 0, // Convert to 1-based later
        warnings: Vec::new(),
    };

    while process_line(cursor, &mut ranges, &mut data, &mut cursor_offset)? {}

    data.lines_per_observation += 1;

    Ok(data)
}

fn process_line<R: BufRead>(
    cursor: &mut LineCursor<R>,
    ranges: &mut Vec<Range<usize>>,
    data: &mut BodyData,
    cursor_offset: &mut usize,
) -> Result<bool> {
    if !cursor.read_line()? {
        return Err(DctError::UnexpectedEofInDictionary);
    }
    let trimmed = cursor.current().trim_ascii();

    if trimmed.is_empty() || trimmed.starts_with('*') {
        return Ok(true);
    }
    if trimmed == "}" {
        return Ok(false);
    }

    tokenize(trimmed, ranges);
    let Some(first_range) = ranges.first() else {
        return Ok(true);
    };
    let first = token(trimmed, first_range);
    let line_number = cursor.line_number();
    if process_directive(first, line_number, data, cursor_offset)? {
        return Ok(true);
    }

    if looks_like_variable_line(first) {
        let column = parse_variable_line(
            ranges,
            trimmed,
            line_number,
            data.lines_per_observation,
            cursor_offset,
        )?;
        data.columns.push(column);
        return Ok(true);
    }

    let warning = DctWarning::UnrecognizedDirective {
        line: line_number,
        content: trimmed.to_string(),
    };
    data.warnings.push(warning);
    Ok(true)
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

    let mut iterator = ranges.iter().map(|range| token(line, range)).peekable();
    // Optional `_column(#)` resets to an absolute byte position;
    // optional `_skip(#)` then advances relatively. Either, both, or
    // neither may appear — neither means "start at the running
    // column pointer".
    let offset = compute_variable_offset(&mut iterator, *cursor_offset).map_err(|()| invalid())?;
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

    *cursor_offset = advance_cursor(offset, input_format);

    let column = Column::new(line_offset, offset, storage_type, name, input_format, label);
    Ok(column)
}

/// Resolves the variable's starting byte offset. Consumes a leading
/// `_column(#)` token if present (absolute), then a leading
/// `_skip(#)` token if present (relative). When neither appears, the
/// offset is the running cursor.
fn compute_variable_offset<'a, I: Iterator<Item = &'a str>>(
    iterator: &mut Peekable<I>,
    cursor_offset: usize,
) -> std::result::Result<usize, ()> {
    let mut offset = cursor_offset;
    if let Some(&token) = iterator.peek()
        && token.starts_with("_column(")
    {
        iterator.next();
        let one_based = parse_column_directive(token).ok_or(())?;
        if one_based < 1 {
            return Err(());
        }
        offset = one_based - 1;
    }
    if let Some(&token) = iterator.peek()
        && let Some(skip) = parse_skip_modifier(token)
    {
        iterator.next();
        offset = offset.saturating_add(skip);
    }
    Ok(offset)
}

/// Returns the column pointer's position after a variable using
/// `input_format` is read from `offset`. Fixed-width formats consume
/// exactly `width` bytes; free-format reads consume input dynamically
/// at runtime, so the parser-time cursor stays where the variable
/// started. Downstream variables that need to follow a free-format
/// read should anchor explicitly with `_column(#)`.
fn advance_cursor(offset: usize, input_format: InputFormat) -> usize {
    match input_format {
        InputFormat::FixedNumeric { width, .. } | InputFormat::FixedString { width } => {
            offset.saturating_add(width)
        }
        InputFormat::FreeNumeric | InputFormat::FreeString => offset,
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    fn parse(input: &[u8]) -> Result<DctSource<Cursor<&[u8]>>> {
        parse_dct(Cursor::new(input))
    }

    #[test]
    fn parses_minimal_dictionary() {
        let src = parse(b"dictionary {\n_column(1) myvar\n}\n").unwrap();
        let schema = src.schema();
        assert_eq!(schema.columns().len(), 1);
        assert_eq!(schema.columns()[0].name(), "myvar");
        assert_eq!(schema.columns()[0].offset(), 0);
        assert_eq!(schema.columns()[0].storage_type(), VariableType::Float);
        assert!(matches!(
            schema.columns()[0].input_format(),
            InputFormat::FreeNumeric
        ));
        assert_eq!(schema.declared_data_path(), None);
        assert!(matches!(src, DctSource::External(_)));
    }

    #[test]
    fn parses_using_clause_emits_path_warning() {
        let src = parse(b"dictionary using foo.raw {\n_column(1) v\n}\n").unwrap();
        assert_eq!(src.schema().declared_data_path(), Some("foo.raw"));
        assert!(
            src.schema().warnings().iter().any(
                |w| matches!(w, DctWarning::DeclaredPathIgnored { path } if path == "foo.raw")
            )
        );
    }

    #[test]
    fn parses_quoted_using_path() {
        let src = parse(b"dictionary using \"path with spaces.raw\" {\n_column(1) v\n}\n").unwrap();
        assert_eq!(
            src.schema().declared_data_path(),
            Some("path with spaces.raw")
        );
    }

    #[test]
    fn parses_infile_prefix() {
        let src = parse(b"infile dictionary using foo.raw {\n_column(1) v\n}\n").unwrap();
        assert_eq!(src.schema().declared_data_path(), Some("foo.raw"));
    }

    #[test]
    fn handles_brace_on_separate_line() {
        let src = parse(b"dictionary using foo.raw\n{\n_column(1) v\n}\n").unwrap();
        assert_eq!(src.schema().declared_data_path(), Some("foo.raw"));
        assert_eq!(src.schema().columns().len(), 1);
    }

    #[test]
    fn skips_comments_inside_and_around_dictionary() {
        let src = parse(
            b"* leading comment\n\
              dictionary {\n\
              * inline comment\n\
              _column(1) v\n\
              }\n",
        )
        .unwrap();
        assert_eq!(src.schema().columns().len(), 1);
    }

    #[test]
    fn parses_storage_types_and_formats() {
        let dict = b"dictionary {\n\
            _column(1) byte b1 %3.0f \"a byte\"\n\
            _column(4) int i1\n\
            _column(6) long l1 %10.0f\n\
            _column(16) float f1 %9.2f\n\
            _column(25) double d1\n\
            _column(33) str s1 %20s\n\
            _column(53) str10 s2\n\
            _column(63) str s3 \"free string\"\n\
            }\n";
        let src = parse(dict).unwrap();
        let cols = src.schema().columns();
        assert_eq!(cols.len(), 8);

        assert_eq!(cols[0].storage_type(), VariableType::Byte);
        assert_eq!(cols[0].label(), Some("a byte"));
        assert!(matches!(
            cols[0].input_format(),
            InputFormat::FixedNumeric {
                width: 3,
                decimals: 0,
                style: NumericStyle::Fixed,
            }
        ));

        assert_eq!(cols[1].storage_type(), VariableType::Int);
        assert!(matches!(cols[1].input_format(), InputFormat::FreeNumeric));

        assert_eq!(cols[2].storage_type(), VariableType::Long);
        assert_eq!(cols[3].storage_type(), VariableType::Float);
        assert_eq!(cols[4].storage_type(), VariableType::Double);

        assert_eq!(cols[5].storage_type(), VariableType::String);
        assert!(matches!(
            cols[5].input_format(),
            InputFormat::FixedString { width: 20 }
        ));

        // str10 (storage hint, no %infmt) → FixedString { 10 }
        assert_eq!(cols[6].storage_type(), VariableType::String);
        assert!(matches!(
            cols[6].input_format(),
            InputFormat::FixedString { width: 10 }
        ));

        // bare str (no width, no %infmt) → FreeString. Has label.
        assert_eq!(cols[7].storage_type(), VariableType::String);
        assert!(matches!(cols[7].input_format(), InputFormat::FreeString));
        assert_eq!(cols[7].label(), Some("free string"));
    }

    #[test]
    fn parses_lrecl() {
        let src = parse(b"dictionary {\nlrecl(80)\n_column(1) v\n}\n").unwrap();
        assert_eq!(src.schema().logical_record_length(), Some(80));
    }

    #[test]
    fn parses_first_line_of_file() {
        let src = parse(b"dictionary {\nfirstlineoffile(5)\n_column(1) v\n}\n").unwrap();
        assert_eq!(src.schema().first_line_of_file(), Some(5));
    }

    #[test]
    fn rejects_duplicate_lrecl() {
        let result = parse(b"dictionary {\nlrecl(80)\nlrecl(120)\n_column(1) v\n}\n");
        assert!(matches!(
            result,
            Err(DctError::DuplicateDirective { directive, .. }) if directive == "lrecl"
        ));
    }

    #[test]
    fn skip_modifier_advances_cursor_relatively() {
        // _skip(M) without _column anchors relative to the running
        // cursor, which is itself advanced by each fixed-width read.
        let dict = b"dictionary {\n\
            _column(1) byte b1 %3.0f\n\
            _skip(2) byte b2 %3.0f\n\
            }\n";
        let src = parse(dict).unwrap();
        let cols = src.schema().columns();
        assert_eq!(cols.len(), 2);
        assert_eq!(cols[0].offset(), 0);
        // b1 finishes at byte 3 (0 + width 3); _skip(2) → 5.
        assert_eq!(cols[1].offset(), 5);
    }

    #[test]
    fn column_then_skip_combines() {
        // _column(N) sets absolute, then _skip(M) advances from there.
        let dict = b"dictionary {\n\
            _column(10) _skip(3) byte b1 %3.0f\n\
            }\n";
        let src = parse(dict).unwrap();
        let cols = src.schema().columns();
        // _column(10) → 0-based 9, then _skip(3) → 12.
        assert_eq!(cols[0].offset(), 12);
    }

    #[test]
    fn bare_skip_advances_one_byte() {
        let dict = b"dictionary {\n\
            _column(1) byte b1 %3.0f\n\
            _skip byte b2 %3.0f\n\
            }\n";
        let src = parse(dict).unwrap();
        let cols = src.schema().columns();
        // b1 ends at 3; bare _skip → 4.
        assert_eq!(cols[1].offset(), 4);
    }

    #[test]
    fn variable_with_no_anchors_uses_running_cursor() {
        // Bare-storage continuation lines pick up where the previous
        // variable ended.
        let dict = b"dictionary {\n\
            _column(1) byte b1 %3.0f\n\
            byte b2 %3.0f\n\
            byte b3 %3.0f\n\
            }\n";
        let src = parse(dict).unwrap();
        let cols = src.schema().columns();
        assert_eq!(cols.len(), 3);
        assert_eq!(cols[0].offset(), 0);
        assert_eq!(cols[1].offset(), 3);
        assert_eq!(cols[2].offset(), 6);
    }

    #[test]
    fn newline_resets_cursor_to_line_start() {
        let dict = b"dictionary {\n\
            _column(1) byte b1 %3.0f\n\
            _skip(5) byte b2 %3.0f\n\
            _newline\n\
            _skip(2) int i1 %5.0f\n\
            }\n";
        let src = parse(dict).unwrap();
        let cols = src.schema().columns();
        assert_eq!(cols[0].line_offset(), 0);
        assert_eq!(cols[0].offset(), 0);
        // b1 ends at 3, _skip(5) → 8.
        assert_eq!(cols[1].line_offset(), 0);
        assert_eq!(cols[1].offset(), 8);
        // _newline resets cursor to 0 on a new physical line, then
        // _skip(2) → 2.
        assert_eq!(cols[2].line_offset(), 1);
        assert_eq!(cols[2].offset(), 2);
    }

    #[test]
    fn detects_embedded_data() {
        let src = parse(b"dictionary {\n_column(1) v\n}\n42\n").unwrap();
        assert!(matches!(src, DctSource::Embedded(_)));
    }

    #[test]
    fn detects_external_data() {
        let src = parse(b"dictionary {\n_column(1) v\n}\n").unwrap();
        assert!(matches!(src, DctSource::External(_)));
    }

    #[test]
    fn handles_crlf_line_endings() {
        let src = parse(b"dictionary {\r\n_column(1) v\r\n}\r\n").unwrap();
        assert_eq!(src.schema().columns().len(), 1);
    }

    #[test]
    fn errors_on_missing_closing_brace() {
        let result = parse(b"dictionary {\n_column(1) v\n");
        assert!(matches!(result, Err(DctError::UnexpectedEofInDictionary)));
    }

    #[test]
    fn errors_on_missing_dictionary_keyword() {
        let result = parse(b"random {\n_column(1) v\n}\n");
        assert!(matches!(
            result,
            Err(DctError::InvalidDictionaryHeader { .. })
        ));
    }

    #[test]
    fn errors_on_invalid_column_directive() {
        let result = parse(b"dictionary {\n_column(abc) v\n}\n");
        assert!(matches!(
            result,
            Err(DctError::InvalidColumnDirective { .. })
        ));
    }

    #[test]
    fn errors_on_zero_column_value() {
        let result = parse(b"dictionary {\n_column(0) v\n}\n");
        assert!(matches!(
            result,
            Err(DctError::InvalidColumnDirective { .. })
        ));
    }

    #[test]
    fn errors_on_invalid_input_format() {
        let result = parse(b"dictionary {\n_column(1) byte v %5.2x\n}\n");
        assert!(matches!(result, Err(DctError::InvalidReadFormat { .. })));
    }

    #[test]
    fn parses_multi_line_observation() {
        let dict = b"dictionary {\n\
            _column(1) byte b1\n\
            _newline\n\
            _column(1) int i1\n\
            _column(5) double d1\n\
            }\n";
        let src = parse(dict).unwrap();
        let schema = src.schema();
        assert_eq!(schema.lines_per_observation(), 2);
        let cols = schema.columns();
        assert_eq!(cols.len(), 3);
        assert_eq!(cols[0].line_offset(), 0);
        assert_eq!(cols[0].offset(), 0);
        assert_eq!(cols[1].line_offset(), 1);
        assert_eq!(cols[1].offset(), 0);
        assert_eq!(cols[2].line_offset(), 1);
        assert_eq!(cols[2].offset(), 4);
    }

    #[test]
    fn newline_with_no_following_columns_still_advances() {
        // Trailing _newline implies an extra physical line per observation
        // (e.g. data files with a footer line per record).
        let dict = b"dictionary {\n\
            _column(1) byte b1\n\
            _newline\n\
            _newline\n\
            _column(1) int i1\n\
            }\n";
        let src = parse(dict).unwrap();
        assert_eq!(src.schema().lines_per_observation(), 3);
        let cols = src.schema().columns();
        assert_eq!(cols[0].line_offset(), 0);
        assert_eq!(cols[1].line_offset(), 2);
    }

    #[test]
    fn single_line_observation_reports_one_line() {
        let src = parse(b"dictionary {\n_column(1) v\n}\n").unwrap();
        assert_eq!(src.schema().lines_per_observation(), 1);
        assert_eq!(src.schema().columns()[0].line_offset(), 0);
    }

    #[test]
    fn warns_on_unrecognized_directive() {
        let src = parse(b"dictionary {\n_column(1) v\nfoobar baz\n}\n").unwrap();
        assert!(
            src.schema()
                .warnings()
                .iter()
                .any(|w| matches!(w, DctWarning::UnrecognizedDirective { .. }))
        );
    }

    #[test]
    fn input_format_variants() {
        assert!(matches!(
            parse_input_format("%5.2f"),
            Some(InputFormat::FixedNumeric {
                width: 5,
                decimals: 2,
                style: NumericStyle::Fixed,
            })
        ));
        assert!(matches!(
            parse_input_format("%5g"),
            Some(InputFormat::FixedNumeric {
                width: 5,
                decimals: 0,
                style: NumericStyle::General,
            })
        ));
        assert!(matches!(
            parse_input_format("%5e"),
            Some(InputFormat::FixedNumeric {
                width: 5,
                decimals: 0,
                style: NumericStyle::Scientific,
            })
        ));
        assert!(matches!(
            parse_input_format("%f"),
            Some(InputFormat::FreeNumeric)
        ));
        assert!(matches!(
            parse_input_format("%g"),
            Some(InputFormat::FreeNumeric)
        ));
        assert!(matches!(
            parse_input_format("%5s"),
            Some(InputFormat::FixedString { width: 5 })
        ));
        assert!(matches!(
            parse_input_format("%s"),
            Some(InputFormat::FreeString)
        ));
        assert!(parse_input_format("%5.2x").is_none());
        assert!(parse_input_format("not_a_format").is_none());
        assert!(parse_input_format("%").is_none());
    }
}
