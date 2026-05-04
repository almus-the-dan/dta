/// Strips trailing `\n`, `\r`, or `\r\n` bytes from `buffer` in
/// place.
///
/// [`std::io::BufRead::read_line`] includes the terminating `\n` in
/// its output and, on CRLF input, the preceding `\r` as well. This
/// helper normalizes both, plus the rarer case of a bare trailing
/// `\r` at the end of input. Classic Mac `\r`-only line separators
/// between lines are not handled — see the module-level note about
/// line endings in `dct/mod.rs`.
pub(super) fn strip_terminator(buffer: &mut String) {
    while buffer.ends_with(['\n', '\r']) {
        buffer.pop();
    }
}
