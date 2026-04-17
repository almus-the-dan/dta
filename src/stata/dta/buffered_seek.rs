use std::io::{Result, Seek, SeekFrom, Write};

/// In-memory adapter that adds [`Seek`] to a write-only sink.
///
/// Wraps a [`Write`]-only sink, collecting all writes into an internal
/// buffer so that the writer chain (which requires `Write + Seek` to
/// patch the XML `<map>` offsets) can target sinks that don't support
/// seeking.
///
/// The entire DTA file is buffered in memory until
/// [`into_inner`](Self::into_inner) is called, which flushes the
/// buffer to the underlying sink and returns it. For seekable sinks
/// (e.g. [`File`](std::fs::File), [`Cursor`](std::io::Cursor)), pass
/// them directly to [`DtaWriter`](super::dta_writer::DtaWriter)
/// without this wrapper to avoid the extra memory cost.
#[derive(Debug)]
pub struct BufferedSeek<W: Write> {
    sink: W,
    buffer: Vec<u8>,
    position: u64,
}

impl<W: Write> BufferedSeek<W> {
    /// Wraps the given sink.
    #[must_use]
    #[inline]
    pub fn new(_sink: W) -> Self {
        todo!()
    }

    /// Flushes the buffered bytes to the underlying sink and returns it.
    ///
    /// # Errors
    ///
    /// Returns any I/O error produced while writing the buffer to the
    /// underlying sink.
    pub fn into_inner(self) -> Result<W> {
        todo!()
    }
}

impl<W: Write> Write for BufferedSeek<W> {
    fn write(&mut self, _data: &[u8]) -> Result<usize> {
        todo!()
    }

    fn flush(&mut self) -> Result<()> {
        todo!()
    }
}

impl<W: Write> Seek for BufferedSeek<W> {
    fn seek(&mut self, _pos: SeekFrom) -> Result<u64> {
        todo!()
    }
}
