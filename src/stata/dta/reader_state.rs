use encoding_rs::Encoding;

#[derive(Debug)]
pub(crate) struct ReaderState<R> {
    reader: R,
    encoding: &'static Encoding,
    buffer: Vec<u8>,
    position: u64,
}

impl<R> ReaderState<R> {
    pub(crate) fn new(reader: R, encoding: &'static Encoding) -> Self {
        Self {
            reader,
            encoding,
            buffer: Vec::new(),
            position: 0,
        }
    }
}
