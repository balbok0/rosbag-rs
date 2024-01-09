use bytes::Bytes;

use super::Result;
use crate::record_types::utils::read_record;
use std::iter::Iterator;

/// Iterator which goes over record header fields
pub(crate) struct FieldIterator {
    buf: Bytes,
}

impl FieldIterator {
    pub(crate) fn new(buf: Bytes) -> Self {
        Self { buf }
    }
}

impl Iterator for FieldIterator {
    type Item = Result<(String, Bytes)>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.buf.is_empty() {
            return None;
        }
        let (name, val, leftover) = match read_record(self.buf.clone()) {
            Ok(v) => v,
            Err(err) => return Some(Err(err)),
        };
        self.buf = leftover;
        Some(Ok((name.to_string(), val)))
    }
}
