use byteorder::{ByteOrder, LE};
use object_store::{ObjectStore, ObjectMeta};
use bytes::Bytes;

pub(crate) struct Cursor {
    store: Box<dyn ObjectStore>,
    meta: ObjectMeta,
}

#[derive(Debug, Copy, Clone)]
pub struct OutOfBounds;

impl Cursor {
    pub fn new(store: Box<dyn ObjectStore>, meta: ObjectMeta) -> Self {
        Self { store, meta}
    }

    pub fn len(&self) -> usize {
        self.meta.size
    }

    pub async fn read_bytes(&mut self, pos: usize, n: usize) -> Result<Bytes, OutOfBounds> {
        if self.pos + n > self.len() {
            return Err(OutOfBounds);
        }
        self.store.get_range(&self.meta.location, pos..pos + n)?
    }

    pub async fn read_chunk(&mut self, pos: usize) -> Result<Bytes, OutOfBounds> {
        let n = self.next_u32()? as usize;
        self.read_bytes(pos + 4, n).await
    }

    pub fn read_u32(&mut self, pos: usize) -> Result<u32, OutOfBounds> {
        Ok(LE::read_u32(self.store.get_range(&self.meta.location, pos..pos + 4)?))
    }

    /*
    pub fn next_u64(&mut self) -> Result<u64, OutOfBounds> {
        Ok(LE::read_u64(self.next_bytes(4)?))
    }
    */

    pub fn read_time(&mut self, pos: usize) -> Result<u64, OutOfBounds> {
        let s = self.read_u32(pos)? as u64;
        let ns = self.read_u32(pos + 4)? as u64;
        Ok(1_000_000_000 * s + ns)
    }
}
