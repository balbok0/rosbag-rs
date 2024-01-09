use std::sync::Arc;

use byteorder::{ByteOrder, LE};
use object_store::{ObjectStore, ObjectMeta};
use bytes::Bytes;
use anyhow::Result;
use crate::error::RosError;


pub(crate) struct Cursor {
    data: Bytes,
    pos: u64,
}

impl Cursor {
    pub fn new(data: Bytes) -> Self {
        Self { data, pos: 0 }
    }

    pub fn seek(&mut self, pos: u64) -> Result<()> {
        if pos > self.len() {
            return Err(RosError::OutOfBounds.into());
        }
        self.pos = pos;
        Ok(())
    }

    pub fn pos(&self) -> u64 {
        self.pos
    }

    pub fn len(&self) -> u64 {
        self.data.len() as u64
    }

    pub fn left(&self) -> u64 {
        self.data.len() as u64 - self.pos()
    }

    pub fn next_bytes(&mut self, n: u64) -> Result<Bytes> {
        if self.pos + n > self.len() {
            return Err(RosError::OutOfBounds.into());
        }
        let s = self.pos as usize;
        self.pos += n;
        Ok(self.data.slice(s..self.pos as usize))
    }

    pub fn next_chunk(&mut self) -> Result<Bytes> {
        let n = self.next_u32()? as u64;
        self.next_bytes(n)
    }

    pub fn next_u32(&mut self) -> Result<u32> {
        Ok(LE::read_u32(&self.next_bytes(4)?))
    }

    /*
    pub fn next_u64(&mut self) -> Result<u64, OutOfBounds> {
        Ok(LE::read_u64(self.next_bytes(4)?))
    }
    */

    pub fn next_time(&mut self) -> Result<u64> {
        let s = self.next_u32()? as u64;
        let ns = self.next_u32()? as u64;
        Ok(1_000_000_000 * s + ns)
    }
}


#[derive(Debug, Clone)]
pub(crate) struct ObjectCursor {
    pub(crate) store: Arc<Box<dyn ObjectStore>>,
    pub(crate) meta: ObjectMeta,
}

impl ObjectCursor {
    pub fn new(store: Arc<Box<dyn ObjectStore>>, meta: ObjectMeta) -> Self {
        Self { store, meta}
    }

    pub fn len(&self) -> usize {
        self.meta.size
    }

    pub async fn read_bytes(&self, pos: usize, n: usize) -> Result<Bytes> {
        if pos + n > self.len() {
            return Err(RosError::OutOfBounds.into());
        }
        self.store.get_range(&self.meta.location, pos..pos + n).await.map_err(anyhow::Error::new)
    }

    pub async fn read_chunk(&self, pos: usize) -> Result<Bytes> {
        let n = self.read_u32(pos).await? as usize;
        self.read_bytes(pos + 4, n).await
    }

    pub async fn read_u32(&self, pos: usize) -> Result<u32> {
        Ok(LE::read_u32(&self.store.get_range(&self.meta.location, pos..pos + 4).await?))
    }

    /*
    pub fn next_u64(&mut self) -> Result<u64, OutOfBounds> {
        Ok(LE::read_u64(self.next_bytes(4)?))
    }
    */

    pub async fn read_time(&self, pos: usize) -> Result<u64> {
        let s = self.read_u32(pos).await? as u64;
        let ns = self.read_u32(pos + 4).await? as u64;
        Ok(1_000_000_000 * s + ns)
    }
}
