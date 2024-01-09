use bytes::Bytes;

use super::utils::{set_field_u32, unknown_field};
use super::{HeaderGen, RecordGen};
use anyhow::Result;
use crate::error::RosError;

use crate::cursor::Cursor;

/// Index record which describes messages offset for `Connection` with
/// `conn_id` ID in the preceding `Chunk`.
#[derive(Debug, Clone)]
pub struct IndexData {
    /// Index data record version (only version 1 is currently cupported)
    pub ver: u32,
    /// Connection ID
    pub conn_id: u32,
    /// Occurrences of timestamps, chunk record offsets and message offsets
    data: Bytes,
}

impl IndexData {
    /// Get entries iterator.
    pub fn entries(&self) -> IndexDataEntriesIterator {
        IndexDataEntriesIterator {
            cursor: Cursor::new(self.data.clone()),
        }
    }
}

#[derive(Default)]
pub(crate) struct IndexDataHeader {
    pub ver: Option<u32>,
    pub conn_id: Option<u32>,
    pub count: Option<u32>,
}

impl RecordGen for IndexData {
    type Header = IndexDataHeader;

    fn read_data(c: &mut Cursor, header: Self::Header) -> Result<Self> {
        let ver = header.ver.ok_or(anyhow::Error::new(RosError::InvalidHeader))?;
        let conn_id = header.conn_id.ok_or(anyhow::Error::new(RosError::InvalidHeader))?;
        let count = header.count.ok_or(anyhow::Error::new(RosError::InvalidHeader))?;

        if ver != 1 {
            return Err(RosError::UnsupportedVersion.into());
        }
        let n = c.next_u32()?;
        if n % 12 != 0 || n / 12 != count {
            return Err(RosError::InvalidRecord.into());
        }
        let data = c.next_bytes(n as u64)?;
        Ok(Self { ver, conn_id, data })
    }
}

impl HeaderGen for IndexDataHeader {
    const OP: u8 = 0x04;

    fn process_field(&mut self, name: &str, val: &[u8]) -> Result<()> {
        match name {
            "ver" => set_field_u32(&mut self.ver, val)?,
            "conn" => set_field_u32(&mut self.conn_id, val)?,
            "count" => set_field_u32(&mut self.count, val)?,
            _ => unknown_field(name, val),
        }
        Ok(())
    }
}

/// Index entry which contains message offset and its timestamp.
#[derive(Debug, Clone, Default)]
pub struct IndexDataEntry {
    /// Time at which the message was received
    pub time: u64,
    /// Offset of message data record in uncompressed chunk data
    pub offset: u32,
}

/// Iterator over `IndexData` entries
pub struct IndexDataEntriesIterator {
    cursor: Cursor,
}

impl Iterator for IndexDataEntriesIterator {
    type Item = IndexDataEntry;

    fn next(&mut self) -> Option<IndexDataEntry> {
        if self.cursor.left() == 0 {
            return None;
        }
        if self.cursor.left() < 12 {
            panic!("unexpected data leftover for entries")
        }
        let time = self.cursor.next_time().expect("already checked");
        let offset = self.cursor.next_u32().expect("already checked");
        Some(IndexDataEntry { time, offset })
    }
}
