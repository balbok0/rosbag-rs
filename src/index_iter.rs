use crate::record_types::{ChunkInfo, Connection, IndexData};
use crate::{record::Record, Cursor, BagError};
use anyhow::Result;

/// Record types which can be stored in the chunk section.
#[derive(Debug, Clone)]
pub enum IndexRecord {
    /// [`IndexData`] record.
    IndexData(IndexData),
    /// [`Connection`] record.
    Connection(Connection),
    /// [`ChunkInfo`] record.
    ChunkInfo(ChunkInfo),
}

/// Iterator over records stored in the chunk section of a rosbag file.
pub struct IndexRecordsIterator {
    pub(crate) cursor: Cursor,
    pub(crate) offset: u64,
}

impl IndexRecordsIterator {
    /// Jump to the given position in the file.
    ///
    /// Be careful to jump only to record beginnings, as incorrect offset position
    /// will result in error on the next iteration and in the worst case
    /// scenario to a long blocking (program will try to read a huge chunk of
    /// data).
    pub fn seek(&mut self, pos: u64) -> Result<()> {
        if pos < self.offset {
            return Err(BagError::OutOfBounds.into());
        }
        Ok(self.cursor.seek(pos - self.offset)?)
    }
}

impl Iterator for IndexRecordsIterator {
    type Item = Result<IndexRecord>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.cursor.left() == 0 {
            return None;
        }
        let res = match Record::next_record(&mut self.cursor) {
            Ok(Record::IndexData(v)) => Ok(IndexRecord::IndexData(v)),
            Ok(Record::Connection(v)) => Ok(IndexRecord::Connection(v)),
            Ok(Record::ChunkInfo(v)) => Ok(IndexRecord::ChunkInfo(v)),
            Ok(v) => Err(BagError::UnexpectedIndexSectionRecord(v.get_type()).into()),
            Err(e) => Err(e),
        };
        Some(res)
    }
}
