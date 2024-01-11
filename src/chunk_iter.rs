use crate::record_types::{Chunk, IndexData};
use crate::{record::Record, Cursor, BagError, Result};

/// Record types which can be stored in the chunk section.
#[derive(Debug, Clone)]
pub enum ChunkRecord {
    /// [`Chunk`] record.
    Chunk(Chunk),
    /// [`IndexData`] record.
    IndexData(IndexData),
}

/// Iterator over records stored in the chunk section of a rosbag file.
pub struct ChunkRecordsIterator {
    pub(crate) cursor: Cursor,
    pub(crate) offset: u64,
}

impl ChunkRecordsIterator {
    /// Jump to the given position in the file.
    ///
    /// Be careful to jump only to record beginnings (e.g. to position listed
    /// in `ChunkInfo` records), as incorrect offset position
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

impl Iterator for ChunkRecordsIterator {
    type Item = Result<ChunkRecord>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.cursor.left() == 0 {
            return None;
        }
        println!("Chunk iter");
        let res = match Record::next_record(&mut self.cursor) {
            Ok(Record::Chunk(v)) => Ok(ChunkRecord::Chunk(v)),
            Ok(Record::IndexData(v)) => Ok(ChunkRecord::IndexData(v)),
            Ok(v) => Err(BagError::UnexpectedChunkSectionRecord(v.get_type()).into()),
            Err(e) => Err(e),
        };
        Some(res)
    }
}
