use anyhow::Result;

use crate::cursor::Cursor;

use crate::field_iter::FieldIterator;
use crate::record_types::{Chunk, ChunkInfo, Connection, IndexData, MessageData, RecordGen};
use crate::error::RosError;

/// Enum with all possible record variants
#[derive(Debug, Clone)]
pub(crate) enum Record {
    Chunk(Chunk),
    Connection(Connection),
    MessageData(MessageData),
    IndexData(IndexData),
    ChunkInfo(ChunkInfo),
}

impl Record {
    pub(crate) fn next_record(c: &mut Cursor) -> Result<Self> {
        let header = c.next_chunk()?;

        let mut op = None;
        for item in FieldIterator::new(header.clone()) {
            let (name, val) = item?;
            if name == "op" {
                if val.len() == 1 {
                    op = Some(val[0]);
                    break;
                } else {
                    return Err(RosError::InvalidRecord.into());
                }
            }
        }

        Ok(match op {
            Some(IndexData::OP) => Record::IndexData(IndexData::read(header, c)?),
            Some(Chunk::OP) => Record::Chunk(Chunk::read(header, c)?),
            Some(ChunkInfo::OP) => Record::ChunkInfo(ChunkInfo::read(header, c)?),
            Some(Connection::OP) => Record::Connection(Connection::read(header, c)?),
            Some(MessageData::OP) => Record::MessageData(MessageData::read(header, c)?),
            _ => return Err(RosError::InvalidRecord.into()),
        })
    }

    /// Get string name of the stored recrod type.
    pub fn get_type(&self) -> &'static str {
        match self {
            Record::Chunk(_) => "Chunk",
            Record::Connection(_) => "Connection",
            Record::MessageData(_) => "MessageData",
            Record::IndexData(_) => "IndexData",
            Record::ChunkInfo(_) => "ChunkInfo",
        }
    }
}
