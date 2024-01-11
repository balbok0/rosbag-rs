//! Utilities for efficient reading of ROS bag files.
//!
//! # Example
#![warn(missing_docs, rust_2018_idioms)]

use std::{io, str, sync::Arc};
use anyhow::Result;

use object_store::{ObjectStore, parse_url};
use url::Url;

const VERSION_STRING: &str = "#ROSBAG V2.0\n";
const VERSION_LEN: usize = VERSION_STRING.len() as usize;
const ROSBAG_HEADER_SIZE: usize = 4096;
const ROSBAG_HEADER_OP: u8 = 0x03;

mod cursor;
mod error;
mod field_iter;
mod record;

mod chunk_iter;
mod index_iter;
mod msg_iter;
pub mod record_types;

use cursor::{Cursor, ObjectCursor};
use field_iter::FieldIterator;
use record_types::utils::{check_op, set_field_u32, set_field_u64};

pub use chunk_iter::{ChunkRecord, ChunkRecordsIterator};
pub use error::RosError as BagError;
pub use index_iter::{IndexRecord, IndexRecordsIterator};
pub use msg_iter::{MessageRecord, MessageRecordsIterator};

/// Open rosbag file.
pub struct RosBag {
    cursor: ObjectCursor,
    start_pos: usize,
    index_pos: usize,
    conn_count: u32,
    chunk_count: u32,
}

/// Bag file header record which contains basic information about the file.
#[derive(Debug, Clone)]
struct BagHeader {
    /// Offset of first record after the chunk section
    index_pos: u64,
    /// Number of unique connections in the file
    conn_count: u32,
    /// Number of chunk records in the file
    chunk_count: u32,
}

async fn parse_bag_header(cursor: ObjectCursor) -> Result<(usize, BagHeader)> {
    let bytes = cursor.read_bytes(0, VERSION_LEN).await?;

    if bytes != VERSION_STRING.as_bytes() {
        return Err(BagError::InvalidHeader.into());
    }

    let header = cursor.read_chunk(VERSION_LEN).await?;
    let header_len = header.len();

    let mut index_pos: Option<u64> = None;
    let mut conn_count: Option<u32> = None;
    let mut chunk_count: Option<u32> = None;
    let mut op: bool = false;

    for item in FieldIterator::new(header) {
        let (name, val) = item?;
        match name.as_str() {
            "op" => {
                check_op(&val, ROSBAG_HEADER_OP)?;
                op = true;
            }
            "index_pos" => set_field_u64(&mut index_pos, &val)?,
            "conn_count" => set_field_u32(&mut conn_count, &val)?,
            "chunk_count" => set_field_u32(&mut chunk_count, &val)?,
            _ => log::warn!("unexpected field in bag header: {}", name),
        }
    }

    let bag_header = match (index_pos, conn_count, chunk_count, op) {
        (Some(index_pos), Some(conn_count), Some(chunk_count), true) => BagHeader {
            index_pos,
            conn_count,
            chunk_count,
        },
        _ => return Err(BagError::InvalidHeader.into()),
    };

    let data_len = cursor.read_u32(VERSION_LEN + header_len + 4).await? as usize;
    println!("Version len: {VERSION_LEN}");
    println!("Header len: {header_len}");
    println!("Data len: {data_len}");

    Ok((VERSION_LEN + ROSBAG_HEADER_SIZE, bag_header))
}

impl RosBag {
    /// Create a new iterator over provided path to ROS bag file.
    pub async fn new<P: Into<Url>>(path: P) -> io::Result<Self> {
        let (store, path) = parse_url(&path.into())?;
        let store = Arc::new(store);
        let cursor = ObjectCursor::new( store.clone(), store.head(&path).await?);

        let (start_pos, header) = parse_bag_header(cursor.clone()).await.map_err(|_| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                "Invalid or unsupported rosbag header",
            )
        })?;

        Ok(Self {
            cursor,
            start_pos: start_pos.try_into().unwrap(),
            conn_count: header.conn_count,
            index_pos: header.index_pos.try_into().unwrap(),
            chunk_count: header.chunk_count,
        })
    }

    /// Get connection count in this rosbag file.
    pub fn get_conn_count(&self) -> u32 {
        self.conn_count
    }

    /// Get chunk count in this rosbag file.
    pub fn get_chunk_count(&self) -> u32 {
        self.chunk_count
    }

    /// Get iterator over records in the chunk section.
    pub async fn chunk_records(&self) -> ChunkRecordsIterator {
        // let cursor = Cursor::new(&self.data[self.start_pos..self.index_pos]);
        // FIXME: This will be too large.
        // Simple fix is to add logic around this iterator (i.e. a super iterator of sorts, which would load only chunks for the next X bytes)
        // Then keep the current logic

        let cursor = Cursor::new(self.cursor.read_bytes(self.start_pos, self.index_pos - self.start_pos).await.unwrap());
        ChunkRecordsIterator {
            cursor,
            offset: self.start_pos as u64,
        }
    }

    /// Get iterator over records in the index section.
    pub async fn index_records(&self) -> IndexRecordsIterator {
        let cursor = Cursor::new(self.cursor.read_bytes(self.index_pos, self.cursor.len() - self.index_pos).await.unwrap());
        IndexRecordsIterator {
            cursor,
            offset: self.index_pos as u64,
        }
    }
}

#[cfg(test)]
mod tests {
    // use crate::rosbag::{ChunkRecord, MessageRecord, IndexRecord, RosBag};
    use super::*;
    use url::Url;
    use anyhow::Result;

    #[tokio::test]
    async fn general_use() -> Result<()> {
        let path = "/data/disk0/20210828_heightmaps_1/20210828_9.bag";
        let path_url = Url::from_file_path(&path).map_err(|_| anyhow::anyhow! { "Could not parse path to url" })?;
        let bag = RosBag::new(path_url).await?;
        // Iterate over records in the chunk section
        for record in bag.chunk_records().await {
            match record? {
                ChunkRecord::Chunk(chunk) => {
                    // iterate over messages in the chunk
                    for msg in chunk.messages() {
                        match msg? {
                            MessageRecord::MessageData(msg_data) => {
                                println!("Message Data");
                                ()
                            }
                            MessageRecord::Connection(conn) => {
                                println!("Connection");
                                ()
                            }
                        }
                    }
                },
                ChunkRecord::IndexData(index_data) => {
                    println!("IndexData");
                    ()
                },
            }
        }
        // Iterate over records in the index section
        for record in bag.index_records().await {
            match record? {
                IndexRecord::IndexData(index_data) => {
                    println!("IIR::IndexData");
                    ()
                }
                IndexRecord::Connection(conn) => {
                    println!("IIR::Connection");
                    ()
                }
                IndexRecord::ChunkInfo(chunk_info) => {
                    println!("ChunkInfo");
                    ()
                }
            }
        }

        Ok(())
    }
}
