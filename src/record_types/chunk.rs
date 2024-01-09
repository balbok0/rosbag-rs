use bytes::{Bytes, Buf};

use super::utils::{set_field_u32, unknown_field};
use super::{HeaderGen, RecordGen};
use anyhow::Result;
use crate::error::RosError;

use crate::cursor::Cursor;
use crate::msg_iter::MessageRecordsIterator;

/// Compression options for `Chunk` data.
#[derive(Debug, Clone, Copy)]
#[non_exhaustive]
pub enum Compression {
    /// Bzip2 compression.
    Bzip2,
    /// Lz4 compression.
    Lz4,
    /// No compression.
    None,
}

impl Compression {
    fn decompress(self, data: Bytes, decompressed_size: Option<u32>) -> Result<Bytes> {
        Ok(match self {
            Compression::Bzip2 => {
                let mut decompressed = Vec::new();
                decompressed.reserve(decompressed_size.map(|s| s as usize).unwrap_or(data.len()));
                let mut decompressor = bzip2::Decompress::new(false);
                decompressor
                    .decompress_vec(&data, &mut decompressed)
                    .map_err(|e| anyhow::Error::new(RosError::Bzip2DecompressionError(e.to_string())))?;
                Bytes::from(decompressed)
            }
            Compression::Lz4 => {
                let mut decoder = lz4::Decoder::new(data.clone().reader())
                    .map_err(|e| anyhow::Error::new(RosError::Lz4DecompressionError(e.to_string())))?;
                let mut decompressed = Vec::new();
                decompressed.reserve(decompressed_size.map(|s| s as usize).unwrap_or(data.len()));
                std::io::copy(&mut decoder, &mut decompressed).map_err(|_| {
                    anyhow::Error::new(RosError::Lz4DecompressionError("Error while decoding".to_string()))
                })?;
                Bytes::from(decompressed)
            }
            Compression::None => data,
        })
    }
}

/// Bulk storage with optional compression for messages data and connection
/// records.
#[derive(Debug, Clone)]
pub struct Chunk {
    /// Compression type for the data
    pub compression: Compression,
    /// Decompressed messages data and connection records
    data: Bytes,
}

impl Chunk {
    /// Get iterator over only messages
    pub fn messages(&self) -> MessageRecordsIterator {
        MessageRecordsIterator::new(self.data.clone())
    }
}

#[derive(Debug, Clone, Default)]
pub(crate) struct ChunkHeader {
    compression: Option<Compression>,
    size: Option<u32>,
}

impl RecordGen for Chunk {
    type Header = ChunkHeader;

    fn read_data(c: &mut Cursor, header: Self::Header) -> Result<Self> {
        let compression = header.compression.ok_or(anyhow::Error::new(RosError::InvalidHeader))?;
        let size = header.size.ok_or(anyhow::Error::new(RosError::InvalidHeader))?;
        let data = compression.decompress(c.next_chunk()?, header.size)?;
        if data.len() != size as usize {
            return Err(RosError::InvalidRecord.into());
        }
        Ok(Self { compression, data })
    }
}

impl HeaderGen for ChunkHeader {
    const OP: u8 = 0x05;

    fn process_field(&mut self, name: &str, val: &[u8]) -> Result<()> {
        match name {
            "compression" => {
                if self.compression.is_some() {
                    return Err(RosError::InvalidHeader.into());
                }
                self.compression = Some(match val {
                    b"none" => Compression::None,
                    b"bz2" => Compression::Bzip2,
                    b"lz4" => Compression::Lz4,
                    _ => return Err(RosError::InvalidHeader.into()),
                });
            }
            "size" => set_field_u32(&mut self.size, val)?,
            _ => unknown_field(name, val),
        }
        Ok(())
    }
}
