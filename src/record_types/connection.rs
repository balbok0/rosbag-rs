use super::utils::{check_op, read_record, unknown_field};
use super::utils::{set_field_str, set_field_u32};
use super::{HeaderGen, RecordGen, Result};
use bytes::Bytes;
use log::warn;
use crate::error::RosError;

use crate::cursor::Cursor;
use crate::field_iter::FieldIterator;

/// Connection record which contains message type for ROS topic.
///
/// Two topic fields exist `storage_topic` and `topic`. This is because messages
/// can be written to the bag file on a topic different from where they were
/// originally published.
#[derive(Debug, Clone)]
pub struct Connection {
    /// Unique connection ID
    pub id: u32,
    /// Topic on which the messages are stored
    pub storage_topic: String,

    /// Name of the topic the subscriber is connecting to
    pub topic: String,
    /// Message type
    pub tp: String,
    /// MD5 hash sum of the message type
    pub md5sum: [u8; 16],
    /// Full text of the message definition
    pub message_definition: String,
    /// Name of node sending data (can be empty)
    pub caller_id: String,
    /// Is publisher in the latching mode? (i.e. sends the last value published
    /// to new subscribers)
    pub latching: bool,
}

#[derive(Default, Debug, Clone)]
pub(crate) struct ConnectionHeader {
    pub id: Option<u32>,
    pub storage_topic: Option<String>,
}

impl RecordGen for Connection {
    type Header = ConnectionHeader;

    fn read_data(c: &mut Cursor, header: Self::Header) -> Result<Self> {
        let id = header.id.ok_or(anyhow::Error::new(RosError::InvalidHeader))?;
        let storage_topic = header.storage_topic.ok_or(anyhow::Error::new(RosError::InvalidHeader))?;

        let buf = c.next_chunk()?;

        let mut topic = None;
        let mut tp = None;
        let mut md5sum = None;
        let mut message_definition = None;
        let mut caller_id = None;
        let mut latching = false;

        for field in FieldIterator::new(buf) {
            let (name, val) = field?;
            match name.as_str() {
                "topic" => set_field_str(&mut topic, val)?,
                "type" => set_field_str(&mut tp, val)?,
                "md5sum" => {
                    if md5sum.is_some() || val.len() != 32 {
                        return Err(RosError::InvalidRecord.into());
                    }
                    let mut res = [0u8; 16];
                    base16ct::lower::decode(val, &mut res).map_err(|_| anyhow::Error::new(RosError::InvalidRecord))?;
                    md5sum = Some(res);
                }
                "message_definition" => set_field_str(&mut message_definition, val)?,
                "callerid" => set_field_str(&mut caller_id, val)?,
                "latching" => {
                    latching = match val.first() {
                        Some(b'1') => true,
                        Some(b'0') => false,
                        _ => return Err(RosError::InvalidRecord.into()),
                    }
                }
                _ => warn!("Unknown field in the connection header: {}", name),
            }
        }

        let topic = topic.ok_or(anyhow::Error::new(RosError::InvalidHeader))?;
        let tp = tp.ok_or(anyhow::Error::new(RosError::InvalidHeader))?;
        let md5sum = md5sum.ok_or(anyhow::Error::new(RosError::InvalidHeader))?;
        let message_definition = message_definition.ok_or(anyhow::Error::new(RosError::InvalidHeader))?;
        let caller_id = caller_id.unwrap_or("".to_string());
        Ok(Self {
            id,
            storage_topic,
            topic,
            tp,
            md5sum,
            message_definition,
            caller_id,
            latching,
        })
    }
}

impl HeaderGen for ConnectionHeader {
    const OP: u8 = 0x07;

    fn read_header(mut header: Bytes) -> Result<Self> {
        let mut rec = Self::default();
        while !header.is_empty() {
            let (name, val, new_header) = read_record(header)?;
            header = new_header;
            match name.as_str() {
                "op" => check_op(&val, Self::OP)?,
                "topic" => set_field_str(&mut rec.storage_topic, val)?,
                _ => rec.process_field(&name, &val)?,
            }
        }
        Ok(rec)
    }

    fn process_field(&mut self, name: &str, val: &[u8]) -> Result<()> {
        match name {
            "conn" => set_field_u32(&mut self.id, val)?,
            _ => unknown_field(name, val),
        }
        Ok(())
    }
}
