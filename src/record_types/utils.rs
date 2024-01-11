use anyhow::Result;
use crate::error::RosError;
use byteorder::{ByteOrder, LE};
use bytes::Bytes;
use std::str;

pub(crate) fn read_record(mut header: Bytes) -> Result<(String, Bytes, Bytes)> {
    if header.len() < 4 {
        return Err(RosError::InvalidHeader.into());
    }
    println!("Header len {}", header.len());
    let n = LE::read_u32(&header[..4]) as usize;
    header = header.slice(4..);

    if header.len() < n {
        return Err(RosError::InvalidHeader.into());
    }
    let rec = header.slice(..n);
    header = header.slice(n..);

    let delim = rec.iter().position(|x| *x == b'=').ok_or(anyhow::Error::new(RosError::InvalidHeader))?;
    // SAFETY: the string is already checked
    let name = String::from_utf8(rec[..delim].to_vec())?;
    let val = rec.slice(delim + 1..);
    Ok((name, val, header))
}

pub(crate) fn unknown_field(name: &str, val: &[u8]) {
    log::warn!("Unknown header field: {}={:?}", name, val);
}

pub(crate) fn check_op(val: &[u8], op: u8) -> Result<()> {
    if val.len() == 1 && val[0] == op {
        Ok(())
    } else {
        Err(RosError::InvalidRecord.into())
    }
}

pub(crate) fn set_field_u64(field: &mut Option<u64>, val: &[u8]) -> Result<()> {
    if val.len() != 8 || field.is_some() {
        Err(RosError::InvalidHeader.into())
    } else {
        *field = Some(LE::read_u64(val));
        Ok(())
    }
}

pub(crate) fn set_field_u32(field: &mut Option<u32>, val: &[u8]) -> Result<()> {
    if val.len() != 4 || field.is_some() {
        Err(RosError::InvalidHeader.into())
    } else {
        *field = Some(LE::read_u32(val));
        Ok(())
    }
}

pub(crate) fn set_field_str<'a>(field: &mut Option<String>, val: Bytes) -> Result<()> {
    if field.is_some() {
        return Err(RosError::InvalidHeader.into());
    }
    *field = Some(String::from_utf8(val.to_vec()).map_err(|_| anyhow::Error::new(RosError::InvalidHeader))?);
    Ok(())
}

pub(crate) fn set_field_time(field: &mut Option<u64>, val: &[u8]) -> Result<()> {
    if val.len() != 8 || field.is_some() {
        return Err(RosError::InvalidHeader.into());
    }
    let s = LE::read_u32(&val[..4]) as u64;
    let ns = LE::read_u32(&val[4..]) as u64;
    *field = Some(1_000_000_000 * s + ns);
    Ok(())
}
