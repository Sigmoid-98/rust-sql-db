use std::io::{Read, Write};
use serde::{Deserialize, Serialize};
use serde::de::DeserializeOwned;
use common_error::{RaftDBError, RaftDBResult};

/// Use the standard Bincode configuration.
const CONFIG: bincode::config::Configuration = bincode::config::standard();

/// Serializes a value using Bincode.
pub fn serialize<T: Serialize>(value: &T) -> Vec<u8> {
    bincode::serde::encode_to_vec(value, CONFIG).expect("value must be serializable")
}

/// Deserializes a value using Bincode.
pub fn deserialize<'de, T: Deserialize<'de>>(bytes: &'de [u8]) -> RaftDBResult<T> {
    Ok(bincode::serde::borrow_decode_from_slice(bytes, CONFIG)?.0)
}

/// Serializes a value to a writer using Bincode.
pub fn serialize_into<W: Write, T: Serialize>(mut writer: W, value: &T) -> RaftDBResult<()> {
    bincode::serde::encode_into_std_write(value, &mut writer, CONFIG)?;
    Ok(())
}

/// Deserializes a value from a reader using Bincode.
pub fn deserialize_from<R: Read, T: DeserializeOwned>(mut reader: R) -> RaftDBResult<T> {
    Ok(bincode::serde::decode_from_std_read(&mut reader, CONFIG)?)
}

/// Deserializes a value from a reader using Bincode, or returns None if the
/// reader is closed.
pub fn maybe_deserialize_from<R: Read, T: DeserializeOwned>(mut reader: R) -> RaftDBResult<Option<T>> {
    match bincode::serde::decode_from_std_read(&mut reader, CONFIG) { 
        Ok(t) => Ok(Some(t)),
        Err(bincode::error::DecodeError::Io { inner, .. })
            if inner.kind() == std::io::ErrorKind::UnexpectedEof 
                || inner.kind() == std::io::ErrorKind::ConnectionReset
                => Ok(None),
        
        Err(err) => Err(RaftDBError::from(err)),
    }
}