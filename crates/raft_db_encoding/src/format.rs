//! Formats raw keys and values, recursively where necessary. Handles both
//! Raft, MVCC, SQL, and raw binary data.

use std::marker::PhantomData;
use itertools::Itertools;
use regex::Regex;

/// Formats encoded keys and values.
pub trait Formatter {
    /// Formats a key.
    fn key(key: &[u8]) -> String;

    /// Formats a value. Also takes the key to determine the kind of value.
    fn value(key: &[u8], value: &[u8]) -> String;

    /// Formats a key/value pair.
    fn key_value(key: &[u8], value: &[u8]) -> String {
        Self::key_maybe_value(key, Some(value))
    }

    /// Formats a key/value pair, where the value may not exist.
    fn key_maybe_value(key: &[u8], value: Option<&[u8]>) -> String {
        let fmt_key = Self::key(key);
        let fmt_value = value.map_or("None".to_string(), |v| Self::value(key, v));
        format!("{fmt_key} → {fmt_value}")
    }
}

/// Formats raw byte slices without any decoding.
pub struct Raw;

impl Formatter for Raw {
    fn key(key: &[u8]) -> String {
        Self::key(key)
    }

    fn value(_key: &[u8], value: &[u8]) -> String {
        Self::bytes(value)
    }
}


impl Raw {
    /// Formats raw bytes as escaped ASCII strings.
    pub fn bytes(bytes: &[u8]) -> String {
        let escaped = bytes.iter().copied().flat_map(std::ascii::escape_default).collect_vec();
        format!("\"{}\"", String::from_utf8_lossy(&escaped))
    }
}


/// Formats Raft log entries. Dispatches to F to format each Raft command.
pub struct Raft<F: Formatter>(PhantomData<F>);

impl<F: Formatter> Formatter for Raft<F> {
    fn key(key: &[u8]) -> String {
        unimplemented!()
    }

    fn value(key: &[u8], value: &[u8]) -> String {
        unimplemented!()
    }
}

impl<F: Formatter> Raft<F> {
    // todo() Formats a Raft entry.
}


/// Formats MVCC keys/values. Dispatches to F to format the inner key/value.
pub struct MVCC<F: Formatter>(PhantomData<F>);

impl<F: Formatter> Formatter for MVCC<F> {
    fn key(key: &[u8]) -> String {
        unimplemented!()
    }

    fn value(key: &[u8], value: &[u8]) -> String {
        unimplemented!()
    }
}


/// Formats SQL keys/values.
pub struct SQL;

impl SQL {
    /// Formats a list of SQL values.
    fn values(values: impl IntoIterator<Item = raft_db_sql::types::Value>) -> String {
        values.into_iter().join(",")
    }

    /// Formats a table schema.
    fn schema(table: raft_db_sql::types::Table) -> String {
        // Put it all on  asingle line.
        let re = Regex::new(r#"\n\s*"#).expect("invalid regex");
        re.replace_all(&table.to_string(), " ").into_owned()
    }
}








