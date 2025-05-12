use raft_db_common::RaftDBResult;
use crate::engine::Transaction;
use crate::types::{Rows, Value};

use itertools::Itertools as _;

pub fn delete(
    txn: &impl Transaction,
    table: String,
    primary_key: usize,
    source: Rows,
) -> RaftDBResult<u64> {
    let ids: Vec<Value> = source
        .map_ok(|row| row.into_iter().nth(primary_key).expect("short row"))
        .try_collect()?;
    let count = ids.len() as u64;
    txn.delete(&table, &ids)?;
    Ok(count)
}