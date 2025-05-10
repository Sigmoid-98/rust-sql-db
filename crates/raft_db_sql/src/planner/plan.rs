use std::collections::HashMap;
use raft_db_common::RaftDBResult;
use crate::parser::ast::Statement;
use crate::types::{Expression, Table};

pub enum Plan {
    /// A CREATE TABLE plan. Creates a new table with the given schema. Errors
    /// if the table already exists or the schema is invalid.
    CreateTable { schema: Table },
    /// A DROP TABLE plan. Drops the given table. Errors if the table does not
    /// exist, unless if_exists is true.
    DropTable { table: String, if_exists: bool },
    /// A DELETE plan. Deletes rows in table that match the rows from source.
    /// primary_key specifies the primary key column index in the source rows.
    Delete { table: String, primary_key: usize, source: Node },
    /// An INSERT plan. Inserts rows from source (typically a Values node) into
    /// table. If column_map is given, it maps table → source column indexes and
    /// must have one entry for every column in source. Table columns not
    /// present in source will get the column's default value if set, or error.
    Insert { table: Table, column_map: Option<HashMap<usize, usize>>, source: Node },
    /// An UPDATE plan. Updates rows in table that match the rows from source,
    /// where primary_key specifies the primary key column index in the source
    /// rows. The given column/expression pairs specify the row updates to make,
    /// evaluated using the existing source row, which must be a complete row
    /// from the update table.
    Update { table: Table, primary_key: usize, source: Node, expressions: Vec<(usize, Expression)> },
    /// A SELECT plan. Recursively executes the query plan tree and returns the
    /// resulting rows.
    Select(Node),
}

impl Plan {
    /// Builds a plan from an AST statement.
    pub fn build(statement: Statement, catalog: &impl Catalog) -> RaftDBResult<Self> {
        
    }
}


pub fn remap_sources(targets: &[Option<usize>]) -> Vec<Option<usize>> {
    let size = targets.iter().filter_map(|v| *v).map(|i| i + 1).max().unwrap_or(0);
    let mut sources = vec![None; size];
    for (from, to) in targets.iter().enumerate() {
        if let Some(to) = to {
            sources[*to] = Some(from);
        }
    }
    sources
}