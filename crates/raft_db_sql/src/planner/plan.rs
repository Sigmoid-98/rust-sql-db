use std::collections::HashMap;
use std::fmt::{Display, Formatter};
use serde::{Deserialize, Serialize};
use raft_db_common::RaftDBResult;
use crate::engine::{Catalog, Transaction};
use crate::parser::ast;
use crate::planner::Planner;
use crate::types::{Expression, Label, Table, Value};

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

impl Display for Plan {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Plan::CreateTable { schema } => write!(f, "CreateTable: {}", schema.name),
            Plan::DropTable { table, .. } => write!(f, "DropTable: {table}"),
            Plan::Delete { table, source, .. } => {
                write!(f, "Delete: {table}")?;
                source.format(f, "", false, true)
            }
            Plan::Insert { table, source, .. } => {
                write!(f, "Insert: {}", table.name)?;
                source.format(f, "", false, true)
            }
            Plan::Update { table, source, expressions, .. } => {
                let expressions = expressions
                    .iter()
                    .map(|(i, expr)| format!("{}={}", table.columns[*i].name, expr.format(source)))
                    .join(", ");
                write!(f, "Update: {} ({expressions})", table.name)?;
                source.format(f, "", false, true)
            }
            Plan::Select(root) => root.format(f, "", true, true),
        }
    }
}

impl Plan {
    /// Builds a plan from an AST statement.
    pub fn build(statement: ast::Statement, catalog: &impl Catalog) -> RaftDBResult<Self> {
        Planner::new(catalog).build(statement)
    }
    
    pub fn execute(self, txn: &(impl Transaction + Catalog)) -> RaftDBResult<Ex> {}
}


/// A query plan node. Returns a row iterator, and can be nested.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum Node {
    /// Computes the given aggregate values for the given group_by buckets
    /// across all rows in the source node. The group_by columns are emitted
    /// first, followed by the aggregate columns, in the given order.
    Aggregate { source: Box<Node>, group_by: Vec<Expression>, aggregates: Vec<Aggregate> },
    /// Filters source rows, by discarding rows for which the predicate
    /// evaluates to false.
    Filter { source: Box<Node>, predicate: Expression },
    /// Joins the left and right sources on the given columns by building an
    /// in-memory hashmap of the right source and looking up matches for each
    /// row in the left source. When outer is true (e.g. LEFT JOIN), a left row
    /// without a right match is emitted anyway, with NULLs for the right row.
    HashJoin {
        left: Box<Node>,
        left_column: usize,
        right: Box<Node>,
        right_column: usize,
        outer: bool,
    },
    /// Looks up the given values in a secondary index and emits matching rows.
    /// NULL and NaN values are considered equal, to allow IS NULL and IS NAN
    /// index lookups, as is -0.0 and 0.0.
    IndexLookup { table: Table, column: usize, values: Vec<Value>, alias: Option<String> },
    /// Looks up the given primary keys and emits their rows.
    KeyLookup { table: Table, keys: Vec<Value>, alias: Option<String> },
    /// Only emits the first limit rows from the source, discards the rest.
    Limit { source: Box<Node>, limit: usize },
    /// Joins the left and right sources on the given predicate by buffering the
    /// right source and iterating over it for every row in the left source.
    /// When outer is true (e.g. LEFT JOIN), a left row without a right match is
    /// emitted anyway, with NULLs for the right row.
    NestedLoopJoin { left: Box<Node>, right: Box<Node>, predicate: Option<Expression>, outer: bool },
    /// Nothing does not emit anything, and is used to short-circuit nodes that
    /// can't emit anything during optimization. It retains the column names of
    /// any replaced nodes for results headers and plan formatting.
    Nothing { columns: Vec<Label> },
    /// Discards the first offset rows from source, emits the rest.
    Offset { source: Box<Node>, offset: usize },
    /// Sorts the source rows by the given sort key. Buffers the entire row set
    /// in memory.
    Order { source: Box<Node>, key: Vec<(Expression, Direction)> },
    /// Projects the input rows by evaluating the given expressions. Aliases are
    /// only used when displaying the plan.
    Projection { source: Box<Node>, expressions: Vec<Expression>, aliases: Vec<Label> },
    /// Remaps source columns to the given target column index, or None to drop
    /// the column. Unspecified target columns yield Value::Null. The source →
    /// target mapping ensures a source column can only be mapped to a single
    /// target column, allowing the value to be moved rather than cloned.
    Remap { source: Box<Node>, targets: Vec<Option<usize>> },
    /// A full table scan, with an optional pushed-down filter. The schema is
    /// used during plan optimization. The alias is only used for formatting.
    Scan { table: Table, filter: Option<Expression>, alias: Option<String> },
    /// A constant set of values.
    Values { rows: Vec<Vec<Expression>> },
}


impl Node {
    /// Recursively formats the node. Prefix is used for tree branch lines. root
    /// is true if this is the root (first) node, and last_child is true if this
    /// is the last child node of the parent.
    pub fn format(
        &self,
        f: &mut std::fmt::Formatter<'_>,
        prefix: &str,
        root: bool,
        last_child: bool,
    ) -> std::fmt::Result {
        // If this is not the root node, emit a newline after the previous node.
        // This avoids a spurious newline at the end of the plan.
        if !root {
            writeln!(f)?;
        }

        // Prefix the node with a tree branch line. Modify the prefix for any
        // child nodes we'll recurse into.
        let prefix = if !last_child {
            write!(f, "{prefix}├─ ")?;
            format!("{prefix}│  ")
        } else if !root {
            write!(f, "{prefix}└─ ")?;
            format!("{prefix}   ")
        } else {
            write!(f, "{prefix}")?;
            prefix.to_string()
        };

        // Format the node.
        match self {
            Self::Aggregate { source, aggregates, group_by } => {
                let aggregates = group_by
                    .iter()
                    .map(|group_by| group_by.format(source))
                    .chain(aggregates.iter().map(|agg| agg.format(source)))
                    .join(", ");
                write!(f, "Aggregate: {aggregates}")?;
                source.format(f, &prefix, false, true)?;
            }
            Self::Filter { source, predicate } => {
                write!(f, "Filter: {}", predicate.format(source))?;
                source.format(f, &prefix, false, true)?;
            }
            Self::HashJoin { left, left_column, right, right_column, outer } => {
                let kind = if *outer { "outer" } else { "inner" };
                let left_column = match left.column_label(*left_column) {
                    Label::None => format!("left #{left_column}"),
                    label => format!("{label}"),
                };
                let right_column = match right.column_label(*right_column) {
                    Label::None => format!("right #{right_column}"),
                    label => format!("{label}"),
                };
                write!(f, "HashJoin: {kind} on {left_column} = {right_column}")?;
                left.format(f, &prefix, false, false)?;
                right.format(f, &prefix, false, true)?;
            }
            Self::IndexLookup { table, column, alias, values } => {
                let column = &table.columns[*column].name;
                write!(f, "IndexLookup: {}.{column}", table.name)?;
                if let Some(alias) = alias {
                    write!(f, " as {alias}.{column}")?;
                }
                if !values.is_empty() && values.len() < 10 {
                    write!(f, " ({})", values.iter().join(", "))?;
                } else {
                    write!(f, " ({} values)", values.len())?;
                }
            }
            Self::KeyLookup { table, alias, keys } => {
                write!(f, "KeyLookup: {}", table.name)?;
                if let Some(alias) = alias {
                    write!(f, " as {alias}")?;
                }
                if !keys.is_empty() && keys.len() < 10 {
                    write!(f, " ({})", keys.iter().join(", "))?;
                } else {
                    write!(f, " ({} keys)", keys.len())?;
                }
            }
            Self::Limit { source, limit } => {
                write!(f, "Limit: {limit}")?;
                source.format(f, &prefix, false, true)?;
            }
            Self::NestedLoopJoin { left, right, predicate, outer, .. } => {
                let kind = if *outer { "outer" } else { "inner" };
                write!(f, "NestedLoopJoin: {kind}")?;
                if let Some(predicate) = predicate {
                    write!(f, " on {}", predicate.format(self))?;
                }
                left.format(f, &prefix, false, false)?;
                right.format(f, &prefix, false, true)?;
            }
            Self::Nothing { .. } => write!(f, "Nothing")?,
            Self::Offset { source, offset } => {
                write!(f, "Offset: {offset}")?;
                source.format(f, &prefix, false, true)?;
            }
            Self::Order { source, key: orders } => {
                let orders = orders
                    .iter()
                    .map(|(expr, dir)| format!("{} {dir}", expr.format(source)))
                    .join(", ");
                write!(f, "Order: {orders}")?;
                source.format(f, &prefix, false, true)?;
            }
            Self::Projection { source, expressions, aliases } => {
                let expressions = expressions
                    .iter()
                    .enumerate()
                    .map(|(i, expr)| match aliases.get(i) {
                        Some(Label::None) | None => expr.format(source),
                        Some(alias) => format!("{} as {alias}", expr.format(source)),
                    })
                    .join(", ");
                write!(f, "Projection: {expressions}")?;
                source.format(f, &prefix, false, true)?;
            }
            Self::Remap { source, targets } => {
                let remap = remap_sources(targets)
                    .into_iter()
                    .map(|from| match from {
                        Some(from) => match source.column_label(from) {
                            Label::None => format!("#{from}"),
                            label => label.to_string(),
                        },
                        None => "Null".to_string(),
                    })
                    .join(", ");
                write!(f, "Remap: {remap}")?;
                let dropped = targets
                    .iter()
                    .enumerate()
                    .filter_map(|(i, v)| {
                        v.is_none().then_some(match source.column_label(i) {
                            Label::None => format!("#{i}"),
                            label => format!("{label}"),
                        })
                    })
                    .join(", ");
                if !dropped.is_empty() {
                    write!(f, " (dropped: {dropped})")?;
                }
                source.format(f, &prefix, false, true)?;
            }
            Self::Scan { table, alias, filter } => {
                write!(f, "Scan: {}", table.name)?;
                if let Some(alias) = alias {
                    write!(f, " as {alias}")?;
                }
                if let Some(filter) = filter {
                    write!(f, " ({})", filter.format(self))?;
                }
            }
            Self::Values { rows, .. } => {
                write!(f, "Values: ")?;
                match rows.len() {
                    1 if rows[0].is_empty() => write!(f, "blank row")?,
                    1 => write!(f, "{}", rows[0].iter().map(|e| e.format(self)).join(", "))?,
                    n => write!(f, "{n} rows")?,
                }
            }
        };

        Ok(())
    }
}


/// An aggregate function.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum Aggregate {
    Average(Expression),
    Count(Expression),
    Max(Expression),
    Min(Expression),
    Sum(Expression),
}

impl Aggregate {
    
    fn format(&self, node: &Node) -> String {
        match self {
            Self::Average(expr) => format!("avg({})", expr.format(node)),
            Self::Count(expr) => format!("count({})", expr.format(node)),
            Self::Max(expr) => format!("max({})", expr.format(node)),
            Self::Min(expr) => format!("min({})", expr.format(node)),
            Self::Sum(expr) => format!("sum({})", expr.format(node)),
        }
    }
}


/// A sort order direction.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum Direction {
    Ascending,
    Descending,
}

impl Display for Direction {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Ascending => f.write_str("asc"),
            Self::Descending => f.write_str("desc"),
        }
    }
}

impl From<ast::Direction> for Direction {
    fn from(value: ast::Direction) -> Self {
        match value {
            ast::Direction::Ascending => Self::Ascending,
            ast::Direction::Descending => Self::Descending,
        }
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