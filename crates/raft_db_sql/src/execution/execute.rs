use raft_db_common::RaftDBResult;
use crate::engine::{Catalog, Transaction};
use crate::execution::{aggregate, write};
use crate::planner::{Node, Plan};
use crate::types::{Label, Rows};

/// A plan execution result.
pub enum ExecutionResult {
    CreateTable { name: String },
    DropTable { name: String, existed: bool },
    Delete { count: u64 },
    Insert { count: u64 },
    Update { count: u64 },
    Select { rows: Rows, columns: Vec<Label>},
}

/// Executes a plan, returning an execution result.
///
/// Takes the transaction and catalog separately, even though Transaction must
/// implement Catalog, to ensure the catalog is primarily used during planning.
pub fn execute_plan(
    plan: Plan,
    txn: &impl Transaction,
    catalog: &impl Catalog,
) -> RaftDBResult<ExecutionResult> {
    Ok(match plan {
        Plan::CreateTable { schema } => {
            let name = schema.name.clone();
            catalog.create_table(schema)?;
            ExecutionResult::CreateTable { name }
        },
        Plan::DropTable { table, if_exists } => {
            let existed = catalog.drop_table(&table, if_exists)?;
            ExecutionResult::DropTable { name: table, existed }
        },
        Plan::Delete { table, primary_key, source } => {
            let source = execute(source, txn)?;
            let count = write::delete(txn, table, primary_key, source)?;
            ExecutionResult::Delete { count }
        },
        Plan::Insert { table, column_map, source } => {}
        Plan::Select(root) => {}
        Plan::Update { table, primary_key, source, expressions } => {}
    })
}

/// Recursively executes a query plan node, returning a row iterator.
///
/// Rows stream through the plan node tree from the branches to the root. Nodes
/// recursively pull input rows upwards from their child node(s), process them,
/// and hand the resulting rows off to their parent node.
///
/// Below is an example of an (unoptimized) query plan:
///
/// SELECT title, released, genres.name AS genre
/// FROM movies INNER JOIN genres ON movies.genre_id = genres.id
/// WHERE released >= 2000 ORDER BY released
///
/// Order: movies.released desc
/// └─ Projection: movies.title, movies.released, genres.name as genre
///    └─ Filter: movies.released >= 2000
///       └─ NestedLoopJoin: inner on movies.genre_id = genres.id
///          ├─ Scan: movies
///          └─ Scan: genres
///
/// Rows flow from the tree leaves to the root. The Scan nodes read and emit
/// table rows from storage. They are passed to the NestedLoopJoin node which
/// joins the rows from the two tables, then the Filter node discards old
/// movies, the Projection node picks out the requested columns, and the Order
/// node sorts them before emitting the rows to the client.
pub fn execute(node: Node, txn: &impl Transaction) -> RaftDBResult<Rows> {
    Ok(match node {
        Node::Aggregate { source, group_by, aggregates } => {
            let source = execute(*source, txn)?;
            aggregate::aggregate(source, group_by, aggregates)?
        },
        Node::Filter { source, predicate } => {
            let source = execute(*source, txn)?;
            transform::filter(source, predicate)
        }
        Node::HashJoin { .. } => {}
        Node::IndexLookup { .. } => {}
        Node::KeyLookup { .. } => {}
        Node::Limit { .. } => {}
        Node::NestedLoopJoin { .. } => {}
        Node::Nothing { .. } => {}
        Node::Offset { .. } => {}
        Node::Order { .. } => {}
        Node::Projection { .. } => {}
        Node::Remap { .. } => {}
        Node::Scan { .. } => {}
        Node::Values { .. } => {}
    })
}