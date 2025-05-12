use raft_db_common::RaftDBResult;
use crate::engine::{Catalog, Transaction};
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


pub fn execute(node: Node, txn: &impl Transaction) -> RaftDBResult<Rows> {
    Ok(match node {
        Node::Aggregate { source, group_by, aggregates } => {
            let source = execute(*source, txn)?;
            aggregates::aggregate(source, group_by, aggregates)?
        },
        Node::Filter { .. } => {}
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