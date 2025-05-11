use raft_db_common::{errinput, RaftDBResult};
use crate::types::Table;

/// A SQL engine. This provides low-level CRUD (create, read, update, delete)
/// operations for table rows, a schema catalog for accessing and modifying
/// table schemas, and interactive SQL sessions that execute client SQL
/// statements. All engine access is transactional with snapshot isolation.
pub trait Engine<'a>: Sized {
    /// The engine's transaction type. This provides both row-level CRUD
    /// operations and transactional access to the schema catalog. It
    /// can't outlive the engine.
    type Transaction: Transaction + Catalog + 'a;

    /// Begins a read-write transaction.
    fn begin(&'a self) -> Result<Self::Transaction>;
    /// Begins a read-only transaction.
    fn begin_read_only(&'a self) -> Result<Self::Transaction>;
    /// Begins a read-only transaction as of a historical version.
    fn begin_as_of(&'a self, version: mvcc::Version) -> Result<Self::Transaction>;

    /// Creates a session for executing SQL statements. Can't outlive engine.
    fn session(&'a self) -> Session<'a, Self> {
        Session::new(self)
    }
}

/// A SQL transaction. Executes transactional CRUD operations on table rows.
/// Provides snapshot isolation (see `storage::mvcc` module for details).
///
/// All methods operate on row batches rather than single rows to amortize the
/// cost. With the Raft engine, each call results in a Raft roundtrip, and we'd
/// rather not have to do that for every single row that's modified.
pub trait Transaction {
    // todo()
}

/// The catalog stores table schema information. It must be implemented for
/// Engine::Transaction, and is thus fully transactional. For simplicity, it
/// only supports creating and dropping tables. There are no ALTER TABLE schema
/// changes, nor CREATE INDEX -- everything has to be specified when the table
/// is initially created.
///
/// This type is separate from Transaction, even though Engine::Transaction
/// requires transactions to implement it. This allows better control of when
/// catalog access should be used (i.e. during planning, not execution).
pub trait Catalog {
    /// Creates a new table. Errors if it already exists.
    fn create_table(&self, table: Table) -> RaftDBResult<()>;
    /// Drops a table. Errors if it does not exist, unless if_exists is true.
    /// Returns true if the table existed and was deleted.
    fn drop_table(&self, table: &str, if_exists: bool) -> RaftDBResult<bool>;
    /// Fetches a table schema, or None if it doesn't exist.
    fn get_table(&self, table: &str) -> RaftDBResult<Option<Table>>;
    /// Returns a list of all table schemas.
    fn list_tables(&self) -> RaftDBResult<Vec<Table>>;
    /// Fetches a table schema, or errors if it does not exist.
    fn must_get_table(&self, table: &str) -> RaftDBResult<Table> {
        self.get_table(table)?.ok_or_else(|| errinput!("table {table} does not exist"))
    }
}