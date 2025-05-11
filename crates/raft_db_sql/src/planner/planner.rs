use std::collections::{HashMap, HashSet};
use raft_db_common::{errinput, RaftDBResult};
use crate::engine::Catalog;
use crate::parser::ast;
use crate::planner::plan::{remap_sources, Plan};
use crate::types::{self, Table};


/// The planner builds an execution plan from a parsed Abstract Syntax Tree,
/// using the catalog for schema information.
pub struct Planner<'a, C: Catalog> {
    catalog: &'a C,
}

impl<'a, C: Catalog> Planner<'a, C> {
    /// Creates a new planner.
    pub fn new(catalog: &'a C) -> Self {
        Self { catalog }
    }

    /// Builds a plan for an AST statement.
    pub fn build(&mut self, statement: ast::Statement) -> RaftDBResult<Plan> {
        match statement {
            ast::Statement::CreateTable { name, columns } => self
            ast::Statement::Begin { .. } => {}
            ast::Statement::Commit => {}
            ast::Statement::Rollback => {}
            ast::Statement::Explain(_) => {}
            ast::Statement::DropTable { .. } => {}
            ast::Statement::Delete { .. } => {}
            ast::Statement::Insert { .. } => {}
            ast::Statement::Update { .. } => {}
            ast::Statement::Select { .. } => {}
        }
    }

    /// Builds an expression from an AST expression, looking up columns and
    /// aggregate expressions in the scope.
    pub fn build_expression(expr: ast::Expression, scope: &Scope) -> RaftDBResult<types::Expression> {
        // Look up aggregate functions or GROUP BY expressions. These were added
        // to the scope when building the Aggregate node, if any.
        if let Some(index) = scope.lookup_aggregate(&expr) {
            return Ok(types::Expression::Column(index));
        }

        // Helper for building a boxed expression.
        let builder = |expr: Box<ast::Expression>| -> RaftDBResult<Box<types::Expression>> {
            Ok(Box::new(Self::build_expression(*expr, scope)?))
        };
        
        Ok(match expr {
            // For simplicity, expression evaluation only supports scalar
            // values, not compound types like tuples. Support for * is
            // therefore special-cased in SELECT and COUNT(*).
            ast::Expression::All => return errinput!("unsupported use of *"),
            ast::Expression::Literal(l) => types::Expression::Constant(match l {
                ast::Literal::Null => types::Value::Null,
                ast::Literal::Boolean(b) => types::Value::Boolean(b),
                ast::Literal::Integer(i) => types::Value::Integer(i),
                ast::Literal::Float(f) => types::Value::Float(f),
                ast::Literal::String(s) => types::Value::String(s),
            }),
            ast::Expression::Column(table, name) => {
                types::Expression::Column(scope.lookup_column(table.as_deref(), &name)?)
            }
            ast::Expression::Function(name, mut args) => match (name.as_str(), args.len()) {
                // NB: aggregate functions are processed above.
                ("sqrt", 1) => types::Expression::SquareRoot(builder(Box::new(args.remove(0)))?),
                (name, n) => return errinput!("unknown function {name} with {n} arguments"),
            }
            ast::Expression::Operator(op) => match op {
                ast::Operator::And(lhs, rhs) => types::Expression::And(builder(lhs)?, builder(rhs)?),
                ast::Operator::Not(expr) => types::Expression::Not(builder(expr)?),
                ast::Operator::Or(lhs, rhs) => types::Expression::Or(builder(lhs)?, builder(rhs)?),

                ast::Operator::Equal(lhs, rhs) => types::Expression::Equal(builder(lhs)?, builder(rhs)?),
                ast::Operator::GreaterThan(lhs, rhs) => types::Expression::GreaterThan(builder(lhs)?, builder(rhs)?),
                ast::Operator::GreaterThanOrEqual(lhs, rhs) => types::Expression::Or(
                    types::Expression::GreaterThan(builder(lhs.clone())?, builder(rhs.clone())?).into(),
                    types::Expression::Equal(builder(lhs)?, builder(rhs)?).into(),
                ),
                ast::Operator::Is(expr, literal) => {
                    let expr = builder(expr)?;
                    let value = match literal {
                        ast::Literal::Null => types::Value::Null,
                        ast::Literal::Float(f) if f.is_nan() => types::Value::Float(f),
                        value => panic!("invalid IS value {value:?}"), // enforced by parser
                    };
                    types::Expression::Is(expr, value)
                }
                ast::Operator::LessThan(lhs, rhs) => types::Expression::LessThan(builder(lhs)?, builder(rhs)?),
                ast::Operator::LessThanOrEqual(lhs, rhs) => types::Expression::Or(
                    types::Expression::LessThan(builder(lhs.clone())?, builder(rhs.clone())?).into(),
                    types::Expression::Equal(builder(lhs)?, builder(rhs)?).into(),
                ),
                ast::Operator::Like(lhs, rhs) => types::Expression::Like(builder(lhs)?, builder(rhs)?),
                ast::Operator::NotEqual(lhs, rhs) => types::Expression::Not(types::Expression::Equal(builder(lhs)?, builder(rhs)?).into()),

                ast::Operator::Add(lhs, rhs) => types::Expression::Add(builder(lhs)?, builder(rhs)?),
                ast::Operator::Divide(lhs, rhs) => types::Expression::Divide(builder(lhs)?, builder(rhs)?),
                ast::Operator::Exponentiate(lhs, rhs) => types::Expression::Exponentiate(builder(lhs)?, builder(rhs)?),
                ast::Operator::Factorial(expr) => types::Expression::Factorial(builder(expr)?),
                ast::Operator::Identity(expr) => types::Expression::Identity(builder(expr)?),
                ast::Operator::Remainder(lhs, rhs) => types::Expression::Remainder(builder(lhs)?, builder(rhs)?),
                ast::Operator::Multiply(lhs, rhs) => types::Expression::Multiply(builder(lhs)?, builder(rhs)?),
                ast::Operator::Negate(expr) => types::Expression::Negate(builder(expr)?),
                ast::Operator::Subtract(lhs, rhs) => types::Expression::Subtract(builder(lhs)?, builder(rhs)?),
            }
        })
    }

    /// Builds and evaluates a constant AST expression. Errors on column refs
    fn evaluate_constant(expr: ast::Expression) -> RaftDBResult<types::Value> {
        Self::build_expression(expr, &Scope::new())?.evaluate(None)
    }
}


/// A scope maps column/table names to input column indexes, for lookups during
/// expression construction. It also tracks aggregate and GROUP BY expressions,
/// as well as hidden columns.
///
/// Expression evaluation generally happens in the context of an input row. This
/// row may come directly from a single table, or it may be the result of a long
/// chain of joins and projections. The scope keeps track of which columns are
/// currently visible and what names they have. During expression planning, the
/// scope is used to resolve column names to column indexes, which are placed in
/// the plan and used during execution.
#[derive(Default)]
pub struct Scope {
    /// The currently visible columns. If empty, only constant expressions can
    /// be used (no column references).
    columns: Vec<types::Label>,
    /// Index of currently visible tables, by query name (e.g. may be aliased).
    tables: HashSet<String>,
    /// Index of fully qualified table.column names to column indexes. Qualified
    /// names are always unique within a scope.
    qualified: HashMap<(String, String), usize>,
    /// Index of unqualified column names to column indexes. If a name points
    /// to multiple columns, lookups will fail with an ambiguous name error.
    unqualified: HashMap<String, Vec<usize>>,
    /// Index of aggregate and GROUP BY expressions to column indexes. This is
    /// used to track output columns of Aggregate nodes and look them up from
    /// expressions in downstream SELECT, HAVING, and ORDER BY clauses. If the
    /// node contains an (inner) Aggregate node, this is never empty.
    aggregates: HashMap<ast::Expression, usize>,
    /// Hidden columns. These are used to pass e.g. ORDER BY and HAVING
    /// expressions through SELECT projection nodes if the expressions aren't
    /// already projected. They should be removed before emitting results.
    hidden: HashSet<usize>,
}

impl Scope {
    /// Creates a new, empty scope.
    pub fn new() -> Self {
        Self::default()
    }

    /// Creates a scope from a table, using the table's original name.
    fn from_table(table: &Table) -> RaftDBResult<Self> {
        let mut scope = Self::new();
        scope.add_table(table, None)?;
        Ok(scope)
    }

    /// Creates a new child scope that inherits from the parent scope.
    pub fn spawn(&self) -> Self {
        let mut child = Scope::new();
        // retain table names
        child.tables = self.tables.clone();
        child
    }


    /// Adds a table to the scope. The label is either the table's original name
    /// or an alias, and must be unique. All table columns are added, in order.
    fn add_table(&mut self, table: &Table, alias: Option<&str>) -> RaftDBResult<()> {
        let name = alias.unwrap_or(&table.name);
        if self.tables.contains(name) {
            return errinput!("duplicate table name {name}");
        }

        for column in &table.columns {
            self.add_column(types::Label::Qualified(name.to_string(), column.name.clone()));
        }
        self.tables.insert(name.to_string());
        Ok(())
    }

    /// Appends a column with the given label to the scope. Returns the column
    /// index.
    fn add_column(&mut self, label: types::Label) -> usize {
        let index = self.columns.len();
        if let types::Label::Qualified(table, column) = &label {
            self.qualified.insert((table.clone(), column.clone()), index);
        }
        if let types::Label::Qualified(_, name) | types::Label::Unqualified(name) = &label {
            self.unqualified.entry(name.clone()).or_default().push(index);
        }
        
        self.columns.push(label);
        index
    }

    /// Looks up a column index by name, if possible.
    fn lookup_column(&self, table: Option<&str>, name: &str) -> RaftDBResult<usize> {
        let fmt_name = || table.map(|table| format!("{table}.{name}")).unwrap_or(name.to_string());
        if self.columns.is_empty() {
            return errinput!("expression must be constant, found column {}", fmt_name());
        }
        if let Some(table) = table {
            if !self.tables.contains(table) {
                return errinput!("unknown table {table}");
            }
            if let Some(index) = self.qualified.get(&(table.to_string(), name.to_string())) {
                return Ok(*index);
            }
        } else if let Some(indexes) = self.unqualified.get(name) {
            if indexes.len() > 1 {
                return errinput!("ambiguous column {name}");
            }
            return Ok(indexes[0]);
        }
        if !self.aggregates.is_empty() {
            return errinput!(
                "column {} must be used in an aggregate or GROUP BY expression",
                fmt_name()
            );
        }
        errinput!("unknown column {}", fmt_name())
    }

    /// Adds an aggregate expression to the scope, returning the new column
    /// index or None if the expression already exists. This is either an
    /// aggregate function or a GROUP BY expression, used to look up the
    /// aggregate output column from e.g. SELECT, HAVING, and ORDER BY.
    fn add_aggregate(&mut self, expr: &ast::Expression, parent: &Scope) -> Option<usize> {
        if self.aggregates.contains_key(expr) {
            return None;
        }
        // If this is a simple column reference (i.e. GROUP BY foo), pass
        // through the column label from the parent scope for lookups.
        let mut label = types::Label::None;
        if let ast::Expression::Column(table, column) = expr {
            // Ignore errors, they will be emitted when building the expression.
            if let Ok(index) = parent.lookup_column(table.as_deref(), column.as_str()) {
                label = parent.columns[index].clone();
            }
        }
        let index = self.add_column(label);
        self.aggregates.insert(expr.clone(), index);
        Some(index)
    }

    /// Looks up an aggregate column index by aggregate function or GROUP BY
    /// expression.
    fn lookup_aggregate(&self, expr: &ast::Expression) -> Option<usize> {
        self.aggregates.get(expr).copied()
    }

    /// Adds a column that passes through a column from the parent scope,
    /// retaining its properties. If hide is true, the column is hidden.
    fn add_passthrough(&mut self, parent: &Scope, parent_index: usize, hide: bool) -> usize {
        let index = self.add_column(parent.columns[parent_index].clone());
        for (expr, i) in &parent.aggregates {
            if *i == parent_index {
                self.aggregates.entry(expr.clone()).or_insert(index);
            }
        }
        if hide || parent.hidden.contains(&parent_index) {
            self.hidden.insert(index);
        }
        index
    }

    /// Merges two scopes, by appending the given scope to self.
    fn merge(&mut self, scope: Scope) -> RaftDBResult<()> {
        for table in scope.tables {
            if self.tables.contains(&table) {
                return errinput!("duplicate table name {table}");
            }
            self.tables.insert(table);
        }
        let offset = self.columns.len();
        for label in scope.columns {
            self.add_column(label);
        }
        for (expr, index) in scope.aggregates {
            self.aggregates.entry(expr).or_insert(index + offset);
        }
        self.hidden.extend(scope.hidden.into_iter().map(|index| index + offset));
        Ok(())
    }

    /// Projects the scope via the given expressions and aliases, creating a new
    /// child scope with one column per expression. These may be a simple column
    /// reference (e.g. "SELECT a, b FROM table"), which passes through the
    /// corresponding column from the original scope and retains its qualified
    /// and unqualified names. Otherwise, for non-trivial column references, a
    /// new column is created for the expression. Explicit aliases may be given.
    fn project(&self, expressions: &[(ast::Expression, Option<String>)]) -> Self {
        let mut child = self.spawn();
        for (expr, alias) in expressions {
            // Use the alias if given, or look up any column references.
            let mut label = types::Label::None;
            if let Some(alias) = alias {
                label = types::Label::Unqualified(alias.clone());
            } else if let ast::Expression::Column(table, column) = expr {
                // Ignore errors, they will be surfaced in build_expression().
                if let Ok(index) = self.lookup_column(table.as_deref(), column.as_str()) {
                    label = self.columns[index].clone();
                }
            }
            let index = child.add_column(label);
            // If this is an aggregate query, then all projected expressions
            // must also be aggregates by definition (an aggregate node can only
            // emit aggregate functions or GROUP BY expressions).
            if !self.aggregates.is_empty() {
                child.aggregates.entry(expr.clone()).or_insert(index);
            }
        }
        child
    }

    /// Remaps the scope using the given targets.
    fn remap(&self, targets: &[Option<usize>]) -> Self {
        let mut child = self.spawn();
        for index in remap_sources(targets).into_iter().flatten() {
            child.add_passthrough(self, index, false);
        }
        child
    }

    /// Removes hidden columns from the scope, returning their indexes or None
    /// if no columns are hidden.
    fn remove_hidden(&mut self) -> Option<HashSet<usize>> {
        if self.hidden.is_empty() {
            return None;
        }
        let hidden = std::mem::take(&mut self.hidden);
        let mut index = 0;
        self.columns.retain(|_| {
            let retain = !hidden.contains(&index);
            index += 1;
            retain
        });
        self.qualified.retain(|_, index| !hidden.contains(index));
        self.unqualified.iter_mut().for_each(|(_, vec)| vec.retain(|i| !hidden.contains(i)));
        self.unqualified.retain(|_, vec| !vec.is_empty());
        self.aggregates.retain(|_, index| !hidden.contains(index));
        Some(hidden)
    }
    
    /// Removes hidden columns from the scope and returns the remaining column
    /// indexes as a Remap targets vector, or None if no columns are hidden. A
    /// Remap targets vector maps parent column indexes to child column indexes,
    /// or None if a column should be dropped.
    fn remap_hidden(&mut self) -> Option<Vec<Option<usize>>> {
        let size = self.columns.len();
        let hidden = self.remove_hidden()?;
        let mut targets = vec![None; size];
        let mut index = 0;
        for (old_index, target) in targets.iter_mut().enumerate() {
            if !hidden.contains(&old_index) {
                *target = Some(index);
                index += 1;
            }
        }
        Some(targets)
    }
}