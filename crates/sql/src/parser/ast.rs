use std::collections::BTreeMap;
use crate::types::{DataType};

/// The statement is the root node of the Abstract Syntax Tree, and describes
/// the syntactic structure of a SQL query. It is built from a raw SQL string by
/// the parser, and passed on to the planner which validates it and builds an
/// execution plan from it.
#[derive(Debug)]
pub enum Statement {
    /// Begin a new transaction.
    Begin { read_only: bool, as_of: Option<u64> },
    /// Commit a transaction.
    Commit,
    /// Roll back a transaction.
    Rollback,
    /// Explain a statement.
    Explain(Box<Statement>),
    /// Create a new table.
    CreateTable { name: String, columns: Vec<Column> },
    /// Drop a table.
    DropTable { name: String, if_exists: bool },
    /// Delete matching rows.
    Delete { table: String, r#where: Option<Expression> },
    /// Insert new rows into a table.
    Insert {
        table: String,
        columns: Option<Vec<String>>, // columns given in values, using default for rest
        values: Vec<Vec<Expression>>, // rows to insert
    },
    /// Update rows in a table.
    Update {
        table: String,
        set: BTreeMap<String, Option<Expression>>, // column -> value, None for default value
        r#where: Option<Expression>,
    },
    /// Select matching rows.
    Select {
        select: Vec<(Expression, Option<String>)>,
        from: Vec<From>,
        r#where: Option<Expression>,
        group_by: Vec<Expression>,
        having: Option<Expression>,
        order_by: Vec<(Expression, Direction)>,
        offset: Option<Expression>,
        limit: Option<Expression>,
    }
}


/// A FROM item.
#[derive(Debug)]
pub enum From {
    Table { name: String, alias: Option<String> },
    Join { left: Box<From>, right: Box<From>, r#type: JoinType, predicate: Option<Expression> },
}

/// A CREATE TABLE column definition.
#[derive(Debug)]
pub struct Column {
    pub name: String,
    pub datatype: DataType,
    pub primary_key: bool,
    pub nullable: Option<bool>,
    pub default: Option<Expression>,
    pub unique: bool,
    pub index: bool,
    pub references: Option<String>,
}

/// JOIN types
#[derive(Debug, PartialEq)]
pub enum JoinType {
    Cross,
    Inner,
    Left,
    Right,
}

impl JoinType {
    /// true if the join is an outer join, where rows with no join matches are
    /// emitted with a NULL match.
    pub fn is_outer(&self) -> bool {
        match self {
            JoinType::Cross | JoinType::Inner => false,
            JoinType::Left | JoinType::Right => true,
        }
    }
}

/// ORDER BY direction
#[derive(Debug)]
pub enum Direction {
    Ascending,
    Descending,
}

/// Expressions. Can be nested.
#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub enum Expression {
    /// All columns, i.e. *.
    All,
    /// A column reference, optionally qualified with a table name.
    Column(Option<String>, String),
    /// A literal value.
    Literal(Literal),
    /// A function call (name and parameters).
    Function(String, Vec<Expression>),
    /// An operator.
    Operator(Operator),
}

impl core::convert::From<Literal> for Expression {
    fn from(literal: Literal) -> Self {
        Self::Literal(literal)
    }
}

impl core::convert::From<Operator> for Expression {
    fn from(operator: Operator) -> Self {
        Self::Operator(operator)
    }
}

impl core::convert::From<Operator> for Box<Expression> {
    fn from(value: Operator) -> Self {
        Box::new(value.into())
    }
}

impl Expression {
    /// Walks the expression tree depth-first, calling a closure for every node.
    /// Halts and returns false if the closure returns false.
    pub fn walk(&self, visitor: &mut impl FnMut(&Expression) -> bool) -> bool {
        if !visitor(self) { 
            return false;
        }
        
        match self {
            | Self::All
            | Self::Column(_, _) 
            | Self::Literal(_) => true,
            
            | Self::Function(_, exprs) => exprs.iter().any(|expr| expr.walk(visitor)),
            
            | Self::Operator(Operator::Add(lhs, rhs)) 
            | Self::Operator(Operator::And(lhs, rhs))
            | Self::Operator(Operator::Divide(lhs, rhs))
            | Self::Operator(Operator::Equal(lhs, rhs))
            | Self::Operator(Operator::Exponentiate(lhs, rhs))
            | Self::Operator(Operator::GreaterThan(lhs, rhs))
            | Self::Operator(Operator::GreaterThanOrEqual(lhs, rhs))
            | Self::Operator(Operator::LessThan(lhs, rhs))
            | Self::Operator(Operator::LessThanOrEqual(lhs, rhs))
            | Self::Operator(Operator::Like(lhs, rhs))
            | Self::Operator(Operator::Multiply(lhs, rhs))
            | Self::Operator(Operator::NotEqual(lhs, rhs))
            | Self::Operator(Operator::Or(lhs, rhs))
            | Self::Operator(Operator::Remainder(lhs, rhs))
            | Self::Operator(Operator::Subtract(lhs, rhs))
            => lhs.walk(visitor) && rhs.walk(visitor),
            
            | Self::Operator(Operator::Factorial(expr))
            | Self::Operator(Operator::Identity(expr))
            | Self::Operator(Operator::Is(expr, _))
            | Self::Operator(Operator::Negate(expr))
            | Self::Operator(Operator::Not(expr))
            => expr.walk(visitor),
        }
    }

    /// Walks the expression tree depth-first while calling a closure until it
    /// returns true. This is the inverse of walk().
    pub fn contains(&self, visitor: &impl Fn(&Expression) -> bool) -> bool {
        !self.walk(&mut |expr| !visitor(expr))
    }

    /// Find and collects expressions for which the given closure returns true,
    /// adding them to c. Does not recurse into matching expressions.
    pub fn collect(&self, visitor: &impl Fn(&Expression) -> bool, c: &mut Vec<Expression>) {
        if visitor(self) {
            c.push(self.clone());
            return;
        }
        
        match self {
            | Self::All
            | Self::Column(_, _)
            | Self::Literal(_) => {},

            | Self::Function(_, exprs) => exprs.iter().for_each(|expr| expr.collect(visitor, c)),

            | Self::Operator(Operator::Add(lhs, rhs))
            | Self::Operator(Operator::And(lhs, rhs))
            | Self::Operator(Operator::Divide(lhs, rhs))
            | Self::Operator(Operator::Equal(lhs, rhs))
            | Self::Operator(Operator::Exponentiate(lhs, rhs))
            | Self::Operator(Operator::GreaterThan(lhs, rhs))
            | Self::Operator(Operator::GreaterThanOrEqual(lhs, rhs))
            | Self::Operator(Operator::LessThan(lhs, rhs))
            | Self::Operator(Operator::LessThanOrEqual(lhs, rhs))
            | Self::Operator(Operator::Like(lhs, rhs))
            | Self::Operator(Operator::Multiply(lhs, rhs))
            | Self::Operator(Operator::NotEqual(lhs, rhs))
            | Self::Operator(Operator::Or(lhs, rhs))
            | Self::Operator(Operator::Remainder(lhs, rhs))
            | Self::Operator(Operator::Subtract(lhs, rhs))
            => {
                lhs.collect(visitor, c);
                rhs.collect(visitor, c);
            },

            | Self::Operator(Operator::Factorial(expr))
            | Self::Operator(Operator::Identity(expr))
            | Self::Operator(Operator::Is(expr, _))
            | Self::Operator(Operator::Negate(expr))
            | Self::Operator(Operator::Not(expr))
            => expr.collect(visitor, c),
        }
    } 
}


/// Expression literal values.
#[derive(Debug, Clone)]
pub enum Literal {
    Null,
    Boolean(bool),
    Integer(i64),
    Float(f64),
    String(String),
}

impl PartialEq for Literal {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Literal::Boolean(a), Literal::Boolean(b)) => a == b,
            (Literal::Integer(a), Literal::Integer(b)) => a == b,
            // Implies NaN == NaN but -NaN != NaN. Similarly with +/-0.0.
            (Literal::Float(a), Literal::Float(b)) => a.to_bits() == b.to_bits(),
            (Literal::String(a), Literal::String(b)) => a == b,
            (l, r) => core::mem::discriminant(l) == core::mem::discriminant(r),
        }
    }
}

impl Eq for Literal {}

impl std::hash::Hash for Literal {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        core::mem::discriminant(self).hash(state);
        match self {
            Literal::Null => {},
            Literal::Boolean(v) => v.hash(state),
            Literal::Integer(v) => v.hash(state),
            Literal::Float(v) => v.to_bits().hash(state),
            Literal::String(v) => v.hash(state),
        }
    }
}

/// Expression operators.
///
/// Since this is a recursive data structure, we have to box each child
/// expression, which incurs a heap allocation. There are clever ways to get
/// around this, but we keep it simple.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Operator {
    And(Box<Expression>, Box<Expression>), // a AND b
    Not(Box<Expression>),                  // NOT a
    Or(Box<Expression>, Box<Expression>),  // a OR b

    Equal(Box<Expression>, Box<Expression>),       // a = b
    GreaterThan(Box<Expression>, Box<Expression>), // a > b
    GreaterThanOrEqual(Box<Expression>, Box<Expression>), // a != b
    Is(Box<Expression>, Literal),                  // IS NULL or IS NAN
    LessThan(Box<Expression>, Box<Expression>),    // a < b
    LessThanOrEqual(Box<Expression>, Box<Expression>), // a <= b
    NotEqual(Box<Expression>, Box<Expression>),    // a != b

    Add(Box<Expression>, Box<Expression>),          // a + b
    Divide(Box<Expression>, Box<Expression>),       // a / b
    Exponentiate(Box<Expression>, Box<Expression>), // a ^ b
    Factorial(Box<Expression>),                     // a!
    Identity(Box<Expression>),                      // +a
    Multiply(Box<Expression>, Box<Expression>),     // a * b
    Negate(Box<Expression>),                        // -a
    Remainder(Box<Expression>, Box<Expression>),    // a % b
    Subtract(Box<Expression>, Box<Expression>),     // a - b

    Like(Box<Expression>, Box<Expression>), // a LIKE b
}

