use serde::{Deserialize, Serialize};
use crate::planner::Node;
use crate::types;
use crate::types::Value;

/// An expression, made up of nested operations and values. Values are either
/// constants or dynamic column references. Evaluates to a final value during
/// query execution, using row values for column references.
///
/// Since this is a recursive data structure, we have to box each child
/// expression, which incurs a heap allocation per expression node. There are
/// clever ways to avoid this, but we keep it simple.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum Expression {
    /// A constant value.
    Constant(Value),
    /// A column reference. Used as row index when evaluating expressions.
    Column(usize),

    /// Logical AND of two booleans: a AND b.
    And(Box<Expression>, Box<Expression>),
    /// Logical OR of two booleans: a OR b.
    Or(Box<Expression>, Box<Expression>),
    /// Logical NOT of a boolean: NOT a.
    Not(Box<Expression>),

    /// Equality comparison of two values: a = b.
    Equal(Box<Expression>, Box<Expression>),
    /// Greater than comparison of two values: a > b.
    GreaterThan(Box<Expression>, Box<Expression>),
    /// Less than comparison of two values: a < b.
    LessThan(Box<Expression>, Box<Expression>),
    /// Checks for the given value: IS NULL or IS NAN.
    Is(Box<Expression>, Value),

    /// Adds two numbers: a + b.
    Add(Box<Expression>, Box<Expression>),
    /// Divides two numbers: a / b.
    Divide(Box<Expression>, Box<Expression>),
    /// Exponentiates two numbers, i.e. a ^ b.
    Exponentiate(Box<Expression>, Box<Expression>),
    /// Takes the factorial of a number: 4! = 4*3*2*1.
    Factorial(Box<Expression>),
    /// The identify function, which simply returns the same number: +a.
    Identity(Box<Expression>),
    /// Multiplies two numbers: a * b.
    Multiply(Box<Expression>, Box<Expression>),
    /// Negates the given number: -a.
    Negate(Box<Expression>),
    /// The remainder after dividing two numbers: a % b.
    Remainder(Box<Expression>, Box<Expression>),
    /// Takes the square root of a number: √a.
    SquareRoot(Box<Expression>),
    /// Subtracts two numbers: a - b.
    Subtract(Box<Expression>, Box<Expression>),

    /// Checks if a string matches a pattern: a LIKE b.
    Like(Box<Expression>, Box<Expression>),
}

impl From<Value> for Expression {
    fn from(value: Value) -> Self {
        Expression::Constant(value)
    }
}

impl From<Value> for Box<Expression> {
    fn from(value: Value) -> Self {
        Box::new(value.into())
    }
}

impl Expression {
    /// Formats the expression, using the given plan node to look up labels for
    /// numeric column references.
    pub fn format(&self, node: &Node) -> String {

        // Precedence levels, for grouping. Matches the parser precedence.
        fn precedence(expr: &Expression) -> u8 {
            use types::Expression::*;
            match expr {
                Column(_) | Constant(_) | SquareRoot(_) => 11,
                Identity(_) | Negate(_) => 10,
                Factorial(_) => 9,
                Exponentiate(_, _) => 8,
                Multiply(_, _) | Divide(_, _) | Remainder(_, _) => 7,
                Add(_, _) | Subtract(_, _) => 6,
                GreaterThan(_, _) | LessThan(_, _) => 5,
                Equal(_, _) | Like(_, _) | Is(_, _) => 4,
                Not(_) => 3,
                And(_, _) => 2,
                Or(_, _) => 1,
            }
        }

        // Helper to format a boxed expression, grouping it with () if needed.
        let format = |expr: &Expression| {
            let mut string = expr.format(node);
            if precedence(expr) < precedence(self) {
                string = format!("({string})");
            }
            string
        };
        
        match self {
            Expression::Constant(value) => format!("{value}"),
            Expression::Column(index) => match node.column_label(*index) {
                Label::None => format!("#{index}"),
                label => format!("{label}"),
            },

            Expression::And(lhs, rhs) => format!("{} AND {}", format(lhs), format(rhs)),
            Expression::Or(lhs, rhs) => format!("{} OR {}", format(lhs), format(rhs)),
            Expression::Not(expr) => format!("NOT {}", format(expr)),

            Expression::Equal(lhs, rhs) => format!("{} = {}", format(lhs), format(rhs)),
            Expression::GreaterThan(lhs, rhs) => format!("{} > {}", format(lhs), format(rhs)),
            Expression::LessThan(lhs, rhs) => format!("{} < {}", format(lhs), format(rhs)),
            Expression::Is(expr, Value::Null) => format!("{} IS NULL", format(expr)),
            Expression::Is(expr, Value::Float(f)) if f.is_nan() => format!("{} IS NAN", format(expr)),
            Expression::Is(_, v) => panic!("unexpected IS value {v}"),

            Expression::Add(lhs, rhs) => format!("{} + {}", format(lhs), format(rhs)),
            Expression::Divide(lhs, rhs) => format!("{} / {}", format(lhs), format(rhs)),
            Expression::Exponentiate(lhs, rhs) => format!("{} ^ {}", format(lhs), format(rhs)),
            Expression::Factorial(expr) => format!("{}!", format(expr)),
            Expression::Identity(expr) => format(expr),
            Expression::Multiply(lhs, rhs) => format!("{} * {}", format(lhs), format(rhs)),
            Expression::Negate(expr) => format!("-{}", format(expr)),
            Expression::Remainder(lhs, rhs) => format!("{} % {}", format(lhs), format(rhs)),
            Expression::SquareRoot(expr) => format!("sqrt({})", format(expr)),
            Expression::Subtract(lhs, rhs) => format!("{} - {}", format(lhs), format(rhs)),

            Expression::Like(lhs, rhs) => format!("{} LIKE {}", format(lhs), format(rhs)),
        }
    }
}
