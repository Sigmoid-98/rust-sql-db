use std::fmt::{write, Display, Formatter};
use serde::{Serialize, Deserialize};





#[derive(Clone, Copy, Debug, Hash, PartialEq, Serialize, Deserialize)]
pub enum DataType {
    Boolean,
    Integer,
    Float,
    String,
}

impl Display for DataType {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            DataType::Boolean => write!(f, "BOOLEAN"),
            DataType::Integer => write!(f, "INTEGER"),
            DataType::Float => write!(f, "FLOAT"),
            DataType::String => write!(f, "STRING"),
        }
    }
}