mod error;

use std::fmt::Display;
use serde::{Deserialize, Serialize};

#[macro_export]
macro_rules! errdata {
    ($($args:tt)*) => {
        $crate::RaftDBError::InvalidData(format!($($args)*)).into()
    };
}

#[macro_export]
macro_rules! errinput {
     ($($args:tt)*) => {
         $crate::RaftDBError::InvalidInput(format!($($args)*)).into()
     };
 }

/// DB errors
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum RaftDBError {
    /// The operation was aborted and must be retried. This typically happens
    /// with e.g. Raft leader changes. This is used instead of implementing
    /// complex retry logic and replay protection in Raft.
    Abort,
    /// Invalid data, typically decoding errors or unexpected internal values.
    InvalidData(String),
    /// Invalid user input, typically sql_parser or query errors.
    InvalidInput(String),
    /// An IO error.
    IO(String),
    /// A write was attempted in a read-only transaction.
    ReadOnly,
    /// A write transaction conflicted with a different writer and lost. The
    /// transaction must be retried.
    Serialization,
}

impl std::error::Error for RaftDBError {}

impl std::fmt::Display for RaftDBError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            RaftDBError::Abort => write!(f, "operation aborted"),
            RaftDBError::InvalidData(msg) => write!(f, "invalid data: {msg}"),
            RaftDBError::InvalidInput(msg) => write!(f, "invalid input: {msg}"),
            RaftDBError::IO(msg) => write!(f, "io error: {msg}"),
            RaftDBError::ReadOnly => write!(f, "read-only transaction"),
            RaftDBError::Serialization => write!(f, "serialization failure, retry transaction"),
        }
    }
}

impl RaftDBError {
    /// Returns whether the error is considered deterministic. Raft state
    /// machine application needs to know whether a command failure is
    /// deterministic on the input command -- if it is, the command can be
    /// considered applied and the error returned to the client, but otherwise
    /// the state machine must panic to prevent node divergence.
    pub fn is_deterministic(&self) -> bool {
        match self {
            // Aborts don't happen during application, only leader changes. But
            // we consider them non-deterministic in case an abort should happen
            // unexpectedly below Raft.
            RaftDBError::Abort => false,
            // Possible data corruption local to this node.
            RaftDBError::InvalidData(_) => false,
            // Input errors are (likely) deterministic. They might not be in
            // case data was corrupted in flight, but we ignore this case.
            RaftDBError::InvalidInput(_) => true,
            // IO errors are typically local to the node (e.g. faulty disk).
            RaftDBError::IO(_) => false,
            // Write commands in read-only transactions are deterministic.
            RaftDBError::ReadOnly => true,
            // Serialization errors are non-deterministic.
            RaftDBError::Serialization => false,
        }
    }
}


impl serde::de::Error for RaftDBError {
    fn custom<T>(msg: T) -> Self
    where
        T: Display
    {
        RaftDBError::InvalidData(msg.to_string())
    }
}

impl serde::ser::Error for RaftDBError {
    fn custom<T>(msg: T) -> Self
    where
        T: Display
    {
        RaftDBError::InvalidData(msg.to_string())
    }
}

impl From<bincode::error::DecodeError> for RaftDBError {
    fn from(err: bincode::error::DecodeError) -> Self {
        RaftDBError::InvalidData(err.to_string())
    }
}

impl From<bincode::error::EncodeError> for RaftDBError {
    fn from(err: bincode::error::EncodeError) -> Self {
        RaftDBError::InvalidData(err.to_string())
    }
}

impl From<config::ConfigError> for RaftDBError {
    fn from(err: config::ConfigError) -> Self {
        RaftDBError::InvalidInput(err.to_string())
    }
}

impl From<crossbeam::channel::RecvError> for RaftDBError {
    fn from(err: crossbeam::channel::RecvError) -> Self {
        RaftDBError::IO(err.to_string())
    }
}

impl From<crossbeam::channel::TryRecvError> for RaftDBError {
    fn from(err: crossbeam::channel::TryRecvError) -> Self {
        RaftDBError::IO(err.to_string())
    }
}

impl<T> From<crossbeam::channel::TrySendError<T>> for RaftDBError {
    fn from(err: crossbeam::channel::TrySendError<T>) -> Self {
        RaftDBError::IO(err.to_string())
    }
}

impl From<hdrhistogram::CreationError> for RaftDBError {
    fn from(err: hdrhistogram::CreationError) -> Self {
        panic!("{err}") // faulty code
    }
}

impl From<hdrhistogram::RecordError> for RaftDBError {
    fn from(err: hdrhistogram::RecordError) -> Self {
        RaftDBError::InvalidInput(err.to_string())
    }
}

impl From<log::ParseLevelError> for RaftDBError {
    fn from(err: log::ParseLevelError) -> Self {
        RaftDBError::InvalidInput(err.to_string())
    }
}

impl From<log::SetLoggerError> for RaftDBError {
    fn from(err: log::SetLoggerError) -> Self {
        panic!("{err}") // faulty code
    }
}

impl From<regex::Error> for RaftDBError {
    fn from(err: regex::Error) -> Self {
        panic!("{err}") // faulty code
    }
}

impl From<rustyline::error::ReadlineError> for RaftDBError {
    fn from(err: rustyline::error::ReadlineError) -> Self {
        RaftDBError::IO(err.to_string())
    }
}

impl From<std::array::TryFromSliceError> for RaftDBError {
    fn from(err: std::array::TryFromSliceError) -> Self {
        RaftDBError::InvalidData(err.to_string())
    }
}

impl From<std::io::Error> for RaftDBError {
    fn from(err: std::io::Error) -> Self {
        RaftDBError::IO(err.to_string())
    }
}

impl From<std::num::ParseFloatError> for RaftDBError {
    fn from(err: std::num::ParseFloatError) -> Self {
        RaftDBError::InvalidInput(err.to_string())
    }
}

impl From<std::num::ParseIntError> for RaftDBError {
    fn from(err: std::num::ParseIntError) -> Self {
        RaftDBError::InvalidInput(err.to_string())
    }
}

impl From<std::num::TryFromIntError> for RaftDBError {
    fn from(err: std::num::TryFromIntError) -> Self {
        RaftDBError::InvalidData(err.to_string())
    }
}

impl From<std::string::FromUtf8Error> for RaftDBError {
    fn from(err: std::string::FromUtf8Error) -> Self {
        RaftDBError::InvalidData(err.to_string())
    }
}

impl<T> From<std::sync::PoisonError<T>> for RaftDBError {
    fn from(err: std::sync::PoisonError<T>) -> Self {
        // This only happens when a different thread panics while holding a
        // mutex. This should be fatal, so we panic here too.
        panic!("{err}")
    }
}


/// A toyDB Result returning Error.
pub type RaftDBResult<T> = std::result::Result<T, RaftDBError>;

impl<T> From<RaftDBError> for RaftDBResult<T> {
    fn from(error: RaftDBError) -> Self {
        Err(error)
    }
}