use std::io;
use thiserror::Error;
use nebuladb_core;

#[derive(Error, Debug)]
pub enum WalError {
    #[error("IO error: {0}")]
    Io(#[from] io::Error),
    
    #[error("Corrupted WAL entry")]
    CorruptedEntry,
    
    #[error("Invalid segment")]
    InvalidSegment,
    
    #[error("Segment full")]
    SegmentFull,
    
    #[error("Invalid configuration: {0}")]
    InvalidConfig(String),
    
    #[error("{0}")]
    Other(String),
}

pub type Result<T> = std::result::Result<T, WalError>;

// Implement From<WalError> for nebuladb_core::Error
impl From<WalError> for nebuladb_core::Error {
    fn from(err: WalError) -> Self {
        match err {
            WalError::Io(e) => nebuladb_core::Error::IoError(e),
            WalError::CorruptedEntry => nebuladb_core::Error::Other("Corrupted WAL entry".to_string()),
            WalError::InvalidSegment => nebuladb_core::Error::Other("Invalid WAL segment".to_string()),
            WalError::SegmentFull => nebuladb_core::Error::Other("WAL segment full".to_string()),
            WalError::InvalidConfig(msg) => nebuladb_core::Error::Other(format!("Invalid WAL config: {}", msg)),
            WalError::Other(msg) => nebuladb_core::Error::Other(msg),
        }
    }
}

// Implement From<nebuladb_core::Error> for WalError
impl From<nebuladb_core::Error> for WalError {
    fn from(err: nebuladb_core::Error) -> Self {
        match err {
            nebuladb_core::Error::IoError(e) => WalError::Io(e),
            _ => WalError::Other(format!("{:?}", err)),
        }
    }
} 