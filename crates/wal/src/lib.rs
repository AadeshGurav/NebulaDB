//! Write-Ahead Log (WAL) for NebulaDB
//! 
//! This module handles the Write-Ahead Logging for durability and crash recovery.
//! Each operation is logged before it's applied to the main storage.

mod entry;
mod log;
pub mod manager;
pub mod config;
pub mod error;

pub use entry::{WalEntry, EntryType, EntryHeader};
pub use log::WalLog;
pub use config::WalConfig;
