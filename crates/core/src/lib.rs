//! Core functionality for NebulaDB
use serde::{Serialize, Deserialize};

/// Represents a database error
#[derive(Debug)]
pub enum Error {
    IoError(std::io::Error),
    Other(String),
}

pub type Result<T> = std::result::Result<T, Error>;

/// Core configuration for the database
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Config {
    pub data_dir: String,
    pub max_size: usize,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            data_dir: String::from("/tmp/nebuladb"),
            max_size: 1024 * 1024 * 1024, // 1GB
        }
    }
}

pub fn add(left: usize, right: usize) -> usize {
    left + right
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn it_works() {
        let result = add(2, 2);
        assert_eq!(result, 4);
    }

    #[test]
    fn test_config_default() {
        let config = Config::default();
        assert_eq!(config.data_dir, "/tmp/nebuladb");
        assert_eq!(config.max_size, 1024 * 1024 * 1024);
    }
}
