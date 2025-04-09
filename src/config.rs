use std::path::PathBuf;
use std::fs::File;
use std::io::Read;
use nebuladb_core::{Result, Error, Config as CoreConfig};
use nebuladb_storage::StorageConfig;
use nebuladb_wal::WalConfig;
use serde::{Serialize, Deserialize};
use crate::interfaces::http::ConnectionPoolConfig;
use crate::interfaces::grpc::GrpcConnectionPoolConfig;

/// System-wide configuration for NebulaDB
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SystemConfig {
    /// Base directory for data files
    pub data_dir: PathBuf,
    
    /// Core database configuration
    pub core: CoreConfig,
    
    /// Storage engine configuration
    pub storage: StorageEngineConfig,
    
    /// Write-ahead log configuration
    pub wal: WalConfig,
    
    /// Interface configuration
    pub interfaces: InterfaceConfig,
    
    /// Multi-threading and concurrency configuration
    pub concurrency: ConcurrencyConfig,
}

/// Storage engine configuration
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct StorageEngineConfig {
    /// Block size in bytes (default: 4MB)
    pub block_size: usize,
    
    /// Compression algorithm
    pub compression_type: String,
    
    /// Auto-flush threshold (number of documents)
    pub flush_threshold: usize,
    
    /// Cache size in MB
    pub cache_size_mb: usize,
}

/// Interface configuration
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct InterfaceConfig {
    /// Enable CLI interface
    pub enable_cli: bool,
    
    /// HTTP interface configuration
    pub http: HttpConfig,
    
    /// gRPC interface configuration
    pub grpc: GrpcConfig,
}

/// HTTP interface configuration
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct HttpConfig {
    /// Enable HTTP interface
    pub enabled: bool,
    
    /// Port to listen on
    pub port: u16,
    
    /// Connection pool configuration
    pub pool: ConnectionPoolConfig,
}

/// gRPC interface configuration
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct GrpcConfig {
    /// Enable gRPC interface
    pub enabled: bool,
    
    /// Port to listen on
    pub port: u16,
    
    /// Connection pool configuration
    pub pool: GrpcConnectionPoolConfig,
}

/// Concurrency configuration
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ConcurrencyConfig {
    /// Maximum number of databases
    pub max_databases: usize,
    
    /// Maximum number of open collections per database
    pub max_collections_per_db: usize,
    
    /// Thread pool size for background operations
    pub background_threads: usize,
    
    /// Use transactions
    pub use_transactions: bool,
    
    /// Transaction timeout in seconds
    pub transaction_timeout: u64,
}

impl Default for SystemConfig {
    fn default() -> Self {
        Self {
            data_dir: PathBuf::from("./data"),
            core: CoreConfig::default(),
            storage: StorageEngineConfig::default(),
            wal: WalConfig {
                dir_path: "./data/wal".to_string(),
                sync_on_write: true,
                checkpoint_interval: 60,
                max_file_size: 64 * 1024 * 1024, // 64MB
            },
            interfaces: InterfaceConfig::default(),
            concurrency: ConcurrencyConfig::default(),
        }
    }
}

impl Default for StorageEngineConfig {
    fn default() -> Self {
        Self {
            block_size: 4 * 1024 * 1024, // 4MB
            compression_type: "zstd".to_string(),
            flush_threshold: 1000,
            cache_size_mb: 128, // 128MB cache
        }
    }
}

impl Default for InterfaceConfig {
    fn default() -> Self {
        Self {
            enable_cli: true,
            http: HttpConfig::default(),
            grpc: GrpcConfig::default(),
        }
    }
}

impl Default for HttpConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            port: 8080,
            pool: ConnectionPoolConfig::default(),
        }
    }
}

impl Default for GrpcConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            port: 50051,
            pool: GrpcConnectionPoolConfig::default(),
        }
    }
}

impl Default for ConcurrencyConfig {
    fn default() -> Self {
        Self {
            max_databases: 10,
            max_collections_per_db: 100,
            background_threads: 4,
            use_transactions: true,
            transaction_timeout: 30,
        }
    }
}

impl SystemConfig {
    /// Load configuration from a file
    pub fn load_from_file(path: &str) -> Result<Self> {
        let mut file = File::open(path)
            .map_err(|e| Error::IoError(e))?;
            
        let mut contents = String::new();
        file.read_to_string(&mut contents)
            .map_err(|e| Error::IoError(e))?;
            
        let config = serde_json::from_str(&contents)
            .map_err(|e| Error::Other(format!("Failed to parse config: {}", e)))?;
            
        Ok(config)
    }
    
    /// Save configuration to a file
    pub fn save_to_file(&self, path: &str) -> Result<()> {
        let contents = serde_json::to_string_pretty(self)
            .map_err(|e| Error::Other(format!("Failed to serialize config: {}", e)))?;
            
        std::fs::write(path, contents)
            .map_err(|e| Error::IoError(e))?;
            
        Ok(())
    }
    
    /// Create a storage config from this system config
    pub fn to_storage_config(&self) -> StorageConfig {
        use nebuladb_storage::CompressionType;
        
        let compression = match self.storage.compression_type.as_str() {
            "none" => CompressionType::None,
            "snappy" => CompressionType::Snappy,
            "lz4" => CompressionType::Lz4,
            _ => CompressionType::Zstd, // Default to zstd
        };
        
        StorageConfig {
            base: self.core.clone(),
            block_size: self.storage.block_size,
            compression,
            flush_threshold: self.storage.flush_threshold,
        }
    }
}
