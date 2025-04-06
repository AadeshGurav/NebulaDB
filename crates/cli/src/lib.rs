//! CLI tools for NebulaDB
//!
//! This module provides command-line tools for interacting with NebulaDB.

use std::path::PathBuf;

use clap::{Parser, Subcommand};
use nebuladb_core::{Error, Result, Config};
use nebuladb_storage::{StorageConfig, wal_integration::DatabaseStore};
use nebuladb_wal::WalConfig;

pub mod repl;
pub mod commands;

/// Command line interface for NebulaDB
#[derive(Parser, Debug)]
#[clap(name = "nebuladb", version, about = "NebulaDB database engine")]
pub struct Cli {
    /// Path to database directory
    #[clap(short, long, value_parser, default_value = "./data")]
    pub db_path: PathBuf,

    /// Database command
    #[clap(subcommand)]
    pub command: Option<Commands>,
}

/// Available commands
#[derive(Subcommand, Debug)]
pub enum Commands {
    /// Start interactive shell
    #[clap(name = "shell")]
    Shell,
    
    /// Run a query
    #[clap(name = "query")]
    Query {
        /// Query string
        #[clap(value_parser)]
        query: String,
    },
    
    /// Create a collection
    #[clap(name = "create")]
    CreateCollection {
        /// Collection name
        #[clap(value_parser)]
        name: String,
    },
}

/// CLI configuration
#[derive(Debug, Clone)]
pub struct CliConfig {
    /// Core configuration
    pub core: Config,
    /// Storage configuration
    pub storage: StorageConfig,
    /// WAL configuration
    pub wal: WalConfig,
}

impl Default for CliConfig {
    fn default() -> Self {
        Self {
            core: Config::default(),
            storage: StorageConfig::default(),
            wal: WalConfig::default(),
        }
    }
}

/// CLI application
pub struct CliApp {
    /// CLI configuration
    config: CliConfig,
    /// Database store
    db: DatabaseStore,
}

impl CliApp {
    /// Create a new CLI application
    pub fn new(config: CliConfig) -> Result<Self> {
        let db = DatabaseStore::new(config.storage.clone(), config.wal.clone())?;
        
        Ok(Self {
            config,
            db,
        })
    }
    
    /// Start the REPL
    pub fn start_repl(&mut self) -> Result<()> {
        repl::start(self)?;
        Ok(())
    }
    
    /// Get a mutable reference to the database store
    pub fn db_mut(&mut self) -> &mut DatabaseStore {
        &mut self.db
    }
    
    /// Get a reference to the database store
    pub fn db(&self) -> &DatabaseStore {
        &self.db
    }
    
    /// Get a reference to the configuration
    pub fn config(&self) -> &CliConfig {
        &self.config
    }
}

/// Run the CLI
pub fn run(cli: Cli) -> Result<()> {
    match cli.command {
        Some(Commands::Shell) => {
            repl::run_repl()
        },
        Some(Commands::Query { query }) => {
            println!("Query: {}", query);
            // TODO: Execute query
            Ok(())
        },
        Some(Commands::CreateCollection { name }) => {
            println!("Creating collection: {}", name);
            // TODO: Create collection
            Ok(())
        },
        None => {
            // Default to shell mode
            repl::run_repl()
        },
    }
}

/// Start the CLI application
pub fn start() -> Result<()> {
    let config = CliConfig::default();
    let mut cli = CliApp::new(config)?;
    
    cli.start_repl()?;
    
    Ok(())
}
