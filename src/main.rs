use std::path::Path;
use std::env;
use std::process;
use std::io;
use nebuladb_core::{Result, Error};
use nebuladb_storage::StorageConfig;
use crate::interfaces::InterfaceManager;
use crate::config::SystemConfig;
use crate::connection_pool::{ConnectionPool, ConnectionPoolConfig};

mod database;
mod interfaces;
mod util;
mod config;
mod connection_pool;

fn print_usage() {
    println!("NebulaDB - A distributed document database");
    println!("Usage:");
    println!("  nebuladb [options]");
    println!();
    println!("Options:");
    println!("  --config <file>       Load configuration from file");
    println!("  --generate-config     Generate a default configuration file");
    println!("  --production          Run in production mode (multiple interfaces)");
    println!("  --help                Show this help message");
}

fn main() -> Result<()> {
    let args: Vec<String> = env::args().collect();
    
    // Parse command line arguments
    let mut config_path = None;
    let mut generate_config = false;
    let mut production_mode = false;
    
    let mut i = 1;
    while i < args.len() {
        match args[i].as_str() {
            "--config" => {
                if i + 1 < args.len() {
                    config_path = Some(args[i + 1].clone());
                    i += 2;
                } else {
                    eprintln!("Error: Missing argument for --config");
                    process::exit(1);
                }
            },
            "--generate-config" => {
                generate_config = true;
                i += 1;
            },
            "--production" => {
                production_mode = true;
                i += 1;
            },
            "--help" => {
                print_usage();
                process::exit(0);
            },
            _ => {
                eprintln!("Unknown option: {}", args[i]);
                print_usage();
                process::exit(1);
            }
        }
    }
    
    // Handle config generation
    if generate_config {
        let config = SystemConfig::default();
        config.save_to_file("nebuladb.json")?;
        println!("Generated default configuration file: nebuladb.json");
        
        if !production_mode && config_path.is_none() {
            return Ok(());
        }
    }
    
    // Load configuration
    let system_config = match config_path {
        Some(path) => {
            println!("Loading configuration from: {}", path);
            SystemConfig::load_from_file(&path)?
        },
        None => {
            println!("Using default configuration");
            SystemConfig::default()
        }
    };
    
    // Create the data directory if it doesn't exist
    let data_dir = system_config.data_dir.as_path();
    if !data_dir.exists() {
        std::fs::create_dir_all(data_dir)
            .map_err(|e| Error::IoError(e))?;
        println!("Created data directory: {:?}", data_dir);
    }
    
    // Create storage config from system config
    let storage_config = system_config.to_storage_config();
    
    // Create interface manager
    let mut manager = InterfaceManager::new(data_dir, storage_config)?;
    
    // Configure connection limits
    manager.configure_connections(
        system_config.concurrency.max_databases * 100, // Assuming ~100 connections per database
        system_config.concurrency.transaction_timeout
    );
    
    // Enable interfaces based on configuration
    if system_config.interfaces.enable_cli {
        println!("Enabling CLI interface");
        manager.enable_cli()?;
    }
    
    if system_config.interfaces.http.enabled || production_mode {
        println!("Enabling HTTP interface on port {}", system_config.interfaces.http.port);
        manager.enable_http(system_config.interfaces.http.port)?;
    }
    
    if system_config.interfaces.grpc.enabled || production_mode {
        println!("Enabling gRPC interface on port {}", system_config.interfaces.grpc.port);
        manager.enable_grpc(system_config.interfaces.grpc.port)?;
    }
    
    println!("Starting NebulaDB in {} mode", 
             if production_mode { "production" } else { "normal" });
    println!("Data directory: {:?}", data_dir);
    println!("Maximum databases: {}", system_config.concurrency.max_databases);
    println!("Maximum collections per database: {}", system_config.concurrency.max_collections_per_db);
    
    // Start all enabled interfaces
    manager.start()?;
    
    Ok(())
}
