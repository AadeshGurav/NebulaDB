use rustyline::{Editor, error::ReadlineError};
use std::path::Path;
use nebuladb_core::Result;
use crate::manager::DatabaseManager;
use nebuladb_storage::StorageConfig;
use crate::utils::{is_valid_json, format_output, matches_query};
use serde_json::Value as JsonValue;

pub fn run_cli() -> Result<()> {
    println!("NebulaDB v0.1.0 Interactive CLI");
    println!("Type 'help' for a list of commands");
    
    // Create a data directory
    let data_dir = Path::new("./data");
    
    // Create storage config
    let config = StorageConfig::default();
    
    // Create database manager
    let mut db_manager = DatabaseManager::new(data_dir, config)?;
    
    // Setup command history with rustyline
    let mut rl = Editor::<()>::new().expect("Failed to create editor");
    // Load history from ~/.nebuladb_history if it exists
    let history_path = dirs::home_dir()
        .unwrap_or_else(|| Path::new(".").to_path_buf())
        .join(".nebuladb_history");
    
    if rl.load_history(&history_path).is_err() {
        println!("No previous history found.");
    }
    
    loop {
        let prompt = match db_manager.get_active_database() {
            Some(db) => format!("nebuladb:{}> ", db),
            None => "nebuladb> ".to_string(),
        };
        
        match rl.readline(&prompt) {
            Ok(line) => {
                let input = line.trim();
                
                if input.is_empty() {
                    continue;
                }
                
                // Add to history
                rl.add_history_entry(input);
                
                let parts: Vec<&str> = input.split_whitespace().collect();
                let command = parts[0].to_lowercase();
                
                match command.as_str() {
                    "help" => {
                        print_help();
                    },
                    "createdb" => {
                        if parts.len() < 2 {
                            println!("Usage: createdb <database_name>");
                            continue;
                        }
                        
                        let name = parts[1];
                        
                        match db_manager.create_database(name) {
                            Ok(_) => {
                                println!("Database '{}' created successfully", name);
                            },
                            Err(e) => {
                                println!("Error creating database '{}': {:?}", name, e);
                            }
                        }
                    },
                    "usedb" => {
                        if parts.len() < 2 {
                            println!("Usage: usedb <database_name>");
                            continue;
                        }
                        
                        let name = parts[1];
                        
                        match db_manager.use_database(name) {
                            Ok(_) => {
                                println!("Using database '{}'", name);
                            },
                            Err(e) => {
                                println!("Error using database '{}': {:?}", name, e);
                            }
                        }
                    },
                    "listdbs" => {
                        match db_manager.list_databases() {
                            Ok(dbs) => {
                                if dbs.is_empty() {
                                    println!("No databases found");
                                } else {
                                    println!("Databases:");
                                    for db in dbs {
                                        println!("  - {}", db);
                                    }
                                }
                            },
                            Err(e) => {
                                println!("Error listing databases: {:?}", e);
                            }
                        }
                    },
                    "createcoll" => {
                        if parts.len() < 2 {
                            println!("Usage: createcoll <collection_name>");
                            continue;
                        }
                        
                        let name = parts[1];
                        
                        match db_manager.create_collection(name) {
                            Ok(_) => {
                                println!("Collection '{}' created successfully", name);
                            },
                            Err(e) => {
                                println!("Error creating collection '{}': {:?}", name, e);
                            }
                        }
                    },
                    "listcolls" => {
                        match db_manager.list_collections() {
                            Ok(colls) => {
                                if colls.is_empty() {
                                    println!("No collections found in the current database");
                                } else {
                                    println!("Collections:");
                                    for coll in colls {
                                        println!("  - {}", coll);
                                    }
                                }
                            },
                            Err(e) => {
                                println!("Error listing collections: {:?}", e);
                            }
                        }
                    },
                    "insert" => {
                        if parts.len() < 4 {
                            println!("Usage: insert <collection> <id> <data>");
                            continue;
                        }
                        
                        let collection_name = parts[1];
                        let id = parts[2].as_bytes();
                        // Join the rest of the parts to form the data
                        let data = parts[3..].join(" ").as_bytes().to_vec();
                        
                        match db_manager.insert_document(collection_name, id, &data) {
                            Ok(_) => {
                                println!("Document inserted successfully");
                            },
                            Err(e) => {
                                println!("Error inserting document: {:?}", e);
                            }
                        }
                    },
                    "json" => {
                        if parts.len() < 4 {
                            println!("Usage: json <collection> <id> <json_data>");
                            println!("Example: json users user123 {{\"name\":\"John\",\"age\":30}}");
                            continue;
                        }
                        
                        let collection_name = parts[1];
                        let id = parts[2].as_bytes();
                        
                        // Join the rest as the JSON string
                        let json_str = parts[3..].join(" ");
                        
                        // Validate JSON
                        if !is_valid_json(&json_str) {
                            println!("Error: Invalid JSON data");
                            continue;
                        }
                        
                        match db_manager.insert_json_document(collection_name, id, &json_str) {
                            Ok(_) => {
                                println!("JSON document inserted successfully");
                            },
                            Err(e) => {
                                println!("Error inserting document: {:?}", e);
                            }
                        }
                    },
                    "get" => {
                        if parts.len() < 3 {
                            println!("Usage: get <collection> <id>");
                            continue;
                        }
                        
                        let collection_name = parts[1];
                        let id = parts[2].as_bytes();
                        
                        match db_manager.get_document(collection_name, id) {
                            Ok(Some(data)) => {
                                let data_str = String::from_utf8_lossy(&data);
                                format_output(&data_str);
                            },
                            Ok(None) => {
                                println!("Document not found");
                            },
                            Err(e) => {
                                println!("Error retrieving document: {:?}", e);
                            }
                        }
                    },
                    "scan" => {
                        if parts.len() < 2 {
                            println!("Usage: scan <collection>");
                            continue;
                        }
                        
                        let collection_name = parts[1];
                        
                        match db_manager.scan_documents(collection_name) {
                            Ok(ids) => {
                                if ids.is_empty() {
                                    println!("No documents found in collection '{}'", collection_name);
                                } else {
                                    println!("Documents in collection '{}':", collection_name);
                                    for id in &ids {
                                        println!("  - {}", String::from_utf8_lossy(id));
                                    }
                                    println!("Total: {} documents", ids.len());
                                }
                            },
                            Err(e) => {
                                println!("Error scanning collection: {:?}", e);
                            }
                        }
                    },
                    "delete" => {
                        if parts.len() < 3 {
                            println!("Usage: delete <collection> <id>");
                            continue;
                        }
                        
                        let collection_name = parts[1];
                        let id = parts[2].as_bytes();
                        
                        match db_manager.delete_document(collection_name, id) {
                            Ok(true) => println!("Document deleted successfully"),
                            Ok(false) => println!("Document not found"),
                            Err(e) => println!("Error deleting document: {:?}", e),
                        }
                    },
                    "find" => {
                        if parts.len() < 2 {
                            println!("Usage: find <collection> [query]");
                            println!("Examples:");
                            println!("  find users                     - Get all documents");
                            println!("  find users {{\"name\":\"John\"}}    - Find documents where name = John");
                            continue;
                        }
                        
                        let collection_name = parts[1];
                        
                        // Parse query if provided
                        let query_str = if parts.len() > 2 {
                            parts[2..].join(" ")
                        } else {
                            "{}".to_string() // Empty query matches all documents
                        };
                        
                        // Validate JSON
                        let query = match serde_json::from_str::<JsonValue>(&query_str) {
                            Ok(q) => q,
                            Err(e) => {
                                println!("Invalid JSON query: {}", e);
                                continue;
                            }
                        };
                        
                        match db_manager.find_documents(collection_name, &query) {
                            Ok(results) => {
                                if results.is_empty() {
                                    println!("No documents matched the query");
                                } else {
                                    println!("Found {} matching document(s):", results.len());
                                    for (id, data) in results {
                                        println!("ID: {}", String::from_utf8_lossy(&id));
                                        let data_str = String::from_utf8_lossy(&data);
                                        format_output(&data_str);
                                        println!("---");
                                    }
                                }
                            },
                            Err(e) => {
                                println!("Error executing query: {:?}", e);
                            }
                        }
                    },
                    "compression" => {
                        if parts.len() < 2 {
                            println!("Usage: compression <type>");
                            println!("Available types: none, snappy, zstd, lz4");
                            continue;
                        }
                        
                        let compression_type = parts[1];
                        
                        match db_manager.set_compression(compression_type) {
                            Ok(_) => {
                                println!("Compression setting updated successfully");
                            },
                            Err(e) => {
                                println!("Error updating compression setting: {:?}", e);
                            }
                        }
                    },
                    "wal" => {
                        let wal_dir = if parts.len() > 1 {
                            Some(Path::new(parts[1]))
                        } else {
                            None
                        };
                        
                        match db_manager.enable_wal(wal_dir) {
                            Ok(_) => {
                                println!("WAL enabled successfully");
                            },
                            Err(e) => {
                                println!("Error enabling WAL: {:?}", e);
                            }
                        }
                    },
                    "checkpoint" => {
                        match db_manager.checkpoint() {
                            Ok(_) => {
                                println!("Checkpoint completed successfully");
                            },
                            Err(e) => {
                                println!("Error during checkpoint: {:?}", e);
                            }
                        }
                    },
                    "exit" | "quit" => {
                        println!("Closing all databases...");
                        
                        match db_manager.close_all() {
                            Ok(_) => {
                                println!("All databases closed successfully");
                            },
                            Err(e) => {
                                println!("Error closing databases: {:?}", e);
                            }
                        }
                        
                        // Save command history
                        if let Err(e) = rl.save_history(&history_path) {
                            eprintln!("Error saving history: {}", e);
                        }
                        
                        println!("Exiting NebulaDB. Goodbye!");
                        break;
                    },
                    _ => {
                        println!("Unknown command: {}. Type 'help' for a list of commands", command);
                    }
                }
            },
            Err(ReadlineError::Interrupted) => {
                println!("Interrupted");
                continue;
            },
            Err(ReadlineError::Eof) => {
                println!("EOF");
                break;
            },
        }
    }
    
    Ok(())
}
