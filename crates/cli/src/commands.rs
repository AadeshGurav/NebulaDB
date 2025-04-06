//! Command handlers for the NebulaDB CLI
//!
//! This module provides handlers for various CLI commands.

use crate::Cli;
use nebuladb_core::Result;
use serde_json::{Value, json};
use std::str;

/// Handle the help command
pub fn handle_help() -> Result<String> {
    let help_text = r#"
NebulaDB CLI Commands:
  help                       Show this help message
  exit, quit                 Exit the CLI
  
Collections:
  create <collection>        Create a new collection
  list                       List all collections
  use <collection>           Set the current collection
  
Documents:
  insert <json>              Insert a document into the current collection
  get <id>                   Get a document by ID
  delete <id>                Delete a document by ID
  update <id> <json>         Update a document by ID
  
Transactions:
  begin                      Begin a transaction
  commit <tx_id>             Commit a transaction
  abort <tx_id>              Abort a transaction
  
System:
  status                     Show system status
  checkpoint                 Force a checkpoint
"#;
    
    Ok(help_text.to_string())
}

/// Handle the create collection command
pub fn handle_create_collection(cli: &mut Cli, collection_name: &str) -> Result<String> {
    let db = cli.db_mut();
    
    if db.collection_exists(collection_name) {
        return Ok(format!("Collection '{}' already exists", collection_name));
    }
    
    db.get_or_create_collection(collection_name)?;
    
    Ok(format!("Collection '{}' created", collection_name))
}

/// Handle the list collections command
pub fn handle_list_collections(cli: &Cli) -> Result<String> {
    let db = cli.db();
    let collections = db.list_collections();
    
    if collections.is_empty() {
        return Ok("No collections found".to_string());
    }
    
    let mut result = String::from("Collections:\n");
    for (i, name) in collections.iter().enumerate() {
        result.push_str(&format!("  {}. {}\n", i + 1, name));
    }
    
    Ok(result)
}

/// State for the current context
pub struct Context {
    /// Current collection
    pub current_collection: Option<String>,
    /// Current transaction ID
    pub current_tx_id: Option<u64>,
}

impl Default for Context {
    fn default() -> Self {
        Self {
            current_collection: None,
            current_tx_id: None,
        }
    }
}

/// Handle the use collection command
pub fn handle_use_collection(ctx: &mut Context, collection_name: &str) -> Result<String> {
    ctx.current_collection = Some(collection_name.to_string());
    
    Ok(format!("Using collection '{}'", collection_name))
}

/// Handle the insert document command
pub fn handle_insert_document(cli: &mut Cli, ctx: &Context, json_str: &str) -> Result<String> {
    let collection_name = match &ctx.current_collection {
        Some(name) => name,
        None => return Ok("No collection selected. Use 'use <collection>' first.".to_string()),
    };
    
    let json: Value = serde_json::from_str(json_str)
        .map_err(|e| nebuladb_core::Error::Other(format!("Invalid JSON: {}", e)))?;
    
    // Extract _id or generate one
    let doc_id = match json.get("_id") {
        Some(id) => id.to_string().into_bytes(),
        None => {
            // Generate a simple ID (in a real implementation, use UUID)
            let id = format!("doc_{}", std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis());
            id.into_bytes()
        },
    };
    
    let doc_data = json.to_string().into_bytes();
    
    let db = cli.db_mut();
    let collection = db.get_or_create_collection(collection_name)?;
    
    // Check if we're in a transaction
    if let Some(tx_id) = ctx.current_tx_id {
        collection.insert_in_transaction(tx_id, &doc_id, &doc_data)?;
        Ok(format!("Document inserted in transaction {}", tx_id))
    } else {
        collection.insert_document(&doc_id, &doc_data)?;
        Ok("Document inserted".to_string())
    }
}

/// Handle the get document command
pub fn handle_get_document(cli: &mut Cli, ctx: &Context, id_str: &str) -> Result<String> {
    let collection_name = match &ctx.current_collection {
        Some(name) => name,
        None => return Ok("No collection selected. Use 'use <collection>' first.".to_string()),
    };
    
    let doc_id = id_str.as_bytes();
    
    let db = cli.db_mut();
    if !db.collection_exists(collection_name) {
        return Ok(format!("Collection '{}' does not exist", collection_name));
    }
    
    let collection = db.get_or_create_collection(collection_name)?;
    
    match collection.read_document(doc_id) {
        Ok(doc_data) => {
            // Parse the JSON data
            match str::from_utf8(&doc_data) {
                Ok(json_str) => match serde_json::from_str::<Value>(json_str) {
                    Ok(json) => Ok(serde_json::to_string_pretty(&json).unwrap_or_else(|_| json_str.to_string())),
                    Err(_) => Ok(format!("Raw data: {:?}", doc_data)),
                },
                Err(_) => Ok(format!("Raw data: {:?}", doc_data)),
            }
        },
        Err(_) => Ok(format!("Document with ID '{}' not found", id_str)),
    }
}

/// Handle the delete document command
pub fn handle_delete_document(cli: &mut Cli, ctx: &Context, id_str: &str) -> Result<String> {
    let collection_name = match &ctx.current_collection {
        Some(name) => name,
        None => return Ok("No collection selected. Use 'use <collection>' first.".to_string()),
    };
    
    let doc_id = id_str.as_bytes();
    
    let db = cli.db_mut();
    if !db.collection_exists(collection_name) {
        return Ok(format!("Collection '{}' does not exist", collection_name));
    }
    
    let collection = db.get_or_create_collection(collection_name)?;
    
    // Check if we're in a transaction
    if let Some(tx_id) = ctx.current_tx_id {
        collection.delete_in_transaction(tx_id, doc_id)?;
        Ok(format!("Document deleted in transaction {}", tx_id))
    } else {
        collection.delete_document(doc_id)?;
        Ok("Document deleted".to_string())
    }
}

/// Handle the update document command
pub fn handle_update_document(cli: &mut Cli, ctx: &Context, id_str: &str, json_str: &str) -> Result<String> {
    let collection_name = match &ctx.current_collection {
        Some(name) => name,
        None => return Ok("No collection selected. Use 'use <collection>' first.".to_string()),
    };
    
    let doc_id = id_str.as_bytes();
    
    let json: Value = serde_json::from_str(json_str)
        .map_err(|e| nebuladb_core::Error::Other(format!("Invalid JSON: {}", e)))?;
    
    let doc_data = json.to_string().into_bytes();
    
    let db = cli.db_mut();
    if !db.collection_exists(collection_name) {
        return Ok(format!("Collection '{}' does not exist", collection_name));
    }
    
    let collection = db.get_or_create_collection(collection_name)?;
    
    // Check if we're in a transaction
    if let Some(tx_id) = ctx.current_tx_id {
        collection.update_in_transaction(tx_id, doc_id, &doc_data)?;
        Ok(format!("Document updated in transaction {}", tx_id))
    } else {
        collection.update_document(doc_id, &doc_data)?;
        Ok("Document updated".to_string())
    }
}

/// Handle the begin transaction command
pub fn handle_begin_transaction(cli: &mut Cli, ctx: &mut Context) -> Result<String> {
    let collection_name = match &ctx.current_collection {
        Some(name) => name,
        None => return Ok("No collection selected. Use 'use <collection>' first.".to_string()),
    };
    
    let db = cli.db_mut();
    let collection = db.get_or_create_collection(collection_name)?;
    
    let tx_id = collection.begin_transaction()?;
    ctx.current_tx_id = Some(tx_id);
    
    Ok(format!("Transaction {} started", tx_id))
}

/// Handle the commit transaction command
pub fn handle_commit_transaction(cli: &mut Cli, ctx: &mut Context, tx_id_str: &str) -> Result<String> {
    let tx_id = tx_id_str.parse::<u64>()
        .map_err(|_| nebuladb_core::Error::Other("Invalid transaction ID".to_string()))?;
    
    // If the specified tx_id matches the current one, clear it
    if ctx.current_tx_id == Some(tx_id) {
        ctx.current_tx_id = None;
    }
    
    let collection_name = match &ctx.current_collection {
        Some(name) => name,
        None => return Ok("No collection selected. Use 'use <collection>' first.".to_string()),
    };
    
    let db = cli.db_mut();
    let collection = db.get_or_create_collection(collection_name)?;
    
    collection.commit_transaction(tx_id)?;
    
    Ok(format!("Transaction {} committed", tx_id))
}

/// Handle the abort transaction command
pub fn handle_abort_transaction(cli: &mut Cli, ctx: &mut Context, tx_id_str: &str) -> Result<String> {
    let tx_id = tx_id_str.parse::<u64>()
        .map_err(|_| nebuladb_core::Error::Other("Invalid transaction ID".to_string()))?;
    
    // If the specified tx_id matches the current one, clear it
    if ctx.current_tx_id == Some(tx_id) {
        ctx.current_tx_id = None;
    }
    
    let collection_name = match &ctx.current_collection {
        Some(name) => name,
        None => return Ok("No collection selected. Use 'use <collection>' first.".to_string()),
    };
    
    let db = cli.db_mut();
    let collection = db.get_or_create_collection(collection_name)?;
    
    collection.abort_transaction(tx_id)?;
    
    Ok(format!("Transaction {} aborted", tx_id))
}

/// Get database status
pub fn handle_status(cli: &Cli, context: &Context) -> Result<String> {
    // Get status from database
    let status = json!({
        "database_path": cli.config.data_dir,
        "current_collection": context.current_collection,
        "current_transaction": context.current_tx_id,
        "uptime_seconds": cli.store.as_ref().map(|s| s.uptime_secs()).unwrap_or(0),
        "collections": cli.store.as_ref().map(|s| s.collection_count()).unwrap_or(0),
        "memory_usage_mb": 0, // TBD
    });
    
    Ok(status.to_string())
}

/// Handle the checkpoint command
pub fn handle_checkpoint(cli: &mut Cli, ctx: &Context) -> Result<String> {
    let collection_name = match &ctx.current_collection {
        Some(name) => name,
        None => return Ok("No collection selected. Use 'use <collection>' first.".to_string()),
    };
    
    let db = cli.db_mut();
    if !db.collection_exists(collection_name) {
        return Ok(format!("Collection '{}' does not exist", collection_name));
    }
    
    let collection = db.get_or_create_collection(collection_name)?;
    collection.flush()?;
    
    Ok(format!("Checkpoint created for collection '{}'", collection_name))
}
