use nebuladb_core::Result;
use crate::interfaces::{InterfaceManager, InterfaceManagerRef};
use std::sync::{Arc, Mutex};

#[derive(Clone)]
/// HTTP interface for accessing the database via REST API
pub struct HttpInterface {
    /// Reference to the interface manager
    manager: InterfaceManagerRef,
    /// Port to listen on
    port: u16,
}

impl HttpInterface {
    /// Create a new HTTP interface
    pub fn new(manager: &InterfaceManager, port: u16) -> Result<Self> {
        // Create a shared reference to the manager
        let manager_ref = Arc::new(Mutex::new(manager.clone()));
        
        Ok(Self {
            manager: manager_ref,
            port,
        })
    }
    
    /// Start the HTTP server
    pub fn start(&self) -> Result<()> {
        println!("HTTP interface would start on port {}", self.port);
        println!("(HTTP interface not yet implemented)");
        
        Ok(())
    }
} 