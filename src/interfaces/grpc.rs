use nebuladb_core::Result;
use crate::interfaces::{InterfaceManager, InterfaceManagerRef};
use std::sync::{Arc, Mutex};

#[derive(Clone)]
/// gRPC interface for accessing the database
pub struct GrpcInterface {
    /// Reference to the interface manager
    manager: InterfaceManagerRef,
    /// Port to listen on
    port: u16,
}

impl GrpcInterface {
    /// Create a new gRPC interface
    pub fn new(manager: &InterfaceManager, port: u16) -> Result<Self> {
        // Create a shared reference to the manager
        let manager_ref = Arc::new(Mutex::new(manager.clone()));
        
        Ok(Self {
            manager: manager_ref,
            port,
        })
    }
    
    /// Start the gRPC server
    pub fn start(&self) -> Result<()> {
        println!("gRPC interface would start on port {}", self.port);
        println!("(gRPC interface not yet implemented)");
        
        Ok(())
    }
} 