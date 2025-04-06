//! REPL (Read-Eval-Print-Loop) for the NebulaDB CLI
//!
//! This module provides an interactive command-line interface to NebulaDB.

use std::io;
use rustyline::error::ReadlineError;
use rustyline::Editor;

use nebuladb_core::Result;

use crate::commands::{self, CommandContext, CommandResult};

/// Run the REPL
pub fn run_repl() -> Result<()> {
    println!("NebulaDB REPL - Type 'help' for a list of commands");
    
    let mut rl = Editor::<()>::new()?;
    let mut ctx = CommandContext::new();
    
    loop {
        let readline = rl.readline("nebuladb> ");
        match readline {
            Ok(line) => {
                rl.add_history_entry(line.as_str());
                
                let trimmed = line.trim();
                if trimmed.is_empty() {
                    continue;
                }
                
                if trimmed == "exit" || trimmed == "quit" {
                    println!("Goodbye!");
                    break;
                }
                
                match commands::parse_and_execute(trimmed, &mut ctx) {
                    Ok(CommandResult::Success(msg)) => {
                        println!("{}", msg);
                    }
                    Ok(CommandResult::Empty) => {}
                    Err(e) => {
                        eprintln!("Error: {:?}", e);
                    }
                }
            }
            Err(ReadlineError::Interrupted) => {
                println!("CTRL-C");
                break;
            }
            Err(ReadlineError::Eof) => {
                println!("CTRL-D");
                break;
            }
            Err(err) => {
                eprintln!("Error: {:?}", err);
                break;
            }
        }
    }
    
    Ok(())
}
