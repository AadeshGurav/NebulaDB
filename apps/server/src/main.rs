use clap::{App, Arg, SubCommand};
use nebuladb_cli::start as start_cli;
use nebuladb_core::{Result, Config};
use std::path::Path;

fn main() -> Result<()> {
    let matches = App::new("NebulaDB")
        .version("0.1.0")
        .author("NebulaDB Team")
        .about("A document-oriented database inspired by MongoDB and CouchDB")
        .subcommand(
            SubCommand::with_name("cli")
                .about("Start the NebulaDB CLI interface")
        )
        .subcommand(
            SubCommand::with_name("server")
                .about("Start the NebulaDB server")
                .arg(
                    Arg::with_name("data-dir")
                        .short("d")
                        .long("data-dir")
                        .value_name("DIR")
                        .help("Sets the data directory")
                        .takes_value(true)
                )
                .arg(
                    Arg::with_name("port")
                        .short("p")
                        .long("port")
                        .value_name("PORT")
                        .help("Sets the server port")
                        .takes_value(true)
                )
        )
        .get_matches();

    match matches.subcommand() {
        ("cli", _) => {
            println!("Starting NebulaDB CLI...");
            start_cli()?;
        },
        ("server", Some(server_matches)) => {
            let data_dir = server_matches.value_of("data-dir").unwrap_or("/tmp/nebuladb");
            let port = server_matches.value_of("port").unwrap_or("8000");
            
            println!("Starting NebulaDB server on port {}...", port);
            println!("Data directory: {}", data_dir);
            
            // In a real implementation, we would start the server here
            // For now, we'll just exit
            println!("Server mode is not yet implemented. Use 'cli' mode instead.");
        },
        _ => {
            // Default to CLI mode if no subcommand is specified
            println!("Starting NebulaDB CLI...");
            start_cli()?;
        },
    }

    Ok(())
}
