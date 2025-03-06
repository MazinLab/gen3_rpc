use clap::{Parser, Subcommand};
use std::net::Ipv4Addr;

pub mod mockclient;
pub mod mockserver;

use mockclient::mockclient;
use mockserver::mockserver;

#[derive(Parser)]
#[command(version, about, long_about = None)]
struct Cli {
    /// Mock the client or server
    #[command(subcommand)]
    command: Mode,
}

#[derive(Subcommand)]
enum Mode {
    Client {
        #[arg(value_parser=clap::value_parser!(Ipv4Addr), default_value_t=Ipv4Addr::new(127, 0, 0, 1))]
        address: std::net::Ipv4Addr,
        #[arg(default_value_t = 4242)]
        port: u16,
    },
    Server {
        #[arg(default_value_t = 4242)]
        port: u16,
    },
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cli = Cli::parse();
    match cli.command {
        Mode::Client { address, port } => mockclient(address, port).await,
        Mode::Server { port } => mockserver(port).await,
    }
}
