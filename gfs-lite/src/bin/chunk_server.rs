//! This module implements the `ChunkServer` for a simplified version of the
//! Google File System (GFS). It manages the storage and retrieval of chunks
//! (data blocks) in the system.

use std::{env, net::{IpAddr, Ipv6Addr}, thread};

use tarpc::{context, server::Channel, tokio_serde::formats::Json};
use tarpc::serde_transport::tcp::connect;
use tokio::sync::mpsc;
use std::net::{TcpListener, SocketAddr, TcpStream};
use std::sync::{Arc, Mutex};
use gfs_lite::chunk::{ChunkServer};



/// The `main` function sets up the `ChunkServer`, connects it to the
/// `ChunkMaster`, and starts listening for chunk operations.
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
	// parse the command-line arguments for the master servers IP-address 
	let _master_addr =
		env::args()
			.nth(1)
			.and_then(|ip| ip.parse::<IpAddr>().ok())
			.unwrap_or(IpAddr::V6(Ipv6Addr::LOCALHOST));

	println!("Connecting to ChunkMaster at {}", _master_addr);



	// connect to the master server
	let master_client = connect((_master_addr, 50001), Json::default).await?;
	let master_addr = SocketAddr::new(_master_addr, 0);
	println!("Connecting to ChunkMaster at {}", master_addr);

	let my_id = master_client
		.register(context::current(), master_addr)
		.await?;
	println!("Registered with ChunkMaster. Server ID: {}", my_id);

	// open a channel for client commands
	let listener = TcpListener::bind("0.0.0.0:6000").await?;
	println!("ChunkServer listening on port {}", listener);

	// initialize the chunk server
	let chunk_server = ChunkServer::new(master_client, my_id);

	// listen for RPC traffic from clients
	for stream in listener.incoming() {
		match stream {
			Ok(stream) => {
				thread::spawn(move || {
					stream
					// handle client
					// remember to send notice to master - dobby
				});
			}
			Err(e) => eprintln!("Connection failed: {}", e),
		}
	}
	Ok(())
}
