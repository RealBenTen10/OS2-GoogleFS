//! This binary crate implements a simplified version of the master server for
//! the Google File System (GFS).

use std::env;
use std::net::{IpAddr, Ipv6Addr, SocketAddrV6};
use gfs_lite::master::GfsMaster;
use tarpc::serde_transport::tcp;
use tarpc::server::{self, Channel};
use tokio::net::TcpListener;
use tarpc::{
	client, context,
	tokio_serde::formats::Json,
	serde_transport::tcp::connect,
};

/// The `main` function sets up and runs the TCP servers for both the `Master`
/// and `ChunkMaster` services. It listens on different ports for each service
/// and spawns tasks to handle incoming connections.
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {



	// Create the GFS master server instance
	let gfs_master = GfsMaster::default();

	// Listen for client requests on port
	let client_listener = TcpListener::bind("127.0.0.1:5000").await.expect("Could not bind client");
	println!("Client service running on 127.0.0.1:5000");

	// Listen for chunk server registrations on port 5001
	let chunk_listener = TcpListener::bind("127.0.0.1:8000").await.expect("Could not bind chunk server");
	println!("Chunk service running on 127.0.0.1:8000");

	// Spawn the client service server
	let client_service = tokio::spawn(async move {
		loop {
			let (socket, _) = client_listener.accept().await.expect("Failed to accept connection");
			//let transport = tcp::Transport::from(socket);

		}
	});

	// Spawn the chunk service server
	let chunk_service = tokio::spawn(async move {
		loop {
			let (socket, _) = chunk_listener.accept().await.expect("Failed to accept connection");
			//let transport = tcp::Transport::from(socket);

		}
	});

	// Wait for shutdown signal
	tokio::signal::ctrl_c().await?;
	println!("Shutting down servers.");

	// Await both services?
	// let _ = tokio::join!(client_service, chunk_service);
	Ok(())
}

