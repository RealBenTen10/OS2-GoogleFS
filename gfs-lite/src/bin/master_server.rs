//! This binary crate implements a simplified version of the master server for
//! the Google File System (GFS).

use std::net::{IpAddr, Ipv6Addr};
use gfs_lite::master::GfsMaster;
use tarpc::serde_transport::tcp;
use tarpc::server::{self, Channel};
use tarpc::{
	tokio_serde::formats::Json,
};
use futures::StreamExt;

/// The `main` function sets up and runs the TCP servers for both the `Master`
/// and `ChunkMaster` services. It listens on different ports for each service
/// and spawns tasks to handle incoming connections.
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {

	// Create the GFS master server instance
	let gfs_master = GfsMaster::default();

	// Listen for client requests on port - 50000 was set by main
	let client_addr= (IpAddr::V6(Ipv6Addr::LOCALHOST), 50000);
	let client_listener = tcp::listen(&client_addr, Json::default).await.expect("Client could not be connected");
	println!("Client service running on {}", client_addr.0);

	// Listen for chunk server registrations on port 50001 - just use the next one
	let chunk_server_addr= (IpAddr::V6(Ipv6Addr::LOCALHOST), 50001);
	let chunk_listener = tcp::listen(&chunk_server_addr, Json::default).await.expect("Chunk could not be connected");
	println!("Chunk service running on {}", chunk_server_addr.0);

	// Start handling incoming connections
	let gfs_master_clone = gfs_master.clone();

	// Spawn a task to handle client requests
	tokio::spawn(async move {
		use gfs_lite::ChunkMaster;
		client_listener
			.for_each_concurrent(None, |stream| async {
				println!("Client connecting");
				match stream {
					Ok(transport) => {
						println!("Master received command from Client");
						let server = gfs_master_clone.clone();
						server::BaseChannel::with_defaults(transport)
							.execute(server.serve());
					}
					Err(e) => eprintln!("Error accepting client connection: {}", e),
				}
			})
			.await;
	});

	// Spawn a task to handle chunk server requests
	tokio::spawn(async move {
		use gfs_lite::Master;
		chunk_listener
			.for_each_concurrent(None, |stream| async {
				println!("Chunk Server connecting");
				match stream {
					Ok(transport) => {
						println!("Master received command from Chunk Server");
						let server = gfs_master.clone();
						let channel = server::BaseChannel::with_defaults(transport);
						channel.execute(server.serve());
					}
					Err(e) => eprintln!("Error accepting chunk server connection: {}", e),
				}
			})
			.await;
	});

	// Wait for shutdown signal
	tokio::signal::ctrl_c().await?;
	println!("Shutting down servers. Dobby has tried everything. I hope dobby was useful");
	Ok(())
}

