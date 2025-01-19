//! This module implements the `ChunkServer` for a simplified version of the
//! Google File System (GFS). It manages the storage and retrieval of chunks
//! (data blocks) in the system.

use std::{env, net::{IpAddr, Ipv6Addr}};
use tarpc::{context, server::Channel, tokio_serde::formats::Json};
use tarpc::serde_transport::tcp::connect;
use tarpc::{client, server::{self}};
use tokio::time::{self, Duration};
use gfs_lite::{chunk::ChunkServer, Chunk, ChunkMasterClient};



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
	let transport = connect((_master_addr, 50001), Json::default).await.expect("Could not connect to master - dobby is sad");
	let chunk_master_client = ChunkMasterClient::new(client::Config::default(), transport).spawn();
	let listener = tarpc::serde_transport::tcp::listen((Ipv6Addr::LOCALHOST, 0), Json::default).await.expect("Could not listen on port");
	let my_addr = listener.local_addr();
	let my_id = chunk_master_client
		.register(context::current(), my_addr)
		.await
		.expect("Could not register with master");
	println!("Registered with ChunkMaster. Server ID: {} - dobby make master proud", my_id);

	// open a channel for client commands
	// let (sender, receiver) = std::sync::mpsc::channel();

	// initialize the chunk server
	let chunk_server = ChunkServer::new(chunk_master_client.clone(), my_id);

	// listen for RPC traffic from clients
	tokio::spawn(async move {
		listener
			.incoming()
			.for_each_concurrent(None, |stream| async {
				match stream {
					Ok(transport) => {
						server::BaseChannel::with_defaults(transport).execute(chunk_server.clone().serve());
					}
					Err(e) => eprintln!("Error while accepting connection: {}", e),
				}
			})
			.await;
	});
	// Communication loop with master
	let chunk_server_clone = chunk_server.clone();
	let chunk_master_client_clone = chunk_master_client.clone();

	tokio::spawn(async move {
		let mut interval = time::interval(Duration::from_secs(10));
		loop {
			interval.tick().await;

			// Send updates to the master
			let inner = chunk_server_clone.0.read().unwrap();
			for url in inner.chunks.keys() {
				if let Err(e) = chunk_master_client_clone
					.insert(context::current(), inner.my_id, url.clone())
					.await
				{
					eprintln!("Failed to update master for chunk {}: {}", url, e);
				} else {
					println!("Updated master with chunk {}", url);
				}
			}
		}
	});

	// Wait for shutdown signal
	tokio::signal::ctrl_c().await?;
	println!("Shutting down chunk server.");
	Ok(())
}
