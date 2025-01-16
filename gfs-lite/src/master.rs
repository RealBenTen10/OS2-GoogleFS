//! This module implements a simplified version of the Google File System (GFS)
//! master server.
//! 
//! The master server primarily focuses on managing chunk servers and handling
//! their metadata.

use std::{
	net::SocketAddr,
	sync::{Arc, RwLock}
};

use tarpc::context::Context;

use crate::{ChunkMaster, Master};

/// `GfsMaster` is a wrapper around `Inner` that provides thread-safe access.
#[derive(Clone, Default)]
pub struct GfsMaster(Arc<RwLock<Inner>>);

/// `Inner` holds the state of the `GfsMaster`.
/// It contains a list of chunk servers and a registry mapping URLs to their
/// respective chunk server addresses.
#[derive(Default)]
struct Inner {
	chunk_servers: Vec<SocketAddr>,          // List of registered chunk servers
	url_to_chunk: std::collections::HashMap<String, SocketAddr>, // URL -> Chunk mapping
}

/// Implementation of the `Master` trait for `GfsMaster`.
impl Master for GfsMaster {
	async fn lookup(self, _: Context, url: String) -> SocketAddr {
		// Get the socketaddres of the chunk server which has the data (url)
		let inner = self.0.read().unwrap();
		inner.url_to_chunk.get(&url).cloned().unwrap_or_else(|| {
			panic!("URL not found: {}", url)
		})
	}
}

/// Implementation of the `ChunkMaster` trait for `GfsMaster`.
impl ChunkMaster for GfsMaster {
	async fn register(self, _: Context, socket_addr: SocketAddr) -> u64 {
		// register new chunk server in inner array/vector
		let mut inner = self.0.write().unwrap();
		inner.chunk_servers.push(socket_addr);
		// Return chunk server ID (position in array/vector)
		inner.chunk_servers.len() as u64
	}

	async fn insert(self, _: Context, sender: u64, url: String) {
		// sender = receiver?
		// sender is chunk server with url (data)
		// we register that the sender now has the url (data)
		let mut inner = self.0.write().unwrap();
		// (sender - 1) since we start counting at 1 but indices start at 0 (len starts at 1)
		if let Some(&server) = inner.chunk_servers.get(sender as usize - 1) {
			// insert pair into hashmap - url is saved on the chunk server
			inner.url_to_chunk.insert(url, server);
		} else {
			panic!("Could not save url '{}' to chunk server at adress '{}' - check if chunk server is already registered", url, sender);
		}
	}

	async fn remove(self, _: Context, url: String) {
		// remove(d) data (url) from chunk server
		// data (url) no longer available (if we didn't replicate or all replications deleted)
		let mut inner = self.0.write().unwrap();
		inner.url_to_chunk.remove(&url);
	}
}

