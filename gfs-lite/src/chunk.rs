//! This module implements the `ChunkServer` for a simplified version of the
//! Google File System (GFS). It manages the storage and retrieval of chunks
//! (data blocks) in the system.

use std::sync::{Arc, RwLock};
use std::collections::HashMap;

use tarpc::context::Context;

use crate::{Chunk, ChunkMasterClient};

/// `ChunkServer` is responsible for handling chunk operations and interacting
/// with the `ChunkMaster`.
#[derive(Clone)]
pub struct ChunkServer(Arc<RwLock<Inner>>);

/// `Inner` holds the state of the `ChunkServer`, including a reference to the
/// `ChunkMasterClient`, a hashmap for chunk storage, and the server's own ID.

struct Inner {
	chunks: HashMap<String, String>,          // URL -> Chunk data
	my_id: u64,                               // Server's unique ID - position in array/vector
	master_client: ChunkMasterClient,         // Client to communicate with the master
}

impl ChunkServer {
	/// Creates a new ChunkServer, given the remote object for the master server
	/// and its ID.
	pub fn new(_master: ChunkMasterClient, _my_id: u64) -> Self {
		Self(Arc::new(RwLock::new(Inner {
			chunks: HashMap::new(),
			my_id: _my_id,
			master_client: _master,
		})))
	}
}

impl Chunk for ChunkServer {
	async fn get(self, _: Context, _url: String) -> Option<String> {
		let inner = self.0.read().unwrap();
		inner.chunks.get(&_url).cloned()
	}
	
	async fn set(self, _ctx: Context, _url: String, _chunk: Option<String>) -> Option<String> {
		let mut inner = self.0.write().unwrap();
		match _chunk {
			Some(data) => inner.chunks.insert(_url, data),
			None => inner.chunks.remove(&_url),
		}
	}
}
