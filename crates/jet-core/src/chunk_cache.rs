use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use crate::commit_store::CommitChunkRef;
use crate::error::Result;

const CHUNK_CACHE_SCHEMA_VERSION: u32 = 1;

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ChunkCache {
    pub schema_version: u32,
    pub by_file_digest: HashMap<String, Vec<CommitChunkRef>>,
}

impl ChunkCache {
    pub fn new() -> Self {
        Self {
            schema_version: CHUNK_CACHE_SCHEMA_VERSION,
            by_file_digest: HashMap::new(),
        }
    }

    pub fn load(repo_root: impl AsRef<Path>) -> Result<Self> {
        let path = cache_path(repo_root);
        if !path.exists() {
            return Ok(Self::new());
        }

        let data = fs::read(path)?;
        Ok(bincode::deserialize(&data)?)
    }

    pub fn save(&self, repo_root: impl AsRef<Path>) -> Result<()> {
        let path = cache_path(repo_root);
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }
        let tmp = path.with_extension("bin.tmp");
        let data = bincode::serialize(self)?;
        fs::write(&tmp, data)?;
        fs::rename(tmp, path)?;
        Ok(())
    }

    pub fn get(&self, file_digest: &str) -> Option<&Vec<CommitChunkRef>> {
        self.by_file_digest.get(file_digest)
    }

    pub fn insert(&mut self, file_digest: String, refs: Vec<CommitChunkRef>) {
        self.by_file_digest.insert(file_digest, refs);
    }
}

fn cache_path(repo_root: impl AsRef<Path>) -> PathBuf {
    repo_root
        .as_ref()
        .join(".jet")
        .join("index")
        .join("chunk-cache.bin")
}
