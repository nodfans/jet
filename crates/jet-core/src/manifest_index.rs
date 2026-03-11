use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::time::UNIX_EPOCH;

use serde::{Deserialize, Serialize};

use crate::commit_store::CommitFileEntry;
use crate::error::Result;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ManifestIndex {
    pub schema_version: u32,
    pub entries: HashMap<String, ManifestEntry>,
}

impl Default for ManifestIndex {
    fn default() -> Self {
        Self {
            schema_version: 1,
            entries: HashMap::new(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ManifestEntry {
    pub path: String,
    pub size: u64,
    pub modified_unix_secs: u64,
    pub modified_nanos: u32,
    pub file: CommitFileEntry,
}

impl ManifestIndex {
    pub fn load(repo_root: impl AsRef<Path>) -> Result<Self> {
        let repo_root = repo_root.as_ref();
        let bin_path = manifest_bin_path(repo_root);
        if bin_path.exists() {
            let data = fs::read(bin_path)?;
            return Ok(bincode::deserialize(&data)?);
        }
        let path = manifest_legacy_json_path(repo_root);
        if !path.exists() {
            return Ok(Self::default());
        }

        let data = fs::read(path)?;
        let index: Self = serde_json::from_slice(&data)?;
        index.save(repo_root)?;
        Ok(index)
    }

    pub fn save(&self, repo_root: impl AsRef<Path>) -> Result<()> {
        let repo_root = repo_root.as_ref();
        let path = manifest_bin_path(repo_root);
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }
        let tmp = path.with_extension("bin.tmp");
        let data = bincode::serialize(self)?;
        fs::write(&tmp, data)?;
        fs::rename(tmp, path)?;
        Ok(())
    }

    pub fn get_if_unchanged(
        &self,
        path: &str,
        size: u64,
        modified_unix_secs: u64,
        modified_nanos: u32,
    ) -> Option<CommitFileEntry> {
        self.entries.get(path).and_then(|entry| {
            if entry.size == size
                && entry.modified_unix_secs == modified_unix_secs
                && entry.modified_nanos == modified_nanos
            {
                Some(entry.file.clone())
            } else {
                None
            }
        })
    }

    pub fn upsert(
        &mut self,
        path: String,
        size: u64,
        modified_unix_secs: u64,
        modified_nanos: u32,
        file: CommitFileEntry,
    ) {
        self.entries.insert(
            path.clone(),
            ManifestEntry {
                path,
                size,
                modified_unix_secs,
                modified_nanos,
                file,
            },
        );
    }
}

pub fn modified_key(metadata: &fs::Metadata) -> Result<(u64, u32)> {
    let modified = metadata
        .modified()?
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default();
    Ok((modified.as_secs(), modified.subsec_nanos()))
}

fn manifest_legacy_json_path(repo_root: impl AsRef<Path>) -> PathBuf {
    repo_root
        .as_ref()
        .join(".jet")
        .join("index")
        .join("manifest.json")
}

fn manifest_bin_path(repo_root: impl AsRef<Path>) -> PathBuf {
    repo_root
        .as_ref()
        .join(".jet")
        .join("index")
        .join("manifest.bin")
}
