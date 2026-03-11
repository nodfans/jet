use std::fs::{self, File};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use crate::error::{JetError, Result};

pub trait CommitStore {
    fn write_commit(&self, commit: &Commit) -> Result<()>;
    fn read_commit(&self, id: &str) -> Result<Commit>;
    fn write_head(&self, id: &str) -> Result<()>;
    fn read_head(&self) -> Result<Option<String>>;
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Commit {
    pub schema_version: u32,
    pub id: String,
    pub parent: Option<String>,
    pub author: String,
    pub message: String,
    pub timestamp_unix: i64,
    #[serde(default)]
    pub files_omitted: bool,
    pub files: Vec<CommitFileEntry>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CommitFileEntry {
    pub path: String,
    pub size: u64,
    pub file_digest: String,
    pub chunks: Vec<CommitChunkRef>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CommitChunkRef {
    pub id: String,
    pub offset: u64,
    pub len: u64,
    pub raw_len: u64,
}

#[derive(Debug, Clone)]
pub struct FsCommitStore {
    commits_root: PathBuf,
    head_file: PathBuf,
}

impl FsCommitStore {
    pub fn new(repo_root: impl AsRef<Path>) -> Result<Self> {
        let jet_root = repo_root.as_ref().join(".jet");
        let commits_root = jet_root.join("commits");
        let refs_root = jet_root.join("refs");

        fs::create_dir_all(&commits_root)?;
        fs::create_dir_all(&refs_root)?;

        Ok(Self {
            commits_root,
            head_file: refs_root.join("HEAD"),
        })
    }

    fn commit_path_json(&self, id: &str) -> PathBuf {
        self.commits_root.join(format!("{id}.json"))
    }

    fn commit_path_bin(&self, id: &str) -> PathBuf {
        self.commits_root.join(format!("{id}.bin"))
    }
}

impl CommitStore for FsCommitStore {
    fn write_commit(&self, commit: &Commit) -> Result<()> {
        let final_path = self.commit_path_bin(&commit.id);
        let tmp_path = self.commits_root.join(format!("{}.bin.tmp", commit.id));

        let data = bincode::serialize(commit)?;
        {
            let mut file = File::create(&tmp_path)?;
            file.write_all(&data)?;
            file.sync_all()?;
        }

        fs::rename(tmp_path, final_path)?;
        Ok(())
    }

    fn read_commit(&self, id: &str) -> Result<Commit> {
        let bin_path = self.commit_path_bin(id);
        if bin_path.exists() {
            let mut file = File::open(bin_path)?;
            let mut buf = Vec::new();
            file.read_to_end(&mut buf)?;
            return Ok(bincode::deserialize(&buf)?);
        }

        let path = self.commit_path_json(id);
        if !path.exists() {
            return Err(JetError::ObjectNotFound(id.to_string()));
        }

        let mut file = File::open(path)?;
        let mut buf = Vec::new();
        file.read_to_end(&mut buf)?;
        Ok(serde_json::from_slice(&buf)?)
    }

    fn write_head(&self, id: &str) -> Result<()> {
        let tmp_path = self.head_file.with_extension("tmp");
        {
            let mut file = File::create(&tmp_path)?;
            file.write_all(id.as_bytes())?;
            file.write_all(b"\n")?;
            file.sync_all()?;
        }
        fs::rename(tmp_path, &self.head_file)?;
        Ok(())
    }

    fn read_head(&self) -> Result<Option<String>> {
        if !self.head_file.exists() {
            return Ok(None);
        }

        let text = fs::read_to_string(&self.head_file)?;
        let trimmed = text.trim();
        if trimmed.is_empty() {
            return Ok(None);
        }

        Ok(Some(trimmed.to_string()))
    }
}

#[cfg(test)]
mod tests {
    use tempfile::tempdir;

    use crate::commit_store::{Commit, CommitStore, FsCommitStore};

    #[test]
    fn commit_round_trip_and_head_update() {
        let dir = tempdir().expect("tempdir");
        let store = FsCommitStore::new(dir.path()).expect("store");

        let commit = Commit {
            schema_version: 1,
            id: "abc123".to_string(),
            parent: None,
            author: "tester".to_string(),
            message: "initial".to_string(),
            timestamp_unix: 0,
            files_omitted: false,
            files: vec![],
        };

        store.write_commit(&commit).expect("write commit");
        store.write_head(&commit.id).expect("write head");

        let loaded = store.read_commit("abc123").expect("read commit");
        let head = store.read_head().expect("read head");

        assert_eq!(loaded.id, commit.id);
        assert_eq!(head.as_deref(), Some("abc123"));
    }
}
