use std::fs;
use std::path::Path;

use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::engine::StagingIndex;
use crate::error::{JetError, Result};
use crate::workspace::{
    MaterializedIndex, WorkspaceManifest, WorkspaceState, default_workspace_local_config_template,
    save_materialized_index, save_workspace_manifest, save_workspace_state,
};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RepoConfig {
    pub schema_version: u32,
    pub repo_id: String,
    pub chunking: ChunkingConfig,
    pub compression: CompressionConfig,
    pub storage: StorageConfig,
    #[serde(default)]
    pub workspace: WorkspaceConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChunkingConfig {
    pub min: u32,
    pub avg: u32,
    pub max: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompressionConfig {
    pub enabled: bool,
    pub level: i32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StorageConfig {
    #[serde(
        default = "default_direct_blob_threshold_bytes",
        alias = "small_file_threshold_bytes"
    )]
    pub direct_blob_threshold_bytes: u64,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct WorkspaceConfig {
    #[serde(default = "default_workspace_hot_paths")]
    pub hot_paths: Vec<String>,
    #[serde(default = "default_workspace_max_hot_file_bytes")]
    pub max_hot_file_bytes: u64,
}

fn default_direct_blob_threshold_bytes() -> u64 {
    8 * 1024 * 1024
}

fn default_workspace_hot_paths() -> Vec<String> {
    vec!["code/...".to_string(), "config/...".to_string()]
}

fn default_workspace_max_hot_file_bytes() -> u64 {
    256 * 1024
}

pub fn init_repo(path: impl AsRef<Path>) -> Result<()> {
    let repo_root = path.as_ref();
    let jet_root = repo_root.join(".jet");
    fs::create_dir_all(jet_root.join("objects"))?;
    fs::create_dir_all(jet_root.join("segments"))?;
    fs::create_dir_all(jet_root.join("index"))?;
    fs::create_dir_all(jet_root.join("commits"))?;
    fs::create_dir_all(jet_root.join("refs"))?;
    fs::create_dir_all(jet_root.join("staging"))?;

    let config = RepoConfig {
        schema_version: 1,
        repo_id: Uuid::new_v4().to_string(),
        chunking: ChunkingConfig {
            min: 1024 * 1024,
            avg: 4 * 1024 * 1024,
            max: 16 * 1024 * 1024,
        },
        compression: CompressionConfig {
            enabled: false,
            level: 1,
        },
        storage: StorageConfig {
            direct_blob_threshold_bytes: default_direct_blob_threshold_bytes(),
        },
        workspace: WorkspaceConfig {
            hot_paths: default_workspace_hot_paths(),
            max_hot_file_bytes: default_workspace_max_hot_file_bytes(),
        },
    };

    let config_path = jet_root.join("config.json");
    if !config_path.exists() {
        let data = serde_json::to_vec(&config)?;
        fs::write(config_path, data)?;
    }

    let staging_path = jet_root.join("staging").join("index.json");
    if !staging_path.exists() {
        fs::write(staging_path, b"{\"schema_version\":1,\"files\":[]}")?;
    }

    let staging_bin_path = jet_root.join("staging").join("index.bin");
    if !staging_bin_path.exists() {
        let data = bincode::serialize(&StagingIndex::default())?;
        fs::write(staging_bin_path, data)?;
    }

    if !jet_root.join("workspace.bin").exists() {
        save_workspace_state(repo_root, &WorkspaceState::new(&config))?;
    }

    if !jet_root.join("materialized-index.bin").exists() {
        save_materialized_index(repo_root, &MaterializedIndex::new())?;
    }

    if !jet_root.join("workspace-manifest.bin").exists() {
        save_workspace_manifest(repo_root, &WorkspaceManifest::new())?;
    }

    let workspace_local_path = jet_root.join("workspace.local.toml");
    if !workspace_local_path.exists() {
        fs::write(
            workspace_local_path,
            default_workspace_local_config_template().as_bytes(),
        )?;
    }

    Ok(())
}

pub fn load_repo_config(path: impl AsRef<Path>) -> Result<RepoConfig> {
    let config_path = path.as_ref().join(".jet").join("config.json");
    if !config_path.exists() {
        return Err(JetError::InvalidRepository {
            path: path.as_ref().to_path_buf(),
        });
    }

    let data = fs::read(config_path)?;
    Ok(serde_json::from_slice(&data)?)
}

pub fn save_repo_config(path: impl AsRef<Path>, config: &RepoConfig) -> Result<()> {
    let config_path = path.as_ref().join(".jet").join("config.json");
    let data = serde_json::to_vec(config)?;
    fs::write(config_path, data)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use tempfile::tempdir;

    use crate::repo::{init_repo, load_repo_config};

    #[test]
    fn creates_jet_repo_layout() {
        let dir = tempdir().expect("tempdir");
        init_repo(dir.path()).expect("init repo");

        assert!(dir.path().join(".jet/config.json").exists());
        assert!(dir.path().join(".jet/objects").exists());
        assert!(dir.path().join(".jet/segments").exists());
        assert!(dir.path().join(".jet/index").exists());
        assert!(dir.path().join(".jet/commits").exists());
        assert!(dir.path().join(".jet/refs").exists());
        assert!(dir.path().join(".jet/staging").exists());
        assert!(dir.path().join(".jet/staging/index.bin").exists());
        assert!(dir.path().join(".jet/workspace.bin").exists());
        assert!(dir.path().join(".jet/materialized-index.bin").exists());
        assert!(dir.path().join(".jet/workspace.local.toml").exists());

        let config = load_repo_config(dir.path()).expect("load config");
        assert_eq!(config.schema_version, 1);
    }
}
