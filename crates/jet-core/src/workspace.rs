use std::collections::BTreeMap;
use std::fs;
use std::path::Path;

use serde::{Deserialize, Serialize};

use crate::commit_store::CommitFileEntry;
use crate::error::Result;
use crate::repo::RepoConfig;

const WORKSPACE_SCHEMA_VERSION: u32 = 1;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkspaceState {
    pub schema_version: u32,
    pub repo_id: String,
    pub current_commit_id: Option<String>,
    pub remote_source: Option<String>,
}

impl WorkspaceState {
    pub fn new(config: &RepoConfig) -> Self {
        Self {
            schema_version: WORKSPACE_SCHEMA_VERSION,
            repo_id: config.repo_id.clone(),
            current_commit_id: None,
            remote_source: None,
        }
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct WorkspaceLocalConfig {
    #[serde(default)]
    pub view: WorkspaceViewConfig,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct WorkspaceViewConfig {
    #[serde(default)]
    pub include: Vec<String>,
    #[serde(default)]
    pub exclude: Vec<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum MaterializedState {
    NotInView,
    Virtual,
    Hydrated,
    Dirty,
    Pending,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MaterializedEntry {
    pub state: MaterializedState,
    pub commit_id: String,
    pub file_digest: String,
    pub size: u64,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct MaterializedIndex {
    pub schema_version: u32,
    pub files: BTreeMap<String, MaterializedEntry>,
}

impl MaterializedIndex {
    pub fn new() -> Self {
        Self {
            schema_version: WORKSPACE_SCHEMA_VERSION,
            files: BTreeMap::new(),
        }
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct WorkspaceManifest {
    pub schema_version: u32,
    pub commit_id: Option<String>,
    pub files: Vec<CommitFileEntry>,
}

impl WorkspaceManifest {
    pub fn new() -> Self {
        Self {
            schema_version: WORKSPACE_SCHEMA_VERSION,
            commit_id: None,
            files: Vec::new(),
        }
    }
}

pub fn load_workspace_state(
    repo_root: impl AsRef<Path>,
    config: &RepoConfig,
) -> Result<WorkspaceState> {
    let path = repo_root.as_ref().join(".jet").join("workspace.bin");
    if !path.exists() {
        let state = WorkspaceState::new(config);
        save_workspace_state(repo_root, &state)?;
        return Ok(state);
    }

    let data = fs::read(path)?;
    Ok(bincode::deserialize(&data)?)
}

pub fn save_workspace_state(repo_root: impl AsRef<Path>, state: &WorkspaceState) -> Result<()> {
    let path = repo_root.as_ref().join(".jet").join("workspace.bin");
    let tmp = path.with_extension("bin.tmp");
    let data = bincode::serialize(state)?;
    fs::write(&tmp, data)?;
    fs::rename(tmp, path)?;
    Ok(())
}

pub fn load_materialized_index(repo_root: impl AsRef<Path>) -> Result<MaterializedIndex> {
    let path = repo_root
        .as_ref()
        .join(".jet")
        .join("materialized-index.bin");
    if !path.exists() {
        let index = MaterializedIndex::new();
        save_materialized_index(repo_root, &index)?;
        return Ok(index);
    }

    let data = fs::read(path)?;
    Ok(bincode::deserialize(&data)?)
}

pub fn save_materialized_index(
    repo_root: impl AsRef<Path>,
    index: &MaterializedIndex,
) -> Result<()> {
    let path = repo_root
        .as_ref()
        .join(".jet")
        .join("materialized-index.bin");
    let tmp = path.with_extension("bin.tmp");
    let data = bincode::serialize(index)?;
    fs::write(&tmp, data)?;
    fs::rename(tmp, path)?;
    Ok(())
}

pub fn load_workspace_local_config(repo_root: impl AsRef<Path>) -> Result<WorkspaceLocalConfig> {
    let path = repo_root.as_ref().join(".jet").join("workspace.local.toml");
    if !path.exists() {
        return Ok(WorkspaceLocalConfig::default());
    }

    let data = fs::read_to_string(path)?;
    Ok(toml::from_str(&data)?)
}

pub fn load_workspace_manifest(repo_root: impl AsRef<Path>) -> Result<WorkspaceManifest> {
    let path = repo_root
        .as_ref()
        .join(".jet")
        .join("workspace-manifest.bin");
    if !path.exists() {
        let manifest = WorkspaceManifest::new();
        save_workspace_manifest(repo_root, &manifest)?;
        return Ok(manifest);
    }

    let data = fs::read(path)?;
    Ok(bincode::deserialize(&data)?)
}

pub fn save_workspace_manifest(
    repo_root: impl AsRef<Path>,
    manifest: &WorkspaceManifest,
) -> Result<()> {
    let path = repo_root
        .as_ref()
        .join(".jet")
        .join("workspace-manifest.bin");
    let tmp = path.with_extension("bin.tmp");
    let data = bincode::serialize(manifest)?;
    fs::write(&tmp, data)?;
    fs::rename(tmp, path)?;
    Ok(())
}

pub fn save_workspace_local_config(
    repo_root: impl AsRef<Path>,
    config: &WorkspaceLocalConfig,
) -> Result<()> {
    let path = repo_root.as_ref().join(".jet").join("workspace.local.toml");
    let data = toml::to_string_pretty(config)?;
    fs::write(path, data)?;
    Ok(())
}

pub fn default_workspace_local_config_template() -> &'static str {
    "# Jet local workspace overrides\n\
     #\n\
     # Uncomment and edit these patterns to narrow the visible working set.\n\
     #\n\
     # [view]\n\
     # include = [\"code/...\", \"config/...\", \"assets/characters/...\"]\n\
     # exclude = [\"assets/tmp/...\"]\n"
}
