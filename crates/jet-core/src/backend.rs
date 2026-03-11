use std::fs;
use std::path::{Path, PathBuf};

use crate::JetRepository;
use crate::commit_store::{Commit, CommitStore, FsCommitStore};
use crate::error::{JetError, Result};
use crate::repo::{RepoConfig, init_repo, load_repo_config};
use crate::workspace::load_workspace_local_config;

pub trait RepoBackend {
    fn repo_root(&self) -> &Path;
    fn repo_config(&self) -> &RepoConfig;
    fn head_commit_id(&self) -> Result<Option<String>>;
    fn read_commit(&self, id: &str) -> Result<Commit>;
    fn clone_to_workspace(&self, destination: impl AsRef<Path>) -> Result<()>;
}

#[derive(Debug, Clone)]
pub struct LocalRepoBackend {
    root: PathBuf,
    config: RepoConfig,
    commit_store: FsCommitStore,
}

impl LocalRepoBackend {
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        let root = path.as_ref().to_path_buf();
        let config = load_repo_config(&root)?;
        let commit_store = FsCommitStore::new(&root)?;
        Ok(Self {
            root,
            config,
            commit_store,
        })
    }
}

impl RepoBackend for LocalRepoBackend {
    fn repo_root(&self) -> &Path {
        &self.root
    }

    fn repo_config(&self) -> &RepoConfig {
        &self.config
    }

    fn head_commit_id(&self) -> Result<Option<String>> {
        self.commit_store.read_head()
    }

    fn read_commit(&self, id: &str) -> Result<Commit> {
        self.commit_store.read_commit(id)
    }

    fn clone_to_workspace(&self, destination: impl AsRef<Path>) -> Result<()> {
        let destination = destination.as_ref();
        ensure_clone_destination_ready(destination)?;
        init_repo(destination)?;

        let source_jet = self.root.join(".jet");
        let destination_jet = destination.join(".jet");

        copy_file_if_exists(
            &source_jet.join("config.json"),
            &destination_jet.join("config.json"),
        )?;
        copy_dir_recursive(
            &source_jet.join("commits"),
            &destination_jet.join("commits"),
        )?;
        copy_dir_recursive(
            &source_jet.join("segments"),
            &destination_jet.join("segments"),
        )?;
        copy_dir_recursive(
            &source_jet.join("objects"),
            &destination_jet.join("objects"),
        )?;
        copy_file_if_exists(
            &source_jet.join("refs").join("HEAD"),
            &destination_jet.join("refs").join("HEAD"),
        )?;

        for file_name in [
            "segments.idx",
            "segments.jsonl",
            "manifest.bin",
            "manifest.json",
            "chunk-cache.bin",
        ] {
            copy_file_if_exists(
                &source_jet.join("index").join(file_name),
                &destination_jet.join("index").join(file_name),
            )?;
        }

        let source_view = load_workspace_local_config(&self.root)?;
        if !source_view.view.include.is_empty() || !source_view.view.exclude.is_empty() {
            fs::write(
                destination_jet.join("workspace.local.toml"),
                fs::read_to_string(source_jet.join("workspace.local.toml"))?,
            )?;
        }

        if let Some(head) = self.head_commit_id()? {
            let repo = JetRepository::open(destination)?;
            repo.set_workspace_remote_source(None)?;
            repo.open_workspace(&head, true)?;
        }

        Ok(())
    }
}

pub fn clone_from_path(source: impl AsRef<Path>, destination: impl AsRef<Path>) -> Result<()> {
    let backend = LocalRepoBackend::open(source)?;
    backend.clone_to_workspace(destination)
}

fn ensure_clone_destination_ready(destination: &Path) -> Result<()> {
    if !destination.exists() {
        return Ok(());
    }

    let mut entries = fs::read_dir(destination)?;
    if entries.next().is_some() {
        return Err(JetError::CloneDestinationNotEmpty {
            path: destination.to_path_buf(),
        });
    }

    Ok(())
}

fn copy_dir_recursive(source: &Path, destination: &Path) -> Result<()> {
    if !source.exists() {
        return Ok(());
    }

    fs::create_dir_all(destination)?;
    for entry in fs::read_dir(source)? {
        let entry = entry?;
        let source_path = entry.path();
        let destination_path = destination.join(entry.file_name());
        let file_type = entry.file_type()?;
        if file_type.is_dir() {
            copy_dir_recursive(&source_path, &destination_path)?;
        } else if file_type.is_file() {
            if let Some(parent) = destination_path.parent() {
                fs::create_dir_all(parent)?;
            }
            fs::copy(source_path, destination_path)?;
        }
    }
    Ok(())
}

fn copy_file_if_exists(source: &Path, destination: &Path) -> Result<()> {
    if !source.exists() {
        return Ok(());
    }

    if let Some(parent) = destination.parent() {
        fs::create_dir_all(parent)?;
    }
    fs::copy(source, destination)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::fs;

    use tempfile::tempdir;

    use crate::JetRepository;
    use crate::backend::{LocalRepoBackend, RepoBackend};
    use crate::repo::init_repo;

    #[test]
    fn local_backend_clone_copies_repo_and_opens_head() {
        let source = tempdir().expect("source");
        init_repo(source.path()).expect("init source");
        fs::create_dir_all(source.path().join("code")).expect("mkdir");
        fs::write(source.path().join("code").join("main.rs"), "fn main() {}\n").expect("write");

        let source_repo = JetRepository::open(source.path()).expect("open source");
        source_repo
            .add_paths(&[source.path().join("code")])
            .expect("add");
        let head = source_repo.commit("initial", "tester").expect("commit");

        let destination = tempdir().expect("destination");
        let clone_root = destination.path().join("clone");

        let backend = LocalRepoBackend::open(source.path()).expect("open backend");
        backend.clone_to_workspace(&clone_root).expect("clone");

        let cloned_repo = JetRepository::open(&clone_root).expect("open clone");
        assert_eq!(
            cloned_repo.head_commit_id().expect("head"),
            Some(head.clone())
        );

        let status = cloned_repo.workspace_status().expect("status");
        assert_eq!(status.current_commit_id, Some(head));
        assert!(clone_root.join("code").join("main.rs").exists());
    }
}
