use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use jet_core::commit_store::{Commit, CommitStore, FsCommitStore};
use jet_core::object_store::{FsObjectStore, ObjectStore};
use jet_core::repo::{RepoConfig, load_repo_config};
use jet_core::{JetError, Result};
use jet_proto::proto;
use jet_proto::proto::repository_service_server::RepositoryService;
use tokio_stream::wrappers::ReceiverStream;

const AUTHORIZATION_METADATA_KEY: &str = "authorization";

#[derive(Debug, Clone, Default)]
pub struct AuthConfig {
    identities_by_token: BTreeMap<String, String>,
    permissions_by_repo: BTreeMap<String, RepoPermissions>,
}

#[derive(Debug, Clone, Default)]
pub struct RepoPermissions {
    pub read: BTreeSet<String>,
    pub write: BTreeSet<String>,
    pub admin: BTreeSet<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RepoAccess {
    Read,
    Write,
}

impl AuthConfig {
    pub fn new(entries: impl IntoIterator<Item = (String, String)>) -> Self {
        let identities_by_token = entries.into_iter().map(|(identity, token)| (token, identity)).collect();
        Self {
            identities_by_token,
            permissions_by_repo: BTreeMap::new(),
        }
    }

    pub fn with_repo_permissions(
        entries: impl IntoIterator<Item = (String, String)>,
        repo_permissions: impl IntoIterator<Item = (String, RepoPermissions)>,
    ) -> Self {
        Self {
            identities_by_token: entries.into_iter().map(|(identity, token)| (token, identity)).collect(),
            permissions_by_repo: repo_permissions.into_iter().collect(),
        }
    }

    fn resolve_identity(&self, token: &str) -> Option<&str> {
        self.identities_by_token.get(token).map(String::as_str)
    }

    fn is_enabled(&self) -> bool {
        !self.identities_by_token.is_empty()
    }

    fn allows(&self, repo: &str, identity: &str, access: RepoAccess) -> bool {
        if self.permissions_by_repo.is_empty() {
            return true;
        }
        let Some(permissions) = self.permissions_by_repo.get(repo) else {
            return false;
        };
        permissions.allows(identity, access)
    }
}

impl RepoPermissions {
    fn allows(&self, identity: &str, access: RepoAccess) -> bool {
        if self.admin.contains(identity) {
            return true;
        }
        match access {
            RepoAccess::Read => self.read.contains(identity) || self.write.contains(identity),
            RepoAccess::Write => self.write.contains(identity),
        }
    }
}

#[derive(Debug, Clone)]
pub struct JetServer {
    repos_root: PathBuf,
    locks: Arc<Mutex<BTreeMap<String, BTreeMap<String, String>>>>,
    auth: AuthConfig,
}

impl JetServer {
    pub fn new(repos_root: impl AsRef<Path>) -> Result<Self> {
        Self::with_auth(repos_root, AuthConfig::default())
    }

    pub fn with_auth(repos_root: impl AsRef<Path>, auth: AuthConfig) -> Result<Self> {
        let repos_root = repos_root.as_ref().to_path_buf();
        fs::create_dir_all(&repos_root)?;
        let repos_root = fs::canonicalize(repos_root)?;
        Ok(Self {
            repos_root,
            locks: Arc::new(Mutex::new(BTreeMap::new())),
            auth,
        })
    }

    fn repo_root(&self, repo: &str) -> Result<PathBuf> {
        let trimmed = repo.trim_matches('/');
        if trimmed.is_empty() || trimmed.contains("..") {
            return Err(JetError::RemoteTransport {
                message: format!("invalid repo path: {repo}"),
            });
        }
        if self.is_direct_repo_match(trimmed) {
            return Ok(self.repos_root.clone());
        }
        let root = self.repos_root.join(trimmed);
        if !root.join(".jet").exists() {
            return Err(JetError::InvalidRepository { path: root });
        }
        Ok(root)
    }

    fn is_direct_repo_match(&self, repo: &str) -> bool {
        if !self.repos_root.join(".jet").exists() {
            return false;
        }
        self.repos_root
            .file_name()
            .and_then(|name| name.to_str())
            .is_some_and(|name| name == repo)
    }

    fn object_store(&self, repo_root: &Path) -> Result<FsObjectStore> {
        let config = load_repo_config(repo_root)?;
        FsObjectStore::new(
            repo_root,
            config.compression.enabled,
            config.compression.level,
        )
    }

    fn locks_path(repo_root: &Path) -> PathBuf {
        repo_root.join(".jet").join("locks.json")
    }

    fn load_repo_locks(&self, repo: &str, repo_root: &Path) -> Result<BTreeMap<String, String>> {
        {
            let state = self.locks.lock().expect("lock");
            if let Some(existing) = state.get(repo) {
                return Ok(existing.clone());
            }
        }

        let path = Self::locks_path(repo_root);
        let locks = if path.exists() {
            let data = fs::read(&path)?;
            serde_json::from_slice(&data)?
        } else {
            BTreeMap::new()
        };
        self.locks
            .lock()
            .expect("lock")
            .insert(repo.to_string(), locks.clone());
        Ok(locks)
    }

    fn save_repo_locks(
        &self,
        repo: &str,
        repo_root: &Path,
        locks: &BTreeMap<String, String>,
    ) -> Result<()> {
        let path = Self::locks_path(repo_root);
        let data = serde_json::to_vec(locks)?;
        fs::write(path, data)?;
        self.locks
            .lock()
            .expect("lock")
            .insert(repo.to_string(), locks.clone());
        Ok(())
    }

    fn authorize<T>(
        &self,
        request: &tonic::Request<T>,
        repo: &str,
        access: RepoAccess,
    ) -> std::result::Result<Option<String>, tonic::Status> {
        if !self.auth.is_enabled() {
            return Ok(None);
        }

        let header = request
            .metadata()
            .get(AUTHORIZATION_METADATA_KEY)
            .ok_or_else(|| tonic::Status::unauthenticated("missing authorization"))?;
        let header = header
            .to_str()
            .map_err(|_| tonic::Status::unauthenticated("invalid authorization"))?;
        let token = header
            .strip_prefix("Bearer ")
            .or_else(|| header.strip_prefix("bearer "))
            .unwrap_or(header)
            .trim();
        let identity = self
            .auth
            .resolve_identity(token)
            .ok_or_else(|| tonic::Status::permission_denied("invalid token"))?;
        if !self.auth.allows(repo, identity, access) {
            return Err(tonic::Status::permission_denied("repo access denied"));
        }
        Ok(Some(identity.to_string()))
    }
}

fn estimated_manifest_entry_bytes(file: &proto::CommitFileEntry) -> usize {
    let chunk_bytes = file.chunks.len() * (64 + 8 + 8 + 8 + 16);
    file.path.len() + file.file_digest.len() + chunk_bytes + 64
}

#[tonic::async_trait]
impl RepositoryService for JetServer {
    type StreamChunksStream = ReceiverStream<std::result::Result<proto::ChunkData, tonic::Status>>;

    async fn get_repo_config(
        &self,
        request: tonic::Request<proto::GetRepoConfigRequest>,
    ) -> std::result::Result<tonic::Response<proto::GetRepoConfigResponse>, tonic::Status> {
        self.authorize(&request, &request.get_ref().repo, RepoAccess::Read)?;
        let repo_root = self
            .repo_root(&request.get_ref().repo)
            .map_err(status_from_error)?;
        let config = load_repo_config(&repo_root).map_err(status_from_error)?;
        Ok(tonic::Response::new(proto::GetRepoConfigResponse {
            config: Some(repo_config_to_proto(&config)),
        }))
    }

    async fn get_head(
        &self,
        request: tonic::Request<proto::GetHeadRequest>,
    ) -> std::result::Result<tonic::Response<proto::GetHeadResponse>, tonic::Status> {
        self.authorize(&request, &request.get_ref().repo, RepoAccess::Read)?;
        let repo_root = self
            .repo_root(&request.get_ref().repo)
            .map_err(status_from_error)?;
        let head = FsCommitStore::new(&repo_root)
            .map_err(status_from_error)?
            .read_head()
            .map_err(status_from_error)?
            .unwrap_or_default();
        Ok(tonic::Response::new(proto::GetHeadResponse {
            commit_id: head,
        }))
    }

    async fn get_commit(
        &self,
        request: tonic::Request<proto::GetCommitRequest>,
    ) -> std::result::Result<tonic::Response<proto::GetCommitResponse>, tonic::Status> {
        self.authorize(&request, &request.get_ref().repo, RepoAccess::Read)?;
        let repo_root = self
            .repo_root(&request.get_ref().repo)
            .map_err(status_from_error)?;
        let mut commit = FsCommitStore::new(&repo_root)
            .map_err(status_from_error)?
            .read_commit(&request.get_ref().commit_id)
            .map_err(status_from_error)?;
        if request.get_ref().metadata_only {
            commit.files.clear();
            commit.files_omitted = true;
        }
        Ok(tonic::Response::new(proto::GetCommitResponse {
            commit: Some(commit_to_proto(&commit)),
        }))
    }

    async fn get_manifest(
        &self,
        request: tonic::Request<proto::GetManifestRequest>,
    ) -> std::result::Result<tonic::Response<proto::GetManifestResponse>, tonic::Status> {
        self.authorize(&request, &request.get_ref().repo, RepoAccess::Read)?;
        let request = request.into_inner();
        let repo_root = self.repo_root(&request.repo).map_err(status_from_error)?;
        let commit = FsCommitStore::new(&repo_root)
            .map_err(status_from_error)?
            .read_commit(&request.commit_id)
            .map_err(status_from_error)?;
        let config = load_repo_config(&repo_root).map_err(status_from_error)?;
        let files = commit
            .files
            .into_iter()
            .filter(|file| path_in_view(&request.include, &request.exclude, &file.path))
            .filter(|file| {
                !request.hot_only
                    || jet_core::engine::should_auto_hydrate_with_patterns(
                        &file.path,
                        file.size,
                        &config.workspace.hot_paths,
                        config.workspace.max_hot_file_bytes,
                    )
            })
            .map(|file| proto::CommitFileEntry {
                path: file.path,
                size: file.size,
                file_digest: file.file_digest,
                chunks: file
                    .chunks
                    .into_iter()
                    .map(|chunk| proto::CommitChunkRef {
                        id: chunk.id,
                        offset: chunk.offset,
                        len: chunk.len,
                        raw_len: chunk.raw_len,
                    })
                    .collect(),
            })
            .collect::<Vec<_>>();

        let total_files = files.len();
        let offset = request.offset as usize;
        let max_bytes = if request.max_bytes == 0 {
            1024 * 1024
        } else {
            request.max_bytes as usize
        };
        let mut page = Vec::new();
        let mut used_bytes = 0usize;
        let mut next_offset = offset;

        for file in files.into_iter().skip(offset) {
            let entry_bytes = estimated_manifest_entry_bytes(&file);
            if !page.is_empty() && used_bytes + entry_bytes > max_bytes {
                break;
            }
            used_bytes += entry_bytes;
            next_offset += 1;
            page.push(file);
        }

        let has_more = next_offset < total_files;
        Ok(tonic::Response::new(proto::GetManifestResponse {
            files: page,
            next_offset: next_offset as u32,
            has_more,
        }))
    }

    async fn has_objects(
        &self,
        request: tonic::Request<proto::HasObjectsRequest>,
    ) -> std::result::Result<tonic::Response<proto::HasObjectsResponse>, tonic::Status> {
        self.authorize(&request, &request.get_ref().repo, RepoAccess::Read)?;
        let repo_root = self
            .repo_root(&request.get_ref().repo)
            .map_err(status_from_error)?;
        let store = self.object_store(&repo_root).map_err(status_from_error)?;
        let present_ids = request
            .get_ref()
            .object_ids
            .iter()
            .filter(|id| local_chunk_exists(&store, id))
            .cloned()
            .collect();
        Ok(tonic::Response::new(proto::HasObjectsResponse {
            present_ids,
        }))
    }

    async fn get_chunks(
        &self,
        request: tonic::Request<proto::GetChunksRequest>,
    ) -> std::result::Result<tonic::Response<proto::GetChunksResponse>, tonic::Status> {
        self.authorize(&request, &request.get_ref().repo, RepoAccess::Read)?;
        let repo_root = self
            .repo_root(&request.get_ref().repo)
            .map_err(status_from_error)?;
        let store = self.object_store(&repo_root).map_err(status_from_error)?;
        let chunks = request
            .get_ref()
            .object_ids
            .iter()
            .map(|id| {
                let data = match store.get_chunk(id) {
                    Ok(data) => data,
                    Err(JetError::ObjectNotFound(_)) => {
                        return Err(tonic::Status::not_found(id.clone()));
                    }
                    Err(JetError::Io(_)) => return Err(tonic::Status::not_found(id.clone())),
                    Err(err) => return Err(status_from_error(err)),
                };
                Ok::<proto::ChunkData, tonic::Status>(proto::ChunkData {
                    id: id.clone(),
                    data,
                })
            })
            .collect::<std::result::Result<Vec<_>, _>>()?;
        Ok(tonic::Response::new(proto::GetChunksResponse { chunks }))
    }

    async fn stream_chunks(
        &self,
        request: tonic::Request<proto::GetChunksRequest>,
    ) -> std::result::Result<tonic::Response<Self::StreamChunksStream>, tonic::Status> {
        self.authorize(&request, &request.get_ref().repo, RepoAccess::Read)?;
        let repo_root = self
            .repo_root(&request.get_ref().repo)
            .map_err(status_from_error)?;
        let store = self.object_store(&repo_root).map_err(status_from_error)?;
        let ids = request.into_inner().object_ids;
        let (tx, rx) = tokio::sync::mpsc::channel(16);
        tokio::spawn(async move {
            for id in ids {
                let item = match store.get_chunk(&id) {
                    Ok(data) => Ok(proto::ChunkData { id, data }),
                    Err(JetError::ObjectNotFound(_)) => Err(tonic::Status::not_found(id)),
                    Err(JetError::Io(_)) => Err(tonic::Status::not_found(id)),
                    Err(err) => Err(status_from_error(err)),
                };
                if tx.send(item).await.is_err() {
                    break;
                }
            }
        });
        Ok(tonic::Response::new(ReceiverStream::new(rx)))
    }

    async fn put_chunks(
        &self,
        request: tonic::Request<proto::PutChunksRequest>,
    ) -> std::result::Result<tonic::Response<proto::PutChunksResponse>, tonic::Status> {
        self.authorize(&request, &request.get_ref().repo, RepoAccess::Write)?;
        let repo_root = self
            .repo_root(&request.get_ref().repo)
            .map_err(status_from_error)?;
        let store = self.object_store(&repo_root).map_err(status_from_error)?;
        let mut stored_count = 0usize;
        for chunk in &request.get_ref().chunks {
            if store
                .put_chunk_with_id(&chunk.id, &chunk.data)
                .map_err(status_from_error)?
                .was_new
            {
                stored_count += 1;
            }
        }
        Ok(tonic::Response::new(proto::PutChunksResponse {
            stored_count: stored_count as u32,
        }))
    }

    async fn put_commit(
        &self,
        request: tonic::Request<proto::PutCommitRequest>,
    ) -> std::result::Result<tonic::Response<proto::PutCommitResponse>, tonic::Status> {
        self.authorize(&request, &request.get_ref().repo, RepoAccess::Write)?;
        let repo_root = self
            .repo_root(&request.get_ref().repo)
            .map_err(status_from_error)?;
        let commit = request
            .into_inner()
            .commit
            .ok_or_else(|| tonic::Status::invalid_argument("missing commit"))?;
        FsCommitStore::new(&repo_root)
            .map_err(status_from_error)?
            .write_commit(&commit_from_proto(commit))
            .map_err(status_from_error)?;
        Ok(tonic::Response::new(proto::PutCommitResponse {}))
    }

    async fn update_head(
        &self,
        request: tonic::Request<proto::UpdateHeadRequest>,
    ) -> std::result::Result<tonic::Response<proto::UpdateHeadResponse>, tonic::Status> {
        self.authorize(&request, &request.get_ref().repo, RepoAccess::Write)?;
        let request = request.into_inner();
        let repo_root = self.repo_root(&request.repo).map_err(status_from_error)?;
        let store = FsCommitStore::new(&repo_root).map_err(status_from_error)?;
        let current = store.read_head().map_err(status_from_error)?;
        let expected = if request.expected_commit_id.is_empty() {
            None
        } else {
            Some(request.expected_commit_id.as_str())
        };
        let updated = current.as_deref() == expected;
        if updated {
            store
                .write_head(&request.new_commit_id)
                .map_err(status_from_error)?;
        }
        Ok(tonic::Response::new(proto::UpdateHeadResponse {
            updated,
            current_commit_id: if updated {
                request.new_commit_id
            } else {
                current.unwrap_or_default()
            },
        }))
    }

    async fn lock_path(
        &self,
        request: tonic::Request<proto::LockPathRequest>,
    ) -> std::result::Result<tonic::Response<proto::LockPathResponse>, tonic::Status> {
        let authenticated_identity =
            self.authorize(&request, &request.get_ref().repo, RepoAccess::Write)?;
        let request = request.into_inner();
        let repo_root = self.repo_root(&request.repo).map_err(status_from_error)?;
        let owner = authenticated_identity.unwrap_or(request.owner.clone());
        let mut locks = self
            .load_repo_locks(&request.repo, &repo_root)
            .map_err(status_from_error)?;
        if let Some(existing_owner) = locks.get(&request.path)
            && existing_owner != &owner
        {
            return Err(tonic::Status::failed_precondition(format!(
                "lock_conflict:{}:{}",
                request.path, existing_owner
            )));
        }
        locks.insert(request.path.clone(), owner.clone());
        self.save_repo_locks(&request.repo, &repo_root, &locks)
            .map_err(status_from_error)?;
        Ok(tonic::Response::new(proto::LockPathResponse {
            lock: Some(proto::LockInfo {
                path: request.path,
                owner,
            }),
        }))
    }

    async fn unlock_path(
        &self,
        request: tonic::Request<proto::UnlockPathRequest>,
    ) -> std::result::Result<tonic::Response<proto::UnlockPathResponse>, tonic::Status> {
        let authenticated_identity =
            self.authorize(&request, &request.get_ref().repo, RepoAccess::Write)?;
        let request = request.into_inner();
        let repo_root = self.repo_root(&request.repo).map_err(status_from_error)?;
        let owner = authenticated_identity.unwrap_or(request.owner.clone());
        let mut locks = self
            .load_repo_locks(&request.repo, &repo_root)
            .map_err(status_from_error)?;
        let Some(existing_owner) = locks.get(&request.path) else {
            return Err(tonic::Status::failed_precondition(format!(
                "lock_owner_mismatch:{}:{}",
                request.path, owner
            )));
        };
        if existing_owner != &owner {
            return Err(tonic::Status::failed_precondition(format!(
                "lock_owner_mismatch:{}:{}",
                request.path, existing_owner
            )));
        }
        locks.remove(&request.path);
        self.save_repo_locks(&request.repo, &repo_root, &locks)
            .map_err(status_from_error)?;
        Ok(tonic::Response::new(proto::UnlockPathResponse {}))
    }

    async fn list_locks(
        &self,
        request: tonic::Request<proto::ListLocksRequest>,
    ) -> std::result::Result<tonic::Response<proto::ListLocksResponse>, tonic::Status> {
        self.authorize(&request, &request.get_ref().repo, RepoAccess::Read)?;
        let request = request.into_inner();
        let repo_root = self.repo_root(&request.repo).map_err(status_from_error)?;
        let locks = self
            .load_repo_locks(&request.repo, &repo_root)
            .map_err(status_from_error)?;
        let prefix = request.prefix;
        let locks = locks
            .into_iter()
            .filter(|(path, _)| prefix.is_empty() || path.starts_with(&prefix))
            .map(|(path, owner)| proto::LockInfo { path, owner })
            .collect();
        Ok(tonic::Response::new(proto::ListLocksResponse { locks }))
    }

    async fn get_current_identity(
        &self,
        request: tonic::Request<proto::GetCurrentIdentityRequest>,
    ) -> std::result::Result<tonic::Response<proto::GetCurrentIdentityResponse>, tonic::Status> {
        let identity = self
            .authorize(&request, &request.get_ref().repo, RepoAccess::Read)?
            .ok_or_else(|| tonic::Status::failed_precondition("auth is not enabled"))?;
        Ok(tonic::Response::new(proto::GetCurrentIdentityResponse {
            identity,
        }))
    }
}

fn local_chunk_exists(store: &FsObjectStore, id: &str) -> bool {
    store.resolve_checkout_chunks(&[id.to_string()]).is_ok()
}

fn path_in_view(include: &[String], exclude: &[String], path: &str) -> bool {
    let included = if include.is_empty() {
        true
    } else {
        include
            .iter()
            .any(|pattern| workspace_pattern_matches(pattern, path))
    };
    included
        && !exclude
            .iter()
            .any(|pattern| workspace_pattern_matches(pattern, path))
}

fn workspace_pattern_matches(pattern: &str, path: &str) -> bool {
    let normalized = pattern.replace('\\', "/");
    let normalized = normalized.trim_end_matches('/');
    if normalized.is_empty() || normalized == "..." {
        return true;
    }
    if let Some(prefix) = normalized.strip_suffix("/...") {
        return path == prefix || path.starts_with(&format!("{prefix}/"));
    }
    path == normalized
}

fn status_from_error(err: JetError) -> tonic::Status {
    match err {
        JetError::ObjectNotFound(message) => tonic::Status::not_found(message),
        JetError::InvalidRepository { path } => {
            tonic::Status::not_found(format!("invalid repository: {}", path.display()))
        }
        other => tonic::Status::internal(other.to_string()),
    }
}

fn repo_config_to_proto(config: &RepoConfig) -> proto::RepoConfig {
    proto::RepoConfig {
        schema_version: config.schema_version,
        repo_id: config.repo_id.clone(),
        chunking: Some(proto::ChunkingConfig {
            min: config.chunking.min,
            avg: config.chunking.avg,
            max: config.chunking.max,
        }),
        compression: Some(proto::CompressionConfig {
            enabled: config.compression.enabled,
            level: config.compression.level,
        }),
        storage: Some(proto::StorageConfig {
            direct_blob_threshold_bytes: config.storage.direct_blob_threshold_bytes,
        }),
        workspace: Some(proto::WorkspaceConfig {
            hot_paths: config.workspace.hot_paths.clone(),
            max_hot_file_bytes: config.workspace.max_hot_file_bytes,
        }),
    }
}

fn commit_to_proto(commit: &Commit) -> proto::Commit {
    proto::Commit {
        schema_version: commit.schema_version,
        id: commit.id.clone(),
        parent: commit.parent.clone().unwrap_or_default(),
        author: commit.author.clone(),
        message: commit.message.clone(),
        timestamp_unix: commit.timestamp_unix,
        files_omitted: commit.files_omitted,
        files: commit
            .files
            .iter()
            .map(|file| proto::CommitFileEntry {
                path: file.path.clone(),
                size: file.size,
                file_digest: file.file_digest.clone(),
                chunks: file
                    .chunks
                    .iter()
                    .map(|chunk| proto::CommitChunkRef {
                        id: chunk.id.clone(),
                        offset: chunk.offset,
                        len: chunk.len,
                        raw_len: chunk.raw_len,
                    })
                    .collect(),
            })
            .collect(),
    }
}

fn commit_from_proto(proto: proto::Commit) -> Commit {
    Commit {
        schema_version: proto.schema_version,
        id: proto.id,
        parent: if proto.parent.is_empty() {
            None
        } else {
            Some(proto.parent)
        },
        author: proto.author,
        message: proto.message,
        timestamp_unix: proto.timestamp_unix,
        files_omitted: proto.files_omitted,
        files: proto
            .files
            .into_iter()
            .map(|file| jet_core::commit_store::CommitFileEntry {
                path: file.path,
                size: file.size,
                file_digest: file.file_digest,
                chunks: file
                    .chunks
                    .into_iter()
                    .map(|chunk| jet_core::commit_store::CommitChunkRef {
                        id: chunk.id,
                        offset: chunk.offset,
                        len: chunk.len,
                        raw_len: chunk.raw_len,
                    })
                    .collect(),
            })
            .collect(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::net::SocketAddr;

    use jet_proto::proto::repository_service_client::RepositoryServiceClient;
    use jet_proto::proto::repository_service_server::RepositoryServiceServer;
    use tempfile::tempdir;
    use tokio::sync::oneshot;
    use tokio_stream::wrappers::TcpListenerStream;

    const TEST_MAX_MESSAGE_BYTES: usize = 256 * 1024 * 1024;

    #[tokio::test]
    async fn serves_repo_metadata_and_locks() {
        let root = tempdir().expect("root");
        let repo_root = root.path().join("game");
        jet_core::repo::init_repo(&repo_root).expect("init repo");
        fs::create_dir_all(repo_root.join("code")).expect("mkdir");
        fs::write(repo_root.join("code").join("main.rs"), "fn main() {}\n").expect("write");
        let repo = jet_core::JetRepository::open(&repo_root).expect("open repo");
        repo.add_paths(&[repo_root.join("code")]).expect("add");
        let head = repo.commit("initial", "tester").expect("commit");

        let service = JetServer::new(root.path()).expect("server");
        let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind");
        let addr: SocketAddr = listener.local_addr().expect("addr");
        let incoming = TcpListenerStream::new(listener);
        let task = tokio::spawn(async move {
            tonic::transport::Server::builder()
                .add_service(
                    RepositoryServiceServer::new(service)
                        .max_decoding_message_size(TEST_MAX_MESSAGE_BYTES)
                        .max_encoding_message_size(TEST_MAX_MESSAGE_BYTES),
                )
                .serve_with_incoming_shutdown(incoming, async {
                    let _ = shutdown_rx.await;
                })
                .await
                .expect("serve");
        });

        let mut client = RepositoryServiceClient::connect(format!("http://{addr}"))
            .await
            .expect("client")
            .max_decoding_message_size(TEST_MAX_MESSAGE_BYTES)
            .max_encoding_message_size(TEST_MAX_MESSAGE_BYTES);

        let head_response = client
            .get_head(proto::GetHeadRequest {
                repo: "game".to_string(),
            })
            .await
            .expect("head")
            .into_inner();
        assert_eq!(head_response.commit_id, head);

        let lock = client
            .lock_path(proto::LockPathRequest {
                repo: "game".to_string(),
                path: "assets/hero.psd".to_string(),
                owner: "artist".to_string(),
            })
            .await
            .expect("lock")
            .into_inner()
            .lock
            .expect("lock info");
        assert_eq!(lock.owner, "artist");

        let locks = client
            .list_locks(proto::ListLocksRequest {
                repo: "game".to_string(),
                prefix: "assets".to_string(),
            })
            .await
            .expect("locks")
            .into_inner()
            .locks;
        assert_eq!(locks.len(), 1);

        let _ = shutdown_tx.send(());
        task.await.expect("join");
    }

    #[test]
    fn resolves_current_directory_repo_by_basename() {
        let root = tempdir().expect("root");
        let repo_root = root.path().join("game");
        jet_core::repo::init_repo(&repo_root).expect("init repo");

        let server = JetServer::new(&repo_root).expect("server");
        let resolved = server.repo_root("game").expect("resolved repo");

        assert_eq!(resolved, fs::canonicalize(&repo_root).expect("canonical repo"));
    }

    #[test]
    fn resolves_relative_current_directory_repo_by_basename() {
        let root = tempdir().expect("root");
        let repo_root = root.path().join("game");
        jet_core::repo::init_repo(&repo_root).expect("init repo");

        let previous = std::env::current_dir().expect("cwd");
        std::env::set_current_dir(&repo_root).expect("chdir");
        let server = JetServer::new(".").expect("server");
        let resolved = server.repo_root("game").expect("resolved repo");
        std::env::set_current_dir(previous).expect("restore cwd");

        assert_eq!(resolved, fs::canonicalize(&repo_root).expect("canonical repo"));
    }
}
