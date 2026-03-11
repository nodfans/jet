use std::collections::HashSet;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use jet_core::commit_store::{Commit, CommitFileEntry, CommitStore, FsCommitStore};
use jet_core::object_store::{FsObjectStore, ObjectStore};
use jet_core::repo::{RepoConfig, init_repo, load_repo_config, save_repo_config};
use jet_core::workspace::WorkspaceManifest;
use jet_core::{JetError, JetRepository, Result, clone_from_path};
use jet_proto::proto;
use tokio_stream::StreamExt;
use tonic::transport::{Channel, Endpoint};
use serde::{Deserialize, Serialize};

mod trace;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RemoteLocation {
    pub endpoint: String,
    pub repo: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RepoSource {
    LocalPath(PathBuf),
    Remote(RemoteLocation),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PushReport {
    pub new_head: String,
    pub commit_count: usize,
    pub chunk_count: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PullReport {
    pub new_head: String,
    pub commit_count: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LockInfo {
    pub path: String,
    pub owner: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AuthIdentity {
    pub identity: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CloneMode {
    All,
    Partial,
}

const INITIAL_REMOTE_METADATA_DEPTH: usize = 1;
const REMOTE_HISTORY_SYNC_WINDOW: usize = 32;
const REMOTE_MANIFEST_PAGE_BYTES: u32 = 1024 * 1024;
const REMOTE_MAX_MESSAGE_BYTES: usize = 256 * 1024 * 1024;
const REMOTE_CHUNK_BATCH_SIZE: usize = 64;
const AUTHORIZATION_METADATA_KEY: &str = "authorization";

#[derive(Clone)]
pub struct GrpcRemoteClient {
    location: RemoteLocation,
    runtime: Arc<tokio::runtime::Runtime>,
    auth_token: Option<String>,
}

impl std::fmt::Debug for GrpcRemoteClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GrpcRemoteClient")
            .field("location", &self.location)
            .finish()
    }
}

impl GrpcRemoteClient {
    pub fn from_source(source: &str) -> Result<Self> {
        let location = parse_remote_location(source).map_err(remote_transport_error)?;
        Self::with_auth_token(location, resolve_auth_token(Some(source), None))
    }

    pub fn from_source_for_repo(source: &str, repo_root: impl AsRef<Path>) -> Result<Self> {
        let location = parse_remote_location(source).map_err(remote_transport_error)?;
        Self::with_auth_token(location, resolve_auth_token(Some(source), Some(repo_root.as_ref())))
    }

    pub fn new(location: RemoteLocation) -> Result<Self> {
        Self::with_auth_token(location, None)
    }

    pub fn with_auth_token(location: RemoteLocation, auth_token: Option<String>) -> Result<Self> {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .map_err(remote_transport_error)?;
        Ok(Self {
            location,
            runtime: Arc::new(runtime),
            auth_token,
        })
    }

    async fn connect(
        &self,
    ) -> std::result::Result<
        proto::repository_service_client::RepositoryServiceClient<Channel>,
        tonic::transport::Error,
    > {
        let endpoint = Endpoint::from_shared(self.location.endpoint.clone())?;
        let channel = endpoint.connect().await?;
        Ok(
            proto::repository_service_client::RepositoryServiceClient::new(channel)
                .max_decoding_message_size(REMOTE_MAX_MESSAGE_BYTES)
                .max_encoding_message_size(REMOTE_MAX_MESSAGE_BYTES),
        )
    }

    fn attach_auth<T>(&self, message: T) -> Result<tonic::Request<T>> {
        let mut request = tonic::Request::new(message);
        if let Some(token) = &self.auth_token {
            let value = tonic::metadata::MetadataValue::try_from(format!("Bearer {token}"))
                .map_err(remote_transport_error)?;
            request.metadata_mut().insert(AUTHORIZATION_METADATA_KEY, value);
        }
        Ok(request)
    }

    pub fn get_repo_config(&self) -> Result<Option<proto::RepoConfig>> {
        let repo = self.location.repo.clone();
        self.runtime.block_on(async {
            let mut client = self.connect().await.map_err(remote_transport_error)?;
            let response = client
                .get_repo_config(self.attach_auth(proto::GetRepoConfigRequest { repo })?)
                .await
                .map_err(map_transport_status_error)?;
            Ok(response.into_inner().config)
        })
    }

    pub fn get_head(&self) -> Result<Option<String>> {
        let repo = self.location.repo.clone();
        self.runtime.block_on(async {
            let mut client = self.connect().await.map_err(remote_transport_error)?;
            let response = client
                .get_head(self.attach_auth(proto::GetHeadRequest { repo })?)
                .await
                .map_err(map_transport_status_error)?;
            let commit_id = response.into_inner().commit_id;
            if commit_id.is_empty() {
                Ok(None)
            } else {
                Ok(Some(commit_id))
            }
        })
    }

    pub fn get_commit(&self, commit_id: &str) -> Result<Option<proto::Commit>> {
        self.get_commit_with_mode(commit_id, false)
    }

    pub fn get_commit_metadata(&self, commit_id: &str) -> Result<Option<proto::Commit>> {
        self.get_commit_with_mode(commit_id, true)
    }

    fn get_commit_with_mode(
        &self,
        commit_id: &str,
        metadata_only: bool,
    ) -> Result<Option<proto::Commit>> {
        let repo = self.location.repo.clone();
        let commit_id = commit_id.to_string();
        self.runtime.block_on(async {
            let mut client = self.connect().await.map_err(remote_transport_error)?;
            let response = client
                .get_commit(self.attach_auth(proto::GetCommitRequest {
                    repo,
                    commit_id: commit_id.clone(),
                    metadata_only,
                })?)
                .await;
            let response = match response {
                Ok(response) => response,
                Err(status) if status.code() == tonic::Code::NotFound => return Ok(None),
                Err(status) => return Err(map_transport_status_error(status)),
            };
            Ok(response.into_inner().commit)
        })
    }

    pub fn get_manifest(
        &self,
        commit_id: &str,
        include: &[String],
        exclude: &[String],
        hot_only: bool,
    ) -> Result<Vec<proto::CommitFileEntry>> {
        let repo = self.location.repo.clone();
        let commit_id = commit_id.to_string();
        let include = include.to_vec();
        let exclude = exclude.to_vec();
        self.runtime.block_on(async {
            let mut client = self.connect().await.map_err(remote_transport_error)?;
            let mut offset = 0_u32;
            let mut files = Vec::new();
            loop {
                let response = client
                    .get_manifest(self.attach_auth(proto::GetManifestRequest {
                        repo: repo.clone(),
                        commit_id: commit_id.clone(),
                        include: include.clone(),
                        exclude: exclude.clone(),
                        hot_only,
                        offset,
                        max_bytes: REMOTE_MANIFEST_PAGE_BYTES,
                    })?)
                    .await;
                let response = match response {
                    Ok(response) => response,
                    Err(status) if status.code() == tonic::Code::NotFound => {
                        return Err(JetError::RemoteCommitNotFound {
                            commit_id: commit_id.clone(),
                        });
                    }
                    Err(status) => return Err(map_transport_status_error(status)),
                };
                let response = response.into_inner();
                files.extend(response.files);
                if !response.has_more {
                    break;
                }
                offset = response.next_offset;
            }
            Ok(files)
        })
    }

    pub fn has_objects(&self, object_ids: &[String]) -> Result<HashSet<String>> {
        let repo = self.location.repo.clone();
        let object_ids = object_ids.to_vec();
        self.runtime.block_on(async {
            let mut client = self.connect().await.map_err(remote_transport_error)?;
            let response = client
                .has_objects(self.attach_auth(proto::HasObjectsRequest { repo, object_ids })?)
                .await
                .map_err(map_transport_status_error)?;
            Ok(response.into_inner().present_ids.into_iter().collect())
        })
    }

    pub fn get_chunks(&self, object_ids: &[String]) -> Result<Vec<proto::ChunkData>> {
        let repo = self.location.repo.clone();
        let object_ids = object_ids.to_vec();
        self.runtime.block_on(async {
            let mut client = self.connect().await.map_err(remote_transport_error)?;
            let mut chunks = Vec::new();
            for batch in object_ids.chunks(REMOTE_CHUNK_BATCH_SIZE) {
                let response = client
                    .stream_chunks(self.attach_auth(proto::GetChunksRequest {
                        repo: repo.clone(),
                        object_ids: batch.to_vec(),
                    })?)
                    .await;
                let mut stream = match response {
                    Ok(response) => response.into_inner(),
                    Err(status) if status.code() == tonic::Code::NotFound => {
                        return Err(JetError::ObjectNotFound(status.message().to_string()));
                    }
                    Err(status) => return Err(map_transport_status_error(status)),
                };
                while let Some(chunk) = stream.next().await {
                    let chunk = match chunk {
                        Ok(chunk) => chunk,
                        Err(status) if status.code() == tonic::Code::NotFound => {
                            return Err(JetError::ObjectNotFound(status.message().to_string()));
                        }
                        Err(status) => return Err(map_transport_status_error(status)),
                    };
                    chunks.push(chunk);
                }
            }
            Ok(chunks)
        })
    }

    pub fn put_chunks(&self, chunks: Vec<proto::ChunkData>) -> Result<usize> {
        let repo = self.location.repo.clone();
        self.runtime.block_on(async {
            let mut client = self.connect().await.map_err(remote_transport_error)?;
            let response = client
                .put_chunks(self.attach_auth(proto::PutChunksRequest { repo, chunks })?)
                .await
                .map_err(map_transport_status_error)?;
            Ok(response.into_inner().stored_count as usize)
        })
    }

    pub fn put_commit(&self, commit: proto::Commit) -> Result<()> {
        let repo = self.location.repo.clone();
        self.runtime.block_on(async {
            let mut client = self.connect().await.map_err(remote_transport_error)?;
            client
                .put_commit(self.attach_auth(proto::PutCommitRequest {
                    repo,
                    commit: Some(commit),
                })?)
                .await
                .map_err(map_transport_status_error)?;
            Ok(())
        })
    }

    pub fn update_head(&self, expected: Option<&str>, new_head: &str) -> Result<bool> {
        let repo = self.location.repo.clone();
        let expected_commit_id = expected.unwrap_or_default().to_string();
        let new_commit_id = new_head.to_string();
        self.runtime.block_on(async {
            let mut client = self.connect().await.map_err(remote_transport_error)?;
            let response = client
                .update_head(self.attach_auth(proto::UpdateHeadRequest {
                    repo,
                    expected_commit_id,
                    new_commit_id,
                })?)
                .await
                .map_err(map_transport_status_error)?;
            Ok(response.into_inner().updated)
        })
    }

    pub fn lock_path(&self, path: &str, owner: &str) -> Result<proto::LockInfo> {
        let repo = self.location.repo.clone();
        let path = path.to_string();
        let owner = owner.to_string();
        self.runtime.block_on(async {
            let mut client = self.connect().await.map_err(status_from_transport_error)?;
            let response = client
                .lock_path(self.attach_auth(proto::LockPathRequest { repo, path, owner })?)
                .await
                .map_err(map_lock_transport_error)?;
            response
                .into_inner()
                .lock
                .ok_or_else(|| JetError::RemoteTransport {
                    message: "missing lock response".to_string(),
                })
        })
    }

    pub fn unlock_path(&self, path: &str, owner: &str) -> Result<()> {
        let repo = self.location.repo.clone();
        let path = path.to_string();
        let owner = owner.to_string();
        self.runtime.block_on(async {
            let mut client = self.connect().await.map_err(status_from_transport_error)?;
            client
                .unlock_path(self.attach_auth(proto::UnlockPathRequest { repo, path, owner })?)
                .await
                .map_err(map_lock_transport_error)?;
            Ok(())
        })
    }

    pub fn list_locks(&self, prefix: Option<&str>) -> Result<Vec<proto::LockInfo>> {
        let repo = self.location.repo.clone();
        let prefix = prefix.unwrap_or_default().to_string();
        self.runtime.block_on(async {
            let mut client = self.connect().await.map_err(remote_transport_error)?;
            let response = client
                .list_locks(self.attach_auth(proto::ListLocksRequest { repo, prefix })?)
                .await
                .map_err(map_transport_status_error)?;
            Ok(response.into_inner().locks)
        })
    }

    pub fn get_current_identity(&self) -> Result<AuthIdentity> {
        let repo = self.location.repo.clone();
        self.runtime.block_on(async {
            let mut client = self.connect().await.map_err(remote_transport_error)?;
            let response = client
                .get_current_identity(self.attach_auth(proto::GetCurrentIdentityRequest { repo })?)
                .await
                .map_err(map_transport_status_error)?;
            Ok(AuthIdentity {
                identity: response.into_inner().identity,
            })
        })
    }
}

pub fn clone_from_source(
    source: &str,
    destination: impl AsRef<Path>,
    mode: CloneMode,
) -> Result<()> {
    match parse_repo_source(source)? {
        RepoSource::LocalPath(path) => {
            clone_from_path(path, &destination)?;
            if mode == CloneMode::Partial {
                let repo = JetRepository::open(destination)?;
                let _ = repo.dehydrate(&[])?;
            }
            Ok(())
        }
        RepoSource::Remote(location) => {
            clone_from_remote(
                &GrpcRemoteClient::with_auth_token(
                    location,
                    resolve_auth_token(Some(source), None),
                )?,
                destination,
                source,
                mode,
            )
        }
    }
}

pub fn hydrate_with_remote(repo_root: impl AsRef<Path>, paths: &[PathBuf]) -> Result<usize> {
    let repo_root = repo_root.as_ref();
    let repo = JetRepository::open(repo_root)?;
    let Some(remote_source) = repo.workspace_remote_source()? else {
        return repo.hydrate(paths);
    };
    let workspace_view = repo.workspace_view()?;
    let head = repo.head_commit_id()?.ok_or(JetError::NoWorkspaceCommit)?;
    let transport = GrpcRemoteClient::from_source_for_repo(&remote_source, repo_root)?;
    let requested_include = requested_path_include_patterns(paths);
    drop(repo);
    fetch_missing_objects_for_commit(
        repo_root,
        &transport,
        &head,
        paths,
        Some(&requested_include),
        Some(&workspace_view.view.exclude),
        false,
    )?;
    JetRepository::open(repo_root)?.hydrate(paths)
}

pub fn open_with_remote(repo_root: impl AsRef<Path>, commit_id: &str) -> Result<()> {
    let repo_root = repo_root.as_ref();
    let repo = JetRepository::open(repo_root)?;
    let commit_store = FsCommitStore::new(repo_root)?;
    let remote_source = repo
        .workspace_remote_source()?
        .ok_or(JetError::NoRemoteConfigured)?;
    let transport = GrpcRemoteClient::from_source_for_repo(&remote_source, repo_root)?;
    let commit = ensure_local_commit_metadata(&commit_store, &transport, commit_id)?;
    if !commit.files_omitted {
        return repo.open_workspace(commit_id, true);
    }

    let workspace_view = repo.workspace_view()?;
    let workspace_manifest = load_remote_workspace_manifest(
        &transport,
        commit_id,
        &workspace_view.view.include,
        &workspace_view.view.exclude,
    )?;
    fetch_missing_objects_for_files(
        repo_root,
        &transport,
        commit_id,
        &workspace_manifest.files,
        &[],
        true,
    )?;
    drop(repo);
    let repo = JetRepository::open(repo_root)?;
    repo.open_workspace_with_files(
        commit_id,
        &workspace_manifest.files,
        &workspace_manifest.files,
        true,
    )?;
    repo.save_workspace_manifest(&workspace_manifest)
}

pub fn push_to_remote(repo_root: impl AsRef<Path>, remote: &str) -> Result<PushReport> {
    let repo_root = repo_root.as_ref();
    let transport = GrpcRemoteClient::from_source_for_repo(remote, repo_root)?;
    let repo = JetRepository::open(repo_root)?;
    let local_head = repo.head_commit_id()?.ok_or(JetError::NoWorkspaceCommit)?;
    let commit_store = FsCommitStore::new(repo_root)?;
    let object_store = local_object_store(repo_root)?;
    let remote_head = transport.get_head()?;

    if let Some(remote_head_id) = remote_head.as_deref()
        && !is_local_ancestor(&commit_store, remote_head_id, &local_head)?
    {
        return Err(JetError::RemotePushRejected {
            remote_head: remote_head_id.to_string(),
        });
    }

    let commits = collect_local_commits_until(&commit_store, &local_head, remote_head.as_deref())?;
    let mut chunk_ids = HashSet::new();
    for commit in &commits {
        for file in &commit.files {
            for chunk in &file.chunks {
                chunk_ids.insert(chunk.id.clone());
            }
        }
    }

    let chunk_ids = chunk_ids.into_iter().collect::<Vec<_>>();
    let present = transport.has_objects(&chunk_ids)?;
    let missing_ids = chunk_ids
        .into_iter()
        .filter(|id| !present.contains(id))
        .collect::<Vec<_>>();
    let chunks = missing_ids
        .iter()
        .map(|id| {
            Ok::<proto::ChunkData, JetError>(proto::ChunkData {
                id: id.clone(),
                data: object_store.get_chunk(id)?,
            })
        })
        .collect::<Result<Vec<_>>>()?;

    if !chunks.is_empty() {
        transport.put_chunks(chunks)?;
    }

    for commit in commits.iter().rev() {
        transport.put_commit(commit_to_proto(commit))?;
    }

    if !transport.update_head(remote_head.as_deref(), &local_head)? {
        let current = transport.get_head()?.unwrap_or_default();
        return Err(JetError::RemotePushRejected {
            remote_head: current,
        });
    }

    Ok(PushReport {
        new_head: local_head,
        commit_count: commits.len(),
        chunk_count: missing_ids.len(),
    })
}

pub fn sync_remote_history(repo_root: impl AsRef<Path>, remote: Option<&str>) -> Result<usize> {
    sync_remote_history_limit(repo_root, remote, REMOTE_HISTORY_SYNC_WINDOW)
}

pub fn sync_remote_history_limit(
    repo_root: impl AsRef<Path>,
    remote: Option<&str>,
    limit: usize,
) -> Result<usize> {
    let repo_root = repo_root.as_ref();
    let repo = JetRepository::open(repo_root)?;
    let remote_source = match remote {
        Some(remote) => remote.to_string(),
        None => repo
            .workspace_remote_source()?
            .ok_or(JetError::NoRemoteConfigured)?,
    };
    let transport = GrpcRemoteClient::from_source_for_repo(&remote_source, repo_root)?;
    let commit_store = FsCommitStore::new(repo_root)?;
    let Some(head) = commit_store.read_head()? else {
        return Ok(0);
    };

    let mut cursor = Some(head);
    let mut missing_start = None;
    while let Some(id) = cursor {
        match commit_store.read_commit(&id) {
            Ok(commit) => cursor = commit.parent,
            Err(JetError::ObjectNotFound(_)) => {
                missing_start = Some(id);
                break;
            }
            Err(err) => return Err(err),
        }
    }

    let Some(start) = missing_start else {
        return Ok(0);
    };

    let commits = collect_remote_commits_until(&transport, &start, None, Some(limit))?;
    for commit in commits.iter().rev() {
        commit_store.write_commit(commit)?;
    }
    Ok(commits.len())
}

pub fn pull_from_remote(repo_root: impl AsRef<Path>, remote: Option<&str>) -> Result<PullReport> {
    let repo_root = repo_root.as_ref();
    let repo = JetRepository::open(repo_root)?;
    repo.ensure_clean_workspace()?;

    let remote_source = match remote {
        Some(remote) => remote.to_string(),
        None => repo
            .workspace_remote_source()?
            .ok_or(JetError::NoRemoteConfigured)?,
    };
    let transport = GrpcRemoteClient::from_source_for_repo(&remote_source, repo_root)?;
    let remote_head = transport.get_head()?.ok_or(JetError::RemoteTransport {
        message: "remote repository has no head".to_string(),
    })?;
    let local_head = repo.head_commit_id()?;

    if let Some(local_head_id) = local_head.as_deref()
        && !is_remote_ancestor(&transport, local_head_id, &remote_head)?
    {
        return Err(JetError::RemotePullRejected);
    }

    let config = transport
        .get_repo_config()?
        .ok_or(JetError::RemoteTransport {
            message: "missing repo config".to_string(),
        })
        .map(repo_config_from_proto)?;
    save_repo_config(repo_root, &config)?;

    let commit_store = FsCommitStore::new(repo_root)?;
    let commits =
        collect_remote_commits_until(&transport, &remote_head, local_head.as_deref(), None)?;
    for commit in commits.iter().rev() {
        commit_store.write_commit(commit)?;
    }

    let workspace_view = repo.workspace_view()?;
    let workspace_manifest = load_remote_workspace_manifest(
        &transport,
        &remote_head,
        &workspace_view.view.include,
        &workspace_view.view.exclude,
    )?;
    if trace::enabled() {
        trace::emit_pull_prefetch(&remote_head);
    }
    fetch_missing_objects_for_files(
        repo_root,
        &transport,
        &remote_head,
        &workspace_manifest.files,
        &[],
        true,
    )?;
    drop(repo);
    let repo = JetRepository::open(repo_root)?;
    repo.set_workspace_remote_source(Some(remote_source))?;
    repo.open_workspace_with_files(
        &remote_head,
        &workspace_manifest.files,
        &workspace_manifest.files,
        true,
    )?;
    repo.save_workspace_manifest(&workspace_manifest)?;

    Ok(PullReport {
        new_head: remote_head,
        commit_count: commits.len(),
    })
}

pub fn lock_remote_path(remote: &str, path: &str, owner: &str) -> Result<LockInfo> {
    GrpcRemoteClient::from_source(remote)?
        .lock_path(path, owner)
        .map(lock_info_from_proto)
}

pub fn unlock_remote_path(remote: &str, path: &str, owner: &str) -> Result<()> {
    GrpcRemoteClient::from_source(remote)?.unlock_path(path, owner)
}

pub fn list_remote_locks(remote: &str, prefix: Option<&str>) -> Result<Vec<LockInfo>> {
    GrpcRemoteClient::from_source(remote)?
        .list_locks(prefix)
        .map(|locks| locks.into_iter().map(lock_info_from_proto).collect())
}

pub fn remote_whoami(remote: &str) -> Result<AuthIdentity> {
    GrpcRemoteClient::from_source(remote)?.get_current_identity()
}

pub fn login_with_token(remote: &str, token: &str) -> Result<AuthIdentity> {
    let location = parse_remote_location(remote).map_err(remote_transport_error)?;
    let client = GrpcRemoteClient::with_auth_token(location.clone(), Some(token.to_string()))?;
    let identity = client.get_current_identity()?;
    save_global_auth_token(&location.endpoint, token)?;
    Ok(identity)
}

pub fn parse_remote_location(source: &str) -> std::result::Result<RemoteLocation, String> {
    let (scheme, rest) = source
        .split_once("://")
        .ok_or_else(|| format!("invalid remote source: {source}"))?;
    let (authority, repo_path) = rest
        .split_once('/')
        .ok_or_else(|| format!("remote source is missing repo path: {source}"))?;
    if authority.is_empty() || repo_path.is_empty() {
        return Err(format!("remote source is incomplete: {source}"));
    }

    let transport_scheme = match scheme {
        "jet" => "http",
        "jets" => "https",
        "http" | "https" => scheme,
        other => return Err(format!("unsupported remote scheme: {other}")),
    };

    Ok(RemoteLocation {
        endpoint: format!("{transport_scheme}://{authority}"),
        repo: repo_path.trim_matches('/').to_string(),
    })
}

pub fn parse_repo_source(source: &str) -> Result<RepoSource> {
    if let Some(path) = source.strip_prefix("file://") {
        return Ok(RepoSource::LocalPath(PathBuf::from(path)));
    }
    if source.contains("://") {
        return Ok(RepoSource::Remote(
            parse_remote_location(source).map_err(remote_transport_error)?,
        ));
    }
    Ok(RepoSource::LocalPath(PathBuf::from(source)))
}

fn clone_from_remote(
    transport: &GrpcRemoteClient,
    destination: impl AsRef<Path>,
    source: &str,
    mode: CloneMode,
) -> Result<()> {
    let destination = destination.as_ref();
    ensure_clone_destination_ready(destination)?;

    let config = transport
        .get_repo_config()?
        .ok_or(JetError::RemoteTransport {
            message: "missing repo config".to_string(),
        })
        .map(repo_config_from_proto)?;
    let head = transport.get_head()?;

    init_repo(destination)?;
    save_repo_config(destination, &config)?;

    let commit_store = FsCommitStore::new(destination)?;
    let Some(head) = head else {
        let repo = JetRepository::open(destination)?;
        repo.set_workspace_remote_source(Some(source.to_string()))?;
        return Ok(());
    };

    let commits =
        collect_remote_commits_until(transport, &head, None, Some(INITIAL_REMOTE_METADATA_DEPTH))?;
    for commit in commits.iter().rev() {
        commit_store.write_commit(commit)?;
    }

    let workspace_manifest = load_remote_workspace_manifest(transport, &head, &[], &[])?;
    fetch_missing_objects_for_files(
        destination,
        transport,
        &head,
        &workspace_manifest.files,
        &[],
        true,
    )?;

    let repo = JetRepository::open(destination)?;
    repo.set_workspace_remote_source(Some(source.to_string()))?;
    repo.open_workspace_with_files(
        &head,
        &workspace_manifest.files,
        &workspace_manifest.files,
        true,
    )?;
    repo.save_workspace_manifest(&workspace_manifest)?;
    if mode == CloneMode::All {
        let _ = hydrate_with_remote(destination, &[])?;
    }
    Ok(())
}

fn fetch_missing_objects_for_commit(
    repo_root: &Path,
    transport: &GrpcRemoteClient,
    commit_id: &str,
    paths: &[PathBuf],
    include: Option<&[String]>,
    exclude: Option<&[String]>,
    default_only: bool,
) -> Result<()> {
    let include = include.map(|v| v.to_vec()).unwrap_or_default();
    let exclude = exclude.map(|v| v.to_vec()).unwrap_or_default();
    let manifest = transport.get_manifest(commit_id, &include, &exclude, default_only)?;
    let files = manifest
        .into_iter()
        .map(commit_file_entry_from_proto)
        .collect::<Vec<_>>();
    fetch_missing_objects_for_files(repo_root, transport, commit_id, &files, paths, default_only)
}

fn fetch_missing_objects_for_files(
    repo_root: &Path,
    transport: &GrpcRemoteClient,
    commit_id: &str,
    files: &[CommitFileEntry],
    paths: &[PathBuf],
    default_only: bool,
) -> Result<()> {
    let config = load_repo_config(repo_root)?;
    let object_store = local_object_store(repo_root)?;
    if trace::enabled() {
        trace::emit_fetch_manifest(commit_id, files.len(), default_only, paths.len());
    }

    let mut wanted = Vec::new();
    for file in files {
        if default_only
            && !jet_core::engine::should_auto_hydrate_with_patterns(
                &file.path,
                file.size,
                &config.workspace.hot_paths,
                config.workspace.max_hot_file_bytes,
            )
        {
            continue;
        }
        if !default_only && !path_matches(paths, &file.path) {
            continue;
        }
        for chunk in &file.chunks {
            if !local_chunk_exists(&object_store, &chunk.id) {
                wanted.push(chunk.id.clone());
            }
        }
    }

    if wanted.is_empty() {
        if trace::enabled() {
            trace::emit_fetch_chunks_wanted(0);
        }
        return Ok(());
    }

    wanted.sort();
    wanted.dedup();
    if trace::enabled() {
        trace::emit_fetch_chunks_wanted(wanted.len());
    }
    let chunks = transport.get_chunks(&wanted).map_err(|err| match err {
        JetError::ObjectNotFound(object_id) => JetError::RemoteObjectMissing {
            commit_id: commit_id.to_string(),
            object_id,
        },
        other => other,
    })?;
    if trace::enabled() {
        trace::emit_fetch_chunks_returned(chunks.len());
    }
    if chunks.len() != wanted.len() {
        let returned = chunks
            .iter()
            .map(|chunk| chunk.id.as_str())
            .collect::<HashSet<_>>();
        if let Some(id) = wanted.iter().find(|id| !returned.contains(id.as_str())) {
            return Err(JetError::RemoteObjectMissing {
                commit_id: commit_id.to_string(),
                object_id: id.clone(),
            });
        }
    }

    let batch = chunks
        .iter()
        .map(|chunk| (chunk.id.as_str(), chunk.data.as_slice()))
        .collect::<Vec<_>>();
    object_store.ensure_chunks_with_ids(&batch)?;
    for chunk in chunks {
        if !object_store.has_chunk_id(&chunk.id)? {
            object_store.ensure_chunk_with_id(&chunk.id, &chunk.data)?;
        }
    }
    Ok(())
}

fn requested_path_include_patterns(paths: &[PathBuf]) -> Vec<String> {
    let mut patterns = Vec::new();
    for path in paths {
        let normalized = path.to_string_lossy().replace('\\', "/");
        let normalized = normalized.trim_end_matches('/').trim_start_matches("./");
        if normalized.is_empty() || normalized == "." {
            continue;
        }
        patterns.push(normalized.to_string());
        patterns.push(format!("{normalized}/..."));
    }
    patterns.sort();
    patterns.dedup();
    patterns
}

fn local_object_store(repo_root: &Path) -> Result<FsObjectStore> {
    let config = load_repo_config(repo_root)?;
    FsObjectStore::new(
        repo_root,
        config.compression.enabled,
        config.compression.level,
    )
}

fn load_remote_workspace_manifest(
    transport: &GrpcRemoteClient,
    commit_id: &str,
    include: &[String],
    exclude: &[String],
) -> Result<WorkspaceManifest> {
    let files = transport
        .get_manifest(commit_id, include, exclude, false)?
        .into_iter()
        .map(commit_file_entry_from_proto)
        .collect();
    Ok(WorkspaceManifest {
        schema_version: 1,
        commit_id: Some(commit_id.to_string()),
        files,
    })
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

fn local_chunk_exists(object_store: &FsObjectStore, id: &str) -> bool {
    object_store.has_chunk_id(id).unwrap_or(false)
}

fn ensure_local_commit_metadata(
    commit_store: &FsCommitStore,
    transport: &GrpcRemoteClient,
    commit_id: &str,
) -> Result<Commit> {
    match commit_store.read_commit(commit_id) {
        Ok(commit) => Ok(commit),
        Err(JetError::ObjectNotFound(_)) => {
            let commit = transport
                .get_commit_metadata(commit_id)?
                .ok_or_else(|| JetError::RemoteCommitNotFound {
                    commit_id: commit_id.to_string(),
                })
                .map(commit_from_proto)?;
            commit_store.write_commit(&commit)?;
            Ok(commit)
        }
        Err(err) => Err(err),
    }
}

fn collect_local_commits_until(
    commit_store: &FsCommitStore,
    start: &str,
    stop: Option<&str>,
) -> Result<Vec<Commit>> {
    let mut out = Vec::new();
    let mut current = Some(start.to_string());
    while let Some(id) = current {
        if stop == Some(id.as_str()) {
            break;
        }
        let commit = commit_store.read_commit(&id)?;
        current = commit.parent.clone();
        out.push(commit);
    }
    Ok(out)
}

fn collect_remote_commits_until(
    transport: &GrpcRemoteClient,
    start: &str,
    stop: Option<&str>,
    max_commits: Option<usize>,
) -> Result<Vec<Commit>> {
    let mut out = Vec::new();
    let mut current = Some(start.to_string());
    while let Some(id) = current {
        if max_commits.is_some_and(|limit| out.len() >= limit) {
            break;
        }
        if stop == Some(id.as_str()) {
            break;
        }
        let commit = transport
            .get_commit_metadata(&id)?
            .ok_or_else(|| JetError::RemoteCommitNotFound {
                commit_id: id.clone(),
            })
            .map(commit_from_proto)?;
        current = commit.parent.clone();
        out.push(commit);
    }
    Ok(out)
}

fn is_local_ancestor(
    commit_store: &FsCommitStore,
    ancestor: &str,
    descendant: &str,
) -> Result<bool> {
    if ancestor == descendant {
        return Ok(true);
    }
    let mut current = Some(descendant.to_string());
    while let Some(id) = current {
        if id == ancestor {
            return Ok(true);
        }
        current = commit_store.read_commit(&id)?.parent;
    }
    Ok(false)
}

fn is_remote_ancestor(
    transport: &GrpcRemoteClient,
    ancestor: &str,
    descendant: &str,
) -> Result<bool> {
    if ancestor == descendant {
        return Ok(true);
    }
    let mut current = Some(descendant.to_string());
    while let Some(id) = current {
        if id == ancestor {
            return Ok(true);
        }
        let parent = transport
            .get_commit_metadata(&id)?
            .ok_or_else(|| JetError::RemoteCommitNotFound {
                commit_id: id.clone(),
            })?
            .parent;
        current = if parent.is_empty() {
            None
        } else {
            Some(parent)
        };
    }
    Ok(false)
}

fn path_matches(paths: &[PathBuf], file_path: &str) -> bool {
    if paths.is_empty() {
        return true;
    }
    paths.iter().any(|path| {
        let path = path.to_string_lossy().replace('\\', "/");
        file_path == path
            || file_path
                .strip_prefix(path.as_str())
                .is_some_and(|rest| rest.starts_with('/'))
    })
}

fn remote_transport_error(err: impl std::fmt::Display) -> JetError {
    JetError::RemoteTransport {
        message: err.to_string(),
    }
}

fn status_from_transport_error(err: tonic::transport::Error) -> JetError {
    remote_transport_error(err)
}

fn map_transport_status_error(err: tonic::Status) -> JetError {
    match err.code() {
        tonic::Code::Unauthenticated | tonic::Code::PermissionDenied => JetError::RemoteUnauthorized,
        _ => JetError::RemoteTransport {
            message: format_status_message(&err),
        },
    }
}

fn format_status_message(status: &tonic::Status) -> String {
    let message = status.message().trim();
    match status.code() {
        tonic::Code::NotFound => {
            if let Some(path) = message.strip_prefix("invalid repository: ") {
                let repo = Path::new(path)
                    .file_name()
                    .and_then(|name| name.to_str())
                    .unwrap_or(path);
                format!("remote repository not found: {repo}")
            } else if message.is_empty() {
                "remote resource not found".to_string()
            } else {
                message.to_string()
            }
        }
        _ if message.is_empty() => status.code().to_string(),
        _ => message.to_string(),
    }
}

fn map_lock_transport_error(err: tonic::Status) -> JetError {
    let message = err.message().to_string();
    if let Some(rest) = message.strip_prefix("lock_conflict:") {
        let mut parts = rest.splitn(2, ':');
        if let (Some(path), Some(owner)) = (parts.next(), parts.next()) {
            return JetError::LockConflict {
                path: path.to_string(),
                owner: owner.to_string(),
            };
        }
    }
    if let Some(rest) = message.strip_prefix("lock_owner_mismatch:") {
        let mut parts = rest.splitn(2, ':');
        if let (Some(path), Some(owner)) = (parts.next(), parts.next()) {
            return JetError::LockOwnershipMismatch {
                path: path.to_string(),
                owner: owner.to_string(),
            };
        }
    }
    map_transport_status_error(err)
}

#[derive(Debug, Default, Deserialize, Serialize)]
struct GlobalCredentials {
    #[serde(default)]
    default_token: Option<String>,
    #[serde(default)]
    servers: Vec<ServerCredential>,
}

#[derive(Debug, Deserialize, Serialize)]
struct ServerCredential {
    endpoint: String,
    token: String,
}

fn resolve_auth_token(source: Option<&str>, repo_root: Option<&Path>) -> Option<String> {
    std::env::var("JET_TOKEN")
        .ok()
        .map(|token| token.trim().to_string())
        .filter(|token| !token.is_empty())
        .or_else(|| repo_root.and_then(load_repo_auth_token))
        .or_else(|| source.and_then(load_global_auth_token))
}

fn load_repo_auth_token(repo_root: &Path) -> Option<String> {
    let path = repo_root.join(".jet").join("credentials");
    let data = fs::read_to_string(path).ok()?;
    parse_credentials_token(&data)
}

fn parse_credentials_token(data: &str) -> Option<String> {
    for line in data.lines() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        if let Some((key, value)) = line.split_once('=') {
            if key.trim() == "token" {
                let token = value.trim().trim_matches('"').trim_matches('\'');
                if !token.is_empty() {
                    return Some(token.to_string());
                }
            }
            continue;
        }
        return Some(line.to_string());
    }
    None
}

fn load_global_auth_token(source: &str) -> Option<String> {
    let endpoint = parse_remote_location(source).ok()?.endpoint;
    let path = user_credentials_path()?;
    let data = fs::read_to_string(path).ok()?;
    let credentials: GlobalCredentials = toml::from_str(&data).ok()?;
    credentials
        .servers
        .into_iter()
        .find(|server| server.endpoint == endpoint && !server.token.trim().is_empty())
        .map(|server| server.token)
        .or(credentials.default_token)
}

fn save_global_auth_token(endpoint: &str, token: &str) -> Result<()> {
    let Some(path) = user_credentials_path() else {
        return Err(JetError::RemoteTransport {
            message: "unable to resolve user credentials path".to_string(),
        });
    };
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }

    let mut credentials = if path.exists() {
        let data = fs::read_to_string(&path)?;
        toml::from_str::<GlobalCredentials>(&data).unwrap_or_default()
    } else {
        GlobalCredentials::default()
    };

    let token = token.trim().to_string();
    if token.is_empty() {
        return Err(JetError::RemoteTransport {
            message: "token must not be empty".to_string(),
        });
    }

    if let Some(existing) = credentials
        .servers
        .iter_mut()
        .find(|server| server.endpoint == endpoint)
    {
        existing.token = token;
    } else {
        credentials.servers.push(ServerCredential {
            endpoint: endpoint.to_string(),
            token,
        });
    }

    let data = toml::to_string_pretty(&credentials).map_err(remote_transport_error)?;
    fs::write(path, data)?;
    Ok(())
}

fn user_credentials_path() -> Option<PathBuf> {
    if let Ok(dir) = std::env::var("JET_CONFIG_DIR") {
        let dir = dir.trim();
        if !dir.is_empty() {
            return Some(PathBuf::from(dir).join("credentials.toml"));
        }
    }
    let home = std::env::var("HOME").ok()?;
    let home = home.trim();
    if home.is_empty() {
        return None;
    }
    Some(
        PathBuf::from(home)
            .join(".config")
            .join("jet")
            .join("credentials.toml"),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn formats_invalid_repository_status_cleanly() {
        let status = tonic::Status::not_found("invalid repository: ./jet-dir");
        assert_eq!(format_status_message(&status), "remote repository not found: jet-dir");
    }
}

pub fn repo_config_to_proto(config: &RepoConfig) -> proto::RepoConfig {
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

pub fn repo_config_from_proto(proto: proto::RepoConfig) -> RepoConfig {
    RepoConfig {
        schema_version: proto.schema_version,
        repo_id: proto.repo_id,
        chunking: proto
            .chunking
            .map(|chunking| jet_core::repo::ChunkingConfig {
                min: chunking.min,
                avg: chunking.avg,
                max: chunking.max,
            })
            .unwrap_or(jet_core::repo::ChunkingConfig {
                min: 0,
                avg: 0,
                max: 0,
            }),
        compression: proto
            .compression
            .map(|compression| jet_core::repo::CompressionConfig {
                enabled: compression.enabled,
                level: compression.level,
            })
            .unwrap_or(jet_core::repo::CompressionConfig {
                enabled: false,
                level: 0,
            }),
        storage: proto
            .storage
            .map(|storage| jet_core::repo::StorageConfig {
                direct_blob_threshold_bytes: storage.direct_blob_threshold_bytes,
            })
            .unwrap_or(jet_core::repo::StorageConfig {
                direct_blob_threshold_bytes: 0,
            }),
        workspace: proto
            .workspace
            .map(|workspace| jet_core::repo::WorkspaceConfig {
                hot_paths: workspace.hot_paths,
                max_hot_file_bytes: workspace.max_hot_file_bytes,
            })
            .unwrap_or(jet_core::repo::WorkspaceConfig {
                hot_paths: vec!["code/...".to_string(), "config/...".to_string()],
                max_hot_file_bytes: 256 * 1024,
            }),
    }
}

pub fn commit_to_proto(commit: &Commit) -> proto::Commit {
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

fn commit_file_entry_from_proto(
    proto: proto::CommitFileEntry,
) -> jet_core::commit_store::CommitFileEntry {
    jet_core::commit_store::CommitFileEntry {
        path: proto.path,
        size: proto.size,
        file_digest: proto.file_digest,
        chunks: proto
            .chunks
            .into_iter()
            .map(|chunk| jet_core::commit_store::CommitChunkRef {
                id: chunk.id,
                offset: chunk.offset,
                len: chunk.len,
                raw_len: chunk.raw_len,
            })
            .collect(),
    }
}

pub fn commit_from_proto(proto: proto::Commit) -> Commit {
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

fn lock_info_from_proto(proto: proto::LockInfo) -> LockInfo {
    LockInfo {
        path: proto.path,
        owner: proto.owner,
    }
}
