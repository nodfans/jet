use std::path::PathBuf;

use thiserror::Error;

#[derive(Debug, Error)]
pub enum JetError {
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),

    #[error("serialization error: {0}")]
    Serde(#[from] serde_json::Error),

    #[error("toml error: {0}")]
    Toml(#[from] toml::de::Error),

    #[error("toml serialization error: {0}")]
    TomlSerialize(#[from] toml::ser::Error),

    #[error("binary serialization error: {0}")]
    BinarySerde(#[from] Box<bincode::ErrorKind>),

    #[error("object not found: {0}")]
    ObjectNotFound(String),

    #[error("invalid repository: {path}")]
    InvalidRepository { path: PathBuf },

    #[error("invalid UTF-8 path")]
    InvalidUtf8Path,

    #[error("staging area is empty")]
    EmptyStaging,

    #[error("system time is before UNIX_EPOCH")]
    InvalidSystemTime,

    #[error("digest mismatch for file: {path}")]
    DigestMismatch { path: String },

    #[error("workspace has no checked out commit")]
    NoWorkspaceCommit,

    #[error("workspace has no remote source configured")]
    NoRemoteConfigured,

    #[error("commit metadata only: {commit_id}")]
    CommitMetadataOnly { commit_id: String },

    #[error("remote commit not found: {commit_id}")]
    RemoteCommitNotFound { commit_id: String },

    #[error("remote object not found for commit {commit_id}: {object_id}")]
    RemoteObjectMissing {
        commit_id: String,
        object_id: String,
    },

    #[error("workspace contains dirty file: {path}")]
    DirtyWorkspaceFile { path: String },

    #[error("clone destination is not empty: {path}")]
    CloneDestinationNotEmpty { path: PathBuf },

    #[error("remote backend is not implemented for source: {remote}")]
    RemoteBackendNotImplemented { remote: String },

    #[error("remote transport error: {message}")]
    RemoteTransport { message: String },

    #[error("remote authentication failed")]
    RemoteUnauthorized,

    #[error("remote push rejected: remote head is {remote_head}")]
    RemotePushRejected { remote_head: String },

    #[error("remote pull rejected: remote head is not a descendant of local head")]
    RemotePullRejected,

    #[error("path is already locked by {owner}: {path}")]
    LockConflict { path: String, owner: String },

    #[error("path is not locked by {owner}: {path}")]
    LockOwnershipMismatch { path: String, owner: String },
}

pub type Result<T> = std::result::Result<T, JetError>;
