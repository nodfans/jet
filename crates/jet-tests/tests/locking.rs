use std::collections::BTreeSet;
use std::path::Path;

use jet_core::JetError;
use jet_core::JetRepository;
use jet_core::repo::init_repo;
use jet_remote::{
    CloneMode, GrpcRemoteClient, RemoteLocation, clone_from_source, list_remote_locks, lock_remote_path,
    unlock_remote_path,
};
use jet_server::{AuthConfig, RepoPermissions};
use tempfile::tempdir;

mod common;

use common::{spawn_server, spawn_server_with_auth, write_file};

#[tokio::test]
async fn remote_locking_roundtrip_stays_usable() {
    let temp = tempdir().expect("tempdir");
    let repos_root = temp.path().join("repos");
    let source_root = repos_root.join("game");
    let clone_root = temp.path().join("clone");

    init_repo(&source_root).expect("init source");
    write_file(&source_root, "code/main.rs", b"fn main() {}\n");

    let source_repo = JetRepository::open(&source_root).expect("open source repo");
    source_repo
        .add_paths(&[Path::new(".").to_path_buf()])
        .expect("add source");
    source_repo.commit("initial", "tester").expect("commit1");

    let (addr, shutdown) = spawn_server(repos_root.clone()).await;
    let remote = format!("http://{addr}/game");

    let remote_for_clone = remote.clone();
    let clone_root_for_clone = clone_root.clone();
    tokio::task::spawn_blocking(move || {
        clone_from_source(&remote_for_clone, &clone_root_for_clone, CloneMode::All)
    })
    .await
    .expect("join clone")
    .expect("clone");

    let remote_for_lock = remote.clone();
    let lock = tokio::task::spawn_blocking(move || {
        lock_remote_path(&remote_for_lock, "assets/hero/model.fbx", "alice")
    })
    .await
    .expect("join lock")
    .expect("lock");
    assert_eq!(lock.path, "assets/hero/model.fbx");
    assert_eq!(lock.owner, "alice");

    let remote_for_list = remote.clone();
    let locks =
        tokio::task::spawn_blocking(move || list_remote_locks(&remote_for_list, Some("assets")))
            .await
            .expect("join locks")
            .expect("locks");
    assert_eq!(locks.len(), 1);
    assert_eq!(locks[0].path, "assets/hero/model.fbx");

    let remote_for_unlock = remote.clone();
    tokio::task::spawn_blocking(move || {
        unlock_remote_path(&remote_for_unlock, "assets/hero/model.fbx", "alice")
    })
    .await
    .expect("join unlock")
    .expect("unlock");

    let remote_for_list_empty = remote.clone();
    let locks = tokio::task::spawn_blocking(move || {
        list_remote_locks(&remote_for_list_empty, Some("assets"))
    })
    .await
    .expect("join locks empty")
    .expect("locks empty");
    assert!(locks.is_empty());

    shutdown.send(()).expect("shutdown");
}

#[tokio::test]
async fn remote_lock_conflicts_and_wrong_owner_unlock_are_rejected() {
    let temp = tempdir().expect("tempdir");
    let repos_root = temp.path().join("repos");
    let source_root = repos_root.join("game");

    init_repo(&source_root).expect("init source");
    write_file(&source_root, "code/main.rs", b"fn main() {}\n");

    let source_repo = JetRepository::open(&source_root).expect("open source repo");
    source_repo
        .add_paths(&[Path::new(".").to_path_buf()])
        .expect("add source");
    source_repo.commit("initial", "tester").expect("commit1");

    let (addr, shutdown) = spawn_server(repos_root.clone()).await;
    let remote = format!("http://{addr}/game");

    let remote_for_alice = remote.clone();
    tokio::task::spawn_blocking(move || {
        lock_remote_path(&remote_for_alice, "assets/hero/model.fbx", "alice")
    })
    .await
    .expect("join alice lock")
    .expect("alice lock");

    let remote_for_bob_lock = remote.clone();
    let conflict = tokio::task::spawn_blocking(move || {
        lock_remote_path(&remote_for_bob_lock, "assets/hero/model.fbx", "bob")
    })
    .await
    .expect("join bob lock")
    .expect_err("bob lock should fail");
    assert!(matches!(
        conflict,
        JetError::LockConflict { ref path, ref owner }
            if path == "assets/hero/model.fbx" && owner == "alice"
    ));

    let remote_for_bob_unlock = remote.clone();
    let wrong_unlock = tokio::task::spawn_blocking(move || {
        unlock_remote_path(&remote_for_bob_unlock, "assets/hero/model.fbx", "bob")
    })
    .await
    .expect("join bob unlock")
    .expect_err("bob unlock should fail");
    assert!(matches!(
        wrong_unlock,
        JetError::LockOwnershipMismatch { ref path, ref owner }
            if path == "assets/hero/model.fbx" && owner == "alice"
    ));

    shutdown.send(()).expect("shutdown");
}

#[tokio::test]
async fn remote_locking_uses_authenticated_identity() {
    let temp = tempdir().expect("tempdir");
    let repos_root = temp.path().join("repos");
    let source_root = repos_root.join("game");

    init_repo(&source_root).expect("init source");
    write_file(&source_root, "code/main.rs", b"fn main() {}\n");

    let source_repo = JetRepository::open(&source_root).expect("open source repo");
    source_repo
        .add_paths(&[Path::new(".").to_path_buf()])
        .expect("add source");
    source_repo.commit("initial", "tester").expect("commit1");

    let auth = AuthConfig::with_repo_permissions(
        [("alice".to_string(), "secret-token".to_string())],
        [(
            "game".to_string(),
            RepoPermissions {
                read: BTreeSet::new(),
                write: BTreeSet::from(["alice".to_string()]),
                admin: BTreeSet::new(),
            },
        )],
    );
    let (addr, shutdown) = spawn_server_with_auth(repos_root.clone(), auth).await;
    let location = RemoteLocation {
        endpoint: format!("http://{addr}"),
        repo: "game".to_string(),
    };
    let client = GrpcRemoteClient::with_auth_token(location, Some("secret-token".to_string()))
        .expect("client");

    let lock = tokio::task::spawn_blocking(move || {
        client.lock_path("assets/hero/model.fbx", "spoofed-owner")
    })
    .await
    .expect("join lock")
    .expect("lock");
    assert_eq!(lock.owner, "alice");

    shutdown.send(()).expect("shutdown");
}
