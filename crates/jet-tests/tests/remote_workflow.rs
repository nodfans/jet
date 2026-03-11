use std::path::Path;

use jet_core::JetRepository;
use jet_core::repo::init_repo;
use jet_remote::{
    CloneMode, clone_from_source, list_remote_locks, lock_remote_path, pull_from_remote, push_to_remote,
    unlock_remote_path,
};
use tempfile::tempdir;

mod common;

use common::{spawn_server, write_file};

#[tokio::test]
async fn remote_workflow_stays_usable() {
    let temp = tempdir().expect("tempdir");
    let repos_root = temp.path().join("repos");
    let source_root = repos_root.join("game");
    let clone_root = temp.path().join("clone");

    init_repo(&source_root).expect("init source");
    write_file(&source_root, "code/main.rs", b"fn main() {}\n");
    write_file(&source_root, "config/app.toml", b"name = \"jet\"\n");

    let source_repo = JetRepository::open(&source_root).expect("open source repo");
    source_repo
        .add_paths(&[Path::new(".").to_path_buf()])
        .expect("add source");
    let _first = source_repo.commit("initial", "tester").expect("commit1");

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

    write_file(
        &source_root,
        "code/main.rs",
        b"fn main() { println!(\"v2\"); }\n",
    );
    source_repo
        .add_paths(&[Path::new(".").to_path_buf()])
        .expect("add source2");
    let second = source_repo.commit("second", "tester").expect("commit2");

    let clone_root_for_pull = clone_root.clone();
    let pull = tokio::task::spawn_blocking(move || pull_from_remote(&clone_root_for_pull, None))
        .await
        .expect("join pull")
        .expect("pull");
    assert_eq!(pull.new_head, second);

    let cloned_repo = JetRepository::open(&clone_root).expect("open clone repo");
    write_file(&clone_root, "code/local.rs", b"pub fn local() {}\n");
    cloned_repo
        .add_paths(&[Path::new("code/local.rs").to_path_buf()])
        .expect("add local");
    let clone_root_for_push = clone_root.clone();
    let remote_for_push = remote.clone();
    let push =
        tokio::task::spawn_blocking(move || push_to_remote(&clone_root_for_push, &remote_for_push))
            .await
            .expect("join push")
            .expect("push");
    assert!(!push.new_head.is_empty());

    let remote_for_lock = remote.clone();
    let lock = tokio::task::spawn_blocking(move || {
        lock_remote_path(&remote_for_lock, "assets/hero/model.fbx", "alice")
    })
    .await
    .expect("join lock")
    .expect("lock");
    assert_eq!(lock.owner, "alice");
    let remote_for_locks = remote.clone();
    let locks =
        tokio::task::spawn_blocking(move || list_remote_locks(&remote_for_locks, Some("assets")))
            .await
            .expect("join locks")
            .expect("locks");
    assert_eq!(locks.len(), 1);
    let remote_for_unlock = remote.clone();
    tokio::task::spawn_blocking(move || {
        unlock_remote_path(&remote_for_unlock, "assets/hero/model.fbx", "alice")
    })
    .await
    .expect("join unlock")
    .expect("unlock");

    shutdown.send(()).expect("shutdown");
}
