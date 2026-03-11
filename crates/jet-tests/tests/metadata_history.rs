use std::fs;
use std::path::{Path, PathBuf};

use jet_core::JetRepository;
use jet_core::commit_store::{CommitStore, FsCommitStore};
use jet_core::repo::init_repo;
use jet_remote::{CloneMode, clone_from_source, open_with_remote};
use tempfile::tempdir;

mod common;

use common::{spawn_server, write_file};

#[tokio::test]
async fn remote_clone_keeps_history_metadata_light_and_can_open_older_commit() {
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
    let first = source_repo.commit("initial", "tester").expect("commit1");

    write_file(
        &source_root,
        "code/main.rs",
        b"fn main() { println!(\"updated\"); }\n",
    );
    source_repo
        .add_paths(&[PathBuf::from(".")])
        .expect("add source2");
    let second = source_repo.commit("second", "tester").expect("commit2");

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

    let store = FsCommitStore::new(&clone_root).expect("clone store");
    let head = store.read_head().expect("head read").expect("head");
    assert_eq!(head, second);

    let head_commit = store.read_commit(&head).expect("head commit");
    assert!(head_commit.files_omitted);
    assert!(store.read_commit(&first).is_err());

    let clone_root_for_open = clone_root.clone();
    let first_for_open = first.clone();
    tokio::task::spawn_blocking(move || open_with_remote(&clone_root_for_open, &first_for_open))
        .await
        .expect("join open")
        .expect("open old");

    let store = FsCommitStore::new(&clone_root).expect("clone store");
    let older = store.read_commit(&first).expect("older commit");
    assert!(older.files_omitted);
    let current = JetRepository::open(&clone_root)
        .expect("open clone")
        .workspace_status()
        .expect("status");
    assert_eq!(current.current_commit_id, Some(first));
    assert_eq!(
        fs::read_to_string(clone_root.join("code").join("main.rs")).expect("read code"),
        "fn main() {}\n"
    );

    shutdown.send(()).expect("shutdown");
}
