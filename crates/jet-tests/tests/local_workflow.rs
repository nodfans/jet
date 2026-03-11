use std::path::Path;

use jet_core::repo::init_repo;
use jet_core::{FsckMode, JetRepository};
use tempfile::tempdir;

mod common;

use common::write_file;

#[test]
fn local_workflow_stays_usable() {
    let dir = tempdir().expect("tempdir");
    init_repo(dir.path()).expect("init repo");

    write_file(dir.path(), "code/main.rs", b"fn main() {}\n");
    write_file(dir.path(), "config/app.toml", b"name = \"jet\"\n");
    write_file(dir.path(), "assets/hero/readme.txt", b"hello\n");

    let repo = JetRepository::open(dir.path()).expect("open repo");
    repo.add_paths(&[Path::new(".").to_path_buf()])
        .expect("add");
    let first = repo.commit("initial", "tester").expect("commit1");

    write_file(
        dir.path(),
        "code/main.rs",
        b"fn main() { println!(\"v2\"); }\n",
    );
    repo.add_paths(&[Path::new(".").to_path_buf()])
        .expect("add2");
    let second = repo.commit("second", "tester").expect("commit2");

    repo.checkout(&first).expect("open first");
    repo.hydrate(&[]).expect("hydrate");

    let status = repo.workspace_status().expect("status");
    assert_eq!(status.current_commit_id.as_deref(), Some(first.as_str()));
    assert!(status.hydrated_count >= 1);

    repo.dehydrate(&[]).expect("dehydrate");
    repo.checkout(&second).expect("open second");
    repo.fsck_with_mode(FsckMode::Quick).expect("quick fsck");
}
