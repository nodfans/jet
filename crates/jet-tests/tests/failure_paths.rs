use std::fs;
use std::collections::BTreeSet;
use std::path::PathBuf;

use jet_core::commit_store::{CommitStore, FsCommitStore};
use jet_core::object_store::{FsObjectStore, ObjectStore};
use jet_core::repo::load_repo_config;
use jet_core::repo::init_repo;
use jet_core::{JetError, JetRepository};
use jet_remote::{
    CloneMode, GrpcRemoteClient, RemoteLocation, clone_from_source, commit_to_proto, hydrate_with_remote,
    login_with_token, open_with_remote, pull_from_remote, push_to_remote, remote_whoami,
};
use jet_server::{AuthConfig, RepoPermissions};
use tempfile::tempdir;

mod common;

use common::{spawn_server, spawn_server_with_auth, write_file};

#[tokio::test]
async fn remote_failures_keep_workspace_consistent() {
    let temp = tempdir().expect("tempdir");
    let repos_root = temp.path().join("repos");
    let source_root = repos_root.join("game");
    let clone_root = temp.path().join("clone");

    init_repo(&source_root).expect("init source");
    write_file(&source_root, "code/main.rs", b"fn main() {}\n");
    write_file(&source_root, "assets/cold.bin", &[7_u8; 8192]);

    let source_repo = JetRepository::open(&source_root).expect("open source repo");
    source_repo
        .add_paths(&[PathBuf::from(".")])
        .expect("add source");
    let first = source_repo.commit("initial", "tester").expect("commit1");

    let (addr, shutdown) = spawn_server(repos_root.clone()).await;
    let remote = format!("http://{addr}/game");

    let remote_for_clone = remote.clone();
    let clone_root_for_clone = clone_root.clone();
    tokio::task::spawn_blocking(move || {
        clone_from_source(&remote_for_clone, &clone_root_for_clone, CloneMode::Partial)
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
        .add_paths(&[PathBuf::from(".")])
        .expect("add source2");
    let second = source_repo.commit("second", "tester").expect("commit2");

    fs::remove_file(
        source_root
            .join(".jet")
            .join("segments")
            .join("00000001.seg"),
    )
    .expect("remove segment");

    let clone_before = JetRepository::open(&clone_root).expect("open clone before");
    let status_before = clone_before.workspace_status().expect("status before");

    let clone_root_for_hydrate = clone_root.clone();
    let hydrate_err = tokio::task::spawn_blocking(move || {
        hydrate_with_remote(&clone_root_for_hydrate, &[PathBuf::from("assets")])
    })
    .await
    .expect("join hydrate")
    .expect_err("hydrate should fail");
    assert!(matches!(
        hydrate_err,
        JetError::RemoteObjectMissing { commit_id, .. } if commit_id == first
    ));

    let clone_after_hydrate = JetRepository::open(&clone_root).expect("open clone after hydrate");
    let status_after_hydrate = clone_after_hydrate
        .workspace_status()
        .expect("status after hydrate");
    assert_eq!(
        status_after_hydrate.current_commit_id,
        status_before.current_commit_id
    );

    let clone_root_for_pull = clone_root.clone();
    let remote_for_pull = remote.clone();
    let pull_err = tokio::task::spawn_blocking(move || {
        pull_from_remote(&clone_root_for_pull, Some(&remote_for_pull))
    })
    .await
    .expect("join pull")
    .expect_err("pull should fail");
    assert!(matches!(
        pull_err,
        JetError::RemoteObjectMissing { commit_id, .. } if commit_id == second
    ));

    let clone_after_pull = JetRepository::open(&clone_root).expect("open clone after pull");
    let status_after_pull = clone_after_pull
        .workspace_status()
        .expect("status after pull");
    assert_eq!(
        status_after_pull.current_commit_id,
        status_before.current_commit_id
    );

    let clone_root_for_open = clone_root.clone();
    let second_for_open = second.clone();
    let open_err = tokio::task::spawn_blocking(move || {
        open_with_remote(&clone_root_for_open, &second_for_open)
    })
    .await
    .expect("join open")
    .expect_err("open should fail");
    assert!(matches!(
        open_err,
        JetError::RemoteObjectMissing { commit_id, .. } if commit_id == second
    ));

    let clone_after_open = JetRepository::open(&clone_root).expect("open clone after open");
    let status_after_open = clone_after_open
        .workspace_status()
        .expect("status after open");
    assert_eq!(
        status_after_open.current_commit_id,
        status_before.current_commit_id
    );

    shutdown.send(()).expect("shutdown");
}

#[tokio::test]
async fn concurrent_push_rejects_stale_writer() {
    let temp = tempdir().expect("tempdir");
    let repos_root = temp.path().join("repos");
    let source_root = repos_root.join("game");
    let clone_a_root = temp.path().join("clone-a");
    let clone_b_root = temp.path().join("clone-b");

    init_repo(&source_root).expect("init source");
    write_file(&source_root, "code/main.rs", b"fn main() {}\n");

    let source_repo = JetRepository::open(&source_root).expect("open source repo");
    source_repo
        .add_paths(&[PathBuf::from(".")])
        .expect("add source");
    let first = source_repo.commit("initial", "tester").expect("commit1");

    let (addr, shutdown) = spawn_server(repos_root.clone()).await;
    let remote = format!("http://{addr}/game");

    let clone_a_root_for_clone = clone_a_root.clone();
    let remote_for_clone_a = remote.clone();
    tokio::task::spawn_blocking(move || {
        clone_from_source(&remote_for_clone_a, &clone_a_root_for_clone, CloneMode::All)
    })
    .await
    .expect("join clone a")
    .expect("clone a");

    let clone_b_root_for_clone = clone_b_root.clone();
    let remote_for_clone_b = remote.clone();
    tokio::task::spawn_blocking(move || {
        clone_from_source(&remote_for_clone_b, &clone_b_root_for_clone, CloneMode::All)
    })
    .await
    .expect("join clone b")
    .expect("clone b");

    write_file(&clone_a_root, "code/alice.rs", b"pub fn alice() {}\n");
    let clone_a_repo = JetRepository::open(&clone_a_root).expect("open clone a");
    clone_a_repo
        .add_paths(&[PathBuf::from("code/alice.rs")])
        .expect("add clone a");
    let alice_head = clone_a_repo.commit("alice", "alice").expect("commit alice");

    write_file(&clone_b_root, "code/bob.rs", b"pub fn bob() {}\n");
    let clone_b_repo = JetRepository::open(&clone_b_root).expect("open clone b");
    clone_b_repo
        .add_paths(&[PathBuf::from("code/bob.rs")])
        .expect("add clone b");
    let bob_head = clone_b_repo.commit("bob", "bob").expect("commit bob");

    let clone_a_root_for_push = clone_a_root.clone();
    let remote_for_push_a = remote.clone();
    let push_a =
        tokio::task::spawn_blocking(move || push_to_remote(&clone_a_root_for_push, &remote_for_push_a))
            .await
            .expect("join push a")
            .expect("push a");
    assert_eq!(push_a.new_head, alice_head);

    let clone_b_root_for_push = clone_b_root.clone();
    let remote_for_push_b = remote.clone();
    let push_b_err =
        tokio::task::spawn_blocking(move || push_to_remote(&clone_b_root_for_push, &remote_for_push_b))
            .await
            .expect("join push b")
            .expect_err("push b should fail");
    assert!(matches!(
        push_b_err,
        JetError::RemotePushRejected { ref remote_head } if remote_head == &alice_head
    ));

    let remote_for_head = remote.clone();
    let remote_head = tokio::task::spawn_blocking(move || {
        GrpcRemoteClient::from_source(&remote_for_head)?
            .get_head()
    })
    .await
    .expect("join remote head")
    .expect("remote head");
    assert_eq!(remote_head, Some(alice_head.clone()));
    assert_ne!(alice_head, bob_head);
    assert_eq!(first.len(), alice_head.len());

    shutdown.send(()).expect("shutdown");
}

#[tokio::test]
async fn partial_push_without_head_update_stays_invisible() {
    let temp = tempdir().expect("tempdir");
    let repos_root = temp.path().join("repos");
    let source_root = repos_root.join("game");
    let writer_root = temp.path().join("writer");
    let observer_root = temp.path().join("observer");
    let fresh_clone_root = temp.path().join("fresh-clone");

    init_repo(&source_root).expect("init source");
    write_file(&source_root, "code/main.rs", b"fn main() {}\n");

    let source_repo = JetRepository::open(&source_root).expect("open source repo");
    source_repo
        .add_paths(&[PathBuf::from(".")])
        .expect("add source");
    let first = source_repo.commit("initial", "tester").expect("commit1");

    let (addr, shutdown) = spawn_server(repos_root.clone()).await;
    let remote = format!("http://{addr}/game");

    let writer_root_for_clone = writer_root.clone();
    let remote_for_writer = remote.clone();
    tokio::task::spawn_blocking(move || {
        clone_from_source(&remote_for_writer, &writer_root_for_clone, CloneMode::All)
    })
    .await
    .expect("join writer clone")
    .expect("clone writer");

    let observer_root_for_clone = observer_root.clone();
    let remote_for_observer = remote.clone();
    tokio::task::spawn_blocking(move || {
        clone_from_source(&remote_for_observer, &observer_root_for_clone, CloneMode::All)
    })
    .await
    .expect("join observer clone")
    .expect("clone observer");

    write_file(&writer_root, "code/partial.rs", b"pub fn partial() {}\n");
    let writer_repo = JetRepository::open(&writer_root).expect("open writer repo");
    writer_repo
        .add_paths(&[PathBuf::from(".")])
        .expect("add writer");
    let second = writer_repo.commit("second", "tester").expect("commit2");

    let commit_store = FsCommitStore::new(&writer_root).expect("commit store");
    let second_commit = commit_store.read_commit(&second).expect("read second");
    let config = load_repo_config(&writer_root).expect("config");
    let object_store = FsObjectStore::new(
        &writer_root,
        config.compression.enabled,
        config.compression.level,
    )
    .expect("object store");
    let chunk_ids = second_commit
        .files
        .iter()
        .flat_map(|file| file.chunks.iter().map(|chunk| chunk.id.clone()))
        .collect::<std::collections::HashSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();
    let chunks = chunk_ids
        .iter()
        .map(|id| {
            Ok(jet_proto::proto::ChunkData {
                id: id.clone(),
                data: object_store.get_chunk(id)?,
            })
        })
        .collect::<Result<Vec<_>, JetError>>()
        .expect("chunks");

    let remote_for_partial_push = remote.clone();
    let second_commit_for_partial_push = second_commit.clone();
    tokio::task::spawn_blocking(move || -> Result<(), JetError> {
        let remote_client = GrpcRemoteClient::from_source(&remote_for_partial_push)?;
        remote_client.put_chunks(chunks)?;
        remote_client.put_commit(commit_to_proto(&second_commit_for_partial_push))?;
        Ok(())
    })
    .await
    .expect("join partial push")
    .expect("partial push");

    let remote_for_visibility = remote.clone();
    let second_for_visibility = second.clone();
    let (remote_head, visible_commit_id) =
        tokio::task::spawn_blocking(move || -> Result<(Option<String>, Option<String>), JetError> {
            let remote_client = GrpcRemoteClient::from_source(&remote_for_visibility)?;
            let head = remote_client.get_head()?;
            let commit_id = remote_client.get_commit(&second_for_visibility)?.map(|commit| commit.id);
            Ok((head, commit_id))
        })
        .await
        .expect("join visibility check")
        .expect("visibility check");
    assert_eq!(remote_head, Some(first.clone()));
    assert_eq!(visible_commit_id, Some(second.clone()));

    let observer_root_for_pull = observer_root.clone();
    let remote_for_pull = remote.clone();
    let pull =
        tokio::task::spawn_blocking(move || pull_from_remote(&observer_root_for_pull, Some(&remote_for_pull)))
            .await
            .expect("join pull")
            .expect("pull");
    assert_eq!(pull.new_head, first);

    let fresh_clone_root_for_clone = fresh_clone_root.clone();
    let remote_for_fresh = remote.clone();
    tokio::task::spawn_blocking(move || {
        clone_from_source(&remote_for_fresh, &fresh_clone_root_for_clone, CloneMode::All)
    })
    .await
    .expect("join fresh clone")
    .expect("fresh clone");
    let fresh_repo = JetRepository::open(&fresh_clone_root).expect("open fresh clone");
    assert_eq!(fresh_repo.head_commit_id().expect("fresh head"), Some(first));

    shutdown.send(()).expect("shutdown");
}

#[tokio::test]
async fn divergent_remote_pull_is_rejected_and_workspace_stays_put() {
    let temp = tempdir().expect("tempdir");
    let repos_root = temp.path().join("repos");
    let source_root = repos_root.join("game");
    let clone_root = temp.path().join("clone");

    init_repo(&source_root).expect("init source");
    write_file(&source_root, "code/main.rs", b"fn main() {}\n");

    let source_repo = JetRepository::open(&source_root).expect("open source repo");
    source_repo
        .add_paths(&[PathBuf::from(".")])
        .expect("add source");
    let first = source_repo.commit("initial", "tester").expect("commit1");

    let (addr, shutdown) = spawn_server(repos_root.clone()).await;
    let remote = format!("http://{addr}/game");

    let clone_root_for_clone = clone_root.clone();
    let remote_for_clone = remote.clone();
    tokio::task::spawn_blocking(move || {
        clone_from_source(&remote_for_clone, &clone_root_for_clone, CloneMode::All)
    })
    .await
    .expect("join clone")
    .expect("clone");

    write_file(
        &source_root,
        "code/main.rs",
        b"fn main() { println!(\"remote\"); }\n",
    );
    source_repo
        .add_paths(&[PathBuf::from(".")])
        .expect("add remote");
    let remote_second = source_repo.commit("remote", "tester").expect("commit remote");

    let clone_repo = JetRepository::open(&clone_root).expect("open clone repo");
    write_file(&clone_root, "code/local.rs", b"pub fn local() {}\n");
    clone_repo
        .add_paths(&[PathBuf::from("code/local.rs")])
        .expect("add local");
    let local_second = clone_repo.commit("local", "tester").expect("commit local");
    assert_ne!(remote_second, local_second);

    let status_before = clone_repo.workspace_status().expect("status before");

    let clone_root_for_pull = clone_root.clone();
    let remote_for_pull = remote.clone();
    let pull_err = tokio::task::spawn_blocking(move || {
        pull_from_remote(&clone_root_for_pull, Some(&remote_for_pull))
    })
    .await
    .expect("join pull")
    .expect_err("pull should reject divergence");
    assert!(matches!(pull_err, JetError::RemotePullRejected));

    let clone_after = JetRepository::open(&clone_root).expect("open clone after");
    let status_after = clone_after.workspace_status().expect("status after");
    assert_eq!(status_after.current_commit_id, Some(local_second));
    assert_eq!(status_after.current_commit_id, status_before.current_commit_id);
    let err = FsCommitStore::new(&clone_root)
        .expect("clone commit store")
        .read_commit(&remote_second)
        .expect_err("remote commit should not be imported");
    assert!(matches!(err, JetError::ObjectNotFound(ref id) if id == &remote_second));
    assert_eq!(first.len(), 64);

    shutdown.send(()).expect("shutdown");
}

#[tokio::test]
async fn transport_unavailable_pull_leaves_workspace_untouched() {
    let temp = tempdir().expect("tempdir");
    let repos_root = temp.path().join("repos");
    let source_root = repos_root.join("game");
    let clone_root = temp.path().join("clone");

    init_repo(&source_root).expect("init source");
    write_file(&source_root, "code/main.rs", b"fn main() {}\n");

    let source_repo = JetRepository::open(&source_root).expect("open source repo");
    source_repo
        .add_paths(&[PathBuf::from(".")])
        .expect("add source");
    let first = source_repo.commit("initial", "tester").expect("commit1");

    let (addr, shutdown) = spawn_server(repos_root.clone()).await;
    let remote = format!("http://{addr}/game");

    let clone_root_for_clone = clone_root.clone();
    let remote_for_clone = remote.clone();
    tokio::task::spawn_blocking(move || {
        clone_from_source(&remote_for_clone, &clone_root_for_clone, CloneMode::All)
    })
    .await
    .expect("join clone")
    .expect("clone");

    shutdown.send(()).expect("shutdown");

    let status_before = JetRepository::open(&clone_root)
        .expect("open clone before pull")
        .workspace_status()
        .expect("status before");

    let clone_root_for_pull = clone_root.clone();
    let remote_for_pull = remote.clone();
    let pull_err = tokio::task::spawn_blocking(move || {
        pull_from_remote(&clone_root_for_pull, Some(&remote_for_pull))
    })
    .await
    .expect("join pull")
    .expect_err("pull should fail");
    assert!(matches!(pull_err, JetError::RemoteTransport { .. }));

    let status_after = JetRepository::open(&clone_root)
        .expect("open clone after pull")
        .workspace_status()
        .expect("status after");
    assert_eq!(status_after.current_commit_id, status_before.current_commit_id);
    assert_eq!(status_after.current_commit_id, Some(first));
}

#[tokio::test]
async fn transport_unavailable_push_leaves_remote_head_unchanged() {
    let temp = tempdir().expect("tempdir");
    let repos_root = temp.path().join("repos");
    let source_root = repos_root.join("game");
    let clone_root = temp.path().join("clone");

    init_repo(&source_root).expect("init source");
    write_file(&source_root, "code/main.rs", b"fn main() {}\n");

    let source_repo = JetRepository::open(&source_root).expect("open source repo");
    source_repo
        .add_paths(&[PathBuf::from(".")])
        .expect("add source");
    let remote_head_before = source_repo.commit("initial", "tester").expect("commit1");

    let (addr, shutdown) = spawn_server(repos_root.clone()).await;
    let remote = format!("http://{addr}/game");

    let clone_root_for_clone = clone_root.clone();
    let remote_for_clone = remote.clone();
    tokio::task::spawn_blocking(move || {
        clone_from_source(&remote_for_clone, &clone_root_for_clone, CloneMode::All)
    })
    .await
    .expect("join clone")
    .expect("clone");

    let clone_repo = JetRepository::open(&clone_root).expect("open clone repo");
    write_file(&clone_root, "code/local.rs", b"pub fn local() {}\n");
    clone_repo
        .add_paths(&[PathBuf::from("code/local.rs")])
        .expect("add local");
    let local_head = clone_repo.commit("local", "tester").expect("commit local");

    shutdown.send(()).expect("shutdown");

    let clone_root_for_push = clone_root.clone();
    let remote_for_push = remote.clone();
    let push_err = tokio::task::spawn_blocking(move || {
        push_to_remote(&clone_root_for_push, &remote_for_push)
    })
    .await
    .expect("join push")
    .expect_err("push should fail");
    assert!(matches!(push_err, JetError::RemoteTransport { .. }));

    let remote_head_after = FsCommitStore::new(&source_root)
        .expect("remote commit store")
        .read_head()
        .expect("read remote head")
        .expect("remote head");
    assert_eq!(remote_head_after, remote_head_before);
    assert_ne!(remote_head_after, local_head);
}

#[tokio::test]
async fn authenticated_remote_rejects_missing_token() {
    let temp = tempdir().expect("tempdir");
    let repos_root = temp.path().join("repos");
    let source_root = repos_root.join("game");

    init_repo(&source_root).expect("init source");
    write_file(&source_root, "code/main.rs", b"fn main() {}\n");

    let source_repo = JetRepository::open(&source_root).expect("open source repo");
    source_repo
        .add_paths(&[PathBuf::from(".")])
        .expect("add source");
    source_repo.commit("initial", "tester").expect("commit1");

    let auth = AuthConfig::with_repo_permissions(
        [
            ("alice".to_string(), "secret-token".to_string()),
            ("bob".to_string(), "read-token".to_string()),
        ],
        [(
            "game".to_string(),
            RepoPermissions {
                read: BTreeSet::from(["bob".to_string()]),
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

    let unauthenticated = GrpcRemoteClient::new(location.clone()).expect("client");
    let unauthorized = tokio::task::spawn_blocking(move || unauthenticated.get_head())
        .await
        .expect("join unauthenticated")
        .expect_err("missing token should fail");
    assert!(matches!(unauthorized, JetError::RemoteUnauthorized));

    let authenticated =
        GrpcRemoteClient::with_auth_token(location, Some("secret-token".to_string()))
            .expect("client");
    let head = tokio::task::spawn_blocking(move || authenticated.get_head())
        .await
        .expect("join authenticated")
        .expect("authenticated head");
    assert!(head.is_some());

    let read_only =
        GrpcRemoteClient::with_auth_token(
            RemoteLocation {
                endpoint: format!("http://{addr}"),
                repo: "game".to_string(),
            },
            Some("read-token".to_string()),
        )
        .expect("client");
    let read_only_head = tokio::task::spawn_blocking(move || read_only.get_head())
        .await
        .expect("join read only")
        .expect("read only head");
    assert!(read_only_head.is_some());

    let read_only_writer =
        GrpcRemoteClient::with_auth_token(
            RemoteLocation {
                endpoint: format!("http://{addr}"),
                repo: "game".to_string(),
            },
            Some("read-token".to_string()),
        )
        .expect("client");
    let write_err = tokio::task::spawn_blocking(move || {
        read_only_writer.lock_path("assets/hero/model.fbx", "spoofed-owner")
    })
    .await
    .expect("join read only write")
    .expect_err("read-only user should not lock");
    assert!(matches!(write_err, JetError::RemoteUnauthorized));

    shutdown.send(()).expect("shutdown");
}

#[tokio::test]
async fn login_persists_credentials_for_whoami() {
    let temp = tempdir().expect("tempdir");
    let repos_root = temp.path().join("repos");
    let source_root = repos_root.join("game");
    let config_dir = temp.path().join("config");

    init_repo(&source_root).expect("init source");
    write_file(&source_root, "code/main.rs", b"fn main() {}\n");

    let source_repo = JetRepository::open(&source_root).expect("open source repo");
    source_repo
        .add_paths(&[PathBuf::from(".")])
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
    let remote = format!("http://{addr}/game");

    unsafe {
        std::env::remove_var("JET_TOKEN");
        std::env::set_var("JET_CONFIG_DIR", &config_dir);
    }

    let login_remote = remote.clone();
    let identity = tokio::task::spawn_blocking(move || login_with_token(&login_remote, "secret-token"))
        .await
        .expect("join login")
        .expect("login");
    assert_eq!(identity.identity, "alice");

    let whoami_remote = remote.clone();
    let whoami = tokio::task::spawn_blocking(move || remote_whoami(&whoami_remote))
        .await
        .expect("join whoami")
        .expect("whoami");
    assert_eq!(whoami.identity, "alice");

    let credentials_path = config_dir.join("credentials.toml");
    let credentials = fs::read_to_string(credentials_path).expect("credentials");
    assert!(credentials.contains(&format!("endpoint = \"http://{addr}\"")));
    assert!(credentials.contains("token = \"secret-token\""));

    unsafe {
        std::env::remove_var("JET_CONFIG_DIR");
    }

    shutdown.send(()).expect("shutdown");
}

#[tokio::test]
async fn clone_failure_does_not_create_destination_directory() {
    let temp = tempdir().expect("tempdir");
    let destination = temp.path().join("missing-repo-clone");

    let err = tokio::task::spawn_blocking({
        let destination = destination.clone();
        move || {
            clone_from_source(
                "http://127.0.0.1:9/missing-repo",
                &destination,
                CloneMode::All,
            )
        }
    })
    .await
    .expect("join clone")
    .expect_err("clone should fail");

    assert!(matches!(err, JetError::RemoteTransport { .. }));
    assert!(!destination.exists());
}
