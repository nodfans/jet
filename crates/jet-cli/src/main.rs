use std::path::PathBuf;
use std::process::ExitCode;
use std::{fmt::Write as _, path::Path};

use clap::{Parser, Subcommand};
use jet_core::CommitStore;
use jet_core::JetError;
use jet_remote::CloneMode;

#[derive(Debug, Parser)]
#[command(
    name = "jet",
    bin_name = "jet",
    version,
    about = None,
    disable_help_subcommand = true
)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Debug, Subcommand)]
enum Commands {
    #[command(about = "Initialize a new repo")]
    Init,
    #[command(about = "Clone a repo into a new workspace", group = clap::ArgGroup::new("clone_mode").args(["all", "partial"]).required(true))]
    Clone {
        #[arg(long, conflicts_with = "partial")]
        all: bool,
        #[arg(long, conflicts_with = "all")]
        partial: bool,
        source: String,
        destination: Option<PathBuf>,
    },
    #[command(about = "Stage files for the next commit")]
    Add { paths: Vec<PathBuf> },
    #[command(about = "Save staged changes as a new commit")]
    Commit {
        #[arg(short, long)]
        message: String,
        #[arg(short, long, default_value = "local")]
        author: String,
    },
    #[command(about = "Show commit history")]
    Log,
    #[command(about = "Switch workspace to a specific commit")]
    Open { commit_id: String },
    #[command(about = "Push local commits to remote")]
    Push { remote: String },
    #[command(about = "Pull latest commits from remote")]
    Pull { remote: Option<String> },
    #[command(about = "Lock a file for exclusive editing")]
    Lock {
        remote: String,
        path: String,
        #[arg(short, long, default_value = "local")]
        owner: String,
    },
    #[command(about = "Release a file lock")]
    Unlock {
        remote: String,
        path: String,
        #[arg(short, long, default_value = "local")]
        owner: String,
    },
    #[command(about = "List all active locks")]
    Locks {
        remote: String,
        prefix: Option<String>,
    },
    #[command(about = "Show view rules")]
    View,
    #[command(about = "Show workspace state")]
    Status,
    #[command(about = "Restore virtual files to disk")]
    Hydrate { paths: Vec<PathBuf> },
    #[command(about = "Release local files to free disk space")]
    Dehydrate { paths: Vec<PathBuf> },
    #[command(about = "Show object store statistics")]
    Stats,
    #[command(about = "Verify repo integrity")]
    Fsck {
        #[arg(
            long,
            help = "Re-hash file contents instead of only checking commit/object reachability"
        )]
        deep: bool,
    },
    #[command(about = "Manage remote credentials")]
    Auth {
        #[command(subcommand)]
        command: AuthCommands,
    },
}

#[derive(Debug, Subcommand)]
enum AuthCommands {
    #[command(about = "Show the current remote identity")]
    Whoami { remote: String },
    #[command(about = "Save a token for a remote server")]
    Login {
        remote: String,
        #[arg(long)]
        token: String,
    },
}

fn main() -> ExitCode {
    match run() {
        Ok(()) => ExitCode::SUCCESS,
        Err(err) => {
            eprintln!("{}", render_error(&err));
            ExitCode::FAILURE
        }
    }
}

fn run() -> anyhow::Result<()> {
    let cli = Cli::parse();
    let cwd = std::env::current_dir()?;

    match cli.command {
        Commands::Init => {
            jet_core::repo::init_repo(&cwd)?;
            println!("{}", format_init_success());
        }
        Commands::Clone {
            all,
            partial,
            source,
            destination,
        } => {
            let mode = if all {
                CloneMode::All
            } else {
                debug_assert!(partial);
                CloneMode::Partial
            };
            let destination =
                destination.unwrap_or_else(|| default_clone_destination_name(&source));
            jet_remote::clone_from_source(&source, &destination, mode)?;
            let repo = jet_core::JetRepository::open(&destination)?;
            let status = repo.workspace_status()?;
            println!("{}", format_clone_success(&source, &destination, mode, &status));
        }
        Commands::Add { paths } => {
            let repo = jet_core::JetRepository::open(&cwd)?;
            let count = repo.add_paths(&paths)?;
            println!("{}", format_count_result("Staged files", count));
        }
        Commands::Commit { message, author } => {
            let repo = jet_core::JetRepository::open(&cwd)?;
            let id = repo.commit(&message, &author)?;
            println!("{}", format_commit_success(&id));
        }
        Commands::Log => {
            let repo = jet_core::JetRepository::open(&cwd)?;
            if repo.workspace_remote_source()?.is_some() {
                let _ = jet_remote::sync_remote_history(&cwd, None)?;
            }
            let head = repo.head_commit_id()?;
            print!("{}", format_log(&repo.log()?, head.as_deref()));
        }
        Commands::Open { commit_id } => {
            let repo = jet_core::JetRepository::open(&cwd)?;
            let remote_source = repo.workspace_remote_source()?;
            let resolved = if commit_id.eq_ignore_ascii_case("head") {
                repo.head_commit_id()?.ok_or(JetError::NoWorkspaceCommit)?
            } else {
                commit_id
            };
            let commit_store = jet_core::FsCommitStore::new(&cwd)?;
            match commit_store.read_commit(&resolved) {
                Ok(commit) if commit.files_omitted && remote_source.is_some() => {
                    jet_remote::open_with_remote(&cwd, &resolved)?;
                }
                Ok(commit) if commit.files_omitted => {
                    return Err(JetError::CommitMetadataOnly {
                        commit_id: resolved,
                    }
                    .into());
                }
                Ok(_) => {
                    repo.checkout(&resolved)?;
                }
                Err(JetError::ObjectNotFound(_)) if remote_source.is_some() => {
                    jet_remote::open_with_remote(&cwd, &resolved)?;
                }
                Err(err) => return Err(err.into()),
            }
            println!("{}", format_open_success(&resolved));
        }
        Commands::Push { remote } => {
            let report = jet_remote::push_to_remote(&cwd, &remote)?;
            print!("{}", format_push_report(&report));
        }
        Commands::Pull { remote } => {
            let report = jet_remote::pull_from_remote(&cwd, remote.as_deref())?;
            print!("{}", format_pull_report(&report));
        }
        Commands::Lock {
            remote,
            path,
            owner,
        } => {
            let lock = jet_remote::lock_remote_path(&remote, &path, &owner)?;
            println!("{}", format_lock_success(&lock.path, &lock.owner));
        }
        Commands::Unlock {
            remote,
            path,
            owner,
        } => {
            jet_remote::unlock_remote_path(&remote, &path, &owner)?;
            println!("{}", format_unlock_success(&path));
        }
        Commands::Locks { remote, prefix } => {
            let locks = jet_remote::list_remote_locks(&remote, prefix.as_deref())?;
            print!("{}", format_locks(&locks));
        }
        Commands::View => {
            let repo = jet_core::JetRepository::open(&cwd)?;
            let view = repo.workspace_view()?;
            print!("{}", format_view(&view.view.include, &view.view.exclude));
        }
        Commands::Status => {
            let repo = jet_core::JetRepository::open(&cwd)?;
            let status = repo.workspace_status()?;
            print!("{}", format_status(&status));
        }
        Commands::Hydrate { paths } => {
            let repo = jet_core::JetRepository::open(&cwd)?;
            let count = if repo.workspace_remote_source()?.is_some() {
                jet_remote::hydrate_with_remote(&cwd, &paths)?
            } else {
                repo.hydrate(&paths)?
            };
            println!("{}", format_count_result("Hydrated files", count));
        }
        Commands::Dehydrate { paths } => {
            let repo = jet_core::JetRepository::open(&cwd)?;
            let count = repo.dehydrate(&paths)?;
            println!("{}", format_count_result("Dehydrated files", count));
        }
        Commands::Stats => {
            let repo = jet_core::JetRepository::open(&cwd)?;
            if repo.workspace_remote_source()?.is_some() {
                let _ = jet_remote::sync_remote_history(&cwd, None)?;
            }
            let head = repo.head_commit_id()?;
            let commits = repo.log()?;
            let stats = repo.stats()?;
            let metadata_only_commits =
                commits.iter().filter(|commit| commit.files_omitted).count();
            print!(
                "{}",
                format_stats(
                    head.as_deref(),
                    commits.len(),
                    metadata_only_commits,
                    stats.object_count,
                    stats.object_bytes,
                )
            );
        }
        Commands::Fsck { deep } => {
            let repo = jet_core::JetRepository::open(&cwd)?;
            let mode = if deep {
                jet_core::FsckMode::Deep
            } else {
                jet_core::FsckMode::Quick
            };
            repo.fsck_with_mode(mode)?;
            println!("{}", format_fsck_ok(deep));
        }
        Commands::Auth { command } => match command {
            AuthCommands::Whoami { remote } => {
                let identity = jet_remote::remote_whoami(&remote)?;
                println!("{}", format_auth_identity(&identity.identity));
            }
            AuthCommands::Login { remote, token } => {
                let identity = jet_remote::login_with_token(&remote, &token)?;
                println!("{}", format_auth_login_success(&remote, &identity.identity));
            }
        },
    }

    Ok(())
}

fn render_error(err: &anyhow::Error) -> String {
    if let Some(err) = err.downcast_ref::<JetError>() {
        return match err {
            JetError::InvalidRepository { path } => {
                format!(
                    "This directory is not a Jet repository: {}\nNext: run `jet init` here, or change into an existing Jet workspace.",
                    path.display()
                )
            }
            JetError::EmptyStaging => {
                "Nothing to commit.\nNext: run `jet add <paths...>` before `jet commit`."
                    .to_string()
            }
            JetError::NoWorkspaceCommit => {
                "This workspace has no active commit.\nNext: run `jet open <commit-id>` first."
                    .to_string()
            }
            JetError::NoRemoteConfigured => {
                "This workspace has no remote configured.\nNext: clone from a remote first, or pass an explicit remote to `jet pull` or `jet push`."
                    .to_string()
            }
            JetError::CommitMetadataOnly { commit_id } => format!(
                "Commit {commit_id} is only available as lightweight metadata.\nNext: reconnect this workspace to its remote and run `jet open {commit_id}` again."
            ),
            JetError::RemoteCommitNotFound { commit_id } => format!(
                "The remote does not have commit {commit_id}.\nNext: verify the commit id or sync the workspace against the correct remote."
            ),
            JetError::RemoteObjectMissing {
                commit_id,
                object_id,
            } => format!(
                "The remote is missing data for commit {commit_id}: {object_id}\nNext: run `jet fsck --deep` on the source repo or repair the remote storage."
            ),
            JetError::DirtyWorkspaceFile { path } => format!(
                "The workspace has uncommitted changes: {path}\nNext: commit the file or restore it before retrying."
            ),
            JetError::ObjectNotFound(id) => format!(
                "Object not found: {id}\nNext: verify the repository state and retry."
            ),
            JetError::DigestMismatch { path } => format!(
                "Integrity check failed while processing {path}.\nNext: run `jet fsck` to inspect the repository."
            ),
            JetError::CloneDestinationNotEmpty { path } => format!(
                "Clone destination is not empty: {}\nNext: choose a new directory or empty the existing one.",
                path.display()
            ),
            JetError::RemoteBackendNotImplemented { remote } => format!(
                "This remote source is recognized but not supported yet: {remote}\nNext: use a local path or an HTTP Jet remote."
            ),
            JetError::RemotePushRejected { remote_head } => format!(
                "Push was rejected because the remote has moved ahead.\nRemote head: {remote_head}\nNext: run `jet pull` and retry."
            ),
            JetError::RemotePullRejected => {
                "Pull was rejected because your local history has diverged.\nNext: re-clone or reconcile the history before retrying."
                    .to_string()
            }
            JetError::RemoteTransport { message } => format!(
                "Could not complete the remote operation: {message}\nNext: verify the server address and try again."
            ),
            JetError::RemoteUnauthorized => {
                "Authentication failed.\nNext: run `jet auth login <remote> --token <token>`, or set `JET_TOKEN`."
                    .to_string()
            }
            JetError::LockConflict { path, owner } => format!(
                "This path is already locked by {owner}: {path}\nNext: wait for the lock to be released or coordinate with the owner."
            ),
            JetError::LockOwnershipMismatch { path, owner } => format!(
                "Unlock was rejected for {path}.\nNext: use the credentials for {owner}, or ask them to unlock it."
            ),
            _ => err.to_string(),
        };
    }

    err.to_string()
}

fn short_id(id: &str) -> &str {
    const SHORT_ID_LEN: usize = 12;

    if id.len() <= SHORT_ID_LEN {
        id
    } else {
        &id[..SHORT_ID_LEN]
    }
}

fn default_clone_destination_name(source: &str) -> PathBuf {
    let normalized = source
        .trim_end_matches('/')
        .strip_prefix("file://")
        .unwrap_or(source)
        .trim_end_matches('/');

    std::path::Path::new(normalized)
        .file_name()
        .map(PathBuf::from)
        .filter(|name| !name.as_os_str().is_empty())
        .unwrap_or_else(|| PathBuf::from("jet-clone"))
}

fn format_init_success() -> String {
    "Initialized Jet repository in .jet/".to_string()
}

fn format_clone_success(
    source: &str,
    destination: &Path,
    mode: CloneMode,
    status: &jet_core::WorkspaceStatus,
) -> String {
    let is_all = mode == CloneMode::All;
    let mode = match mode {
        CloneMode::All => "all",
        CloneMode::Partial => "partial",
    };
    let mut out = format!(
        "Cloned repository from {source} to {} ({mode})",
        destination.display()
    );
    if is_all {
        let _ = write!(
            out,
            "\nWorkspace ready: {} hydrated, {} virtual",
            status.hydrated_count, status.virtual_count
        );
    } else {
        let _ = write!(
            out,
            "\nWorkspace ready: {} hydrated, {} virtual\nNext: run `jet hydrate <paths...>` to materialize more files.",
            status.hydrated_count, status.virtual_count
        );
    }
    out
}

fn format_count_result(label: &str, count: usize) -> String {
    format!("{label}: {count}")
}

fn format_commit_success(id: &str) -> String {
    format!("Created commit {}", short_id(id))
}

fn format_open_success(commit_id: &str) -> String {
    format!("Opened workspace at {}", short_id(commit_id))
}

fn format_push_report(report: &jet_remote::PushReport) -> String {
    let mut out = String::new();
    let _ = writeln!(out, "Pushed {}", short_id(&report.new_head));
    let _ = writeln!(out, "Commits uploaded: {}", report.commit_count);
    let _ = writeln!(out, "Chunks uploaded:  {}", report.chunk_count);
    out
}

fn format_pull_report(report: &jet_remote::PullReport) -> String {
    let mut out = String::new();
    let _ = writeln!(out, "Pulled {}", short_id(&report.new_head));
    let _ = writeln!(out, "Commits imported: {}", report.commit_count);
    out
}

fn format_lock_success(path: &str, owner: &str) -> String {
    format!("Locked {path}\nOwner: {owner}")
}

fn format_unlock_success(path: &str) -> String {
    format!("Unlocked {path}")
}

fn format_locks(locks: &[jet_remote::LockInfo]) -> String {
    if locks.is_empty() {
        return "No locks\n".to_string();
    }
    let mut out = String::new();
    for lock in locks {
        let _ = writeln!(out, "{}  {}", lock.owner, lock.path);
    }
    out
}

fn format_view(includes: &[String], excludes: &[String]) -> String {
    let mut out = String::new();
    append_view_summary(&mut out, "include", includes);
    append_view_summary(&mut out, "exclude", excludes);
    out
}

fn format_log(commits: &[jet_core::commit_store::Commit], head: Option<&str>) -> String {
    let mut out = String::new();
    for commit in commits {
        let marker = if head == Some(commit.id.as_str()) {
            "HEAD"
        } else {
            "    "
        };
        let _ = writeln!(out, "{marker} {}", short_id(&commit.id));
        let _ = writeln!(out, "Author: {}", commit.author);
        let _ = writeln!(out, "Time:   {}", commit.timestamp_unix);
        if commit.files_omitted {
            let _ = writeln!(out, "Files:  metadata-only");
        } else {
            let _ = writeln!(out, "Files:  {}", commit.files.len());
        }
        let _ = writeln!(out, "Message: {}", commit.message);
        let _ = writeln!(out);
    }
    out
}

fn format_status(status: &jet_core::WorkspaceStatus) -> String {
    let mut out = String::new();
    let _ = writeln!(
        out,
        "Commit: {}",
        status
            .current_commit_id
            .as_deref()
            .map(short_id)
            .unwrap_or("(none)")
    );
    let _ = writeln!(
        out,
        "Remote: {}",
        status.remote_source.as_deref().unwrap_or("(none)")
    );
    append_view_summary(&mut out, "include", &status.view_includes);
    append_view_summary(&mut out, "exclude", &status.view_excludes);
    let _ = writeln!(out, "Virtual:      {}", status.virtual_count);
    let _ = writeln!(out, "Hydrated:     {}", status.hydrated_count);
    let _ = writeln!(out, "Dirty:        {}", status.dirty_count);
    let _ = writeln!(out, "Pending:      {}", status.pending_count);
    let _ = writeln!(out, "Not in view:  {}", status.not_in_view_count);
    append_path_preview(&mut out, "dirty", &status.dirty_paths);
    append_path_preview(&mut out, "pending", &status.pending_paths);
    append_path_preview(&mut out, "not-in-view", &status.not_in_view_paths);
    out
}

fn format_stats(
    head: Option<&str>,
    commit_count: usize,
    metadata_only_commits: usize,
    object_count: u64,
    object_bytes: u64,
) -> String {
    let mut out = String::new();
    let _ = writeln!(out, "Head:    {}", head.map(short_id).unwrap_or("(none)"));
    let _ = writeln!(out, "Commits: {}", commit_count);
    let _ = writeln!(out, "Metadata-only commits: {}", metadata_only_commits);
    let _ = writeln!(
        out,
        "Full commits: {}",
        commit_count.saturating_sub(metadata_only_commits)
    );
    let _ = writeln!(out, "Objects: {}", object_count);
    let _ = writeln!(out, "Bytes:   {}", object_bytes);
    out
}

fn format_fsck_ok(deep: bool) -> String {
    format!("fsck OK ({})", if deep { "deep" } else { "quick" })
}

fn format_auth_identity(identity: &str) -> String {
    format!("Authenticated as {identity}")
}

fn format_auth_login_success(remote: &str, identity: &str) -> String {
    format!("Saved credentials for {remote}\nAuthenticated as {identity}")
}

fn append_view_summary(out: &mut String, label: &str, patterns: &[String]) {
    if patterns.is_empty() {
        let _ = writeln!(out, "View {label}: (default)");
    } else {
        let _ = writeln!(out, "View {label}: {}", patterns.join(", "));
    }
}

fn append_path_preview(out: &mut String, label: &str, paths: &[String]) {
    if paths.is_empty() {
        return;
    }
    let pretty_label = match label {
        "dirty" => "Dirty paths",
        "pending" => "Pending paths",
        "not-in-view" => "Not in view",
        _ => label,
    };
    let _ = writeln!(out, "{pretty_label}:");
    for path in paths {
        let _ = writeln!(out, "  {path}");
    }
}
