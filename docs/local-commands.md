# Jet Commands

This document describes the current Jet command set.

## Core Flow

1. `jet init`
2. `jet clone --all <source> [dest]`
3. `jet clone --partial <source> [dest]`
4. `jet add <paths...>`
5. `jet commit -m "<message>" [-a <author>]`
6. `jet open <commit_id>`
7. `jet view`
8. `jet hydrate [paths...]`
9. `jet dehydrate [paths...]`
10. `jet push <remote>`
11. `jet pull [remote]`
12. `jet lock <remote> <path> [-o <owner>]`
13. `jet unlock <remote> <path> [-o <owner>]`
14. `jet locks <remote> [prefix]`
15. `jet auth <subcommand>`
16. `jet status`

## Command Summary

### `jet init`

Creates `.jet/` and initializes:

- `config.json`
- `staging/index.bin`
- `workspace.bin`
- `materialized-index.bin`
- `workspace.local.toml`

### `jet add <paths...>`

Stages files or directories into Jet.

- Reuses manifest entries when file metadata is unchanged
- Uses direct-blob storage for smaller files
- Uses chunked storage for larger files
- Applies large-file chunk reuse and cache paths where possible

### `jet clone --all <source> [dest]`

Clones a Jet repository source into a local workspace and materializes the full current workspace.

- Local paths and `file://` sources are supported
- Remote HTTP/Jet sources are supported for metadata + object transfer
- Remote auth uses `JET_TOKEN` if set
- Opens the cloned workspace at the source `HEAD`
- Materializes all files in the current workspace commit

### `jet clone --partial <source> [dest]`

Clones a Jet repository source into a local workspace without materializing every file.

- Local paths and `file://` sources are supported
- Remote HTTP/Jet sources are supported for metadata + object transfer
- Remote auth uses `JET_TOKEN` if set
- Copies durable repo data from the source `.jet/`
- Initializes a new local workspace
- Opens the cloned workspace at the source `HEAD`
- Keeps workspace files virtual until hydrated
- Keeps workspace-local overrides local to the new clone

### `jet commit -m "<message>" [-a <author>]`

Creates a commit from the current staging area and updates `HEAD`.

### `jet open <commit_id>`

Switches the local workspace metadata to a commit. This is metadata-first and does not imply a full restore.

- `HEAD` is accepted as a shortcut to the current head commit

### `jet view`

Prints the current workspace `include` and `exclude` rules from `.jet/workspace.local.toml`.

### `jet hydrate [paths...]`

Writes selected files from the current workspace commit into the local working directory.

### `jet dehydrate [paths...]`

Removes clean local files and marks them virtual again. Dirty files are protected.

### `jet push <remote>`

Pushes the current local `HEAD` to a remote source.

- Uses `JET_TOKEN` first, then `.jet/credentials` when present
- Uploads missing chunks
- Uploads missing commits
- Updates remote `HEAD` with a fast-forward check

### `jet pull [remote]`

Pulls the latest remote `HEAD` into the current workspace.

- Fast-forward only
- Uses the workspace remote if `remote` is omitted
- Uses `JET_TOKEN` first, then `.jet/credentials` when present
- Prefetches default hot-path objects before opening the new workspace

### `jet lock <remote> <path> [-o <owner>]`

Locks a remote path for exclusive editing.

- When remote auth is enabled, the server uses the authenticated identity as the owner
- Without auth, the explicit `-o/--owner` value is used

### `jet unlock <remote> <path> [-o <owner>]`

Unlocks a remote path owned by the caller.

- When remote auth is enabled, the server ignores the client-provided owner and uses the authenticated identity

### `jet locks <remote> [prefix]`

Lists current remote locks, optionally filtered by path prefix.

### `jet status`

Shows the current workspace commit, file-state counts, and a short preview of dirty, pending, or not-in-view paths.

### `jet auth whoami <remote>`

Shows which authenticated identity the remote currently sees for your token.

### `jet auth login <remote> --token <token>`

Validates a token against the remote and stores it in the user credentials file.

- Saves credentials in `~/.config/jet/credentials.toml`
- Keys credentials by server endpoint
- Makes future `clone/pull/push/lock` calls work without re-exporting `JET_TOKEN`

### `jet log`

Prints commit history from the current `HEAD`.

- Marks the current `HEAD`
- Shows a short commit id and file count per commit

### `jet stats`

Shows object-count and object-bytes for the local object store.

- Also shows the current `HEAD`
- Also shows the local commit count

### `jet fsck`

Verifies commit-to-object integrity across the local repository.

## Remote Auth

Jet remote auth is optional and disabled by default.

- Start the server with `jet-server --auth-config /etc/jet/auth.toml`
- Or use `jet-server --auth-token alice:secret` for a small local setup
- Set `JET_TOKEN=secret` in the client environment
- Or store the token in `.jet/credentials`
- Or run `jet auth login <remote> --token <token>` to save a user-level credential

`.jet/credentials` accepts either:

- `token = "secret"`
- `secret`

Minimal auth config:

```toml
[[users]]
name = "alice"
tokens = ["secret"]

[[repos]]
name = "game"
write = ["alice"]
```

For a fuller multi-user example, see [examples/auth.toml](/Users/joma/Documents/Code/jet/examples/auth.toml).
