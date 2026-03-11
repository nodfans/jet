# Manage Auth

This guide covers the common server-side auth changes for a Jet team deployment.

Auth is managed in the server config file, for example `/etc/jet/auth.toml`.

`jet-server` validates this file on startup and will fail fast on:

- duplicate user names
- duplicate tokens
- empty user names or tokens
- repo ACL entries that reference unknown users

## Add a User

Add a new `[[users]]` entry with a unique token:

```toml
[[users]]
name = "david"
tokens = ["jet_david_token"]
```

Then grant repo access:

```toml
[[repos]]
name = "game"
read = ["alice", "bob", "charlie", "david"]
write = ["alice", "bob"]
admin = ["alice"]
```

After restarting `jet-server`, the new user can run:

```bash
jet auth login http://server:4220/game --token jet_david_token
jet auth whoami http://server:4220/game
```

## Rotate or Revoke a Token

To rotate a token, replace the old token with a new one:

```toml
[[users]]
name = "alice"
tokens = ["jet_alice_token_v2"]
```

To revoke access completely, remove the user token and remove the user from repo permission lists.

After changing the config, restart `jet-server`.

## Grant Repo Access

Read access allows:

- `clone`
- `pull`
- `jet auth whoami`
- `locks`

Write access allows:

- `push`
- `lock`
- `unlock`

Example:

```toml
[[repos]]
name = "cinematic"
read = ["alice", "charlie"]
write = ["alice"]
admin = ["alice"]
```

## Remove Repo Access

Remove the user from the repo permission lists:

```toml
[[repos]]
name = "game"
read = ["alice", "bob"]
write = ["alice"]
admin = ["alice"]
```

If the user should lose all access, also remove their token from `[[users]]`.

## Notes

- One user can have multiple tokens during rotation
- `write` does not need to be repeated in `read`; write users already have read access
- `lock` ownership comes from the authenticated identity, not the client-supplied owner
