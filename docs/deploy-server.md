# Deploy Jet Server

Jet remote mode uses one `jet-server` process to serve many repositories from a single `repos_root`.

Example layout:

```text
/srv/jet/game
/srv/jet/cinematic
/srv/jet/demo
```

Start the server manually:

```bash
jet-server \
  --listen 0.0.0.0:4220 \
  --repos-root /srv/jet \
  --auth-config /etc/jet/auth.toml
```

If `repos_root` itself is already a Jet repo, `jet-server` also serves that repo directly under its folder name.  
Example: starting inside `/Users/me/game` serves `http://host:4220/game`.

Example auth config:

```toml
[[users]]
name = "alice"
tokens = ["jet_alice_token"]

[[users]]
name = "bob"
tokens = ["jet_bob_token"]

[[repos]]
name = "game"
read = ["alice", "bob"]
write = ["alice"]
admin = ["alice"]
```

Client flow:

```bash
jet auth login http://server:4220/game --token jet_alice_token
jet auth whoami http://server:4220/game
jet clone http://server:4220/game
```

Notes:

- One `jet-server` can serve many repos
- `repos_root` is the parent directory that contains repo folders
- Auth is optional; omit `--auth-config` to run without auth
- `clone` and `pull` require `read`
- `push`, `lock`, and `unlock` require `write`

Deployment templates:

- [examples/jet-server.service](/Users/joma/Documents/Code/jet/examples/jet-server.service)
- [examples/com.nodfans.jet-server.plist](/Users/joma/Documents/Code/jet/examples/com.nodfans.jet-server.plist)

Admin auth operations:

- [manage auth](/Users/joma/Documents/Code/jet/docs/admin-auth.md)
