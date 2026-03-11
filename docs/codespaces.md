# Test Jet with GitHub Codespaces

This is the fastest way to test Jet remote mode without setting up a separate machine.

## 1. Open a Codespace

Open a Codespace on this repository. The devcontainer builds:

- `jet`
- `jet-server`

Port `4220` is forwarded for `jet-server`.

## 2. Start the server inside the Codespace

From the repository root:

```bash
bash scripts/start_codespace_server.sh
```

If you want auth enabled:

```bash
JET_SERVER_AUTH_CONFIG=/workspaces/jet/examples/auth.toml bash scripts/start_codespace_server.sh
```

By default this serves repos from the current repository directory.  
If the current directory is already a Jet repo, it is served under its folder name.

## 3. Copy the forwarded URL

In Codespaces, copy the forwarded port URL for `4220`.

It will look like:

```text
https://<codespace>-4220.app.github.dev
```

Use that URL as the Jet remote:

```bash
jet clone --all https://<codespace>-4220.app.github.dev/game
```

## 4. Local auth flow

If auth is enabled on the Codespace server:

```bash
jet auth login https://<codespace>-4220.app.github.dev/game --token <token>
jet auth whoami https://<codespace>-4220.app.github.dev/game
```

## Notes

- Codespaces is good for workflow validation and moderate-size data
- Large datasets will consume storage and compute quota quickly
- One `jet-server` can serve multiple repos from the chosen `repos_root`
