# Jet

Nothing beats a Jet2 holiday...

Jet is a large-file version control system for mixed repositories.

Current workflow:

- local: `init`, `add`, `commit`, `open`, `hydrate`, `dehydrate`, `status`
- remote: `clone`, `pull`, `push`, `lock`, `unlock`, `locks`

## Install

Direct install from GitHub:

```bash
curl -fsSL https://raw.githubusercontent.com/nodfans/jet/main/install.sh | bash
```

From a local clone:

```bash
bash scripts/install.sh
```

The installer updates your shell config so new terminals can run `jet`
directly. If you need to add it manually, use:

```bash
export PATH="$HOME/.local/bin:$PATH"
```

## Quick Start

Local:

```bash
jet init
jet add .
jet commit -m "initial" -a you
jet status
```

Open an older revision and restore only what you need:

```bash
jet open <commit-id>
jet hydrate code assets/shared/psd
jet dehydrate assets/shared/psd
```

Remote:

```bash
jet-server --listen 127.0.0.1:4220 --repos-root /path/to/repos
jet clone --all http://127.0.0.1:4220/game
jet pull
jet push
```

Remote with auth:

```bash
jet-server --listen 127.0.0.1:4220 --repos-root /path/to/repos --auth-config /etc/jet/auth.toml
jet auth login http://127.0.0.1:4220/game --token secret
jet auth whoami http://127.0.0.1:4220/game
jet clone --all http://127.0.0.1:4220/game
jet pull
jet push
```

Use [`examples/auth.toml`](/Users/joma/Documents/Code/jet/examples/auth.toml) as a starting point for multi-user server auth.

You can also store the token in `.jet/credentials`:

```bash
token = "secret"
```

When auth is enabled, lock ownership comes from the authenticated identity.

Locks:

```bash
jet lock http://127.0.0.1:4220/game assets/hero/model.fbx -o alice
jet locks http://127.0.0.1:4220/game
jet unlock http://127.0.0.1:4220/game assets/hero/model.fbx -o alice
```

## Commands

```bash
jet --help
jet <command> --help
```

More details:

- [local commands](/Users/joma/Documents/Code/jet/docs/local-commands.md)
- [codespaces](/Users/joma/Documents/Code/jet/docs/codespaces.md)
- [github actions benchmark](/Users/joma/Documents/Code/jet/docs/github-actions.md)
- [deploy server](/Users/joma/Documents/Code/jet/docs/deploy-server.md)
- [manage auth](/Users/joma/Documents/Code/jet/docs/admin-auth.md)
- [product testing](/Users/joma/Documents/Code/jet/docs/product-testing.md)
- [architecture](/Users/joma/Documents/Code/jet/Architecture.md)

## Testing

Bench and product-flow scripts:

- [scripts/product_flow_local.sh](/Users/joma/Documents/Code/jet/scripts/product_flow_local.sh)
- [scripts/product_flow_local_large.sh](/Users/joma/Documents/Code/jet/scripts/product_flow_local_large.sh)
- [scripts/product_flow_remote.sh](/Users/joma/Documents/Code/jet/scripts/product_flow_remote.sh)
- [scripts/product_flow_remote_large.sh](/Users/joma/Documents/Code/jet/scripts/product_flow_remote_large.sh)
- [scripts/benchmark_remote_actions.sh](/Users/joma/Documents/Code/jet/scripts/benchmark_remote_actions.sh)
- [scripts/compare_local_lfs.sh](/Users/joma/Documents/Code/jet/scripts/compare_local_lfs.sh)

Integration tests:

```bash
cargo test -p jet-tests
```
