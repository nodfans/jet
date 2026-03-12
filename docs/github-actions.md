# Test Jet Remote in GitHub Actions

Use GitHub Actions when you want a quick remote `push` / `pull` benchmark without
setting up a separate server.

## Workflow

Run:

- `.github/workflows/remote-benchmark.yml`

It:

- builds `jet` and `jet-server`
- creates a temporary remote repo
- generates large binary files
- measures `clone --all`, `clone --partial`, `pull`, and `push`
- uploads `remote-benchmark.txt` as an artifact

## Inputs

- `file_count`
- `file_size_mb`

Default run:

- `4` files
- `64 MB` each

## Notes

- This is good for relative comparisons and remote workflow validation
- It is not a stable public-server benchmark
- Runner performance will vary from run to run
