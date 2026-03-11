#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
INSTALL_DIR="${JET_INSTALL_DIR:-$HOME/.local/bin}"
MODE="symlink"

usage() {
  cat <<EOF
Usage: bash scripts/install.sh [--copy] [--install-dir DIR]

Builds release binaries and installs:
  jet
  jet-server

Defaults:
  install dir: \$HOME/.local/bin
  mode: symlink

Environment:
  JET_INSTALL_DIR   Override install dir
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --copy)
      MODE="copy"
      shift
      ;;
    --install-dir)
      INSTALL_DIR="${2:?missing install dir}"
      shift 2
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "unknown argument: $1" >&2
      usage >&2
      exit 1
      ;;
  esac
done

PATH_LINE="export PATH=\"${INSTALL_DIR}:\$PATH\""

detect_rc_file() {
  local shell_name
  shell_name="$(basename "${SHELL:-}")"
  case "${shell_name}" in
    zsh)
      echo "$HOME/.zshrc"
      ;;
    bash)
      echo "$HOME/.bashrc"
      ;;
    *)
      echo "$HOME/.zshrc"
      ;;
  esac
}

if ! command -v cargo >/dev/null 2>&1; then
  echo "cargo is required" >&2
  exit 1
fi

echo "Building release binaries..."
cargo build --release -p jet-cli -p jet-server --manifest-path "${ROOT_DIR}/Cargo.toml"

mkdir -p "${INSTALL_DIR}"

install_one() {
  local name="$1"
  local source_path="${ROOT_DIR}/target/release/${name}"
  local dest_path="${INSTALL_DIR}/${name}"

  if [[ ! -x "${source_path}" ]]; then
    echo "missing built binary: ${source_path}" >&2
    exit 1
  fi

  if [[ "${MODE}" == "copy" ]]; then
    cp "${source_path}" "${dest_path}"
    chmod +x "${dest_path}"
  else
    ln -sfn "${source_path}" "${dest_path}"
  fi

  echo "Installed ${name} -> ${dest_path}"
}

install_one "jet"
install_one "jet-server"

ensure_path_in_rc() {
  local rc_file="$1"
  if [[ -f "${rc_file}" ]] && grep -Fq "${PATH_LINE}" "${rc_file}"; then
    return
  fi

  {
    echo
    echo "# Added by Jet installer"
    echo "${PATH_LINE}"
  } >> "${rc_file}"
  echo "Updated PATH in ${rc_file}"
}

path_needs_update=true
case ":$PATH:" in
  *":${INSTALL_DIR}:"*)
    path_needs_update=false
    ;;
esac

if [[ "${path_needs_update}" == true ]]; then
  rc_file="$(detect_rc_file)"
  ensure_path_in_rc "${rc_file}"
  echo
  echo "Open a new shell to use 'jet' directly."
else
  echo
  echo "'jet' is already on PATH for this shell."
fi
