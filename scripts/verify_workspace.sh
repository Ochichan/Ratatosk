#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

cd "${REPO_ROOT}"

echo "[verify] cargo fmt --all --check"
cargo fmt --all --check

echo "[verify] cargo check --workspace"
cargo check --workspace

echo "[verify] cargo clippy --workspace --all-targets -- -D warnings"
cargo clippy --workspace --all-targets -- -D warnings

echo "[verify] cargo test --workspace"
cargo test --workspace

# The shared-memory transport is feature-gated (Unix only); keep it compiling and
# tested on Unix CI so the experimental path cannot rot silently.
if [[ "$(uname -s)" != "Windows_NT" && "${OS:-}" != "Windows_NT" ]]; then
  echo "[verify] cargo clippy -p ratatosk-server --features shm-transport --all-targets -- -D warnings"
  cargo clippy -p ratatosk-server --features shm-transport --all-targets -- -D warnings

  echo "[verify] cargo test -p ratatosk-server --features shm-transport"
  cargo test -p ratatosk-server --features shm-transport
fi
