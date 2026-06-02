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
