#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
OUT_DIR="${1:-$ROOT_DIR/benchmarks}"
SAMPLE_SIZE="${SAMPLE_SIZE:-20}"
WITH_MIMALLOC="${WITH_MIMALLOC:-0}"

mkdir -p "$OUT_DIR"
cd "$ROOT_DIR"

if command -v cargo >/dev/null 2>&1; then
  CARGO_CMD=(cargo)
else
  CARGO_CMD=(nix shell nixpkgs#cargo nixpkgs#rustc nixpkgs#pkg-config -c cargo)
fi

TS="$(date +%Y%m%d-%H%M%S)"
DEFAULT_LOG="$OUT_DIR/baseline-default-$TS.log"

echo "[bench] default allocator -> $DEFAULT_LOG"
{
  echo "# Ratatosk baseline ($TS)"
  echo "# profile: default allocator"
  "${CARGO_CMD[@]}" bench -p ratatosk-server --bench pipeline -- --sample-size "$SAMPLE_SIZE"
} | tee "$DEFAULT_LOG"

if [[ "$WITH_MIMALLOC" == "1" ]]; then
  MIMALLOC_LOG="$OUT_DIR/baseline-mimalloc-$TS.log"
  echo "[bench] mimalloc allocator -> $MIMALLOC_LOG"
  {
    echo "# Ratatosk baseline ($TS)"
    echo "# profile: mimalloc"
    "${CARGO_CMD[@]}" bench -p ratatosk-server --features mimalloc --bench pipeline -- --sample-size "$SAMPLE_SIZE"
  } | tee "$MIMALLOC_LOG"
fi

echo "[bench] done"
