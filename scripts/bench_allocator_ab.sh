#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
OUT_DIR="${1:-$ROOT_DIR/benchmarks}"
RUNS="${RUNS:-3}"
SAMPLE_SIZE="${SAMPLE_SIZE:-20}"
BENCH_TARGET="${BENCH_TARGET:-pipeline}"

mkdir -p "$OUT_DIR"
cd "$ROOT_DIR"

if command -v cargo >/dev/null 2>&1; then
  CARGO_CMD=(cargo)
else
  CARGO_CMD=(nix shell nixpkgs#cargo nixpkgs#rustc nixpkgs#pkg-config -c cargo)
fi

echo "[ab] runs=$RUNS sample_size=$SAMPLE_SIZE bench=$BENCH_TARGET out_dir=$OUT_DIR"

run_once() {
  local mode="$1"
  local features=()
  if [[ "$mode" == "mimalloc" ]]; then
    features=(--features mimalloc)
  fi

  local ts
  ts="$(date +%Y%m%d-%H%M%S)"
  local log="$OUT_DIR/ab-${mode}-${BENCH_TARGET}-${ts}.log"

  echo "[ab] ${mode} -> ${log}"
  {
    echo "# Ratatosk allocator A/B ($ts)"
    echo "# mode: ${mode}"
    "${CARGO_CMD[@]}" bench -p ratatosk-server "${features[@]}" --bench "$BENCH_TARGET" -- --sample-size "$SAMPLE_SIZE"
  } | tee "$log"

  if [[ "$BENCH_TARGET" == "pipeline" ]]; then
    echo "[ab] guardrail check (${mode})"
    python3 "$ROOT_DIR/scripts/perf_guardrail_check.py" --log "$log" || true
  fi
}

for run in $(seq 1 "$RUNS"); do
  echo "[ab] run ${run}/${RUNS}"
  run_once "default"
  if ! run_once "mimalloc"; then
    echo "[ab] warning: mimalloc run failed (likely offline dependency fetch issue)"
  fi
done

echo "[ab] done"
