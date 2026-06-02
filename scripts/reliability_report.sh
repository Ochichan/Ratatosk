#!/usr/bin/env bash
#
# reliability_report.sh — Ratatosk per-release reliability report generator.
#
# Phase 7 of docs/RELEASE_ROADMAP.md. Runs (or records the availability of) the
# evidence that backs Ratatosk's reliability claims and writes a single markdown
# report. Each line is PASS / FAIL / SKIP — SKIP is explicit (tool/binary
# missing), never silent. The report is the artifact attached to a release.
#
# Usage:
#   ./scripts/reliability_report.sh                 # run what is available
#   RELIABILITY_RUN_RECOVERY=0 ./scripts/reliability_report.sh   # skip slow drills
#
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT_DIR"
OUT_DIR="${RELIABILITY_OUT_DIR:-$ROOT_DIR/benchmarks}"
TS="$(date +%Y%m%d-%H%M%S)"
VERSION="$(awk -F\" '/^version/ {print $2; exit}' Cargo.toml 2>/dev/null || echo "unknown")"
REPORT="$OUT_DIR/reliability-report-$TS.md"
mkdir -p "$OUT_DIR"

RUN_RECOVERY="${RELIABILITY_RUN_RECOVERY:-1}"
RUN_PERF="${RELIABILITY_RUN_PERF:-1}"

declare -a LINES=()
overall_fail=0

add() { # status text
  local status="$1" text="$2"
  LINES+=("- $status: $text")
  echo "[reliability][$status] $text"
  [[ "$status" == "FAIL" ]] && overall_fail=1 || true
}

run_step() { # name cmd...
  local name="$1"; shift
  if "$@" >/dev/null 2>&1; then
    add PASS "$name"
  else
    add FAIL "$name"
  fi
}

echo "[reliability] generating report for v$VERSION -> $REPORT"

# 1. Quality gate (fmt/clippy/test) — the warning-free baseline.
run_step "Format gate (cargo fmt --check)" cargo fmt --all --check
run_step "Build gate (cargo check)" cargo check --workspace --quiet
run_step "Lint gate (cargo clippy -D warnings)" cargo clippy --workspace --all-targets -- -D warnings
run_step "Test gate (cargo test)" cargo test --workspace --quiet

# 2. Supply chain.
if command -v cargo-audit >/dev/null 2>&1 || cargo audit --version >/dev/null 2>&1; then
  run_step "Supply chain (cargo audit)" cargo audit
else
  add SKIP "Supply chain (cargo audit) — cargo-audit not installed"
fi
if command -v cargo-deny >/dev/null 2>&1 || cargo deny --version >/dev/null 2>&1; then
  run_step "License/ban policy (cargo deny check)" cargo deny check
else
  add SKIP "License/ban policy (cargo deny) — cargo-deny not installed"
fi

# 3. Command ledger consistency (docs/code/ledger 3-way).
run_step "Gap ledger consistency" python3 scripts/redis_gap_ledger.py check

# 4. Strict-mode regression (product-boundary guard).
run_step "Strict-mode regression" bash -c 'cargo test -p ratatosk-engine --lib -- strict_mode'

# 5. Persistence recovery matrix.
if [[ "$RUN_RECOVERY" == "1" ]]; then
  if [[ -x scripts/recovery_matrix.sh ]]; then
    run_step "AOF/RDB recovery matrix (crash/truncation/missing-manifest/rewrite)" \
      bash scripts/recovery_matrix.sh
  else
    add SKIP "Recovery matrix — scripts/recovery_matrix.sh not executable"
  fi
else
  add SKIP "Recovery matrix — disabled via RELIABILITY_RUN_RECOVERY=0"
fi

# 6. Backup / restore / rollback drill.
if [[ -x scripts/backup_restore_drill.sh ]]; then
  run_step "Backup / restore / rollback drill" bash scripts/backup_restore_drill.sh
else
  add SKIP "Backup/restore/rollback drill — script not present yet"
fi

# 7. Perf guardrail.
if [[ "$RUN_PERF" == "1" ]]; then
  PERF_LOG="$OUT_DIR/reliability-perf-$TS.log"
  if bash scripts/bench_baseline.sh "$OUT_DIR" >"$PERF_LOG" 2>&1 \
    && python3 scripts/perf_guardrail_check.py --log "$PERF_LOG" >/dev/null 2>&1; then
    add PASS "Perf guardrail (set@256, ping@256)"
  else
    add FAIL "Perf guardrail (set@256, ping@256) — see $PERF_LOG"
  fi
else
  add SKIP "Perf guardrail — disabled via RELIABILITY_RUN_PERF=0"
fi

# 8. Differential / fuzz (Phase 5 — recorded as skip until wired into this host).
add SKIP "Redis/Valkey differential subset — run via .github/workflows/redis-interop.yml"
add SKIP "RESP fuzz — run via cargo fuzz target (Phase 5)"

# --- write report ----------------------------------------------------------
{
  echo "# Ratatosk v$VERSION Reliability Report"
  echo
  echo "_Generated: $TS_"
  echo
  printf '%s\n' "${LINES[@]}"
  echo
  if ((overall_fail == 0)); then
    echo "**Overall: PASS** (no FAIL lines; SKIP entries are explicit and external-tool-gated)."
  else
    echo "**Overall: FAIL** — at least one gate failed; not releasable."
  fi
} >"$REPORT"

echo "[reliability] wrote $REPORT"
exit "$overall_fail"
