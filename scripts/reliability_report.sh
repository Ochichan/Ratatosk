#!/usr/bin/env bash
#
# reliability_report.sh — Ratatosk per-release reliability report generator.
#
# Phase 7 of docs/RELEASE_ROADMAP.md. Runs (or records the availability of) the
# evidence that backs Ratatosk's reliability claims and writes a single markdown
# report. The default release policy qualifies only when every known check
# records PASS. Unavailable checks remain explicit SKIPs and make required
# evidence incomplete.
#
# Usage:
#   ./scripts/reliability_report.sh
#   RELIABILITY_RUN_RECOVERY=0 ./scripts/reliability_report.sh
#   RELIABILITY_REQUIRED_CHECKS=id,id,... ./scripts/reliability_report.sh  # scoped
#
# Exit status: 0 = declared required checks passed, 1 = failed/incomplete evidence,
#              2 = invalid required-check policy or report integrity error.
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

KNOWN_CHECK_IDS=(
  format build lint test audit deny gap-ledger strict-mode recovery-matrix
  backup-restore-drill perf-guardrail redis-differential resp-fuzz
)
DEFAULT_REQUIRED_CHECKS="format,build,lint,test,audit,deny,gap-ledger,strict-mode,recovery-matrix,backup-restore-drill,perf-guardrail,redis-differential,resp-fuzz"
REQUIRED_POLICY="${RELIABILITY_REQUIRED_CHECKS-$DEFAULT_REQUIRED_CHECKS}"
POLICY_DISPLAY="$REQUIRED_POLICY"
if [[ -n "${RELIABILITY_REQUIRED_CHECKS+x}" ]]; then
  POLICY_SCOPE="custom"
else
  POLICY_SCOPE="default-release"
fi

declare -a REQUIRED_CHECK_IDS=()
declare -a RESULT_IDS=()
declare -a RESULT_STATUSES=()
declare -a RESULT_TEXTS=()
declare -a RESULT_DETAILS=()
declare -a POLICY_ERRORS=()
declare -a INTEGRITY_ERRORS=()
declare -a MISSING_REQUIRED_IDS=()

is_known_check() {
  local wanted="$1" known
  for known in "${KNOWN_CHECK_IDS[@]}"; do
    [[ "$known" == "$wanted" ]] && return 0
  done
  return 1
}

is_required_check() {
  local wanted="$1" required
  for required in "${REQUIRED_CHECK_IDS[@]}"; do
    [[ "$required" == "$wanted" ]] && return 0
  done
  return 1
}

validate_required_policy() {
  local id

  if [[ "$REQUIRED_POLICY" =~ [[:cntrl:]] ]]; then
    POLICY_DISPLAY="<invalid: contains control characters>"
    POLICY_ERRORS+=("required-check policy must not contain control characters")
    return
  fi
  if [[ -z "$REQUIRED_POLICY" || ",$REQUIRED_POLICY," == *",,"* ]]; then
    POLICY_ERRORS+=("required-check policy must be a non-empty comma-separated list without empty entries")
    return
  fi

  IFS=',' read -r -a REQUIRED_CHECK_IDS <<< "$REQUIRED_POLICY"
  for id in "${REQUIRED_CHECK_IDS[@]}"; do
    if [[ ! "$id" =~ ^[a-z0-9][a-z0-9-]*$ ]]; then
      POLICY_ERRORS+=("invalid required check ID '$id'")
      continue
    fi
    if ! is_known_check "$id"; then
      POLICY_ERRORS+=("unknown required check ID '$id'")
    fi
  done

  local i j
  for ((i = 0; i < ${#REQUIRED_CHECK_IDS[@]}; i++)); do
    for ((j = i + 1; j < ${#REQUIRED_CHECK_IDS[@]}; j++)); do
      if [[ "${REQUIRED_CHECK_IDS[$i]}" == "${REQUIRED_CHECK_IDS[$j]}" ]]; then
        POLICY_ERRORS+=("duplicate required check ID '${REQUIRED_CHECK_IDS[$i]}'")
      fi
    done
  done
}

add_result() { # id status text detail
  local id="$1" status="$2" text="$3" detail="${4:-}" existing

  if ! is_known_check "$id"; then
    INTEGRITY_ERRORS+=("result used unknown check ID '$id'")
    return
  fi
  case "$status" in
    PASS|FAIL|SKIP) ;;
    *)
      INTEGRITY_ERRORS+=("check '$id' used invalid status '$status'")
      return
      ;;
  esac
  for existing in ${RESULT_IDS[@]+"${RESULT_IDS[@]}"}; do
    if [[ "$existing" == "$id" ]]; then
      INTEGRITY_ERRORS+=("check '$id' recorded more than once")
      return
    fi
  done

  RESULT_IDS+=("$id")
  RESULT_STATUSES+=("$status")
  RESULT_TEXTS+=("$text")
  RESULT_DETAILS+=("$detail")
  echo "[reliability][$status][$id] $text${detail:+ — $detail}"
}

has_warning_line() {
  local escape=$'\033'
  LC_ALL=C sed "s/${escape}\\[[0-9;]*m//g" "$1" | grep -Eq \
    '^[[:space:]]*(warning(\[[^]]+\])?:|(WARN|WARNING)([[:space:]:]|$)|\[[^]]*(WARN|WARNING)[^]]*\]|[0-9]{4}-[0-9]{2}-[0-9]{2}T[^[:space:]]+[[:space:]]+(WARN|WARNING)([[:space:]:]|$))'
}

run_step() { # id name cmd...
  local id="$1" name="$2" log command_rc tee_rc warning_rc
  local -a pipe_status
  shift 2
  log="$OUT_DIR/reliability-$id-$TS.log"

  echo "[reliability][RUN][$id] $name -> $log"
  if "$@" 2>&1 | tee "$log"; then
    pipe_status=("${PIPESTATUS[@]}")
  else
    pipe_status=("${PIPESTATUS[@]}")
  fi
  command_rc="${pipe_status[0]}"
  tee_rc="${pipe_status[1]}"

  if ((command_rc != 0)); then
    add_result "$id" FAIL "$name" "exit=$command_rc; log=$log"
  elif ((tee_rc != 0)); then
    add_result "$id" FAIL "$name" "exit=0; log-write-exit=$tee_rc; log=$log"
  elif has_warning_line "$log"; then
    add_result "$id" FAIL "$name" \
      "exit=0; warning-line-detected=warning:/WARN/WARNING; log=$log"
  else
    warning_rc="$?"
    if ((warning_rc > 1)); then
      add_result "$id" FAIL "$name" "exit=0; warning-scan-exit=$warning_rc; log=$log"
    else
      add_result "$id" PASS "$name" "exit=0; log=$log"
    fi
  fi
}

result_status() {
  local wanted="$1" i
  for ((i = 0; i < ${#RESULT_IDS[@]}; i++)); do
    if [[ "${RESULT_IDS[$i]}" == "$wanted" ]]; then
      printf '%s' "${RESULT_STATUSES[$i]}"
      return 0
    fi
  done
  return 1
}

evaluate_results() {
  local required
  for required in "${REQUIRED_CHECK_IDS[@]}"; do
    if ! result_status "$required" >/dev/null; then
      MISSING_REQUIRED_IDS+=("$required")
    fi
  done
}

write_report() {
  local exit_code="$1" overall_state="$2" i label message
  {
    echo "# Ratatosk v$VERSION Reliability Report"
    echo
    echo "_Generated: ${TS}_"
    echo
    echo "Required checks: \`$POLICY_DISPLAY\`"
    echo "Policy scope: \`$POLICY_SCOPE\`"
    if [[ "$POLICY_SCOPE" == "custom" ]]; then
      echo "Policy note: only declared required checks determine this scoped result; optional results are informational."
    fi
    echo
    for ((i = 0; i < ${#RESULT_IDS[@]}; i++)); do
      if is_required_check "${RESULT_IDS[$i]}"; then
        label="required"
      else
        label="optional"
      fi
      printf -- '- %s `%s` (%s): %s%s\n' \
        "${RESULT_STATUSES[$i]}" "${RESULT_IDS[$i]}" "$label" \
        "${RESULT_TEXTS[$i]}" "${RESULT_DETAILS[$i]:+ — ${RESULT_DETAILS[$i]}}"
    done
    for label in ${MISSING_REQUIRED_IDS[@]+"${MISSING_REQUIRED_IDS[@]}"}; do
      echo "- ERROR \`$label\` (required): no result recorded"
    done
    for message in ${POLICY_ERRORS[@]+"${POLICY_ERRORS[@]}"}; do
      echo "- ERROR \`required-policy\`: $message"
    done
    for message in ${INTEGRITY_ERRORS[@]+"${INTEGRITY_ERRORS[@]}"}; do
      echo "- ERROR \`report-integrity\`: $message"
    done
    echo
    case "$exit_code" in
      0)
        if [[ "$POLICY_SCOPE" == "custom" ]]; then
          echo "**Overall: PASS (SCOPED)** — every declared custom check passed; this is not full release qualification."
        else
          echo "**Overall: PASS** — every default release check passed; release evidence is complete."
        fi
        ;;
      1)
        if [[ "$overall_state" == "FAIL" ]]; then
          echo "**Overall: FAIL** — at least one required check failed; not releasable."
        else
          echo "**Overall: INCOMPLETE** — required evidence was skipped or not recorded; not releasable."
        fi
        ;;
      2) echo "**Overall: ERROR** — required-check policy or report integrity is invalid." ;;
    esac
  } >"$REPORT"
}

perf_guardrail_command() {
  local -a pipe_status
  if bash scripts/bench_baseline.sh "$OUT_DIR" 2>&1 | tee "$PERF_LOG"; then
    :
  else
    pipe_status=("${PIPESTATUS[@]}")
    if ((pipe_status[0] != 0)); then
      return "${pipe_status[0]}"
    fi
    return "${pipe_status[1]}"
  fi
  if python3 scripts/perf_guardrail_check.py --log "$PERF_LOG" 2>&1 | tee -a "$PERF_LOG"; then
    :
  else
    pipe_status=("${PIPESTATUS[@]}")
    if ((pipe_status[0] != 0)); then
      return "${pipe_status[0]}"
    fi
    return "${pipe_status[1]}"
  fi
}

validate_required_policy
echo "[reliability] generating report for v$VERSION -> $REPORT"

if ((${#POLICY_ERRORS[@]} > 0)); then
  write_report 2 ERROR
  printf '[reliability][ERROR][required-policy] %s\n' "${POLICY_ERRORS[@]}" >&2
  echo "[reliability] wrote $REPORT"
  exit 2
fi

# 1. Quality gate (fmt/check/clippy/test) — the warning-free baseline.
run_step format "Format gate (cargo fmt --check)" cargo fmt --all --check
run_step build "Build gate (cargo check)" cargo check --workspace --quiet
run_step lint "Lint gate (cargo clippy -D warnings)" cargo clippy --workspace --all-targets -- -D warnings
run_step test "Test gate (cargo test)" cargo test --workspace --quiet

# 2. Supply chain.
if command -v cargo-audit >/dev/null 2>&1 || cargo audit --version >/dev/null 2>&1; then
  run_step audit "Supply chain (cargo audit)" cargo audit
else
  add_result audit SKIP "Supply chain (cargo audit)" "cargo-audit not installed"
fi
if command -v cargo-deny >/dev/null 2>&1 || cargo deny --version >/dev/null 2>&1; then
  run_step deny "License/ban policy (cargo deny check)" cargo deny check
else
  add_result deny SKIP "License/ban policy (cargo deny)" "cargo-deny not installed"
fi

# 3. Command ledger consistency (docs/code/ledger 3-way).
run_step gap-ledger "Gap ledger consistency" python3 scripts/redis_gap_ledger.py check

# 4. Strict-mode regression (product-boundary guard).
run_step strict-mode "Strict-mode regression" bash -c 'cargo test -p ratatosk-engine --lib -- strict_mode'

# 5. Persistence recovery matrix.
if [[ "$RUN_RECOVERY" == "1" ]]; then
  if [[ -x scripts/recovery_matrix.sh ]]; then
    run_step recovery-matrix \
      "AOF/RDB recovery matrix (crash/truncation/missing-manifest/rewrite)" \
      bash scripts/recovery_matrix.sh
  else
    add_result recovery-matrix SKIP "Recovery matrix" "scripts/recovery_matrix.sh not executable"
  fi
else
  add_result recovery-matrix SKIP "Recovery matrix" "disabled via RELIABILITY_RUN_RECOVERY=$RUN_RECOVERY"
fi

# 6. Backup / restore / rollback drill.
if [[ -x scripts/backup_restore_drill.sh ]]; then
  run_step backup-restore-drill "Backup / restore / rollback drill" \
    bash scripts/backup_restore_drill.sh
else
  add_result backup-restore-drill SKIP "Backup/restore/rollback drill" "script not executable"
fi

# 7. Perf guardrail.
if [[ "$RUN_PERF" == "1" ]]; then
  PERF_LOG="$OUT_DIR/reliability-perf-$TS.log"
  run_step perf-guardrail "Perf guardrail (set@256, ping@256)" perf_guardrail_command
else
  add_result perf-guardrail SKIP "Perf guardrail" "disabled via RELIABILITY_RUN_PERF=$RUN_PERF"
fi

# 8. Differential / fuzz are recorded as unavailable until bounded commands are wired.
add_result redis-differential SKIP "Redis/Valkey differential subset" \
  "run via .github/workflows/redis-interop.yml; not executed by this report"
add_result resp-fuzz SKIP "RESP fuzz" "cargo fuzz target not executed by this report"

evaluate_results

exit_code=0
overall_state="PASS"
if ((${#INTEGRITY_ERRORS[@]} > 0)); then
  exit_code=2
  overall_state="ERROR"
elif ((${#MISSING_REQUIRED_IDS[@]} > 0)); then
  exit_code=1
  overall_state="INCOMPLETE"
else
  for required_id in "${REQUIRED_CHECK_IDS[@]}"; do
    result_status_value="$(result_status "$required_id")"
    case "$result_status_value" in
      FAIL)
        exit_code=1
        overall_state="FAIL"
        break
        ;;
      SKIP)
        exit_code=1
        overall_state="INCOMPLETE"
        ;;
    esac
  done
fi

write_report "$exit_code" "$overall_state"
echo "[reliability] wrote $REPORT"
exit "$exit_code"
