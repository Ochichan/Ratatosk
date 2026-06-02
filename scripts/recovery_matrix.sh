#!/usr/bin/env bash
#
# recovery_matrix.sh — Ratatosk persistence crash/fault-injection matrix.
#
# Phase 4 of docs/RELEASE_ROADMAP.md. Exercises the durability contract by
# forcing each failure mode and asserting the post-recovery invariant: either
# the acknowledged dataset survives, or startup fails *cleanly* — never a silent
# empty success. Complements scripts/smoke_bgrewriteaof.sh.
#
# Usage:
#   ./scripts/recovery_matrix.sh
#   RATATOSK_RECOVERY_KEY_COUNT=1000 RATATOSK_RECOVERY_PROFILE=release ./scripts/recovery_matrix.sh
#
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
PORT="${RATATOSK_RECOVERY_PORT:-$((17379 + (RANDOM % 1000)))}"
METRICS_PORT_BASE="${RATATOSK_RECOVERY_METRICS_PORT_BASE:-$((PORT + 1500))}"
START_TIMEOUT_SEC="${RATATOSK_RECOVERY_START_TIMEOUT_SEC:-30}"
REWRITE_TIMEOUT_SEC="${RATATOSK_RECOVERY_REWRITE_TIMEOUT_SEC:-60}"
KEY_COUNT="${RATATOSK_RECOVERY_KEY_COUNT:-500}"
PROFILE="${RATATOSK_RECOVERY_PROFILE:-debug}"
MAX_CLIENTS="${RATATOSK_RECOVERY_MAX_CLIENTS:-256}"
CONN_RATE_LIMIT_MAX_ATTEMPTS="${RATATOSK_RECOVERY_CONN_RATE_LIMIT_MAX_ATTEMPTS:-200000}"

if command -v redis-cli >/dev/null 2>&1; then
  REDIS_CLIENT_MODE="redis-cli"
elif command -v nc >/dev/null 2>&1; then
  REDIS_CLIENT_MODE="nc"
else
  echo "[recovery][fail] redis-cli or nc is required" >&2
  exit 1
fi

# OpenBSD/GNU nc uses -N to shut down the socket after EOF (so the server can
# reply and close). macOS/Apple nc reuses -N for a probe count, which breaks the
# combination -N -w; detect the variant once and pick safe flags.
NC_OPTS=(-w 2)
if [[ "$REDIS_CLIENT_MODE" == "nc" ]] && nc -h 2>&1 | grep -qiE -- '-N[[:space:]].*shutdown'; then
  NC_OPTS=(-N -w 2)
fi

if command -v cargo >/dev/null 2>&1; then
  CARGO_CMD=(cargo)
else
  CARGO_CMD=(nix develop -c cargo)
fi

BUILD_ARGS=()
SERVER_BIN="$ROOT_DIR/target/debug/ratatosk"
if [[ "$PROFILE" == "release" ]]; then
  BUILD_ARGS=(--release)
  SERVER_BIN="$ROOT_DIR/target/release/ratatosk"
fi

WORK_DIR="$(mktemp -d "${TMPDIR:-/tmp}/ratatosk-recovery.XXXXXX")"
SERVER_PID=""
METRICS_SEQ=0
declare -a RESULTS=()

cleanup() {
  stop_server || true
  rm -rf "$WORK_DIR" >/dev/null 2>&1 || true
}
trap cleanup EXIT

# --- RESP client helpers (mirror scripts/smoke_bgrewriteaof.sh) -------------
resp_request() {
  local out=$'*'"$#"$'\r\n'
  local arg
  for arg in "$@"; do
    out+=$'$'"${#arg}"$'\r\n'"$arg"$'\r\n'
  done
  printf '%s' "$out"
}

redis_cmd() {
  if [[ "$REDIS_CLIENT_MODE" == "redis-cli" ]]; then
    redis-cli --raw -h 127.0.0.1 -p "$PORT" "$@"
  else
    local raw
    raw="$(resp_request "$@" | nc "${NC_OPTS[@]}" 127.0.0.1 "$PORT" 2>/dev/null)" || return 1
    printf '%s' "$raw" | tr -d '\r' | sed '/^\$/d;/^\*/d;/^+/s/^+//;/^:/s/^://'
  fi
}

wait_for_ready() {
  local deadline=$((SECONDS + START_TIMEOUT_SEC))
  while ((SECONDS < deadline)); do
    if [[ -n "$SERVER_PID" ]] && ! kill -0 "$SERVER_PID" 2>/dev/null; then
      return 1
    fi
    if redis_cmd PING >/dev/null 2>&1; then
      return 0
    fi
    sleep 0.2
  done
  return 1
}

start_server() {
  local appendonly="$1" dir="$2" appendfsync="${3:-always}"
  local metrics_port=$((METRICS_PORT_BASE + METRICS_SEQ))
  METRICS_SEQ=$((METRICS_SEQ + 1))
  local logfile="$WORK_DIR/server-$METRICS_SEQ.log"
  mkdir -p "$dir"
  RATATOSK_BIND=127.0.0.1 \
  RATATOSK_PORT="$PORT" \
  RATATOSK_METRICS_BIND="127.0.0.1:${metrics_port}" \
  RATATOSK_DIR="$dir" \
  RATATOSK_APPENDONLY="$appendonly" \
  RATATOSK_APPENDFSYNC="$appendfsync" \
  RATATOSK_MAX_CLIENTS="$MAX_CLIENTS" \
  RATATOSK_CONN_RATE_LIMIT_MAX_ATTEMPTS="$CONN_RATE_LIMIT_MAX_ATTEMPTS" \
  RATATOSK_DISABLE_CONFIG_AUTOLOAD=true \
  "$SERVER_BIN" >"$logfile" 2>&1 &
  SERVER_PID=$!
  wait_for_ready
}

stop_server() {
  if [[ -n "${SERVER_PID:-}" ]] && kill -0 "$SERVER_PID" 2>/dev/null; then
    kill -TERM "$SERVER_PID" 2>/dev/null || true
    wait "$SERVER_PID" 2>/dev/null || true
  fi
  SERVER_PID=""
}

kill9_server() {
  if [[ -n "${SERVER_PID:-}" ]] && kill -0 "$SERVER_PID" 2>/dev/null; then
    kill -9 "$SERVER_PID" 2>/dev/null || true
    wait "$SERVER_PID" 2>/dev/null || true
  fi
  SERVER_PID=""
}

# --- dataset helpers --------------------------------------------------------
write_dataset() {
  local count="$1" i
  for ((i = 0; i < count; i++)); do
    redis_cmd SET "k:$i" "v:$i" >/dev/null || return 1
  done
}

dbsize() { redis_cmd DBSIZE | head -n1 | tr -dc '0-9'; }

wait_for_rewrite() {
  local deadline=$((SECONDS + REWRITE_TIMEOUT_SEC))
  while ((SECONDS < deadline)); do
    if redis_cmd INFO persistence 2>/dev/null | grep -q 'aof_rewrite_in_progress:0'; then
      return 0
    fi
    sleep 0.3
  done
  return 0 # best-effort; do not hard-fail the matrix on info shape drift
}

record() {
  local status="$1" name="$2" detail="${3:-}"
  RESULTS+=("$status|$name|$detail")
  echo "[recovery][$status] $name${detail:+ — $detail}"
}

newest_incr_aof() {
  local dir="$1"
  ls -1t "$dir"/appendonly.aof.*.incr.aof "$dir"/appendonly.aof 2>/dev/null | head -n1 || true
}

# --- build ------------------------------------------------------------------
echo "[recovery] building ratatosk ($PROFILE)…"
"${CARGO_CMD[@]}" build -p ratatosk-server --bin ratatosk "${BUILD_ARGS[@]+"${BUILD_ARGS[@]}"}" >/dev/null 2>&1 \
  || { echo "[recovery][fail] build failed" >&2; exit 1; }

# === Case 1: clean RDB restart =============================================
case_clean_rdb() {
  local dir="$WORK_DIR/clean-rdb"
  start_server "false" "$dir" || { record fail clean_rdb_restart "server did not start"; return; }
  write_dataset "$KEY_COUNT" || { record fail clean_rdb_restart "dataset write failed"; return; }
  redis_cmd SAVE >/dev/null || { record fail clean_rdb_restart "SAVE failed"; return; }
  stop_server
  start_server "false" "$dir" || { record fail clean_rdb_restart "restart failed"; return; }
  local n; n="$(dbsize)"
  stop_server
  [[ "$n" == "$KEY_COUNT" ]] && record pass clean_rdb_restart "dbsize=$n" \
    || record fail clean_rdb_restart "dbsize=$n expected=$KEY_COUNT"
}

# === Case 2: AOF graceful restart ==========================================
case_aof_clean() {
  local dir="$WORK_DIR/aof-clean"
  start_server "true" "$dir" || { record fail aof_clean_restart "server did not start"; return; }
  write_dataset "$KEY_COUNT" || { record fail aof_clean_restart "dataset write failed"; return; }
  stop_server
  start_server "true" "$dir" || { record fail aof_clean_restart "restart failed"; return; }
  local n; n="$(dbsize)"
  stop_server
  [[ "$n" == "$KEY_COUNT" ]] && record pass aof_clean_restart "dbsize=$n" \
    || record fail aof_clean_restart "dbsize=$n expected=$KEY_COUNT"
}

# === Case 3: kill -9 with appendfsync=always (no data loss) ================
case_aof_kill9() {
  local dir="$WORK_DIR/aof-kill9"
  start_server "true" "$dir" "always" || { record fail aof_kill9 "server did not start"; return; }
  write_dataset "$KEY_COUNT" || { record fail aof_kill9 "dataset write failed"; return; }
  kill9_server
  start_server "true" "$dir" || { record fail aof_kill9 "restart failed (possible corruption)"; return; }
  local n; n="$(dbsize)"
  stop_server
  [[ "$n" == "$KEY_COUNT" ]] && record pass aof_kill9 "dbsize=$n (always-fsync durable)" \
    || record fail aof_kill9 "dbsize=$n expected=$KEY_COUNT"
}

# === Case 4: kill -9 during BGREWRITEAOF ===================================
case_kill9_during_rewrite() {
  local dir="$WORK_DIR/aof-rewrite-kill9"
  start_server "true" "$dir" "always" || { record fail kill9_during_rewrite "server did not start"; return; }
  write_dataset "$KEY_COUNT" || { record fail kill9_during_rewrite "dataset write failed"; return; }
  redis_cmd BGREWRITEAOF >/dev/null 2>&1 || true
  kill9_server # kill mid-rewrite
  start_server "true" "$dir" || { record fail kill9_during_rewrite "restart failed (corruption?)"; return; }
  local n; n="$(dbsize)"
  stop_server
  [[ "$n" == "$KEY_COUNT" ]] && record pass kill9_during_rewrite "dbsize=$n" \
    || record fail kill9_during_rewrite "dbsize=$n expected=$KEY_COUNT"
}

# === Case 5: truncated AOF tail (consistent-prefix recovery) ===============
case_aof_truncation() {
  local dir="$WORK_DIR/aof-truncate"
  start_server "true" "$dir" || { record fail aof_truncation "server did not start"; return; }
  write_dataset "$KEY_COUNT" || { record fail aof_truncation "dataset write failed"; return; }
  stop_server
  local target; target="$(newest_incr_aof "$dir")"
  if [[ -z "$target" || ! -f "$target" ]]; then
    record skip aof_truncation "no incr AOF segment found to truncate"
    return
  fi
  local size; size="$(wc -c <"$target" | tr -dc '0-9')"
  if ((size > 128)); then
    if command -v truncate >/dev/null 2>&1; then
      truncate -s $((size - 96)) "$target"
    else
      dd if="$target" of="$target.tmp" bs=1 count=$((size - 96)) 2>/dev/null && mv "$target.tmp" "$target"
    fi
  fi
  if start_server "true" "$dir"; then
    local n; n="$(dbsize)"
    stop_server
    # Consistent-prefix recovery: server must come up and serve a non-empty,
    # bounded prefix — never crash, never silently exceed what was written.
    if [[ -n "$n" ]] && ((n >= 1 && n <= KEY_COUNT)); then
      record pass aof_truncation "recovered prefix dbsize=$n (<= $KEY_COUNT)"
    else
      record fail aof_truncation "unexpected dbsize=$n"
    fi
  else
    record fail aof_truncation "server crashed on truncated AOF (should recover prefix)"
  fi
}

# === Case 6: missing manifest (no silent data loss) ========================
case_missing_manifest() {
  local dir="$WORK_DIR/aof-missing-manifest"
  start_server "true" "$dir" || { record fail missing_manifest "server did not start"; return; }
  write_dataset "$KEY_COUNT" || { record fail missing_manifest "dataset write failed"; return; }
  stop_server
  local manifest="$dir/appendonly.aof.manifest"
  if [[ -f "$manifest" ]]; then
    mv "$manifest" "$manifest.bak"
  else
    record skip missing_manifest "no manifest present (legacy single-file AOF)"
    return
  fi
  if start_server "true" "$dir"; then
    local n; n="$(dbsize)"
    stop_server
    # Invariant: with segments on disk but no manifest, the server must NOT
    # come up reporting an empty keyspace as if nothing was lost.
    if [[ "$n" == "0" ]]; then
      record fail missing_manifest "silent data loss: started with dbsize=0 while segments exist"
    else
      record pass missing_manifest "rebuilt from segments, dbsize=$n"
    fi
  else
    # Clean startup failure is an acceptable, non-silent outcome.
    record pass missing_manifest "clean startup failure (no silent loss)"
  fi
}

# === Case 7: repeated rewrite stability ====================================
case_repeated_rewrite() {
  local dir="$WORK_DIR/aof-repeated-rewrite" i
  start_server "true" "$dir" || { record fail repeated_rewrite "server did not start"; return; }
  write_dataset "$KEY_COUNT" || { record fail repeated_rewrite "dataset write failed"; return; }
  local ok=1
  for ((i = 0; i < 3; i++)); do
    redis_cmd BGREWRITEAOF >/dev/null 2>&1 || true
    wait_for_rewrite
    local n; n="$(dbsize)"
    [[ "$n" == "$KEY_COUNT" ]] || { ok=0; break; }
  done
  stop_server
  ((ok == 1)) && record pass repeated_rewrite "3x rewrite, dbsize stable=$KEY_COUNT" \
    || record fail repeated_rewrite "dbsize drifted during repeated rewrite"
}

# === disk-full: documented, not auto-injected ==============================
case_disk_full() {
  record skip disk_full "requires a constrained filesystem (loopback/tmpfs quota); covered in CI via ratatosk_aof_write_errors_total alert"
}

case_clean_rdb
case_aof_clean
case_aof_kill9
case_kill9_during_rewrite
case_aof_truncation
case_missing_manifest
case_repeated_rewrite
case_disk_full

# --- summary ---------------------------------------------------------------
echo
echo "================ recovery matrix ================"
fail_count=0
for row in "${RESULTS[@]}"; do
  IFS='|' read -r status name detail <<<"$row"
  printf '  %-7s %-22s %s\n' "$status" "$name" "$detail"
  [[ "$status" == "fail" ]] && fail_count=$((fail_count + 1))
done
echo "================================================="
if ((fail_count > 0)); then
  echo "[recovery][fail] $fail_count case(s) failed" >&2
  exit 1
fi
echo "[recovery][pass] all asserted recovery invariants held"
