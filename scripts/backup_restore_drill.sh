#!/usr/bin/env bash
set -euo pipefail

# Phase 4 backup / restore / rollback drill for Ratatosk.
#
# Flow:
#   1. Build the server (debug by default).
#   2. Start it on a random port against a temp DATA dir.
#   3. Write a deterministic dataset, SAVE + BGREWRITEAOF, verify persistence files.
#   4. BACKUP: copy the data dir aside.
#   5. Simulate loss: stop, wipe data dir, RESTORE from backup, restart, verify intact.
#   6. ROLLBACK: take a second backup, mutate data, stop, restore the FIRST backup,
#      restart, verify the pre-mutation state.
#
# Prints '[drill][pass]' / '[drill][fail]' per stage and exits nonzero on mismatch.

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
PORT="${RATATOSK_DRILL_PORT:-$((18379 + (RANDOM % 1000)))}"
METRICS_PORT="${RATATOSK_DRILL_METRICS_PORT:-$((PORT + 1000))}"
START_TIMEOUT_SEC="${RATATOSK_DRILL_START_TIMEOUT_SEC:-30}"
PERSIST_TIMEOUT_SEC="${RATATOSK_DRILL_PERSIST_TIMEOUT_SEC:-60}"
KEY_COUNT="${RATATOSK_DRILL_KEY_COUNT:-500}"
SAMPLE_COUNT="${RATATOSK_DRILL_SAMPLE_COUNT:-25}"
PROFILE="${RATATOSK_DRILL_PROFILE:-debug}"
MAX_CLIENTS="${RATATOSK_DRILL_MAX_CLIENTS:-128}"
CONN_RATE_LIMIT_MAX_ATTEMPTS="${RATATOSK_DRILL_CONN_RATE_LIMIT_MAX_ATTEMPTS:-20000}"
DBFILENAME="dump.rdb"
AOF_MANIFEST="appendonly.aof.manifest"

if command -v redis-cli >/dev/null 2>&1; then
  REDIS_CLIENT_MODE="redis-cli"
elif command -v nc >/dev/null 2>&1; then
  REDIS_CLIENT_MODE="nc"
else
  echo "[drill][fail] redis-cli or nc is required" >&2
  exit 1
fi

# OpenBSD/GNU nc uses -N to shut down the socket after EOF; macOS/Apple nc reuses
# -N for a probe count (which breaks -N -w). Detect the variant once.
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

WORK_DIR="$(mktemp -d "${TMPDIR:-/tmp}/ratatosk-backup-restore-drill.XXXXXX")"
DATA_DIR="$WORK_DIR/data"
BACKUP_A="$WORK_DIR/backup-a"
BACKUP_B="$WORK_DIR/backup-b"
SERVER_PID=""
CURRENT_LOGFILE=""

cleanup() {
  if [[ -n "${SERVER_PID:-}" ]] && kill -0 "$SERVER_PID" >/dev/null 2>&1; then
    kill -TERM "$SERVER_PID" >/dev/null 2>&1 || true
    wait "$SERVER_PID" >/dev/null 2>&1 || true
  fi
  rm -rf "$WORK_DIR" >/dev/null 2>&1 || true
}
trap cleanup EXIT

fail() {
  echo "[drill][fail] $*" >&2
  if [[ -n "$CURRENT_LOGFILE" && -f "$CURRENT_LOGFILE" ]]; then
    echo "[drill] --- server log tail ($CURRENT_LOGFILE) ---" >&2
    tail -n 60 "$CURRENT_LOGFILE" >&2 || true
  fi
  exit 1
}

pass() {
  echo "[drill][pass] $*"
}

assert_eq() {
  local got="$1"
  local want="$2"
  local context="$3"
  if [[ "$got" != "$want" ]]; then
    fail "$context (got='$got', want='$want')"
  fi
}

# Deterministic value for a given key index, so restores are exactly verifiable.
expected_value() {
  printf 'drill-value-%06d-payload' "$1"
}

resp_request() {
  local argc="$#"
  printf '*%s\r\n' "$argc"
  for arg in "$@"; do
    printf '$%s\r\n%s\r\n' "${#arg}" "$arg"
  done
}

redis_cmd_nc() {
  local raw first first_char
  raw="$(resp_request "$@" | nc "${NC_OPTS[@]}" 127.0.0.1 "$PORT" 2>/dev/null)" || return 1
  first="$(printf '%s' "$raw" | head -n 1 | tr -d '\r')"
  first_char="${first:0:1}"

  case "$first_char" in
    '+'|'-'|':')
      printf '%s\n' "${first:1}"
      ;;
    '$')
      if [[ "$first" == '$-1' ]]; then
        printf '\n'
      else
        printf '%s' "$raw" | tail -n +2 | sed 's/\r$//'
      fi
      ;;
    '*')
      printf '%s\n' "$raw" | tr -d '\r'
      ;;
    *)
      return 1
      ;;
  esac
}

redis_cmd() {
  local attempts=0
  while (( attempts < 5 )); do
    if [[ "$REDIS_CLIENT_MODE" == "redis-cli" ]]; then
      if redis-cli --raw -h 127.0.0.1 -p "$PORT" "$@"; then
        return 0
      fi
    else
      if redis_cmd_nc "$@"; then
        return 0
      fi
    fi
    attempts=$((attempts + 1))
    sleep 0.05
  done
  return 1
}

info_field() {
  local key="$1"
  redis_cmd INFO persistence | awk -F: -v key="$key" '
    $1 == key {
      gsub("\r", "", $2);
      print $2;
      exit;
    }
  '
}

wait_for_ready() {
  local deadline=$((SECONDS + START_TIMEOUT_SEC))
  while (( SECONDS < deadline )); do
    if redis_cmd PING >/dev/null 2>&1; then
      return 0
    fi
    if ! kill -0 "$SERVER_PID" >/dev/null 2>&1; then
      fail "server exited during startup"
    fi
    sleep 0.2
  done
  fail "server did not become ready within ${START_TIMEOUT_SEC}s"
}

start_server() {
  local dir="$1"
  local logfile="$2"

  CURRENT_LOGFILE="$logfile"
  RATATOSK_BIND=127.0.0.1 \
  RATATOSK_PORT="$PORT" \
  RATATOSK_METRICS_BIND="127.0.0.1:${METRICS_PORT}" \
  RATATOSK_DIR="$dir" \
  RATATOSK_APPENDONLY=true \
  RATATOSK_MAX_CLIENTS="$MAX_CLIENTS" \
  RATATOSK_CONN_RATE_LIMIT_MAX_ATTEMPTS="$CONN_RATE_LIMIT_MAX_ATTEMPTS" \
  RATATOSK_APPENDFSYNC=always \
  "$SERVER_BIN" >"$logfile" 2>&1 &
  SERVER_PID="$!"
  wait_for_ready
}

stop_server() {
  if [[ -n "${SERVER_PID:-}" ]] && kill -0 "$SERVER_PID" >/dev/null 2>&1; then
    kill -TERM "$SERVER_PID" >/dev/null 2>&1 || true
    wait "$SERVER_PID" >/dev/null 2>&1 || true
  fi
  SERVER_PID=""
}

# Write KEY_COUNT keys with deterministic values.
populate_dataset() {
  local i value
  for i in $(seq 1 "$KEY_COUNT"); do
    value="$(expected_value "$i")"
    if ! redis_cmd SET "drill:key:$i" "$value" >/dev/null; then
      fail "SET failed while populating dataset at key index $i"
    fi
  done
}

# Force both persistence representations to disk and wait for AOF rewrite to settle.
persist_to_disk() {
  local save_reply rewrite_reply
  if ! save_reply="$(redis_cmd SAVE)"; then
    fail "SAVE request failed"
  fi
  assert_eq "$save_reply" "OK" "SAVE should report OK"

  if ! rewrite_reply="$(redis_cmd BGREWRITEAOF)"; then
    fail "BGREWRITEAOF request failed"
  fi
  if [[ "$rewrite_reply" != "Background append only file rewriting started" ]]; then
    fail "unexpected BGREWRITEAOF reply: '$rewrite_reply'"
  fi

  local deadline=$((SECONDS + PERSIST_TIMEOUT_SEC))
  local state status
  while (( SECONDS < deadline )); do
    state="$(info_field aof_rewrite_in_progress || true)"
    status="$(info_field aof_last_rewrite_status || true)"
    if [[ "$state" == "0" && "$status" == "ok" ]]; then
      return 0
    fi
    sleep 0.2
  done
  fail "AOF rewrite did not complete within ${PERSIST_TIMEOUT_SEC}s"
}

verify_persistence_files() {
  local dir="$1"
  if [[ ! -s "$dir/$DBFILENAME" ]]; then
    fail "expected RDB file '$dir/$DBFILENAME' to exist and be non-empty"
  fi
  if [[ ! -s "$dir/$AOF_MANIFEST" ]]; then
    fail "expected AOF manifest '$dir/$AOF_MANIFEST' to exist and be non-empty"
  fi
}

# Verify DBSIZE plus a deterministic sample of keys against expected values.
verify_dataset() {
  local label="$1"
  local dbsize
  if ! dbsize="$(redis_cmd DBSIZE)"; then
    fail "$label: DBSIZE request failed"
  fi
  assert_eq "$dbsize" "$KEY_COUNT" "$label: DBSIZE should match populated key count"

  local sample step i value want
  step=$(( KEY_COUNT / SAMPLE_COUNT ))
  (( step < 1 )) && step=1
  sample=0
  for (( i = 1; i <= KEY_COUNT; i += step )); do
    want="$(expected_value "$i")"
    if ! value="$(redis_cmd GET "drill:key:$i")"; then
      fail "$label: GET failed for drill:key:$i"
    fi
    assert_eq "$value" "$want" "$label: value mismatch for drill:key:$i"
    sample=$((sample + 1))
  done
  echo "[drill] $label: DBSIZE=$dbsize, sampled $sample keys OK"
}

backup_data_dir() {
  local dest="$1"
  rm -rf "$dest"
  mkdir -p "$dest"
  # Trailing '/.' copies directory contents (including dotfiles) into dest.
  cp -a "$DATA_DIR/." "$dest/"
}

restore_data_dir() {
  local src="$1"
  rm -rf "$DATA_DIR"
  mkdir -p "$DATA_DIR"
  cp -a "$src/." "$DATA_DIR/"
}

# ---------------------------------------------------------------------------

echo "[drill] using client mode: $REDIS_CLIENT_MODE"
echo "[drill] workdir: $WORK_DIR"
echo "[drill] port: $PORT, keys: $KEY_COUNT"

echo "[drill] building ratatosk-server ($PROFILE profile)"
if ! "${CARGO_CMD[@]}" build -p ratatosk-server --bin ratatosk "${BUILD_ARGS[@]+"${BUILD_ARGS[@]}"}"; then
  fail "build failed"
fi
pass "stage 0: build"

mkdir -p "$DATA_DIR"

# --- Stage 1: seed + persist + verify on-disk files ------------------------
start_server "$DATA_DIR" "$WORK_DIR/server-seed.log"
populate_dataset
verify_dataset "seed"
persist_to_disk
verify_persistence_files "$DATA_DIR"
stop_server
pass "stage 1: seed dataset, SAVE + BGREWRITEAOF, persistence files present"

# --- Stage 2: BACKUP (first backup, the rollback target) -------------------
backup_data_dir "$BACKUP_A"
verify_persistence_files "$BACKUP_A"
pass "stage 2: backup A captured from data dir"

# --- Stage 3: simulate loss -> RESTORE A -> verify intact ------------------
rm -rf "$DATA_DIR"
if [[ -e "$DATA_DIR" ]]; then
  fail "data dir wipe did not remove '$DATA_DIR'"
fi
restore_data_dir "$BACKUP_A"
verify_persistence_files "$DATA_DIR"
start_server "$DATA_DIR" "$WORK_DIR/server-restore-a.log"
verify_dataset "restore-A"
pass "stage 3: data loss simulated, restored from backup A, dataset intact"

# --- Stage 4: second backup, then mutate so rollback is observable ---------
# Persist current (still-pristine) state, then snapshot it as backup B.
persist_to_disk
stop_server
backup_data_dir "$BACKUP_B"
verify_persistence_files "$BACKUP_B"

start_server "$DATA_DIR" "$WORK_DIR/server-mutate.log"
# Mutate: overwrite half the keys and add new ones so DBSIZE and values drift.
mutated=0
for (( i = 1; i <= KEY_COUNT; i += 2 )); do
  if ! redis_cmd SET "drill:key:$i" "MUTATED-$i" >/dev/null; then
    fail "mutation SET failed for drill:key:$i"
  fi
  mutated=$((mutated + 1))
done
for (( i = 1; i <= 50; i += 1 )); do
  if ! redis_cmd SET "drill:extra:$i" "extra-$i" >/dev/null; then
    fail "mutation SET failed for drill:extra:$i"
  fi
done
persist_to_disk

# Confirm the mutation actually changed observable state before we roll back.
mutated_dbsize="$(redis_cmd DBSIZE)"
if [[ "$mutated_dbsize" == "$KEY_COUNT" ]]; then
  fail "mutation did not change DBSIZE (still $mutated_dbsize); rollback would be meaningless"
fi
mutated_sample="$(redis_cmd GET drill:key:1)"
if [[ "$mutated_sample" == "$(expected_value 1)" ]]; then
  fail "mutation did not change drill:key:1; rollback would be meaningless"
fi
stop_server
echo "[drill] mutation applied: DBSIZE=$mutated_dbsize (was $KEY_COUNT), drill:key:1='$mutated_sample'"
pass "stage 4: backup B captured, dataset mutated and re-persisted"

# --- Stage 5: ROLLBACK -> restore FIRST backup (A) -> verify pristine ------
restore_data_dir "$BACKUP_A"
verify_persistence_files "$DATA_DIR"
start_server "$DATA_DIR" "$WORK_DIR/server-rollback.log"
verify_dataset "rollback-A"
# Extra keys from the mutation must be gone after rolling back to backup A.
extra_probe="$(redis_cmd GET drill:extra:1)"
if [[ -n "$extra_probe" ]]; then
  fail "rollback left mutated key drill:extra:1 present (got='$extra_probe')"
fi
stop_server
pass "stage 5: rolled back to backup A, pre-mutation state restored"

echo "[drill][pass] all stages passed"
echo "[drill] PASS"
