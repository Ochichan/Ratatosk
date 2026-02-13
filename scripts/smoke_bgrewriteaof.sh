#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
PORT="${RATATOSK_SMOKE_PORT:-$((16379 + (RANDOM % 1000)))}"
METRICS_PORT_BASE="${RATATOSK_SMOKE_METRICS_PORT_BASE:-$((PORT + 1000))}"
START_TIMEOUT_SEC="${RATATOSK_SMOKE_START_TIMEOUT_SEC:-30}"
REWRITE_TIMEOUT_SEC="${RATATOSK_SMOKE_REWRITE_TIMEOUT_SEC:-60}"
KEY_COUNT="${RATATOSK_SMOKE_KEY_COUNT:-2500}"
PAYLOAD_BYTES="${RATATOSK_SMOKE_PAYLOAD_BYTES:-256}"
PROFILE="${RATATOSK_SMOKE_PROFILE:-debug}"
MAX_CLIENTS="${RATATOSK_SMOKE_MAX_CLIENTS:-128}"
CONN_RATE_LIMIT_MAX_ATTEMPTS="${RATATOSK_SMOKE_CONN_RATE_LIMIT_MAX_ATTEMPTS:-20000}"

if command -v redis-cli >/dev/null 2>&1; then
  REDIS_CLIENT_MODE="redis-cli"
elif command -v nc >/dev/null 2>&1; then
  REDIS_CLIENT_MODE="nc"
else
  echo "[smoke][fail] redis-cli or nc is required" >&2
  exit 1
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

WORK_DIR="$(mktemp -d "${TMPDIR:-/tmp}/ratatosk-bgrewriteaof-smoke.XXXXXX")"
SERVER_PID=""
CURRENT_LOGFILE=""

cleanup() {
  if [[ -n "${SERVER_PID:-}" ]] && kill -0 "$SERVER_PID" >/dev/null 2>&1; then
    kill -TERM "$SERVER_PID" >/dev/null 2>&1 || true
    wait "$SERVER_PID" >/dev/null 2>&1 || true
  fi
}
trap cleanup EXIT

fail() {
  echo "[smoke][fail] $*" >&2
  if [[ -n "$CURRENT_LOGFILE" && -f "$CURRENT_LOGFILE" ]]; then
    echo "[smoke] --- server log tail ($CURRENT_LOGFILE) ---" >&2
    tail -n 60 "$CURRENT_LOGFILE" >&2 || true
  fi
  exit 1
}

assert_eq() {
  local got="$1"
  local want="$2"
  local context="$3"
  if [[ "$got" != "$want" ]]; then
    fail "$context (got='$got', want='$want')"
  fi
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
  raw="$(resp_request "$@" | nc -N -w 1 127.0.0.1 "$PORT" 2>/dev/null)" || return 1
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
      # Not needed in this smoke path, but return raw payload for debugging.
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
  local appendonly="$1"
  local dir="$2"
  local metrics_port="$3"
  local logfile="$4"

  CURRENT_LOGFILE="$logfile"
  RATATOSK_BIND=127.0.0.1 \
  RATATOSK_PORT="$PORT" \
  RATATOSK_METRICS_BIND="127.0.0.1:${metrics_port}" \
  RATATOSK_DIR="$dir" \
  RATATOSK_APPENDONLY="$appendonly" \
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

populate_dataset() {
  local payload
  payload="$(head -c "$PAYLOAD_BYTES" /dev/zero | tr '\0' 'x')"
  for i in $(seq 1 "$KEY_COUNT"); do
    if ! redis_cmd SET "smoke:key:$i" "$payload" >/dev/null; then
      fail "SET failed while preparing rewrite dataset at key index $i"
    fi
  done
}

echo "[smoke] using client mode: $REDIS_CLIENT_MODE"
echo "[smoke] building ratatosk-server ($PROFILE profile)"
"${CARGO_CMD[@]}" build -p ratatosk-server --bin ratatosk "${BUILD_ARGS[@]}"

echo "[smoke] scenario 1/2: appendonly=true (BGREWRITEAOF success path)"
AOF_ON_DIR="$WORK_DIR/aof-on"
mkdir -p "$AOF_ON_DIR"
start_server "true" "$AOF_ON_DIR" "$METRICS_PORT_BASE" "$WORK_DIR/server-aof-on.log"
if ! ping_reply="$(redis_cmd PING)"; then
  fail "PING failed with appendonly enabled"
fi
assert_eq "$ping_reply" "PONG" "PING should succeed with appendonly enabled"

populate_dataset

if ! first_reply="$(redis_cmd BGREWRITEAOF)"; then
  fail "BGREWRITEAOF request failed on appendonly=true instance"
fi
assert_eq \
  "$first_reply" \
  "Background append only file rewriting started" \
  "first BGREWRITEAOF should start rewrite"

in_progress_seen=0
for _ in $(seq 1 40); do
  rewrite_state="$(info_field aof_rewrite_in_progress || true)"
  if [[ "$rewrite_state" == "1" ]]; then
    if ! second_reply="$(redis_cmd BGREWRITEAOF)"; then
      fail "second BGREWRITEAOF request failed while probing in-progress gate"
    fi
    if [[ "$second_reply" == "ERR Background AOF rewrite already in progress" ]]; then
      in_progress_seen=1
    elif [[ "$second_reply" == "Background append only file rewriting started" ]]; then
      echo "[smoke][warn] second BGREWRITEAOF raced with rewrite completion; gate not observed"
    else
      fail "unexpected second BGREWRITEAOF reply: '$second_reply'"
    fi
    break
  fi
  sleep 0.05
done
if [[ "$in_progress_seen" -ne 1 ]]; then
  echo "[smoke][warn] rewrite finished too quickly to observe in-progress gate"
fi
deadline=$((SECONDS + REWRITE_TIMEOUT_SEC))
last_state=""
last_status=""
while (( SECONDS < deadline )); do
  last_state="$(info_field aof_rewrite_in_progress || true)"
  last_status="$(info_field aof_last_rewrite_status || true)"
  if [[ "$last_state" == "0" && "$last_status" == "ok" ]]; then
    break
  fi
  sleep 0.2
done
assert_eq "$last_state" "0" "rewrite should complete before timeout"
assert_eq "$last_status" "ok" "last rewrite status should be ok"
stop_server

echo "[smoke] scenario 2/2: appendonly=false (BGREWRITEAOF disabled path)"
AOF_OFF_DIR="$WORK_DIR/aof-off"
mkdir -p "$AOF_OFF_DIR"
start_server "false" "$AOF_OFF_DIR" "$((METRICS_PORT_BASE + 1))" "$WORK_DIR/server-aof-off.log"
if ! disabled_reply="$(redis_cmd BGREWRITEAOF)"; then
  fail "BGREWRITEAOF request failed on appendonly=false instance"
fi
assert_eq \
  "$disabled_reply" \
  "ERR BGREWRITEAOF requires appendonly to be enabled" \
  "BGREWRITEAOF should fail when appendonly is disabled"
stop_server

echo "[smoke] PASS"
echo "[smoke] workdir: $WORK_DIR"
