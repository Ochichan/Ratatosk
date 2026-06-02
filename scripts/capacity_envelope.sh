#!/usr/bin/env bash
set -euo pipefail

# Phase 6 capacity envelope measurement for Ratatosk.
#
# Builds the release server, starts it on a random high port against a temp dir,
# waits for readiness, then measures a latency/throughput/memory envelope:
#   * redis-benchmark across SET, GET, PING at pipeline depths 1 and 16
#   * a key-count RAM probe (load KEY_COUNT keys via redis-cli pipe mode,
#     read INFO memory used_memory and Prometheus ratatosk_memory_used_bytes)
# A markdown report is emitted to benchmarks/capacity-envelope-<timestamp>.md.
#
# Env overrides (consistent with the other scripts):
#   KEY_COUNT     number of keys for the RAM probe          (default 100000)
#   PORT          server port                                (default random high)
#   PROFILE       release|debug                              (default release)
#   BENCH_REQUESTS redis-benchmark request count per case    (default 100000)
#   BENCH_CLIENTS  redis-benchmark parallel connections      (default 50)
#   PAYLOAD_BYTES  value size for SET/GET benchmark + probe  (default 64)
#   START_TIMEOUT_SEC server readiness timeout in seconds    (default 30)

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
PORT="${PORT:-$((26379 + (RANDOM % 1000)))}"
METRICS_PORT="${METRICS_PORT:-$((PORT + 1000))}"
PROFILE="${PROFILE:-release}"
KEY_COUNT="${KEY_COUNT:-100000}"
BENCH_REQUESTS="${BENCH_REQUESTS:-100000}"
BENCH_CLIENTS="${BENCH_CLIENTS:-50}"
PAYLOAD_BYTES="${PAYLOAD_BYTES:-64}"
START_TIMEOUT_SEC="${START_TIMEOUT_SEC:-30}"
PIPELINE_DEPTHS=(1 16)
WORKLOADS=(SET GET PING)
OUT_DIR="${OUT_DIR:-$ROOT_DIR/benchmarks}"

if command -v redis-cli >/dev/null 2>&1; then
  REDIS_CLIENT_MODE="redis-cli"
elif command -v nc >/dev/null 2>&1; then
  REDIS_CLIENT_MODE="nc"
else
  echo "[capacity][fail] redis-cli or nc is required" >&2
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

WORK_DIR="$(mktemp -d "${TMPDIR:-/tmp}/ratatosk-capacity-envelope.XXXXXX")"
DATA_DIR="$WORK_DIR/data"
SERVER_LOG="$WORK_DIR/server.log"
SERVER_PID=""
mkdir -p "$DATA_DIR" "$OUT_DIR"

cleanup() {
  if [[ -n "${SERVER_PID:-}" ]] && kill -0 "$SERVER_PID" >/dev/null 2>&1; then
    kill -TERM "$SERVER_PID" >/dev/null 2>&1 || true
    wait "$SERVER_PID" >/dev/null 2>&1 || true
  fi
  rm -rf "$WORK_DIR" >/dev/null 2>&1 || true
}
trap cleanup EXIT

fail() {
  echo "[capacity][fail] $*" >&2
  if [[ -f "$SERVER_LOG" ]]; then
    echo "[capacity] --- server log tail ($SERVER_LOG) ---" >&2
    tail -n 60 "$SERVER_LOG" >&2 || true
  fi
  exit 1
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
  raw="$(resp_request "$@" | nc -w 2 127.0.0.1 "$PORT" 2>/dev/null)" || return 1
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

info_memory_field() {
  local key="$1"
  redis_cmd INFO memory | awk -F: -v key="$key" '
    $1 == key {
      gsub("\r", "", $2);
      print $2;
      exit;
    }
  '
}

# Scrape a single Prometheus gauge value from the metrics endpoint.
# The exporter (metrics-exporter-prometheus, http-listener) serves the text
# exposition format; try /metrics first, then the root path.
scrape_metric() {
  local name="$1"
  local body=""
  local url
  for url in "http://127.0.0.1:${METRICS_PORT}/metrics" "http://127.0.0.1:${METRICS_PORT}/"; do
    if command -v curl >/dev/null 2>&1; then
      body="$(curl -fsS --max-time 5 "$url" 2>/dev/null || true)"
    elif command -v wget >/dev/null 2>&1; then
      body="$(wget -qO- --timeout=5 "$url" 2>/dev/null || true)"
    else
      return 1
    fi
    if [[ -n "$body" ]]; then
      break
    fi
  done
  [[ -n "$body" ]] || return 1
  printf '%s\n' "$body" | awk -v m="$name" '
    $1 == m { print $2; found=1; exit }
    END { if (!found) exit 1 }
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
  RATATOSK_BIND=127.0.0.1 \
  RATATOSK_PORT="$PORT" \
  RATATOSK_METRICS_BIND="127.0.0.1:${METRICS_PORT}" \
  RATATOSK_DIR="$DATA_DIR" \
  RATATOSK_APPENDONLY="false" \
  "$SERVER_BIN" >"$SERVER_LOG" 2>&1 &
  SERVER_PID="$!"
  wait_for_ready
}

# Parse a redis-benchmark CSV line of the form:
#   "TEST","rps","avg","min","p50","p95","p99","max"
# Newer redis-benchmark CSV reports percentiles; fall back to rps-only when the
# percentile columns are absent.
declare -A RESULT_RPS RESULT_P50 RESULT_P99

run_benchmark_case() {
  local workload="$1"
  local pipeline="$2"
  local label="${workload}-p${pipeline}"
  local args=(-h 127.0.0.1 -p "$PORT" -n "$BENCH_REQUESTS" -c "$BENCH_CLIENTS"
    -P "$pipeline" -t "$(echo "$workload" | tr '[:upper:]' '[:lower:]')"
    -q --csv)
  if [[ "$workload" != "PING" ]]; then
    args+=(-d "$PAYLOAD_BYTES")
  fi

  local out
  if ! out="$(redis-benchmark "${args[@]}" 2>/dev/null)"; then
    echo "[capacity][warn] redis-benchmark failed for $label" >&2
    RESULT_RPS[$label]="n/a"
    RESULT_P50[$label]="n/a"
    RESULT_P99[$label]="n/a"
    return 0
  fi

  # redis-benchmark may report PING_INLINE/PING_BULK or set/get rows; take the
  # first data row (skip a possible header).
  local line
  line="$(printf '%s\n' "$out" | grep -i -E '^"?(SET|GET|PING)' | head -n 1)"
  if [[ -z "$line" ]]; then
    line="$(printf '%s\n' "$out" | grep -E '^".*",".*"' | head -n 1)"
  fi
  if [[ -z "$line" ]]; then
    RESULT_RPS[$label]="n/a"
    RESULT_P50[$label]="n/a"
    RESULT_P99[$label]="n/a"
    return 0
  fi

  # Strip quotes, split on comma.
  local cleaned
  cleaned="$(printf '%s' "$line" | tr -d '"')"
  IFS=',' read -r -a cols <<<"$cleaned"
  local rps="${cols[1]:-n/a}"
  local p50="n/a"
  local p99="n/a"
  if (( ${#cols[@]} >= 7 )); then
    p50="${cols[4]:-n/a}"
    p99="${cols[6]:-n/a}"
  fi
  RESULT_RPS[$label]="$rps"
  RESULT_P50[$label]="$p50"
  RESULT_P99[$label]="$p99"
  echo "[capacity] $label -> rps=$rps p50=${p50}ms p99=${p99}ms"
}

# Load KEY_COUNT keys quickly. Prefer redis-cli pipe mode; fall back to nc.
load_keys() {
  local payload
  payload="$(head -c "$PAYLOAD_BYTES" /dev/zero | tr '\0' 'x')"
  echo "[capacity] loading $KEY_COUNT keys for RAM probe (payload ${PAYLOAD_BYTES}B)"
  if [[ "$REDIS_CLIENT_MODE" == "redis-cli" ]]; then
    {
      local i
      for ((i = 0; i < KEY_COUNT; i++)); do
        printf 'SET cap:key:%d %s\r\n' "$i" "$payload"
      done
    } | redis-cli -h 127.0.0.1 -p "$PORT" --pipe >/dev/null 2>&1 \
      || fail "redis-cli --pipe load failed"
  else
    {
      local i
      for ((i = 0; i < KEY_COUNT; i++)); do
        resp_request SET "cap:key:$i" "$payload"
      done
    } | nc -w 5 127.0.0.1 "$PORT" >/dev/null 2>&1 \
      || fail "nc bulk load failed"
  fi
}

human_bytes() {
  local b="$1"
  if ! [[ "$b" =~ ^[0-9]+$ ]]; then
    printf '%s' "$b"
    return 0
  fi
  awk -v b="$b" 'BEGIN {
    split("B KiB MiB GiB TiB", u, " ");
    i = 1;
    while (b >= 1024 && i < 5) { b /= 1024; i++ }
    printf (i == 1 ? "%d %s" : "%.2f %s"), b, u[i];
  }'
}

echo "[capacity] client mode: $REDIS_CLIENT_MODE"
echo "[capacity] building ratatosk-server ($PROFILE profile)"
( cd "$ROOT_DIR" && "${CARGO_CMD[@]}" build -p ratatosk-server --bin ratatosk "${BUILD_ARGS[@]+"${BUILD_ARGS[@]}"}" )
[[ -x "$SERVER_BIN" ]] || fail "server binary not found at $SERVER_BIN"

echo "[capacity] starting server on port $PORT (metrics $METRICS_PORT, dir $DATA_DIR)"
start_server

ping_reply="$(redis_cmd PING || true)"
[[ "$ping_reply" == "PONG" ]] || fail "server did not answer PING with PONG (got '$ping_reply')"

# --- latency / throughput envelope ---
BENCH_AVAILABLE=1
if ! command -v redis-benchmark >/dev/null 2>&1; then
  BENCH_AVAILABLE=0
  echo "[capacity][skip] redis-benchmark not found; latency/throughput envelope skipped"
else
  echo "[capacity] running redis-benchmark envelope (${BENCH_REQUESTS} reqs, ${BENCH_CLIENTS} clients)"
  for workload in "${WORKLOADS[@]}"; do
    for depth in "${PIPELINE_DEPTHS[@]}"; do
      run_benchmark_case "$workload" "$depth"
    done
  done
fi

# --- RAM probe ---
load_keys

dbsize="$(redis_cmd DBSIZE || true)"
used_memory="$(info_memory_field used_memory || true)"
used_memory_human="$(info_memory_field used_memory_human || true)"
prom_used_bytes="$(scrape_metric ratatosk_memory_used_bytes || true)"

echo "[capacity] dbsize=$dbsize used_memory=$used_memory prom_used_bytes=${prom_used_bytes:-n/a}"

# RAM-per-1M-keys estimate from used_memory (integer bytes).
ram_per_million="n/a"
if [[ "$used_memory" =~ ^[0-9]+$ && "$dbsize" =~ ^[0-9]+$ && "$dbsize" -gt 0 ]]; then
  ram_per_million="$(awk -v u="$used_memory" -v n="$dbsize" 'BEGIN { printf "%.0f", (u / n) * 1000000 }')"
fi

# --- markdown report ---
TS="$(date +%Y%m%d-%H%M%S)"
REPORT="$OUT_DIR/capacity-envelope-$TS.md"

{
  echo "# Ratatosk capacity envelope ($TS)"
  echo
  echo "Phase 6 capacity measurement."
  echo
  echo "| parameter | value |"
  echo "|---|---|"
  echo "| profile | $PROFILE |"
  echo "| port | $PORT |"
  echo "| metrics port | $METRICS_PORT |"
  echo "| client mode | $REDIS_CLIENT_MODE |"
  echo "| bench requests | $BENCH_REQUESTS |"
  echo "| bench clients | $BENCH_CLIENTS |"
  echo "| payload bytes | $PAYLOAD_BYTES |"
  echo "| key count (RAM probe) | $KEY_COUNT |"
  echo
  echo "## Latency / throughput envelope"
  echo
  if [[ "$BENCH_AVAILABLE" -eq 1 ]]; then
    echo "| workload | p50 (ms) | p99 (ms) | throughput (req/s) |"
    echo "|---|---|---|---|"
    for workload in "${WORKLOADS[@]}"; do
      for depth in "${PIPELINE_DEPTHS[@]}"; do
        label="${workload}-p${depth}"
        echo "| ${workload} (pipeline ${depth}) | ${RESULT_P50[$label]:-n/a} | ${RESULT_P99[$label]:-n/a} | ${RESULT_RPS[$label]:-n/a} |"
      done
    done
  else
    echo "_redis-benchmark not available on this host; latency/throughput envelope was skipped._"
  fi
  echo
  echo "## Memory (RAM) probe"
  echo
  echo "| metric | value |"
  echo "|---|---|"
  echo "| keys loaded (DBSIZE) | ${dbsize:-n/a} |"
  echo "| INFO used_memory (bytes) | ${used_memory:-n/a} |"
  echo "| INFO used_memory_human | ${used_memory_human:-n/a} |"
  echo "| prometheus ratatosk_memory_used_bytes | ${prom_used_bytes:-n/a} |"
  if [[ "$ram_per_million" != "n/a" ]]; then
    echo "| estimated RAM per 1M keys | ${ram_per_million} bytes ($(human_bytes "$ram_per_million")) |"
  else
    echo "| estimated RAM per 1M keys | n/a (insufficient data) |"
  fi
  echo
  echo "> RAM-per-1M-keys is extrapolated linearly from used_memory / DBSIZE at"
  echo "> ${PAYLOAD_BYTES}-byte values. Real workloads with larger values, more"
  echo "> data types, or fragmentation will differ."
} >"$REPORT"

echo "[capacity] report written: $REPORT"
echo "[capacity] PASS"
