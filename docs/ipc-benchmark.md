# IPC Latency Benchmark

`ratatosk-ipc-bench` measures lockstep RESP request/response round trips between a
benchmark client process and a spawned Ratatosk server process. It covers TCP loopback
and Unix-domain sockets plus the optional shared-memory (`shm`) transport, with optional
`redis-server` comparison rows. It uses a monotonic `Instant` clock on one host; it does
not pipeline requests.

## Run

Build the server, then run the release harness from the workspace root:

```bash
cargo build -p ratatosk-server --release
cargo run -p ratatosk-ipc-bench --release -- \
  --server target/release/ratatosk \
  --samples 50000 --warmup 5000 --conns 1,4 \
  --payloads ping,set64,set1k,get64 --transports tcp,unix --tag first
```

The server path defaults to `RATATOSK_BIN`, if set, otherwise
`target/release/ratatosk`. Output defaults to `benchmarks/ipc`; use `--out <dir>` to
change it. To add comparison rows, pass `--redis-server <path-to-redis-server>` (or use
`--with-redis` to discover `redis-server` on `PATH`).

Each invocation writes `<UTC-timestamp>-<host-label>[-<tag>].json` and refreshes
`<out>/latest.json`. `--host-label <str>` defaults to `local`; it is deliberately a
user-provided label rather than the machine hostname. The harness also prints a compact
Markdown summary table.

## Artifact contract

The JSON artifact has these top-level fields:

- `schema_version` and `created_at_utc` identify the format and run time.
- `environment` records `cpu_model`, `os`, `kernel_version`, `arch`, `git_commit`,
  `client_build_profile`, `client_rustc_version`, `host_label`, and `server_binary`.
  `server_binary` contains a workspace-relative path when possible (otherwise only the
  binary file name), `size_bytes`, and `sha256` (or `null` when neither `shasum` nor
  `sha256sum` is available). `ratatosk_server_env` records every `RATATOSK_*` variable
  passed to the spawned server; benchmark temporary-workspace paths are redacted as
  `<tmp>`.
- `config` records the safe server identity plus requested `samples`, `warmup`,
  `connections`, `payloads`, `transports`, `shm_client_spin`, and optional `tag`.
- `redis` describes whether the optional comparison server was enabled or skipped.
- `resource_usage` contains client CPU seconds from `RUSAGE_SELF` plus server CPU seconds
  and peak RSS bytes from `RUSAGE_CHILDREN` after each server exits. RSS is normalized to
  bytes on Linux and macOS.
- `results` is an array of one row per server/transport/payload/concurrency measurement.

Every completed `results` row has `server`, `status: "ok"`, `transport`, `payload`,
`concurrency`, `sample_count`, `warmup_count`, `duration_seconds`, `p50_us`, `p95_us`,
`p99_us`, `p99_9_us`, `max_us`, `errors`, `drops`, `retries`, and
`throughput_req_per_sec`. Latencies are `f64` microseconds; `max_us` is the largest
observed sample. An unavailable transport is represented as `status: "skipped"` with a
`reason`, rather than as a latency result.

`--samples` and `--warmup` are totals for a result row. With multiple connections the
harness divides them across connection threads, merges the per-thread samples for the
percentiles, and reports aggregate throughput. Each value is capped at 50,000,000.

Check a saved artifact with:

```bash
python3 scripts/perf_guardrail_check.py --ipc benchmarks/ipc/latest.json
```

The default p99 limits are 60 microseconds for `ping` over TCP, 50 microseconds for
`ping` over Unix sockets, 20 microseconds for `ping` over shared memory, 40 microseconds
for other shared-memory payloads, and 100 microseconds for every other row. Guardrails
apply only to rows with `concurrency == 1`; rows with other connection counts are printed
for visibility but are not gated. Use `--ipc-p99-us-max <microseconds>` to override all
single-connection IPC p99 limits for a particular check. Skipped rows are not evaluated.

## Interpretation

These measurements are per-host observations: CPU topology, frequency scaling, kernel
scheduler behavior, local load, allocator choice, and build configuration can materially
change them. They are useful regression evidence for a comparable host, but are not SLO
evidence and must not be used as a substitute for production telemetry or workload tests.
