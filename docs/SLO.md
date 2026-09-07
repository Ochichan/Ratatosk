# Ratatosk SLO / SLI

> Single-node service-level objectives. These are an **operational contract for a
> single Ratatosk instance**, not a distributed availability promise — there is no
> replication or failover (see `docs/PRODUCT_CONTRACT.md`). Availability here means
> "this process serves correct replies"; surviving host loss is the operator's job
> (backups + restart), quantified by the durability contract in
> `docs/operations.md`.

Established by Phase 2 of `docs/RELEASE_ROADMAP.md`. Last synced: **2026-09-08**.

---

## 1. SLIs (how each objective is measured)

Every SLI maps to a metric already exported by the Prometheus endpoint
(`127.0.0.1:9090`, see `crates/ratatosk-server/src/metrics.rs`) or to `INFO` /
`PING HEALTH`. No SLI depends on a metric that does not exist.

| SLI | Definition | Source signal |
|---|---|---|
| Availability | fraction of scrape windows where `PING HEALTH` reports `status:healthy` and the process is up | `up`, `ratatosk_aof_write_latched`, health payload |
| Command latency | per-command server-side duration | `ratatosk_command_duration_seconds` (histogram) |
| Read latency (p99) | p99 of duration for read commands | `histogram_quantile(0.99, rate(ratatosk_command_duration_seconds_bucket[5m]))` |
| Durability freshness | staleness of the last successful AOF/RDB persistence | `ratatosk_aof_queue_depth`, `ratatosk_aof_write_errors_total`, `ratatosk_rdb_save_errors_total` |
| Error rate | rejected / errored ops over total | `ratatosk_aof_write_rejected_total`, `ratatosk_connections_rejected_total`, `ratatosk_accept_errors_total` |
| Memory accuracy | age of the cached memory estimate | `ratatosk_memory_estimate_age_ticks` |
| Lock health | meta-lock wait/hold latency | `ratatosk_server_state_lock_wait_ms`, `ratatosk_server_state_lock_hold_ms` |

---

## 2. SLOs (targets)

Targets are **single-instance, default config, within the published capacity
envelope** (measured with `scripts/capacity_envelope.sh`; the published envelope
is a release deliverable, not yet part of `docs/operations.md`). They are deliberately
modest and honest — "safe within this workload", not "fastest in the world".

| Objective | Target (28-day window) | Measured by |
|---|---|---|
| Process availability | **99.9%** of 1m windows healthy | `avg_over_time((ratatosk_aof_write_latched == bool 0)[28d])` + `up` |
| Read p99 latency | **≤ 1 ms** at ≤ 50k ops/s pipelined | `ratatosk_command_duration_seconds` histogram, aggregated over read commands (`GET`, `MGET`, `HGET`, `LRANGE`, ...) via the per-command `command` label |
| Write p99 latency | **≤ 2 ms** (`appendfsync everysec`) | `ratatosk_command_duration_seconds` histogram, aggregated over write commands (`SET`, `HSET`, `LPUSH`, `XADD`, ...) via the per-command `command` label |
| Durable-write loss window | **≤ 1 s** of acknowledged writes (`everysec`); **0** (`always`) | durability contract, `docs/operations.md` |
| AOF write error budget | **0** sustained write errors | `increase(ratatosk_aof_write_errors_total[5m]) == 0` |
| Memory estimate freshness | **< 100 ticks** stale | `ratatosk_memory_estimate_age_ticks` |

### Error budget

- Availability budget over 28 days at 99.9% = **~40m 19s** of unhealthy time.
- When >50% of the budget is consumed in a rolling 7-day window, freeze
  non-essential config changes and prioritize persistence recovery (durability
  contract and recovery matrix in `docs/operations.md`, `scripts/recovery_matrix.sh`).

---

## 3. SLO → alert wiring

The availability and durability objectives are backed by Prometheus alerts in
`monitoring/prometheus/ratatosk-alerts.yml`; the Alertmanager routing tree lives
in `monitoring/alertmanager/alertmanager.yml`. The latency objectives have no
alert yet and are verified by the benchmark guardrail instead. Every page-level
alert must satisfy the runbook policy in `docs/operations.md` (Part 6 §5);
per-alert runbook pages are not published yet, so the last column names the
first response only.

| SLO breached | Alert | Severity | First response |
|---|---|---|---|
| Availability (AOF latched) | `RatatoskAofWritesLatched` | page | check disk health and recent AOF errors; restart after the cause is fixed (`docs/operations.md` durability contract) |
| Write durability | `RatatoskAofWriteErrors` | page | inspect disk writability and persistence logs |
| Snapshot durability | `RatatoskRdbSaveErrors` | ticket | verify persistence directory access and free space |
| Memory accuracy | `RatatoskMemoryEstimateStale` | ticket | compare `INFO memory` `mem_estimate_age_ticks`; check `server_cron` is running |
| Connection health | `RatatoskAcceptErrorBurst`, `RatatoskFdUtilizationHigh` | ticket | raise the fd limit or `maxclients`; look for connection storms |
| Blocking liveness | `RatatoskBlockingRetryDeadlinesExhausted` | ticket | inspect blocking-command producers and timeouts |

---

## 4. Reporting

- Dashboard: `monitoring/grafana/ratatosk-single-node-dashboard.json`.
- Validate alert rules before shipping: `promtool check rules monitoring/prometheus/ratatosk-alerts.yml`.
- The per-release reliability report (`scripts/reliability_report.sh`) records
  whether the perf guardrail and recovery matrix passed for the tagged build.
