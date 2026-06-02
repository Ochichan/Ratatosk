# Ratatosk SLO / SLI

> Single-node service-level objectives. These are an **operational contract for a
> single Ratatosk instance**, not a distributed availability promise — there is no
> replication or failover (see `docs/PRODUCT_CONTRACT.md`). Availability here means
> "this process serves correct replies"; surviving host loss is the operator's job
> (backups + restart), quantified by the durability contract in
> `docs/operations.md`.

Established by Phase 2 of `docs/RELEASE_ROADMAP.md`. Last synced: **2026-06-02**.

---

## 1. SLIs (how each objective is measured)

Every SLI maps to a metric already exported by the Prometheus endpoint
(`127.0.0.1:9090`, see `crates/ratatosk-server/src/metrics.rs`) or to `INFO` /
`PING HEALTH`. No SLI depends on a metric that does not exist.

| SLI | Definition | Source signal |
|---|---|---|
| Availability | fraction of scrape windows where `PING HEALTH` = `ok` and the process is up | `up`, `ratatosk_aof_write_latched`, health payload |
| Command latency | per-command server-side duration | `ratatosk_command_duration_seconds` (histogram) |
| Read latency (p99) | p99 of duration for read commands | `histogram_quantile(0.99, rate(ratatosk_command_duration_seconds_bucket[5m]))` |
| Durability freshness | staleness of the last successful AOF/RDB persistence | `ratatosk_aof_queue_depth`, `ratatosk_aof_write_errors_total`, `ratatosk_rdb_save_errors_total` |
| Error rate | rejected / errored ops over total | `ratatosk_aof_write_rejected_total`, `ratatosk_connections_rejected_total`, `ratatosk_accept_errors_total` |
| Memory accuracy | age of the cached memory estimate | `ratatosk_memory_estimate_age_ticks` |
| Lock health | meta-lock wait/hold latency | `ratatosk_server_state_lock_wait_ms`, `ratatosk_server_state_lock_hold_ms` |

---

## 2. SLOs (targets)

Targets are **single-instance, default config, within the published capacity
envelope** (`docs/operations.md` capacity section). They are deliberately
modest and honest — "safe within this workload", not "fastest in the world".

| Objective | Target (28-day window) | Measured by |
|---|---|---|
| Process availability | **99.9%** of 1m windows healthy | `avg_over_time((ratatosk_aof_write_latched == bool 0)[28d])` + `up` |
| Read p99 latency | **≤ 1 ms** at ≤ 50k ops/s pipelined | command-duration histogram, `cmd` label = read |
| Write p99 latency | **≤ 2 ms** (`appendfsync everysec`) | command-duration histogram, `cmd` label = write |
| Durable-write loss window | **≤ 1 s** of acknowledged writes (`everysec`); **0** (`always`) | durability contract, `docs/operations.md` |
| AOF write error budget | **0** sustained write errors | `increase(ratatosk_aof_write_errors_total[5m]) == 0` |
| Memory estimate freshness | **< 100 ticks** stale | `ratatosk_memory_estimate_age_ticks` |

### Error budget

- Availability budget over 28 days at 99.9% = **~40m 19s** of unhealthy time.
- When >50% of the budget is consumed in a rolling 7-day window, freeze
  non-essential config changes and prioritize the persistence/lock runbooks.

---

## 3. SLO → alert → runbook wiring

Each objective has a Prometheus alert (`monitoring/prometheus/ratatosk-alerts.yml`)
and a runbook entry (`docs/operations.md` runbook section). The Alertmanager
routing tree lives in `monitoring/alertmanager/alertmanager.yml`.

| SLO breached | Alert | Severity | Runbook |
|---|---|---|---|
| Availability (AOF latched) | `RatatoskAofWritesLatched` | page | AOF latch recovery |
| Write durability | `RatatoskAofWriteErrors` | page | AOF write-error recovery |
| Snapshot durability | `RatatoskRdbSaveErrors` | ticket | RDB save recovery |
| Memory accuracy | `RatatoskMemoryEstimateStale` | ticket | memory-estimate refresh |
| Connection health | `RatatoskAcceptErrorBurst`, `RatatoskFdUtilizationHigh` | ticket | fd / accept saturation |
| Blocking liveness | `RatatoskBlockingRetryDeadlinesExhausted` | ticket | blocking-waiter saturation |

---

## 4. Reporting

- Dashboard: `monitoring/grafana/ratatosk-single-node-dashboard.json`.
- Validate alert rules before shipping: `promtool check rules monitoring/prometheus/ratatosk-alerts.yml`.
- The per-release reliability report (`scripts/reliability_report.sh`) records
  whether the perf guardrail and recovery matrix passed for the tagged build.
