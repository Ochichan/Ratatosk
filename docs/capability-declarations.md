# Capability Declarations

Deployment model, distribution boundaries, and honest limitations for Ratatosk.

Last updated: 2026-03-16

---

## Deployment Model

| Property | Value |
|----------|-------|
| Supported deployment | **Single-node only** |
| Horizontal scaling | Not supported |
| Clustering | Not implemented |
| Replication | Metadata skeleton only; no network replication stream |
| Failover | Not supported |

Ratatosk is a standalone, single-process, in-memory data server. It listens on a single TCP endpoint and serves all clients from one process. There is no built-in mechanism to distribute data across multiple Ratatosk instances.

---

## What "Cluster" Means Here

Ratatosk accepts cluster-related commands at the protocol level for client compatibility, but **no distributed system exists behind them**.

| Command | Behavior |
|---------|----------|
| `CLUSTER INFO` | Returns `cluster_enabled:0` with zeroed counters |
| `CLUSTER MYID` | Returns a local node ID (not part of any cluster) |
| `CLUSTER KEYSLOT` | Computes CRC16 hash slot (local calculation only) |
| `CLUSTER COUNTKEYSINSLOT` / `GETKEYSINSLOT` | Operates on local keyspace |
| `CLUSTER HELP` | Lists subcommands with standalone caveat |
| All other `CLUSTER` subcommands | Return `ERR` (disabled in standalone mode) |
| `READONLY` / `READWRITE` / `ASKING` | Return `OK` (no-op) |
| `SENTINEL` (all subcommands except `HELP`) | Return `ERR not configured as a Sentinel` |

There is no cluster bus, no node table, no slot migration, no MOVED/ASK redirection, and no gossip protocol. Cluster-aware Redis clients will not function correctly against Ratatosk.

---

## What "Replication" Means Here

Ratatosk maintains a replication metadata skeleton for protocol compatibility. Actual data replication does not exist.

| Command | Behavior |
|---------|----------|
| `REPLICAOF NO ONE` | Accepted (already standalone) |
| `REPLICAOF <host> <port>` | Returns `ERR` |
| `SLAVEOF` | Same as `REPLICAOF` |
| `PSYNC` | Returns `ERR` (standalone mode) |
| `REPLCONF` | Accepts LISTENING-PORT/CAPA/IP-ADDRESS/ACK/GETACK for metadata tracking |
| `ROLE` | Returns master/replica mode and logical replication offset |
| `INFO replication` | Returns local replication metadata |
| `WAIT` | Returns immediately with currently tracked replica ACK count (no blocking) |
| `WAITAOF` | Returns immediately with local AOF health (no blocking) |

Key limitations:

- **No backlog**: There is no replication backlog buffer.
- **No network stream**: No data is transmitted to any replica.
- **No partial resync**: `PSYNC` is rejected entirely.
- **`WAIT` is non-blocking**: It returns the current replica ACK count instantly rather than waiting for acknowledgement during the timeout window.
- **0 replicas in practice**: `WAIT` and `WAITAOF` will typically return 0 because no actual replication connections exist.

---

## Data Durability vs Distribution

Ratatosk provides **durability** (surviving restarts) but not **distribution** (surviving node failure with zero downtime).

| Mechanism | What it provides | What it does NOT provide |
|-----------|-----------------|------------------------|
| RDB snapshot | Point-in-time backup, fast reload | Real-time redundancy |
| AOF append-only file | Command-level durability (fsync policy) | Replication to another node |
| Background save (`BGSAVE`) | Non-blocking snapshot creation | Offsite backup or failover |
| AOF manifest (BASE + INCR) | Structured recovery ordering | Distribution or sharding |

Recovery order on startup: RDB load, then AOF replay. Both operate on the local filesystem only.

**Ratatosk is a single point of failure.** If the node goes down, the service is unavailable until the process restarts and replays persisted data.

---

## Concurrency Model

| Property | Value |
|----------|-------|
| Network I/O | Per-client tokio tasks (concurrent) |
| State mutation | `SharedState` wrapping `Mutex<ServerState>` (serialized) |
| Lock-free stats | `AtomicStatsState` — 10 atomic counters (no lock per request) |
| Lock-free config reads | `arc_swap::ArcSwap<ConfigState>` via `config_cache.load()` |
| Lock-free client ID | `AtomicU64` for new connection ID allocation |
| Pub/Sub delivery | Per-subscriber `tokio::sync::mpsc::channel` (push, no polling) |
| Background threads | Lazy-free, RDB save, AOF rewrite |
| I/O thread pool | Placeholder only (not active) |

Command execution is effectively serial for state mutation. The mutex is held for the duration of each mutating command. However, `SharedState` eliminates ~9 lock acquisitions per client request by moving stats, config reads, and client ID allocation to lock-free paths. Pub/Sub delivery operates outside the mutex via mpsc channels.

---

## Lua Scripting (feature-gated)

| Property | Value |
|----------|-------|
| Feature gate | `lua-scripting` |
| Lua version | 5.1 (vendored via `mlua`) |
| Sandbox | TABLE+STRING+MATH+OS+BASE libs only |
| Memory limit | 1 MB per script |
| Instruction limit | 100K per script |
| Nested EVAL | Rejected |

Supported commands when `lua-scripting` is enabled:

| Command | Status |
|---------|--------|
| `EVAL` / `EVAL_RO` | Functional |
| `EVALSHA` / `EVALSHA_RO` | Functional |
| `SCRIPT LOAD` | Functional |
| `SCRIPT EXISTS` | Functional |
| `SCRIPT FLUSH` | Functional |
| `FCALL` / `FUNCTION *` | **Not implemented** (Redis Functions are not supported) |

When `lua-scripting` is disabled, `EVAL` and `EVALSHA` return unsupported errors.

---

## Capability Tier Summary

From the command ledger (`docs/redis-gap-ledger.json`), as of 2026-03-16:

| Tier | Count | Meaning |
|------|-------|---------|
| `distributed_parity` | 0 | No command achieves Redis distributed semantics |
| `behavioral_subset` | 281 | Locally correct behavior for common use cases |
| `baseline_local` | 45 | Standalone-compatible response shell |
| `syntax_only` | 30 | Parses and responds but lacks backing subsystem |
| `unsupported` | 64 | Returns explicit error |

The runtime exposes `ratatosk_capability_tier` in `COMMAND DOCS` responses so clients can programmatically inspect implementation depth.

---

## Honest Summary

Ratatosk is a **standalone in-memory data server** with broad Redis command vocabulary.

It is well suited for:

- Single-node cache with TTL and eviction
- Pub/Sub event bus (local, mpsc push delivery)
- Session/rate-limit counter storage
- Persistent data with RDB + AOF durability
- Lua 5.1 scripting (`lua-scripting` feature gate) — EVAL/EVALSHA with sandboxed execution

It is **not** suited for:

- Multi-node deployments requiring automatic failover
- Workloads that depend on Redis Cluster slot routing
- Sentinel-based service discovery
- Write durability guarantees via replica acknowledgement (`WAIT` with actual replicas)
- Redis Functions execution (`FCALL` / `FUNCTION` family)
