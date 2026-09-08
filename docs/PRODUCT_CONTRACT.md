# Ratatosk Product Contract

> **Single source of the product boundary.** README and CLI `--help` repeat the
> sentence in §1 verbatim; `COMMAND DOCS` and the gap ledger carry the per-command
> tier it promises. If any surface drifts from this document, this document wins
> and the surface is the bug. Established by Phase 0 of `docs/RELEASE_ROADMAP.md`.

Last synced with code + ledger: **2026-09-08**.

---

## 1. The contract sentence (verbatim, every surface)

> **EN:** Ratatosk is a single-node, Redis-compatible, RESP2/RESP3 in-memory
> server for cache, Pub/Sub, and local durability. It is **not** a Redis Cluster,
> Sentinel, or replication-compatible drop-in replacement. Every command exposes
> a capability tier; the supported subset is tested against Redis.

> **KO:** Ratatosk는 캐시 · Pub/Sub · 로컬 지속성을 위한 단일 노드 Redis 호환
> RESP2/RESP3 인메모리 서버다. Redis Cluster · Sentinel · replication 호환 drop-in
> 대체재가 **아니다**. 모든 명령은 capability tier를 노출하며, 지원 subset은
> Redis와 대조 검증된다.

---

## 2. What Ratatosk is / is not

| Provides | Does not provide |
|---|---|
| Single-node RESP2/RESP3 server over TCP and Unix domain sockets | Redis Cluster (bus / MOVED / ASK routing) |
| Cache + Pub/Sub + keyspace notifications | Sentinel failover |
| Local RDB snapshot + AOF durability | Real network replication stream (PSYNC) |
| Broad Redis command surface (420 entries) | Replica-backed `WAIT` / `WAITAOF` semantics |
| Per-command capability tier metadata | Redis Functions parity |
| Tested supported subset vs Redis | Search / JSON / Vector modules |
| Experimental shared-memory transport (`--features shm-transport`, Unix only; not part of the support contract — see `docs/shm-transport.md`) | Sub-microsecond latency guarantees of any kind; measured numbers live in `benchmarks/ipc/` and `docs/IPC_TRANSPORT_DECISION.md` |

The out-of-scope column is **published, not hidden** — see `docs/RELEASE_ROADMAP.md`
Phase 9. Naming the gaps is part of the honesty contract.

---

## 3. Capability tiers

Every command carries exactly one tier, exposed at runtime through
`COMMAND DOCS <name>` as the field `ratatosk_capability_tier`, and recorded in
`docs/redis-gap-ledger.json` (the ledger is the source of truth; the markdown is
generated). Two tests enforce this: `command_docs_exposes_capability_tier_for_every_command`
asserts **no command is missing a tier**, and `capability_tier_matches_gap_ledger_for_every_spec`
asserts the **runtime tier equals the ledger** for every command spec (closing the
code↔ledger drift audited and reconciled on 2026-06-02).

| tier | count | meaning |
|---|---:|---|
| `behavioral_subset` | 275 | Useful Redis-compatible behavior; may omit some edge / distributed semantics. The differential-tested target set. |
| `baseline_local` | 76 | Standalone single-node baseline; does not claim distributed Redis parity (e.g. `CLUSTER SLOTS` returns local-only info). |
| `syntax_only` | 6 | Syntax/arity is accepted but there is **no Redis-equivalent operational effect**. ⚠️ silent-success risk. |
| `unsupported` | 63 | Not supported in this build (distributed / functions / commands that always error on a single node). |
| `distributed_parity` | 0 | No distributed parity is claimed anywhere. |

> The 2026-06-02 audit reclassified 39 commands whose tier had drifted: working
> single-node commands wrongly marked `syntax_only`/`unsupported` (e.g. `MONITOR`,
> `CLUSTER SLOTS`, ACL/FUNCTION/SCRIPT read paths) became `baseline_local`, and
> commands that only error or no-op (`DEBUG`, `FAILOVER`, `SHUTDOWN`, `SFLUSH`)
> were corrected to `unsupported`/`syntax_only`. This is why `syntax_only` shrank
> from 30 to 6 — most of those were honest local commands, not empty shells.

> ⚠️ **The marketing rule.** "Command surface 420" must never be phrased as
> "420 commands identical to Redis". The honest phrasing is **"command surface
> 420; supported semantics vary by tier"**.

---

## 4. Compatibility mode — the silent-success guard

`syntax_only` and `unsupported` commands can hand a client or migration tool a
*success-shaped* reply that means nothing. `compatibility-mode` controls whether
that is allowed.

| mode | default | behavior |
|---|---|---|
| `compat` | ✅ default | `syntax_only` / no-op commands are accepted (Redis-client-friendly). Tiers still tell the truth via `COMMAND DOCS`. |
| `strict` | opt-in | Commands whose tier is `unsupported` or `syntax_only`, plus `WAIT` / `WAITAOF` (which imply replica-backed acknowledgement a single node cannot honour), return a **structured error** instead of a misleading reply. |

Set it three ways (precedence: env > CONFIG SET > file > default):

```
# ratatosk.conf
compatibility-mode strict
```
```bash
RATATOSK_COMPATIBILITY_MODE=strict
redis-cli CONFIG SET compatibility-mode strict
```

Strict error shape (stable, machine-parseable):

```
ERR command WAIT is not supported in Ratatosk strict compatibility mode; reason=requires replica-backed acknowledgement that a single-node server cannot provide
```

Representative commands blocked in strict mode: `WAIT`, `WAITAOF`,
`CLIENT PAUSE`, `CLIENT UNBLOCK`, `CLUSTER *` (except informational
`CLUSTER INFO/SLOTS/SHARDS/MYID/KEYSLOT/...`), `SENTINEL *`, `FUNCTION LOAD/DELETE/RESTORE`,
`FCALL`/`FCALL_RO`, `READONLY`/`READWRITE`/`ASKING`. Commands clients need for
discovery and operation (`GET`/`SET`, `PING`, `INFO`, `CONFIG`, `CLIENT LIST`,
`COMMAND DOCS`, `CLUSTER INFO`) are **never** blocked.

Enforced by `crates/ratatosk-engine/src/command/cmd_command_metadata.rs`
(`strict_mode_error`) and gated in `execute_argv`
(`crates/ratatosk-engine/src/command/mod.rs`). Tests live under
`cargo test -p ratatosk-engine -- strict_mode`.

---

## 4b. Protected mode — the unauthenticated-exposure guard

A small deployment should never accidentally expose an unauthenticated
data store on a routable interface. `protected-mode` (default `yes`) makes a
non-loopback bind **refuse to start** while the `default` ACL user is still
`nopass`.

| Mode | Default? | Behavior on a non-loopback bind with a `nopass` default user |
|---|---|---|
| `yes` | ✅ default | Startup fails unless a password is bootstrapped via `RATATOSK_DEFAULT_USER_PASSWORD` / `RATATOSK_DEFAULT_USER_PASSWORD_HASH`. |
| `no` | opt-in | Explicitly insecure: starts with the `nopass` default user (equivalent to `RATATOSK_ALLOW_DEFAULT_USER_NOPASS=true`). |

Loopback binds are unaffected (the convenience default for local development).
Set via `protected-mode` (config file) or `RATATOSK_PROTECTED_MODE` (env). The
bind-time guard is evaluated **at startup** in
`crates/ratatosk-server/src/event_loop.rs` (`bootstrap_default_user_for_bind`);
`CONFIG GET/SET protected-mode` exposes and updates the introspectable value
(persisted by `CONFIG REWRITE`) but does not re-evaluate a running server's bind.
Tests: `cargo test -p ratatosk-server -- protected_mode bootstrap` (bind-guard tests) and `cargo test -p ratatosk-engine -- config_get_set_protected_mode_round_trips` (CONFIG GET/SET round-trip).

The active mode is observable at runtime: `INFO server` exposes
`ratatosk_protected_mode`, and `CONFIG GET protected-mode` returns it.

### Health reasons

`PING HEALTH` returns a `reasons:` field naming the exact conditions behind a
non-`healthy` status (e.g. `AOF writes are latched after an I/O error`,
`memory usage … exceeds maxmemory …`, `audit chain integrity is dirty`), so
operators get a cause, not just a status word. `reasons:none` when healthy.

---

## 5. Contract enforcement matrix

| Item | Risk if unmanaged | Mitigation (status) |
|---|---|---|
| `done=420` phrasing | read as "identical to Redis" | "surface 420, semantics vary by tier" (✅ this doc, README; CLI `--help` states the tier rule without the count) |
| `syntax_only` (6) | client/tool reads false success | `strict` mode → ERR; `compat` mode documented (✅ implemented) |
| Cluster/Sentinel helpers | looks cluster-capable | tiered `unsupported`; strict ERR (✅) |
| `WAIT` / `WAITAOF` | metadata read as durability | strict ERR; not replica-backed (✅) |
| Lua / Functions | feature-gated vs ledger drift | `FUNCTION LOAD/DELETE/RESTORE` and `FCALL`/`FCALL_RO` unsupported; other `FUNCTION` subcommands `baseline_local`; `EVAL` family behind the `lua-scripting` feature and `unsupported` in the default build |
| no-op admin | ops automation reads false success | tiered + strict ERR + documented (✅) |
| Unauthenticated remote bind | data store exposed without a password | `protected-mode yes` default → non-loopback + `nopass` default user fails startup (✅) |

---

## 6. Where this is enforced / verified

- Runtime tier exposure: `COMMAND DOCS` → `ratatosk_capability_tier`.
- Ledger consistency: `python3 scripts/redis_gap_ledger.py check` (CI: `gap-ledger.yml`).
- Strict-mode regression: `cargo test -p ratatosk-engine -- strict_mode`.
- Ship gate (all green to ship v1.0.0 GA): `docs/RELEASE_ROADMAP.md` §4.
