# Changelog

All notable changes to this project will be documented in this file.

The format is based on Keep a Changelog and the versioning policy in
`docs/operations.md` (Part 7: Support & Versioning Policy).

## [Unreleased]

### Security

- **RESP parser stack exhaustion**: a few kilobytes of nested arrays (`*1\r\n`
  repeated) overflowed the parser's stack and aborted the whole process before
  authentication. Aggregates may now nest at most 128 levels; deeper input is a
  protocol error that closes only the offending connection.
- **Unbounded string growth**: `SETRANGE` and `BITFIELD SET/INCRBY` with a huge
  offset resized the value to that size and aborted on the failed allocation.
  Like Redis, results beyond `proto-max-bulk-len` (512 MiB) are rejected with
  `ERR string exceeds maximum allowed size`; `APPEND` has the same cap.
- **Reply injection**: error replies quote client input (an unknown command
  name, an option). A CR/LF in that input could end the error line early and
  forge further replies. The encoder now replaces CR and LF in simple strings
  and errors, and the parser rejects them there.
- RDB loading and `RESTORE` no longer reserve memory from untrusted length
  fields: a corrupt length now fails the load instead of aborting the process.
  NaN scores, empty or unordered containers, and stream groups with duplicate
  consumers or pending IDs in a payload are rejected.
- AOF manifests may only name files inside their own directory.
- The audit log and audit chain state default to the data directory instead of
  the shared `/tmp`, where every instance on a host clobbered one chain.
- The insecure-bind guard parses addresses: a hostname that merely starts with
  `127.` is no longer treated as loopback.
- `AUTH`/`HELLO` failure limits are shared across reconnects, Lua callbacks
  honour read-only scripts, and the Lua OS library is removed.
- Dependency advisories: `h2` 0.4.19 (RUSTSEC-2026-0258, reachable through the
  metrics HTTP listener), `rand` 0.8.8 (RUSTSEC-2026-0097), `anyhow` 1.0.104
  (RUSTSEC-2026-0190), and `crossbeam-epoch` 0.9.21 (RUSTSEC-2026-0204).

### Fixed

- Positive `COUNT` arguments of `LPOP`/`RPOP`, `LMPOP`/`BLMPOP`, `ZPOPMIN`/`ZPOPMAX`,
  `ZMPOP`/`BZMPOP`, `SPOP`, `SRANDMEMBER`, `HRANDFIELD` and `ZRANDMEMBER` no
  longer fail above 100000, as in Redis. A count larger than the collection
  returns the whole collection. Only the negative (repeating) count of the three
  random commands keeps the 100000 limit.
- **Prometheus endpoint never served, and leaked memory**: the exporter was
  installed without its HTTP listener or upkeep task, so nothing listened on
  `RATATOSK_METRICS_BIND` and every histogram sample was retained forever
  (resident memory grew by roughly 60 bytes per command). `/metrics` now
  answers, histograms are drained, and latency metrics are real bucketed
  histograms, as the `histogram_quantile` queries in `docs/SLO.md` and the
  dashboard expect.
- `INFO memory` `used_memory` and `ratatosk_memory_used_bytes` stayed at 0
  unless `maxmemory` was set. The estimate is now always maintained; its
  periodic full-scan correction runs outside the server lock, and no write
  between the scan and the counter update is lost from the count.
- The `WATCH` version table gained an entry for every key ever written and never
  shrank. Versions are now kept only for watched keys. `WATCH` also aborts
  `EXEC` when a watched key expires or is removed by `FLUSHDB`, `FLUSHALL`, or
  `SWAPDB`, as in Redis, including when the lock-free read path removes the
  expired key; `SWAPDB` keeps per-database memory figures correct.
- `RESET` left the connection subscribed to its channels on the server.
- AOF startup repair cut the file back at any damage, silently dropping every
  later record, and did so even in a segment later manifest segments depend on.
  Only a torn tail of the last segment holding data is cut back now; other
  damage stops startup. Damaged bytes are never executed as inline commands:
  inline syntax is accepted only as a top-level request, not inside an array.
- `LRANGE`, `LTRIM`, and `ZRANGE` returned an element for a range whose end
  stays negative (`0 -100`); `GETRANGE`, `BITCOUNT`, and `BITPOS` now clamp such
  ranges the way Redis does. The lock-free read path agrees with the engine.
- `ZADD`/`ZINCRBY` silently dropped `+inf`/`-inf` scores. Score ranges treat
  `±inf` as inclusive bounds and reject NaN; `ZUNION`, `ZINTER`, `ZDIFF` and
  their `STORE`/`CARD` forms accept plain sets, turn NaN into 0, and check every
  input's type. Score-range reads seek the sorted index instead of scanning it.
- Bare `HELLO` switched the connection to RESP3; it now reports and keeps the
  current protocol. `HELLO` error texts match Redis.
- `PUBSUB NUMSUB`/`SHARDNUMSUB` without channels, `QUIT` with arguments, and
  `ACL GENPASS` sizing follow Redis.
- Inline commands end at LF and honour `redis-cli`/telnet quoting; empty
  requests (`*0`, a blank line) get no reply.
- `DUMP`/`RESTORE` preserve hash-field TTLs and stream consumer groups.
- `CONFIG REWRITE` flushes and fsyncs the new file before renaming it.
- A `#` inside a config value is literal; IPv6 `bind` addresses work.
- Descriptor exhaustion is retried as a transient accept error on macOS too.
- `scripts/smoke_bgrewriteaof.sh`, `bench_allocator_ab.sh`, and
  `capacity_envelope.sh` failed under macOS bash 3.2 (the smoke test even
  exited 0 without running); the Linux autostart unit now creates its data
  directory, finds `cargo`, and keeps running when the metrics port is taken.
- The allocator A/B benches (`--features mimalloc`/`jemalloc`) installed no
  global allocator, so they measured the system allocator twice. Each bench now
  installs the selected one.
- The Nix `ratatosk-nextest` check copies the gap ledger its tier test reads.
- Capability-tier counts in the README and contract docs (75 `baseline_local`,
  64 `unsupported`); the gap-ledger check now verifies those tables.

### Added

- **Unix-domain socket listener** (`unixsocket`, `unixsocketperm`), guarded by
  a `<path>.lock` ownership lock, and an **experimental shared-memory transport**
  behind the `shm-transport` feature (`shm-socket`). See
  `docs/IPC_TRANSPORT_DECISION.md` and `docs/shm-transport.md`.
- `ratatosk-ipc-bench`, a two-process round-trip latency harness for TCP, Unix
  sockets, and shared memory.
- A per-directory instance lock (`<dir>/ratatosk.lock`): a second server using
  the same data directory refuses to start instead of interleaving AOF writes.
- The server crate exposes the `lua-scripting` feature, and CI builds and tests
  it.
- **Timestamped AOF (`REDIS-AOF-002`)**: new writes carry a per-command wall-clock
  timestamp so replay re-executes each command at its recorded time, preserving
  relative expiries and pre-deadline mutations. Version 1 and legacy RESP files
  remain readable and are upgraded in place on first append. See
  `docs/operations.md` "Persistence format upgrade" for the migration and
  rollback procedure; a binary-only downgrade after new writes is unsupported.
- **Hash-field TTL and stream group state in RDB**: snapshots retain hash-field
  absolute deadlines (`HEXPIRE` family) and stream group / consumer / PEL state
  using private type bytes 128 and 129. Older Ratatosk encodings stay loadable.
- **Sidecar handoff**: `RATATOSK_PORT=0` binds an ephemeral loopback port and,
  when `RATATOSK_BOUND_ADDR_FILE` is set, writes the bound address to that file
  once the listener is ready (the path is preflight-checked at startup). The
  release archive includes `ratatosk-sidecar-evidence.json` with the binary
  hash and the health / port / shutdown contract.
- **Strict compatibility mode** (`compatibility-mode strict|compat`, default `compat`):
  rejects `unsupported`/`syntax_only` commands plus `WAIT`/`WAITAOF` with a
  structured error instead of a misleading success. Config file directive,
  `RATATOSK_COMPATIBILITY_MODE` env, and `CONFIG SET compatibility-mode` runtime
  toggle; persisted by `CONFIG REWRITE`.
- **Protected mode** (`protected-mode yes|no`, default `yes`): a non-loopback bind
  refuses to start while the `default` ACL user is still `nopass`, unless a
  bootstrap password (`RATATOSK_DEFAULT_USER_PASSWORD`/`_HASH`) is supplied.
  `protected-mode no` (≡ `RATATOSK_ALLOW_DEFAULT_USER_NOPASS=true`) is the explicit
  insecure opt-out. Config directive, `RATATOSK_PROTECTED_MODE` env, and
  `CONFIG GET/SET protected-mode`; persisted by `CONFIG REWRITE`. (Roadmap Phase 3.)
- **`PING HEALTH` reasons**: the health report now includes a `reasons:` field that
  names the exact conditions behind a `degraded`/`unhealthy` status (AOF latch,
  persistence dir / AOF not writable, memory critical, last RDB/AOF-rewrite
  failure, low disk, audit chain dirty); `reasons:none` when healthy. Status
  thresholds unchanged. (Roadmap Phase 2.)
- **`INFO server` feature flags**: `ratatosk_compatibility_mode` and
  `ratatosk_protected_mode` are now reported so the active contract boundary and
  security posture are observable from `INFO`.
- **RESP fuzz target** (`crates/ratatosk-resp/fuzz/`): a cargo-fuzz/libFuzzer
  harness (`resp_parse`) over the parser/encoder. The invariants (no panic,
  parser always makes progress, and `encode ∘ parse` round-trips) live in
  `ratatosk_resp::fuzz_support` and are unit-tested under the stable build; the
  fuzz crate is an isolated workspace so the normal gates are unaffected.
  Operator-run (nightly + `cargo install cargo-fuzz`) — see the crate's README.
- **`INFO memory` section** (previously absent): `used_memory` (logical dataset
  estimate), `used_memory_human`, `maxmemory`, `maxmemory_human`,
  `maxmemory_policy`, `mem_used_memory_source:logical_estimate`, and
  `mem_estimate_age_ticks` (estimate drift — the same value as the
  `ratatosk_memory_estimate_age_ticks` Prometheus gauge, so INFO and Prometheus
  agree). Allocator memory and fragmentation are intentionally omitted (they
  require an RSS reader) rather than reported inaccurately.
- `docs/PRODUCT_CONTRACT.md` — single source of the product boundary and
  capability-tier policy (Phase 0 of the release roadmap).
- Per-command capability-tier audit: reconciled 39 code↔gap-ledger tier
  mismatches (working single-node commands like `MONITOR`/`CLUSTER SLOTS` were
  mislabeled `syntax_only`/`unsupported`; `DEBUG`/`FAILOVER`/`SHUTDOWN`/`SFLUSH`
  were corrected to `unsupported`/`syntax_only`). A new test
  (`capability_tier_matches_gap_ledger_for_every_spec`) locks runtime tier =
  ledger for every command spec.
- `docs/SLO.md` — single-node SLO/SLI targets wired to real metrics and alerts.
- Persistence recovery matrix (`scripts/recovery_matrix.sh`): crash / kill-9 /
  truncated-AOF / missing-manifest / repeated-rewrite invariants.
- Backup / restore / rollback drill (`scripts/backup_restore_drill.sh`).
- Capacity envelope measurement (`scripts/capacity_envelope.sh`).
- Per-release reliability report generator (`scripts/reliability_report.sh`).
- Durability contract table (by fsync policy) and TLS termination recipes
  (stunnel / nginx / Envoy) in `docs/operations.md`.
- Alertmanager starter config (`monitoring/alertmanager/`).
- SBOM (CycloneDX) GitHub Actions workflow (`.github/workflows/sbom.yml`).
- Ship-readiness master plan in `docs/operations.md` (Part 8)
- Product contract in `docs/operations.md` (Part 1)
- Support and versioning policy in `docs/operations.md` (Part 7)
- Observability guide plus Prometheus/Grafana starter assets
- Dependabot configuration
- CODEOWNERS
- Release workflow skeleton
- Performance guardrail workflow

### Changed

- `RATATOSK_BOUND_ADDR_FILE` is written once the dataset has loaded, so it
  doubles as a readiness signal and is never written by a failed startup.
- `DUMP` emits payload version `RATSK2`; `RESTORE` still accepts `RATSK1`.
- CI workflows run with read-only repository permissions, and the security scan
  also runs weekly.
- **License changed from MIT to GPL-3.0-or-later.** The workspace `license`
  field, `flake.nix` metadata, and `LICENSE` file were updated together.
- The product contract sentence now says the supported subset is tested against
  Redis; the interop suite and CI never exercised Valkey.
- README rewritten around the problem Ratatosk solves and the capability-tier
  contract; maintainer-only planning and harness files are no longer tracked.
- README and CLI `--help` now state the single-node / capability-tier product
  contract verbatim; `ratatosk.conf` documents `compatibility-mode` and
  `protected-mode` with a pointer to it.
- `cargo-deny` configuration updated to match the current tool schema

### Removed

- The unused domain types in `ratatosk-core` (`RedisError`, `ClientId`,
  `DbIndex`, `SlotId`, `CommandFlags`, `AclCategory`) and the unused
  `ratatosk_persist::embedded` module, which dropped stream group PEL state;
  `RdbSaver`/`RdbLoader` serialize to any writer or reader instead.
- Unused dependencies (`mio`, `memmap2`, `hex`, `tikv-jemalloc-ctl`, among
  others).

### Migration notes

- `compatibility-mode` defaults to `compat`, preserving existing behavior. Opt
  into `strict` to surface compatibility footguns.
- The metrics exporter now really binds `RATATOSK_METRICS_BIND` (default
  `127.0.0.1:9090`). Hosts running several instances need a distinct address
  per instance, or `RATATOSK_ALLOW_NO_METRICS=true`; each instance also needs
  its own data directory.
- Audit files move from `/tmp` to the data directory unless
  `RATATOSK_AUDIT_LOG` / `RATATOSK_AUDIT_CHAIN_STATE` are set.
- AOF startup repair only cuts back a torn tail: an incomplete last record,
  optionally followed by zero fill. Damage followed by more records, or in a
  segment that later segments build on, now stops startup and leaves the file
  untouched instead of silently dropping the writes after it. The error names
  the byte offset; after taking a backup, truncating the file to that offset
  keeps the records before the damage.
- An AOF replays its commands, not their results, so the range, `±inf` score,
  and sorted-set operation fixes above also apply to commands an older build
  logged. `LTRIM l 0 -100`, for example, kept one element before and now
  empties the list. Run `BGREWRITEAOF` on the old build before upgrading to
  carry its dataset over exactly.
- `RATSK2` `DUMP` payloads cannot be restored by older Ratatosk builds.

