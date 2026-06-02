# Changelog

All notable changes to this project will be documented in this file.

The format is based on Keep a Changelog and the versioning policy in
`docs/operations.md` (Part 7: Support & Versioning Policy).

## [Unreleased]

### Added

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
- `docs/SLO.md` — single-node SLO/SLI targets wired to real metrics, alerts, and runbooks.
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

- README, CLI `--help`, and `ratatosk.conf` now state the single-node /
  capability-tier product contract verbatim.
- `cargo-deny` configuration updated to match the current tool schema

### Migration notes

- No breaking changes. `compatibility-mode` defaults to `compat`, preserving
  existing behavior. Opt into `strict` to surface compatibility footguns.
- Rollback: revert the config directive; no on-disk format change is introduced.

