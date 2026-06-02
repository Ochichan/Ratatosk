# Ratatosk

Ratatosk is a single-node, Redis-compatible, RESP2/RESP3 in-memory server for
cache, Pub/Sub, and local durability. It is **not** a Redis Cluster, Sentinel, or
replication-compatible drop-in replacement. Every command exposes a capability
tier (`COMMAND DOCS`); the supported subset is tested against Redis/Valkey.

The command surface is broad (420 entries) but **semantics vary by tier** —
"command name exists" is not "identical to Redis". Run with
`compatibility-mode strict` to make unsupported and syntax-only commands fail
loudly instead of returning a misleading success. See
[`docs/PRODUCT_CONTRACT.md`](docs/PRODUCT_CONTRACT.md) for the full boundary and
tier policy.

Current project boundary:

- Single-node only
- RESP2/RESP3 TCP server
- Cache + Pub/Sub + local persistence
- RDB snapshot + AOF durability
- Broad Redis command surface, but not full Redis distributed parity

For the detailed implementation boundary, see:

- `docs/PRODUCT_CONTRACT.md` (product boundary, capability-tier policy, strict/compat mode — the single source of the contract)
- `docs/architecture.md` (architecture, internals, capability declarations, Redis gap analysis, command ledger)
- `docs/operations.md` (configuration, ecosystem, health, observability, product contract, ship readiness)
- `docs/optimization.md` (performance baseline, RAM/CPU optimization plan and checklist)
- `docs/RELEASE_ROADMAP.md` (live v1.0.0 GA execution tracker: phases, exit gates, ship gate, verification commands)

## Workspace Layout

- `crates/ratatosk-core`: shared types, flags, errors, time utilities
- `crates/ratatosk-resp`: RESP parser and encoder
- `crates/ratatosk-engine`: keyspace, command execution, eviction, expiry, tracking
- `crates/ratatosk-persist`: RDB/AOF codecs and recovery
- `crates/ratatosk-server`: TCP server, event loop, runtime orchestration

## What Ratatosk Is Good For

- Local or single-host cache
- Pub/Sub event fanout
- Session and rate-limit storage
- Durable local data service with Redis-compatible clients

## What Ratatosk Does Not Provide

- Redis Cluster
- Sentinel failover
- Real network replication stream
- Replica-backed `WAIT`/`WAITAOF` semantics
- Redis Functions parity

## Run

```bash
cargo run -p ratatosk-server --bin ratatosk --release
```

Ratatosk resolves configuration in this order:

- built-in defaults
- `--config /path/to/ratatosk.conf`
- `RATATOSK_CONFIG=/path/to/ratatosk.conf`
- auto-loaded `./ratatosk.conf` when present
- environment variable overrides

Disable implicit local config discovery when you want explicit startup only:

- `--no-config-autoload`
- `RATATOSK_DISABLE_CONFIG_AUTOLOAD=true`

Useful operator commands:

```bash
# Validate the resolved configuration and startup preflight checks
cargo run -p ratatosk-server --bin ratatosk -- --check-config

# Inspect the effective config in Redis-style text form
cargo run -p ratatosk-server --bin ratatosk -- --print-config text

# Inspect the same config in JSON
cargo run -p ratatosk-server --bin ratatosk -- --print-config json

# Start with an explicit config file
cargo run -p ratatosk-server --bin ratatosk -- --config ./ratatosk.conf
```

Default listener:

- `127.0.0.1:6379`

Recommended coexistence port when Redis may also be running:

- `RATATOSK_PORT=6380`

Remote bind hardening:

- non-loopback bind still requires `RATATOSK_ALLOW_INSECURE_BIND=true`
- `protected-mode yes` (the default) makes a non-loopback bind refuse to start while the `default` ACL user is still `nopass`
- for non-loopback bind, set `RATATOSK_DEFAULT_USER_PASSWORD=...` or `RATATOSK_DEFAULT_USER_PASSWORD_HASH=...` to bootstrap a password
- `protected-mode no` (or `RATATOSK_ALLOW_DEFAULT_USER_NOPASS=true`) is the explicitly insecure operator opt-out

Config workflow:

- the shipped [`ratatosk.conf`](ratatosk.conf) is a real startup config, not a placeholder
- `CONFIG REWRITE` persists the current runtime config back to `ratatosk.conf` under the active `dir`
- file values with spaces are emitted with quoting so generated configs round-trip cleanly

## Nix

Ratatosk can also be built and run directly with Nix flakes.

```bash
nix build .#ratatosk
nix run .#ratatosk
nix develop
```

Available flake outputs:

- `packages.<system>.ratatosk`: default release build of the `ratatosk` server binary
- `apps.<system>.ratatosk`: run the packaged server with `nix run`
- `devShells.<system>.default`: Rust + Nix development shell

The package also installs:

- example config: `$out/share/examples/ratatosk/ratatosk.conf`
- docs bundle: `$out/share/doc/ratatosk`

## Quality Gate

The repository is kept warning-free under the local quality gate below:

```bash
cargo fmt --all --check
cargo check --workspace --quiet
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace --quiet
```

CI mirrors the same baseline in `.github/workflows/rust-ci.yml`.

## Key Docs

- `docs/architecture.md`: architecture, eviction/expiry, persistence, capability declarations, Redis gap analysis, command ledger
- `docs/operations.md`: configuration, ecosystem integration, ports, health protocol, observability, product contract, versioning policy, ship readiness
- `docs/optimization.md`: performance baseline, RAM/CPU optimization master plan, execution checklist
- `AUTOSTART_RUNBOOK_KO.md`: systemd user autostart guide
