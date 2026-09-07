# Ratatosk

Ratatosk is a single-node, Redis-compatible, RESP2/RESP3 in-memory server for
cache, Pub/Sub, and local durability. It is **not** a Redis Cluster, Sentinel,
or replication-compatible drop-in replacement. Every command exposes a
capability tier; the supported subset is tested against Redis. Written in Rust.

Licensed under the GNU GPL v3.0 or later. See [`LICENSE`](LICENSE).

## The problem

Most software that "needs Redis" needs about a dozen commands on one host: a
cache with TTLs, a lock with `SET NX`, a Pub/Sub channel, a stream, a counter.
Getting that today means one of two things:

- Run the real thing. Redis and its forks are excellent, but they carry the
  weight of a distributed system you are not using, and their licensing has
  changed more than once.
- Run a "Redis-compatible" server. These accept your client's commands, but the
  compatibility boundary is usually undocumented. A command can be accepted,
  return `OK`, and do something different from Redis. You find out in
  production.

Ratatosk targets the single-host case and makes the compatibility boundary
explicit and machine-checkable instead of implied.

## How Ratatosk answers it

**Every command declares what it actually does.** All 420 catalogued commands
carry a capability tier, visible through `COMMAND DOCS` and recorded in the
[gap ledger](docs/redis-gap-ledger.md):

| Tier | Meaning | Count |
|---|---|---|
| `behavioral_subset` | implemented and tested for real Redis semantics on a single node | 275 |
| `baseline_local` | works locally with standalone semantics (for example `WAIT` answers immediately, `CLUSTER` reports one node) | 76 |
| `syntax_only` | parsed and acknowledged, no real effect | 6 |
| `unsupported` | rejected | 63 |
| `distributed_parity` | reserved; nothing claims it | 0 |

A test locks the runtime tier of every command spec to the ledger, so the docs
cannot drift from the binary.

**Strict mode turns silent mismatches into errors.** With
`compatibility-mode strict`, `syntax_only` and `unsupported` commands fail with
a structured error instead of a misleading success. The default `compat` mode
keeps Redis-style leniency for existing clients.

**The supported subset is diff-tested against Redis.** The interop suite
starts a real `redis-server`, runs the same command sequences against both, and
compares replies. CI runs it on every push.

**Durability is a contract, not a checkbox.** RDB snapshots plus a
manifest-backed AOF with BASE and INCR files, timestamped entries, and a
per-fsync-policy durability table in [`docs/operations.md`](docs/operations.md).
A recovery matrix script exercises crash, `kill -9`, truncated-AOF,
missing-manifest, and repeated-rewrite paths.

**It runs comfortably next to something else.** Ratatosk is built to be a
sidecar: `RATATOSK_PORT=0` picks a free port and writes it to
`RATATOSK_BOUND_ADDR_FILE`, `PING HEALTH` reports readiness with named reasons,
SIGTERM drains gracefully within a configurable grace window, and a Prometheus
endpoint plus a Grafana dashboard ship in [`monitoring/`](monitoring/).

**Memory safety by construction.** Four of the five crates are
`#![forbid(unsafe_code)]`. The server crate's only `unsafe` blocks set
environment variables inside its own tests.

## What it is good for

- Local or single-host cache, session, and rate-limit storage
- Pub/Sub fanout and Streams with consumer groups on one machine
- A durable local data service for an app that speaks a Redis client
- Development and CI environments that need Redis semantics without a daemon
  from another package manager

## What it does not provide

- Redis Cluster, Sentinel, or failover
- A real replication stream or replica-backed `WAIT`/`WAITAOF`
- Redis Functions parity, or Search / JSON / Vector modules
- Lua scripting in the default build (`EVAL` family is behind the
  `lua-scripting` feature and rejected otherwise)

The full boundary and tier policy live in
[`docs/PRODUCT_CONTRACT.md`](docs/PRODUCT_CONTRACT.md).

## Run

```bash
cargo run -p ratatosk-server --bin ratatosk --release
```

The server listens on `127.0.0.1:6379` and exports Prometheus metrics on
`127.0.0.1:9090`. Set `RATATOSK_PORT=6380` to coexist with a local Redis.

Configuration is resolved in this order: built-in defaults, `--config PATH`,
`RATATOSK_CONFIG`, an auto-loaded `./ratatosk.conf` when present, then
`RATATOSK_*` environment overrides. Pass `--no-config-autoload` (or set
`RATATOSK_DISABLE_CONFIG_AUTOLOAD=true`) to skip the implicit file.

```bash
# Validate the resolved configuration and startup preflight checks
ratatosk --check-config

# Print the effective config as Redis-style text or JSON
ratatosk --print-config text
ratatosk --print-config json

# Start with an explicit config file
ratatosk --config ./ratatosk.conf
```

The shipped [`ratatosk.conf`](ratatosk.conf) is a real startup config, and
`CONFIG REWRITE` writes the running config back to it.

### Binding beyond loopback

Ratatosk refuses to expose an unauthenticated server by accident:

- A non-loopback bind requires `RATATOSK_ALLOW_INSECURE_BIND=true`.
- With `protected-mode yes` (the default) a non-loopback bind also refuses to
  start while the `default` ACL user has no password. Set
  `RATATOSK_DEFAULT_USER_PASSWORD` or `RATATOSK_DEFAULT_USER_PASSWORD_HASH` to
  bootstrap one.
- `protected-mode no` is the explicit, insecure opt-out.

There is no built-in TLS. `docs/operations.md` has termination recipes for
stunnel, nginx, and Envoy.

### Nix

```bash
nix build .#ratatosk     # release binary
nix run .#ratatosk       # run it
nix develop              # Rust toolchain shell
```

The package installs an example config under `share/examples/ratatosk/` and
the docs under `share/doc/ratatosk`.

## Workspace layout

| Crate | Role |
|---|---|
| `ratatosk-core` | shared types, flags, errors, time utilities |
| `ratatosk-resp` | RESP2/RESP3 zero-copy parser and encoder, with a libFuzzer target |
| `ratatosk-engine` | keyspace, command handlers, eviction, expiry, Pub/Sub, ACL, client tracking |
| `ratatosk-persist` | RDB and AOF codecs, manifest, recovery |
| `ratatosk-server` | TCP accept loop, per-client I/O, config, metrics, persistence runtime |

Dependencies point one way: `server → {engine, persist} → resp → core`.

## Quality gate

The repository is kept warning-free:

```bash
cargo fmt --all --check
cargo check --workspace --quiet
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace --quiet
```

CI runs the same gate plus the Redis interop suite, a latency guardrail on the
pipeline benchmarks, `cargo-deny`, and an SBOM build.
To make the interop comparison mandatory locally:

```bash
RATATOSK_REQUIRE_REDIS_INTEROP=1 cargo test -p ratatosk-server --test redis_interop
```

## Documentation

- [`docs/PRODUCT_CONTRACT.md`](docs/PRODUCT_CONTRACT.md): the product boundary and capability-tier policy, and the source every other surface repeats
- [`docs/architecture.md`](docs/architecture.md): internals, state model, persistence design, Redis gap analysis
- [`docs/operations.md`](docs/operations.md): configuration reference, durability contract, health protocol, observability, versioning policy
- [`docs/SLO.md`](docs/SLO.md): single-node SLO and SLI definitions wired to exported metrics
- [`docs/optimization.md`](docs/optimization.md): performance baselines and the memory/CPU optimization record
- [`docs/redis-gap-ledger.md`](docs/redis-gap-ledger.md): per-command tier and notes, generated from `docs/redis-gap-ledger.json`
- [`CHANGELOG.md`](CHANGELOG.md)

## Status

Ratatosk is pre-1.0 (`0.1.0`). It follows Semantic Versioning, but in the
`0.y.z` range a minor release may still contain breaking changes; the stable
contract in `docs/operations.md` applies from `1.0.0`. Ratatosk is an
independent project and is not affiliated with Redis Ltd.
