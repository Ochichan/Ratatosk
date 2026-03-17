# Ratatosk

Ratatosk is a standalone, Redis-compatible in-memory data server written in Rust.

Current project boundary:

- Single-node only
- RESP2/RESP3 TCP server
- Cache + Pub/Sub + local persistence
- RDB snapshot + AOF durability
- Broad Redis command surface, but not full Redis distributed parity

For the detailed implementation boundary, see:

- `docs/capability-declarations.md`
- `docs/architecture-ratatosk.md`
- `docs/redis-gap-analysis.md`
- `docs/redis-gap-ledger.md`

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

Default listener:

- `127.0.0.1:6379`

Recommended coexistence port when Redis may also be running:

- `RATATOSK_PORT=6380`

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

- `docs/ecosystem.md`: deployment and integration guidance
- `docs/persistence.md`: RDB/AOF runtime model
- `docs/eviction-and-expiry.md`: memory and TTL behavior
- `docs/performance.md`: benchmark baseline and guardrails
- `AUTOSTART_RUNBOOK_KO.md`: systemd user autostart guide
