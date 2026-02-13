# Ops Readiness: Fix All 27 Issues

## TL;DR

> **Quick Summary**: Fix every operational readiness gap in Ratatosk — from zero crash diagnostics and hardcoded INFO output, to missing persistence wiring, audit logging, and clock safety. All confirmed issues from the comprehensive ops-review are addressed in one sweep.
>
> **Deliverables**:
> - Crash diagnostics (panic hook, debug info, build metadata)
> - Error context propagation across all public API boundaries
> - Clock safety (Instant for durations, SystemTime only for wall-clock display)
> - Full persistence integration (startup RDB/AOF load, BGSAVE, shutdown flush)
> - Accurate INFO command output with real counters
> - Security audit logging (AUTH failures, destructive ops)
> - Client read timeouts (slowloris protection)
> - Production panic elimination (unreachable!/expect() → proper error handling)
> - Startup banner, --version flag, build.rs with git hash
> - Tests for all new behavior
>
> **Estimated Effort**: Large
> **Parallel Execution**: YES — 4 waves
> **Critical Path**: Task 1 → Task 4 → Task 8 → Task 11 → Task 14

---

## Context

### Original Request
User ran ops-review and identified 27+ operational readiness gaps. Said "전부 다 고칠 계획 세워 보자" — fix all of them, no exclusions.

### Interview Summary
**Key Discussions**:
- Test strategy: **Tests-after** — implement first, add tests after. Existing `cargo test` infra with `#[cfg(test)]` modules.
- Metrics approach: **Redis-native only** — fix INFO command accuracy with real `u64` counters in `StatsState`. No Prometheus, no external metrics crate.
- Persistence scope: **Full integration** — wire `ratatosk-persist` into `ratatosk-server` for startup load (RDB then AOF), BGSAVE/SAVE commands, and shutdown AOF flush.

**Research Findings**:
- `ratatosk-persist` is NOT in `ratatosk-server/Cargo.toml` dependencies — must add it first
- `StatsState` uses plain `u64` (not atomic) — fine because single-threaded command execution behind `Mutex<ServerState>`
- `ServerConfig` has no `dir`/`dbfilename` fields — must extend config for persistence paths
- Event loop already has SIGUSR1 handler stub: `"persistence not yet implemented"` — ready to wire
- `ServerState::with_default_dbs()` always starts empty — no persistence load path
- 5 production panic sites confirmed (2 `unreachable!()`, 3 `expect()`)
- Zero `.with_context()` calls in entire codebase

### Metis Review
**Identified Gaps** (addressed):
- BGSAVE strategy needed: `tokio::task::spawn_blocking` with snapshot clone (single-threaded model, no fork). Addressed in Task 8.
- `ratatosk-persist` dependency missing from server: Prerequisite added as Task 4.
- Config extension needed for persistence paths (`dir`, `dbfilename`, `appendonly`, `appendfsync`): Included in Task 4.
- Error context scope: Limited to public API boundaries only (not every internal `?`) to avoid explosion. Addressed in Task 3.
- AOF command interception point: After successful command execution in `execute()` dispatch. Addressed in Task 8.
- Scope creep danger zones: AOF rewrite, CONFIG REWRITE, full Clock trait abstraction, new INFO sections beyond fixing existing ones — all explicitly excluded.

---

## Work Objectives

### Core Objective
Bring Ratatosk from "compiles and handles commands" to "safe to run in production" by eliminating every operational readiness gap identified in the review.

### Concrete Deliverables
- `build.rs` generating version/git metadata
- Panic hook in `main.rs` logging to tracing before abort
- `.with_context()` on all `?` at public crate API boundaries
- `Instant`-based durations replacing `SystemTime` arithmetic for TTL/expiry/blocking
- `ratatosk-persist` wired into server (startup load, BGSAVE, SAVE, shutdown flush)
- Real counters in `StatsState` for commands, connections, network bytes, evictions, expirations
- Accurate `INFO` output sections (clients, stats, persistence, keyspace)
- `tracing::warn!` on AUTH failure, FLUSHALL, FLUSHDB, CONFIG SET
- Client read timeout (idle timeout via `tokio::time::timeout`)
- All production `unreachable!()`/`expect()` replaced with proper error paths
- `--version` CLI flag via clap
- Startup banner log (bind addr, port, version, PID, db count)
- `strip = "debuginfo"` instead of `strip = true` (keeps symbol names for backtraces)
- Tests for each fix

### Definition of Done
- [x] `cargo build --release` succeeds
- [x] `cargo test` passes (all existing + new tests)
- [x] `cargo clippy -- -D warnings` passes
- [x] `./target/release/ratatosk --version` prints version + git hash
- [x] Server starts, loads RDB if present, logs startup banner
- [x] BGSAVE creates dump.rdb, server restarts and loads it
- [x] `INFO` command returns real connected_clients count
- [x] AUTH failure produces a tracing warning
- [x] No `unreachable!()` or `expect()` in production code paths

### Must Have
- Every fix has at least one test
- All changes pass `cargo clippy -- -D warnings`
- No new dependencies beyond what's already in workspace (except `vergen` or equivalent for build.rs)
- Persistence uses existing `ratatosk-persist` crate APIs — no rewrite

### Must NOT Have (Guardrails)
- ❌ No AOF rewrite implementation (complex, separate project)
- ❌ No CONFIG REWRITE command
- ❌ No full `Clock` trait abstraction (just switch to `Instant` where appropriate)
- ❌ No new INFO sections (only fix existing hardcoded ones)
- ❌ No Prometheus/OpenTelemetry/external metrics crate
- ❌ No disk space pre-check (OS-dependent, low ROI vs complexity)
- ❌ No log rotation implementation (document recommendation only)
- ❌ No ACL persistence (LOAD/SAVE remain no-ops — separate feature)
- ❌ No schema migration framework
- ❌ No health check HTTP endpoint (would require new listener)
- ❌ No `AtomicU64` for stats — `StatsState` is behind `Mutex<ServerState>`, plain `u64` is correct
- ❌ No changes to benchmark files (they may use `expect()` — that's fine for benchmarks)

---

## Verification Strategy

> **UNIVERSAL RULE: ZERO HUMAN INTERVENTION**
>
> ALL tasks are verified by running `cargo test`, `cargo clippy`, `cargo build --release`, and specific command-line checks. No human visual inspection required.

### Test Decision
- **Infrastructure exists**: YES (`#[cfg(test)]` modules throughout, `cargo test` works)
- **Automated tests**: Tests-after (implement, then add `#[cfg(test)]` tests in same file or `tests/` dir)
- **Framework**: `cargo test` (built-in Rust test framework)

### Agent-Executed QA Scenarios (MANDATORY — ALL tasks)

**Verification Tool by Deliverable Type:**

| Type | Tool | How Agent Verifies |
|------|------|-------------------|
| **Rust code** | Bash (`cargo test`, `cargo clippy`, `cargo build`) | Compile, test, lint |
| **CLI behavior** | Bash / interactive_bash | Run binary, check output |
| **Server behavior** | Bash (redis-cli or netcat) | Connect, send commands, verify responses |

---

## Execution Strategy

### Parallel Execution Waves

```
Wave 1 (Start Immediately — Foundation, no interdependencies):
├── Task 1: build.rs + --version + strip fix (Cargo.toml, main.rs)
├── Task 2: Panic hook + startup banner (main.rs, event_loop.rs)
├── Task 3: Error context propagation (all crates, public API boundaries)
├── Task 5: Security audit logging (cmd_acl.rs, cmd_server.rs)
├── Task 6: Production panic elimination (cmd_key.rs, cmd_string.rs, cmd_set.rs, mod.rs)
└── Task 7: Client read timeout (client.rs)

Wave 2 (After Wave 1 — depends on foundation):
├── Task 4: Persistence foundation (Cargo.toml deps, config extension, ServerState fields)
├── Task 9: StatsState counters + accurate INFO (keyspace.rs, cmd_server.rs)
└── Task 10: Clock safety — Instant for durations (time.rs, expiry.rs, client.rs)

Wave 3 (After Task 4 — persistence integration):
├── Task 8: Persistence wiring (startup load, BGSAVE, SAVE, shutdown flush)
└── Task 11: estimate_used_memory optimization (eviction.rs)

Wave 4 (After all — integration + final):
└── Task 12: Integration tests + final verification

Critical Path: Task 1 → Task 4 → Task 8 → Task 12
Parallel Speedup: ~50% faster than sequential
```

### Dependency Matrix

| Task | Depends On | Blocks | Can Parallelize With |
|------|------------|--------|---------------------|
| 1 | None | 4 (build.rs exists) | 2, 3, 5, 6, 7 |
| 2 | None | 12 | 1, 3, 5, 6, 7 |
| 3 | None | 12 | 1, 2, 5, 6, 7 |
| 4 | 1 (Cargo.toml touched) | 8 | 9, 10 |
| 5 | None | 12 | 1, 2, 3, 6, 7 |
| 6 | None | 12 | 1, 2, 3, 5, 7 |
| 7 | None | 12 | 1, 2, 3, 5, 6 |
| 8 | 4 | 12 | 11 |
| 9 | None (logically independent) | 12 | 4, 10 |
| 10 | None | 12 | 4, 9 |
| 11 | None (but logical after 9) | 12 | 8 |
| 12 | ALL | None | None (final) |

### Agent Dispatch Summary

| Wave | Tasks | Recommended Agents |
|------|-------|-------------------|
| 1 | 1, 2, 3, 5, 6, 7 | 6 parallel agents (quick/unspecified-low) |
| 2 | 4, 9, 10 | 3 parallel agents (unspecified-high for 4, unspecified-low for 9/10) |
| 3 | 8, 11 | 2 parallel agents (deep for 8, quick for 11) |
| 4 | 12 | 1 agent (unspecified-high) |

---

## TODOs

### Wave 1 — Foundation (No Dependencies, All Parallel)

---

- [x] 1. Build metadata: `build.rs` + `--version` flag + strip fix

  **What to do**:
  - Create `crates/ratatosk-server/build.rs` that writes `GIT_HASH`, `BUILD_DATE`, `CARGO_PKG_VERSION` to environment variables via `println!("cargo:rustc-env=...")`
    - Use `std::process::Command` to run `git rev-parse --short HEAD` (no external crate needed)
    - Fallback to `"unknown"` if git command fails (e.g., in Docker build without .git)
  - In `main.rs`, add clap CLI parsing with `--version` flag
    - Already depends on `clap = { workspace = true }` with derive feature
    - Version string: `env!("CARGO_PKG_VERSION")` + `" ("` + `env!("GIT_HASH")` + `")"` 
  - Change `Cargo.toml:89` from `strip = true` to `strip = "debuginfo"` — keeps symbol names for panic backtraces while still stripping DWARF debug info
  - Add test that version string contains expected format

  **Must NOT do**:
  - Don't add `vergen` or other heavy build-time crates — `std::process::Command` + `git` is sufficient
  - Don't change `panic = "abort"` — that's a deliberate choice for performance. The panic hook (Task 2) handles diagnostics before abort.

  **Recommended Agent Profile**:
  - **Category**: `quick`
    - Reason: Small, well-scoped changes to 3 files (build.rs, main.rs, Cargo.toml)
  - **Skills**: []
    - No special skills needed — straightforward Rust file creation and editing
  - **Skills Evaluated but Omitted**:
    - `frontend-ui-ux`: No UI involved
    - `playwright`: No browser testing

  **Parallelization**:
  - **Can Run In Parallel**: YES
  - **Parallel Group**: Wave 1 (with Tasks 2, 3, 5, 6, 7)
  - **Blocks**: Task 4 (Cargo.toml is touched here — Task 4 also modifies it)
  - **Blocked By**: None

  **References**:

  **Pattern References**:
  - `crates/ratatosk-server/src/main.rs:1-19` — Current entry point; add clap arg parsing after line 12 (tracing init), before config loading. Currently uses `anyhow::Result<()>` which is fine for main.
  - `crates/ratatosk-server/Cargo.toml:23` — Already has `clap = { workspace = true }` dependency. Check workspace Cargo.toml for clap features (need `derive`).

  **API/Type References**:
  - `Cargo.toml:85-89` — Release profile. Line 89 `strip = true` → change to `strip = "debuginfo"`.

  **Documentation References**:
  - Rust reference: `build.rs` scripts run before compilation and can set `rustc-env` vars
  - clap derive: `#[derive(Parser)]` with `#[command(version)]` auto-generates `--version` from cargo metadata

  **WHY Each Reference Matters**:
  - `main.rs` is where clap parsing must be added — currently has no CLI parsing at all
  - `Cargo.toml:89` is the exact line to change for strip setting
  - clap is already a dependency so no new crate needed

  **Acceptance Criteria**:

  - [ ] `crates/ratatosk-server/build.rs` exists and compiles
  - [ ] `cargo build --release -p ratatosk-server` succeeds
  - [ ] `./target/release/ratatosk --version` outputs version string containing semver + git hash (or "unknown")
  - [ ] `Cargo.toml` line 89 reads `strip = "debuginfo"` not `strip = true`
  - [ ] `cargo test -p ratatosk-server` passes (including any new test)

  **Agent-Executed QA Scenarios**:

  ```
  Scenario: --version flag prints version with git hash
    Tool: Bash
    Preconditions: cargo build --release -p ratatosk-server succeeds
    Steps:
      1. Run: ./target/release/ratatosk --version
      2. Capture stdout
      3. Assert: output matches regex `ratatosk \d+\.\d+\.\d+`
      4. Assert: output contains a 7+ char hex string (git hash) OR "unknown"
    Expected Result: Version string with embedded build metadata
    Evidence: stdout captured

  Scenario: strip = "debuginfo" preserves symbol names
    Tool: Bash
    Preconditions: cargo build --release -p ratatosk-server succeeds
    Steps:
      1. Run: nm ./target/release/ratatosk 2>/dev/null | head -5 || readelf -s ./target/release/ratatosk | grep -c "FUNC" 
      2. Assert: at least some symbol names present (count > 0)
    Expected Result: Binary has symbol names for backtraces
    Evidence: nm/readelf output captured
  ```

  **Commit**: YES
  - Message: `feat(server): add build.rs, --version flag, preserve backtrace symbols`
  - Files: `crates/ratatosk-server/build.rs`, `crates/ratatosk-server/src/main.rs`, `Cargo.toml`
  - Pre-commit: `cargo test -p ratatosk-server && cargo clippy -p ratatosk-server -- -D warnings`

---

- [x] 2. Panic hook + startup banner

  **What to do**:
  - In `main.rs`, before `tracing_subscriber::fmt::init()`, install a custom panic hook:
    ```
    std::panic::set_hook(Box::new(|info| {
        // Use eprintln since tracing may not be initialized or may be broken
        eprintln!("FATAL PANIC: {info}");
        // Also try tracing in case it works
        tracing::error!(%info, "process panicking — will abort");
    }));
    ```
    Note: with `panic = "abort"`, this hook runs THEN the process aborts. No unwinding.
  - Add startup banner after config is loaded, before `event_loop::run()`:
    ```
    tracing::info!(
        bind = %config.bind,
        port = config.port,
        pid = std::process::id(),
        version = env!("CARGO_PKG_VERSION"),
        max_clients = config.max_clients,
        "Ratatosk server starting"
    );
    ```
  - Add test for panic hook presence (can't easily test abort behavior, but can verify hook is set)

  **Must NOT do**:
  - Don't install the hook inside a library crate — panic hooks belong in the binary crate only
  - Don't try to catch/recover from panics — `panic = "abort"` means the hook is fire-and-forget

  **Recommended Agent Profile**:
  - **Category**: `quick`
    - Reason: Two small additions to main.rs/event_loop.rs
  - **Skills**: []
  - **Skills Evaluated but Omitted**:
    - None relevant

  **Parallelization**:
  - **Can Run In Parallel**: YES
  - **Parallel Group**: Wave 1 (with Tasks 1, 3, 5, 6, 7)
  - **Blocks**: Task 12
  - **Blocked By**: None

  **References**:

  **Pattern References**:
  - `crates/ratatosk-server/src/main.rs:10-18` — Current main function. Panic hook goes before tracing init (line 12), startup banner goes after config load (after line 14).
  - `crates/ratatosk-server/src/event_loop.rs:169-170` — `run()` function entry point. The banner should be logged in `main.rs` before calling `run()`, NOT inside `run()`.

  **Documentation References**:
  - `CLAUDE.md` → "Graceful shutdown" pattern shows `Arc<AtomicBool>` — panic hook is the crash counterpart

  **WHY Each Reference Matters**:
  - `main.rs` is the ONLY place to install panic hooks (binary crate entry point)
  - The startup banner needs config values (bind, port) which are available after `ServerConfig::from_env()`

  **Acceptance Criteria**:

  - [ ] Panic hook installed before tracing init
  - [ ] Startup banner logged with bind, port, PID, version, max_clients
  - [ ] `cargo test -p ratatosk-server` passes
  - [ ] `cargo clippy -p ratatosk-server -- -D warnings` passes

  **Agent-Executed QA Scenarios**:

  ```
  Scenario: Startup banner appears in server output
    Tool: Bash
    Preconditions: cargo build --release -p ratatosk-server succeeds
    Steps:
      1. Run: RUST_LOG=info timeout 2 ./target/release/ratatosk 2>&1 || true
      2. Capture stderr (tracing outputs to stderr by default)
      3. Assert: output contains "Ratatosk server starting"
      4. Assert: output contains "bind" and "port" and "pid"
    Expected Result: Startup banner with structured fields
    Evidence: stderr captured
  ```

  **Commit**: YES
  - Message: `feat(server): add panic hook and startup banner logging`
  - Files: `crates/ratatosk-server/src/main.rs`
  - Pre-commit: `cargo test -p ratatosk-server && cargo clippy -p ratatosk-server -- -D warnings`

---

- [x] 3. Error context propagation at public API boundaries

  **What to do**:
  - Add `.with_context(|| format!("..."))` or `.context("...")` to every `?` operator at **public crate API boundaries** — specifically:
    - `ratatosk-server`: `config.rs` (all parse operations), `client.rs` (I/O operations), `event_loop.rs` (bind, accept)
    - `ratatosk-persist`: `rdb/loader.rs` (file open, read, parse), `rdb/saver.rs` (file create, write), `aof/writer.rs` (append, fsync), `aof/recovery.rs` (replay)
    - `ratatosk-engine`: NOT a target — engine errors are already domain-typed (`CommandError`). Only add context where `anyhow` is used.
  - Focus on boundaries where errors cross crate lines or reach the user/operator
  - Pattern: `file.read_exact(&mut buf).context("reading RDB header")?` not `file.read_exact(&mut buf)?`
  - Do NOT add context to every internal `?` — only at crate-public function boundaries and I/O operations
  - Add a test that verifies error messages contain context strings (e.g., corrupt RDB → error message contains "loading RDB")

  **Must NOT do**:
  - Don't add `.context()` to internal/private functions — only public API boundaries
  - Don't refactor error types — just add context strings to existing `?` operators
  - Don't touch `ratatosk-core` or `ratatosk-resp` — they use `thiserror` enums which are already descriptive

  **Recommended Agent Profile**:
  - **Category**: `unspecified-high`
    - Reason: Sweeping change across multiple crates, needs careful judgment about which `?` operators to annotate
  - **Skills**: []
  - **Skills Evaluated but Omitted**:
    - None relevant — pure Rust editing

  **Parallelization**:
  - **Can Run In Parallel**: YES
  - **Parallel Group**: Wave 1 (with Tasks 1, 2, 5, 6, 7)
  - **Blocks**: Task 12
  - **Blocked By**: None

  **References**:

  **Pattern References**:
  - `crates/ratatosk-server/src/client.rs:302` — bare `stream.read_buf()` with no context — example of what needs fixing
  - `crates/ratatosk-server/src/config.rs:40-45` — `.map_err()` already present for port parsing — these are already fine, don't double-wrap
  - `crates/ratatosk-persist/src/rdb/loader.rs:70-71` — version check with no context on why it failed

  **API/Type References**:
  - `anyhow` crate is already a workspace dependency — `use anyhow::Context` is available everywhere

  **Documentation References**:
  - `CLAUDE.md` → "모든 에러 전파에 `.with_context(|| format!("..."))` 필수 — bare 에러 메시지 금지"

  **WHY Each Reference Matters**:
  - `client.rs:302` is the canonical example of a bare `?` on I/O that produces an unhelpful error
  - `config.rs` shows the project already has SOME error context via `.map_err()` — don't duplicate
  - `CLAUDE.md` mandates this pattern — this task brings the codebase into compliance

  **Acceptance Criteria**:

  - [ ] All `?` operators on I/O operations in server/persist crates have `.context()` or `.with_context()`
  - [ ] `config.rs` parse errors retain their existing `.map_err()` (not double-wrapped)
  - [ ] `cargo test` passes
  - [ ] `cargo clippy -- -D warnings` passes
  - [ ] At least one test verifies error context string is present in error chain

  **Agent-Executed QA Scenarios**:

  ```
  Scenario: Error messages contain context after propagation
    Tool: Bash (cargo test)
    Preconditions: Tests written for error context
    Steps:
      1. Run: cargo test -p ratatosk-persist -- --test-threads=1 2>&1
      2. Assert: all tests pass
      3. Check that test names include "context" or "error_message"
    Expected Result: Tests validate error context propagation
    Evidence: Test output captured
  ```

  **Commit**: YES
  - Message: `fix(all): add .context() to error propagation at public API boundaries`
  - Files: `crates/ratatosk-server/src/client.rs`, `crates/ratatosk-server/src/event_loop.rs`, `crates/ratatosk-persist/src/rdb/loader.rs`, `crates/ratatosk-persist/src/rdb/saver.rs`, `crates/ratatosk-persist/src/aof/writer.rs`, `crates/ratatosk-persist/src/aof/recovery.rs`
  - Pre-commit: `cargo test && cargo clippy -- -D warnings`

---

- [x] 5. Security audit logging (AUTH failures + destructive operations)

  **What to do**:
  - In `cmd_acl.rs`, add `tracing::warn!` when AUTH fails (wrong password or unknown user):
    - Location: `cmd_acl.rs:352-374` (the AUTH command handler)
    - Log: `tracing::warn!(user = %username, client_id = %client_id, "AUTH failed: invalid credentials")`
    - Also log successful AUTH at `tracing::info!` level for audit completeness
  - In `cmd_server.rs`, add `tracing::warn!` for destructive operations:
    - `FLUSHALL` (around line 979-999): `tracing::warn!(client_id = %client_id, "FLUSHALL executed")`
    - `FLUSHDB` (similar location): `tracing::warn!(client_id = %client_id, db = db_index, "FLUSHDB executed")`
    - `CONFIG SET` (around line 435-513): `tracing::info!(client_id = %client_id, param = %param, "CONFIG SET executed")`
  - The `client_id` is available from the execution context — check how `execute()` passes client info to command handlers
  - Add tests that verify log output (use `tracing_subscriber::fmt::TestWriter` or check tracing test utils)

  **Must NOT do**:
  - Don't add rate limiting to auth failure logging — that's a separate concern
  - Don't block or reject connections after N failures — that's a separate feature
  - Don't log passwords or password hashes

  **Recommended Agent Profile**:
  - **Category**: `quick`
    - Reason: Adding tracing calls at known locations — small, targeted changes
  - **Skills**: []
  - **Skills Evaluated but Omitted**:
    - None relevant

  **Parallelization**:
  - **Can Run In Parallel**: YES
  - **Parallel Group**: Wave 1 (with Tasks 1, 2, 3, 6, 7)
  - **Blocks**: Task 12
  - **Blocked By**: None

  **References**:

  **Pattern References**:
  - `crates/ratatosk-engine/src/command/cmd_acl.rs:352-374` — AUTH command handler. This is where auth failure happens. Need to find where the error response is sent and add `tracing::warn!` before it.
  - `crates/ratatosk-engine/src/command/cmd_server.rs:979-999` — FLUSHALL handler. Add warn log before the flush operation.
  - `crates/ratatosk-engine/src/command/cmd_server.rs:435-513` — CONFIG SET handler. Add info log after successful config change.
  - `crates/ratatosk-engine/src/command/mod.rs` — `execute()` function — check how client_id is passed to handlers (likely via a context struct or direct parameter)

  **API/Type References**:
  - `tracing::warn!` and `tracing::info!` — already used throughout the codebase
  - Client ID is available as `i64` from `ServerState::alloc_client_id()` — check how it flows to command handlers

  **WHY Each Reference Matters**:
  - `cmd_acl.rs:352-374` is the EXACT location where AUTH failures must be logged — the error path
  - `cmd_server.rs` handlers are where destructive ops happen — audit trail must be here
  - Understanding `execute()` context is critical to know how to access `client_id` in handlers

  **Acceptance Criteria**:

  - [ ] AUTH failure produces `tracing::warn!` with username (not password)
  - [ ] FLUSHALL/FLUSHDB produce `tracing::warn!` with client_id
  - [ ] CONFIG SET produces `tracing::info!` with parameter name
  - [ ] `cargo test -p ratatosk-engine` passes
  - [ ] `cargo clippy -p ratatosk-engine -- -D warnings` passes

  **Agent-Executed QA Scenarios**:

  ```
  Scenario: AUTH failure is logged
    Tool: Bash (cargo test)
    Preconditions: Test written that triggers AUTH failure and captures tracing output
    Steps:
      1. Run: cargo test -p ratatosk-engine -- audit_log 2>&1
      2. Assert: test passes
    Expected Result: Test validates warn-level log on AUTH failure
    Evidence: Test output captured

  Scenario: FLUSHALL is logged
    Tool: Bash (cargo test)
    Preconditions: Test written that runs FLUSHALL and captures tracing output
    Steps:
      1. Run: cargo test -p ratatosk-engine -- flush_audit 2>&1
      2. Assert: test passes
    Expected Result: Test validates warn-level log on FLUSHALL
    Evidence: Test output captured
  ```

  **Commit**: YES
  - Message: `feat(engine): add security audit logging for AUTH, FLUSHALL, FLUSHDB, CONFIG SET`
  - Files: `crates/ratatosk-engine/src/command/cmd_acl.rs`, `crates/ratatosk-engine/src/command/cmd_server.rs`
  - Pre-commit: `cargo test -p ratatosk-engine && cargo clippy -p ratatosk-engine -- -D warnings`

---

- [x] 6. Eliminate production panics (unreachable! / expect())

  **What to do**:
  - Replace all `unreachable!()` and `expect()` in production code with proper error returns:
    - `cmd_key.rs:558` — `unreachable!()` → return a `CommandError` (WRONGTYPE or internal error)
    - `cmd_string.rs:478` — `unreachable!()` → return a `CommandError`
    - `cmd_set.rs:416` — `expect()` → `.ok_or(CommandError::...)?` or match with error arm
    - `mod.rs:5693-5695` — `expect()` → proper error handling
    - `mod.rs:8995` — `expect()` → proper error handling
    - `mod.rs:9061` — `expect()` → proper error handling
  - For each site, read the surrounding context to understand WHAT invariant is being asserted, then decide if it should be an internal error or a user-facing error
  - Add tests that exercise the previously-unreachable paths (if reachable via malformed input) or document why they're unreachable with a comment + `debug_assert!`

  **Must NOT do**:
  - Don't touch `expect()` in test code or benchmark code — only production paths
  - Don't replace with silent fallback — if it was `unreachable!()`, there should be either an error return or a `debug_assert!` + error return
  - Don't change the logic, just the error handling

  **Recommended Agent Profile**:
  - **Category**: `unspecified-low`
    - Reason: 6 targeted replacements at known locations. Needs reading context but changes are small.
  - **Skills**: []
  - **Skills Evaluated but Omitted**:
    - None relevant

  **Parallelization**:
  - **Can Run In Parallel**: YES
  - **Parallel Group**: Wave 1 (with Tasks 1, 2, 3, 5, 7)
  - **Blocks**: Task 12
  - **Blocked By**: None

  **References**:

  **Pattern References**:
  - `crates/ratatosk-engine/src/command/cmd_key.rs:558` — `unreachable!()`. Read surrounding match arms to understand what type was expected.
  - `crates/ratatosk-engine/src/command/cmd_string.rs:478` — `unreachable!()`. Same approach.
  - `crates/ratatosk-engine/src/command/cmd_set.rs:416` — `expect()`. Read what's being unwrapped and what error message says.
  - `crates/ratatosk-engine/src/command/mod.rs:5693-5695` — `expect()`. Large file — read ±20 lines for context.
  - `crates/ratatosk-engine/src/command/mod.rs:8995` — `expect()`. Same.
  - `crates/ratatosk-engine/src/command/mod.rs:9061` — `expect()`. Same.

  **API/Type References**:
  - Look for `CommandError` enum definition — use appropriate variant for each replacement
  - `CLAUDE.md` → "No-Panic: 런타임 코드에서 `unwrap()`, `expect()` 금지"

  **WHY Each Reference Matters**:
  - Each location is a potential crash site in production. Must read ±20 lines to understand the invariant before replacing.

  **Acceptance Criteria**:

  - [ ] Zero `unreachable!()` in non-test code under `crates/ratatosk-engine/src/command/`
  - [ ] Zero `expect()` in non-test, non-benchmark code under `crates/ratatosk-engine/src/command/`
  - [ ] Each replaced site has either an error return or `debug_assert!` + error return
  - [ ] `cargo test -p ratatosk-engine` passes
  - [ ] `cargo clippy -p ratatosk-engine -- -D warnings` passes

  **Agent-Executed QA Scenarios**:

  ```
  Scenario: No unreachable/expect in production command code
    Tool: Bash (grep)
    Preconditions: Changes applied
    Steps:
      1. Run: grep -rn 'unreachable!()' crates/ratatosk-engine/src/command/ --include='*.rs' | grep -v '#\[cfg(test)\]' | grep -v 'mod tests'
      2. Run: grep -rn '\.expect(' crates/ratatosk-engine/src/command/ --include='*.rs' | grep -v '#\[cfg(test)\]' | grep -v 'mod tests' | grep -v 'bench'
      3. Assert: both commands produce zero output (after filtering test/bench code)
    Expected Result: No panic-capable code in production paths
    Evidence: grep output captured (should be empty)
  ```

  **Commit**: YES
  - Message: `fix(engine): replace unreachable!/expect() with proper error handling in command dispatch`
  - Files: `crates/ratatosk-engine/src/command/cmd_key.rs`, `crates/ratatosk-engine/src/command/cmd_string.rs`, `crates/ratatosk-engine/src/command/cmd_set.rs`, `crates/ratatosk-engine/src/command/mod.rs`
  - Pre-commit: `cargo test -p ratatosk-engine && cargo clippy -p ratatosk-engine -- -D warnings`

---

- [x] 7. Client read timeout (slowloris protection)

  **What to do**:
  - In `client.rs`, wrap the client read loop with a `tokio::time::timeout`:
    - Add a configurable idle timeout (default 300 seconds — matches Redis `timeout 0` but we want a safe default)
    - Location: `client.rs:302` area — the main `stream.read_buf()` call
    - When timeout fires: close the connection cleanly with a log message
    - Pattern: `match tokio::time::timeout(idle_duration, stream.read_buf(&mut buf)).await { ... }`
  - Add `client_timeout_sec` to `ServerConfig` with env var `RATATOSK_CLIENT_TIMEOUT` (default: 0 = disabled, matching Redis default; but document that a nonzero value like 300 is recommended for production)
  - Add test that verifies timeout fires on idle connection

  **Must NOT do**:
  - Don't timeout connections that are actively in Pub/Sub subscribe mode — they're expected to be idle
  - Don't set an aggressive default — 0 (disabled) matches Redis. Document the recommendation.
  - Don't add complexity for partial-read timeouts — just idle timeout between commands

  **Recommended Agent Profile**:
  - **Category**: `quick`
    - Reason: Single file change + config addition
  - **Skills**: []
  - **Skills Evaluated but Omitted**:
    - None relevant

  **Parallelization**:
  - **Can Run In Parallel**: YES
  - **Parallel Group**: Wave 1 (with Tasks 1, 2, 3, 5, 6)
  - **Blocks**: Task 12
  - **Blocked By**: None

  **References**:

  **Pattern References**:
  - `crates/ratatosk-server/src/client.rs:302` — The bare `stream.read_buf()` that needs timeout wrapping
  - `crates/ratatosk-server/src/client.rs:1-636` — Full client handler; understand the read loop structure before modifying
  - `crates/ratatosk-server/src/config.rs:9-16` — `ServerConfig` struct; add `client_timeout_sec: u64` field here
  - `crates/ratatosk-server/src/config.rs:31-92` — `from_env()` method; add `RATATOSK_CLIENT_TIMEOUT` parsing here

  **API/Type References**:
  - `tokio::time::timeout` — wraps a future with a deadline
  - `ClientIoLimits` struct in `client.rs` — may need to add timeout field here if that's how config reaches the client handler

  **WHY Each Reference Matters**:
  - `client.rs:302` is the exact vulnerability point — bare read with no timeout
  - `config.rs` is where the timeout value must be configurable
  - `ClientIoLimits` is the mechanism for passing server config to client handlers

  **Acceptance Criteria**:

  - [ ] `RATATOSK_CLIENT_TIMEOUT` env var is parsed in `ServerConfig`
  - [ ] When timeout > 0, idle clients are disconnected after `timeout` seconds
  - [ ] When timeout = 0, no timeout is applied (Redis-compatible default)
  - [ ] `cargo test -p ratatosk-server` passes
  - [ ] `cargo clippy -p ratatosk-server -- -D warnings` passes

  **Agent-Executed QA Scenarios**:

  ```
  Scenario: Client timeout disconnects idle connection
    Tool: Bash (cargo test)
    Preconditions: Test written with short timeout (e.g., 1 second)
    Steps:
      1. Run: cargo test -p ratatosk-server -- client_timeout 2>&1
      2. Assert: test passes
    Expected Result: Idle connection is closed after timeout
    Evidence: Test output captured
  ```

  **Commit**: YES
  - Message: `feat(server): add configurable client read timeout for slowloris protection`
  - Files: `crates/ratatosk-server/src/client.rs`, `crates/ratatosk-server/src/config.rs`
  - Pre-commit: `cargo test -p ratatosk-server && cargo clippy -p ratatosk-server -- -D warnings`

---

### Wave 2 — Counters, Config, Clock (After Wave 1)

---

- [x] 4. Persistence foundation: dependency, config extension, ServerState fields

  **What to do**:
  - Add `ratatosk-persist = { workspace = true }` to `crates/ratatosk-server/Cargo.toml` dependencies
  - Extend `ServerConfig` with persistence fields:
    - `dir: PathBuf` (default: `"."`) — env: `RATATOSK_DIR`
    - `dbfilename: String` (default: `"dump.rdb"`) — env: `RATATOSK_DBFILENAME`
    - `appendonly: bool` (default: `false`) — env: `RATATOSK_APPENDONLY`
    - `appendfsync: String` (default: `"everysec"`) — env: `RATATOSK_APPENDFSYNC` (values: `always`, `everysec`, `no`)
  - Add persistence-related fields to `ServerState` (or a companion struct):
    - `rdb_save_in_progress: bool`
    - `last_rdb_save_status: Option<Result<(), String>>`
    - `aof_enabled: bool`
  - Ensure `ConfigState` in `keyspace.rs` can also surface these values for `CONFIG GET`/`CONFIG SET`
  - Add tests for config parsing of new env vars

  **Must NOT do**:
  - Don't wire the actual persistence logic yet — that's Task 8
  - Don't add AOF rewrite config fields (excluded from scope)
  - Don't make persistence mandatory — defaults should work without any env vars set

  **Recommended Agent Profile**:
  - **Category**: `unspecified-high`
    - Reason: Touches Cargo.toml, config.rs, keyspace.rs — needs understanding of how config flows through the system
  - **Skills**: []
  - **Skills Evaluated but Omitted**:
    - None relevant

  **Parallelization**:
  - **Can Run In Parallel**: YES
  - **Parallel Group**: Wave 2 (with Tasks 9, 10)
  - **Blocks**: Task 8
  - **Blocked By**: Task 1 (both touch Cargo.toml — serialize to avoid conflicts)

  **References**:

  **Pattern References**:
  - `crates/ratatosk-server/Cargo.toml:12-28` — Current dependencies. Add `ratatosk-persist` here.
  - `crates/ratatosk-server/src/config.rs:9-16` — `ServerConfig` struct. Add persistence fields.
  - `crates/ratatosk-server/src/config.rs:31-92` — `from_env()`. Add parsing for new env vars.
  - `crates/ratatosk-engine/src/keyspace.rs:1526-1540` — `ServerState` struct. Add persistence state fields.

  **API/Type References**:
  - `ratatosk-persist` crate — check its public API for types needed (FsyncPolicy enum, etc.)
  - `ConfigState` in `keyspace.rs` — check how `CONFIG GET`/`CONFIG SET` reads values

  **Documentation References**:
  - `CLAUDE.md` → "Config: 파일 파싱 + 환경변수 override. CONFIG SET/CONFIG GET 런타임 변경"

  **WHY Each Reference Matters**:
  - `Cargo.toml` must have the persist dependency before Task 8 can use it
  - `ServerConfig` is how persistence paths reach the event loop
  - `ConfigState` is how CONFIG GET/SET exposes persistence config at runtime

  **Acceptance Criteria**:

  - [ ] `ratatosk-persist` is in server's Cargo.toml
  - [ ] `cargo build -p ratatosk-server` succeeds with new dependency
  - [ ] `ServerConfig` has `dir`, `dbfilename`, `appendonly`, `appendfsync` fields
  - [ ] Default config works without any persistence env vars
  - [ ] `cargo test -p ratatosk-server` passes (including config parsing tests)

  **Agent-Executed QA Scenarios**:

  ```
  Scenario: Default config compiles and has persistence defaults
    Tool: Bash (cargo test)
    Preconditions: Changes applied
    Steps:
      1. Run: cargo test -p ratatosk-server -- config 2>&1
      2. Assert: tests pass
      3. Assert: test verifies default dir is "." and dbfilename is "dump.rdb"
    Expected Result: Persistence config defaults work
    Evidence: Test output captured
  ```

  **Commit**: YES
  - Message: `feat(server): add ratatosk-persist dependency and persistence config fields`
  - Files: `crates/ratatosk-server/Cargo.toml`, `crates/ratatosk-server/src/config.rs`, `crates/ratatosk-engine/src/keyspace.rs`
  - Pre-commit: `cargo test && cargo clippy -- -D warnings`

---

- [x] 9. StatsState counters + accurate INFO output

  **What to do**:
  - Extend `StatsState` in `keyspace.rs` with real counters:
    - `connected_clients: u64` — increment on connect, decrement on disconnect (need hook from server)
    - `total_connections_received: u64` — already tracked via `next_client_id`, but add explicit counter
    - `total_net_input_bytes: u64` — increment in client read path
    - `total_net_output_bytes: u64` — increment in client write path
    - `evicted_keys: u64` — increment in eviction.rs
    - `expired_keys: u64` — increment in expiry.rs
    - `keyspace_hits: u64` — increment on successful key lookup
    - `keyspace_misses: u64` — increment on failed key lookup
    - `instantaneous_ops_per_sec: u64` — calculate from `total_commands_processed` delta in server_cron
  - Fix `cmd_server.rs` INFO sections:
    - `INFO clients`: Replace hardcoded `connected_clients:1` with `state.stats.connected_clients`
    - `INFO stats`: Replace hardcoded `instantaneous_ops_per_sec:0` and `total_net_input_bytes:0` with real values
    - `INFO keyspace`: Generate real db0..dbN entries with key counts
    - `INFO persistence`: Add `rdb_last_save_time`, `rdb_last_bgsave_status` from state
  - For `instantaneous_ops_per_sec`: store previous `total_commands_processed` in StatsState, compute delta in `server_cron`, divide by time interval
  - For network bytes: the counter increment happens in `client.rs` (server crate), but `StatsState` is in engine crate. Solution: `StatsState` has the fields, server crate increments them via `&mut ServerState` after each read/write.

  **Must NOT do**:
  - Don't add new INFO sections (memory, replication, etc.) — only fix existing hardcoded ones
  - Don't add per-command latency histograms — that's a separate feature
  - Don't use AtomicU64 — `StatsState` is behind `Mutex<ServerState>`, plain `u64` is correct

  **Recommended Agent Profile**:
  - **Category**: `unspecified-high`
    - Reason: Touches keyspace.rs (large file) and cmd_server.rs (1000 lines), needs careful counter placement
  - **Skills**: []
  - **Skills Evaluated but Omitted**:
    - None relevant

  **Parallelization**:
  - **Can Run In Parallel**: YES
  - **Parallel Group**: Wave 2 (with Tasks 4, 10)
  - **Blocks**: Task 12
  - **Blocked By**: None (logically independent, but placed in Wave 2 to reduce Wave 1 size)

  **References**:

  **Pattern References**:
  - `crates/ratatosk-engine/src/keyspace.rs:1018-1041` — Current `StatsState` with `total_commands_processed` and `last_save_unix_sec`. Extend this struct.
  - `crates/ratatosk-engine/src/command/cmd_server.rs:828-830` — Hardcoded `connected_clients:1`. Replace with real counter.
  - `crates/ratatosk-engine/src/command/cmd_server.rs:844-846` — Hardcoded `instantaneous_ops_per_sec:0`, `total_net_input_bytes:0`. Replace.
  - `crates/ratatosk-engine/src/eviction.rs` — Where `evicted_keys` counter should be incremented
  - `crates/ratatosk-engine/src/expiry.rs` — Where `expired_keys` counter should be incremented
  - `crates/ratatosk-server/src/event_loop.rs:142-167` — `server_cron()` — add ops/sec calculation here

  **API/Type References**:
  - `StatsState::mark_command_processed()` — existing pattern for counter increment. Follow same style.
  - `ServerState` methods — check for existing getter patterns to follow

  **WHY Each Reference Matters**:
  - `StatsState` is the struct to extend — all counters live here
  - `cmd_server.rs:828-846` are the EXACT hardcoded lines to fix
  - `server_cron` is where periodic calculations (ops/sec) should happen

  **Acceptance Criteria**:

  - [ ] `INFO clients` returns real `connected_clients` count (not hardcoded 1)
  - [ ] `INFO stats` returns real `instantaneous_ops_per_sec` (not hardcoded 0)
  - [ ] `INFO stats` returns real `total_net_input_bytes` (not hardcoded 0)
  - [ ] `INFO keyspace` returns real db key counts
  - [ ] `evicted_keys` and `expired_keys` counters increment correctly
  - [ ] `cargo test -p ratatosk-engine` passes
  - [ ] `cargo clippy -- -D warnings` passes

  **Agent-Executed QA Scenarios**:

  ```
  Scenario: INFO returns real connected_clients count
    Tool: Bash (cargo test)
    Preconditions: Test creates ServerState with known connected client count
    Steps:
      1. Run: cargo test -p ratatosk-engine -- info_clients_real 2>&1
      2. Assert: test passes
      3. Test verifies INFO output contains actual count, not "connected_clients:1"
    Expected Result: Real client count in INFO output
    Evidence: Test output captured

  Scenario: Expired keys counter increments
    Tool: Bash (cargo test)
    Preconditions: Test sets key with TTL, runs expiry cycle
    Steps:
      1. Run: cargo test -p ratatosk-engine -- expired_keys_counter 2>&1
      2. Assert: test passes
    Expected Result: Counter tracks expired keys accurately
    Evidence: Test output captured
  ```

  **Commit**: YES
  - Message: `feat(engine): add real stats counters and fix hardcoded INFO output`
  - Files: `crates/ratatosk-engine/src/keyspace.rs`, `crates/ratatosk-engine/src/command/cmd_server.rs`, `crates/ratatosk-engine/src/eviction.rs`, `crates/ratatosk-engine/src/expiry.rs`, `crates/ratatosk-server/src/event_loop.rs`
  - Pre-commit: `cargo test && cargo clippy -- -D warnings`

---

- [x] 10. Clock safety: Instant for durations, SystemTime for display only

  **What to do**:
  - In `ratatosk-core/src/time.rs`:
    - Keep `now_ms()` and `now_sec()` (wall-clock) for: timestamps displayed to users (OBJECT IDLETIME, DEBUG SLEEP, slowlog timestamp), RDB/AOF metadata
    - Add `monotonic_ms() -> u64` using `Instant` (or `coarsetime` for reduced syscall overhead) for: TTL/expiry deadlines, blocking command timeouts, ops/sec interval calculations
    - Document clearly in the module: "Use `monotonic_*` for durations and deadlines. Use `now_*` only for display/persistence timestamps."
  - In `expiry.rs`:
    - The expiry system stores `expire_at_ms` as wall-clock timestamps (these are persisted in RDB, so must remain wall-clock for compatibility)
    - The COMPARISON in `active_expire_cycle` should use `now_ms()` (wall-clock) since `expire_at_ms` is wall-clock — this is actually CORRECT for expiry
    - The REAL fix: Add a startup adjustment if server detects clock jumped backward (log warning, don't panic)
  - In `client.rs`:
    - Client blocking deadlines (BLPOP timeout etc.) should use `Instant` for the deadline, not SystemTime
    - `client.rs:57` — check what's stored and whether it's a deadline calculation
  - In `event_loop.rs`:
    - ops/sec calculation in server_cron should use `Instant::elapsed()` not SystemTime delta

  **Must NOT do**:
  - Don't create a full `Clock` trait abstraction — that's over-engineering for this fix
  - Don't change `expire_at_ms` storage format — it's wall-clock and persisted in RDB (changing would break compatibility)
  - Don't change `started_at_ms` in ServerState — it's a display value
  - Don't add `coarsetime` dependency unless there's clear evidence of syscall overhead

  **Recommended Agent Profile**:
  - **Category**: `unspecified-low`
    - Reason: Targeted changes in time.rs, expiry.rs, client.rs. Small scope but needs careful reasoning about which clocks to use where.
  - **Skills**: []
  - **Skills Evaluated but Omitted**:
    - None relevant

  **Parallelization**:
  - **Can Run In Parallel**: YES
  - **Parallel Group**: Wave 2 (with Tasks 4, 9)
  - **Blocks**: Task 12
  - **Blocked By**: None

  **References**:

  **Pattern References**:
  - `crates/ratatosk-core/src/time.rs:1-42` — Current `now_ms()` and `now_sec()` using `SystemTime`. Add `monotonic_ms()` here.
  - `crates/ratatosk-engine/src/expiry.rs:63-70` — Expiry comparison using `now_ms()`. This is actually correct (expire_at_ms is wall-clock). Add clock-jump detection warning.
  - `crates/ratatosk-server/src/client.rs:57` — Check what's stored for blocking deadline.
  - `crates/ratatosk-server/src/event_loop.rs:142-167` — server_cron interval timing.

  **Documentation References**:
  - `CLAUDE.md` → "SystemTime (wall-clock) used for TTL/expiry/blocking deadlines — NTP clock jumps break key expiration"

  **WHY Each Reference Matters**:
  - `time.rs` is the central time module — all time functions must be here
  - `expiry.rs` uses `now_ms()` to compare against wall-clock deadlines — actually correct, but needs clock-jump safety
  - `client.rs:57` likely stores a blocking timeout as wall-clock — should use Instant

  **Acceptance Criteria**:

  - [ ] `monotonic_ms()` function exists in `ratatosk-core/src/time.rs`
  - [ ] Client blocking deadlines use `Instant` not `SystemTime`
  - [ ] Module-level doc comment in `time.rs` explains when to use each
  - [ ] `cargo test` passes
  - [ ] `cargo clippy -- -D warnings` passes

  **Agent-Executed QA Scenarios**:

  ```
  Scenario: Monotonic clock function exists and is monotonic
    Tool: Bash (cargo test)
    Preconditions: Test written in ratatosk-core
    Steps:
      1. Run: cargo test -p ratatosk-core -- monotonic 2>&1
      2. Assert: test passes
      3. Test verifies two successive calls return non-decreasing values
    Expected Result: Monotonic time never goes backward
    Evidence: Test output captured
  ```

  **Commit**: YES
  - Message: `fix(core): add monotonic clock, use Instant for durations and deadlines`
  - Files: `crates/ratatosk-core/src/time.rs`, `crates/ratatosk-server/src/client.rs`, `crates/ratatosk-engine/src/expiry.rs`
  - Pre-commit: `cargo test && cargo clippy -- -D warnings`

---

### Wave 3 — Persistence Wiring + Memory (After Task 4)

---

- [x] 8. Persistence wiring: startup load, BGSAVE, SAVE, shutdown flush

  **What to do**:
  This is the largest and most critical task. Wire `ratatosk-persist` into the server lifecycle:

  **A) Startup load** (in `event_loop.rs::run()`, before the accept loop):
  1. Check if RDB file exists at `config.dir/config.dbfilename`
  2. If exists: call `ratatosk_persist::rdb::loader::load(path)` → get loaded keyspace data
  3. Populate `ServerState` dbs from loaded data (need a `ServerState::load_from_rdb(data)` method)
  4. If AOF enabled AND AOF files exist: replay AOF on top of RDB state
  5. Log: `tracing::info!(keys = count, "loaded RDB snapshot")` or `tracing::info!("no RDB file found, starting empty")`

  **B) BGSAVE** (background save):
  1. Strategy: Clone the `ServerState`'s db data (it's `Vec<HashMap<Bytes, StoredValue>>` — `Bytes::clone()` is cheap RC bump, `StoredValue` needs `Clone`)
  2. `tokio::task::spawn_blocking(move || { rdb::saver::save(&snapshot, path) })`
  3. Track status in `ServerState`: `rdb_save_in_progress`, `last_rdb_save_status`, `last_rdb_save_time`
  4. Wire to SIGUSR1 handler (replace the stub at `event_loop.rs:296`)
  5. Wire to BGSAVE command in cmd_server.rs (check existing handler — it likely returns "not implemented")

  **C) SAVE** (foreground save):
  1. Synchronous save on the current thread (blocks event loop — that's intentional, same as Redis)
  2. Wire to SAVE command in cmd_server.rs

  **D) AOF** (if appendonly=true):
  1. After each successful write command in `execute()`, append the command to AOF
  2. Respect `appendfsync` policy (always/everysec/no)
  3. AOF writer state lives alongside the event loop (not inside ServerState — it's I/O, not data)

  **E) Shutdown flush**:
  1. In the shutdown path (after `break` in event_loop, before lazy-free shutdown):
  2. If AOF enabled: flush and fsync the AOF
  3. Optionally: trigger a final RDB save (configurable, default: no — too slow for graceful shutdown)
  4. Log: `tracing::info!("persistence flushed before shutdown")`

  **Must NOT do**:
  - Don't implement AOF rewrite (excluded from scope)
  - Don't implement automatic periodic BGSAVE (save-on-N-changes-in-M-seconds) — just manual BGSAVE/SAVE + SIGUSR1
  - Don't block the event loop for BGSAVE — must be async via spawn_blocking
  - Don't add fork-based save — use clone + spawn_blocking (simpler, safe with Rust ownership)

  **Recommended Agent Profile**:
  - **Category**: `deep`
    - Reason: Complex integration task spanning 3 crates, needs understanding of event loop lifecycle, persistence APIs, and data flow
  - **Skills**: []
  - **Skills Evaluated but Omitted**:
    - None relevant — deep Rust systems programming

  **Parallelization**:
  - **Can Run In Parallel**: YES (with Task 11)
  - **Parallel Group**: Wave 3
  - **Blocks**: Task 12
  - **Blocked By**: Task 4 (persistence config and dependency)

  **References**:

  **Pattern References**:
  - `crates/ratatosk-server/src/event_loop.rs:191-193` — `ServerState::with_default_dbs()` — this is where RDB load should happen instead
  - `crates/ratatosk-server/src/event_loop.rs:295-297` — SIGUSR1 stub: `"persistence not yet implemented"` — wire BGSAVE here
  - `crates/ratatosk-server/src/event_loop.rs:299-309` — Shutdown path — add persistence flush before lazy-free shutdown
  - `crates/ratatosk-persist/src/rdb/loader.rs` — RDB loading API. Check `pub fn load(...)` signature.
  - `crates/ratatosk-persist/src/rdb/saver.rs` — RDB saving API. Check `pub fn save(...)` signature.
  - `crates/ratatosk-persist/src/aof/writer.rs` — AOF writer API. Check constructor and append methods.
  - `crates/ratatosk-persist/src/aof/recovery.rs` — AOF replay API for startup.
  - `crates/ratatosk-engine/src/command/mod.rs` — `execute()` function — where AOF command interception should hook in (after successful write command execution)

  **API/Type References**:
  - `StoredValue` in `keyspace.rs:267-275` — has `#[derive(Clone)]` — Bytes clone is RC bump (cheap)
  - `ServerState::new()` / `with_default_dbs()` — may need a `from_loaded_data()` constructor

  **Documentation References**:
  - `CLAUDE.md` → "Recovery: RDB 로드 → AOF replay. 둘 다 없으면 빈 상태로 시작"
  - `CLAUDE.md` → "RdbLoader: 파일 → keyspace 복원. CRC64 검증. 손상 감지 시 abort."
  - `CLAUDE.md` → "AOF: Manifest 기반: BASE file + INCR files"

  **WHY Each Reference Matters**:
  - `event_loop.rs:191` is where the empty-start happens — must replace with conditional load
  - `event_loop.rs:295-297` is the SIGUSR1 stub waiting to be wired
  - Persist crate APIs must be understood to integrate correctly
  - `execute()` is the AOF interception point

  **Acceptance Criteria**:

  - [ ] Server loads RDB at startup if file exists
  - [ ] Server starts empty if no RDB file (existing behavior preserved)
  - [ ] BGSAVE command creates RDB file at configured path
  - [ ] SAVE command creates RDB file synchronously
  - [ ] SIGUSR1 triggers BGSAVE
  - [ ] AOF appends write commands when appendonly=true
  - [ ] Shutdown flushes AOF if enabled
  - [ ] `cargo test` passes
  - [ ] `cargo clippy -- -D warnings` passes

  **Agent-Executed QA Scenarios**:

  ```
  Scenario: BGSAVE creates RDB and reload restores data
    Tool: Bash (integration test)
    Preconditions: Server built, test creates ServerState with data
    Steps:
      1. cargo test -p ratatosk-server -- persistence_roundtrip 2>&1
      2. Test: create state → insert keys → save RDB → create new state → load RDB → verify keys match
      3. Assert: test passes
    Expected Result: RDB roundtrip preserves all data
    Evidence: Test output captured

  Scenario: Server starts empty when no RDB exists
    Tool: Bash (cargo test)
    Preconditions: Test ensures no RDB file at path
    Steps:
      1. cargo test -p ratatosk-server -- startup_no_rdb 2>&1
      2. Assert: test passes, state has 0 keys
    Expected Result: Clean start without persistence files
    Evidence: Test output captured

  Scenario: SIGUSR1 triggers background save
    Tool: Bash (cargo test)
    Preconditions: Test with server running
    Steps:
      1. cargo test -p ratatosk-server -- sigusr1_bgsave 2>&1
      2. Assert: test passes
    Expected Result: Signal triggers RDB save
    Evidence: Test output captured
  ```

  **Commit**: YES
  - Message: `feat(server): wire persistence — startup RDB/AOF load, BGSAVE, SAVE, shutdown flush`
  - Files: `crates/ratatosk-server/src/event_loop.rs`, `crates/ratatosk-server/src/main.rs`, `crates/ratatosk-engine/src/keyspace.rs`, `crates/ratatosk-engine/src/command/cmd_server.rs`, `crates/ratatosk-engine/src/command/mod.rs`
  - Pre-commit: `cargo test && cargo clippy -- -D warnings`

---

- [x] 11. Optimize estimate_used_memory (O(n) → cached)

  **What to do**:
  - Current `estimate_used_memory()` in `eviction.rs` does a full O(n) keyspace scan every call
  - It's called from `server_cron()` at 10Hz — scanning entire keyspace 10 times/second is excessive
  - Fix: Track memory delta incrementally:
    - Add `estimated_memory_bytes: u64` to `ServerState` or `StatsState`
    - Increment on key insert (estimate size of new key+value)
    - Decrement on key delete
    - `estimate_used_memory()` just reads the cached value
  - Alternative (simpler): Only call `estimate_used_memory()` when `maxmemory > 0`, and reduce frequency (every 100ms instead of every tick). Use the cached result for intermediate ticks.
  - The incremental approach is more accurate; the frequency-reduction approach is simpler. **Recommend**: frequency reduction (cache the result for N ticks) as it's less invasive.

  **Must NOT do**:
  - Don't change the eviction algorithm itself
  - Don't add per-key memory tracking overhead (that would slow down every operation)
  - Don't remove `estimate_used_memory()` entirely — it's needed for INFO memory and eviction

  **Recommended Agent Profile**:
  - **Category**: `quick`
    - Reason: Small optimization in eviction.rs — cache the result, reduce call frequency
  - **Skills**: []
  - **Skills Evaluated but Omitted**:
    - None relevant

  **Parallelization**:
  - **Can Run In Parallel**: YES (with Task 8)
  - **Parallel Group**: Wave 3
  - **Blocks**: Task 12
  - **Blocked By**: None (logically after Task 9 for counter placement, but can run independently)

  **References**:

  **Pattern References**:
  - `crates/ratatosk-engine/src/eviction.rs` — `estimate_used_memory()` function. Read the full implementation to understand what it scans.
  - `crates/ratatosk-server/src/event_loop.rs:142-167` — `server_cron()` calls `estimate_used_memory()` — this is where the caching/frequency reduction should be applied.

  **WHY Each Reference Matters**:
  - `eviction.rs` is where the O(n) scan lives — must understand what it measures
  - `server_cron` is the caller — frequency reduction happens here

  **Acceptance Criteria**:

  - [ ] `estimate_used_memory()` is not called on every cron tick (cached or frequency-reduced)
  - [ ] Memory estimation remains accurate enough for eviction decisions
  - [ ] `cargo test -p ratatosk-engine` passes
  - [ ] `cargo clippy -- -D warnings` passes

  **Agent-Executed QA Scenarios**:

  ```
  Scenario: Memory estimation is cached between cron ticks
    Tool: Bash (cargo test)
    Preconditions: Test written verifying cache behavior
    Steps:
      1. cargo test -p ratatosk-engine -- memory_estimate_cache 2>&1
      2. Assert: test passes
    Expected Result: Repeated calls within cache window return same value without full scan
    Evidence: Test output captured
  ```

  **Commit**: YES
  - Message: `perf(engine): cache estimate_used_memory result to avoid O(n) scan every cron tick`
  - Files: `crates/ratatosk-engine/src/eviction.rs`, `crates/ratatosk-server/src/event_loop.rs`
  - Pre-commit: `cargo test && cargo clippy -- -D warnings`

---

### Wave 4 — Integration & Final Verification

---

- [x] 12. Integration tests + final verification

  **What to do**:
  - Run full test suite: `cargo test`
  - Run clippy: `cargo clippy -- -D warnings`
  - Run release build: `cargo build --release`
  - Verify --version: `./target/release/ratatosk --version`
  - Verify startup banner: `RUST_LOG=info timeout 2 ./target/release/ratatosk 2>&1`
  - Verify no remaining `unreachable!()` / `expect()` in production code (grep check)
  - Verify no hardcoded values in INFO output (grep for `connected_clients:1`, `instantaneous_ops_per_sec:0`)
  - Write an end-to-end integration test (if not already covered):
    - Start server → connect via TCP → send PING → get PONG
    - SET key → GET key → verify value
    - INFO → verify connected_clients shows real count
    - BGSAVE → verify RDB file created
  - Fix any issues found during integration

  **Must NOT do**:
  - Don't add new features during integration — only fix regressions
  - Don't skip any verification step

  **Recommended Agent Profile**:
  - **Category**: `unspecified-high`
    - Reason: Integration testing across entire codebase, may need to fix issues from earlier tasks
  - **Skills**: []
  - **Skills Evaluated but Omitted**:
    - `playwright`: No browser involved

  **Parallelization**:
  - **Can Run In Parallel**: NO
  - **Parallel Group**: Wave 4 (sequential, final)
  - **Blocks**: None (final task)
  - **Blocked By**: ALL previous tasks

  **References**:

  **All previous task outputs** — this task validates everything.

  **Acceptance Criteria**:

  - [ ] `cargo test` — ALL tests pass (0 failures)
  - [ ] `cargo clippy -- -D warnings` — 0 warnings
  - [ ] `cargo build --release` — succeeds
  - [ ] `./target/release/ratatosk --version` — prints version + git hash
  - [ ] No `unreachable!()` in production command code (grep verified)
  - [ ] No hardcoded `connected_clients:1` or `instantaneous_ops_per_sec:0` (grep verified)
  - [ ] End-to-end test passes (if written)

  **Agent-Executed QA Scenarios**:

  ```
  Scenario: Full build and test suite passes
    Tool: Bash
    Preconditions: All previous tasks completed
    Steps:
      1. cargo test 2>&1
      2. Assert: 0 failures
      3. cargo clippy -- -D warnings 2>&1
      4. Assert: 0 warnings
      5. cargo build --release 2>&1
      6. Assert: build succeeds
    Expected Result: Clean build, all tests pass, no clippy warnings
    Evidence: Build and test output captured

  Scenario: --version prints build metadata
    Tool: Bash
    Preconditions: Release build exists
    Steps:
      1. ./target/release/ratatosk --version
      2. Assert: output contains version number and git hash
    Expected Result: Version info available
    Evidence: stdout captured

  Scenario: No hardcoded INFO values remain
    Tool: Bash (grep)
    Preconditions: All changes applied
    Steps:
      1. grep -rn 'connected_clients:1' crates/ --include='*.rs' | grep -v test | grep -v bench
      2. grep -rn 'instantaneous_ops_per_sec:0' crates/ --include='*.rs' | grep -v test | grep -v bench
      3. Assert: both produce zero results
    Expected Result: No hardcoded stats in production code
    Evidence: grep output captured (should be empty)

  Scenario: No production panics remain
    Tool: Bash (grep)
    Preconditions: All changes applied
    Steps:
      1. grep -rn 'unreachable!()' crates/ratatosk-engine/src/command/ --include='*.rs' | grep -v test
      2. grep -rn '\.expect(' crates/ratatosk-engine/src/command/ --include='*.rs' | grep -v test | grep -v bench
      3. Assert: both produce zero results (or only in explicitly acceptable locations)
    Expected Result: No panic-capable code in production paths
    Evidence: grep output captured
  ```

  **Commit**: YES (if any fixes were needed)
  - Message: `test(all): add integration tests and fix integration issues`
  - Files: Any files that needed fixing
  - Pre-commit: `cargo test && cargo clippy -- -D warnings`

---

## Commit Strategy

| After Task | Message | Key Files | Verification |
|------------|---------|-----------|--------------|
| 1 | `feat(server): add build.rs, --version flag, preserve backtrace symbols` | build.rs, main.rs, Cargo.toml | cargo test + cargo build --release |
| 2 | `feat(server): add panic hook and startup banner logging` | main.rs | cargo test |
| 3 | `fix(all): add .context() to error propagation at public API boundaries` | client.rs, loader.rs, saver.rs, writer.rs | cargo test |
| 4 | `feat(server): add ratatosk-persist dependency and persistence config fields` | Cargo.toml, config.rs, keyspace.rs | cargo test + cargo build |
| 5 | `feat(engine): add security audit logging for AUTH, FLUSHALL, FLUSHDB, CONFIG SET` | cmd_acl.rs, cmd_server.rs | cargo test |
| 6 | `fix(engine): replace unreachable!/expect() with proper error handling` | cmd_key.rs, cmd_string.rs, cmd_set.rs, mod.rs | cargo test + grep verification |
| 7 | `feat(server): add configurable client read timeout` | client.rs, config.rs | cargo test |
| 8 | `feat(server): wire persistence — startup RDB/AOF load, BGSAVE, SAVE, shutdown flush` | event_loop.rs, keyspace.rs, cmd_server.rs | cargo test |
| 9 | `feat(engine): add real stats counters and fix hardcoded INFO output` | keyspace.rs, cmd_server.rs, eviction.rs, expiry.rs | cargo test |
| 10 | `fix(core): add monotonic clock, use Instant for durations and deadlines` | time.rs, client.rs, expiry.rs | cargo test |
| 11 | `perf(engine): cache estimate_used_memory result` | eviction.rs, event_loop.rs | cargo test |
| 12 | `test(all): integration tests and final verification` | various | cargo test + full verification |

---

## Success Criteria

### Verification Commands
```bash
cargo test                                    # All tests pass
cargo clippy -- -D warnings                   # Zero warnings
cargo build --release -p ratatosk-server      # Release builds
./target/release/ratatosk --version           # Prints version + git hash
```

### Final Checklist
- [x] All 12 tasks completed and committed
- [x] Zero `unreachable!()` / `expect()` in production command code
- [x] Zero hardcoded values in INFO output
- [x] Panic hook installed
- [x] Startup banner logged
- [x] Persistence loads RDB at startup
- [x] BGSAVE/SAVE commands work
- [x] AUTH failures are logged
- [x] FLUSHALL/FLUSHDB are logged
- [x] Client read timeout is configurable
- [x] Clock uses Instant for durations
- [x] Error messages have context
- [x] strip = "debuginfo" preserves backtrace symbols
- [x] All tests pass
- [x] All clippy warnings resolved
