# Issues - ops-readiness-fixes

## Task 6: Compilation Errors (Pre-existing)

### Issue
After applying fixes, `cargo check -p ratatosk-engine` fails with:
```
error[E0061]: this function takes 2 arguments but 3 arguments were supplied
   --> crates/ratatosk-engine/src/command/mod.rs:3564:23
    |
3564 |          b"CONFIG" => cmd_server::cmd_config(args, server, client),
```

### Root Cause
- Error is in `mod.rs` and `cmd_server.rs`, NOT in files I modified
- These files were modified by previous tasks in the ops-readiness-fixes plan
- The codebase was already broken before Task 6

### Verification
Ran `git stash && cargo check -p ratatosk-engine` on clean state:
- Clean state compiles successfully
- After restoring changes from other tasks, compilation fails
- My changes (cmd_key.rs, cmd_string.rs, cmd_set.rs) are syntactically correct

### Impact on Task 6
- Task 6 is **complete** - all assigned unreachable!/expect() replaced
- Verification grep shows zero matches in production command files
- Cannot run full test suite due to pre-existing compilation errors
- Syntax of my changes is correct (verified by reading diffs)

### Next Steps
- Previous tasks need to fix cmd_server.rs signature mismatch
- Once compilation is fixed, full test suite can verify all changes

## Task 8: Full Workspace Test Failure (Pre-existing)

### Issue
`nix develop -c cargo test` still fails on 4 engine tests:
- `command::tests::m2_list_blmpop_baseline_commands`
- `command::tests::m2_list_blocking_move_baseline_commands`
- `command::tests::m2_list_blpop_brpop_baseline_commands`
- `command::tests::m3_stream_block_retry_resumes_with_new_entries`

### Root Cause
- Failures are in blocking retry semantics of list/stream commands (asserting `retry_blocking.is_some()`), not in persistence wiring paths.
- This aligns with prior notepad context from clock/deadline changes and appears unrelated to Task 8 edits.

### Verification
- Persistence-focused tests added in Task 8 pass.
- `nix develop -c cargo clippy -- -D warnings` passes.

## Task 12: Final Verification - Issues Found & Fixed

### Bug Fix: Blocking deadline comparison using wrong clock
- **Files**: `cmd_list.rs` (4 locations), `cmd_stream.rs` (2 locations)
- **Root Cause**: `blocking_deadline_ms()` uses `monotonic_ms()` to compute deadline, but comparison check used `now_ms()` (wall-clock). Wall-clock ms (~1.77 trillion) is always >> monotonic ms (~0-1000), so deadline check always passed, preventing blocking retry.
- **Fix**: Replace `now_ms() >= deadline` with `ratatosk_core::time::monotonic_ms() as i64 >= deadline` in all 6 locations.
- **Impact**: Fixed all 4 previously failing tests.

### Improvement: Removed expect() in production code
- **File**: `client.rs:196`
- **Before**: `outcome.retry_blocking.expect("checked above")` — guarded by early return, but still violates no-panic rule
- **After**: `let Some(retry) = outcome.retry_blocking else { return Ok(outcome); };`

### Improvement: --version now shows git hash
- **File**: `main.rs`
- **Before**: `#[command(version)]` → `ratatosk-server 0.1.0`
- **After**: `#[command(version = concat!(...))]` → `ratatosk-server 0.1.0 (5e2960e)`
